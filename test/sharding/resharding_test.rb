# frozen_string_literal: true

require "test_helper"

# These tests run only when the queue database is sharded, which requires
# setting SOLID_QUEUE_SHARDED when running them:
#   SOLID_QUEUE_SHARDED=1 bin/rails test test/sharding
return unless SolidQueue.sharded?

class ReshardingTest < ActiveSupport::TestCase
  self.use_transactional_tests = false

  SMALL_SPLIT = %i[ queue_shard_one queue_shard_two ]
  FULL_SPLIT = %i[ queue_shard_one queue_shard_two queue_shard_three ]

  teardown do
    SolidQueue.shards = nil
    SolidQueue.previous_shards = nil

    on_each_shard { SolidQueue::BatchExecution.delete_all if SolidQueue::Batch.migrated? }
    on_each_shard { SolidQueue::Job.delete_all }
    on_each_shard { SolidQueue::Batch.delete_all if SolidQueue::Batch.migrated? }
    on_each_shard { SolidQueue::Semaphore.delete_all }
    JobResult.delete_all
  end

  test "a moved concurrency key keeps routing to its old shard while a live semaphore remains there" do
    result = result_with_moved_key
    old_home = old_home_for(result)

    with_split(SMALL_SPLIT) { NonOverlappingUpdateResultJob.perform_later(result, name: "A") }

    with_split(FULL_SPLIT, previous: SMALL_SPLIT) do
      job = NonOverlappingUpdateResultJob.perform_later(result, name: "A")

      assert_equal old_home, SolidQueue::Job.shard_for(job)
      on_shard(old_home) { assert SolidQueue::Job.exists?(active_job_id: job.job_id) }
    end
  end

  test "a moved concurrency key routes to its new home once its old semaphore expires" do
    result = result_with_moved_key
    old_home, new_home = old_home_for(result), new_home_for(result)

    with_split(SMALL_SPLIT) { NonOverlappingUpdateResultJob.perform_later(result, name: "A") }
    on_shard(old_home) { SolidQueue::Semaphore.update_all(expires_at: 1.minute.ago) }

    with_split(FULL_SPLIT, previous: SMALL_SPLIT) do
      job = NonOverlappingUpdateResultJob.perform_later(result, name: "A")

      assert_equal new_home, SolidQueue::Job.shard_for(job)
      on_shard(new_home) { assert SolidQueue::Job.exists?(active_job_id: job.job_id) }
    end
  end

  test "jobs without concurrency keys or batches follow the new split immediately" do
    with_split(FULL_SPLIT, previous: SMALL_SPLIT) do
      jobs = 40.times.map { AddToBufferJob.perform_later("hey") }

      assert_includes jobs.map { |job| SolidQueue::Job.shard_for(job) }.uniq, :queue_shard_three
    end
  end

  test "batches keep their shard when the split changes" do
    skip "Batches schema not installed" unless SolidQueue::Batch.migrated?

    batch = with_split(SMALL_SPLIT) do
      SolidQueue::Batch.enqueue { AddToBufferJob.perform_later("member") }
    end
    batch_shard = SolidQueue::Batch.shard_from(batch.active_job_batch_id)
    assert_includes SMALL_SPLIT, batch_shard

    with_split(FULL_SPLIT, previous: SMALL_SPLIT) do
      batch.enqueue { AddToBufferJob.perform_later("late add") }
    end

    on_shard(batch_shard) do
      assert SolidQueue::Batch.exists?(active_job_batch_id: batch.active_job_batch_id)
      assert_equal 2, SolidQueue::Job.where.not(batch_id: nil).count
    end
  end

  test "retried batch members return to the batch's shard" do
    skip "Batches schema not installed" unless SolidQueue::Batch.migrated?

    member = nil
    batch = with_split(SMALL_SPLIT) do
      SolidQueue::Batch.enqueue { member = AddToBufferJob.perform_later("member") }
    end

    with_split(FULL_SPLIT, previous: SMALL_SPLIT) do
      retried = AddToBufferJob.deserialize(member.serialize)

      assert_equal SolidQueue::Batch.shard_from(batch.active_job_batch_id), SolidQueue::Job.shard_for(retried)
    end
  end

  test "new batches pick their shard from the current split" do
    skip "Batches schema not installed" unless SolidQueue::Batch.migrated?

    with_split(FULL_SPLIT, previous: SMALL_SPLIT) do
      shards = 30.times.map do
        batch = SolidQueue::Batch.enqueue { AddToBufferJob.perform_later("hey") }
        SolidQueue::Batch.shard_from(batch.active_job_batch_id)
      end

      assert_includes shards.uniq, :queue_shard_three
    end
  end

  test "shard status reports moved keys until their semaphores clear" do
    result = result_with_moved_key
    old_home = old_home_for(result)

    with_split(SMALL_SPLIT) { NonOverlappingUpdateResultJob.perform_later(result, name: "A") }

    with_split(FULL_SPLIT, previous: SMALL_SPLIT) do
      report = SolidQueue::ShardStatus.check.find { |candidate| candidate.shard == old_home }
      assert_equal 1, report.moved_key_semaphores
      assert_not report.clear?

      on_shard(old_home) { SolidQueue::Semaphore.delete_all }

      report = SolidQueue::ShardStatus.check.find { |candidate| candidate.shard == old_home }
      assert report.clear?
    end
  end

  test "shard status reports pending work on shards out of the split" do
    job = with_split(SMALL_SPLIT) do
      job = nil
      50.times do
        job = AddToBufferJob.perform_later("hey")
        break if SolidQueue::Job.shard_for(job) == :queue_shard_two
        job = nil
      end
      job
    end
    assert job, "Expected at least one job to land on queue_shard_two"

    with_split(%i[ queue_shard_one ], previous: SMALL_SPLIT) do
      report = SolidQueue::ShardStatus.check.find { |candidate| candidate.shard == :queue_shard_two }

      assert_operator report.pending_jobs, :>, 0
      assert_not report.clear?
    end
  end

  private
    def with_split(current, previous: [])
      SolidQueue.shards = current
      SolidQueue.previous_shards = previous
      yield
    ensure
      SolidQueue.shards = nil
      SolidQueue.previous_shards = nil
    end

    # A JobResult whose concurrency key lands on different shards under the
    # two splits
    def result_with_moved_key
      50.times do
        result = JobResult.create!(queue_name: "default")
        return result if old_home_for(result) != new_home_for(result)

        result.destroy
      end
      flunk "Couldn't find a concurrency key the resplit moves"
    end

    def old_home_for(result)
      SolidQueue.router_for(SMALL_SPLIT).node(concurrency_key_for(result))
    end

    def new_home_for(result)
      SolidQueue.router_for(FULL_SPLIT).node(concurrency_key_for(result))
    end

    def concurrency_key_for(result)
      NonOverlappingUpdateResultJob.new(result, name: "A").concurrency_key
    end
end
