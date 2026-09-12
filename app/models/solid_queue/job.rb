# frozen_string_literal: true

module SolidQueue
  class Job < Record
    class EnqueueError < StandardError; end

    # Raised when a job's class can't be resolved anymore, typically because it
    # was renamed or removed in a deploy while jobs referencing it were in
    # flight. It subclasses NameError, which is what resolving the class raises.
    class ClassMissingError < NameError
      def self.for(job)
        new("Job class #{job.class_name.inspect} could not be resolved")
      end
    end

    include Executable, Clearable, Recurrable, Batchable

    serialize :arguments, coder: JSON

    class << self
      def enqueue_all(active_jobs)
        # Bulk enqueues bypass ActiveJob#enqueue, so batch membership is captured here
        current_batch_id = Batch.current_batch_id

        active_jobs.each do |job|
          job.scheduled_at ||= Time.current
          job.batch_id = current_batch_id || job.batch_id
        end

        if SolidQueue.sharded?
          active_jobs.group_by { |active_job| shard_for(active_job) }.each do |shard, jobs_in_shard|
            Record.connected_to(shard: shard) { enqueue_all_together(jobs_in_shard) }
          end
        else
          enqueue_all_together(active_jobs)
        end

        active_jobs.count(&:successfully_enqueued?)
      end

      def enqueue(active_job, scheduled_at: Time.current)
        active_job.scheduled_at = scheduled_at

        connected_to_shard_for(active_job) do
          create_from_active_job(active_job).tap do |enqueued_job|
            active_job.provider_job_id = enqueued_job.id if enqueued_job.persisted?
            active_job.successfully_enqueued = enqueued_job.persisted?
          end
        end
      end

      # The shard a job is enqueued in, where it will remain for its whole life.
      # Jobs joining a batch go where the batch lives, read from the batch's
      # identifier, so a batch is unaffected by the shard list changing. Jobs
      # with concurrency controls are distributed by their concurrency key, so
      # that jobs sharing a key land on the same shard and the unique indexes
      # that enforce their limits apply to all of them; while previous_shards
      # is set after a change to the shard list, a moved key keeps routing to
      # its old shard until no live semaphore remains there. Other jobs are
      # distributed uniformly by their Active Job ID; retried and resumed jobs
      # keep it, so they return to their shard.
      def shard_for(active_job)
        if Batch.migrated? && (batch_shard = Batch.shard_from(active_job.try(:batch_id)))
          batch_shard
        elsif active_job.concurrency_key.present?
          shard_for_concurrency_key(active_job.concurrency_key)
        else
          SolidQueue.shard_router.node(active_job.job_id)
        end
      end

      private
        DEFAULT_PRIORITY = 0
        DEFAULT_QUEUE_NAME = "default"

        def shard_for_concurrency_key(concurrency_key)
          shard = SolidQueue.shard_router.node(concurrency_key)
          return shard if SolidQueue.previous_shards.empty?

          previous_shard = SolidQueue.previous_shard_router.node(concurrency_key)
          return shard if previous_shard == shard

          live_semaphore_on?(previous_shard, concurrency_key) ? previous_shard : shard
        end

        def live_semaphore_on?(shard, concurrency_key)
          Record.connected_to(shard: shard) do
            Semaphore.where(key: concurrency_key).where("expires_at > ?", Time.current).exists?
          end
        end

        def connected_to_shard_for(active_job, &block)
          if SolidQueue.sharded?
            Record.connected_to(shard: shard_for(active_job), &block)
          else
            block.call
          end
        end

        def enqueue_all_together(active_jobs)
          active_jobs_by_job_id = active_jobs.index_by(&:job_id)

          transaction do
            jobs = create_all_from_active_jobs(active_jobs)
            prepare_all_for_execution(jobs).each do |enqueued_job|
              active_jobs_by_job_id[enqueued_job.active_job_id].provider_job_id = enqueued_job.id
              active_jobs_by_job_id[enqueued_job.active_job_id].successfully_enqueued = true
            end
          end
        end

        def create_from_active_job(active_job)
          create!(**attributes_from_active_job(active_job))
        rescue ActiveRecord::ActiveRecordError => e
          enqueue_error = EnqueueError.new("#{e.class.name}: #{e.message}").tap do |error|
            error.set_backtrace e.backtrace
          end
          raise enqueue_error
        end

        def create_all_from_active_jobs(active_jobs)
          job_rows = active_jobs.map { |job| attributes_from_active_job(job) }
          insert_all(job_rows)
          where(active_job_id: active_jobs.map(&:job_id)).order(id: :asc)
        end

        def attributes_from_active_job(active_job)
          {
            queue_name: active_job.queue_name || DEFAULT_QUEUE_NAME,
            active_job_id: active_job.job_id,
            priority: active_job.priority || DEFAULT_PRIORITY,
            scheduled_at: active_job.scheduled_at,
            class_name: active_job.class.name,
            arguments: active_job.serialize,
            concurrency_key: active_job.concurrency_key
          }.tap do |attributes|
            # The Active Job level carries the batch's portable identifier; the
            # column keeps the batch's row id, local to the shard both live on
            attributes[:batch_id] = Batch.local_id_for(active_job.batch_id) if Batch.migrated?
          end
        end
    end
  end
end
