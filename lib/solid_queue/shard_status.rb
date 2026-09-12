# frozen_string_literal: true

module SolidQueue
  # Reports what still ties old shards down after the shard list changes:
  # live semaphores for concurrency keys that now route elsewhere and, for
  # shards out of the current split, unfinished batches and pending jobs.
  # `previous_shards` can be removed, and a draining shard disconnected,
  # once its report comes back clear.
  class ShardStatus
    Report = Struct.new(:shard, :moved_key_semaphores, :latest_semaphore_expires_at, :unfinished_batches, :pending_jobs) do
      def clear?
        moved_key_semaphores.zero? && unfinished_batches.zero? && pending_jobs.zero?
      end
    end

    class << self
      def check
        shards_to_check.map { |shard| report_on(shard) }
      end

      def shards_to_check
        SolidQueue.previous_shards | (connected_shards - SolidQueue.shards)
      end

      private
        def connected_shards
          SolidQueue.sharded? ? SolidQueue.connects_to[:shards].keys : []
        end

        def report_on(shard)
          Record.connected_to(shard: shard) do
            moved = live_semaphores_for_moved_keys(shard)
            draining = !SolidQueue.shards.include?(shard)

            Report.new \
              shard,
              moved.count,
              moved.map(&:last).max,
              draining && Batch.migrated? ? Batch.unfinished.count : 0,
              draining ? Job.where(finished_at: nil).count : 0
          end
        end

        def live_semaphores_for_moved_keys(shard)
          Semaphore.where("expires_at > ?", Time.current).pluck(:key, :expires_at).select do |key, _expires_at|
            SolidQueue.shard_router.node(key) != shard
          end
        end
    end
  end
end
