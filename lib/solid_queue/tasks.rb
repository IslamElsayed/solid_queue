namespace :solid_queue do
  desc "Install Solid Queue"
  task :install do
    Rails::Command.invoke :generate, [ "solid_queue:install" ]
  end

  desc "Copy any new Solid Queue migrations to the application"
  task :update do
    Rails::Command.invoke :generate, [ "solid_queue:update" ]
  end

  desc "start solid_queue supervisor to dispatch and process jobs"
  task start: :environment do
    SolidQueue::Supervisor.start
  end

  desc "validate the Solid Queue configuration for the current Rails env without starting any process"
  task check: :environment do
    configuration = SolidQueue::Configuration.new
    exit 1 unless configuration.check
  end

  namespace :shards do
    desc "Report what still ties old shards down after the shard list changed"
    task status: :environment do
      if !SolidQueue.sharded?
        puts "Solid Queue isn't sharded; nothing to report."
      elsif (reports = SolidQueue::ShardStatus.check).empty?
        puts "The current shard list covers every connected shard; nothing to report."
      else
        reports.each do |report|
          if report.clear?
            puts "#{report.shard}: clear"
          else
            parts = []
            if report.moved_key_semaphores > 0
              parts << "#{report.moved_key_semaphores} live semaphores for moved keys (latest expires at #{report.latest_semaphore_expires_at})"
            end
            parts << "#{report.unfinished_batches} unfinished batches" if report.unfinished_batches > 0
            parts << "#{report.pending_jobs} pending jobs" if report.pending_jobs > 0
            puts "#{report.shard}: #{parts.join(", ")}"
          end
        end

        if reports.all?(&:clear?)
          puts "All clear: previous_shards can be removed, and drained shards can leave the configuration."
        end
      end
    end
  end
end
