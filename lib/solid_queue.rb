# frozen_string_literal: true

require "solid_queue/version"
require "solid_queue/engine"

require "active_job"
require "active_job/queue_adapters"
require "active_job/batch_id"

require "active_support"
require "active_support/core_ext/numeric/time"

require "zlib"

require "zeitwerk"

loader = Zeitwerk::Loader.for_gem(warn_on_extra_files: false)
loader.ignore("#{__dir__}/solid_queue/tasks.rb")
loader.ignore("#{__dir__}/generators")
loader.ignore("#{__dir__}/puma")
loader.setup

module SolidQueue
  extend self

  DEFAULT_LOGGER = ActiveSupport::Logger.new($stdout)

  mattr_accessor :logger, default: DEFAULT_LOGGER
  mattr_accessor :app_executor, :on_thread_error, :connects_to

  mattr_accessor :use_skip_locked, default: true

  mattr_accessor :process_heartbeat_interval, default: 60.seconds
  mattr_accessor :process_alive_threshold, default: 5.minutes
  mattr_accessor :fork_boot_timeout, default: 5.minutes

  mattr_accessor :shutdown_timeout, default: 5.seconds

  mattr_accessor :silence_polling, default: true

  mattr_accessor :supervisor_pidfile
  mattr_accessor :supervisor, default: false

  mattr_accessor :preserve_finished_jobs, default: true
  mattr_accessor :clear_finished_jobs_after, default: 1.day
  mattr_accessor :default_concurrency_control_period, default: 3.minutes

  mattr_reader :time_zone

  def time_zone=(zone)
    @@time_zone = if zone
      resolved = zone.respond_to?(:tzinfo) ? zone : ActiveSupport::TimeZone[zone]
      resolved&.tzinfo&.name || zone.to_s
    end
  end

  delegate :on_start, :on_stop, :on_exit, to: Supervisor

  def schedule_recurring_task(key, **options)
    RecurringTask.create_dynamic_task(key, **options)
  end

  def unschedule_recurring_task(key)
    RecurringTask.delete_dynamic_task(key)
  end

  [ Dispatcher, Scheduler, Worker ].each do |process|
    define_singleton_method(:"on_#{process.name.demodulize.downcase}_start") do |&block|
      process.on_start(&block)
    end

    define_singleton_method(:"on_#{process.name.demodulize.downcase}_stop") do |&block|
      process.on_stop(&block)
    end

    define_singleton_method(:"on_#{process.name.demodulize.downcase}_exit") do |&block|
      process.on_exit(&block)
    end
  end

  def supervisor?
    supervisor
  end

  def silence_polling?
    silence_polling
  end

  def preserve_finished_jobs?
    preserve_finished_jobs
  end

  def sharded?
    connects_to.is_a?(Hash) && connects_to.key?(:shards)
  end

  # The shards receiving new work. Defaults to every shard in connects_to;
  # set it explicitly to leave out a shard that stays connected but shouldn't
  # take new jobs, such as one being drained before removal.
  def shards
    @shards.presence&.map(&:to_sym) || (sharded? ? connects_to[:shards].keys : [])
  end

  attr_writer :shards, :previous_shards

  # The split in effect before the last change to the shard list, kept during
  # the transition so concurrency-controlled jobs keep meeting semaphores
  # created under it. Remove it once `solid_queue:shards:status` reports the
  # old shards clear; removing it early can at worst let a moved key exceed
  # its limit for up to its duration, the same trade a non-graceful shutdown
  # already makes.
  def previous_shards
    (@previous_shards || []).map(&:to_sym)
  end

  def shard_router
    router_for(shards)
  end

  def previous_shard_router
    router_for(previous_shards)
  end

  def router_for(shard_names)
    (@routers ||= {})[shard_names] ||= MaglevHash.new(shard_names)
  end

  def deprecator
    @deprecator ||= ActiveSupport::Deprecation.new(next_major_version, "SolidQueue")
  end

  def instrument(channel, **options, &block)
    ActiveSupport::Notifications.instrument("#{channel}.solid_queue", **options, &block)
  end

  ActiveSupport.run_load_hooks(:solid_queue, self)
end
