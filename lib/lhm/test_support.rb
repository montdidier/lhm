# frozen_string_literal: true
module Lhm
  module TestMigrator
    def initialize(*)
      super
      @name = @origin.name
    end

    def execute
      @statements.each do |stmt|
        @connection.execute(tagged(stmt))
      end
    end
  end

  module TestInvoker
    def run(options = {})
      normalize_options(options)
      set_session_lock_wait_timeouts
      @migrator.run
    rescue => e
      Lhm.logger.error("LHM run failed with exception=#{e.class} message=#{e.message}")
      raise
    end
  end

  # Patch LHM to execute ALTER TABLE directly on original tables,
  # without the online migration dance.
  # This mode is designed for local/CI environments where we can speed
  # things up by not invoking "real" LHM logic.
  def self.execute_inline!
    Lhm::Migrator.prepend(TestMigrator)
    Lhm::Invoker.prepend(TestInvoker)
  end
end
