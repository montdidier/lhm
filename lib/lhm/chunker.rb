# Copyright (c) 2011 - 2013, SoundCloud Ltd., Rany Keddo, Tobias Bielohlawek, Tobias
# Schmidt
require 'lhm/command'
require 'lhm/sql_helper'
require 'lhm/printer'
require 'lhm/chunk_insert'
require 'lhm/chunk_finder'

module Lhm
  class Chunker
    include Command
    include SqlHelper

    attr_reader :connection

    # Copy from origin to destination in chunks of size `stride`.
    # Use the `throttler` class to sleep between each stride.
    def initialize(migration, connection = nil, options = {})
      @migration = migration
      @connection = connection
      @chunk_finder = ChunkFinder.new(migration, connection, options)
      @options = options
      @verifier = options[:verifier]
      if @throttler = options[:throttler]
        @throttler.connection = @connection if @throttler.respond_to?(:connection=)
      end
      @start = @chunk_finder.start
      @limit = @chunk_finder.limit
      @printer = options[:printer] || Printer::Percentage.new
      @retry_helper = SqlRetry.new(
        @connection,
        {
          log_prefix: "Chunker"
        }.merge!(options.fetch(:retriable, {}))
      )
    end

    def execute
      return if @chunk_finder.table_empty?
      @next_to_insert = @start
      while @next_to_insert <= @limit || (@start == @limit)
        stride = @throttler.stride
        top = upper_id(@next_to_insert, stride)
        verify_can_run

        affected_rows = ChunkInsert.new(@migration, @connection, bottom, top, @options).insert_and_return_count_of_rows_created
        expected_rows = top - bottom + 1

        if affected_rows < expected_rows
          raise_on_non_pk_duplicate_warning
        end

        if @throttler && affected_rows > 0
          @throttler.run
        end
        @printer.notify(bottom, @limit)
        @next_to_insert = top + 1
        break if @start == @limit
      end
      @printer.end
    rescue => e
      @printer.exception(e) if @printer.respond_to?(:exception)
      raise
    end

    private

    def raise_on_non_pk_duplicate_warning
      @connection.query("show warnings").each do |level, code, message|
        unless message.match?(/Duplicate entry .+ for key 'PRIMARY'/)
          raise Error.new("Unexpected warning found for inserted row: #{message}")
        end
      end
    end

    def bottom
      @next_to_insert
    end

    def verify_can_run
      return unless @verifier
      @retry_helper.with_retries do |retriable_connection|
        raise "Verification failed, aborting early" if !@verifier.call(retriable_connection)
      end
    end

    def upper_id(next_id, stride)
      sql = "select id from `#{ @migration.origin_name }` where id >= #{ next_id } order by id limit 1 offset #{ stride - 1}"
      top = @retry_helper.with_retries do |retriable_connection|
        retriable_connection.select_value(sql)
      end

      [top ? top.to_i : @limit, @limit].min
    end

    def validate
      return if @chunk_finder.table_empty?
      @chunk_finder.validate
    end
  end
end
