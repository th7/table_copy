require "table_copy/version"
require 'table_copy/pg'
require 'table_copy/pg/source'
require 'table_copy/pg/destination'
require 'table_copy/copier'

module TableCopy
  class << self
    attr_writer :logger

    def logger
      @logger ||= Logger.new($stdout)
    end

    def links
      @links ||= configure_links
    end

    def deferred_config(&block)
      @deferred_config = block
    end

    def add_link(name, source, destination)
      links_to_add[name] = TableCopy::Copier.new(source, destination)
    end

    private

    def configure_links
      synchronized do
        return @links if @links
        @deferred_config.call if @deferred_config
        links_to_add
      end
    end

    def links_to_add
      @links_to_add ||= {}
    end

    def synchronized
      @semaphore ||= Mutex.new
      @semaphore.synchronize do
        yield
      end
    end
  end
end
