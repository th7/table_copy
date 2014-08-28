require 'logger'
require 'table_copy/copier'

module TableCopy
  class << self
    attr_writer :logger

    def logger
      @logger ||= Logger.new($stdout)
    end

    def links
      if configured?
        @links
      else
        configure_links
        @links
      end
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
        return @links if configured?
        @deferred_config.call if @deferred_config
        @links = links_to_add
      end
    end

    def configured?
      @links && !@links.empty?
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
