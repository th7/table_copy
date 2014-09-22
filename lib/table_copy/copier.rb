require 'pg'
require 'table_copy/error'

module TableCopy
  class Copier
    class Error < TableCopy::Error; end

    attr_reader :source, :destination

    def initialize(source, destination)
      @source      = source
      @destination = destination
    end

    def update
      with_rescue do
        if destination.none? || source.kind_of?(TableCopy::PG::Query)
          droppy
        elsif (max_sequence = destination.max_sequence)
          update_data(max_sequence)
        else
          diffy_update
        end
      end
    end

    def droppy
      logger.info { "Droppy #{destination.table_name}" }
      views = destination.query_views

      destination.transaction do
        destination.drop(cascade: true)
        create_table
        moved_count = destination.copy_data_from(source)
        logger.info { "#{moved_count} rows moved to #{destination.table_name}" }
        destination.create_indexes
        logger.info { "Completed #{source.indexes.count} indexes on #{destination.table_name}." }
      end

      destination.create_views(views).each do |view_name, view_status|
        logger.info { "#{view_status ? 'Created' : 'Failed to create'} view #{view_name} for #{destination.table_name}" }
      end
    end

    def find_deletes
      logger.info { "Find deletes #{destination.table_name}" }
      assert_source_not_query
      destination.transaction do
        destination.create_temp(source.fields_ddl)
        moved_count = destination.copy_data_from(source, temp: true, pk_only: true)
        logger.info { "#{moved_count} rows moved to temp_#{destination.table_name}" }
        destination.delete_not_in_temp
        logger.info { "Deletions from #{destination.table_name} complete." }
      end
    end

    def diffy
      logger.info { "Diffy #{destination.table_name}" }
      assert_source_not_query
      destination.transaction do
        destination.create_temp(source.fields_ddl)
        moved_count = destination.copy_data_from(source, temp: true)
        logger.info { "#{moved_count} rows moved to temp_#{destination.table_name}" }
        destination.copy_from_temp
        logger.info { "Upsert to #{destination.table_name} complete" }
        destination.delete_not_in_temp
        logger.info { "Deletions from #{destination.table_name} complete." }
      end
    end

    private

    def diffy_update
      logger.info "Diffy Update #{destination.table_name}"
      destination.transaction do
        destination.create_temp(source.fields_ddl)
        moved_count = destination.copy_data_from(source, temp: true)
        logger.info "#{moved_count} rows moved to temp_#{destination.table_name}"
        destination.copy_from_temp
        logger.info "Upsert to #{destination.table_name} complete."
      end
    end

    def update_data(max_sequence)
      logger.info "Update #{destination.table_name}"
      destination.transaction do
        destination.create_temp(source.fields_ddl)
        moved_count = destination.copy_data_from(source, temp: true, update: max_sequence)
        logger.info "#{moved_count} rows moved to temp_#{destination.table_name}"
        destination.copy_from_temp(except: nil)
        logger.info "Upsert to #{destination.table_name} complete."
      end
    end

    def create_table
      logger.info { "Creating table #{destination.table_name}" }
      destination.create(source.fields_ddl)
    end

    def with_rescue
      yield
    rescue ::PG::UndefinedTable => e
      ([e.inspect] + e.backtrace).each { |l| logger.warn(l) }
      create_table
      yield
    rescue ::PG::UndefinedColumn => e
      ([e.inspect] + e.backtrace).each { |l| logger.warn(l) }
      droppy
    end

    def assert_source_not_query
      if source.kind_of? TableCopy::PG::Query
        raise TableCopy::Copier::Error, 'Cannot run this operation with query as source'
      end
    end

    def logger
      TableCopy.logger
    end
  end
end
