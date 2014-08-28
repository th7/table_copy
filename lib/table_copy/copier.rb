require 'pg'

module TableCopy
  class Copier
    attr_reader :source_table, :destination_table

    def initialize(source_table, destination_table)
      @source_table      = source_table
      @destination_table = destination_table
    end

    def update
      if destination_table.none?
        droppy
      elsif (max_sequence = destination_table.max_sequence)
        update_data(max_sequence)
      else
        diffy_update
      end
    rescue ::PG::UndefinedTable => e
      ([e.inspect] + e.backtrace).each { |l| logger.warn(l) }
      create_table
      retry
    rescue ::PG::UndefinedColumn => e
      ([e.inspect] + e.backtrace).each { |l| logger.warn(l) }
      droppy
    end

    def droppy
      logger.info { "Droppy #{destination_table.table_name}" }
      destination_table.transaction do
        destination_table.drop(cascade: true)
        create_table
        moved_count = destination_table.copy_data_from(source_table)
        logger.info { "#{moved_count} rows moved to #{destination_table.table_name}" }
        destination_table.create_indexes(source_table.indexes)
        logger.info { "Completed #{source_table.indexes.count} indexes on #{destination_table.table_name}." }
      end
    end

    def find_deletes
      logger.info { "Find deletes #{destination_table.table_name}" }
      destination_table.transaction do
        destination_table.create_temp(source_table.fields_ddl)
        moved_count = destination_table.copy_data_from(source_table, temp: true, pk_only: true)
        logger.info { "#{moved_count} rows moved to temp_#{destination_table.table_name}" }
        destination_table.delete_not_in_temp
        logger.info { "Deletetions from #{destination_table.table_name} complete." }
      end
    end

    def diffy
      logger.info { "Diffy #{destination_table.table_name}" }
      destination_table.transaction do
        destination_table.create_temp(source_table.fields_ddl)
        moved_count = destination_table.copy_data_from(source_table, temp: true)
        logger.info { "#{moved_count} rows moved to temp_#{destination_table.table_name}" }
        destination_table.copy_from_temp
        logger.info { "Upsert to #{destination_table.table_name} complete" }
        destination_table.delete_not_in_temp
        logger.info { "Deletetions from #{destination_table.table_name} complete." }
      end
    end

    private

    def diffy_update
      logger.info "Diffy Update #{destination_table.table_name}"
      destination_table.transaction do
        destination_table.create_temp(source_table.fields_ddl)
        moved_count = destination_table.copy_data_from(source_table, temp: true)
        logger.info "#{moved_count} rows moved to temp_#{destination_table.table_name}"
        destination_table.copy_from_temp
        logger.info "Upsert to #{destination_table.table_name} complete."
      end
    end

    def update_data(max_sequence)
      logger.info "Update #{destination_table.table_name}"
      destination_table.transaction do
        destination_table.create_temp(source_table.fields_ddl)
        moved_count = destination_table.copy_data_from(source_table, temp: true, update: max_sequence)
        logger.info "#{moved_count} rows moved to temp_#{destination_table.table_name}"
        destination_table.copy_from_temp(except: nil)
        logger.info "Upsert to #{destination_table.table_name} complete."
      end
    end

    def create_table
      logger.info { "Creating table #{destination_table.table_name}" }
      destination_table.create(source_table.fields_ddl)
    end

    def logger
      TableCopy.logger
    end
  end
end
