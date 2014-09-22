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
      assert_source_not_query
      with_rescue do
        if destination.none?
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
        if same_db?
          destination.with_conn { |c| c.exec(select_into)}
          logger.info { "Select into #{destination} complete." }
        else
          create_table
          moved_count = copy_to_destination
          logger.info { "#{moved_count} rows moved to #{destination.table_name}" }
        end
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
        moved_count = copy_pk_to_temp
        logger.info { "#{moved_count} rows moved to temp_#{destination.table_name}" }
        destination.delete_not_in_temp
        logger.info { "Deletions from #{destination.table_name} complete." }
      end
    end

    def diffy
      logger.info { "Diffy #{destination.table_name}" }
      assert_source_not_query
      destination.transaction do
        moved_count = copy_to_temp
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
        moved_count = copy_to_temp
        logger.info "#{moved_count} rows moved to temp_#{destination.table_name}"
        destination.copy_from_temp
        logger.info "Upsert to #{destination.table_name} complete."
      end
    end

    def update_data(max_sequence)
      logger.info "Update #{destination.table_name}"
      destination.transaction do
        moved_count = copy_updated_to_temp(max_sequence)
        logger.info "#{moved_count} rows moved to temp_#{destination.table_name}"
        destination.copy_from_temp(except: nil)
        logger.info "Upsert to #{destination.table_name} complete."
      end
    end

    def create_table
      logger.info { "Creating table #{destination.table_name}" }
      destination.create(source.fields_ddl)
    end

    def create_temp
      destination.create_temp(source.fields_ddl)
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
      if source_is_query?
        raise TableCopy::Copier::Error, 'Cannot run this operation with query as source'
      elsif same_db? && !source_is_query?
        msg = 'Intra DB table copying not supported. Check "CREATE TABLE AS" or "SELECT INTO".'
        raise TableCopy::Copier::Error, msg
      end
    end

    def copy_to_destination
      fl       = destination.fields_list
      from_sql = "select #{fl} from #{source}"
      copy_to  = "#{destination} (#{fl})"
      copy_data(from_sql, copy_to)
    end

    def copy_to_temp
      fl       = destination.fields_list
      from_sql = "select #{fl} from #{source}"
      copy_to  = "#{create_temp} (#{fl})"
      copy_data(from_sql, copy_to)
    end

    def copy_pk_to_temp
      fl       = destination.primary_key
      from_sql = "select #{fl} from #{source}"
      copy_to  = "#{create_temp} (#{fl})"
      copy_data(from_sql, copy_to)
    end

    def copy_updated_to_temp(max_sequence)
      fl       = destination.fields_list
      where    = "where #{destination.sequence_field} > '#{max_sequence}'"
      from_sql = "select #{fl} from #{source} #{where}"
      copy_to  = "#{create_temp} (#{fl})"
      copy_data(from_sql, copy_to)
    end

    def copy_data(from_sql, copy_to)
      count = 0
      with_connections do |source_conn, dest_conn|
        if source_conn == dest_conn
          dest_conn.exec(destination.upsert_sql(from_sql))
        else
          source_conn.copy_data("copy (#{from_sql}) to stdout csv")  do
            dest_conn.copy_data("copy #{copy_to} from stdout csv") do
              while row = source_conn.get_copy_data
                count += 1
                dest_conn.put_copy_data(row)
              end
            end
          end
        end
      end
      count
    end

    def with_connections
      result = nil
      source.with_conn do |sc|
        destination.with_conn do |dc|
          result = yield sc, dc
        end
      end
      result
    end

    def select_into
      "select #{destination.fields_list} into #{destination} from #{source}"
    end

    def same_db?
      with_connections { |sc, dc| result = sc == dc }
    end

    def source_is_query?
      source.kind_of? TableCopy::PG::Query
    end

    def logger
      TableCopy.logger
    end
  end
end
