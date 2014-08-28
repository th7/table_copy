module TableCopy
  module PG
    class Destination
      attr_reader :table_name, :conn_method, :indexes, :fields, :primary_key, :sequence_field

      def initialize(args)
        @table_name     = args[:table_name]
        @primary_key    = args[:primary_key]
        @sequence_field = args[:sequence_field]
        @conn_method    = args[:conn_method]
        @indexes        = args[:indexes]
        @fields         = args[:fields]
      end

      def with_conn(&block)
        conn_method.call(&block)
      end

      def transaction
        with_conn do |conn|
          begin
            conn.exec('begin')
            yield
            conn.exec('commit')
          rescue StandardError => e
            conn.exec('rollback')
            raise e
          end
        end
      end

      def create(fields_ddl)
        with_conn do |conn|
          conn.exec("create table #{table_name} (#{fields_ddl})")
        end
      end

      def drop(opts={})
        cascade = ' cascade' if opts[:cascade]
        with_conn do |conn|
          conn.exec("#{drop_sql}#{cascade}")
        end
      end

      def create_indexes(indexes)
        indexes.each do |index|
          create_ddl = index.class.new(table_name, index.name, index.columns).create
          with_conn do |conn|
            conn.exec(create_ddl)
          end
        end
      end

      def to_s
        table_name
      end

      def max_sequence
        return unless sequence_field
        with_conn do |conn|
          row = conn.exec(max_sequence_sql).first
          row['max'] if row
        end
      end

      def indexes
        @indexes ||= []
      end

      def copy_data_from(source_table, temp: nil, pk_only: false, update: false)
        temp = 'temp_' if temp
        fl = pk_only ? primary_key : fields_list
        where = "where #{sequence_field} > '#{update}'" if update && sequence_field
        count = 0
        source_table.copy_from(fl, where) do |source_conn|
          with_conn do |conn|
            conn.copy_data("COPY #{temp}#{table_name} (#{fl}) FROM STDOUT CSV") do
              while row = source_conn.get_copy_data
                count += 1
                conn.put_copy_data(row)
              end
            end
          end
        end
        count
      end

      def temp_size
        with_conn do |conn|
          conn.exec("select count(*) from temp_#{table_name}").first['count']
        end
      end

      def fields_list
        @fields_list ||= fields.map(&:name).join(', ')
      end

      def create_temp(fields_ddl)
        with_conn do |conn|
          conn.exec("create temp table temp_#{table_name} (#{fields_ddl}) on commit drop")
        end
      end

      def copy_from_temp(except: except_statement)
        with_conn do |conn|
          conn.exec(upsert_sql(except))
        end
      end

      def delete_not_in_temp
        with_conn do |conn|
          conn.exec("delete from #{table_name} where #{primary_key} in (select #{primary_key} from #{table_name} except select #{primary_key} from temp_#{table_name})")
        end
      end

      def none?
        with_conn do |conn|
          conn.exec("select count(*) from #{table_name}").first['count'] == '0'
        end
      end

      private

      attr_reader :primary_key

      def drop_sql
        @drop_sql ||= "drop table if exists #{table_name}"
      end

      def max_sequence_sql
        @max_sequence_sql ||= "select max(#{sequence_field}) from #{table_name}"
      end

      def update(attrs)
        sql = "update #{table_name} set "
        parts = attrs.keys.map.with_index(1) do |key, i|
          "#{key}=$#{i}"
        end
        sql << parts.join(',')
        sql << " where #{primary_key}=#{attrs[primary_key]}"

        with_conn do |conn|
          conn.exec_params(sql, attrs.values)
        end
      end

      def insert(attrs)
        sql = "insert into #{table_name} (#{attrs.keys.join(',')}) "
        parts = attrs.map.with_index(1) do |(key, value), i|
          "$#{i}"
        end
        sql << "values (#{parts.join(',')})"

        with_conn do |conn|
          conn.exec_params(sql, attrs.values)
        end
      end

      def upsert_sql(except=except_statement)
        "with new_values as (
          select #{fields_list} from temp_#{table_name}
          #{except}
        )
        ,upsert as (
          UPDATE #{table_name}
          SET #{set_statement(fields.map(&:name))}
          FROM new_values as nv
          WHERE #{table_name}.#{primary_key} = nv.#{primary_key}
          RETURNING #{return_statement(fields.map(&:name))}
        )

        INSERT INTO #{table_name} (#{fields_list})
               SELECT *
               FROM new_values as nv
               WHERE NOT EXISTS (SELECT 1
                                 FROM #{table_name}
                                 WHERE #{table_name}.#{primary_key} = nv.#{primary_key});"
      end

      def except_statement
        @except_statement ||= "except select #{fields_list} from #{table_name}"
      end

      def set_statement(keys)
        keys.map.with_index(1) do |key, i|
          "#{key}=nv.#{key}"
        end.join(',')
      end

      def return_statement(keys)
        keys.map.with_index(1) do |key, i|
          "nv.#{key}"
        end.join(',')
      end
    end
  end
end
