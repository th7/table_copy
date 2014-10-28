module TableCopy
  module PG
    class Destination
      attr_reader :table_name, :conn_method, :indexes, :fields, :fields_proc, :primary_key, :sequence_field, :after_create, :soft_delete_field

      def initialize(args)
        @table_name        = args[:table_name]
        @primary_key       = args[:primary_key]
        @sequence_field    = args[:sequence_field]
        @conn_method       = args[:conn_method]
        @indexes           = args[:indexes] || []
        @fields            = args[:fields]
        @fields_proc       = args[:fields_proc]
        @after_create      = args[:after_create]
        @soft_delete_field = args[:soft_delete_field]
      end

      def transaction
        with_conn do |conn|
          begin
            conn.exec('begin')
            yield
            conn.exec('commit')
          rescue Exception => e
            conn.exec('rollback')
            raise e
          end
        end
      end

      def create(fields_ddl)
        sd = ", #{soft_delete_field} bool default false" if soft_delete_field
        with_conn do |conn|
          conn.exec("create table #{table_name} (#{fields_ddl}#{sd})")
        end
        after_create.call(table_name) if after_create
      end

      def drop(opts={})
        cascade = ' cascade' if opts[:cascade]
        with_conn do |conn|
          conn.exec("#{drop_sql}#{cascade}")
        end
      end

      def create_indexes
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

      def create_temp(fields_ddl)
        with_conn do |conn|
          conn.exec("create temp table temp_#{table_name} (#{fields_ddl}) on commit drop")
        end
      end

      def none?
        with_conn do |conn|
          conn.exec("select count(*) from #{table_name}").first['count'] == '0'
        end
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

      def copy_from_temp(except: except_statement)
        with_conn do |conn|
          conn.exec(upsert_sql(except))
        end
      end

      def delete_not_in_temp
        with_conn do |conn|
          if soft_delete_field
            conn.exec("update #{table_name} set #{soft_delete_field}=true where #{not_in_temp} and (#{soft_delete_field} is null or #{soft_delete_field} != true)")
          else
            conn.exec("delete from #{table_name} where #{not_in_temp}")
          end
        end
      end

      def query_views
        with_conn do |conn|
          conn.exec(views_sql)
        end
      end

      def create_views(views)
        with_conn do |conn|
          views.inject({}) do |result, view|
            begin
              conn.exec("create or replace view #{view['viewname']} as (#{view['definition'].gsub(/;\z/, '')})")
              result[view['viewname']] = true
            rescue ::PG::UndefinedTable, ::PG::UndefinedColumn => e
              result[view['viewname']] = false
            end
            result
          end
        end
      end

      private

      def not_in_temp
        "#{primary_key} in (select #{primary_key} from #{table_name} except select #{primary_key} from temp_#{table_name})"
      end

      attr_reader :primary_key

      def fields_list
        if fields_proc
          fields_proc.call.join(', ')
        else
          @fields_list ||= fields.join(', ')
        end
      end

      def with_conn(&block)
        conn_method.call(&block)
      end


      def drop_sql
        @drop_sql ||= "drop table if exists #{table_name}"
      end

      def max_sequence_sql
        @max_sequence_sql ||= "select max(#{sequence_field}) from #{table_name}"
      end

      def upsert_sql(except=except_statement)
        "with new_values as (
          select #{fields_list} from temp_#{table_name}
          #{except}
        )
        ,upsert as (
          UPDATE #{table_name}
          SET #{set_statement(fields)}
          FROM new_values as nv
          WHERE #{table_name}.#{primary_key} = nv.#{primary_key}
          RETURNING #{return_statement(fields)}
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

      def views_sql
        <<-SQL
        select viewname, definition from pg_views where viewname in
          (SELECT distinct dependee.relname
            FROM pg_depend
            JOIN pg_rewrite ON pg_depend.objid = pg_rewrite.oid
            JOIN pg_class as dependee ON pg_rewrite.ev_class = dependee.oid
            JOIN pg_class as dependent ON pg_depend.refobjid = dependent.oid
            JOIN pg_attribute ON pg_depend.refobjid = pg_attribute.attrelid
                AND pg_depend.refobjsubid = pg_attribute.attnum
            WHERE dependent.relname = '#{table_name}'
            AND pg_attribute.attnum > 0)
        SQL
      end
    end
  end
end
