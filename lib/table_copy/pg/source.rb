require 'table_copy/pg/field'
require 'table_copy/pg/index'

module TableCopy
  module PG
    class Source
      attr_reader :table_name, :conn_method, :infer_pk_proc

      def initialize(args)
        @table_name    = args[:table_name]
        @conn_method   = args[:conn_method]
        @infer_pk_proc = args[:infer_pk_proc]
      end

      def to_s
        table_name
      end

      def primary_key
        @primary_key ||= get_primary_key
      end

      def fields_ddl
        @fields_ddl ||= fields_objects.map(&:ddl).join(",\n  ")
      end

      def indexes
        @indexes ||= viable_index_columns.map { |name, columns| TableCopy::PG::Index.new(table_name, name, columns) }
      end

      def copy_from(fields_list_arg, where=nil)
        with_conn do |conn|
          conn.copy_data("copy (select #{fields_list_arg} from #{table_name} #{where}) to stdout csv")  do
            yield conn
          end
        end
      end

      def fields
        @field_names ||= fields_objects.map(&:name)
      end

      private

      def with_conn(&block)
        conn_method.call(&block)
      end

      def fields_objects
        @fields_objects ||= with_conn do |conn|
          conn.exec(fields_sql).map { |r| TableCopy::PG::Field.new(r) }
        end
      end

      def viable_index_columns
        @viable_index_columns ||= index_columns.select do |name, columns|
          (columns - fields).empty?
        end
      end

      def index_columns
        @index_columns ||= raw_indexes.inject({}) do |indexes, ri|
          index_name = ri['index_name']
          indexes[index_name] ||= []
          indexes[index_name] << ri['column_name']
          indexes
        end
      end

      def raw_indexes
        @raw_indexes || with_conn do |conn|
          @raw_indexes = conn.exec(indexes_sql)
        end
      end

      def indexes_sql
        <<-SQL
          select
              i.relname as index_name,
              a.attname as column_name
          from
              pg_class t,
              pg_class i,
              pg_index ix,
              pg_attribute a
          where
              t.oid = ix.indrelid
              and i.oid = ix.indexrelid
              and a.attrelid = t.oid
              and a.attnum = ANY(ix.indkey)
              and t.relkind = 'r'
              and t.relname = '#{table_name}'
          order by
              t.relname,
              i.relname;
        SQL
      end

      def fields_sql
        <<-SQL
          SELECT *
          FROM information_schema.columns
          WHERE table_schema='public' AND table_name='#{table_name}'
        SQL
      end

      def get_primary_key
        with_conn do |conn|
          rows = conn.exec(primary_key_sql)
          if (row = rows.first) && row['attname']
            row['attname']
          elsif infer_pk_proc
            inferred_pk = infer_pk_proc.call(table_name)
            TableCopy.logger.warn "No explicit PK found for #{table_name}. Falling back to #{inferred_pk}."
            inferred_pk
          else
            TableCopy.logger.warn "No explicit PK found for #{table_name}. Falling back to \"id\"."
            'id'
          end
        end
      end

      def primary_key_sql
        <<-SQL
          SELECT
          pg_attribute.attname,
          format_type(pg_attribute.atttypid, pg_attribute.atttypmod)
        FROM pg_index, pg_class, pg_attribute
        WHERE
          pg_class.oid = '#{table_name}'::regclass AND
          indrelid = pg_class.oid AND
          pg_attribute.attrelid = pg_class.oid AND
          pg_attribute.attnum = any(pg_index.indkey)
          AND indisprimary
        SQL
      end
    end
  end
end
