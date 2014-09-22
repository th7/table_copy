require 'table_copy/pg/ddl'

module TableCopy
  module PG
    class Query
      attr_reader :query, :conn_method

      def initialize(args)
        @query       = args[:query]
        @conn_method = args[:conn_method]
      end

      def fields
        empty_result.fields
      end

      def indexes
        []
      end

      def fields_ddl
        with_conn do |conn|
          DDL.for(empty_result, conn)
        end
      end

      def copy_from
        with_conn do |conn|
          conn.copy_data("copy (#{query}) to stdout csv")  do
            yield conn
          end
        end
      end

      private

      def empty_result
        with_conn do |conn|
          conn.exec("select * from (#{query}) fields_query limit 0")
        end
      end

      def with_conn(&block)
        conn_method.call(&block)
      end

      def ddl_query
        fields.map { |f| "pg_typeof(#{f}) #{f}"}.join(', ')
      end
    end
  end
end
