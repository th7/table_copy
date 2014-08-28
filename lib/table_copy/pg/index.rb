module TableCopy
  module PG
    class Index
      attr_reader :table, :name, :columns

      def initialize(table, name, columns)
        @table   = table
        @name    = name
        @columns = columns
      end

      def create
        @create ||= "create index on #{table} using btree (#{columns.join(', ')})"
      end

      def drop
        @drop ||= "drop index if exists #{name}"
      end
    end
  end
end
