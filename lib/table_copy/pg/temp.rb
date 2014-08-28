module TableCopy
  module PG
    class Temp
      attr_reader :table_name, :conn

      def initialize(table_name, conn)
        @table_name = table_name
        @conn       = conn
      end
    end
  end
end
