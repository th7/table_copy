module TableCopy
  module PG
    class Field
      attr_reader :name, :type_name, :data_limit

      def initialize(attrs)
        @name = attrs['column_name']
        data_type = attrs['data_type']

        if data_type =~ /character/
          @data_limit = attrs['character_maximum_length']
        end

        if data_type == 'ARRAY' && attrs['udt_name'] == '_varchar'
          @type_name = 'character varying'
          @data_limit = '256'
          @array_ddl = '[]'
        end

        @type_name ||= data_type
      end

      def ddl
        @ddl ||= "#{name} #{type_name}#{data_limit_ddl}#{array_ddl}"
      end

      def auto_index?
        @type_name =~ /int|timestamp|bool/
      end

      def ==(other)
        to_s == other.to_s
      end

      def eql?(other)
        to_s == other.to_s
      end

      def to_s
        name
      end

      def hash
        to_s.hash
      end

      private

      def data_limit_ddl
        "(#{data_limit})" if @data_limit
      end

      def array_ddl
        @array_ddl
      end
    end
  end
end
