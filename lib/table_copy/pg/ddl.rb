require 'table_copy/error'

module TableCopy
  module PG
    module DDL
      class DuplicateField < TableCopy::Error; end

      class << self
        def for(pg_result, conn)
          used_fields = Set.new
          ddl_query = pg_result.num_fields.times.map do |column_num|
            type_oid   = pg_result.ftype(column_num)
            type_mod   = pg_result.fmod(column_num)
            field_name = pg_result.fname(column_num)

            unless used_fields.add?(field_name)
              raise DuplicateField, "\"#{field_name}\" used multiple times"
            end

            "format_type(#{type_oid}, #{type_mod}) as #{field_name}"
          end.join(', ')

          conn.exec("select #{ddl_query}").first.map do |field_name, data_type|
            "#{field_name} #{data_type}"
          end.join(",\n")
        end
      end
    end
  end
end
