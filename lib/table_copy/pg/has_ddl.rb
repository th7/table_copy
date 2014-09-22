module TableCopy
  module PG
    module HasDDL
      def ddl_for(pg_result, conn)
        ddl_query = pg_result.fields.map do |field_name|
          column_num = pg_result.fnumber(field_name)
          type_oid   = pg_result.ftype(column_num)
          type_mod   = pg_result.fmod(column_num)
          "format_type(#{type_oid}, #{type_mod}) as #{field_name}"
        end.join(', ')

        conn.exec("select #{ddl_query}").first.map do |field_name, data_type|
          "#{field_name} #{data_type}"
        end.join(",\n")
      end
    end
  end
end
