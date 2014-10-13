class DB
  attr_reader :table_name, :view_name, :conn

  def initialize(table_name: 'table_name', view_name: 'view_name', conn: $pg_conn)
    @table_name = table_name
    @view_name  = view_name
    @conn       = conn
  end

  def with_conn
    yield conn
  end

  def table_exists?(name=table_name)
    conn.exec("select count(*) from pg_tables where tablename='#{name}'").first['count'] == '1'
  end

  def view_exists?(name=view_name)
    conn.exec("select count(*) from pg_views where viewname='#{name}'").first['count'] == '1'
  end

  def insert_data(name=table_name)
    conn.exec("insert into #{name} values(#{next_val}, 'foo', '{bar, baz}')")
  end

  def row_count(name=table_name)
    conn.exec("select count(*) from #{name}").first['count'].to_i
  end

  def create_table(name=table_name)
    conn.exec("create table #{name} (#{create_fields_ddl})")
  end

  def create_fields_ddl
    "column1 integer, column2 varchar(123), column3 varchar(256)[]"
  end

  def create_view(name=view_name, t_name: table_name)
    conn.exec("create view #{name} as (select * from #{t_name})")
  end

  def drop_table(name=table_name)
    conn.exec("drop table if exists #{name} cascade")
  end

  def indexes(name=table_name)
    conn.exec(indexes_sql(name))
  end

  def add_field(name, t_name: table_name)
    conn.exec("alter table #{t_name} add #{name} #{data_types.sample}")
  end

  def drop_field(name, t_name: table_name)
    conn.exec("alter table #{t_name} drop column #{name}")
  end

  def has_field?(name, t_name: table_name)
    conn.exec("select #{name} from #{t_name}")
    true
  rescue PG::UndefinedColumn
    false
  end

  def delete_row(name=table_name, pk=nil)
    conn.exec("delete from #{name} where column1=#{pk || min_pk(name)}")
  end

  def exec(sql)
    conn.exec(sql)
  end

  private

  def min_pk(name)
    conn.exec("select min(column1) from #{name}").first['min']
  end

  def next_val
    @next_val ||= 0
    @next_val += 1
  end

  def data_types
    [
      'integer',
      'varchar(123)',
      'varchar(256)[]'
    ]
  end

  def indexes_sql(name)
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
          and t.relname = '#{name}'
      order by
          t.relname,
          i.relname;
    SQL
  end
end
