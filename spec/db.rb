class DB
  attr_reader :table_name, :view_name

  def initialize(table_name: 'table_name', view_name: 'view_name')
    @table_name = table_name
    @view_name  = view_name
  end

  def conn
    $pg_conn
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
    conn.exec("insert into #{name} values(1, 'foo', '{bar, baz}')")
  end

  def row_count(name=table_name)
    conn.exec("select count(*) from #{name}").first['count'].to_i
  end

  def create_table(name=table_name)
    conn.exec("create table #{name} (column1 integer, column2 varchar(123), column3 varchar(256)[])")
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

  private

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
