require 'table_copy/pg/destination'
require 'table_copy/pg/index'

describe TableCopy::PG::Destination do
  let(:conn) { $pg_conn }
  let(:table_name) { 'table_name' }
  let(:indexes_sql) {
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
  }

  def with_conn
    yield conn
  end

  def table_exists?(name=table_name)
    conn.exec("select count(*) from pg_tables where tablename='#{name}'").first['count'] == '1'
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

  let(:dest) { TableCopy::PG::Destination.new(
    table_name: table_name,
    conn_method: method(:with_conn),
    indexes: [ TableCopy::PG::Index.new(table_name, nil, ['column1']) ],
    fields: [ 'column1', 'column2', 'column3' ],
    primary_key: 'column1',
    sequence_field: 'column1'
  )}

  after do
    conn.exec("drop table if exists #{table_name}")
  end

  describe '#to_s' do
    it 'returns the table name' do
      expect(dest.to_s).to eq table_name
    end
  end

  context 'a table exists' do
    before do
      create_table
    end

    describe '#none?' do
      it 'indicates whether the table has any data' do
        expect {
          insert_data
        }.to change {
          dest.none?
        }.from(true).to(false)
      end
    end

    describe '#transaction' do
      context 'no error is raised' do
        it 'opens and commits a transaction' do
          expect {
            dest.transaction do
              insert_data
            end
          }.to change {
            conn.exec("select count(*) from #{table_name}").first['count'].to_i
          }.by(1)
        end
      end

      context 'an error is raised' do
        it 'opens but does not commit a transaction' do
          expect {
            begin
              dest.transaction do
                insert_data
                raise
              end
            rescue RuntimeError; end
          }.not_to change {
            conn.exec("select count(*) from #{table_name}").first['count'].to_i
          }
        end
      end
    end

    describe '#drop' do
      it 'drops a table' do
        expect {
          dest.drop
        }.to change {
          table_exists?
        }.from(true).to(false)
      end
    end

    describe '#create_indexes' do
      it 'creates indexes' do
        expect {
          dest.create_indexes
        }.to change {
          conn.exec(indexes_sql).count
        }.from(0).to(1)
      end
    end

    describe '#max_sequence' do
      context 'no sequence field' do
        it 'returns nil' do
          expect(dest.max_sequence).to be_nil
        end
      end

      context 'sequence field specified' do
        let(:dest) { TableCopy::PG::Destination.new(
          table_name: table_name,
          conn_method: method(:with_conn),
          sequence_field: 'column1'
        )}

        context 'no rows' do
          it 'returns nil' do
            expect(dest.max_sequence).to be_nil
          end
        end

        context 'with rows' do
          before do
            insert_data
          end

          it 'returns the max value of the sequence field' do
            expect(dest.max_sequence).to eq '1'
          end
        end
      end
    end

    describe '#copy_data_from' do
      let(:source) { TableCopy::PG::Source.new({}) }
      let(:source_conn) { double }

      context 'all fields and rows' do
        before do
          expect(source).to receive(:copy_from).with('column1, column2, column3', nil).and_yield(source_conn)
          expect(source_conn).to receive(:get_copy_data).and_return("1,foo,\"{bar,baz}\"\n")
          expect(source_conn).to receive(:get_copy_data).and_return(nil)
        end

        context 'default options' do
          it 'inserts data' do
            expect {
              dest.copy_data_from(source)
            }.to change {
              row_count
            }.from(0).to(1)
          end
        end

        context 'temp is true' do
          before do
            create_table("temp_#{table_name}")
          end

          after do
            conn.exec("drop table if exists temp_#{table_name}")
          end

          it 'inserts data into temp table' do
            expect {
              dest.copy_data_from(source, temp: true)
            }.to change {
              row_count("temp_#{table_name}")
            }.from(0).to(1)
          end
        end
      end

      context 'pk_only is true' do
        before do
          expect(source).to receive(:copy_from).with('column1', nil).and_yield(source_conn)
          expect(source_conn).to receive(:get_copy_data).and_return("1\n")
          expect(source_conn).to receive(:get_copy_data).and_return(nil)
        end

        it 'inserts data' do
          expect {
            dest.copy_data_from(source, pk_only: true)
          }.to change {
            row_count
          }.from(0).to(1)
        end
      end

      context 'update value is given' do
        before do
          expect(source).to receive(:copy_from).with('column1, column2, column3', "where column1 > 'a_value'").and_yield(source_conn)
          expect(source_conn).to receive(:get_copy_data).and_return("1,foo,\"{bar,baz}\"\n")
          expect(source_conn).to receive(:get_copy_data).and_return(nil)
        end

        it 'inserts data' do
          expect {
            dest.copy_data_from(source, update: 'a_value')
          }.to change {
            row_count
          }.from(0).to(1)
        end
      end
    end

    context 'with temp table' do
      before do
        create_table("temp_#{table_name}")
      end

      after do
        conn.exec("drop table if exists temp_#{table_name}")
      end

      describe '#copy_from_temp' do
        before do
          insert_data("temp_#{table_name}")
        end

        it 'upserts from the temp table' do
          expect {
            dest.copy_from_temp
          }.to change {
            row_count
          }.from(0).to(1)

          expect {
            dest.copy_from_temp
          }.not_to change {
            row_count
          }.from(1)
        end
      end

      describe '#delete_not_in_temp' do
        before do
          insert_data
        end

        it 'deletes row that are not in the temp table' do
          expect {
            dest.delete_not_in_temp
          }.to change {
            row_count
          }.from(1).to(0)
        end
      end
    end
  end

  describe '#create' do
    it 'creates a table' do
      expect {
        dest.create('column1 integer')
      }.to change {
        table_exists?
      }.from(false).to(true)
    end
  end

  describe '#create_temp' do
    it 'creates a temporary table' do
      dest.transaction do
        expect {
          dest.create_temp('column1 integer')
        }.to change {
          table_exists?("temp_#{table_name}")
        }.from(false).to(true)
      end
      expect(table_exists?("temp_#{table_name}")).to eq false
    end
  end
end
