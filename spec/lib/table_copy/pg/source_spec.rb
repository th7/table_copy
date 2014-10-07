require 'table_copy/pg/source'
require 'yaml'

describe TableCopy::PG::Source do
  let(:conn) { $pg_conn }
  let(:table_name) { 'table_name' }

  def with_conn
    yield conn
  end

  let(:source) { TableCopy::PG::Source.new(
    table_name: table_name,
    conn_method: method(:with_conn)
  )}

  after do
    conn.exec("drop table if exists #{table_name}")
  end

  describe '#to_s' do
    it 'returns the table name' do
      expect(source.to_s).to eq table_name
    end
  end

  describe '#primary_key' do
    context 'primary key is defined' do
      let(:pk) { 'primary_key' }
      before do
        conn.exec("create table #{table_name} (#{pk} integer primary key)")
      end

      it 'returns the name of the primary key' do
        expect(source.primary_key).to eq pk
      end
    end

    context 'pk is not defined' do
      before do
        conn.exec("create table #{table_name} (#{pk} integer)")
      end

      context 'pk inferrence proc is defined' do
        let(:pk) { "#{table_name}_id" }

        let(:source) { TableCopy::PG::Source.new(
          table_name: table_name,
          conn_method: method(:with_conn),
          infer_pk_proc: Proc.new { |tn| "#{tn}_id" }
        )}

        it 'returns the name of the primary key' do
          expect(source.primary_key).to eq pk
        end
      end

      context 'pk inferrence proc is not defined' do
        let(:pk) { "#{table_name}_id" }

        it 'returns "id"' do
          expect(source.primary_key).to eq 'id'
        end
      end
    end
  end

  context 'a table exists' do
    before do
      conn.exec("create table #{table_name} (column1 integer, column2 varchar(123), column3 varchar(256)[])")
    end

    describe '#fields_ddl' do
      it 'returns correct fields ddl' do
        expect(source.fields_ddl.gsub("\n", '')).to eq 'column1 integer,  column2 character varying(123),  column3 character varying(256)[]'
      end
    end

    describe '#fields' do
      it 'returns an array of field names' do
        expect(source.fields).to eq [ 'column1', 'column2', 'column3' ]
      end
    end

    context 'indexes exist' do
      before do
        conn.exec("create index on #{table_name} (column1)")
      end

      describe '#indexes' do
        it 'returns an array of indexes' do
          expect(source.indexes.count).to eq 1
          index = source.indexes.first
          expect(index.table).to eq table_name
          expect(index.columns).to eq [ 'column1' ]
        end
      end
    end

    context 'a row exists' do
      before do
        conn.exec("insert into #{table_name} values(1, 'foo', '{bar, baz}')")
      end

      it 'yields a copying connection' do
        source.copy_from('column1, column2, column3') do |copy_conn|
          expect(copy_conn.get_copy_data).to eq "1,foo,\"{bar,baz}\"\n"
          expect(copy_conn.get_copy_data).to be_nil
        end
      end
    end

  end

  # context 'debugging specs' do
  #   it 'works' do
  #     conn.exec("create table #{table_name} (column1 integer,  column2 character varying(123),  column3 character varying(256)[])")
  #   end
  # end
end
