require 'table_copy/pg/ddl'

describe TableCopy::PG::DDL do
  let(:db) { DB.new }
  let(:table1) { db.table_name }
  let(:result) { db.with_conn { |c| c.exec("select * from #{table1} limit 0") } }
  let(:ddl) { TableCopy::PG::DDL }
  before { db.create_table }
  after  { db.drop_table }

  describe '.for' do
    it 'returns the expected ddl' do
      db.with_conn do |conn|
        expect(ddl.for(result, conn)).to eq "column1 integer,\ncolumn2 character varying(123),\ncolumn3 character varying(256)[]"
      end
    end

    context 'multiple tables' do
      let(:table2) { 'table2' }
      let(:result) { db.with_conn { |c| c.exec("#{select} from #{table1} inner join #{table2} on #{table1}.column1 = #{table2}.column1")} }

      before { db.with_conn { |c| c.exec("create table #{table2} (column1 integer, column2 integer)") } }
      after  { db.drop_table(table2) }

      context 'with duplicate field' do
        let(:select) { "select #{table1}.column2, #{table2}.column2" }

        it 'raises a descriptive error' do
          db.with_conn do |conn|
            expect {
              ddl.for(result, conn)
            }.to raise_error TableCopy::PG::DDL::DuplicateField, /column2/
          end
        end
      end

      context 'without duplicate field' do
        let(:select) { "select #{table1}.column2 t1_col2, #{table2}.column2 t2_col2" }

        it 'returns the expected ddl' do
          db.with_conn do |conn|
            expect(ddl.for(result, conn)).to eq "t1_col2 character varying(123),\nt2_col2 integer"
          end
        end
      end
    end
  end
end
