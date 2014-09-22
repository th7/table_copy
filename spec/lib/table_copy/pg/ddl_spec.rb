require 'table_copy/pg/ddl'

describe TableCopy::PG::DDL do
  let(:db) { DB.new }
  let(:result) { db.with_conn { |c| c.exec("select * from #{db.table_name} limit 0") } }
  let(:ddl) { TableCopy::PG::DDL }
  before { db.create_table }
  after  { db.drop_table }

  describe '.for' do
    it 'returns the expected ddl' do
      db.with_conn do |conn|
        expect(ddl.for(result, conn)).to eq "column1 integer,\ncolumn2 character varying(123),\ncolumn3 character varying(256)[]"
      end
    end
  end
end
