require 'table_copy/pg/has_ddl'

describe TableCopy::PG::HasDDL do
  let(:db) { DB.new }
  let(:result) { db.with_conn { |c| c.exec("select * from #{db.table_name} limit 0") } }
  let(:includer) { Class.new { include TableCopy::PG::HasDDL }.new }

  before { db.create_table }
  after  { db.drop_table }

  describe '#ddl_for' do
    it 'returns the expected ddl' do
      db.with_conn do |conn|
        expect(includer.ddl_for(result, conn)).to eq "column1 integer,\ncolumn2 character varying(123),\ncolumn3 character varying(256)[]"
      end
    end
  end
end
