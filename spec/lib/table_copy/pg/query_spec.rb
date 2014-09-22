require 'table_copy/pg/query'

describe TableCopy::PG::Query do
  let(:db) { DB.new }
  let(:query) { TableCopy::PG::Query.new(query: "select * from #{db.table_name}", conn_method: db.method(:with_conn)) }

  before { db.create_table }
  after  { db.drop_table   }

  describe '#fields' do
    it 'returns an array of field names' do
      expect(query.fields).to eq [ 'column1', 'column2', 'column3' ]
    end
  end

  describe '#indexes' do
    it 'returns an empty array' do
      expect(query.indexes).to eq []
    end
  end

  describe '#fields_ddl' do
    it 'returns the expected ddl' do
      expect(query.fields_ddl).to eq "column1 integer,\ncolumn2 character varying(123),\ncolumn3 character varying(256)[]"
    end
  end
end
