require 'table_copy/pg/index'

describe TableCopy::PG::Index do
  let(:columns) { [ 'column1', 'column2', 'column3' ] }
  let(:table) { 'table_name' }
  let(:name) { 'index_name' }
  let(:index) { TableCopy::PG::Index.new(table, name, columns) }

  describe '#create' do
    let(:expected) { 'create index on table_name using btree (column1, column2, column3)' }

    it 'returns a correct create index statement' do
      expect(index.create).to eq expected
    end
  end

  describe '#drop' do
    let(:expected) { 'drop index if exists index_name' }

    it 'returns a correct drop index statement' do
      expect(index.drop).to eq expected
    end
  end
end
