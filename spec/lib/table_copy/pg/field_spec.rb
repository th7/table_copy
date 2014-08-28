require 'table_copy/pg/field'

describe TableCopy::PG::Field do
  let(:field) { TableCopy::PG::Field.new(field_attrs) }
  let(:field_attrs) { {
    'column_name' => 'column_name'
  } }

  context 'for a varchar field' do
    let(:field_attrs) { {
      'column_name' => 'column_name',
      'data_type'   => 'character varying',
      'character_maximum_length' => '256'
    } }

    describe '#ddl' do
      it 'returns a correct segment of ddl' do
        expect(field.ddl).to eq 'column_name character varying(256)'
      end
    end

    describe '#auto_index?' do
      it 'returns falsey' do
        expect(field.auto_index?).to be_falsey
      end
    end
  end

  context 'for an integer field' do
    let(:field_attrs) { {
      'column_name' => 'column_name',
      'data_type'   => 'integer'
    } }

    describe '#ddl' do
      it 'returns a correct segment of ddl' do
        expect(field.ddl).to eq 'column_name integer'
      end
    end

    describe '#auto_index?' do
      it 'returns truthy' do
        expect(field.auto_index?).to be_truthy
      end
    end
  end

  context 'for an array field' do
    let(:field_attrs) { {
      'column_name' => 'column_name',
      'data_type'   => 'ARRAY',
      'udt_name'    => '_varchar'
    } }

    describe '#ddl' do
      it 'returns a correct segment of ddl' do
        expect(field.ddl).to eq 'column_name character varying(256)[]'
      end
    end

    describe '#auto_index?' do
      it 'returns falsey' do
        expect(field.auto_index?).to be_falsey
      end
    end
  end
end
