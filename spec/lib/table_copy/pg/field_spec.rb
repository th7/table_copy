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

  describe '#==' do
    it 'equals its string name' do
      expect(field == 'column_name').to eq true
    end

    it 'equals a field with the same name' do
      field2 = TableCopy::PG::Field.new(field_attrs)
      expect(field == field2).to eq true
    end

    it 'does not equal a string of another name' do
      expect(field == 'other_name').to eq false
    end

    it 'does not equal a field of another name' do
      field2 = TableCopy::PG::Field.new('name' => 'other_name')
      expect(field == field2).to eq false
    end
  end

  describe '#eql?' do
    it 'equals its string name' do
      expect(field.eql? 'column_name').to eq true
    end

    it 'equals a field with the same name' do
      field2 = TableCopy::PG::Field.new(field_attrs)
      expect(field.eql? field2).to eq true
    end

    it 'does not equal a string of another name' do
      expect(field.eql? 'other_name').to eq false
    end

    it 'does not equal a field of another name' do
      field2 = TableCopy::PG::Field.new('name' => 'other_name')
      expect(field.eql? field2).to eq false
    end
  end

  describe '#to_s' do
    it 'returns the name' do
      expect(field.to_s).to eq field_attrs['column_name']
    end
  end

  describe '#hash' do
    it 'returns the hash of the name' do
      expect(field.hash).to eq field_attrs['column_name'].hash
    end
  end

  describe 'when subtracting arrays' do
    it 'can be subtracted using a string' do
      expect([field] - [field_attrs['column_name']]).to eq []
    end
  end
end
