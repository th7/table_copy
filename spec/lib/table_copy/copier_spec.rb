require 'table_copy/copier'
require 'table_copy/pg/source'
require 'table_copy/pg/destination'

describe TableCopy::Copier do
  let(:copier) { TableCopy::Copier.new(source, destination) }
  let(:source) { TableCopy::PG::Source.new({}) }
  let(:destination) { TableCopy::PG::Destination.new({}) }
  let(:fields_ddl) { 'fake fields ddl' }

  describe '#update' do
    context 'no destination table' do
      it 'calls droppy' do
        expect(destination).to receive(:none?).and_return(true)
        expect(copier).to receive(:droppy)
        copier.update
      end
    end

    context 'destination table exists' do
      before do
        expect(destination).to receive(:none?).and_return(false)
      end

      context 'a max sequence is available' do
        before do
          expect(destination).to receive(:max_sequence).and_return(2345)
        end

        it 'passes the max sequence to update data' do
          expect(copier).to receive(:update_data).with(2345)
          copier.update
        end
      end

      context 'no max sequence is available' do
        before do
          expect(destination).to receive(:max_sequence).and_return(nil)
        end

        it 'calls diffy_update' do
          expect(copier).to receive(:diffy_update)
          copier.update
        end
      end
    end

    context 'PG::UndefinedTable is raised' do
      before do
        expect(destination).to receive(:none?).and_raise(PG::UndefinedTable, 'Intentionally raised.').once
        expect(destination).to receive(:none?).and_return(true) # handle the retry
      end

      it "passes the source's ddl to #create on the destination" do
        expect(source).to receive(:fields_ddl).and_return(fields_ddl)
        expect(destination).to receive(:create).with(fields_ddl)
        expect(copier).to receive(:droppy) # handle the retry
        copier.update
      end
    end

    context 'PG::UndefinedColumn is raised' do
      let(:fields_ddl) { 'fake fields ddl' }

      before do
        expect(destination).to receive(:none?).and_raise(PG::UndefinedColumn, 'Intentionally raised.').once
      end

      it 'calls droppy' do
        expect(copier).to receive(:droppy)
        copier.update
      end
    end
  end

  context 'within a transaction in the destination' do
    before do
      expect(destination).to receive(:transaction).and_yield
    end

    describe '#droppy' do
      it 'drops and rebuilds the destination table' do
        expect(destination).to receive(:drop).with(cascade: true)
        expect(source).to receive(:fields_ddl).and_return(fields_ddl)
        expect(destination).to receive(:create).with(fields_ddl)
        expect(destination).to receive(:copy_data_from).with(source)
        expect(destination).to receive(:create_indexes)
        copier.droppy
      end
    end

    context 'after creating a temp table' do
      before do
        expect(source).to receive(:fields_ddl).and_return(fields_ddl)
        expect(destination).to receive(:create_temp).with(fields_ddl)
      end

      describe '#find_deletes' do
        it 'finds and removes deleted rows' do
          expect(destination).to receive(:copy_data_from).with(source, temp: true, pk_only: true)
          expect(destination).to receive(:delete_not_in_temp)
          copier.find_deletes
        end
      end

      describe '#diffy' do
        it 'copies data form temp and finds and removes deleted rows' do
          expect(destination).to receive(:copy_data_from).with(source, temp: true)
          expect(destination).to receive(:copy_from_temp)
          expect(destination).to receive(:delete_not_in_temp)
          copier.diffy
        end
      end
    end

  end

end
