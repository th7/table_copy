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

        it 'updates the table with new data' do
          expect(destination).to receive(:transaction).and_yield
          expect(source).to receive(:fields_ddl).and_return(fields_ddl)
          expect(destination).to receive(:create_temp).with(fields_ddl)
          expect(destination).to receive(:copy_data_from).with(source, temp: true, update: 2345)
          expect(destination).to receive(:copy_from_temp).with(except: nil)
          copier.update
        end
      end

      context 'no max sequence is available' do
        before do
          expect(destination).to receive(:max_sequence).and_return(nil)
        end

        it 'calls diffy_update' do
          expect(destination).to receive(:transaction).and_yield
          expect(source).to receive(:fields_ddl).and_return(fields_ddl)
          expect(destination).to receive(:create_temp).with(fields_ddl)
          expect(destination).to receive(:copy_data_from).with(source, temp: true)
          expect(destination).to receive(:copy_from_temp)
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
        expect(destination).to receive(:query_views).and_return('views')
        expect(destination).to receive(:drop).with(cascade: true)
        expect(source).to receive(:fields_ddl).and_return(fields_ddl)
        expect(destination).to receive(:create).with(fields_ddl)
        expect(destination).to receive(:create_views).with('views').and_return([])
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

  describe 'integration tests', speed: 'slow' do
    let(:db1) { DB.new(conn: $pg_conn,  table_name: table_name1) }
    let(:db2) { DB.new(conn: $pg_conn2, table_name: table_name2) }

    let(:table_name1) { 'table_name1' }
    let(:table_name2) { 'table_name2' }

    let(:source) { TableCopy::PG::Source.new(
      table_name: table_name1,
      conn_method: db1.method(:with_conn)
    ) }

    let(:sequence_field) { 'column1' }

    let(:dest) { TableCopy::PG::Destination.new(
      table_name: table_name2,
      conn_method: db2.method(:with_conn),
      primary_key: 'column1',
      sequence_field: sequence_field,
      indexes: source.indexes,
      fields:  source.fields
    )}

    let(:copier) { TableCopy::Copier.new(source, dest) }

    before do
      db1.create_table
    end

    after do
      db1.drop_table
    end

    describe '#update' do
      context 'no destination table' do
        after { db2.drop_table }

        it 'creates the table' do
          expect {
            copier.update
          }.to change {
            db2.table_exists?
          }.from(false).to(true)
        end
      end

      context 'destination table exists' do
        before do
          db2.create_table
        end

        after do
          db2.drop_table
        end

        before do
          db1.insert_data
          db1.insert_data
          db2.insert_data
        end

        context 'a max sequence is available' do
          it 'updates the table with new data' do
            expect {
              copier.update
            }.to change {
              db2.row_count
            }.from(1).to(2)
          end
        end

        context 'no max sequence is available' do
          let(:sequence_field) { nil }

          it 'updates the table with new data' do
            expect(destination.max_sequence).to eq nil

            expect {
              copier.update
            }.to change {
              db2.row_count
            }.from(1).to(2)
          end
        end

        context 'a field is added' do
          let(:new_field) { 'new_field' }

          before do
            db1.add_field(new_field)
          end

          it 'adds the field to the destination table' do
            expect {
              copier.update
            }.to change {
              db2.has_field?(new_field)
            }.from(false).to(true)
          end
        end
      end
    end

    context 'within a transaction in the destination' do
      before do
        db2.create_table
      end

      after do
        db2.drop_table
      end

      describe '#droppy' do
        let(:new_field) { 'new_field' }

        before do
          db1.add_field(new_field)
        end

        it 'drops and rebuilds the destination table' do
          expect {
            copier.droppy
          }.to change {
            db2.has_field?(new_field)
          }.from(false).to(true)
        end

        context 'with pre-existing views' do
          before do
            db2.create_view
          end

          it 'rebuilds views' do
            expect {
              copier.droppy
            }.not_to change {
              db2.view_exists?
            }.from(true)
          end

          context 'a view becomes invalid' do
            before do
              db1.drop_field('column2')
              db2.exec("create view view_name2 as (select column1 from #{db2.table_name})")
            end

            it 'rebuilds valid views' do
              expect {
                copier.droppy
              }.to change {
                db2.view_exists?
              }.from(true).to(false)
            end
          end
        end
      end

      context 'destination has rows absent from source' do
        before { 3.times { db2.insert_data } }

        describe '#find_deletes' do
          it 'finds and removes deleted rows' do
            expect {
              copier.find_deletes
            }.to change {
              db2.row_count
            }.from(3).to(0)
          end
        end

        describe '#diffy' do
          before do
            5.times { db1.insert_data }
            db1.delete_row
          end

          it 'copies data from source' do
            expect {
              copier.diffy
            }.to change {
              db2.row_count
            }.from(3).to(4) # +2 -1
          end

          it 'finds and removes deleted rows' do
            expect {
              copier.diffy
            }.to change {
              db2.row_count
            }.from(3).to(4) # +2 -1
          end
        end
      end
    end
  end
end
