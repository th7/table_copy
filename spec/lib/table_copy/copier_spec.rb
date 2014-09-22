require 'table_copy/copier'
require 'table_copy/pg/source'
require 'table_copy/pg/destination'

describe TableCopy::Copier do
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

    let(:source_query) { TableCopy::PG::Query.new(
      query: "select * from #{table_name1}",
      conn_method: db1.method(:with_conn)
    ) }

    let(:source_same_conn) { TableCopy::PG::Query.new(
      query: "select * from #{table_name1}",
      conn_method: db2.method(:with_conn)
    ) }

    let(:dest_query) { TableCopy::PG::Destination.new(
      table_name: table_name2,
      conn_method: db2.method(:with_conn),
      indexes: source.indexes,
      fields:  source.fields
    ) }

    before do
      db1.create_table
    end

    after do
      db1.drop_table
    end

    describe '#update' do
      context 'no destination table' do
        after { db2.drop_table }

        context 'source is a table' do
          it 'creates the table' do
            expect {
              copier.update
            }.to change {
              db2.table_exists?
            }.from(false).to(true)
          end
        end

        context 'source is a query' do
          let(:source) { source_query }
          let(:dest)   { dest_query }

          it 'raises an error' do
            expect {
                copier.find_deletes
              }.to raise_error TableCopy::Copier::Error
          end
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

          context 'source is a table' do
            it 'updates the table with new data' do
              expect(dest.max_sequence).to eq nil

              expect {
                copier.update
              }.to change {
                db2.row_count
              }.from(1).to(2)
            end
          end
        end

        context 'a field is added' do
          let(:new_field) { 'new_field' }

          before do
            db1.add_field(new_field)
          end

          context 'source is a table' do
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

        context 'source is a table' do
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

        context 'source is a query' do
          let(:dest)   { dest_query }

          context 'from separate database' do
            let(:source) { source_query }

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

          context 'from the same database' do
            let(:source) { source_same_conn }
            let(:db2_2)  { DB.new(conn: $pg_conn2, table_name: table_name1) }

            before do
              db2_2.create_table
              db2_2.add_field(new_field)
            end

            after do
              db2_2.drop_table
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
                  db2_2.drop_field('column2')
                  db2.exec("create view view_name2 as (select column1 from #{db2_2.table_name})")
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
        end
      end

      context 'destination has rows absent from source' do
        before { 3.times { db2.insert_data } }

        context 'source is a table' do
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

        context 'source is a query' do
          let(:source) { source_query }
          let(:dest)   { dest_query }

          describe '#find_deletes' do
            it 'raises an error' do
              expect {
                copier.find_deletes
              }.to raise_error TableCopy::Copier::Error
            end
          end

          describe '#diffy' do
            it 'raises an error' do
              expect {
                copier.find_deletes
              }.to raise_error TableCopy::Copier::Error
            end
          end
        end
      end
    end
  end
end
