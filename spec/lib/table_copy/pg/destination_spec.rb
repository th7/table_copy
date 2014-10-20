require 'table_copy/pg/destination'
require 'table_copy/pg/source'
require 'table_copy/pg/index'

describe TableCopy::PG::Destination do
  let(:table_name) { 'table_name' }
  let(:view_name)  { 'view_name' }
  let(:db) { DB.new(table_name: table_name, view_name: view_name) }

  let(:conf) { {
    table_name: table_name,
    conn_method: db.method(:with_conn),
    indexes: [ TableCopy::PG::Index.new(table_name, nil, ['column1']) ],
    fields: [ 'column1', 'column2', 'column3' ],
    primary_key: 'column1',
    sequence_field: 'column1'
  } }

  let(:dest) { TableCopy::PG::Destination.new(conf) }

  after do
    db.drop_table
  end

  describe '#to_s' do
    it 'returns the table name' do
      expect(dest.to_s).to eq table_name
    end
  end

  context 'a table exists' do
    before do
      dest.create(db.create_fields_ddl)
    end

    let(:expected_view) {
      [
        {
          "viewname"   => view_name,
          "definition" => "SELECT #{table_name}.column1, #{table_name}.column2, #{table_name}.column3 FROM #{table_name};"
        }
      ]
    }

    context 'a view exists' do
      before do
        db.create_view
      end

      describe '#query_views' do
        it 'returns a hash of name => query for views dependent on this table' do
          expect(dest.query_views.to_a).to eq expected_view
        end
      end
    end

    describe '#create_views' do
      it 'creates the given views' do
        expect {
          dest.create_views(expected_view)
        }.to change {
          db.view_exists?
        }.from(false).to(true)
      end

      it 'returns a hash of name => success' do
        expect(dest.create_views(expected_view)).to eq({ view_name => true })
      end

      context 'a view fails to be created' do
        let(:failing_view) { {
          "viewname"   => 'another_view_name',
          "definition" => "SELECT #{table_name}.column_does_no_exist FROM #{table_name};"
        } }

        let(:expected_result) { {
          view_name => true,
          'another_view_name' => false
        } }

        it 'returns a hash of name => success' do
          expect(dest.create_views(expected_view << failing_view)).to eq(expected_result)
        end
      end
    end

    describe '#none?' do
      it 'indicates whether the table has any data' do
        expect {
          db.insert_data
        }.to change {
          dest.none?
        }.from(true).to(false)
      end
    end

    describe '#transaction' do
      context 'no error is raised' do
        it 'opens and commits a transaction' do
          expect {
            dest.transaction do
              db.insert_data
            end
          }.to change {
            db.row_count
          }.by(1)
        end
      end

      context 'an error is raised' do
        it 'opens but does not commit a transaction' do
          expect {
            begin
              dest.transaction do
                db.insert_data
                raise
              end
            rescue RuntimeError; end
          }.not_to change {
            db.row_count
          }
        end
      end

    end

    describe '#drop' do
      it 'drops a table' do
        expect {
          dest.drop
        }.to change {
          db.table_exists?
        }.from(true).to(false)
      end
    end

    describe '#create_indexes' do
      it 'creates indexes' do
        expect {
          dest.create_indexes
        }.to change {
          db.indexes.count
        }.from(0).to(1)
      end
    end

    describe '#max_sequence' do
      context 'no sequence field' do
        it 'returns nil' do
          expect(dest.max_sequence).to be_nil
        end
      end

      context 'sequence field specified' do
        let(:dest) { TableCopy::PG::Destination.new(
          table_name: table_name,
          conn_method: db.method(:with_conn),
          sequence_field: 'column1'
        )}

        context 'no rows' do
          it 'returns nil' do
            expect(dest.max_sequence).to be_nil
          end
        end

        context 'with rows' do
          before do
            db.insert_data
          end

          it 'returns the max value of the sequence field' do
            expect(dest.max_sequence).to eq '1'
          end
        end
      end
    end

    describe '#copy_data_from' do
      let(:source) { TableCopy::PG::Source.new({}) }
      let(:source_conn) { double }

      context 'all fields and rows' do
        before do
          expect(source).to receive(:copy_from).with('column1, column2, column3', nil).and_yield(source_conn)
          expect(source_conn).to receive(:get_copy_data).and_return("1,foo,\"{bar,baz}\"\n")
          expect(source_conn).to receive(:get_copy_data).and_return(nil)
        end

        context 'default options' do
          it 'inserts data' do
            expect {
              dest.copy_data_from(source)
            }.to change {
              db.row_count
            }.from(0).to(1)
          end
        end

        context 'temp is true' do
          before do
            db.create_table("temp_#{table_name}")
          end

          after do
            db.drop_table("temp_#{table_name}")
          end

          it 'inserts data into temp table' do
            expect {
              dest.copy_data_from(source, temp: true)
            }.to change {
              db.row_count("temp_#{table_name}")
            }.from(0).to(1)
          end
        end
      end

      context 'pk_only is true' do
        before do
          expect(source).to receive(:copy_from).with('column1', nil).and_yield(source_conn)
          expect(source_conn).to receive(:get_copy_data).and_return("1\n")
          expect(source_conn).to receive(:get_copy_data).and_return(nil)
        end

        it 'inserts data' do
          expect {
            dest.copy_data_from(source, pk_only: true)
          }.to change {
            db.row_count
          }.from(0).to(1)
        end
      end

      context 'update value is given' do
        before do
          expect(source).to receive(:copy_from).with('column1, column2, column3', "where column1 > 'a_value'").and_yield(source_conn)
          expect(source_conn).to receive(:get_copy_data).and_return("1,foo,\"{bar,baz}\"\n")
          expect(source_conn).to receive(:get_copy_data).and_return(nil)
        end

        it 'inserts data' do
          expect {
            dest.copy_data_from(source, update: 'a_value')
          }.to change {
            db.row_count
          }.from(0).to(1)
        end
      end
    end

    context 'with temp table' do
      before do
        db.create_table("temp_#{table_name}")
      end

      after do
        db.drop_table("temp_#{table_name}")
      end

      describe '#copy_from_temp' do
        before do
          db.insert_data("temp_#{table_name}")
        end

        it 'upserts from the temp table' do
          expect {
            dest.copy_from_temp
          }.to change {
            db.row_count
          }.from(0).to(1)

          expect {
            dest.copy_from_temp
          }.not_to change {
            db.row_count
          }.from(1)
        end
      end

      describe '#delete_not_in_temp' do
        before do
          db.insert_data
        end

        context 'no soft delete' do
          it 'deletes row that are not in the temp table' do
            expect {
              dest.delete_not_in_temp
            }.to change {
              db.row_count
            }.from(1).to(0)
          end
        end

        context 'with soft delete' do
          let(:soft_delete_field) { 'is_soft_deleted' }
          let(:conf2) { conf.merge(soft_delete_field: soft_delete_field) }
          let(:dest)  { TableCopy::PG::Destination.new(conf2) }

          it 'sets the field to true' do
            expect {
              dest.delete_not_in_temp
            }.to change {
              db.with_conn { |c| c.exec("select #{soft_delete_field} from #{table_name}").first[soft_delete_field]}
            }.from('f').to('t')
          end
        end
      end
    end
  end

  describe '#create' do
    it 'creates a table' do
      expect {
        dest.create('column1 integer')
      }.to change {
        db.table_exists?
      }.from(false).to(true)
    end

    context 'an after_create proc is specified' do
      let(:after_create) { double }
      let(:conf2) { conf.merge(after_create: after_create) }
      let(:dest) { TableCopy::PG::Destination.new(conf2) }

      it 'calls the after_create proc, passing the table_name' do
        expect(after_create).to receive(:call).with(dest.table_name)
        dest.create('column1 integer')
      end
    end
  end

  describe '#create_temp' do
    it 'creates a temporary table' do
      dest.transaction do
        expect {
          dest.create_temp('column1 integer')
        }.to change {
          db.table_exists?("temp_#{table_name}")
        }.from(false).to(true)
      end
      expect(db.table_exists?("temp_#{table_name}")).to eq false
    end
  end
end
