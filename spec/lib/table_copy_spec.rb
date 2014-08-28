require 'table_copy'

describe TableCopy do
  let(:tc) { TableCopy }

  describe '.logger' do
    it 'defaults to a ruby logger' do
      expect(tc.logger).to be_kind_of Logger
    end
  end

  describe '.logger=' do
    before { @old_logger = tc.logger }
    after { tc.logger = @old_logger }

    it 'reassigns .logger' do
      val = 'asdf5678'
      expect {
        tc.logger = val
      }.to change {
        tc.logger
      }.to(val)
    end
  end

  describe '.links' do
    context 'no config' do
      it 'returns an empty hash' do
        expect(tc.links).to eq({})
      end
    end

    context 'config block defined' do
      let(:source) { 'fake source' }
      let(:destination) { 'fake destination' }
      let(:link_name) { 'a link name' }
      let(:expected_value) { 'an expected value' }

      let(:config) { Proc.new {} }

      before do
        TableCopy.deferred_config(&config)
      end

      after do
        TableCopy.links.clear
        TableCopy.deferred_config {}
      end

      it 'returns a hash of link_name => Copier' do
        expect(TableCopy::Copier).to receive(:new).with(source, destination).and_return(expected_value)

        expect(config).to receive(:call) do
          TableCopy.add_link(link_name, source, destination)
        end

        expect(tc.links.keys).to eq [ link_name ]
        expect(tc.links[link_name]).to eq expected_value
      end
    end
  end

  describe '.deferred_config' do
    it 'exists and is tested incidentally' do
      expect { TableCopy.deferred_config }.not_to raise_error
    end
  end

  describe '.add_link' do
    let(:source) { 'fake source' }
    let(:destination) { 'fake destination' }
    let(:link_name) { 'a link name' }

    after { TableCopy.links.clear }

    it 'adds a Copier to the links hash' do
      expect {
        TableCopy.add_link(link_name, source, destination)
      }.to change {
        TableCopy.links
      }.from({})

      copier = tc.links[link_name]
      expect(copier).to be_kind_of TableCopy::Copier
      expect(copier.source_table).to eq source
      expect(copier.destination_table).to eq destination
    end
  end
end
