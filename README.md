# TableCopy

Move and update data on a table by table basis between two databases. Currently only supports Postgres in a limited fashion.

This gem could be made more flexible with a bit of work, but for now is pretty limited to my specific purposes.

## Installation

Add this line to your application's Gemfile:

    gem 'table_copy'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install table_copy

## Usage

Run ```table_copy --init``` for an example initializer. Then, access each table link by ```TableCopy.links['link_name']```. You can call ```link.update; link.droppy; link.diffy```

Update will attempt to use a sequence field to look for changes. If that field is not available, it will run a diffy(update) operation.

Diffy(update) will copy the source table to a temp table, diff it with the destination table, and upsert any changes to the destination.

Diffy will perform a diffy(update) and will also diff ids in the destination table against the temp table to find deletions.

Droppy will drop the destination table and rebuild/populate it.

TableCopy also supports using a query as a source. Simply use
```
source = TableCopy::PG::Query.new(query: some_sql, conn_method: SourceDB.method(:with_conn))
```
instead of the usual ```TableCopy::PG::Source``` class.

For now, you cannot run ```update``` when using a Query as a source -- it will fall back to droppy. An error will be raised if you attempt to run diffy.

### *Very* rough benchmarks:
- Copy 1M rows ~15 sec
- Index 1M rows ~2 sec per numeric field, ~40 sec per char field
- Diff 1M rows ~40 sec
- Upsert 100k rows into 1M row table ~60 sec

## Contributing

1. Fork it ( https://github.com/th7/table_copy/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request
