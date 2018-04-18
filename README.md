# mongo-to-s3
Exports whitelisted `mongo` fields to an `s3` bucket, kicking off an `s3-to-redshift` job at the end of the run.
 
## Usage:
```
  -config string
        String corresponding to an env var config
  -collections string
        Comma-separated strings corresponding to the mongo collections you wish to pull from. Empty or not set pulls all collections included in the conf file. (default: "")
  -database string
        Database url if using existing instance (required)
  -bucket string
        s3 bucket to upload to
```

## Behavior

`mongo-to-s3` does a few things:

1. connects to the provided mongo `database`
2. determines the correct "data date" by rounding down to the nearest hour
3. parses the provided config file
4. for each table in the config file
  - pulls the whitelisted fields from mongo
  - flattens objects into dot-separated fields
  - streams to gzipped, timestamped JSON files on s3
5. kicks off an [s3-to-redshift](https://github.com/Clever/s3-to-redshift) job to process this data

Right now, `mongo-to-s3` will attempt export all fields/tables in the `X_config.yml` whitelist which it's called with.

## Updating config files

Configs are env vars in `YAML` and follow this format:
```yaml
tablename-whateveryouwant:
  dest: <redshift_table_name>
  source: <mongo_table_name>
  columns:
    -
      dest: _data_timestamp
      type: timestamp
      sortord: 1
    -
      dest: <column_name_in_redshift>
      source: <column_name_in_mongo>
      type: text
      primarykey: true
      notnull: true
      distkey:  true
  meta:
    datadatecolumn: _data_timestamp
    schema: <redshift_schema_name>
```

Inrternal note: configs are located in [ark-config](https://github.com/Clever/ark-config/blob/master/apps/mongo-to-s3/production.yml)

There are a few tricky things, including some items that are changing in the near future.

Tricky things:
1) The `datadatecolumn` is to help keep track of the date of the data going into the data warehouse, and to prevent us from overwriting new data with old.
Therefore, we want to set it to approximately when the data was created.

Currently, we do this via a special column that we specify in the `meta` section.
Whatever column you specify here will be overwritten with the date the `mongo-to-s3` worker is run, rounded down to the nearest hour.
Note that we don't require a `source` here as we populate it in `mongo-to-s3`.

2) We currently don't support more than one `sortkey`, so the only valid value for `sortord` is 1

3) You also have to set `notnull` for `primarykey` columns, even though that is implied.

4) Accepted column types are:
- boolean
- float
- int
- timestamp
- text (256 characters)
- longtext (65535 characters)

It should be easy to add more, however.

5) You may want to think about issues if some data arrives sooner than other data to the data warehouse. For instance, suppose item A is only "active" if an item B exists in the database and points to A. If you've synched over A significantly before B, it may appear that A is 'inactive' until B is synched over. In reality, A has always been 'active'.

6) While you pass *collections* to run on as parameters to `mongo-to-s3`, this worker will post jobs to `s3-to-redshft` with the *destination table* names as parameters.
