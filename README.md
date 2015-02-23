# pandas-tsdb: timeseries database interface for Pandas

## What is it
time-series databases fit naturaly with Pandas dataframes. Plan is to make this a PR against Pandas when underlying backend(s) is released.

The dataframe column names that start with `_` will be treated as metadata that do not need to be saved to database unless as tags/labels, e.g. `_pk` can be used as primary key to group different measurements together.
Column names should follow a namespace notation with dots (`.`) e.g. `activity.calories` is measurement about calories but has namesapce of `activity`. 
Namespaces can be automatically added to columns prior to recording to database, and taken out when reading results.

## Supported backends
At this point the only supported backend is InfluxDB, but there is at least one more we will add later.

## License
BSD


