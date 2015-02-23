# pandas-tsdb: timeseries database interface for Pandas

## What is it
time-series databases fit naturaly with Pandas dataframes. Plan is to make this a PR against Pandas when underlying backend(s) is released.
The dataframe column names that start with `_` will be treated as metadata that do not need to be saved to database unless as tags/labels.
Column names should follow a namespace notation with `.` namespaces can be automatically added to column prior to recording to database, and take out when reading results.


## License
BSD


