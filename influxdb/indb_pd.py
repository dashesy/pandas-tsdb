"""InfluxDB interface to Pandas dataframe

Copyright Amiigo Inc.
"""

import pandas as pd
import numpy as np
import json
from indb_io import push_indb, query_indb, InfluxDBError


class EmptyInput(InfluxDBError):
    pass


class QueryError(InfluxDBError):
    pass


class InvalidData(InfluxDBError):
    pass


class MeasurementNotFound(QueryError):
    pass


def _is_null(val, zero_null=False):
    """check if value is missing
    :param val: value to check
    :param zero_null: if all-zero value should be treated like missing
    :return:
    """
    if val is None:
        return True
    try:
        if np.all(np.isnan(val)):
            return True
    except:
        pass
    if zero_null:
        try:
            if np.all(np.array(val) == 0):
                return True
        except:
            pass
    return False


def _get_namespace(sensor):
    """get sensor namespace
    :param sensor: namespace of the sensor, e.g. bio.bpm, activities.steps
    """
    parts = sensor.split(".")
    if len(parts) < 2:
        return None, sensor
    return parts[0], ".".join(parts[1:])


def _json_valid(val):
    """return a jason serializable value
    """
    try:
        json.dumps(val)
    except TypeError:
        if hasattr(val, 'to_json'):
            return val.to_json()
        if hasattr(val, 'tolist'):
            return val.tolist()
        if hasattr(val, 'tostring'):
            return val.tostring()
        # string is always good 
        val = str(val)
    
    return val


class InDBJson(dict):
    """dict but my dict
    """
    pass    


def df_to_indb(df,
               name=None,
               retention_policy=None,
               database=None,
               tag_columns=None,
               ignore_sensors=None,
               labels=None,
               zero_null=True,
               time=None,
               precision='ms',
               use_iso_format=False):
    """convert dataframe to InfluxDB json body (a list of dictionaries)
    convert datetime index and vectors if necessary.

    :param df: dataframe; by convention the index is time and info axis has name name
    :param name: if provided will be used as the measurement name.
          it can be also a callable that finds measurement and sensor name from colum name
    :param retention_policy: InfluxDB policy of data retention
    :param database: InfluxDB database to generate json for
    :param tag_columns: list of tag columns
            or a callable that gets name of columns and returns if they are tags
    :param ignore_sensors: name of sensors to ignore,
            or a callable that returns if a column should be ignored.
    :param labels:  dictionary of per-frame extra tags (user_id, sensor_id, ...)
           tags in labels take precedence over those in dataframe
    :param zero_null: if should ignore zero readings same as missing
    :param time: default per-frame timestamp of entire frame (or name of column that is time)
    :param precision: required time precision
    :param use_iso_format: if time should be in string format
    """
    if df is None or len(df) == 0:
        raise EmptyInput("Empty sensor data")
    df = df.copy()
    if not labels:
        labels = {}

    # ignore all-missing rows and columns
    df = df.dropna(axis=1, how='all').dropna(axis=0, how='all')
    if len(df) == 0:
        raise EmptyInput("Empty sensor data")

    epoch = np.datetime64('1970-01-01T00:00Z')

    def _get_mname(sensor):
        """get measurement and sensor name of a sensor
        """
        if callable(name):
            return name(sensor)
        return _get_namespace(sensor)

    def _get_ts(idx, sensors):
        """find time since epoch time from index and columns
        """
        try:
            ts = np.array([np.datetime64(idx0) for idx0 in idx])
        except:
            ts = None
        # try other special field for time
        if ts is None:
            if isinstance(time, basestring) and time in sensors:
                try:
                    ts = sensors[time].apply(np.datetime64)
                except:
                    ts = sensors[time].apply(np.datetime64, args=['us'])
        if ts is None and time:
            ts = [np.datetime64(time)]
        
        if ts is None:
            # InfluxDB backend understands if there is no time and uses now()
            return ts
        if use_iso_format:
            # only nanosecond text is supported
            return [str(np.datetime64(t, 'ns')) for t in ts]
        ts = (ts - epoch) / np.timedelta64(1, precision)
        ts = [int(t) for t in ts]
        return ts
    
    df['time'] = _get_ts(df.index, df)

    def _is_tag(col):
        if callable(tag_columns):
            return tag_columns(col)
        if not tag_columns:
            return False
        return col in tag_columns

    # update labels with unique inside-df-tags that are not already in labels
    for label in [tag for tag in df if _is_tag(tag) and tag not in labels]:
        if len(pd.Series.unique(df[label])) == 1:
            # factor out shared tag
            labels[label] = df[label][0]
            df.drop(label, axis=1, inplace=True)
    
    to_precision = {
        'us': 'u',
        'ns': 'n',
    }
    points = []
    data = {
        'points': points,
        'precision': to_precision.get(precision, precision),
    }
    if retention_policy:
        data['retentionPolicy'] = retention_policy
    if database:
        data['database'] = database
        
    if len(pd.Series.unique(df['time'])) == 1:
        # factor out shared timestamp
        ts = df['time'][0]
        df.drop('time', axis=1, inplace=True)
        if not _is_null(ts):
            data['timestamp'] = ts

    # common tags
    if labels:
        # tags key/value pairs must be both strings
        labels = {str(k): str(v) for k, v in labels.iteritems()}
        data['tags'] = labels
    
    for _, sensors in df.iterrows():
        sensors = sensors.dropna()
        mlabels = {}

        # extract per-row tags
        for sensor, val in sensors.iteritems():
            sensor = str(sensor)
            if _is_null(val):
                continue
            if _is_tag(sensor):
                # tags key/value pairs must be both strings
                mlabels[sensor] = str(val)

        try:
            ts = sensors.pop('time')
        except:
            ts = None
        measurements = {}
        for sensor, val in sensors.iteritems():
            sensor = str(sensor)
            if _is_null(val, zero_null=zero_null):
                continue
            if _is_tag(sensor):
                continue
            if ignore_sensors:
                if callable(ignore_sensors) and ignore_sensors(sensor):
                    continue
                if sensor in ignore_sensors:
                    continue

            mname = name
            psensor = sensor
            if mname is None:
                # detect measurement name based on column name
                mname, psensor = _get_mname(sensor)
            if mname is None:
                raise InvalidData('No measurement name for {sensor}'.format(sensor=sensor))
            if mname not in measurements:
                measurements[mname] = {}
            measurements[mname][psensor] = _json_valid(val)

        for mname, fields in measurements.iteritems():
            indb = {
                'name': mname,
                'fields': fields,
            }
            if not _is_null(ts):
                if isinstance(ts, basestring) and np.datetime64(ts) == np.datetime64('NaT'):
                    raise InvalidData('Invalid NaT in time')
                indb['timestamp'] = ts
                if not use_iso_format:
                    # FIXME: not always need to specify precision for all points
                    indb['precision'] = to_precision.get(precision, precision)
            if mlabels:
                indb['tags'] = mlabels
            points.append(indb)
            
    if not points:
        raise EmptyInput("Empty sensor data")
    
    return InDBJson(data)


def dict_to_indb(data, **kwargs):
    """convert single dictionary to indb json body
    Look at df_to_indb for additional arguments
    """
    df = pd.DataFrame([data])
    return df_to_indb(df, **kwargs)


def list_to_indb(data, **kwargs):
    """convert a list of dictionaries to indb json
    Look at df_to_indb for additional arguments
    """
    
    df = pd.DataFrame(data)
    return df_to_indb(df, **kwargs)


def record_indb(auth, data,
                name=None,
                retention_policy=None,
                database=None,
                tag_columns=None,
                ignore_sensors=None,
                labels=None,
                zero_null=True,
                time=None,
                precision='ms',
                use_iso_format=False,
                compress=False,
                ):
    """convert data to InfluxDB json and push it to server
    """
    
    def _re_apply(dct):
        if database:
            dct['database'] = database
        if retention_policy:
            dct['retentionPolicy'] = retention_policy

    kwargs = dict(
        name=name,
        retention_policy=retention_policy,
        database=database,
        tag_columns=tag_columns,
        ignore_sensors=ignore_sensors,
        labels=labels,
        zero_null=zero_null,
        time=time,
        precision=precision,
        use_iso_format=use_iso_format,
    )
    if isinstance(data, pd.DataFrame):
        json_body = df_to_indb(data, **kwargs)
    elif isinstance(data, list):
        json_body = list_to_indb(data, **kwargs)
    elif isinstance(data, InDBJson):
        json_body = data
        _re_apply(data)
    elif isinstance(data, dict):
        json_body = dict_to_indb(data, **kwargs)
        _re_apply(data)
    else:
        # if unknown let it pass and maybe it can be recorded!
        json_body = data
    return push_indb(auth, json_body, compress=compress)


def response_to_df(data, sensor_id='sensor_id'):
    """convert InfluxDB response (result of a query) to dataframe
    :param data: json data from InfluxDB backend
    :param sensor_id: the tag that (along with time) uniquely defines a row of one sensor
    """

    if not data:
        return pd.DataFrame()
    
    if isinstance(data, basestring):
        data = json.loads(data)
    
    if isinstance(data, dict):
        data = data.get('results', [])
            
    sdfs = {}  # sensor dataframes
    for chunk_idx, chunk in enumerate(data):
        error = chunk.get('error')
        if error:
            pe = error.lower()
            if 'measurement' in pe and 'not found' in pe:
                raise MeasurementNotFound('{chunk_idx}: {error}'.format(
                    error=error,
                    chunk_idx=chunk_idx
                ))
            raise QueryError('{chunk_idx}: {error}'.format(
                error=error,
                chunk_idx=chunk_idx
            ))
        rows = chunk.get('series')
        if not rows:
            continue
        for row in rows:
            tags = row.get('tags', {})
            columns = row.get('columns')
            values = row.get('values')
            if not columns or not values:
                continue
            name = row.get('name')
            
            def _name_of(col):
                if col == 'time' or not name:
                    return col
                return '{name}.{col}'.format(col=col, name=name)

            columns = [_name_of(col) for col in columns]
            df = pd.DataFrame(values, columns=columns)
            if 'time' in df:
                df['time'] = df['time'].apply(np.datetime64)
                df.set_index('time', inplace=True)

            # bring back in-dataframe-tags
            for tag, val in tags.iteritems():
                df[tag] = val

            pk = tags.get(sensor_id, 'unknown') or 'unknown'
            if pk not in sdfs:
                sdfs[pk] = []

            sdfs[pk].append(df)
            
    if len(sdfs) == 0:
        return pd.DataFrame()

    dfs = []
    for ses in sdfs.itervalues():
        if len(ses) == 0:
            continue
        df = pd.concat(ses)
        dfs.append(df)

    if len(dfs) == 0:
        return pd.DataFrame()

    data = pd.concat(dfs).sort_index()
    return data


def query_indb_df(auth, query,
                  database=None,
                  chunked=False,
                  sensor_id='sensor_id'):
    """ construct a dataframe from sensors
    Look at query_indb and response_to_df for parameters
    """

    data = query_indb(auth, query, database=database, chunked=chunked)
    return response_to_df(data, sensor_id=sensor_id)
