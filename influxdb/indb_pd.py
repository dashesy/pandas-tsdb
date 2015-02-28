"""InfluxDB interface to Pandas dataframe

Copyright Amiigo Inc.
"""

import pandas as pd
import re
import numpy as np
import json
from indb_io import push_indb, query_indb, InfluxDBError


# keys to translate from dataframe to labels/tags and back
default_inline_labels = {
                    '_pk': 'pk',
                    '_loc': 'loc', 
                    '_user_id': 'user_id',
                    '_session': 'session',
                }

# labels that do not go to labels/tags
special_labels = {
            '_time': 'time',
        } 

class InfluxDBEmpty(InfluxDBError):
    pass

class InfluxDBSyntaxError(InfluxDBError):
    pass

def is_null(val, zero_null=False):
    """flexible way to check if value is missing
    Inputs:
        val       - value to check
        zero_null - if all-zero value should be treated like missing
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

def get_parts(ns):
    """get parts of namespace
    Inputs:
        ns       - namespace of the sensors, e.g. amiigo.bio., amiigo.activities.
    """
    if not ns:
        return []
    parts = ns.split(".")
    ns_len = len(parts)
    parts = [".".join(parts[-ns_len + ii:]) for ii in range(ns_len - 1)]
    return parts

def filter_ns(parts, sensor):
    """filter sensor name based on partials of namespace
    """
    psensor = sensor
    prev_match = ''
    for p in parts:
        if sensor.startswith(p) and len(p) > len(prev_match):
            prev_match = p
            psensor = psensor[len(p):]
    return psensor

def json_valid(val):
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
               labels=None,
               retentionPolicy=None,
               database=None, 
               ns=None,
               subset=None,
               inlabels=None,
               ts = None,
               zero_null=True,
               precision='ms',
               strtime=False,
               force_precision=True,
               **kwargs):
    """ convert dataframe to InfluxDB json body (a list of dictionaries)
    convert datetime index and vectors if necessary.
    geolocation is taken from df['_geo'] if available otherwise from meta['geo']

    Inputs:
        df        - dataframe **indexed by datetime**
        labels    - extra metadata (user_id, pk, age, sex, height, weight, swversion, hwversion)
        retentionPolicy - policy of data retention
        database  - database to generate json for
        ns        - namespace of the sensors, e.g. amiigo.bio., amiigo.activities.
        subset    - list of columns prefixes to always incluse (e.g. can be ['_diag.'])
        inlabels  - labels inside the df
        ts        - default timestamp of entire df (datetime)
        zero_null - if should ignore zero readings same as missing
        precision - required precision
        strtime   - if timestamp should be in string format
        force_precision - if should add precision for each data point
    """
    if df is None or len(df) == 0:
        raise InfluxDBEmpty("Empty sensor data")
    df = df.copy()
    if not labels:
        labels = {}
    if ts:
        labels['time'] = ts
    if not subset:
        subset = []
    if inlabels is None:
        inlabels = default_inline_labels

    def _fix_special(sensor):
        for label, v in  inlabels.iteritems():
            if sensor == v:
                return label
        for label, v in  special_labels.iteritems():
            if sensor == v:
                return label
        return sensor
    
    # rename special and in-dataframe labels
    df.rename(columns=_fix_special, inplace=True)

    if not ns:
        ns = ''
    if ns and not ns.endswith("."):
        ns = ns + "."
    parts = get_parts(ns)
    
    def _fix_ns(sensor):
        return filter_ns(parts, sensor)
    
    if parts:
        df.rename(columns=_fix_ns, inplace=True)

    # ignore all-missing rows and columns
    df = df.dropna(axis=1, how='all').dropna(axis=0, how='all')
    if len(df) == 0:
        raise InfluxDBEmpty("Empty sensor data")

    epoch = np.datetime64('1970-01-01T00:00Z')
    def _get_ts(idx, sensors):
        """find time since epoch time from index and columns
        """
        ts = None
        try:
            ts = np.array([np.datetime64(idx0) for idx0 in idx])
        except:
            ts = None
        # try other special field for time
        if ts is None and '_time' in sensors:
            try:
                ts = sensors['_time'].apply(np.datetime64)
            except:
                ts = sensors['_time'].apply(np.datetime64, args=['us'])
        if ts is None:
            ts = labels.get('time')
            if ts:
                ts = np.datetime64(ts)
        
        if ts is None:
            # backend understands if there is no time
            return ts
        if strtime:
            # only nanosecond text is supported
            return [str(np.datetime64(t, 'ns')) for t in ts]
        ts = (ts - epoch) / np.timedelta64(1, precision)
        ts = [int(t) for t in ts]
        return ts
    
    df['time'] = _get_ts(df.index, df)
    
    # update tags with unique inside-df-labels
    for label, v in  inlabels.iteritems():
        # if label passed in labels, it takes precedence over in-dataframe labels
        if label in df and not labels.get(label):
            if len(pd.Series.unique(df[label])) == 1:
                # factor out shared tag
                labels[v] = df[label][0]
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
    if retentionPolicy:
        data['retentionPolicy'] = retentionPolicy
    if database:
        data['database'] = database
        
    if len(pd.Series.unique(df['time'])) == 1:
        # factor out shared timestamp
        ts = df['time'][0]
        df.drop('time', axis=1, inplace=True)
        if not is_null(ts):
            data['timestamp'] = ts
         
    if labels:
        # tags key/value pairs must be both strings
        mlabels = {str(k): str(v) for k, v in labels.iteritems()}
        data['tags'] = mlabels
    
    for _, sensors in df.iterrows():
        sensors = sensors.dropna()
        mlabels = {}
        # tags that are different from rest of the list
        for sensor, val in sensors.iteritems():
            if is_null(val):
                continue
            if sensor.startswith('_') and sensor not in subset:
                if sensor not in inlabels:
                    continue
                if sensor not in labels or labels[sensor] != val:
                    mlabels[inlabels[sensor]] = val
                    
        ts = sensors.get('time')
        for sensor, val in sensors.iteritems():
            if sensor == 'time':
                continue
            if is_null(val, zero_null=True):
                continue
            if sensor.startswith('_') and sensor not in subset:
                continue
            val = json_valid(val)
            sensor = '{ns}{sensor}'.format(ns=ns, sensor=sensor)
            indb = {
                'name': sensor,
                'fields': {'value': val},
            }
            if force_precision:
                indb['precision'] = to_precision.get(precision, precision)
            if not is_null(ts):
                indb['timestamp'] = ts
            if mlabels:
                # tags key/value pairs must be both strings
                mlabels = {str(k): str(v) for k, v in mlabels.iteritems()}
                indb['tags'] = mlabels
            points.append(indb)
            
    if not points:
        raise InfluxDBEmpty("Empty sensor data")
    
    return InDBJson(data)


def dict_to_indb(data, zero_null=True, **kwargs):
    """convert single dictionary to indb json body
    Inputs:
        data      - dictionary to record its fields
    Look at df_to_indb for additional arguments
    """
    df = pd.DataFrame([data])
    return df_to_indb(df, zero_null=zero_null, **kwargs)


def list_to_indb(data, zero_null=True, **kwargs):
    """convert a list of dictionaries to indb json
    Inputs:
        data      - list of dictionaries each dictionary a record in time with multiple sensors
    Look at df_to_indb for additional arguments
    """
    
    df = pd.DataFrame(data)
    return df_to_indb(df, zero_null=zero_null, **kwargs)

def record_indb(auth, data, **kwargs):
    """ convert data to InfluxDB json and push it to server
    Inputs:
        auth    - authentication to use
        data    - data to record in the backend
        
        kwargs - other arguments to pass to record_indb_json
    """
    
    def _re_apply(dct):
        if kwargs.get('database'):
            data['database'] = kwargs['database'] 
        if kwargs.get('retentionPolicy'):
            data['retentionPolicy'] = kwargs['retentionPolicy'] 

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
    return push_indb(auth, json_body, **kwargs)


def response_to_df(data, ns=None, inlabels=None,
               **kwargs):
    """convert InfluxDB response to dataframe (result ofa query)
    Inputs:
        data    - json data from InfluxDB backend
        ns      - namespace of the sensors, columns will be created stripping the namespace
        inlabels  - labels inside the df
        
        kwargs    - other optional arguments (below)
        ns1, ns2, ...    - extra namespaces to factor out
    """

    if not data:
        return pd.DataFrame()
    
    if isinstance(data, basestring):
        data = json.loads(data)
    
    if isinstance(data, dict):
        data = data.get('results', [])
            
    if inlabels is None:
        inlabels = default_inline_labels

    all_ns = [ns] + [v for k, v in kwargs.iteritems() if re.match('ns\d+$', k)]

    def _get_col(col):
        ncol = col
        prev_match = ''
        for ns in all_ns:
            if not ns:
                continue
            if len(ns) > len(prev_match) and col.startswith(ns):
                prev_match = ns
                ncol = col[len(ns):]
        return ncol

    sdfs = {} # session dataframes
    for chunk_idx, chunk in enumerate(data):
        error = chunk.get('error')
        if error:
            raise InfluxDBSyntaxError('{chunk_idx}: {error}'.format(
                                                                error=error, 
                                                                chunk_idx=chunk_idx
                                                            )) 
        rows = chunk.get('series')
        if not rows:
            continue
        for row in rows:
            lbls = row.get('tags', {})
            columns = row.get('columns')
            values = row.get('values')
            if not columns or not values:
                continue
            name = row.get('name')
            
            def _name_of(col):
                if col != 'time' and name: 
                    if col == 'value':
                        return name
                    return '{name}.{col}'.format(name=name, col=_get_col(col))
                return col
            columns = [_name_of(col) for col in columns]
            df = pd.DataFrame(values, columns=columns)
            if 'time' in df:
                df['time'] = df['time'].apply(np.datetime64)
                df.set_index('time', inplace=True)

            # bring back in-dataframe-labels
            for label, v in  inlabels.iteritems():
                if v in lbls:
                    df[label] = lbls[v]

            pk = lbls.get('pk', 'unknown') or 'unknown'
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


def query_indb_df(*args, **kwargs):
    """ construct a dataframe from sensors
    Look at query_indb and response_to_df for parameters
    """

    data = query_indb(*args, **kwargs)
    return response_to_df(data, **kwargs)
