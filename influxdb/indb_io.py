"""InfluxDB input and output

Copyright Amiigo Inc.
"""

import requests
import gzip
import json
import StringIO


class InfluxDBError(ValueError):
    pass


class InfluxDBAuthError(InfluxDBError):
    pass


class UnknownError(InfluxDBError):
    pass


class QueryError(InfluxDBError):
    pass


class MeasurementNotFound(QueryError):
    pass


class FieldNotFound(QueryError):
    pass


class EmptyInput(InfluxDBError):
    pass


class InvalidData(InfluxDBError):
    pass


def _raise_error(error, chunk_idx=None, code=200):
    if chunk_idx is not None:
        error = '{code}: {chunk_idx}: {error}'.format(
            error=error,
            chunk_idx=chunk_idx,
            code=code
        )
    pe = error.lower()
    if 'measurement' in pe and 'not found' in pe:
        raise MeasurementNotFound(error)
    if 'unknown' in pe and 'field' in pe and 'tag' in pe:
        raise FieldNotFound(error)
    raise QueryError(error)


def _raise_response_error(response):
    chunk_idx = None
    error = response.reason
    code = response.status_code
    try:
        response = response.json()
    except:
        pass
    else:
        error = response.get('error')
        for chunk_idx, chunk in enumerate(response.get('results', [])):
            error = chunk.get('error')
            if error:
                break
    _raise_error(error, chunk_idx=chunk_idx, code=code)


class InfluxDBIOError(ValueError):
    def __init__(self, response):
        error = response.reason
        self.code = response.status_code
        try:
            response = response.json()
        except:
            pass
        else:
            error = response.get('error')
            if not error:
                for cnt, res in enumerate(response.get('results', [])):
                    error = res.get('error')
                    if error:
                        error = "{error}{cnt}".format(cnt='' if not cnt else " query: {0}".format(cnt), error=error)
                        break 
            
        super(InfluxDBIOError, self).__init__(
            "{0}: {1}".format(self.code, error))
        self.error = error


def push_indb(auth, data, compress=False, raise_error=True):
    """ push data to backend
    :param data: InfluxDB json data
    :param compress: if should compress data
    :param raise_error: if should raise error on response errors
    """
    js = data
    data = None 
    headers = {}
    if compress:
        s = StringIO.StringIO()
        g = gzip.GzipFile(fileobj=s, mode='w')
        g.write(json.dumps(data))
        g.close()
        data = s.getvalue()
        js = None
        headers.update({
            'Content-Type': 'application/gzip',
        })

    auth = auth.copy()
    auth.pop('qurl', None)
    url = auth.pop('wurl')
    response = requests.post(url,
                             json=js,
                             data=data,
                             params=auth,
                             headers=headers)

    if not raise_error:
        try:
            return response.json()['result']
        except:
            return response

    if response.status_code != 200:
        _raise_response_error(response)

    if response.content: 
        return response.json()['result']
    return response


def get_auth_indb(username=None, password=None,
                  url=None, qurl=None, wurl=None,
                  port=8086):
    """authentication
    :param username,password: username and password
    :param qurl: api base url, will be used to set unspecified qurl or wurl
    :param qurl: api query url
    :param wurl: api write url
    :param port: port number
    """
    if url:
        if not qurl:
            qurl = '{url}:{port}/query'.format(url=url, port=port)
        if not wurl:
            wurl = '{url}:{port}/write'.format(url=url, port=port)
    if not qurl:
        qurl = 'http://localhost:8086/query'
    if not wurl:
        wurl = 'http://localhost:8086/write'
    params = {
        'u': username,
        'p': password,
        'qurl': qurl,
        'wurl': wurl,
    }
    # At this time only simple username/password
    if not params.get('u') or not params.get('p'):
        raise InfluxDBAuthError('Authentication parameters not specified')
    
    return params


def query_indb(auth, query, database=None, chunked=False, raise_error=True):
    """query the backend
    :param auth: authentication by get_auth_indb
    :param query: sql-like query
    :param database: database to query
    :param chunked: if chunked response is needed
    :param raise_error: if should raise error on response errors
    """
    if not auth or not auth.get('u') or not auth.get('p'):
        raise InfluxDBAuthError('Authentication parameters not specified')
    
    params = auth.copy()
    params['q'] = query
    
    if database:
        params['db'] = database
        
    if chunked:
        params['chunked'] = chunked

    params.pop('wurl', None)
    url = params.pop('qurl')
    response = requests.get(url,
                            params=params)
    if response.status_code != 200:
        if raise_error:
            _raise_response_error(response)
        return response

    if chunked:
        _decoder = json.JSONDecoder()
        # Author: Adrian Sampson <adrian@radbox.org>
        # Source: https://gist.github.com/sampsyo/920215

        def loads(s):
            """A generator reading a sequence of JSON values from a string."""
            while s:
                s = s.strip()
                obj, pos = _decoder.raw_decode(s)
                if not pos:
                    raise ValueError('no JSON object found at %i' % pos)
                yield obj
                s = s[pos:]
                
        return list(loads(response.content.decode()))

    data = response.json()['results']
    if not raise_error:
        return data

    # simple error checks
    for chunk_idx, chunk in enumerate(data):
        error = chunk.get('error')
        if error:
            _raise_error(error, chunk_idx=chunk_idx)

    return data
