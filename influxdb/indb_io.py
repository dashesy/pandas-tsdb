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


def push_indb(auth, data, compress=False):
    """ push data to backend
    :param data: InfluxDB json data
    :param compress: if should compress data
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

    if response.status_code != 200:
        raise InfluxDBIOError(response)
    
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


def query_indb(auth, query, database=None, chunked=False):
    """query the backend
    :param auth: authentication by get_auth_indb
    :param query: sql-like query
    :param database: database to query
    :param chunked: if chunked response is needed
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
        raise InfluxDBIOError(response)

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

    return response.json()['results']
