# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END

"""
Experimental browser for extracting reading data from the FogLAMP data buffer

Supports a number of REST API:

  http://<address>/foglamp/asset
     - Return a summary count of all asset readings
  http://<address>/foglamp/asset/{asset_code}
    - Return a set of asset readings for the given asset 
  http://<address>/foglamp/asset/{asset_code}/{reading}
    - Return a set of sensor readings for the specified asset and sensor
  http://<address>/foglamp/asset/{asset_code}/{reading}/summary
    - Return a summary (min, max and average) for the specified asset and sensor
  http://<address>/foglamp/asset/{asset_code}/{reading}/series
    - Return a time series (min, max and average) for the specified asset and
      sensor averages over seconds, minutes or hours. The selection of seconds, minutes
      or hours is done via the group query parameter

  All but the /foglamp/asset API call take a set of optional query parameters
    limit=x     Return the first x rows only
    skip=x      skip first n entries and used with limit to implemented paged interfaces
    seconds=x   Limit the data return to be less than x seconds old
    minutes=x   Limit the data returned to be less than x minutes old
    hours=x     Limit the data returned to be less than x hours old

  Note seconds, minutes and hours can not be combined in a URL. If they are then only seconds
  will have an effect.
"""

from aiohttp import web

from foglamp.common.storage_client.payload_builder import PayloadBuilder
from foglamp.services.core import connect


__author__ = "Mark Riddoch, Ashish Jabble"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"


__DEFAULT_LIMIT = 20
__DEFAULT_OFFSET = 0
__TIMESTAMP_FMT = 'YYYY-MM-DD HH24:MI:SS.MS'


def setup(app):
    """ Add the routes for the API endpoints supported by the data browser """
    app.router.add_route('GET', '/foglamp/asset', asset_counts)
    app.router.add_route('GET', '/foglamp/asset/{asset_code}', asset)
    app.router.add_route('GET', '/foglamp/asset/{asset_code}/{reading}', asset_reading)
    app.router.add_route('GET', '/foglamp/asset/{asset_code}/{reading}/summary', asset_summary)
    app.router.add_route('GET', '/foglamp/asset/{asset_code}/{reading}/series', asset_averages)


def prepare_limit_skip_payload(request, _dict):
    """ limit skip clause validation

    Args:
        request: request query params
        _dict: main payload dict
    Returns:
        chain payload dict
    """
    limit = __DEFAULT_LIMIT
    if 'limit' in request.query and request.query['limit'] != '':
        try:
            limit = int(request.query['limit'])
            if limit < 0:
                raise ValueError
        except ValueError:
            raise web.HTTPBadRequest(reason="Limit must be a positive integer")

    offset = __DEFAULT_OFFSET
    if 'skip' in request.query and request.query['skip'] != '':
        try:
            offset = int(request.query['skip'])
            if offset < 0:
                raise ValueError
        except ValueError:
            raise web.HTTPBadRequest(reason="Skip/Offset must be a positive integer")

    payload = PayloadBuilder(_dict).LIMIT(limit)
    if offset:
        payload = PayloadBuilder(_dict).SKIP(offset)

    return payload.chain_payload()


async def asset_counts(request):
    """ Browse all the assets for which we have recorded readings and
    return a readings count.

    Returns:
           json result on basis of SELECT asset_code, count(*) FROM readings GROUP BY asset_code;

    :Example:
            curl -X GET http://localhost:8081/foglamp/asset
    """
    payload = PayloadBuilder().AGGREGATE(["count", "*"]).ALIAS("aggregate", ("*", "count", "count"))\
        .GROUP_BY("asset_code").payload()

    results = {}
    try:
        _storage = connect.get_storage()
        results = _storage.query_tbl_with_payload('readings', payload)
        response = results['rows']
        asset_json = [{"count": r['count'], "assetCode": r['asset_code']} for r in response]
    except KeyError:
        raise web.HTTPBadRequest(reason=results['message'])
    except Exception as ex:
        raise web.HTTPException(reason=str(ex))

    return web.json_response(asset_json)


async def asset(request):
    """ Browse a particular asset for which we have recorded readings and
    return a readings with timestamps for the asset. The number of readings
    return is defaulted to a small number (20), this may be changed by supplying
    the query parameter ?limit=xx&skip=xx

    Returns:
          json result on basis of SELECT TO_CHAR(user_ts, '__TIMESTAMP_FMT') as "timestamp", (reading)::jsonFROM readings WHERE asset_code = 'asset_code' ORDER BY user_ts DESC LIMIT 20 OFFSET 0;

    :Example:
            curl -X GET http://localhost:8081/foglamp/asset/fogbench%2Fhumidity
            curl -X GET http://localhost:8081/foglamp/asset/fogbench%2Fhumidity?limit=1
            curl -X GET "http://localhost:8081/foglamp/asset/fogbench%2Fhumidity?limit=1&skip=1"
    """
    asset_code = request.match_info.get('asset_code', '')
    _select = PayloadBuilder().SELECT(("reading", "user_ts")).ALIAS("return", ("user_ts", "timestamp")). \
        FORMAT("return", ("user_ts", __TIMESTAMP_FMT)).chain_payload()
    _where = PayloadBuilder(_select).WHERE(["asset_code", "=", asset_code]).chain_payload()
    _and_where = where_clause(request, _where)

    # Add the order by and limit, offset clause
    _limit_skip_payload = prepare_limit_skip_payload(request, _and_where)
    payload = PayloadBuilder(_limit_skip_payload).ORDER_BY(["timestamp", "desc"]).payload()

    results = {}
    try:
        _storage = connect.get_storage()
        results = _storage.query_tbl_with_payload('readings', payload)
        response = results['rows']
    except KeyError:
        raise web.HTTPBadRequest(reason=results['message'])
    except Exception as ex:
        raise web.HTTPException(reason=str(ex))

    return web.json_response(response)


async def asset_reading(request):
    """ Browse a particular sensor value of a particular asset for which we have recorded readings and
    return the timestamp and reading value for that sensor. The number of rows returned
    is limited to a small number, this number may be altered by use of
    the query parameter limit=xxx&skip=xxx.

    The readings returned can also be time limited by use of the query
    parameter seconds=sss. This defines a number of seconds that the reading
    must have been processed in. Older readings than this will not be returned.

    The readings returned can also be time limited by use of the query
    parameter minutes=mmm. This defines a number of minutes that the reading
    must have been processed in. Older readings than this will not be returned.

    The readings returned can also be time limited by use of the query
    parameter hours=hh. This defines a number of hours that the reading
    must have been processed in. Older readings than this will not be returned.

    Only one of hour, minutes or seconds should be supplied

    Returns:
           json result on basis of SELECT TO_CHAR(user_ts, '__TIMESTAMP_FMT') as "timestamp", reading->>'reading' FROM readings WHERE asset_code = 'asset_code' ORDER BY user_ts DESC LIMIT 20 OFFSET 0;

    :Example:
            curl -X GET http://localhost:8081/foglamp/asset/fogbench%2Fhumidity/temperature
            curl -X GET http://localhost:8081/foglamp/asset/fogbench%2Fhumidity/temperature?limit=1
            curl -X GET http://localhost:8081/foglamp/asset/fogbench%2Fhumidity/temperature?skip=10
            curl -X GET "http://localhost:8081/foglamp/asset/fogbench%2Fhumidity/temperature?limit=1&skip=10"
    """
    asset_code = request.match_info.get('asset_code', '')
    reading = request.match_info.get('reading', '')

    _select = PayloadBuilder().SELECT(("user_ts", ["reading", reading]))\
        .ALIAS("return", ("user_ts", "timestamp"), ("reading", reading))\
        .FORMAT("return", ("user_ts", __TIMESTAMP_FMT)).chain_payload()
    _where = PayloadBuilder(_select).WHERE(["asset_code", "=", asset_code]).chain_payload()
    _and_where = where_clause(request, _where)

    # Add the order by and limit, offset clause
    _limit_skip_payload = prepare_limit_skip_payload(request, _and_where)
    payload = PayloadBuilder(_limit_skip_payload).ORDER_BY(["timestamp", "desc"]).payload()

    results = {}
    try:
        _storage = connect.get_storage()
        results = _storage.query_tbl_with_payload('readings', payload)
        response = results['rows']
    except KeyError:
        raise web.HTTPBadRequest(reason=results['message'])
    except Exception as ex:
        raise web.HTTPException(reason=str(ex))

    return web.json_response(response)


async def asset_summary(request):
    """ Browse all the assets for which we have recorded readings and
    return a summary for a particular sensor. The values that are
    returned are the min, max and average values of the sensor.

    The readings summarised can also be time limited by use of the query
    parameter seconds=sss. This defines a number of seconds that the reading
    must have been processed in. Older readings than this will not be summarised.

    The readings summarised can also be time limited by use of the query
    parameter minutes=mmm. This defines a number of minutes that the reading
    must have been processed in. Older readings than this will not be summarised.

    The readings summarised can also be time limited by use of the query
    parameter hours=hh. This defines a number of hours that the reading
    must have been processed in. Older readings than this will not be summarised.

    Only one of hour, minutes or seconds should be supplied

    Returns:
           json result on basis of SELECT MIN(reading->>'reading'), MAX(reading->>'reading'), AVG((reading->>'reading')::float) FROM readings WHERE asset_code = 'asset_code';

    :Example:
            curl -X GET http://localhost:8081/foglamp/asset/fogbench%2Fhumidity/temperature/summary
    """
    asset_code = request.match_info.get('asset_code', '')
    reading = request.match_info.get('reading', '')
    _aggregate = PayloadBuilder().AGGREGATE(["min", ["reading", reading]], ["max", ["reading", reading]],
                                            ["avg", ["reading", reading]])\
        .ALIAS('aggregate', ('reading', 'min', 'min'), ('reading', 'max', 'max'),
               ('reading', 'avg', 'average')).chain_payload()
    _where = PayloadBuilder(_aggregate).WHERE(["asset_code", "=", asset_code]).chain_payload()
    _and_where = where_clause(request, _where)
    payload = PayloadBuilder(_and_where).payload()

    results = {}
    try:
        _storage = connect.get_storage()
        results = _storage.query_tbl_with_payload('readings', payload)
        # for aggregates, so there can only ever be one row
        response = results['rows'][0]
    except KeyError:
        raise web.HTTPBadRequest(reason=results['message'])
    except Exception as ex:
        raise web.HTTPException(reason=str(ex))

    return web.json_response({reading: response})


async def asset_averages(request):
    """ Browse all the assets for which we have recorded readings and
    return a series of averages per second, minute or hour.

    The readings averaged can also be time limited by use of the query
    parameter seconds=sss. This defines a number of seconds that the reading
    must have been processed in. Older readings than this will not be summarised.

    The readings averaged can also be time limited by use of the query
    parameter minutes=mmm. This defines a number of minutes that the reading
    must have been processed in. Older readings than this will not be summarised.

    The readings averaged can also be time limited by use of the query
    parameter hours=hh. This defines a number of hours that the reading
    must have been processed in. Older readings than this will not be summarised.

    Only one of hour, minutes or seconds should be supplied

    The amount of time covered by each returned value is set using the
    query parameter group. This may be set to seconds, minutes or hours

    Returns:
            on the basis of
            SELECT min((reading->>'reading')::float) AS "min",
                   max((reading->>'reading')::float) AS "max",
                   avg((reading->>'reading')::float) AS "average",
                   to_char(user_ts, 'YYYY-MM-DD HH24:MI:SS') AS "timestamp"
            FROM foglamp.readings
                   WHERE asset_code = 'asset_code' AND
                     reading ? 'reading'
            GROUP BY to_char(user_ts, 'YYYY-MM-DD HH24:MI:SS')
            ORDER BY timestamp DESC;

    :Example:
            curl -X GET http://localhost:8081/foglamp/asset/fogbench%2Fhumidity/temperature/series
            curl -X GET "http://localhost:8081/foglamp/asset/fogbench%2Fhumidity/temperature/series?limit=1&skip=1"
            curl -X GET http://localhost:8081/foglamp/asset/fogbench%2Fhumidity/temperature/series?hours=1
            curl -X GET http://localhost:8081/foglamp/asset/fogbench%2Fhumidity/temperature/series?minutes=60
            curl -X GET http://localhost:8081/foglamp/asset/fogbench%2Fhumidity/temperature/series?seconds=3600
            curl -X GET http://localhost:8081/foglamp/asset/fogbench%2Fhumidity/temperature/series?group=seconds
            curl -X GET http://localhost:8081/foglamp/asset/fogbench%2Fhumidity/temperature/series?group=minutes
            curl -X GET http://localhost:8081/foglamp/asset/fogbench%2Fhumidity/temperature/series?group=hours
    """
    asset_code = request.match_info.get('asset_code', '')
    reading = request.match_info.get('reading', '')

    ts_restraint = 'YYYY-MM-DD HH24:MI:SS'
    if 'group' in request.query and request.query['group'] != '':
        _group = request.query['group']
        if _group in ('seconds', 'minutes', 'hours'):
            if _group == 'seconds':
                ts_restraint = 'YYYY-MM-DD HH24:MI:SS'
            elif _group == 'minutes':
                ts_restraint = 'YYYY-MM-DD HH24:MI'
            elif _group == 'hours':
                ts_restraint = 'YYYY-MM-DD HH24'
        else:
            raise web.HTTPBadRequest(reason="{} is not a valid group".format(_group))

    _aggregate = PayloadBuilder().AGGREGATE(["min", ["reading", reading]], ["max", ["reading", reading]],
                                            ["avg", ["reading", reading]])\
        .ALIAS('aggregate', ('reading', 'min', 'min'), ('reading', 'max', 'max'),
               ('reading', 'avg', 'average')).chain_payload()
    _where = PayloadBuilder(_aggregate).WHERE(["asset_code", "=", asset_code]).chain_payload()
    _and_where = where_clause(request, _where)

    # Add the GROUP BY
    _group = PayloadBuilder(_and_where).GROUP_BY("user_ts").ALIAS("group", ("user_ts", "timestamp"))\
        .FORMAT("group", ("user_ts", ts_restraint)).chain_payload()

    # Add LIMIT, OFFSET, ORDER BY timestamp DESC
    _limit_skip_payload = prepare_limit_skip_payload(request, _group)
    payload = PayloadBuilder(_limit_skip_payload).ORDER_BY(["timestamp", "desc"]).payload()

    results = {}
    try:
        _storage = connect.get_storage()
        results = _storage.query_tbl_with_payload('readings', payload)
        response = results['rows']
    except KeyError:
        raise web.HTTPBadRequest(reason=results['message'])
    except Exception as ex:
        raise web.HTTPException(reason=str(ex))

    return web.json_response(response)


def where_clause(request, where):
    val = 0
    try:
        if 'seconds' in request.query and request.query['seconds'] != '':
            val = int(request.query['seconds'])
        elif 'minutes' in request.query and request.query['minutes'] != '':
            val = int(request.query['minutes']) * 60
        elif 'hours' in request.query and request.query['hours'] != '':
            val = int(request.query['hours']) * 60 * 60

        if val < 0:
            raise ValueError
    except ValueError:
        raise web.HTTPBadRequest(reason="Time must be a positive integer")

    # if no time units then NO AND_WHERE condition applied
    if val == 0:
        return where

    payload = PayloadBuilder(where).AND_WHERE(['user_ts', 'newer', val]).chain_payload()
    return payload
