import functools
import datetime as dt
import pytz

def memoize(obj):
    cache = obj.cache = {}

    @functools.wraps(obj)
    def memoizer(*args, **kwargs):
        key = str(args) + str(kwargs)
        if key not in cache:
            cache[key] = obj(*args, **kwargs)
        return cache[key]
    return memoizer

def memoize_with_timeout(obj):
    cache = obj.cache = {}
    cache['timeout'] = 3600
    cache['start_time'] = dt.datetime.now(pytz.UTC)

    @functools.wraps(obj)
    def memoizer(*args, **kwargs):
        secs_elapsed = (dt.datetime.now(pytz.UTC) - cache['start_time']).total_seconds()
        key = str(args) + str(kwargs)
        if key not in cache:
            cache[key] = obj(*args, **kwargs)
        if secs_elapsed > cache['timeout']:
            cache[key] = obj(*args, **kwargs)
            cache['start_time'] = dt.datetime.now(pytz.UTC)
        return cache[key]
    return memoizer
