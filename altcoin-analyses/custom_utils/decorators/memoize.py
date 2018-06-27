import functools
import datetime as dt
import pytz

from itertools import tee
from types import GeneratorType

Tee = tee([], 1)[0].__class__

def memoize_generator(f):
    cache={}
    def ret(*args):
        if args not in cache:
            cache[args]=f(*args)
        if isinstance(cache[args], (GeneratorType, Tee)):
            # the original can't be used any more,
            # so we need to change the cache as well
            cache[args], r = tee(cache[args])
            return r
        return cache[args]
    return ret

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
