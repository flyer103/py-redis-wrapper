#!/usr/bin/env python3

# --------------------------------------------
# Author: flyer <flyer103@gmail.com>
# Date: 2015/03/15 16:39:52
# --------------------------------------------

"""Wrap the string operations to redis.
"""

import redis

from redisbase import Base


class RString(Base):
    """"""
    def __init__(self, name, ttl=-1, host='localhost', port=6379, db=0, **kwargs):
        super(RString, self).__init__(name, ttl, host, port, db, **kwargs)

    def get(self):
        """get the value of the key"""
        return self.client.get(self._get_name())

    def mget(self, lst=[]):
        """get the values of all the given keys"""
        tmp_lst = ['{0}:{1}'.format(self._get_prefix(), key) for key in lst]
        try:
            return self.client.mget(tmp_lst)
        except redis.ResponseError:
            return []
        except Exception:
            return None

    def set(self, value):
        """set the string value of the key"""
        default_ttl = self.get_default_ttl()
        if default_ttl > 0:
            return self.setex(default_ttl, value)
        else:
            return self.client.set(self._get_name(), value)

    def setex(self, seconds, value):
        """set the value and expiration of a key"""
        return self.client.setex(self._get_name(), seconds, value)
        
    def mset(self, dic={}):
        """set multiple keys to multiple values"""
        tmp_dict = {'{0}:{1}'.format(self._get_prefix(), k): v for k, v in dic.items()}
        
        try:
            ret = self.client.mset(tmp_dict)
        except redis.ResponseError:
            # `tmp_dict` may be empty
            ret = True
        except Exception:
            ret = False

        host, port, db, kwargs = self.get_redis_config()
        for key in dic:
            RString(key, self.get_default_ttl(), host, port, db, **kwargs).refresh()
            
        return ret

    def getset(self, value):
        """set the string value of the key and return its old value"""
        return self.client.getset(self._get_name(), value)
 
    def incr_by(self, value=1):
        """increment the integer value of the key by the given amount"""
        return self.client.incrby(self._get_name(), value)

    def incr(self):
        """increment the integer value of the key by 1"""
        return self.incr_by()

    def decr_by(self, value=1):
        """decrement the integer value of the key by the given number"""
        return self.client.decr(self._get_name(), value)

    def decr(self):
        """decrement the integer value of the key by 1"""
        return self.decr_by()

    def get_length(self):
        """get the length of the value"""
        return self.client.strlen(self._get_name())

    
if __name__ == '__main__':
    rdb = RString('cache', 10)

    print('prefix: "{0}"\n'.format(rdb._get_prefix()))

    print('name of the key: "{0}"'.format(rdb.get_name()))
    print('origin ttl: {0}'.format(rdb.get_default_ttl()))
    print('value: {0}'.format(rdb.get()))
    print('values of keys: {0}\n'.format(rdb.mget()))

    print('set key "cache" to "flyer"')
    rdb.set('flyer')
    print('value: {0}'.format(rdb.get()))
    print('length: {0}'.format(rdb.get_length()))
    print('ttl: {0}\n'.format(rdb.get_ttl()))

    print('multi set')
    print('empty dict: {0}'.format(rdb.mset()))
    print('normal dict: {0}'.format(rdb.mset({'name': 'flyer', 'gender': 'male'})))
    print('values: {0}\n'.format(rdb.mget(['name', 'gender'])))

    print('getset: {0}'.format(rdb.getset('test')))
    print('ttl: {0}\n'.format(rdb.get_ttl()))

    print('set key "cache" to 1 and increment by 10')
    rdb.set(1)
    print('length: {0}'.format(rdb.get_length()))
    rdb.incr_by(10)
    print('value of key "cache": {0}'.format(rdb.get()))
    print('length: {0}'.format(rdb.get_length()))
    print('after +1, value of key "cache": {0}'.format(rdb.incr()))
    print('decrement by 3, value: {0}'.format(rdb.decr_by(3)))
    print('after -1, value: {0}\n'.format(rdb.decr()))
