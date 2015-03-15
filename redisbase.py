#!/usr/bin/env python3

# --------------------------------------------
# Author: flyer <flyer103@gmail.com>
# Date: 2015/03/11 21:03:20
# --------------------------------------------

"""Wrap the base operations to redis.
"""

import redis

from rediswrapper import Wrapper


class Base(object):
    """"""
    PREFIX = {
        'base': 'base',
        'rstring': 'str',
        'rhash': 'hash',
        'rlist': 'list',
        'rset': 'set',
        'rzset': 'zset'
    }
    
    def __init__(self, name, ttl=-1, host='localhost', port=6379, db=0, **kwargs):
        self.host = host
        self.port = port
        self.db = db
        self.kwargs = kwargs
        
        self.name_ud = name     # user defined name
        self._set_name(name)    # stored name
        self.ttl = ttl
        self.client = Wrapper(self.host, self.port, self.db, **self.kwargs).client

    def get_redis_config(self):
        """get the config of redis"""
        return self.host, self.port, self.db, self.kwargs
    
    def _get_prefix(self):
        """get the prefix"""
        return Base.PREFIX[type(self).__name__.lower()]
        
    def get_name(self):
        """get the user-defined name of the key"""
        return self.name_ud

    def set_name(self, name):
        """set the user-defined name of the key"""
        self.name_ud = name

    def _get_name(self):
        """get the stored name"""
        return self.name_stored
        
    def _set_name(self, name):
        """set the stored name of the key"""
        self.name_stored = '{0}:{1}'.format(self._get_prefix(), self.name_ud)

    def delete(self):
        """delete the key.

        Return:
        + 0, the key doesn't exist before
        + 1, the key existed before and is deleted successfully
        """
        return self.client.delete(self._get_name())

    def exists(self):
        """detect whether the key exists"""
        return self.client.exists(self._get_name())

    def get_ttl(self):
        """get the ttl of the key"""
        return self.client.ttl(self._get_name())

    def get_default_ttl(self):
        """get the original ttl"""
        return self.ttl
    
    def set_ttl(self, ttl=0):
        """set the ttl of the key"""
        self.ttl = ttl
        
        return (ttl > 0) and self.client.expire(self._get_name(), ttl)

    def refresh(self):
        """refresh the ttl of the key"""
        return self.set_ttl(self.get_default_ttl())

    def rename(self, name):
        """rename the key"""
        self.set_name(name)
        old_name = self._get_name()
        self._set_name(name)
        new_name = self._get_name()
        
        try:
            return self.client.rename(old_name, new_name)
        except redis.ResponseError:
            # the key may not exist
            return True
        except Exception:
            return False

    def get_type(self):
        """determine the type of the key"""
        return self.client.type(self._get_name())


if __name__ == '__main__':
    base = Base('cache', 30)
    base_1 = Base('cache', 30)
    print('id of base is {0}, id of base_1 is {1}\n'.format(id(base.client), id(base_1.client)))

    print('prefix: {0}'.format(base._get_prefix()))
    print('redis config: {0}'.format(base.get_redis_config()))
    print('get_name: {0}'.format(base.get_name()))
    print('delete cache: {0}'.format(base.delete()))
    print('Does the key "cache" exists? {0}'.format(base.exists()))
    print('ttl: {0}'.format(base.get_ttl()))
    print('default ttl: {0}'.format(base.get_default_ttl()))
    print('set ttl: {0}'.format(base.set_ttl(10)))
    print('default ttl: {0}'.format(base.get_default_ttl()))
    print('refresh: {0}'.format(base.refresh()))
    print('rename: {0}'.format(base.rename('new')))
    print('type: {0}'.format(base.get_type()))
    print('sotred_name: "{0}"\n'.format(base._get_name()))
