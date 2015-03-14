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
    def __init__(self, name, ttl=-1, host='localhost', port=6379, db=0, **kwargs):
        self.name = name
        self.ttl = ttl
        self.client = Wrapper(host=host, port=port, db=db, **kwargs).client

    def get_name(self):
        """get the name of the key"""
        return self.name

    def set_name(self, name):
        """set the name of the key"""
        self.name = name

    def delete(self):
        """delete the key.

        Return:
        + 0, the key doesn't exist before
        + 1, the key existed before and is deleted successfully
        """
        return self.client.delete(self.get_name())

    def exists(self):
        """detect whether the key exists"""
        return self.client.exists(self.get_name())

    def get_ttl(self):
        """get the ttl of the key"""
        return self.client.ttl(self.get_name())

    def set_ttl(self, ttl=0):
        """set the ttl of the key"""
        self.ttl = ttl
        
        return (ttl > 0) and self.client.expire(self.get_name(), ttl)

    def refresh(self):
        """refresh the ttl of the key"""
        return self.set_ttl(self.ttl)

    def rename(self, name):
        """rename the key"""
        self.set_name(name)
        
        try:
            return self.client.rename(self.get_name(), name)
        except redis.ResponseError:
            # the key may not exist
            return True
        except Exception:
            return False


if __name__ == '__main__':
    base = Base('cache', 30)
    base_1 = Base('cache', 30)
    print('id of base is {0}, id of base_1 is {1}'.format(id(base.client), id(base_1.client)))

    print('get_name: {0}'.format(base.get_name()))
    print('delete cache: {0}'.format(base.delete()))
    print('Does the key "cache" exists? {0}'.format(base.exists()))
    print('ttl: {0}'.format(base.get_ttl()))
    print('set ttl: {0}'.format(base.set_ttl(10)))
    print('refresh: {0}'.format(base.refresh()))
    print('rename: {0}'.format(base.rename('new')))
