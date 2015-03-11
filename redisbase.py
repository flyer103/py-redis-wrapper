#!/usr/bin/env python3

# --------------------------------------------
# Author: flyer <flyer103@gmail.com>
# Date: 2015/03/11 21:03:20
# --------------------------------------------

"""Wrap the base operations to redis.
"""

from rediswrapper import Wrapper


class Base(object):
    """"""
    def __init__(self, name, ttl=-1, host='localhost', port=6379, db=0, **kwargs):
        self.name = name
        self.ttl = ttl
        self.rdb = Base.instance(host, port, db, **kwargs)

    @staticmethod
    def instance(host='localhost', port=6379, db=0, **kwargs):
        return Wrapper(host=host, port=port, db=db, **kwargs)

    def get_name(self):
        """get the name of the key"""
        return self.name

    def delete(self):
        """delete the key"""
        return self.rdb.delete(self.get_name())

    def exists(self):
        """detect whether the key exists"""
        return self.rdb.exists(self.get_name())

    def get_ttl(self):
        """get the ttl of the key"""
        return self.rdb.ttl(self.get_name())

    def set_ttl(self, ttl=0):
        """set the ttl of the key"""
        return (ttl > 0) && self.rdb.expire(self.get_name(), ttl);

    def refresh(self):
        """refresh the ttl of the key"""
        return self.set_ttl(self.ttl)

    def rename(self, old, new):
        """rename the key 'old' to 'new'"""
        try:
            return self.rdb.rename(old, new)
        except Exception:
            return False
