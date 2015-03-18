#!/usr/bin/env python3

# --------------------------------------------
# Author: flyer <flyer103@gmail.com>
# Date: 2015/03/18 20:20:20
# --------------------------------------------

"""Wrap the list operations to redis.
"""

from redisbase import Base


class RList(Base):
    """"""
    def __init__(self, name, ttl=-1, host='localhost', port=6379, db=0, **kwargs):
        super(RList, self).__init__(name, ttl, host, port, db, **kwargs)

    def lpush(self, value, *args):
        """prepend one or multiple values to a list"""
        return self._lpush(value, *args)

    def _lpush(self, value, *args):
        """prepend one or multiple values to a list and set expiration of the key"""
        ret = self.client.lpush(self._get_name(), value, *args)
        
        if self.get_ttl() < 0:
            self.set_ttl(self.get_default_ttl())

        return ret

    def rpop(self):
        """remove and get the last element in a list"""
        return self.client.rpop(self._get_name()) 

    def brpop(self):
        """remove and get the last element in a list, or block until one is avaiable"""
        return self.client.brpop(self._get_name())

    def rpush(self, value, *args):
        """append one or multiple values to a list"""
        return self._rpush(self._get_name(), value, *args)

    def _rpush(self, value, *args):
        """prepend one or multiple values to a list and set expiration of the key"""
        ret = self.client.rpush(self._get_name(), value, *args)
        
        if self.get_ttl() < 0:
            self.set_ttl(self.get_default_ttl())

        return ret    

    def lpop(self):
        """remove and get the first element in a list"""
        return self.client.lpop(self._get_name())

    def blpop(self):
        """remove and get the first element in a list, or block until one ie avaiable"""
        return self.client.blpop(self._get_name())

    def length(self):
        """get the length of a list"""
        return self.client.llen(self._get_name())

    def lrange(self, start, stop):
        """get a range of elements from a list"""
        return self.client.lrange(self._get_name(), start, stop)


if __name__ == '__main__':
    rdb = RList('mylist', 10)

    # count = rdb.lpush(1, 2, 3, 4)
    # print('push 1, 2, 3, 4, count: {0}'.format(count))
    # print('rpop, value: {0}, again: {1}'.format(rdb.rpop(), rdb.rpop()))

    count = rdb.rpush(1, 2, 3, 4)
    print('push 1, 2, 3, 4, count: {0}'.format(count))
    print('lpop, value: {0}, again: {1}'.format(rdb.lpop(), rdb.lpop()))
    print('length of the left is: {0}'.format(rdb.length()))
    print('range 0~10: {0}'.format(rdb.lrange(0, 10)))
