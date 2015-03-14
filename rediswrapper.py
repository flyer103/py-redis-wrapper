#!/usr/bin/env python3

# --------------------------------------------
# Author: flyer <flyer103@gmail.com>
# Date: 2015/03/11 20:41:37
# --------------------------------------------

"""Wrap the connetion operation to redis.
"""

import redis


class Singleton(type):

    def __init__(cls, name, bases, dic):
        super(Singleton, cls).__init__(name, bases, dic)
        cls.instance = None

    def __call__(cls, *args, **kwargs):
        if cls.instance is None:
            cls.instance = super(Singleton, cls).__call__(*args, **kwargs)

        return cls.instance


class Wrapper(object, metaclass=Singleton):
    """"""
    def __init__(self, host='localhost', port=6379, db=0, **kwargs):
        self.pool_max_connections = kwargs.get('pool_max_connections', 3)
        self.redis_operation_timeout_sec = kwargs.get('redis_operation_timeout_sec', 10)
        self.error_pool_full_wait_sec = kwargs.get('error_pool_full_wait_sec', 0.1)
        self.error_hard_retry_limit = kwargs.get('error_hard_retry_limit', 100)
        self.error_server_full_wait_sec = kwargs.get('error_server_full_wait_sec', 2)
        self.error_host_unknown_wait_sec = kwargs.get('error_host_unknown_wait_sec', 20)
        self.error_server_port_dead_wait_sec = kwargs.get('error_port_dead_wait_sec', 5)

        conn_pool_args = (host, port, db)
        redis_conn_pool = redis.ConnectionPool(host=host,
                                               port=port,
                                               db=db,
                                               retry_on_timeout=True,
                                               socket_timeout=self.redis_operation_timeout_sec,
                                               max_connections=self.pool_max_connections)
        self.client = redis.StrictRedis(connection_pool=redis_conn_pool)
        self.client.ping()
            

if __name__ == '__main__':
    import sys
    
    try:
        rdb = Wrapper()
    except Exception as e:
        sys.exit(e)

    print('before set key "name", value: '.format(rdb.client.get('name')))
    print('set key "name" to "flyer"')
    rdb.client.set('name', 'flyer')
    print('after set, the value of key "name" is ',format(rdb.client.get('name').decode('utf-8')))
    rdb.client.delete('name')

    rdb_1 = Wrapper()
    print('id of rdb is {0}, id of rdb1 is {1}'.format(id(rdb), id(rdb_1)))
