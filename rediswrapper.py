#!/usr/bin/env python3

# --------------------------------------------
# Author: flyer <flyer103@gmail.com>
# Date: 2015/03/11 20:41:37
# --------------------------------------------

"""Wrap the connetion operation to redis.
"""

import redis


class Wrapper(object):
    """"""
    instances = {}

    @staticmethod
    def client(host='localhost', port=6379, db=0, **kwargs):
        kwargs.update({'host': host, 'port': port, 'db': db})
        key = tuple(sorted(kwargs.items()))
        if not Wrapper.instances.get(key):
            pool_max_connections = kwargs.get('pool_max_connections', 3)
            redis_operation_timeout_sec = kwargs.get('redis_operation_timeout_sec', 10)
            error_pool_full_wait_sec = kwargs.get('error_pool_full_wait_sec', 0.1)
            error_hard_retry_limit = kwargs.get('error_hard_retry_limit', 100)
            error_server_full_wait_sec = kwargs.get('error_server_full_wait_sec', 2)
            error_host_unknown_wait_sec = kwargs.get('error_host_unknown_wait_sec', 20)
            error_server_port_dead_wait_sec = kwargs.get('error_port_dead_wait_sec', 5)

            conn_pool_args = (host, port, db)
            redis_conn_pool = redis.ConnectionPool(host=host,
                                                   port=port,
                                                   db=db,
                                                   retry_on_timeout=True,
                                                   socket_timeout=redis_operation_timeout_sec,
                                                   max_connections=pool_max_connections)
            Wrapper.instances[key] = redis.StrictRedis(connection_pool=redis_conn_pool)
        
        return Wrapper.instances[key]
            

if __name__ == '__main__':
    import sys
    
    rdb_0 = Wrapper.client()
    rdb_1 = Wrapper.client(db=0)
    rdb_2 = Wrapper.client(db=1)
    print('rdb0: {0}, rdb1: {1}, rdb2: {2}'.format(id(rdb_0), id(rdb_1), id(rdb_2)))

    print('before set key "name", value: '.format(rdb_0.get('name')))
    print('set key "name" to "flyer"')
    rdb_0.set('name', 'flyer')
    print('after set, the value of key "name" is ',format(rdb_0.get('name').decode('utf-8')))
    rdb_0.delete('name')
