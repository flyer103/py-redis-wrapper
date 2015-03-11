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
    REDIS_DB_CONN_POOLS = {}
    
    def __init__(self, host='localhost', port=6379, db=0, **kwargs):
        self.pool_max_connections = kwargs.get('pool_max_connections', 3)
        self.redis_operation_timeout_sec = kwargs.get('redis_operation_timeout_sec', 10)
        self.error_pool_full_wait_sec = kwargs.get('error_pool_full_wait_sec', 0.1)
        self.error_hard_retry_limit = kwargs.get('error_hard_retry_limit', 100)
        self.error_server_full_wait_sec = kwargs.get('error_server_full_wait_sec', 2)
        self.error_host_unknown_wait_sec = kwargs.get('error_host_unknown_wait_sec', 20)
        self.error_server_port_dead_wait_sec = kwargs.get('error_port_dead_wait_sec', 5)

        conn_pool_args = (host, port, db)
        if conn_pool_args in Wrapper.REDIS_DB_CONN_POOLS:
            redis_conn_pool = Wrapper.REDIS_DB_CONN_POOLS[conn_pool_args]
        else:
            redis_conn_pool = redis.ConnectionPool(host=host,
                                                   port=port,
                                                   db=db,
                                                   retry_on_timeout=True,
                                                   socket_timeout=self.redis_operation_timeout_sec,
                                                   max_connections=self.pool_max_connections)
            Wrapper.REDIS_DB_CONN_POOLS[conn_pool_args] = redis_conn_pool

        self.client = redis.StrictRedis(connection_pool=redis_conn_pool)
        self.client.ping()
