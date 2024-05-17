import argparse
import logging
import math
import sys
from collections import deque
from functools import reduce
from itertools import chain
from sys import getsizeof, stderr
from venv import logger

from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import ExecutionProfile, Cluster
from cassandra.policies import WhiteListRoundRobinPolicy
from sortedcontainers import SortedDict
from threading import Thread, Event


class Estimator(object):
    """ The estimator class containes connetion, stats methods """

    def __init__(self, connection, stop_event, keyspace=None,
                 table=None, execution_timeout=None, token_step=None, rows_per_request=None, pagination=5000):
        self.connection = connection
        self.stop_event = stop_event
        self.keyspace = keyspace
        self.table = table
        self.execution_timeout = execution_timeout
        self.token_step = token_step
        self.rows_per_request = rows_per_request
        self.pagination = pagination
        self.rows_in_kilo_bytes = []

    def mean(self, lst):
        """ Calculate the mean of list of Cassandra values """
        final_mean = 0.0

        for n in lst:
            final_mean += n
        final_mean = final_mean / float(len(lst))
        return final_mean

    def weighted_mean(self, lst):
        """ Calculates the weighted mean of a list of Cassandra values """

        total = 0
        total_weight = 0
        normalized_weights = []

        # Set up some lists for our weighted values, and weighted means
        weights = [1 + n for n in range(len(lst))]
        normalized_weights = [0 for n in range(len(lst))]

        # Calculate a total of all weights
        total_weight = reduce(lambda y, x: x + y, weights)

        # Divide each weight by the sum of all weights
        for q, r in enumerate(weights):
            normalized_weights[q] = r / float(total_weight)

        # Add values of original List multipled by weighted values
        for q, r in enumerate(lst):
            total += r * normalized_weights[q]

        return total

    def median(self, lst):
        """ Calculate the median of Cassandra values """
        """ The middle value in the set """

        tmp_lst = sorted(lst)
        index = (len(lst) - 1) // 2

        # If the set has an even number of entries, combine the middle two
        # Otherwise print the middle value
        if len(lst) % 2 == 0:
            return ((tmp_lst[index] + tmp_lst[index + 1]) / 2.0)
        else:
            return tmp_lst[index]

    def quartiles(self, lst, q):
        """ Quartiles in stats are values that devide your data into quarters """
        lst.sort()
        cnt = len(lst)
        ith = int(math.floor(q * (cnt)))
        return lst[ith]

    def total_size(self, obj, handlers={}, verbose=False):
        """
            Gets ~ memory footprint an object and all of its contents.
            Finds the contents of the following builtin containers and
            their subclasses:  tuple, list, deque, dict, set and frozenset.
            To search other containers, add handlers to iterate over their contents:
            handlers = {SomeContainerClass: iter, OtherContainerClass: OtherContainerClass.get_elements}
        """
        dict_handler = lambda d: chain.from_iterable(d.items())
        all_handlers = {tuple: iter,
                        list: iter,
                        deque: iter,
                        dict: dict_handler,
                        set: iter,
                        frozenset: iter,
                        }
        all_handlers.update(handlers)
        seen = set()
        default_size = getsizeof(0)
        obj_empty_size = getsizeof('')

        def sizeof(obj):
            if id(obj) in seen:
                return 0
            seen.add(id(obj))
            s = getsizeof(obj, default_size)

            if verbose:
                print(s, type(obj), repr(obj), file=stderr)

            for typ, handler in all_handlers.items():
                if isinstance(obj, typ):
                    # Recursively call the function sizeof for each object handler to estimate the sum of bytes
                    s += sum(map(sizeof, handler(obj)))
                    break
            return s

        return sizeof(obj) - obj_empty_size

    def get_total_column_size(self):
        """ The method returns total size of field names in ResultSets """
        session = self.connection
        columns_results_stmt = session.prepare(
            "select column_name from system_schema.columns where keyspace_name=? and table_name=?")
        columns_results_stmt.consistency_level = ConsistencyLevel.LOCAL_ONE
        columns_results = session.execute(columns_results_stmt, [self.keyspace, self.table])
        clmsum = 0
        for col in columns_results:
            clmsum += self.total_size(col.column_name, verbose=False)
        return clmsum / 1024

    def get_partition_key(self):
        """ The method returns parition key """
        session = self.connection
        pk_results_stmt = session.prepare(
            "select column_name, position from system_schema.columns where keyspace_name=? and table_name=? and kind='partition_key' ALLOW FILTERING")
        pk_results_stmt.consistency_level = ConsistencyLevel.LOCAL_ONE
        pk_results = session.execute(pk_results_stmt, [self.keyspace, self.table])
        sd = SortedDict()
        for p in pk_results:
            sd[p.position] = p.column_name

        if len(sd) > 1:
            pk_string = ','.join(sd.values())
        else:
            pk_string = sd.values()[0]
        return pk_string

    def get_columns(self):
        """ The method returns all columns """
        session = self.connection
        columns_results_stmt = session.prepare(
            "select column_name from system_schema.columns where keyspace_name=? and table_name=?")
        columns_results_stmt.consistency_level = ConsistencyLevel.LOCAL_ONE
        columns_results = session.execute(columns_results_stmt, [self.keyspace, self.table])
        clms = []
        for c in columns_results:
            clms.append(c.column_name)
        if len(clms) > 1:
            cl_string = ','.join(clms)
        else:
            cl_string = clms[0]
        return cl_string

    def row_sampler(self):
        a = 0
        session = self.connection
        cl = self.get_columns()
        pk = self.get_partition_key()
        tbl_lookup_stmt = session.prepare(
            "SELECT * FROM " + self.keyspace + ".\"" + self.table + "\" WHERE token(" + pk + ")>? AND token(" + pk + ")<? LIMIT " + str(
                self.rows_per_request))
        tbl_lookup_stmt.consistency_level = ConsistencyLevel.LOCAL_ONE
        tbl_lookup_stmt.fetch_size = int(self.pagination)

        ring = []
        ring_values_by_step = []
        rows_in_kilo_bytes = []

        for r in session.cluster.metadata.token_map.ring:
            ring.append(r.value)
        for i in ring[::self.token_step]:
            ring_values_by_step.append(i)
        it = iter(ring_values_by_step)

        for val in it:
            try:
                results = session.execute(tbl_lookup_stmt, [val, next(it)])
                for row in results:
                    for value in row:
                        s1 = str(value)
                        a += (self.total_size(s1, verbose=False) / 1024)
                    rows_in_kilo_bytes.append(a)
                    a = 0
                    if self.stop_event.is_set():
                        break
            except StopIteration:
                logger.info("no more token pairs")
        self.rows_in_kilo_bytes = rows_in_kilo_bytes


def main():
    def add_helper(n):
        return columns_in_kilo_bytes + n

    logging.getLogger('cassandra').setLevel(logging.ERROR)
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

    p_hostname = "127.0.0.1"
    p_port = "9042"
    p_username = None
    p_password = None
    p_keyspace = "simple_cobi"
    p_execution_timeout = 360
    p_token_step = 4
    p_rows_per_request = 1000
    p_pagination = 200

    connection = get_connection(p_hostname, p_port, p_username, p_password)

    all_tables_in_keyspace = get_tables(connection, p_keyspace)

    total_max = 0
    table_name_with_max_rows = ""

    result = list()

    for table_name in all_tables_in_keyspace:
        # Event object used to send signals from one thread to another
        stop_event = Event()

        estimator = Estimator(connection, stop_event, p_keyspace, table_name, p_execution_timeout, p_token_step, p_rows_per_request,
                              p_pagination)

        action_thread = Thread(target=estimator.row_sampler())
        action_thread.start()
        action_thread.join(timeout=estimator.execution_timeout)
        stop_event.set()

        columns_in_kilo_bytes = estimator.get_total_column_size()
        rows_in_kilo_bytes = estimator.rows_in_kilo_bytes

        if (len(rows_in_kilo_bytes) == 0):
            continue

        rows_columns_in_kilo_bytes = map(add_helper, rows_in_kilo_bytes)
        val = list(rows_columns_in_kilo_bytes)
        current_max = max(val)
        current_avg = sum(val) / len(val)

        result.append((current_max, current_avg, table_name))

        if (total_max < current_max):
            total_max = current_max
            table_name_with_max_rows = table_name

    result.sort(key=lambda a: a[0])

    logging.info("Estimated size of column names and values in a row:")
    for result in result:
        logging.info("table=%s, max=%s kb, avg=%s kb", result[2], '{:.2f}'.format(result[0]), '{:.2f}'.format(result[1]))

    logging.info("summary:")
    logging.info("max row size =%s kb, table_name_with_max_rows = %s", '{:.4f}'.format(total_max), table_name_with_max_rows)


def get_tables(connection, keyspace):
    # Query the system schema for table names
    query = f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{keyspace}'"
    result = connection.execute(query)

    # Extract table names from the result
    tables = [row.table_name for row in result]

    return tables


def get_connection(host, port, username=None, password=None):
    """ Returns Cassandra session """

    auth_provider = PlainTextAuthProvider(username, password)
    node1_profile = ExecutionProfile(load_balancing_policy=WhiteListRoundRobinPolicy([host]))
    profiles = {'node1': node1_profile}
    cluster = Cluster([host], port=port, auth_provider=auth_provider, ssl_context=None,
                      control_connection_timeout=360, execution_profiles=profiles)
    session = cluster.connect()
    return session


if __name__ == "__main__":
    main()
