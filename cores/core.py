import logging
from functools import wraps

import utils


def debug(f):
    """
    to make a function transactional
    Arguments:
    - `f`: function
    """

    @wraps(f)
    def wrapper(*args, **kwds):
        """
        """
        log = args[0].log
        try:
            log.info('enter plugin: %s' % args[0].__class__.__name__)
            val = f(*args, **kwds)
            log.info('success exit plugin: %s' % args[0].__class__.__name__)
            return val
        except BaseException:
            log.info('error exit plugin: %s' % args[0].__class__.__name__)
            raise

    return wrapper


class BasePlugin(object):
    """
    base plugin class
    """

    def __init__(self, task_conf, context, date):
        """
        """
        self.task_conf = task_conf
        self.context = context
        self.date = date
        self.log = logging.getLogger(self.__class__.__name__)

    def get_time_str(self, delta, format=None):
        rtime = utils.get_time_str(self.date, delta, format)
        return rtime

    def desc_data_flow(self, **conf):
        return DataFlow()


class BaseRule(object):
    """
    base rule class
    """

    def __init__(self, task_conf, context, date):
        self.task_conf = task_conf
        self.context = context
        self.date = date
        self.log = logging.getLogger(self.__class__.__name__)

    def get_time_str(self, delta, format=None):
        rtime = utils.get_time_str(self.date, delta, format)
        return rtime


class DataDesc(object):
    ALL_COLUMN = '*'

    def __init__(self, table_ref_str, columns, data_time):
        self.table_ref_str = table_ref_str
        self.columns = columns
        self.data_time = data_time

    def __eq__(self, obj):
        if self is obj:
            return True
        else:
            return self.table_ref_str == obj.table_ref_str \
                and self.columns == self.columns \
                and self.data_time == self.data_time


class DataFlow(object):
    def __init__(self):
        self.in_data = []
        self.out_data = []

    def in_table(self, table_ref_str, data_time):
        self.in_data.append(DataDesc(table_ref_str, '*', data_time))

    def in_column(self, table_ref_str, columns, data_time):
        self.in_data.append(DataDesc(table_ref_str, columns, data_time))

    def out_table(self, table_ref_str, data_time):
        self.out_data.append(DataDesc(table_ref_str, '*', data_time))

    def out_column(self, table_ref_str, columns, data_time):
        self.out_data.append(DataDesc(table_ref_str, columns, data_time))

    def connect(self, dataflow):
        for i in dataflow.in_data:
            try:
                self.out_data.remove(i)
            except BaseException:
                if i not in self.in_data:
                    self.in_data.append(i)

        for i in dataflow.out_data:
            if i not in self.out_data:
                self.out_data.append(i)
