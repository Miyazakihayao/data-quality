# encoding=utf-8
import datetime
import logging
from string import Template
import os

import utils
import schema

GLOBAL_PARAM_CONF = "conf" + os.sep + "global_param.yaml"
SELF_PARAM_CONF_PATH = '_global_conf' + os.sep + 'param_conf.yaml'
SELF_DB_CONF_PATH = '_global_conf' + os.sep + 'db_conf.yaml'

logger = logging.getLogger('meta.MetaManager')


class MetaManager(object):
    """
    meta manager
    """

    def __init__(self, core_conf):
        self.task_base_path = os.environ['TASK_HOME']
        self.global_param = utils.load_yaml(GLOBAL_PARAM_CONF, home=True)
        self.meta_container = {}
        self.db_meta_container = {}
        self.var_param = {}
        self.task_conf_container = {}
        self.init_meta()
        self.init_param_conf()
        self.init_db_conf()
        self.init_task_conf()

    def init_meta(self):
        if os.path.exists("table_meta"):
            for f in os.listdir("table_meta"):
                logger.info("load table meta [ %s ]" % f)
                self._import_table_meta("table_meta" + os.sep + f)

    def init_param_conf(self):
        if self.task_base_path[-1:] != os.sep:
            self.task_base_path += os.sep
        param_conf = utils.load_yaml(self.task_base_path + SELF_PARAM_CONF_PATH)
        logger.debug('init param info is %s ' % param_conf)
        self.global_param.update(param_conf)

    def init_db_conf(self):
        if self.task_base_path[-1:] != os.sep:
            self.task_base_path += os.sep
        db_conf = utils.load_yaml(self.task_base_path + SELF_DB_CONF_PATH)
        logger.debug('init db info is %s ' % db_conf)
        self.db_meta_container.update(db_conf)

    def update_var_param(self, vars={}):
        logger.debug('update var info is %s ' % vars)
        self.var_param.update(vars)

    def init_task_conf(self):
        if os.path.exists("conf"):
            for f in os.listdir("conf"):
                logger.info("load task conf [ %s ]" % f)
                task_conf = utils.load_yaml("conf" + os.sep + f)
                self.task_conf_container.update(task_conf)

    def get_global_int(self, key, default):
        return int(self.global_param.get(key, default))

    def get_global_str(self, key, default):
        return self.global_param.get(key, default)

    def get_global(self):
        return self.global_param

    def decorate(self, str, extra_param):
        """
        substitute the variable in the string into value
        Arguments:
        - `self`:
        - `str`:
        """
        t = Template(str)
        p = {}
        p.update(self.global_param)
        p.update(self.var_param)
        p.update(extra_param)
        return t.safe_substitute(p)

    def decorate_time_str(self, str, cur_date, time_offset):
        return self.decorate(str, get_time_var_scope(cur_date, time_offset))

    # insert data to td_table
    def _import_table_meta(self, file):
        obj = utils.load_yaml(file)
        schema.check_schema(schema.TABLE_META_SCHEMA, obj, file)
        app_name = obj['app_name']
        tb_name = obj['tb_name']
        tb_type = obj['type']
        self.meta_container[app_name + '.' + tb_name + '.' + tb_type] = obj

    def get_col_str_by_app_table(self, app_name, tb_name, tb_type):
        col_list = self.get_col_list_by_app_table(app_name, tb_name, tb_type)
        return ','.join(col_list)

    def get_col_list_by_app_table(self, app_name, tb_name, tb_type):
        tb_obj = self.meta_container.get(app_name + '.' + tb_name + '.' + tb_type)
        col_list = []
        for col in tb_obj['field_list']:
            col_name = col['field']
            col_list.append(col_name)
        return col_list

    def get_col_list_by_table_ref(self, table_ref_str):
        app, table, tb_type = table_ref_str.split('.')
        return self.get_col_list_by_app_table(app, table, tb_type)

    def get_col_type_list_by_app_table(self, app_name, tb_name, tb_type):
        tb_obj = self.meta_container.get(app_name + '.' + tb_name + '.' + tb_type)
        col_type_list = []
        for col in tb_obj['field_list']:
            col_type = col['type']
            col_type_list.append(col_type)
        return col_type_list

    def get_col_type_list_by_table_ref(self, table_ref_str):
        app, table, tb_type = table_ref_str.split('.')
        return self.get_col_type_list_by_app_table(app, table, tb_type)

    def get_col_valid_list_by_app_table(self, app_name, tb_name, tb_type):
        tb_obj = self.meta_container.get(app_name + '.' + tb_name + '.' + tb_type)
        valid_list = []
        for col in tb_obj['field_list']:
            valid = col.get('valid', None)
            valid_list.append(valid)
        return valid_list

    def get_col_valid_list_by_table_ref(self, table_ref_str):
        app, table, tb_type = table_ref_str.split('.')
        return self.get_col_valid_list_by_app_table(app, table, tb_type)

    def get_col2inx_by_app_table(self, app_name, tb_name, tb_type):
        tb_obj = self.meta_container.get(app_name + '.' + tb_name + '.' + tb_type)
        col_list = tb_obj['field_list']
        col2idx = {}
        for i in range(len(col_list)):
            col_name = col_list[i]['field']
            col2idx[col_name] = i
        return col2idx

    def get_col2inx_by_table_ref(self, table_ref_str):
        app, table, tb_type = table_ref_str.split('.')
        return self.get_col2inx_by_app_table(app, table, tb_type)

    def get_table_meta_by_table(self, app_name, tb_name, tb_type):
        tb_obj = self.meta_container.get(app_name + '.' + tb_name + '.' + tb_type)
        return tb_obj

    def get_table_meta(self, meta, cur_date):
        app, table, type = meta['table'].split(".")
        meta_data = self.get_table_meta_by_table(app_name=app, tb_name=table, tb_type=type)
        data = meta_data.copy()
        logger.debug("table meta[app_name:%s, tb_name:%s, tb_type:%s]: %s" % (app, table, type, data))
        if data:
            old_str = data['db']
            data['db'] = self.decorate(data['db'],
                                       get_time_var_scope(cur_date, meta.get('time_offset', '0D')))
            logger.info('data db %s -> %s' % (old_str, data['db']))
            old_md5_str = data.get('md5_path', None)
            if old_md5_str:
                data['md5_path'] = self.decorate(data['md5_path'],
                                                 get_time_var_scope(cur_date, meta.get('time_offset', '0D')))
                logger.info('data md5 file %s -> %s' % (old_md5_str, data['md5_path']))
            old_del_filter_str = data.get('del_filter', None)
            if old_del_filter_str:
                data['del_filter'] = self.decorate(data['del_filter'],
                                                   get_time_var_scope(cur_date, meta.get('time_offset', '0D')))
                logger.info('data del_filter %s -> %s' % (old_del_filter_str, data['del_filter']))
            old_partition = data.get('partition', None)
            if old_partition:
                data['partition'] = self.decorate(data['partition'],
                                                  get_time_var_scope(cur_date, meta.get('time_offset', '0D')))
                logger.info('data partition %s -> %s' % (old_partition, data['partition']))
            return data
        else:
            return None

    def get_table_meta_by_table_ref(self, table_ref_str, cur_date, time_offset):
        app, table, type = table_ref_str.split(".")
        meta_data = self.get_table_meta_by_table(app_name=app, tb_name=table, tb_type=type)
        data = meta_data.copy()
        logger.debug("table meta[app_name:%s, tb_name:%s, tb_type:%s]: %s" % (app, table, type, data))
        if data:
            old_str = data['db']
            data['db'] = self.decorate(data['db'], get_time_var_scope(cur_date, time_offset))
            logger.info('data db %s -> %s' % (old_str, data['db']))
            old_md5_str = data.get('md5_path', None)
            if old_md5_str:
                data['md5_path'] = self.decorate(data['md5_path'], get_time_var_scope(cur_date, time_offset))
                logger.info('data file %s -> %s' % (old_md5_str, data['md5_path']))
            return data
        else:
            return None

    def render_task_meta(self, meta, cur_date):
        old_partition = meta.get('partition', None)
        if old_partition:
            meta['partition'] = self.decorate(meta['partition'],
                                              get_time_var_scope(cur_date, meta.get('time_offset', '0D')))
            logger.info('partition %s -> %s' % (old_partition, meta['partition']))
        old_filter = meta.get('filter', None)
        if old_filter:
            meta['filter'] = self.decorate(meta['filter'],
                                           get_time_var_scope(cur_date, meta.get('time_offset', '0D')))
            logger.info('filter %s -> %s' % (old_filter, meta['filter']))
        old_to = meta.get('to', None)
        if old_to:
            meta['to'] = self.decorate(meta['to'],
                                       get_time_var_scope(cur_date, meta.get('time_offset', '0D')))
            logger.info('mail to %s -> %s' % (old_to, meta['to']))
        return meta

    def get_db_meta_by_cluster(self, cluster_name):
        db_obj = self.db_meta_container.get(cluster_name)
        return db_obj

    def get_task_conf(self, conf_key):
        task_conf_obj = self.task_conf_container.get(conf_key)
        return task_conf_obj


def get_date_str(date, delta):
    import_date = datetime.datetime.strptime(date, '%Y%m%d')
    date_rtn = import_date + datetime.timedelta(delta)
    return date_rtn.strftime('%Y%m%d')


def get_time_var_scope(date, delta):
    time_result = utils.get_time(date, delta)
    return {
        'YYYYmm': time_result.strftime('%Y%m'),
        'YYYYmmdd': time_result.strftime('%Y%m%d'),
        'YYYYmmddHH': time_result.strftime('%Y%m%d%H'),
        'YYYYmmddHHMM': time_result.strftime('%Y%m%d%H%M'),
        'YYYY': time_result.strftime('%Y'),
        'mm': time_result.strftime('%m'),
        'dd': time_result.strftime('%d'),
        'HH': time_result.strftime('%H'),
        'MM': time_result.strftime('%M')
    }
