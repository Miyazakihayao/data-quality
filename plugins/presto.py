# encoding=utf-8

import cores.core as core
import pandas as pd
from data_engine.data_entity import *
from data_engine.data_connector import *
from cores.exception import RuleNotFoundError
import os


class Actuator(core.BasePlugin):
    def desc_data_flow(self, db, sql, time_offset='0D'):
        d = core.DataFlow()
        #        d.in_table(source['table'], self.get_time_str(source.get('time_offset','0D')))
        # d.out_table(target['table'], self.get_time_str(target.get('time_offset', '0D')))
        return d

    def execute(self, db, target, time_offset='0D',
                file=None, var_exprs=None, rule=None):
        try:
            self.log.info('entering presto.actuator')
            obj_meta_mgr = self.context.meta_manager
            date = self.date
            # 从元数据库td_table表中获取相关信息
            tgt_result = obj_meta_mgr.get_table_meta(target, date)
            tgt_location = tgt_result['db']
            tgt_app, tgt_tb_name, tgt_type = target['table'].split('.')
            tgt_file_encoding = tgt_result.get('encoding', 'utf8')
            sql = ''
            if file:
                f = open(file, 'r')
                for line in f:
                    lr = self.context.meta_manager.decorate_time_str(
                        line, self.date, time_offset)
                    print(lr.rstrip())
                    sql = sql + lr.rstrip() + os.linesep
                f.close()

            if tgt_type.lower() == 'mysql':
                db_obj = obj_meta_mgr.get_db_meta_by_cluster(db)
                if db_obj:
                    self.db_view_to_mysql(sql, db_obj, obj_meta_mgr, target, tgt_location, tgt_file_encoding,
                                          time_offset,
                                          var_exprs, rule)
                else:
                    raise MysqlError("db_obj[%s] is null" % db)

            else:
                raise MysqlError("the target type not supported!")

            self.log.info('presto_actuator() exiting normal')
        except BaseException:
            raise

    def db_view_to_mysql(self, sql, db_obj, obj_meta_manager, target, tgt_location, tgt_encoding, time_offset,
                         var_exprs, rulevalue):
        """
        Arguments:
        - `src_exinfo`
        - `tgt_location`
        - `tgt_encoding`
        """
        # tgt_file_path = tgt_location
        # self.log.info('tgt_file_path : %s' % tgt_file_path)
        #
        # utils.mkdirs_noexisted(tgt_file_path)
        # # 如果有变量字义，则根据变量来替换sql中的变量引用
        # if var_exprs:
        #     run_date = utils.get_time(self.date, time_offset)
        #     params = {}
        #     for k in var_exprs:
        #         r = eval(var_exprs[k])
        #         params[k] = r
        #     sql = self.context.meta_manager.decorate(sql, params)

        rule_len = len(rulevalue.split("."))
        rule_sql = ''
        if rule_len == 2:
            rule_id, attr_id = rulevalue.split(".")
            self.log.info("rule_id: " + rule_id)
            rule = self.context.rule_manager.get_rule(rule_id)

            if hasattr(rule, attr_id):
                a = getattr(rule, attr_id)
                p = a(self.task_conf, self.context, self.date)
                # 获取sql
                rule_sql = (
                    p.get_sql_statement(
                        db_obj,
                        target,
                        obj_meta_manager,
                        attr_id))
            else:
                raise RuleNotFoundError(
                    'rule %s.%s not found' %
                    (rule, attr_id))
        if rule_len == 3:
            rule_id, attr_id, indicator = rulevalue.split(".")
            self.log.info("rule_id: " + rule_id)
            rule = self.context.rule_manager.get_rule(rule_id)
            if hasattr(rule, attr_id):
                a = getattr(rule, attr_id)
                p = a(self.task_conf, self.context, self.date)
                p.log.info('indicator: %s' % (indicator))
                # 获取sql
                rule_sql = sql
            else:
                raise RuleNotFoundError(
                    'rule %s.%s not found' %
                    (rule, attr_id))
        # 获取连接
        presto_connect_string = "presto://%s:%d/%s/%s" % (
            db_obj['host'], db_obj['port'], db_obj['catalog'], db_obj['database'])
        connect_presto = DataConnector(presto_connect_string)
        engine = connect_presto.get_engine()
        df = pd.read_sql(rule_sql, engine)
        # 进一步规范列名
        df.columns = [
            'db_type',
            'db_name',
            'table_name',
            'col_name',
            'task_time',
            'rule_name',
            'value',
            'details']
        # 返回字典数据集
        result_dicts = df.to_dict(orient='records')
        # 转化成mysql实体
        result_lists = []
        for task_result in result_dicts:
            result_lists.append(dict2obj(task_result))

        db_obj = obj_meta_manager.get_db_meta_by_cluster(tgt_location)
        # 数据插入
        db_mysql_connect_string = "%s+pymysql://%s:%s@%s:%d/%s?charset=%s" % (

            db_obj['type'], db_obj['user'], db_obj['password'], db_obj['host'], db_obj['port'], db_obj['database'],
            db_obj['charset'])

        data_update(db_mysql_connect_string, result_lists)

        self.log.info('db_to_mysql() exiting normal')
