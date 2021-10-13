import cores.core as core
import cores.utils as utils

date_format = {"%Y%m%d": "concat(y,m,d)", "%Y%m%d%H": "concat(y,m,d,h)"}


class Numberofrows(core.BaseRule):
    """
    表行数检测å
    """

    @staticmethod
    def get_rule_template():
        return """
        select
        '%s' as db_type,
        '%s' as db_name,
        '%s' as table_name,
        '%s' as col_name,
        '%s' as task_time,
        '%s' as rule_name,
        count(*) as value,
        null as details
        from  %s.%s where %s %s
        """

    def get_sql_statement(self, db_obj, target, obj_meta_manager, attr_id):
        db_type = db_obj['type']
        db_name = db_obj['database']
        tgt_result = obj_meta_manager.get_table_meta(target, self.date)
        tables = tgt_result['table_names']
        col_name = ''
        task_time = self.date
        rule_name = attr_id
        partion_format = 'and ' + \
                         date_format[tgt_result['partition']] + "='" + utils.get_time_str_format(self.date,
                                                                                                 format=tgt_result[
                                                                                                     'partition']) + "'"
        querys = [(self.get_rule_template() % (
            db_type,
            db_name,
            qs['table_name'],
            col_name,
            task_time,
            rule_name,
            db_obj['database'],
            qs['table_name'],
            qs['filter'], partion_format)
                   ) for qs in tables]
        self.log.info('rule: %s' % (self.__class__.__name__))
        return " union all ".join(querys)


class Fieldnonempty(core.BaseRule):
    """
    字段非空检测
    """

    @staticmethod
    def get_rule_template():
        # return """
        # select count(*) from ${table} where (${filter}) and (${field} is null)
        # """
        return """
        select
        '%s' as db_type,
        '%s' as db_name,
        '%s' as table_name,
        '%s' as col_name,
        '%s' as task_time,
        '%s' as rule_name,
        count(*) as value,
        null as details
        from  %s.%s where %s and %s is null %s
        """

    def get_sql_statement(self, db_obj, target, obj_meta_manager, attr_id):
        db_type = db_obj['type']
        db_name = db_obj['database']
        tgt_result = obj_meta_manager.get_table_meta(target, self.date)
        tables = tgt_result['table_names']
        task_time = self.date
        rule_name = attr_id
        partion_format = 'and ' + \
                         date_format[tgt_result['partition']] + "='" + utils.get_time_str_format(self.date,
                                                                                                 format=tgt_result[
                                                                                                     'partition']) + "'"
        querys = [(self.get_rule_template() % (
            db_type,
            db_name,
            qs['table_name'],
            qs['field'],
            task_time,
            rule_name,
            db_obj['database'],
            qs['table_name'],
            qs['filter'],
            qs['field'], partion_format)
                   ) for qs in tables]
        self.log.info('rule: %s' % (self.__class__.__name__))
        return " union all ".join(querys)


class Theprimarykey(core.BaseRule):
    """
    主键检测
    """

    @staticmethod
    def get_rule_template():
        # return """
        # select count(*) from ${table} where ${filter} and (${field_concat}) in (select ${field_concat} from ${table} where ${filter} group by ${field_concat} having count(*) > 1)
        # """
        return """
        select
        '%s' as db_type,
        '%s' as db_name,
        '%s' as table_name,
        '%s' as col_name,
        '%s' as task_time,
        '%s' as rule_name,
        count(*) as value,
        null as details
        from  %s.%s where %s and %s in (select %s from %s.%s where %s %s group by %s having count(*) > 1) %s
        """

    def get_sql_statement(self, db_obj, target, obj_meta_manager, attr_id):
        db_type = db_obj['type']
        db_name = db_obj['database']
        tgt_result = obj_meta_manager.get_table_meta(target, self.date)
        tables = tgt_result['table_names']
        col_name = ''
        task_time = self.date
        rule_name = attr_id
        partion_format = 'and ' + \
                         date_format[tgt_result['partition']] + "='" + utils.get_time_str_format(self.date,
                                                                                                 format=tgt_result[
                                                                                                     'partition']) + "'"
        querys = [(self.get_rule_template() % (
            db_type,
            db_name,
            qs['table_name'],
            col_name,
            task_time,
            rule_name,
            db_obj['database'],
            qs['table_name'],
            qs['filter'],
            qs['primarykey'],
            qs['primarykey'],
            db_obj['database'],
            qs['table_name'],
            qs['filter'],
            partion_format,
            qs['primarykey'], partion_format)
                   ) for qs in tables]
        self.log.info('rule: %s' % (self.__class__.__name__))
        return " union all ".join(querys)


class Theaverage(core.BaseRule):
    """
    平均值检测
    """

    @staticmethod
    def get_rule_template():
        # return """
        # select avg(${field}) from ${table} where ${filter}
        # """
        return """
        select
        '%s' as db_type,
        '%s' as db_name,
        '%s' as table_name,
        '%s' as col_name,
        '%s' as task_time,
        '%s' as rule_name,
        avg(%s) as value,
        null as details
        from  %s.%s where %s %s
        """

    def get_sql_statement(self, db_obj, target, obj_meta_manager, attr_id):
        db_type = db_obj['type']
        db_name = db_obj['database']
        tgt_result = obj_meta_manager.get_table_meta(target, self.date)
        tables = tgt_result['table_names']
        task_time = self.date
        rule_name = attr_id
        partion_format = 'and ' + \
                         date_format[tgt_result['partition']] + "='" + utils.get_time_str_format(self.date,
                                                                                                 format=tgt_result[
                                                                                                     'partition']) + "'"
        querys = [(self.get_rule_template() % (
            db_type,
            db_name,
            qs['table_name'],
            qs['field'],
            task_time,
            rule_name,
            qs['field'],
            db_obj['database'],
            qs['table_name'],
            qs['filter'], partion_format)
                   ) for qs in tables]
        self.log.info('rule: %s' % (self.__class__.__name__))
        return " union all ".join(querys)


class Summation(core.BaseRule):
    """
    总和检测
    """

    @staticmethod
    def get_rule_template():
        # return """
        # select sum(${field}) from ${table} where ${filter}
        # """
        return """
        select
        '%s' as db_type,
        '%s' as db_name,
        '%s' as table_name,
        '%s' as col_name,
        '%s' as task_time,
        '%s' as rule_name,
        sum(%s) as value,
        null as details
        from  %s.%s where %s %s
        """

    def get_sql_statement(self, db_obj, target, obj_meta_manager, attr_id):
        db_type = db_obj['type']
        db_name = db_obj['database']
        tgt_result = obj_meta_manager.get_table_meta(target, self.date)
        tables = tgt_result['table_names']
        task_time = self.date
        rule_name = attr_id
        partion_format = 'and ' + \
                         date_format[tgt_result['partition']] + "='" + utils.get_time_str_format(self.date,
                                                                                                 format=tgt_result[
                                                                                                     'partition']) + "'"
        querys = [(self.get_rule_template() % (
            db_type,
            db_name,
            qs['table_name'],
            qs['field'],
            task_time,
            rule_name,
            qs['field'],
            db_obj['database'],
            qs['table_name'],
            qs['filter'], partion_format)
                   ) for qs in tables]
        self.log.info('rule: %s' % (self.__class__.__name__))
        return " union all ".join(querys)


class Maximum(core.BaseRule):
    """
    最大值检测
    """

    @staticmethod
    def get_rule_template():
        # return """
        # select max(${field}) from ${table} where ${filter}
        # """
        return """
        select
        '%s' as db_type,
        '%s' as db_name,
        '%s' as table_name,
        '%s' as col_name,
        '%s' as task_time,
        '%s' as rule_name,
        max(%s) as value,
        null as details
        from  %s.%s where %s %s
        """

    def get_sql_statement(self, db_obj, target, obj_meta_manager, attr_id):
        db_type = db_obj['type']
        db_name = db_obj['database']
        tgt_result = obj_meta_manager.get_table_meta(target, self.date)
        tables = tgt_result['table_names']
        task_time = self.date
        rule_name = attr_id
        partion_format = 'and ' + \
                         date_format[tgt_result['partition']] + "='" + utils.get_time_str_format(self.date,
                                                                                                 format=tgt_result[
                                                                                                     'partition']) + "'"
        querys = [(self.get_rule_template() % (
            db_type,
            db_name,
            qs['table_name'],
            qs['field'],
            task_time,
            rule_name,
            qs['field'],
            db_obj['database'],
            qs['table_name'],
            qs['filter'], partion_format)
                   ) for qs in tables]
        self.log.info('rule: %s' % (self.__class__.__name__))
        return " union all ".join(querys)


class Minimum(core.BaseRule):
    """
    最小值检测
    """

    @staticmethod
    def get_rule_template():
        # return """
        # select min(${field}) from ${table} where ${filter}
        # """
        return """
        select
        '%s' as db_type,
        '%s' as db_name,
        '%s' as table_name,
        '%s' as col_name,
        '%s' as task_time,
        '%s' as rule_name,
        min(%s) as value,
        null as details
        from  %s.%s where %s %s
        """

    def get_sql_statement(self, db_obj, target, obj_meta_manager, attr_id):
        db_type = db_obj['type']
        db_name = db_obj['database']
        tgt_result = obj_meta_manager.get_table_meta(target, self.date)
        tables = tgt_result['table_names']
        task_time = self.date
        rule_name = attr_id
        partion_format = 'and ' + \
                         date_format[tgt_result['partition']] + "='" + utils.get_time_str_format(self.date,
                                                                                                 format=tgt_result[
                                                                                                     'partition']) + "'"
        querys = [(self.get_rule_template() % (
            db_type,
            db_name,
            qs['table_name'],
            qs['field'],
            task_time,
            rule_name,
            qs['field'],
            db_obj['database'],
            qs['table_name'],
            qs['filter'], partion_format)
                   ) for qs in tables]
        self.log.info('rule: %s' % (self.__class__.__name__))
        return " union all ".join(querys)


class Regularexpression(core.BaseRule):
    """
    正则表达式检测
    """

    @staticmethod
    def get_rule_template():
        # return """
        # select count(*) from ${table} where (${filter}) and regexp_like(${field} , '${regexp}')
        # """
        return """
            select
            '%s' as db_type,
            '%s' as db_name,
            '%s' as table_name,
            '%s' as col_name,
            '%s' as task_time,
            '%s' as rule_name,
            count(*) as value,
            null as details
            from  %s.%s where %s and regexp_like(%s , '%s')  %s
            """

    def get_sql_statement(self, db_obj, target, obj_meta_manager, attr_id):
        db_type = db_obj['type']
        db_name = db_obj['database']
        tgt_result = obj_meta_manager.get_table_meta(target, self.date)
        tables = tgt_result['table_names']
        task_time = self.date
        rule_name = attr_id
        partion_format = 'and ' + \
                         date_format[tgt_result['partition']] + "='" + utils.get_time_str_format(self.date,
                                                                                                 format=tgt_result[
                                                                                                     'partition']) + "'"
        querys = [(self.get_rule_template() % (
            db_type,
            db_name,
            qs['table_name'],
            qs['field'],
            task_time,
            rule_name + '_' + qs['regname'],
            db_obj['database'],
            qs['table_name'],
            qs['filter'],
            qs['field'],
            qs['regexp'], partion_format)
                   ) for qs in tables]
        self.log.info('rule: %s' % (self.__class__.__name__))
        return " union all ".join(querys)


class Timeformat(core.BaseRule):
    """
    时间格式检测
    """

    @staticmethod
    def get_rule_template():
        # return """
        # select count(*) from ${table} where (${filter}) and regexp_like(${field} , '${regexp}')
        # """
        return """
               select
               '%s' as db_type,
               '%s' as db_name,
               '%s' as table_name,
               '%s' as col_name,
               '%s' as task_time,
               '%s' as rule_name,
               count(*) as value,
               null as details
               from  %s.%s where %s and regexp_like(%s , '%s') %s
               """

    def get_sql_statement(self, db_obj, target, obj_meta_manager, attr_id):
        db_type = db_obj['type']
        db_name = db_obj['database']
        tgt_result = obj_meta_manager.get_table_meta(target, self.date)
        tables = tgt_result['table_names']
        task_time = self.date
        rule_name = attr_id
        partion_format = 'and ' + \
                         date_format[tgt_result['partition']] + "='" + utils.get_time_str_format(self.date,
                                                                                                 format=tgt_result[
                                                                                                     'partition']) + "'"
        querys = [(self.get_rule_template() % (
            db_type,
            db_name,
            qs['table_name'],
            qs['field'],
            task_time,
            rule_name,
            db_obj['database'],
            qs['table_name'],
            qs['filter'],
            qs['field'],
            qs['regexp'], partion_format)
                   ) for qs in tables]
        self.log.info('rule: %s' % (self.__class__.__name__))
        return " union all ".join(querys)


class Numericalformat(core.BaseRule):
    """
    数值格式检测
    """

    @staticmethod
    def get_rule_template():
        # return """
        # select count(*) from ${table} where (${filter}) and regexp_like(${field} , '${regexp}')
        # """
        return """
                 select
                 '%s' as db_type,
                 '%s' as db_name,
                 '%s' as table_name,
                 '%s' as col_name,
                 '%s' as task_time,
                 '%s' as rule_name,
                 count(*) as value,
                 null as details
                 from  %s.%s where %s and regexp_like(%s , '%s') %s
                 """

    def get_sql_statement(self, db_obj, target, obj_meta_manager, attr_id):
        db_type = db_obj['type']
        db_name = db_obj['database']
        tgt_result = obj_meta_manager.get_table_meta(target, self.date)
        tables = tgt_result['table_names']
        task_time = self.date
        rule_name = attr_id
        partion_format = 'and ' + \
                         date_format[tgt_result['partition']] + "='" + utils.get_time_str_format(self.date,
                                                                                                 format=tgt_result[
                                                                                                     'partition']) + "'"
        querys = [(self.get_rule_template() % (
            db_type,
            db_name,
            qs['table_name'],
            qs['field'],
            task_time,
            rule_name,
            db_obj['database'],
            qs['table_name'],
            qs['filter'],
            qs['field'],
            qs['regexp'], partion_format)
                   ) for qs in tables]
        self.log.info('rule: %s' % (self.__class__.__name__))
        return " union all ".join(querys)


class Enumerationvalue(core.BaseRule):
    """
    枚举值检测
    """

    @staticmethod
    def get_rule_template():
        # return """
        # select count(*) from ${table} where (${filter}) and (${field} not in ( ${list} ) or ${field} is null)
        # """
        return """
                     select
                     '%s' as db_type,
                     '%s' as db_name,
                     '%s' as table_name,
                     '%s' as col_name,
                     '%s' as task_time,
                     '%s' as rule_name,
                     count(*) as value,
                     null as details
                     from  %s.%s where %s and (%s not in ( %s ) or %s is null) %s
                     """

    def get_sql_statement(self, db_obj, target, obj_meta_manager, attr_id):
        db_type = db_obj['type']
        db_name = db_obj['database']
        tgt_result = obj_meta_manager.get_table_meta(target, self.date)
        tables = tgt_result['table_names']
        task_time = self.date
        rule_name = attr_id
        partion_format = 'and ' + \
                         date_format[tgt_result['partition']] + "='" + utils.get_time_str_format(self.date,
                                                                                                 format=tgt_result[
                                                                                                     'partition']) + "'"
        querys = [(self.get_rule_template() % (
            db_type,
            db_name,
            qs['table_name'],
            qs['field'],
            task_time,
            rule_name,
            db_obj['database'],
            qs['table_name'],
            qs['filter'],
            qs['field'],
            qs['enumvalue'],
            qs['field'], partion_format)
                   ) for qs in tables]
        self.log.info('rule: %s' % (self.__class__.__name__))
        return " union all ".join(querys)
