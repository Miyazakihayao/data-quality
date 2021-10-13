# encoding=utf-8


from typing import List
from data_engine.data_connector import *
from cores.exception import MysqlError

# 创建对象的基类:
Base = declarative_base()


class qualitis_task_result(Base):
    # 表的名字:
    __tablename__ = 'qualitis_task_result'

    # 表的结构:
    db_type = Column('db_type', String(100))
    db_name = Column('db_name', String(100))
    table_name = Column('table_name', String(100))
    col_name = Column('col_name', String(100))
    task_time = Column('task_time', String(100))
    rule_name = Column('rule_name', String(100))
    details = Column('details', String(10000))
    value = Column(DECIMAL(20, 6))
    __table_args__ = (
        PrimaryKeyConstraint(
            'db_type',
            'db_name',
            'table_name',
            'col_name',
            'task_time',
            'rule_name'),
        {},
    )


def dict2obj(rowdict) -> qualitis_task_result:
    return qualitis_task_result(db_type=rowdict['db_type'], db_name=rowdict['db_name'],
                                table_name=rowdict['table_name'], col_name=rowdict['col_name'],
                                task_time=rowdict['task_time'], rule_name=rowdict['rule_name'], value=rowdict['value'],
                                details=rowdict['details']
                                )


Vector = List[qualitis_task_result]


def data_update(connect_string, results: Vector):
    con_mysql = DataConnector(connect_string)
    session = con_mysql.get_connection()
    try:
        for result in results:
            r = session.query(qualitis_task_result).filter(qualitis_task_result.db_type == result.db_type,
                                                           qualitis_task_result.db_name == result.db_name,
                                                           qualitis_task_result.table_name == result.table_name,
                                                           qualitis_task_result.col_name == result.col_name,
                                                           qualitis_task_result.task_time == result.task_time,
                                                           qualitis_task_result.rule_name == result.rule_name).update(
                {"value": (result.value), "details": (result.details)})
            if r == 0:
                session.add(result)

        session.commit()
        session.close()
    except BaseException:
        raise MysqlError("update target table opreate faild!")


def data_insert(connect_string, results):
    con_mysql = DataConnector(connect_string)
    session = con_mysql.get_connection()
    try:
        session.add_all(results)
        session.commit()
        session.close()
    except BaseException:
        raise MysqlError("insert target table opreate faild!")


def data_query(connect_string, **kwargs) -> Vector:
    log = logging.getLogger('data_query')
    con_mysql = DataConnector(connect_string)
    session = con_mysql.get_connection()
    filter_dict = gen_filter_statements(**kwargs)
    try:
        query = session.query(qualitis_task_result).filter(
            eval(filter_dict['db_type']),
            eval(filter_dict['db_name']),
            eval(filter_dict['table_name']),
            eval(filter_dict['col_name']),
            eval(filter_dict['rule_name']),
            eval(filter_dict['task_time_begin']),
            eval(filter_dict['task_time_end']),
            eval(filter_dict['exp']))
        log.info('query is: %s' % (query))
        results = query.all()
        log.info('filter is: %s' % (filter_dict))
        session.close()
        return results
    except BaseException:
        raise MysqlError("query source table opreate faild!")


def gen_filter_statements(**kwargs):
    dic = {}
    dic['db_type'] = 'qualitis_task_result.db_type == "%s"' % (kwargs.get('db_type')) if (
            kwargs.get('db_type') is not None and kwargs.get('db_type') != '') else '1==1'
    dic['db_name'] = 'qualitis_task_result.db_name == "%s"' % (kwargs.get('db_name')) if (
            kwargs.get('db_name') is not None and kwargs.get('db_name') != '') else '1==1'
    dic['table_name'] = 'qualitis_task_result.table_name == "%s"' % (kwargs.get('table_name')) if (
            kwargs.get('table_name') is not None and kwargs.get('table_name') != '') else '1==1'
    dic['col_name'] = 'qualitis_task_result.col_name == "%s"' % (kwargs.get('col_name')) if (
            kwargs.get('col_name') is not None and kwargs.get('col_name') != '') else '1==1'
    dic['rule_name'] = 'qualitis_task_result.rule_name == "%s"' % (kwargs.get('rule_name')) if (
            kwargs.get('rule_name') is not None and kwargs.get('rule_name') != '') else '1==1'
    dic['task_time_begin'] = 'qualitis_task_result.task_time >= "%s"' % (kwargs.get('task_time_begin')) if (
            kwargs.get('task_time_begin') is not None and kwargs.get('task_time_begin') != '') else '1==1'
    dic['task_time_end'] = 'qualitis_task_result.task_time <= "%s"' % (kwargs.get('task_time_end')) if (
            kwargs.get('task_time_end') is not None and kwargs.get('task_time_end') != '') else '1==1'
    dic['exp'] = 'qualitis_task_result.value %s' % (kwargs.get('exp')) if (
            kwargs.get('exp') is not None and kwargs.get('exp') != '') else '1==1'
    return dic
