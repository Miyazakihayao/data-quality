# encoding=utf-8
import subprocess
import logging
import os

from exception import TransferError

logger = logging.getLogger('plugins_utils')


def get_tmp_path():
    return "/tmp/etljet"


def get_java_path():
    java_path = "java"
    java_home = os.environ.get('JAVA_HOME', None)
    if java_home:
        if java_home.endswith(os.sep):
            java_path = java_home + 'bin' + os.sep + 'java'
        else:
            java_path = java_home + os.sep + 'bin' + os.sep + 'java'
    return java_path


def get_pg_path():
    pg_path = 'psql'
    pg_home = os.environ.get('PG_HOME', None)
    if pg_home:
        if pg_home.endswith(os.sep):
            pg_path = pg_home + 'psql'
        else:
            pg_path = pg_home + os.sep + 'psql'
    return pg_path


def get_mysql_path():
    mysql_path = "mysql"
    mysql_home = os.environ.get('MYSQL_HOME', None)
    if mysql_home:
        if mysql_home.endswith(os.sep):
            mysql_path = mysql_home + 'mysql'
        else:
            mysql_path = mysql_home + os.sep + 'mysql'
    return mysql_path


def execute_sql(db_obj, meta_manager, sql, tgt_file_path=None, encoding="utf8", date=None, time_offset=None):
    if not tgt_file_path:
        tgt_file_path = ""
    else:
        tgt_file_path = " > " + tgt_file_path

    db_type = 'mysql'
    if 'type' in db_obj:
        db_type = db_obj['type']
    if db_type == 'postgres':
        logger.info("access postgres db.")
        sql_cmd = _assemble_postgres_script(db_obj, meta_manager, sql, tgt_file_path, encoding, date, time_offset)
    else:
        logger.info("access mysql db.")
        sql_cmd = _assemble_mysql_script(db_obj, meta_manager, sql, tgt_file_path, encoding, date, time_offset)
    logger.info(sql_cmd)

    (status, output) = subprocess.getstatusoutput(sql_cmd + " 2>/dev/null ")
    if status == 0:
        return output
    else:
        raise TransferError("sql execute fail, pls check the cmd!")


def _assemble_mysql_script(db_obj, meta_manager, sql, tgt_file_path=None, encoding="utf8", date=None, time_offset=None):
    sql = _assemble_mysql_file_script(sql)
    # 获取文件
    if encoding:
        sql = 'set names ' + encoding + ';' + sql
    if date:
        sql = meta_manager.decorate_time_str(sql, date, time_offset)

    sql_cmd = "%s -u%s -p'%s' -h%s -P%d -D%s -Ne '%s' %s" % (
        get_mysql_path(), db_obj['user'], db_obj['password'], db_obj['host'], db_obj['port'], db_obj['database'], sql,
        tgt_file_path)
    return sql_cmd


def _assemble_mysql_file_script(sql):
    if sql.endswith('.sql'):
        if sql.endswith(';'):
            sql = 'source ' + sql
        else:
            sql = 'source ' + sql + ';'
    elif sql.endswith('.sql;'):
        sql = 'source ' + sql

    return sql


def _assemble_postgres_script(db_obj, meta_manager, sql, tgt_file_path=None, encoding="utf8", date=None,
                              time_offset=None):
    sql = _assemble_postgres_file_script(sql)
    # 获取文件
    if encoding:
        sql = '(echo "set client_encoding to ' + encoding + ';' + sql + '")'
    if date:
        sql = meta_manager.decorate_time_str(sql, date, time_offset)

    sql_cmd = "%s | %s -U %s -h %s -p %s -t -d %s %s" % (
        sql, get_pg_path(), db_obj['user'],
        db_obj['host'], db_obj['port'], db_obj['database'], tgt_file_path)
    return sql_cmd


def _assemble_postgres_file_script(sql):
    if sql.endswith('.sql'):
        if sql.endswith(';'):
            sql = '\i ' + sql
        else:
            sql = '\i ' + sql + ';'
    elif sql.endswith('.sql;'):
        sql = '\i ' + sql

    return sql


def clear_local(file_name):
    logger.info("status=%s, output=%s" % subprocess.getstatusoutput("rm -r %s" % file_name))


def clear_hdfs(file_name):
    logger.info("status=%s, output=%s" % subprocess.getstatusoutput("hadoop fs -rmr %s" % file_name))
