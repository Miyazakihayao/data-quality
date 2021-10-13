# encoding=utf-8
import os
import datetime
import yaml
import socket

YYYYMM_LEN = 6
YYYYMMDD_LEN = 8
YYYYMMDDHH_LEN = 10
YYYYMMDDHHMM_LEN = 12
YYYYMMDDHHMMSS_LEN = 14


class UtilsError(Exception):
    pass


def get_home_path(r_path):
    try:
        task_home = os.environ['DATAQA_HOME']
    except Exception:
        task_home = "."
    if task_home[-1:] != os.sep:
        task_home += os.sep
    return task_home + r_path


#
def load_yaml(path, home=False):
    if home:
        file_path = get_home_path(path)
    else:
        file_path = path
    f = open(file_path, "r")
    return yaml.safe_load(f)


def get_time(date, delta):
    len_date = len(date)
    if len_date == YYYYMM_LEN:
        import_time = datetime.datetime.strptime(date, '%Y%m')
    elif len_date == YYYYMMDD_LEN:
        import_time = datetime.datetime.strptime(date, '%Y%m%d')
    elif len_date == YYYYMMDDHH_LEN:
        import_time = datetime.datetime.strptime(date, '%Y%m%d%H')
    elif len_date == YYYYMMDDHHMM_LEN:
        import_time = datetime.datetime.strptime(date, '%Y%m%d%H%M')
    elif len_date == YYYYMMDDHHMMSS_LEN:
        import_time = datetime.datetime.strptime(date, '%Y%m%d%H%M%S')
    else:
        raise RuntimeError('invalid time format')

    delta_v = int(delta[:-1])
    delta_f = delta[-1:]

    time_delta = {
        'D': lambda x: datetime.timedelta(days=x),
        'H': lambda x: datetime.timedelta(hours=x),
        'M': lambda x: datetime.timedelta(minutes=x),
    }[delta_f](delta_v)

    time_result = import_time + time_delta
    return time_result


def mkdirs_noexisted(path, is_dir=False):
    """mkdir if not exists"""
    tgt_dir = path
    if not is_dir:
        tgt_dir = path[0:path.rfind(os.sep)]
    if os.path.exists(tgt_dir):
        pass
    else:
        try:
            os.makedirs(tgt_dir)
        except OSError as ose:
            if "File exists" in ("%s" % ose):
                pass
            else:
                raise


def get_time(date, delta):
    len_date = len(date)
    if len_date == YYYYMM_LEN:
        import_time = datetime.datetime.strptime(date, '%Y%m')
    elif len_date == YYYYMMDD_LEN:
        import_time = datetime.datetime.strptime(date, '%Y%m%d')
    elif len_date == YYYYMMDDHH_LEN:
        import_time = datetime.datetime.strptime(date, '%Y%m%d%H')
    elif len_date == YYYYMMDDHHMM_LEN:
        import_time = datetime.datetime.strptime(date, '%Y%m%d%H%M')
    elif len_date == YYYYMMDDHHMMSS_LEN:
        import_time = datetime.datetime.strptime(date, '%Y%m%d%H%M%S')
    else:
        raise RuntimeError('invalid time format')

    delta_v = int(delta[:-1])
    delta_f = delta[-1:]

    time_delta = {
        'D': lambda x: datetime.timedelta(days=x),
        'H': lambda x: datetime.timedelta(hours=x),
        'M': lambda x: datetime.timedelta(minutes=x),
    }[delta_f](delta_v)

    time_result = import_time + time_delta
    return time_result


def get_time_str(date, delta, format=None):
    rtime = get_time(date, delta)
    len_date = len(date)
    if len_date == YYYYMM_LEN:
        rtime_str = datetime.datetime.strptime(rtime, '%Y%m')
    elif len_date == YYYYMMDD_LEN:
        rtime_str = datetime.datetime.strftime(rtime, '%Y%m%d')
    elif len_date == YYYYMMDDHH_LEN:
        rtime_str = datetime.datetime.strftime(rtime, '%Y%m%d%H')
    elif len_date == YYYYMMDDHHMM_LEN:
        rtime_str = datetime.datetime.strftime(rtime, '%Y%m%d%H%M')
    elif len_date == YYYYMMDDHHMMSS_LEN:
        rtime_str = datetime.datetime.strftime(rtime, '%Y%m%d%H%M%S')
    else:
        raise RuntimeError('invalid time format')

    return rtime_str


def get_time_str_format(date, delta='0D', format=None):
    rtime = get_time(date, delta)
    try:
        rtime_str = datetime.datetime.strftime(rtime, format)
    except BaseException:
        raise RuntimeError('invalid time format')

    return rtime_str


def getIpAddr():
    myname = socket.getfqdn(socket.gethostname())
    myaddr = socket.gethostbyname(myname)
    return myaddr


def None2empty(o):
    if not o:
        return ''
    return o


if __name__ == '__main__':
    print(get_home_path("a"))
    env_dist = os.environ  # environ是在os.py中定义的一个dict environ = {}
    print(get_time_str('20210201', '1D'))
    print(getIpAddr())
    # 打印所有环境变量，遍历字典
    for key in env_dist:
        print(key + ' : ' + env_dist[key])
