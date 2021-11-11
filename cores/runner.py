# encoding=utf-8
import os
import imp
import time
import pprint
import logging.config
import getopt
from datetime import datetime
import traceback
import importlib
import sys
import utils
import schema
from core import *
from exception import ValidateError, PluginNotFoundError
import meta


importlib.reload(sys)
FILE_LOGGING_CONF = "conf" + os.sep + "logging.conf"
# 配置环境
os.environ['DATAQA_HOME'] = "/Users/hushuai/PycharmProjects/git/data-quality"
os.environ['TASK_HOME'] = "/Users/hushuai/PycharmProjects/git/data-quality/tasks"
logging.config.fileConfig(utils.get_home_path(FILE_LOGGING_CONF))
__metaclass__ = type
DATAQA_HOME = os.environ['DATAQA_HOME']
sys.path.append(DATAQA_HOME + os.sep + "plugins")
sys.path.append(DATAQA_HOME + os.sep + "cores")
sys.path.append(DATAQA_HOME)
FILE_CORE_CONF = "conf" + os.sep + "core.yaml"
FILE_TASK_HOME = "tasks" + os.sep + "%s.yaml"
FILE_TASK = "task.yaml"
DIR_PLUGIN = "plugins"
DIR_RULE = "rules"
PHASE_EXEC = "execute"


class CoreFactory(object):
    log = logging.getLogger('CoreFactory')

    def __init__(self):
        self.load_core()

    def load_core(self):
        conf = utils.load_yaml(FILE_CORE_CONF, home=True)
        schema.check_schema(schema.CORE_SCHEMA, conf, FILE_CORE_CONF)
        self.core_conf = conf
        self.log.debug(pprint.pformat(self.core_conf))
        self.phase_list = self.core_conf.get('phase_list')

    def load_klass(self, class_name):
        lc = class_name.split('.')
        if len(lc) == 3:
            package, module, klass = lc
            self.log.info("load interceptor: %s.%s.%s" % (package, module, klass))
            p = __import__("%s.%s" % (package, module), globals(), locals(), [], -1)
            m = getattr(p, module)
            k = getattr(m, klass)
            return k
        elif len(lc) == 2:
            module, klass = lc
            m = __import__(module)
            k = getattr(m, klass)
            return k
        raise ValidateError('class name invalid: %s' % class_name)

    def __str__(self):
        return str(self.core_conf)


class RuntimeContext(object):
    """
    包含任务执行时的相关信息
    """

    def __init__(self):
        """
        """
        pass

    def _set_core_conf(self, core_conf):
        self._core_conf = core_conf

    def get_core_conf(self):
        return self._core_conf

    def _set_plugin_manager(self, mgr):
        self._plugin_manager = mgr

    def get_plugin_manager(self):
        return self._plugin_manager

    def _set_rule_manager(self, rur):
        self._rule_manager = rur

    def get_rule_manager(self):
        return self._rule_manager

    def _set_meta_manager(self, mgr):
        self._meta_manager = mgr

    def get_meta_manager(self):
        return self._meta_manager

    def _set_phase_list(self, phase_list):
        self._phase_list = phase_list

    def get_phase_list(self):
        return self._phase_list

    core_conf = property(get_core_conf, doc="core conf")
    plugin_manager = property(get_plugin_manager, doc="plugin conf")
    rule_manager = property(get_rule_manager, doc="rule conf")
    meta_manager = property(get_meta_manager, doc="meta manager")
    phase_list = property(get_phase_list, doc="phase list")


class PluginManager:
    """
        plugin文件的名字出去扩展名为plugin的唯一id
    """

    def __init__(self):
        self.plugins = dict()
        plugin_dir = utils.get_home_path(DIR_PLUGIN)
        for f in os.listdir(plugin_dir):
            if f.endswith("py") and f != '__init__.py':
                file_path = os.path.join(plugin_dir, f)
                plugin_id = f[0:-3]
                p = imp.load_source(plugin_id, file_path)
                self.plugins[plugin_id] = p

    def get_plugin(self, plugin_id):
        return self.plugins[plugin_id]


class RuleManager:
    """
        plugin文件的名字出去扩展名为plugin的唯一id
    """

    def __init__(self):
        self.rules = dict()
        rule_dir = utils.get_home_path(DIR_RULE)
        for f in os.listdir(rule_dir):
            if f.endswith("py") and f != '__init__.py':
                file_path = os.path.join(rule_dir, f)
                rule_id = f[0:-3]
                p = imp.load_source(rule_id, file_path)
                self.rules[rule_id] = p

    def get_rule(self, rule_id):
        return self.rules[rule_id]


class TaskWrapper(object):
    log = logging.getLogger('TaskWrapper')

    def __init__(self, conf, context, date):
        self.task_conf = conf
        self.context = context
        self.date = date

    def exec_phase(self, phase):
        self.log.debug("try: " + phase)
        step_list = self.task_conf.get(phase)
        if not step_list:
            return
        self.log.info("exec: " + phase)
        for step in step_list:
            plugin_id, attr_id = step['step'].split(".")
            self.log.info("plugin: " + plugin_id)
            plugin = self.context.plugin_manager.get_plugin(plugin_id)
            step_ops = step.copy()
            step_ops.pop('step')
            if hasattr(plugin, attr_id):
                a = getattr(plugin, attr_id)
                p = a(self.task_conf, self.context, self.date)
                try:
                    p.execute(**step_ops)
                except BaseException:
                    self.log.error("exec_phase error, params is : " + pprint.pformat(step_ops))
                    raise
            else:
                raise PluginNotFoundError('plugin %s.%s not found' % (plugin_id, attr_id))


class TaskRunner:
    log = logging.getLogger('TaskRunner')

    def __init__(self):
        pass

    def init_context(self):
        self.log.info("begin init_context")
        self.factory = CoreFactory()
        self.context = RuntimeContext()
        self.context._set_plugin_manager(PluginManager())
        self.context._set_rule_manager(RuleManager())
        self.context._set_phase_list(self.factory.phase_list)
        self.errorMsg = None

    def init_task_conf(self, path):
        """
        初始化任务相关配置信息
        :param path:
        :return:
        """
        self.log.info("begin init_task_conf")
        self.task_conf = utils.load_yaml(path)
        self.context.task_name = self.task_conf.get("task_name")
        meta_manager = meta.MetaManager(self.factory.core_conf)
        self.context._set_meta_manager(meta_manager)

    def run(self, date, path=FILE_TASK, force=False, vars=None):
        self.log.info("begin run task")
        self.init_context()
        self.init_task_conf(path)
        self.context.meta_manager.update_var_param(vars)
        phase_list = self.context.phase_list
        self.log.info("phase_list is %s" % phase_list)
        success = True
        # run and retry
        retry_limit = self.context.meta_manager.get_global_int('global.retry.times', 3)
        for i in range(0, retry_limit):
            try:
                self.run_inner(date, phase_list, force)
                success = True
            except BaseException:
                self.errorMsg = traceback.format_exc()
                self.log.error(self.errorMsg)
                left_time = retry_limit - i - 1
                self.log.error('job %s failed, %d retry left' % (self.context.task_name, left_time))
                if left_time > 0:
                    time.sleep(self.context.meta_manager.get_global_int('global.retry.margin', 30))
                success = False
            if success:
                break

        if not success:
            self.log.error('job %s failed, exit 1' % self.context.task_name)
        return success

    def getError(self):
        return self.errorMsg

    def run_inner(self, date, phase_list, force):
        tw = TaskWrapper(self.task_conf, self.context, date)
        for phase in phase_list:
            try:
                tw.exec_phase(phase)
            except Exception:
                raise


def usage():
    print('usage runtask -d date [-f file] [-F] [--var]')


def main():
    try:
        opts, args = getopt.getopt(
            sys.argv[1:],
            'd:f:F', ['date=', 'file=', 'force', 'var='])
        print(opts, args)

        date = datetime.strftime(datetime.now(), '%Y%m%d')
        file = FILE_TASK
        force = False
        vars = {}
        for o, v in opts:
            if o in ('-d', '--date'):
                date = v
            if o in ('-f', '--file'):
                file = v
            if o in '--var':
                var_arr = v.split(",")
                for var_item in var_arr:
                    key, val = var_item.split(":")
                    vars[key] = val
        t = TaskRunner()
        success = t.run(date=date, path=file, force=force, vars=vars)
        sys.exit(0 if success else 1)
    except BaseException:
        raise
        usage()


if __name__ == "__main__":
    # 设置环境变量
    # os.environ['DATAQA_HOME'] = "/Users/hushuai/PycharmProjects/git/data-quality"
    # os.environ['TASK_HOME'] = "/Users/hushuai/PycharmProjects/git/data-quality/tasks"
    main()
