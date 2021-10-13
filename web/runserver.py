# -*- coding: utf-8 -*-
'''
Created on 2014-2-10

'''

from __future__ import absolute_import
import logging.config
import os
import sys
import importlib

importlib.reload(sys)
os.environ['DATAQA_HOME'] = "/Users/hushuai/PycharmProjects/git/data-quality"
os.environ['TASK_HOME'] = "/Users/hushuai/PycharmProjects/git/data-quality/tasks"
DATAQA_HOME = os.environ['DATAQA_HOME']
sys.path.append(DATAQA_HOME)
from web.app import app
from web import conf as web_conf
from web import my_logging

log_file = my_logging.getLoggingFileName(__file__)
root_logger = logging.getLogger()
my_logging.configureLogger(root_logger, log_file, web_conf.log_level)


def run_on_default_server():
    if app.debug:
        # app.run(port=PORT, debug=True)                  # disable to access on other computer
        app.run(host='127.0.0.1', port=web_conf.PORT, debug=True)  # allow to access on other computer
    else:
        app.run(host='127.0.0.1', port=web_conf.PORT)


if __name__ == "__main__":
    run_on_default_server()
