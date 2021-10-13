# -*- coding: utf-8 -*-
'''
Created on 2014-2-10

'''
from __future__ import absolute_import
import os
import logging

_basedir = os.path.abspath(os.path.dirname(__file__))

PORT = 5000
DEBUG = False  # enable reload

REALM_NAME = 'http://127.0.0.1:5000'

# mysql
host = '127.0.0.1'
username = 'root'
password = '123456'
database = 'qualitis'
port = 3307
charset = 'utf8'

# logging
log_level = logging.INFO
