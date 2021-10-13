# -*- coding: utf-8 -*-
'''
Created on 2014-2-10

'''
from __future__ import absolute_import, print_function, unicode_literals
from flask import Flask, g, render_template, send_from_directory, make_response, jsonify
import os.path
from cores.exception import InvalidUsage
from cores import database_meta
from web.app.api import mod as checksModule

_basedir = os.path.abspath(os.path.dirname(__file__))
configPy = os.path.join(os.path.join(_basedir, os.path.pardir), 'conf.py')

app = Flask(__name__)  # create our application object
app.config.from_pyfile(configPy)
app.config['JSON_SORT_KEYS'] = False
flask_sqlalchemy_used = True  # when Flask-SQLAlchemy used

# if app.debug:
#     from flask_debugtoolbar import DebugToolbarExtension
#     toolbar = DebugToolbarExtension(app)

app.register_blueprint(checksModule)


def connect_db():  # when Flask-SQLAlchemy not used
    return database_meta.openConnection()


@app.before_request
def before_request():
    app.logger.info("before_request() called.")

    """Make sure we are connected to the database each request."""
    if not flask_sqlalchemy_used:
        connect_db()


@app.teardown_request
def teardown_request(response):
    app.logger.info("after_request() called.")

    return response


# ----------------------------------------
# controllers
# ----------------------------------------

@app.errorhandler(InvalidUsage)
def handle_invalid_usage(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response
