# -*- coding: utf-8 -*-
'''
Created on 2014-2-10

'''
from __future__ import absolute_import
from flask import Blueprint, abort, request, flash, jsonify
from cores.model_meta import dashboard_rule_log
import json

mod = Blueprint('checks', __name__)  # register the users blueprint module


@mod.route("/api/v1.0/details/<rule_name>", methods=('GET', 'POST'))
def getCheckItemCfg_view(rule_name):
    dicts = request.args.to_dict()
    cfg = dashboard_rule_log.getruleresults(**dicts)
    details = []
    for row in cfg:
        dic = {
            'db_type': row.db_type,
            'db_name': row.db_name,
            'table_name': row.table_name,
            'col_name': row.col_name,
            'rule_name': row.rule_name,
            'task_time': row.task_time,
            'value': str(row.value),
            'details': json.loads(row.details)
        }
        details.append(dic)

    return jsonify({'results': details}), 201


# -----------------------------------------
# test api below
# -----------------------------------------


@mod.route("/api/v1.0/test/simple", methods=('GET', 'POST'))
def test_simple_view():
    return jsonify({'request.method': request.method, 'save_status': 'successful'}), 201


@mod.route("/api/v1.0/test/str_arg/<check_itm_code>", methods=('POST', 'GET'))
def test_str_argument_view(check_itm_code):
    abort(400)
    return jsonify({'request.method': request.method, 'item': check_itm_code, 'save_status': 'successful'}), 201


@mod.route("/api/v1.0/test/int_arg/<int:seq_no>", methods=('POST', 'GET'))
def test_int_argument_view(seq_no):
    return jsonify({'request.method': request.method, 'seq_no': seq_no, 'save_status': 'successful'}), 201


@mod.route("/api/v1.0/test/json_post/<check_itm_code>", methods=('POST', 'GET'))
def test_json_post_view(check_itm_code):
    if not request.json:
        abort(400)  # bad request

    if 'value' not in request.json:
        abort(400)  # bad request

    value = request.json['value']
    detail_msg = request.json.get('detail_msg', "")  # if detail_msg is not set, use empty
    return jsonify(
        {'request.method': request.method, 'value': value, 'detail_msg': detail_msg, 'save_status': 'successful'}), 201
