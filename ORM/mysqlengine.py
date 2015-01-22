#!/usr/bin/env python3.4
# -*- coding: utf-8 -*-
__author__ = 'Moskvitin Maxim'

import mysql.connector


def build_condition(condition_options):
    condition = "where "
    is_first  = True
    for key in condition_options:
        if is_first:
            is_first = False
        else:
            condition += " and "
        condition += ("%s == %s", key, str(condition_options[key]))
    return condition


class MySQLEngine(object):
    def __init__(self, connection_options):
        self.connection = mysql.connector.connect(**connection_options)

    def load_classes(self):
        pass

    def do_query(self, query_options):
        if query_options.type == "select":
            query = ("Select * from %s\n", query_options.entity)
            query += build_condition(query_options.condition)
            cursor = self.connection.cursor(dictionary=True)
            cursor.execute(query)
            for row in cursor:
                return row
            return None

