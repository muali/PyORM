#!/usr/bin/env python3.4
# -*- coding: utf-8 -*-
__author__ = 'Moskvitin Maxim'

import mysql.connector


def build_condition(condition_options):
    condition = "where "
    is_first = True
    for key in condition_options:
        if is_first:
            is_first = False
        else:
            condition += " and "
        condition += ("%s == %s", key, str(condition_options[key]))
    return condition


class MySQLEngine(object):
    def __init__(self, connection_options):
        self.connection_pool = mysql.connector.pooling.MySQLConnectionPool(**connection_options)

    def load_classes(self):
        pass

    def do_select(self, query_options):
        connection = self.connection_pool.get_connection()
        query = ("select * from %s\n", query_options.entity)
        query += build_condition(query_options.condition)
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query)
        result = None
        for row in cursor:
            result = row
            break
        cursor.close()
        connection.close()
        return result

    def do_queries(self, queries_options):
        connection = self.connection_pool.get_connection()
        for query_options in queries_options:
            self.do_query(query_options, connection)
        connection.commit()
        connection.close()

    @staticmethod
    def do_query(query_options, connection):
        if query_options.type == "insert":
            query = ("insert into %s\n", query_options.entity)
            fields = "("
            values = "values ("
            is_first = True
            for key in query_options.data:
                if is_first:
                    is_first = False
                else:
                    fields += ','
                    values += ','
                fields += key
                values += str(query_options.data[key])
            query += fields + ")\n" + values + ')'
            cursor = connection.cursor()
            cursor.execute(query)

        if query_options.type == "update":
            query = ("update %s\n", query_options.entity)
            query += "set "
            is_first = True
            for key in query_options.data:
                if is_first:
                    is_first = False
                else:
                    query += ','
                query += ("%s = %s", key, str(query_options.data))
            query += '\n' + build_condition(query_options.condition)
            cursor = connection.cursor()
            cursor.execute(query)



