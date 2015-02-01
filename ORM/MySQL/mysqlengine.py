#!/usr/bin/env python3.4
# -*- coding: utf-8 -*-
__author__ = 'Moskvitin Maxim'

import mysql.connector
import ORM
from ORM.MySQL.mysqlbase import *
from contextlib import closing

def build_condition(condition_options):
    return "where " + " and ".join(["%s = %%(%s)s" % (key, key) for key in condition_options])


class Table:
    def __init__(self, name):
        self.name = name
        self.columns = []

    def add_column(self, column):
        self.columns.append(column)


class MySQLEngine(object):
    altered_objects = []

    def __init__(self, **connection_options):
        self.connection_pool = mysql.connector.pooling.MySQLConnectionPool(**connection_options)
        self.load_classes()

    def get_connection(self):
        return self.connection_pool.get_connection()

    def load_classes(self):
        def init_decorator(init):
            def decorated(obj, **kwargs):
                init(obj, self, **kwargs)
            return decorated
        tables = self.get_db_schema()
        for table in tables:
            new_class = self.create_class(table)
            new_class.__init__ = init_decorator(new_class.__init__)
            setattr(ORM, new_class.__name__, new_class)

    def get_db_schema(self):
        tables = []
        with closing(self.get_connection()) as connection:
            with closing(connection.cursor()) as cursor:
                cursor.execute("show tables")
                for (name,) in cursor:
                    tables.append(Table(name))
            with closing(connection.cursor(dictionary=True)) as cursor:
                for table in tables:
                    cursor.execute("desc %s" % table.name)
                    for column in cursor:
                        table.add_column(column)
        return tables

    @staticmethod
    def create_class(table):
        return type(table.name, (MySQLBase, ), {
            "primary_keys": [column["Field"] for column in table.columns if column["Key"] == "PRI"],
            "fields": [column["Field"] for column in table.columns]
        })

    def get_object(self, query_options):
        with closing(self.get_connection()) as connection, closing(connection.cursor(dictionary=True)) as cursor:
            query = "select * from %s\n" % query_options.entity
            query += build_condition(query_options.condition)
            cursor.execute(query, query_options.condition)
            result = cursor.fetchone()
            return result

    def commit(self):
        connection = self.get_connection()
        cursor = connection.cursor()
        try:
            connection.start_transaction()
            for obj in self.altered_objects:
                if obj.has_changes:
                    obj.commit_with_rollback(cursor)
            connection.commit()
        except:
            connection.rollback()
            for obj in self.altered_objects:
                if obj.has_changes:
                    obj.is_valid = False
            raise
        finally:
            cursor.close()
            connection.close()

        for obj in self.altered_objects:
            obj.keep_state_as_valid()
            obj.has_changes = False
        self.altered_objects = []

    def rollback(self):
        for obj in self.altered_objects:
            if obj.has_changes:
                obj.rollback()

    @staticmethod
    def do_update(query_options, cursor):
        query = "update %s\n" % query_options.entity
        query += "set "
        query += ",".join(["%s = %%(%s)s" % (key, key) for key in query_options.data.keys()])
        query += '\n' + build_condition(query_options.condition)
        data = query_options.data
        data.update(query_options.condition)
        cursor.execute(query, data)

    @staticmethod
    def do_insert(query_options, cursor):
        query = "insert into %s\n" % query_options.entity
        query += "(" + ",".join([key for key in query_options.data.keys()]) + ")\n"
        query += "values (" + ",".join(["%%(%s)s" % key for key in query_options.data.keys()]) + ")"
        cursor.execute(query, query_options.data)

    @staticmethod
    def do_query(query_options, cursor):
        query_to_method = {
            "Update": MySQLEngine.do_update,
            "Insert": MySQLEngine.do_insert
        }
        query_to_method[query_options.method](query_options, cursor)