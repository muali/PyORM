#!/usr/bin/env python3.4
# -*- coding: utf-8 -*-
__author__ = 'Moskvitin Maxim'

import mysql.connector
from ORM.utils.singleton import ExtendedSingleton
from ORM.utils.extendableobject import ExtendableObject
import ORM


class InvalidModificationException(Exception):
    pass


def build_condition(condition_options):
    condition = "where "
    is_first = True
    for key in condition_options:
        if is_first:
            is_first = False
        else:
            condition += " and "
        condition += "%s = %%(%s)s" % (key, key)
    return condition


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

    def load_classes(self):
        def init_decorator(init):
            def decorated(obj, **kwargs):
                init(obj, self, **kwargs)
            return decorated
        tables = self.get_db_schema()
        for table in tables:
            new_class = self.create_class(table)
            new_class.__init__ = init_decorator(newclass.__init__)
            setattr(ORM, new_class.__name__, newclass)

    def get_db_schema(self):
        connection = self.connection_pool.get_connection()
        cursor = connection.cursor()
        cursor.execute("show tables")
        tables = []
        for (name,) in cursor:
            tables.append(Table(name))
        cursor.close()
        cursor = connection.cursor(dictionary=True)
        for table in tables:
            cursor.execute("desc %s" % table.name)
            for column in cursor:
                table.add_column(column)
        return tables

    @staticmethod
    def create_class(table):
        return type(table.name, (MySQLBase, ), {
            "_primary_keys": [column["Field"] for column in table.columns if column["Key"] == "PRI"],
            "_fields": [column["Field"] for column in table.columns]
        })

    def get_object(self, query_options):
        connection = self.connection_pool.get_connection()
        query = "select * from %s\n" % query_options.entity
        query += build_condition(query_options.condition)
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query, query_options.condition)
        result = cursor.fetchone()
        cursor.close()
        connection.close()
        return result

    def commit(self):
        connection = self.connection_pool.get_connection()
        try:
            connection.start_transaction()
            for obj in self.altered_objects:
                if obj.has_changes:
                    obj.commit(connection.cursor())
            connection.commit()
        except:
            connection.rollback()
            for obj in self.altered_objects:
                if obj.has_changes:
                    obj.is_valid = False
            raise
        finally:
            connection.close()
        for obj in self.altered_objects:
            obj.keep_state_as_valid()
            obj.has_changes = False
        self.altered_objects = []

    @staticmethod
    def do_query(query_options, cursor):
        query = ""
        if query_options.type == "Insert":
            query += "insert into %s\n" % query_options.entity
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
                values += "%%(%s)s" % key
            query += fields + ")\n" + values + ')'
            cursor.execute(query, query_options.data)
        else:
            query += "update %s\n" % query_options.entity
            query += "set "
            is_first = True
            data = {}
            for key in query_options.data:
                if is_first:
                    is_first = False
                else:
                    query += ','
                query += "%s = %%(%s)s" % (key, key)
            query += '\n' + build_condition(query_options.condition)
            cursor.execute(query, data, query_options.condition)