#!/usr/bin/env python3.4
# -*- coding: utf-8 -*-
__author__ = 'Moskvitin Maxim'

import mysql.connector
from ORM.utils.singleton import ExtendedSingleton


def build_condition(condition_options):
    condition = "where "
    is_first = True
    for key in condition_options:
        if is_first:
            is_first = False
        else:
            condition += " and "
        condition += "%s == %%(%s)s" % (key, key)
    return condition


class Table:
    def __init__(self, name):
        self.name = name
        self.columns = []

    def add_column(self, column):
        self.columns += column


class MySQLBase(object, metaclass=ExtendedSingleton):
    _primary_keys = []
    _fields = []
    last_valid_state = {}

    def __init__(self, engine, **kwargs):
        self.engine = engine

        if len(kwargs) != len(self._primary_keys):
            raise TypeError("Expected: %d arguments, but recieved %d"
                  % (len(self._primary_keys), len(kwargs)))

        query_options = object()
        query_options.entity = self.__name__
        query_options.condition = kwargs

        for key, value in kwargs:
            if key not in self._primary_keys:
                raise TypeError("Unexpected arg: %s" % key)

        data = engine.get_object(query_options)
        if data is None:
            for field in self._fields:
                object.__setattr__(self, field)
            for key, value in kwargs:
                object.__setattr__(self, key, value)
            query_options.data = kwargs
            query_options.type = "insert"
            delattr(query_options, "condition")
            engine.queries_options += query_options
        else:
            for key, value in data:
                object.__setattr__(self, key, value)

        for field in self._fields:
            self.last_valid_state[field] = getattr(self, field)

    def get_condition(self):
        condition = {}
        for key in self._primary_keys:
            condition[key] = getattr(self, key)
        return condition

    def __setattr__(self, key, value):
        if key in self._primary_keys:
            raise Exception()
            #TODO
        if key in self._fields:
            query_options = object()
            query_options.type = "update"
            query_options.entity = self.__name__
            query_options.condition = self.get_condition()
            self.engine.queries_options += query_options
        object.__setattr__(self, key, value)

    def __getattr__(self, item):
        #TODO validate
        object.__getattr__(self, item)


class MySQLEngine(object):
    queries_options = []

    def __init__(self, connection_options):
        self.connection_pool = mysql.connector.pooling.MySQLConnectionPool(**connection_options)

    def load_classes(self):
        def init_decorator(init):
            def decorated(obj, **kwargs):
                init(obj, self, **kwargs)
            return decorated
        tables = self.get_db_schema()
        for table in tables:
            newclass = self.create_class(table)
            newclass.__init__ = init_decorator(newclass.__init__)
            globals()[newclass.__name__] = newclass

    def get_db_schema(self):
        connection = self.connection_pool.get_connection()
        cursor = connection.cursor()
        cursor.execute("show tables")
        tables = []
        for (name,) in cursor:
            tables += Table(name)
        cursor.close()
        cursor = connection.cursor(dictionary=True)
        for table in tables:
            cursor.execute("desc %s" % table)
            for column in cursor:
                table.add_column(column)
        return tables

    @staticmethod
    def create_class(table):
        return type(table.name, MySQLBase, {
            "_primary_keys": [column.Field for column in table.columns if column.Key == "PRI"],
            "_fields": [column.Field for column in table.columns]
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

    def do_queries(self):
        try:
            connection = self.connection_pool.get_connection()
            try:
                for query_options in self.queries_options:
                    self.do_query(query_options, connection)
                connection.commit()
            except:
                connection.rollback()
                raise
            connection.close()
        except:
            self.queries_options = []
            raise

    @staticmethod
    def do_query(query_options, connection):
        query = ""
        cursor = connection.cursor()
        if query_options.type == "insert":
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

        if query_options.type == "update":
            query += ("update %s\n", query_options.entity)
            query += "set "
            is_first = True
            data = {}
            for key in query_options.data:
                if is_first:
                    is_first = False
                else:
                    query += ','
                query += "%s = %%(%s)s" % (key, "__" + key)
                data["__" + key] = query_options.data[key]
            query += '\n' + build_condition(query_options.condition)
            cursor.execute(query, data, query_options.condition)

        cursor.close()