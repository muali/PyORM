#!/usr/bin/env python3.4
# -*- coding: utf-8 -*-
__author__ = 'Moskvitin Maxim'

from ORM.MySQL.mysqlmeta import *
from ORM.utils.extendableobject import *
from ORM.exceptions import *
from contextlib import closing


class MySQLBase(metaclass=MySQLMeta):
    fields = []
    last_valid_state = {}
    is_valid = True
    commit_method = ""
    fields_to_commit = []
    has_changes = False

    def __init__(self, engine, **kwargs):
        self.engine = engine

        if len(kwargs) != len(self.fields) and len(kwargs) != len(self.primary_keys):
            raise TypeError("Expected %d or %d args, but got %d" % (len(self.primary_keys), len(self.fields), len(kwargs)))

        is_new_object = False
        if len(kwargs) == len(self.fields):
            is_new_object = True
            for key in kwargs:
                if key not in self.fields:
                    raise TypeError("Unexpected args %s" % key)

        if len(kwargs) == len(self.primary_keys):
            for key in kwargs:
                if key not in self.primary_keys:
                    raise TypeError("Unexpected args %s" % key)
            query_options = ExtendableObject()
            query_options.entity = type(self).__name__
            query_options.condition = kwargs
            data = engine.get_object(query_options)
            if data is None and not is_new_object:
                raise DatabaseException("Object didn't exists")
            if data is not None:
                is_new_object = False
                self.commit_method = "Update"
                for key, value in data.items():
                    super().__setattr__(key, value)

        if is_new_object:
            query_options = ExtendableObject()
            query_options.entity = type(self).__name__
            query_options.condition = {}
            for key in kwargs:
                if key in self.primary_keys:
                    query_options.condition[key] = kwargs[key]
            data = engine.get_object(query_options)
            if data is not None:
                raise DatabaseException("Object exists")
            self.commit_method = "Insert"
            for key, value in kwargs.items():
                super().__setattr__(key, value)
            engine.altered_objects.append(self)
            self.last_valid_state = None
            self.has_changes = True

        self.keep_state_as_valid()

    def __setattr__(self, key, value):
        if key in self.primary_keys:
            raise DatabaseException("Altering %s, which is id of object of %s class" % (key, self.__name__))
        if key in self.fields:
            if key not in self.fields_to_commit:
                self.fields_to_commit.append(key)
            if self not in self.engine.altered_objects:
                self.engine.altered_objects.append(self)
            self.has_changes = True
        super().__setattr__(key, value)

    def keep_state_as_valid(self):
        self.last_valid_state = {}
        for field in self.fields:
            self.last_valid_state[field] = getattr(self, field)

    def get_condition(self):
        condition = {}
        for key in self.primary_keys:
            condition[key] = getattr(self, key)
        return condition

    def commit_with_rollback(self, cursor):
        query_options = ExtendableObject()
        query_options.method = self.commit_method
        query_options.entity = type(self).__name__
        query_options.data = {}
        if self.commit_method == "Insert":
            for field in self.fields:
                query_options.data[field] = getattr(self, field)
        else:
            for field in self.fields_to_commit:
                query_options.data[field] = getattr(self, field)
            query_options.condition = self.get_condition()
        self.engine.do_query(query_options, cursor)

    def commit(self):
        with closing(self.engine.get_connection()) as connection, closing(connection.cursor()) as cursor:
            connection.start_transaction()
            try:
                self.commit_with_rollback(cursor)
                connection.commit()
                self.keep_state_as_valid()
                self.has_changes = False
            except:
                connection.rollback()
                raise

    def rollback(self):
        for key, value in self.last_valid_state.items:
            super().__setattr__(key, value)