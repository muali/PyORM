#!/usr/bin/env python3.4
# -*- coding: utf-8 -*-
__author__ = 'Moskvitin Maxim'

from ORM.MySQL.mysqlmeta import *
from ORM.utils.extendableobject import *


class MySQLBase(ExtendableObject, metaclass=MySQLMeta):
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
                    ExtendableObject.__setattr__(self, key, value)
                self.keep_state_as_valid()

        if is_new_object:
            object_exists = False
            pk_tuple = self.build_pk_tuple(**kwargs)
            if pk_tuple in self.instance_map:
                object_exists = True
            else:
                query_options = ExtendableObject()
                query_options.entity = type(self).__name__
                query_options.condition = {}
                for key in kwargs:
                    if key in self.primary_keys:
                        query_options.condition[key] = kwargs[key]
                data = engine.get_object(query_options)
                object_exists = data is not None
            if object_exists:
                raise DatabaseException("Object exists")
            self.commit_method = "Insert"
            for key, value in kwargs.items():
                ExtendableObject.__setattr__(self, key, value)
            engine.altered_objects.append(self)
            self.last_valid_state = None
            self.has_changes = True

        self.is_valid = True

    def __setattr__(self, key, value):
        if key in self._primary_keys:
            raise DatabaseException("Altering %s, which is id of object of %s class" % (key, self.__name__))
        if key in self.fields:
            if key not in self.fields_to_commit:
                self.fields_to_commit.append(key)
            if self not in self.engine.altered_objects:
                self.engine.altered_objects.append(self)
            self.has_changes = True
        ExtendableObject.__setattr__(self, key, value)

    def __getattr__(self, item):
        self.validate()
        ExtendableObject.__getattr__(self, item)

    def __delattr__(self, item):
        if item in self.fields:
            raise DatabaseException("It's impossible to remove field from MySQL object")
        ExtendableObject.__delattr__(self, item)

    def validate(self):
        if self.is_valid:
            return
        if self.last_valid_state is None:
            raise DatabaseException("Object is in invalid state and it's impossible to validate it automatically")
        for key, value in self.last_valid_state.items:
            ExtendableObject.__setattr__(self, key, value)
        self.is_valid = True

    def keep_state_as_valid(self):
        self.is_valid = True
        for field in self.fields:
            self.last_valid_state[field] = getattr(self, field)

    def get_condition(self):
        condition = {}
        for key in self._primary_keys:
            condition[key] = getattr(self, key)
        return condition

    def commit(self, cursor=None):
        connection = None
        if cursor is None:
            connection = self.engine.get_connection()
            connection.start_transaction()
            cursor = connection.cursor()
        query_options = ExtendableObject()
        query_options.method = self.commit_method
        query_options.data = {}
        if self.commit_method == "Insert":
            for field in self.fields:
                query_options.data[field] = ExtendableObject.__getattr__(self, field)
        else:
            for field in self.fields_to_commit:
                query_options.data[field] = ExtendableObject.__getattr__(self, field)
        try:
            self.engine.do_query(query_options, cursor)
            if connection is not None:
                connection.commit()
                self.keep_state_as_valid()
                self.has_changes = False
        except:
            if connection is not None:
                connection.rollback()
            self.is_valid = False
            raise
        finally:
            cursor.close()
            connection.close()

    def rollback(self):
        self.validate()