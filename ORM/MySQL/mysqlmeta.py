#!/usr/bin/env python3.4
# -*- coding: utf-8 -*-
__author__ = 'Moskvitin Maxim'

from ORM.exceptions import *

"""
    Metaclass for MySQL entity class
    Behavior like a singleton if primary key is specified
"""


class MySQLMeta(type):
    instance_map = {}
    primary_keys = []

    def __call__(cls, *args, **kwargs):
        is_new_instance = False
        pk_dict = {}
        for key, value in kwargs.items():
            if key in cls.primary_keys:
                pk_dict[key] = value
            else:
                is_new_instance = True
        pk_tuple = (cls.__name__, ) + tuple(sorted(pk_dict.items()))
        if pk_tuple in cls.instance_map and is_new_instance:
            raise DatabaseException("Object exists")
        if pk_tuple not in cls.instance_map:
            cls.instance_map[pk_tuple] = super(MySQLMeta, cls).__call__(*args, **kwargs)
        return cls.instance_map[pk_tuple]
