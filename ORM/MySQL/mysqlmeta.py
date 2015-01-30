#!/usr/bin/env python3.4
# -*- coding: utf-8 -*-
__author__ = 'Moskvitin Maxim'

import inspect

"""
    Metaclass for MySQL entity class
    Behavior like a singleton if primary key is specified
"""


class MySQLMeta(type):
    instance_map = {}
    primary_keys = []

    def __call__(cls, *args, **kwargs):
        pk_tuple = cls.build_pk_tuple(**kwargs)
        if pk_tuple not in cls.instance_map or is_new_instance:
            cls.instance_map[pk_tuple] = super(MySQLMeta, cls).__call__(*args, **kwargs)
        return cls.instance_map[pk_tuple]

    def build_pk_tuple(cls, **kwargs):
        pk_dict = {}
        is_new_instance = False
        for key, value in kwargs.items():
            if key in cls.primary_keys:
                pk_dict[key] = value
            else:
                is_new_instance = True
        pk_tuple = tuple(sorted(pk_dict.items()))
        return pk_tuple