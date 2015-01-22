#!/usr/bin/env python3.4
# -*- coding: utf-8 -*-
__author__ = 'Moskvitin Maxim'

import inspect


class Singleton(type):
    instance = None

    def __call__(cls, *args, **kwargs):
        if cls.instance is None:
            cls.instance = super(Singleton, cls).__call__(*args, **kwargs)
        return cls.instance


class ExtendedSingleton(type):
    instance_map = {}

    def __call__(cls, *args, **kwargs):
        spec = inspect.getargspec(cls.__init__)
        arg_map = {}
        if spec.defaults is not None:
            for i in range(0, len(spec.defaults)):
                arg_map[spec.args[len(spec.args) - len(spec.defaults) + i]] = spec.defaults[i]

        for i in range(0, min(len(spec.args) - 1, len(args))):
            arg_map[spec.args[i + 1]] = args[i]

        vararg = None
        if len(spec.args) < len(args):
            vararg = args[len(spec.args):]

        keywords = {}
        for key in kwargs:
            if key in spec.args:
                arg_map[key] = kwargs[key]
            else:
                keywords[key] = kwargs[key]

        if vararg is not None:
            arg_map[spec.varargs] = vararg

        if len(keywords) > 0:
            arg_map[spec.keywords] = tuple(sorted(keywords.items()))

        arg_tuple = tuple(sorted(arg_map.items()))
        if arg_tuple not in cls.instance_map:
            cls.instance_map[arg_tuple] = super(ExtendedSingleton, cls).__call__(*args, **kwargs)

        return cls.instance_map[arg_tuple]
