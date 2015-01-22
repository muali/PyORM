#!/usr/bin/env python3.4
# -*- coding: utf-8 -*-
__author__ = 'Moskvitin Maxim'


class ExtendableObject(object):
    def __getattr__(self, item):
        return self.__dict__[item]

    def __setattr__(self, key, value):
        if key not in self.__dict__:
            raise AttributeError("'%s' object has no attribute %s" % (self.__name__, key))
        self.__dict__[key] = value

    def __delattr__(self, item):
        del self.__dict__[item]