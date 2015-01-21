#!/usr/bin/env python3.4
# -*- coding: utf-8 -*-
__author__ = 'Moskvitin Maxim'

from ORM.utils.singleton import Singleton


class Database(metaclass=Singleton):
    changes = []

    def __init__(self, dbengine, connection_options):
        pass

    def commit(self):
        pass
