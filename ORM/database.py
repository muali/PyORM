#!/usr/bin/env python3.4
# -*- coding: utf-8 -*-
__author__ = 'Moskvitin Maxim'

from ORM.utils.singleton import Singleton
from ORM.mysqlengine import MySQLEngine


class Database(metaclass=Singleton):
    changes = []

    def __init__(self, dbengine):
        pass

    def commit(self):
        pass
