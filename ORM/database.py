#!/usr/bin/env python3.4
# -*- coding: utf-8 -*-
__author__ = 'Moskvitin Maxim'

from ORM.utils.singleton import Singleton
from ORM.mysqlengine import MySQLEngine
import ORM

class Database(metaclass=Singleton):
    engine = None

    def __init__(self, engine_type, **connection_options):
        if engine_type == "MySQL":
            self.engine = MySQLEngine(connection_options)
        self.engine.load_classes()

    def commit(self):
        self.engine.do_queries()

ORM.Database = Database