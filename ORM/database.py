#!/usr/bin/env python3.4
# -*- coding: utf-8 -*-
__author__ = 'Moskvitin Maxim'

from ORM.utils.singleton import Singleton
from ORM.MySQL.mysqlengine import MySQLEngine


# return True and update first query if merge is possible
def merge_query_options(first, second):
    valid_to_merge = True
    if first.entity == second.entity:
        for key in first.condition:
            if first.condition[key] != second.condition[key]:
                valid_to_merge = False
    if valid_to_merge:
        for key in second.data:
            first.data[key] = second.data[key]
        return True
    return False


class Database(metaclass=Singleton):
    engine = None

    def __init__(self, engine_type, **connection_options):
        if engine_type == "MySQL":
            self.engine = MySQLEngine(connection_options)
        self.engine.load_classes()

    def commit(self):
        self.merge_query()
        self.engine.do_queries()

    def merge_query(self):
        new_queries_options = []
        i = 0
        while True:
            for j in range(i + 1, len(self.engine.queries_options)):
                if not merge_query_options(self.engine.queries_options[i], self.engine.queries_options[j]):
                    break
            new_queries_options.append(self.engine.queries_options[i])
            if i == len(self.engine.queries_options) - 1:
                break
            i = j
        self.engine.queries_options = new_queries_options