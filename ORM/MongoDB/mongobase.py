#!/usr/bin/env python3.4
# -*- coding: utf-8 -*-
__author__ = 'Moskvitin Maxim'

from ORM.MongoDB.mongometa import MongoDBMeta
from ORM.exceptions import DatabaseException


class MongoBase(object, metaclass=MongoDBMeta):
    _mongo_dict = {}

    def __init__(self, engine, **kwargs):
        super().__setattr__("engine", engine)
        if "_id" not in kwargs:
            raise DatabaseException("_id field not specified")
        data = engine.get_object(type(self).__name__, kwargs["_id"])
        if data is not None and len(kwargs) > 1:
            raise DatabaseException("Object exists")
        if data is None:
            super().__setattr__("_mongo_dict", kwargs)
            engine.altered_objects.append(self)
        else:
            super().__setattr__("_mongo_dict", data)

    def __getattr__(self, item):
        if item in self._mongo_dict:
            return self._mongo_dict[item]
        else:
            return None

    def __setattr__(self, key, value):
        if self not in self.engine.altered_objects:
            self.engine.altered_objects.append(self)
        self._mongo_dict[key] = value

    def __delattr__(self, item):
        if item in self._mongo_dict:
            if self not in self.engine.altered_objects:
                self.engine.altered_objects.append(self)
            del self._mongo_dict[item]
        else:
            super().__delattr__(item)

    def commit(self):
        self.engine.save(type(self).__name__, self._mongo_dict)
