#!/usr/bin/env python3.4
# -*- coding: utf-8 -*-
__author__ = 'Moskvitin Maxim'

from pymongo import MongoClient
from ORM.MongoDB.mongobase import MongoBase
import ORM


class MongoDBEngine(object):
    altered_objects = []

    def __init__(self, database, name=None, password=None, **connection_options):
        self.client = MongoClient(**connection_options)
        self.db = self.client[database]
        if name and password:
            self.db.authenticate(name, password)
        self.load_classes()

    def load_classes(self):
        def init_decorator(init):
            def decorated(obj, **kwargs):
                init(obj, self, **kwargs)
            return decorated
        collections = self.db.collection_names()
        for collection in collections:
            new_class = type(collection, (MongoBase, ), {})
            new_class.__init__ = init_decorator(new_class.__init__)
            setattr(ORM, new_class.__name__, new_class)

    def get_object(self, entity, _id):
        collection = self.db[entity]
        return collection.find_one({"_id": _id})

    def commit(self):
        for obj in self.altered_objects:
            obj.commit()

    def save(self, collection_name, obj_dict):
        collection = self.db[collection_name]
        collection.save(obj_dict)