#!/usr/bin/env python3.4
# -*- coding: utf-8 -*-
__author__ = 'Moskvitin Maxim'

import unittest
from pymongo import MongoClient
from ORM.MongoDB.mongodbengine import MongoDBEngine
from ORM.exceptions import DatabaseException
import ORM


class TestMongoDBEngine(unittest.TestCase):
    db = None
    db_name = "PyORMTest"

    def setUp(self):
        client = MongoClient()
        db = client[self.db_name]
        collection = db["testData"]
        for i in range(0, 100):
            collection.save({"_id": i, "x": i})
        self.db = MongoDBEngine(self.db_name)

    def test_class_load(self):
        self.assertTrue(hasattr(ORM, 'testData'))

    def test_selection(self):
        obj1 = ORM.testData(_id=1)
        self.assertEqual(obj1.x, 1)
        self.assertRaises(DatabaseException, ORM.testData, _id=1, x=1)
        self.assertRaises(DatabaseException, ORM.testData)
        self.assertRaises(TypeError, ORM.testData, 1)
        self.assertRaises(DatabaseException, ORM.testData, x=1)

    def test_insertion(self):
        new_obj = ORM.testData(_id=101, x=200)
        self.db.commit()
        client = MongoClient()
        db = client[self.db_name]
        new_obj_from_db = db.testData.find_one({"_id": 101})
        self.assertEqual(new_obj_from_db["x"], 200)

    def test_update(self):
        obj1 = ORM.testData(_id=1)
        obj1.x = 201
        obj1.y = 100
        obj1.commit()
        client = MongoClient()
        db = client[self.db_name]
        obj1_from_db = db.testData.find_one({"_id": 1})
        self.assertEqual(201, obj1_from_db["x"])
        self.assertEqual(100, obj1_from_db["y"])

    def tearDown(self):
        from ORM.MongoDB.mongometa import MongoDBMeta
        MongoDBMeta.instance_map = {}
        client = MongoClient()
        client.drop_database(self.db_name)