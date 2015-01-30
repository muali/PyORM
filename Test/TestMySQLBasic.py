#!/usr/bin/env python3.4
# -*- coding: utf-8 -*-
__author__ = 'Moskvitin Maxim'

import unittest
from ORM.MySQL.mysqlengine import *
import ORM
import mysql.connector


class TestMySQLEngine(unittest.TestCase):
    db = None
    username = "TestUser"
    db_name = "PyORMTest"
    password = "123"

    def setUp(self):
        connection = mysql.connector.connect(user=self.username, password=self.password)
        cursor = connection.cursor()
        cursor.execute("CREATE DATABASE %s" % self.db_name)
        connection.commit()
        cursor.close()
        connection.close()
        connection = mysql.connector.connect(user=self.username, password=self.password, database=self.db_name)
        cursor = connection.cursor()
        cursor.execute(
            """
            CREATE TABLE city (
                ID int NOT NULL,
                name char(35) NOT NULL,
                population int,
                CONSTRAINT PRIMARY KEY (ID)
            )
            """
        )
        cursor.execute(
            """
            INSERT INTO city (ID, name, population)
                VALUES (1, 'Kabul', 1780000),
                    (2, 'Qandahar', 237500),
                    (3, 'Herat', 186800),
                    (4, 'Mazar-e-Sharif', 127800),
                    (5, 'Amsterdam', 731200),
                    (6, 'Rotterdam', 593321),
                    (7, 'Haag', 440900),
                    (8, 'Utrecht', 234323),
                    (9, 'Eindhoven', 201843),
                    (10, 'Tilburg', 193238)
           """
        )
        cursor.execute(
            """
            CREATE TABLE ComplexIDTest (
                ID1 int NOT NULL,
                ID2 int NOT NULL,
                val1 char(10) NOT NULL,
                val2 char(10),
                CONSTRAINT PRIMARY KEY (ID1, ID2)
            )
            """
        )
        cursor.execute(
            """
            INSERT INTO ComplexIDTest (ID1, ID2, val1, val2)
                VALUES (1, 1, 'aaaaa', 'aaaaa'),
                    (1, 2, 'aaaaa', 'bbbbb'),
                    (2, 3, 'bbbbb', 'ccccc'),
                    (1, 3, 'aaaaa', 'ccccc'),
                    (3, 3, 'ccccc', 'ccccc'),
                    (4, 5, 'ddddd', 'eeeee'),
                    (5, 6, 'fffff', 'ggggg'),
                    (6, 4, 'ggggg', NULL),
                    (2, 8, 'bbbbb', NULL)
            """
        )
        connection.commit()
        cursor.close()
        connection.close()
        self.db = MySQLEngine(user=self.username, password=self.password, database=self.db_name)

    def test_class_load(self):
        self.assertTrue(hasattr(ORM, 'city'))
        self.assertTrue(hasattr(ORM, 'complexidtest'))

    def test_selection_simple_key(self):
        city1 = ORM.city(ID=1)
        self.assertEqual(city1.name, "Kabul")
        self.assertEqual(city1.population, 1780000)
        self.assertRaises(DatabaseException, ORM.city, ID=11)
        self.assertRaises(TypeError, ORM.city)
        self.assertRaises(TypeError, ORM.city, 1)
        self.assertRaises(TypeError, ORM.city, name="Kabul")
        self.assertRaises(DatabaseException, ORM.city, ID=1, name="Kabul")

    def test_selection_complex_key(self):
        obj1 = ORM.complexidtest(ID1=1, ID2=3)
        self.assertEqual(obj1.val1, "aaaaa")
        self.assertEqual(obj1.val2, "ccccc")
        obj2 = ORM.complexidtest(ID1=6, ID2=4)
        self.assertEqual(obj2.val1, "ggggg")
        self.assertEqual(obj2.val2, None)
        self.assertRaises(DatabaseException, ORM.complexidtest, ID2=9, ID1=2)
        self.assertRaises(TypeError, ORM.complexidtest, ID1=1)
        self.assertRaises(TypeError, ORM.complexidtest, ID2=1)
        self.assertRaises(TypeError, ORM.complexidtest, ID1=1, ID=1)
        self.assertRaises(TypeError, ORM.complexidtest, 1, 2)

    def test_insertion(self):
        new_city = ORM.city(ID=11, name="Saint-Petersburg", population=None)
        new_city.name = "Saint-Petersburg"
        self.db.commit()
        connection = mysql.connector.connect(user=self.username, password=self.password, database=self.db_name)
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT * FROM city WHERE ID=11")
        new_city_from_db = cursor.fetchone()
        self.assertEqual(new_city_from_db["name"], "Saint-Petersburg")
        self.assertEqual(new_city_from_db["population"], None)
        cursor.close()
        connection.close()
        new_city2 = ORM.city(ID=12, name="a"*50, population=None)
        self.assertRaises(Exception, self.db.commit)

    def tearDown(self):
        MySQLMeta.instance_map = {}
        connection = mysql.connector.connect(user=self.username, password=self.password, database=self.db_name)
        cursor = connection.cursor()
        cursor.execute("DROP DATABASE %s" % self.db_name)
        connection.commit()
        cursor.close()
        connection.close()