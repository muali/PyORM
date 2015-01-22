#!/usr/bin/env python3.4
# -*- coding: utf-8 -*-
__author__ = 'Moskvitin Maxim'

import unittest
from ORM.database import Database
import ORM

class TestMySQLBasic(unittest.TestCase):
    database = None

    def setUp(self):
        database = Database("MySQL", user="TestUser", password="123", database="world")

    def test_class_load(self):
        self.assertTrue(hasattr(ORM, 'city'))