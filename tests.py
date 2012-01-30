import unittest
from time import time
from pybean import SQLiteWriter, Store
import resource
import uuid
import json

   

def memory_usage():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

class TestPybean(unittest.TestCase):
    def setUp(self):
        pass


    def test_unknown_column(self):
        db = self.get_fluid_save()
        try:
            db.find("book","doesntexist = 1").next()
            self.assertTrue(False)
        except StopIteration:
            self.assertTrue(True)

    def test_boolean(self):
        return
        db = self.get_fluid_save()
        book = db.new("book")
        book.title = "another title"
        book.published = True
        db.save(book)
        for book in db.find("book"):
            self.assertTrue(book.published is True)

    def get_frozen_save(self):
        return Store(SQLiteWriter(":memory:"))

    def get_fluid_save(self):
        return Store(SQLiteWriter(":memory:", False))

    def test_new_bean_type(self):
        bean = self.get_frozen_save().new("book")
        self.assertEqual(bean.__class__.__name__, "book")

    def test_bean_save(self):
        db = self.get_fluid_save()
        bean = db.new("book")
        bean.title = "mac beth"
        bean.year = 1606
        db.save(bean)

    def test_get_linked(self):
        db = self.get_fluid_save()
        book = db.new("book")
        book.title = "a book with many authors"
        author1 = db.new("author")
        author1.name = "john doe"
        author2 = db.new("author")
        author2.name = "jane doe"
        book2 = db.new("book")
        book.title = "a book with one author"
        author3 = db.new("author")
        author3.name = "shouldnotseeme"
        db.link(book2, author3)
        db.link(book, author1)
        db.link(book, author2)
        for author in db.get_linked(book, "author"):
            self.assertNotEqual(author.name, "shouldnotseeme")
            self.assertTrue(author.name in ["john doe", "jane doe"])
    
    def test_find(self):
        db = self.get_fluid_save()
        book = db.new("book")
        book.title = "test book"
        db.save(book)
        for book in db.find("book"):
            self.assertEqual(book.title, "test book")
    def test_find_sql(self):
        db = self.get_fluid_save()
        book1 = db.new("book")
        book1.title = "tests book1"
        db.save(book1)
        book2 = db.new("book")
        book2.title = "test book2"
        db.save(book2)
        for book in db.find("book", "title like ?",["%book2%"]):
            self.assertEqual(book.title, "test book2")

    def test_load(self):
        return
        print memory_usage()
        db = self.get_fluid_save()
        print memory_usage() / 1024
        for i in range(10000):
            book = db.new("book")
            book.title = "some random title"
            book.id = i
            db.save(book)
        print memory_usage() / 1024
        memory = 0
        for book in db.find("book"):
            mem = memory_usage()
            if mem > memory:
                print mem / 1024
                memory = mem

    def test_count(self):
        db = self.get_fluid_save()
        self.assertEqual(db.count("book"), 0)
        book1 = db.new("book")
        book1.title = "title1"
        book2 = db.new("book")
        book2.title = "title2"
        db.save(book1)
        db.save(book2)
        self.assertEqual(db.count("book"), 2)
        self.assertEqual(db.count("book", "title like ?", ["title1"]), 1)

    
if __name__ == '__main__':
    unittest.main()
