import unittest
from time import time
from pybean import SQLiteWriter, Store

class TestPybean(unittest.TestCase):
    def setUp(self):
        pass

    def get_frozen_store(self):
        return Store(SQLiteWriter(":memory:"))

    def get_fluid_store(self):
        return Store(SQLiteWriter(":memory:", False))

    def test_new_bean_type(self):
        bean = self.get_frozen_store().new("book")
        self.assertEqual(bean.__class__.__name__, "book")

    def test_bean_store(self):
        db = self.get_fluid_store()
        bean = db.new("book")
        bean.title = "mac beth"
        bean.year = 1606
        db.store(bean)

    def test_get_linked(self):
        db = self.get_fluid_store()
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
        db = self.get_fluid_store()
        book = db.new("book")
        book.title = "test book"
        db.store(book)
        for book in db.find("book"):
            self.assertEqual(book.title, "test book")
    def test_find_sql(self):
        db = self.get_fluid_store()
        book1 = db.new("book")
        book1.title = "tests book1"
        db.store(book1)
        book2 = db.new("book")
        book2.title = "test book2"
        db.store(book2)
        for book in db.find("book", "title like ?",["%book2%"]):
            self.assertEqual(book.title, "test book2")

if __name__ == '__main__':
    unittest.main()
