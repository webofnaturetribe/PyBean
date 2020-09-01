from pybean import Store, SQLiteWriter
library = Store(SQLiteWriter(":memory:", frozen=False))

book = library.new("book")
book.title = "Boost development with pybean"
book.author = "Charles Xavier"
library.save(book)

for book in library.find("book","author like ?",["Charles Xavier"]):
        print book.title

library.delete(book)
library.commit()