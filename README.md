# PyBean

PyBean is intended as a proof-of-concept of a Python RedBeanPHP implementation.

Beans are simple Python objects with a type that maps to the table name
and properties representing the cells of a row.

PyBean is used in development mode (AKA "frozen=False"), where tables and columns are
created on the fly, or in production mode (the default) where schema will not be altered.

## Quick example

    library = Store(SQLiteWriter(":memory:", frozen=False))
    book = library.new("book")
    book.title = "Boost development with pybean"
    book.author = "Charles Xavier"
    library.store(book)
    for book in library.find_by_sql("book","author like ?",["Charles Xavier"]):
            print book.title
    library.trash(book)
