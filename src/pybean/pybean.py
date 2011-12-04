import sqlite3
import uuid

class SQLiteWriter(object):

    """
    In frozen mode (the default), the writer will not alter db schema.
    Just add frozen=False to enable column creation (or just add False
    as second parameter):

    query_writer = SQLiteWriter(":memory:", False)
    """
    def __init__(self, db_path, frozen = True):
        self.db = sqlite3.connect(db_path)
        self.db.row_factory = sqlite3.Row
        self.frozen = frozen
    
    def replace(self, bean):
        keys = []
        values = []
        self.__create_table(bean.__class__.__name__)
        columns = self.__get_columns(bean.__class__.__name__)
        for key in bean.__dict__:
            keys.append(key)
            if key == "uuid":
                values.append(sqlite3.Binary(bean.__dict__[key].bytes))
            else:
                if key not in columns:
                    self.__create_column(bean.__class__.__name__, key)
                values.append(str(bean.__dict__[key]))
        cursor = self.db.cursor()
        sql  = "replace into " + bean.__class__.__name__ + "(" 
        sql += ",".join(keys) + ") values (" 
        sql += ",".join(["?" for i in keys])  +  ")"
        cursor.execute(sql, values)
        self.db.commit()
   
    def __create_column(self, table, column):
        if self.frozen:
            return
        self.db.cursor().execute("alter table " + table + " add " + column)

    def __get_columns(self, table):
        columns = []
        if self.frozen:
            return columns
        cursor = self.db.cursor()
        cursor.execute("PRAGMA table_info(" + table  + ")")
        for row in cursor:
            columns.append(row["name"])
        return columns

    def __create_table(self, table):
        if self.frozen:
            return
        self.db.cursor().execute("create table if not exists " + table + "(uuid primary key)")

    def get_rows(self, sql, replace=[]):
        cursor = self.db.cursor()
        cursor.execute(sql,replace)
        for row in cursor:
            yield row
    
    def delete(self, bean):
        self.db.cursor().execute("delete from " + bean.__class__.__name__ + " where uuid=?",
                [sqlite3.Binary(bean.uuid.bytes)])
        self.db.commit()

class Store(object):
    """
    A SQL writer should be passed to the constructor:

    beans_store = Store(SQLiteWriter(":memory"), frozen=False)
    """
    def __init__(self, SQLWriter):
        self.writer = SQLWriter 
    
    def new(self, table_name):
        new_object = type(table_name,(object,),{})()
        new_object.uuid = uuid.uuid4()
        return new_object

    def store(self, bean):
        self.writer.replace(bean)
    
    def load(self, table_name, uuid):
        for row in self.writer.get_rows("select * from " + table_name + " where uuid=?", [buffer(uuid.bytes)]):
            return self.__row_to_object(table_name, row)

    def find_by_sql(self, table_name, sql, replace=[]):
        for row in self.writer.get_rows("select * from " + table_name + " where " + sql, replace):
            yield self.__row_to_object(table_name, row)
    
    def trash(self, bean):
        self.writer.delete(bean)

    def __row_to_object(self, table_name, row):
        new_object = type(table_name,(object,),{})()
        for key in row.keys():
            if key == "uuid":
                new_object.uuid = uuid.UUID(bytes=row[key])
            else:
                new_object.__dict__[key] = row[key]
        return new_object
