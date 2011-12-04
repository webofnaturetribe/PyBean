import sqlite3
import uuid
from pkg_resources import parse_version

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
        self.db.cursor().execute("PRAGMA foreign_keys=ON")
    
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
    
    def link(self, bean_a, bean_b):
        table_a = bean_a.__class__.__name__
        table_b = bean_b.__class__.__name__
        assoc_table = self.__create_assoc_table(table_a, table_b)
        self.db.cursor().execute("replace into " + assoc_table + "("+table_a+"_uuid,"+table_b+"_uuid) values(?,?)", 
                [buffer(bean_a.uuid.bytes), buffer(bean_b.uuid.bytes)])
        self.db.commit()
    
    def unlink(self, bean_a, bean_b):
        table_a = bean_a.__class__.__name__
        table_b = bean_b.__class__.__name__
        assoc_table = self.__create_assoc_table(table_a, table_b)
        self.db.cursor().execute("delete from " + assoc_table + " where " + table_a + "_uuid=? and " + table_b + "_uuid=?", 
                [buffer(bean_a.uuid.bytes), buffer(bean_b.uuid.bytes)])
        self.db.commit()

    def __create_assoc_table(self, table_a, table_b):
        assoc_table = "_".join(sorted([table_a, table_b]))
        if self.frozen == False:
            sql = "create table if not exists " + assoc_table + "("
            sql+= table_a + "_uuid NOT NULL REFERENCES " + table_a + "(uuid) ON DELETE cascade,"
            sql+= table_b + "_uuid NOT NULL REFERENCES " + table_b + "(uuid) ON DELETE cascade,"
            sql+= " PRIMARY KEY (" + table_a + "_uuid," + table_b + "_uuid));"
            self.db.cursor().execute(sql)
            # no real support for foreign keys until sqlite3 v3.6.19 so here's the hack
            if cmp(parse_version(sqlite3.sqlite_version),parse_version("3.6.19")) < 0:
                sql = "create trigger if not exists fk_" + table_a + "_" + assoc_table
                sql+= " before delete on " + table_a
                sql+= " for each row begin delete from " + assoc_table + " where " + table_a + "_uuid = OLD.uuid;end;"
                self.db.cursor().execute(sql)
                sql = "create trigger if not exists fk_" + table_b + "_" + assoc_table
                sql+= " before delete on " + table_b
                sql+= " for each row begin delete from " + assoc_table + " where " + table_b + "_uuid = OLD.uuid;end;"
                self.db.cursor().execute(sql)
        return assoc_table

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
    
    def link(self, bean_a, bean_b):
        self.store(bean_a)
        self.store(bean_b)
        self.writer.link(bean_a, bean_b)
    
    def unlink(self, bean_a, bean_b):
        self.writer.unlink(bean_a, bean_b)

    def __row_to_object(self, table_name, row):
        new_object = type(table_name,(object,),{})()
        for key in row.keys():
            if key == "uuid":
                new_object.uuid = uuid.UUID(bytes=row[key])
            else:
                new_object.__dict__[key] = row[key]
        return new_object
