import sqlite3
import uuid

class SQLiteWriter(object):
    def __init__(self, db_path, freeze = True):
        self.db = sqlite3.connect(db_path)
        self.db.row_factory = sqlite3.Row
        self.freeze = freeze
    
    def replace(self, bean):
        keys = []
        values = []
        self.create_table(bean.__class__.__name__)
        columns = self.get_columns(bean.__class__.__name__)
        for key in bean.__dict__:
            keys.append(key)
            if key == "uuid":
                uid = uuid.UUID(hex=bean.__dict__[key])
                #values.append(buffer(uid.bytes))
                values.append(sqlite3.Binary(uid.bytes))
            else:
                if key not in columns:
                    self.create_column(bean.__class__.__name__, key)
                values.append(str(bean.__dict__[key]))
        cursor = self.db.cursor()
        sql  = "replace into " + bean.__class__.__name__ + "(" 
        sql += ",".join(keys) + ") values (" 
        sql += ",".join(["?" for i in keys])  +  ")"
        cursor.execute(sql, values)
        self.db.commit()
        
        # Get uuid back in hex
        #cursor.execute("select * from tabletest where uuid = ?", [sqlite3.Binary(uid.bytes)])
        #for row in cursor:
        #    uid = uuid.UUID(bytes=row["uuid"])
        #    print uid.hex
    
    def create_column(self, table, column):
        if self.freeze:
            return
        self.db.cursor().execute("alter table " + table + " add " + column)

    def get_columns(self, table):
        columns = []
        if self.freeze:
            return columns
        cursor = self.db.cursor()
        cursor.execute("PRAGMA table_info(" + table  + ")")
        for row in cursor:
            columns.append(row["name"])
        return columns

    def create_table(self, table):
        if self.freeze:
            return
        self.db.cursor().execute(
            "create table if not exists " + table + "(uuid primary key)"
            )


class Store(object):
    def __init__(self, SQLWriter):
        self.writer = SQLWriter 
    
    def new(self, table_name):
        new_object = type(table_name,(object,),{})()
        new_object.uuid = uuid.uuid4().hex
        return new_object

    def store(self, bean):
        self.writer.replace(bean)
    
    def load(table_name, uuid):
        pass

if __name__ == "__main__":
    db = Store(SQLiteWriter("/Users/mickael/pybean.sqlite", False))
    bean = db.new("tabletest")
    bean.title = "test title"
    bean.content = "test content"
    print bean.uuid
    db.store(bean)
