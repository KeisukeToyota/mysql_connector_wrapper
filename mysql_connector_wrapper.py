import mysql.connector


class MySQLConnect:

    # コンストラクタ
    def __init__(self, user='', passwd='', db='', charset='utf8'):
        self.user = user
        self.passwd = passwd
        self.db = db
        self.charset = charset
        self.cnn = mysql.connector.connect(user=self.user,
                                           password=self.passwd,
                                           charset=self.charset,
                                           db=self.db)
        self.cur = self.cnn.cursor(buffered=True)

    # 文の実行、実行結果を整形して返す
    def result(self, string=''):
        self.cur.execute(string)
        raw = self.cur.fetchall()
        result = []
        for i in range(len(raw)):
            record = {}
            for j in range(len(self.cur.column_names)):
                record.update({self.cur.column_names[j]: raw[i][j]})
            result.append(record)
        return result

    # 挿入
    def insert(self, table='', data={}):
        try:
            self.cur.execute('select * from ' + table)
            string1 = ''
            string2 = ''
            for i in range(len(data)):
                string1 += '%s, '
                if isinstance(list(data.values())[i], str or bool or None):
                    string2 += '%s, '
                elif isinstance(list(data.values())[i], int):
                    string2 += '%d, '
            string1 = string1[:-2]
            string2 = string2[:-2]
            string1 = string1 % tuple(data.keys())
            insert = "insert into %s (" % table
            insert = insert + string1 + ") values (" + string2 + ")"
            insert = insert.replace('%s', '\'%s\'')
            insert = insert % tuple(data.values())
            insert = insert.replace('\'None\'', 'null')
            insert = insert.replace('\'True\'', '1')
            insert = insert.replace('\'False\'', '0')
            self.cur.execute(insert)
            self.cnn.commit()
            print(True)
        except:
            print(False)

    # 削除
    def delete(self, table='', where={}, query=''):
        try:
            self.cur.execute('select * from ' + table)
            string = ''
            for i in range(len(where)):
                if isinstance(list(where.values())[i], str):
                    string += '%s=\'%s\' and '
                elif isinstance(list(where.values())[i], int):
                    string += '%s=%d and '
            string = string[:-5]
            w = tuple(where.items())
            where = ()
            for i in range(len(w)):
                where += w[i]
            string = string % where
            delete = 'delete from %s where ' % table + string
            if query and where:
                delete = delete + ' and ' + query
            else:
                delete = delete + query
            self.cur.execute(delete)
            self.cnn.commit()
            print(True)
        except:
            print(False)

    # アップデート
    def update(self, table='', value={}, where={}, query=''):
        try:
            self.cur.execute('select * from ' + table)
            string1 = ''
            string2 = ''
            for i in range(len(value)):
                if isinstance(list(value.values())[i], str):
                    string1 += '%s=\'%s\', '
                elif isinstance(list(value.values())[i], int):
                    string1 += '%s=%d, '
            for i in range(len(where)):
                if isinstance(list(where.values())[i], str):
                    string2 += '%s=\'%s\' and '
                elif isinstance(list(where.values())[i], int):
                    string2 += '%s=%d and '
            string1 = string1[:-2]
            string2 = string2[:-5]
            v = tuple(value.items())
            w = tuple(where.items())
            value = ()
            where = ()
            for i in range(len(v)):
                value += v[i]
            for i in range(len(w)):
                where += w[i]
            string1 = string1 % value
            string2 = string2 % where
            update = 'update %s set ' % table
            update = update + string1 + ' where ' + string2
            update = update + ' and ' + \
                query if(query and where) else update + query
            self.cur.execute(update)
            self.cnn.commit()
            print(True)
        except:
            print(False)

    # close
    def close(self):
        self.cur.close()
        self.cnn.close()

    # カラム名
    def table_column_name(self, table=''):
        self.cur.execute('show columns from ' + table)
        raw = self.cur.fetchall()
        for column in raw:
            print(column[0] + ' ' + column[1])

    # 実行
    def query(self, string=''):
        try:
            self.cur.execute(string)
            self.cnn.commit()
            print(True)
        except:
            print(False)

    # テーブル削除
    def drop_table(self, table=''):
        try:
            self.cur.execute('drop table ' + table)
            self.cnn.commit()
            print(True)
        except:
            print(False)
