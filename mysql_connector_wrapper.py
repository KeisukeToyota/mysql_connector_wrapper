#coding:utf-8
import mysql.connector

class MySQL_Connect:

  #コンストラクタ
  def __init__(self,user='',passwd='',db=''):
    self.user = user
    self.passwd = passwd
    self.db = db
    self.charset = 'utf8'
    self.cnn = mysql.connector.connect(user=self.user,
                                       password=self.passwd,
                                       charset=self.charset,
                                       db=self.db)
    self.cur = self.cnn.cursor(buffered=True)


  #文の実行、実行結果を成形して返す（リストの中にディクショナリ）
  def db_result(self,string=''):
    self.cur.execute(string)
    raw = self.cur.fetchall()
    result = []
    for i in range(len(raw)):
      record = {}
      for j in range(len(self.cur.column_names)):
        record.update({self.cur.column_names[j]:raw[i][j]})
      result.append(record)
    return result


  #挿入
  def db_insert(self,table='',data={}):
    try:
      self.cur.execute('select * from %s' % table)
      string1 = ''
      string2 = ''
      for i in range(len(data)):
        string1 += '%s, '
        if isinstance(data.values()[i],str):
          string2 += '%s, '
        elif isinstance(data.values()[i],int):
          string2 += '%d, '
      string1 = string1[:-2]
      string2 = string2[:-2]
      string1 = string1 % tuple(data.keys())
      insert = "insert into %s (" % table
      insert = insert + string1 + ") values (" + string2 + ")"
      insert = insert.replace('%s','\'%s\'')
      insert = insert % tuple(data.values())
      self.cur.execute(insert)
      self.cnn.commit()
      print(True)
    except:
      print(False)


  #削除
  def db_delete(self,table='',where={},query=''):
    try:
      self.cur.execute('select * from %s' % table)
      string = ''
      for i in range(len(where)):
        if isinstance(where.values()[i],str):
          string += '%s=\'%s\' and '
        elif isinstance(where.values()[i],int):
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


  #アップデート
  def db_update(self,table='',value={},where={},query=''):
    try:
      self.cur.execute('select * from %s' % table)
      string1 = ''
      string2 = ''
      for i in range(len(value)):
        if isinstance(value.values()[i],str):
          string1 += '%s=\'%s\', '
        elif isinstance(value.values()[i],int):
          string1 += '%s=%d, '
      for i in range(len(where)):
        if isinstance(where.values()[i],str):
          string2 += '%s=\'%s\' and '
        elif isinstance(where.values()[i],int):
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
      if query and where:
        update = update + ' and ' + query
      else:
        update = update + query
      self.cur.execute(update)
      self.cnn.commit()
      print(True)
    except:
      print(False)








import mysql_operation
sql = mysql_operation.MySQL_Connect('root','Toyota_0715','cyber')
