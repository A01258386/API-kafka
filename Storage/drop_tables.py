import sqlite3

conn = sqlite3.connect('flve.sqlite')

c = conn.cursor()
c.execute('''
          DROP TABLE drive
          ''')

c.execute('''
          DROP TABLE fly
          ''')

conn.commit()
conn.close()
