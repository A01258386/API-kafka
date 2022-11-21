import sqlite3

conn = sqlite3.connect('readings.sqlite')

c = conn.cursor()
c.execute('''
          CREATE TABLE drive
          (id INTEGER PRIMARY KEY, 
           speed INTEGER NOT NULL,
           timestamp VARCHAR(100) NOT NULL,
           lat INTEGER NOT NULL,
           `long` INTEGER NOT NULL,
           date_created VARCHAR(100) NOT NULL,
           trace_id VARCHAR(100) NOT NULL)
          ''')

c.execute('''
          CREATE TABLE fly
          (id INTEGER PRIMARY KEY,
           altitute INTEGER NOT NULL,
           air_pressure INTEGER NOT NULL,
           city VARCHAR(100) NOT NULL,
           weight INTEGER NOT NULL,
           date_created VARCHAR(100) NOT NULL,
           trace_id VARCHAR(100) NOT NULL)
          ''')

conn.commit()
conn.close()
