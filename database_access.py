import sqlite3

conn = sqlite3.connect('imdb.db')
c = conn.cursor()

c.execute('''SELECT * FROM name_basics LIMIT 1''') 
result = c.fetchone()  
print(result)
conn.close()

