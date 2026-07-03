import sqlite3
con = sqlite3.connect("cryptotaxcalc.db")
cur = con.cursor()
rows = cur.execute("SELECT substr(timestamp,1,10) as d, COUNT(*) FROM realized_events GROUP BY substr(timestamp,1,10) ORDER BY d").fetchall()
print(rows)
con.close()
