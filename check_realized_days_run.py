import sqlite3
RUN_ID = 162
con = sqlite3.connect("cryptotaxcalc.db")
cur = con.cursor()
rows = cur.execute("SELECT substr(timestamp,1,10) as d, COUNT(*) FROM realized_events WHERE run_id=? GROUP BY substr(timestamp,1,10) ORDER BY d", (RUN_ID,)).fetchall()
print("run", RUN_ID, rows)
con.close()
