import sqlite3
con = sqlite3.connect("cryptotaxcalc.db")
cur = con.cursor()
rows = cur.execute("SELECT date, rate FROM fx_rates WHERE base='BNB' AND quote='EUR' AND date BETWEEN '2023-12-31' AND '2024-01-07' ORDER BY date").fetchall()
print("BNB/EUR rates:", rows)
con.close()
