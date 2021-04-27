import sqlite3
import random
from operator import div
from time import perf_counter


data = [
    ("".join(random.choices("abciujhfusdfsf", k=8)), "".join(random.choices("abciujhfusdfsf", k=8)))
    for _ in range(10_000)
]
print(data[0])

db = sqlite3.connect(":memory:")
db.execute("CREATE TABLE IF NOT EXISTS test(key TEXT, value TEXT)")

start = perf_counter()
db.executemany("INSERT INTO test(key, value) VALUES (?, ?)", data)
stop = perf_counter() - start
print("insert took: {}ms".format(stop * 1000))

start = perf_counter()
for key in db.execute("SELECT key FROM test").fetchall():
    _ = db.execute("SELECT * FROM test WHERE key = ?", key).fetchone()
stop = perf_counter() - start
print("select took: {}ms".format(stop * 1000))

start = perf_counter()
payload = {k: v for k, v in data}
stop = perf_counter() - start
print("dict insert took: {}ms".format(stop * 1000))


start = perf_counter()
for key in payload:
    _ = payload.get(key)
stop = perf_counter() - start
print("dict select took: {}ms".format(stop * 1000))


