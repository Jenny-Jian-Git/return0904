import sys, os, csv, time
import mysql.connector as mc
from dotenv import load_dotenv

"""
One-time CSV importer for MySQL using executemany (no LOCAL INFILE, no pandas).
- Reads DB creds from .env
- Import order: product -> orders -> returns
Usage:
  python load_once.py
  # folder must contain product.csv, orders.csv, returns.csv
"""

# 載入環境變數
load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}

FILES = {
    "product": "product.csv",
    "orders":  "orders.csv",
    "returns": "returns.csv",
}
COLS = {
    "product": ["barcode","productid","color","size","product_name","supplier","cost","sellprice","category1","category2","category3","img_url"],
    "orders":  ["orderid","orderdate","barcode","sell_qty"],
    "returns": ["orderid","returndate","barcode","return_qty","reason"],
}
INT_COLS = {
    "product": ["cost","sellprice"],
    "orders":  ["sell_qty"],
    "returns": ["return_qty"],
}


def chunked(iterable, n=1000):
    buf = []
    for item in iterable:
        buf.append(item)
        if len(buf) >= n:
            yield buf
            buf = []
    if buf:
        yield buf

def iter_rows(path, table):
    """逐行讀取 CSV，並轉換為 tuple"""
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        cols = COLS[table]
        int_cols = set(INT_COLS.get(table, []))
        for row in reader:
            rec = []
            for c in cols:
                v = row.get(c, "")
                if c in int_cols:
                    try:
                        v = int(v) if str(v).strip() != "" else None
                    except:
                        v = None
                rec.append(v)
            yield tuple(rec)

def insert_table(cur, table, rows):
    cols = COLS[table]
    placeholders = ",".join(["%s"] * len(cols))
    sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders})"
    total = 0
    t0 = time.time()
    for batch in chunked(rows, n=1000):
        cur.executemany(sql, batch)
        total += len(batch)
    return total, time.time() - t0

def main():
    if len(sys.argv) < 2:
        print("用法: python load_once.py /path/to/folder")
        sys.exit(2)
    base = os.path.abspath(sys.argv[1])
    for t, fn in FILES.items():
        if not os.path.exists(os.path.join(base, fn)):
            raise FileNotFoundError(f"缺少 {t} 檔案：{os.path.join(base, fn)}")

    conn = mc.connect(**DB_CONFIG, autocommit=False)
    cur  = conn.cursor()
    cur.execute("SET NAMES utf8mb4")
    cur.execute("SET SESSION sql_mode = 'STRICT_ALL_TABLES'")
    cur.execute("SET time_zone = '+00:00'")
    try:
        # Import order: product -> orders -> returns
        for table in ["product","orders","returns"]:
            path = os.path.join(base, FILES[table])
            rows = list(iter_rows(path, table))
            print(f"[load] {table:<7} rows={len(rows)} ...", end="", flush=True)
            n, sec = insert_table(cur, table, rows)
            conn.commit()
            print(f" done: inserted={n}, {sec:.2f}s")

        # quick sanity check
        cur.execute("""
        SELECT COUNT(*) FROM returns r
        LEFT JOIN orders o USING(orderid, barcode)
        WHERE o.orderid IS NULL OR r.return_qty > o.sell_qty
        """)
        bad = cur.fetchone()[0]
        print(f"[check] returns join & qty ok? bad_rows={bad} (應為 0)")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()
