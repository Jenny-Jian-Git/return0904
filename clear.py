import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import json, os
from dotenv import load_dotenv
from return_reason_cata import classify_reason

'''
    此程式會讀取 returns 表 → 套用原因分類 → 產生 returns_clean 表
'''
# 載入環境變數
load_dotenv()
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}

# 建立 SQLAlchemy 連線
url = URL.create(
    "mysql+mysqlconnector",
    username=DB_CONFIG["user"],
    password=DB_CONFIG["password"],
    host=DB_CONFIG["host"],
    port=DB_CONFIG["port"],
    database=DB_CONFIG["database"],
    query={"charset":"utf8mb4"}
)
engine = create_engine(url)

# 讀取 returns
df = pd.read_sql("SELECT return_id, orderid, returndate, barcode, return_qty, reason FROM returns", engine)

# 套用分類
df["reason_category_l1"] = ""
df["reason_tags"] = ""
df["match_terms"] = ""

for i, row in df.iterrows():
    primary, tags_l2, matches = classify_reason(row["reason"])
    df.at[i, "reason_category_l1"] = primary
    df.at[i, "reason_tags"] = json.dumps(tags_l2, ensure_ascii=False)
    df.at[i, "match_terms"] = json.dumps(matches, ensure_ascii=False)

# 寫回新表
df.to_sql("returns_clean", engine, if_exists="replace", index=False)
print("✅ returns_clean 已更新：含 reason_category_l1 / reason_tags / match_terms")
