# -*- coding: utf-8 -*-
# analytics.py
from __future__ import annotations
from dataclasses import dataclass
from functools import lru_cache
import json
import numpy as np
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import os
from dotenv import load_dotenv

'''提供 KPI、退貨原因佔比、熱點、四象限、Top5、時滯、金額統計'''

load_dotenv()

QUADRANT_NOTES = {
    "右上：高銷售×高退貨": "優先處理的爆品地雷",
    "右下：高銷售×低退貨": "明星款，加量加曝光",
    "左上：低銷售×高退貨": "高風險，必要時下架",
    "左下：低銷售×低退貨": "穩定長尾，做關聯推薦",
}
QUADRANT_ORDER = [
    "右上：高銷售×高退貨",
    "右下：高銷售×低退貨",
    "左上：低銷售×高退貨",
    "左下：低銷售×低退貨",
]

# ===== 連線 & 載入 =====
def get_engine(db: dict):
    url = URL.create("mysql+mysqlconnector", username=db["user"], password=db["password"],
                 host=db["host"], port=db["port"], database=db["database"], query={"charset":"utf8mb4"})
    return create_engine(url)

@lru_cache(maxsize=1)
def load_base_df(db_host, db_user, db_password, db_name, db_port=3306) -> pd.DataFrame:
    """從 MySQL 撈三表，建立 base 明細。結果快取（同一參數重用）。"""
    eng = get_engine({"host":db_host,"user":db_user,"password":db_password,"database":db_name,"port":db_port})
    p = pd.read_sql("""
        SELECT barcode, productid, product_name, supplier, sellprice, category1, category2, category3,color,size
        FROM product
    """, eng)
    o = pd.read_sql("SELECT orderid, orderdate, barcode, sell_qty FROM orders", eng)
    r = pd.read_sql("""
        SELECT return_id, orderid, returndate, barcode, return_qty,
               COALESCE(reason_category_l1,'其他') AS reason_cat, reason_tags
        FROM returns_clean
    """, eng)

    o["orderdate"]   = pd.to_datetime(o["orderdate"], errors="coerce")
    r["returndate"]  = pd.to_datetime(r["returndate"], errors="coerce")

    base = o.merge(r, on=["orderid","barcode"], how="left") \
            .merge(p, on="barcode", how="left")
    base["return_qty"]   = base["return_qty"].fillna(0)
    base["sales_amount"] = base["sell_qty"] * base["sellprice"]
    base["loss_amount"]  = base["return_qty"] * base["sellprice"]
    base["order_ym"]     = base["orderdate"].dt.to_period("M").astype(str)
    base["event_ym"]     = base["returndate"].dt.to_period("M").astype(str)
    base["lag_days"]     = (base["returndate"] - base["orderdate"]).dt.days

    def parse_tags(s):
        if pd.isna(s): return []
        try:
            v = json.loads(s)
            return v if isinstance(v, list) else []
        except Exception:
            return []
    base["tags_l2"] = base["reason_tags"].apply(parse_tags)
    return base

# ===== 篩選參數 =====
@dataclass
class Filters:
    productid: list[str] | None = None
    category2: list[str] | None = None
    category3: list[str] | None = None
    reason_l1: list[str] | None = None
    order_from: str | None = None   # 'YYYY-MM-DD'
    order_to:   str | None = None
    return_from: str | None = None
    return_to:   str | None = None

def apply_filters(df: pd.DataFrame, f: Filters) -> pd.DataFrame:
    d = df.copy()
    if f.productid: d = d[d["productid"].isin(f.productid)]
    if f.category2: d = d[d["category2"].isin(f.category2)]
    if f.category3: d = d[d["category3"].isin(f.category3)]
    if f.reason_l1: d = d[d["reason_cat"].isin(f.reason_l1)]
    if f.order_from:  d = d[d["orderdate"]  >= pd.to_datetime(f.order_from)]
    if f.order_to:    d = d[d["orderdate"]  <  pd.to_datetime(f.order_to)]
    if f.return_from: d = d[(~d["returndate"].isna()) & (d["returndate"] >= pd.to_datetime(f.return_from))]
    if f.return_to:   d = d[(~d["returndate"].isna()) & (d["returndate"] <  pd.to_datetime(f.return_to))]
    return d

# ===== 統計函式（每一個統計 = 一個純函式） =====
def kpi_cards(df: pd.DataFrame) -> dict:
    """計算總銷售、退貨率、損失金額、退貨時滯"""
    sales_qty   = float(df["sell_qty"].sum())
    return_qty  = float(df["return_qty"].sum())
    return_rate = (return_qty / sales_qty * 100) if sales_qty else 0.0
    loss_amount = float(df["loss_amount"].sum())
    median_lag  = float(df["lag_days"].median()) if df["lag_days"].notna().any() else 0.0
    return {"sales_qty": int(sales_qty), "return_qty": int(return_qty),
            "return_rate_pct": round(return_rate, 2),
            "loss_amount": round(loss_amount, 0),
            "median_lag_days": round(median_lag, 0)}


def event_return_rate(df: pd.DataFrame, freq: str = "D"):
    """
    事件退貨率趨勢（依 returndate 聚合）
    freq: "D"=日、"M"=月
    退貨率 = 當期退貨件數 / 當期銷售件數 * 100
    注意：事件法的分母用同日(週/月)銷售，解讀是「該時段的退貨壓力」
    """
    if freq not in {"D", "M"}:
        raise ValueError("freq must be 'D'|'M'")

    # 依粒度建立 period 欄
    if freq == "D":
        sale_idx  = df["orderdate"].dt.date
        retur_idx = df["returndate"].dt.date
        x_label   = "日期"
    else:  # "M"
        sale_idx  = df["orderdate"].dt.to_period("M").astype(str)
        retur_idx = df["returndate"].dt.to_period("M").astype(str)
        x_label   = "月份"

    sales = df.groupby(sale_idx)["sell_qty"].sum().rename("銷售件數")
    rets  = df.groupby(retur_idx)["return_qty"].sum().rename("退貨件數")

    trend = pd.concat([sales, rets], axis=1).fillna(0).reset_index()
    trend = trend.rename(columns={"index": x_label})
    trend["退貨率(%)"] = trend["退貨件數"] / trend["銷售件數"].replace(0, np.nan) * 100

    # 日粒度時，順便給一條 7 日移動平均線以便閱讀
    fig = px.line(trend, x=x_label, y="退貨率(%)", markers=True, title=f"事件退貨率趨勢（{x_label}）")
    if freq == "D" and len(trend) >= 7:
        trend_ma = trend.copy()
        trend_ma["退貨率(%)_7日MA"] = trend_ma["退貨率(%)"].rolling(7, min_periods=1).mean()
        fig.add_scatter(x=trend_ma[x_label], y=trend_ma["退貨率(%)_7日MA"], mode="lines", name="7日MA")

    fig.update_layout(height=360, yaxis_tickformat=".2f")
    return trend, fig


def reason_l1_share(df: pd.DataFrame) -> tuple[pd.DataFrame, "plotly.graph_objs.Figure"]:
    """
        退貨原因（L1）佔比
    """
    cat = df.groupby("reason_cat")["return_qty"].sum().sort_values(ascending=False).reset_index()
    cat["佔比(%)"] = (cat["return_qty"] / cat["return_qty"].sum() * 100).round(2) if len(cat) else 0
    fig = px.bar(cat, x="reason_cat", y="佔比(%)", text="佔比(%)")
    fig.update_traces(texttemplate="%{text:.2f}%", textposition="outside", cliponaxis=False)
    fig.update_yaxes(range=[0, cat["佔比(%)"].max() * 1.25])   # 上方多留 25% 空間放文字
    fig.update_layout(
        height=480,                               # 調高
        margin=dict(t=40, b=100, l=40, r=20),     # 加底部邊距，避免 x 軸類別字被卡
        xaxis_title="原因類別", yaxis_title="比例(%)",
        uniformtext_minsize=10, uniformtext_mode="hide"
    )
    fig.update_layout(xaxis_tickangle=-30) 
    return cat, fig

def heatmap(df: pd.DataFrame, level: str = "category3") -> tuple[pd.DataFrame, "plotly.graph_objs.Figure"]:
    '''熱點圖（類別 × 原因）'''
    pt = df.pivot_table(index=level, columns="reason_cat", values="return_qty", aggfunc="sum", fill_value=0)
    level_map = {
        "category1": "大類",
        "category2": "中類",
        "category3": "小類"
    }
    level_name = level_map.get(level, level)  # 找不到就原樣顯示
    fig = px.imshow(pt, aspect="auto", color_continuous_scale="Blues", title=f"{level_name} × 原因 熱點")
    fig.update_layout(xaxis_title="原因分類",yaxis_title=level_name)
    return pt, fig



def scatter_quadrant(df: pd.DataFrame) -> tuple[pd.DataFrame, "plotly.graph_objs.Figure", float, float]:
    '''銷售額 vs 退貨率 四象限圖'''
    prod = (df.groupby(["barcode","product_name"]).agg(
        sales_qty=("sell_qty","sum"),
        return_qty=("return_qty","sum"),
        sales_amount=("sales_amount","sum"),
    ).reset_index())
    prod["return_rate(%)"] = (prod["return_qty"] / prod["sales_qty"].replace(0, np.nan) * 100)
    med_sales = float(prod["sales_amount"].median()) if len(prod) else 0.0
    med_rr    = float(prod["return_rate(%)"].median()) if len(prod) else 0.0
    fig = px.scatter(prod, x="sales_amount", y="return_rate(%)",
                     hover_data=["barcode","product_name","sales_qty","return_qty","sales_amount","return_rate(%)"], 
                     title="銷售額 vs 退貨率（四象限）")
    fig.add_vline(med_sales, line_dash="dash", line_color="gray")
    fig.add_hline(med_rr, line_dash="dash", line_color="gray")
    fig.update_layout(height=420, xaxis_title="銷售額", yaxis_title="退貨率(%)")
    fig.update_traces(
        hovertemplate="<br>".join([
            "商品條碼: %{customdata[0]}",
            "商品名稱: %{customdata[1]}",
            "銷售數量: %{customdata[2]}",
            "退貨數量: %{customdata[3]}",
    ])
)
    return prod, fig, med_sales, med_rr


def quadrant_matrix(prod_df: pd.DataFrame, med_sales: float, med_rr: float,
                    notes_map: dict | None = None) -> pd.DataFrame:
    """
    回傳四象限 Top5 矩陣（含備註欄）
    欄位：四象限 Top5｜備註｜TOP1~TOP5（格式：barcode｜商品名）
    """
    notes = notes_map or QUADRANT_NOTES

    def quad(r):
        hs, hr = r["sales_amount"] >= med_sales, r["return_rate(%)"] >= med_rr
        if hs and hr:  return "右上：高銷售×高退貨"
        if hs and not hr: return "右下：高銷售×低退貨"
        if (not hs) and hr: return "左上：低銷售×高退貨"
        return "左下：低銷售×低退貨"

    df = prod_df.copy()
    df["quadrant"] = df.apply(quad, axis=1)

    def pick_top5(sub: pd.DataFrame, q: str):
        if q == "右上：高銷售×高退貨":
            sub = sub.sort_values(["return_rate(%)", "sales_amount"], ascending=[False, False])
        elif q == "右下：高銷售×低退貨":
            sub = sub.sort_values(["sales_amount", "return_rate(%)"], ascending=[False, True])
        elif q == "左上：低銷售×高退貨":
            sub = sub.sort_values(["return_rate(%)", "sales_amount"], ascending=[False, False])
        else:
            sub = sub.sort_values(["sales_amount", "return_rate(%)"], ascending=[False, True])
        items = sub.head(5).apply(lambda r: f"{r['barcode']}｜{r['product_name']}", axis=1).tolist()
        return items + [""] * (5 - len(items))

    rows = []
    for q in QUADRANT_ORDER:
        t1, t2, t3, t4, t5 = pick_top5(df[df["quadrant"] == q], q)
        rows.append({
            "四象限 Top5": q,
            "備註": notes.get(q, ""),
            "TOP1": t1, "TOP2": t2, "TOP3": t3, "TOP4": t4, "TOP5": t5
        })

    return pd.DataFrame(rows)

def top5_per_reason(df: pd.DataFrame, n: int = 5, denom: str = "total") -> pd.DataFrame:
    """
    各原因 TopN 高風險商品
    denom="total": 分母=該商品期間內總銷售量（建議）
    denom="reason": 分母=該商品在該原因所對應的列之 sell_qty（通常偏小，不建議）
    """
    # 1) 分母：商品總銷售（不看是否有退貨）
    sales_all = (df.groupby(["barcode","product_name"])
                   .agg(sales_qty=("sell_qty","sum"))
                   .reset_index())

    # 2) 分子：按原因的退貨量（只取有退貨且有原因的列）
    ret_reason = (df[(df["return_qty"] > 0) & (df["reason_cat"].notna())]
                    .groupby(["reason_cat","barcode","product_name"])
                    .agg(return_qty=("return_qty","sum"))
                    .reset_index())

    out_base = ret_reason.merge(sales_all, on=["barcode","product_name"], how="left")

    # 可選：若你硬要用「只計入該原因列的銷售量」當分母
    if denom == "reason":
        sales_reason = (df[(df["return_qty"] > 0) & (df["reason_cat"].notna())]
                          .groupby(["reason_cat","barcode","product_name"])
                          .agg(sales_qty_reason=("sell_qty","sum"))
                          .reset_index())
        out_base = out_base.merge(sales_reason, on=["reason_cat","barcode","product_name"], how="left")
        out_base["denom_sales"] = out_base["sales_qty_reason"]
    else:
        out_base["denom_sales"] = out_base["sales_qty"]

    # 3) 退貨率（分母為 0 的避開）
    out_base["return_rate(%)"] = (out_base["return_qty"] /
                                  out_base["denom_sales"].replace(0, np.nan) * 100).round(2)

    # 4) 各原因取 TopN
    blocks = []
    for rc, sub in out_base.groupby("reason_cat"):
        s = (sub.sort_values(["return_rate(%)","return_qty","denom_sales"],
                             ascending=[False, False, False])
                .head(n)
                .loc[:, ["reason_cat","barcode","product_name","denom_sales","return_qty","return_rate(%)"]]
                .rename(columns={"reason_cat": "原因",
                                "barcode": "商品條碼",
                                "product_name": "商品名稱",
                                "denom_sales": "銷售件數",
                                "return_qty": "退貨件數",
                                "return_rate(%)": "退貨率(%)"}))
        blocks.append(s)
    return pd.concat(blocks, ignore_index=True) if blocks else pd.DataFrame()

def lag_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
        退貨時滯統計（天）— 以原因(L1)分組，輸出中文欄位
        欄位：原因、筆數、平均(天)、中位數(天)、標準差(天)、最小(天)、最大(天)
    """
    g = df.dropna(subset=["lag_days"]).groupby("reason_cat")["lag_days"].describe()
    ret = (g.loc[:, ["count","mean","50%","std","min","max"]]
         .reset_index()  # 先把 reason_cat 拉回欄位
         .rename(columns={
             "reason_cat": "原因",
             "count": "筆數",
             "mean": "平均(天)",
             "50%": "中位數(天)",
             "std": "標準差(天)",
             "min": "最小(天)",
             "max": "最大(天)"
         })
         .sort_values("中位數(天)"))
    for col in ["平均(天)", "標準差(天)", "中位數(天)"]:
        ret[col] = ret[col].round(2)
    return ret

def loss_by_reason(df: pd.DataFrame, level="l1") -> pd.DataFrame:
    '''退貨金額按原因分類彙總'''
    if level=="l1":
        d = (df.groupby("reason_cat", as_index=False)["loss_amount"].sum() 
        .sort_values("loss_amount", ascending=False)
        .rename(columns={"reason_cat": "原因分類", "loss_amount": "退貨金額"}))
        d["佔比(%)"] = (d["退貨金額"]/d["退貨金額"].sum()*100).round(2)
        return d
    # L2：展開 tags
    rows=[]
    for _, r in df[["loss_amount","tags_l2"]].iterrows():
        for t in (r["tags_l2"] or []):
            rows.append((t, r["loss_amount"]))
    l2 = pd.DataFrame(rows, columns=["退貨原因","退貨金額"])
    if l2.empty:
        return pd.DataFrame(columns=["退貨原因","退貨金額","佔比(%)"])

    d = (l2.groupby("退貨原因", as_index=False)["退貨金額"].sum()
            .sort_values("退貨金額", ascending=False))
    d["佔比(%)"] = (d["退貨金額"] / max(d["退貨金額"].sum(), 1) * 100).round(2)
    return d

def summary_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    商品退貨統計表
    欄位：barcode, 商品編號, 商品名稱, 顏色(color), size, 銷售數量, 退貨數量, 退貨金額, 退貨率%
    """
    if df.empty:
        return pd.DataFrame(columns=[
            "barcode","商品編號","商品名稱","顏色","size",
            "銷售數量","退貨數量","退貨金額","退貨率%"
        ])

    d = (df.groupby(["barcode","productid","product_name","color","size"], as_index=False)
           .agg(銷售數量=("sell_qty","sum"),
                退貨數量=("return_qty","sum"),
                退貨金額=("loss_amount","sum")))
    d["退貨率%"] = (d["退貨數量"] / d["銷售數量"].replace(0, np.nan) * 100).round(2)

    d = d.rename(columns={
        "productid": "商品編號",
        "product_name": "商品名稱",
        "color": "顏色",
        "size": "size"
    })

    # 合計列放最上面
    total_sales   = d["銷售數量"].sum()
    total_returns = d["退貨數量"].sum()
    total_loss    = d["退貨金額"].sum()
    total_rate    = round(total_returns / total_sales * 100, 2) if total_sales > 0 else 0.0

    total_row = pd.DataFrame([{
        "barcode": "合計",
        "商品編號": "",
        "商品名稱": "",
        "顏色": "",
        "size": "",
        "銷售數量": total_sales,
        "退貨數量": total_returns,
        "退貨金額": total_loss,
        "退貨率%": total_rate
    }])

    d = pd.concat([total_row, d], ignore_index=True)

    return d[["barcode","商品編號","商品名稱","顏色","size",
              "銷售數量","退貨數量","退貨金額","退貨率%"]]
