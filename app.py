# -*- coding: utf-8 -*-
# app_gradio.py
import gradio as gr
import pandas as pd
import numpy as np, json
import os
from dotenv import load_dotenv
from openai import AzureOpenAI
from analytic import (load_base_df, Filters, apply_filters,
                       kpi_cards,event_return_rate , reason_l1_share,
                       heatmap, scatter_quadrant, quadrant_matrix,
                       top5_per_reason, lag_stats, loss_by_reason,summary_table)
'''
    退貨分析儀表板、AI 建議
'''
load_dotenv()

client = AzureOpenAI(
    api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-10-21"),
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
    api_key=os.getenv("AZURE_OPENAI_API_KEY")   )
DEPLOYMENT_NAME = os.getenv("AZURE_OPENAI_DEPLOYMENT") 

# ===== 初始化（載一次，之後靠篩選切子集）=====
db = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME")
}
base = load_base_df(db["host"], db["user"], db["password"], db["database"], db["port"])

# 給 UI 的選單值
opts_productid = sorted(base["productid"].dropna().unique().tolist())
opts_c2 = sorted(base["category2"].dropna().unique().tolist())
opts_c3 = sorted(base["category3"].dropna().unique().tolist())
opts_reason = sorted(base["reason_cat"].fillna("其他").unique().tolist())

def run_dashboard(productid, c2, c3, reason, order_from, order_to, return_from, return_to,granularity):
    f = Filters(productid=productid or None, category2=c2 or None, category3=c3 or None,
                reason_l1=reason or None, order_from=order_from or None, order_to=order_to or None,
                return_from=return_from or None, return_to=return_to or None)
    df = apply_filters(base, f)
    
    # KPI
    kpi = kpi_cards(df)
    kpi_md = (f"**銷售件數**：{kpi['sales_qty']:,}｜"
              f"**退貨件數**：{kpi['return_qty']:,}｜"
              f"**退貨率**：{kpi['return_rate_pct']:.2f}%｜"
              f"**退貨金額**：{kpi['loss_amount']:,}｜"
              f"**退貨時滯(中位)**：{kpi['median_lag_days']:.0f} 天")
    # summary
    summary_df = summary_table(df)
    # 趨勢
    freq_map = {"日":"D",  "月":"M"}
    trend_df, trend_fig = event_return_rate(df, freq=freq_map.get(granularity, "D"))
    # L1 佔比
    cat_df, cat_fig = reason_l1_share(df)
    # 熱點
    h2_df, h2_fig = heatmap(df, level="category2")
    h3_df, h3_fig = heatmap(df, level="category3")
    # 四象限 + 矩陣
    prod_df, sc_fig, med_sales, med_rr = scatter_quadrant(df)
    
    matrix_df = quadrant_matrix(prod_df, med_sales, med_rr)

    # 各原因 Top5
    top5_df = top5_per_reason(df, n=5)
    # 時滯與損失
    lag_df = lag_stats(df)
    loss_l1_df = loss_by_reason(df, "l1")
    loss_l2_df = loss_by_reason(df, "l2")

    
    return (kpi_md, summary_df, trend_fig, cat_fig, h2_fig, h3_fig, sc_fig,
            matrix_df, top5_df, lag_df, loss_l1_df, loss_l2_df)


# === AI 分析函式 ===
def run_ai_reco(productid, c2, c3, reason, order_from, order_to, return_from, return_to, goal_msg):
    f = Filters(productid=productid or None, category2=c2 or None, category3=c3 or None,
                reason_l1=reason or None, order_from=order_from or None, order_to=order_to or None,
                return_from=return_from or None, return_to=return_to or None)
    df = apply_filters(base, f)

    if df.empty:
        return "⚠️ 沒有符合條件的資料，無法生成建議。", pd.DataFrame()

    # 簡單整理上下文
    kpi = kpi_cards(df)
    lag = kpi["median_lag_days"]
    reason_df, _ = reason_l1_share(df)
    top_reason = reason_df.iloc[0]["reason_cat"] if not reason_df.empty else "其他"
    top_reason_pct = reason_df.iloc[0]["佔比(%)"] if not reason_df.empty else 0
    prod_df, _, _, _ = scatter_quadrant(df)
    worst_item = ""
    if not prod_df.empty:
        worst = prod_df.sort_values("return_rate(%)", ascending=False).iloc[0]
        worst_item = f"{worst['barcode']}-{worst['product_name']} 退貨率 {worst['return_rate(%)']:.1f}%"

    ctx = (
        f"銷售 {kpi['sales_qty']} 件，退貨 {kpi['return_qty']} 件，退貨率 {kpi['return_rate_pct']}%，"
        f"損失金額約 {kpi['loss_amount']} 元，中位退貨時滯 {lag} 天。"
        f"主要退貨原因是 {top_reason}，佔比 {top_reason_pct}%。"
        f"退貨率最高的商品為 {worst_item}。"
    )

    messages = [
        {"role": "system", "content": "If you are a general manager of a clothing e-commerce company, please answer in a professional and sharp tone.。"},
        {"role": "user", "content": f"根據以下數據：{ctx}\n請用大約100字中文，總結重點並給出一個建議。\n目標：{goal_msg}"}
    ]
    #呼叫 Azure OpenAI 生成摘要建議
    resp = client.chat.completions.create(
        model=os.getenv("AZURE_OPENAI_DEPLOYMENT"),  # ← 使用環境變數裡的部署名稱
        messages=messages,
        max_tokens=150
    )

    summary = resp.choices[0].message.content
    return summary


# Gradio UI
with gr.Blocks(title="退貨分析儀表板") as demo:
    gr.Markdown("## 📦 退貨分析儀表板 🧐")

    with gr.Row():
        productid = gr.Dropdown(choices=opts_productid, multiselect=True, label="商品編號")
        c2 = gr.Dropdown(choices=opts_c2, multiselect=True, label="中類")
        c3 = gr.Dropdown(choices=opts_c3, multiselect=True, label="小類")
        reason = gr.Dropdown(choices=opts_reason, multiselect=True, label="退貨原因（L1）")

    with gr.Row():
        order_from  = gr.Textbox(label="訂單起日 YYYY-MM-DD", placeholder="2025-01-01")
        order_to    = gr.Textbox(label="訂單訖日 YYYY-MM-DD", placeholder="2025-09-01")
        return_from = gr.Textbox(label="退貨起日 YYYY-MM-DD", placeholder="2025-01-01")
        return_to   = gr.Textbox(label="退貨訖日 YYYY-MM-DD", placeholder="2025-09-01")
        granularity = gr.Radio(choices=["日","月"], value="日", label="粒度（事件日期）")  # ← 這行很關鍵
    btn = gr.Button("更新圖表", variant="primary")

    gr.Markdown("**商品退貨摘要**")
    kpi_md = gr.Markdown()
    gr.Markdown("**商品退貨統計表**")
    summary_tbl = gr.Dataframe(interactive=False, wrap=False)
    trend_fig = gr.Plot()
    cat_fig   = gr.Plot()
    with gr.Row():
        h2_fig = gr.Plot()
        h3_fig = gr.Plot()
    sc_fig   = gr.Plot()

    gr.Markdown("**四象限 Top5 矩陣（barcode｜商品名）**")
    matrix_df = gr.Dataframe(interactive=False, wrap=False)

    gr.Markdown("**各原因 Top5 高風險商品**")
    top5_df = gr.Dataframe(interactive=False, wrap=False)

    with gr.Row():
        lag_df    = gr.Dataframe(interactive=False, wrap=False, label="退貨時滯統計（天）")
        loss_l1   = gr.Dataframe(interactive=False, wrap=False, label="退貨金額 vs 退貨原因（L1）")
        loss_l2   = gr.Dataframe(interactive=False, wrap=False, label="退貨金額 vs 退貨原因（L2 細標籤）")

    btn.click(
        fn=run_dashboard,
        inputs=[productid,c2,c3,reason,order_from,order_to,return_from,return_to,granularity],
        outputs=[kpi_md, summary_tbl, trend_fig, cat_fig, h2_fig, h3_fig, sc_fig,
                matrix_df, top5_df, lag_df, loss_l1, loss_l2]
    )
    gr.Markdown("---") 
    with gr.Row():
        goal_msg = gr.Textbox(
            label="AI 目標描述",
            value="簡要說明退貨狀況重點，並提出一個具體改善建議，重點放在降低退貨率與減少損失。",
            lines=2
        )
    btn_ai = gr.Button("🧠 生成 AI 建議", variant="secondary")
    ai_summary = gr.Markdown()

    btn_ai.click(
        fn=run_ai_reco,
        inputs=[productid, c2, c3, reason, order_from, order_to, return_from, return_to, goal_msg],
        outputs=[ai_summary]
    )



if __name__ == "__main__":
    demo.launch()
