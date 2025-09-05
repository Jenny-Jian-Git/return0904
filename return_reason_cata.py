import re
import json
import unicodedata

'''
    1.同義詞映射
    2.L2 細分類規則
    3.分類優先順序
'''
# 正規化：全半形、去雜訊、同義詞映射
SYNONYMS = {
    "寄錯": "錯發", "送錯": "錯發", "錯寄": "錯發",
    "漏寄": "漏發", "少寄": "漏發",
    "價差": "價格問題", "補差價": "價格問題",
    "降價": "促銷", "打折": "促銷", "折扣": "促銷",
}
def normalize(text: str) -> str:
    if not isinstance(text, str): return ""
    t = unicodedata.normalize("NFKC", text).lower()
    t = re.sub(r"\s+", "", t)
    for k,v in SYNONYMS.items(): t = t.replace(k, v)
    return t

# L2 細分類規則
# L2 規則：每條規則 = 一個「更細的標籤」
RULES = [
    # 尺寸/版型
    {"category":"尺寸/版型","tag":"版型不符","pattern":r"(版型|領口).*(偏小|太小|過小|較小|不合|不符|過寬|過窄|太寬|太窄|偏寬|偏窄|不協調|不修身|不好看|不)"},
    {"category":"尺寸/版型","tag":"尺寸不符","pattern":r"(尺寸).*(不合|不符|過寬|過窄|太寬|太窄|偏寬|偏窄|不協調|不理想)"},
    {"category":"尺寸/版型","tag":"尺寸偏小","pattern":r"(尺寸).*(偏小|太小|過小|較小)"},
    {"category":"尺寸/版型","tag":"尺寸偏大","pattern":r"(尺寸).*(偏大|太大|過大|較大)"},
    {"category":"尺寸/版型","tag":"尺寸偏短","pattern":r"(尺寸).*(偏短|太短|過短|較短|顯短)"},
    {"category":"尺寸/版型","tag":"尺寸偏長","pattern":r"(尺寸).*(偏長|太長|過長|較長|略長)"},
    {"category":"尺寸/版型","tag":"袖長過長","pattern":r"(袖長|袖子).*(過長|太長|偏長|較長|略長)"},
    {"category":"尺寸/版型","tag":"袖長過短","pattern":r"(袖長|袖子).*(過短|太短|偏短|較短|顯短)"},
    {"category":"尺寸/版型","tag":"衣長過長","pattern":r"(衣長).*(過長|太長|偏長|較長|略長)"},
    {"category":"尺寸/版型","tag":"衣長過短","pattern":r"(衣長).*(過短|太短|偏短|較短|顯短|短)"},
    {"category":"尺寸/版型","tag":"褲長過長","pattern":r"(褲長|褲管).*(過長|太長|偏長|較長|略長|差距|偏差|修)"},
    {"category":"尺寸/版型","tag":"褲長過短","pattern":r"(褲長|褲管).*(過短|太短|偏短|較短|顯短)"},
    {"category":"尺寸/版型","tag":"裙子過長","pattern":r"(裙子|裙長).*(過長|太長|偏長|較長|略長|偏高|偏差)"},
    {"category":"尺寸/版型","tag":"裙子過短","pattern":r"(裙子|裙長).*(過短|太短|偏短|較短|顯短)"},
    {"category":"尺寸/版型","tag":"腰圍不合","pattern":r"(腰圍|腰臀|腰頭|腰線|腰身).*(不合|不符|過寬|過窄|太寬|太窄|偏寬|偏窄|不|偏高|位置)"},
    {"category":"尺寸/版型","tag":"臀圍不合","pattern":r"(臀圍).*(不合|不符|過寬|過窄|太寬|太窄|偏寬|偏窄)"},
    {"category":"尺寸/版型","tag":"胸圍不合","pattern":r"(胸圍).*(不合|不符|過寬|過窄|太寬|太窄|偏寬|偏窄)"},
    {"category":"尺寸/版型","tag":"肩寬不合","pattern":r"(肩寬|肩線).*(不合|不符|過寬|過窄|太寬|太窄|偏寬|偏窄|偏差|偏高)"},
    # 顏色因素
    {"category":"顏色因素","tag":"色差","pattern":r"(色差|與(圖片|照片).*(不符|不同|落差)|照片.*落差|實物.*落差)"},
    {"category":"顏色因素","tag":"顏色偏黃","pattern":r"(顏色|色調).*(偏黃)"},
    {"category":"顏色因素","tag":"顏色偏暗","pattern":r"(顏色|色調).*(偏暗|較暗|偏灰)"},
    {"category":"顏色因素","tag":"顏色偏藍","pattern":r"(顏色|色調).*(偏藍)"},
    # 材質/舒適度
    {"category":"材質/舒適度","tag":"材質偏薄","pattern":r"(太薄|過薄|偏薄|透光|透度)"},
    {"category":"材質/舒適度","tag":"不透氣/悶熱","pattern":r"(不透氣|悶熱)"},
    {"category":"材質/舒適度","tag":"刺癢/扎人/粗糙","pattern":r"(刺癢|扎人|粗糙|粒感)"},
    {"category":"材質/舒適度","tag":"質感差","pattern":r"(質感差|材質(差|不好|不舒服))"},
    {"category":"材質/舒適度","tag":"蓬度不足","pattern":r"(蓬度|蓬鬆).*(不足|不夠|不夠蓬)"},
    {"category":"材質/舒適度","tag":"光澤不足","pattern":r"(光澤).*(不足|不夠)"},
    # 瑕疵因素
    {"category":"瑕疵因素","tag":"破洞/破裂","pattern":r"(瑕疵|破|破洞|破損|破裂|裂縫|裂痕|損壞|損傷|變形|斷裂|脫毛)"},
    {"category":"瑕疵因素","tag":"污漬/髒污","pattern":r"(污漬|髒污|汙漬|口紅|粉底|髒|汙|色點)"},
    {"category":"瑕疵因素","tag":"脫線/開線","pattern":r"(脫線|開線)"},
    {"category":"瑕疵因素","tag":"車線","pattern":r"(車線|車縫).*(不齊|不直|不平|歪|不良|問題)"},
    {"category":"瑕疵因素","tag":"拉鍊故障","pattern":r"拉鍊.*(壞|卡|頓|滑順度)"},
    {"category":"瑕疵因素","tag":"釦子鬆動/缺失","pattern":r"(釦|扣)子.*(鬆|掉|少|偏)"},
    {"category":"瑕疵因素","tag":"異味","pattern":r"(異味)"},
    {"category":"瑕疵因素","tag":"起毛球","pattern":r"(起毛球)"},
    {"category":"瑕疵因素","tag":"其他異常","pattern":r"(不同長|(標示).*(不符|差距))"},
    # 設計/期待落差
    {"category":"設計/期待落差","tag":"款式不喜歡","pattern":r"(款式|風格|設計).*(不喜歡|不合|不符)"},
    {"category":"設計/期待落差","tag":"期待落差","pattern":r"(與期待不符|與預期不符|落差大|不夠平整|挺|未設計)"},
    {"category":"設計/期待落差","tag":"設計不良","pattern":r"((口袋).*(深度)|織目不均|彈性褲|摺線不夠俐落|設計比例|下擺|壓線|配置)"},
    # 物流/包裝
    {"category":"物流/包裝","tag":"配送延遲","pattern":r"(配送|出貨|到貨|物流).*(時間|延遲|延誤|太慢|未送達|遲到|拖延)"},
    {"category":"物流/包裝","tag":"包裝破損","pattern":r"(外箱|包裝).*(破損|擠壓|毀損|緩衝不足)"},
    {"category":"物流/包裝","tag":"錯發","pattern":r"(寄錯|錯發|送錯|錯寄)"},
    {"category":"物流/包裝","tag":"漏發","pattern":r"(漏發|漏寄|少寄|少給|缺件|少貨)"},
    # 活動因素
    {"category":"活動因素","tag":"錯過促銷/價格波動","pattern":r"(促銷|特價|優惠|活動|價格問題|價格調整|想參加晚一檔活動|優惠券|折價券)"},
    # 個人因素
    {"category":"個人因素","tag":"誤購/重複購買","pattern":r"(買錯|誤購|誤買|重複購買|多買)"},
    {"category":"個人因素","tag":"後悔/不需要","pattern":r"(後悔|不需要了|不想要|改變(心意|主意))"},
]

PRIORITY = ["瑕疵因素","物流/包裝","尺寸/版型","材質/舒適度","顏色因素","設計/期待落差","活動因素","個人因素"]

# 分類優先順序
def classify_reason(text: str):
    t = normalize(text)
    cat_scores = {c:0 for c in set(r["category"] for r in RULES)}
    tags_l2 = []               # 更細標籤
    matches = []               # 關鍵字命中詳情

    for rule in RULES:
        for m in re.finditer(rule["pattern"], t):
            cat_scores[rule["category"]] += 1
            tags_l2.append(rule["tag"])
            matches.append({
                "category": rule["category"],
                "tag": rule["tag"],
                "pattern": rule["pattern"],
                "text": m.group(0),
                "start": m.start(),
                "end": m.end()
            })

    # 去重並保序
    def dedupe(seq): 
        seen=set(); out=[]
        for x in seq:
            if x not in seen:
                seen.add(x); out.append(x)
        return out
    tags_l2 = dedupe(tags_l2)

    # 主分類：最高分；若並列，用 PRIORITY
    if any(cat_scores.values()):
        max_s = max(cat_scores.values())
        tied = [c for c,s in cat_scores.items() if s==max_s and s>0]
        primary = sorted(tied, key=lambda c: PRIORITY.index(c))[0]
    else:
        primary = "其他"

    return primary, tags_l2, matches