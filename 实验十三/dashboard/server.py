"""
实验十三：前端多维交互联动与高级可视化挑战
大数据分析看板 API 服务（增强版）
—— 支持双向联动、正则检索、子维度下钻、高频词统计
"""
import pandas as pd
import os
import re
from collections import Counter
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

# 加载 .env 文件中的 API Key（优先项目根目录，其次 dashboard 目录）
for env_dir in [Path(__file__).resolve().parent.parent.parent, Path(__file__).resolve().parent]:
    env_path = env_dir / ".env"
    if env_path.exists():
        load_dotenv(env_path)
        print(f"[OK] 已加载环境变量: {env_path}")

app = FastAPI(title="大数据分析看板 API（增强版）")

# ============================================================
# 数据加载层
# ============================================================
FEATURES_PATH = "../../实验十/batch_1000_features.csv"
RAW_PATH = "../../实验九/data/online_shopping_10_cats.csv"

use_raw = True
df_features = None
if os.path.exists(FEATURES_PATH):
    df_features = pd.read_csv(FEATURES_PATH, encoding="gb18030")
    if df_features["cat"].nunique() >= 3:
        df = df_features.copy()
        use_raw = False
        print(f"[OK] 已加载 LLM 增强数据: {len(df)} 条, {df['cat'].nunique()} 个品类")

if use_raw:
    df = pd.read_csv(RAW_PATH, encoding="utf-8")
    df["sentiment"] = df["label"].map({1: "正面", 0: "负面"})
    print(f"[OK] 已加载原始数据（回退模式）: {len(df)} 条")

# 确保 sentiment 列存在
if "sentiment" not in df.columns:
    df["sentiment"] = df["label"].map({1: "正面", 0: "负面"})

print(f"数据概览: {len(df)} 条记录, {df['cat'].nunique()} 个品类")

# ============================================================
# 子维度关键词映射表（用于下钻分析）
# ============================================================
SUB_CATEGORY_KEYWORDS = {
    "物流配送": ["物流", "快递", "配送", "发货", "送到", "收货", "速度", "慢", "快"],
    "产品质量": ["质量", "品质", "正品", "假货", "好用", "不好用", "耐用", "坏了", "瑕疵", "次品"],
    "价格性价比": ["价格", "便宜", "贵", "性价比", "划算", "不值", "价钱", "实惠", "降价", "优惠"],
    "服务态度": ["服务", "态度", "客服", "售后", "热情", "冷漠", "退货", "换货", "保修", "投诉"],
    "外观包装": ["外观", "包装", "好看", "漂亮", "颜值", "破损", "颜色", "款式", "设计", "精致"],
    "功能体验": ["功能", "体验", "使用", "效果", "操作", "流畅", "卡顿", "方便", "实用", "性能"],
}

def extract_sub_category(review_text):
    """基于关键词匹配提取评论的子维度归属"""
    if not isinstance(review_text, str):
        return "其他"
    text = review_text
    scores = {}
    for sub_cat, keywords in SUB_CATEGORY_KEYWORDS.items():
        score = sum(1 for kw in keywords if kw in text)
        if score > 0:
            scores[sub_cat] = score
    if not scores:
        return "其他"
    return max(scores, key=scores.get)


# 为 df 添加子维度列（缓存，启动时计算一次）
print("[INFO] 正在计算子维度标签（关键词匹配），请稍候...")
df["sub_category"] = df["review"].apply(extract_sub_category)
sub_cat_counts = df["sub_category"].value_counts()
print(f"子维度分布: {dict(sub_cat_counts)}")


# ============================================================
# 工具函数：numpy → Python 原生类型转换
# ============================================================
def clean_records(records):
    """将 DataFrame records 中的 numpy 类型转为 Python 原生类型"""
    clean = []
    for r in records:
        item = {}
        for k, v in r.items():
            if hasattr(v, "item"):
                item[k] = v.item()
            elif isinstance(v, (pd.Timestamp,)):
                item[k] = str(v)
            else:
                item[k] = v
        clean.append(item)
    return clean


# ============================================================
# CORS 中间件
# ============================================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
# 接口 A —— 品类分布统计（增强：支持 sentiment 过滤）
# ============================================================
@app.get("/api/category-distribution")
def get_category_distribution(sentiment: str = None):
    """
    返回各品类的样本数量，供前端柱状图使用。
    支持按情感类型过滤（sentiment=正面/负面），实现双向联动。
    """
    filtered = df if sentiment is None or sentiment == "" else df[df["sentiment"] == sentiment]
    stats = filtered["cat"].value_counts()
    return {
        "categories": stats.index.tolist(),
        "counts": stats.values.tolist(),
        "filter_sentiment": sentiment or "全部",
    }


# ============================================================
# 接口 B —— 情感分析概览（增强：支持 cat 过滤）
# ============================================================
@app.get("/api/sentiment-overview")
def get_sentiment_overview(cat: str = None):
    """
    返回各品类的情感分布，供前端堆叠柱状图使用。
    支持按品类过滤（cat=手机），实现双向联动。
    """
    filtered = df if cat is None or cat == "" else df[df["cat"] == cat]
    pivot = filtered.groupby(["cat", "sentiment"]).size().unstack(fill_value=0)
    result = []
    for cat_name in pivot.index:
        row = {"category": cat_name}
        for col in pivot.columns:
            row[col] = int(pivot.loc[cat_name, col])
        result.append(row)
    return {"data": result, "filter_cat": cat or "全部"}


# ============================================================
# 接口 C —— 按条件筛选评论（增强：+ sentiment + regex query）
# ============================================================
@app.get("/api/reviews")
def get_reviews(
    cat: str = None,
    sentiment: str = None,
    query: str = None,
    limit: int = Query(default=20, ge=1, le=200),
):
    """
    多条件筛选评论列表。
    - cat:       品类过滤
    - sentiment: 情感过滤（正面/负面）
    - query:     正则表达式关键词检索
    - limit:     返回条数限制
    """
    filtered = df

    # 品类过滤
    if cat is not None and cat != "":
        filtered = filtered[filtered["cat"] == cat]

    # 情感过滤
    if sentiment is not None and sentiment != "":
        filtered = filtered[filtered["sentiment"] == sentiment]

    # 正则关键词检索
    if query is not None and query.strip() != "":
        q = query.strip()
        try:
            # 尝试正则匹配
            filtered = filtered[filtered["review"].str.contains(q, case=False, na=False, regex=True)]
        except Exception as e:
            # 正则语法错误时降级为普通字符串包含
            print(f"[WARN] 正则语法错误 '{q}': {e}，降级为普通匹配")
            filtered = filtered[filtered["review"].str.contains(q, case=False, na=False, regex=False)]

    records = filtered.head(limit).to_dict(orient="records")
    return {
        "total": int(len(filtered)),
        "data": clean_records(records),
        "filters": {
            "cat": cat or "全部",
            "sentiment": sentiment or "全部",
            "query": query or "无",
        },
    }


# ============================================================
# 接口 D —— 子维度下钻统计（新增，支持 Drill-down）
# ============================================================
@app.get("/api/sub-category-stats")
def get_sub_category_stats(cat: str = None):
    """
    返回指定品类下的子维度（物流/质量/价格/服务/外观/功能）分布。
    若 cat 为空，返回全部品类的子维度汇总。
    """
    filtered = df if cat is None or cat == "" else df[df["cat"] == cat]
    stats = filtered["sub_category"].value_counts()
    return {
        "categories": stats.index.tolist(),
        "counts": stats.values.tolist(),
        "parent_cat": cat or "全部品类",
    }


# ============================================================
# 接口 E —— 高频词统计（新增，支持词云/气泡图）
# ============================================================
@app.get("/api/keywords")
def get_keywords(
    cat: str = None,
    sentiment: str = None,
    query: str = None,
    top_n: int = Query(default=50, ge=10, le=200),
):
    """
    返回当前过滤条件下的高频词及其词频，供词云图使用。
    使用简单的 jieba 分词（如不可用则退化为字符级切分）。
    """
    filtered = df

    if cat is not None and cat != "":
        filtered = filtered[filtered["cat"] == cat]
    if sentiment is not None and sentiment != "":
        filtered = filtered[filtered["sentiment"] == sentiment]
    if query is not None and query.strip() != "":
        q = query.strip()
        try:
            filtered = filtered[filtered["review"].str.contains(q, case=False, na=False, regex=True)]
        except Exception:
            filtered = filtered[filtered["review"].str.contains(q, case=False, na=False, regex=False)]

    # 停用词表（中文常见停用词 + 电商领域无意义词）
    STOP_WORDS = set([
        "的", "了", "在", "是", "我", "有", "和", "就", "不", "人", "都", "一", "一个",
        "上", "也", "很", "到", "说", "要", "去", "你", "会", "着", "没有", "看", "好",
        "自己", "这", "他", "她", "它", "们", "那", "些", "所", "为", "因为", "所以",
        "但是", "然而", "可以", "这个", "那个", "什么", "怎么", "怎么样", "非常", "真的",
        "还", "比较", "挺", "蛮", "太", "特别", "相当", "买", "给", "让", "用", "把",
        "被", "从", "对", "与", "及", "或", "且", "啊", "吧", "呢", "哦", "嗯", "哈",
        "嘛", "呀", "哟", "啦", "吗", "么", "…", "......", "，", "。", "！", "？",
    ])

    # 尝试使用 jieba 分词
    try:
        import jieba
        all_text = " ".join(filtered["review"].dropna().astype(str).tolist())
        words = jieba.lcut(all_text)
    except ImportError:
        # 降级方案：使用正则提取 2-4 字的中文词组
        all_text = "".join(filtered["review"].dropna().astype(str).tolist())
        words = re.findall(r"[一-鿿]{2,4}", all_text)

    # 过滤停用词和短词
    filtered_words = [w.strip() for w in words if w.strip() not in STOP_WORDS and len(w.strip()) >= 2]

    # 词频统计
    word_counts = Counter(filtered_words).most_common(top_n)

    return {
        "data": [{"name": w, "value": c} for w, c in word_counts],
        "total_words": len(filtered_words),
        "unique_words": len(set(filtered_words)),
        "filters": {
            "cat": cat or "全部",
            "sentiment": sentiment or "全部",
            "query": query or "无",
        },
    }


# ============================================================
# 接口 F —— 系统状态检测（任务4：防御性编程）
# ============================================================
@app.get("/api/system-status")
def get_system_status():
    """
    返回当前系统运行状态，包括：
    - LLM API Key 配置情况
    - 数据文件可用性
    - 降级运行标记
    前端根据此接口的返回值决定是否展示降级提示横幅。
    """
    # 检测 API Key 配置
    API_KEY_NAMES = [
        "DEEPSEEK_API_KEY", "SILICONFLOW_API_KEY",
        "DASHSCOPE_API_KEY", "OPENAI_API_KEY", "MOONSHOT_API_KEY",
    ]
    configured_keys = [k for k in API_KEY_NAMES if os.environ.get(k)]
    llm_active = len(configured_keys) > 0

    # 检测数据文件
    data_files_status = {}
    for fpath in [FEATURES_PATH, RAW_PATH]:
        exists = os.path.exists(fpath)
        label = os.path.basename(fpath)
        data_files_status[label] = exists

    # 构建降级原因
    reasons = []
    if not llm_active:
        reasons.append("API_KEY_MISSING")
    if not any(data_files_status.values()):
        reasons.append("DATA_FILE_MISSING")

    return {
        "status": "degraded" if reasons else "healthy",
        "llm_active": llm_active,
        "configured_keys": configured_keys,
        "reasons": reasons if reasons else None,
        "data_files_available": any(data_files_status.values()),
        "data_files": data_files_status,
        "record_count": len(df),
        "category_count": df["cat"].nunique(),
    }


# ============================================================
# 静态文件服务 —— 必须写在所有 @app.get 路由的最后面！
# ============================================================
app.mount("/", StaticFiles(directory="frontend", html=True), name="frontend")
