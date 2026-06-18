"""
实验十二：FastAPI 数据接口封装与前端可视化基础
大数据分析看板 API 服务
"""
import pandas as pd
import os
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

app = FastAPI(title="大数据分析看板 API")

# ============================================================
# 数据加载层
# ============================================================
# 优先加载 LLM 增强宽表；若不存在则回退到原始数据集
FEATURES_PATH = "../../实验十/batch_1000_features.csv"
RAW_PATH = "../../实验九/data/online_shopping_10_cats.csv"

# 优先加载 LLM 增强宽表；若品类单一（如仅有"书籍"）则回退到原始数据集
use_raw = True
if os.path.exists(FEATURES_PATH):
    df_features = pd.read_csv(FEATURES_PATH, encoding="gb18030")
    if df_features["cat"].nunique() >= 3:  # 至少 3 个品类才使用 LLM 数据
        df = df_features
        use_raw = False
        print(f"已加载 LLM 增强数据: {len(df)} 条, {df['cat'].nunique()} 个品类")

if use_raw:
    df = pd.read_csv(RAW_PATH, encoding="utf-8")
    df["sentiment"] = df["label"].map({1: "正面", 0: "负面"})
    print(f"已加载原始数据（回退模式）: {len(df)} 条")

# 确保有 sentiment 列
if "sentiment" not in df.columns:
    df["sentiment"] = df["label"].map({1: "正面", 0: "负面"})

print(f"数据概览: {len(df)} 条记录, {df['cat'].nunique()} 个品类")
print(f"品类分布: {dict(df['cat'].value_counts())}")

# ============================================================
# CORS 中间件 —— 解决前后端跨域问题
# ============================================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],      # 开发环境允许所有来源
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================
# 接口 A —— 品类分布统计
# ============================================================
@app.get("/api/category-distribution")
def get_category_distribution():
    """返回各品类的样本数量，供前端饼图/柱状图使用"""
    stats = df["cat"].value_counts()
    return {
        "categories": stats.index.tolist(),
        "counts": stats.values.tolist()
    }

# ============================================================
# 接口 B —— 情感分析概览
# ============================================================
@app.get("/api/sentiment-overview")
def get_sentiment_overview():
    """返回各品类的情感分布，供前端堆叠柱状图使用"""
    pivot = df.groupby(["cat", "sentiment"]).size().unstack(fill_value=0)
    result = []
    for cat_name in pivot.index:
        row = {"category": cat_name}
        for col in pivot.columns:
            row[col] = int(pivot.loc[cat_name, col])
        result.append(row)
    return {"data": result}

# ============================================================
# 接口 C —— 按品类筛选评论（带查询参数）
# ============================================================
@app.get("/api/reviews")
def get_reviews(cat: str = None, limit: int = Query(default=20, ge=1, le=200)):
    """按品类筛选评论列表，支持分页限制"""
    filtered = df if cat is None else df[df["cat"] == cat]
    records = filtered.head(limit).to_dict(orient="records")
    # 将 numpy 类型转换为 Python 原生类型，确保 JSON 序列化正常
    clean_records = []
    for r in records:
        clean = {}
        for k, v in r.items():
            if hasattr(v, "item"):  # numpy scalar
                clean[k] = v.item()
            else:
                clean[k] = v
        clean_records.append(clean)
    return {"total": int(len(filtered)), "data": clean_records}

# ============================================================
# 静态文件服务 —— 必须写在所有 @app.get 路由的最后面！
# ============================================================
app.mount("/", StaticFiles(directory="frontend", html=True), name="frontend")
