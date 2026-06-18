#!/usr/bin/env python3
"""
run_fusion_pipeline.py —— 实验十一：异构特征融合与模型训练闭环

覆盖任务：
  任务1: NLP 特征流水线 —— TF-IDF 字符级向量化（500维）
  任务2: 稀疏-稠密异构特征融合 —— scipy.sparse.hstack 拼接
  任务3: LightGBM 消融实验 —— Baseline A vs Baseline B vs Fused C
  任务4: SHAP 模型可解释性 —— 单样本瀑布图 + 全局特征重要性

用法:
  python run_fusion_pipeline.py
  python run_fusion_pipeline.py --csv ../实验十/batch_1000_features.csv
"""

import argparse
import os
import sys
import time
import warnings

import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import OrdinalEncoder, LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    accuracy_score,
    roc_auc_score,
    classification_report,
    confusion_matrix,
)
from scipy.sparse import hstack, csr_matrix, issparse

warnings.filterwarnings("ignore")

# ============================================================
# 全局配置
# ============================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REPORT_DIR = os.path.join(BASE_DIR, "report")
ASSETS_DIR = os.path.join(REPORT_DIR, "assets")

# 默认输入 CSV：实验九的输出结果（LLM 特征已预计算，文件存放在实验十目录）
DEFAULT_CSV = os.path.join(BASE_DIR, "..", "实验十", "batch_1000_features.csv")
# 本实验目录下的原始数据集（cat + label + review，有正负标签）
LOCAL_SOURCE_CSV = os.path.join(BASE_DIR, "online_shopping_10_cats (1).csv")
# 原实验九目录下的数据集
SOURCE_CSV = os.path.join(BASE_DIR, "..", "实验九",
                          "data", "online_shopping_10_cats.csv")

# LLM 标签列（实验九提取的结构化特征）
# 完整模式（batch_1000_features.csv）: cat + sentiment + category
# 平衡模式（原始 CSV）: 仅 cat
LLM_COLS_FULL = ["cat", "sentiment", "category"]
LLM_COLS_MINIMAL = ["cat"]

# TF-IDF 参数
TFIDF_MAX_FEATURES = 500
TFIDF_ANALYZER = "char"  # 字符级，避免中文分词误差

# LightGBM 参数
LGB_PARAMS = {
    "n_estimators": 100,
    "random_state": 42,
    "verbose": -1,
}

# 训练/测试划分
TEST_SIZE = 0.2
RANDOM_STATE = 42


# ============================================================
# 辅助函数
# ============================================================
def print_section(title: str):
    """打印分节标题"""
    print()
    print("=" * 65)
    print(f"  {title}")
    print("=" * 65)


def print_subsection(title: str):
    """打印子标题"""
    print(f"\n--- {title} ---")


def load_data(csv_path: str, balanced: bool = False) -> pd.DataFrame:
    """加载数据，支持从原始数据集平衡采样

    Args:
        csv_path: 首选 CSV 路径（实验九的 batch_1000_features.csv）
        balanced: 若为 True，从本实验目录下的原始数据集分层采样 500正+500负，
                  使用 cat 列作为结构化标签特征
    """
    if balanced:
        # 确定用哪个原始数据文件
        source_path = LOCAL_SOURCE_CSV if os.path.exists(LOCAL_SOURCE_CSV) else SOURCE_CSV
        print(f"[INFO] --balanced 模式：从 {os.path.basename(source_path)} 分层采样...")

        for enc in ["utf-8-sig", "utf-8", "gbk", "gb18030", "gb2312"]:
            try:
                source_df = pd.read_csv(source_path, encoding=enc)
                print(f"[INFO] 成功读取，共 {len(source_df)} 行，列名: {list(source_df.columns)}")
                break
            except (UnicodeDecodeError, UnicodeError):
                continue

        # 分层采样：500 positive + 500 negative
        pos = source_df[source_df["label"] == 1].sample(n=500, random_state=42)
        neg = source_df[source_df["label"] == 0].sample(n=500, random_state=42)
        df = pd.concat([pos, neg], ignore_index=True)
        df = df.sample(frac=1, random_state=42).reset_index(drop=True)
        print(f"[INFO] 分层采样完成: 正样本=500, 负样本=500")
        print(f"[INFO] 结构化特征列: ['cat'] (作为 LLM 标签的等价特征)")
        return df

    # 默认模式：读取实验九的 batch_1000_features.csv
    for enc in ["utf-8-sig", "utf-8", "gbk", "gb18030", "gb2312"]:
        try:
            df = pd.read_csv(csv_path, encoding=enc)
            print(f"[INFO] 成功以 {enc} 编码读取，共 {len(df)} 行 {len(df.columns)} 列")
            print(f"[INFO] 列名: {list(df.columns)}")
            break
        except (UnicodeDecodeError, UnicodeError):
            continue
    else:
        raise ValueError(f"无法以任何已知编码读取文件: {csv_path}")

    # 检查标签平衡
    label_dist = df["label"].value_counts().to_dict()
    print(f"[INFO] 标签分布: {label_dist}")

    if len(label_dist) < 2:
        print()
        print("  ╔══════════════════════════════════════════════════════════╗")
        print("  ║  [WARNING]  标签严重不平衡！所有 1000 条 label 相同     ║")
        print("  ║  分类器无法学到有意义的决策边界。                        ║")
        print("  ║                                                          ║")
        print("  ║  建议: python run_fusion_pipeline.py --balanced          ║")
        print("  ║        (从原始数据中采样 500正 + 500负)                   ║")
        print("  ╚══════════════════════════════════════════════════════════╝")
        print()

    return df


# ============================================================
# 任务 1：TF-IDF 字符级向量化
# ============================================================
def task1_tfidf_vectorize(df: pd.DataFrame):
    """
    任务 1：NLP 特征流水线
    对 review 列进行字符级 TF-IDF 向量化，生成 500 维稀疏矩阵
    """
    print_section("任务 1：TF-IDF 字符级向量化")

    print(f"[INFO] analyzer='char' — 逐字统计，无需中文分词")
    print(f"[INFO] max_features={TFIDF_MAX_FEATURES} — 保留 Top-500 高频字符")

    tfidf = TfidfVectorizer(
        analyzer=TFIDF_ANALYZER,
        max_features=TFIDF_MAX_FEATURES,
    )
    X_text_sparse = tfidf.fit_transform(df["review"].fillna(""))

    print(f"\n  TF-IDF 稀疏矩阵维度: {X_text_sparse.shape}")
    print(f"  非零元素数: {X_text_sparse.nnz}")
    print(f"  稀疏度: {X_text_sparse.nnz / (X_text_sparse.shape[0] * X_text_sparse.shape[1]) * 100:.2f}%")
    print(f"  数据类型: {type(X_text_sparse).__name__}")
    print(f"  Top-10 高频字符: {tfidf.get_feature_names_out()[:10].tolist()}")

    return tfidf, X_text_sparse


# ============================================================
# 任务 2：稀疏-稠密异构特征融合
# ============================================================
def task2_sparse_dense_fusion(df, X_text_sparse, llm_cols):
    """
    任务 2：稀疏-稠密矩阵拼接
    scipy.sparse.hstack 将 TF-IDF 稀疏矩阵 + LLM 稠密标签矩阵水平拼接
    """
    print_section("任务 2：稀疏-稠密异构特征融合（Sparse-Dense Fusion）")

    # Step 1: 提取并编码 LLM 标签
    print_subsection("Step 1: LLM 标签 OrdinalEncoder 编码")
    df_llm = df[llm_cols].fillna("Unknown").copy()
    print(f"  LLM 标签列: {llm_cols}")
    print(f"  缺失值统计:\n{df[llm_cols].isnull().sum().to_string()}")

    encoder = OrdinalEncoder()
    X_dense = encoder.fit_transform(df_llm)
    print(f"  X_dense 维度: {X_dense.shape}")
    print(f"  各列类别数: {[len(cats) for cats in encoder.categories_]}")
    for i, col in enumerate(llm_cols):
        cats = encoder.categories_[i]
        print(f"    {col}: {cats.tolist()}")

    # Step 2: 稀疏-稠密拼接
    print_subsection("Step 2: scipy.sparse.hstack 水平拼接")
    X_fused = hstack([X_text_sparse, csr_matrix(X_dense)])

    y = df["label"].values  # 目标变量

    # ★★★ 矩阵拼接验证 —— 截图位置 1 ★★★
    print()
    print("  ╔══════════════════════════════════════════════════════╗")
    print("  ║        ★ 矩阵拼接验证 —— 请截取以下输出 ★          ║")
    print("  ╠══════════════════════════════════════════════════════╣")
    print(f"  ║  TF-IDF 特征维度:     {str(X_text_sparse.shape):>25s}  ║")
    print(f"  ║  LLM 标签维度:        {str(X_dense.shape):>25s}  ║")
    print(f"  ║  ─────────────────────────────────────────────  ║")
    print(f"  ║  融合后 X_fused 维度: {str(X_fused.shape):>25s}  ║")
    print(f"  ║                                            ║")
    print(f"  ║  列数验证: {TFIDF_MAX_FEATURES} (TF-IDF) + {X_dense.shape[1]} (LLM标签) = {TFIDF_MAX_FEATURES + X_dense.shape[1]} 列  ║")
    print(f"  ║  预期结果: (1000, {TFIDF_MAX_FEATURES + X_dense.shape[1]})                       ║")
    print("  ╚══════════════════════════════════════════════════════╝")
    print()

    # 验证正确性
    expected_cols = TFIDF_MAX_FEATURES + X_dense.shape[1]
    assert X_fused.shape[0] == X_text_sparse.shape[0], \
        f"行数不匹配: {X_fused.shape[0]} vs {X_text_sparse.shape[0]}"
    assert X_fused.shape[1] == expected_cols, \
        f"列数不匹配: {X_fused.shape[1]} vs {expected_cols}"
    print(f"[验证通过] [PASS] 行数={X_fused.shape[0]}, 列数={X_fused.shape[1]} = {TFIDF_MAX_FEATURES}+{X_dense.shape[1]}")

    return X_dense, X_fused, y


# ============================================================
# 任务 3：LightGBM 消融实验
# ============================================================
def task3_ablation_study(X_text_sparse, X_dense, X_fused, y):
    """
    任务 3：消融实验（Ablation Study）
    Baseline A: 纯 TF-IDF（500维）
    Baseline B: 纯 结构化标签（N维）
    Fused C:    TF-IDF + 结构化标签（500+N维）
    """
    print_section("任务 3：LightGBM 消融实验（Ablation Study）")

    # 获取实际维度用于展示
    dim_a = X_text_sparse.shape[1]
    dim_b = X_dense.shape[1]
    dim_c = X_fused.shape[1]

    # Lazy import —— 仅在使用时才导入
    import lightgbm as lgb

    # 统一划分（确保三组实验在完全相同的 train/test 上比较）
    # 检查标签分布，若只有单类别则不能用 stratify
    unique_labels = np.unique(y)
    stratify_param = y if len(unique_labels) >= 2 else None
    if stratify_param is None:
        print("[WARNING] 标签只有单一类别，无法使用 stratify，结果将无意义！")

    X_train_c, X_test_c, y_train, y_test = train_test_split(
        X_fused, y, test_size=TEST_SIZE, random_state=RANDOM_STATE,
        stratify=stratify_param,
    )

    # 因为 sparse matrix 不支持按行切片取子集（取的是部分列），
    # 我们需要用索引数组来切割原始矩阵
    # 重新做三次 split，random_state 相同保证划分一致
    X_train_a, X_test_a, _, _ = train_test_split(
        X_text_sparse, y, test_size=TEST_SIZE, random_state=RANDOM_STATE,
        stratify=stratify_param,
    )
    X_train_b, X_test_b, _, _ = train_test_split(
        X_dense, y, test_size=TEST_SIZE, random_state=RANDOM_STATE,
        stratify=stratify_param,
    )

    print(f"训练集: {X_train_c.shape[0]} 条")
    print(f"测试集: {X_test_c.shape[0]} 条")
    print(f"正负样本比: {np.mean(y_train):.2f} / {1 - np.mean(y_train):.2f}")

    experiments = [
        ("Baseline A", f"纯 TF-IDF ({dim_a}维)", X_train_a, X_test_a),
        ("Baseline B", f"纯 结构化标签 ({dim_b}维)", X_train_b, X_test_b),
        ("Fused C",   f"TF-IDF + 结构化 ({dim_c}维)", X_train_c, X_test_c),
    ]

    results = []
    for name, desc, X_tr, X_te in experiments:
        print_subsection(f"训练 {name}: {desc}")
        t0 = time.time()

        clf = lgb.LGBMClassifier(**LGB_PARAMS)
        clf.fit(X_tr, y_train)
        y_pred = clf.predict(X_te)
        y_proba = clf.predict_proba(X_te)[:, 1]

        elapsed = time.time() - t0

        acc = accuracy_score(y_test, y_pred)
        auc = roc_auc_score(y_test, y_proba)

        print(f"  训练耗时: {elapsed:.2f}s")
        print(f"  Accuracy:  {acc:.4f}")
        print(f"  AUC:       {auc:.4f}")

        results.append({
            "name": name,
            "desc": desc,
            "clf": clf,
            "acc": acc,
            "auc": auc,
            "elapsed": elapsed,
            "y_pred": y_pred,
            "y_proba": y_proba,
        })

    # ★★★ 消融实验对照表 ★★★
    print()
    print("  ╔══════════════════════════════════════════════════════════════════════╗")
    print("  ║              ★ 消融实验对照表 —— 请截取以下输出 ★                  ║")
    print("  ╠════════════════════════════════════════════╦══════════╦══════════════╣")
    print("  ║  组别                    特征维度          ║ Accuracy ║     AUC      ║")
    print("  ╠════════════════════════════════════════════╬══════════╬══════════════╣")
    for r in results:
        name_tag = r["name"]
        desc_tag = r["desc"]
        acc_val = r["acc"]
        auc_val = r["auc"]
        print(f"  ║  {name_tag:<10s}  {desc_tag:<24s}  ║  {acc_val:<6.4f}  ║  {auc_val:<6.4f}     ║")
    print("  ╚════════════════════════════════════════════╩══════════╩══════════════╝")
    print()

    # 找出最佳
    best = max(results, key=lambda r: r["auc"])
    print(f"[结论] 最佳模型: {best['name']} ({best['desc']})")
    print(f"       AUC={best['auc']:.4f}, Accuracy={best['acc']:.4f}")

    # 协同增益分析
    acc_a, acc_b, acc_c = results[0]["acc"], results[1]["acc"], results[2]["acc"]
    auc_a, auc_b, auc_c = results[0]["auc"], results[1]["auc"], results[2]["auc"]

    print()
    if acc_c > max(acc_a, acc_b):
        gain = (acc_c - max(acc_a, acc_b)) * 100
        print(f"[分析] [协同增益] 异构融合存在协同增益：Fused C 超出最强单源 {gain:.2f} 个百分点")
    elif acc_c >= max(acc_a, acc_b) - 0.005:
        print(f"[分析] [注意] 异构融合未显著优于最强单源，特征间可能存在信息冗余")
    else:
        print(f"[分析] [下降] 融合后性能下降，可能引入了噪声")

    return results, y_test


# ============================================================
# 任务 4：SHAP 模型可解释性分析
# ============================================================
def task4_shap_analysis(results, y_test, X_fused, tfidf, X_test_c, llm_cols):
    """
    任务 4：SHAP 特征归因分析
    - 全局特征重要性（摘要图）
    - 单样本瀑布图（瀑布图）
    """
    print_section("任务 4：SHAP 模型可解释性分析")

    import shap
    import matplotlib
    matplotlib.use("Agg")  # 非交互式后端，避免弹出窗口
    import matplotlib.pyplot as plt

    # 取 Fused C 模型
    fused_result = results[2]
    clf_c = fused_result["clf"]

    # 构建特征名列表：500 个 TF-IDF 字符 + LLM 标签列
    tfidf_names = [f"字_{c}" for c in tfidf.get_feature_names_out()]
    feature_names = tfidf_names + llm_cols

    print(f"[INFO] 特征总数: {len(feature_names)}")
    print(f"[INFO]   TF-IDF 字符特征: {len(tfidf_names)} 个（字_XX）")
    print(f"[INFO]   LLM 标签特征:   {len(llm_cols)} 个（{', '.join(llm_cols)}）")

    # Step 1: 构建 SHAP TreeExplainer
    print_subsection("Step 1: 构建 SHAP TreeExplainer")
    explainer = shap.TreeExplainer(clf_c)
    shap_values = explainer.shap_values(X_test_c)

    if issparse(shap_values):
        shap_values = shap_values.toarray()

    print(f"  SHAP values 维度: {shap_values.shape}")
    print(f"  Expected value (基线): {explainer.expected_value:.4f}")

    # Step 2: 全局特征重要性 —— SHAP 摘要图
    print_subsection("Step 2: 绘制全局 SHAP 摘要图（Summary Plot）")
    fig, ax = plt.subplots(figsize=(12, 8))
    shap.summary_plot(
        shap_values, X_test_c.toarray(),
        feature_names=feature_names,
        max_display=20,
        show=False,
    )
    summary_path = os.path.join(ASSETS_DIR, "SHAP_summary.png")
    plt.tight_layout()
    plt.savefig(summary_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  已保存: {summary_path}")

    # Step 3: 全局特征重要性 —— SHAP 条形图
    print_subsection("Step 3: 绘制 SHAP 特征重要性条形图")
    fig, ax = plt.subplots(figsize=(10, 8))
    shap.summary_plot(
        shap_values, X_test_c.toarray(),
        feature_names=feature_names,
        plot_type="bar",
        max_display=20,
        show=False,
    )
    bar_path = os.path.join(ASSETS_DIR, "SHAP_bar.png")
    plt.tight_layout()
    plt.savefig(bar_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  已保存: {bar_path}")

    # Step 4: 单样本瀑布图 —— ★★★ 截图位置 2 ★★★
    print_subsection("Step 4: 绘制单样本 SHAP 瀑布图（Waterfall Plot）")

    # 使用测试集第 0 号样本
    row_data = np.asarray(X_test_c[0].todense()).flatten()
    explanation = shap.Explanation(
        values=shap_values[0].flatten(),
        base_values=float(explainer.expected_value),
        data=row_data,
        feature_names=feature_names,
    )

    fig, ax = plt.subplots(figsize=(10, 8))
    shap.plots.waterfall(explanation, max_display=15, show=False)
    waterfall_path = os.path.join(ASSETS_DIR, "SHAP_waterfall.png")
    plt.tight_layout()
    plt.savefig(waterfall_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  已保存: {waterfall_path}")

    # 分析 Top 推力特征
    print()
    print("  ╔════════════════════════════════════════════════════════════╗")
    print("  ║         ★ SHAP 瀑布图已保存 —— 请截取图片 ★             ║")
    print("  ╠════════════════════════════════════════════════════════════╣")
    print(f"  ║  文件路径: {waterfall_path}")
    print("  ╚════════════════════════════════════════════════════════════╝")
    print()

    # 统计 LLM 特征 vs TF-IDF 特征的 SHAP 贡献
    shap_abs_mean = np.abs(shap_values).mean(axis=0)
    llm_indices = list(range(len(tfidf_names), len(feature_names)))
    tfidf_indices = list(range(0, len(tfidf_names)))

    llm_shap_total = shap_abs_mean[llm_indices].sum()
    tfidf_shap_total = shap_abs_mean[tfidf_indices].sum()

    print(f"  结构化标签特征（{len(llm_indices)}维）SHAP 总贡献:   {llm_shap_total:.4f}  (平均每维: {llm_shap_total/max(len(llm_indices),1):.4f})")
    print(f"  TF-IDF 字频特征（{len(tfidf_indices)}维）SHAP 总贡献: {tfidf_shap_total:.4f}  (平均每维: {tfidf_shap_total/len(tfidf_indices):.4f})")
    if llm_shap_total > 0 and tfidf_shap_total > 0:
        per_dim_ratio = (llm_shap_total / max(len(llm_indices), 1)) / (tfidf_shap_total / len(tfidf_indices))
        print(f"  单维度结构化标签 vs 单维度字频特征贡献比: {per_dim_ratio:.1f}x")
    print()
    print()

    # 列出所有特征中最重要的 Top-10
    top_indices = np.argsort(shap_abs_mean)[::-1][:10]
    print("  Top-10 最重要特征（按 SHAP 绝对值均值排序）:")
    print(f"  {'排名':<6} {'特征名':<20} {'SHAP均值':>10} {'类型':<12}")
    print(f"  {'-'*50}")
    for rank, idx in enumerate(top_indices, 1):
        name = feature_names[idx]
        val = shap_abs_mean[idx]
        ftype = "结构化标签" if idx >= len(tfidf_names) else "TF-IDF字符"
        marker = " *" if idx >= len(tfidf_names) else ""
        print(f"  {rank:<6} {name:<20} {val:>10.4f} {ftype:<12}{marker}")

    return shap_values, feature_names


# ============================================================
# 主流程
# ============================================================
def main():
    parser = argparse.ArgumentParser(
        description="实验十一：异构特征融合与模型训练闭环"
    )
    parser.add_argument(
        "--csv", default=DEFAULT_CSV,
        help=f"实验九输出的 CSV 文件路径（默认: {DEFAULT_CSV}）"
    )
    parser.add_argument(
        "--balanced", action="store_true",
        help="从原始 6 万条数据中分层采样 500 正 + 500 负（推荐！）"
    )
    args = parser.parse_args()

    # 确保输出目录存在
    os.makedirs(ASSETS_DIR, exist_ok=True)

    print("=" * 65)
    print("  实验十一：异构特征融合与模型训练闭环")
    print("  Sparse-Dense Fusion + LightGBM Ablation + SHAP")
    print("=" * 65)
    print(f"  输入数据: {args.csv}")
    if args.balanced:
        print(f"  采样模式: 平衡采样 (500正+500负)")
    print(f"  输出目录: {ASSETS_DIR}")
    print(f"  TF-IDF:   analyzer='char', max_features={TFIDF_MAX_FEATURES}")
    print(f"  LightGBM: n_estimators={LGB_PARAMS['n_estimators']}, random_state={LGB_PARAMS['random_state']}")

    # ---- 加载数据 ----
    print_section("数据加载")
    df = load_data(args.csv, balanced=args.balanced)

    # 动态检测可用的 LLM 标签列
    # 完整模式: cat + sentiment + category
    # 平衡模式: 仅 cat
    available_llm_cols = [c for c in LLM_COLS_FULL if c in df.columns]
    if not available_llm_cols:
        available_llm_cols = [c for c in LLM_COLS_MINIMAL if c in df.columns]
    print(f"[INFO] 可用 LLM/结构化特征列: {available_llm_cols}")

    # 检查必要列
    required_cols = ["review", "label"] + available_llm_cols
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        print(f"[ERROR] 缺少必要列: {missing}")
        sys.exit(1)

    print(f"[INFO] label 分布:\n{df['label'].value_counts().to_string()}")

    # ---- 任务 1 ----
    tfidf, X_text_sparse = task1_tfidf_vectorize(df)

    # ---- 任务 2 ----
    X_dense, X_fused, y = task2_sparse_dense_fusion(df, X_text_sparse, available_llm_cols)

    # ---- 任务 3 ----
    # 预先为任务 4 准备 X_test_c（需要和任务 3 相同的划分）
    from sklearn.model_selection import train_test_split as tts
    unique_y = np.unique(y)
    stratify_y = y if len(unique_y) >= 2 else None
    X_train_c, X_test_c, y_train, y_test = tts(
        X_fused, y, test_size=TEST_SIZE, random_state=RANDOM_STATE, stratify=stratify_y
    )
    results, y_test = task3_ablation_study(X_text_sparse, X_dense, X_fused, y)

    # ---- 任务 4 ----
    task4_shap_analysis(
        results, y_test, X_fused, tfidf, X_test_c, available_llm_cols
    )

    # ---- 完成 ----
    print_section("实验完成")
    print(f"  所有图片已保存至: {ASSETS_DIR}")
    print()
    print("  >>> 需要提交的截图：")
    print(f"     1. 终端中「矩阵拼接验证」框内的输出 → 粘贴到报告 5.2.3 节")
    print(f"     2. {os.path.join(ASSETS_DIR, 'SHAP_waterfall.png')} → 粘贴到报告 5.4.3 节")
    print(f"     3. {os.path.join(ASSETS_DIR, 'SHAP_summary.png')} → 可选，补充分析")
    print(f"     4. {os.path.join(ASSETS_DIR, 'SHAP_bar.png')} → 可选，补充分析")
    print()


if __name__ == "__main__":
    main()
