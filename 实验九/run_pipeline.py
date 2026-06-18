#!/usr/bin/env python3
"""
run_pipeline.py —— 实验九：大模型 API 接入与非结构化特征提取

覆盖任务：
  任务2: API 连通性测试
  任务3: Prompt 模板设计与单条解析闭环
  任务4: 小批量串行处理与 DataFrame 重构 (+ 耗时统计)
  任务5: 特征拼接与 CSV 落盘持久化
  任务6: A/B 测试 —— 基座模型热切换 (DeepSeek → Qwen)

用法:
  python run_pipeline.py                          # 默认: DeepSeek, 5条测试
  python run_pipeline.py --model B                # A/B 测试: Qwen 小模型
  python run_pipeline.py --model A --start 200 --num 10  # 自定义切片区间
"""

import argparse
import json
import os
import sys
import time

import pandas as pd
from openai import OpenAI


# ============================================================
# 全局配置
# ============================================================
BASE_URL = "https://api.siliconflow.cn/v1"

MODEL_CONFIG = {
    "A": {  # 主力模型
        "name": "deepseek-ai/DeepSeek-V4-Flash",
        "label": "DeepSeek-V4-Flash (主力推理模型)",
    },
    "B": {  # 免费小模型 (A/B 对比)
        "name": "Qwen/Qwen3.5-9B",
        "label": "Qwen3.5-9B (免费开源小模型)",
    },
}

# 数据集路径
DATA_PATH = os.path.join(os.path.dirname(__file__), "data", "online_shopping_10_cats.csv")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "outputs")


# ============================================================
# 任务3: Prompt 模板设计
# ============================================================
PROMPT_TEMPLATE = """你是一个专业的电商数据流清洗组件，专门负责从非结构化评论文本中提取结构化特征。

你的任务：
分析以下商品评论，从中提取3个维度的核心特征。

评论内容：
"{review}"

提取要求：
1. sentiment（情感倾向）：严格限制在 [正面, 负面, 中性] 范围内。判断标准：
   - 正面：评论表达了满意、认可、喜欢、推荐等积极情绪
   - 负面：评论表达了不满、失望、厌恶、投诉等消极情绪
   - 中性：仅客观描述事实，无明显情感倾向，或正负面评价均有且难分主次

2. category（问题归属）：严格限制在 [物流, 质量, 价格, 服务, 综合] 范围内。判断标准：
   - 物流：主要涉及配送速度、包装完整性、快递态度等运输相关问题
   - 质量：主要涉及产品本身的质量、性能、耐用度、外观等问题
   - 价格：主要涉及性价比、降价、优惠活动、价格高/低等问题
   - 服务：主要涉及客服态度、售后处理、安装服务、维修等人员交互问题
   - 综合：无法明确归入以上单一类别，或评论涉及多个方面且无主导话题

3. summary（核心诉求概括）：严格限制在15个汉字以内，用最简洁的语言概括评论的核心观点。

请仅返回一个纯净的 JSON 对象，绝不要包含诸如'好的'、'这是你的JSON'等任何额外解释性文本，也不要包含 Markdown 代码块标记。
输出格式示例：
{{"sentiment": "正面", "category": "质量", "summary": "产品质量优秀值得购买"}}"""


# ============================================================
# 客户端初始化
# ============================================================
def init_client():
    """任务2: 安全读取 API Key 并初始化 OpenAI 客户端"""
    api_key = os.getenv("SILICONFLOW_API_KEY")
    if not api_key:
        # 尝试从 .env 文件读取
        try:
            from dotenv import load_dotenv
            env_path = os.path.join(os.path.dirname(__file__), ".env")
            load_dotenv(env_path)
            api_key = os.getenv("SILICONFLOW_API_KEY")
        except ImportError:
            pass

    if not api_key:
        print("=" * 60)
        print("⚠ 安全提示: 未检测到 SILICONFLOW_API_KEY 环境变量")
        print("-" * 60)
        print("请选择以下任一方式配置 API Key:")
        print("  1. 设置系统环境变量: set SILICONFLOW_API_KEY=sk-xxxxxxxx")
        print("  2. 在 .env 文件中添加: SILICONFLOW_API_KEY=sk-xxxxxxxx")
        print("  3. 临时输入 (不推荐提交到 Git):")
        print("=" * 60)
        api_key = input("请输入你的 SiliconFlow API Key (输入不会回显): ").strip()
        if not api_key:
            raise RuntimeError("必须提供有效的 API Key 才能继续执行。")

    return OpenAI(api_key=api_key, base_url=BASE_URL)


# ============================================================
# 任务2: API 连通性测试
# ============================================================
def test_connectivity(client, model_name):
    """发送简短测试请求，打印完整底层响应对象"""
    print("\n" + "=" * 60)
    print("任务2: API 连通性测试")
    print(f"模型: {model_name}")
    print("=" * 60)

    response = client.chat.completions.create(
        model=model_name,
        messages=[
            {"role": "user", "content": "你好，请回复'测试成功'两个字。"}
        ],
    )

    print("\n>>> 底层响应对象 (完整打印) <<<")
    print(response)
    print("\n>>> 提取的回复文本 <<<")
    print(response.choices[0].message.content)

    print("\n✓ API 连通性测试通过")
    return response


# ============================================================
# 任务3 & 4: 结构化特征抽取核心函数
# ============================================================
def extract_features(text: str, client, model_name: str) -> dict:
    """
    将一条非结构化评论文本送入 LLM，提取 sentiment / category / summary 三个结构化特征。

    防御性编程：
    - 捕获 json.JSONDecodeError，返回错误标记字典，确保外部循环不会因单条失败而崩溃。
    - 捕获 API 调用异常，同样返回兜底字典。
    """
    prompt = PROMPT_TEMPLATE.format(review=text)

    try:
        response = client.chat.completions.create(
            model=model_name,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0.1,  # 低温度提高格式服从度
            max_tokens=256,
        )

        raw_text = response.choices[0].message.content.strip()

        # 防御: 清理可能的 Markdown 代码块标记
        if raw_text.startswith("```"):
            lines = raw_text.split("\n")
            # 移除第一行 (```json 或 ```) 和最后一行 (```)
            if lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            raw_text = "\n".join(lines).strip()

        result = json.loads(raw_text)

        # 兜底校验: 确保必要字段存在
        result.setdefault("sentiment", "解析失败")
        result.setdefault("category", "解析失败")
        result.setdefault("summary", "解析失败")

        return result

    except json.JSONDecodeError as e:
        return {
            "sentiment": "JSON解析错误",
            "category": "JSON解析错误",
            "summary": str(e)[:50],
        }
    except Exception as e:
        return {
            "sentiment": "API错误",
            "category": "API错误",
            "summary": str(e)[:50],
        }


# ============================================================
# 任务3 (闭环验证): 单条解析测试
# ============================================================
def test_single_extraction(client, model_name):
    """使用一条真实复杂评价，验证 Prompt → LLM → JSON → Dict 的完整闭环"""
    print("\n" + "=" * 60)
    print("任务3: 单条解析闭环验证")
    print("=" * 60)

    test_review = ("平板电脑用了不到一个月就出现屏幕闪烁问题，联系客服一直推脱不予处理，"
                   "售后体验极差，产品质量也令人担忧，价格也不算便宜，非常失望。")

    print(f"输入评论:\n  {test_review}\n")

    result = extract_features(test_review, client, model_name)

    print("LLM 返回的 JSON 字典:")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    print(f"\n  sentiment = {result.get('sentiment')}")
    print(f"  category  = {result.get('category')}")
    print(f"  summary   = {result.get('summary')}")

    print("\n✓ 单条解析闭环验证通过 —— 格式合法，键值可提取")
    return result


# ============================================================
# 任务4: 小批量串行处理 + DataFrame 重构 + 时耗统计
# ============================================================
def batch_process_reviews(client, model_name, start_idx=0, num_reviews=5):
    """
    从数据集中切片指定数量的评论，串行调用 LLM 提取特征，
    返回 DataFrame 并统计总时耗。

    参数:
        start_idx:  切片的起始索引 (对应 df.iloc[start_idx])
        num_reviews: 处理的评论条数
    """
    print("\n" + "=" * 60)
    print(f"任务4: 小批量串行处理 ({num_reviews}条) + 耗时统计")
    print(f"模型: {model_name}")
    print("=" * 60)

    # 读取数据集
    df = pd.read_csv(DATA_PATH, encoding="utf-8")
    print(f"数据集总行数: {len(df)}, 切片区间: iloc[{start_idx}:{start_idx + num_reviews}]")

    # 切片获取测试评论
    review_slice = df["review"].iloc[start_idx:start_idx + num_reviews]
    reviews = review_slice.tolist()

    results = []

    print(f"\n开始串行处理 {len(reviews)} 条评论文本...")
    t_start = time.perf_counter()

    for i, review in enumerate(reviews):
        t_item_start = time.perf_counter()
        features = extract_features(review, client, model_name)
        t_item_elapsed = time.perf_counter() - t_item_start

        features["_review_index"] = i
        features["_elapsed_sec"] = round(t_item_elapsed, 2)
        results.append(features)

        print(f"  [{i+1}/{len(reviews)}] "
              f"耗时 {t_item_elapsed:.2f}s | "
              f"sentiment={features.get('sentiment', '?')} | "
              f"category={features.get('category', '?')} | "
              f"summary={features.get('summary', '?')}")

    t_total = time.perf_counter() - t_start

    # 构建 DataFrame
    results_df = pd.DataFrame(results)
    # 移除内部辅助字段，保留纯特征列
    display_df = results_df.drop(columns=["_review_index", "_elapsed_sec"], errors="ignore")

    print(f"\n{'='*60}")
    print(f">>> 结构化特征表 (Pandas DataFrame) <<<")
    print(f"{'='*60}")
    print(display_df.to_string(index=True))

    print(f"\n{'='*60}")
    print(f">>> 基准时耗统计 <<<")
    print(f"  处理条数:  {len(reviews)}")
    print(f"  总耗时:    {t_total:.2f} 秒")
    print(f"  平均耗时:  {t_total / len(reviews):.2f} 秒/条")
    print(f"  最快单条:  {min(r['_elapsed_sec'] for r in results):.2f} 秒")
    print(f"  最慢单条:  {max(r['_elapsed_sec'] for r in results):.2f} 秒")
    print(f"{'='*60}")

    return results_df, t_total, review_slice


# ============================================================
# 任务5: 特征拼接与 CSV 落盘持久化
# ============================================================
def merge_and_save(original_slice, features_df, model_label):
    """将原始数据与 LLM 提取特征水平拼接，并导出为 CSV"""
    print("\n" + "=" * 60)
    print("任务5: 特征拼接与 CSV 落盘")
    print("=" * 60)

    # 重置索引，确保水平拼接对齐
    original_reset = original_slice.reset_index(drop=True)
    features_clean = features_df.drop(columns=["_review_index", "_elapsed_sec"], errors="ignore")
    features_reset = features_clean.reset_index(drop=True)

    # 水平拼接
    augmented = pd.concat([original_reset, features_reset], axis=1)

    print("\n>>> 拼接后的宽表结构 <<<")
    print(f"列名: {augmented.columns.tolist()}")
    print(f"行数: {len(augmented)}")
    print(f"\n{augmented.to_string(max_colwidth=40)}")

    # 持久化落盘
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    csv_path = os.path.join(OUTPUT_DIR, "augmented_reviews_sample.csv")
    augmented.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"\n✓ 特征拼接完成，已保存至: {csv_path}")
    print("  编码: utf-8-sig (兼容 Excel / WPS 直接打开)")

    return augmented


# ============================================================
# 命令行参数
# ============================================================
def parse_args():
    parser = argparse.ArgumentParser(
        description="实验九：大模型 API 接入与非结构化特征提取",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python run_pipeline.py                           # 默认: DeepSeek 模型, 5条测试
  python run_pipeline.py --model B                 # A/B 测试: 切换至 Qwen 小模型
  python run_pipeline.py --start 200 --num 10      # 自定义切片区间
  python run_pipeline.py --start 0 --num 1         # 仅单条调试
        """,
    )
    parser.add_argument("--model", type=str, default="A", choices=["A", "B"],
                        help="模型选择: A=DeepSeek-V4-Flash (默认), B=Qwen3.5-4B")
    parser.add_argument("--start", type=int, default=0,
                        help="数据集切片起始索引 (默认: 0)")
    parser.add_argument("--num", type=int, default=5,
                        help="处理的评论条数 (默认: 5)")
    return parser.parse_args()


# ============================================================
# 主流程
# ============================================================
def main():
    args = parse_args()
    model_config = MODEL_CONFIG[args.model]
    model_name = model_config["name"]
    model_label = model_config["label"]

    print("=" * 60)
    print("实验九：大模型 API 接入与非结构化特征提取")
    print(f"模型: {model_label}")
    print(f"Base URL: {BASE_URL}")
    print("=" * 60)

    # ── 初始化客户端 ──
    client = init_client()
    print(f"✓ 客户端初始化成功 (base_url={BASE_URL})")

    # ── 任务2: API 连通性测试 ──
    test_connectivity(client, model_name)

    # ── 任务3: 单条解析闭环验证 ──
    test_single_extraction(client, model_name)

    # ── 任务4: 小批量处理 + DataFrame 重构 + 时耗统计 ──
    features_df, total_time, review_slice = batch_process_reviews(
        client, model_name,
        start_idx=args.start,
        num_reviews=args.num,
    )

    # ── 任务5: 特征拼接与 CSV 落盘 ──
    merge_and_save(review_slice, features_df, model_label)

    # ── 汇总输出 ──
    print("\n" + "=" * 60)
    print("全部任务完成")
    print("=" * 60)
    print(f"  模型:           {model_label}")
    print(f"  处理条数:       {args.num}")
    print(f"  同步串行总耗时:  {total_time:.2f} 秒")
    print(f"  平均单条耗时:    {total_time / args.num:.2f} 秒/条")
    print(f"  输出文件:        {os.path.join(OUTPUT_DIR, 'augmented_reviews_sample.csv')}")
    print("=" * 60)


if __name__ == "__main__":
    main()
