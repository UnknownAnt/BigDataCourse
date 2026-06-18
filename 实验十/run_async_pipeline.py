#!/usr/bin/env python3
"""
run_async_pipeline.py —— 实验十：高并发管道与容错机制

覆盖任务：
  任务1: 异步客户端改造（AsyncOpenAI）
  任务2: @retry 指数退避重试装饰器
  任务3: Semaphore 并发度控制
  任务4: 并发批量处理 1000 条真实数据 + tqdm 进度条 + 耗时统计
  任务5: 结果落盘 batch_1000_features.csv

用法:
  python run_async_pipeline.py                        # 默认: 1000条, 并发20
  python run_async_pipeline.py --num 100 --concurrency 10
  python run_async_pipeline.py --num 1000 --concurrency 20
"""

import argparse
import asyncio
import json
import os
import sys
import time

import pandas as pd
from openai import AsyncOpenAI
from tqdm.asyncio import tqdm_asyncio
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

# ============================================================
# 全局配置
# ============================================================
BASE_URL = "https://api.siliconflow.cn/v1"
MODEL_NAME = "deepseek-ai/DeepSeek-V4-Flash"

DATA_PATH = os.path.join(os.path.dirname(__file__),
                         "..", "实验九",
                         "data", "online_shopping_10_cats.csv")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "outputs")


# ============================================================
# Prompt 模板 (复用实验九)
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
# 客户端初始化 (异步版)
# ============================================================
def init_async_client() -> AsyncOpenAI:
    """安全读取 API Key 并初始化 AsyncOpenAI 客户端"""
    api_key = os.getenv("SILICONFLOW_API_KEY")
    if not api_key:
        try:
            from dotenv import load_dotenv
            env_path = os.path.join(os.path.dirname(__file__),
                                    "..", "实验九", ".env")
            load_dotenv(env_path)
            api_key = os.getenv("SILICONFLOW_API_KEY")
        except ImportError:
            pass

    if not api_key:
        print("未检测到 SILICONFLOW_API_KEY 环境变量")
        api_key = input("请输入你的 SiliconFlow API Key: ").strip()
        if not api_key:
            raise RuntimeError("必须提供有效的 API Key。")

    return AsyncOpenAI(api_key=api_key, base_url=BASE_URL)


# ============================================================
# 核心: 带 @retry 的异步特征抽取函数
# ============================================================
@retry(
    retry=retry_if_exception_type((Exception,)),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(5),
    reraise=True,
)
async def extract_features_async(
    text: str,
    client: AsyncOpenAI,
    model_name: str,
    semaphore: asyncio.Semaphore,
) -> dict:
    """
    异步版本的特征抽取函数。

    关键改造点：
    1. async/await 替代同步调用，释放事件循环
    2. @retry 装饰器实现指数退避重试 (2s → 4s → 8s → 16s → 32s, 最多5次)
    3. Semaphore 控制并发上限，防止 API 限流
    """
    async with semaphore:
        prompt = PROMPT_TEMPLATE.format(review=text)

        try:
            response = await client.chat.completions.create(
                model=model_name,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
                temperature=0.1,
                max_tokens=256,
            )

            raw_text = response.choices[0].message.content.strip()

            # 防御: 清理可能的 Markdown 代码块标记
            if raw_text.startswith("```"):
                lines = raw_text.split("\n")
                if lines[0].startswith("```"):
                    lines = lines[1:]
                if lines and lines[-1].strip() == "```":
                    lines = lines[:-1]
                raw_text = "\n".join(lines).strip()

            result = json.loads(raw_text)
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
            error_str = str(e)
            # 401/403 认证错误不重试（重试也没用）
            if "401" in error_str or "403" in error_str:
                return {
                    "sentiment": "认证失败",
                    "category": "认证失败",
                    "summary": "API Key 无效，请更新 .env",
                }
            # 429 限流、超时等临时性错误交给 @retry 重试
            if "429" in error_str or "rate" in error_str.lower() or "timeout" in error_str.lower():
                raise
            return {
                "sentiment": "API错误",
                "category": "API错误",
                "summary": error_str[:50],
            }


# ============================================================
# 任务4: 并发批量处理 + tqdm 进度条
# ============================================================
async def batch_process_async(
    client: AsyncOpenAI,
    model_name: str,
    start_idx: int = 0,
    num_reviews: int = 1000,
    concurrency: int = 20,
):
    """
    并发批量处理评论文本。

    核心机制：
    - asyncio.Semaphore(concurrency) 控制并发上限
    - tqdm_asyncio.gather() 带进度条的并发任务收集
    - time.perf_counter() 精确计时
    """
    print("\n" + "=" * 60)
    print(f"任务4: 异步并发处理 ({num_reviews}条)")
    print(f"并发度: {concurrency} | 模型: {model_name}")
    print("=" * 60)

    # 读取数据集
    df = pd.read_csv(DATA_PATH, encoding="utf-8")
    total_rows = len(df)
    print(f"数据集总行数: {total_rows}")

    if start_idx + num_reviews > total_rows:
        num_reviews = total_rows - start_idx
        print(f"自动调整为: start={start_idx}, num={num_reviews}")

    review_slice = df["review"].iloc[start_idx:start_idx + num_reviews]
    reviews = review_slice.tolist()

    # 创建信号量控制并发
    semaphore = asyncio.Semaphore(concurrency)

    print(f"\n开始并发处理 {len(reviews)} 条评论 (并发上限={concurrency})...")
    t_start = time.perf_counter()

    # 创建所有异步任务
    tasks = [
        extract_features_async(review, client, model_name, semaphore)
        for review in reviews
    ]

    # tqdm_asyncio.gather: 带进度条的并发收集
    results_raw = await tqdm_asyncio.gather(*tasks, desc="处理进度")

    t_total = time.perf_counter() - t_start

    # 构建结果 DataFrame
    results = []
    for i, features in enumerate(results_raw):
        features["_review_index"] = i
        results.append(features)

    results_df = pd.DataFrame(results)

    # 统计
    success_count = sum(
        1 for r in results
        if r.get("sentiment") not in ("JSON解析错误", "API错误", "解析失败")
    )
    error_count = len(results) - success_count

    print(f"\n{'='*60}")
    print(f">>> 并发处理统计 <<<")
    print(f"  处理条数:    {len(reviews)}")
    print(f"  成功:        {success_count}")
    print(f"  失败:        {error_count}")
    print(f"  总耗时:      {t_total:.2f} 秒 ({t_total/60:.1f} 分钟)")
    print(f"  平均单条:    {t_total / len(reviews):.3f} 秒/条")
    print(f"  有效吞吐:    {success_count / t_total:.1f} 条/秒")
    print(f"{'='*60}")

    return results_df, t_total, review_slice


# ============================================================
# 任务5: 结果落盘
# ============================================================
def save_results(original_slice, features_df, num_reviews, concurrency, total_time):
    """将原始数据与异步提取的特征拼接后落盘"""
    print("\n" + "=" * 60)
    print("任务5: 结果落盘")
    print("=" * 60)

    original_reset = original_slice.reset_index(drop=True)
    features_clean = features_df.drop(columns=["_review_index"], errors="ignore")
    features_reset = features_clean.reset_index(drop=True)

    augmented = pd.concat([original_reset, features_reset], axis=1)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    csv_path = os.path.join(OUTPUT_DIR, "batch_1000_features.csv")
    try:
        augmented.to_csv(csv_path, index=False, encoding="utf-8-sig")
    except PermissionError:
        csv_path = os.path.join(OUTPUT_DIR, "batch_1000_features_new.csv")
        augmented.to_csv(csv_path, index=False, encoding="utf-8-sig")
        print(f"[提示] 原文件被占用，已保存至新文件名")

    print(f"已保存至: {csv_path}")
    print(f"总行数: {len(augmented)}")
    print(f"列名: {augmented.columns.tolist()}")
    print(f"编码: utf-8-sig (兼容 Excel)")

    # 同时保存一个统计摘要
    summary_path = os.path.join(OUTPUT_DIR, "run_summary.txt")
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write(f"实验十: 高并发管道与容错机制\n")
        f.write(f"{'='*40}\n")
        f.write(f"处理条数:    {num_reviews}\n")
        f.write(f"并发度:      {concurrency}\n")
        f.write(f"总耗时:      {total_time:.2f} 秒 ({total_time/60:.1f} 分钟)\n")
        f.write(f"平均单条:    {total_time / num_reviews:.3f} 秒/条\n")
        f.write(f"有效吞吐:    {num_reviews / total_time:.1f} 条/秒\n")
    print(f"统计摘要: {summary_path}")

    return augmented


# ============================================================
# 命令行参数
# ============================================================
def parse_args():
    parser = argparse.ArgumentParser(
        description="实验十: 高并发管道与容错机制",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python run_async_pipeline.py                          # 默认: 1000条, 并发20
  python run_async_pipeline.py --num 100 --concurrency 10
  python run_async_pipeline.py --num 1000 --concurrency 20
        """,
    )
    parser.add_argument("--start", type=int, default=0,
                        help="数据集切片起始索引 (默认: 0)")
    parser.add_argument("--num", type=int, default=1000,
                        help="处理的评论条数 (默认: 1000)")
    parser.add_argument("--concurrency", type=int, default=20,
                        help="并发上限 (默认: 20)")
    return parser.parse_args()


# ============================================================
# 主流程
# ============================================================
async def main():
    args = parse_args()

    print("=" * 60)
    print("实验十: 高并发管道与容错机制")
    print(f"模型: {MODEL_NAME}")
    print(f"Base URL: {BASE_URL}")
    print(f"并发度: {args.concurrency}")
    print("=" * 60)

    # 初始化异步客户端
    client = init_async_client()
    print(f"异步客户端初始化成功")

    # 预检: 测试 API 连通性
    print("\n预检: 测试 API 连通性...")
    try:
        test_resp = await client.chat.completions.create(
            model=MODEL_NAME,
            messages=[{"role": "user", "content": "回复OK"}],
            max_tokens=5,
        )
        print(f"API 连通性测试通过: {test_resp.choices[0].message.content.strip()}")
    except Exception as e:
        print(f"API 连通性测试失败: {e}")
        print("请检查 .env 文件中的 SILICONFLOW_API_KEY 是否有效。")
        print("获取新 Key: https://cloud.siliconflow.cn")
        return

    # 并发批量处理
    features_df, total_time, review_slice = await batch_process_async(
        client, MODEL_NAME,
        start_idx=args.start,
        num_reviews=args.num,
        concurrency=args.concurrency,
    )

    # 结果落盘
    save_results(review_slice, features_df, args.num, args.concurrency, total_time)

    # 汇总
    print("\n" + "=" * 60)
    print("全部任务完成")
    print("=" * 60)
    print(f"  处理条数:       {args.num}")
    print(f"  并发度:         {args.concurrency}")
    print(f"  异步并发总耗时: {total_time:.2f} 秒 ({total_time/60:.1f} 分钟)")
    print(f"  平均单条耗时:   {total_time / args.num:.3f} 秒/条")
    print(f"  输出文件:       {os.path.join(OUTPUT_DIR, 'batch_1000_features.csv')}")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
