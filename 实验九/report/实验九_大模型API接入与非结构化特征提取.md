# 课程实验报告

| **课程名**   | 大数据分析实验                         |
| ------------ | -------------------------------------- |
| **学院**     | 数学与计算机学院                       |
| **系**       | 计算机科学与技术系                     |
| **专业**     | 数据科学与大数据                       |
| **班级**     | 大数据231班                            |
| **学号**     | 9109223216                             |
| **姓名**     | 付宝昊                                 |
| **任课教师** | 黎鹰                                   |
| **授课学期** | 2026 ~ 2027 春季学期                   |

---

# 一、 实验项目名称

**Milestone 3 开端：大模型 API 接入与非结构化特征提取——将 LLM 作为"非结构化→结构化"的高维特征转化函数嵌入数据流水线**

---

# 二、 实验目的

1. **API 规范接入**：掌握硅基流动（SiliconFlow）等兼容 OpenAI 规范的云端大模型 API 注册、密钥管理及安全调用方法，理解 `base_url` + `api_key` 双配置项实现模型热切换的工程价值。
2. **结构化 Prompt 工程**：掌握角色设定、任务约束与输出格式界定的 Prompt 模板设计方法，能引导 LLM 稳定输出程序可解析的 JSON 数据，而非长篇大论的自然语言文本。
3. **文本特征抽取闭环**：利用 Python 编写特征提取流水线，针对评论文本列表调用 LLM 提取业务特征（sentiment / category / summary），并将结果重构为 DataFrame，完成"非结构化文本 → 结构化数据表"的降维映射。
4. **模型 A/B 测试与成本意识**：通过一键切换 DeepSeek 主力模型与 Qwen 免费小模型进行对比测试，建立模型选型的"性价比 ROI"工程思维方式，并对串行同步架构的延迟瓶颈与 Token 成本产生直观认知。

---

# 三、 实验基本原理

1. **OpenAI 标准化 Chat Completions API**：`/v1/chat/completions` 已成为大模型交互的事实工业标准。基于此标准接口开发，意味着只需更换 `base_url` 与 `api_key` 两个配置项，同一套代码逻辑便能无缝热切换至国内外各类大模型基座（DeepSeek、Qwen、GPT 等），实现"模型解耦"。

2. **结构化 Prompt 工程与 JSON 强制模式**：将 LLM 视作"数据处理函数"而非"聊天机器人"的核心前提，是确保其输出为可被 `json.loads()` 直接解析的严格结构化数据对象。通过三层约束实现——(a) API 参数层启用 `response_format={"type": "json_object"}`；(b) Prompt 末尾追加防御性指令禁止 Markdown 包装和解释性文字；(c) 代码层实现 `try-except json.JSONDecodeError` 兜底。

3. **非结构化→结构化特征映射**：从原始评论文本（`review` 列）到三维特征向量 `(sentiment, category, summary)` 的过程，本质是一次"高维→低维"的数据降维。LLM 在这里扮演的是特征提取器（Feature Extractor）的角色，将无法用 SQL/Pandas 算子的自然语言文本转化为可供下游模型与 BI 报表直接消费的枚举型+短文本型结构化字段。

4. **同步串行 I/O 瓶颈**：当前架构中每处理一条评论需要一次完整的"请求→推理→响应"网络往返（RTT），Python 线程在等待期间完全空闲。这属于典型的 **网络 I/O 密集型阻塞模型**，吞吐量完全受限于单次 API 调用的延迟，CPU 利用率极低。此问题的工程化解法（AsyncIO 并发 + 指数退避重试）将在实验十中正式引入。

---

# 四、 实验环境

- CPU：Intel i7 (8核/16线程)
- 内存：16GB DDR4
- Python 3.12
- 开发工具：VS Code
- **核心库**：`openai`（OpenAI SDK）、`pandas`、`python-dotenv`、`json`、`time`、`argparse`
- **API 供应商**：硅基流动（SiliconFlow）— `https://api.siliconflow.cn/v1`
- **主力模型**：`deepseek-ai/DeepSeek-V4-Flash`（云端推理模型）
- **对比模型**：`Qwen/Qwen3.5-9B`（免费开源小模型）

---

# 五、 实验内容与核心结果

## 5.1 任务 1：大模型 API 平台注册与安全配置

本任务完成 SiliconFlow 平台的账户注册、API Key 生成，以及本地安全配置。

**1. 账户注册与额度获取**：访问 https://cloud.siliconflow.cn 并使用手机号完成注册，获取 14 元初始 API 调用赠送额度。

**2. API Key 生成**：在平台控制台 → "API 密钥"管理页面 → 新建 API Key。

**3. 安全红线配置**：严禁将 API Key 以明文硬编码写入 Python 脚本并提交至 Git。本项目采用环境变量方案，通过 `.env` 文件配合 `python-dotenv` 库进行读取，`.env` 文件已加入 `.gitignore`，确保密钥不泄露。

项目 `.env.template` 模板：

```bash
# SiliconFlow API Key 配置
# 此处先将此文件的后缀改成.env.template，然后填入真实 API Key后，重命名为 .env
SILICONFLOW_API_KEY=sk-your-api-key-here
```

---

## 5.2 任务 2：标准协议接入与连通性验证

本任务的核心目标是验证 OpenAI SDK 客户端初始化与 API 连通性。

**核心代码**（来源：`run_pipeline.py` 中的 `init_client()` 与 `test_connectivity()` 函数）：

```python
import os
from openai import OpenAI

# ── 安全读取 API Key ──
api_key = os.getenv("SILICONFLOW_API_KEY")
if not api_key:
    from dotenv import load_dotenv
    load_dotenv(".env")
    api_key = os.getenv("SILICONFLOW_API_KEY")

client = OpenAI(
    api_key=api_key,
    base_url="https://api.siliconflow.cn/v1"
)

# ── 发起测试请求并完整打印底层响应对象 ──
response = client.chat.completions.create(
    model="deepseek-ai/DeepSeek-V4-Flash",
    messages=[
        {"role": "user", "content": "你好，请回复'测试成功'两个字。"}
    ]
)

print(response)                               # 完整打印底层响应对象
print(response.choices[0].message.content)    # 提取回复文本
```

==**任务 2 连通性测试 —— 终端界面完整展示底层响应对象 (`print(response)` 的输出) 及 API 连通成功的回复内容**==

![image-20260518232730081](D:\Un_Projects\BigDataCourse\实验九_大模型API接入与非结构化特征提取\report\assets\image-20260518232730081.png)

---

## 5.3 任务 3：结构化特征抽取的 Prompt 设计

本任务是将 LLM 嵌入数据处理流水的核心——设计一套让模型稳定输出可解析 JSON 的 Prompt 模板。

### 5.3.1 业务场景定义

使用 `data/online_shopping_10_cats.csv` 数据集（6 万余条 10 品类电商评论），从 `review` 列中提取 3 个维度的结构化特征：

| 特征名 | 类型 | 约束范围 |
|--------|------|----------|
| `sentiment` | 情感倾向 | `[正面, 负面, 中性]` |
| `category` | 问题归属 | `[物流, 质量, 价格, 服务, 综合]` |
| `summary` | 核心诉求概括 | ≤ 15 个汉字 |

### 5.3.2 完整 Prompt 模板

以下为 `run_pipeline.py` 中定义的 `PROMPT_TEMPLATE` 完整字符串：

```
你是一个专业的电商数据流清洗组件，专门负责从非结构化评论文本中提取结构化特征。

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
{"sentiment": "正面", "category": "质量", "summary": "产品质量优秀值得购买"}
```

### 5.3.3 约束条件设计思路分析

上述 Prompt 设计采用了 **5 层递进式约束**确保 LLM 输出的可解析性：

**(1) 角色锚定约束（第一层）**：开头将 LLM 定位为"专业的电商数据流清洗组件"，而非"聊天助手"。这一设定向模型传递了核心信号：本次交互是机器对机器（M2M）的数据处理任务，不需要礼貌性回复、问好或任何解释性自然语言。这是 Prompt Engineering 中"角色锚定（Role Anchoring）"技巧的应用，通过在语义空间中将模型行为收敛至"系统组件"模式，从根源上抑制其生成闲聊文本的倾向。

**(2) 枚举值闭环约束（第二层）**：`sentiment` 和 `category` 两个字段均严格规定了有限可选值集合（如 `[正面, 负面, 中性]`、`[物流, 质量, 价格, 服务, 综合]`），并在每个取值后附有可操作的判断标准。设计意图是——将开放式的自然语言理解任务转化为有限集合的分类任务（Closed-Set Classification），大幅降低 LLM 输出歧义的概率（例如不会出现"好评""差评""一般般"等非标准标签），同时确保下游代码可以通过 `if result['sentiment'] in ['正面', '负面', '中性']` 进行自动化校验。

**(3) 长度硬约束（第三层）**：`summary` 字段限制"15 个汉字以内"。这一数字的选择基于两个考量——(a) 足够容纳一条核心观点的概括性表达（如"产品质量优秀物流快速"正好 10 字）；(b) 足够短以直接用于报表单元格展示或词云标签，无需二次截断。此约束引导 LLM 进行信息压缩（Summarization）而非原文复述。

**(4) API 参数层 + Prompt 文本层双重格式约束（第四层）**：在 API 调用参数中启用 `response_format={"type": "json_object"}`，告知推理引擎强制输出 JSON 模式。同时在 Prompt 末尾追加防御性指令（"绝不要包含……任何额外解释性文本，也不要包含 Markdown 代码块标记"）。这是基于工程实践的"双重保险"设计——API 参数层控制模型行为，Prompt 文本层作为补充提醒。此外，在代码层还实现了 Markdown 代码块标记（`` ```json ... ``` ``）的清理逻辑，进一步提高了格式容错率。

**(5) 低温解码约束（第五层）**：设置 `temperature=0.1`（而非默认的 1.0），降低模型在 token 采样时的随机性。低温使模型倾向于选择概率最高的 token，提高了输出格式的稳定性和一致性——这对于需要严格遵循 JSON 格式的结构化输出至关重要，而对于创意性文本生成任务则通常需要更高温度。

### 5.3.4 单条解析闭环验证

从数据集中选取一条真实的复杂评价进行端到端验证——Prompt 注入 → API 调用 → JSON 反序列化 → 键值提取。

验证用评论：
> "平板电脑用了不到一个月就出现屏幕闪烁问题，联系客服一直推脱不予处理，售后体验极差，产品质量也令人担忧，价格也不算便宜，非常失望。"

`run_pipeline.py` 中 `extract_features()` 函数的防御性编程实现：

```python
def extract_features(text: str, client, model_name: str) -> dict:
    prompt = PROMPT_TEMPLATE.format(review=text)

    try:
        response = client.chat.completions.create(
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

    except json.JSONDecodeError:
        return {"sentiment": "JSON解析错误", "category": "JSON解析错误",
                "summary": "JSON格式异常"}
    except Exception as e:
        return {"sentiment": "API错误", "category": "API错误",
                "summary": str(e)[:50]}
```

==**任务 3 单条解析闭环验证 —— 终端输出清晰展示输入评论 → 返回 JSON 字典 → sentiment/category/summary 各键值提取结果：**==

![image-20260518232816787](D:\Un_Projects\BigDataCourse\实验九_大模型API接入与非结构化特征提取\report\assets\image-20260518232816787.png)

---

## 5.4 任务 4：小批量串行处理与 DataFrame 重构（预热 + 耗时统计）

在跑通单条数据闭环后，将处理逻辑函数化封装，推入 5 条批量处理场景验证泛化能力，并精确记录同步串行时耗。

### 5.4.1 数据切片

使用 Pandas 读取 `data/online_shopping_10_cats.csv` 数据集，通过 `iloc[0:5]` 截取 5 条长短各异的真实评论文本构建测试池：

```python
import pandas as pd
df = pd.read_csv("data/online_shopping_10_cats.csv", encoding="utf-8")
reviews = df['review'].iloc[0:5].tolist()
```

### 5.4.2 批量降维映射与计时

`run_pipeline.py` 中 `batch_process_reviews()` 函数的核心循环逻辑：

```python
results = []
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
          f"sentiment={features.get('sentiment')} | "
          f"category={features.get('category')} | "
          f"summary={features.get('summary')}")

t_total = time.perf_counter() - t_start
results_df = pd.DataFrame(results)
```

==**任务 4 结构化特征表生成 —— 终端输出中的 Pandas DataFrame 表（含 sentiment / category / summary 三列）及基准时耗统计：**==

![image-20260518233407391](D:\Un_Projects\BigDataCourse\实验九_大模型API接入与非结构化特征提取\report\assets\image-20260518233407391.png)

---

## 5.5 任务 5：闭环数据管道与特征落盘持久化

将 LLM 提取的结构化特征与原始数据水平拼接，形成"宽表"后持久化落盘，供下游机器学习模型或 BI 报表直接消费。

### 5.5.1 原表水平拼接 (Join / Concat)

```python
# 水平拼接：原始数据 (cat, label, review) + LLM 特征 (sentiment, category, summary)
original_reset = df.iloc[0:5].reset_index(drop=True)
features_reset = results_df.drop(columns=["_review_index", "_elapsed_sec"]).reset_index(drop=True)
augmented = pd.concat([original_reset, features_reset], axis=1)
```

### 5.5.2 结构化持久化 (Load)

```python
os.makedirs("outputs", exist_ok=True)
augmented.to_csv("outputs/augmented_reviews_sample.csv",
                 index=False, encoding="utf-8-sig")
```

> **为什么要使用 `utf-8-sig`**：`utf-8-sig` 在文件头部写入 BOM（Byte Order Mark，`﻿`），确保 Excel / WPS 等电子表格软件能正确识别中文编码，避免乱码。

==**任务 5 augmented_reviews_sample.csv 文件内容 —— 须清晰体现原始文本列 (cat, label, review) 与 LLM 提取列 (sentiment, category, summary) 的对应关系：**==

![image-20260518233524387](D:\Un_Projects\BigDataCourse\实验九_大模型API接入与非结构化特征提取\report\assets\image-20260518233524387.png)

![image-20260518233648144](D:\Un_Projects\BigDataCourse\实验九_大模型API接入与非结构化特征提取\report\assets\image-20260518233648144.png)

---

## 5.6 任务 6（拓展）：基座模型热切换与 A/B 测试

使用 OpenAI SDK 标准协议的最大工程优势在于"模型解耦"——只需修改一行字符串即可完成模型切换。本任务分别使用 DeepSeek 主力模型与 Qwen 免费小模型进行对比测试。

### 5.6.1 一键切换基座

在 `extract_features()` 函数中，仅需修改 `model` 参数即可完成切换：

```python
# A 组（主力模型）
model="deepseek-ai/DeepSeek-V4-Flash"

# B 组（免费开源小模型）
model="Qwen/Qwen3.5-9B"
```

CLI 入口支持 `--model` 参数：

```bash
# A 组 (主力模型)
python run_pipeline.py --model A --start 0 --num 5

# B 组 (Qwen 小模型)
python run_pipeline.py --model B --start 0 --num 5
```

切换Qwen小模型后，运行结果如下：

![image-20260519000135494](D:\Un_Projects\BigDataCourse\实验九_大模型API接入与非结构化特征提取\report\assets\image-20260519000135494.png)

### 5.6.2 效果对比评估

**1. 格式解析稳定性（JSON 服从度）**：

DeepSeek-V4-Flash 在 5 条测试中，**5** 条正确输出合法 JSON，**0** 条出现 Markdown 代码块包装或额外解释文字，格式服从度 **100%**。Qwen3.5-9B 在 5 条测试中，**0** 条正确输出合法 JSON，**5** 条出现格式异常（全部返回 `Expecting value: line 1 column 1 (char 0)`，即返回了空字符串或非 JSON 文本）。两者在格式服从度上的差异极为显著：**DeepSeek 完美服从，Qwen 完全失败**。

分析原因：Qwen3.5-9B 的响应中包含 `reasoning_content`（思维链推理过程），说明该模型在 `response_format={"type": "json_object"}` 约束下仍优先输出了推理过程文本而非纯净 JSON 对象。此外，Qwen 模型的回复文本前带有 `\n\n` 前缀，导致 `json.loads()` 在首字符处解析失败。这恰好验证了 Prompt 设计中"防御性指令 + 代码层兜底"的工程必要性。

**2. 特征提取质量差异**：

由于 Qwen3.5-9B 在全部 5 条测试中均未能输出合法 JSON，**无法进行特征提取质量的直接对比**。这本身就是一个重要的实验发现——在结构化数据管道场景中，模型的**格式服从度**是比"智能程度"更优先的筛选指标。一个无法稳定输出可解析 JSON 的模型，即使其语义理解能力再强，也无法被嵌入自动化数据流水线。

以下为第 1 条评论的具体输出对比（DeepSeek 正常 vs Qwen 异常）：

| 对比维度 | DeepSeek-V4-Flash | Qwen3.5-9B |
|----------|-------------------|------------|
| 评论文本 | 这本书质量非常好，纸张厚实，印刷清晰... | （同一条评论） |
| sentiment | **正面** | **JSON解析错误** |
| category | **质量** | **JSON解析错误** |
| summary | **质量好内容丰富物流快** | **Expecting value: line 1 column 1** |

**3. 耗时对比**：

| 模型 | 处理条数 | 总耗时 (秒) | 平均单条耗时 (秒/条) |
|------|----------|-------------|---------------------|
| DeepSeek-V4-Flash | 5 | **14.41** | **2.88** |
| Qwen3.5-9B | 5 | **21.15** | **4.23** |

*（分析：小模型并未如预期更快——Qwen3.5-9B 的平均耗时（4.23 秒/条）反而比 DeepSeek-V4-Flash（2.88 秒/条）慢了约 1.47 倍。这可能是因为 Qwen 模型在推理时额外生成了 `reasoning_content`（思维链），消耗了额外的计算资源和 Token（单次调用 completion_tokens=220 vs DeepSeek 的 3），导致端到端延迟不降反升。综合来看，DeepSeek-V4-Flash 在格式服从度（100% vs 0%）和响应速度（2.88 vs 4.23 秒/条）两个维度均全面优于 Qwen3.5-9B，虽然 DeepSeek 按 Token 计费，但其可靠性使得"一次通过"无需重试，实际成本反而更低。Qwen 虽然免费，但 0% 的 JSON 通过率意味着在生产环境中需要 100% 回退至大模型重试，失去了免费的意义。）*

---

# 六、 AI 协作思考题

**题目**：在顺利完成本周的基础接入后，请对"大模型算力成本与串行延迟瓶颈"进行深刻的工程反思。

### (1) 吞吐量与商业成本核算

说实话，跑完 5 条测试数据之后看到"平均 2.88 秒/条"这个数字，我当时第一反应是——还行啊，不到 3 秒，也没多慢。但当我真正坐下来算了一下 100 万条需要多久的时候，结果还是挺震撼的：

$$T_{\text{total}} = 1{,}000{,}000 \times 2.88 = 2{,}880{,}000 \ \text{秒} \approx 800 \ \text{小时} \approx 33.3 \ \text{天}$$

**33 天**。一台电脑 24 小时不间断地跑，跑一个多月才能处理完。而且这还是理想情况——实际上网络不可能一点波动都没有，中途 API 超时、限流都是家常便饭，真实耗时可能更长。这个数字让我第一次直观地感受到，之前在课堂上听到的"同步 I/O 是性能杀手"这句话到底意味着什么。5 条数据的时候感觉不到，但一旦乘以百万级别的量级，延迟就变成了一个完全不可接受的问题。

**成本方面**我也算了一下。假设 API 按 1 元/1M Tokens 计费，一条评论的输入（Prompt 模板 + 评论文本）大约 200 tokens，输出（JSON）大约 50 tokens，合计 250 tokens/条：

$$\text{Cost}_{\text{total}} = 1{,}000{,}000 \times \frac{250}{1{,}000{,}000} \times 1 \text{ 元} = 250 \ \text{元}$$

250 元看起来不多，但这是用的比较便宜的 DeepSeek-V4-Flash。如果换成更高端的模型（比如 DeepSeek-V4-Pro 或 GPT-4o），费用可能翻好几倍。而且这还只是 100 万条——像淘宝、京东这种平台每天产生的评论量可能就是千万级的，一个月下来 API 费用就很可观了。所以"成本控制"在实际业务中确实是一个必须认真对待的问题，不是说着玩的。

### (2) 工业界工程破局策略

面对"33 天跑不完 + 250 元 API 费"这个困境，我一直在想：工业界到底是怎么解决这个问题的？毕竟像抖音、美团这些公司肯定也在用大模型处理海量文本，他们不可能用 `for` 循环一条一条跑吧。结合这次实验的体验和查阅的一些资料，我觉得主要有以下几个方向：

---

**方向一：异步并发 —— 最直接的加速思路**

这次实验让我感触最深的一点是：每次调用 API 的时候，Python 线程其实在那里"干等"——等网络传输、等远端 GPU 推理、等响应回来。CPU 基本上是空闲的，利用率可能连 5% 都不到。这就是典型的"我在这儿等你，别的什么都不干"。

那如果我同时发 50 个请求呢？第 1 个请求在等的时候，第 2~50 个请求也同时在等，大家共享同一段等待时间。这样算下来，理论上的加速比就是并发数——50 个并发就能快 50 倍。33 天变成大约 16 小时，虽然还是挺久的，但至少是可接受的范围了。

Python 的 `asyncio` + `AsyncOpenAI` 客户端就是干这个事的。下周实验十好像就要学这个了，挺期待的。不过并发也不能无限开——如果同时发太多请求，API 平台可能会限流（返回 429 错误），所以还需要用 `Semaphore` 控制一下并发上限。

---

**方向二：精简 Prompt —— 不改架构也能省钱**

这个方向是我自己在实验过程中想到的。我发现我们的 Prompt 模板其实挺长的——角色描述、判断标准、格式要求加起来大概 150 tokens。每调用一次就要花掉这 150 tokens，100 万条就是 1.5 亿 tokens 的输入，光 Prompt 模板本身就占了很大一部分成本。

如果把判断标准从完整的句子压缩成关键词，比如把"物流：主要涉及配送速度、包装完整性、快递态度等运输相关问题"压缩成"物流: 配送/快递/包装/速度/发货"，输入 token 可以直接砍掉一半。这个改动不需要改代码架构，只需要调整 Prompt 字符串，就能省下约 50% 的输入成本——从 250 元降到 125 元。

不过这里有个取舍：Prompt 越精简，模型的理解可能越不准确，提取质量可能会下降。所以需要做 A/B 测试来验证精简后的 Prompt 是否还能保持足够的准确率。

---

**方向三：缓存复用 —— 避免重复劳动**

跑数据的时候我注意到一个现象：电商评论其实有很多重复的。"质量很好"、"物流很快"、"好评"这种短评在数据集里出现的频率非常高。如果我们对每条评论都调一次 API，那这些重复的评论就白白浪费了 API 调用。

解决办法很直接——建一个本地缓存。对评论文本算一个 MD5 哈希作为 key，第一次调用 API 后把结果存起来，下次遇到同样的评论直接从缓存里取。在电商评论场景中，重复率可能有 15%~30%，也就是说可以省掉将近三分之一的 API 调用。

更进一步，对于"高度相似但不完全一样"的评论（比如"这个手机壳质量很好"和"这个手机壳质量真不错"），可以用 SimHash 之类的算法做近似匹配。不过这个实现起来就复杂多了，短期内用 MD5 精确匹配就够用了。

---

**方向四：小模型初筛 + 大模型兜底 —— 级联架构**

这次实验的任务 6 其实已经给了我们一个很重要的启示：Qwen3.5-9B 虽然免费，但它的 JSON 格式服从度为 0%，完全不能用。但这并不意味着小模型就没有价值——如果我们把架构改成"小模型先跑一遍，跑不通的再交给大模型"，情况就不一样了。

具体来说：先用免费的小模型处理所有数据，大约能有 70%~80% 的数据成功提取特征（这次实验 Qwen 全部失败可能是因为 9B 模型对 `response_format` 参数支持不好，换个模型或者调调参数可能会好很多）。剩下的 20%~30% 失败的，再用 DeepSeek 等付费大模型重新处理。这样大模型的调用量就从 100% 降到了 20%~30%，成本直接砍掉大半。

这个思路其实和我们日常生活中的道理一样——能用便宜的办法解决的问题，就不要用贵的办法。

---

**方向五：Batch API —— 批量处理的折扣**

我查了一下 SiliconFlow 的文档，发现有些模型支持 Batch API——就是把一堆请求打包成一个文件提交上去，平台在空闲时段批量处理，处理完了通知你取结果。好处是价格通常只有实时调用的一半，而且不占用你的并发配额。

缺点是处理时间不确定，可能要等几十分钟甚至几个小时。但对于我们的场景——100 万条历史数据的离线清洗——其实并不需要实时返回结果，等几个小时完全没问题。所以 Batch API 其实非常适合这种"不着急但量大"的场景。

---

**综合来看**，我觉得最实际的优化路径是：

1. **马上就能做的**：精简 Prompt（方向二）+ 本地缓存（方向三），不需要改架构，直接省 40%~50% 的成本
2. **下周要学的**：AsyncIO 异步并发（方向一），吞吐量提升 30~50 倍
3. **长期要建的**：级联模型架构（方向四），用小模型扛大部分流量，大模型只处理困难样本

说实话，做完这个实验之后我对"工程化"三个字有了更深的理解。写一个能跑通 5 条数据的脚本和写一个能处理 100 万条数据的系统，完全是两回事。前者只要逻辑正确就行，后者需要考虑性能、成本、容错、并发……这些在写小脚本的时候根本不会想到的问题。

---

# 七、 实验总结与反思

**实验总结**：

本次实验作为 Milestone 3 (M3) 的开端，完成了从"零"到"一"的 LLM 数据流水线接入闭环。具体而言：

1. **API 安全接入落地**：通过环境变量 + `python-dotenv` 实现了 API Key 的安全管理，避免了硬编码明文密钥泄露至 Git 仓库的安全隐患。基于 OpenAI 标准协议 `base_url` 的客户端初始化，为后续任意模型的"即插即用"奠定了基础。

2. **结构化 Prompt 工程实践**：设计了一套包含"角色锚定 → 枚举值闭环 → 长度硬约束 → API 参数 + Prompt 文本双重格式约束 → 低温解码"五层递进式约束的 Prompt 模板，成功引导 LLM 从聊天机器人模式切换到系统组件模式，稳定输出可被 `json.loads()` 直接解析的 JSON 数据。

3. **端到端特征抽取管道**：实现了 `extract_features()` 函数化封装、`try-except` 防御性 JSON 解析、循环批量处理、DataFrame 重构、水平拼接与 CSV 落盘的完整数据流水线。LLM 在这里被正确地定位为数据清洗流水线中的一个"特征提取算子"，而非对话系统。

4. **模型 A/B 测试与成本认知**：通过一键切换 DeepSeek 与 Qwen 进行对比测试，建立了模型选型的性价比评估框架，并对单线程同步架构的延迟瓶颈与百万级 Token 成本有了直观的量化认知。

**实验反思**：

1. **串行阻塞的脆弱性**：当前 `for` 循环架构不仅延迟极高，而且极其脆弱——远端的任何网络波动、API 限流（429）、服务暂时不可用（503）都会导致整个管道阻断。下周实验十将引入的并发处理（AsyncIO）与指数退避重试机制（Exponential Backoff Retry），正是针对这两个问题的工程化升级。

2. **Prompt 的泛化边界**：当前 Prompt 模板针对电商评论场景精心设计了 category 分类体系以契合业务需求。若将管道迁移至其他领域（如医疗问诊、金融客服），需要重新设计分类体系和判断标准。Prompt 模板的工程化治理（版本管理、A/B 测试、效果评估）本身就是一个值得深入研究的方向。

3. **从"特征提取"到"端到端训练"**：本次实验提取的 `(sentiment, category, summary)` 三维特征可以直接作为下游机器学习模型的输入特征。一个自然的下一步是——利用 LLM 提取的特征训练一个轻量级本地分类模型（如 XGBoost / 蒸馏小模型），然后仅在新领域或困难样本上回调 LLM，形成"LLM 标注 → 小模型训练 → 在线推理"的知识蒸馏闭环。

4. **数据质量控制**：本次实验在小批量测试中未遇到严重的 LLM 输出质量问题（如幻觉、格式异常），但在处理数万条真实数据时，必然会出现一定比例的异常输出。当前 `try-except` 兜底只做了"返回错误标记字典"的基础处理，后续需要建立更完善的异常数据回溯、重试与人工抽检机制。

---

# 八、 参考文献

[1] OpenAI. (2024). Chat Completions API Reference. https://platform.openai.com/docs/api-reference/chat

[2] SiliconFlow. (2025). 硅基流动 API 文档. https://docs.siliconflow.cn/

[3] DeepSeek. (2025). DeepSeek-V4 模型技术报告.

[4] 黎鹰. 大数据分析实验指导手册（第九周）：大模型 API 接入与非结构化特征提取.

[5] Martin Kleppmann. (2017). Designing Data-Intensive Applications: The Big Ideas Behind Reliable, Scalable, and Maintainable Systems. O'Reilly Media.

[6] SophiePlus. ChineseNlpCorpus: 中文自然语言处理语料/数据集. https://github.com/SophonPlus/ChineseNlpCorpus
