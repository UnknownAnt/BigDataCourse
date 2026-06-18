import os
import time
import logging
import hashlib
from typing import Dict, Any, Optional

import polars as pl


class MlDataPipelineOpt:
    """
    基于 Polars 的标准数据处理流水线 (优化版)。
    
    采用 Lazy API 应对亿级数据。
    本版本引入两大优化点：
    1. Transform 阶段中，将链式的正则替换合并为带有字典映射条件的 replace_strict。
    2. Load 阶段针对物理分区生成中的 "计算冗余"（同一 LazyGraph 会因 Partition 的数量而重复多倍耗时计算）
       提供先暂存统一计算中间盘、再做低成本廉价拆分写出的解决方案。
    同时，该版本严格补充了 PEP 8 风格的 Type Hinting 及文档注释。
    """
    
    def __init__(self, input_path: str, output_path: str, config: Optional[Dict[str, Any]] = None) -> None:
        """
        初始化大容量数据处理管线实例。
        
        Args:
            input_path (str): 待处理的高维度大文件路径 (支持 .csv / .parquet)
            output_path (str): 模型规整后存储的目标地址 (文件目录 / 文件名)
            config (Optional[Dict[str, Any]]): 含有各类规则字典的参数传递实体
        """
        self.input_path: str = input_path
        self.output_path: str = output_path
        self.config: Dict[str, Any] = config or {}
        self.lazy_frame: Optional[pl.LazyFrame] = None
        
        self.logger: logging.Logger = logging.getLogger(self.__class__.__name__)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - [%(levelname)s] - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)

    def extract(self) -> None:
        """
        Phase 1: 数据提取
        负责将指定文件转化为懒加载计算流对象，并应用 Predicate Pushdown 及 Projection 下推的基础。
        """
        self.logger.info(f"==> 开始提取数据 (Extract) 从: {self.input_path}")
        
        if self.input_path.endswith('.csv'):
            has_header: bool = self.config.get('has_header', True)
            self.lazy_frame = pl.scan_csv(self.input_path, has_header=has_header)
            
            # 使用 rename 生成投影。
            # 优化点：Polars 能够无缝将本层以下的过滤法则直接穿透下推给底层的 Scan 动作。
            rename_cols: Optional[Dict[str, str]] = self.config.get('rename_cols')
            if rename_cols:
                self.lazy_frame = self.lazy_frame.rename(rename_cols)
                
        elif self.input_path.endswith('.parquet') or "parquet" in self.input_path:
            self.lazy_frame = pl.scan_parquet(self.input_path)
        else:
            raise ValueError("不支持的文件格式。")
            
        self.logger.info("Extract 阶段已成功完成 (Lazy加载)。")

    def transform(self) -> None:
        """
        Phase 2: 数据核心转换
        负责叠加并生成一系列过滤清洗、加密脱敏的高性能查询图逻辑。
        """
        self.logger.info("==> 开始数据清洗与转换 (Transform)...")
        if self.lazy_frame is None:
            raise ValueError("延迟任务中无实际数据载体")
            
        q: pl.LazyFrame = self.lazy_frame

        # 1. 对异常值和预设区间做过滤截断
        if 'filter_rules' in self.config:
            filter_expr = self.config['filter_rules']
            for expr in filter_expr:
                q = q.filter(expr)

        # 2. 对字典错别字进行标准映射
        if 'typo_corrections' in self.config and 'typo_column' in self.config:
            typo_col: str = self.config['typo_column']
            typos: Dict[str, str] = self.config['typo_corrections']
            # 优化点: 抛弃缓慢由于 Python 解释器内循环衍生的 .replace() 链，
            # 改为向原生方法单次馈入全量 mapping 实现全自动图融合。
            q = q.with_columns(
                pl.col(typo_col).replace(typos, default=pl.col(typo_col)).alias(typo_col)
            )

        # 3. 数据敏感信息算法加密脱敏
        if 'hash_column' in self.config:
            hash_col: str = self.config['hash_column']
            new_hash_col: str = self.config.get('new_hash_column', f"masked_{hash_col}")
            q = q.with_columns([
                pl.col(hash_col).cast(pl.Utf8).map_elements(
                    lambda x: hashlib.sha256(b"null").hexdigest() if x is None else hashlib.sha256(str(x).encode('utf-8')).hexdigest(),
                    return_dtype=pl.Utf8
                ).alias(new_hash_col)
            ])
            if self.config.get('drop_original_hash_column', True):
                q = q.drop([hash_col])

        # 4. 精密业务主键去重
        if 'dedup_subset' in self.config:
            q = q.unique(subset=self.config['dedup_subset'], keep="first")
            
        self.lazy_frame = q
        self.logger.info("Transform 阶段各项规则映射成功。")

    def load(self) -> None:
        """
        Phase 3: 加载数据结果集到物理存储带中
        特别针对存在 partition_col 的应用情况规避冗余的 DAG 计算阻塞。
        """
        self.logger.info(f"==> 开始加载及处理分区分发保存至: {self.output_path}")
        if self.lazy_frame is None:
            raise ValueError("查询图流为空")

        output_dir: str = os.path.dirname(self.output_path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)

        partition_col: Optional[str] = self.config.get('partition_by')
        compression: str = self.config.get('compression', 'snappy')

        if partition_col:
            os.makedirs(self.output_path, exist_ok=True)
            # ======================== 核心性能瓶颈改进 ========================
            # 【过去】：针对 N 个独立行为类分区，以 `filter(xx).sink_parquet` 的方式遍历，结果相当于把 Extract 和 Transform 在 1 亿行上原封不动地跑了 N 遍！这是致命的 OOM 与 性能问题。
            # 【当前】：阻断重复计算网络，我们让这 1 亿行的复杂清洗先统一落盘成一个清爽、高度压缩的 Temporary Parquet，再将它的游标以极低的成本做拆分页写入。
            
            temp_path: str = os.path.join(self.output_path, "_pipeline_global_temp.parquet")
            self.logger.info("优化策略启动：将全量处理逻辑的结果合并输出为统一暂存层，切断循环重计...")
            self.lazy_frame.sink_parquet(temp_path, compression=compression)
            
            self.logger.info("统一计算完成，进入高速低成本分区裂变...")
            temp_lazy: pl.LazyFrame = pl.scan_parquet(temp_path)
            
            unique_vals = temp_lazy.select(pl.col(partition_col).unique()).collect()[partition_col].drop_nulls().to_list()
            
            for val in unique_vals:
                partition_dir: str = os.path.join(self.output_path, f"{partition_col}={val}")
                os.makedirs(partition_dir, exist_ok=True)
                part_file: str = os.path.join(partition_dir, "data.parquet")
                
                temp_lazy.filter(pl.col(partition_col) == val).sink_parquet(part_file, compression=compression)
                
            os.remove(temp_path)
            self.logger.info(f"所有的 {len(unique_vals)} 个物理子分区已极速导出完毕，临时池已销毁。")
        else:
            self.lazy_frame.sink_parquet(self.output_path, compression=compression)

    def run(self) -> None:
        """主入口"""
        start: float = time.time()
        self.extract()
        self.transform()
        self.load()
        self.logger.info(f"==== SUCCESS: 总计耗时 {time.time() - start:.2f} 秒 ====")
