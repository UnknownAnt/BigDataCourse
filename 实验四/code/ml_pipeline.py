import os
import time
import logging
import hashlib
import polars as pl
from typing import Dict, Any


class MlDataPipeline:
    """
    基于 Polars 的标准数据处理流水线。
    包含数据提取 (Extract)、数据转换 (Transform)、数据加载 (Load) 三个阶段。
    
    采用 Lazy API 应对亿级数据，避免内存溢出。
    支持：类型清洗、字段规范化、异常过滤、哈希脱敏、多主键精密去重、按字段分区存储等。
    """
    
    def __init__(self, input_path: str, output_path: str, config: Dict[str, Any] = None):
        self.input_path = input_path
        self.output_path = output_path
        self.config = config or {}
        self.lazy_frame = None
        
        # 配置内置 Logger
        self.logger = logging.getLogger(self.__class__.__name__)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - [%(levelname)s] - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)

    def extract(self):
        """Phase 1: 提取阶段"""
        self.logger.info(f"==> 开始提取数据 (Extract) 从: {self.input_path}")
        try:
            if not os.path.exists(self.input_path) and '*' not in self.input_path:
                self.logger.warning(f"输入路径 {self.input_path} 不存在，提取可能失败。")

            # 根据扩展名动态识别 CSV 或 Parquet 并执行 LazyLoad
            if self.input_path.endswith('.csv'):
                has_header = self.config.get('has_header', True)
                self.lazy_frame = pl.scan_csv(self.input_path, has_header=has_header)
                # 动态改名
                if 'rename_cols' in self.config:
                    self.lazy_frame = self.lazy_frame.rename(self.config['rename_cols'])
                    self.logger.info("已应用表头重命名规则。")
                    
            elif self.input_path.endswith('.parquet') or ".parquet" in self.input_path:
                self.lazy_frame = pl.scan_parquet(self.input_path)
            else:
                raise ValueError("不支持的文件格式。仅支持 .csv 或 .parquet 及其通配符路径。")
                
            self.logger.info(f"Extract 阶段已成功完成 (Lazy加载，未进入内存)。")
            
        except Exception as e:
            self.logger.error(f"提取阶段发生异常: {e}")
            raise

    def transform(self):
        """Phase 2: 转换与清洗阶段"""
        self.logger.info("==> 开始数据清洗与转换 (Transform)...")
        try:
            if self.lazy_frame is None:
                raise ValueError("没有找到加载的数据对象，请先执行 extract()。")
                
            q = self.lazy_frame

            # 1. 应用业务清洗/过滤规则
            if 'filter_rules' in self.config:
                filter_exprs = self.config['filter_rules']
                for rule in filter_exprs:
                    q = q.filter(rule)
                self.logger.info(f"已应用 {len(filter_exprs)} 条数据过滤规则。")
            
            # 2. 字段规范化（错别字修复）
            if 'typo_corrections' in self.config and 'typo_column' in self.config:
                typo_col = self.config['typo_column']
                typos = self.config['typo_corrections']
                replace_expr = pl.col(typo_col)
                for typo, correct in typos.items():
                    replace_expr = replace_expr.replace(typo, correct)
                q = q.with_columns([replace_expr.alias(typo_col)])
                self.logger.info(f"已应用 {typo_col} 字段的标准化修复规则。")
            
            # 3. 隐私数据哈希脱敏
            if 'hash_column' in self.config:
                def hash_func(val: str) -> str:
                    if val is None:
                        return hashlib.sha256(b"null").hexdigest()
                    return hashlib.sha256(str(val).encode('utf-8')).hexdigest()
                
                hash_col = self.config['hash_column']
                new_hash_col = self.config.get('new_hash_column', f"masked_{hash_col}")
                q = q.with_columns([
                    pl.col(hash_col).cast(pl.Utf8).map_elements(hash_func, return_dtype=pl.Utf8).alias(new_hash_col)
                ])
                if self.config.get('drop_original_hash_column', True):
                    q = q.drop([hash_col])
                self.logger.info(f"已完成 {hash_col} 的 SHA256 脱敏。")
            
            # 4. 精密去重
            if 'dedup_subset' in self.config:
                subset = self.config['dedup_subset']
                q = q.unique(subset=subset, keep="first")
                self.logger.info(f"基于键值组 {subset} 进行去重配置已完成。")
                
            # 更新 LazyFrame
            self.lazy_frame = q
            self.logger.info("Transform 阶段各项规则配置成功 (等待 Load 时触发计算)。")
            
        except Exception as e:
            self.logger.error(f"数据转换阶段发生异常: {e}")
            raise

    def load(self):
        """Phase 3: 加载与存储阶段"""
        self.logger.info(f"==> 开始加载及持久化存储 (Load) 至: {self.output_path}")
        try:
            if self.lazy_frame is None:
                raise ValueError("没有需要存储的数据，检查是否跳过了前期步骤。")
            
            output_dir = os.path.dirname(self.output_path)
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)

            partition_col = self.config.get('partition_by', None)
            compression = self.config.get('compression', 'snappy')
            
            # 分场景控制：分为整体输出与按目录分区输出
            if partition_col:
                self.logger.info(f"使用列 [{partition_col}] 高效分区存储至目录：{self.output_path}")
                os.makedirs(self.output_path, exist_ok=True)
                
                # 为了避免全量 collect 导致内存溢出(OOM)，先提取出唯一的 partition 值
                self.logger.info(f"正在分析分区列唯一值...")
                unique_vals = self.lazy_frame.select(pl.col(partition_col).unique()).collect(engine="streaming")[partition_col].drop_nulls().to_list()
                
                for val in unique_vals:
                    partition_dir = os.path.join(self.output_path, f"{partition_col}={val}")
                    os.makedirs(partition_dir, exist_ok=True)
                    partition_file = os.path.join(partition_dir, "data.parquet")
                    self.logger.info(f"正在导出物理分区: {partition_col}={val}")
                    
                    part_q = self.lazy_frame.filter(pl.col(partition_col) == val)
                    try:
                        # 尝试全流式处理写入
                        part_q.sink_parquet(partition_file, compression=compression)
                    except Exception as e:
                        # 降级：如果引擎不支持当前的 streaming graph 则 collect
                        part_df = part_q.collect(engine="streaming")
                        part_df.write_parquet(partition_file, compression=compression)
                        
                self.logger.info(f"物理分区导出完毕。总计 {len(unique_vals)} 个分区。")
            else:
                self.logger.info(f"以 {compression} 压缩等级，启动流式单文件写入进程...")
                self.lazy_frame.sink_parquet(self.output_path, compression=compression)
            
            self.logger.info("Load 阶段已全部完成。数据已成功持久化。")
            
        except Exception as e:
            self.logger.error(f"数据加载与持久化时发生异常: {e}")
            raise

    def run(self):
        """主入口，一键串联起所有管道"""
        start_time = time.time()
        self.logger.info("================ START PROCESSING ================")
        try:
            self.extract()
            self.transform()
            self.load()
            elapsed_time = time.time() - start_time
            self.logger.info(f"==== PIPELINE SUCCESS: 总计耗时 {elapsed_time:.2f} 秒 ====")
        except Exception as e:
            self.logger.error(f"==== PIPELINE FAILED: 流水线异常中断! 原因: {e} ====")
            raise
