# %% [code]
# ---------- 导入库 ----------
import csv
import sqlite3
import re
import json
from rich.console import Console
from rich.table import Table
import pandas as pd
import matplotlib.pyplot as plt
import calendar
import os
import shutil
from pathlib import Path
import datetime
import logging
import argparse
from typing import Dict, List, Tuple, Union, Any

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('MoneyCount')

# %% [code]
# ---------- 配置管理器 ----------
class ConfigManager:
    """集中管理配置信息和文件操作"""
    def __init__(self):
        self.CURRENT_MONTH = "202506"
        self.DB_PATH = "moneycount.db"
        self.BASE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
        
        # 目录配置
        self.CONFIG_DIR = self.BASE_DIR / "config"
        self.DATA_DIR = self.BASE_DIR / "data"
        self.PROMPT_DIR = self.BASE_DIR / "prompts"
        self.SQL_DIR = self.BASE_DIR / "sql_templates"
        self.LOG_DIR = self.BASE_DIR / "logs"
        
        # 确保目录存在
        self._ensure_directories()
        
        # 加载配置
        self.TAG_MAPPING = self.load_config("tag_mapping.json", {})
        self.AUTO_TAG_MAPPING = self.load_config("auto_tag_mapping.json", {})
        self.SENSITIVE_WORDS = self.load_config("sensitive_words_v1.json", {})
        
        # 日志文件设置
        self._setup_logging()
    
    def _ensure_directories(self):
        """确保所有必要目录存在"""
        self.CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        self.DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.PROMPT_DIR.mkdir(parents=True, exist_ok=True)
        self.SQL_DIR.mkdir(parents=True, exist_ok=True)
        self.LOG_DIR.mkdir(parents=True, exist_ok=True)
        
        # 创建数据子目录
        for subdir in ["wx", "zfb", "bank"]:
            (self.DATA_DIR / subdir).mkdir(parents=True, exist_ok=True)
    
    def _setup_logging(self):
        """设置文件日志"""
        log_file = self.LOG_DIR / f"moneycount_{self.CURRENT_MONTH}.log"
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        logger.addHandler(file_handler)
    
    def load_config(self, filename: str, default: Any = None) -> Dict:
        """从JSON文件加载配置，如果文件不存在则返回默认值"""
        path = self.CONFIG_DIR / filename
        if not path.exists():
            # 如果文件不存在，创建默认配置文件
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(default or {}, f, ensure_ascii=False, indent=2)
            logger.info(f"创建默认配置文件: {filename}")
            return default or {}
        
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"加载配置文件 {filename} 出错: {e}")
            return default or {}
    
    def save_config(self, filename: str, data: Dict):
        """保存配置到文件"""
        path = self.CONFIG_DIR / filename
        try:
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(f"配置文件已保存: {filename}")
        except Exception as e:
            logger.error(f"保存配置文件 {filename} 出错: {e}")
    
    def load_sql_template(self, template_name: str, variables: Dict = None) -> str:
        """加载SQL模板并替换变量"""
        template_path = self.SQL_DIR / f"{template_name}.sql"
        if not template_path.exists():
            # 创建默认SQL模板文件
            with open(template_path, 'w', encoding='utf-8') as f:
                f.write(f"-- {template_name} SQL模板\n")
                f.write("-- 使用 $$变量名$$ 格式定义变量\n")
                f.write("SELECT 'SQL模板未配置' AS error;")
            logger.warning(f"SQL模板 {template_name} 不存在，已创建空模板")
            return ""
        
        try:
            with open(template_path, 'r', encoding='utf-8') as f:
                sql_template = f.read()
                
                # 替换变量
                if variables:
                    for var_name, var_value in variables.items():
                        placeholder = f"$${var_name}$$"
                        sql_template = sql_template.replace(placeholder, str(var_value))
                
                return sql_template
        except Exception as e:
            logger.error(f"加载SQL模板 {template_name} 出错: {e}")
            return ""
    
    def load_prompt(self, prompt_name: str) -> str:
        """从Markdown文件加载提示词"""
        prompt_path = self.PROMPT_DIR / f"{prompt_name}.md"
        if not prompt_path.exists():
            # 创建示例提示词文件
            with open(prompt_path, 'w', encoding='utf-8') as f:
                f.write("# 默认提示词模板\n\n请在此处编写提示词内容")
            logger.warning(f"提示词文件 {prompt_name}.md 不存在，已创建空模板")
            return ""
        
        try:
            with open(prompt_path, 'r', encoding='utf-8') as f:
                return f.read()
        except Exception as e:
            logger.error(f"加载提示词 {prompt_name} 出错: {e}")
            return ""
    
    def get_current_table_name(self) -> str:
        """获取当前月份对应的表名"""
        return f"pay_{self.CURRENT_MONTH}"
    
    def get_data_path(self, data_type: str, month_year: str = None) -> Path:
        """
        获取数据文件路径
        :param data_type: 'wx', 'zfb' 或 'bank'
        :param month_year: 格式为YYYYMM，如202507
        :return: 文件路径
        """
        month_year = month_year or self.CURRENT_MONTH
        return self.DATA_DIR / data_type / f"{month_year}.csv"
    
    def set_current_month(self, month: str):
        """设置当前月份并更新日志"""
        if re.match(r"^\d{6}$", month):
            self.CURRENT_MONTH = month
            self._setup_logging()
            logger.info(f"当前月份设置为: {month}")
        else:
            logger.error(f"无效的月份格式: {month}, 应为YYYYMM格式")

# 初始化配置管理器
config = ConfigManager()

# %% [code]
# ---------- 工具函数 ----------
def sanitize_column_name(name: str) -> str:
    """清理列名，确保符合SQLite标识符规范"""
    cleaned_name = re.sub(r'[^a-zA-Z0-9_]', '', name)
    if cleaned_name and cleaned_name[0].isdigit():
        cleaned_name = 'col_' + cleaned_name
    return cleaned_name.lower()

def get_column_type(column_name: str) -> str:
    """根据列名确定数据类型"""
    money_keywords = ['金额', '退款', '价格', '费用', '余额', '服务费']
    if any(keyword in column_name for keyword in money_keywords):
        return 'REAL'
    return 'TEXT'

def is_money_column(column_name: str) -> bool:
    """判断是否为金额列"""
    money_keywords = ['金额', '退款', '价格', '费用', '余额', '服务费']
    return any(keyword in column_name for keyword in money_keywords)

def clean_money_value(value: str) -> float:
    """清理金额数据"""
    if not value or value.strip() == "":
        return 0.0
    
    # 尝试多种格式清理
    cleaned_value = re.sub(r'[^\d.-]', '', value)
    try:
        return float(cleaned_value) if cleaned_value else 0.0
    except ValueError:
        logger.warning(f"无法解析金额值: '{value}'")
        return 0.0

def rename_existing_table(cursor: sqlite3.Cursor, table_name: str) -> bool:
    """检查表是否存在，如果存在则将其重命名"""
    try:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        if cursor.fetchone():
            # 生成带时间戳的新表名
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            new_table_name = f"{table_name}_backup_{timestamp}"
            cursor.execute(f"ALTER TABLE {table_name} RENAME TO {new_table_name}")
            logger.info(f"表 {table_name} 已重命名为 {new_table_name}")
            return True
        return False
    except Exception as e:
        logger.error(f"重命名表时发生错误: {e}")
        return False

def get_month_date_range(monthyear: str) -> Tuple[str, str]:
    """获取指定月份的起止日期"""
    try:
        year = int(monthyear[:4])
        month = int(monthyear[4:])
        _, last_day = calendar.monthrange(year, month)
        start_date = f"{year}-{month:02d}-01"
        end_date = f"{year}-{month:02d}-{last_day}"
        return start_date, end_date
    except Exception as e:
        logger.error(f"获取月份日期范围错误: {e}")
        return f"{monthyear[:4]}-{monthyear[4:]}-01", f"{monthyear[:4]}-{monthyear[4:]}-31"

def apply_sensitive_word_filter(text: str) -> str:
    """应用敏感词过滤到文本"""
    if not text:
        return text
    
    for word, replacement in config.SENSITIVE_WORDS.items():
        text = text.replace(word, replacement)
    return text

# %% [code]
# ---------- 数据准备功能 ----------
def prepare_data_files(month_year: str):
    """
    准备数据文件：重命名并移动到指定目录
    :param month_year: 格式为YYYYMM，如202507
    """
    try:
        source_dir = Path("/home/luany/桌面/moneycount") / month_year
        if not source_dir.exists():
            logger.error(f"源目录不存在: {source_dir}")
            return False
        
        # 处理微信文件
        wx_files = list(source_dir.glob("*微信*"))
        if wx_files:
            target = config.get_data_path("wx", month_year)
            file = wx_files[0]
            if file.suffix == ".xlsx":
                # 需要先将Excel转为CSV
                import pandas as pd
                try:
                    df = pd.read_excel(file)
                    df.to_csv(target, index=False, encoding='utf-8')
                    logger.info(f"微信Excel文件已转换为CSV: {file} -> {target}")
                except Exception as e:
                    logger.error(f"转换微信Excel文件失败: {e}")
            else:
                shutil.copy(file, target)
                logger.info(f"微信文件已复制: {file} -> {target}")
        else:
            logger.warning(f"未找到微信文件: {source_dir}")
        
        # 处理支付宝文件
        zfb_files = list(source_dir.glob("*alipay*"))
        if zfb_files:
            target = config.get_data_path("zfb", month_year)
            shutil.copy(zfb_files[0], target)
            logger.info(f"支付宝文件已复制: {zfb_files[0]} -> {target}")
        else:
            logger.warning(f"未找到支付宝文件: {source_dir}")
        
        # 处理银行文件
        bank_files = list(source_dir.glob("*bank*"))
        if bank_files:
            target = config.get_data_path("bank", month_year)
            shutil.copy(bank_files[0], target)
            logger.info(f"银行文件已复制: {bank_files[0]} -> {target}")
        else:
            logger.warning(f"未找到银行文件: {source_dir}")
        
        return True
    except Exception as e:
        logger.error(f"准备数据文件时出错: {e}")
        return False

# %% [code]
# ---------- CSV导入功能 ----------
def import_csv_to_sqlite(csv_file_path: Path, strategy: str, 
                         header_row: int = 0, start_row: int = 0, 
                         skip_columns: List[int] = None, params: Dict = None) -> bool:
    """
    导入CSV文件到SQLite数据库
    :return: 是否成功
    """
    skip_columns = skip_columns or []
    params = params or {}
    
    # 添加月份参数
    params['monthyear'] = config.CURRENT_MONTH
    table_name = config.get_current_table_name()
    strategy_table_name = f"t_{table_name}_{strategy}"

    try:
        conn = sqlite3.connect(config.DB_PATH)
        cursor = conn.cursor()
        
        # 重命名已存在的表或创建新表
        rename_existing_table(cursor, strategy_table_name)
        
        # 尝试多种编码
        encodings = ['utf-8', 'gbk', 'gb2312', 'gb18030', 'latin1']
        
        for encoding in encodings:
            try:
                with open(csv_file_path, 'r', encoding=encoding) as csvfile:
                    csv_reader = csv.reader(csvfile)
                    
                    # 跳过header_row之前的行
                    for _ in range(header_row):
                        next(csv_reader)
                    
                    # 读取表头行
                    headers = next(csv_reader)
                    
                    # 过滤掉要跳过的列
                    filtered_headers = [h for i, h in enumerate(headers) if i not in skip_columns]
                    cleaned_headers = [sanitize_column_name(header) for header in filtered_headers]
                    
                    # 创建临时表
                    columns_def = ', '.join([f'[{col}] {get_column_type(col)}' for col in cleaned_headers])
                    create_strategy_table_query = f"""
                        CREATE TABLE IF NOT EXISTS {strategy_table_name} (
                            {columns_def}
                        )
                    """
                    cursor.execute(create_strategy_table_query)
                    
                    # 跳过start_row之前的行
                    remaining_rows_to_skip = start_row - (header_row + 1)
                    if remaining_rows_to_skip > 0:
                        for _ in range(remaining_rows_to_skip):
                            next(csv_reader)
                    
                    # 批量插入数据
                    batch_size = 100
                    batch = []
                    
                    for row in csv_reader:
                        if not row:  # 跳过空行
                            continue
                        
                        filtered_row = []
                        for i, value in enumerate(row):
                            if i not in skip_columns:
                                if i < len(cleaned_headers) and is_money_column(cleaned_headers[i]):
                                    filtered_row.append(clean_money_value(value))
                                else:
                                    filtered_row.append(value.strip())
                        
                        if len(filtered_row) == len(cleaned_headers):
                            batch.append(tuple(filtered_row))
                            
                            if len(batch) >= batch_size:
                                placeholders = ','.join(['?' for _ in filtered_row])
                                insert_query = f"INSERT INTO {strategy_table_name} VALUES ({placeholders})"
                                cursor.executemany(insert_query, batch)
                                batch = []
                    
                    # 插入剩余数据
                    if batch:
                        placeholders = ','.join(['?' for _ in filtered_row])
                        insert_query = f"INSERT INTO {strategy_table_name} VALUES ({placeholders})"
                        cursor.executemany(insert_query, batch)
                
                logger.info(f"成功使用 {encoding} 编码导入CSV到临时表 {strategy_table_name}")
                break
            except UnicodeDecodeError:
                logger.warning(f"编码 {encoding} 失败，尝试下一种编码")
                continue
            except Exception as e:
                logger.error(f"导入CSV时出错: {e}")
                conn.rollback()
                return False
        else:
            logger.error("无法使用任何编码解码文件")
            return False
        
        # 创建正式表 - 使用SQL模板
        create_table_sql = config.load_sql_template(
            "create_table", 
            {"table_name": table_name}
        )
        if create_table_sql:
            cursor.execute(create_table_sql)
        else:
            # 后备方案
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id BIGINT NOT NULL,
                    pay_time VARCHAR NOT NULL,
                    pay_monthyear VARCHAR NOT NULL,
                    pay_source VARCHAR,
                    pay_note VARCHAR,
                    pay_money NUMERIC NOT NULL,
                    pay_tag VARCHAR,
                    app_source VARCHAR,
                    PRIMARY KEY (id, pay_monthyear, pay_money)
                )
            """)
        
        # 根据策略执行相应的SQL将数据导入正式表
        strategy_handlers = {
            'wx': handle_wx_strategy,
            'zfb': handle_zfb_strategy,
            'yh': handle_yh_strategy,
            'bank': handle_bank_strategy,
            'other': handle_other_strategy
        }
        
        if strategy in strategy_handlers:
            strategy_handlers[strategy](cursor, table_name, strategy_table_name, params)
        else:
            logger.error(f"未知的策略: {strategy}")
        
        # 应用敏感词过滤
        apply_sensitive_filtering(cursor, table_name)
        
        # 提交事务
        conn.commit()
        logger.info(f"成功执行{strategy}策略并导入到正式表 {table_name}")
        return True
        
    except Exception as e:
        logger.error(f"导入过程中发生错误: {e}")
        if 'conn' in locals():
            conn.rollback()
        return False
    finally:
        if 'conn' in locals():
            conn.close()

def handle_wx_strategy(cursor, table_name, strategy_table_name, params):
    """处理微信策略"""
    # 微信支出
    wx_expense_sql = config.load_sql_template(
        "wx_expense",
        {
            "table_name": table_name,
            "strategy_table_name": strategy_table_name,
            "monthyear": params.get('monthyear')
        }
    )
    if wx_expense_sql:
        cursor.execute(wx_expense_sql)
    
    # 微信收入
    wx_income_sql = config.load_sql_template(
        "wx_income",
        {
            "table_name": table_name,
            "strategy_table_name": strategy_table_name,
            "monthyear": params.get('monthyear')
        }
    )
    if wx_income_sql:
        cursor.execute(wx_income_sql)

def handle_zfb_strategy(cursor, table_name, strategy_table_name, params):
    """处理支付宝策略"""
    # 支付宝支出
    zfb_expense_sql = config.load_sql_template(
        "zfb_expense",
        {
            "table_name": table_name,
            "strategy_table_name": strategy_table_name,
            "monthyear": params.get('monthyear')
        }
    )
    if zfb_expense_sql:
        cursor.execute(zfb_expense_sql)
    
    # 支付宝退款
    zfb_refund_sql = config.load_sql_template(
        "zfb_refund",
        {
            "table_name": table_name,
            "strategy_table_name": strategy_table_name,
            "monthyear": params.get('monthyear')
        }
    )
    if zfb_refund_sql:
        cursor.execute(zfb_refund_sql)

def handle_yh_strategy(cursor, table_name, strategy_table_name, params):
    """处理银行信用卡策略"""
    logger.info("银行信用卡导入策略尚未实现")

def handle_bank_strategy(cursor, table_name, strategy_table_name, params):
    """处理银行策略"""
    bank_sql = config.load_sql_template(
        "bank_import",
        {
            "table_name": table_name,
            "strategy_table_name": strategy_table_name,
            "monthyear": params.get('monthyear')
        }
    )
    if bank_sql:
        cursor.execute(bank_sql)

def handle_other_strategy(cursor, table_name, strategy_table_name, params):
    """处理其他策略"""
    logger.info("其他导入策略尚未实现")

def apply_sensitive_filtering(cursor, table_name):
    """应用敏感词过滤"""
    if not config.SENSITIVE_WORDS:
        return
    
    try:
        # 对pay_source应用敏感词过滤
        update_source_sql = config.load_sql_template(
            "update_sensitive_source",
            {"table_name": table_name}
        )
        
        # 对pay_note应用敏感词过滤
        update_note_sql = config.load_sql_template(
            "update_sensitive_note",
            {"table_name": table_name}
        )
        
        if update_source_sql:
            cursor.execute(update_source_sql)
        
        if update_note_sql:
            cursor.execute(update_note_sql)
        
        logger.info("已应用敏感词过滤")
    except Exception as e:
        logger.error(f"应用敏感词过滤时出错: {e}")

# %% [code]
# ---------- 查询功能 ----------
def query_payment_data(params: Dict) -> Tuple[bool, Union[str, List[Dict]]]:
    """
    查询支付数据
    :return: (是否成功, 结果或错误消息)
    """
    try:
        conn = sqlite3.connect(config.DB_PATH)
        cursor = conn.cursor()
        
        table_name = config.get_current_table_name()
        
        # 检查表是否存在
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        if not cursor.fetchone():
            return False, f"表格 {table_name} 不存在"
        
        # 构建查询条件
        conditions = []
        query_params = []
        
        # 标签筛选
        if params.get('tag'):
            conditions.append("pay_tag = ?")
            query_params.append(params['tag'])
        
        # 关键词搜索
        if params.get('key'):
            conditions.append("(pay_source LIKE ? OR pay_note LIKE ?)")
            query_params.extend([f"%{params['key']}%", f"%{params['key']}%"])
        
        # 金额范围筛选
        if params.get('lt'):
            try:
                conditions.append("pay_money < ?")
                query_params.append(float(params['lt']))
            except ValueError:
                return False, "金额上限格式错误"
        
        if params.get('gt'):
            try:
                conditions.append("pay_money > ?")
                query_params.append(float(params['gt']))
            except ValueError:
                return False, "金额下限格式错误"
        
        # 日期范围筛选
        if params.get('start_date'):
            try:
                conditions.append("pay_time >= ?")
                query_params.append(params['start_date'])
            except:
                return False, "起始日期格式错误"
        
        if params.get('end_date'):
            try:
                conditions.append("pay_time <= ?")
                query_params.append(params['end_date'])
            except:
                return False, "结束日期格式错误"
        
        # 构建完整的SQL查询
        base_query = f"SELECT * FROM {table_name}"
        if conditions:
            base_query += " WHERE " + " AND ".join(conditions)
        
        # 添加排序
        sort_field = params.get('sort', 'pay_money')
        sort_order = 'DESC' if params.get('desc', True) else 'ASC'
        query = f"{base_query} ORDER BY {sort_field} {sort_order}"
        
        # 执行查询
        cursor.execute(query, query_params)
        
        # 获取列名
        columns = [description[0] for description in cursor.description]
        
        # 将结果转换为字典列表
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        return True, results
        
    except sqlite3.Error as e:
        return False, f"数据库错误: {str(e)}"
    finally:
        if 'conn' in locals():
            conn.close()

def display_query_results(results: List[Dict], params: Dict):
    """
    使用rich库显示查询结果
    """
    console = Console()
    
    if not results:
        console.print("[red]没有找到数据[/red]")
        return
    
    # 创建表格
    table = Table(
        title=f"支付记录 ({config.CURRENT_MONTH})",
        show_header=True,
        header_style="bold magenta",
        border_style="blue",
        title_style="bold cyan"
    )
    
    # 添加列
    table.add_column("日期", style="cyan")
    table.add_column("金额", justify="right", style="green")
    table.add_column("来源", style="yellow")
    table.add_column("标签", style="red")
    table.add_column("备注")
    
    # 添加数据行
    for row in results:
        money = f"¥{float(row['pay_money']):.2f}"
        money_style = "[bold red]" + money + "[/bold red]" if float(row['pay_money']) > 100 else "[green]" + money + "[/green]"
            
        table.add_row(
            row['pay_time'],
            money_style,
            row['pay_source'],
            row['pay_tag'],
            row.get('pay_note', '')
        )
    
    # 显示查询条件
    console.print("\n[bold yellow]查询条件:[/bold yellow]")
    conditions = []
    if params.get('tag'):
        conditions.append(f"标签: {params['tag']}")
    if params.get('key'):
        conditions.append(f"关键词: {params['key']}")
    if params.get('lt'):
        conditions.append(f"金额 < {params['lt']}")
    if params.get('gt'):
        conditions.append(f"金额 > {params['gt']}")
    if params.get('start_date'):
        conditions.append(f"起始日期: {params['start_date']}")
    if params.get('end_date'):
        conditions.append(f"结束日期: {params['end_date']}")
    
    if conditions:
        console.print(" | ".join(conditions))
    
    # 打印表格
    console.print("\n")
    console.print(table)
    
    # 打印统计信息
    total_amount = sum(float(row['pay_money']) for row in results)
    console.print(f"\n[bold green]总金额: ¥{total_amount:.2f}[/bold green]")
    console.print(f"[dim]共 {len(results)} 条记录[/dim]")

# %% [code]
# ---------- 更新功能 ----------
def update_payment_tags(params: Dict) -> Tuple[bool, str]:
    """
    批量更新支付标签
    :return: (是否成功, 消息)
    """
    try:
        conn = sqlite3.connect(config.DB_PATH)
        cursor = conn.cursor()
        
        table_name = config.get_current_table_name()
        
        # 检查表是否存在
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        if not cursor.fetchone():
            return False, f"表格 {table_name} 不存在"
        
        # 检查标签词是否在映射中
        tag_word = params.get('tag_word')
        if tag_word not in config.TAG_MAPPING:
            return False, f"不支持的标签词: {tag_word}"
            
        new_tag = config.TAG_MAPPING[tag_word]
        
        # 构建查询条件
        conditions = []
        query_params = []
        
        # 关键词搜索
        if params.get('key'):
            conditions.append("(pay_source LIKE ? OR pay_note LIKE ?)")
            query_params.extend([f"%{params['key']}%", f"%{params['key']}%"])
        
        # 金额范围筛选
        if params.get('lt'):
            try:
                conditions.append("pay_money < ?")
                query_params.append(float(params['lt']))
            except ValueError:
                return False, "金额上限格式错误"
        
        if params.get('gt'):
            try:
                conditions.append("pay_money > ?")
                query_params.append(float(params['gt']))
            except ValueError:
                return False, "金额下限格式错误"
        
        # 加载更新SQL模板
        update_sql = config.load_sql_template(
            "update_tags",
            {"table_name": table_name}
        )
        
        if not update_sql:
            # 后备方案
            update_sql = f"UPDATE {table_name} SET pay_tag = ?"
        
        if conditions:
            update_sql += " WHERE " + " AND ".join(conditions)
            
        # 组合参数
        update_params = [new_tag] + query_params
        
        # 执行更新
        cursor.execute(update_sql, update_params)
        
        # 获取更新的行数
        rows_affected = cursor.rowcount
        
        # 提交事务
        conn.commit()
        return True, f"成功更新 {rows_affected} 条记录的标签为 {new_tag}"
        
    except sqlite3.Error as e:
        return False, f"数据库错误: {str(e)}"
    finally:
        if 'conn' in locals():
            conn.close()

def auto_update_tags_based_on_history() -> List[str]:
    """根据历史记录习惯自动更新标签"""
    results = []
    for source, tag in config.AUTO_TAG_MAPPING.items():
        params = {
            "key": source,
            "tag_word": tag
        }
        success, message = update_payment_tags(params)
        results.append(f"{source} -> {tag}: {message}")
    return results

# %% [code]
# ---------- 手动添加记录功能 ----------
def add_manual_record(params: Dict) -> Tuple[bool, str]:
    """
    手动添加记录到数据库
    :param params: 包含以下键的字典:
        - pay_time: 交易时间
        - pay_source: 交易来源
        - pay_note: 交易备注
        - pay_money: 交易金额
        - monthyear: 年月 (YYYYMM格式)
        - pay_tag: 交易标签 (可选)
    """
    try:
        conn = sqlite3.connect(config.DB_PATH)
        cursor = conn.cursor()
        
        # 设置默认值
        monthyear = params.get('monthyear', config.CURRENT_MONTH)
        
        if 'pay_time' not in params or not params['pay_time']:
            # 默认为当月2号 00:00:00
            pay_date = datetime.datetime.strptime(monthyear, "%Y%m").replace(day=2)
            params['pay_time'] = pay_date.strftime("%Y-%m-%d %H:%M:%S")
        
        # 生成ID (使用时间戳)
        id = str(int(datetime.datetime.strptime(params['pay_time'], "%Y-%m-%d %H:%M:%S").timestamp()))
        
        # 获取表名
        table_name = f"pay_{monthyear}"
        
        # 检查表是否存在
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        if not cursor.fetchone():
            # 表不存在则创建
            create_table_sql = config.load_sql_template(
                "create_table", 
                {"table_name": table_name}
            )
            if create_table_sql:
                cursor.execute(create_table_sql)
            else:
                # 后备方案
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        id BIGINT NOT NULL,
                        pay_time VARCHAR NOT NULL,
                        pay_monthyear VARCHAR NOT NULL,
                        pay_source VARCHAR,
                        pay_note VARCHAR,
                        pay_money NUMERIC NOT NULL,
                        pay_tag VARCHAR,
                        app_source VARCHAR,
                        PRIMARY KEY (id, pay_monthyear, pay_money)
                    )
                """)
        
        # 加载插入SQL模板
        insert_sql = config.load_sql_template(
            "insert_manual_record",
            {"table_name": table_name}
        )
        
        if not insert_sql:
            # 后备方案
            insert_sql = f"""
            INSERT INTO [{table_name}] 
                (id, pay_time, pay_monthyear, pay_source, pay_note, pay_money, pay_tag, app_source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
        
        # 插入记录
        cursor.execute(insert_sql, (
            id,
            params['pay_time'],
            monthyear,
            params['pay_source'],
            params['pay_note'],
            float(params['pay_money']),
            params.get('pay_tag', 'manual'),
            'manual'
        ))
        
        conn.commit()
        return True, "记录添加成功"
    
    except ValueError as ve:
        return False, f"金额格式错误: {str(ve)}"
    except Exception as e:
        return False, f"添加记录失败: {str(e)}"
    finally:
        if 'conn' in locals():
            conn.close()

# %% [code]
# ---------- 图表分析功能 ----------
def query_monthly_data() -> Tuple[bool, Union[str, pd.DataFrame]]:
    """查询月度数据"""
    try:
        conn = sqlite3.connect(config.DB_PATH)
        cursor = conn.cursor()
        
        table_name = config.get_current_table_name()
        
        # 检查表是否存在
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        if not cursor.fetchone():
            return False, f"表格 {table_name} 不存在"
        
        # 查询所有数据
        query_sql = config.load_sql_template(
            "query_monthly_data",
            {"table_name": table_name}
        )
        
        if not query_sql:
            # 后备方案
            query_sql = f"""
                SELECT pay_time, pay_money, pay_tag 
                FROM {table_name}
                WHERE pay_monthyear = '{config.CURRENT_MONTH}'
            """
        
        cursor.execute(query_sql)
        
        # 转换为DataFrame
        df = pd.DataFrame(cursor.fetchall(), columns=['pay_time', 'pay_money', 'pay_tag'])
        
        # 处理日期格式
        try:
            df['pay_time'] = pd.to_datetime(df['pay_time'], errors='coerce')
            df = df.dropna(subset=['pay_time'])
        except Exception as e:
            logger.error(f"日期处理错误: {e}")
        
        return True, df
        
    except sqlite3.Error as e:
        return False, f"数据库错误: {str(e)}"
    finally:
        if 'conn' in locals():
            conn.close()

def plot_monthly_charts(df: pd.DataFrame) -> plt.Figure:
    """绘制月度图表"""
    # 设置字体
    plt.rcParams['font.sans-serif'] = ['Arial', 'Helvetica', 'DejaVu Sans', 'sans-serif']
    plt.rcParams['axes.unicode_minus'] = False
    
    # 创建一个图形，包含两个子图
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
    
    # 1. 饼图 - 按标签统计
    tag_sum = df.groupby('pay_tag')['pay_money'].sum()
    sorted_tags = sorted(tag_sum.index)
    tag_sum = tag_sum[sorted_tags]
    total = tag_sum.sum()
    
    if total > 0:
        percentages = tag_sum / total * 100
        labels = [f'{tag}\n¥{amount:.2f}\n({percent:.1f}%)' 
                for tag, amount, percent in zip(tag_sum.index, tag_sum, percentages)]
        
        ax1.pie(tag_sum, labels=labels, autopct='', startangle=90)
        ax1.set_title(f'{config.CURRENT_MONTH[:4]}年{config.CURRENT_MONTH[4:]}月消费类别分布')
    else:
        ax1.text(0.5, 0.5, '无消费数据', ha='center', va='center')
        ax1.set_title('无消费数据')
    
    # 2. 柱状图 - 按日期统计
    start_date, end_date = get_month_date_range(config.CURRENT_MONTH)
    date_range = pd.date_range(start=start_date, end=end_date)
    
    # 按日期分组求和
    if not df.empty:
        df['date'] = df['pay_time'].dt.date
        daily_sum = df.groupby('date')['pay_money'].sum()
        
        # 创建完整的日期序列，填充缺失值为0
        daily_sum_full = pd.Series(0, index=date_range.date)
        daily_sum_full.update(daily_sum)
    else:
        daily_sum_full = pd.Series(0, index=date_range.date)
    
    # 绘制柱状图
    bars = ax2.bar(daily_sum_full.index.astype(str), daily_sum_full.values)
    ax2.set_title(f'{config.CURRENT_MONTH[:4]}年{config.CURRENT_MONTH[4:]}月每日消费金额')
    ax2.set_xlabel('日期')
    ax2.set_ylabel('金额 (元)')
    plt.xticks(rotation=45)
    
    # 在每个柱子上方显示金额
    for bar in bars:
        height = bar.get_height()
        if height > 0:
            ax2.text(
                bar.get_x() + bar.get_width()/2., 
                height + 0.1,
                f'¥{height:.0f}',
                ha='center',
                va='bottom',
                fontsize=8
            )
    
    plt.tight_layout()
    return fig

# %% [code]
# ---------- 主程序 ----------
def main():
    """主程序入口"""
    parser = argparse.ArgumentParser(description='个人财务管理系统')
    parser.add_argument('--month', type=str, default=config.CURRENT_MONTH, 
                       help='处理的月份，格式为YYYYMM，默认为当前配置月份')
    parser.add_argument('--modes', type=str, default="import,auto_update,query", 
                       help='执行的功能模式，用逗号分隔，可选值: import,auto_update,manual_update,query,chart,deepseek,balance')
    
    args = parser.parse_args()
    
    # 设置当前月份
    if args.month != config.CURRENT_MONTH:
        config.set_current_month(args.month)
    
    # 获取用户选择
    selected_modes = [mode.strip() for mode in args.modes.split(',')]
    
    logger.info(f"当前处理月份: {config.CURRENT_MONTH}")
    logger.info(f"将执行的功能: {', '.join(selected_modes)}")
    
    # 执行选中的功能
    if 'import' in selected_modes:
        logger.info("\n=== 导入数据 ===")
        prepare_data_files(config.CURRENT_MONTH)
        
        # 导入微信数据
        wx_path = config.get_data_path('wx')
        if wx_path.exists():
            logger.info("导入微信数据...")
            import_csv_to_sqlite(
                csv_file_path=wx_path,
                strategy='wx',
                header_row=16,
                start_row=17,
                skip_columns=[8, 9]
            )
        else:
            logger.warning("微信数据文件不存在，跳过导入")
        
        # 导入支付宝数据
        zfb_path = config.get_data_path('zfb')
        if zfb_path.exists():
            logger.info("导入支付宝数据...")
            import_csv_to_sqlite(
                csv_file_path=zfb_path,
                strategy='zfb',
                header_row=4,
                start_row=5,
                skip_columns=[0, 1, 3, 16]
            )
        else:
            logger.warning("支付宝数据文件不存在，跳过导入")
        
        # 导入分期数据
        bank_path = config.get_data_path('bank')
        if bank_path.exists():
            logger.info("导入分期数据...")
            import_csv_to_sqlite(
                csv_file_path=bank_path,
                strategy='bank',
                header_row=0,
                start_row=1
            )
        else:
            logger.warning("银行数据文件不存在，跳过导入")
    
    if 'auto_update' in selected_modes:
        logger.info("\n=== 自动更新标签 ===")
        update_results = auto_update_tags_based_on_history()
        for result in update_results:
            logger.info(result)
    
    if 'manual_update' in selected_modes:
        logger.info("\n=== 手动更新标签 ===")
        up_params = {
            "monthyear": config.CURRENT_MONTH,
            "key": input("输入要更新的关键词（如'肯德基'）: ").strip(),
            "tag_word": input("输入标签词（如'主餐'）: ").strip(),
            "lt": input("输入最大金额（留空跳过）: ").strip() or "",
            "gt": input("输入最小金额（留空跳过）: ").strip() or ""
        }
        
        # 检查标签词是否有效
        if up_params["tag_word"] not in config.TAG_MAPPING:
            print(f"错误: 无效的标签词 '{up_params['tag_word']}'")
            print("可用标签词:", ", ".join(config.TAG_MAPPING.keys()))
        else:
            success, message = update_payment_tags(up_params)
            print("\n" + message)
    
    if 'query' in selected_modes:
        logger.info("\n=== 查询数据 ===")
        query_params = {
            "tag": input("输入标签筛选（留空跳过）: ").strip() or "",
            "key": input("输入关键词筛选（留空跳过）: ").strip() or "",
            "lt": input("输入最大金额（留空跳过）: ").strip() or "",
            "gt": input("输入最小金额（留空跳过）: ").strip() or "",
            "start_date": input("输入起始日期（YYYY-MM-DD，留空跳过）: ").strip() or "",
            "end_date": input("输入结束日期（YYYY-MM-DD，留空跳过）: ").strip() or "",
            "sort": input("排序字段（默认pay_money）: ").strip() or "pay_money",
            "desc": input("降序排序？(y/n, 默认y): ").strip().lower() != 'n'
        }
        success, results = query_payment_data(query_params)
        if success:
            display_query_results(results, query_params)
        else:
            print(f"查询失败: {results}")
    
    if 'chart' in selected_modes:
        logger.info("\n=== 绘制图表 ===")
        success, df = query_monthly_data()
        if success:
            fig = plot_monthly_charts(df)
            plt.show()
            
            # 输出统计信息
            print("\n统计信息:")
            if not df.empty:
                total = df['pay_money'].sum()
                print(f"总消费: ¥{total:.2f}")
                print(f"平均每日消费: ¥{total / len(df['pay_money']):.2f}")
                print("\n按标签统计:")
                tag_stats = df.groupby('pay_tag').agg({
                    'pay_money': ['sum', 'count']
                })
                print(tag_stats)
            else:
                print("本月无消费数据")
        else:
            print(f"数据查询失败: {df}")
    
    if 'deepseek' in selected_modes:
        logger.info("\n=== 生成DeepSeek提示词 ===")
        prompt_text = config.load_prompt("bank_statement_prompt").format(month=config.CURRENT_MONTH).strip()
        print("\n" + prompt_text)
    
    if 'balance' in selected_modes:
        logger.info("\n=== 手动添加平账记录 ===")
        balance_params = {
            "monthyear": config.CURRENT_MONTH,
            "pay_time": input("输入交易时间 (YYYY-MM-DD HH:MM:SS，留空使用当月2号): ").strip(),
            "pay_source": input("输入交易来源: ").strip(),
            "pay_note": input("输入交易备注: ").strip(),
            "pay_money": input("输入交易金额 (应为负数): ").strip(),
            "pay_tag": input("输入交易标签（可选，默认social_ex）: ").strip() or "social_ex"
        }
        
        # 验证金额是否为负数
        try:
            balance_params['pay_money'] = float(balance_params['pay_money'])
            if balance_params['pay_money'] >= 0:
                print("警告: 平账金额应为负数，已自动转换为负数")
                balance_params['pay_money'] = -abs(balance_params['pay_money'])
        except ValueError:
            print("错误: 金额必须是数字")
            exit()
        
        success, message = add_manual_record(balance_params)
        print("\n" + message)
    
    logger.info("\n所有选定功能执行完成！")

if __name__ == "__main__":
    main()