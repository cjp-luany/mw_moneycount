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

# %% [code]
# ---------- 全局参数设置 ----------
# 设置当前处理的月份（格式：YYYYMM）
CURRENT_MONTH = "202506"

# 数据库路径
DB_PATH = "moneycount.db"

# 配置文件路径
CONFIG_DIR = Path("config")
DATA_DIR = Path("data")
PROMPT_DIR = Path("prompts")
SQL_DIR = Path("sql_templates")

# 确保配置目录存在
CONFIG_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)
PROMPT_DIR.mkdir(parents=True, exist_ok=True)
SQL_DIR.mkdir(parents=True, exist_ok=True)

# 加载配置文件
def load_config(filename, default=None):
    """从JSON文件加载配置，如果文件不存在则返回默认值"""
    path = CONFIG_DIR / filename
    if not path.exists():
        # 如果文件不存在，创建默认配置文件
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(default or {}, f, ensure_ascii=False, indent=2)
        return default or {}
    
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"加载配置文件 {filename} 出错: {e}")
        return default or {}

# 加载SQL模板
def load_sql_template(template_name, variables=None):
    """加载SQL模板并替换变量"""
    template_path = SQL_DIR / f"{template_name}.sql"
    if not template_path.exists():
        # 创建默认SQL模板文件
        with open(template_path, 'w', encoding='utf-8') as f:
            f.write(f"-- {template_name} SQL模板\n")
            f.write("-- 使用 $$变量名$$ 格式定义变量\n")
            f.write("SELECT 'SQL模板未配置' AS error;")
        print(f"警告: SQL模板 {template_name} 不存在，已创建空模板")
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
        print(f"加载SQL模板 {template_name} 出错: {e}")
        return ""

# 加载配置
TAG_MAPPING = load_config("tag_mapping.json", {})

AUTO_TAG_MAPPING = load_config("auto_tag_mapping.json", {})

# 加载敏感词
SENSITIVE_WORDS = load_config("sensitive_words_v1.json", {})

# %% [code]
# ---------- 敏感词处理函数 ----------
def apply_sensitive_word_filter(text):
    """应用敏感词过滤到文本"""
    if not text:
        return text
    
    for word, replacement in SENSITIVE_WORDS.items():
        text = text.replace(word, replacement)
    return text

# %% [code]
# ---------- 提示词加载函数 ----------
def load_prompt(prompt_name):
    """从Markdown文件加载提示词"""
    prompt_path = PROMPT_DIR / f"{prompt_name}.md"
    if not prompt_path.exists():
        # 创建示例提示词文件
        with open(prompt_path, 'w', encoding='utf-8') as f:
            f.write("# 默认提示词模板\n\n请在此处编写提示词内容")
        print(f"提示词文件 {prompt_name}.md 不存在，已创建空模板")
        return ""
    
    try:
        with open(prompt_path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        print(f"加载提示词 {prompt_name} 出错: {e}")
        return ""
    
# %% [code]
# ---------- 分期账单提示词功能 ----------
def generate_prompt(month_input):
    """
    根据输入月份生成财务分析提示词
    """
    # 从文件加载提示词模板
    return load_prompt("bank_statement_prompt").format(month=month_input).strip()

# %% [code]
# ---------- 工具函数 ----------
def sanitize_column_name(name):
    """清理列名，确保符合SQLite标识符规范"""
    cleaned_name = re.sub(r'\W+', '', name)
    if cleaned_name and cleaned_name[0].isdigit():
        cleaned_name = 'col_' + cleaned_name
    return cleaned_name.lower()

def get_column_type(column_name):
    """根据列名确定数据类型"""
    money_keywords = ['金额', '退款', '价格', '费用', '余额', '服务费']
    if any(keyword in column_name for keyword in money_keywords):
        return 'REAL'
    return 'TEXT'

def is_money_column(column_name):
    """判断是否为金额列"""
    money_keywords = ['金额', '退款', '价格', '费用', '余额', '服务费']
    return any(keyword in column_name for keyword in money_keywords)

def clean_money_value(value):
    """清理金额数据"""
    if not value:
        return 0.0
    cleaned_value = value.replace('¥', '').replace(',', '').strip()
    try:
        return float(cleaned_value)
    except ValueError:
        return cleaned_value

def rename_existing_table(cursor, table_name):
    """检查表是否存在，如果存在则将其重命名"""
    try:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        if cursor.fetchone():
            new_table_name = f"{table_name}_"
            while True:
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (new_table_name,))
                if not cursor.fetchone():
                    break
                new_table_name += "_"
            return True
        return False
    except Exception as e:
        print(f"重命名表时发生错误: {e}")
        return False

def get_current_table_name():
    """获取当前月份对应的表名"""
    return f"pay_{CURRENT_MONTH}"

def get_month_date_range(monthyear):
    """获取指定月份的起止日期"""
    year = int(monthyear[:4])
    month = int(monthyear[4:])
    _, last_day = calendar.monthrange(year, month)
    start_date = f"{year}-{month:02d}-01"
    end_date = f"{year}-{month:02d}-{last_day}"
    return start_date, end_date

def get_data_path(data_type, month_year):
    """
    获取数据文件路径
    :param data_type: 'wx', 'zfb' 或 'bank'
    :param month_year: 格式为YYYYMM，如202507
    :return: 文件路径
    """
    return str(DATA_DIR / data_type / f"{month_year}.csv")

def prepare_data_files(month_year):
    """
    准备数据文件：重命名并移动到指定目录
    :param month_year: 格式为YYYYMM，如202507
    """
    # 基础路径
    base_path = Path("/home/luany/桌面/moneycount")
    source_dir = base_path / month_year
    data_dir = DATA_DIR
    
    # 确保目标目录存在
    for subdir in ["wx", "zfb", "bank"]:
        (data_dir / subdir).mkdir(parents=True, exist_ok=True)
    
    # 处理微信文件
    for file in source_dir.glob("*微信*"):
        if file.suffix in [".xlsx", ".csv"]:
            target = data_dir / "wx" / f"{month_year}.csv"
            if file.suffix == ".xlsx":
                # 需要先将Excel转为CSV
                import pandas as pd
                pd.read_excel(file).to_csv(target, index=False)
            else:
                shutil.copy(file, target)
            print(f"微信文件已处理: {file} -> {target}")
            break
    
    # 处理支付宝文件
    for file in source_dir.glob("*alipay*"):
        target = data_dir / "zfb" / f"{month_year}.csv"
        shutil.copy(file, target)
        print(f"支付宝文件已处理: {file} -> {target}")
        break
    
    # 处理银行文件
    for file in source_dir.glob("*bank*"):
        target = data_dir / "bank" / f"{month_year}.csv"
        shutil.copy(file, target)
        print(f"银行文件已处理: {file} -> {target}")
        break

# %% [code]
# ---------- CSV导入功能（增加敏感词处理）----------
def import_csv_to_sqlite(csv_file_path, strategy, header_row=0, start_row=0, skip_columns=None, params=None):
    """
    导入CSV文件到SQLite数据库（增加敏感词处理）
    :param csv_file_path: CSV文件路径
    :param strategy: 导入策略 ('wx', 'zfb', 'yh', 'other')
    :param header_row: 表头所在的行号（从0开始）
    :param start_row: 数据开始的行号（从0开始）
    :param skip_columns: 要跳过的列索引列表
    :param params: 策略所需的额外参数
    """
    if skip_columns is None:
        skip_columns = []
    if params is None:
        params = {}
    
    # 添加月份参数
    params['monthyear'] = CURRENT_MONTH
    table_name = get_current_table_name()

    try:
        encodings = ['gb2312', 'gbk', 'gb18030', 'utf-8']
        
        for encoding in encodings:
            try:
                conn = sqlite3.connect(DB_PATH)
                cursor = conn.cursor()

                # 首先导入CSV到临时表
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
                    strategy_table_name = f"t_{table_name}_{strategy}"

                    # 重命名已存在的表或创建新表
                    if rename_existing_table(cursor, strategy_table_name):
                        print(f"表 {strategy_table_name} 已存在并已重命名")
                    
                    # 创建临时表
                    create_strategy_table_query = f"""
                        CREATE TABLE IF NOT EXISTS {strategy_table_name} (
                            {', '.join([f'[{col}] {get_column_type(col)}' for col in cleaned_headers])}
                        )
                    """
                    cursor.execute(create_strategy_table_query)

                    # 跳过start_row之前的行
                    remaining_rows_to_skip = start_row - (header_row + 1)
                    if remaining_rows_to_skip > 0:
                        for _ in range(remaining_rows_to_skip):
                            next(csv_reader)

                    # 插入数据到临时表
                    for row in csv_reader:
                        if not row:  # 跳过空行
                            continue

                        filtered_row = []
                        for i, value in enumerate(row):
                            if i not in skip_columns:
                                if i < len(cleaned_headers) and is_money_column(cleaned_headers[i]):
                                    filtered_row.append(clean_money_value(value))
                                else:
                                    filtered_row.append(value)

                        if len(filtered_row) == len(cleaned_headers):
                            placeholders = ','.join(['?' for _ in filtered_row])
                            insert_query = f"INSERT INTO {strategy_table_name} VALUES ({placeholders})"
                            try:
                                cursor.execute(insert_query, filtered_row)
                            except Exception as e:
                                print(f"插入失败: {e}")

                # 创建正式表 - 使用SQL模板
                create_table_sql = load_sql_template(
                    "create_table", 
                    {"table_name": table_name}
                )
                if create_table_sql:
                    cursor.execute(create_table_sql)
                else:
                    # 后备方案
                    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (id BIGINT NOT NULL, pay_time VARCHAR NOT NULL, pay_monthyear VARCHAR NOT NULL, pay_source VARCHAR, pay_note VARCHAR, pay_money NUMERIC NOT NULL, pay_tag VARCHAR, app_source VARCHAR, PRIMARY KEY (id, pay_monthyear, pay_money))")

                # 根据策略执行相应的SQL将数据导入正式表
                if strategy == 'wx':
                    # 微信支出
                    wx_expense_sql = load_sql_template(
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
                    wx_income_sql = load_sql_template(
                        "wx_income",
                        {
                            "table_name": table_name,
                            "strategy_table_name": strategy_table_name,
                            "monthyear": params.get('monthyear')
                        }
                    )
                    if wx_income_sql:
                        cursor.execute(wx_income_sql)

                elif strategy == 'zfb':
                    # 支付宝支出
                    zfb_expense_sql = load_sql_template(
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
                    zfb_refund_sql = load_sql_template(
                        "zfb_refund",
                        {
                            "table_name": table_name,
                            "strategy_table_name": strategy_table_name,
                            "monthyear": params.get('monthyear')
                        }
                    )
                    if zfb_refund_sql:
                        cursor.execute(zfb_refund_sql)

                elif strategy == 'yh':
                    # 银行信用卡策略SQL
                    print("银行信用卡导入策略尚未实现")

                elif strategy == 'bank':
                    # 银行账单导入
                    bank_sql = load_sql_template(
                        "bank_import",
                        {
                            "table_name": table_name,
                            "strategy_table_name": strategy_table_name,
                            "monthyear": params.get('monthyear')
                        }
                    )
                    if bank_sql:
                        cursor.execute(bank_sql)

                elif strategy == 'other':
                    # 其他SQL
                    print("其他导入策略尚未实现")
                
                # 应用敏感词过滤
                if SENSITIVE_WORDS:
                    # 对pay_source应用敏感词过滤
                    update_source_sql = load_sql_template(
                        "update_sensitive_source",
                        {"table_name": table_name}
                    )
                    
                    # 对pay_note应用敏感词过滤
                    update_note_sql = load_sql_template(
                        "update_sensitive_note",
                        {"table_name": table_name}
                    )
                    
                    # 获取所有需要处理的记录
                    cursor.execute(f"SELECT id, pay_source, pay_note FROM {table_name}")
                    records = cursor.fetchall()
                    
                    for record in records:
                        record_id, pay_source, pay_note = record
                        
                        # 应用敏感词过滤
                        clean_source = apply_sensitive_word_filter(pay_source)
                        clean_note = apply_sensitive_word_filter(pay_note)
                        
                        # 更新记录
                        if update_source_sql:
                            cursor.execute(update_source_sql, (clean_source, record_id, CURRENT_MONTH))
                        if update_note_sql:
                            cursor.execute(update_note_sql, (clean_note, record_id, CURRENT_MONTH))
                    
                    print(f"已对 {len(records)} 条记录应用敏感词过滤")

                # 提交事务
                conn.commit()
                print(f"成功使用 {encoding} 编码导入CSV并执行{strategy}策略")
                return

            except UnicodeDecodeError:
                conn.close()
                continue
            except sqlite3.Error as e:
                print(f"SQLite错误: {e}")
                conn.close()

        print("无法使用指定的任何编码解码文件")

    except Exception as e:
        print(f"发生错误: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

# %% [code]
# ---------- 查询功能 ----------
def query_payment_data(params):
    """
    查询支付数据
    :param params: 查询参数字典
    :return: (success, result)
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        table_name = get_current_table_name()
        
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
        
        # 构建完整的SQL查询
        query = f"SELECT * FROM {table_name}"
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY pay_money DESC"
        
        # 执行查询
        cursor.execute(query, query_params)
        
        # 获取列名
        columns = [description[0] for description in cursor.description]
        
        # 将结果转换为字典列表
        results = []
        for row in cursor.fetchall():
            results.append(dict(zip(columns, row)))
        
        return True, results
        
    except sqlite3.Error as e:
        return False, f"数据库错误: {str(e)}"
    finally:
        if 'conn' in locals():
            conn.close()

def display_query_results(results, params):
    """
    使用rich库显示查询结果
    """
    console = Console()
    
    if not results:
        console.print("[red]没有找到数据[/red]")
        return
    
    # 创建表格
    table = Table(
        title=f"支付记录 ({CURRENT_MONTH})",
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
def update_payment_tags(params):
    """
    批量更新支付标签
    :param params: 更新参数字典
    :return: (success, message)
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        table_name = get_current_table_name()
        
        # 检查表是否存在
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        if not cursor.fetchone():
            return False, f"表格 {table_name} 不存在"
        
        # 检查标签词是否在映射中
        tag_word = params.get('tag_word')
        if tag_word not in TAG_MAPPING:
            return False, f"不支持的标签词: {tag_word}"
            
        new_tag = TAG_MAPPING[tag_word]
        
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
        update_sql = load_sql_template(
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

def auto_update_tags_based_on_history():
    """根据历史记录习惯自动更新标签"""
    results = []
    for source, tag in AUTO_TAG_MAPPING.items():
        params = {
            "key": source,
            "tag_word": tag
        }
        success, message = update_payment_tags(params)
        results.append(f"{source} -> {tag}: {message}")
    return results

# %% [code]
# ---------- 手动添加平账记录功能 ----------
def add_manual_balance_record(params):
    """
    手动添加平账记录到数据库
    :param params: 包含以下键的字典:
        - pay_time: 交易时间 (可选，默认为当月2号)
        - pay_source: 交易来源
        - pay_note: 交易备注
        - pay_money: 交易金额 (应为负数)
        - monthyear: 年月 (YYYYMM格式)
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # 设置默认值
        if 'pay_time' not in params or not params['pay_time']:
            # 默认为当月2号 00:00:00
            pay_date = datetime.datetime.strptime(params['monthyear'], "%Y%m").replace(day=2)
            params['pay_time'] = pay_date.strftime("%Y-%m-%d %H:%M:%S")
        
        # 生成ID (使用时间戳)
        id = str(int(datetime.datetime.strptime(params['pay_time'], "%Y-%m-%d %H:%M:%S").timestamp()))
        
        # 获取表名
        table_name = f"pay_{params['monthyear']}"
        
        # 检查表是否存在
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        if not cursor.fetchone():
            # 表不存在则创建
            create_table_sql = load_sql_template(
                "create_table", 
                {"table_name": table_name}
            )
            if create_table_sql:
                cursor.execute(create_table_sql)
            else:
                # 后备方案
                cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (id BIGINT NOT NULL, pay_time VARCHAR NOT NULL, pay_monthyear VARCHAR NOT NULL, pay_source VARCHAR, pay_note VARCHAR, pay_money NUMERIC NOT NULL, pay_tag VARCHAR, app_source VARCHAR, PRIMARY KEY (id, pay_monthyear, pay_money))")
        
        # 加载插入SQL模板
        insert_sql = load_sql_template(
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
            params['monthyear'],
            params['pay_source'],
            params['pay_note'],
            float(params['pay_money']),  # 确保转换为float
            'social_ex',
            'add'
        ))
        
        conn.commit()
        return True, "平账记录添加成功"
    
    except ValueError as ve:
        return False, f"金额格式错误: {str(ve)}"
    except Exception as e:
        return False, f"添加平账记录失败: {str(e)}"
    finally:
        if 'conn' in locals():
            conn.close()

# %% [code]
# ---------- 图表分析功能 ----------
def query_monthly_data():
    """查询月度数据"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        table_name = get_current_table_name()
        
        # 检查表是否存在
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        if not cursor.fetchone():
            return False, f"表格 {table_name} 不存在"
        
        # 查询所有数据
        query_sql = load_sql_template(
            "query_monthly_data",
            {"table_name": table_name}
        )
        
        if not query_sql:
            # 后备方案
            query_sql = f"SELECT pay_time, pay_money, pay_tag FROM {table_name}"
        
        cursor.execute(query_sql)
        
        # 转换为DataFrame
        df = pd.DataFrame(cursor.fetchall(), columns=['pay_time', 'pay_money', 'pay_tag'])
        
        # 处理日期格式
        df['pay_time'] = pd.to_datetime(df['pay_time'], errors='coerce', format='mixed')
        
        # 处理可能的NaT值
        df = df.dropna(subset=['pay_time'])
        
        return True, df
        
    except sqlite3.Error as e:
        return False, f"数据库错误: {str(e)}"
    finally:
        if 'conn' in locals():
            conn.close()

def plot_monthly_charts(df):
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
    percentages = tag_sum / total * 100
    
    labels = [f'{tag}\n¥{amount:.2f}\n({percent:.1f}%)' 
              for tag, amount, percent in zip(tag_sum.index, tag_sum, percentages)]
    
    ax1.pie(tag_sum, labels=labels, autopct='', startangle=90)
    ax1.set_title(f'{CURRENT_MONTH[:4]}年{CURRENT_MONTH[4:]}月消费类别分布')
    
    # 2. 柱状图 - 按日期统计
    start_date, end_date = get_month_date_range(CURRENT_MONTH)
    date_range = pd.date_range(start=start_date, end=end_date)
    
    # 按日期分组求和
    daily_sum = df.groupby('pay_time')['pay_money'].sum()
    
    # 创建完整的日期序列，填充缺失值为0
    daily_sum_full = pd.Series(0, index=date_range)
    daily_sum_full.update(daily_sum)
    
    # 绘制柱状图
    bars = ax2.bar(daily_sum.index.strftime('%d'), daily_sum.values)
    ax2.set_title(f'{CURRENT_MONTH[:4]}年{CURRENT_MONTH[4:]}月每日消费金额')
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
# ---------- 示例使用 ----------
if __name__ == "__main__":
    # 设置当前月份
    CURRENT_MONTH = "202506"
    
    # 动态生成文件路径
    def get_data_path(source, month=CURRENT_MONTH):
        """根据数据源和月份生成文件路径"""
        return f"data/{source}/{month}.csv"
    
    # 模式选择
    # 扩展模式选择
    def select_modes():
        """让用户选择要执行的功能模块"""
        print("\n请选择要执行的功能（可多选，用逗号分隔）:")
        print("1. import - 导入数据")
        print("2. auto_update - 自动更新标签")
        print("3. manual_update - 手动更新标签")  # 新增手动更新选项
        print("4. query - 查询数据")
        print("5. chart - 绘制图表")
        print("6. deepseek - 生成提示词用于deepseek和识别分期账单")
        print("7. balance - 手动添加平账记录")  # 新增平账记录选项
        
        choices = input("请输入选择（如：1,2,4）: ").strip().split(',')
        modes = []
        for choice in choices:
            choice = choice.strip()
            if choice == '1':
                modes.append('import')
            elif choice == '2':
                modes.append('auto_update')
            elif choice == '3':  # 新增手动更新
                modes.append('manual_update')
            elif choice == '4':
                modes.append('query')
            elif choice == '5':
                modes.append('chart')
            elif choice == '6':
                modes.append('deepseek')
            elif choice == '7':  # 新增平账记录
                modes.append('balance')
        return modes

    
    # 获取用户选择
    selected_modes = select_modes()
    if not selected_modes:
        print("未选择任何功能，程序退出")
        exit()
    
    print(f"\n当前处理月份: {CURRENT_MONTH}")
    print(f"将执行的功能: {', '.join(selected_modes)}")
    
    # 执行选中的功能
    if 'import' in selected_modes:
        print("\n=== 导入数据 ===")

        # 准备文件
        prepare_data_files(CURRENT_MONTH)

        # 导入微信数据
        print("\n导入微信数据...")
        import_csv_to_sqlite(
            csv_file_path=get_data_path('wx'),
            strategy='wx',
            header_row=16,
            start_row=17,
            skip_columns=[8, 9]
        )

        # 导入支付宝数据
        print("\n导入支付宝数据...")
        import_csv_to_sqlite(
            csv_file_path=get_data_path('zfb'),
            strategy='zfb',
            header_row=4,
            start_row=5,
            skip_columns=[0, 1, 3, 16]
        )

        # 导入分期数据
        print("\n导入分期数据...")
        import_csv_to_sqlite(
            csv_file_path=get_data_path('bank'),
            strategy='bank',
            header_row=0,
            start_row=1
        )
    
    if 'auto_update' in selected_modes:
        print("\n=== 自动更新标签 ===")
        update_results = auto_update_tags_based_on_history()
        for result in update_results:
            print(result)

    if 'manual_update' in selected_modes:
        print("\n=== 手动更新标签 ===")
        up_params = {
            "monthyear": CURRENT_MONTH,
            "key": input("输入要更新的关键词（如'肯德基'）: ").strip(),
            "tag_word": input("输入标签词（如'主餐'）: ").strip(),
            "lt": input("输入最大金额（留空跳过）: ").strip() or "",
            "gt": input("输入最小金额（留空跳过）: ").strip() or ""
        }
        
        # 检查标签词是否有效
        if up_params["tag_word"] not in TAG_MAPPING:
            print(f"错误: 无效的标签词 '{up_params['tag_word']}'")
            print("可用标签词:", ", ".join(TAG_MAPPING.keys()))
        else:
            success, message = update_payment_tags(up_params)
            print("\n" + message)
    
    if 'query' in selected_modes:
        print("\n=== 查询数据 ===")
        query_params = {
            "tag": input("输入标签筛选（留空跳过）: ").strip() or "",
            "key": input("输入关键词筛选（留空跳过）: ").strip() or "",
            "lt": input("输入最大金额（留空跳过）: ").strip() or "",
            "gt": input("输入最小金额（留空跳过）: ").strip() or ""
        }
        success, results = query_payment_data(query_params)
        if success:
            display_query_results(results, query_params)
        else:
            print(f"查询失败: {results}")
    
    if 'chart' in selected_modes:
        print("\n=== 绘制图表 ===")
        success, df = query_monthly_data()
        if success:
            fig = plot_monthly_charts(df)
            plt.show()
            
            # 输出统计信息
            print("\n统计信息:")
            print(f"总消费: ¥{df['pay_money'].sum():.2f}")
            print(f"平均每日消费: ¥{df['pay_money'].sum() / len(df['pay_money']):.2f}")
            print("\n按标签统计:")
            tag_stats = df.groupby('pay_tag').agg({
                'pay_money': ['sum', 'count']
            })
            print(tag_stats)
        else:
            print(f"数据查询失败: {df}")
    
    if 'ocr' in selected_modes:
        print("\n=== 处理银行对账单 ===")
        bank_image = input("请输入银行对账单图片路径: ").strip()
        if bank_image:
            csv_path = import_ocr_bank_statement(bank_image)
            import_bank_csv_to_db(csv_path)
        else:
            print("未提供图片路径，跳过OCR处理")

    if 'deepseek' in selected_modes:
        prompt_text = generate_prompt(CURRENT_MONTH)
        print("\n" + prompt_text)
    
    if 'balance' in selected_modes:
        print("\n=== 手动添加平账记录 ===")
        balance_params = {
            "monthyear": CURRENT_MONTH,
            "pay_time": input("输入交易时间 (YYYY-MM-DD HH:MM:SS，留空使用当月2号): ").strip(),
            "pay_source": input("输入交易来源: ").strip(),
            "pay_note": input("输入交易备注: ").strip(),
            "pay_money": input("输入交易金额 (应为负数): ").strip()
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
        
        success, message = add_manual_balance_record(balance_params)
        print("\n" + message)
    
    print("\n所有选定功能执行完成！")