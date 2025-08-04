# MoneyCount - 个人财务分析工具

## 1. 项目概述
MoneyCount 是一个基于 Python 和 Jupyter Notebook 的个人财务分析工具，旨在简化个人记账和财务分析流程。项目通过自动化处理微信、支付宝和银行账单数据，提供强大的记账、分类统计、可视化分析和智能报表功能。

### 核心功能
- **账单数据导入**：支持微信、支付宝和银行账单的CSV导入
- **智能分类系统**：自动标签映射和手动标签管理
- **多维度查询**：按标签、金额范围、关键词等条件筛选数据
- **可视化分析**：月度消费分布图、每日消费趋势图等
- **敏感信息过滤**：保护用户隐私数据
- **平账记录管理**：手动添加调整记录

## 2. 环境安装

### 前置要求
- Python 3.7+
- Jupyter Notebook/Lab

### 安装步骤
```bash
# 克隆仓库
git clone https://github.com/yourusername/moneycount.git
cd moneycount

# 创建虚拟环境
python -m venv venv
source venv/bin/activate  # Linux/MacOS
venv\Scripts\activate    # Windows

# 安装依赖
pip install -r requirements.txt

# 启动Jupyter
jupyter notebook
```

### DeepSeek集成
项目集成了DeepSeek API用于智能账单分析：
1. 获取DeepSeek API密钥
2. 在配置文件中设置API密钥
3. 使用`generate_prompt()`函数生成分析提示词

## 3. 数据准备

### 账单文件要求
1. **微信支付**：
   - 导出路径：微信支付 > 账单 > 导出账单
   - 格式要求：CSV格式

2. **支付宝**：
   - 导出路径：支付宝 > 账单 > 右上角导出
   - 格式要求：CSV格式

3. **银行账单**：
   - 从网银系统导出交易记录
   - 格式要求：CSV格式

### 文件命名规范
```
data/
├── wx/         # 微信账单
│   └── 202506.csv
├── zfb/        # 支付宝账单
│   └── 202506.csv
└── bank/       # 银行账单
    └── 202506.csv
```

## 4. 快速上手

### 基本工作流程
1. **配置当前月份**：
   ```python
   CURRENT_MONTH = "202506"  # 修改为当前处理月份
   ```

2. **准备数据文件**：
   ```python
   prepare_data_files(CURRENT_MONTH)
   ```

3. **导入账单数据**：
   ```python
   # 导入微信账单
   wx_path = get_data_path('wx', CURRENT_MONTH)
   import_csv_to_sqlite(wx_path, 'wx')
   
   # 导入支付宝账单
   zfb_path = get_data_path('zfb', CURRENT_MONTH)
   import_csv_to_sqlite(zfb_path, 'zfb')
   
   # 导入银行账单
   bank_path = get_data_path('bank', CURRENT_MONTH)
   import_csv_to_sqlite(bank_path, 'bank')
   ```

4. **数据查询与分析**：
   ```python
   # 查询特定标签消费
   params = {'tag': 'food'}
   success, results = query_payment_data(params)
   display_query_results(results, params)
   
   # 生成月度消费图表
   success, df = query_monthly_data()
   fig = plot_monthly_charts(df)
   plt.show()
   ```

5. **使用DeepSeek分析**：
   ```python
   prompt = generate_prompt(CURRENT_MONTH)
   print("将以下提示词复制到DeepSeek进行分析：")
   print(prompt)
   ```

## 5. 项目结构

```
moneycount/
├── config/                  # 配置文件
│   ├── tag_mapping.json     # 标签映射配置
│   ├── auto_tag_mapping.json # 自动标签规则
│   └── sensitive_words.json # 敏感词过滤
├── data/                    # 账单数据
├── prompts/                 # DeepSeek提示词模板
├── sql_templates/           # SQL查询模板
├── moneycount.ipynb         # 主笔记本文件
├── README.md                # 项目文档
└── requirements.txt         # 依赖列表
```

## 6. TODO 列表

### 近期开发计划
- [ ] **服务器部署**：将应用迁移到Flask/Django框架
- [ ] **RESTful API开发**：创建数据访问和分析接口
- [ ] **移动端应用**：开发iOS/Android客户端
- [ ] **自动化报表**：添加定期邮件报告功能
- [ ] **多用户支持**：实现用户账户系统

### 未来功能规划
- [ ] **投资分析模块**：股票、基金收益跟踪
- [ ] **预算管理**：设置和跟踪消费预算
- [ ] **债务管理**：信用卡、贷款跟踪
- [ ] **多货币支持**：自动汇率转换
- [ ] **云同步**：支持Dropbox/Google Drive同步

## 7. 贡献指南

欢迎贡献代码！请遵循以下流程：
1. Fork 仓库
2. 创建特性分支 (`git checkout -b feature/your-feature`)
3. 提交更改 (`git commit -am 'Add some feature'`)
4. 推送到分支 (`git push origin feature/your-feature`)
5. 创建Pull Request

## 8. 技术支持
[保留，这部分还没做]
遇到问题？请参考：
- [问题追踪](https://github.com/yourusername/moneycount/issues)
- 邮箱支持：support@moneycount.com
- 社区论坛：https://community.moneycount.com

## 9. 许可协议

本项目采用 [MIT License](LICENSE)，欢迎自由使用和修改。使用DeepSeek API时请遵守其相关条款。

---

**让财务管理变得简单高效 - MoneyCount助您掌握每一分钱的去向！**