INSERT INTO [$$table_name$$] (id, pay_time, pay_monthyear, pay_source, pay_note, pay_money, pay_tag, app_source) 
SELECT
  strftime('%s',[交易创建时间]) as id,
  [交易创建时间] as pay_time,
  '$$monthyear$$' as pay_monthyear,
  [交易对方] as pay_source,
  [商品名称] as pay_note,
  [金额元] as pay_money,
  'N' as 'pay_tag',
  'ali' as 'app_source'
FROM '$$strategy_table_name$$'  where [收支] like '支出%'  and [交易状态] like '交易成功%' and [成功退款元]=0