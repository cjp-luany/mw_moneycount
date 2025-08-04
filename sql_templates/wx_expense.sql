-- 微信支出记录导入
INSERT INTO [$$table_name$$] (id, pay_time, pay_monthyear, pay_source, pay_note, pay_money, pay_tag, app_source)
SELECT
  strftime('%s',[交易时间]) as id,
  [交易时间] as pay_time,
  '$$monthyear$$' as pay_monthyear,
  [交易对方] as pay_source,
  [商品] as pay_note,
  cast([金额元] as real)  as pay_money,
  'N' as 'pay_tag',
  'wx' as 'app_source'
FROM '$$strategy_table_name$$' where [收支]='支出'