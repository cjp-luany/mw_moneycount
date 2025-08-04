INSERT INTO [$$table_name$$] (id, pay_time, pay_monthyear, pay_source, pay_note, pay_money, pay_tag, app_source)
SELECT
  strftime('%s', [pay_time]) as id,
  [pay_time] as pay_time,
  '$$monthyear$$' as pay_monthyear,
  [pay_source] as pay_source,
  [pay_note] as pay_note,
  CASE 
    WHEN [pay_note] LIKE '%[退款]%' OR [pay_note] LIKE '%退款%' THEN -cast([pay_money] as real)
    ELSE cast([pay_money] as real)
  END as pay_money,
  'credit' as pay_tag,
  'bank' as app_source
FROM '$$strategy_table_name$$'