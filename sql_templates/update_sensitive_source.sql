-- 更新敏感词 - 来源字段
UPDATE $$table_name$$
SET pay_source = ?
WHERE id = ? AND pay_monthyear = ?