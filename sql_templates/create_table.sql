-- 创建支付记录表
CREATE TABLE IF NOT EXISTS $$table_name$$  (
  id BIGINT NOT NULL,
  pay_time VARCHAR NOT NULL,
  pay_monthyear VARCHAR NOT NULL,
  pay_source VARCHAR,
  pay_note VARCHAR,
  pay_money NUMERIC NOT NULL,
  pay_tag VARCHAR,
  app_source VARCHAR,
  PRIMARY KEY (id, pay_monthyear, pay_money)