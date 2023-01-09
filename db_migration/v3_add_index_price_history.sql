SET @ticker_date_index_exists := (SELECT COUNT(*)
  FROM information_schema.statistics
  WHERE table_schema = database()
  AND table_name = 'price_history' AND index_name like 'ticker_date_idx');

set @sqlstmt := if(NOT @ticker_date_index_exists,
  'ALTER TABLE price_history ADD UNIQUE KEY ticker_date_idx (ticker, date)',
  'SELECT ''ticker_date_idx already exists''');

PREPARE stmt FROM @sqlstmt;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;