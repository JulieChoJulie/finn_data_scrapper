CREATE TABLE IF NOT EXISTS price_history
  (date DATE, open DOUBLE, close DOUBLE, adj_close DOUBLE,
   high DOUBLE, low DOUBLE, volumn BIGINT, ticker BIGINT);