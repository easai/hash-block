-- This query lists monthly average weights, total transaction counts, maximum sizes, total block counts, and largest block hash values in 2020.
WITH MonthlyBlockStats AS (
  -- Calculates monthly average weights, total transaction counts, maximum sizes, and total block counts.
  SELECT
    timestamp_month,
    AVG(weight) AS avg_weight,
    SUM(transaction_count) AS total_transactions,
    MAX(size) AS max_size,
    COUNT(*) AS total_blocks
  FROM
    `bigquery-public-data.crypto_bitcoin.blocks`
  WHERE
    timestamp BETWEEN '2020-01-01' AND '2020-12-31'
  GROUP BY
    timestamp_month
),
LargestBlockHashes AS (
  -- Selects the largest block hash values for each month.
  SELECT
    timestamp_month,
    size,
    ARRAY_AGG(`hash` ORDER BY size DESC LIMIT 1)[OFFSET(0)] AS largest_block_hash
  FROM
    `bigquery-public-data.crypto_bitcoin.blocks`
  WHERE
    timestamp BETWEEN '2020-01-01' AND '2020-12-31'
  GROUP BY
    timestamp_month, size
)
-- Selects the months, average weights, transaction totals, maximum sizes, largest block hash values, and total block counts.
SELECT
  mbs.timestamp_month,
  mbs.avg_weight,
  mbs.total_transactions,
  mbs.max_size,
  lbh.largest_block_hash,
  mbs.total_blocks,
  -- Compares the average weight with the last month's value. Sets the value to 'Increased' if it is increasing, 'Decreased' if it is decreasing, and 'Stable' otherwise.
  CASE 
    WHEN mbs.avg_weight > LAG(mbs.avg_weight) OVER (ORDER BY mbs.timestamp_month)
    THEN 'Increased'
    WHEN mbs.avg_weight < LAG(mbs.avg_weight) OVER (ORDER BY mbs.timestamp_month)
    THEN 'Decreased'
    ELSE 'Stable'
  END AS avg_weight_trend,
  -- Compares the total number of transactions with the last month's value. Sets the value to 'Increased' if it is increasing, 'Decreased' if it is decreasing, and 'Stable' otherwise.
  CASE
    WHEN mbs.total_transactions > LAG(mbs.total_transactions) OVER (ORDER BY mbs.timestamp_month)
    THEN 'Increased'
    WHEN mbs.total_transactions < LAG(mbs.total_transactions) OVER (ORDER BY mbs.timestamp_month)
    THEN 'Decreased'
    ELSE 'Stable'
  END AS total_transactions_trend
FROM
  MonthlyBlockStats mbs
-- Joins CTE MonthlyBlockStats with CTE LargestBlockHashes by month.
JOIN
  LargestBlockHashes lbh ON mbs.timestamp_month = lbh.timestamp_month AND mbs.max_size = lbh.size
ORDER BY
  mbs.timestamp_month;
  
