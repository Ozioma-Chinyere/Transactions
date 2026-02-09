-- Create the transaction records table in the database:

DROP TABLE IF EXISTS transactions_tb;

CREATE TABLE IF NOT EXISTS transactions_tb (
    serial_number INT,
    user_id NUMERIC,
    event_time TIMESTAMP WITH TIME ZONE,
    order_id NUMERIC,
    product_id NUMERIC,
    category_id NUMERIC,
    category_code VARCHAR(100),
    brand VARCHAR(25),
    price DECIMAL(8, 2)
);

-- View the structure of the created table:
SELECT * FROM transactions_tb;

-- Load data into the table from 2 csv files using the copy command
COPY transactions_tb 
FROM 'C:\ProgramData\MySQL\MySQL Server 8.0\Uploads\input_files\raw_sql_transaction_record_1.csv'
WITH (FORMAT CSV, HEADER);

-- Load 2nd csv file
COPY transactions_tb 
FROM 'C:\ProgramData\MySQL\MySQL Server 8.0\Uploads\input_files\raw_sql_transaction_record_2.csv'
WITH (FORMAT CSV, HEADER);

-- Confirm total no. of rows.
SELECT COUNT(*) 
FROM transactions_tb;

-- Drop the serial_number column.
ALTER TABLE transactions_tb
DROP COLUMN serial_number;

-- CONFIRM table structure
SELECT * 
FROM transactions_tb
LIMIT 5;

-- check for null values in all the columns
SELECT
  COUNT(*) FILTER (WHERE user_id IS NULL) AS user_id,
  COUNT(*) FILTER (WHERE event_time IS NULL) AS event_time,
  COUNT(*) FILTER (WHERE order_id IS NULL) AS order_id,
  COUNT(*) FILTER (WHERE product_id IS NULL) AS product_id,
  COUNT(*) FILTER (WHERE category_id IS NULL) AS category_id,
  COUNT(*) FILTER (WHERE category_code IS NULL) AS category_code,
  COUNT(*) FILTER (WHERE brand IS NULL) AS brand,
  COUNT(*) FILTER (WHERE price IS NULL) AS price
 FROM transactions_tb;
 -- missing values: user_id = 1637398, category_code = 612202, brand = 112670

SELECT user_id, COUNT(*) AS total_per_userid
FROM transactions_tb
GROUP BY user_id
ORDER BY total_per_userid DESC;
-- I suspect that the user_id column records the data of the staff that carried out the transaction.
-- This suggests that 1637398 rows were recorded without the processing staff inputing their user_id


/*I think the user_id data records the id of the staff that handled the sale. 
This data is not very relevant for analysis since there is no further data on users. Data cleaning efforts would 
therefore focus on the category_code and brand columns because those columns would be relevant to 
the analysis.*/

-- DATA CLEANING

-- Total count of rows to be deleted
SELECT COUNT(*)
FROM transactions_tb
WHERE category_code IS NULL OR brand IS NULL; 
-- 669392 

-- View a sample of these rows before deletion
SELECT *
FROM transactions_tb
WHERE category_code IS NULL OR brand IS NULL
LIMIT 20; 

START TRANSACTION;

DELETE
FROM transactions_tb
WHERE category_code IS NULL OR brand IS NULL;


SELECT COUNT(*)
FROM transactions_tb
WHERE category_code IS NULL OR brand IS NULL;

SELECT *
FROM transactions_tb
WHERE category_code IS NULL OR brand IS NULL;

-- Confirm total no. of rows.
SELECT COUNT(*) 
FROM transactions_tb;
-- Remaining rows - 1,532,172 (2201567 - 669392)

-- random sampling of data to confirm clean data.
SELECT *
FROM transactions_tb
LIMIT 10 OFFSET 100;

-- confirm that there are no null values for category_code and brand columns.
SELECT
  COUNT(*) FILTER (WHERE user_id IS NULL) AS user_id,
  COUNT(*) FILTER (WHERE event_time IS NULL) AS event_time,
  COUNT(*) FILTER (WHERE order_id IS NULL) AS order_id,
  COUNT(*) FILTER (WHERE product_id IS NULL) AS product_id,
  COUNT(*) FILTER (WHERE category_id IS NULL) AS category_id,
  COUNT(*) FILTER (WHERE category_code IS NULL) AS category_code,
  COUNT(*) FILTER (WHERE brand IS NULL) AS brand,
  COUNT(*) FILTER (WHERE price IS NULL) AS price
 FROM transactions_tb;
 -- user_id = 1111457 missing values.

-- save changes
COMMIT;

-- EXPLORATORY DATA ANALYSIS

-- Create a view with year, month, day, weekday and hour extracted from event_time

DROP VIEW IF EXISTS transactions_vw;

CREATE VIEW transactions_vw AS (
SELECT user_id,
event_time,
EXTRACT(YEAR FROM event_time) AS year,
EXTRACT(MONTH FROM event_time) AS month,
EXTRACT(DAY FROM event_time) AS day,
TO_CHAR(event_time, 'Day') AS weekday,
EXTRACT(HOUR FROM event_time) AS hour,
order_id,
product_id,
category_id,
category_code,
brand,
price
FROM transactions_tb
);

-- Preview transactions_vw
SELECT *
FROM transactions_vw
LIMIT 10;

-- Find the time range captured in the data:
SELECT MIN(event_time), MAX(event_time)
FROM transactions_vw;
-- Shows a range between 1970 and 2020 - suggests a 50 year timeframe.

-- Sales per year
SELECT year, COUNT(*) AS yearly_sales
FROM transactions_vw
GROUP BY year;
-- only 2 years 1970 and 2020 are captured in the dataset.

-- Analysis on 1970 data
-- Time Series Analysis for 1970 data
SELECT month, COUNT (*)
FROM transactions_vw
WHERE year = 1970
GROUP BY month
ORDER BY month;
-- all of 1970 records were for month 1

SELECT day, COUNT (*)
FROM transactions_vw
WHERE year = 1970
GROUP BY day
ORDER BY day;
-- all of 1970 records were under day 1.

SELECT weekday, COUNT (*)
FROM transactions_vw
WHERE year = 1970
GROUP BY weekday
ORDER BY weekday;
-- all for Thursday

-- hourly sales
SELECT hour, COUNT(*) AS hourly_sales
FROM transactions_vw
WHERE year = 1970
GROUP BY hour
ORDER BY hour;
-- all for 8am.

-- how much missing values for user_id in the 1970  data?
SELECT COUNT(*)
FROM transactions_vw
WHERE year = 1970
AND user_id IS NULL;
-- 9428 from 10256 means that only 828 out of 1970 data was entered with the user_id

-- 1970 sales trends.

-- Total categories in 1970
SELECT COUNT(DISTINCT category_code)
from transactions_vw
WHERE year = 1970;
-- 84 categories

-- Total brands in 1970 data
SELECT COUNT(DISTINCT brand)
from transactions_vw
WHERE year = 1970;
-- 203 brands captured

-- brand diversity by each category in 1970
SELECT category_code, COUNT(DISTINCT brand) AS brand_count
FROM transactions_vw
WHERE year = 1970
GROUP BY category_code
ORDER BY brand_count DESC;
-- category "appliances.kitchen.kettle" lead with 25 brands.


-- 10 Best performing category by volume in 1970
SELECT category_code, COUNT(*) AS sales_by_category
FROM transactions_vw
WHERE year = 1970
GROUP BY category_code
ORDER BY sales_by_category DESC
LIMIT 10;

-- 10 Best performing brands by volume in 1970
SELECT brand, COUNT(*) AS sales_by_brand
FROM transactions_vw
WHERE year = 1970
GROUP BY brand
ORDER BY sales_by_brand DESC
LIMIT 10;

-- Top earning category in 1970
SELECT category_code, SUM(price) AS top_earning_category
FROM transactions_vw
WHERE year = 1970
GROUP BY category_code
ORDER BY top_earning_category DESC
LIMIT 10;

-- Top performing smartphone brand by volume and by revenue in 1970
SELECT brand, COUNT(*) AS total_sales_by_volume
FROM transactions_vw
WHERE year = 1970
AND category_code = 'electronics.smartphone'
GROUP BY brand
ORDER BY total_sales_by_volume DESC;


SELECT brand, SUM(price) AS total_sales_by_revenue
FROM transactions_vw
WHERE year = 1970
AND category_code = 'electronics.smartphone'
GROUP BY brand
ORDER BY total_sales_by_revenue DESC;


-- Top earning brand in 1970 by revenue
SELECT brand, SUM(price) AS top_earning_brand
FROM transactions_vw
WHERE year = 1970
GROUP BY brand
ORDER BY top_earning_brand DESC
LIMIT 20;

-- Top earning brand in 1970 by volume
SELECT brand, COUNT(*) AS most_selling_brand
FROM transactions_vw
WHERE year = 1970
GROUP BY brand
ORDER BY most_selling_brand DESC
LIMIT 10;


-- Average prices for these TOP 10 performing brands

WITH top_brands AS (
  SELECT brand,
         COUNT(*) AS bestselling_brand,
         ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS rank
  FROM transactions_vw
  WHERE year = 1970
  GROUP BY brand
  ORDER BY bestselling_brand DESC
  LIMIT 10
)
SELECT tb.rank, t.brand, ROUND(AVG(t.price), 2) AS avg_price
FROM transactions_vw t
JOIN top_brands tb ON t.brand = tb.brand
WHERE t.year = 1970
GROUP BY tb.rank, t.brand
ORDER BY tb.rank;

-- 15 worst performing categories by volume
SELECT category_code, COUNT(*) AS worst_sales_by_category
FROM transactions_vw
WHERE year = 1970
GROUP BY category_code
ORDER BY worst_sales_by_category 
LIMIT 15;

-- 15 worst performing categories by revenue
SELECT category_code, SUM(price) AS worst_revenue_by_category
FROM transactions_vw
WHERE year = 1970
GROUP BY category_code
ORDER BY worst_revenue_by_category 
LIMIT 15;

-- 15 worst performing brands by volume
SELECT brand, COUNT(*) AS worst_sales_by_brand
FROM transactions_vw
WHERE year = 1970
GROUP BY brand
ORDER BY worst_sales_by_brand 
LIMIT 15;

-- 15 worst performing brands by revenue
SELECT brand, SUM(price) AS worst_revenue_by_brand
FROM transactions_vw
WHERE year = 1970
GROUP BY brand
ORDER BY worst_revenue_by_brand 
LIMIT 15;


-- 2020 TRANSACTIONS

-- 2020 time series analysis
-- Monthly transactions in 2020
-- Monthly transactions by volume
SELECT month, COUNT(*) AS monthly_sales
FROM transactions_vw
WHERE year = 2020
GROUP BY month
ORDER BY month;

-- Monthly transactions by revenue
SELECT month, SUM(price) AS monthly_sales_revenue
FROM transactions_vw
WHERE year = 2020
GROUP BY month
ORDER BY month;

--daily
-- daily transactions by volume
SELECT day, COUNT(*) AS daily_sales
FROM transactions_vw
WHERE year = 2020
GROUP BY day
ORDER BY day;

-- daily transactions by revenue
SELECT day, SUM(price) AS daily_sales_revenue
FROM transactions_vw
WHERE year = 2020
GROUP BY day
ORDER BY day;

-- weekday
-- weekday sales by volume
SELECT weekday, COUNT(*)
FROM transactions_vw
WHERE year = 2020
GROUP BY weekday, EXTRACT(DOW FROM event_time)
ORDER BY EXTRACT(DOW FROM event_time);

-- weekday sales by revenue
SELECT weekday, SUM(price) AS weekday_sales_revenue
FROM transactions_vw
WHERE year = 2020
GROUP BY weekday, EXTRACT(DOW FROM event_time)
ORDER BY EXTRACT(DOW FROM event_time);

-- hourly sales by volume
SELECT hour, COUNT(*) AS hourly_sales
FROM transactions_vw
WHERE year = 2020
GROUP BY hour
ORDER BY hour;

-- hourly sales by revenue
SELECT hour, SUM(price) AS hourly_sales_revenue
FROM transactions_vw
WHERE year = 2020
GROUP BY hour
ORDER BY hour;

-- hourly sales data suggests that the transactions come from a 24 hour shop or that the shop allows for 24 hours online transactions.

-- how many missing values for user_id in the 2020  data?
SELECT COUNT(*)
FROM transactions_vw
WHERE year = 2020
AND user_id IS NULL;
-- 1102029 transactions out of 1521919 was recorded without user_id
-- only 419890 transactions were recorded with the user_id


-- 2020 sales trends
-- BEST PERFORMANCE
-- Categories
SELECT COUNT(DISTINCT category_code)
FROM  transactions_vw
WHERE year = 2020;
-- 123 categories

-- 10 Best performing category by volume in 2020
SELECT category_code, count(*) AS sales_by_category
FROM transactions_vw
WHERE year = 2020
GROUP BY category_code
ORDER BY sales_by_category DESC
LIMIT 10;

-- 10 Best performing category by revenue in 2020
SELECT category_code, SUM(price) AS revenue_by_category
FROM transactions_vw
WHERE year = 2020
GROUP BY category_code
ORDER BY revenue_by_category DESC
LIMIT 10;

-- Brands
SELECT COUNT(DISTINCT brand)
FROM transactions_vw
WHERE year = 2020;
-- 589 brands are representation in 2020 data.

-- categories with most brands
-- brand diversity within each category in 2020
SELECT category_code, COUNT(DISTINCT brand) AS brand_count
FROM transactions_vw
WHERE year = 2020
GROUP BY category_code
ORDER BY brand_count DESC;
-- category "electronics.audio.headphone" leads with 50 different brands

-- 10 Best performing brands by volume in 2020
SELECT brand, count(*) AS brand_sales_by_volume
FROM transactions_vw
WHERE year = 2020
GROUP BY brand
ORDER BY brand_sales_by_volume DESC
LIMIT 10;
-- samsung = 326751, ava = 82154, apple = 70034

-- 10 Best performing brands by revenue in 2020
SELECT brand, SUM(price) AS brand_sales_by_revenue
FROM transactions_vw
WHERE year = 2020
GROUP BY brand
ORDER BY brand_sales_by_revenue DESC
LIMIT 10;
-- samsung = 88691835.72, apple = 47396785.34, lg = 25601893.26
-- ava is in 10th place

-- Average prices for these TOP 10 performing brands
WITH top_brands AS (
  SELECT brand,
         COUNT(*) AS bestselling_brand,
         ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS rank
  FROM transactions_vw
  WHERE year = 2020
  GROUP BY brand
  ORDER BY bestselling_brand DESC
  LIMIT 10
)
SELECT tb.rank, t.brand, ROUND(AVG(t.price), 2) AS avg_price
FROM transactions_vw t
JOIN top_brands tb ON t.brand = tb.brand
WHERE t.year = 2020
GROUP BY tb.rank, t.brand
ORDER BY tb.rank;
-- among the top 10 performing brands by volume, top 1-3 in average price are:
-- apple = 676.77, lg = 474.29 and in 3rd place bosch = 319.36
-- This explains why apple is 3rd in sales by volume with apple = 70034 and 2nd in sales by revenue with 47396785.34
-- on the average, an apple product costs 2.5 times more than a samsung product.


-- ANALYZE 2 TOP PERFORMING CATEGORIES

-- SMARTPHONES

-- best performing smartphone brand by volume for 2020
SELECT brand, COUNT(*) AS total_sales
FROM transactions_vw
WHERE year = 2020
AND category_code = 'electronics.smartphone'
GROUP BY brand
ORDER BY total_sales DESC;
-- Samsung leads with 177952 products sold, followed by huawei with 46351

-- best performing smartphone brand by revenue for 2020
SELECT brand, SUM(price) AS total_revenue
FROM transactions_vw
WHERE year = 2020
AND category_code = 'electronics.smartphone'
GROUP BY brand
ORDER BY total_revenue DESC;
-- most revenue generated by Samsung - with revenue - 44803375.35 followed by apple with 35040765.91

-- most expensive smartphone brand in 2020
SELECT brand, MAX(price) AS maximum_price
FROM transactions_vw
WHERE year = 2020
AND category_code = 'electronics.smartphone'
GROUP BY brand
ORDER BY maximum_price DESC;
-- lg has highest product price of 9606.48

-- Cheapest smartphone brand
SELECT brand, MIN(price) AS lowest_price
FROM transactions_vw
WHERE year = 2020
AND category_code = 'electronics.smartphone'
GROUP BY brand
ORDER BY lowest_price;
-- huawei, sony and samsung at price 0.00 (giveaway?)

-- average prices of smartphone per brand
SELECT brand, ROUND(AVG(price), 2) AS average_price
FROM transactions_vw
WHERE year = 2020
AND category_code = 'electronics.smartphone'
GROUP BY brand
ORDER BY average_price DESC;
-- lg has highest average price at 9606.48

-- how many lg phones are included in these transactions?
SELECT COUNT(*)
FROM transactions_vw
WHERE category_code = 'electronics.smartphone'
AND brand = 'lg';
-- only one lg smartphone is in the dataset. This explains why it is both the most expensive and with the highest average.

-- REFRIGERATORS (2ND BEST PERFORMING CATEGORY)

-- best performing refrigerator brand by volume for 2020
SELECT brand, COUNT(*) AS total_sales
FROM transactions_vw
WHERE year = 2020
AND category_code = 'appliances.kitchen.refrigerators'
GROUP BY brand
ORDER BY total_sales DESC;
-- samsung leads with 11674, followed by lg with 11019

-- best performing refrigerator brand by revenue for 2020
SELECT brand, SUM(price) AS total_revenue
FROM transactions_vw
WHERE year = 2020
AND category_code = 'appliances.kitchen.refrigerators'
GROUP BY brand
ORDER BY total_revenue DESC;
--samsung leads with 8524970.68, followed by lg with 7052503.03

-- most expensive refrigerator brand in 2020
SELECT brand, MAX(price) AS highest_price
FROM transactions_vw
WHERE year = 2020
AND category_code = 'appliances.kitchen.refrigerators'
GROUP BY brand
ORDER BY highest_price DESC;
-- lg is most expensive with their top tier product as 9173.59

-- cheapest refrigerator brand
SELECT brand, MIN(price) AS lowest_price
FROM transactions_vw
WHERE year = 2020
AND category_code = 'appliances.kitchen.refrigerators'
GROUP BY brand
ORDER BY lowest_price;
-- organ at 0.44

-- average prices of refrigerators per brand
SELECT brand, ROUND(AVG(price), 2) AS average_price
FROM transactions_vw
WHERE year = 2020
AND category_code = 'appliances.kitchen.refrigerators'
GROUP BY brand
ORDER BY average_price DESC;
-- smeg has highest average - 2847.20

-- 2020 WORST PERFORMANCE FOR CATEGORIES AND BRANDS
-- 10 worst performing category by volume in 2020
SELECT category_code, count(*) AS sales_by_category
FROM transactions_vw
WHERE year = 2020
GROUP BY category_code
ORDER BY sales_by_category
LIMIT 10;
-- "apparel.costume", "apparel.shoes" are least with 2 sales each

-- 10 worst performing brands by volume in 2020
SELECT brand, count(*) AS brand_sales_by_volume
FROM transactions_vw
WHERE year = 2020
GROUP BY brand
ORDER BY brand_sales_by_volume;
-- 63 brands had only 1 sale.