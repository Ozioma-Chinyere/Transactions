-- Create the transaction records table in the database:

DROP TABLE IF EXISTS transactions_tb;

CREATE TABLE IF NOT EXISTS transactions_tb (
    serial_number INT,
    user_id NUMERIC(20,0),
    event_time TIMESTAMP WITH TIME ZONE,
    order_id NUMERIC(20,0),
    product_id NUMERIC(20,0),
    category_id NUMERIC(23,0),
    category_code VARCHAR(50),
    brand VARCHAR(20),
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

-- data preview
SELECT * 
FROM transactions_tb
LIMIT 5;

-- Confirm total no. of rows.
SELECT COUNT(*) 
FROM transactions_tb;
-- 2201567

-- check for null values in all the columns
SELECT
  COUNT(*) FILTER (WHERE serial_number IS NULL) AS serial_number,
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


-- Cross-checking for uniqueness in the serial_number column 
SELECT COUNT(DISTINCT serial_number)
FROM transactions_tb;
-- all 2201567 serial numbers are unique

SELECT * 
FROM transactions_tb
LIMIT 5;
-- Though unique, the values look like exported index from a dataframe


/*
DATA CLEANING AND PREPARATION PLAN

1. Refactor Row Identifiers
   Action: Replace the serial_number column with a native PostgreSQL surrogate key.
   Rationale:
     - The existing serial_number in the base table is probably an artifact of a DataFrame export (index).
     - While it may be unique, it lacks native database constraints, sequence
       management, and long-term reliability as a primary key.
     - It does not represent a business identifier and adds unnecessary table bloat.
   Approach:
     • In a clean Base Table - transactions_clean - do not include the serial_number 
     • Add a BIGSERIAL PRIMARY KEY column (id), which provides:
         - Automatic Uniqueness: PostgreSQL manages the sequence internally.
         - Future-Proofing: BIGINT supports extremely large row counts.
         - Referential Integrity: A stable, indexed anchor for materialized views
           and future joins.

2. Handle missing values in the `user_id` column.
   - Approximately 74% of records have NULL `user_id`.
   - Dropping these rows would cause unacceptable data loss.
   - Approach:
       • Treat NULL values as anonymous or guest users.
       • Create a derived column:
           CASE WHEN user_id IS NULL THEN 'anonymous'
                ELSE 'registered'
           END AS user_type
     This preserves all records while enabling segmentation in user-level analysis.

3. Extract temporal features from the `normalized_event_time` column.
   Rationale:
    - In the original dataset, event_time is set to timezone UTC
    - The local computer runs on GMT +8
    - If not handled accurately, this could lead to a misrepresentation of the hour values and skew the interpretation of hourly sales.
   Approach:
    - Add a normalized_event_time column while creating the clean base table - transactions_clean with datatype TIMESTAMP
    - Populate it by setting normalized_event_time = event_time - INTERVAL '8 hours';
    - For time-based analysis, derive time components from the normalized_event_time:
       • year
       • month
       • day
       • weekday (string) - using TRIM to remove padding for cleaner groupings
       • hour
    - These features support seasonality analysis, hourly patterns,
      weekday/weekend behavior, and time-series exploration.

4. Preserve columns with no missing values.
   - The following columns contain complete data and will be retained as-is:
       • order_id
       • product_id
       • category_id
       • price

5. Handle missing values in the `category_code` column.
   - Approximately 28% of records have NULL `category_code`.
   - Each `category_id` is expected to map to a single `category_code`.
   - Approach:
       • Check whether any `category_id` maps to multiple category_code values.
       • If inconsistencies exist, select the most frequent or first non-null
         category_code per category_id.
       • Use this mapping to fill missing category_code values.
       • If no mapping exists, assign 'unlabelled (ID: )'.

6. Handle missing values in the `brand` column.
   - Approximately 5% of records have NULL `brand`.
   - Each `product_id` is expected to map to a single brand.
   - Approach:
       • Verify whether any product_id maps to multiple brands.
       • If consistent, build a mapping from product_id → brand using non-null values.
       • Use this mapping to fill missing brand values.
       • If no mapping exists, assign 'unknown (Prod: )'.

7. Create an analytical view transactions_vw from the transactions_clean table.
   - Implement all data cleaning and preparation steps in the view rather than modifying the base data.
   - Benefits:
       • Preservation of raw data integrity.
       • Reproducibility of analytical results.
       • Flexibility to adjust or extend transformation logic without altering
         the underlying dataset.
*/


/* -----------------------------------------------------------
   STEP 1: Create a clean Base Table - transactions_clean - with the following changes from the base table:
 - Does not include the serial_number column
 - Add a native PostgreSQL surrogate key (id BIGSERIAL)
 - Add the normalized_event_time column (normalized_event_time TIMESTAMP) that represents the timezone as UTC
 - Populate the new table - transactions_clean from the transactions_tb having applied these changes.
------------------------------------------------------------*/


-- CREATE THE transactions_clean TABLE
CREATE TABLE transactions_clean (
    id BIGSERIAL PRIMARY KEY,
    user_id NUMERIC(20,0),
    event_time TIMESTAMP,            -- Original for reference
    normalized_event_time TIMESTAMP, -- Reverted to UTC    
    order_id NUMERIC(20,0),
    product_id NUMERIC(20,0),
    category_id NUMERIC(23,0),
    category_code VARCHAR(50),
    brand VARCHAR(20),
    price DECIMAL(8, 2)    
);


/* --- OPTIMIZATION STRATEGY--- */
-- Temporal Index (For Seasonality & Hourly Analysis)
CREATE INDEX idx_clean_time ON transactions_clean(normalized_event_time);
-- Categorical Indexes (For Category and Product Analysis)
CREATE INDEX idx_clean_category ON transactions_clean(category_code);
CREATE INDEX idx_clean_brand ON transactions_clean(brand);
-- User Segmentation Index
CREATE INDEX idx_clean_user_type ON transactions_clean(user_id);



-- INSERT DATA INTO THE transactions_clean TABLE
INSERT INTO transactions_clean (
    user_id, event_time, normalized_event_time,  order_id, product_id, 
    category_id, category_code, brand, price 
)
SELECT 
    user_id,
    event_time,
    event_time - INTERVAL '8 hours', -- Normalization to UTC     
    order_id, product_id, category_id, category_code, brand, price 
FROM transactions_tb;


-- Preview the transactions clean table.
SELECT * 
FROM transactions_clean
LIMIT 5;

--confirm row count
SELECT COUNT(*)
FROM transactions_clean;


-- Step 5 prep: Check whether any `category_id` maps to multiple category_code values 
SELECT category_id, COUNT(DISTINCT category_code) AS max_names
FROM transactions_clean
WHERE category_code IS NOT NULL
GROUP BY category_id
HAVING COUNT(DISTINCT category_code) > 1;
-- One category_id is mapped to 3 category_codes. Use most frequent or first non-null category_code

-- Step 6 prep: Verify whether any product_id maps to multiple brands
SELECT product_id, COUNT(DISTINCT brand)
FROM transactions_tb
WHERE brand IS NOT NULL
GROUP BY product_id
HAVING COUNT(DISTINCT brand) > 1;
-- No rows returned. Map the missing brands to corresponding product_id.


/* -----------------------------------------------------------
   STEP 2–7: CREATE ANALYTICAL VIEW
   - User segmentation
   - Temporal feature extraction from normalized_event_time
   - Category/brand imputation
   - Clean analytical structure for EDA
------------------------------------------------------------*/
-- CREATE THE ANALYTICAL VIEW
CREATE VIEW transactions_vw AS 
WITH category_map AS (
    -- Mapping category codes to category ids
    SELECT category_id, MAX(category_code) AS mapped_category_code
    FROM transactions_tb
    WHERE category_code IS NOT NULL
    GROUP BY category_id
),
brand_map AS (
    -- Mapping brands to product IDs
    SELECT product_id, MAX(brand) AS mapped_brand
    FROM transactions_tb
    WHERE brand IS NOT NULL
    GROUP BY product_id
)
SELECT
    t.id,
    t.user_id,
    -- Step 2: User Segmentation
    CASE
        WHEN t.user_id IS NULL THEN 'anonymous'
        ELSE 'registered'
    END AS user_type,
    t.normalized_event_time,
    -- Step 3: Temporal Feature Extraction
    EXTRACT(YEAR FROM t.normalized_event_time) AS year, 
    EXTRACT(MONTH FROM t.normalized_event_time) AS month,
    EXTRACT(DAY FROM t.normalized_event_time) AS day,
    TRIM(TO_CHAR(t.normalized_event_time, 'Day')) AS weekday,
    EXTRACT(HOUR FROM t.normalized_event_time) AS hour,
    -- Step 4: Direct Retentions
    t.order_id,
    t.product_id,
    t.category_id,
    -- Step 5 & 6: Categorical Imputation - label missing data but keep the category_id and product_id visible 
    COALESCE(t.category_code, cm.mapped_category_code, 'unlabelled (ID: ' || t.category_id::text || ')') AS category_code,
    COALESCE(t.brand, bm.mapped_brand, 'unknown (Prod: ' || t.product_id::text || ')' ) AS brand,
    t.price
FROM transactions_clean t
LEFT JOIN category_map cm ON t.category_id = cm.category_id
LEFT JOIN brand_map bm ON t.product_id = bm.product_id;


--Materialized View Preview
SELECT *
FROM transactions_vw
LIMIT 10;

--check for number of rows with unlabelled category_code
SELECT COUNT(*)
FROM transactions_vw
WHERE category_code LIKE 'unlabelled%';
/* 611252 - Only 950 rows had a usable mapping
This implies that for the missing data, Only 950 rows had a 
category_id that appeared elsewhere with a valid name.
*/

--check for number of rows with unknown brand
SELECT COUNT(*)
FROM transactions_vw
WHERE brand LIKE 'unknown%';
/* 112670 - This means that that for these 112,670 rows,
those specific products never have a brand name associated with them.
It is safe to assume that these product are most likely unbranded or 'generic'.
*/

--Confirm the result of the data cleaning and preparation.
SELECT
  COUNT(*) FILTER (WHERE id IS NULL) AS id,
  COUNT(*) FILTER (WHERE user_id IS NULL) AS user_id,
  COUNT(*) FILTER (WHERE user_type IS NULL) AS user_type,  
  COUNT(*) FILTER (WHERE normalized_event_time IS NULL) AS normalized_event_time,
  COUNT(*) FILTER (WHERE year IS NULL) AS year,
  COUNT(*) FILTER (WHERE month IS NULL) AS month,
  COUNT(*) FILTER (WHERE day IS NULL) AS day,
  COUNT(*) FILTER (WHERE weekday IS NULL) AS weekday,
  COUNT(*) FILTER (WHERE hour IS NULL) AS hour,
  COUNT(*) FILTER (WHERE order_id IS NULL) AS order_id,
  COUNT(*) FILTER (WHERE product_id IS NULL) AS product_id,
  COUNT(*) FILTER (WHERE category_id IS NULL) AS category_id,
  COUNT(*) FILTER (WHERE category_code IS NULL) AS category_code,
  COUNT(*) FILTER (WHERE brand IS NULL) AS brand,
  COUNT(*) FILTER (WHERE price IS NULL) AS price
 FROM transactions_vw;
-- only the user_id column has 1637398 null values. 