-- NOTE: This script requires the transactions_clean table and transactions_vw view.
-- created by running 'transactions_data_cleaning.sql' first.

/* ---------------------------------------------------------------------------------------------------------
   EXPLORATORY DATA ANALYSIS (EDA) STRATEGY
   Objective:
     Understand transaction behavior across users, time, categories,
     and brands using both volume (count) and revenue (sum of price)
     as core business metrics.

   Core Metrics:
     • Transaction count
     • Unique users
     • Total revenue
     • Average order value (AOV)
     • Revenue per transaction

------------------------------------------------------------
   1. USER TYPE SEGMENTATION
------------------------------------------------------------
Goal:
   Compare behavioral differences between registered and anonymous users.
Metrics:
   • transaction volume
   • revenue contribution
   • average order value (AOV)
   • product and category diversity
   • share of unknown brands or unlabelled categories
Key Questions:
   • Who generates more revenue?
   • Are registered users more valuable per transaction?
   • Do anonymous users buy different categories or brands?
   • Are anonymous purchases lower-value or more impulsive?
   • Does missingness (brand/category) correlate with user type?

------------------------------------------------------------
   2. TEMPORAL ANALYSIS (event_time + derived features)
------------------------------------------------------------
Goal:
   Identify time-based patterns in customer activity and revenue.
Dimensions:
   • year
   • month
   • day
   • weekday
   • hour
Metrics:
   • volume trends
   • revenue trends
   • peak hours/days
   • seasonality patterns
   • hourly AOV
Key Questions:
   • When do customers buy most?
   • Are there seasonal or monthly cycles?
   • Which hours drive the highest revenue?
   • Do anonymous vs registered users behave differently over time?
   • Are unknown brands or unlabelled categories time-clustered?

------------------------------------------------------------
   3. CATEGORY ANALYSIS (labelled vs unlabelled)
------------------------------------------------------------
Goal:
   Understand category performance and the impact of missing category labels.
Segments:
   • labelled categories (valid category_code)
   • unlabelled categories (missing or unmapped)
Metrics:
   • transaction volume
   • revenue share
   • category growth trends
   • AOV by category
Key Questions:
   • Which categories dominate sales?
   • How important are unlabelled categories to revenue?
   • Are unlabelled categories concentrated in specific category_id groups?
   • Do unlabelled categories correlate with anonymous users?
   • Should these categories be prioritized for data quality improvements?

------------------------------------------------------------
   4. BRAND ANALYSIS (known vs unknown)
------------------------------------------------------------
Goal:
   Evaluate brand performance and the structure of missing brand data.
Segments:
   • known brands
   • unknown brands (missing or unmapped)
Metrics:
   • transaction volume
   • revenue contribution
   • long-tail brand distribution
   • AOV by brand
Key Questions:
   • Which brands drive the most revenue?
   • Is revenue concentrated in a few brands?
   • Are unknown brands associated with lower prices?
   • Do anonymous users buy more unknown brands?
   • Are unknown brands clustered around specific product_id groups?

------------------------------------------------------------
   5. UNIFIED VOLUME VS REVENUE ANALYSIS
------------------------------------------------------------
Goal:
   Compare high-volume vs high-value segments across all dimensions.
Metrics:
   • transaction count
   • total revenue
   • revenue per transaction
   • AOV by segment
Key Questions:
   • Are high-volume segments also high-value?
   • Which segments drive revenue efficiency?
   • Where is monetization strongest?
   • Which segments show anomalies or outliers?
   • Where should enrichment or data cleaning be prioritized?
------------------------------------------------------------
   
OUTCOMES: The Analytical Deliverables
By the end of this EDA, I hope to define:
⦁	The Revenue Engine: A prioritized list of user segments, brands, and categories that contribute the most to the bottom line.
⦁	The Persona Gap: A clear comparison of how guest users differ from registered users in terms of loyalty (AOV) and intent.
⦁	The Demand Calendar: A map of peak shopping hours and days to optimize marketing spend.
⦁	The Data Integrity Score: A quantification of how much "hidden" revenue is currently trapped in unlabelled columns, providing a business case for better data governance.
--------------------------------------------------------------------------------------------------------------*/

/* USER TYPE SEGMENTATION */
SELECT 
    user_type, -- Volume Metrics    
    COUNT(*) AS transaction_volume,
    ROUND((COUNT(*) * 100 / SUM(COUNT(*)) OVER())::numeric, 2) AS volume_percentage,     
    COUNT(DISTINCT user_id) AS unique_registered_users, -- User Metrics     
    ROUND(SUM(price)::numeric, 2) AS total_revenue,  -- Revenue Metrics
    ROUND((SUM(price) * 100 / SUM(SUM(price)) OVER())::numeric, 2) AS revenue_percentage,
    ROUND(AVG(price)::numeric, 2) AS aov, -- Efficiency Metrics    
    -- Diversity Metrics
    COUNT(DISTINCT category_id) AS category_diversity,
    COUNT(DISTINCT product_id) AS product_diversity,
    -- Data Quality Metric
    ROUND((SUM(CASE WHEN category_code LIKE 'unlabelled%' THEN price ELSE 0 END)/ SUM(price) * 100)::numeric, 2
    ) AS percent_unlabelled_revenue
FROM transactions_vw
GROUP BY user_type
ORDER BY total_revenue DESC;
-- export output as '01_user_segmentation_summary.csv'


--PRE CHECK ON NORMALIZED EVENT TIME BEFORE TEMPORAL ANALYSIS
-- Time range captured in the data:
SELECT MIN(normalized_event_time), MAX(normalized_event_time)
FROM transactions_vw;
-- Shows a range between 1970 and 2020 - suggests a 50 year timeframe.
-- MIN = "1970-01-01 00:03:40" and MAX = "2020-11-21 10:10:30"

-- Sales per year
SELECT year, COUNT(*) AS yearly_sales
FROM transactions_vw
GROUP BY year;
-- 15553 FOR 1970, 2186014 for 2020.

-- distinct event_time values in the year 1970 and 2020.
SELECT year, COUNT(DISTINCT normalized_event_time)
FROM transactions_vw
GROUP BY year;
-- 1 distinct event_time value for 1970, 1299918  distinct event_time values for 2020
/*	1970	1
	2020	1299918 */

-- transaction count and total revenue from 1970
SELECT 
    weekday AS weekday_of_sales,
    COUNT(*) AS transaction_count,
    ROUND(SUM(price)::numeric, 2) AS total_revenue
FROM transactions_vw
WHERE year = 1970
GROUP BY weekday
ORDER BY weekday;	
-- "Thursday"	15553	2174872.60

/*  
Observation 1:
All 15,553 rows from 1970 share one identical event_time value.
These records account for approximately 0.7% of the dataset. They contain invalid or missing timestamps and 
likely defaulted to the Unix epoch. 
Approach:
These records will be excluded from time-based analysis but retained for non-temporal segmentation
to preserve revenue integrity.
Observation 2:
The maximum value of normalized_event_time = "2020-11-21 10:10:30". This suggests when the data was extracted.
Note it's impact on the temporal analysis.
Implications of Observation2:
The Nov 21 cutoff implies Missing Black Friday, Cyber Monday, and December records.
Hence, poor November and zero December results is because these records are not covered in the dataset - 
and not that there are no sales.
*/

/* 2. TEMPORAL ANALYSIS (features derived from normalized_evert_time) */
-- Monthly Seasonality Check
SELECT 
    month,
    user_type,
    COUNT(*) AS monthly_volume,
	ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER())::numeric, 2) AS monthly_volume_pct,
	ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(PARTITION BY month))::numeric, 2) AS volume_within_month_pct,
    ROUND(SUM(price)::numeric, 2) AS monthly_revenue,
	ROUND((SUM(price) * 100.0 / SUM(SUM(price)) OVER())::numeric, 2) AS monthly_revenue_pct,
	ROUND((SUM(price) * 100.0 / SUM(SUM(price)) OVER(PARTITION BY month))::numeric, 2) AS revenue_within_month_pct,
    ROUND(AVG(price)::numeric, 2) AS aov
FROM transactions_vw
WHERE year = 2020
GROUP BY month, user_type
ORDER BY month, user_type;
-- export output as '02_monthly_seasonality_check.csv'

-- Payday / Day of Month Pattern 
SELECT 
    day,
    -- Volume Metrics
    COUNT(*) AS day_volume,
    ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER())::numeric, 2) AS volume_pct,    
    -- Revenue Metrics
    ROUND(SUM(price)::numeric, 2) AS day_revenue,
    ROUND((SUM(price) * 100.0 / SUM(SUM(price)) OVER())::numeric, 2) AS revenue_pct,    
    -- Efficiency Metric
    ROUND(AVG(price)::numeric, 2) AS aov
FROM transactions_vw
WHERE year = 2020
GROUP BY day
ORDER BY day;
-- export output as '03_payday_analysis.csv'

-- Weekday sales rythm
SELECT 
    weekday,
    -- Volume Metrics  
    user_type,   
    COUNT(*) AS transaction_volume,
    ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER())::numeric, 2) AS volume_total_pct, -- % of transactions
    ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(PARTITION BY weekday))::numeric, 2) AS volume_share_within_day_pct, -- % of day's volume by user type
    -- Revenue Metrics
    ROUND(SUM(price)::numeric, 2) AS total_revenue,
    ROUND((SUM(price) * 100.0 / SUM(SUM(price)) OVER())::numeric, 2) AS revenue_total_pct, -- % of all revenue
    ROUND((SUM(price) * 100.0 / SUM(SUM(price)) OVER(PARTITION BY weekday))::numeric, 2) AS revenue_share_within_day_pct, -- % of day's revenue by user type
    -- Efficiency Metric
    ROUND(AVG(price)::numeric, 2) AS aov
FROM transactions_vw
WHERE year = 2020  -- filter our only 2020 data for analysis(exclude 1970 data)
GROUP BY weekday, user_type
ORDER BY 
    CASE 
        WHEN weekday = 'Monday' THEN 1
        WHEN weekday = 'Tuesday' THEN 2
        WHEN weekday = 'Wednesday' THEN 3
        WHEN weekday = 'Thursday' THEN 4
        WHEN weekday = 'Friday' THEN 5
        WHEN weekday = 'Saturday' THEN 6
        WHEN weekday = 'Sunday' THEN 7
    END, 
    user_type;
-- export output as '04_weekday_sales_rythm.csv'

-- Hourly Peak Analysis
SELECT 
    hour,
    user_type,
    COUNT(*) AS transaction_volume,
    -- Volume percentage within the user's specific group
    ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(PARTITION BY user_type))::numeric, 2) AS hourly_volume_share,
    ROUND(AVG(price)::numeric, 2) AS hourly_aov,
    -- Labeling the high-efficiency windows
    CASE 
        WHEN AVG(price) > AVG(AVG(price)) OVER(PARTITION BY user_type) THEN '⭐ Premium Window'
        ELSE 'Standard'
    END AS efficiency_rating
FROM transactions_vw
WHERE year = 2020
GROUP BY hour, user_type
ORDER BY hour ASC, user_type;
-- export output as '05_hourly_analysis.csv'


-- PRODUCT ANALYSIS (CATEGORIES AND BRANDS)------
/*
------------------------------------------------------------
   3. CATEGORY ANALYSIS (labelled vs unlabelled)
------------------------------------------------------------
Goal:
   Understand category performance and the impact of missing category labels.
Segments:
   • labelled categories (valid category_code)
   • unlabelled categories (missing or unmapped)
Metrics:
   • transaction volume
   • revenue share
   • category growth trends
   • AOV by category
Key Questions:
   • Which categories dominate sales?
   • How important are unlabelled categories to revenue?
   • Are unlabelled categories concentrated in specific category_id groups?
   • Do unlabelled categories correlate with anonymous users?
   • Should these categories be prioritized for data quality improvements?
*/

-- 3. CATEGORY ANALYSIS
-- preliminary category analysis
SELECT 
    category_code,
    user_type,
    CASE WHEN category_code LIKE 'unlabelled%' THEN 'Unlabelled' ELSE 'Labelled' END AS label_status,
    COUNT(*) AS transaction_volume,
    -- % of transactions for this user type
    ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(PARTITION BY user_type))::numeric, 2) AS vol_share_pct,
    ROUND(SUM(price)::numeric, 2) AS total_revenue,
    -- % of revenue for this user type
    ROUND((SUM(price) * 100.0 / SUM(SUM(price)) OVER(PARTITION BY user_type))::numeric, 2) AS rev_share_pct,
    ROUND(AVG(price)::numeric, 2) AS aov
FROM transactions_vw
GROUP BY category_code, user_type, label_status
ORDER BY total_revenue DESC;


-- side-by-side category analysis for comparative analysis between anonymous and registered users
SELECT 
    category_code,
    MAX(CASE WHEN category_code LIKE 'unlabelled%' THEN 'Unlabelled' ELSE 'Labelled' END) AS label_status,
    -- Volume
    COUNT(CASE WHEN user_type = 'anonymous' THEN 1 END) AS anon_vol,
    COUNT(CASE WHEN user_type = 'registered' THEN 1 END) AS reg_vol,
    -- Revenue
    ROUND(SUM(CASE WHEN user_type = 'anonymous' THEN price ELSE 0 END)::numeric, 2) AS anon_rev,
    ROUND(SUM(CASE WHEN user_type = 'registered' THEN price ELSE 0 END)::numeric, 2) AS reg_rev,
	ROUND(SUM(price)::numeric, 2) AS total_rev,
    -- AOV
    ROUND(AVG(CASE WHEN user_type = 'anonymous' THEN price END)::numeric, 2) AS anon_aov,
    ROUND(AVG(CASE WHEN user_type = 'registered' THEN price END)::numeric, 2) AS reg_aov
FROM transactions_vw
GROUP BY category_code
ORDER BY total_rev DESC;

-- share of wallet and share of basket for unlabelled categories
-- answers the question: Which category sells more: labelled or unlabelled between registered and anonymous users?
SELECT 
    CASE WHEN category_code LIKE 'unlabelled%' THEN 'Unlabelled' ELSE 'labelled' END AS category_status,
    -- Volume Comparison: What % of an Anonymous user's basket is "Unknown"?
    ROUND((COUNT(CASE WHEN user_type = 'anonymous' THEN 1 END) * 100.0 / 
        SUM(COUNT(CASE WHEN user_type = 'anonymous' THEN 1 END)) OVER())::numeric, 2) AS anon_vol_share_pct,
    ROUND((COUNT(CASE WHEN user_type = 'registered' THEN 1 END) * 100.0 / 
        SUM(COUNT(CASE WHEN user_type = 'registered' THEN 1 END)) OVER())::numeric, 2) AS reg_vol_share_pct,
    -- Revenue Comparison: What % of Anonymous revenue comes from "Unlabelled"?
    ROUND((SUM(CASE WHEN user_type = 'anonymous' THEN price ELSE 0 END) * 100.0 / 
        SUM(SUM(CASE WHEN user_type = 'anonymous' THEN price ELSE 0 END)) OVER())::numeric, 2) AS anon_rev_share_pct,
    ROUND((SUM(CASE WHEN user_type = 'registered' THEN price ELSE 0 END) * 100.0 / 
        SUM(SUM(CASE WHEN user_type = 'registered' THEN price ELSE 0 END)) OVER())::numeric, 2) AS reg_rev_share_pct
FROM transactions_vw
GROUP BY 1;


-- master category analysis for transaction volume, market share, revenue and aov analysis across categories
SELECT 
    category_code,
	CASE WHEN category_code LIKE 'unlabelled%' THEN 'Unlabelled' ELSE 'Labelled' END AS label_status,
	-- Volume
    COUNT(CASE WHEN user_type = 'anonymous' THEN 1 END) AS anon_vol,
    COUNT(CASE WHEN user_type = 'registered' THEN 1 END) AS reg_vol,
	COUNT(*) AS total_vol,
	-- Share of Basket/Volume comparison: Market Share by Volume
	ROUND((COUNT(CASE WHEN user_type = 'anonymous' THEN 1 END) * 100.0 / 
        SUM(COUNT(CASE WHEN user_type = 'anonymous' THEN 1 END)) OVER())::numeric, 2) AS anon_vol_share_pct,
    ROUND((COUNT(CASE WHEN user_type = 'registered' THEN 1 END) * 100.0 / 
        SUM(COUNT(CASE WHEN user_type = 'registered' THEN 1 END)) OVER())::numeric, 2) AS reg_vol_share_pct,
	ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER())::numeric, 2) AS total_vol_share_pct,
	-- Revenue
    ROUND(SUM(CASE WHEN user_type = 'anonymous' THEN price ELSE 0 END)::numeric, 2) AS anon_rev,
    ROUND(SUM(CASE WHEN user_type = 'registered' THEN price ELSE 0 END)::numeric, 2) AS reg_rev,
	ROUND(SUM(price)::numeric, 2) AS total_rev,
	-- Revenue Comparison/Share of Wallet (Percentages): Market Share by Revenue
    ROUND((SUM(CASE WHEN user_type = 'anonymous' THEN price ELSE 0 END) * 100.0 / 
        SUM(SUM(CASE WHEN user_type = 'anonymous' THEN price ELSE 0 END)) OVER())::numeric, 2) AS anon_rev_share_pct,
    ROUND((SUM(CASE WHEN user_type = 'registered' THEN price ELSE 0 END) * 100.0 / 
        SUM(SUM(CASE WHEN user_type = 'registered' THEN price ELSE 0 END)) OVER())::numeric, 2) AS reg_rev_share_pct,
	ROUND((SUM(price) * 100.0 / SUM(SUM(price)) OVER())::numeric, 2) AS total_rev_share_pct,	
	-- AOV
    ROUND(AVG(CASE WHEN user_type = 'anonymous' THEN price END)::numeric, 2) AS anon_aov,
    ROUND(AVG(CASE WHEN user_type = 'registered' THEN price END)::numeric, 2) AS reg_aov
FROM transactions_vw
GROUP BY category_code
ORDER BY total_rev DESC;
-- save output as '06_category_master.csv'	

--UNLABELLED CATEGORIES - deep dive into best performing unlabelled categories
SELECT 
    category_code, -- This contains the 'unlabelled (ID: ...)' string
    COUNT(*) AS transaction_volume,
    ROUND(SUM(price)::numeric, 2) AS total_revenue,
    ROUND(AVG(price)::numeric, 2) AS category_aov,
    -- Identify the dominant brand in this hidden category to help classify it
    (SELECT brand FROM transactions_vw t2 
     WHERE t2.category_code = t1.category_code 
     GROUP BY brand ORDER BY COUNT(*) DESC LIMIT 1) AS anchor_brand,
    -- Calculate the revenue share relative to ALL unlabelled revenue
    ROUND((SUM(price) * 100.0 / SUM(SUM(price)) OVER())::numeric, 2) AS share_of_hidden_revenue
FROM transactions_vw t1
WHERE category_code LIKE 'unlabelled%'
GROUP BY category_code
ORDER BY total_revenue DESC
LIMIT 15;
-- export output as '07_unlabelled_categories.csv'


/*
------------------------------------------------------------
   4. BRAND ANALYSIS (known vs unknown)
------------------------------------------------------------
Goal:
   Evaluate brand performance and the structure of missing brand data.
Segments:
   • known brands
   • unknown brands (missing or unmapped)
Metrics:
   • transaction volume
   • revenue contribution
   • long-tail brand distribution
   • AOV by brand
Key Questions:
   • Which brands drive the most revenue?
   • Is revenue concentrated in a few brands?
   • Are unknown brands associated with lower prices?
   • Do anonymous users buy more unknown brands?
   • Are unknown brands clustered around specific product_id groups?

*/

-- BRAND ANALYSIS
-- preliminary brand analysis
SELECT 
    brand,
    user_type,
    CASE WHEN brand LIKE 'unknown%' THEN 'Unknown' ELSE 'Known' END AS brand_status,
    COUNT(*) AS transaction_volume,
    -- % of transactions for this user type
    ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(PARTITION BY user_type))::numeric, 2) AS vol_share_pct,
    ROUND(SUM(price)::numeric, 2) AS total_revenue,
    -- % of revenue for this user type
    ROUND((SUM(price) * 100.0 / SUM(SUM(price)) OVER(PARTITION BY user_type))::numeric, 2) AS rev_share_pct,
    ROUND(AVG(price)::numeric, 2) AS aov
FROM transactions_vw
GROUP BY brand, user_type, brand_status
ORDER BY total_revenue DESC;


-- side-by-side brand analysis for comparative analysis between anonymous and registered users
SELECT 
    brand,
    MAX(CASE WHEN brand LIKE 'unknown%' THEN 'Unknown' ELSE 'Known' END) AS brand_status,
    -- Volume Comparison
    COUNT(CASE WHEN user_type = 'anonymous' THEN 1 END) AS anon_vol,
    COUNT(CASE WHEN user_type = 'registered' THEN 1 END) AS reg_vol,
    COUNT(*) AS total_vol,
    -- Revenue Comparison
    ROUND(SUM(CASE WHEN user_type = 'anonymous' THEN price ELSE 0 END)::numeric, 2) AS anon_rev,
    ROUND(SUM(CASE WHEN user_type = 'registered' THEN price ELSE 0 END)::numeric, 2) AS reg_rev,
    ROUND(SUM(price)::numeric, 2) AS total_rev,
    -- Value Comparison (AOV)
    ROUND(AVG(CASE WHEN user_type = 'anonymous' THEN price END)::numeric, 2) AS anon_aov,
    ROUND(AVG(CASE WHEN user_type = 'registered' THEN price END)::numeric, 2) AS reg_aov
FROM transactions_vw
GROUP BY brand
ORDER BY total_rev DESC;

-- share of wallet and share of basket for unknown brands
-- answers the question: Who Buys More Unknown Brands
SELECT 
    CASE WHEN brand LIKE 'unknown%' THEN 'Unknown' ELSE 'Known' END AS brand_status,
    -- Volume Comparison: What % of an Anonymous user's basket is "Unknown"?
    ROUND((COUNT(CASE WHEN user_type = 'anonymous' THEN 1 END) * 100.0 / 
        SUM(COUNT(CASE WHEN user_type = 'anonymous' THEN 1 END)) OVER())::numeric, 2) AS anon_vol_share_pct,
    ROUND((COUNT(CASE WHEN user_type = 'registered' THEN 1 END) * 100.0 / 
        SUM(COUNT(CASE WHEN user_type = 'registered' THEN 1 END)) OVER())::numeric, 2) AS reg_vol_share_pct,
    -- Revenue Comparison: What % of Anonymous revenue comes from "Unknown"?
    ROUND((SUM(CASE WHEN user_type = 'anonymous' THEN price ELSE 0 END) * 100.0 / 
        SUM(SUM(CASE WHEN user_type = 'anonymous' THEN price ELSE 0 END)) OVER())::numeric, 2) AS anon_rev_share_pct,
    ROUND((SUM(CASE WHEN user_type = 'registered' THEN price ELSE 0 END) * 100.0 / 
        SUM(SUM(CASE WHEN user_type = 'registered' THEN price ELSE 0 END)) OVER())::numeric, 2) AS reg_rev_share_pct
FROM transactions_vw
GROUP BY 1;


-- master brand analysis: for transaction volume, market share, revenue and aov analysis across brands 
SELECT 
    brand,
    MAX(CASE WHEN brand LIKE 'unknown%' THEN 'Unknown' ELSE 'Known' END) AS brand_status,
	-- Volume
    COUNT(CASE WHEN user_type = 'anonymous' THEN 1 END) AS anon_vol,
    COUNT(CASE WHEN user_type = 'registered' THEN 1 END) AS reg_vol,
	COUNT(*) AS total_vol,
	-- Share of Basket/Volume comparison: Market Share by Volume
	ROUND((COUNT(CASE WHEN user_type = 'anonymous' THEN 1 END) * 100.0 / 
        SUM(COUNT(CASE WHEN user_type = 'anonymous' THEN 1 END)) OVER())::numeric, 2) AS anon_vol_share_pct,
    ROUND((COUNT(CASE WHEN user_type = 'registered' THEN 1 END) * 100.0 / 
        SUM(COUNT(CASE WHEN user_type = 'registered' THEN 1 END)) OVER())::numeric, 2) AS reg_vol_share_pct,
	ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER())::numeric, 2) AS total_vol_share_pct,
	-- Revenue
    ROUND(SUM(CASE WHEN user_type = 'anonymous' THEN price ELSE 0 END)::numeric, 2) AS anon_rev,
    ROUND(SUM(CASE WHEN user_type = 'registered' THEN price ELSE 0 END)::numeric, 2) AS reg_rev,
	ROUND(SUM(price)::numeric, 2) AS total_rev,
	-- Revenue Comparison/Share of Wallet (Percentages): Market Share by Revenue
    ROUND((SUM(CASE WHEN user_type = 'anonymous' THEN price ELSE 0 END) * 100.0 / 
        SUM(SUM(CASE WHEN user_type = 'anonymous' THEN price ELSE 0 END)) OVER())::numeric, 2) AS anon_rev_share_pct,
    ROUND((SUM(CASE WHEN user_type = 'registered' THEN price ELSE 0 END) * 100.0 / 
        SUM(SUM(CASE WHEN user_type = 'registered' THEN price ELSE 0 END)) OVER())::numeric, 2) AS reg_rev_share_pct,
	ROUND((SUM(price) * 100.0 / SUM(SUM(price)) OVER())::numeric, 2) AS total_rev_share_pct,	
	-- AOV
    ROUND(AVG(CASE WHEN user_type = 'anonymous' THEN price END)::numeric, 2) AS anon_aov,
    ROUND(AVG(CASE WHEN user_type = 'registered' THEN price END)::numeric, 2) AS reg_aov
FROM transactions_vw
GROUP BY brand
ORDER BY total_rev DESC;
-- save output as '08_brand_master.csv'

-- Analysis of Unknown brands: deep dive into best performing unknown brands
SELECT 
    brand, -- This contains the 'unknown (Prod: ...)' string
    COUNT(*) AS transaction_volume,
    ROUND(SUM(price)::numeric, 2) AS total_revenue,
    ROUND(AVG(price)::numeric, 2) AS product_aov,
    -- The category clue: helps identify what the product is
    MAX(category_code) AS category_hint,
    -- Revenue share within the "Unknown" brand segment
    ROUND((SUM(price) * 100.0 / SUM(SUM(price)) OVER())::numeric, 2) AS share_of_unknown_revenue
FROM transactions_vw
WHERE brand LIKE 'unknown%'
GROUP BY brand
ORDER BY total_revenue DESC
LIMIT 20;
-- export output as '09_unknown_brands.csv'


/*
------------------------------------------------------------
   5. UNIFIED VOLUME VS REVENUE ANALYSIS
------------------------------------------------------------
Goal:
   Compare high-volume vs high-value segments across all dimensions.
Metrics:
   • transaction count
   • total revenue
   • revenue per transaction
   • AOV by segment
Key Questions:
   • Are high-volume segments also high-value?
   • Which segments drive revenue efficiency?
   • Where is monetization strongest?
   • Which segments show anomalies or outliers?
   • Where should enrichment or data cleaning be prioritized?
------------------------------------------------------------ */

WITH grand_totals AS (
    SELECT 
        COUNT(*)::numeric AS total_vol,
        SUM(price)::numeric AS total_rev,
        AVG(price)::numeric AS global_aov
    FROM transactions_vw
),
category_segments AS (
    SELECT 
        'Category' AS dimension,
        category_code AS segment_name,
        COUNT(*) AS vol,
        SUM(price) AS rev,
        AVG(price) AS aov
    FROM transactions_vw
    GROUP BY 1, 2
),
brand_segments AS (
    SELECT 
        'Brand' AS dimension,
        brand AS segment_name,
        COUNT(*) AS vol,
        SUM(price) AS rev,
        AVG(price) AS aov
    FROM transactions_vw
    GROUP BY 1, 2
),
time_segments AS (
    -- Grouping by operational windows (Morning, Afternoon, Evening, Night)
    SELECT 
        'Time Window' AS dimension,
        CASE 
            WHEN hour BETWEEN 5 AND 10 THEN '01. Morning (Premium Window)'
            WHEN hour BETWEEN 11 AND 16 THEN '02. Afternoon (Peak Volume)'
            WHEN hour BETWEEN 17 AND 22 THEN '03. Evening (Standard)'
            ELSE '04. Night (Low Activity)'
        END AS segment_name,
        COUNT(*) AS vol,
        SUM(price) AS rev,
        AVG(price) AS aov
    FROM transactions_vw
    WHERE year = 2020 -- Exclude 1970 only for time-based segmenting
    GROUP BY 1, 2
),
unified_base AS (
    SELECT * FROM category_segments
    UNION ALL
    SELECT * FROM brand_segments
    UNION ALL
    SELECT * FROM time_segments
)
SELECT 
    dimension,
    segment_name,
    vol AS transaction_count,
    ROUND(rev::numeric, 2) AS total_revenue,
    ROUND(aov::numeric, 2) AS segment_aov,
    -- Efficiency Metrics
    ROUND((vol * 100.0 / g.total_vol)::numeric, 2) AS vol_share_pct,
    ROUND((rev * 100.0 / g.total_rev)::numeric, 2) AS rev_share_pct,
    ROUND(((rev / g.total_rev) / (vol / g.total_vol))::numeric, 2) AS efficiency_index,
    -- Strategic Quadrant Assignment
    CASE 
        WHEN (rev / g.total_rev) > (vol / g.total_vol) AND aov > g.global_aov THEN 'STAR (High Value & Efficient)'
        WHEN (rev / g.total_rev) < (vol / g.total_vol) AND aov < g.global_aov THEN 'CASH COW (High Volume Traffic)'
        WHEN (rev / g.total_rev) > (vol / g.total_vol) AND aov < g.global_aov THEN 'EFFICIENCY PLAY (Low AOV/High Share)'
        ELSE 'LONG TAIL / OPPORTUNITY'
    END AS strategic_role
FROM unified_base, grand_totals g
WHERE rev > (g.total_rev * 0.005) -- Only show segments driving > 0.5% of total revenue
ORDER BY dimension, rev DESC;
-- export output as '10_unified_strategy_portfolio.csv'
