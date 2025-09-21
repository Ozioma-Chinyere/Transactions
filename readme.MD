

***
## Project Title

**Retail Sales / E-Commerce Transaction Analysis using SQL (PostgreSQL)**

---
## Project Overview

This project analyzes retail and/or e-commerce transaction data using **SQL (PostgreSQL)**. The dataset contains millions of records of product sales, including information on categories, brands, prices, and timestamps. The goal is to uncover key sales trends, identify top-performing products and brands, and understand customer purchasing behavior. The analysis includes data cleaning, exploratory data analysis (EDA), and trend analysis across different years, categories, and brands, to derive actionable insights for business stakeholders.

---
## The Dataset

The dataset consists of transaction records stored in a table named `transactions_tb`. The data was loaded from two CSV files and includes the following columns:

* `user_id`: Unique identifier for the user.
* `event_time`: Timestamp of the transaction.
* `order_id`: Unique identifier for each order.
* `product_id`: Unique identifier for the product.
* `category_id`: Unique identifier for the product category.
* `category_code`: Hierarchical code for the product category (e.g., `electronics.smartphone`).
* `brand`: The brand of the product.
* `price`: The price of the product.

---
## Data Analysis Steps

The analysis was performed in a structured sequence to ensure data integrity and reliable results.

### 1. Data Preparation and Cleaning

* The `transactions_tb` table was created and populated with data from two CSV files.
* Initial data exploration was performed to check for null values and data types.
* The `serial_number` column was dropped as it was not relevant to the analysis.
* Transactions with missing values in `category_code` or `brand` were removed to ensure the quality of the product-related analysis. Transactions with missing values in the user_id column were allowed because 1) it includes more that 60% of the dataset and 2) because of the lack of supporting user data.
* A view named `transactions_vw` was created to simplify the analysis by extracting time-based components like `year`, `month`, `day`, `weekday`, and `hour` from the `event_time` column.

### 2. Exploratory Data Analysis (EDA)

* **Time Range Analysis:** The data spans two distinct years, 1970 and 2020. Further analysis suggest that the 1970 data could be test data, since all transactions were recorded on a single day and at a single hour (January 1, 1970, at 8 AM). The primary focus of the analysis was therefore on the 2020 data, which contained more realistic and diverse transaction patterns.
* **Time Series Trends (2020 Data):** The analysis explored sales trends by month, day, and hour to identify peak periods for both sales volume and revenue.
* **Top and Worst Performers:** SQL queries were used to identify the top 10 best-performing and worst-performing categories and brands based on both sales volume and revenue.
* **Brand Diversity:** The number of unique brands within each category was examined to understand brand diversity. For instance, the **electronics.audio.headphone** category had the highest brand diversity in 2020.
* **Category-Specific Deep Dive:** A detailed analysis was conducted on the two top-performing categories—**electronics.smartphone** and **appliances.kitchen.refrigerators**—to determine best-selling brands, average prices, and price ranges within each category.

---
## Key Findings

### Overall Trends
* The 2020 data showed a clear representation of e-commerce transactions, with sales occurring throughout the day, month, and week.
* **Samsung** emerged as the **dominant brand** in 2020, leading in both total sales volume and revenue.

### Top-Performing Categories
* The **electronics.smartphone** category was the top performer in terms of both sales volume and revenue in 2020.
* The **appliances.kitchen.refrigerators** category was the second-highest earner by revenue.

### Brand Performance (2020)
* **Smartphones:** Samsung was the top brand by both volume and revenue. Analysis of the `lg` brand showed that while one of its products had the highest individual price, its overall representation in the dataset was minimal, suggesting it's an outlier.
* **Refrigerators:** The brand **LG** had the most expensive product, while **Smeg** had the highest average price. This indicates potential premium positioning for these brands.

### Worst-Performing Categories and Brands
* The categories `apparel.costume` and `apparel.shoes` had the lowest sales volume in 2020, suggesting they may be niche or underrepresented in the dataset.
* A significant number of brands (63) had only a single sale in 2020, indicating a long tail of less popular or new products.

---
## Recommendations

* **Focus on Top Categories:** The business should prioritize marketing and inventory for top-performing categories like **electronics.smartphone** and **appliances.kitchen.refrigerators**.
* **Leverage Brand Strengths:** Develop marketing strategies around the top-performing brands, such as **Samsung**, to capitalize on their popularity.
* **Analyze Low Performers:** Further investigation is needed to understand why certain categories and brands perform poorly. This could involve exploring product availability, pricing strategies, or customer reviews.
* **Address Data Anomalies:** The presence of a few transactions with a price of **$0.00** warrants further investigation to determine if they represent giveaways, returns, or data entry errors.
* **Address Data Entry Irregularities:** The fact that majority of transactions are without a `user_id` suggests that `user_id` is not a strict requirement for entering transaction data. Data collection should be modified to ensure that a transaction cannot be completed without the required data like: `user_id`, `category_code` and `brand`. This can be achieved by defining a `NOT NULL` constraint for these columns.

---
## Notes

- The `user_id` column was not analyzed in depth due to the huge number of null values and the lack of supporting user data.