***
# 📘 Case Study: Retail E‑Commerce Analytics Engineering

---
## 📖 About This Case Study
This document provides a deep-dive into the technical evolution of the Retail Strategic Analysis project. While the Main README provides the executive summary and strategic findings, this Case Study documents the "why" and "how" behind the data engineering decisions, including the transition from basic descriptive statistics to a robust, business-oriented Analytics Engineering pipeline.

---
## 🗂️  Project Overview

This project is a comprehensive SQL-based end-to-end data pipeline and strategic analysis of over 2.2 million e-commerce transaction records. The initial approach was to uncover key sales trends, identify top-performing products and brands, and understand customer purchasing behavior as well as analyze trends across different years, categories, and brands, to derive actionable insights for business stakeholders as is seen in `01_Initial_EDA/transaction_analysis.sql`. The goal of the advanced analysis is to firstly adopt the analytics engineering approach by separating data transformation from business logic, secondly, to apply data engineering best practices in the data cleaning, preparation and transformation phase, and thirdly, to use advanced SQL for business-oriented analytics which culminates in a **Unified Strategy Portfolio** that classifies brands and categories into strategic quadrants (Stars vs. Cash Cows) using a custom **Efficiency Index**.

---
##  📊 The Dataset

The dataset consists of transaction records stored in a table named `transactions_tb`. The data was loaded from two CSV files and includes the following columns:

* `user_id`: Unique identifier for the user.
* `event_time`: Timestamp of the transaction.
* `order_id`: Unique identifier for each order.
* `product_id`: Unique identifier for the product.
* `category_id`: Unique identifier for the product category.
* `category_code`: Hierarchical code for the product category (e.g., `electronics.smartphone`).
* `brand`: The brand of the product.
* `price`: The price of the product.


###  📈 Initial Approach (See `02_initial_EDA/transaction_analysis.sql`)
- Focused on basic data cleaning and exploratory analysis.
- Dropped all rows with missing `category_code` or `brand`, resulting in significant data loss.
- Performed simple aggregations (top/worst categories and brands, time trends).
- No advanced handling of missing data or user segmentation.

### 🔍 Problem Context
The dataset contained:
* High null rates in `brand`, `category_code`, and `user_id`
* Timestamp inconsistencies affecting seasonality analysis
* A risk of discarding over **70% of transactions** if naive cleaning was applied

A traditional EDA approach would have produced misleading insights or destroyed revenue signal.

### 🏆  Advanced Approach (See `03_advanced_analysis/transactions.sql` and `transactions_data_cleaning.sql`)
- Designed a robust data pipeline with a surrogate key and normalized timestamps.
- Implemented advanced imputation for missing categories and brands, preserving data and labeling unresolvable cases.
- Introduced user segmentation (`registered` vs. `anonymous`) for deeper behavioral insights.
- Created an analytical view for reproducibility and flexible analysis.
- Developed comprehensive EDA: user segmentation, temporal trends, labeled/unlabelled category and brand analysis, unified volume vs. revenue strategy.
- Export of all key analyses to CSV for transparency and further use.


### 🔄 Before & After: At a Glance

| Aspect                | Initial Analysis (`transaction_analysis.sql`) | Advanced Analysis (`transactions.sql`) |
|-----------------------|----------------------------------------------|----------------------------------------|
| Data Cleaning         | Dropped nulls, lost data                     | Imputation, labeling, data preserved   |
| Feature Engineering   | Basic time features                          | Normalized time, user type, more       |
| Analysis Depth        | Descriptive stats                            | Strategic, segmented, unified metrics  |
| Outputs               | None                                         | CSV exports                  |
| SQL Techniques        | Basic SELECT/GROUP BY                        | CTEs, window functions, advanced logic |
| Business Insights     | Top/worst performers                         | Efficiency, strategic quadrants        |

---

## 📁 Repository Structure
The repository is organized to showcase the full data lifecycle:

*   01_input_data/: Raw CSV transaction records (Split into two for processing efficiency).

*   02_initial_EDA/: Original exploration script (transaction_analysis.sql) documenting the baseline study.

*   03_advanced_Analysis/:
    *   transactions_data_cleaning.sql: Transformation layer. Handles time-zone normalization (UTC), categorical imputation, and data recovery for $14M+ in unlabelled revenue.
    *   transactions.sql: The strategic engine. Implements Window Functions and CTEs to generate business insights.

*   04_data_output_csvs/: Clean, aggregated datasets ready for visualization tools.
    *   01_user_segmentation_summary.csv
    *   02_monthly_seasonality.csv
    *   03_weekday_user_rythm.csv
    *   04_payday_analysis.csv
    *   05_hourly_analysis.csv
    *   06_category_master.csv
    *   07_unlabelled_categories.csv
    *   08_brand_master.csv
    *   09_unknown_brands.csv
    *   10_unified_strategy_portfolio.csv
     
*   Documentation: 
	* README.md
	* CASE_STUDY.md

---
## 🛠️ Data Engineering & Cleaning Logic

To ensure data integrity, I implemented a **Source-to-Analytics pipeline** with the following transformations:

**1️⃣ Source‑to‑Analytics Pipeline:**
Instead of deleting data, the pipeline was designed to **recover, label, and preserve** information:
* Surrogate keys for analytical consistency
* Analytical view (`transactions_vw`) separating transformation from consumption

**2️⃣ Categorical Imputation:**
Missing brands and categories were reconstructed using:
* Product‑level anchors (`product_id`, `category_id`)
* Self‑joins and COALESCE logic
📈 **Impact:** Recovered **$14.5M+** in revenue previously hidden in unlabelled rows.

**3️⃣ Temporal Normalization:**
* Corrected UTC drift using an **8‑hour offset**
* Prevented false off‑hour and off‑season conclusions

**4️⃣ User Segmentation Strategy:**
* **74% of `user_id`s were null**
* Instead of deletion, users were classified as `anonymous` vs `registered`
📊 Enabled meaningful Guest vs. Member behavioral analysis without sacrificing data integrity.

**5️⃣ Anomaly Detection:**
* Identified Unix‑epoch (1970) timestamp corruption
* Isolated the affected rows from growth and seasonality calculations

---

## 🧭 Key Strategic Framework
The core of this analysis is the Unified Strategy Portfolio, which uses the following metrics:

*   The Efficiency Index: See logic, calculation and interpretation below. This identifies segments that generate disproportionate value relative to their operational footprint.

*   Strategic Role Assignment:

    *   ⭐ **STARS:** High AOV (Average Order Value) and High Efficiency (e.g., Apple, Samsung, Bosch).
    *   🐄 **CASH COWS:** High Volume, lower AOV; the "Traffic Drivers" (e.g., Xiaomi, Tefal).
    *   ⚡ **EFFICIENCY PLAYS:** Segments with high revenue share despite lower AOV.
    *	🌱 **LONG TAIL:** Optimization and experimentation opportunities

### 🧮 How it is Calculated: The Efficiency Index
To move beyond simple volume counts, I developed a custom **Efficiency Index** ($\text{EI}$). This metric identifies which segments are "pulling their weight" by comparing their financial impact to their operational footprint.

$$Efficiency Index = \frac{\text{Revenue Share \%}}{\text{Volume Share \%}}$$
*   Logic: If a brand represents **10%** of all transactions (Volume) but generates **20%** of total revenue, its Efficiency Index is **2.0**.
*   Interpretation: 
    *   $EI > 1.0$: The segment is **High-Yield**. It generates more revenue per "click" than the average product.
    *   $EI < 1.0$: The segment is **High-Volume/Low-Margin**. It drives traffic to the site  but requires higher volume to match the revenue of "Stars."

---
## 💡 Top Business Insights

* **Membership Value Gap**
  Registered users show **27% higher AOV** on premium brands like Apple ($720 vs $567). Converting guest users in "Star" categories will further improve revenue.

* **Morning Premium Window**
  High-AOV transactions peak between 5 AM and 8 AM. Though the volume is lower, this window delivers peak Efficiency Index which is ideal for high‑ticket promotions.

* **Brand Dominance and Concentration Risk**
  Samsung is the undisputed market leader, contributing **26.5% of total revenue**, maintaining a high Efficiency Index (1.63) across both user segments. This highlights a dependency risk.

---
## 🔧 Technical Stack

Database: PostgreSQL 15+

SQL Mastery: > * Window Functions: SUM() OVER, AVG() OVER for market share and benchmarking.

CTEs: Multi-layered Common Table Expressions for strategic synthesis.

Data Recovery: Advanced COALESCE logic and Product-ID mapping.

Categorical Imputation: via self-joins and COALESCE.

Indexing: optimized for temporal and categorical queries.

---
## 💡 Strategic Results: 
The technical outputs of this stack (specifically the Unified Strategy Portfolio) are used to drive the business quadrant analysis and executive recommendations. To view the final Strategic Quadrant visualization and business conclusions, Return to the Primary Project Overview. ➡️ [Retail Sales / E‑Commerce Strategic Analysis (PostgreSQL)](./readme.MD)

---
## 🏁 Technical Reflection
*  Scalability: The use of a View (transactions_vw) ensures that if new data is added to transactions_tb, the entire analysis updates instantly without rewriting logic.
*  Integrity: By prioritizing "Data Recovery" over "Data Deletion" (as seen in the 1970 and null-segmentation handling), we preserved the financial accuracy of the $14.5M unlabelled segment.

---
## ⚙️ How to Reproduce
1. **Database Setup**: Execute the DDL in `03_advanced_Analysis/transactions_data_cleaning.sql` to create the base tables and load the CSVs from `01_input_data`.
2. **Data Transformation**: Run the remainder of `transactions_data_cleaning.sql` to generate the `transactions_vw` (Analytical View).
3. **Run Analysis**: Execute `03_advanced_Analysis/transactions.sql` to generate the strategic outputs.
---