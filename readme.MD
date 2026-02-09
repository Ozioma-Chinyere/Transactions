# 🚀 Retail Sales / E‑Commerce Strategic Analysis (PostgreSQL)

## 🧠 Project Overview

This repository showcases an **end‑to‑end analytics engineering pipeline** and strategic analysis built on **2.2M+ e‑commerce transactions** using PostgreSQL. The project demonstrates how raw, imperfect transactional data can be transformed into **decision‑grade business insights** using modern SQL, data modeling, and analytical frameworks. 

The work follows an **Analytics Engineering** approach:

* Separation of **data transformation** from **business logic**
* Preservation and recovery of revenue‑critical data
* Advanced SQL for stakeholder‑ready metrics

The analysis culminates in a **Unified Strategy Portfolio** that classifies brands and categories into strategic roles (Stars, Cash Cows, Efficiency Plays) using a custom **Efficiency Index**.


📘 **Full Technical & Business Case Study** >
For a detailed walkthrough of the SQL refactoring process, the logic behind the "Efficiency Index," and the step-by-step data recovery of $14.5M in unlabelled revenue, please refer to the ➡️ [Case Study – Retail E‑Commerce Analytics Engineering](./CASE_STUDY.md)

---

## 📊 Dataset

Transaction data stored in `transactions_tb` with:
`user_id`, `event_time`, `order_id`, `product_id`, `category_id`, `category_code`, `brand`, `price`

---

## 🔄 Before & After: At a Glance
Below is a summary of the architectural improvements made in the advanced phase. For the full technical comparison table and query logic, see Section 2 of the Case Study.

| Aspect              | Initial EDA                    | Advanced Analytics Engineering                     |
| ------------------- | ------------------------------ | -------------------------------------------------- |
| Data Integrity      | Dropped nulls, large data loss | Imputation & labeling; **100% revenue preserved**  |
| Feature Engineering | Basic timestamps               | Normalized time, surrogate keys, user segmentation |
| SQL Techniques      | GROUP BY                       | CTEs, Window Functions, self‑joins                 |
| Business Value      | Descriptive metrics            | Strategic segmentation & efficiency modeling       |

---

## 🛠️ Key Achievements: Data Engineering & Cleaning Logic

* **Categorical Imputation:**
  Recovered missing `brand` and `category_code` using using Product ID anchors like `category_id` and `product_id`, unlocking **$14.5M in previously hidden revenue**.

* **Temporal Normalization:**
  Applied UTC‑aligned +8h correction to ensure peak‑hour and seasonality analyses reflect real business behavior.

* **User Segmentation:**
  Classified **74% null `user_id`s** as anonymous users, preserving the dataset while enabling Guest vs. Registered analysis.

* **Anomaly Detection and Mitigation:**
  Isolated Unix‑epoch timestamp corruption (1970) to prevent distortion of growth and seasonality trends.

---

## 🧭 Strategic Framework

### 🧮 Efficiency Index (EI)

A custom metric to evaluate **value efficiency**, not just volume:

$$Efficiency Index = \frac{\text{Revenue Share \%}}{\text{Volume Share \%}}$$

* **EI > 1.0** → High‑yield segments (revenue‑efficient)
* **EI < 1.0** → Traffic drivers (volume‑led)

### 📈 Unified Strategy Portfolio

```mermaid
quadrantChart
    title Unified Strategy Portfolio: Brand Positioning
    x-axis Low Transaction Volume --> High Transaction Volume
    y-axis Low Revenue Contribution --> High Revenue Contribution
    quadrant-1 STAR (High Value & Efficient)
    quadrant-2 EFFICIENCY PLAY (High Revenue/Low Vol)
    quadrant-3 LONG TAIL (Opportunity)
    quadrant-4 CASH COW (Traffic Drivers)
    "Samsung": [0.85, 0.90]
    "Apple": [0.35, 0.95]
    "Xiaomi": [0.90, 0.30]
    "Huawei": [0.45, 0.55]
    "Asus": [0.25, 0.60]
    "Beko": [0.65, 0.20]
```

---

## 💡 Key Business Insights

* **Membership Value Gap:** Registered users show **27% higher AOV** on premium brands (e.g. Apple).
* **Morning Premium Window:** 05:00–08:00 delivers the highest Efficiency Index despite lower volume.
* **Market Dominance:** Samsung drives **26.5% of total revenue** with consistently high efficiency.

---

## 📁 Repository Structure

```
01_input_data/              # Raw CSV files
02_initial_EDA/             # Baseline exploration (transaction_analysis.sql)
03_advanced_Analysis/       # Advanced analytics layer
  ├─ transactions_data_cleaning.sql  # Transformation & imputation logic
  ├─ transactions.sql                # Strategic analytics engine
04_data_output_csvs/        # 10 Cleaned datasets ready for BI/Dash-boarding
README.md                   
CASE_STUDY.md               # Technical Deep-Dive
```

---

## 🔧 Tech Stack

* **Database:** PostgreSQL 15+
* **SQL:** CTEs, Window Functions, self‑joins, analytical views
* **Modeling:** Analytics engineering principles
* **Outputs:** CSVs ready for dash-boarding

---
## 🏁 Conclusion & Recommendations
*   Incentivize Registration: Implement "Member-Only" discounts on Stars (Smartphones/Notebooks) to bridge the AOV gap.
*   Operational Timing: Focus premium product launches during the Morning Premium Window (05:00-08:00).
*   Data Governance: Implement NOT NULL constraints on brand and category_id at the application level to reduce the 12% "Hidden Revenue" in future cycles.

---

## ⚙️ How to Reproduce

1. Run DDL & load data via `transactions_data_cleaning.sql`
2. Build analytical view `transactions_vw`
3. Execute `transactions.sql` to generate strategy outputs

---

