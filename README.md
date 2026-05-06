# SQL Data Warehouse: Sales Integration Project

## 📌 Project Overview
This project focuses on building a modern, structured SQL Data Warehouse using a **Medallion Architecture** approach. The primary goal is to integrate disparate data from ERP and CRM systems into a unified analytical environment to drive business intelligence and strategic decision-making.

## 🏗️ Architecture: The Medallion Approach
The data pipeline follows a layered architecture to ensure data quality and traceability:

1.  **Bronze (Raw Layer):** Initial ingestion of source CSV data (ERP and CRM) into staging tables without modifications.
2.  **Silver (Cleansed Layer):** Data is cleansed, standardized, and validated. This stage resolves data quality issues identified in the source systems.
3.  **Gold (Curated Layer):** Data is modeled into a user-friendly format optimized for analytical queries, combining both sources into a single "Source of Truth."

## 🚀 Project Requirements

### A. Building the Data Warehouse (Data Engineering)
**Objective:** Develop a robust SQL Server-based warehouse to consolidate sales data.

* **Data Sources:** Integration of two primary source systems:
    * **ERP:** Operational sales and transaction data.
    * **CRM:** Customer relationship and interaction data.
* **Data Quality:** Implementation of cleansing logic to ensure high-quality data for downstream consumption.
* **Integration:** Merging ERP and CRM datasets into a unified analytical model.
* **Scope:** The project focuses on the **latest dataset only**. No historization of data is required for this phase.
* **Documentation:** Comprehensive mapping and data model documentation for stakeholders and analysts.

### B. BI: Analytics & Reporting (Data Analysis)
**Objective:** Leverage SQL-based analytics to extract actionable insights.

* **Customer Behavior:** Analysis of purchasing patterns and interaction history.
* **Product Performance:** Identifying top-performing products and revenue drivers.
* **Sales Trends:** Monitoring growth and key business metrics over time.

## 🛠️ Tech Stack
* **Database:** SQL Server
* **Language:** SQL
* **Architecture:** Medallion (Bronze, Silver, Gold)
* **Source Format:** CSV

## 📜 License
This project is licensed under the **MIT License**.
