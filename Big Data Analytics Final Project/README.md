# AUCA Big Data Analytics Final Project
## Distributed Multi-Model Analytics for E-commerce Data

**Due Date:** 31-01-2026  
**Project Format:** Individual  
**Focus:** Strategic application of MongoDB, HBase, and Apache Spark for big data analytics

---

## Project Overview
This project demonstrates the application of multiple NoSQL data models and distributed processing for e-commerce analytics:
- **MongoDB**: Document model for flexible, hierarchical data
- **HBase**: Wide-column model for time-series and analytical queries
- **Apache Spark**: Distributed batch and stream processing

---

## Learning Objectives
- Understand trade-offs between NoSQL data models
- Design efficient schemas for MongoDB and HBase
- Implement data ingestion and querying techniques
- Develop batch and stream processing with Spark
- Integrate multiple data stores for complex analytics
- Generate business insights from large datasets

---

## Project Structure
```
├── src/
│   ├── data_generator.py          # Synthetic data generation
│   ├── mongodb_ingestion.py       # MongoDB data loading
│   ├── hbase_ingestion.py         # HBase data loading
│   ├── spark_analytics.py         # Spark batch processing
│   ├── spark_streaming.py         # Spark stream processing
│   └── insights_generator.py      # Analytics and reporting
├── data/
│   ├── users.json                 # User profiles
│   ├── products.json              # Product catalog
│   ├── categories.json            # Category hierarchy
│   ├── sessions.json              # User sessions
│   └── transactions.json          # Transaction records
├── config/
│   ├── mongodb_config.py          # MongoDB connection settings
│   ├── hbase_config.py            # HBase connection settings
│   └── spark_config.py            # Spark configuration
├── notebooks/
│   ├── 01_data_exploration.ipynb  # EDA and data understanding
│   ├── 02_mongodb_queries.ipynb    # MongoDB analysis
│   ├── 03_hbase_queries.ipynb      # HBase analysis
│   └── 04_spark_analytics.ipynb    # Spark results
└── requirements.txt               # Python dependencies
```

---

## Dataset Description

### Entities

#### Users (users.json)
- User profiles with demographic and registration information
- Fields: user_id, geo_data (city, state, country), registration_date, last_active

#### Categories (categories.json)
- Hierarchical product classification system
- Fields: category_id, name, subcategories

#### Products (products.json)
- Product catalog with pricing and inventory
- Fields: product_id, category_id, name, price, stock, created_date

#### Sessions (sessions.json)
- User browsing sessions with interactions
- Fields: session_id, user_id, start_time, products_viewed, duration

#### Transactions (transactions.json)
- Purchase records with details
- Fields: transaction_id, user_id, session_id, products, amounts, timestamp

---

## Setup Instructions

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Generate Synthetic Data
```bash
python src/data_generator.py
```

### 3. Set Up MongoDB
```bash
python src/mongodb_ingestion.py
```

### 4. Set Up HBase
```bash
python src/hbase_ingestion.py
```

### 5. Run Spark Analytics
```bash
python src/spark_analytics.py
```

### 6. Generate Insights
```bash
python src/insights_generator.py
```

---

## Key Analytical Tasks

### MongoDB Queries
- User segmentation by geography and activity
- Product category analysis
- Session-based user behavior patterns

### HBase Queries
- Time-series transaction analysis
- Revenue trends and seasonality
- Customer lifetime value calculations

### Spark Analytics
- Distributed data processing across models
- Machine learning pipeline (optional)
- Integration of insights from multiple sources

---

## Deliverables
1. Functional data ingestion pipelines
2. Optimized schemas for MongoDB and HBase
3. Comprehensive Spark analytics jobs
4. Analysis notebooks with visualizations
5. Business insights and recommendations
6. Project documentation

---

## Timeline
- **By 25-01-2026**: Data generation and ingestion complete
- **By 28-01-2026**: Analytics queries and Spark jobs working
- **By 31-01-2026**: Final insights and documentation ready

