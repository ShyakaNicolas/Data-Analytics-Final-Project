# Implementation Steps - AUCA Big Data Analytics Final Project

## Phase 1: Environment Setup (Week 1)

### Step 1: Install Python Dependencies
```bash
cd "c:\Users\nicolas.shyaka\Documents\Personal\AUCA\Big Data Analytics Final Project"
pip install -r requirements.txt
```
**What it does:** Installs all required packages (Faker, pandas, MongoDB driver, HBase client, Spark, Jupyter)

---

## Phase 2: Data Generation (Week 1)

### Step 2: Generate Synthetic Dataset
```bash
python src/data_generator.py
```
**Output files created in `data/` folder:**
- `users.json` - 1000 user records
- `products.json` - 500 product records
- `categories.json` - 10 category hierarchies
- `sessions.json` - 5000 session records
- `transactions.json` - 10000 transaction records

**What to verify:**
- All JSON files created successfully
- Data looks reasonable (dates, prices, IDs)
- File sizes are appropriate

---

## Phase 3: MongoDB Setup (Week 1)

### Step 3: Install & Start MongoDB
**On Windows:**
- Download MongoDB Community Edition from https://www.mongodb.com/try/download/community
- Install and start the MongoDB service
- Default connection: `mongodb://localhost:27017`

**Verify connection:**
```bash
python -c "from pymongo import MongoClient; client = MongoClient(); print('Connected to MongoDB')"
```

### Step 4: Design MongoDB Schema
Create schema design document:
- **Collections:** users, products, categories, sessions, transactions
- **Indexes:** User ID, transaction timestamp, category hierarchy
- **Denormalization strategy:** Embed frequently accessed subcategories in products

### Step 5: Load Data into MongoDB
```bash
python src/mongodb_ingestion.py
```
**What it does:**
- Connects to MongoDB
- Creates database: `ecommerce_db`
- Creates collections: users, products, categories, sessions, transactions
- Loads all JSON data
- Creates indexes for performance

**Verify:**
```bash
# In MongoDB shell (mongosh)
use ecommerce_db
db.users.countDocuments()        # Should show 1000
db.transactions.countDocuments() # Should show 10000
```

---

## Phase 4: HBase Setup (Week 1-2)

### Step 6: Install & Start HBase
**On Windows (using WSL2 recommended):**
- Install Hadoop/HBase from https://hbase.apache.org/downloads.html
- Or use Docker: `docker run -d -p 16010:16010 harisekhon/hbase`

**Verify connection:**
```bash
python -c "import happybase; connection = happybase.Connection('localhost'); print('Connected to HBase')"
```

### Step 7: Design HBase Schema
Create schema design document:
- **Tables:** transactions_ts, user_sessions, product_metrics
- **Row Key Design:** `[user_id]_[timestamp]` for time-series
- **Column Families:** `data`, `metrics`, `metadata`
- **Compression:** Enable for production

### Step 8: Load Data into HBase
```bash
python src/hbase_ingestion.py
```
**What it does:**
- Connects to HBase
- Creates tables with appropriate row key designs
- Bulk loads transactions as time-series data
- Aggregates session data

---

## Phase 5: Spark Setup (Week 2)

### Step 9: Install & Configure Spark
```bash
# Spark comes with PySpark (installed via requirements.txt)
# Verify:
python -c "import pyspark; print(pyspark.__version__)"
```

### Step 10: Develop Spark Analytics Jobs

#### 10a: Batch Processing Job
```bash
python src/spark_analytics.py
```
**Analyzes:**
- User segmentation by geography
- Product popularity and profitability
- Transaction trends
- Customer lifetime value

#### 10b: Streaming Job (Simulated)
```bash
python src/spark_streaming.py
```
**Simulates real-time:**
- Session tracking
- Revenue tracking
- Anomaly detection

---

## Phase 6: Analytics & Insights (Week 2)

### Step 11: MongoDB Analytical Queries
**Implement queries for:**
- Top 10 products by revenue
- User segmentation by geography
- Category performance analysis
- Average session metrics by device type

### Step 12: HBase Analytical Queries
**Implement queries for:**
- Revenue trends over time
- Customer lifetime value calculations
- Seasonal patterns
- Peak transaction times

### Step 13: Spark Integration
**Run integrated analytics:**
- Combine MongoDB and HBase data
- Perform distributed joins
- Generate aggregate statistics
- Machine learning models (optional)

### Step 14: Generate Insights Report
```bash
python src/insights_generator.py
```
**Produces:**
- Key metrics and KPIs
- Business recommendations
- Visualizations (charts, graphs)
- Export to CSV/HTML

---

## Phase 7: Jupyter Notebooks (Week 2-3)

### Step 15: Create Exploration Notebooks

#### Notebook 1: Data Exploration
- Load and display sample data
- Statistical summaries
- Data quality checks
- Distribution analysis

#### Notebook 2: MongoDB Analysis
- Complex queries and aggregations
- Index effectiveness testing
- Query performance analysis
- Visualization of results

#### Notebook 3: HBase Analysis
- Time-series data retrieval
- Range scan examples
- Aggregation patterns
- Performance comparisons

#### Notebook 4: Spark Analytics
- Multi-source data integration
- Distributed processing examples
- ML pipelines
- Final insights visualization

**Launch notebooks:**
```bash
jupyter notebook
```

---

## Phase 8: Testing & Validation (Week 3)

### Step 16: Validate Data Integrity
- Count records in each store
- Verify data consistency across stores
- Test query performance
- Validate aggregations

### Step 17: Performance Testing
- Measure query response times
- Test with different data volumes
- Profile Spark jobs
- Optimize slow queries

### Step 18: Documentation
- Schema documentation
- Query examples
- Performance notes
- Architecture diagrams

---

## Phase 9: Final Deliverables (By 31-01-2026)

### Step 19: Prepare Final Submission

**Deliverables Checklist:**
- ✅ Working data ingestion pipeline
- ✅ MongoDB with 5+ analytical queries
- ✅ HBase with time-series analytics
- ✅ Spark jobs (batch + streaming)
- ✅ Jupyter notebooks with visualizations
- ✅ Insights report with recommendations
- ✅ Project documentation
- ✅ Performance analysis
- ✅ Architecture diagrams
- ✅ Code comments and docstrings

---

## Quick Reference Commands

### Start Services
```bash
# MongoDB (if installed locally)
mongod

# HBase (if installed locally)
start-hbase.cmd  # Windows
./bin/start-hbase.sh  # Linux/Mac

# Jupyter
jupyter notebook
```

### Generate Data
```bash
python src/data_generator.py
```

### Run Ingestion
```bash
python src/mongodb_ingestion.py
python src/hbase_ingestion.py
```

### Run Analytics
```bash
python src/spark_analytics.py
python src/insights_generator.py
```

---

## File Structure After Implementation

```
├── README.md                          # Project overview
├── IMPLEMENTATION_STEPS.md           # This file
├── requirements.txt                   # Dependencies
├── src/
│   ├── data_generator.py             # ✅ Generate synthetic data
│   ├── mongodb_ingestion.py          # Load to MongoDB
│   ├── hbase_ingestion.py            # Load to HBase
│   ├── spark_analytics.py            # Batch processing
│   ├── spark_streaming.py            # Stream processing
│   ├── insights_generator.py         # Analytics & reports
│   └── utils.py                      # Helper functions
├── data/
│   ├── users.json                    # Generated user data
│   ├── products.json                 # Generated product data
│   ├── categories.json               # Generated categories
│   ├── sessions.json                 # Generated sessions
│   └── transactions.json             # Generated transactions
├── config/
│   ├── mongodb_config.py             # MongoDB settings
│   ├── hbase_config.py               # HBase settings
│   └── spark_config.py               # Spark settings
├── notebooks/
│   ├── 01_data_exploration.ipynb     # EDA notebook
│   ├── 02_mongodb_queries.ipynb      # MongoDB analysis
│   ├── 03_hbase_queries.ipynb        # HBase analysis
│   └── 04_spark_analytics.ipynb      # Spark results
├── outputs/
│   ├── reports/                      # Analytics reports
│   ├── visualizations/               # Charts and graphs
│   └── metrics.json                  # Performance metrics
└── logs/
    └── execution.log                 # Job logs
```

---

## Troubleshooting

### MongoDB connection fails
- Verify MongoDB is running: `mongosh`
- Check firewall settings
- Verify connection string in config

### HBase connection fails
- Verify HBase is running: `jps` (should show HMaster)
- Check port 9090 is accessible
- Verify Java installation

### Spark jobs fail
- Check Python version (3.8+)
- Verify PySpark installation: `pyspark`
- Check data files exist in `data/` folder
- Review error logs

### Memory issues
- Reduce data size for testing
- Configure Spark memory: `spark.driver.memory 2g`
- Use data sampling for development

---

## Timeline Summary

| Date | Milestone |
|------|-----------|
| By 22-01 | ✅ Data generation complete |
| By 25-01 | ✅ MongoDB & HBase loaded |
| By 28-01 | ✅ Spark jobs working |
| By 30-01 | ✅ Notebooks & insights done |
| By 31-01 | ✅ Final submission ready |

---

**Next Step:** Start with Step 2 - Generate the synthetic dataset!

