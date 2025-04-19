
# ✈️ METAR Weather Analysis: A Data Engineering and Machine Learning Approach

## 🔧 Technologies Used

- Google Cloud Dataproc
- Apache Spark
- PySpark
- Streamlit

---

## 📘 About METAR Data

**What is METAR?**

- METAR stands for *Meteorological Aerodrome Report*.
- A standardized weather report used in aviation, updated at least hourly.

**Example METAR String:**

```
EGLL 171050Z 25015KT 9999 SCT025 07/03 Q1018 NOSIG
```

---

## 👨‍💻 Data Engineering Perspective

### 1. Data Ingestion
- **Sources**: NOAA, AviationWeather.gov, FTP feeds.
- **Tools**:
  - Python (`requests`, `pandas`)
  - Apache NiFi / Airflow

### 2. Parsing and Cleaning
- METARs are cryptic — you need to parse them into structured formats
- Libraries: `python-metar`, regex parsers
- Extracted fields: station code, time, wind, visibility, temp, pressure, etc.

### 3. Data Storage
- **Short-term**: Kafka, Redis  
- **Long-term**: PostgreSQL, MongoDB, AWS S3, Azure Blob, HDFS

### 4. Data Modeling
- Schemas for raw and parsed METAR data, including geospatial metadata.

### 5. ETL / ELT Pipelines
- Airflow / Prefect, dbt for transformation

### 6. Analytics & Visualization
- Live airport weather dashboards and trends using:
  - Superset, Grafana, Power BI, Tableau

### 7. Use Cases
- Real-time monitoring, predictive maintenance, route optimization, alerting systems

---

## 🧱 Example Tech Stack

| Layer         | Tech Stack                                      |
|---------------|--------------------------------------------------|
| Ingestion     | Python, Airflow, Kafka                           |
| Parsing       | `python-metar`, Pandas                           |
| Storage       | PostgreSQL, S3, MongoDB                          |
| Processing    | Spark, dbt, Airflow                              |
| Visualization | Superset, Tableau, Streamlit                     |
| Deployment    | Docker, Kubernetes, GitHub Actions (CI/CD)       |

### 🔗 Sources:
- [Optimizing METAR Forecasts (Medium)](https://robinat539.medium.com/optimizing-airport-metar-forecasts-with-machine-learning-and-llm-techniques-4da2f13025e9)
- [Drone Pilot Ground School](https://www.dronepilotgroundschool.com/reading-aviation-routine-weather-metar-report/)

---

## 🎯 Project Objective

An end-to-end educational pipeline to process and forecast aviation weather using METAR reports through batch and streaming modes.

---

## 🚀 Project Goals

- 🔄 Automate METAR data ingestion for European airports.
- 📊 Analyze historical patterns in aviation weather.
- 🧠 Train ML models to predict upcoming METAR trends.
- 🌐 Streamlit web app for real-time data, visualization, and forecasts.

---

## 🧠 Architecture Diagram


![image](https://github.com/user-attachments/assets/8d576e00-2da6-41b4-8441-110471253437)


## 🧩 Key Components

### Phase 1: Data Pipeline Workflow

1. **Data Retrieval**  
   Pull archived METAR data using automated jobs.

2. **Initial Transformation**  
   Parse & normalize strings, extract weather attributes.

3. **Data Lake Ingestion**  
   Store in Google Cloud Storage (GCS).

4. **Data Warehouse Loading**  
   Load into BigQuery for fast SQL querying.

5. **Advanced Transformation (PySpark)**  
   Aggregation, feature engineering, trend analysis using Dataproc.

6. **Visualization with Looker**  
   Dashboards for variability, trends, real-time status.

---

### Phase 2: Near Real-time Ingestion

- Set up infrastructure to process live METAR data.
- Train & tune ML models with historical data.

### Phase 3: Web Dashboard

- Streamlit-based app to show:
  - Upcoming METAR predictions
  - Historical and real-time trends

---

## 📈 Looker Report Overview

Interactive charts covering:

- Temperature, wind speed/direction, weather events
- Regional & airport-specific insights

🔗 [Looker Report](https://lookerstudio.google.com/u/0/reporting/ef5cab41-deeb-498e-95de-c29cf52a3fe6/page/lk6ND)

---
## 📊 Looker Results

![image](https://github.com/user-attachments/assets/0d68b182-baca-4dab-96ae-8f771deafd30)

## 🗃️ Data Pipeline Summary

1. **Collection**  
   Scraped from public source → GCS

2. **Processing**  
   PySpark → aggregated data → SQL-ready tables

3. **Warehouse Integration**  
   BigQuery as the backend

4. **Visualization**  
   Looker connected to BigQuery

📄 Data Source:  
[PL__ASOS Network](https://mesonet.agron.iastate.edu/request/download.phtml?network=PL__ASOS)

---

## ⚙️ Project Setup Guide

### 🔍 Prerequisites

Install the following:
- Spark, PySpark
- Google Cloud SDK
- Prefect
- Terraform

---

### 🧬 Setup Instructions
![image](https://github.com/user-attachments/assets/654cbc5a-0836-4c79-bf51-a080c0f936b6)

```bash
# 1. Clone the repo
git clone https://github.com/SindhuReddy-hub/Project-Data-Engineering.git
cd Project-Data-Engineering



# 2. Setup virtual environment
python -m venv venv
source venv/bin/activate  # Windows: .\venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt
```

### ☁️ Google Cloud Configuration

```bash
gcloud config set project Project-Data-Engineering

gcloud auth login
```

---

### 🌱 Terraform Setup

Edit `terraform.tfvars`:

```hcl
project = "Project-Data-Engineering"
```

Deploy:

```bash
cd terraform/
terraform init
terraform plan
terraform apply
```

---

### ⚙️ Prefect Configuration

```bash
# Start Prefect server
prefect orion start  # Dashboard: http://127.0.0.1:4200

# Register blocks
cd prefect_orchestration/prefect_blocks
python gcp_credentials_blocks.py
python gcs_buckets_blocks.py

# Configure deployments
python prefect_orchestration/deployments/deployments_config.py

# Start agent
prefect agent start -q "default"
```

---

### 🚀 Launch Stage 1 – Data Ingestion

```bash
cd prefect_orchestration/deployments
python deployments_run.py --stage="S1"
```

---

### 🔄 Stage 2 – Data Transformation

Ensure paths in `gcloud_submit_job.sh` are correct.

Run PySpark job:

```bash
gcloud dataproc jobs submit pyspark \
--cluster=metar-cluster \
--region=europe-west2 \
--jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
--files=gs://code-metar-bucket-2/code/sql_queries_config.yaml \
gs://code-metar-bucket-2/code/pyspark_sql.py \
-- \
  --input=gs://batch-metar-bucket-2/data/ES__ASOS/*/* \
  --bq_output=reports.ES__ASOS \
  --temp_bucket=dataproc-staging-bucket-metar-bucket-2
```

Upload supporting files:

```bash
gsutil cp pyspark_sql.py gs://code-metar-bucket-2/code/
gsutil cp sql_queries_config.yaml gs://code-metar-bucket-2/code/
```

Trigger Stage 2:

```bash
python deployments_run.py --stage="S2"
```
