🎵 Spotify ETL Pipeline using AWS & Snowflake

Automated ETL pipeline for extracting, transforming, and loading (ETL) Spotify data into Snowflake for real-time analytics and visualization.

🔹 Overview

This project automates the end-to-end flow of Spotify playlist data using AWS Lambda, S3, Snowflake, and Power BI.
The pipeline extracts data from the Spotify Web API, processes and validates it, and loads it into Snowflake for analytics dashboards in Power BI.

🔹 Workflow
1. Extract (Data Ingestion)

AWS Lambda connects to Spotify API using Spotipy library.
Extracted data is stored in Amazon S3 → raw_data/to_processed/.
Amazon EventBridge (CloudWatch) triggers the Lambda on a schedule (daily/hourly).

2. Transform (Data Processing)

A second AWS Lambda function cleans, validates, and transforms raw JSON.
Validations include record count and mandatory track checks.
Transformed data is saved to Amazon S3 → processed/.
A separate tracklist file (tracklist/) is also maintained.

3. Load (Data Warehousing & Analytics)

Snowpipe auto-ingests processed data from S3 into Snowflake tables.
Snowflake schemas organize data into tracks, albums, and artists.
Power BI connects to Snowflake for interactive dashboards and analytics.

🔹 Tech Stack

Cloud Services: AWS Lambda, S3, EventBridge (CloudWatch), Snowflake, Power BI

Programming Languages: Python (Spotipy, Boto3, Pandas)

Data Warehouse: Snowflake

Visualization: Power BI

API Integration: Spotify Web API

Automation: AWS EventBridge, Snowpipe

🔹 Key Features

✔ Fully automated ETL workflow using AWS Lambda and Snowflake
✔ Real-time ingestion with Snowpipe + S3 notifications
✔ Data validation during transformation (record counts, track checks)
✔ Scalable, serverless, cost-efficient architecture
✔ Analytics-ready data in Snowflake for BI tools
✔ Power BI dashboards for trend analysis and insights

🔹 Future Enhancements

Add CI/CD pipeline with GitHub Actions for Lambda deployments.
Extend transformations to compute artist popularity trends.
Implement error notifications via SNS/Slack on failed ETL runs.
Add historical data snapshots for trend analysis.

🔹 Getting Started

Clone the repo & configure AWS and Snowflake credentials.
Deploy Lambda functions with required IAM roles.
Run the SQL scripts in Snowflake to create database, schema, and Snowpipe.
Schedule EventBridge rules for automation.
Connect Power BI to Snowflake → build dashboards.

🚀 With this pipeline, Spotify data flows seamlessly from API → S3 → Snowflake → Power BI, enabling real-time analytics and insights.
