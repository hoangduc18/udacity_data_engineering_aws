# STEDI Human Balance Analytics

## Project Overview

The **STEDI Human Balance Analytics** project involves building a data lakehouse solution using AWS tools to process, curate, and store sensor data from the **STEDI Step Trainer**. This device and its companion mobile app gather motion data, which is used to train a machine learning model that detects human steps in real-time. As a data engineer, your task is to prepare this data for machine learning analysis by the Data Science team.

## Project Details

STEDI has developed the **STEDI Step Trainer**, a device designed to train users in balance exercises. The step trainer records motion data through sensors, while a companion mobile app captures accelerometer readings (X, Y, and Z directions). Millions of users have adopted this device, and a subset of them has consented to share their data for research purposes.

The goal of this project is to extract, process, and curate data from the step trainer and mobile app to create a clean, structured dataset that STEDI’s Data Science team can use to develop machine learning models. Privacy is a priority, so only data from consenting users will be included in the dataset.

## Requirements

To achieve these goals, the following steps are undertaken:

1. **Data Ingestion and Landing Zones**:
   - Set up **S3 directories** to simulate data sources: `customer_landing`, `step_trainer_landing`, and `accelerometer_landing`.
   - Copy data into these directories to simulate incoming data from the step trainer and mobile app.

2. **Glue Tables for Initial Data Exploration**:
   - Create **Glue tables** for `customer_landing` and `accelerometer_landing`.
   - Query these tables using **AWS Athena** to understand the data structure. Save screenshots of these queries as `customer_landing.png` and `accelerometer_landing.png`.

3. **Data Curation for Trusted Zone**:
   - Write Glue jobs to:
     - Filter customer data, keeping only those who consented to share data, and save it as `customer_trusted`.
     - Filter accelerometer data based on the customers in `customer_trusted`, creating an `accelerometer_trusted` table.
   - Verify the trusted data with Athena and save a screenshot as `customer_trusted.png`.

4. **Data Quality Enhancement**:
   - Resolve a data quality issue in the customer data caused by repeated serial numbers. Match customers with accurate serial numbers using the step trainer data.
   - Curate this cleaned customer data into a new table, `customers_curated`, ensuring only customers with accelerometer data and consent are included.

5. **Final Data Curation and Aggregation**:
   - Use **Glue Studio** to:
     - Create a `step_trainer_trusted` table that filters step trainer records for customers in `customers_curated`.
     - Generate an aggregated table, `machine_learning_curated`, combining step trainer and accelerometer readings for the same timestamp for consenting customers.

## Technologies Used

- **AWS S3**: Storage for landing zone data.
- **AWS Glue**: Data transformation and curation.
- **AWS Athena**: Data exploration and validation.
- **Apache Spark**: Distributed data processing.
- **Python**: Scripted jobs for data processing.

## Project Files

- **customer_landing.sql**: SQL script for creating the customer landing table.
- **accelerometer_landing.sql**: SQL script for creating the accelerometer landing table.
- **Screenshots**:
  - `customer_landing.png`
  - `accelerometer_landing.png`
  - `customer_trusted.png`

## Getting Started

To replicate this project, you’ll need access to an **AWS environment** with **S3**, **Glue**, and **Athena** services. Follow these steps:

1. Clone this repository.
2. Use the SQL scripts provided to set up Glue tables for landing zones.
3. Upload data to the appropriate S3 directories.
4. Run Glue jobs to curate the data according to project requirements.
5. Validate curated data with Athena queries and ensure privacy compliance.
