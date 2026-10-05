# Automated ETL and ELT Data Pipeline

## Project Overview

This project demonstrates an automated data engineering pipeline using Python, MySQL, SQL, CSV files, data validation, incremental loading, error handling, audit logging, and ELT processing.

The project implements both ETL and ELT approaches.

### ETL

Extract → Transform → Load

CSV files are extracted using Python, validated and transformed, and then loaded into MySQL.

### ELT

Extract → Load → Transform

CSV data is first loaded into MySQL staging tables. SQL transformations are then performed inside MySQL before loading the final tables.

---

# Project Objective

The objective of this project is to build a reliable data pipeline that can:

- Extract data from CSV files
- Validate incoming data
- Transform raw data
- Load data into MySQL
- Perform incremental loading
- Handle invalid records
- Maintain error logs
- Maintain audit logs
- Implement an ELT workflow
- Store data in staging and final tables
- Prevent duplicate processing
- Support repeatable pipeline execution

---

# Architecture

```text
                    SOURCE
                      |
                      v
                CSV FILES
                      |
          +-----------+-----------+
          |                       |
          v                       v
        ETL                       ELT
          |                       |
          v                       v
   Python Extraction       MySQL Staging
          |                       |
          v                       v
      Validation          SQL Transformation
          |                       |
          v                       v
     Transformation               |
          |                       |
          v                       v
  Incremental Loading      Final MySQL Tables
          |                       |
          +-----------+-----------+
                      |
                      v
               Audit Logging
                      |
                      v
                Error Logging