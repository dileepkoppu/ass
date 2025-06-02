# Transaction Pattern Detection System

This project is a real-time detection pipeline using **PySpark on Databricks**, integrated with **AWS S3** and **PostgreSQL (RDS)**.

---

##  Overview

### What It Does

####  Mechanism X (Chunk Creator)
- Runs every second
- Reads the next 10,000 rows from a local file (`transactions.csv`)
- Uploads each chunk to a folder in **AWS S3**

#### ⚙ Mechanism Y (Detection Engine)
- Watches the S3 folder for new chunks
- On new chunk arrival, performs 3 detections:
  - **PatId1 (UPGRADE)**: Customer in top 1% for a merchant (by count) and bottom 1% by weight — merchant must have 50K+ txns
  - **PatId2 (CHILD)**: Customer made 80+ txns with a merchant, and avg txn < ₹23
  - **PatId3 (DEI-NEEDED)**: Merchant where Female < Male and Female > 100
- Writes 50 detection results per unique file to another S3 folder
- Uses PostgreSQL to:
  - Track total transaction count per merchant
  - Track processed chunk files

##  Tech Stack

- **Databricks (PySpark)**
- **AWS S3** (input/output chunk files)
- **PostgreSQL RDS** (tracking merchants and processed chunks)


## Setup Steps

1. Mount **S3 bucket** on Databricks
2. Upload:
   - `transactions.csv`
   - `CustomerImportance.csv`
3. Start:
   - **Mechanism X** notebook (push chunks to S3 every second)
   - **Mechanism Y** notebook (watch S3, detect patterns)
4. Output detection files will be saved to S3 (50 records per file)

## Output

- Results saved in S3:
  - Format: CSV
  - One file per 50 detection rows
  - Fields: `YStartTime`, `detectionTime`, `patternId`, `actionType`, `customerName`, `MerchantId`
