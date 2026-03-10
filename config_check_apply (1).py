# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG ecommerce;
# MAGIC USE SCHEMA bronze;

# COMMAND ----------

# MAGIC
# MAGIC %sql DROP TABLE IF EXISTS bronze_customer
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ecommerce;
# MAGIC USE SCHEMA bronze;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS bronze_customer (
# MAGIC     customer_id STRING,
# MAGIC     customer_name STRING,
# MAGIC     email STRING,
# MAGIC     phone STRING,
# MAGIC     address STRING,
# MAGIC     segment STRING,
# MAGIC     country STRING,
# MAGIC     city STRING,
# MAGIC     state STRING,
# MAGIC     postal_code STRING,
# MAGIC     region STRING,
# MAGIC     ingestion_timestamp TIMESTAMP,
# MAGIC     batch_id STRING,
# MAGIC     source_file_name STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ecommerce;
# MAGIC USE SCHEMA sliver;

# COMMAND ----------

# MAGIC %sql DROP TABLE IF EXISTS dim_customer

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dim_customer (
# MAGIC     customer_sk            BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     customer_id            STRING,
# MAGIC     customer_name          STRING,
# MAGIC     email                  STRING,
# MAGIC     phone_number           STRING,
# MAGIC     address                STRING,
# MAGIC     customer_segment       STRING,
# MAGIC     country                STRING,
# MAGIC     city                   STRING,
# MAGIC     state                  STRING,
# MAGIC     postal_code            STRING,
# MAGIC     region                 STRING,
# MAGIC
# MAGIC     -- SCD Type 2 Columns
# MAGIC     effective_start_date   DATE,
# MAGIC     effective_end_date     DATE,
# MAGIC     is_current             STRING,
# MAGIC
# MAGIC     -- Audit Columns
# MAGIC     record_created_date    TIMESTAMP,
# MAGIC     record_updated_date    TIMESTAMP,
# MAGIC     batch_id               STRING,
# MAGIC     record_hash            STRING,
# MAGIC     source_file_name       STRING
# MAGIC     
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ecommerce;
# MAGIC USE SCHEMA error;

# COMMAND ----------

# MAGIC
# MAGIC %sql DROP TABLE IF EXISTS error_customer_error

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS error_customer_error (
# MAGIC
# MAGIC     -- Raw Source Columns
# MAGIC     customer_id          STRING,
# MAGIC     customer_name        STRING,
# MAGIC     email                STRING,
# MAGIC     phone                STRING,
# MAGIC     address              STRING,
# MAGIC     segment              STRING,
# MAGIC     country              STRING,
# MAGIC     city                 STRING,
# MAGIC     state                STRING,
# MAGIC     postal_code          STRING,
# MAGIC     region               STRING,
# MAGIC
# MAGIC     -- Error Columns
# MAGIC     error_code           STRING,
# MAGIC     error_description    STRING,
# MAGIC
# MAGIC     -- Audit Columns
# MAGIC     source_system        STRING,
# MAGIC     batch_id             STRING,
# MAGIC     file_name            STRING,
# MAGIC     ingestion_timestamp  TIMESTAMP,
# MAGIC     error_timestamp      TIMESTAMP,
# MAGIC     source_file_name       STRING
# MAGIC
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ecommerce;
# MAGIC USE SCHEMA error;

# COMMAND ----------

# MAGIC %sql DROP TABLE IF EXISTS dim_customer_error

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dim_customer_error (
# MAGIC
# MAGIC     -- Business Key
# MAGIC     customer_id            STRING,
# MAGIC
# MAGIC     -- Transformed Columns
# MAGIC     customer_name          STRING,
# MAGIC     email                  STRING,
# MAGIC     phone_number           STRING,
# MAGIC     address                STRING,
# MAGIC     customer_segment       STRING,
# MAGIC     country                STRING,
# MAGIC     city                   STRING,
# MAGIC     state                  STRING,
# MAGIC     postal_code            STRING,
# MAGIC     region                 STRING,
# MAGIC
# MAGIC     -- Error Columns
# MAGIC     error_code             STRING,
# MAGIC     error_description      STRING,
# MAGIC     failed_stage           STRING,
# MAGIC
# MAGIC     -- Audit Columns
# MAGIC     source_system          STRING,
# MAGIC     batch_id               STRING,
# MAGIC     record_hash            STRING,
# MAGIC     error_timestamp        TIMESTAMP,
# MAGIC     source_file_name       STRING
# MAGIC
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ecommerce;
# MAGIC USE SCHEMA bronze;

# COMMAND ----------

# MAGIC
# MAGIC %sql DROP TABLE IF EXISTS bronze_product

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bronze_product (
# MAGIC     product_id STRING,
# MAGIC     category STRING,
# MAGIC     sub_category STRING,
# MAGIC     product_name STRING,
# MAGIC     state STRING,
# MAGIC     price_per_product STRING,   -- kept as STRING in bronze (raw format)
# MAGIC     
# MAGIC     batch_id STRING,
# MAGIC     ingestion_timestamp TIMESTAMP,
# MAGIC     source_file_name STRING    
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ecommerce;
# MAGIC USE SCHEMA sliver;

# COMMAND ----------

# MAGIC %sql DROP TABLE IF EXISTS silver_dim_product

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver_dim_product (
# MAGIC     
# MAGIC     -- Surrogate Key
# MAGIC     product_sk BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     
# MAGIC     -- Business Key
# MAGIC     product_id STRING NOT NULL,
# MAGIC     
# MAGIC     -- Attributes
# MAGIC     category STRING,
# MAGIC     sub_category STRING,
# MAGIC     product_name STRING,
# MAGIC     state STRING,
# MAGIC     price_per_product DECIMAL(10,2),
# MAGIC     
# MAGIC     -- SCD Type 2 Columns
# MAGIC     effective_start_date DATE NOT NULL,
# MAGIC     effective_end_date DATE NOT NULL,
# MAGIC     is_current STRING NOT NULL,
# MAGIC     version_number INT NOT NULL,
# MAGIC     
# MAGIC     -- Audit Columns
# MAGIC     created_date TIMESTAMP,
# MAGIC     updated_date TIMESTAMP,
# MAGIC     batch_id STRING,
# MAGIC     source_system STRING,
# MAGIC     source_file_name       STRING
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ecommerce;
# MAGIC USE SCHEMA error;

# COMMAND ----------

# MAGIC %sql DROP TABLE IF EXISTS bronze_product_error

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bronze_product_error (
# MAGIC     
# MAGIC     -- Raw Data Columns
# MAGIC     product_id STRING,
# MAGIC     category STRING,
# MAGIC     sub_category STRING,
# MAGIC     product_name STRING,
# MAGIC     state STRING,
# MAGIC     price_per_product STRING,
# MAGIC     
# MAGIC     -- Error Metadata
# MAGIC     error_column STRING,
# MAGIC     error_description STRING,
# MAGIC     error_record STRING,              -- full raw row (JSON)
# MAGIC     error_timestamp TIMESTAMP,
# MAGIC     
# MAGIC     -- Audit
# MAGIC     batch_id STRING,
# MAGIC     source_file_name STRING,
# MAGIC     pipeline_name STRING
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ecommerce;
# MAGIC USE SCHEMA error;

# COMMAND ----------

# MAGIC %sql DROP TABLE IF EXISTS silver_dim_product_error

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver_dim_product_error (
# MAGIC     
# MAGIC     -- Business Key
# MAGIC     product_id STRING,
# MAGIC     
# MAGIC     -- Cleaned Columns Attempted
# MAGIC     category STRING,
# MAGIC     sub_category STRING,
# MAGIC     product_name STRING,
# MAGIC     state STRING,
# MAGIC     price_per_product STRING,
# MAGIC     
# MAGIC     -- Error Information
# MAGIC     error_type STRING,                 -- DATA_TYPE / BUSINESS_RULE / DUPLICATE
# MAGIC     error_column STRING,
# MAGIC     error_description STRING,
# MAGIC     
# MAGIC     -- SCD Info (if applicable)
# MAGIC     attempted_version INT,
# MAGIC     
# MAGIC     -- Audit
# MAGIC     batch_id STRING,
# MAGIC     source_system STRING,
# MAGIC     error_timestamp TIMESTAMP,
# MAGIC     source_file_name STRING
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ecommerce;
# MAGIC USE SCHEMA bronze;

# COMMAND ----------

# MAGIC %sql DROP TABLE IF EXISTS bronze_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bronze_orders (
# MAGIC     
# MAGIC     row_id STRING,
# MAGIC     order_id STRING,
# MAGIC     order_date STRING,
# MAGIC     ship_date STRING,
# MAGIC     ship_mode STRING,
# MAGIC     customer_id STRING,
# MAGIC     product_id STRING,
# MAGIC     quantity STRING,
# MAGIC     price STRING,
# MAGIC     discount STRING,
# MAGIC     profit STRING,
# MAGIC     
# MAGIC     -- Metadata
# MAGIC     batch_id STRING,
# MAGIC     ingestion_timestamp TIMESTAMP,
# MAGIC     source_file_name STRING
# MAGIC
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ecommerce;
# MAGIC USE SCHEMA sliver;

# COMMAND ----------

# MAGIC %sql DROP TABLE IF EXISTS silver_fact_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver_fact_orders (
# MAGIC
# MAGIC     -- Surrogate Key
# MAGIC     order_sk BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     
# MAGIC     -- Business Keys (Composite)
# MAGIC     order_id STRING NOT NULL,
# MAGIC     product_id STRING NOT NULL,
# MAGIC     
# MAGIC     -- Business Columns
# MAGIC     order_date DATE,
# MAGIC     ship_date DATE,
# MAGIC     ship_mode STRING,
# MAGIC     customer_id STRING,
# MAGIC     quantity INT,
# MAGIC     price DECIMAL(10,2),
# MAGIC     discount DECIMAL(5,2),
# MAGIC     profit DECIMAL(10,2),
# MAGIC     
# MAGIC     -- SCD Type 2 Columns
# MAGIC     effective_start_date DATE NOT NULL,
# MAGIC     effective_end_date DATE NOT NULL,
# MAGIC     is_current STRING NOT NULL,
# MAGIC     version_number INT NOT NULL,
# MAGIC     
# MAGIC     -- Audit Columns
# MAGIC     created_date TIMESTAMP,
# MAGIC     updated_date TIMESTAMP,
# MAGIC     batch_id STRING,
# MAGIC     source_system STRING,
# MAGIC     source_file_name STRING
# MAGIC
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ecommerce;
# MAGIC USE SCHEMA error;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bronze_orders_error (
# MAGIC
# MAGIC     row_id STRING,
# MAGIC     order_id STRING,
# MAGIC     order_date STRING,
# MAGIC     ship_date STRING,
# MAGIC     ship_mode STRING,
# MAGIC     customer_id STRING,
# MAGIC     product_id STRING,
# MAGIC     quantity STRING,
# MAGIC     price STRING,
# MAGIC     discount STRING,
# MAGIC     profit STRING,
# MAGIC
# MAGIC     error_column STRING,
# MAGIC     error_description STRING,
# MAGIC     error_record STRING,
# MAGIC     error_timestamp TIMESTAMP,
# MAGIC
# MAGIC     batch_id STRING,
# MAGIC     source_file_name STRING,
# MAGIC     pipeline_name STRING
# MAGIC
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ecommerce;
# MAGIC USE SCHEMA error;

# COMMAND ----------

# MAGIC %sql DROP TABLE IF EXISTS silver_fact_orders_error

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver_fact_orders_error (
# MAGIC
# MAGIC     order_id STRING,
# MAGIC     product_id STRING,
# MAGIC     
# MAGIC     error_type STRING,
# MAGIC     error_column STRING,
# MAGIC     error_description STRING,
# MAGIC     attempted_version INT,
# MAGIC     
# MAGIC     batch_id STRING,
# MAGIC     source_system STRING,
# MAGIC     error_timestamp TIMESTAMP,
# MAGIC     source_file_name STRING
# MAGIC
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ecommerce;
# MAGIC USE SCHEMA gold;

# COMMAND ----------

# MAGIC %sql DROP TABLE IF EXISTS gold.gold_sales_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.gold_sales_summary (
# MAGIC
# MAGIC     year INT,
# MAGIC     product_category STRING,
# MAGIC     product_sub_category STRING,
# MAGIC     customer STRING,
# MAGIC     transaction_amount DECIMAL(12,2),
# MAGIC
# MAGIC     -- Audit Columns
# MAGIC     created_at TIMESTAMP,
# MAGIC     updated_at TIMESTAMP,
# MAGIC     source_file_name STRING
# MAGIC
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ecommerce;
# MAGIC USE SCHEMA error;

# COMMAND ----------

# MAGIC %sql DROP TABLE IF EXISTS gold_sales_summary_error

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold_sales_summary_error (
# MAGIC
# MAGIC     year INT,
# MAGIC     product_category STRING,
# MAGIC     product_sub_category STRING,
# MAGIC     customer STRING,
# MAGIC     transaction_amount STRING,
# MAGIC
# MAGIC     error_code STRING,
# MAGIC     error_description STRING,
# MAGIC
# MAGIC     error_record_timestamp TIMESTAMP,
# MAGIC     source_file_name STRING
# MAGIC     )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ecommerce;
# MAGIC USE SCHEMA sliver;

# COMMAND ----------

# MAGIC %sql DROP TABLE IF EXISTS  batch_table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS batch_table (
# MAGIC     batch_id INT,
# MAGIC     batch_start_date TIMESTAMP,
# MAGIC     batch_end_date TIMESTAMP,
# MAGIC     source_file_name STRING,
# MAGIC     record_count INT,
# MAGIC     source_schema STRING,
# MAGIC     source_table STRING,
# MAGIC     target_schema STRING,
# MAGIC     target_table STRING,
# MAGIC     batch_status STRING,
# MAGIC     created_date TIMESTAMP,
# MAGIC     updated_date TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;