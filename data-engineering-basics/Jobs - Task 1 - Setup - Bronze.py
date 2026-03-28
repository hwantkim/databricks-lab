# Databricks notebook source
# MAGIC %md
# MAGIC # 태스크 1 - 설정 및 브론즈 테이블
# MAGIC 이 노트북은 **02-Creating a Simple Lakeflow Job** 노트북의 지침에 따라 작업의 태스크 1에 사용됩니다

# COMMAND ----------

# MAGIC %md
# MAGIC ## 작업 매개변수 캡처

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 환경 구성

# COMMAND ----------

# MAGIC %md
# MAGIC 1. 기본 카탈로그와 스키마를 설정합니다.

# COMMAND ----------

# 카탈로그와 스키마 설정
spark.sql(f'USE CATALOG {catalog_name}')
spark.sql(f'USE SCHEMA {schema_name}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 브론즈
# MAGIC **목표:** **myfiles** 볼륨의 모든 CSV 파일을 사용하여 테이블을 생성합니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 빈 테이블과 컬럼 생성
# MAGIC CREATE TABLE IF NOT EXISTS current_employees_bronze_job (
# MAGIC   ID INT,
# MAGIC   FirstName STRING,
# MAGIC   Country STRING,
# MAGIC   Role STRING
# MAGIC   );

# COMMAND ----------

# 브론즈 원시 수집 테이블을 생성하고 행에 파일 이름 포함
spark.sql(f'''
  COPY INTO current_employees_bronze_job
  FROM '/Volumes/{catalog_name}/{schema_name}/myfiles/'
  FILEFORMAT = CSV
  FORMAT_OPTIONS (
    'header' = 'true', 
    'inferSchema' = 'true'
)
''').display()
