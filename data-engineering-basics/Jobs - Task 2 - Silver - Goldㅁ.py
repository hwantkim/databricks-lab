# Databricks notebook source
# MAGIC %md
# MAGIC # 작업 2 - 실버 - 골드 테이블
# MAGIC 이 노트북은 **Jobs - Creating a Simple Lakeflow Job** 노트북의 지시사항에 따라 작업 2에 사용됩니다

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
# MAGIC ### 실버
# MAGIC **목표**: 브론즈 테이블을 변환하여 실버 테이블을 생성합니다.
# MAGIC
# MAGIC 1. **current_employees_bronze_job** 테이블에서 **current_employees_silver_job** 테이블을 생성합니다. 
# MAGIC
# MAGIC     테이블은 다음을 수행합니다:
# MAGIC     - **ID**, **FirstName**, **Country** 컬럼을 선택합니다.
# MAGIC     - **Role** 컬럼을 대문자로 변환합니다.
# MAGIC     - 두 개의 새 컬럼을 추가합니다: **CurrentTimeStamp**와 **CurrentDate**.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 최종 실버 테이블에 데이터를 병합하는 데 사용할 임시 뷰 생성
# MAGIC CREATE OR REPLACE TABLE current_employees_silver_job AS 
# MAGIC SELECT 
# MAGIC   ID,
# MAGIC   FirstName,
# MAGIC   Country,
# MAGIC   upper(Role) as Role,                 -- Role 컬럼을 대문자로 변환
# MAGIC   current_timestamp() as CurrentTimeStamp,    -- 현재 날짜 시간 가져오기
# MAGIC   date(CurrentTimeStamp) as CurrentDate       -- 날짜 가져오기
# MAGIC FROM current_employees_bronze_job;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 골드
# MAGIC **목표:** 실버 테이블을 집계하여 최종 골드 테이블을 생성합니다.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 골드
# MAGIC **목표:** 실버 테이블을 집계하여 최종 골드 테이블을 생성합니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW temp_view_total_roles_job AS 
# MAGIC SELECT
# MAGIC   Role, 
# MAGIC   count(*) as TotalEmployees
# MAGIC FROM current_employees_silver_job
# MAGIC GROUP BY Role;

# COMMAND ----------

# MAGIC %md
# MAGIC 2. 지정된 컬럼으로 **total_roles_gold_job**이라는 최종 골드 테이블을 생성합니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS total_roles_gold_job (
# MAGIC   Role STRING,
# MAGIC   TotalEmployees INT
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC 3. 집계된 임시 뷰 **temp_view_total_roles_job**의 모든 행을 **total_roles_gold_job** 테이블에 삽입하여 테이블의 기존 데이터를 덮어씁니다. 이는 테이블의 데이터를 덮어쓰지만 기존 스키마와 테이블 정의 및 속성은 유지합니다.
# MAGIC
# MAGIC     다음을 확인하세요:
# MAGIC     - **num_affected_rows**가 *4*
# MAGIC     - **num_inserted_rows**가 *4*

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE TABLE total_roles_gold_job
# MAGIC SELECT * 
# MAGIC FROM temp_view_total_roles_job;
