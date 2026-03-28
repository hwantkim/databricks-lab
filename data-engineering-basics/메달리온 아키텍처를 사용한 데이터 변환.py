# Databricks notebook source
####################################################################################
# catalog, schema, volume 이름에 대한 Python 변수 설정 (필요한 경우 변경)
catalog_name = "dbacademy"
schema_name = "transforming_data"
volume_name = "myfiles"
####################################################################################

####################################################################################
# catalog, schema, volume이 아직 없으면 생성
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.{volume_name}")
####################################################################################

####################################################################################
# 지정된 catalog.schema.volume에 employees.csv 파일 생성
import pandas as pd
data = [
    ["1111", "Kristi", "USA", "Manager"],
    ["2222", "Sophia", "Greece", "Developer"],
    ["3333", "Peter", "USA", "Developer"],
    ["4444", "Zebi", "Pakistan", "Administrator"]
]
columns = ["ID", "Firstname", "Country", "Role"] 
df = pd.DataFrame(data, columns=columns)
file_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/employees.csv"
df.to_csv(file_path, index=False)
################################################################################

####################################################################################
# 지정된 catalog.schema.volume에 employees2.csv 파일 생성
data = [
    [5555, 'Alex','USA', 'Instructor'],
    [6666, 'Sanjay','India', 'Instructor']
]
columns = ["ID","Firstname", "Country", "Role"]

## DataFrame 생성
df = pd.DataFrame(data, columns=columns)

## 코스 Catalog.Schema.Volume에 CSV 파일 생성
df.to_csv(f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/employees2.csv", index=False)
####################################################################################

# COMMAND ----------

## 기본 catalog과 schema 설정
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_name}")

# COMMAND ----------

spark.sql(f"LIST '/Volumes/{catalog_name}/{schema_name}/{volume_name}/' ").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 데모 목적으로 테이블이 존재하면 삭제
# MAGIC DROP TABLE IF EXISTS current_employees_bronze;
# MAGIC
# MAGIC -- 빈 테이블과 열 생성
# MAGIC CREATE TABLE IF NOT EXISTS current_employees_bronze (
# MAGIC   ID INT,
# MAGIC   FirstName STRING,
# MAGIC   Country STRING,
# MAGIC   Role STRING
# MAGIC   );

# COMMAND ----------

## bronze 원시 수집 테이블을 생성하고 행에 대한 CSV 파일 이름을 포함
spark.sql(f'''
  COPY INTO current_employees_bronze
  FROM '/Volumes/{catalog_name}/{schema_name}/{volume_name}/'
  FILEFORMAT = CSV
  FORMAT_OPTIONS (
    'header' = 'true', 
    'inferSchema' = 'true'
)
''').display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM current_employees_bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 데이터를 최종 silver 테이블에 병합하는 데 사용할 임시 뷰 생성
# MAGIC CREATE OR REPLACE TABLE current_employees_silver AS 
# MAGIC SELECT 
# MAGIC   ID,
# MAGIC   FirstName,
# MAGIC   Country,
# MAGIC   upper(Role) as Role,                 -- Role 열을 대문자로 변환
# MAGIC   current_timestamp() as CurrentTimeStamp,    -- 현재 날짜시간 가져오기
# MAGIC   date(CurrentTimeStamp) as CurrentDate       -- 날짜 가져오기
# MAGIC FROM current_employees_bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM current_employees_silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW temp_view_total_roles AS 
# MAGIC SELECT
# MAGIC   Role, 
# MAGIC   count(*) as TotalEmployees
# MAGIC FROM current_employees_silver
# MAGIC GROUP BY Role;
# MAGIC
# MAGIC
# MAGIC SELECT *
# MAGIC FROM temp_view_total_roles;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS total_roles_gold (
# MAGIC   Role STRING,
# MAGIC   TotalEmployees INT
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE TABLE total_roles_gold
# MAGIC SELECT * 
# MAGIC FROM temp_view_total_roles;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM total_roles_gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY total_roles_gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP SCHEMA IF EXISTS transforming_data CASCADE;
