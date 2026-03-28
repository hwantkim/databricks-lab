# Databricks notebook source
####################################################################################
# 카탈로그, 스키마, 볼륨 이름을 위한 Python 변수 설정 (원하면 변경 가능)
catalog_name = "dbacademy"
schema_name = "ingesting_data"
volume_name = "myfiles"
####################################################################################

####################################################################################
# 카탈로그, 스키마, 볼륨이 없으면 생성합니다
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.{volume_name}")
####################################################################################

####################################################################################
# 지정된 catalog.schema.volume에 employees.csv라는 파일을 생성합니다
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

# COMMAND ----------

## 기본 카탈로그와 스키마 설정
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 스키마에서 사용 가능한 테이블 표시
# MAGIC SHOW TABLES;

# COMMAND ----------

# # 기본 카탈로그와 스키마 설정 (Spark 3.4.0 이상 필요)
spark.catalog.setCurrentCatalog(catalog_name)
spark.catalog.setCurrentDatabase(schema_name)

# 스키마에서 사용 가능한 테이블 표시
spark.catalog.listTables(schema_name)

# COMMAND ----------

spark.sql(f"LIST '/Volumes/{catalog_name}/{schema_name}/{volume_name}/' ").display()

# COMMAND ----------

#  데모를 위해 테이블이 있으면 삭제합니다
spark.sql(f"DROP TABLE IF EXISTS current_employees_ctas;")

#  CTAS를 사용하여 테이블 생성
spark.sql(f"""
CREATE TABLE current_employees_ctas
AS
SELECT ID, FirstName, Country, Role 
FROM read_files(
  '/Volumes/{catalog_name}/{schema_name}/{volume_name}/',
  format => 'csv',
  header => true,
  inferSchema => true
 );""")

#  스키마에서 사용 가능한 테이블 표시
spark.sql(f"SHOW TABLES;").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM employees_upload

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 데모를 위해 테이블이 있으면 삭제합니다
# MAGIC DROP TABLE IF EXISTS current_employees_copyinto;
# MAGIC
# MAGIC -- 열 데이터 타입으로 빈 테이블 생성
# MAGIC CREATE TABLE current_employees_copyinto (
# MAGIC   ID INT,
# MAGIC   FirstName STRING,
# MAGIC   Country STRING,
# MAGIC   Role STRING
# MAGIC );

# COMMAND ----------

spark.sql(f"""
COPY INTO current_employees_copyinto
  FROM '/Volumes/{catalog_name}/{schema_name}/{volume_name}/'
  FILEFORMAT = CSV
  FORMAT_OPTIONS (
      'header' = 'true', 
      'inferSchema' = 'true'
    )
  """).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM current_employees_copyinto;

# COMMAND ----------

## 볼륨에 새 employees2.csv 파일을 생성합니다
dbutils.fs.cp(f'/Volumes/{catalog_name}/{schema_name}/{volume_name}/employees.csv', f'/Volumes/{catalog_name}/{schema_name}/{volume_name}/employees2.csv')

## myfiles 볼륨의 파일들을 표시합니다
files = dbutils.fs.ls(f'/Volumes/dbacademy/{schema_name}/myfiles')
display(files)

# COMMAND ----------

spark.sql(f"""
SELECT 
  ID, 
  FirstName, 
  Country, 
  Role 
FROM read_files(
  '/Volumes/{catalog_name}/{schema_name}/{volume_name}/employees2.csv',
  format => 'csv',
  header => true,
  inferSchema => true
 );""").display()

# COMMAND ----------

spark.sql(f"""
COPY INTO current_employees_copyinto
  FROM '/Volumes/dbacademy/ingesting_data/myfiles'
  FILEFORMAT = CSV
  FORMAT_OPTIONS (
      'header' = 'true', 
      'inferSchema' = 'true'
    )
  """).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC Select *
# MAGIC from current_employees_copyinto;

# COMMAND ----------

# MAGIC %sql
# MAGIC dbacademy.ingesting_data.current_employees_copyinto
