# Databricks notebook source
# MAGIC %md
# MAGIC # 간단한 Lakeflow Job 생성하기
# MAGIC
# MAGIC Lakeflow Jobs는 Databricks에서 모든 처리 작업을 예약하고 오케스트레이션할 수 있는 도구 모음을 제공합니다.
# MAGIC
# MAGIC **목표:** Databricks Lakeflow Jobs를 사용하여 두 개의 태스크로 구성된 Job을 생성합니다. 데모 목적으로 Job은 두 개의 노트북으로 분리되어 있습니다:
# MAGIC - **Jobs - Task 1 - Setup - Bronze**
# MAGIC - **Jobs - Task 2 - Silver - Gold**

# COMMAND ----------

####################################################################################
# catalog, schema, volume 이름을 위한 python 변수 설정 (원하는 경우 변경)
catalog_name = "dbacademy"
schema_name = "create_job"
volume_name = "myfiles"
####################################################################################

####################################################################################
# catalog, schema, volume이 없는 경우 생성
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

path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
newpath = path.replace('간단한 Lakeflow Job 생성하기','')
task1path = newpath + 'Jobs - Task 1 - Setup - Bronze'
task2path = newpath + 'Jobs - Task 2 - Silver - Gold'

print(f'Name your LakeFlow Job: {schema_name}_Example\n')
print(f'Catalog name: {catalog_name}')
print(f'Schema name: {schema_name}\n')
print(f'NOTEBOOK PATHS FOR TASKS:\n')
print(f'  * Task 1 notebook path: \n   {task1path}\n')
print(f'  * Task 2 notebook path: \n   {task2path}')
