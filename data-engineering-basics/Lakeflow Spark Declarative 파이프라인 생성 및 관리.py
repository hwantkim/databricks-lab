# Databricks notebook source
####################################################################################
# 카탈로그, 스키마, 볼륨 이름에 대한 Python 변수 설정 (필요시 변경)
catalog_name = "dbacademy"
schema_name = "create_pipeline"
volume_name = "myfiles"
####################################################################################

####################################################################################
# 카탈로그, 스키마, 볼륨이 아직 존재하지 않는 경우 생성
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.{volume_name}")
####################################################################################

####################################################################################
# 지정된 catalog.schema.volume에 employees.csv라는 파일을 생성
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
# 지정된 catalog.schema.volume에 employees2.csv라는 파일을 생성
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

####################################################################################
# 루트 폴더 및 소스 코드 파일의 경로 출력
path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
newpath = path.replace('Lakeflow Spark Declarative 파이프라인 생성 및 관리','Pipeline Files')
rootpath = newpath
sourcefilepath = newpath + '/Pipeline - 1.py'
print(f'태스크를 위한 노트북 경로:\n')
print(f'  * 루트 폴더 경로: \n   {rootpath}\n')
print(f'  * 소스 파일 경로: \n   {sourcefilepath}')
####################################################################################
