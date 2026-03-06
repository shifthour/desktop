# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
# MAGIC ^1_3 02/10/09 11:16:45 Batch  15017_40611 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/29/08 08:55:59 Batch  14639_32164 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/28/07 10:13:19 Batch  14607_36806 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/26/07 09:37:10 Batch  14575_34636 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 08/30/07 07:19:35 Batch  14487_26382 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 08/27/07 10:06:05 Batch  14484_36371 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_3 03/26/07 12:34:37 Batch  14330_45286 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 03/07/07 15:24:05 Batch  14311_55447 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_1 02/26/07 15:19:29 Batch  14302_55338 PROMOTE bckcetl ids20 dsadm rc for Brent
# MAGIC ^1_1 02/26/07 15:12:16 Batch  14302_54748 INIT bckcett testIDS30 dsadm rc for Brent 
# MAGIC ^1_1 02/23/07 13:40:37 Batch  14299_49241 PROMOTE bckcett testIDS30 u08717 Brent
# MAGIC ^1_1 02/23/07 13:39:28 Batch  14299_49172 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC Â© Copyright 2007 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  IdsClmAllDelCntl
# MAGIC                
# MAGIC 
# MAGIC PROCESSING:   If the deleted claim exist in the CUST_SVC_TASK_LINK table update the CLM_SK to NA.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Brent Leland            2007-02-22       IAD Prod. Sup.      Original Programming.                                                                  devlIDS10

# MAGIC User created file /ids/prod/update/IdsClmDelList.dat is moved by control job to /ids/prod/landing
# MAGIC IDS Claim Hit List Update to Customer Service Task Link
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Retrieve parameter values
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# Stage: IdsClmDelList (CSeqFileStage)
schema_IdsClmDelList = StructType([
    StructField("CLM_ID", StringType(), nullable=False),
    StructField("SRC_SYS_CD_SK", IntegerType(), nullable=False)
])
df_IdsClmDelList = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_IdsClmDelList)
    .load(f"{adls_path_raw}/landing/IdsClmDelList.dat")
)

# Stage: CLM (DB2Connector) - reading from IDS database
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""
SELECT clm.SRC_SYS_CD_SK, clm.CLM_ID, clm.CLM_SK 
FROM {IDSOwner}.CLM clm
JOIN {IDSOwner}.CUST_SVC_TASK_LINK cust
    ON clm.CLM_SK = cust.CLM_SK
"""
df_CLM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Stage: Trans1 (CTransformerStage) - Primary Link (Claims) + Lookup Link (Lkup)
df_Trans1_Join = df_IdsClmDelList.alias("Claims").join(
    df_CLM.alias("Lkup"),
    (col("Claims.SRC_SYS_CD_SK") == col("Lkup.SRC_SYS_CD_SK")) &
    (col("Claims.CLM_ID") == col("Lkup.CLM_ID")),
    how="left"
)
df_Trans1 = df_Trans1_Join.filter(col("Lkup.CLM_SK").isNotNull())

# Only one output column (CLM_SK) is sent forward
df_Update = df_Trans1.select(col("Lkup.CLM_SK").alias("CLM_SK"))

# Stage: CUST_SVC_TASK_LINK_D (CODBCStage) - Perform database MERGE (update on match, insert on no match)
temp_table = "STAGING.IdsClmDelCustSvcUpd_CUST_SVC_TASK_LINK_D_temp"
drop_temp_sql = f"DROP TABLE IF EXISTS {temp_table}"
execute_dml(drop_temp_sql, jdbc_url, jdbc_props)

(
    df_Update
    .write
    .jdbc(url=jdbc_url, table=temp_table, mode="overwrite", properties=jdbc_props)
)

merge_sql = f"""
MERGE INTO {IDSOwner}.CUST_SVC_TASK_LINK AS T
USING {temp_table} AS S
ON T.CLM_SK = S.CLM_SK
WHEN MATCHED THEN
  UPDATE SET T.CLM_SK = 1
WHEN NOT MATCHED THEN
  INSERT (CLM_SK)
  VALUES (S.CLM_SK);
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)