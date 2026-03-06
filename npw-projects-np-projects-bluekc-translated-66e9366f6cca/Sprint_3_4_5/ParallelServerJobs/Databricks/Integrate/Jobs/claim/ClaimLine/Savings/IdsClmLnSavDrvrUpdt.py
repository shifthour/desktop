# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
# MAGIC ^1_3 02/10/09 11:16:45 Batch  15017_40611 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/29/08 08:55:59 Batch  14639_32164 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/28/07 10:13:19 Batch  14607_36806 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/26/07 09:37:10 Batch  14575_34636 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 08/30/07 07:19:35 Batch  14487_26382 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 08/27/07 10:06:05 Batch  14484_36371 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 06/21/07 14:03:38 Batch  14417_50622 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 05/25/07 11:20:32 Batch  14390_40859 PROMOTE bckcetl ids20 dsadm rc for steph 
# MAGIC ^1_1 05/25/07 11:11:59 Batch  14390_40332 INIT bckcett testIDS30 dsadm rc for steph 
# MAGIC ^1_1 05/16/07 11:48:45 Batch  14381_42530 PROMOTE bckcett testIDS30 u08717 Brent
# MAGIC ^1_1 05/16/07 11:43:58 Batch  14381_42243 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC Â© Copyright 2007 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  Claim Line Savings sequencer
# MAGIC                
# MAGIC 
# MAGIC PROCESSING:   This program will read the driver table, accumulate amounts by claim, claim line, and claim line disallow if the bypass indicator is 'Y', then sets all rows for that claim line
# MAGIC                             to the correct disallow code.  It will also create a second driver table, W_CLM_LN_SAV_DRVR2, with claim lines with multiple disallow codes that need to be updated.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Steph Goddard         4/25/2007       HDMS                    New Program                                                                              devlIDS30                      Brent Leland               05/15/2007

# MAGIC This program will calculate override disallow source codes
# MAGIC Determine which responsibility source code takes precedence if more than one
# MAGIC Create second driver table of records with more than one responsibility source code so they can be resolved
# MAGIC Update all rows with the responsibility source code determined to take precedence
# MAGIC Extract all records from the driver table with a bypass indicator of 'Y'
# MAGIC Summarize by responsibility source code - count 'N', 'P', and 'M'
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import when, col, lit, rpad, sum as sum_, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# --------------------------------------------------------------------------------
# Stage: GetW_CLM_LN_SAV_DRVR (DB2Connector - Read)
# --------------------------------------------------------------------------------
extract_query_GetW_CLM_LN_SAV_DRVR = """
SELECT DRVR.CLM_ID as CLM_ID,
       DRVR.CLM_LN_SEQ_NO as CLM_LN_SEQ_NO,
       DRVR.RESP_SRC_CD as RESP_SRC_CD,
       DRVR.CLM_LN_DSALW_TYP_SRC_CD as CLM_LN_DSALW_TYP_SRC_CD
FROM {owner}.W_CLM_LN_SAV_DRVR DRVR
WHERE BYPS_IN = 'Y'
""".format(owner=IDSOwner)

df_GetW_CLM_LN_SAV_DRVR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_GetW_CLM_LN_SAV_DRVR)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: hf_clmlnsavupdt_drvr (CHashedFileStage Scenario A - Deduplicate)
# --------------------------------------------------------------------------------
# Key columns for dedup: CLM_ID, CLM_LN_SEQ_NO, RESP_SRC_CD
df_hf_clmlnsavupdt_drvr = dedup_sort(
    df_GetW_CLM_LN_SAV_DRVR,
    ["CLM_ID","CLM_LN_SEQ_NO","RESP_SRC_CD"],
    []
)

# --------------------------------------------------------------------------------
# Stage: W_CLM_LN_SAV_DRVR (DB2Connector - Read)
# --------------------------------------------------------------------------------
extract_query_W_CLM_LN_SAV_DRVR = """
SELECT DRVR.CLM_ID as CLM_ID,
       DRVR.CLM_LN_SEQ_NO as CLM_LN_SEQ_NO,
       DRVR.RESP_SRC_CD as RESP_SRC_CD,
       DRVR.CLM_LN_DSALW_TYP_SRC_CD as CLM_LN_DSALW_TYP_SRC_CD
FROM {owner}.W_CLM_LN_SAV_DRVR DRVR
WHERE DRVR.BYPS_IN = 'Y'
""".format(owner=IDSOwner)

df_W_CLM_LN_SAV_DRVR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_W_CLM_LN_SAV_DRVR)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: split_it (CTransformerStage)
# --------------------------------------------------------------------------------
df_split_it = (
    df_W_CLM_LN_SAV_DRVR
    .select(
        col("CLM_ID").alias("CLM_ID"),
        col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        when(col("RESP_SRC_CD")=='N', lit(1)).otherwise(lit(0)).alias("SRC_CD_N"),
        when(col("RESP_SRC_CD")=='P', lit(1)).otherwise(lit(0)).alias("SRC_CD_P"),
        when(col("RESP_SRC_CD")=='M', lit(1)).otherwise(lit(0)).alias("SRC_CD_M")
    )
)

# --------------------------------------------------------------------------------
# Stage: Aggregator_2 (AGGREGATOR)
# --------------------------------------------------------------------------------
df_Aggregator_2 = (
    df_split_it
    .groupBy("CLM_ID","CLM_LN_SEQ_NO")
    .agg(
        sum_("SRC_CD_N").alias("SRC_CD_N"),
        sum_("SRC_CD_P").alias("SRC_CD_P"),
        sum_("SRC_CD_M").alias("SRC_CD_M")
    )
)

# --------------------------------------------------------------------------------
# Stage: RespCd (CTransformerStage)
# --------------------------------------------------------------------------------
df_RespCd = df_Aggregator_2.withColumn(
    "SourceCode",
    F.when(col("SRC_CD_N") >= col("SRC_CD_P"),
           F.when(col("SRC_CD_N") >= col("SRC_CD_M"), lit("N"))
            .otherwise(
               F.when(col("SRC_CD_P") >= col("SRC_CD_M"), lit("P")).otherwise(lit("M"))
            )
    ).otherwise(
       F.when(col("SRC_CD_P") >= col("SRC_CD_M"), lit("P")).otherwise(lit("M"))
    )
).withColumn(
    "Update",
    F.when((col("SourceCode")=="N") & (col("SRC_CD_N")==1), lit("Y"))
     .otherwise(
        F.when((col("SourceCode")=="P") & (col("SRC_CD_P")==1), lit("Y"))
         .otherwise(
            F.when((col("SourceCode")=="M") & (col("SRC_CD_M")==1), lit("Y"))
             .otherwise(lit("N"))
         )
     )
)

df_Source = (
    df_RespCd
    .filter(col("Update")=="Y")
    .select(
        col("CLM_ID"),
        col("CLM_LN_SEQ_NO"),
        col("SourceCode").alias("RESP_SRC_CD")
    )
)

df_Driver2 = (
    df_RespCd
    .filter(col("Update")=="N")
    .select(
        col("CLM_ID"),
        col("CLM_LN_SEQ_NO"),
        col("SourceCode").alias("RESP_SRC_CD")
    )
)

# --------------------------------------------------------------------------------
# Stage: W_CLM_LN_SAV_DRVR2 (CSeqFileStage Write)
# --------------------------------------------------------------------------------
# Columns: CLM_ID (varchar(20)), CLM_LN_SEQ_NO (int), RESP_SRC_CD (char(1))
df_Driver2_final = df_Driver2.select(
    rpad("CLM_ID",20," ").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    rpad("RESP_SRC_CD",1," ").alias("RESP_SRC_CD")
)

write_files(
    df_Driver2_final,
    f"{adls_path}/load/W_CLM_LN_SAV_DRVR2.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# --------------------------------------------------------------------------------
# Stage: AddSource (CTransformerStage)
# --------------------------------------------------------------------------------
df_AddSource = (
    df_Source
    .select(
        rpad("CLM_ID",20," ").alias("CLM_ID"),
        col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        rpad("RESP_SRC_CD",1," ").alias("RESP_SRC_CD")
    )
    .alias("Source")
    .join(
        df_hf_clmlnsavupdt_drvr
          .select(
              rpad("CLM_ID",20," ").alias("CLM_ID"),
              col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
              rpad("RESP_SRC_CD",1," ").alias("RESP_SRC_CD"),
              rpad("CLM_LN_DSALW_TYP_SRC_CD",20," ").alias("CLM_LN_DSALW_TYP_SRC_CD")
          )
          .alias("drvr_lkup"),
        (
            (F.col("Source.CLM_ID") == F.col("drvr_lkup.CLM_ID")) &
            (F.col("Source.CLM_LN_SEQ_NO") == F.col("drvr_lkup.CLM_LN_SEQ_NO")) &
            (F.col("Source.RESP_SRC_CD") == F.col("drvr_lkup.RESP_SRC_CD"))
        ),
        "left"
    )
    .select(
        col("Source.CLM_ID").alias("CLM_ID"),
        col("Source.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        col("Source.RESP_SRC_CD").alias("RESP_SRC_CD"),
        col("drvr_lkup.CLM_LN_DSALW_TYP_SRC_CD").alias("CLM_LN_DSALW_TYP_SRC_CD")
    )
)

# --------------------------------------------------------------------------------
# Stage: UpdtW_CLM_LN_SAV_DRVR (DB2Connector - Update)
# --------------------------------------------------------------------------------
execute_dml(
    "DROP TABLE IF EXISTS STAGING.IdsClmLnSavDrvrUpdt_UpdtW_CLM_LN_SAV_DRVR_temp",
    jdbc_url,
    jdbc_props
)

df_AddSource_final = df_AddSource.select(
    rpad("CLM_ID",20," ").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    rpad("RESP_SRC_CD",1," ").alias("RESP_SRC_CD"),
    rpad("CLM_LN_DSALW_TYP_SRC_CD",20," ").alias("CLM_LN_DSALW_TYP_SRC_CD")
)

df_AddSource_final.write.jdbc(
    url=jdbc_url,
    table="STAGING.IdsClmLnSavDrvrUpdt_UpdtW_CLM_LN_SAV_DRVR_temp",
    mode="overwrite",
    properties=jdbc_props
)

merge_sql_UpdtW_CLM_LN_SAV_DRVR = f"""
MERGE INTO {IDSOwner}.W_CLM_LN_SAV_DRVR AS T
USING STAGING.IdsClmLnSavDrvrUpdt_UpdtW_CLM_LN_SAV_DRVR_temp AS S
ON
    T.CLM_ID = S.CLM_ID
    AND T.CLM_LN_SEQ_NO = S.CLM_LN_SEQ_NO
WHEN MATCHED THEN
    UPDATE SET
        T.RESP_SRC_CD = S.RESP_SRC_CD,
        T.CLM_LN_DSALW_TYP_SRC_CD = S.CLM_LN_DSALW_TYP_SRC_CD
WHEN NOT MATCHED THEN
    INSERT (CLM_ID, CLM_LN_SEQ_NO, RESP_SRC_CD, CLM_LN_DSALW_TYP_SRC_CD)
    VALUES (S.CLM_ID, S.CLM_LN_SEQ_NO, S.RESP_SRC_CD, S.CLM_LN_DSALW_TYP_SRC_CD)
;
"""

execute_dml(merge_sql_UpdtW_CLM_LN_SAV_DRVR, jdbc_url, jdbc_props)