# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2007, 2008, 2009 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:  
# MAGIC SEQ Job- EdwPdeRecRptSubmtCntrCntrDtlF
# MAGIC PROCESSING:  This Job Extracts the data from P2P Source File OPTUMRX.PBM.RPT_DDPS_P2P_RECEIVABLE* and loads the data into EDW Table PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_F
# MAGIC 
# MAGIC                     
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                                              Date                 Project/Altiris #                Change Description                                                       Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------               -------------------    -----------------------------------    ---------------------------------------------------------                 ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Rekha Radhakrishna                     2022-02-03           408843                               Initial Programming                                                       EnterpriseDev3                  Jaideep Mankala         02/24/2022


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
from pyspark.sql.functions import col, lit, when, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------

EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')
EDWDB = get_widget_value('EDWDB','')
EDWAcct = get_widget_value('EDWAcct','')
EDWPW = get_widget_value('EDWPW','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

extract_query = f"SELECT FILE_ID,SUBMT_CNTR_SEQ_ID,CNTR_SEQ_ID,DTL_SEQ_ID,SRC_SYS_CD,CRT_RUN_CYC_EXCTN_DT_SK,PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_SK FROM {EDWOwner}.K_PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_F"
df_Db2_K_PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_F = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)
df_Lnk_KTableIn = df_Db2_K_PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_F

schema_Seq_PDE = StructType([
    StructField("PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_SK", IntegerType(), False),
    StructField("FILE_ID", StringType(), False),
    StructField("SUBMT_CNTR_SEQ_ID", StringType(), False),
    StructField("CNTR_SEQ_ID", StringType(), False),
    StructField("DTL_SEQ_ID", StringType(), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), False),
    StructField("PDE_REC_RPT_SUBMT_CNTR_CNTR_SK", IntegerType(), False),
    StructField("SUBMT_CMS_CNTR_ID", StringType(), False),
    StructField("CMS_CNTR_ID", StringType(), False),
    StructField("CUR_MCARE_BNFCRY_ID", StringType(), False),
    StructField("LAST_SUBMT_MCARE_BNFCRY_ID", StringType(), False),
    StructField("LAST_SUBMT_CARDHLDR_ID", StringType(), False),
    StructField("DRUG_COV_STTUS_CD_TX", StringType(), False),
    StructField("CUR_MO_GROS_DRUG_CST_ABOVE_AMT", DecimalType(38, 10), False),
    StructField("CUR_MO_GROS_DRUG_CST_BELOW_AMT", DecimalType(38, 10), False),
    StructField("CUR_MO_TOT_GROS_DRUG_CST_AMT", DecimalType(38, 10), False),
    StructField("CUR_MO_LOW_INCM_CST_SHARING_AMT", DecimalType(38, 10), False),
    StructField("CUR_MO_COV_PLN_PD_AMT", DecimalType(38, 10), False),
    StructField("CUR_SUBMT_DUE_AMT", DecimalType(38, 10), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
])
df_Seq_PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_F = (
    spark.read.format("csv")
    .option("header", True)
    .option("sep", ",")
    .option("quote", "\"")
    .option("escape", "\"")
    .schema(schema_Seq_PDE)
    .load(f"{adls_path}/verified/PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_F.txt")
)
df_Lnk_DtlSeq = df_Seq_PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_F

df_Lnk_Copy = df_Lnk_DtlSeq.select(
    col("PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_SK").alias("PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_SK"),
    col("FILE_ID").alias("FILE_ID"),
    col("SUBMT_CNTR_SEQ_ID").alias("SUBMT_CNTR_SEQ_ID"),
    col("CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
    col("DTL_SEQ_ID").alias("DTL_SEQ_ID"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("PDE_REC_RPT_SUBMT_CNTR_CNTR_SK").alias("PDE_REC_RPT_SUBMT_CNTR_CNTR_SK"),
    col("SUBMT_CMS_CNTR_ID").alias("SUBMT_CMS_CNTR_ID"),
    col("CMS_CNTR_ID").alias("CMS_CNTR_ID"),
    col("CUR_MCARE_BNFCRY_ID").alias("CUR_MCARE_BNFCRY_ID"),
    col("LAST_SUBMT_MCARE_BNFCRY_ID").alias("LAST_SUBMT_MCARE_BNFCRY_ID"),
    col("LAST_SUBMT_CARDHLDR_ID").alias("LAST_SUBMT_CARDHLDR_ID"),
    col("DRUG_COV_STTUS_CD_TX").alias("DRUG_COV_STTUS_CD_TX"),
    col("CUR_MO_GROS_DRUG_CST_ABOVE_AMT").alias("CUR_MO_GROS_DRUG_CST_ABOVE_AMT"),
    col("CUR_MO_GROS_DRUG_CST_BELOW_AMT").alias("CUR_MO_GROS_DRUG_CST_BELOW_AMT"),
    col("CUR_MO_TOT_GROS_DRUG_CST_AMT").alias("CUR_MO_TOT_GROS_DRUG_CST_AMT"),
    col("CUR_MO_LOW_INCM_CST_SHARING_AMT").alias("CUR_MO_LOW_INCM_CST_SHARING_AMT"),
    col("CUR_MO_COV_PLN_PD_AMT").alias("CUR_MO_COV_PLN_PD_AMT"),
    col("CUR_SUBMT_DUE_AMT").alias("CUR_SUBMT_DUE_AMT"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
)

df_Lnk_Remove_Dup = df_Lnk_Copy.select(
    col("FILE_ID").alias("FILE_ID"),
    col("SUBMT_CNTR_SEQ_ID").alias("SUBMT_CNTR_SEQ_ID"),
    col("CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
    col("DTL_SEQ_ID").alias("DTL_SEQ_ID"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
)

df_Lnk_AllCol_Join = df_Lnk_Copy.select(
    col("FILE_ID").alias("FILE_ID"),
    col("SUBMT_CNTR_SEQ_ID").alias("SUBMT_CNTR_SEQ_ID"),
    col("CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
    col("DTL_SEQ_ID").alias("DTL_SEQ_ID"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("PDE_REC_RPT_SUBMT_CNTR_CNTR_SK").alias("PDE_REC_RPT_SUBMT_CNTR_CNTR_SK"),
    col("SUBMT_CMS_CNTR_ID").alias("SUBMT_CMS_CNTR_ID"),
    col("CMS_CNTR_ID").alias("CMS_CNTR_ID"),
    col("CUR_MCARE_BNFCRY_ID").alias("CUR_MCARE_BNFCRY_ID"),
    col("LAST_SUBMT_MCARE_BNFCRY_ID").alias("LAST_SUBMT_MCARE_BNFCRY_ID"),
    col("LAST_SUBMT_CARDHLDR_ID").alias("LAST_SUBMT_CARDHLDR_ID"),
    col("DRUG_COV_STTUS_CD_TX").alias("DRUG_COV_STTUS_CD_TX"),
    col("CUR_MO_GROS_DRUG_CST_ABOVE_AMT").alias("CUR_MO_GROS_DRUG_CST_ABOVE_AMT"),
    col("CUR_MO_GROS_DRUG_CST_BELOW_AMT").alias("CUR_MO_GROS_DRUG_CST_BELOW_AMT"),
    col("CUR_MO_TOT_GROS_DRUG_CST_AMT").alias("CUR_MO_TOT_GROS_DRUG_CST_AMT"),
    col("CUR_MO_LOW_INCM_CST_SHARING_AMT").alias("CUR_MO_LOW_INCM_CST_SHARING_AMT"),
    col("CUR_MO_COV_PLN_PD_AMT").alias("CUR_MO_COV_PLN_PD_AMT"),
    col("CUR_SUBMT_DUE_AMT").alias("CUR_SUBMT_DUE_AMT"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
)

df_Remove_Duplicates = dedup_sort(
    df_Lnk_Remove_Dup,
    ["FILE_ID", "SUBMT_CNTR_SEQ_ID", "CNTR_SEQ_ID", "DTL_SEQ_ID", "SRC_SYS_CD"],
    [
        ("FILE_ID","A"),
        ("SUBMT_CNTR_SEQ_ID","A"),
        ("CNTR_SEQ_ID","A"),
        ("DTL_SEQ_ID","A"),
        ("SRC_SYS_CD","A")
    ]
)
df_Lnk_RmDup = df_Remove_Duplicates.select(
    col("FILE_ID"),
    col("SUBMT_CNTR_SEQ_ID"),
    col("CNTR_SEQ_ID"),
    col("DTL_SEQ_ID"),
    col("SRC_SYS_CD")
)

df_Jn1_NKey = df_Lnk_RmDup.alias("left").join(
    df_Lnk_KTableIn.alias("right"),
    on=[
        col("left.FILE_ID") == col("right.FILE_ID"),
        col("left.SUBMT_CNTR_SEQ_ID") == col("right.SUBMT_CNTR_SEQ_ID"),
        col("left.CNTR_SEQ_ID") == col("right.CNTR_SEQ_ID"),
        col("left.DTL_SEQ_ID") == col("right.DTL_SEQ_ID"),
        col("left.SRC_SYS_CD") == col("right.SRC_SYS_CD")
    ],
    how="left"
)
df_Lnk_Xfm1 = df_Jn1_NKey.select(
    col("left.FILE_ID").alias("FILE_ID"),
    col("left.SUBMT_CNTR_SEQ_ID").alias("SUBMT_CNTR_SEQ_ID"),
    col("left.CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
    col("left.DTL_SEQ_ID").alias("DTL_SEQ_ID"),
    col("left.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("right.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("right.PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_SK").alias("PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_SK"),
)

df_enriched = df_Lnk_Xfm1.withColumn("orig_key", col("PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_SK"))
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_SK",<schema>,<secret_name>)

df_Lnk_KTableLoad_pre = df_enriched.filter(
    (col("orig_key").isNull()) | (col("orig_key") == lit(0))
)
df_Lnk_KTableLoad = df_Lnk_KTableLoad_pre.select(
    col("FILE_ID").alias("FILE_ID"),
    col("SUBMT_CNTR_SEQ_ID").alias("SUBMT_CNTR_SEQ_ID"),
    col("CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
    col("DTL_SEQ_ID").alias("DTL_SEQ_ID"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    lit(CurrRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_SK").alias("PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_SK"),
)

temp_table_load = "STAGING.PdeRecRptSubmtCntrCntrDtlFExtr_Db2_K_PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_F_Load_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_load}", jdbc_url, jdbc_props)
df_Lnk_KTableLoad.write.jdbc(
    url=jdbc_url,
    table=temp_table_load,
    mode="error",
    properties=jdbc_props
)
merge_sql = f"""
MERGE INTO {EDWOwner}.K_PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_F AS T
USING {temp_table_load} AS S
ON
(
    T.FILE_ID = S.FILE_ID
    AND T.SUBMT_CNTR_SEQ_ID = S.SUBMT_CNTR_SEQ_ID
    AND T.CNTR_SEQ_ID = S.CNTR_SEQ_ID
    AND T.DTL_SEQ_ID = S.DTL_SEQ_ID
    AND T.SRC_SYS_CD = S.SRC_SYS_CD
)
WHEN MATCHED THEN
    UPDATE SET
    T.CRT_RUN_CYC_EXCTN_DT_SK = S.CRT_RUN_CYC_EXCTN_DT_SK,
    T.PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_SK = S.PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_SK
WHEN NOT MATCHED THEN
    INSERT
    (
        FILE_ID,
        SUBMT_CNTR_SEQ_ID,
        CNTR_SEQ_ID,
        DTL_SEQ_ID,
        SRC_SYS_CD,
        CRT_RUN_CYC_EXCTN_DT_SK,
        PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_SK
    )
    VALUES
    (
        S.FILE_ID,
        S.SUBMT_CNTR_SEQ_ID,
        S.CNTR_SEQ_ID,
        S.DTL_SEQ_ID,
        S.SRC_SYS_CD,
        S.CRT_RUN_CYC_EXCTN_DT_SK,
        S.PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_SK
    );
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

df_Lnk_Jn = df_enriched.select(
    col("FILE_ID"),
    col("SUBMT_CNTR_SEQ_ID"),
    col("CNTR_SEQ_ID"),
    col("DTL_SEQ_ID"),
    col("SRC_SYS_CD"),
    when((col("orig_key").isNull()) | (col("orig_key") == lit(0)), lit(CurrRunCycleDate))
    .otherwise(col("CRT_RUN_CYC_EXCTN_DT_SK"))
    .alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_SK").alias("PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_SK"),
)

df_Jn2Nkey = df_Lnk_AllCol_Join.alias("left").join(
    df_Lnk_Jn.alias("right"),
    on=[
        col("left.FILE_ID") == col("right.FILE_ID"),
        col("left.SUBMT_CNTR_SEQ_ID") == col("right.SUBMT_CNTR_SEQ_ID"),
        col("left.CNTR_SEQ_ID") == col("right.CNTR_SEQ_ID"),
        col("left.DTL_SEQ_ID") == col("right.DTL_SEQ_ID"),
        col("left.SRC_SYS_CD") == col("right.SRC_SYS_CD")
    ],
    how="inner"
)
df_Lnk_Copy_2 = df_Jn2Nkey.select(
    col("right.PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_SK").alias("PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_SK"),
    col("left.FILE_ID").alias("FILE_ID"),
    col("left.SUBMT_CNTR_SEQ_ID").alias("SUBMT_CNTR_SEQ_ID"),
    col("left.CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
    col("left.DTL_SEQ_ID").alias("DTL_SEQ_ID"),
    col("left.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("right.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("left.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("left.PDE_REC_RPT_SUBMT_CNTR_CNTR_SK").alias("PDE_REC_RPT_SUBMT_CNTR_CNTR_SK"),
    col("left.SUBMT_CMS_CNTR_ID").alias("SUBMT_CMS_CNTR_ID"),
    col("left.CMS_CNTR_ID").alias("CMS_CNTR_ID"),
    col("left.CUR_MCARE_BNFCRY_ID").alias("CUR_MCARE_BNFCRY_ID"),
    col("left.LAST_SUBMT_MCARE_BNFCRY_ID").alias("LAST_SUBMT_MCARE_BNFCRY_ID"),
    col("left.LAST_SUBMT_CARDHLDR_ID").alias("LAST_SUBMT_CARDHLDR_ID"),
    col("left.DRUG_COV_STTUS_CD_TX").alias("DRUG_COV_STTUS_CD_TX"),
    col("left.CUR_MO_GROS_DRUG_CST_ABOVE_AMT").alias("CUR_MO_GROS_DRUG_CST_ABOVE_AMT"),
    col("left.CUR_MO_GROS_DRUG_CST_BELOW_AMT").alias("CUR_MO_GROS_DRUG_CST_BELOW_AMT"),
    col("left.CUR_MO_TOT_GROS_DRUG_CST_AMT").alias("CUR_MO_TOT_GROS_DRUG_CST_AMT"),
    col("left.CUR_MO_LOW_INCM_CST_SHARING_AMT").alias("CUR_MO_LOW_INCM_CST_SHARING_AMT"),
    col("left.CUR_MO_COV_PLN_PD_AMT").alias("CUR_MO_COV_PLN_PD_AMT"),
    col("left.CUR_SUBMT_DUE_AMT").alias("CUR_SUBMT_DUE_AMT"),
    col("left.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("left.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
)

df_Lnk_SeqExtr = df_Lnk_Copy_2.select(
    col("PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_SK").alias("PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_SK"),
    col("FILE_ID").alias("FILE_ID"),
    col("SUBMT_CNTR_SEQ_ID").alias("SUBMT_CNTR_SEQ_ID"),
    col("CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
    col("DTL_SEQ_ID").alias("DTL_SEQ_ID"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("PDE_REC_RPT_SUBMT_CNTR_CNTR_SK").alias("PDE_REC_RPT_SUBMT_CNTR_CNTR_SK"),
    col("SUBMT_CMS_CNTR_ID").alias("SUBMT_CMS_CNTR_ID"),
    col("CMS_CNTR_ID").alias("CMS_CNTR_ID"),
    col("CUR_MCARE_BNFCRY_ID").alias("CUR_MCARE_BNFCRY_ID"),
    col("LAST_SUBMT_MCARE_BNFCRY_ID").alias("LAST_SUBMT_MCARE_BNFCRY_ID"),
    col("LAST_SUBMT_CARDHLDR_ID").alias("LAST_SUBMT_CARDHLDR_ID"),
    col("DRUG_COV_STTUS_CD_TX").alias("DRUG_COV_STTUS_CD_TX"),
    col("CUR_MO_GROS_DRUG_CST_ABOVE_AMT").alias("CUR_MO_GROS_DRUG_CST_ABOVE_AMT"),
    col("CUR_MO_GROS_DRUG_CST_BELOW_AMT").alias("CUR_MO_GROS_DRUG_CST_BELOW_AMT"),
    col("CUR_MO_TOT_GROS_DRUG_CST_AMT").alias("CUR_MO_TOT_GROS_DRUG_CST_AMT"),
    col("CUR_MO_LOW_INCM_CST_SHARING_AMT").alias("CUR_MO_LOW_INCM_CST_SHARING_AMT"),
    col("CUR_MO_COV_PLN_PD_AMT").alias("CUR_MO_COV_PLN_PD_AMT"),
    col("CUR_SUBMT_DUE_AMT").alias("CUR_SUBMT_DUE_AMT"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
)

df_final = (
    df_Lnk_SeqExtr
    .withColumn("FILE_ID", rpad("FILE_ID", 255, " "))
    .withColumn("SUBMT_CNTR_SEQ_ID", rpad("SUBMT_CNTR_SEQ_ID", 255, " "))
    .withColumn("CNTR_SEQ_ID", rpad("CNTR_SEQ_ID", 255, " "))
    .withColumn("DTL_SEQ_ID", rpad("DTL_SEQ_ID", 255, " "))
    .withColumn("SRC_SYS_CD", rpad("SRC_SYS_CD", 255, " "))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", rpad("CRT_RUN_CYC_EXCTN_DT_SK", 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", 10, " "))
    .withColumn("SUBMT_CMS_CNTR_ID", rpad("SUBMT_CMS_CNTR_ID", 255, " "))
    .withColumn("CMS_CNTR_ID", rpad("CMS_CNTR_ID", 255, " "))
    .withColumn("CUR_MCARE_BNFCRY_ID", rpad("CUR_MCARE_BNFCRY_ID", 255, " "))
    .withColumn("LAST_SUBMT_MCARE_BNFCRY_ID", rpad("LAST_SUBMT_MCARE_BNFCRY_ID", 255, " "))
    .withColumn("LAST_SUBMT_CARDHLDR_ID", rpad("LAST_SUBMT_CARDHLDR_ID", 255, " "))
    .withColumn("DRUG_COV_STTUS_CD_TX", rpad("DRUG_COV_STTUS_CD_TX", 255, " "))
)

write_files(
    df_final,
    f"{adls_path}/verified/PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_F_Load.txt",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)