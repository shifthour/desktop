# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2007, 2008, 2009 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:  
# MAGIC SEQ Job- EdwPdePayblRptCntrFCntl
# MAGIC PROCESSING: This Job Extracts the data from P2P Source File OPTUMRX.PBM.RPT_DDPS_P2P_PAYABLE* and loads the data into EDW Table PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_F
# MAGIC 
# MAGIC                     
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                                              Date                 Project/Altiris #                Change Description                                                       Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------               -------------------    -----------------------------------    ---------------------------------------------------------                 ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Rekha Radhakrishna                     2022-02-03           408843                               Initial Programming                                                       EnterpriseDev3                 Jaideep Mankala         02/24/2022


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType
)
from pyspark.sql.functions import (
    col,
    lit,
    when,
    rpad,
    coalesce
)
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


edw_owner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')
CurrRunCycle = get_widget_value('CurrRunCycle','')

jdbc_url_EDW, jdbc_props_EDW = get_db_config(edw_secret_name)

extract_query_Db2_K_PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_F = (
    "SELECT FILE_ID, CNTR_SEQ_ID, SUBMT_CNTR_SEQ_ID, DTL_SEQ_ID, SRC_SYS_CD, "
    "CRT_RUN_CYC_EXCTN_DT_SK, PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_SK "
    f"FROM {edw_owner}.K_PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_F"
)

df_Db2_K_PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_F = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_EDW)
    .options(**jdbc_props_EDW)
    .option("query", extract_query_Db2_K_PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_F)
    .load()
)

schema_Seq_PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_F = StructType([
    StructField("PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_SK", IntegerType(), True),
    StructField("FILE_ID", StringType(), True),
    StructField("CNTR_SEQ_ID", StringType(), True),
    StructField("SUBMT_CNTR_SEQ_ID", StringType(), True),
    StructField("DTL_SEQ_ID", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_SK", IntegerType(), True),
    StructField("CMS_CNTR_ID", StringType(), True),
    StructField("SUBMT_CMS_CNTR_ID", StringType(), True),
    StructField("CUR_MCARE_BNFCRY_ID", StringType(), True),
    StructField("LAST_SUBMT_MCARE_BNFCRY_ID", StringType(), True),
    StructField("DRUG_COV_STTUS_CD_TX", StringType(), True),
    StructField("CUR_MO_GROS_DRUG_CST_BELOW_AMT", DecimalType(), True),
    StructField("CUR_MO_GROS_DRUG_CST_ABOVE_AMT", DecimalType(), True),
    StructField("CUR_MO_TOT_GROS_DRUG_CST_AMT", DecimalType(), True),
    StructField("CUR_MO_LOW_INCM_CST_SHARING_AMT", DecimalType(), True),
    StructField("CUR_MO_COV_PLN_PD_AMT", DecimalType(), True),
    StructField("CUR_SUBMT_DUE_AMT", DecimalType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True)
])

df_Seq_PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_F = (
    spark.read.format("csv")
    .option("header", True)
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_Seq_PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_F)
    .load(f"{adls_path}/verified/PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_F.txt")
)

df_Xfm_DET = df_Seq_PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_F.select(
    col("PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_SK"),
    col("FILE_ID"),
    col("CNTR_SEQ_ID"),
    col("SUBMT_CNTR_SEQ_ID"),
    col("DTL_SEQ_ID"),
    col("SRC_SYS_CD"),
    col("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_SK"),
    col("CMS_CNTR_ID"),
    col("SUBMT_CMS_CNTR_ID"),
    col("CUR_MCARE_BNFCRY_ID"),
    col("LAST_SUBMT_MCARE_BNFCRY_ID"),
    col("DRUG_COV_STTUS_CD_TX"),
    col("CUR_MO_GROS_DRUG_CST_BELOW_AMT"),
    col("CUR_MO_GROS_DRUG_CST_ABOVE_AMT"),
    col("CUR_MO_TOT_GROS_DRUG_CST_AMT"),
    col("CUR_MO_LOW_INCM_CST_SHARING_AMT"),
    col("CUR_MO_COV_PLN_PD_AMT"),
    col("CUR_SUBMT_DUE_AMT"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_Copy_forRemoveDup = df_Xfm_DET.select(
    col("FILE_ID"),
    col("CNTR_SEQ_ID"),
    col("SUBMT_CNTR_SEQ_ID"),
    col("DTL_SEQ_ID"),
    col("SRC_SYS_CD")
)

df_Copy_forAllColJoin = df_Xfm_DET.select(
    col("FILE_ID"),
    col("CNTR_SEQ_ID"),
    col("SUBMT_CNTR_SEQ_ID"),
    col("DTL_SEQ_ID"),
    col("SRC_SYS_CD"),
    col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_SK"),
    col("CMS_CNTR_ID"),
    col("SUBMT_CMS_CNTR_ID"),
    col("CUR_MCARE_BNFCRY_ID"),
    col("LAST_SUBMT_MCARE_BNFCRY_ID"),
    col("DRUG_COV_STTUS_CD_TX"),
    col("CUR_MO_GROS_DRUG_CST_BELOW_AMT"),
    col("CUR_MO_GROS_DRUG_CST_ABOVE_AMT"),
    col("CUR_MO_TOT_GROS_DRUG_CST_AMT"),
    col("CUR_MO_LOW_INCM_CST_SHARING_AMT"),
    col("CUR_MO_COV_PLN_PD_AMT"),
    col("CUR_SUBMT_DUE_AMT"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_Remove_Dup = dedup_sort(
    df_Copy_forRemoveDup,
    ["FILE_ID","CNTR_SEQ_ID","SUBMT_CNTR_SEQ_ID","DTL_SEQ_ID","SRC_SYS_CD"],
    [("FILE_ID","A"),("CNTR_SEQ_ID","A"),("SUBMT_CNTR_SEQ_ID","A"),("DTL_SEQ_ID","A"),("SRC_SYS_CD","A")]
)

df_Jn1_NKey = df_Remove_Dup.alias("Lnk_RmDup").join(
    df_Db2_K_PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_F.alias("Lnk_KTableIn"),
    on=[
        "FILE_ID",
        "CNTR_SEQ_ID",
        "SUBMT_CNTR_SEQ_ID",
        "DTL_SEQ_ID",
        "SRC_SYS_CD"
    ],
    how="left"
).select(
    col("Lnk_RmDup.FILE_ID").alias("FILE_ID"),
    col("Lnk_RmDup.CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
    col("Lnk_RmDup.SUBMT_CNTR_SEQ_ID").alias("SUBMT_CNTR_SEQ_ID"),
    col("Lnk_RmDup.DTL_SEQ_ID").alias("DTL_SEQ_ID"),
    col("Lnk_RmDup.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("Lnk_KTableIn.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("Lnk_KTableIn.PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_SK").alias("PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_SK")
)

df_Transformer_StageVar = df_Jn1_NKey.withColumn(
    "tempPDE",
    coalesce(col("PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_SK"), lit(0))
).withColumn(
    "isNew",
    when(col("tempPDE") == 0, lit(True)).otherwise(lit(False))
).withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK",
    when(col("isNew"), lit(CurrRunCycleDate)).otherwise(col("CRT_RUN_CYC_EXCTN_DT_SK"))
).withColumn(
    "tempPDE",
    when(col("isNew"), lit(None)).otherwise(col("tempPDE"))
)

df_Transformer_StageVar = df_Transformer_StageVar.drop("PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_SK")
df_Transformer_StageVar = df_Transformer_StageVar.withColumnRenamed("tempPDE","PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_SK")

df_enriched = SurrogateKeyGen(
    df_Transformer_StageVar,
    <DB sequence name>,
    "PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_SK",
    <schema>,
    <secret_name>
)

df_Lnk_KTableLoad = df_enriched.filter(col("isNew") == True).select(
    col("FILE_ID"),
    col("CNTR_SEQ_ID"),
    col("SUBMT_CNTR_SEQ_ID"),
    col("DTL_SEQ_ID"),
    col("SRC_SYS_CD"),
    col("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_SK")
)

temp_table_name = "STAGING.PdePayblRptCntrSubmtCntrDtlFExtr_Db2_K_PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_F_Load_temp"
execute_dml(
    f"DROP TABLE IF EXISTS {temp_table_name}",
    jdbc_url_EDW,
    jdbc_props_EDW
)

df_Lnk_KTableLoad.write \
    .format("jdbc") \
    .option("url", jdbc_url_EDW) \
    .options(**jdbc_props_EDW) \
    .option("dbtable", temp_table_name) \
    .mode("overwrite") \
    .save()

merge_sql = (
    f"MERGE {edw_owner}.K_PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_F AS T "
    f"USING {temp_table_name} AS S "
    f"ON "
    f"(T.FILE_ID = S.FILE_ID AND "
    f" T.CNTR_SEQ_ID = S.CNTR_SEQ_ID AND "
    f" T.SUBMT_CNTR_SEQ_ID = S.SUBMT_CNTR_SEQ_ID AND "
    f" T.DTL_SEQ_ID = S.DTL_SEQ_ID AND "
    f" T.SRC_SYS_CD = S.SRC_SYS_CD) "
    f"WHEN MATCHED THEN UPDATE SET "
    f"T.CRT_RUN_CYC_EXCTN_DT_SK = S.CRT_RUN_CYC_EXCTN_DT_SK, "
    f"T.PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_SK = S.PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_SK "
    f"WHEN NOT MATCHED THEN INSERT "
    f"(FILE_ID, CNTR_SEQ_ID, SUBMT_CNTR_SEQ_ID, DTL_SEQ_ID, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_DT_SK, PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_SK) "
    f"VALUES "
    f"(S.FILE_ID, S.CNTR_SEQ_ID, S.SUBMT_CNTR_SEQ_ID, S.DTL_SEQ_ID, S.SRC_SYS_CD, S.CRT_RUN_CYC_EXCTN_DT_SK, S.PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_SK);"
)
execute_dml(merge_sql, jdbc_url_EDW, jdbc_props_EDW)

df_Lnk_Jn = df_enriched.select(
    col("FILE_ID"),
    col("CNTR_SEQ_ID"),
    col("SUBMT_CNTR_SEQ_ID"),
    col("DTL_SEQ_ID"),
    col("SRC_SYS_CD"),
    col("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_SK"),
    col("isNew")
)

df_Jn2Nkey = df_Copy_forAllColJoin.alias("AllCol").join(
    df_Lnk_Jn.alias("Jn"),
    on=[
        "FILE_ID",
        "CNTR_SEQ_ID",
        "SUBMT_CNTR_SEQ_ID",
        "DTL_SEQ_ID",
        "SRC_SYS_CD"
    ],
    how="inner"
).select(
    col("Jn.PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_SK").alias("PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_SK"),
    col("AllCol.FILE_ID").alias("FILE_ID"),
    col("AllCol.CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
    col("AllCol.SUBMT_CNTR_SEQ_ID").alias("SUBMT_CNTR_SEQ_ID"),
    col("AllCol.DTL_SEQ_ID").alias("DTL_SEQ_ID"),
    col("AllCol.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("Jn.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("AllCol.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("AllCol.PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_SK").alias("PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_SK"),
    col("AllCol.CMS_CNTR_ID").alias("CMS_CNTR_ID"),
    col("AllCol.SUBMT_CMS_CNTR_ID").alias("SUBMT_CMS_CNTR_ID"),
    col("AllCol.CUR_MCARE_BNFCRY_ID").alias("CUR_MCARE_BNFCRY_ID"),
    col("AllCol.LAST_SUBMT_MCARE_BNFCRY_ID").alias("LAST_SUBMT_MCARE_BNFCRY_ID"),
    col("AllCol.DRUG_COV_STTUS_CD_TX").alias("DRUG_COV_STTUS_CD_TX"),
    col("AllCol.CUR_MO_GROS_DRUG_CST_BELOW_AMT").alias("CUR_MO_GROS_DRUG_CST_BELOW_AMT"),
    col("AllCol.CUR_MO_GROS_DRUG_CST_ABOVE_AMT").alias("CUR_MO_GROS_DRUG_CST_ABOVE_AMT"),
    col("AllCol.CUR_MO_TOT_GROS_DRUG_CST_AMT").alias("CUR_MO_TOT_GROS_DRUG_CST_AMT"),
    col("AllCol.CUR_MO_LOW_INCM_CST_SHARING_AMT").alias("CUR_MO_LOW_INCM_CST_SHARING_AMT"),
    col("AllCol.CUR_MO_COV_PLN_PD_AMT").alias("CUR_MO_COV_PLN_PD_AMT"),
    col("AllCol.CUR_SUBMT_DUE_AMT").alias("CUR_SUBMT_DUE_AMT"),
    col("AllCol.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("AllCol.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_Copy5 = df_Jn2Nkey.select(
    col("PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_SK"),
    col("FILE_ID"),
    col("CNTR_SEQ_ID"),
    col("SUBMT_CNTR_SEQ_ID"),
    col("DTL_SEQ_ID"),
    col("SRC_SYS_CD"),
    col("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_SK"),
    col("CMS_CNTR_ID"),
    col("SUBMT_CMS_CNTR_ID"),
    col("CUR_MCARE_BNFCRY_ID"),
    col("LAST_SUBMT_MCARE_BNFCRY_ID"),
    col("DRUG_COV_STTUS_CD_TX"),
    col("CUR_MO_GROS_DRUG_CST_BELOW_AMT"),
    col("CUR_MO_GROS_DRUG_CST_ABOVE_AMT"),
    col("CUR_MO_TOT_GROS_DRUG_CST_AMT"),
    col("CUR_MO_LOW_INCM_CST_SHARING_AMT"),
    col("CUR_MO_COV_PLN_PD_AMT"),
    col("CUR_SUBMT_DUE_AMT"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_Seq_PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_F_Extr = df_Copy5.select(
    rpad(col("PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_SK").cast(StringType()), <...>, " ").cast(IntegerType()).alias("PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_SK"),
    rpad(col("FILE_ID"), <...>, " ").alias("FILE_ID"),
    rpad(col("CNTR_SEQ_ID"), <...>, " ").alias("CNTR_SEQ_ID"),
    rpad(col("SUBMT_CNTR_SEQ_ID"), <...>, " ").alias("SUBMT_CNTR_SEQ_ID"),
    rpad(col("DTL_SEQ_ID"), <...>, " ").alias("DTL_SEQ_ID"),
    rpad(col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    rpad(col("PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_SK").cast(StringType()), <...>, " ").cast(IntegerType()).alias("PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_SK"),
    rpad(col("CMS_CNTR_ID"), <...>, " ").alias("CMS_CNTR_ID"),
    rpad(col("SUBMT_CMS_CNTR_ID"), <...>, " ").alias("SUBMT_CMS_CNTR_ID"),
    rpad(col("CUR_MCARE_BNFCRY_ID"), <...>, " ").alias("CUR_MCARE_BNFCRY_ID"),
    rpad(col("LAST_SUBMT_MCARE_BNFCRY_ID"), <...>, " ").alias("LAST_SUBMT_MCARE_BNFCRY_ID"),
    rpad(col("DRUG_COV_STTUS_CD_TX"), <...>, " ").alias("DRUG_COV_STTUS_CD_TX"),
    col("CUR_MO_GROS_DRUG_CST_BELOW_AMT").alias("CUR_MO_GROS_DRUG_CST_BELOW_AMT"),
    col("CUR_MO_GROS_DRUG_CST_ABOVE_AMT").alias("CUR_MO_GROS_DRUG_CST_ABOVE_AMT"),
    col("CUR_MO_TOT_GROS_DRUG_CST_AMT").alias("CUR_MO_TOT_GROS_DRUG_CST_AMT"),
    col("CUR_MO_LOW_INCM_CST_SHARING_AMT").alias("CUR_MO_LOW_INCM_CST_SHARING_AMT"),
    col("CUR_MO_COV_PLN_PD_AMT").alias("CUR_MO_COV_PLN_PD_AMT"),
    col("CUR_SUBMT_DUE_AMT").alias("CUR_SUBMT_DUE_AMT"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

write_files(
    df_Seq_PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_F_Extr,
    f"{adls_path}/verified/PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_F_Load.txt",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=True,
    quote="\"",
    nullValue=None
)