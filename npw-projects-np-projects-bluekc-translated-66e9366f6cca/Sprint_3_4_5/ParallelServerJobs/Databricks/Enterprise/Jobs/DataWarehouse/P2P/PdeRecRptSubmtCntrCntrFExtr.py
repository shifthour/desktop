# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2007, 2008, 2009 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:  
# MAGIC SEQ Job- EdwPdeRecRptSubmtCntrCntrFCntl
# MAGIC PROCESSING:  This Job Extracts the data from P2P Source File OPTUMRX.PBM.RPT_DDPS_P2P_RECEIVABLE* and loads the data into EDW Table PDE_REC_RPT_SUBMT_CNTR_CNTR_F
# MAGIC 
# MAGIC                     
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                                           Date                 Project/Altiris #                     Change Description                                                           Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------               -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Rekha Radhakrishna                  2022-02-03                                                            Initial Programming                                                            EnterpriseDev3               Jaideep Mankala         02/24/2022  
# MAGIC Rekha Radhakrishna                  2022-03-03                                                          Corrected the mapping field for
# MAGIC                                                                                                                                     CUR_MO_GROSS_DRUG_CST_BELOW_AMT &         EnterpriseDev3             Jaideep Mankala          03/04/2022
# MAGIC                                                                                                                                      CUR_MO_GROSS_DRUG_CST_ABOVE_AMT


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, DecimalType
)
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# Retrieve parameter values
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')

# -----------------------------------------------------------------------------
# 1) Read Seq_PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_F_Key
# -----------------------------------------------------------------------------
schema_Seq_PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_F_Key = StructType([
    StructField("Key", IntegerType(), nullable=False),
    StructField("Key1", IntegerType(), nullable=False),
    StructField("PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_SK", IntegerType(), nullable=False),
    StructField("DTL_SEQ_ID", StringType(), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), nullable=False),
    StructField("CUR_MCARE_BNFCRY_ID", StringType(), nullable=False),
    StructField("LAST_SUBMT_MCARE_BNFCRY_ID", StringType(), nullable=False),
    StructField("LAST_SUBMT_CARDHLDR_ID", StringType(), nullable=False),
    StructField("DRUG_COV_STTUS_CD_TX", StringType(), nullable=False),
    StructField("CUR_MO_GROS_DRUG_CST_ABOVE_AMT", DecimalType(38,10), nullable=False),
    StructField("CUR_MO_GROS_DRUG_CST_BELOW_AMT", DecimalType(38,10), nullable=False),
    StructField("CUR_MO_TOT_GROS_DRUG_CST_AMT", DecimalType(38,10), nullable=False),
    StructField("CUR_MO_LOW_INCM_CST_SHARING_AMT", DecimalType(38,10), nullable=False),
    StructField("CUR_MO_COV_PLN_PD_AMT", DecimalType(38,10), nullable=False),
    StructField("CUR_SUBMT_DUE_AMT", DecimalType(38,10), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False)
])
df_Seq_PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_F_Key_0 = (
    spark.read.format("csv")
    .option("header", True)
    .option("sep", ",")
    .option("quote", '"')
    .option("escape", '"')
    .option("nullValue", None)
    .schema(schema_Seq_PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_F_Key)
    .load(f"{adls_path}/verified/PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_F_Key.txt")
)
df_Seq_PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_F_Key = df_Seq_PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_F_Key_0.select(
    "Key",
    "Key1",
    "PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_SK",
    "DTL_SEQ_ID",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "CUR_MCARE_BNFCRY_ID",
    "LAST_SUBMT_MCARE_BNFCRY_ID",
    "LAST_SUBMT_CARDHLDR_ID",
    "DRUG_COV_STTUS_CD_TX",
    "CUR_MO_GROS_DRUG_CST_ABOVE_AMT",
    "CUR_MO_GROS_DRUG_CST_BELOW_AMT",
    "CUR_MO_TOT_GROS_DRUG_CST_AMT",
    "CUR_MO_LOW_INCM_CST_SHARING_AMT",
    "CUR_MO_COV_PLN_PD_AMT",
    "CUR_SUBMT_DUE_AMT",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK"
)

# -----------------------------------------------------------------------------
# 2) Read DB2_K_PDE_REC_RPT_SUBMT_CNTR_CNTR_F (input from EDW)
# -----------------------------------------------------------------------------
jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)
extract_query_Db2_K_PDE_REC_RPT_SUBMT_CNTR_CNTR_F = (
    f"SELECT FILE_ID,SUBMT_CNTR_SEQ_ID,CNTR_SEQ_ID,SRC_SYS_CD,CRT_RUN_CYC_EXCTN_DT_SK,"
    f"PDE_REC_RPT_SUBMT_CNTR_CNTR_SK FROM {EDWOwner}.K_PDE_REC_RPT_SUBMT_CNTR_CNTR_F"
)
df_Db2_K_PDE_REC_RPT_SUBMT_CNTR_CNTR_F = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", extract_query_Db2_K_PDE_REC_RPT_SUBMT_CNTR_CNTR_F)
    .load()
)

# -----------------------------------------------------------------------------
# 3) Read Seq_PDE_REC_RPT_SUBMT_CNTR_CNTR_F
# -----------------------------------------------------------------------------
schema_Seq_PDE_REC_RPT_SUBMT_CNTR_CNTR_F = StructType([
    StructField("Key", IntegerType(), nullable=False),
    StructField("Key1", IntegerType(), nullable=False),
    StructField("PDE_REC_RPT_SUBMT_CNTR_CNTR_SK", IntegerType(), nullable=False),
    StructField("FILE_ID", StringType(), nullable=False),
    StructField("SUBMT_CNTR_SEQ_ID", StringType(), nullable=False),
    StructField("CNTR_SEQ_ID", StringType(), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), nullable=False),
    StructField("PDE_REC_RPT_SUBMT_CNTR_SK", IntegerType(), nullable=False),
    StructField("SUBMT_CMS_CNTR_ID", StringType(), nullable=False),
    StructField("CMS_CNTR_ID", StringType(), nullable=False),
    StructField("AS_OF_YR", StringType(), nullable=False),
    StructField("AS_OF_MO", StringType(), nullable=False),
    StructField("FILE_CRTN_DT", StringType(), nullable=False),
    StructField("FILE_CRTN_TM", StringType(), nullable=False),
    StructField("RPT_ID", StringType(), nullable=False),
    StructField("DRUG_COV_STTUS_CD_TX", StringType(), nullable=False),
    StructField("BNFCRY_CT", IntegerType(), nullable=False),
    StructField("CUR_MO_GROS_DRUG_CST_ABOVE_AMT", DecimalType(38,10), nullable=False),
    StructField("CUR_MO_GROS_DRUG_CST_BELOW_AMT", DecimalType(38,10), nullable=False),
    StructField("CUR_MO_TOT_GROS_DRUG_CST_AMT", DecimalType(38,10), nullable=False),
    StructField("CUR_MO_LOW_INCM_CST_SHARING_AMT", DecimalType(38,10), nullable=False),
    StructField("CUR_MO_COV_PLN_PD_AMT", DecimalType(38,10), nullable=False),
    StructField("TOT_DTL_RCRD_CT", IntegerType(), nullable=False),
    StructField("CUR_MO_SUBMT_DUE_AMT", DecimalType(38,10), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False)
])
df_Seq_PDE_REC_RPT_SUBMT_CNTR_CNTR_F_0 = (
    spark.read.format("csv")
    .option("header", True)
    .option("sep", ",")
    .option("quote", '"')
    .option("escape", '"')
    .option("nullValue", None)
    .schema(schema_Seq_PDE_REC_RPT_SUBMT_CNTR_CNTR_F)
    .load(f"{adls_path}/verified/PDE_REC_RPT_SUBMT_CNTR_CNTR_F.txt")
)
df_Seq_PDE_REC_RPT_SUBMT_CNTR_CNTR_F = df_Seq_PDE_REC_RPT_SUBMT_CNTR_CNTR_F_0.select(
    "Key",
    "Key1",
    "PDE_REC_RPT_SUBMT_CNTR_CNTR_SK",
    "FILE_ID",
    "SUBMT_CNTR_SEQ_ID",
    "CNTR_SEQ_ID",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "PDE_REC_RPT_SUBMT_CNTR_SK",
    "SUBMT_CMS_CNTR_ID",
    "CMS_CNTR_ID",
    "AS_OF_YR",
    "AS_OF_MO",
    "FILE_CRTN_DT",
    "FILE_CRTN_TM",
    "RPT_ID",
    "DRUG_COV_STTUS_CD_TX",
    "BNFCRY_CT",
    "CUR_MO_GROS_DRUG_CST_ABOVE_AMT",
    "CUR_MO_GROS_DRUG_CST_BELOW_AMT",
    "CUR_MO_TOT_GROS_DRUG_CST_AMT",
    "CUR_MO_LOW_INCM_CST_SHARING_AMT",
    "CUR_MO_COV_PLN_PD_AMT",
    "TOT_DTL_RCRD_CT",
    "CUR_MO_SUBMT_DUE_AMT",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK"
)

# -----------------------------------------------------------------------------
# 4) Xfm_SHD_STR (Transformer): swap certain columns
# -----------------------------------------------------------------------------
df_Xfm_SHD_STR_Lnk_Copy = df_Seq_PDE_REC_RPT_SUBMT_CNTR_CNTR_F.select(
    F.col("Key").alias("Key"),
    F.col("Key1").alias("Key1"),
    F.col("PDE_REC_RPT_SUBMT_CNTR_CNTR_SK").alias("PDE_REC_RPT_SUBMT_CNTR_CNTR_SK"),
    F.col("FILE_ID").alias("FILE_ID"),
    F.col("SUBMT_CNTR_SEQ_ID").alias("SUBMT_CNTR_SEQ_ID"),
    F.col("CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("PDE_REC_RPT_SUBMT_CNTR_SK").alias("PDE_REC_RPT_SUBMT_CNTR_SK"),
    F.col("SUBMT_CMS_CNTR_ID").alias("SUBMT_CMS_CNTR_ID"),
    F.col("CMS_CNTR_ID").alias("CMS_CNTR_ID"),
    F.col("AS_OF_YR").alias("AS_OF_YR"),
    F.col("AS_OF_MO").alias("AS_OF_MO"),
    F.col("FILE_CRTN_DT").alias("FILE_CRTN_DT"),
    F.col("FILE_CRTN_TM").alias("FILE_CRTN_TM"),
    F.col("RPT_ID").alias("RPT_ID"),
    F.col("DRUG_COV_STTUS_CD_TX").alias("DRUG_COV_STTUS_CD_TX"),
    F.col("BNFCRY_CT").alias("BNFCRY_CT"),
    F.col("CUR_MO_GROS_DRUG_CST_BELOW_AMT").alias("CUR_MO_GROS_DRUG_CST_ABOVE_AMT"),
    F.col("CUR_MO_GROS_DRUG_CST_ABOVE_AMT").alias("CUR_MO_GROS_DRUG_CST_BELOW_AMT"),
    F.col("CUR_MO_TOT_GROS_DRUG_CST_AMT").alias("CUR_MO_TOT_GROS_DRUG_CST_AMT"),
    F.col("CUR_MO_LOW_INCM_CST_SHARING_AMT").alias("CUR_MO_LOW_INCM_CST_SHARING_AMT"),
    F.col("CUR_MO_COV_PLN_PD_AMT").alias("CUR_MO_COV_PLN_PD_AMT"),
    F.col("TOT_DTL_RCRD_CT").alias("TOT_DTL_RCRD_CT"),
    F.col("CUR_MO_SUBMT_DUE_AMT").alias("CUR_MO_SUBMT_DUE_AMT"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# -----------------------------------------------------------------------------
# 5) Copy stage outputs: Lnk_Remove_Dup and Lnk_AllCol_Join
# -----------------------------------------------------------------------------
df_Copy_Lnk_Remove_Dup = df_Xfm_SHD_STR_Lnk_Copy.select(
    "FILE_ID",
    "SUBMT_CNTR_SEQ_ID",
    "CNTR_SEQ_ID",
    "SRC_SYS_CD"
)

df_Copy_Lnk_AllCol_Join = df_Xfm_SHD_STR_Lnk_Copy.select(
    "Key",
    "Key1",
    "FILE_ID",
    "SUBMT_CNTR_SEQ_ID",
    "CNTR_SEQ_ID",
    "SRC_SYS_CD",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "PDE_REC_RPT_SUBMT_CNTR_SK",
    "SUBMT_CMS_CNTR_ID",
    "CMS_CNTR_ID",
    "AS_OF_YR",
    "AS_OF_MO",
    "FILE_CRTN_DT",
    "FILE_CRTN_TM",
    "RPT_ID",
    "DRUG_COV_STTUS_CD_TX",
    "BNFCRY_CT",
    "CUR_MO_GROS_DRUG_CST_ABOVE_AMT",
    "CUR_MO_GROS_DRUG_CST_BELOW_AMT",
    "CUR_MO_TOT_GROS_DRUG_CST_AMT",
    "CUR_MO_LOW_INCM_CST_SHARING_AMT",
    "CUR_MO_COV_PLN_PD_AMT",
    "TOT_DTL_RCRD_CT",
    "CUR_MO_SUBMT_DUE_AMT",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK"
)

# -----------------------------------------------------------------------------
# 6) Remove_Duplicates_14
# -----------------------------------------------------------------------------
df_deduped = dedup_sort(
    df_Copy_Lnk_Remove_Dup,
    partition_cols=["FILE_ID","SUBMT_CNTR_SEQ_ID","CNTR_SEQ_ID","SRC_SYS_CD"],
    sort_cols=[
        ("FILE_ID","A"),
        ("SUBMT_CNTR_SEQ_ID","A"),
        ("CNTR_SEQ_ID","A"),
        ("SRC_SYS_CD","A")
    ]
)
df_Remove_Duplicates_14_Lnk_RmDup = df_deduped.select(
    "FILE_ID",
    "SUBMT_CNTR_SEQ_ID",
    "CNTR_SEQ_ID",
    "SRC_SYS_CD"
)

# -----------------------------------------------------------------------------
# 7) Jn1_NKey (left join)
# -----------------------------------------------------------------------------
df_Jn1_NKey = df_Remove_Duplicates_14_Lnk_RmDup.alias("Lnk_RmDup").join(
    df_Db2_K_PDE_REC_RPT_SUBMT_CNTR_CNTR_F.alias("Lnk_KTableIn"),
    on=[
        F.col("Lnk_RmDup.FILE_ID") == F.col("Lnk_KTableIn.FILE_ID"),
        F.col("Lnk_RmDup.SUBMT_CNTR_SEQ_ID") == F.col("Lnk_KTableIn.SUBMT_CNTR_SEQ_ID"),
        F.col("Lnk_RmDup.CNTR_SEQ_ID") == F.col("Lnk_KTableIn.CNTR_SEQ_ID"),
        F.col("Lnk_RmDup.SRC_SYS_CD") == F.col("Lnk_KTableIn.SRC_SYS_CD")
    ],
    how="left"
)
df_Jn1_NKey_Lnk_Xfm1 = df_Jn1_NKey.select(
    F.col("Lnk_RmDup.FILE_ID").alias("FILE_ID"),
    F.col("Lnk_RmDup.SUBMT_CNTR_SEQ_ID").alias("SUBMT_CNTR_SEQ_ID"),
    F.col("Lnk_RmDup.CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
    F.col("Lnk_RmDup.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Lnk_KTableIn.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("Lnk_KTableIn.PDE_REC_RPT_SUBMT_CNTR_CNTR_SK").alias("PDE_REC_RPT_SUBMT_CNTR_CNTR_SK")
)

# -----------------------------------------------------------------------------
# 8) Transformer: NextSurrogateKey logic => SurrogateKeyGen
#    We store original PDE_REC_RPT_SUBMT_CNTR_CNTR_SK to apply link constraints
# -----------------------------------------------------------------------------
df_Transformer_input = df_Jn1_NKey_Lnk_Xfm1.withColumn(
    "original_PDE_REC_RPT_SUBMT_CNTR_CNTR_SK",
    F.when(
        (F.col("PDE_REC_RPT_SUBMT_CNTR_CNTR_SK").isNull()) | (F.col("PDE_REC_RPT_SUBMT_CNTR_CNTR_SK") == 0),
        F.lit(0)
    ).otherwise(F.col("PDE_REC_RPT_SUBMT_CNTR_CNTR_SK"))
)

df_enriched = df_Transformer_input
df_enriched = SurrogateKeyGen(
    df_enriched,
    <DB sequence name>,
    "PDE_REC_RPT_SUBMT_CNTR_CNTR_SK",
    <schema>,
    <secret_name>
)

# Lnk_KTableLoad: rows where original PDE_REC_RPT_SUBMT_CNTR_CNTR_SK = 0
df_Transformer_Lnk_KTableLoad = (
    df_enriched
    .filter(F.col("original_PDE_REC_RPT_SUBMT_CNTR_CNTR_SK") == 0)
    .select(
        F.col("FILE_ID").alias("FILE_ID"),
        F.col("SUBMT_CNTR_SEQ_ID").alias("SUBMT_CNTR_SEQ_ID"),
        F.col("CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.lit(CurrRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.col("PDE_REC_RPT_SUBMT_CNTR_CNTR_SK").alias("PDE_REC_RPT_SUBMT_CNTR_CNTR_SK")
    )
)

# Lnk_Jn: all rows, CRt_RUN_CYC_EXCTN_DT_SK depends on whether original was 0
df_Transformer_Lnk_Jn = (
    df_enriched
    .select(
        F.col("FILE_ID").alias("FILE_ID"),
        F.col("SUBMT_CNTR_SEQ_ID").alias("SUBMT_CNTR_SEQ_ID"),
        F.col("CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.when(F.col("original_PDE_REC_RPT_SUBMT_CNTR_CNTR_SK") == 0, CurrRunCycleDate)
         .otherwise(F.col("CRT_RUN_CYC_EXCTN_DT_SK")).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.col("PDE_REC_RPT_SUBMT_CNTR_CNTR_SK").alias("PDE_REC_RPT_SUBMT_CNTR_CNTR_SK")
    )
)

# -----------------------------------------------------------------------------
# 9) Db2_K_PDE_REC_RPT_SUBMT_CNTR_CNTR_F_Load => MERGE into EDW
# -----------------------------------------------------------------------------
# Create a temp table from df_Transformer_Lnk_KTableLoad
execute_dml(
    f"DROP TABLE IF EXISTS STAGING.PdeRecRptSubmtCntrCntrFExtr_Db2_K_PDE_REC_RPT_SUBMT_CNTR_CNTR_F_Load_temp",
    jdbc_url_edw,
    jdbc_props_edw
)
(
    df_Transformer_Lnk_KTableLoad.write.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("dbtable","STAGING.PdeRecRptSubmtCntrCntrFExtr_Db2_K_PDE_REC_RPT_SUBMT_CNTR_CNTR_F_Load_temp")
    .mode("append")
    .save()
)
merge_sql_Db2_K_PDE_REC_RPT_SUBMT_CNTR_CNTR_F_Load = f"""
MERGE INTO {EDWOwner}.K_PDE_REC_RPT_SUBMT_CNTR_CNTR_F AS T
USING STAGING.PdeRecRptSubmtCntrCntrFExtr_Db2_K_PDE_REC_RPT_SUBMT_CNTR_CNTR_F_Load_temp AS S
   ON T.PDE_REC_RPT_SUBMT_CNTR_CNTR_SK = S.PDE_REC_RPT_SUBMT_CNTR_CNTR_SK
WHEN MATCHED THEN UPDATE
   SET T.FILE_ID = S.FILE_ID,
       T.SUBMT_CNTR_SEQ_ID = S.SUBMT_CNTR_SEQ_ID,
       T.CNTR_SEQ_ID = S.CNTR_SEQ_ID,
       T.SRC_SYS_CD = S.SRC_SYS_CD,
       T.CRT_RUN_CYC_EXCTN_DT_SK = S.CRT_RUN_CYC_EXCTN_DT_SK
WHEN NOT MATCHED THEN INSERT
  (
    PDE_REC_RPT_SUBMT_CNTR_CNTR_SK,
    FILE_ID,
    SUBMT_CNTR_SEQ_ID,
    CNTR_SEQ_ID,
    SRC_SYS_CD,
    CRT_RUN_CYC_EXCTN_DT_SK
  )
  VALUES
  (
    S.PDE_REC_RPT_SUBMT_CNTR_CNTR_SK,
    S.FILE_ID,
    S.SUBMT_CNTR_SEQ_ID,
    S.CNTR_SEQ_ID,
    S.SRC_SYS_CD,
    S.CRT_RUN_CYC_EXCTN_DT_SK
  );
"""
execute_dml(merge_sql_Db2_K_PDE_REC_RPT_SUBMT_CNTR_CNTR_F_Load, jdbc_url_edw, jdbc_props_edw)

# -----------------------------------------------------------------------------
# 10) Jn2Nkey (inner join on Lnk_AllCol_Join and Lnk_Jn)
# -----------------------------------------------------------------------------
df_Jn2Nkey = df_Copy_Lnk_AllCol_Join.alias("Lnk_AllCol_Join").join(
    df_Transformer_Lnk_Jn.alias("Lnk_Jn"),
    on=[
        F.col("Lnk_AllCol_Join.FILE_ID") == F.col("Lnk_Jn.FILE_ID"),
        F.col("Lnk_AllCol_Join.SUBMT_CNTR_SEQ_ID") == F.col("Lnk_Jn.SUBMT_CNTR_SEQ_ID"),
        F.col("Lnk_AllCol_Join.CNTR_SEQ_ID") == F.col("Lnk_Jn.CNTR_SEQ_ID"),
        F.col("Lnk_AllCol_Join.SRC_SYS_CD") == F.col("Lnk_Jn.SRC_SYS_CD")
    ],
    how="inner"
)
df_Jn2Nkey_Lnk_Xfm = df_Jn2Nkey.select(
    F.col("Lnk_AllCol_Join.Key").alias("Key"),
    F.col("Lnk_AllCol_Join.Key1").alias("Key1"),
    F.col("Lnk_Jn.PDE_REC_RPT_SUBMT_CNTR_CNTR_SK").alias("PDE_REC_RPT_SUBMT_CNTR_CNTR_SK"),
    F.col("Lnk_AllCol_Join.FILE_ID").alias("FILE_ID"),
    F.col("Lnk_AllCol_Join.CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
    F.col("Lnk_AllCol_Join.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Lnk_Jn.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("Lnk_AllCol_Join.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("Lnk_AllCol_Join.CMS_CNTR_ID").alias("CMS_CNTR_ID"),
    F.col("Lnk_AllCol_Join.AS_OF_YR").alias("AS_OF_YR"),
    F.col("Lnk_AllCol_Join.AS_OF_MO").alias("AS_OF_MO"),
    F.col("Lnk_AllCol_Join.FILE_CRTN_DT").alias("FILE_CRTN_DT"),
    F.col("Lnk_AllCol_Join.FILE_CRTN_TM").alias("FILE_CRTN_TM"),
    F.col("Lnk_AllCol_Join.RPT_ID").alias("RPT_ID"),
    F.col("Lnk_AllCol_Join.DRUG_COV_STTUS_CD_TX").alias("DRUG_COV_STTUS_CD_TX"),
    F.col("Lnk_AllCol_Join.BNFCRY_CT").alias("BNFCRY_CT"),
    F.col("Lnk_AllCol_Join.CUR_MO_GROS_DRUG_CST_ABOVE_AMT").alias("CUR_MO_GROS_DRUG_CST_ABOVE_AMT"),
    F.col("Lnk_AllCol_Join.CUR_MO_GROS_DRUG_CST_BELOW_AMT").alias("CUR_MO_GROS_DRUG_CST_BELOW_AMT"),
    F.col("Lnk_AllCol_Join.CUR_MO_TOT_GROS_DRUG_CST_AMT").alias("CUR_MO_TOT_GROS_DRUG_CST_AMT"),
    F.col("Lnk_AllCol_Join.CUR_MO_LOW_INCM_CST_SHARING_AMT").alias("CUR_MO_LOW_INCM_CST_SHARING_AMT"),
    F.col("Lnk_AllCol_Join.CUR_MO_COV_PLN_PD_AMT").alias("CUR_MO_COV_PLN_PD_AMT"),
    F.col("Lnk_AllCol_Join.TOT_DTL_RCRD_CT").alias("TOT_DTL_RCRD_CT"),
    F.col("Lnk_AllCol_Join.CUR_MO_SUBMT_DUE_AMT").alias("CUR_MO_SUBMT_DUE_AMT"),
    F.col("Lnk_AllCol_Join.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_AllCol_Join.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_AllCol_Join.SUBMT_CNTR_SEQ_ID").alias("SUBMT_CNTR_SEQ_ID"),
    F.col("Lnk_AllCol_Join.SUBMT_CMS_CNTR_ID").alias("SUBMT_CMS_CNTR_ID"),
    F.col("Lnk_AllCol_Join.PDE_REC_RPT_SUBMT_CNTR_SK").alias("PDE_REC_RPT_SUBMT_CNTR_SK")
)

# -----------------------------------------------------------------------------
# 11) XFM => Lnk_SeqExtr and Lnk_LkpKey
# -----------------------------------------------------------------------------
df_XFM_input = df_Jn2Nkey_Lnk_Xfm

df_XFM_Lnk_SeqExtr = df_XFM_input.select(
    F.col("PDE_REC_RPT_SUBMT_CNTR_CNTR_SK").alias("PDE_REC_RPT_SUBMT_CNTR_CNTR_SK"),
    F.col("FILE_ID").alias("FILE_ID"),
    F.col("SUBMT_CNTR_SEQ_ID").alias("SUBMT_CNTR_SEQ_ID"),
    F.col("CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("PDE_REC_RPT_SUBMT_CNTR_SK").alias("PDE_REC_RPT_SUBMT_CNTR_SK"),
    F.col("SUBMT_CMS_CNTR_ID").alias("SUBMT_CMS_CNTR_ID"),
    F.col("CMS_CNTR_ID").alias("CMS_CNTR_ID"),
    F.col("AS_OF_YR").alias("AS_OF_YR"),
    F.col("AS_OF_MO").alias("AS_OF_MO"),
    F.col("FILE_CRTN_DT").alias("FILE_CRTN_DT"),
    F.col("FILE_CRTN_TM").alias("FILE_CRTN_TM"),
    F.col("RPT_ID").alias("RPT_ID"),
    F.col("DRUG_COV_STTUS_CD_TX").alias("DRUG_COV_STTUS_CD_TX"),
    F.col("BNFCRY_CT").alias("BNFCRY_CT"),
    F.col("CUR_MO_GROS_DRUG_CST_ABOVE_AMT").alias("CUR_MO_GROS_DRUG_CST_ABOVE_AMT"),
    F.col("CUR_MO_GROS_DRUG_CST_BELOW_AMT").alias("CUR_MO_GROS_DRUG_CST_BELOW_AMT"),
    F.col("CUR_MO_TOT_GROS_DRUG_CST_AMT").alias("CUR_MO_TOT_GROS_DRUG_CST_AMT"),
    F.col("CUR_MO_LOW_INCM_CST_SHARING_AMT").alias("CUR_MO_LOW_INCM_CST_SHARING_AMT"),
    F.col("CUR_MO_COV_PLN_PD_AMT").alias("CUR_MO_COV_PLN_PD_AMT"),
    F.col("TOT_DTL_RCRD_CT").alias("TOT_DTL_RCRD_CT"),
    F.col("CUR_MO_SUBMT_DUE_AMT").alias("CUR_MO_SUBMT_DUE_AMT"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_XFM_Lnk_LkpKey = df_XFM_input.select(
    F.col("Key").alias("Key"),
    F.col("Key1").alias("Key1"),
    F.col("PDE_REC_RPT_SUBMT_CNTR_CNTR_SK").alias("PDE_REC_RPT_SUBMT_CNTR_CNTR_SK"),
    F.col("FILE_ID").alias("FILE_ID"),
    F.col("SUBMT_CNTR_SEQ_ID").alias("SUBMT_CNTR_SEQ_ID"),
    F.col("CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
    F.col("PDE_REC_RPT_SUBMT_CNTR_SK").alias("PDE_REC_RPT_SUBMT_CNTR_SK"),
    F.col("SUBMT_CMS_CNTR_ID").alias("SUBMT_CMS_CNTR_ID"),
    F.col("CMS_CNTR_ID").alias("CMS_CNTR_ID")
)

# -----------------------------------------------------------------------------
# 12) Write Seq_PDE_REC_RPT_SUBMT_CNTR_CNTR_F_Extr
# -----------------------------------------------------------------------------
# Enforce column order and apply rpad if needed for char/varchar
df_final_SeqExtr = df_XFM_Lnk_SeqExtr.select(
    F.col("PDE_REC_RPT_SUBMT_CNTR_CNTR_SK"),
    F.col("FILE_ID"),
    F.col("SUBMT_CNTR_SEQ_ID"),
    F.col("CNTR_SEQ_ID"),
    F.col("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("PDE_REC_RPT_SUBMT_CNTR_SK"),
    F.col("SUBMT_CMS_CNTR_ID"),
    F.col("CMS_CNTR_ID"),
    F.col("AS_OF_YR"),
    F.col("AS_OF_MO"),
    F.col("FILE_CRTN_DT"),
    F.col("FILE_CRTN_TM"),
    F.col("RPT_ID"),
    F.col("DRUG_COV_STTUS_CD_TX"),
    F.col("BNFCRY_CT"),
    F.col("CUR_MO_GROS_DRUG_CST_ABOVE_AMT"),
    F.col("CUR_MO_GROS_DRUG_CST_BELOW_AMT"),
    F.col("CUR_MO_TOT_GROS_DRUG_CST_AMT"),
    F.col("CUR_MO_LOW_INCM_CST_SHARING_AMT"),
    F.col("CUR_MO_COV_PLN_PD_AMT"),
    F.col("TOT_DTL_RCRD_CT"),
    F.col("CUR_MO_SUBMT_DUE_AMT"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)
write_files(
    df_final_SeqExtr,
    f"{adls_path}/verified/PDE_REC_RPT_SUBMT_CNTR_CNTR_F_Load.txt",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote='"',
    nullValue=None
)

# -----------------------------------------------------------------------------
# 13) Lkp_Key (PxLookup left join):
#     Primary link: df_Seq_PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_F_Key
#     Lookup link: df_XFM_Lnk_LkpKey
# -----------------------------------------------------------------------------
df_Lkp_Key_join = df_Seq_PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_F_Key.alias("Lnk_DtlIntSeq").join(
    df_XFM_Lnk_LkpKey.alias("Lnk_LkpKey"),
    on=[
        (F.col("Lnk_DtlIntSeq.Key") == F.col("Lnk_LkpKey.Key")) &
        (F.col("Lnk_DtlIntSeq.Key1") == F.col("Lnk_LkpKey.Key1"))
    ],
    how="left"
)
df_Lkp_Key_Lnk_DtlSeq = df_Lkp_Key_join.select(
    F.col("Lnk_DtlIntSeq.PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_SK").alias("PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_SK"),
    F.col("Lnk_LkpKey.FILE_ID").alias("FILE_ID"),
    F.col("Lnk_LkpKey.SUBMT_CNTR_SEQ_ID").alias("SUBMT_CNTR_SEQ_ID"),
    F.col("Lnk_LkpKey.CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
    F.col("Lnk_DtlIntSeq.DTL_SEQ_ID").alias("DTL_SEQ_ID"),
    F.col("Lnk_DtlIntSeq.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Lnk_DtlIntSeq.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("Lnk_DtlIntSeq.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("Lnk_LkpKey.PDE_REC_RPT_SUBMT_CNTR_CNTR_SK").alias("PDE_REC_RPT_SUBMT_CNTR_CNTR_SK"),
    F.col("Lnk_LkpKey.SUBMT_CMS_CNTR_ID").alias("SUBMT_CMS_CNTR_ID"),
    F.col("Lnk_LkpKey.CMS_CNTR_ID").alias("CMS_CNTR_ID"),
    F.col("Lnk_DtlIntSeq.CUR_MCARE_BNFCRY_ID").alias("CUR_MCARE_BNFCRY_ID"),
    F.col("Lnk_DtlIntSeq.LAST_SUBMT_MCARE_BNFCRY_ID").alias("LAST_SUBMT_MCARE_BNFCRY_ID"),
    F.col("Lnk_DtlIntSeq.LAST_SUBMT_CARDHLDR_ID").alias("LAST_SUBMT_CARDHLDR_ID"),
    F.col("Lnk_DtlIntSeq.DRUG_COV_STTUS_CD_TX").alias("DRUG_COV_STTUS_CD_TX"),
    F.col("Lnk_DtlIntSeq.CUR_MO_GROS_DRUG_CST_ABOVE_AMT").alias("CUR_MO_GROS_DRUG_CST_ABOVE_AMT"),
    F.col("Lnk_DtlIntSeq.CUR_MO_GROS_DRUG_CST_BELOW_AMT").alias("CUR_MO_GROS_DRUG_CST_BELOW_AMT"),
    F.col("Lnk_DtlIntSeq.CUR_MO_TOT_GROS_DRUG_CST_AMT").alias("CUR_MO_TOT_GROS_DRUG_CST_AMT"),
    F.col("Lnk_DtlIntSeq.CUR_MO_LOW_INCM_CST_SHARING_AMT").alias("CUR_MO_LOW_INCM_CST_SHARING_AMT"),
    F.col("Lnk_DtlIntSeq.CUR_MO_COV_PLN_PD_AMT").alias("CUR_MO_COV_PLN_PD_AMT"),
    F.col("Lnk_DtlIntSeq.CUR_SUBMT_DUE_AMT").alias("CUR_SUBMT_DUE_AMT"),
    F.col("Lnk_DtlIntSeq.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_DtlIntSeq.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# -----------------------------------------------------------------------------
# 14) Write Seq_PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_F
# -----------------------------------------------------------------------------
df_final_Seq_PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_F = df_Lkp_Key_Lnk_DtlSeq.select(
    F.col("PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_SK"),
    F.col("FILE_ID"),
    F.col("SUBMT_CNTR_SEQ_ID"),
    F.col("CNTR_SEQ_ID"),
    F.col("DTL_SEQ_ID"),
    F.col("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("PDE_REC_RPT_SUBMT_CNTR_CNTR_SK"),
    F.col("SUBMT_CMS_CNTR_ID"),
    F.col("CMS_CNTR_ID"),
    F.col("CUR_MCARE_BNFCRY_ID"),
    F.col("LAST_SUBMT_MCARE_BNFCRY_ID"),
    F.col("LAST_SUBMT_CARDHLDR_ID"),
    F.col("DRUG_COV_STTUS_CD_TX"),
    F.col("CUR_MO_GROS_DRUG_CST_ABOVE_AMT"),
    F.col("CUR_MO_GROS_DRUG_CST_BELOW_AMT"),
    F.col("CUR_MO_TOT_GROS_DRUG_CST_AMT"),
    F.col("CUR_MO_LOW_INCM_CST_SHARING_AMT"),
    F.col("CUR_MO_COV_PLN_PD_AMT"),
    F.col("CUR_SUBMT_DUE_AMT"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)
write_files(
    df_final_Seq_PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_F,
    f"{adls_path}/verified/PDE_REC_RPT_SUBMT_CNTR_CNTR_DTL_F.txt",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote='"',
    nullValue=None
)