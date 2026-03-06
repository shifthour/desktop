# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2007, 2008, 2009 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:  
# MAGIC SEQ Job- EdwPdePayblRptCntrFCntl
# MAGIC PROCESSING: This Job Extracts the data from P2P Source File OPTUMRX.PBM.RPT_DDPS_P2P_PAYABLE* and loads the data into EDW Table PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_F
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
CurrRunCycle = get_widget_value('CurrRunCycle','')

# Read Copy_of_Seq_PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_F
schema_Copy_of_Seq_PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_F = StructType([
    StructField("Key", IntegerType(), False),
    StructField("Key1", IntegerType(), False),
    StructField("PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_SK", IntegerType(), False),
    StructField("DTL_SEQ_ID", StringType(), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), False),
    StructField("CUR_MCARE_BNFCRY_ID", StringType(), False),
    StructField("LAST_SUBMT_MCARE_BNFCRY_ID", StringType(), False),
    StructField("DRUG_COV_STTUS_CD_TX", StringType(), False),
    StructField("CUR_MO_GROS_DRUG_CST_BELOW_AMT", DecimalType(38,10), False),
    StructField("CUR_MO_GROS_DRUG_CST_ABOVE_AMT", DecimalType(38,10), False),
    StructField("CUR_MO_TOT_GROS_DRUG_CST_AMT", DecimalType(38,10), False),
    StructField("CUR_MO_LOW_INCM_CST_SHARING_AMT", DecimalType(38,10), False),
    StructField("CUR_MO_COV_PLN_PD_AMT", DecimalType(38,10), False),
    StructField("CUR_SUBMT_DUE_AMT", DecimalType(38,10), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False)
])
df_Copy_of_Seq_PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_F = (
    spark.read.csv(
        f"{adls_path}/verified/PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_F_Key.txt",
        header=True,
        sep=",",
        quote="\"",
        nullValue=None,
        schema=schema_Copy_of_Seq_PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_F
    )
)

# Read Db2_K_PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_F
jdbc_url_EDW, jdbc_props_EDW = get_db_config(edw_secret_name)
extract_query_Db2_K_PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_F = (
    "SELECT FILE_ID, CNTR_SEQ_ID, SUBMT_CNTR_SEQ_ID, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_DT_SK, "
    "PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_SK "
    "FROM " + EDWOwner + ".K_PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_F"
)
df_Db2_K_PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_F = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_EDW)
    .options(**jdbc_props_EDW)
    .option("query", extract_query_Db2_K_PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_F)
    .load()
)

# Read Seq_PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_F
schema_Seq_PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_F = StructType([
    StructField("Key", IntegerType(), False),
    StructField("Key1", IntegerType(), False),
    StructField("PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_SK", IntegerType(), False),
    StructField("FILE_ID", StringType(), False),
    StructField("CNTR_SEQ_ID", StringType(), False),
    StructField("SUBMT_CNTR_SEQ_ID", StringType(), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), False),
    StructField("PDE_PAYBL_RPT_CNTR_SK", IntegerType(), False),
    StructField("CMS_CNTR_ID", StringType(), False),
    StructField("SUBMT_CMS_CNTR_ID", StringType(), False),
    StructField("AS_OF_YR", StringType(), False),
    StructField("AS_OF_MO", StringType(), False),
    StructField("FILE_CRTN_DT", StringType(), False),
    StructField("FILE_CRTN_TM", StringType(), False),
    StructField("RPT_ID", StringType(), False),
    StructField("DRUG_COV_STTUS_CD_TX", StringType(), False),
    StructField("BNFCRY_CT", IntegerType(), False),
    StructField("CUR_MO_GROS_DRUG_CST_BELOW_AMT", DecimalType(38,10), False),
    StructField("CUR_MO_GROS_DRUG_CST_ABOVE_AMT", DecimalType(38,10), False),
    StructField("CUR_MO_TOT_GROS_DRUG_CST_AMT", DecimalType(38,10), False),
    StructField("CUR_MO_LOW_INCM_CST_SHARING_AMT", DecimalType(38,10), False),
    StructField("CUR_MO_COV_PLN_PD_AMT", DecimalType(38,10), False),
    StructField("TOT_DTL_RCRD_CT", IntegerType(), False),
    StructField("CUR_MO_SUBMT_DUE_AMT", DecimalType(38,10), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False)
])
df_Seq_PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_F = (
    spark.read.csv(
        f"{adls_path}/verified/PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_F.txt",
        header=True,
        sep=",",
        quote="\"",
        nullValue=None,
        schema=schema_Seq_PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_F
    )
)

# Xfm_SHD_STR
df_Xfm_SHD_STR = df_Seq_PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_F.select(
    col("Key").alias("Key"),
    col("Key1").alias("Key1"),
    col("FILE_ID").alias("FILE_ID"),
    col("SUBMT_CNTR_SEQ_ID").alias("SUBMT_CNTR_SEQ_ID"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("SUBMT_CMS_CNTR_ID").alias("SUBMT_CMS_CNTR_ID"),
    col("AS_OF_YR").alias("AS_OF_YR"),
    col("AS_OF_MO").alias("AS_OF_MO"),
    col("FILE_CRTN_DT").alias("FILE_CRTN_DT"),
    col("FILE_CRTN_TM").alias("FILE_CRTN_TM"),
    col("RPT_ID").alias("RPT_ID"),
    col("DRUG_COV_STTUS_CD_TX").alias("DRUG_COV_STTUS_CD_TX"),
    col("BNFCRY_CT").alias("BNFCRY_CT"),
    col("CUR_MO_GROS_DRUG_CST_BELOW_AMT").alias("CUR_MO_GROS_DRUG_CST_BELOW_AMT"),
    col("CUR_MO_GROS_DRUG_CST_ABOVE_AMT").alias("CUR_MO_GROS_DRUG_CST_ABOVE_AMT"),
    col("CUR_MO_TOT_GROS_DRUG_CST_AMT").alias("CUR_MO_TOT_GROS_DRUG_CST_AMT"),
    col("CUR_MO_LOW_INCM_CST_SHARING_AMT").alias("CUR_MO_LOW_INCM_CST_SHARING_AMT"),
    col("CUR_MO_COV_PLN_PD_AMT").alias("CUR_MO_COV_PLN_PD_AMT"),
    col("TOT_DTL_RCRD_CT").alias("TOT_DTL_RCRD_CT"),
    col("CUR_MO_SUBMT_DUE_AMT").alias("CUR_MO_SUBMT_DUE_AMT"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("PDE_PAYBL_RPT_CNTR_SK").alias("PDE_PAYBL_RPT_CNTR_SK"),
    col("CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
    col("CMS_CNTR_ID").alias("CMS_CNTR_ID")
)

# Copy: Two outputs -> Lnk_Remove_Dup and Lnk_AllCol_Join
df_Lnk_Remove_Dup = df_Xfm_SHD_STR.select(
    col("FILE_ID").alias("FILE_ID"),
    col("CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
    col("SUBMT_CNTR_SEQ_ID").alias("SUBMT_CNTR_SEQ_ID"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD")
)
df_Lnk_AllCol_Join = df_Xfm_SHD_STR.select(
    col("Key").alias("Key"),
    col("Key1").alias("Key1"),
    col("FILE_ID").alias("FILE_ID"),
    col("SUBMT_CNTR_SEQ_ID").alias("SUBMT_CNTR_SEQ_ID"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("SUBMT_CMS_CNTR_ID").alias("SUBMT_CMS_CNTR_ID"),
    col("AS_OF_YR").alias("AS_OF_YR"),
    col("AS_OF_MO").alias("AS_OF_MO"),
    col("FILE_CRTN_DT").alias("FILE_CRTN_DT"),
    col("FILE_CRTN_TM").alias("FILE_CRTN_TM"),
    col("RPT_ID").alias("RPT_ID"),
    col("DRUG_COV_STTUS_CD_TX").alias("DRUG_COV_STTUS_CD_TX"),
    col("BNFCRY_CT").alias("BNFCRY_CT"),
    col("CUR_MO_GROS_DRUG_CST_BELOW_AMT").alias("CUR_MO_GROS_DRUG_CST_BELOW_AMT"),
    col("CUR_MO_GROS_DRUG_CST_ABOVE_AMT").alias("CUR_MO_GROS_DRUG_CST_ABOVE_AMT"),
    col("CUR_MO_TOT_GROS_DRUG_CST_AMT").alias("CUR_MO_TOT_GROS_DRUG_CST_AMT"),
    col("CUR_MO_LOW_INCM_CST_SHARING_AMT").alias("CUR_MO_LOW_INCM_CST_SHARING_AMT"),
    col("CUR_MO_COV_PLN_PD_AMT").alias("CUR_MO_COV_PLN_PD_AMT"),
    col("TOT_DTL_RCRD_CT").alias("TOT_DTL_RCRD_CT"),
    col("CUR_MO_SUBMT_DUE_AMT").alias("CUR_MO_SUBMT_DUE_AMT"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("PDE_PAYBL_RPT_CNTR_SK").alias("PDE_PAYBL_RPT_CNTR_SK"),
    col("CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
    col("CMS_CNTR_ID").alias("CMS_CNTR_ID")
)

# Remove_Duplicates1
df_deduped = dedup_sort(
    df_Lnk_Remove_Dup,
    ["FILE_ID","CNTR_SEQ_ID","SUBMT_CNTR_SEQ_ID","SRC_SYS_CD"],
    [("FILE_ID","A"),("CNTR_SEQ_ID","A"),("SUBMT_CNTR_SEQ_ID","A"),("SRC_SYS_CD","A")]
)
df_Lnk_RmDup = df_deduped.select(
    col("FILE_ID").alias("FILE_ID"),
    col("CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
    col("SUBMT_CNTR_SEQ_ID").alias("SUBMT_CNTR_SEQ_ID"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

# Jn1_NKey (left outer join)
left_jn1 = df_Lnk_RmDup.alias("Lnk_RmDup")
right_jn1 = df_Db2_K_PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_F.alias("Lnk_KTableIn")
df_Jn1_NKey = (
    left_jn1.join(
        right_jn1,
        [
            left_jn1.FILE_ID == right_jn1.FILE_ID,
            left_jn1.CNTR_SEQ_ID == right_jn1.CNTR_SEQ_ID,
            left_jn1.SUBMT_CNTR_SEQ_ID == right_jn1.SUBMT_CNTR_SEQ_ID,
            left_jn1.SRC_SYS_CD == right_jn1.SRC_SYS_CD
        ],
        how="left"
    )
    .select(
        col("Lnk_RmDup.FILE_ID").alias("FILE_ID"),
        col("Lnk_RmDup.CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
        col("Lnk_RmDup.SUBMT_CNTR_SEQ_ID").alias("SUBMT_CNTR_SEQ_ID"),
        col("Lnk_RmDup.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("Lnk_KTableIn.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        col("Lnk_KTableIn.PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_SK").alias("PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_SK")
    )
)

# Transformer (handles PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_SK logic, then split)
df_Transformer = (
    df_Jn1_NKey
    .withColumn(
        "to_insert_flag",
        when(
            (col("PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_SK").isNull()) | (col("PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_SK") == 0),
            lit(1)
        ).otherwise(lit(0))
    )
)

df_enriched = df_Transformer
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_SK",<schema>,<secret_name>)

df_insert = df_enriched.filter(col("to_insert_flag") == 1).select(
    col("FILE_ID").alias("FILE_ID"),
    col("CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
    col("SUBMT_CNTR_SEQ_ID").alias("SUBMT_CNTR_SEQ_ID"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    lit(CurrRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_SK").alias("PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_SK")
)

jdbc_url, jdbc_props = get_db_config(edw_secret_name)
execute_dml("DROP TABLE IF EXISTS STAGING.PdePayblRptCntrSubmtCntrF_Load_temp", jdbc_url, jdbc_props)
df_insert.write.format("jdbc").option("url", jdbc_url).options(**jdbc_props).option("dbtable","STAGING.PdePayblRptCntrSubmtCntrF_Load_temp").mode("overwrite").save()
merge_sql = (
    "MERGE INTO " + EDWOwner + ".K_PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_F AS T "
    "USING STAGING.PdePayblRptCntrSubmtCntrF_Load_temp AS S "
    "ON T.FILE_ID = S.FILE_ID AND T.CNTR_SEQ_ID = S.CNTR_SEQ_ID AND T.SUBMT_CNTR_SEQ_ID = S.SUBMT_CNTR_SEQ_ID AND T.SRC_SYS_CD = S.SRC_SYS_CD "
    "WHEN NOT MATCHED THEN INSERT (FILE_ID, CNTR_SEQ_ID, SUBMT_CNTR_SEQ_ID, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_DT_SK, PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_SK) "
    "VALUES (S.FILE_ID, S.CNTR_SEQ_ID, S.SUBMT_CNTR_SEQ_ID, S.SRC_SYS_CD, S.CRT_RUN_CYC_EXCTN_DT_SK, S.PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_SK);"
)
execute_dml(merge_sql, jdbc_url, jdbc_props)

df_Lnk_Jn = (
    df_enriched
    .withColumn(
        "CRT_RUN_CYC_EXCTN_DT_SK",
        when(col("to_insert_flag") == 1, CurrRunCycleDate).otherwise(col("CRT_RUN_CYC_EXCTN_DT_SK"))
    )
    .drop("to_insert_flag")
    .select(
        col("FILE_ID").alias("FILE_ID"),
        col("CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
        col("SUBMT_CNTR_SEQ_ID").alias("SUBMT_CNTR_SEQ_ID"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        col("PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_SK").alias("PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_SK")
    )
)

# Jn2Nkey (inner join on df_Lnk_AllCol_Join and df_Lnk_Jn)
left_jn2 = df_Lnk_AllCol_Join.alias("Lnk_AllCol_Join")
right_jn2 = df_Lnk_Jn.alias("Lnk_Jn")
df_Jn2Nkey = (
    left_jn2.join(
        right_jn2,
        [
            left_jn2.FILE_ID == right_jn2.FILE_ID,
            left_jn2.CNTR_SEQ_ID == right_jn2.CNTR_SEQ_ID,
            left_jn2.SUBMT_CNTR_SEQ_ID == right_jn2.SUBMT_CNTR_SEQ_ID,
            left_jn2.SRC_SYS_CD == right_jn2.SRC_SYS_CD
        ],
        how="inner"
    )
    .select(
        col("Lnk_AllCol_Join.Key").alias("Key"),
        col("Lnk_AllCol_Join.Key1").alias("Key1"),
        col("Lnk_AllCol_Join.FILE_ID").alias("FILE_ID"),
        col("Lnk_AllCol_Join.SUBMT_CNTR_SEQ_ID").alias("SUBMT_CNTR_SEQ_ID"),
        col("Lnk_AllCol_Join.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("Lnk_AllCol_Join.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        col("Lnk_AllCol_Join.SUBMT_CMS_CNTR_ID").alias("SUBMT_CMS_CNTR_ID"),
        col("Lnk_AllCol_Join.AS_OF_YR").alias("AS_OF_YR"),
        col("Lnk_AllCol_Join.AS_OF_MO").alias("AS_OF_MO"),
        col("Lnk_AllCol_Join.FILE_CRTN_DT").alias("FILE_CRTN_DT"),
        col("Lnk_AllCol_Join.FILE_CRTN_TM").alias("FILE_CRTN_TM"),
        col("Lnk_AllCol_Join.RPT_ID").alias("RPT_ID"),
        col("Lnk_AllCol_Join.DRUG_COV_STTUS_CD_TX").alias("DRUG_COV_STTUS_CD_TX"),
        col("Lnk_AllCol_Join.BNFCRY_CT").alias("BNFCRY_CT"),
        col("Lnk_AllCol_Join.CUR_MO_GROS_DRUG_CST_BELOW_AMT").alias("CUR_MO_GROS_DRUG_CST_BELOW_AMT"),
        col("Lnk_AllCol_Join.CUR_MO_GROS_DRUG_CST_ABOVE_AMT").alias("CUR_MO_GROS_DRUG_CST_ABOVE_AMT"),
        col("Lnk_AllCol_Join.CUR_MO_TOT_GROS_DRUG_CST_AMT").alias("CUR_MO_TOT_GROS_DRUG_CST_AMT"),
        col("Lnk_AllCol_Join.CUR_MO_LOW_INCM_CST_SHARING_AMT").alias("CUR_MO_LOW_INCM_CST_SHARING_AMT"),
        col("Lnk_AllCol_Join.CUR_MO_COV_PLN_PD_AMT").alias("CUR_MO_COV_PLN_PD_AMT"),
        col("Lnk_AllCol_Join.TOT_DTL_RCRD_CT").alias("TOT_DTL_RCRD_CT"),
        col("Lnk_AllCol_Join.CUR_MO_SUBMT_DUE_AMT").alias("CUR_MO_SUBMT_DUE_AMT"),
        col("Lnk_AllCol_Join.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("Lnk_AllCol_Join.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("Lnk_AllCol_Join.PDE_PAYBL_RPT_CNTR_SK").alias("PDE_PAYBL_RPT_CNTR_SK"),
        col("Lnk_AllCol_Join.CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
        col("Lnk_AllCol_Join.CMS_CNTR_ID").alias("CMS_CNTR_ID"),
        col("Lnk_Jn.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        col("Lnk_Jn.PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_SK").alias("PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_SK")
    )
)

# Xfm_DET_SK_Map
df_Xfm_DET_SK_Map = df_Jn2Nkey

df_Lnk_SeqExtr = df_Xfm_DET_SK_Map.select(
    col("PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_SK").alias("PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_SK"),
    col("FILE_ID").alias("FILE_ID"),
    col("CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
    col("SUBMT_CNTR_SEQ_ID").alias("SUBMT_CNTR_SEQ_ID"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("PDE_PAYBL_RPT_CNTR_SK").alias("PDE_PAYBL_RPT_CNTR_SK"),
    col("CMS_CNTR_ID").alias("CMS_CNTR_ID"),
    col("SUBMT_CMS_CNTR_ID").alias("SUBMT_CMS_CNTR_ID"),
    col("AS_OF_YR").alias("AS_OF_YR"),
    col("AS_OF_MO").alias("AS_OF_MO"),
    col("FILE_CRTN_DT").alias("FILE_CRTN_DT"),
    col("FILE_CRTN_TM").alias("FILE_CRTN_TM"),
    col("RPT_ID").alias("RPT_ID"),
    col("DRUG_COV_STTUS_CD_TX").alias("DRUG_COV_STTUS_CD_TX"),
    col("BNFCRY_CT").alias("BNFCRY_CT"),
    col("CUR_MO_GROS_DRUG_CST_BELOW_AMT").alias("CUR_MO_GROS_DRUG_CST_BELOW_AMT"),
    col("CUR_MO_GROS_DRUG_CST_ABOVE_AMT").alias("CUR_MO_GROS_DRUG_CST_ABOVE_AMT"),
    col("CUR_MO_TOT_GROS_DRUG_CST_AMT").alias("CUR_MO_TOT_GROS_DRUG_CST_AMT"),
    col("CUR_MO_LOW_INCM_CST_SHARING_AMT").alias("CUR_MO_LOW_INCM_CST_SHARING_AMT"),
    col("CUR_MO_COV_PLN_PD_AMT").alias("CUR_MO_COV_PLN_PD_AMT"),
    col("TOT_DTL_RCRD_CT").alias("TOT_DTL_RCRD_CT"),
    col("CUR_MO_SUBMT_DUE_AMT").alias("CUR_MO_SUBMT_DUE_AMT"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_Lnk_LkpKey = df_Xfm_DET_SK_Map.select(
    col("Key").alias("Key"),
    col("Key1").alias("Key1"),
    col("PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_SK").alias("PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_SK"),
    col("FILE_ID").alias("FILE_ID"),
    col("CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
    col("SUBMT_CNTR_SEQ_ID").alias("SUBMT_CNTR_SEQ_ID"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("PDE_PAYBL_RPT_CNTR_SK").alias("PDE_PAYBL_RPT_CNTR_SK"),
    col("CMS_CNTR_ID").alias("CMS_CNTR_ID"),
    col("SUBMT_CMS_CNTR_ID").alias("SUBMT_CMS_CNTR_ID")
)

# Write Seq_PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_F_Extr (final file)
# Apply rpad on char/varchar columns
df_final_SeqExtr = df_Lnk_SeqExtr.select(
    col("PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_SK"),
    rpad(col("FILE_ID"), 255, " ").alias("FILE_ID"),
    rpad(col("CNTR_SEQ_ID"), 255, " ").alias("CNTR_SEQ_ID"),
    rpad(col("SUBMT_CNTR_SEQ_ID"), 255, " ").alias("SUBMT_CNTR_SEQ_ID"),
    rpad(col("SRC_SYS_CD"), 255, " ").alias("SRC_SYS_CD"),
    rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("PDE_PAYBL_RPT_CNTR_SK"),
    rpad(col("CMS_CNTR_ID"), 255, " ").alias("CMS_CNTR_ID"),
    rpad(col("SUBMT_CMS_CNTR_ID"), 255, " ").alias("SUBMT_CMS_CNTR_ID"),
    rpad(col("AS_OF_YR"), 255, " ").alias("AS_OF_YR"),
    rpad(col("AS_OF_MO"), 255, " ").alias("AS_OF_MO"),
    rpad(col("FILE_CRTN_DT"), 255, " ").alias("FILE_CRTN_DT"),
    rpad(col("FILE_CRTN_TM"), 255, " ").alias("FILE_CRTN_TM"),
    rpad(col("RPT_ID"), 255, " ").alias("RPT_ID"),
    rpad(col("DRUG_COV_STTUS_CD_TX"), 255, " ").alias("DRUG_COV_STTUS_CD_TX"),
    col("BNFCRY_CT"),
    col("CUR_MO_GROS_DRUG_CST_BELOW_AMT"),
    col("CUR_MO_GROS_DRUG_CST_ABOVE_AMT"),
    col("CUR_MO_TOT_GROS_DRUG_CST_AMT"),
    col("CUR_MO_LOW_INCM_CST_SHARING_AMT"),
    col("CUR_MO_COV_PLN_PD_AMT"),
    col("TOT_DTL_RCRD_CT"),
    col("CUR_MO_SUBMT_DUE_AMT"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

write_files(
    df_final_SeqExtr,
    f"{adls_path}/verified/PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_F_Load.txt",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

# PxLookup: LkpKey => primary: df_Copy_of_Seq_PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_F , lookup: df_Lnk_LkpKey
df_LkpKey_join = (
    df_Copy_of_Seq_PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_F.alias("Lnk_DtlIntSeq")
    .join(
        df_Lnk_LkpKey.alias("Lnk_LkpKey"),
        [
            col("Lnk_DtlIntSeq.Key") == col("Lnk_LkpKey.Key"),
            col("Lnk_DtlIntSeq.Key1") == col("Lnk_LkpKey.Key1")
        ],
        how="left"
    )
)

df_Lnk_DtlSeq = df_LkpKey_join.select(
    col("Lnk_DtlIntSeq.PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_SK").alias("PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_SK"),
    col("Lnk_LkpKey.FILE_ID").alias("FILE_ID"),
    col("Lnk_LkpKey.CNTR_SEQ_ID").alias("CNTR_SEQ_ID"),
    col("Lnk_LkpKey.SUBMT_CNTR_SEQ_ID").alias("SUBMT_CNTR_SEQ_ID"),
    col("Lnk_DtlIntSeq.DTL_SEQ_ID").alias("DTL_SEQ_ID"),
    col("Lnk_LkpKey.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("Lnk_DtlIntSeq.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("Lnk_DtlIntSeq.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("Lnk_LkpKey.PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_SK").alias("PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_SK"),
    col("Lnk_LkpKey.CMS_CNTR_ID").alias("CMS_CNTR_ID"),
    col("Lnk_LkpKey.SUBMT_CMS_CNTR_ID").alias("SUBMT_CMS_CNTR_ID"),
    col("Lnk_DtlIntSeq.CUR_MCARE_BNFCRY_ID").alias("CUR_MCARE_BNFCRY_ID"),
    col("Lnk_DtlIntSeq.LAST_SUBMT_MCARE_BNFCRY_ID").alias("LAST_SUBMT_MCARE_BNFCRY_ID"),
    col("Lnk_DtlIntSeq.DRUG_COV_STTUS_CD_TX").alias("DRUG_COV_STTUS_CD_TX"),
    col("Lnk_DtlIntSeq.CUR_MO_GROS_DRUG_CST_BELOW_AMT").alias("CUR_MO_GROS_DRUG_CST_BELOW_AMT"),
    col("Lnk_DtlIntSeq.CUR_MO_GROS_DRUG_CST_ABOVE_AMT").alias("CUR_MO_GROS_DRUG_CST_ABOVE_AMT"),
    col("Lnk_DtlIntSeq.CUR_MO_TOT_GROS_DRUG_CST_AMT").alias("CUR_MO_TOT_GROS_DRUG_CST_AMT"),
    col("Lnk_DtlIntSeq.CUR_MO_LOW_INCM_CST_SHARING_AMT").alias("CUR_MO_LOW_INCM_CST_SHARING_AMT"),
    col("Lnk_DtlIntSeq.CUR_MO_COV_PLN_PD_AMT").alias("CUR_MO_COV_PLN_PD_AMT"),
    col("Lnk_DtlIntSeq.CUR_SUBMT_DUE_AMT").alias("CUR_SUBMT_DUE_AMT"),
    col("Lnk_DtlIntSeq.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("Lnk_DtlIntSeq.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# Write Seq_PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_F
df_final_Dtl = df_Lnk_DtlSeq.select(
    col("PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_SK"),
    rpad(col("FILE_ID"), 255, " ").alias("FILE_ID"),
    rpad(col("CNTR_SEQ_ID"), 255, " ").alias("CNTR_SEQ_ID"),
    rpad(col("SUBMT_CNTR_SEQ_ID"), 255, " ").alias("SUBMT_CNTR_SEQ_ID"),
    rpad(col("DTL_SEQ_ID"), 255, " ").alias("DTL_SEQ_ID"),
    rpad(col("SRC_SYS_CD"), 255, " ").alias("SRC_SYS_CD"),
    rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_SK"),
    rpad(col("CMS_CNTR_ID"), 255, " ").alias("CMS_CNTR_ID"),
    rpad(col("SUBMT_CMS_CNTR_ID"), 255, " ").alias("SUBMT_CMS_CNTR_ID"),
    rpad(col("CUR_MCARE_BNFCRY_ID"), 255, " ").alias("CUR_MCARE_BNFCRY_ID"),
    rpad(col("LAST_SUBMT_MCARE_BNFCRY_ID"), 255, " ").alias("LAST_SUBMT_MCARE_BNFCRY_ID"),
    rpad(col("DRUG_COV_STTUS_CD_TX"), 255, " ").alias("DRUG_COV_STTUS_CD_TX"),
    col("CUR_MO_GROS_DRUG_CST_BELOW_AMT"),
    col("CUR_MO_GROS_DRUG_CST_ABOVE_AMT"),
    col("CUR_MO_TOT_GROS_DRUG_CST_AMT"),
    col("CUR_MO_LOW_INCM_CST_SHARING_AMT"),
    col("CUR_MO_COV_PLN_PD_AMT"),
    col("CUR_SUBMT_DUE_AMT"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)
write_files(
    df_final_Dtl,
    f"{adls_path}/verified/PDE_PAYBL_RPT_CNTR_SUBMT_CNTR_DTL_F.txt",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)