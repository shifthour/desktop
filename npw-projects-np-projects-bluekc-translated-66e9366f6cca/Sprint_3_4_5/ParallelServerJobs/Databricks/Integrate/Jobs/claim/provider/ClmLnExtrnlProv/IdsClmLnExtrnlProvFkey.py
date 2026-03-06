# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2024 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the primary key.   Preps the file for the foreign key lookup process that follows.
# MAGIC       
# MAGIC PROCESSING:
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                            Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------      --------------------------------       -------------------------------   ----------------------------       
# MAGIC Revathi Boojireddy  2024-06-08        US 616333            Original Programing                                               IntegrateDev1               Jeyaprasanna             2024-06-27

# MAGIC Read common record format file.
# MAGIC Writing Sequential File to /load
# MAGIC This hash file is written to by all claim foriegn key jobs.
# MAGIC check for foreign keys - write out record to recycle file if errors
# MAGIC Create default rows for UNK and NA
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
Source = get_widget_value("Source","")
InFile = get_widget_value("InFile","")
Logging = get_widget_value("Logging","")
RunID = get_widget_value("RunID","")
SrcSysCdSk = get_widget_value("SrcSysCdSk","")

df_ClmLnExtrnlProvPkey_schema = StructType([
    StructField("CLM_ID", StringType(), False),
    StructField("CLM_LN_SEQ_NO", IntegerType(), False),
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("CLM_LN_EXTRNL_PROV_SK", IntegerType(), False),
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("SVC_PROV_ID", StringType(), False),
    StructField("SVC_PROV_NPI", StringType(), False),
    StructField("SVC_PROV_NM", StringType(), False),
    StructField("SVC_PROV_SPEC_TX", StringType(), False),
    StructField("SVC_PROV_TYP_TX", StringType(), False),
    StructField("CDPP_TAXONOMY_CD", StringType(), False),
    StructField("CDPP_SVC_FAC_ST", StringType(), False),
    StructField("SVC_FCLTY_LOC_ZIP_CD", StringType(), False),
    StructField("SVC_FCLTY_LOC_NO", StringType(), False),
    StructField("SVC_FCLTY_LOC_NO_QLFR_TX", StringType(), False),
    StructField("CDPP_CLASS_PROV", StringType(), False),
    StructField("SVC_PROV_TYP_PPO_AVLBL_IN", StringType(), False),
    StructField("ADTNL_DATA_ELE_TX", StringType(), False),
    StructField("ATCHMT_SRC_ID_DTM", TimestampType(), False),
    StructField("SVC_PROV_INDN_HLTH_SVC_IN", StringType(), True),
    StructField("FLEX_NTWK_PROV_CST_GRPNG_TX", StringType(), True),
    StructField("MKT_ID", StringType(), True),
    StructField("HOST_PROV_ITS_TIER_DSGTN_TX", StringType(), True),
    StructField("SVC_PROV_ZIP_CD", StringType(), False),
    StructField("CLM_LN_SK", IntegerType(), False)
])

df_ClmLnExtrnlProvPkey = (
    spark.read
    .option("header", "false")
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(df_ClmLnExtrnlProvPkey_schema)
    .csv(f"{adls_path}/key/{InFile}")
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"Select TXNMY_CD_SK, TXNMY_CD from {IDSOwner}.TXNMY_CD"
df_IDS_TXNMY_CD = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_hf_txnmy_cd = dedup_sort(df_IDS_TXNMY_CD, ["TXNMY_CD"], [])

df_join = df_ClmLnExtrnlProvPkey.alias("Key").join(
    df_hf_txnmy_cd.alias("Exist_Ids"),
    F.col("Key.CDPP_TAXONOMY_CD") == F.col("Exist_Ids.TXNMY_CD"),
    "left"
)

df_enriched = (
    df_join
    .withColumn(
        "svCdMpngSrcSysCd",
        GetFkeyCodes(F.lit("IDS"), F.col("Key.CLM_LN_EXTRNL_PROV_SK"), F.lit("SOURCE SYSTEM"), F.col("Key.SRC_SYS_CD"), F.lit(Logging))
    )
    .withColumn(
        "svCdppSvcFacSt",
        F.expr("case when length(trim(Key.CDPP_SVC_FAC_ST))=0 then 'UNK' else Key.CDPP_SVC_FAC_ST end")
    )
    .withColumn(
        "svSvcFcltyLocStCdSk",
        GetFkeyCodes(F.lit("FACETS"), F.col("Key.CLM_LN_EXTRNL_PROV_SK"), F.lit("STATE"), F.col("svCdppSvcFacSt"), F.lit(Logging))
    )
    .withColumn(
        "ClmLnSk",
        GetFkeyClmLn(F.col("Key.SRC_SYS_CD"), F.col("Key.CLM_LN_EXTRNL_PROV_SK"), F.col("Key.CLM_ID"), F.col("Key.CLM_LN_SEQ_NO"), F.lit(Logging))
    )
    .withColumn(
        "svSvcProvClsCdSk",
        GetFkeyCodes(F.lit("FACETS"), F.col("Key.CLM_LN_EXTRNL_PROV_SK"), F.lit("PROVIDER CLASSIFICATION"), F.col("Key.CDPP_CLASS_PROV"), F.lit(Logging))
    )
    .withColumn("PassThru", F.col("Key.PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("Key.CLM_LN_EXTRNL_PROV_SK")))
)

df_fkey = (
    df_enriched
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == "Y"))
    .select(
        F.col("Key.CLM_LN_EXTRNL_PROV_SK").alias("CLM_LN_EXTRNL_PROV_SK"),
        F.col("Key.CLM_ID").alias("CLM_ID"),
        F.col("Key.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.col("Key.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("Key.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("ClmLnSk").alias("CLM_LN_SK"),
        F.expr(
            "case when Exist_Ids.TXNMY_CD_SK is null or trim(cast(Exist_Ids.TXNMY_CD_SK as string))='' then 0 else Exist_Ids.TXNMY_CD_SK end"
        ).alias("SVC_PROV_TXNMY_CD_SK"),
        F.expr(
            "case when svSvcFcltyLocStCdSk is null or trim(cast(svSvcFcltyLocStCdSk as string))='' then 0 else svSvcFcltyLocStCdSk end"
        ).alias("SVC_FCLTY_LOC_ST_CD_SK"),
        F.expr(
            "case when svSvcProvClsCdSk is null or trim(cast(svSvcProvClsCdSk as string))='' then 0 else svSvcProvClsCdSk end"
        ).alias("SVC_PROV_CLS_CD_SK"),
        F.col("Key.SVC_PROV_INDN_HLTH_SVC_IN").alias("SVC_PROV_INDN_HLTH_SVC_IN"),
        F.expr(
            "case when Key.SVC_PROV_TYP_PPO_AVLBL_IN is null or length(trim(Key.SVC_PROV_TYP_PPO_AVLBL_IN))=0 then 'N' else Key.SVC_PROV_TYP_PPO_AVLBL_IN end"
        ).alias("SVC_PROV_TYP_PPO_AVLBL_IN"),
        F.col("Key.ATCHMT_SRC_ID_DTM").alias("ATCHMT_SRC_ID_DTM"),
        F.expr(
            "case when Key.SVC_PROV_ID is null or length(trim(Key.SVC_PROV_ID))=0 then '' else Key.SVC_PROV_ID end"
        ).alias("SVC_PROV_ID"),
        F.expr(
            "case when Key.ADTNL_DATA_ELE_TX is null or length(trim(Key.ADTNL_DATA_ELE_TX))=0 then '' else Key.ADTNL_DATA_ELE_TX end"
        ).alias("ADTNL_DATA_ELE_TX"),
        F.col("Key.FLEX_NTWK_PROV_CST_GRPNG_TX").alias("FLEX_NTWK_PROV_CST_GRPNG_TX"),
        F.col("Key.HOST_PROV_ITS_TIER_DSGTN_TX").alias("HOST_PROV_ITS_TIER_DSGTN_TX"),
        F.col("Key.MKT_ID").alias("MKT_ID"),
        F.expr(
            "case when Key.SVC_FCLTY_LOC_NO is null or length(trim(Key.SVC_FCLTY_LOC_NO))=0 then '0' else Key.SVC_FCLTY_LOC_NO end"
        ).alias("SVC_FCLTY_LOC_NO"),
        F.expr(
            "case when Key.SVC_FCLTY_LOC_NO_QLFR_TX is null or length(trim(Key.SVC_FCLTY_LOC_NO_QLFR_TX))=0 then '' else Key.SVC_FCLTY_LOC_NO_QLFR_TX end"
        ).alias("SVC_FCLTY_LOC_NO_QLFR_TX"),
        F.expr(
            "case when Key.SVC_FCLTY_LOC_ZIP_CD is null or length(trim(Key.SVC_FCLTY_LOC_ZIP_CD))=0 then '' else Key.SVC_FCLTY_LOC_ZIP_CD end"
        ).alias("SVC_FCLTY_LOC_ZIP_CD"),
        F.expr(
            "case when Key.SVC_PROV_NPI is null or length(trim(Key.SVC_PROV_NPI))=0 then '' else Key.SVC_PROV_NPI end"
        ).alias("SVC_PROV_NPI"),
        F.expr(
            "case when Key.SVC_PROV_NM is null or length(trim(Key.SVC_PROV_NM))=0 then '' else Key.SVC_PROV_NM end"
        ).alias("SVC_PROV_NM"),
        F.expr(
            "case when Key.SVC_PROV_SPEC_TX is null or length(trim(Key.SVC_PROV_SPEC_TX))=0 then '' else Key.SVC_PROV_SPEC_TX end"
        ).alias("SVC_PROV_SPEC_TX"),
        F.expr(
            "case when Key.SVC_PROV_TYP_TX is null or length(trim(Key.SVC_PROV_TYP_TX))=0 then '' else Key.SVC_PROV_TYP_TX end"
        ).alias("SVC_PROV_TYP_TX"),
        F.expr(
            "case when Key.SVC_PROV_ZIP_CD is null or length(trim(Key.SVC_PROV_ZIP_CD))=0 then '' else Key.SVC_PROV_ZIP_CD end"
        ).alias("SVC_PROV_ZIP_CD")
    )
)

df_recycle = (
    df_enriched
    .filter(F.col("ErrCount") > 0)
    .select(
        F.expr("GetRecycleKey(Key.CLM_LN_EXTRNL_PROV_SK)").alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("Key.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("Key.DISCARD_IN").alias("DISCARD_IN"),
        F.col("Key.PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("Key.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        (F.col("Key.RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
        F.col("Key.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Key.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("Key.CLM_LN_EXTRNL_PROV_SK").alias("CLM_LN_EXTRNL_PROV_SK"),
        F.col("Key.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("Key.CLM_ID").alias("CLM_ID"),
        F.col("Key.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("Key.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("Key.CLM_LN_SK").alias("CLM_LN_SK"),
        F.col("Key.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("Key.SVC_PROV_ID").alias("SVC_PROV_ID"),
        F.col("Key.SVC_PROV_NPI").alias("SVC_PROV_NPI"),
        F.col("Key.SVC_PROV_NM").alias("SVC_PROV_NM"),
        F.col("Key.SVC_PROV_SPEC_TX").alias("SVC_PROV_SPEC_TX"),
        F.col("Key.SVC_PROV_TYP_TX").alias("SVC_PROV_TYP_TX"),
        F.col("Key.CDPP_TAXONOMY_CD").alias("CDPP_TAXONOMY_CD"),
        F.col("Key.CDPP_SVC_FAC_ST").alias("CDPP_SVC_FAC_ST"),
        F.col("Key.SVC_FCLTY_LOC_ZIP_CD").alias("SVC_FCLTY_LOC_ZIP_CD"),
        F.col("Key.SVC_FCLTY_LOC_NO").alias("SVC_FCLTY_LOC_NO"),
        F.col("Key.SVC_FCLTY_LOC_NO_QLFR_TX").alias("SVC_FCLTY_LOC_NO_QLFR_TX"),
        F.col("Key.CDPP_CLASS_PROV").alias("CDPP_CLASS_PROV"),
        F.col("Key.SVC_PROV_TYP_PPO_AVLBL_IN").alias("SVC_PROV_TYP_PPO_AVLBL_IN"),
        F.col("Key.ADTNL_DATA_ELE_TX").alias("ADTNL_DATA_ELE_TX"),
        F.col("Key.ATCHMT_SRC_ID_DTM").alias("ATCHMT_SRC_ID_DTM"),
        F.col("Key.SVC_PROV_INDN_HLTH_SVC_IN").alias("SVC_PROV_INDN_HLTH_SVC_IN"),
        F.col("Key.FLEX_NTWK_PROV_CST_GRPNG_TX").alias("FLEX_NTWK_PROV_CST_GRPNG_TX"),
        F.col("Key.MKT_ID").alias("MKT_ID"),
        F.col("Key.HOST_PROV_ITS_TIER_DSGTN_TX").alias("HOST_PROV_ITS_TIER_DSGTN_TX"),
        F.col("Key.SVC_PROV_ZIP_CD").alias("SVC_PROV_ZIP_CD")
    )
)

df_recycle_clms = (
    df_enriched
    .filter(F.col("ErrCount") > 0)
    .select(
        F.col("Key.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Key.CLM_ID").alias("CLM_ID"),
        F.col("Key.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO")
    )
)

df_defaultUNK = spark.range(1).select(
    F.lit(0).alias("CLM_LN_EXTRNL_PROV_SK"),
    F.lit("UNK").alias("CLM_ID"),
    F.lit(0).alias("CLM_LN_SEQ_NO"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CLM_LN_SK"),
    F.lit(0).alias("SVC_PROV_TXNMY_CD_SK"),
    F.lit(0).alias("SVC_FCLTY_LOC_ST_CD_SK"),
    F.lit(0).alias("SVC_PROV_CLS_CD_SK"),
    F.lit("N").alias("SVC_PROV_INDN_HLTH_SVC_IN"),
    F.lit("N").alias("SVC_PROV_TYP_PPO_AVLBL_IN"),
    F.lit("1753-01-01 00:00:00.000000").alias("ATCHMT_SRC_ID_DTM"),
    F.lit("UNK").alias("SVC_PROV_ID"),
    F.lit("UNK").alias("ADTNL_DATA_ELE_TX"),
    F.lit("UNK").alias("FLEX_NTWK_PROV_CST_GRPNG_TX"),
    F.lit("UNK").alias("HOST_PROV_ITS_TIER_DSGTN_TX"),
    F.lit("UNK").alias("MKT_ID"),
    F.lit("UNK").alias("SVC_FCLTY_LOC_NO"),
    F.lit("UNK").alias("SVC_FCLTY_LOC_NO_QLFR_TX"),
    F.lit("UNK").alias("SVC_FCLTY_LOC_ZIP_CD"),
    F.lit("UNK").alias("SVC_PROV_NPI"),
    F.lit("UNK").alias("SVC_PROV_NM"),
    F.lit("UNK").alias("SVC_PROV_SPEC_TX"),
    F.lit("UNK").alias("SVC_PROV_TYP_TX"),
    F.lit("UNK").alias("SVC_PROV_ZIP_CD")
)

df_defaultNA = spark.range(1).select(
    F.lit(1).alias("CLM_LN_EXTRNL_PROV_SK"),
    F.lit("NA").alias("CLM_ID"),
    F.lit(1).alias("CLM_LN_SEQ_NO"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("CLM_LN_SK"),
    F.lit(1).alias("SVC_PROV_TXNMY_CD_SK"),
    F.lit(1).alias("SVC_FCLTY_LOC_ST_CD_SK"),
    F.lit(1).alias("SVC_PROV_CLS_CD_SK"),
    F.lit("N").alias("SVC_PROV_INDN_HLTH_SVC_IN"),
    F.lit("N").alias("SVC_PROV_TYP_PPO_AVLBL_IN"),
    F.lit("1753-01-01 00:00:00.000000").alias("ATCHMT_SRC_ID_DTM"),
    F.lit("NA").alias("SVC_PROV_ID"),
    F.lit("NA").alias("ADTNL_DATA_ELE_TX"),
    F.lit("NA").alias("FLEX_NTWK_PROV_CST_GRPNG_TX"),
    F.lit("NA").alias("HOST_PROV_ITS_TIER_DSGTN_TX"),
    F.lit("NA").alias("MKT_ID"),
    F.lit("NA").alias("SVC_FCLTY_LOC_NO"),
    F.lit("NA").alias("SVC_FCLTY_LOC_NO_QLFR_TX"),
    F.lit("NA").alias("SVC_FCLTY_LOC_ZIP_CD"),
    F.lit("NA").alias("SVC_PROV_NPI"),
    F.lit("NA").alias("SVC_PROV_NM"),
    F.lit("NA").alias("SVC_PROV_SPEC_TX"),
    F.lit("NA").alias("SVC_PROV_TYP_TX"),
    F.lit("NA").alias("SVC_PROV_ZIP_CD")
)

common_col_order = [
    "CLM_LN_EXTRNL_PROV_SK","CLM_ID","CLM_LN_SEQ_NO","SRC_SYS_CD_SK","CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_LN_SK","SVC_PROV_TXNMY_CD_SK","SVC_FCLTY_LOC_ST_CD_SK","SVC_PROV_CLS_CD_SK","SVC_PROV_INDN_HLTH_SVC_IN",
    "SVC_PROV_TYP_PPO_AVLBL_IN","ATCHMT_SRC_ID_DTM","SVC_PROV_ID","ADTNL_DATA_ELE_TX","FLEX_NTWK_PROV_CST_GRPNG_TX",
    "HOST_PROV_ITS_TIER_DSGTN_TX","MKT_ID","SVC_FCLTY_LOC_NO","SVC_FCLTY_LOC_NO_QLFR_TX","SVC_FCLTY_LOC_ZIP_CD",
    "SVC_PROV_NPI","SVC_PROV_NM","SVC_PROV_SPEC_TX","SVC_PROV_TYP_TX","SVC_PROV_ZIP_CD"
]

df_collector = (
    df_fkey.select(common_col_order)
    .unionByName(df_defaultUNK.select(common_col_order))
    .unionByName(df_defaultNA.select(common_col_order))
)

df_collector_final = (
    df_collector
    .withColumn("SVC_PROV_INDN_HLTH_SVC_IN", F.rpad(F.col("SVC_PROV_INDN_HLTH_SVC_IN"), 1, " "))
    .withColumn("SVC_PROV_TYP_PPO_AVLBL_IN", F.rpad(F.col("SVC_PROV_TYP_PPO_AVLBL_IN"), 1, " "))
)

write_files(
    df_recycle,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

write_files(
    df_recycle_clms,
    "hf_claim_recycle_keys.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

write_files(
    df_collector_final.select(common_col_order),
    f"{adls_path}/load/CLM_LN_EXTRNL_PROV.{Source}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)