# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:  Extract data from Clcnl_Cncpt
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Ralph Tucker                  4/2/2008           3036                               Originally Programmed                         devlEDW                         Steph Goddard            04/15/2008
# MAGIC 
# MAGIC Syed Husseini                  10/14/2013       5114                               Newly Programmed                             EnterpriseWhrsDevl        Jag Yelavarthi              2013-12-09

# MAGIC Code SK lookups for Denormalization
# MAGIC Write CARE_OPP_D Data into a Sequential file for Load Job IdsEdwCareOppDLoad.
# MAGIC Read all the Data from IDS CARE_OPP Table;
# MAGIC Add Defaults and Null Handling.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Job Name: IdsEdwCareOppDExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
ExtractRunCycle = get_widget_value('ExtractRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT CARE_OPP_SK, SRC_SYS_CD_SK, CARE_OPP_ID, BCBSKC_CLNCL_PGM_TYP_CD_SK, CRT_RUN_CYC_EXCTN_SK, LAST_UPDT_RUN_CYC_EXCTN_SK, CARE_OPP_PRI_COND_CD_SK, SH_DESC FROM {IDSOwner}.CARE_OPP WHERE LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtractRunCycle}"
df_db2_CARE_OPP_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

extract_query = f"SELECT CD_MPPNG_SK, COALESCE(TRGT_CD,'UNK') AS TRGT_CD, COALESCE(TRGT_CD_NM,'UNK') AS TRGT_CD_NM FROM {IDSOwner}.CD_MPPNG"
df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_cpy_cd_mppng_SrcSysCdLkup = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_cpy_cd_mppng_ClnclPgmTypCd = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_cpy_cd_mppng_CareOppPriCondCd = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_lkp_Codes = (
    df_db2_CARE_OPP_in.alias("lnk_IdsEdwCareOppExtr_InABC")
    .join(
        df_cpy_cd_mppng_SrcSysCdLkup.alias("SrcSysCdLkup"),
        F.col("lnk_IdsEdwCareOppExtr_InABC.SRC_SYS_CD_SK") == F.col("SrcSysCdLkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_cd_mppng_ClnclPgmTypCd.alias("ClnclPgmTypCd"),
        F.col("lnk_IdsEdwCareOppExtr_InABC.BCBSKC_CLNCL_PGM_TYP_CD_SK") == F.col("ClnclPgmTypCd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_cd_mppng_CareOppPriCondCd.alias("CareOppPriCondCd"),
        F.col("lnk_IdsEdwCareOppExtr_InABC.CARE_OPP_PRI_COND_CD_SK") == F.col("CareOppPriCondCd.CD_MPPNG_SK"),
        "left"
    )
    .select(
        F.col("lnk_IdsEdwCareOppExtr_InABC.CARE_OPP_SK").alias("CARE_OPP_SK"),
        F.col("lnk_IdsEdwCareOppExtr_InABC.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("lnk_IdsEdwCareOppExtr_InABC.CARE_OPP_ID").alias("CARE_OPP_ID"),
        F.col("lnk_IdsEdwCareOppExtr_InABC.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_IdsEdwCareOppExtr_InABC.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_IdsEdwCareOppExtr_InABC.BCBSKC_CLNCL_PGM_TYP_CD_SK").alias("BCBSKC_CLNCL_PGM_TYP_CD_SK"),
        F.col("lnk_IdsEdwCareOppExtr_InABC.CARE_OPP_PRI_COND_CD_SK").alias("CARE_OPP_PRI_COND_CD_SK"),
        F.col("lnk_IdsEdwCareOppExtr_InABC.SH_DESC").alias("SH_DESC"),
        F.col("SrcSysCdLkup.TRGT_CD").alias("SRC_SYS_CD"),
        F.col("ClnclPgmTypCd.TRGT_CD").alias("BCBSKC_CLNCL_PGM_TYP_CD"),
        F.col("ClnclPgmTypCd.TRGT_CD_NM").alias("BCBSKC_CLNCL_PGM_TYP_NM"),
        F.col("CareOppPriCondCd.TRGT_CD").alias("CARE_OPP_PRI_COND_CD"),
        F.col("CareOppPriCondCd.TRGT_CD_NM").alias("CARE_OPP_PRI_COND_NM")
    )
)

df_xfrm_BusinessLogic_lnk_Detail = (
    df_lkp_Codes.filter((F.col("CARE_OPP_SK") != 0) & (F.col("CARE_OPP_SK") != 1))
    .select(
        F.col("CARE_OPP_SK").alias("CARE_OPP_SK"),
        F.when(F.col("SRC_SYS_CD").isNull(), 'UNK').otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
        F.col("CARE_OPP_ID").alias("CARE_OPP_ID"),
        F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.when(F.col("BCBSKC_CLNCL_PGM_TYP_CD").isNull(), 'UNK').otherwise(F.col("BCBSKC_CLNCL_PGM_TYP_CD")).alias("BCBSKC_CLNCL_PGM_TYP_CD"),
        F.when(F.col("BCBSKC_CLNCL_PGM_TYP_NM").isNull(), 'UNK').otherwise(F.col("BCBSKC_CLNCL_PGM_TYP_NM")).alias("BCBSKC_CLNCL_PGM_TYP_NM"),
        F.when(F.col("CARE_OPP_PRI_COND_CD").isNull(), 'UNK').otherwise(F.col("CARE_OPP_PRI_COND_CD")).alias("CARE_OPP_PRI_COND_CD"),
        F.when(F.col("CARE_OPP_PRI_COND_NM").isNull(), 'UNK').otherwise(F.col("CARE_OPP_PRI_COND_NM")).alias("CARE_OPP_PRI_COND_NM"),
        F.col("SH_DESC").alias("CARE_OPP_SH_DESC"),
        F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("BCBSKC_CLNCL_PGM_TYP_CD_SK").alias("BCBSKC_CLNCL_PGM_TYP_CD_SK"),
        F.col("CARE_OPP_PRI_COND_CD_SK").alias("CARE_OPP_PRI_COND_CD_SK")
    )
)

df_xfrm_BusinessLogic_lnk_Detail = df_xfrm_BusinessLogic_lnk_Detail.withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
)

schema_na_unk = [
    StructField("CARE_OPP_SK", IntegerType()),
    StructField("SRC_SYS_CD", StringType()),
    StructField("CARE_OPP_ID", StringType()),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType()),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType()),
    StructField("BCBSKC_CLNCL_PGM_TYP_CD", StringType()),
    StructField("BCBSKC_CLNCL_PGM_TYP_NM", StringType()),
    StructField("CARE_OPP_PRI_COND_CD", StringType()),
    StructField("CARE_OPP_PRI_COND_NM", StringType()),
    StructField("CARE_OPP_SH_DESC", StringType()),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType()),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType()),
    StructField("BCBSKC_CLNCL_PGM_TYP_CD_SK", IntegerType()),
    StructField("CARE_OPP_PRI_COND_CD_SK", IntegerType())
]

df_xfrm_BusinessLogic_lnk_Na = spark.createDataFrame(
    [(1, 'NA', 'NA', '1753-01-01', '1753-01-01', 'NA', 'NA', 'NA', 'NA', '', 100, 100, 1, 1)],
    StructType(schema_na_unk)
)
df_xfrm_BusinessLogic_lnk_Na = df_xfrm_BusinessLogic_lnk_Na.withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
)

df_xfrm_BusinessLogic_lnk_Unk = spark.createDataFrame(
    [(0, 'UNK', 'UNK', '1753-01-01', '1753-01-01', 'UNK', 'UNK', 'UNK', 'UNK', '', 100, 100, 0, 0)],
    StructType(schema_na_unk)
)
df_xfrm_BusinessLogic_lnk_Unk = df_xfrm_BusinessLogic_lnk_Unk.withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
)

df_Fnl_Care_Opp_D = df_xfrm_BusinessLogic_lnk_Detail.union(df_xfrm_BusinessLogic_lnk_Na).union(df_xfrm_BusinessLogic_lnk_Unk)

df_Fnl_Care_Opp_D = df_Fnl_Care_Opp_D.select(
    "CARE_OPP_SK",
    "SRC_SYS_CD",
    "CARE_OPP_ID",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "BCBSKC_CLNCL_PGM_TYP_CD",
    "BCBSKC_CLNCL_PGM_TYP_NM",
    "CARE_OPP_PRI_COND_CD",
    "CARE_OPP_PRI_COND_NM",
    "CARE_OPP_SH_DESC",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "BCBSKC_CLNCL_PGM_TYP_CD_SK",
    "CARE_OPP_PRI_COND_CD_SK"
)

write_files(
    df_Fnl_Care_Opp_D,
    f"{adls_path}/load/CARE_OPP_D.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)