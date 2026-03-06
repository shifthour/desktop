# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Modifications:                        
# MAGIC                                               Project/                                                                                                 Code                  Date
# MAGIC Developer        Date              Altiris #           Change Description                                                        Reviewer            Reviewed
# MAGIC ----------------------  -------------------   -------------------   ------------------------------------------------------------------------------------   ----------------------     -------------------   
# MAGIC Hugh Sisson    6/26/2007                           Original program                                                              Steph Goddard   8/30/07
# MAGIC 
# MAGIC Rama Kamjula   11/14/2013     5114           Rewritten to parallel from Server version                        Jag Yelavarthi      2014-01-17

# MAGIC JobName: IdsEdwAplLvlRvwrDExtr
# MAGIC 
# MAGIC Job creates loadfile for APL_LVL_RVWR_D  in EDW
# MAGIC IDS Extract from Apl_LVL_RVWR
# MAGIC IDS extract from APL_USER
# MAGIC Null Handling and Business Logic
# MAGIC Creates load file for  APL_LVL_RVWR_D
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
ExtractRunCycle = get_widget_value('ExtractRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# ------------------------------------------------
# db2_AplLvlRvwr
# ------------------------------------------------
query_db2_AplLvlRvwr = (
    "SELECT \n"
    "APL_LVL_RVWR_SK,\n"
    "COALESCE(CD.TRGT_CD, 'UNK') SRC_SYS_CD,\n"
    "APL_ID,\n"
    "APL_LVL_SEQ_NO,\n"
    "EFF_DT_SK,\n"
    "RVWR.CRT_RUN_CYC_EXCTN_SK,\n"
    "RVWR.LAST_UPDT_RUN_CYC_EXCTN_SK,\n"
    "APL_LVL_SK,\n"
    "APL_RVWR_SK,\n"
    "LAST_UPDT_USER_SK,\n"
    "APL_REP_LVL_RVWR_TERM_CD_SK,\n"
    "LAST_UPDT_DTM,\n"
    "TERM_DT_SK,\n"
    "APL_RVWR_ID,\n"
    "APL_RVWR_NM\n"
    "FROM " + IDSOwner + ".APL_LVL_RVWR RVWR\n"
    "LEFT JOIN  " + IDSOwner + ".CD_MPPNG CD   ON  CD.CD_MPPNG_SK  =  RVWR.SRC_SYS_CD_SK\n"
    "WHERE RVWR.LAST_UPDT_RUN_CYC_EXCTN_SK >= " + ExtractRunCycle
    + ";"
)
df_db2_AplLvlRvwr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_db2_AplLvlRvwr)
    .load()
)

# ------------------------------------------------
# db2_CD_MPPNG
# ------------------------------------------------
query_db2_CD_MPPNG = (
    "SELECT \n"
    "CD_MPPNG_SK,\n"
    "TRGT_CD,\n"
    "TRGT_CD_NM\n"
    "FROM  " + IDSOwner + ".CD_MPPNG"
)
df_db2_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_db2_CD_MPPNG)
    .load()
)

# ------------------------------------------------
# db2_APP_USER
# ------------------------------------------------
query_db2_APP_USER = (
    "SELECT \n"
    "USER_SK,\n"
    "USER_ID\n"
    "FROM " + IDSOwner + ".APP_USER APP;"
)
df_db2_APP_USER = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_db2_APP_USER)
    .load()
)

# ------------------------------------------------
# lkp_Codes (PxLookup)
# ------------------------------------------------
df_lkp_primary = df_db2_AplLvlRvwr.alias("lnk_AplLvlRvwrExtract")
df_lkp_join_1 = df_lkp_primary.join(
    df_db2_APP_USER.alias("lnk_AppUser"),
    (F.col("lnk_AplLvlRvwrExtract.LAST_UPDT_USER_SK") == F.col("lnk_AppUser.USER_SK")),
    "left"
)
df_lkp_join_2 = df_lkp_join_1.join(
    df_db2_CD_MPPNG.alias("lnk_AplLvlLocCd"),
    (F.col("lnk_AplLvlRvwrExtract.APL_REP_LVL_RVWR_TERM_CD_SK") == F.col("lnk_AplLvlLocCd.CD_MPPNG_SK")),
    "left"
)
df_lkp_Codes = df_lkp_join_2.select(
    F.col("lnk_AplLvlRvwrExtract.APL_LVL_RVWR_SK").alias("APL_LVL_RVWR_SK"),
    F.col("lnk_AplLvlRvwrExtract.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_AplLvlRvwrExtract.APL_ID").alias("APL_ID"),
    F.col("lnk_AplLvlRvwrExtract.APL_LVL_SEQ_NO").alias("APL_LVL_SEQ_NO"),
    F.col("lnk_AplLvlRvwrExtract.APL_RVWR_ID").alias("APL_RVWR_ID"),
    F.col("lnk_AplLvlRvwrExtract.EFF_DT_SK").alias("EFF_DT_SK"),
    F.col("lnk_AplLvlRvwrExtract.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_AplLvlRvwrExtract.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_AplLvlRvwrExtract.APL_LVL_SK").alias("APL_LVL_SK"),
    F.col("lnk_AplLvlRvwrExtract.APL_RVWR_SK").alias("APL_RVWR_SK"),
    F.col("lnk_AplLvlRvwrExtract.LAST_UPDT_USER_SK").alias("LAST_UPDT_USER_SK"),
    F.col("lnk_AplLvlRvwrExtract.APL_REP_LVL_RVWR_TERM_CD_SK").alias("APL_REP_LVL_RVWR_TERM_CD_SK"),
    F.col("lnk_AplLvlRvwrExtract.LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    F.col("lnk_AplLvlRvwrExtract.TERM_DT_SK").alias("TERM_DT_SK"),
    F.col("lnk_AplLvlRvwrExtract.APL_RVWR_NM").alias("APL_RVWR_NM"),
    F.col("lnk_AppUser.USER_ID").alias("USER_ID"),
    F.col("lnk_AplLvlLocCd.TRGT_CD").alias("TRGT_CD"),
    F.col("lnk_AplLvlLocCd.TRGT_CD_NM").alias("TRGT_CD_NM")
)

# ------------------------------------------------
# xfm_BusinessLogic (CTransformerStage)
# ------------------------------------------------
# Output link lnk_AplLvlRvwrD_out
df_xfm_BusinessLogic_1 = df_lkp_Codes.filter(
    (F.col("APL_LVL_RVWR_SK") != 0) & (F.col("APL_LVL_RVWR_SK") != 1)
).select(
    F.col("APL_LVL_RVWR_SK").alias("APL_LVL_RVWR_SK"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("APL_ID").alias("APL_ID"),
    F.col("APL_LVL_SEQ_NO").alias("APL_LVL_SEQ_NO"),
    F.col("APL_RVWR_ID").alias("APL_RVWR_ID"),
    F.col("EFF_DT_SK").alias("APL_LVL_RVWR_EFF_DT_SK"),
    F.lit(CurrRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(CurrRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("APL_LVL_SK").alias("APL_LVL_SK"),
    F.col("APL_RVWR_SK").alias("APL_RVWR_SK"),
    F.col("LAST_UPDT_USER_SK").alias("LAST_UPDT_USER_SK"),
    F.col("TERM_DT_SK").alias("APL_LVL_RVWR_TERM_DT_SK"),
    F.when(
        (F.col("TRGT_CD").isNull()) | (F.trim(F.col("TRGT_CD")) == ''),
        'NA'
    ).otherwise(F.col("TRGT_CD")).alias("APL_LVL_RVWR_TERM_RSN_CD"),
    F.when(
        (F.col("TRGT_CD_NM").isNull()) | (F.trim(F.col("TRGT_CD_NM")) == ''),
        'NA'
    ).otherwise(F.col("TRGT_CD_NM")).alias("APL_LVL_RVWR_TERM_RSN_NM"),
    F.col("APL_RVWR_NM").alias("APL_RVWR_NM"),
    F.substring(F.col("LAST_UPDT_DTM"), 1, 10).alias("LAST_UPDT_DT_SK"),
    F.when(
        (F.col("USER_ID").isNull()) | (F.trim(F.col("USER_ID")) == ''),
        'NA'
    ).otherwise(F.col("USER_ID")).alias("LAST_UPDT_USER_ID"),
    F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("APL_REP_LVL_RVWR_TERM_CD_SK").alias("APL_LVL_RVWR_TERM_RSN_CD_SK")
)

# Output link lnk_NA
df_xfm_BusinessLogic_NA = spark.createDataFrame(
    [
        (
            1,           # APL_LVL_RVWR_SK
            "NA",        # SRC_SYS_CD
            "NA",        # APL_ID
            0,           # APL_LVL_SEQ_NO
            "NA",        # APL_RVWR_ID
            "1753-01-01",# APL_LVL_RVWR_EFF_DT_SK
            "1753-01-01",# CRT_RUN_CYC_EXCTN_DT_SK
            "1753-01-01",# LAST_UPDT_RUN_CYC_EXCTN_DT_SK
            1,           # APL_LVL_SK
            1,           # APL_RVWR_SK
            1,           # LAST_UPDT_USER_SK
            "1753-01-01",# APL_LVL_RVWR_TERM_DT_SK
            "NA",        # APL_LVL_RVWR_TERM_RSN_CD
            "NA",        # APL_LVL_RVWR_TERM_RSN_NM
            None,        # APL_RVWR_NM
            "1753-01-01",# LAST_UPDT_DT_SK
            "NA",        # LAST_UPDT_USER_ID
            100,         # CRT_RUN_CYC_EXCTN_SK
            100,         # LAST_UPDT_RUN_CYC_EXCTN_SK
            1            # APL_LVL_RVWR_TERM_RSN_CD_SK
        )
    ],
    StructType([
        StructField("APL_LVL_RVWR_SK", IntegerType(), True),
        StructField("SRC_SYS_CD", StringType(), True),
        StructField("APL_ID", StringType(), True),
        StructField("APL_LVL_SEQ_NO", IntegerType(), True),
        StructField("APL_RVWR_ID", StringType(), True),
        StructField("APL_LVL_RVWR_EFF_DT_SK", StringType(), True),
        StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
        StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
        StructField("APL_LVL_SK", IntegerType(), True),
        StructField("APL_RVWR_SK", IntegerType(), True),
        StructField("LAST_UPDT_USER_SK", IntegerType(), True),
        StructField("APL_LVL_RVWR_TERM_DT_SK", StringType(), True),
        StructField("APL_LVL_RVWR_TERM_RSN_CD", StringType(), True),
        StructField("APL_LVL_RVWR_TERM_RSN_NM", StringType(), True),
        StructField("APL_RVWR_NM", StringType(), True),
        StructField("LAST_UPDT_DT_SK", StringType(), True),
        StructField("LAST_UPDT_USER_ID", StringType(), True),
        StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
        StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
        StructField("APL_LVL_RVWR_TERM_RSN_CD_SK", IntegerType(), True)
    ])
)

# Output link lnk_UNK
df_xfm_BusinessLogic_UNK = spark.createDataFrame(
    [
        (
            0,            # APL_LVL_RVWR_SK
            "UNK",        # SRC_SYS_CD
            "UNK",        # APL_ID
            0,            # APL_LVL_SEQ_NO
            "UNK",        # APL_RVWR_ID
            "1753-01-01", # APL_LVL_RVWR_EFF_DT_SK
            "1753-01-01", # CRT_RUN_CYC_EXCTN_DT_SK
            "1753-01-01", # LAST_UPDT_RUN_CYC_EXCTN_DT_SK
            0,            # APL_LVL_SK
            0,            # APL_RVWR_SK
            0,            # LAST_UPDT_USER_SK
            "1753-01-01", # APL_LVL_RVWR_TERM_DT_SK
            "UNK",        # APL_LVL_RVWR_TERM_RSN_CD
            "UNK",        # APL_LVL_RVWR_TERM_RSN_NM
            None,         # APL_RVWR_NM
            "1753-01-01", # LAST_UPDT_DT_SK
            "UNK",        # LAST_UPDT_USER_ID
            100,          # CRT_RUN_CYC_EXCTN_SK
            100,          # LAST_UPDT_RUN_CYC_EXCTN_SK
            0             # APL_LVL_RVWR_TERM_RSN_CD_SK
        )
    ],
    StructType([
        StructField("APL_LVL_RVWR_SK", IntegerType(), True),
        StructField("SRC_SYS_CD", StringType(), True),
        StructField("APL_ID", StringType(), True),
        StructField("APL_LVL_SEQ_NO", IntegerType(), True),
        StructField("APL_RVWR_ID", StringType(), True),
        StructField("APL_LVL_RVWR_EFF_DT_SK", StringType(), True),
        StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
        StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
        StructField("APL_LVL_SK", IntegerType(), True),
        StructField("APL_RVWR_SK", IntegerType(), True),
        StructField("LAST_UPDT_USER_SK", IntegerType(), True),
        StructField("APL_LVL_RVWR_TERM_DT_SK", StringType(), True),
        StructField("APL_LVL_RVWR_TERM_RSN_CD", StringType(), True),
        StructField("APL_LVL_RVWR_TERM_RSN_NM", StringType(), True),
        StructField("APL_RVWR_NM", StringType(), True),
        StructField("LAST_UPDT_DT_SK", StringType(), True),
        StructField("LAST_UPDT_USER_ID", StringType(), True),
        StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
        StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
        StructField("APL_LVL_RVWR_TERM_RSN_CD_SK", IntegerType(), True)
    ])
)

# ------------------------------------------------
# fnl_Combine (PxFunnel)
# ------------------------------------------------
df_fnl_Combine = (
    df_xfm_BusinessLogic_1
    .unionByName(df_xfm_BusinessLogic_NA)
    .unionByName(df_xfm_BusinessLogic_UNK)
)

# ------------------------------------------------
# seq_APL_LVL_RVWR_D (PxSequentialFile)
# ------------------------------------------------
# Final column order with rpad for char(10)
df_final = df_fnl_Combine.select(
    F.col("APL_LVL_RVWR_SK"),
    F.col("SRC_SYS_CD"),
    F.col("APL_ID"),
    F.col("APL_LVL_SEQ_NO"),
    F.col("APL_RVWR_ID"),
    F.rpad(F.col("APL_LVL_RVWR_EFF_DT_SK"), 10, " ").alias("APL_LVL_RVWR_EFF_DT_SK"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("APL_LVL_SK"),
    F.col("APL_RVWR_SK"),
    F.col("LAST_UPDT_USER_SK"),
    F.rpad(F.col("APL_LVL_RVWR_TERM_DT_SK"), 10, " ").alias("APL_LVL_RVWR_TERM_DT_SK"),
    F.col("APL_LVL_RVWR_TERM_RSN_CD"),
    F.col("APL_LVL_RVWR_TERM_RSN_NM"),
    F.col("APL_RVWR_NM"),
    F.rpad(F.col("LAST_UPDT_DT_SK"), 10, " ").alias("LAST_UPDT_DT_SK"),
    F.col("LAST_UPDT_USER_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("APL_LVL_RVWR_TERM_RSN_CD_SK")
)

write_files(
    df_final,
    f"{adls_path}/load/APL_LVL_RVWR_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)