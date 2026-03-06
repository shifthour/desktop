# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:  Extracting data from APL_LVL_LTR_PRT
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                2/08/2007           Appeals/3028              Originally Programmed                        devlEDW10                   Steph Goddard             09/15/2007
# MAGIC 
# MAGIC 
# MAGIC Srikanth Mettpalli            06/3/2013          5114                    Original Programming                                  EnterpriseWrhsDevl        Jag Yelavarthi              2014-01-17
# MAGIC                                                                                       (Server to Parallel Conversion)

# MAGIC Code SK lookups for Denormalization
# MAGIC Write APL_LVL_LTR_PRT_D Data into a Sequential file for Load Job IdsEdwAplLvlLtrPrtDLoad.
# MAGIC IDS APL_LVL_LTR_PRT extract from IDS
# MAGIC Add Defaults and Null Handling.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Job Name: IdsEdwAplLvlLtrPrtDExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, monotonically_increasing_id
from pyspark.sql.functions import rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
ExtractRunCycle = get_widget_value('ExtractRunCycle','')
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

extract_query_db2_CD_MPPNG_in = f"SELECT CD_MPPNG_SK,COALESCE(TRGT_CD,'UNK') TRGT_CD,COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM from {IDSOwner}.CD_MPPNG"
df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CD_MPPNG_in)
    .load()
)

extract_query_db2_APL_LVL_LTR_PRT_in = f"""SELECT 
APL_LVL_LTR_PRT.APL_LVL_LTR_PRT_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
APL_LVL_LTR_PRT.APL_ID,
APL_LVL_LTR_PRT.SEQ_NO,
APL_LVL_LTR_PRT.APL_LVL_LTR_STYLE_CD_SK,
APL_LVL_LTR_PRT.LTR_SEQ_NO,
APL_LVL_LTR_PRT.LTR_DEST_ID,
APL_LVL_LTR_PRT.PRT_SEQ_NO,
APL_LVL_LTR_PRT.APL_LVL_LTR_SK,
APL_LVL_LTR_PRT.RQST_DT_SK,
APL_LVL_LTR_PRT.RQST_USER_SK,
APL_LVL_LTR_PRT.SUBMT_DT_SK,
APL_LVL_LTR_PRT.PRT_DT_SK,
APL_LVL_LTR_PRT.LAST_UPDT_DTM,
APL_LVL_LTR_PRT.LAST_UPDT_USER_SK,
APL_LVL_LTR_PRT.EXPL_TX,
APL_LVL_LTR_PRT.PRT_DESC
FROM {IDSOwner}.APL_LVL_LTR_PRT APL_LVL_LTR_PRT
LEFT JOIN {IDSOwner}.CD_MPPNG CD
ON APL_LVL_LTR_PRT.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
WHERE APL_LVL_LTR_PRT.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtractRunCycle}"""
df_db2_APL_LVL_LTR_PRT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_APL_LVL_LTR_PRT_in)
    .load()
)

extract_query_db2_APP_USER_in = f"SELECT APP_USER.USER_SK, APP_USER.USER_ID FROM {IDSOwner}.APP_USER APP_USER"
df_db2_APP_USER_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_APP_USER_in)
    .load()
)

df_Ref_Lst_updt_sk_Lkup = df_db2_APP_USER_in.select(
    col("USER_SK").alias("USER_SK"),
    col("USER_ID").alias("USER_ID")
)
df_Ref_rqst_dt_sk_Lkup = df_db2_APP_USER_in.select(
    col("USER_SK").alias("USER_SK"),
    col("USER_ID").alias("USER_ID")
)

inABC = df_db2_APL_LVL_LTR_PRT_in.alias("inABC")
aplStyle = df_db2_CD_MPPNG_in.alias("aplStyle")
rqst = df_Ref_rqst_dt_sk_Lkup.alias("rqst")
lst = df_Ref_Lst_updt_sk_Lkup.alias("lst")
df_lkp_Codes_pre = (
    inABC.join(
        aplStyle,
        inABC["APL_LVL_LTR_STYLE_CD_SK"] == aplStyle["CD_MPPNG_SK"],
        "left"
    )
    .join(
        rqst,
        inABC["RQST_USER_SK"] == rqst["USER_SK"],
        "left"
    )
    .join(
        lst,
        inABC["LAST_UPDT_USER_SK"] == lst["USER_SK"],
        "left"
    )
)

df_lkp_Codes = df_lkp_Codes_pre.select(
    col("inABC.APL_LVL_LTR_PRT_SK").alias("APL_LVL_LTR_PRT_SK"),
    col("inABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("inABC.APL_ID").alias("APL_ID"),
    col("inABC.SEQ_NO").alias("APL_LVL_SEQ_NO"),
    col("aplStyle.TRGT_CD").alias("APL_LVL_LTR_STYLE_CD"),
    col("inABC.LTR_SEQ_NO").alias("APL_LVL_LTR_SEQ_NO"),
    col("inABC.LTR_DEST_ID").alias("APL_LVL_LTR_DEST_ID"),
    col("inABC.PRT_SEQ_NO").alias("APL_LVL_LTR_PRT_SEQ_NO"),
    col("inABC.RQST_USER_SK").alias("PRT_RQST_USER_SK"),
    col("inABC.LAST_UPDT_USER_SK").alias("LAST_UPDT_USER_SK"),
    col("inABC.APL_LVL_LTR_SK").alias("APL_LVL_LTR_SK"),
    col("inABC.EXPL_TX").alias("APL_LVL_LTR_EXPL_TX"),
    col("inABC.PRT_DT_SK").alias("APL_LVL_LTR_PRT_DT_SK"),
    col("inABC.PRT_DESC").alias("APL_LVL_LTR_PRT_DESC"),
    col("inABC.RQST_DT_SK").alias("APL_LVL_LTR_PRT_RQST_DT_SK"),
    col("inABC.SUBMT_DT_SK").alias("APL_LVL_LTR_PRT_SUBMT_DT_SK"),
    col("aplStyle.TRGT_CD_NM").alias("APL_LVL_LTR_STYLE_NM"),
    col("inABC.LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    col("lst.USER_ID").alias("LAST_UPDT_USER_ID"),
    col("rqst.USER_ID").alias("PRT_RQST_USER_ID"),
    col("inABC.APL_LVL_LTR_STYLE_CD_SK").alias("APL_LVL_LTR_STYLE_CD_SK")
)

df_xfrm_BusinessLogic_main = df_lkp_Codes.where(
    (col("APL_LVL_LTR_PRT_SK") != 1) & (col("APL_LVL_LTR_PRT_SK") != 0)
).select(
    col("APL_LVL_LTR_PRT_SK").alias("APL_LVL_LTR_PRT_SK"),
    when(col("SRC_SYS_CD") == "", "NA").otherwise(col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    col("APL_ID").alias("APL_ID"),
    col("APL_LVL_SEQ_NO").alias("APL_LVL_SEQ_NO"),
    when(col("APL_LVL_LTR_STYLE_CD").isNull(), "NA").otherwise(col("APL_LVL_LTR_STYLE_CD")).alias("APL_LVL_LTR_STYLE_CD"),
    col("APL_LVL_LTR_SEQ_NO").alias("APL_LVL_LTR_SEQ_NO"),
    col("APL_LVL_LTR_DEST_ID").alias("APL_LVL_LTR_DEST_ID"),
    col("APL_LVL_LTR_PRT_SEQ_NO").alias("APL_LVL_LTR_PRT_SEQ_NO"),
    lit(CurrRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    lit(CurrRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("PRT_RQST_USER_SK").alias("PRT_RQST_USER_SK"),
    col("LAST_UPDT_USER_SK").alias("LAST_UPDT_USER_SK"),
    col("APL_LVL_LTR_SK").alias("APL_LVL_LTR_SK"),
    col("APL_LVL_LTR_EXPL_TX").alias("APL_LVL_LTR_EXPL_TX"),
    col("APL_LVL_LTR_PRT_DT_SK").alias("APL_LVL_LTR_PRT_DT_SK"),
    col("APL_LVL_LTR_PRT_DESC").alias("APL_LVL_LTR_PRT_DESC"),
    col("APL_LVL_LTR_PRT_RQST_DT_SK").alias("APL_LVL_LTR_PRT_RQST_DT_SK"),
    col("APL_LVL_LTR_PRT_SUBMT_DT_SK").alias("APL_LVL_LTR_PRT_SUBMT_DT_SK"),
    when(col("APL_LVL_LTR_STYLE_NM").isNull(), "NA").otherwise(col("APL_LVL_LTR_STYLE_NM")).alias("APL_LVL_LTR_STYLE_NM"),
    TimestampToDate(col("LAST_UPDT_DTM")).alias("LAST_UPDT_DT_SK"),
    when(trim(col("LAST_UPDT_USER_ID")) == "", "NA").otherwise(col("LAST_UPDT_USER_ID")).alias("LAST_UPDT_USER_ID"),
    when(trim(col("PRT_RQST_USER_ID")) == "", "NA").otherwise(col("PRT_RQST_USER_ID")).alias("PRT_RQST_USER_ID"),
    lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("APL_LVL_LTR_STYLE_CD_SK").alias("APL_LVL_LTR_STYLE_CD_SK")
)

df_temp_for_NA = df_lkp_Codes.limit(1)
df_xfrm_BusinessLogic_lnk_NA_out = df_temp_for_NA.select(
    lit(1).alias("APL_LVL_LTR_PRT_SK"),
    lit("NA").alias("SRC_SYS_CD"),
    lit("NA").alias("APL_ID"),
    lit(0).alias("APL_LVL_SEQ_NO"),
    lit("NA").alias("APL_LVL_LTR_STYLE_CD"),
    lit(0).alias("APL_LVL_LTR_SEQ_NO"),
    lit("NA").alias("APL_LVL_LTR_DEST_ID"),
    lit(0).alias("APL_LVL_LTR_PRT_SEQ_NO"),
    lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    lit(1).alias("PRT_RQST_USER_SK"),
    lit(1).alias("LAST_UPDT_USER_SK"),
    lit(1).alias("APL_LVL_LTR_SK"),
    lit(None).alias("APL_LVL_LTR_EXPL_TX"),
    lit("1753-01-01").alias("APL_LVL_LTR_PRT_DT_SK"),
    lit(None).alias("APL_LVL_LTR_PRT_DESC"),
    lit("1753-01-01").alias("APL_LVL_LTR_PRT_RQST_DT_SK"),
    lit("1753-01-01").alias("APL_LVL_LTR_PRT_SUBMT_DT_SK"),
    lit("NA").alias("APL_LVL_LTR_STYLE_NM"),
    lit("1753-01-01").alias("LAST_UPDT_DT_SK"),
    lit("NA").alias("LAST_UPDT_USER_ID"),
    lit("NA").alias("PRT_RQST_USER_ID"),
    lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("APL_LVL_LTR_STYLE_CD_SK")
)

df_temp_for_UNK = df_lkp_Codes.limit(1)
df_xfrm_BusinessLogic_lnk_UNK_out = df_temp_for_UNK.select(
    lit(0).alias("APL_LVL_LTR_PRT_SK"),
    lit("UNK").alias("SRC_SYS_CD"),
    lit("UNK").alias("APL_ID"),
    lit(0).alias("APL_LVL_SEQ_NO"),
    lit("UNK").alias("APL_LVL_LTR_STYLE_CD"),
    lit(0).alias("APL_LVL_LTR_SEQ_NO"),
    lit("UNK").alias("APL_LVL_LTR_DEST_ID"),
    lit(0).alias("APL_LVL_LTR_PRT_SEQ_NO"),
    lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    lit(0).alias("PRT_RQST_USER_SK"),
    lit(0).alias("LAST_UPDT_USER_SK"),
    lit(0).alias("APL_LVL_LTR_SK"),
    lit(None).alias("APL_LVL_LTR_EXPL_TX"),
    lit("1753-01-01").alias("APL_LVL_LTR_PRT_DT_SK"),
    lit(None).alias("APL_LVL_LTR_PRT_DESC"),
    lit("1753-01-01").alias("APL_LVL_LTR_PRT_RQST_DT_SK"),
    lit("1753-01-01").alias("APL_LVL_LTR_PRT_SUBMT_DT_SK"),
    lit("UNK").alias("APL_LVL_LTR_STYLE_NM"),
    lit("1753-01-01").alias("LAST_UPDT_DT_SK"),
    lit("UNK").alias("LAST_UPDT_USER_ID"),
    lit("UNK").alias("PRT_RQST_USER_ID"),
    lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("APL_LVL_LTR_STYLE_CD_SK")
)

df_fnl_Data = df_xfrm_BusinessLogic_lnk_NA_out.unionByName(df_xfrm_BusinessLogic_lnk_UNK_out).unionByName(df_xfrm_BusinessLogic_main)

final_columns = [
    ("APL_LVL_LTR_PRT_SK", None),
    ("SRC_SYS_CD", None),
    ("APL_ID", None),
    ("APL_LVL_SEQ_NO", None),
    ("APL_LVL_LTR_STYLE_CD", None),
    ("APL_LVL_LTR_SEQ_NO", None),
    ("APL_LVL_LTR_DEST_ID", None),
    ("APL_LVL_LTR_PRT_SEQ_NO", None),
    ("CRT_RUN_CYC_EXCTN_DT_SK", 10),
    ("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", 10),
    ("PRT_RQST_USER_SK", None),
    ("LAST_UPDT_USER_SK", None),
    ("APL_LVL_LTR_SK", None),
    ("APL_LVL_LTR_EXPL_TX", None),
    ("APL_LVL_LTR_PRT_DT_SK", 10),
    ("APL_LVL_LTR_PRT_DESC", None),
    ("APL_LVL_LTR_PRT_RQST_DT_SK", 10),
    ("APL_LVL_LTR_PRT_SUBMT_DT_SK", 10),
    ("APL_LVL_LTR_STYLE_NM", None),
    ("LAST_UPDT_DT_SK", 10),
    ("LAST_UPDT_USER_ID", None),
    ("PRT_RQST_USER_ID", None),
    ("CRT_RUN_CYC_EXCTN_SK", None),
    ("LAST_UPDT_RUN_CYC_EXCTN_SK", None),
    ("APL_LVL_LTR_STYLE_CD_SK", None)
]

df_final_select = df_fnl_Data.select(
    [
        when(final_columns[i][1].is_(None), col(final_columns[i][0]))
        .otherwise(rpad(col(final_columns[i][0]), final_columns[i][1], " "))
        .alias(final_columns[i][0])
        if final_columns[i][1] is None
        else rpad(col(final_columns[i][0]), final_columns[i][1], " ").alias(final_columns[i][0])
        for i in range(len(final_columns))
    ]
)

write_files(
    df_final_select,
    f"{adls_path}/load/APL_LVL_LTR_PRT_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)