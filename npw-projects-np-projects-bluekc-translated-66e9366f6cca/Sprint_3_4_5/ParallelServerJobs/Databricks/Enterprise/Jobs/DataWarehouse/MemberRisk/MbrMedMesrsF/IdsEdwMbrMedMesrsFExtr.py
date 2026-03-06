# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                          Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                1/03/2008         CLINICALS/3036            Originally Programmed                        devlEDW10                  Steph Goddard            01/07/2008
# MAGIC 
# MAGIC Bhoomi Dasari                03/18/2008       CLINICALS/3036            Added four new fields                        devlEDW                       Steph Goddard            05/14/2008
# MAGIC 
# MAGIC 
# MAGIC Rama Kamjula               10/28/2013          5114                            Rewritten from Server to Parallel          EnterpriseWrhsDevl      Peter Marshall             12/11/2013

# MAGIC JobName: IdsEdwMbrMedMesrsFExtr
# MAGIC 
# MAGIC Job creates loadfile for MBR_MED_MESRS_F  in EDW
# MAGIC MBR_MED_MESRS extract  from IDS
# MAGIC Creates load file for MBR_MED_MESRS_F
# MAGIC Extracts IDS- MBR data
# MAGIC Null Handling and Business Logic
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, when, coalesce, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
ExtractRunCycle = get_widget_value('ExtractRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_db2_MBR_MED_MESRS = f"""
SELECT 
  MESRS.MBR_MED_MESRS_SK,
  COALESCE(CD.TRGT_CD, 'NA') SRC_SYS_CD,
  MESRS.MBR_UNIQ_KEY,
  MESRS.PRCS_YR_MO_SK,
  MESRS.CRT_RUN_CYC_EXCTN_SK,
  MESRS.MBR_SK,
  MESRS.ACTURL_UNDWRT_FTR_RISK_SCOR_NO,
  MESRS.AGE_GNDR_RISK_SCORE_NO,
  MESRS.FTR_IP_RISK_SCORE_NO,
  MESRS.FTR_TOT_RISK_SCORE_NO,
  MESRS.IP_STAY_PROBLTY_NO,
  MESRS.CARE_OPP_ATCHD_IN,
  MESRS.CASE_DEFN_ATCHD_IN,
  MESRS.CLNCL_IN_ATCHD_IN,
  MESRS.MED_ALERT_ATCHD_IN
FROM {IDSOwner}.MBR_MED_MESRS MESRS
LEFT JOIN {IDSOwner}.CD_MPPNG CD
       ON MESRS.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
WHERE MESRS.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtractRunCycle}
"""
df_db2_MBR_MED_MESRS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_MBR_MED_MESRS)
    .load()
)

extract_query_db2_MBR = f"""
SELECT 
  MBR_UNIQ_KEY,
  INDV_BE_KEY
FROM {IDSOwner}.MBR
"""
df_db2_MBR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_MBR)
    .load()
)

df_lkp_codes = df_db2_MBR_MED_MESRS.alias("lnk_MbrMedMesrs_In").join(
    df_db2_MBR.alias("lnk_Mbr_In"),
    col("lnk_MbrMedMesrs_In.MBR_UNIQ_KEY") == col("lnk_Mbr_In.MBR_UNIQ_KEY"),
    "left"
).select(
    col("lnk_MbrMedMesrs_In.MBR_MED_MESRS_SK").alias("MBR_MED_MESRS_SK"),
    col("lnk_MbrMedMesrs_In.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnk_MbrMedMesrs_In.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("lnk_MbrMedMesrs_In.PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    col("lnk_MbrMedMesrs_In.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("lnk_MbrMedMesrs_In.MBR_SK").alias("MBR_SK"),
    col("lnk_MbrMedMesrs_In.CARE_OPP_ATCHD_IN").alias("CARE_OPP_ATCHD_IN"),
    col("lnk_MbrMedMesrs_In.CASE_DEFN_ATCHD_IN").alias("CASE_DEFN_ATCHD_IN"),
    col("lnk_MbrMedMesrs_In.CLNCL_IN_ATCHD_IN").alias("CLNCL_IN_ATCHD_IN"),
    col("lnk_MbrMedMesrs_In.MED_ALERT_ATCHD_IN").alias("MED_ALERT_ATCHD_IN"),
    col("lnk_MbrMedMesrs_In.ACTURL_UNDWRT_FTR_RISK_SCOR_NO").alias("ACTURL_UNDWRT_FTR_RISK_SCOR_NO"),
    col("lnk_MbrMedMesrs_In.AGE_GNDR_RISK_SCORE_NO").alias("AGE_GNDR_RISK_SCORE_NO"),
    col("lnk_MbrMedMesrs_In.FTR_IP_RISK_SCORE_NO").alias("FTR_IP_RISK_SCORE_NO"),
    col("lnk_MbrMedMesrs_In.FTR_TOT_RISK_SCORE_NO").alias("FTR_TOT_RISK_SCORE_NO"),
    col("lnk_MbrMedMesrs_In.IP_STAY_PROBLTY_NO").alias("IP_STAY_PROBLTY_NO"),
    col("lnk_Mbr_In.INDV_BE_KEY").alias("INDV_BE_KEY")
)

df_lnk_MbrMedMesrs_Out = df_lkp_codes.filter(
    (col("MBR_MED_MESRS_SK") != 0) & (col("MBR_MED_MESRS_SK") != 1)
).select(
    col("MBR_MED_MESRS_SK"),
    col("SRC_SYS_CD"),
    col("MBR_UNIQ_KEY"),
    col("PRCS_YR_MO_SK"),
    lit(CurrRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    lit(CurrRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("MBR_SK"),
    col("CARE_OPP_ATCHD_IN"),
    col("CASE_DEFN_ATCHD_IN"),
    col("CLNCL_IN_ATCHD_IN"),
    col("MED_ALERT_ATCHD_IN"),
    col("ACTURL_UNDWRT_FTR_RISK_SCOR_NO"),
    col("AGE_GNDR_RISK_SCORE_NO"),
    col("FTR_IP_RISK_SCORE_NO"),
    col("FTR_TOT_RISK_SCORE_NO"),
    coalesce(col("INDV_BE_KEY"), lit(0)).alias("INDV_BE_KEY"),
    col("IP_STAY_PROBLTY_NO"),
    lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_lnk_NA = spark.range(1).select(
    lit(1).cast("int").alias("MBR_MED_MESRS_SK"),
    lit("NA").alias("SRC_SYS_CD"),
    lit(1).cast("int").alias("MBR_UNIQ_KEY"),
    lit("175301").alias("PRCS_YR_MO_SK"),
    lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    lit(1).cast("int").alias("MBR_SK"),
    lit("N").alias("CARE_OPP_ATCHD_IN"),
    lit("N").alias("CASE_DEFN_ATCHD_IN"),
    lit("N").alias("CLNCL_IN_ATCHD_IN"),
    lit("N").alias("MED_ALERT_ATCHD_IN"),
    lit(0).cast("int").alias("ACTURL_UNDWRT_FTR_RISK_SCOR_NO"),
    lit(0).cast("int").alias("AGE_GNDR_RISK_SCORE_NO"),
    lit(0).cast("int").alias("FTR_IP_RISK_SCORE_NO"),
    lit(0).cast("int").alias("FTR_TOT_RISK_SCORE_NO"),
    lit(1).cast("int").alias("INDV_BE_KEY"),
    lit(0).cast("int").alias("IP_STAY_PROBLTY_NO"),
    lit(100).cast("int").alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(100).cast("int").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_lnk_UNK = spark.range(1).select(
    lit(0).cast("int").alias("MBR_MED_MESRS_SK"),
    lit("UNK").alias("SRC_SYS_CD"),
    lit(0).cast("int").alias("MBR_UNIQ_KEY"),
    lit("175301").alias("PRCS_YR_MO_SK"),
    lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    lit(0).cast("int").alias("MBR_SK"),
    lit("N").alias("CARE_OPP_ATCHD_IN"),
    lit("N").alias("CASE_DEFN_ATCHD_IN"),
    lit("N").alias("CLNCL_IN_ATCHD_IN"),
    lit("N").alias("MED_ALERT_ATCHD_IN"),
    lit(0).cast("int").alias("ACTURL_UNDWRT_FTR_RISK_SCOR_NO"),
    lit(0).cast("int").alias("AGE_GNDR_RISK_SCORE_NO"),
    lit(0).cast("int").alias("FTR_IP_RISK_SCORE_NO"),
    lit(0).cast("int").alias("FTR_TOT_RISK_SCORE_NO"),
    lit(0).cast("int").alias("INDV_BE_KEY"),
    lit(0).cast("int").alias("IP_STAY_PROBLTY_NO"),
    lit(100).cast("int").alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(100).cast("int").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

columns_funnel = [
    "MBR_MED_MESRS_SK",
    "SRC_SYS_CD",
    "MBR_UNIQ_KEY",
    "PRCS_YR_MO_SK",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "MBR_SK",
    "CARE_OPP_ATCHD_IN",
    "CASE_DEFN_ATCHD_IN",
    "CLNCL_IN_ATCHD_IN",
    "MED_ALERT_ATCHD_IN",
    "ACTURL_UNDWRT_FTR_RISK_SCOR_NO",
    "AGE_GNDR_RISK_SCORE_NO",
    "FTR_IP_RISK_SCORE_NO",
    "FTR_TOT_RISK_SCORE_NO",
    "INDV_BE_KEY",
    "IP_STAY_PROBLTY_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK"
]

df_funnel_48 = (
    df_lnk_MbrMedMesrs_Out.select(columns_funnel)
    .unionByName(df_lnk_NA.select(columns_funnel))
    .unionByName(df_lnk_UNK.select(columns_funnel))
    .orderBy(col("MBR_MED_MESRS_SK").desc())
)

df_final = df_funnel_48.select(
    col("MBR_MED_MESRS_SK"),
    col("SRC_SYS_CD"),
    col("MBR_UNIQ_KEY"),
    rpad(col("PRCS_YR_MO_SK"), 6, " ").alias("PRCS_YR_MO_SK"),
    rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("MBR_SK"),
    rpad(col("CARE_OPP_ATCHD_IN"), 1, " ").alias("CARE_OPP_ATCHD_IN"),
    rpad(col("CASE_DEFN_ATCHD_IN"), 1, " ").alias("CASE_DEFN_ATCHD_IN"),
    rpad(col("CLNCL_IN_ATCHD_IN"), 1, " ").alias("CLNCL_IN_ATCHD_IN"),
    rpad(col("MED_ALERT_ATCHD_IN"), 1, " ").alias("MED_ALERT_ATCHD_IN"),
    col("ACTURL_UNDWRT_FTR_RISK_SCOR_NO"),
    col("AGE_GNDR_RISK_SCORE_NO"),
    col("FTR_IP_RISK_SCORE_NO"),
    col("FTR_TOT_RISK_SCORE_NO"),
    col("INDV_BE_KEY"),
    col("IP_STAY_PROBLTY_NO"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

write_files(
    df_final,
    f"{adls_path}/load/MBR_MED_MESRS_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)