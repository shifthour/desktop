# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2009 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                    HRA\\Seq\\EdwMbrHraExtr6Seq
# MAGIC                    EdwClnclHlthScreenCntl
# MAGIC 
# MAGIC Processing:
# MAGIC                     This is a new DataStage job that loads the EDW P_MBR_HLTH_RISK data into the Claim DataMart table MBRSH_DM_MBR_HLTH_RISK as UPDATE
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                Project/                                                                                                                                                                             Code                  Date
# MAGIC Developer         Date              Altiris #           Change Description                                                                                                  ds environment         Reviewer            Reviewed
# MAGIC ----------------------   -------------------   -------------------   ------------------------------------------------------------------------------------------------------------------------------   ------------------------       ----------------------     -------------------   
# MAGIC SAndrew          2010-01-14    Alineo               Initial Programming                                                                                                 EnterpriseCurDevl    Steph Goddard   01/20/2010

# MAGIC Loads the Health Risk data into the  Claim DataMart table MBRSH_DM_MBR_HLTH_RISK
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')

# --- Stage: EDW (DB2Connector) ---
jdbc_url_EDW, jdbc_props_EDW = get_db_config(edw_secret_name)
extract_query_EDW = """SELECT 
P_MBR_HLTH_RISK.SRC_SYS_CD,
P_MBR_HLTH_RISK.MBR_UNIQ_KEY,
P_MBR_HLTH_RISK.MBR_SRVY_RSPN_DT,
P_MBR_HLTH_RISK.BRST_XRAY_IN,
P_MBR_HLTH_RISK.CERV_SCRN_IN,
P_MBR_HLTH_RISK.COLON_SCRN_IN,
P_MBR_HLTH_RISK.EXRCS_IN,
P_MBR_HLTH_RISK.HYPRTN_IN,
P_MBR_HLTH_RISK.NTRTN_IN,
P_MBR_HLTH_RISK.STRS_IN,
P_MBR_HLTH_RISK.TOBAC_IN,
P_MBR_HLTH_RISK.WT_IN,
P_MBR_HLTH_RISK.LAST_UPDT_RUN_CYC_NO,
P_MBR_HLTH_RISK.LAST_UPDT_DT 
FROM #$EDWOwner#.P_MBR_HLTH_RISK   P_MBR_HLTH_RISK
#$EDWOwner#.P_MBR_HLTH_RISK
"""
df_EDW = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_EDW)
    .options(**jdbc_props_EDW)
    .option("query", extract_query_EDW)
    .load()
)

# --- Stage: BusinessLogic (CTransformerStage) ---
df_BusinessLogic = df_EDW.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("MBR_SRVY_RSPN_DT").alias("MBR_SRVY_RSPN_DT"),
    rpad(col("BRST_XRAY_IN"), 1, " ").alias("BRST_XRAY_IN"),
    rpad(col("CERV_SCRN_IN"), 1, " ").alias("CERV_SCRN_IN"),
    rpad(col("COLON_SCRN_IN"), 1, " ").alias("COLON_SCRN_IN"),
    rpad(col("EXRCS_IN"), 1, " ").alias("EXRCS_IN"),
    rpad(col("HYPRTN_IN"), 1, " ").alias("HYPRTN_IN"),
    rpad(col("NTRTN_IN"), 1, " ").alias("NTRTN_IN"),
    rpad(col("STRS_IN"), 1, " ").alias("STRS_IN"),
    rpad(col("TOBAC_IN"), 1, " ").alias("TOBAC_IN"),
    rpad(col("WT_IN"), 1, " ").alias("WT_IN"),
    col("LAST_UPDT_RUN_CYC_NO").alias("LAST_UPDT_RUN_CYC_NO")
)

# --- Stage: MBRSH_DM_MBR_HLTH_RISK (CODBCStage) ---
jdbc_url_ClmMart, jdbc_props_ClmMart = get_db_config(clmmart_secret_name)
execute_dml(f"DROP TABLE IF EXISTS STAGING.EdwMbrshDmMbrHlthRiskExtr_MBRSH_DM_MBR_HLTH_RISK_temp", jdbc_url_ClmMart, jdbc_props_ClmMart)

df_BusinessLogic.write.format("jdbc") \
    .option("url", jdbc_url_ClmMart) \
    .options(**jdbc_props_ClmMart) \
    .option("dbtable", "STAGING.EdwMbrshDmMbrHlthRiskExtr_MBRSH_DM_MBR_HLTH_RISK_temp") \
    .mode("append") \
    .save()

merge_sql = f"""
MERGE INTO #$ClmMartOwner#.MBRSH_DM_MBR_HLTH_RISK AS T
USING STAGING.EdwMbrshDmMbrHlthRiskExtr_MBRSH_DM_MBR_HLTH_RISK_temp AS S
ON (T.SRC_SYS_CD = S.SRC_SYS_CD AND T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY)
WHEN MATCHED THEN
  UPDATE SET
    T.MBR_SRVY_RSPN_DT = S.MBR_SRVY_RSPN_DT,
    T.BRST_XRAY_IN = S.BRST_XRAY_IN,
    T.CERV_SCRN_IN = S.CERV_SCRN_IN,
    T.COLON_SCRN_IN = S.COLON_SCRN_IN,
    T.EXRCS_IN = S.EXRCS_IN,
    T.HYPRTN_IN = S.HYPRTN_IN,
    T.NTRTN_IN = S.NTRTN_IN,
    T.STRS_IN = S.STRS_IN,
    T.TOBAC_IN = S.TOBAC_IN,
    T.WT_IN = S.WT_IN,
    T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO
WHEN NOT MATCHED THEN
  INSERT (
    SRC_SYS_CD,
    MBR_UNIQ_KEY,
    MBR_SRVY_RSPN_DT,
    BRST_XRAY_IN,
    CERV_SCRN_IN,
    COLON_SCRN_IN,
    EXRCS_IN,
    HYPRTN_IN,
    NTRTN_IN,
    STRS_IN,
    TOBAC_IN,
    WT_IN,
    LAST_UPDT_RUN_CYC_NO
  )
  VALUES (
    S.SRC_SYS_CD,
    S.MBR_UNIQ_KEY,
    S.MBR_SRVY_RSPN_DT,
    S.BRST_XRAY_IN,
    S.CERV_SCRN_IN,
    S.COLON_SCRN_IN,
    S.EXRCS_IN,
    S.HYPRTN_IN,
    S.NTRTN_IN,
    S.STRS_IN,
    S.TOBAC_IN,
    S.WT_IN,
    S.LAST_UPDT_RUN_CYC_NO
  );
"""
execute_dml(merge_sql, jdbc_url_ClmMart, jdbc_props_ClmMart)