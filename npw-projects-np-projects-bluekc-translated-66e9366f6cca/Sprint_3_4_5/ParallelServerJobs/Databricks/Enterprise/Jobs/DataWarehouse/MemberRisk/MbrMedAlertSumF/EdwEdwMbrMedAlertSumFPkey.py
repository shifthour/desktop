# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC 
# MAGIC Archana Palivela     06/28/2013        5114                              Original Programming                                                                            EnterpriseWrhsDevl      Jag Yelavarthi            2013-12-09  
# MAGIC 
# MAGIC Manasa Andru          2016-10-13       TFS - 12321                   Changed the dataset path from load to ds directory on Unix                    EnterpriseDev1          Jag Yelavarthi             2016-10-19

# MAGIC Job Name: IdsEdwProvNtwkCtFPkey
# MAGIC Read MBR_MED_ALERT_SUM_F.ds Dataset created in the IdsEdwProvNtwkCtFExtr Job.
# MAGIC Copy for creating two output streams
# MAGIC This job will be used when there is a need to Assign primary keys to incoming data before loading into Target table.
# MAGIC Extract dataset is created in the extract job ran prior to this.
# MAGIC Every K table will need a dedicated Database SEQUENCE to provide and track new numbers to use.
# MAGIC New KEYS generated for each run will be Inserted into the K table.
# MAGIC Read K_MBR_MED_ALERT_SUM_F Table to pull the Natural Keys and the Skey.
# MAGIC Left Join on Natural Keys
# MAGIC Left Join on Natural Keys
# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC WriteMBR_MED_ALERT_SUM_F.dat for the IdsEdwProvNtwkCtFLoad Job.
# MAGIC Insert Only the Newly generated Pkeys into the K Table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, when, rpad
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, LongType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

df_ds_MBR_MED_ALERT_SUM_F_out = spark.read.parquet(f"{adls_path}/ds/MBR_MED_ALERT_SUM_F.parquet")
df_ds_MBR_MED_ALERT_SUM_F_out = df_ds_MBR_MED_ALERT_SUM_F_out.select(
    "MBR_MED_ALERT_SUM_SK",
    "SRC_SYS_CD",
    "MBR_UNIQ_KEY",
    "PRCS_YR_MO_SK",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "CLS_SK",
    "GRP_SK",
    "MBR_SK",
    "MBR_MED_MESRS_SK",
    "SUB_SK",
    "SUBGRP_SK",
    "ASTHMTC_CT",
    "ASTHMTC_PRSTNT_WITH_ICS_CT",
    "ASTHMTC_WITH_LABA_AND_ICS_CT",
    "ASTHMTC_WITH_OV_SIX_MO_CT",
    "CAD_CT",
    "CAD_WITH_LDL_TST_CT",
    "CAD_WITH_STATIN_CT",
    "CHF_CT",
    "CHF_WITH_ACE_INHBTR_CT",
    "CHF_WITH_ANUL_PHYS_VST_CT",
    "COPD_CT",
    "COPD_WITH_ANUL_PHYS_VST_CT",
    "COPD_WITH_RX_CMPLNC_CT",
    "DBTC_CT",
    "DBTC_WITH_ANUL_EYE_EXAM_CT",
    "DBTC_WITH_ANUL_MICROAL_TST_CT",
    "DBTC_WITH_ANUL_LDL_TST_CT",
    "DBTC_WITH_CTL_HBG_A1C_CT",
    "DBTC_WITH_CTL_LIPIDS_CT",
    "DBTC_WITH_HBG_A1C_BIYRLY_CT",
    "CLS_ID",
    "GRP_ID",
    "SUBGRP_ID",
    "SUB_UNIQ_KEY",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK"
)

df_cpy_Multistreams_lnk_EdwEdwMbrMedAlertSumFPkey_L_in = df_ds_MBR_MED_ALERT_SUM_F_out.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    rpad(col("PRCS_YR_MO_SK"), 6, " ").alias("PRCS_YR_MO_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("CLS_SK").alias("CLS_SK"),
    col("GRP_SK").alias("GRP_SK"),
    col("MBR_SK").alias("MBR_SK"),
    col("MBR_MED_MESRS_SK").alias("MBR_MED_MESRS_SK"),
    col("SUB_SK").alias("SUB_SK"),
    col("SUBGRP_SK").alias("SUBGRP_SK"),
    col("ASTHMTC_CT").alias("ASTHMTC_CT"),
    col("ASTHMTC_PRSTNT_WITH_ICS_CT").alias("ASTHMTC_PRSTNT_WITH_ICS_CT"),
    col("ASTHMTC_WITH_LABA_AND_ICS_CT").alias("ASTHMTC_WITH_LABA_AND_ICS_CT"),
    col("ASTHMTC_WITH_OV_SIX_MO_CT").alias("ASTHMTC_WITH_OV_SIX_MO_CT"),
    col("CAD_CT").alias("CAD_CT"),
    col("CAD_WITH_LDL_TST_CT").alias("CAD_WITH_LDL_TST_CT"),
    col("CAD_WITH_STATIN_CT").alias("CAD_WITH_STATIN_CT"),
    col("CHF_CT").alias("CHF_CT"),
    col("CHF_WITH_ACE_INHBTR_CT").alias("CHF_WITH_ACE_INHBTR_CT"),
    col("CHF_WITH_ANUL_PHYS_VST_CT").alias("CHF_WITH_ANUL_PHYS_VST_CT"),
    col("COPD_CT").alias("COPD_CT"),
    col("COPD_WITH_ANUL_PHYS_VST_CT").alias("COPD_WITH_ANUL_PHYS_VST_CT"),
    col("COPD_WITH_RX_CMPLNC_CT").alias("COPD_WITH_RX_CMPLNC_CT"),
    col("DBTC_CT").alias("DBTC_CT"),
    col("DBTC_WITH_ANUL_EYE_EXAM_CT").alias("DBTC_WITH_ANUL_EYE_EXAM_CT"),
    col("DBTC_WITH_ANUL_MICROAL_TST_CT").alias("DBTC_WITH_ANUL_MICROAL_TST_CT"),
    col("DBTC_WITH_ANUL_LDL_TST_CT").alias("DBTC_WITH_ANUL_LDL_TST_CT"),
    col("DBTC_WITH_CTL_HBG_A1C_CT").alias("DBTC_WITH_CTL_HBG_A1C_CT"),
    col("DBTC_WITH_CTL_LIPIDS_CT").alias("DBTC_WITH_CTL_LIPIDS_CT"),
    col("DBTC_WITH_HBG_A1C_BIYRLY_CT").alias("DBTC_WITH_HBG_A1C_BIYRLY_CT"),
    col("CLS_ID").alias("CLS_ID"),
    col("GRP_ID").alias("GRP_ID"),
    col("SUBGRP_ID").alias("SUBGRP_ID"),
    col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_cpy_Multistreams_lnk_EdwEdwMbrMedAlertSumFDataK_L_in = df_ds_MBR_MED_ALERT_SUM_F_out.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    rpad(col("PRCS_YR_MO_SK"), 6, " ").alias("PRCS_YR_MO_SK")
)

extract_query = (
    "SELECT SRC_SYS_CD, MBR_UNIQ_KEY, PRCS_YR_MO_SK, CRT_RUN_CYC_EXCTN_DT_SK, "
    "MBR_MED_ALERT_SUM_SK, CRT_RUN_CYC_EXCTN_SK "
    "FROM " + EDWOwner + ".K_MBR_MED_ALERT_SUM_F"
)
df_db2_K_MBR_MED_ALERT_SUM_F_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_jn_MbrMedAlertCtF = df_cpy_Multistreams_lnk_EdwEdwMbrMedAlertSumFDataK_L_in.alias("lnk_EdwEdwMbrMedAlertSumFDataK_L_in").join(
    df_db2_K_MBR_MED_ALERT_SUM_F_in.alias("lnk_KEdwEdwMbrMedAlertSumFPkey_out"),
    [
        col("lnk_EdwEdwMbrMedAlertSumFDataK_L_in.SRC_SYS_CD") == col("lnk_KEdwEdwMbrMedAlertSumFPkey_out.SRC_SYS_CD"),
        col("lnk_EdwEdwMbrMedAlertSumFDataK_L_in.MBR_UNIQ_KEY") == col("lnk_KEdwEdwMbrMedAlertSumFPkey_out.MBR_UNIQ_KEY"),
        col("lnk_EdwEdwMbrMedAlertSumFDataK_L_in.PRCS_YR_MO_SK") == col("lnk_KEdwEdwMbrMedAlertSumFPkey_out.PRCS_YR_MO_SK")
    ],
    how="left"
)

df_jn_MbrMedAlertCtF = df_jn_MbrMedAlertCtF.select(
    col("lnk_EdwEdwMbrMedAlertSumFDataK_L_in.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnk_EdwEdwMbrMedAlertSumFDataK_L_in.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("lnk_EdwEdwMbrMedAlertSumFDataK_L_in.PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    col("lnk_KEdwEdwMbrMedAlertSumFPkey_out.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("lnk_KEdwEdwMbrMedAlertSumFPkey_out.MBR_MED_ALERT_SUM_SK").alias("MBR_MED_ALERT_SUM_SK"),
    col("lnk_KEdwEdwMbrMedAlertSumFPkey_out.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK")
)

df_enriched = df_jn_MbrMedAlertCtF.withColumn("MBR_MED_ALERT_SUM_SK_original", col("MBR_MED_ALERT_SUM_SK"))
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"MBR_MED_ALERT_SUM_SK",<schema>,<secret_name>)
df_enriched = df_enriched.withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK",
    when(col("MBR_MED_ALERT_SUM_SK_original").isNull(), lit(EDWRunCycleDate)).otherwise(col("CRT_RUN_CYC_EXCTN_DT_SK"))
)
df_enriched = df_enriched.withColumn(
    "CRT_RUN_CYC_EXCTN_SK",
    when(col("MBR_MED_ALERT_SUM_SK_original").isNull(), lit(EDWRunCycle)).otherwise(col("CRT_RUN_CYC_EXCTN_SK"))
)

df_lnk_Pkey_out = df_enriched.select(
    col("SRC_SYS_CD"),
    col("MBR_UNIQ_KEY"),
    col("PRCS_YR_MO_SK"),
    col("MBR_MED_ALERT_SUM_SK"),
    col("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("CRT_RUN_CYC_EXCTN_SK")
)

df_lnk_K_EdwEdwMbrMedAlertSumF_out = df_enriched.filter(col("MBR_MED_ALERT_SUM_SK_original").isNull()).select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    rpad(col("PRCS_YR_MO_SK"), 6, " ").alias("PRCS_YR_MO_SK"),
    rpad(lit(EDWRunCycleDate), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("MBR_MED_ALERT_SUM_SK").alias("MBR_MED_ALERT_SUM_SK"),
    lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK")
)

execute_dml(f"DROP TABLE IF EXISTS STAGING.EdwEdwMbrMedAlertSumFPkey_db2_K_MBR_MED_ALERT_SUM_F_load_temp", jdbc_url, jdbc_props)

df_lnk_K_EdwEdwMbrMedAlertSumF_out.write.format("jdbc") \
  .option("url", jdbc_url) \
  .options(**jdbc_props) \
  .option("dbtable", "STAGING.EdwEdwMbrMedAlertSumFPkey_db2_K_MBR_MED_ALERT_SUM_F_load_temp") \
  .mode("overwrite") \
  .save()

merge_sql = (
    f"MERGE INTO {EDWOwner}.K_MBR_MED_ALERT_SUM_F AS T "
    f"USING STAGING.EdwEdwMbrMedAlertSumFPkey_db2_K_MBR_MED_ALERT_SUM_F_load_temp AS S "
    f"ON "
    f"(T.SRC_SYS_CD = S.SRC_SYS_CD AND "
    f"T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY AND "
    f"T.PRCS_YR_MO_SK = S.PRCS_YR_MO_SK) "
    f"WHEN MATCHED "
    f"THEN UPDATE SET T.SRC_SYS_CD = T.SRC_SYS_CD "
    f"WHEN NOT MATCHED "
    f"THEN INSERT "
    f"(SRC_SYS_CD, MBR_UNIQ_KEY, PRCS_YR_MO_SK, CRT_RUN_CYC_EXCTN_DT_SK, MBR_MED_ALERT_SUM_SK, CRT_RUN_CYC_EXCTN_SK) "
    f"VALUES "
    f"(S.SRC_SYS_CD, S.MBR_UNIQ_KEY, S.PRCS_YR_MO_SK, S.CRT_RUN_CYC_EXCTN_DT_SK, S.MBR_MED_ALERT_SUM_SK, S.CRT_RUN_CYC_EXCTN_SK);"
)
execute_dml(merge_sql, jdbc_url, jdbc_props)

df_jn_Pkey = df_cpy_Multistreams_lnk_EdwEdwMbrMedAlertSumFPkey_L_in.alias("lnk_EdwEdwMbrMedAlertSumFPkey_L_in").join(
    df_lnk_Pkey_out.alias("lnk_Pkey_out"),
    [
        col("lnk_EdwEdwMbrMedAlertSumFPkey_L_in.SRC_SYS_CD") == col("lnk_Pkey_out.SRC_SYS_CD"),
        col("lnk_EdwEdwMbrMedAlertSumFPkey_L_in.MBR_UNIQ_KEY") == col("lnk_Pkey_out.MBR_UNIQ_KEY"),
        col("lnk_EdwEdwMbrMedAlertSumFPkey_L_in.PRCS_YR_MO_SK") == col("lnk_Pkey_out.PRCS_YR_MO_SK")
    ],
    how="left"
)

df_jn_Pkey = df_jn_Pkey.select(
    col("lnk_Pkey_out.MBR_MED_ALERT_SUM_SK").alias("MBR_MED_ALERT_SUM_SK"),
    col("lnk_EdwEdwMbrMedAlertSumFPkey_L_in.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnk_EdwEdwMbrMedAlertSumFPkey_L_in.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("lnk_EdwEdwMbrMedAlertSumFPkey_L_in.PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    col("lnk_Pkey_out.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("lnk_EdwEdwMbrMedAlertSumFPkey_L_in.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("lnk_EdwEdwMbrMedAlertSumFPkey_L_in.CLS_SK").alias("CLS_SK"),
    col("lnk_EdwEdwMbrMedAlertSumFPkey_L_in.GRP_SK").alias("GRP_SK"),
    col("lnk_EdwEdwMbrMedAlertSumFPkey_L_in.MBR_SK").alias("MBR_SK"),
    col("lnk_EdwEdwMbrMedAlertSumFPkey_L_in.MBR_MED_MESRS_SK").alias("MBR_MED_MESRS_SK"),
    col("lnk_EdwEdwMbrMedAlertSumFPkey_L_in.SUB_SK").alias("SUB_SK"),
    col("lnk_EdwEdwMbrMedAlertSumFPkey_L_in.SUBGRP_SK").alias("SUBGRP_SK"),
    col("lnk_EdwEdwMbrMedAlertSumFPkey_L_in.ASTHMTC_CT").alias("ASTHMTC_CT"),
    col("lnk_EdwEdwMbrMedAlertSumFPkey_L_in.ASTHMTC_PRSTNT_WITH_ICS_CT").alias("ASTHMTC_PRSTNT_WITH_ICS_CT"),
    col("lnk_EdwEdwMbrMedAlertSumFPkey_L_in.ASTHMTC_WITH_LABA_AND_ICS_CT").alias("ASTHMTC_WITH_LABA_AND_ICS_CT"),
    col("lnk_EdwEdwMbrMedAlertSumFPkey_L_in.ASTHMTC_WITH_OV_SIX_MO_CT").alias("ASTHMTC_WITH_OV_SIX_MO_CT"),
    col("lnk_EdwEdwMbrMedAlertSumFPkey_L_in.CAD_CT").alias("CAD_CT"),
    col("lnk_EdwEdwMbrMedAlertSumFPkey_L_in.CAD_WITH_LDL_TST_CT").alias("CAD_WITH_LDL_TST_CT"),
    col("lnk_EdwEdwMbrMedAlertSumFPkey_L_in.CAD_WITH_STATIN_CT").alias("CAD_WITH_STATIN_CT"),
    col("lnk_EdwEdwMbrMedAlertSumFPkey_L_in.CHF_CT").alias("CHF_CT"),
    col("lnk_EdwEdwMbrMedAlertSumFPkey_L_in.CHF_WITH_ACE_INHBTR_CT").alias("CHF_WITH_ACE_INHBTR_CT"),
    col("lnk_EdwEdwMbrMedAlertSumFPkey_L_in.CHF_WITH_ANUL_PHYS_VST_CT").alias("CHF_WITH_ANUL_PHYS_VST_CT"),
    col("lnk_EdwEdwMbrMedAlertSumFPkey_L_in.COPD_CT").alias("COPD_CT"),
    col("lnk_EdwEdwMbrMedAlertSumFPkey_L_in.COPD_WITH_ANUL_PHYS_VST_CT").alias("COPD_WITH_ANUL_PHYS_VST_CT"),
    col("lnk_EdwEdwMbrMedAlertSumFPkey_L_in.COPD_WITH_RX_CMPLNC_CT").alias("COPD_WITH_RX_CMPLNC_CT"),
    col("lnk_EdwEdwMbrMedAlertSumFPkey_L_in.DBTC_CT").alias("DBTC_CT"),
    col("lnk_EdwEdwMbrMedAlertSumFPkey_L_in.DBTC_WITH_ANUL_EYE_EXAM_CT").alias("DBTC_WITH_ANUL_EYE_EXAM_CT"),
    col("lnk_EdwEdwMbrMedAlertSumFPkey_L_in.DBTC_WITH_ANUL_MICROAL_TST_CT").alias("DBTC_WITH_ANUL_MICROAL_TST_CT"),
    col("lnk_EdwEdwMbrMedAlertSumFPkey_L_in.DBTC_WITH_ANUL_LDL_TST_CT").alias("DBTC_WITH_ANUL_LDL_TST_CT"),
    col("lnk_EdwEdwMbrMedAlertSumFPkey_L_in.DBTC_WITH_CTL_HBG_A1C_CT").alias("DBTC_WITH_CTL_HBG_A1C_CT"),
    col("lnk_EdwEdwMbrMedAlertSumFPkey_L_in.DBTC_WITH_CTL_LIPIDS_CT").alias("DBTC_WITH_CTL_LIPIDS_CT"),
    col("lnk_EdwEdwMbrMedAlertSumFPkey_L_in.DBTC_WITH_HBG_A1C_BIYRLY_CT").alias("DBTC_WITH_HBG_A1C_BIYRLY_CT"),
    col("lnk_EdwEdwMbrMedAlertSumFPkey_L_in.CLS_ID").alias("CLS_ID"),
    col("lnk_EdwEdwMbrMedAlertSumFPkey_L_in.GRP_ID").alias("GRP_ID"),
    col("lnk_EdwEdwMbrMedAlertSumFPkey_L_in.SUBGRP_ID").alias("SUBGRP_ID"),
    col("lnk_EdwEdwMbrMedAlertSumFPkey_L_in.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    col("lnk_Pkey_out.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("lnk_EdwEdwMbrMedAlertSumFPkey_L_in.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_jn_Pkey_final = df_jn_Pkey.select(
    col("MBR_MED_ALERT_SUM_SK"),
    col("SRC_SYS_CD"),
    col("MBR_UNIQ_KEY"),
    rpad(col("PRCS_YR_MO_SK"), 6, " ").alias("PRCS_YR_MO_SK"),
    rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("CLS_SK"),
    col("GRP_SK"),
    col("MBR_SK"),
    col("MBR_MED_MESRS_SK"),
    col("SUB_SK"),
    col("SUBGRP_SK"),
    col("ASTHMTC_CT"),
    col("ASTHMTC_PRSTNT_WITH_ICS_CT"),
    col("ASTHMTC_WITH_LABA_AND_ICS_CT"),
    col("ASTHMTC_WITH_OV_SIX_MO_CT"),
    col("CAD_CT"),
    col("CAD_WITH_LDL_TST_CT"),
    col("CAD_WITH_STATIN_CT"),
    col("CHF_CT"),
    col("CHF_WITH_ACE_INHBTR_CT"),
    col("CHF_WITH_ANUL_PHYS_VST_CT"),
    col("COPD_CT"),
    col("COPD_WITH_ANUL_PHYS_VST_CT"),
    col("COPD_WITH_RX_CMPLNC_CT"),
    col("DBTC_CT"),
    col("DBTC_WITH_ANUL_EYE_EXAM_CT"),
    col("DBTC_WITH_ANUL_MICROAL_TST_CT"),
    col("DBTC_WITH_ANUL_LDL_TST_CT"),
    col("DBTC_WITH_CTL_HBG_A1C_CT"),
    col("DBTC_WITH_CTL_LIPIDS_CT"),
    col("DBTC_WITH_HBG_A1C_BIYRLY_CT"),
    col("CLS_ID"),
    col("GRP_ID"),
    col("SUBGRP_ID"),
    col("SUB_UNIQ_KEY"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

write_files(
    df_jn_Pkey_final,
    f"{adls_path}/load/MBR_MED_ALERT_SUM_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)