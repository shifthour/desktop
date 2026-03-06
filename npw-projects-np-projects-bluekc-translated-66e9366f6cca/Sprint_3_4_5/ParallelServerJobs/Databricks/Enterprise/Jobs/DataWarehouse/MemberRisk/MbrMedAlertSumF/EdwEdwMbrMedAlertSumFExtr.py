# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC DESCRIPTION:   Pulls data from MBR_MED_ALERT_I and creates MBR_MED_ALERT_SUM_F file
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer               Date                         Change Description                                                                           Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------             ----------------------------     -----------------------------------------------------------------------                                   ----------------         ------------------------------------       ----------------------------      ----------------
# MAGIC Bhoomi Dasari       2009-02-06                Initial programming                                                                               3863                devlEDWcur                       Steph Goddard         02/12/2009
# MAGIC 
# MAGIC Archana Palivela       2013-0-24                Converted from Server job to parallel job                                              5114               EnterpriseWrhsDevl           Jag Yelavarthi          2013-12-09 
# MAGIC 
# MAGIC Manasa Andru          2016-10-13          Changed the dataset path from load to ds directory on Unix                   TFS - 12321     EnterpriseDev1                  Jag Yelavarthi          2016-10-19

# MAGIC lookup with CdMapping table to get code and name details based on given keys
# MAGIC Null Handling
# MAGIC Creates load file for MBR_MED_ALERT_SUM_F
# MAGIC Extract  Data from CD_Mapping table
# MAGIC JobName: IdsEdwClsPlnDtlIExtr
# MAGIC 
# MAGIC This job joins tablesCLS_PLN_DTL, CLS_PLN, GRP, PCA_ADM, ALPHA_PFX and map with 
# MAGIC cd mapping table to get Code and Name from code mapping table.
# MAGIC Extract Mbr_Med_Alert_Sum related data from EDW
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


# Retrieve parameter values
EDWRunCycle = get_widget_value("EDWRunCycle","")
EDWRunCycleDate = get_widget_value("EDWRunCycleDate","")
edw_secret_name = get_widget_value("edw_secret_name","")

# Set up EDW database connection
jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)

# ----------------------------------------------------------------------------------------------------
# db2_MBR_MED_ALERT_I Stage
# ----------------------------------------------------------------------------------------------------
extract_query_db2_MBR_MED_ALERT_I = (
    "SELECT \n"
    "     MBR_MED_ALERT_I.SRC_SYS_CD,\n"
    "     MBR_MED_ALERT_I.MBR_UNIQ_KEY,\n"
    "     MBR_MED_ALERT_I.PRCS_YR_MO_SK,\n"
    "     MBR_MED_ALERT_I.MBR_MED_MESRS_SK,\n"
    "     MBR_D.MBR_SK,\n"
    "     MBR_D.CLS_SK,\n"
    "     MBR_D.GRP_SK,\n"
    "     MBR_D.SUBGRP_SK,\n"
    "     MBR_D.SUB_SK,\n"
    "     MBR_D.CLS_ID,\n"
    "     MBR_D.GRP_ID,\n"
    "     MBR_D.SUBGRP_ID,\n"
    "     MBR_D.SUB_UNIQ_KEY \n"
    "FROM \n"
    "     #$EDWOwner#.MBR_MED_ALERT_I MBR_MED_ALERT_I LEFT OUTER JOIN #$EDWOwner#.MBR_D MBR_D\n"
    "          ON MBR_MED_ALERT_I.MBR_UNIQ_KEY = MBR_D.MBR_UNIQ_KEY\n"
    f"WHERE\n"
    f"     MBR_MED_ALERT_I.LAST_UPDT_RUN_CYC_EXCTN_SK >= {EDWRunCycle}"
)

df_db2_MBR_MED_ALERT_I = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", extract_query_db2_MBR_MED_ALERT_I)
    .load()
)

# ----------------------------------------------------------------------------------------------------
# Remove_Duplicates_113 (PxRemDup)
# ----------------------------------------------------------------------------------------------------
df_remove_dupes_113_in = dedup_sort(
    df_db2_MBR_MED_ALERT_I,
    ["SRC_SYS_CD", "MBR_UNIQ_KEY", "PRCS_YR_MO_SK"],
    [("SRC_SYS_CD","A"), ("MBR_UNIQ_KEY","A"), ("PRCS_YR_MO_SK","A")]
)

df_remove_dupes_113_out = df_remove_dupes_113_in.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    F.col("MBR_MED_MESRS_SK").alias("MBR_MED_MESRS_SK"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("CLS_SK").alias("CLS_SK"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("SUBGRP_SK").alias("SUBGRP_SK"),
    F.col("SUB_SK").alias("SUB_SK"),
    F.col("CLS_ID").alias("CLS_ID"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY")
)

# ----------------------------------------------------------------------------------------------------
# db2_MBR_MED_ALERT_I_Out Stage
# ----------------------------------------------------------------------------------------------------
extract_query_db2_MBR_MED_ALERT_I_Out = (
    "SELECT \n"
    "MBR_MED_ALERT_SK ,\n"
    "SRC_SYS_CD,\n"
    "MBR_UNIQ_KEY,\n"
    "PRCS_YR_MO_SK,\n"
    "MED_ALERT_ID\n"
    "FROM #$EDWOwner#.MBR_MED_ALERT_I\n"
    f"WHERE LAST_UPDT_RUN_CYC_EXCTN_SK >= {EDWRunCycle}"
)

df_db2_MBR_MED_ALERT_I_Out = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", extract_query_db2_MBR_MED_ALERT_I_Out)
    .load()
)

# ----------------------------------------------------------------------------------------------------
# cpy_MBR_MED_ALERT_Cpy (PxCopy)
# ----------------------------------------------------------------------------------------------------
df_cpy_MBR_MED_ALERT_Cpy_out = df_db2_MBR_MED_ALERT_I_Out.select(
    F.col("MBR_MED_ALERT_SK").alias("MBR_MED_ALERT_SK"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    F.col("MED_ALERT_ID").alias("MED_ALERT_ID")
)

# ----------------------------------------------------------------------------------------------------
# Filter_116 (PxFilter) -- Split into 20 outputs
# ----------------------------------------------------------------------------------------------------
df_filter_116_in = df_cpy_MBR_MED_ALERT_Cpy_out

df_filter_116_0 = df_filter_116_in.filter(F.col("MED_ALERT_ID") == "10047")
df_filter_116_1 = df_filter_116_in.filter(F.col("MED_ALERT_ID") == "10017")
df_filter_116_2 = df_filter_116_in.filter(F.col("MED_ALERT_ID") == "10019")
df_filter_116_3 = df_filter_116_in.filter(F.col("MED_ALERT_ID") == "10018")
df_filter_116_4 = df_filter_116_in.filter(F.col("MED_ALERT_ID") == "10048")
df_filter_116_5 = df_filter_116_in.filter(F.col("MED_ALERT_ID") == "10039")
df_filter_116_6 = df_filter_116_in.filter(F.col("MED_ALERT_ID") == "10043")
df_filter_116_7 = df_filter_116_in.filter(F.col("MED_ALERT_ID") == "10049")
df_filter_116_8 = df_filter_116_in.filter(F.col("MED_ALERT_ID") == "10044")
df_filter_116_9 = df_filter_116_in.filter(F.col("MED_ALERT_ID") == "10042")
df_filter_116_10 = df_filter_116_in.filter(F.col("MED_ALERT_ID") == "10041")
df_filter_116_11 = df_filter_116_in.filter(F.col("MED_ALERT_ID") == "10054")
df_filter_116_12 = df_filter_116_in.filter(F.col("MED_ALERT_ID") == "missing")
df_filter_116_13 = df_filter_116_in.filter(F.col("MED_ALERT_ID") == "10050")
df_filter_116_14 = df_filter_116_in.filter(F.col("MED_ALERT_ID") == "10027")
df_filter_116_15 = df_filter_116_in.filter(F.col("MED_ALERT_ID") == "10028")
df_filter_116_16 = df_filter_116_in.filter(F.col("MED_ALERT_ID") == "10051")
df_filter_116_17 = df_filter_116_in.filter(F.col("MED_ALERT_ID") == "missing")
df_filter_116_18 = df_filter_116_in.filter(F.col("MED_ALERT_ID") == "missing")
df_filter_116_19 = df_filter_116_in.filter(F.col("MED_ALERT_ID") == "10029")

# ----------------------------------------------------------------------------------------------------
# lkp_CdMppng (PxLookup) -- Primary link + 20 lookup links
# ----------------------------------------------------------------------------------------------------
df_lkp_CdMppng = df_remove_dupes_113_out

df_lkp_CdMppng = df_lkp_CdMppng.join(
    df_filter_116_0.select(
        "SRC_SYS_CD","MBR_UNIQ_KEY","PRCS_YR_MO_SK",
        F.col("MBR_MED_ALERT_SK").alias("ASTHMTC_CT")
    ),
    on=["SRC_SYS_CD","MBR_UNIQ_KEY","PRCS_YR_MO_SK"],
    how="left"
).join(
    df_filter_116_1.select(
        "SRC_SYS_CD","MBR_UNIQ_KEY","PRCS_YR_MO_SK",
        F.col("MBR_MED_ALERT_SK").alias("ASTHMTC_PRSTNT_WITH_ICS_CT")
    ),
    on=["SRC_SYS_CD","MBR_UNIQ_KEY","PRCS_YR_MO_SK"],
    how="left"
).join(
    df_filter_116_2.select(
        "SRC_SYS_CD","MBR_UNIQ_KEY","PRCS_YR_MO_SK",
        F.col("MBR_MED_ALERT_SK").alias("ASTHMTC_WITH_LABA_AND_ICS_CT")
    ),
    on=["SRC_SYS_CD","MBR_UNIQ_KEY","PRCS_YR_MO_SK"],
    how="left"
).join(
    df_filter_116_3.select(
        "SRC_SYS_CD","MBR_UNIQ_KEY","PRCS_YR_MO_SK",
        F.col("MBR_MED_ALERT_SK").alias("ASTHMTC_WITH_OV_SIX_MO_CT")
    ),
    on=["SRC_SYS_CD","MBR_UNIQ_KEY","PRCS_YR_MO_SK"],
    how="left"
).join(
    df_filter_116_4.select(
        "SRC_SYS_CD","MBR_UNIQ_KEY","PRCS_YR_MO_SK",
        F.col("MBR_MED_ALERT_SK").alias("CAD_CT")
    ),
    on=["SRC_SYS_CD","MBR_UNIQ_KEY","PRCS_YR_MO_SK"],
    how="left"
).join(
    df_filter_116_5.select(
        "SRC_SYS_CD","MBR_UNIQ_KEY","PRCS_YR_MO_SK",
        F.col("MBR_MED_ALERT_SK").alias("CAD_WITH_LDL_TST_CT")
    ),
    on=["SRC_SYS_CD","MBR_UNIQ_KEY","PRCS_YR_MO_SK"],
    how="left"
).join(
    df_filter_116_6.select(
        "SRC_SYS_CD","MBR_UNIQ_KEY","PRCS_YR_MO_SK",
        F.col("MBR_MED_ALERT_SK").alias("CAD_WITH_STATIN_CT")
    ),
    on=["SRC_SYS_CD","MBR_UNIQ_KEY","PRCS_YR_MO_SK"],
    how="left"
).join(
    df_filter_116_7.select(
        "SRC_SYS_CD","MBR_UNIQ_KEY","PRCS_YR_MO_SK",
        F.col("MBR_MED_ALERT_SK").alias("CHF_CT")
    ),
    on=["SRC_SYS_CD","MBR_UNIQ_KEY","PRCS_YR_MO_SK"],
    how="left"
).join(
    df_filter_116_8.select(
        "SRC_SYS_CD","MBR_UNIQ_KEY","PRCS_YR_MO_SK",
        F.col("MBR_MED_ALERT_SK").alias("CHF_WITH_ACE_INHBTR_CT")
    ),
    on=["SRC_SYS_CD","MBR_UNIQ_KEY","PRCS_YR_MO_SK"],
    how="left"
).join(
    df_filter_116_9.select(
        "SRC_SYS_CD","MBR_UNIQ_KEY","PRCS_YR_MO_SK",
        F.col("MBR_MED_ALERT_SK").alias("CHF_WITH_ANUL_PHYS_VST_CT")
    ),
    on=["SRC_SYS_CD","MBR_UNIQ_KEY","PRCS_YR_MO_SK"],
    how="left"
).join(
    df_filter_116_10.select(
        "SRC_SYS_CD","MBR_UNIQ_KEY","PRCS_YR_MO_SK",
        F.col("MBR_MED_ALERT_SK").alias("COPD_CT")
    ),
    on=["SRC_SYS_CD","MBR_UNIQ_KEY","PRCS_YR_MO_SK"],
    how="left"
).join(
    df_filter_116_11.select(
        "SRC_SYS_CD","MBR_UNIQ_KEY","PRCS_YR_MO_SK",
        F.col("MBR_MED_ALERT_SK").alias("COPD_WITH_ANUL_PHYS_VST_CT")
    ),
    on=["SRC_SYS_CD","MBR_UNIQ_KEY","PRCS_YR_MO_SK"],
    how="left"
).join(
    df_filter_116_12.select(
        "SRC_SYS_CD","MBR_UNIQ_KEY","PRCS_YR_MO_SK",
        F.col("MBR_MED_ALERT_SK").alias("COPD_WITH_RX_CMPLNC_CT")
    ),
    on=["SRC_SYS_CD","MBR_UNIQ_KEY","PRCS_YR_MO_SK"],
    how="left"
).join(
    df_filter_116_13.select(
        "SRC_SYS_CD","MBR_UNIQ_KEY","PRCS_YR_MO_SK",
        F.col("MBR_MED_ALERT_SK").alias("DBTC_CT")
    ),
    on=["SRC_SYS_CD","MBR_UNIQ_KEY","PRCS_YR_MO_SK"],
    how="left"
).join(
    df_filter_116_14.select(
        "SRC_SYS_CD","MBR_UNIQ_KEY","PRCS_YR_MO_SK",
        F.col("MBR_MED_ALERT_SK").alias("DBTC_WITH_ANUL_EYE_EXAM_CT")
    ),
    on=["SRC_SYS_CD","MBR_UNIQ_KEY","PRCS_YR_MO_SK"],
    how="left"
).join(
    df_filter_116_15.select(
        "SRC_SYS_CD","MBR_UNIQ_KEY","PRCS_YR_MO_SK",
        F.col("MBR_MED_ALERT_SK").alias("DBTC_WITH_ANUL_MICROAL_TST_CT")
    ),
    on=["SRC_SYS_CD","MBR_UNIQ_KEY","PRCS_YR_MO_SK"],
    how="left"
).join(
    df_filter_116_16.select(
        "SRC_SYS_CD","MBR_UNIQ_KEY","PRCS_YR_MO_SK",
        F.col("MBR_MED_ALERT_SK").alias("DBTC_WITH_ANUL_LDL_TST_CT")
    ),
    on=["SRC_SYS_CD","MBR_UNIQ_KEY","PRCS_YR_MO_SK"],
    how="left"
).join(
    df_filter_116_17.select(
        "SRC_SYS_CD","MBR_UNIQ_KEY","PRCS_YR_MO_SK",
        F.col("MBR_MED_ALERT_SK").alias("DBTC_WITH_CTL_HBG_A1C_CT")
    ),
    on=["SRC_SYS_CD","MBR_UNIQ_KEY","PRCS_YR_MO_SK"],
    how="left"
).join(
    df_filter_116_18.select(
        "SRC_SYS_CD","MBR_UNIQ_KEY","PRCS_YR_MO_SK",
        F.col("MBR_MED_ALERT_SK").alias("DBTC_WITH_CTL_LIPIDS_CT")
    ),
    on=["SRC_SYS_CD","MBR_UNIQ_KEY","PRCS_YR_MO_SK"],
    how="left"
).join(
    df_filter_116_19.select(
        "SRC_SYS_CD","MBR_UNIQ_KEY","PRCS_YR_MO_SK",
        F.col("MBR_MED_ALERT_SK").alias("DBTC_WITH_HBG_A1C_BIYRLY_CT")
    ),
    on=["SRC_SYS_CD","MBR_UNIQ_KEY","PRCS_YR_MO_SK"],
    how="left"
)

df_lkp_CdMppng_out = df_lkp_CdMppng.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    F.col("CLS_SK").alias("CLS_SK"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("MBR_MED_MESRS_SK").alias("MBR_MED_MESRS_SK"),
    F.col("SUB_SK").alias("SUB_SK"),
    F.col("SUBGRP_SK").alias("SUBGRP_SK"),
    F.col("ASTHMTC_CT").alias("ASTHMTC_CT"),
    F.col("ASTHMTC_PRSTNT_WITH_ICS_CT").alias("ASTHMTC_PRSTNT_WITH_ICS_CT"),
    F.col("ASTHMTC_WITH_LABA_AND_ICS_CT").alias("ASTHMTC_WITH_LABA_AND_ICS_CT"),
    F.col("ASTHMTC_WITH_OV_SIX_MO_CT").alias("ASTHMTC_WITH_OV_SIX_MO_CT"),
    F.col("CAD_CT").alias("CAD_CT"),
    F.col("CAD_WITH_LDL_TST_CT").alias("CAD_WITH_LDL_TST_CT"),
    F.col("CAD_WITH_STATIN_CT").alias("CAD_WITH_STATIN_CT"),
    F.col("CHF_CT").alias("CHF_CT"),
    F.col("CHF_WITH_ACE_INHBTR_CT").alias("CHF_WITH_ACE_INHBTR_CT"),
    F.col("CHF_WITH_ANUL_PHYS_VST_CT").alias("CHF_WITH_ANUL_PHYS_VST_CT"),
    F.col("COPD_CT").alias("COPD_CT"),
    F.col("COPD_WITH_ANUL_PHYS_VST_CT").alias("COPD_WITH_ANUL_PHYS_VST_CT"),
    F.col("COPD_WITH_RX_CMPLNC_CT").alias("COPD_WITH_RX_CMPLNC_CT"),
    F.col("DBTC_CT").alias("DBTC_CT"),
    F.col("DBTC_WITH_ANUL_EYE_EXAM_CT").alias("DBTC_WITH_ANUL_EYE_EXAM_CT"),
    F.col("DBTC_WITH_ANUL_MICROAL_TST_CT").alias("DBTC_WITH_ANUL_MICROAL_TST_CT"),
    F.col("DBTC_WITH_ANUL_LDL_TST_CT").alias("DBTC_WITH_ANUL_LDL_TST_CT"),
    F.col("DBTC_WITH_CTL_HBG_A1C_CT").alias("DBTC_WITH_CTL_HBG_A1C_CT"),
    F.col("DBTC_WITH_CTL_LIPIDS_CT").alias("DBTC_WITH_CTL_LIPIDS_CT"),
    F.col("DBTC_WITH_HBG_A1C_BIYRLY_CT").alias("DBTC_WITH_HBG_A1C_BIYRLY_CT"),
    F.col("CLS_ID").alias("CLS_ID"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY")
)

# ----------------------------------------------------------------------------------------------------
# xfm_BusinessLogic (CTransformerStage)
# ----------------------------------------------------------------------------------------------------
df_businesslogic_in = df_lkp_CdMppng_out

# 1) lnkEdwEdwMbrMedAlertSumF_Out (constraint: MBR_MED_MESRS_SK != 0 AND != 1)
df_lnkeDWEdwMbrMedAlertSumF_Out = df_businesslogic_in.filter(
    (F.col("MBR_MED_MESRS_SK") != 0) & (F.col("MBR_MED_MESRS_SK") != 1)
)

df_lnkeDWEdwMbrMedAlertSumF_Out_trans = df_lnkeDWEdwMbrMedAlertSumF_Out.select(
    F.lit(0).alias("MBR_MED_ALERT_SUM_SK"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.when(F.col("CLS_SK").isNull(), F.lit(0)).otherwise(F.col("CLS_SK")).alias("CLS_SK"),
    F.when(F.col("GRP_SK").isNull(), F.lit(0)).otherwise(F.col("GRP_SK")).alias("GRP_SK"),
    F.when(F.col("MBR_SK").isNull(), F.lit(0)).otherwise(F.col("MBR_SK")).alias("MBR_SK"),
    F.col("MBR_MED_MESRS_SK").alias("MBR_MED_MESRS_SK"),
    F.when(F.col("SUB_SK").isNull(), F.lit(0)).otherwise(F.col("SUB_SK")).alias("SUB_SK"),
    F.when(F.col("SUBGRP_SK").isNull(), F.lit(0)).otherwise(F.col("SUBGRP_SK")).alias("SUBGRP_SK"),
    F.when(F.col("ASTHMTC_CT").isNull(), F.lit(0)).otherwise(F.lit(1)).alias("ASTHMTC_CT"),
    F.when(F.col("ASTHMTC_PRSTNT_WITH_ICS_CT").isNull(), F.lit(0)).otherwise(F.lit(1)).alias("ASTHMTC_PRSTNT_WITH_ICS_CT"),
    F.when(F.col("ASTHMTC_WITH_LABA_AND_ICS_CT").isNull(), F.lit(0)).otherwise(F.lit(1)).alias("ASTHMTC_WITH_LABA_AND_ICS_CT"),
    F.when(F.col("ASTHMTC_WITH_OV_SIX_MO_CT").isNull(), F.lit(0)).otherwise(F.lit(1)).alias("ASTHMTC_WITH_OV_SIX_MO_CT"),
    F.when(F.col("CAD_CT").isNull(), F.lit(0)).otherwise(F.lit(1)).alias("CAD_CT"),
    F.when(F.col("CAD_WITH_LDL_TST_CT").isNull(), F.lit(0)).otherwise(F.lit(1)).alias("CAD_WITH_LDL_TST_CT"),
    F.when(F.col("CAD_WITH_STATIN_CT").isNull(), F.lit(0)).otherwise(F.lit(1)).alias("CAD_WITH_STATIN_CT"),
    F.when(F.col("CHF_CT").isNull(), F.lit(0)).otherwise(F.lit(1)).alias("CHF_CT"),
    F.when(F.col("CHF_WITH_ACE_INHBTR_CT").isNull(), F.lit(0)).otherwise(F.lit(1)).alias("CHF_WITH_ACE_INHBTR_CT"),
    F.when(F.col("CHF_WITH_ANUL_PHYS_VST_CT").isNull(), F.lit(0)).otherwise(F.lit(1)).alias("CHF_WITH_ANUL_PHYS_VST_CT"),
    F.when(F.col("COPD_CT").isNull(), F.lit(0)).otherwise(F.lit(1)).alias("COPD_CT"),
    F.when(F.col("COPD_WITH_ANUL_PHYS_VST_CT").isNull(), F.lit(0)).otherwise(F.lit(1)).alias("COPD_WITH_ANUL_PHYS_VST_CT"),
    F.when(F.col("COPD_WITH_RX_CMPLNC_CT").isNull(), F.lit(0)).otherwise(F.lit(1)).alias("COPD_WITH_RX_CMPLNC_CT"),
    F.when(F.col("DBTC_CT").isNull(), F.lit(0)).otherwise(F.lit(1)).alias("DBTC_CT"),
    F.when(F.col("DBTC_WITH_ANUL_EYE_EXAM_CT").isNull(), F.lit(0)).otherwise(F.lit(1)).alias("DBTC_WITH_ANUL_EYE_EXAM_CT"),
    F.when(F.col("DBTC_WITH_ANUL_MICROAL_TST_CT").isNull(), F.lit(0)).otherwise(F.lit(1)).alias("DBTC_WITH_ANUL_MICROAL_TST_CT"),
    F.when(F.col("DBTC_WITH_ANUL_LDL_TST_CT").isNull(), F.lit(0)).otherwise(F.lit(1)).alias("DBTC_WITH_ANUL_LDL_TST_CT"),
    F.when(F.col("DBTC_WITH_CTL_HBG_A1C_CT").isNull(), F.lit(0)).otherwise(F.lit(1)).alias("DBTC_WITH_CTL_HBG_A1C_CT"),
    F.when(F.col("DBTC_WITH_CTL_LIPIDS_CT").isNull(), F.lit(0)).otherwise(F.lit(1)).alias("DBTC_WITH_CTL_LIPIDS_CT"),
    F.when(F.col("DBTC_WITH_HBG_A1C_BIYRLY_CT").isNull(), F.lit(0)).otherwise(F.lit(1)).alias("DBTC_WITH_HBG_A1C_BIYRLY_CT"),
    F.when(F.col("CLS_ID").isNull(), F.lit("UNK")).otherwise(F.col("CLS_ID")).alias("CLS_ID"),
    F.when(F.col("GRP_ID").isNull(), F.lit("UNK")).otherwise(F.col("GRP_ID")).alias("GRP_ID"),
    F.when(F.col("SUBGRP_ID").isNull(), F.lit("UNK")).otherwise(F.col("SUBGRP_ID")).alias("SUBGRP_ID"),
    F.when(F.col("SUB_UNIQ_KEY").isNull(), F.lit(0)).otherwise(F.col("SUB_UNIQ_KEY")).alias("SUB_UNIQ_KEY"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# 2) Lnk_NA_Out (constraint: only first row, with constants)
df_lnk_NA_Out = df_businesslogic_in.limit(1).select(
    F.lit(1).alias("MBR_MED_ALERT_SUM_SK"),
    F.lit("NA").alias("SRC_SYS_CD"),
    F.lit(1).alias("MBR_UNIQ_KEY"),
    F.lit("175301").alias("PRCS_YR_MO_SK"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(1).alias("CLS_SK"),
    F.lit(1).alias("GRP_SK"),
    F.lit(1).alias("MBR_SK"),
    F.lit(1).alias("MBR_MED_MESRS_SK"),
    F.lit(1).alias("SUB_SK"),
    F.lit(1).alias("SUBGRP_SK"),
    F.lit(0).alias("ASTHMTC_CT"),
    F.lit(0).alias("ASTHMTC_PRSTNT_WITH_ICS_CT"),
    F.lit(0).alias("ASTHMTC_WITH_LABA_AND_ICS_CT"),
    F.lit(0).alias("ASTHMTC_WITH_OV_SIX_MO_CT"),
    F.lit(0).alias("CAD_CT"),
    F.lit(0).alias("CAD_WITH_LDL_TST_CT"),
    F.lit(0).alias("CAD_WITH_STATIN_CT"),
    F.lit(0).alias("CHF_CT"),
    F.lit(0).alias("CHF_WITH_ACE_INHBTR_CT"),
    F.lit(0).alias("CHF_WITH_ANUL_PHYS_VST_CT"),
    F.lit(0).alias("COPD_CT"),
    F.lit(0).alias("COPD_WITH_ANUL_PHYS_VST_CT"),
    F.lit(0).alias("COPD_WITH_RX_CMPLNC_CT"),
    F.lit(0).alias("DBTC_CT"),
    F.lit(0).alias("DBTC_WITH_ANUL_EYE_EXAM_CT"),
    F.lit(0).alias("DBTC_WITH_ANUL_MICROAL_TST_CT"),
    F.lit(0).alias("DBTC_WITH_ANUL_LDL_TST_CT"),
    F.lit(0).alias("DBTC_WITH_CTL_HBG_A1C_CT"),
    F.lit(0).alias("DBTC_WITH_CTL_LIPIDS_CT"),
    F.lit(0).alias("DBTC_WITH_HBG_A1C_BIYRLY_CT"),
    F.lit("NA").alias("CLS_ID"),
    F.lit("NA").alias("GRP_ID"),
    F.lit("NA").alias("SUBGRP_ID"),
    F.lit(1).alias("SUB_UNIQ_KEY"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# 3) Lnk_UNK_Out (constraint: only first row, with constants)
df_lnk_UNK_Out = df_businesslogic_in.limit(1).select(
    F.lit(0).alias("MBR_MED_ALERT_SUM_SK"),
    F.lit("UNK").alias("SRC_SYS_CD"),
    F.lit(0).alias("MBR_UNIQ_KEY"),
    F.lit("175301").alias("PRCS_YR_MO_SK"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(0).alias("CLS_SK"),
    F.lit(0).alias("GRP_SK"),
    F.lit(0).alias("MBR_SK"),
    F.lit(0).alias("MBR_MED_MESRS_SK"),
    F.lit(0).alias("SUB_SK"),
    F.lit(0).alias("SUBGRP_SK"),
    F.lit(0).alias("ASTHMTC_CT"),
    F.lit(0).alias("ASTHMTC_PRSTNT_WITH_ICS_CT"),
    F.lit(0).alias("ASTHMTC_WITH_LABA_AND_ICS_CT"),
    F.lit(0).alias("ASTHMTC_WITH_OV_SIX_MO_CT"),
    F.lit(0).alias("CAD_CT"),
    F.lit(0).alias("CAD_WITH_LDL_TST_CT"),
    F.lit(0).alias("CAD_WITH_STATIN_CT"),
    F.lit(0).alias("CHF_CT"),
    F.lit(0).alias("CHF_WITH_ACE_INHBTR_CT"),
    F.lit(0).alias("CHF_WITH_ANUL_PHYS_VST_CT"),
    F.lit(0).alias("COPD_CT"),
    F.lit(0).alias("COPD_WITH_ANUL_PHYS_VST_CT"),
    F.lit(0).alias("COPD_WITH_RX_CMPLNC_CT"),
    F.lit(0).alias("DBTC_CT"),
    F.lit(0).alias("DBTC_WITH_ANUL_EYE_EXAM_CT"),
    F.lit(0).alias("DBTC_WITH_ANUL_MICROAL_TST_CT"),
    F.lit(0).alias("DBTC_WITH_ANUL_LDL_TST_CT"),
    F.lit(0).alias("DBTC_WITH_CTL_HBG_A1C_CT"),
    F.lit(0).alias("DBTC_WITH_CTL_LIPIDS_CT"),
    F.lit(0).alias("DBTC_WITH_HBG_A1C_BIYRLY_CT"),
    F.lit("UNK").alias("CLS_ID"),
    F.lit("UNK").alias("GRP_ID"),
    F.lit("UNK").alias("SUBGRP_ID"),
    F.lit(0).alias("SUB_UNIQ_KEY"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# ----------------------------------------------------------------------------------------------------
# Funnel_145 (PxFunnel)
# ----------------------------------------------------------------------------------------------------
df_funnel_145_out = (
    df_lnk_UNK_Out
    .unionByName(df_lnk_NA_Out)
    .unionByName(df_lnkeDWEdwMbrMedAlertSumF_Out_trans)
)

# ----------------------------------------------------------------------------------------------------
# MBR_MED_ALERT_SUM_F (PxDataSet -> translate to Parquet)
# Write the final DataFrame with proper column order and rpad if char/varchar
# ----------------------------------------------------------------------------------------------------
df_final = df_funnel_145_out.select(
    F.col("MBR_MED_ALERT_SUM_SK").alias("MBR_MED_ALERT_SUM_SK"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.rpad(F.col("PRCS_YR_MO_SK"), 6, " ").alias("PRCS_YR_MO_SK"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("CLS_SK").alias("CLS_SK"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("MBR_MED_MESRS_SK").alias("MBR_MED_MESRS_SK"),
    F.col("SUB_SK").alias("SUB_SK"),
    F.col("SUBGRP_SK").alias("SUBGRP_SK"),
    F.col("ASTHMTC_CT").alias("ASTHMTC_CT"),
    F.col("ASTHMTC_PRSTNT_WITH_ICS_CT").alias("ASTHMTC_PRSTNT_WITH_ICS_CT"),
    F.col("ASTHMTC_WITH_LABA_AND_ICS_CT").alias("ASTHMTC_WITH_LABA_AND_ICS_CT"),
    F.col("ASTHMTC_WITH_OV_SIX_MO_CT").alias("ASTHMTC_WITH_OV_SIX_MO_CT"),
    F.col("CAD_CT").alias("CAD_CT"),
    F.col("CAD_WITH_LDL_TST_CT").alias("CAD_WITH_LDL_TST_CT"),
    F.col("CAD_WITH_STATIN_CT").alias("CAD_WITH_STATIN_CT"),
    F.col("CHF_CT").alias("CHF_CT"),
    F.col("CHF_WITH_ACE_INHBTR_CT").alias("CHF_WITH_ACE_INHBTR_CT"),
    F.col("CHF_WITH_ANUL_PHYS_VST_CT").alias("CHF_WITH_ANUL_PHYS_VST_CT"),
    F.col("COPD_CT").alias("COPD_CT"),
    F.col("COPD_WITH_ANUL_PHYS_VST_CT").alias("COPD_WITH_ANUL_PHYS_VST_CT"),
    F.col("COPD_WITH_RX_CMPLNC_CT").alias("COPD_WITH_RX_CMPLNC_CT"),
    F.col("DBTC_CT").alias("DBTC_CT"),
    F.col("DBTC_WITH_ANUL_EYE_EXAM_CT").alias("DBTC_WITH_ANUL_EYE_EXAM_CT"),
    F.col("DBTC_WITH_ANUL_MICROAL_TST_CT").alias("DBTC_WITH_ANUL_MICROAL_TST_CT"),
    F.col("DBTC_WITH_ANUL_LDL_TST_CT").alias("DBTC_WITH_ANUL_LDL_TST_CT"),
    F.col("DBTC_WITH_CTL_HBG_A1C_CT").alias("DBTC_WITH_CTL_HBG_A1C_CT"),
    F.col("DBTC_WITH_CTL_LIPIDS_CT").alias("DBTC_WITH_CTL_LIPIDS_CT"),
    F.col("DBTC_WITH_HBG_A1C_BIYRLY_CT").alias("DBTC_WITH_HBG_A1C_BIYRLY_CT"),
    F.col("CLS_ID").alias("CLS_ID"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

write_files(
    df_final,
    "MBR_MED_ALERT_SUM_F.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)