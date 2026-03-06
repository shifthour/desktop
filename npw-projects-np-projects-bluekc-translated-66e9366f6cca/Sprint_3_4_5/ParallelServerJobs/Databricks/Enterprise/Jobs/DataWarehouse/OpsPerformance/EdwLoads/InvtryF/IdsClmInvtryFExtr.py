# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2015 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     IdsOpsDashboardClmInvtryCntl
# MAGIC 
# MAGIC Processing:
# MAGIC                     *
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC              Previous Run Successful:    Restart, no other steps necessary
# MAGIC              Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                               Project/                                                                                                 Code                  Date
# MAGIC Developer        Date              Altiris #           Change Description                         Enviroment              Reviewer            Reviewed
# MAGIC ----------------------  -------------------   -------------------   --------------------------------------------------- ---------------------------------   ----------------------     -------------------   
# MAGIC ADasarathy    07/19/2015     5407               Original Program                                EnterpriseDev1    Kalyan Neelam    07/20/2015

# MAGIC JobName: IdsEdwClmInvtryFExtr
# MAGIC Job creates loadfile for CLM_INVTRY_F  in EDW
# MAGIC Write CLM_INVTRY_F Data into a Sequential file for Load Job IdsEdwClmInvtryFLoad.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC ASG_USER_SK,
# MAGIC GRP_SK,
# MAGIC PROD_SK,
# MAGIC PROV_SK,
# MAGIC OPS_WORK_UNIT_SK.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
CurrDate = get_widget_value('CurrDate','')
CurrRunCycle = get_widget_value('CurrRunCycle','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

extract_query_db2_CLM_INVNTRY_in = (
    f"SELECT CLM_INVTRY_SK,SRC_SYS_CD_SK,CLM_INVTRY_KEY_ID,CRT_RUN_CYC_EXCTN_SK,"
    f"LAST_UPDT_RUN_CYC_EXCTN_SK,GRP_SK,OPS_WORK_UNIT_SK,PROD_SK,PROV_SK,"
    f"CLM_INVTRY_PEND_CAT_CD_SK,CLM_STTUS_CHG_RSN_CD_SK,CLM_STTUS_CD_SK,CLM_SUBTYP_CD_SK,"
    f"CLM_TYP_CD_SK,INPT_DT_SK,RCVD_DT_SK,EXTR_DT_SK,STTUS_DT_SK,INVTRY_CT,ASG_USER_SK,"
    f"WORK_ITEM_CT FROM {IDSOwner}.CLM_INVTRY"
)
df_db2_CLM_INVNTRY_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CLM_INVNTRY_in)
    .load()
)

extract_query_db2_APP_USER_Lkup = f"SELECT USER_SK, USER_ID FROM {IDSOwner}.APP_USER;"
df_db2_APP_USER_Lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_APP_USER_Lkup)
    .load()
)

extract_query_db2_GRP_Lkup = f"SELECT GRP_SK, GRP_ID FROM {IDSOwner}.GRP;"
df_db2_GRP_Lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_GRP_Lkup)
    .load()
)

extract_query_db2_PROD_Lkup = f"SELECT PROD_SK, PROD_ID FROM {IDSOwner}.PROD;"
df_db2_PROD_Lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_PROD_Lkup)
    .load()
)

extract_query_db2_PROV_Lkup = f"SELECT PROV_SK, PROV_ID FROM {IDSOwner}.PROV"
df_db2_PROV_Lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_PROV_Lkup)
    .load()
)

extract_query_db2_OPS_WORK_UNIT_Lkup = (
    f"SELECT SRC_VAL_TX, OPS_WORK_UNIT_SK FROM {IDSOwner}.P_OPS_WORK_UNIT_XREF "
    f"WHERE OPS_WORK_UNIT_SK NOT IN (0,1)"
)
df_db2_OPS_WORK_UNIT_Lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_OPS_WORK_UNIT_Lkup)
    .load()
)

extract_query_db2_CD_MPPNG_Extr = (
    f"SELECT CD_MPPNG_SK,COALESCE(TRGT_CD,'NA') TRGT_CD,COALESCE(TRGT_CD_NM,'NA') TRGT_CD_NM "
    f"FROM {IDSOwner}.CD_MPPNG"
)
df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CD_MPPNG_Extr)
    .load()
)

df_Buffer = df_db2_CD_MPPNG_Extr

df_ref_CLM_INVTRY_PEND_CAT_Lkup = df_Buffer.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_ref_CLM_SUBTYP_Lkup = df_Buffer.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_ref_CLM_TYP_Lkup = df_Buffer.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_ref_CLM_STTUS_CHG_RSN_Lkup = df_Buffer.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_ref_CLM_STTUS_Lkup = df_Buffer.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_ref_SRC_SYS_CD_Lkup = df_Buffer.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Lookup = (
    df_db2_CLM_INVNTRY_in.alias("lnk_Extract")
    .join(
        df_ref_CLM_INVTRY_PEND_CAT_Lkup.alias("ref_CLM_INVTRY_PEND_CAT_Lkup"),
        F.col("lnk_Extract.CLM_INVTRY_PEND_CAT_CD_SK")
        == F.col("ref_CLM_INVTRY_PEND_CAT_Lkup.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_ref_CLM_STTUS_Lkup.alias("ref_CLM_STTUS_Lkup"),
        F.col("lnk_Extract.CLM_STTUS_CD_SK") == F.col("ref_CLM_STTUS_Lkup.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_ref_CLM_STTUS_CHG_RSN_Lkup.alias("ref_CLM_STTUS_CHG_RSN_Lkup"),
        F.col("lnk_Extract.CLM_STTUS_CHG_RSN_CD_SK")
        == F.col("ref_CLM_STTUS_CHG_RSN_Lkup.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_ref_CLM_TYP_Lkup.alias("ref_CLM_TYP_Lkup"),
        F.col("lnk_Extract.CLM_TYP_CD_SK") == F.col("ref_CLM_TYP_Lkup.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_ref_CLM_SUBTYP_Lkup.alias("ref_CLM_SUBTYP_Lkup"),
        F.col("lnk_Extract.CLM_SUBTYP_CD_SK") == F.col("ref_CLM_SUBTYP_Lkup.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_ref_SRC_SYS_CD_Lkup.alias("ref_SRC_SYS_CD_Lkup"),
        F.col("lnk_Extract.SRC_SYS_CD_SK") == F.col("ref_SRC_SYS_CD_Lkup.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_db2_APP_USER_Lkup.alias("ref_APP_USER_Lkup"),
        F.col("lnk_Extract.ASG_USER_SK") == F.col("ref_APP_USER_Lkup.USER_SK"),
        "left",
    )
    .join(
        df_db2_GRP_Lkup.alias("ref_GRP_Lkup"),
        F.col("lnk_Extract.GRP_SK") == F.col("ref_GRP_Lkup.GRP_SK"),
        "left",
    )
    .join(
        df_db2_PROD_Lkup.alias("ref_PROD_Lkup"),
        F.col("lnk_Extract.PROD_SK") == F.col("ref_PROD_Lkup.PROD_SK"),
        "left",
    )
    .join(
        df_db2_PROV_Lkup.alias("ref_PROV_Lkup"),
        F.col("lnk_Extract.PROV_SK") == F.col("ref_PROV_Lkup.PROV_SK"),
        "left",
    )
    .join(
        df_db2_OPS_WORK_UNIT_Lkup.alias("ref_OPS_WORK_UNIT_Lkup"),
        F.col("lnk_Extract.OPS_WORK_UNIT_SK") == F.col("ref_OPS_WORK_UNIT_Lkup.OPS_WORK_UNIT_SK"),
        "left",
    )
    .select(
        F.col("lnk_Extract.CLM_INVTRY_SK").alias("CLM_INVTRY_SK"),
        F.col("ref_SRC_SYS_CD_Lkup.TRGT_CD").alias("SRC_SYS_CD"),
        F.col("lnk_Extract.CLM_INVTRY_KEY_ID").alias("CLM_INVTRY_KEY_ID"),
        F.col("lnk_Extract.ASG_USER_SK").alias("ASSIGNED_APPLICATION_USER_SK"),
        F.col("lnk_Extract.GRP_SK").alias("GRP_SK"),
        F.col("lnk_Extract.OPS_WORK_UNIT_SK").alias("OPS_WORK_UNIT_SK"),
        F.col("lnk_Extract.PROD_SK").alias("PROD_SK"),
        F.col("lnk_Extract.PROV_SK").alias("PROV_SK"),
        F.col("ref_CLM_INVTRY_PEND_CAT_Lkup.TRGT_CD").alias("CLM_INVTRY_PEND_CAT_CD"),
        F.col("ref_CLM_INVTRY_PEND_CAT_Lkup.TRGT_CD_NM").alias("CLM_INVTRY_PEND_CAT_NM"),
        F.col("ref_CLM_STTUS_Lkup.TRGT_CD").alias("CLM_STTUS_CD"),
        F.col("ref_CLM_STTUS_Lkup.TRGT_CD_NM").alias("CLM_STTUS_NM"),
        F.col("ref_CLM_STTUS_CHG_RSN_Lkup.TRGT_CD").alias("CLM_STTUS_CHG_RSN_CD"),
        F.col("ref_CLM_STTUS_CHG_RSN_Lkup.TRGT_CD_NM").alias("CLM_STTUS_CHG_RSN_NM"),
        F.col("ref_CLM_SUBTYP_Lkup.TRGT_CD").alias("CLM_SUBTYP_CD"),
        F.col("ref_CLM_SUBTYP_Lkup.TRGT_CD_NM").alias("CLM_SUBTYP_NM"),
        F.col("ref_CLM_TYP_Lkup.TRGT_CD").alias("CLM_TYP_CD"),
        F.col("ref_CLM_TYP_Lkup.TRGT_CD_NM").alias("CLM_TYP_NM"),
        F.col("lnk_Extract.EXTR_DT_SK").alias("EXTR_DT_SK"),
        F.col("lnk_Extract.INPT_DT_SK").alias("INPT_DT_SK"),
        F.col("lnk_Extract.RCVD_DT_SK").alias("RCVD_DT_SK"),
        F.col("lnk_Extract.STTUS_DT_SK").alias("STTUS_DT_SK"),
        F.col("lnk_Extract.INVTRY_CT").alias("INVTRY_CT"),
        F.col("lnk_Extract.WORK_ITEM_CT").alias("WORK_ITEM_CT"),
        F.col("ref_APP_USER_Lkup.USER_ID").alias("APP_USER_ID"),
        F.col("ref_GRP_Lkup.GRP_ID").alias("GRP_ID"),
        F.col("ref_PROD_Lkup.PROD_ID").alias("PROD_ID"),
        F.col("ref_PROV_Lkup.PROV_ID").alias("PROV_ID"),
        F.col("ref_OPS_WORK_UNIT_Lkup.SRC_VAL_TX").alias("OPS_WORK_UNIT_ID"),
        F.col("lnk_Extract.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_Extract.CLM_INVTRY_PEND_CAT_CD_SK").alias("CLM_INVTRY_PEND_CAT_CD_SK"),
        F.col("lnk_Extract.CLM_STTUS_CD_SK").alias("CLM_STTUS_CD_SK"),
        F.col("lnk_Extract.CLM_STTUS_CHG_RSN_CD_SK").alias("CLM_STTUS_CHG_RSN_CD_SK"),
        F.col("lnk_Extract.CLM_SUBTYP_CD_SK").alias("CLM_SUBTYP_CD_SK"),
        F.col("lnk_Extract.CLM_TYP_CD_SK").alias("CLM_TYP_CD_SK"),
    )
)

df_xfm_BusinessLogic = df_Lookup.select(
    F.col("CLM_INVTRY_SK"),
    F.col("SRC_SYS_CD"),
    F.col("CLM_INVTRY_KEY_ID"),
    F.lit(CurrDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(CurrDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("ASSIGNED_APPLICATION_USER_SK"),
    F.col("GRP_SK"),
    F.col("OPS_WORK_UNIT_SK"),
    F.col("PROD_SK"),
    F.col("PROV_SK"),
    F.col("CLM_INVTRY_PEND_CAT_CD"),
    F.col("CLM_INVTRY_PEND_CAT_NM"),
    F.col("CLM_STTUS_CD"),
    F.col("CLM_STTUS_NM"),
    F.col("CLM_STTUS_CHG_RSN_CD"),
    F.col("CLM_STTUS_CHG_RSN_NM"),
    F.col("CLM_SUBTYP_CD"),
    F.col("CLM_SUBTYP_NM"),
    F.col("CLM_TYP_CD"),
    F.col("CLM_TYP_NM"),
    F.col("EXTR_DT_SK"),
    F.col("INPT_DT_SK"),
    F.col("RCVD_DT_SK"),
    F.col("STTUS_DT_SK"),
    F.col("INVTRY_CT"),
    F.col("WORK_ITEM_CT"),
    F.col("APP_USER_ID"),
    F.col("GRP_ID"),
    F.col("PROD_ID"),
    F.col("PROV_ID"),
    F.col("OPS_WORK_UNIT_ID"),
    F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_INVTRY_PEND_CAT_CD_SK"),
    F.col("CLM_STTUS_CD_SK"),
    F.col("CLM_STTUS_CHG_RSN_CD_SK"),
    F.col("CLM_SUBTYP_CD_SK"),
    F.col("CLM_TYP_CD_SK"),
)

df_Seq_CLM_INVTRY_F = df_xfm_BusinessLogic.select(
    F.col("CLM_INVTRY_SK"),
    F.col("SRC_SYS_CD"),
    F.col("CLM_INVTRY_KEY_ID"),
    rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("ASSIGNED_APPLICATION_USER_SK"),
    F.col("GRP_SK"),
    F.col("OPS_WORK_UNIT_SK"),
    F.col("PROD_SK"),
    F.col("PROV_SK"),
    F.col("CLM_INVTRY_PEND_CAT_CD"),
    F.col("CLM_INVTRY_PEND_CAT_NM"),
    F.col("CLM_STTUS_CD"),
    F.col("CLM_STTUS_NM"),
    F.col("CLM_STTUS_CHG_RSN_CD"),
    F.col("CLM_STTUS_CHG_RSN_NM"),
    F.col("CLM_SUBTYP_CD"),
    F.col("CLM_SUBTYP_NM"),
    F.col("CLM_TYP_CD"),
    F.col("CLM_TYP_NM"),
    rpad(F.col("EXTR_DT_SK"), 10, " ").alias("EXTR_DT_SK"),
    rpad(F.col("INPT_DT_SK"), 10, " ").alias("INPT_DT_SK"),
    rpad(F.col("RCVD_DT_SK"), 10, " ").alias("RCVD_DT_SK"),
    rpad(F.col("STTUS_DT_SK"), 10, " ").alias("STTUS_DT_SK"),
    F.col("INVTRY_CT"),
    F.col("WORK_ITEM_CT"),
    F.col("APP_USER_ID"),
    F.col("GRP_ID"),
    F.col("PROD_ID"),
    F.col("PROV_ID"),
    F.col("OPS_WORK_UNIT_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_INVTRY_PEND_CAT_CD_SK"),
    F.col("CLM_STTUS_CD_SK"),
    F.col("CLM_STTUS_CHG_RSN_CD_SK"),
    F.col("CLM_SUBTYP_CD_SK"),
    F.col("CLM_TYP_CD_SK"),
)

write_files(
    df_Seq_CLM_INVTRY_F,
    f"{adls_path}/load/CLM_INVTRY_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)