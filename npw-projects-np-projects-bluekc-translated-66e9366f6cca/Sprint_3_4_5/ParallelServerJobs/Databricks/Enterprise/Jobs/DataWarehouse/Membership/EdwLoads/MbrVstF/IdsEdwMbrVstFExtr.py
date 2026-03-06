# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2019 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALL BY: IdsEdwSpiraMemVstCntl
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC 	   Pulls data from IDS MBR_VST table and load into the EDW MBR_VST_FACT
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                                     Date                   Project       Description                       Environment                      Code Reviewer                      Review Date
# MAGIC ----------------------------------         --------------------------------    -------------     ------------------------------------      -----------------------------        ------------------------------------       -----------------------------------
# MAGIC Sravya Gorla                          04-19-2019           Spira             Initial Version                                      EnterpriseDev2                      Kalyan Neelam                       2019-04-22


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import *
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value("IDSOwner","")
EDWRunCycleDate = get_widget_value("EDWRunCycleDate","")
EDWRunCycle = get_widget_value("EDWRunCycle","")
IDSRunCycle = get_widget_value("IDSRunCycle","")
RunID = get_widget_value("RunID","")
EDWOwner = get_widget_value("EDWOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
edw_secret_name = get_widget_value("edw_secret_name","")

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_db2_MBR_VST = f"SELECT MBR_VST_SK, MBR_UNIQ_KEY, VST_ID, SRC_SYS_CD_SK, CRT_RUN_CYC_EXCTN_SK, LAST_UPDT_RUN_CYC_EXCTN_SK, PRI_POL_ID, SEC_POL_ID, PATN_EXTRNL_VNDR_ID, VST_DT_SK, VST_STTUS_CD, VST_CLSD_DT_SK, BILL_RVWED_DT_SK, BILL_RVWED_BY_NM, PROV_EXTRNL_VNDR_ID, SVC_PROV_NTNL_PROV_ID, SVC_PROV_SK, REL_GRP_PROV_SK, REL_IPA_PROV_SK, MBR_SK FROM {IDSOwner}.MBR_VST WHERE LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSRunCycle}"
df_db2_MBR_VST = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_MBR_VST)
    .load()
)

jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)
extract_query_db2_MBR_D = f"SELECT MBR_SK, MBR_FULL_NM, GRP_SK, GRP_ID, GRP_NM FROM {EDWOwner}.MBR_D"
df_db2_MBR_D = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", extract_query_db2_MBR_D)
    .load()
)

extract_query_db2_PROV_D = f"SELECT PROV_SK, PROV_ID, PROV_NM FROM {EDWOwner}.PROV_D"
df_db2_PROV_D = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", extract_query_db2_PROV_D)
    .load()
)

df_CpyOfProvD_lnkRelGrpProvSk = df_db2_PROV_D.select(
    F.col("PROV_SK").alias("PROV_SK"),
    F.col("PROV_ID").alias("PROV_ID"),
    F.col("PROV_NM").alias("PROV_NM")
)

df_CpyOfProvD_lnkIpaProvSk = df_db2_PROV_D.select(
    F.col("PROV_SK").alias("PROV_SK"),
    F.col("PROV_ID").alias("PROV_ID"),
    F.col("PROV_NM").alias("PROV_NM")
)

df_CpyOfProvD_lnkSvcProvSk = df_db2_PROV_D.select(
    F.col("PROV_SK").alias("PROV_SK"),
    F.col("PROV_ID").alias("PROV_ID"),
    F.col("PROV_NM").alias("PROV_NM")
)

df_Lpk_Prov_SK = (
    df_db2_MBR_VST
    .join(
        df_CpyOfProvD_lnkSvcProvSk,
        df_db2_MBR_VST["SVC_PROV_SK"] == df_CpyOfProvD_lnkSvcProvSk["PROV_SK"],
        "left"
    )
    .join(
        df_CpyOfProvD_lnkIpaProvSk,
        df_db2_MBR_VST["REL_IPA_PROV_SK"] == df_CpyOfProvD_lnkIpaProvSk["PROV_SK"],
        "left"
    )
    .join(
        df_CpyOfProvD_lnkRelGrpProvSk,
        df_db2_MBR_VST["REL_GRP_PROV_SK"] == df_CpyOfProvD_lnkRelGrpProvSk["PROV_SK"],
        "left"
    )
    .select(
        df_db2_MBR_VST["MBR_VST_SK"].alias("MBR_VST_SK"),
        df_db2_MBR_VST["MBR_UNIQ_KEY"].alias("MBR_UNIQ_KEY"),
        df_db2_MBR_VST["VST_ID"].alias("VST_ID"),
        df_db2_MBR_VST["SRC_SYS_CD_SK"].alias("SRC_SYS_CD_SK"),
        df_db2_MBR_VST["CRT_RUN_CYC_EXCTN_SK"].alias("CRT_RUN_CYC_EXCTN_SK"),
        df_db2_MBR_VST["LAST_UPDT_RUN_CYC_EXCTN_SK"].alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        df_db2_MBR_VST["PRI_POL_ID"].alias("PRI_POL_ID"),
        df_db2_MBR_VST["SEC_POL_ID"].alias("SEC_POL_ID"),
        df_db2_MBR_VST["PATN_EXTRNL_VNDR_ID"].alias("PATN_EXTRNL_VNDR_ID"),
        df_db2_MBR_VST["VST_DT_SK"].alias("VST_DT_SK"),
        df_db2_MBR_VST["VST_STTUS_CD"].alias("VST_STTUS_CD"),
        df_db2_MBR_VST["VST_CLSD_DT_SK"].alias("VST_CLSD_DT_SK"),
        df_db2_MBR_VST["BILL_RVWED_DT_SK"].alias("BILL_RVWED_DT_SK"),
        df_db2_MBR_VST["BILL_RVWED_BY_NM"].alias("BILL_RVWED_BY_NM"),
        df_db2_MBR_VST["PROV_EXTRNL_VNDR_ID"].alias("PROV_EXTRNL_VNDR_ID"),
        df_db2_MBR_VST["SVC_PROV_NTNL_PROV_ID"].alias("SVC_PROV_NTNL_PROV_ID"),
        df_db2_MBR_VST["SVC_PROV_SK"].alias("SVC_PROV_SK"),
        df_db2_MBR_VST["REL_GRP_PROV_SK"].alias("REL_GRP_PROV_SK"),
        df_db2_MBR_VST["REL_IPA_PROV_SK"].alias("REL_IPA_PROV_SK"),
        df_db2_MBR_VST["MBR_SK"].alias("MBR_SK"),
        df_CpyOfProvD_lnkSvcProvSk["PROV_ID"].alias("SVC_PROV_ID"),
        df_CpyOfProvD_lnkSvcProvSk["PROV_NM"].alias("SVC_PROV_NM"),
        df_CpyOfProvD_lnkIpaProvSk["PROV_ID"].alias("IPA_PROV_ID"),
        df_CpyOfProvD_lnkIpaProvSk["PROV_NM"].alias("IPA_PROV_NM"),
        df_CpyOfProvD_lnkRelGrpProvSk["PROV_ID"].alias("REL_GRP_PROV_ID"),
        df_CpyOfProvD_lnkRelGrpProvSk["PROV_NM"].alias("REL_GRP_PROV_NM"),
    )
)

df_Lkp_Mbr_Sk = (
    df_Lpk_Prov_SK
    .join(
        df_db2_MBR_D,
        df_Lpk_Prov_SK["MBR_SK"] == df_db2_MBR_D["MBR_SK"],
        "left"
    )
    .select(
        df_Lpk_Prov_SK["MBR_VST_SK"].alias("MBR_VST_SK"),
        df_Lpk_Prov_SK["MBR_UNIQ_KEY"].alias("MBR_UNIQ_KEY"),
        df_Lpk_Prov_SK["VST_ID"].alias("VST_ID"),
        df_Lpk_Prov_SK["SRC_SYS_CD_SK"].alias("SRC_SYS_CD_SK"),
        df_Lpk_Prov_SK["CRT_RUN_CYC_EXCTN_SK"].alias("CRT_RUN_CYC_EXCTN_SK"),
        df_Lpk_Prov_SK["LAST_UPDT_RUN_CYC_EXCTN_SK"].alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        df_Lpk_Prov_SK["PRI_POL_ID"].alias("PRI_POL_ID"),
        df_Lpk_Prov_SK["SEC_POL_ID"].alias("SEC_POL_ID"),
        df_Lpk_Prov_SK["PATN_EXTRNL_VNDR_ID"].alias("PATN_EXTRNL_VNDR_ID"),
        df_Lpk_Prov_SK["VST_DT_SK"].alias("VST_DT_SK"),
        df_Lpk_Prov_SK["VST_STTUS_CD"].alias("VST_STTUS_CD"),
        df_Lpk_Prov_SK["VST_CLSD_DT_SK"].alias("VST_CLSD_DT_SK"),
        df_Lpk_Prov_SK["BILL_RVWED_DT_SK"].alias("BILL_RVWED_DT_SK"),
        df_Lpk_Prov_SK["BILL_RVWED_BY_NM"].alias("BILL_RVWED_BY_NM"),
        df_Lpk_Prov_SK["PROV_EXTRNL_VNDR_ID"].alias("PROV_EXTRNL_VNDR_ID"),
        df_Lpk_Prov_SK["SVC_PROV_NTNL_PROV_ID"].alias("SVC_PROV_NTNL_PROV_ID"),
        df_Lpk_Prov_SK["SVC_PROV_SK"].alias("SVC_PROV_SK"),
        df_Lpk_Prov_SK["REL_GRP_PROV_SK"].alias("REL_GRP_PROV_SK"),
        df_Lpk_Prov_SK["REL_IPA_PROV_SK"].alias("REL_IPA_PROV_SK"),
        df_Lpk_Prov_SK["MBR_SK"].alias("MBR_SK"),
        df_Lpk_Prov_SK["SVC_PROV_ID"].alias("SVC_PROV_ID"),
        df_Lpk_Prov_SK["SVC_PROV_NM"].alias("SVC_PROV_NM"),
        df_Lpk_Prov_SK["IPA_PROV_ID"].alias("IPA_PROV_ID"),
        df_Lpk_Prov_SK["IPA_PROV_NM"].alias("IPA_PROV_NM"),
        df_Lpk_Prov_SK["REL_GRP_PROV_ID"].alias("REL_GRP_PROV_ID"),
        df_Lpk_Prov_SK["REL_GRP_PROV_NM"].alias("REL_GRP_PROV_NM"),
        df_db2_MBR_D["MBR_FULL_NM"].alias("MBR_FULL_NM"),
        df_db2_MBR_D["GRP_SK"].alias("GRP_SK"),
        df_db2_MBR_D["GRP_ID"].alias("GRP_ID"),
        df_db2_MBR_D["GRP_NM"].alias("GRP_NM")
    )
)

df_xmf_businessLogic = df_Lkp_Mbr_Sk.select(
    F.col("MBR_VST_SK").alias("MBR_VST_SK"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("VST_ID").alias("VST_ID"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD"),
    F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("PRI_POL_ID").alias("PRI_POL_ID"),
    F.col("SEC_POL_ID").alias("SEC_POL_ID"),
    F.col("PATN_EXTRNL_VNDR_ID").alias("PATN_EXTRNL_VNDR_ID"),
    F.col("PROV_EXTRNL_VNDR_ID").alias("PROV_EXTRNL_VNDR_ID"),
    F.col("VST_DT_SK").alias("VST_DT_SK"),
    F.col("VST_STTUS_CD").alias("VST_STTUS_CD"),
    F.col("VST_CLSD_DT_SK").alias("VST_CLSD_DT_SK"),
    F.col("BILL_RVWED_DT_SK").alias("BILL_RVWED_DT_SK"),
    F.col("BILL_RVWED_BY_NM").alias("BILL_RVWED_BY_NM"),
    F.col("SVC_PROV_NTNL_PROV_ID").alias("SVC_PROV_NTNL_PROV_ID"),
    F.col("SVC_PROV_SK").alias("SVC_PROV_SK"),
    F.when(
        (F.col("SVC_PROV_ID").isNull()) | (F.col("SVC_PROV_ID") == ""),
        F.lit("UNK")
    ).otherwise(F.col("SVC_PROV_ID")).alias("SVC_PROV_ID"),
    F.when(
        (F.col("SVC_PROV_NM").isNull()) | (F.col("SVC_PROV_NM") == ""),
        F.lit("UNK")
    ).otherwise(F.col("SVC_PROV_NM")).alias("SVC_PROV_NM"),
    F.col("REL_IPA_PROV_SK").alias("REL_IPA_PROV_SK"),
    F.when(
        (F.col("IPA_PROV_ID").isNull()) | (F.col("IPA_PROV_ID") == ""),
        F.lit("UNK")
    ).otherwise(F.col("IPA_PROV_ID")).alias("REL_IPA_PROV_ID"),
    F.when(
        (F.col("IPA_PROV_NM").isNull()) | (F.col("IPA_PROV_NM") == ""),
        F.lit("UNK")
    ).otherwise(F.col("IPA_PROV_NM")).alias("REL_IPA_PROV_NM"),
    F.col("REL_GRP_PROV_SK").alias("REL_GRP_PROV_SK"),
    F.when(
        (F.col("REL_GRP_PROV_ID").isNull()) | (F.col("REL_GRP_PROV_ID") == ""),
        F.lit("UNK")
    ).otherwise(F.col("REL_GRP_PROV_ID")).alias("REL_GRP_PROV_ID"),
    F.when(
        (F.col("REL_GRP_PROV_NM").isNull()) | (F.col("REL_GRP_PROV_NM") == ""),
        F.lit("UNK")
    ).otherwise(F.col("REL_GRP_PROV_NM")).alias("REL_GRP_PROV_NM"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.when(
        (F.col("MBR_FULL_NM").isNull()) | (F.col("MBR_FULL_NM") == ""),
        F.lit("UNK")
    ).otherwise(F.col("MBR_FULL_NM")).alias("MBR_FULL_NM"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_MBR_VST_F_Load = df_xmf_businessLogic.select(
    F.col("MBR_VST_SK").alias("MBR_VST_SK"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("VST_ID").alias("VST_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("PRI_POL_ID").alias("PRI_POL_ID"),
    F.col("SEC_POL_ID").alias("SEC_POL_ID"),
    F.col("PATN_EXTRNL_VNDR_ID").alias("PATN_EXTRNL_VNDR_ID"),
    F.col("PROV_EXTRNL_VNDR_ID").alias("PROV_EXTRNL_VNDR_ID"),
    F.rpad(F.col("VST_DT_SK"), 10, " ").alias("VST_DT_SK"),
    F.col("VST_STTUS_CD").alias("VST_STTUS_CD"),
    F.rpad(F.col("VST_CLSD_DT_SK"), 10, " ").alias("VST_CLSD_DT_SK"),
    F.rpad(F.col("BILL_RVWED_DT_SK"), 10, " ").alias("BILL_RVWED_DT_SK"),
    F.col("BILL_RVWED_BY_NM").alias("BILL_RVWED_BY_NM"),
    F.col("SVC_PROV_NTNL_PROV_ID").alias("SVC_PROV_NTNL_PROV_ID"),
    F.col("SVC_PROV_SK").alias("SVC_PROV_SK"),
    F.col("SVC_PROV_ID").alias("SVC_PROV_ID"),
    F.col("SVC_PROV_NM").alias("SVC_PROV_NM"),
    F.col("REL_IPA_PROV_SK").alias("REL_IPA_PROV_SK"),
    F.col("REL_IPA_PROV_ID").alias("REL_IPA_PROV_ID"),
    F.col("REL_IPA_PROV_NM").alias("REL_IPA_PROV_NM"),
    F.col("REL_GRP_PROV_SK").alias("REL_GRP_PROV_SK"),
    F.col("REL_GRP_PROV_ID").alias("REL_GRP_PROV_ID"),
    F.col("REL_GRP_PROV_NM").alias("REL_GRP_PROV_NM"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("MBR_FULL_NM").alias("MBR_FULL_NM"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

write_files(
    df_MBR_VST_F_Load,
    f"{adls_path}/load/MBR_VST_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)