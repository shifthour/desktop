# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2015 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls records from IDS, lookups CD_MPPNG table. This job output is used in EDW load job.  
# MAGIC       
# MAGIC                                                
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                                            Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                                       ----------------------------------   ---------------------------------    -------------------------   
# MAGIC                
# MAGIC 
# MAGIC Raja Gummadi               2015-10-13             5328                          Original Programming                                                              EnterpriseDev2             Kalyan Neelam              2015-10-14

# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC 
# MAGIC CD_MPPNG_SK
# MAGIC SUB_EXCH_CHAN_CD_SK
# MAGIC SUB_EXCH_ENR_METH_CD_SK
# MAGIC SUB_EXCH_TYP_CD_SK
# MAGIC Write MBR_ENR_D Data into a Sequential file for Load Job IdsEdwMbrEnrDLoad.
# MAGIC Read all the Data from IDS MBR_ENR Table;
# MAGIC Add Defaults and Null Handling.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Fkey Lookup for CD SK Mapping.
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC 
# MAGIC QHP_SK
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
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"SELECT CD_MPPNG_SK, COALESCE(TRGT_CD,'UNK') TRGT_CD, COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM FROM {IDSOwner}.CD_MPPNG"
    )
    .load()
)

df_cpy_cd_mppng = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_db2_MBR_EXCH_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"SELECT DISTINCT ME.MBR_EXCH_SK, COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD, ME.MBR_UNIQ_KEY, ME.EFF_DT_SK, ME.CRT_RUN_CYC_EXCTN_SK, ME.LAST_UPDT_RUN_CYC_EXCTN_SK, ME.MBR_SK, ME.QHP_SK, ME.SUB_EXCH_CHAN_CD_SK, ME.SUB_EXCH_ENR_METH_CD_SK, ME.SUB_EXCH_TYP_CD_SK, ME.APTC_IN, ME.SRC_SYS_LAST_UPDT_DTM, ME.TERM_DT_SK, ME.EXCH_MBR_ID, ME.EXCH_POL_ID, ME.PAYMT_TRANS_ID FROM {IDSOwner}.MBR_EXCH ME LEFT JOIN {IDSOwner}.CD_MPPNG CD ON ME.SRC_SYS_CD_SK = CD.CD_MPPNG_SK AND ME.MBR_EXCH_SK NOT IN (0,1)"
    )
    .load()
)

df_db2_QHP_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"SELECT QHP.QHP_SK, QHP.QHP_ID FROM {IDSOwner}.QHP QHP"
    )
    .load()
)

df_lkp_Codes1 = (
    df_db2_MBR_EXCH_in.alias("lnk_IdsEdwMbrExchDExtr_InABC")
    .join(
        df_db2_QHP_in.alias("Ref_QhpSk_Lkup"),
        F.col("lnk_IdsEdwMbrExchDExtr_InABC.QHP_SK") == F.col("Ref_QhpSk_Lkup.QHP_SK"),
        "left"
    )
    .select(
        F.col("lnk_IdsEdwMbrExchDExtr_InABC.MBR_EXCH_SK").alias("MBR_EXCH_SK"),
        F.col("lnk_IdsEdwMbrExchDExtr_InABC.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("lnk_IdsEdwMbrExchDExtr_InABC.EFF_DT_SK").alias("MBR_EXCH_EFF_DT_SK"),
        F.col("lnk_IdsEdwMbrExchDExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_IdsEdwMbrExchDExtr_InABC.MBR_SK").alias("MBR_SK"),
        F.col("lnk_IdsEdwMbrExchDExtr_InABC.QHP_SK").alias("QHP_SK"),
        F.col("lnk_IdsEdwMbrExchDExtr_InABC.TERM_DT_SK").alias("MBR_EXCH_TERM_DT_SK"),
        F.col("lnk_IdsEdwMbrExchDExtr_InABC.SRC_SYS_LAST_UPDT_DTM").alias("SRC_SYS_LAST_UPDT_DTM"),
        F.col("lnk_IdsEdwMbrExchDExtr_InABC.EXCH_MBR_ID").alias("EXCH_MBR_ID"),
        F.col("lnk_IdsEdwMbrExchDExtr_InABC.EXCH_POL_ID").alias("EXCH_POL_ID"),
        F.col("Ref_QhpSk_Lkup.QHP_ID").alias("QHP_ID"),
        F.col("lnk_IdsEdwMbrExchDExtr_InABC.APTC_IN").alias("APTC_IN"),
        F.col("lnk_IdsEdwMbrExchDExtr_InABC.PAYMT_TRANS_ID").alias("PAYMT_TRANS_ID"),
        F.col("lnk_IdsEdwMbrExchDExtr_InABC.SUB_EXCH_CHAN_CD_SK").alias("SUB_EXCH_CHAN_CD_SK"),
        F.col("lnk_IdsEdwMbrExchDExtr_InABC.SUB_EXCH_ENR_METH_CD_SK").alias("SUB_EXCH_ENR_METH_CD_SK"),
        F.col("lnk_IdsEdwMbrExchDExtr_InABC.SUB_EXCH_TYP_CD_SK").alias("SUB_EXCH_TYP_CD_SK")
    )
)

df_lkp_Codes = (
    df_lkp_Codes1.alias("lnk_FkeyLkpData_out")
    .join(
        df_cpy_cd_mppng.alias("Ref_SubExchCnlCd_Lkup"),
        F.col("lnk_FkeyLkpData_out.SUB_EXCH_CHAN_CD_SK") == F.col("Ref_SubExchCnlCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_cd_mppng.alias("Ref_SubExchEnrMethCd_Lkup"),
        F.col("lnk_FkeyLkpData_out.SUB_EXCH_ENR_METH_CD_SK") == F.col("Ref_SubExchEnrMethCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_cd_mppng.alias("Ref_SubExhTypCd_Lkup"),
        F.col("lnk_FkeyLkpData_out.SUB_EXCH_TYP_CD_SK") == F.col("Ref_SubExhTypCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .select(
        F.col("lnk_FkeyLkpData_out.MBR_EXCH_SK").alias("MBR_EXCH_SK"),
        F.col("lnk_FkeyLkpData_out.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("lnk_FkeyLkpData_out.MBR_EXCH_EFF_DT_SK").alias("MBR_EXCH_EFF_DT_SK"),
        F.col("lnk_FkeyLkpData_out.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_FkeyLkpData_out.MBR_SK").alias("MBR_SK"),
        F.col("lnk_FkeyLkpData_out.QHP_SK").alias("QHP_SK"),
        F.col("Ref_SubExchCnlCd_Lkup.TRGT_CD").alias("SUB_EXCH_CHAN_CD"),
        F.col("Ref_SubExchCnlCd_Lkup.TRGT_CD_NM").alias("SUB_EXCH_CHAN_NM"),
        F.col("Ref_SubExchEnrMethCd_Lkup.TRGT_CD").alias("SUB_EXCH_ENR_METH_CD"),
        F.col("Ref_SubExchEnrMethCd_Lkup.TRGT_CD_NM").alias("SUB_EXCH_ENR_METH_NM"),
        F.col("Ref_SubExchExhTypCd_Lkup.TRGT_CD").alias("SUB_EXCH_TYP_CD"),
        F.col("Ref_SubExchExhTypCd_Lkup.TRGT_CD_NM").alias("SUB_EXCH_TYP_NM"),
        F.col("lnk_FkeyLkpData_out.APTC_IN").alias("QHP_APTC_IN"),
        F.col("lnk_FkeyLkpData_out.MBR_EXCH_TERM_DT_SK").alias("MBR_EXCH_TERM_DT_SK"),
        F.col("lnk_FkeyLkpData_out.SRC_SYS_LAST_UPDT_DTM").alias("SRC_SYS_LAST_UPDT_DTM"),
        F.col("lnk_FkeyLkpData_out.EXCH_MBR_ID").alias("EXCH_MBR_ID"),
        F.col("lnk_FkeyLkpData_out.EXCH_POL_ID").alias("EXCH_POL_ID"),
        F.col("lnk_FkeyLkpData_out.QHP_ID").alias("QHP_ID"),
        F.col("lnk_FkeyLkpData_out.PAYMT_TRANS_ID").alias("PAYMT_TRANS_ID"),
        F.col("lnk_FkeyLkpData_out.SUB_EXCH_CHAN_CD_SK").alias("SUB_EXCH_CHAN_CD_SK"),
        F.col("lnk_FkeyLkpData_out.SUB_EXCH_ENR_METH_CD_SK").alias("SUB_EXCH_ENR_METH_CD_SK"),
        F.col("lnk_FkeyLkpData_out.SUB_EXCH_TYP_CD_SK").alias("SUB_EXCH_TYP_CD_SK")
    )
)

df_xfrm_BusinessLogic_OutABC = df_lkp_Codes.select(
    F.col("MBR_EXCH_SK"),
    F.col("MBR_UNIQ_KEY"),
    F.col("MBR_EXCH_EFF_DT_SK"),
    F.col("SRC_SYS_CD"),
    F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("MBR_SK"),
    F.col("QHP_SK"),
    F.when(trim(F.col("SUB_EXCH_CHAN_CD")) == "", "UNK").otherwise(F.col("SUB_EXCH_CHAN_CD")).alias("SUB_EXCH_CHAN_CD"),
    F.when(trim(F.col("SUB_EXCH_CHAN_NM")) == "", "UNK").otherwise(F.col("SUB_EXCH_CHAN_NM")).alias("SUB_EXCH_CHAN_NM"),
    F.when(trim(F.col("SUB_EXCH_ENR_METH_CD")) == "", "UNK").otherwise(F.col("SUB_EXCH_ENR_METH_CD")).alias("SUB_EXCH_ENR_METH_CD"),
    F.when(trim(F.col("SUB_EXCH_ENR_METH_NM")) == "", "UNK").otherwise(F.col("SUB_EXCH_ENR_METH_NM")).alias("SUB_EXCH_ENR_METH_NM"),
    F.when(trim(F.col("SUB_EXCH_TYP_CD")) == "", "UNK").otherwise(F.col("SUB_EXCH_TYP_CD")).alias("SUB_EXCH_TYP_CD"),
    F.when(trim(F.col("SUB_EXCH_TYP_NM")) == "", "UNK").otherwise(F.col("SUB_EXCH_TYP_NM")).alias("SUB_EXCH_TYP_NM"),
    F.col("QHP_APTC_IN"),
    F.when(
        F.col("QHP_ID").substr(F.length(F.col("QHP_ID")) - 1, 2).isin("00", "01"),
        "N"
    ).otherwise("Y").alias("QHP_CSR_IN"),
    F.col("MBR_EXCH_TERM_DT_SK"),
    F.col("SRC_SYS_LAST_UPDT_DTM"),
    F.col("EXCH_MBR_ID"),
    F.col("EXCH_POL_ID"),
    F.col("QHP_ID"),
    F.col("PAYMT_TRANS_ID"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("SUB_EXCH_CHAN_CD_SK"),
    F.col("SUB_EXCH_ENR_METH_CD_SK"),
    F.col("SUB_EXCH_TYP_CD_SK")
)

df_xfrm_BusinessLogic_NA = df_lkp_Codes.limit(1).select(
    F.lit(1).alias("MBR_EXCH_SK"),
    F.lit(0).alias("MBR_UNIQ_KEY"),
    F.lit("1753-01-01").alias("MBR_EXCH_EFF_DT_SK"),
    F.lit("NA").alias("SRC_SYS_CD"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(1).alias("MBR_SK"),
    F.lit(1).alias("QHP_SK"),
    F.lit("NA").alias("SUB_EXCH_CHAN_CD"),
    F.lit("NA").alias("SUB_EXCH_CHAN_NM"),
    F.lit("NA").alias("SUB_EXCH_ENR_METH_CD"),
    F.lit("NA").alias("SUB_EXCH_ENR_METH_NM"),
    F.lit("NA").alias("SUB_EXCH_TYP_CD"),
    F.lit("NA").alias("SUB_EXCH_TYP_NM"),
    F.lit("N").alias("QHP_APTC_IN"),
    F.lit("N").alias("QHP_CSR_IN"),
    F.lit("2199-12-31").alias("MBR_EXCH_TERM_DT_SK"),
    F.lit("1753-01-01-00.00.00.000000").alias("SRC_SYS_LAST_UPDT_DTM"),
    F.lit("NA").alias("EXCH_MBR_ID"),
    F.lit("NA").alias("EXCH_POL_ID"),
    F.lit("NA").alias("QHP_ID"),
    F.lit("NA").alias("PAYMT_TRANS_ID"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("SUB_EXCH_CHAN_CD_SK"),
    F.lit(1).alias("SUB_EXCH_ENR_METH_CD_SK"),
    F.lit(1).alias("SUB_EXCH_TYP_CD_SK")
)

df_xfrm_BusinessLogic_UNK = df_lkp_Codes.limit(1).select(
    F.lit(0).alias("MBR_EXCH_SK"),
    F.lit(0).alias("MBR_UNIQ_KEY"),
    F.lit("1753-01-01").alias("MBR_EXCH_EFF_DT_SK"),
    F.lit("UNK").alias("SRC_SYS_CD"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(0).alias("MBR_SK"),
    F.lit(0).alias("QHP_SK"),
    F.lit("UNK").alias("SUB_EXCH_CHAN_CD"),
    F.lit("UNK").alias("SUB_EXCH_CHAN_NM"),
    F.lit("UNK").alias("SUB_EXCH_ENR_METH_CD"),
    F.lit("UNK").alias("SUB_EXCH_ENR_METH_NM"),
    F.lit("UNK").alias("SUB_EXCH_TYP_CD"),
    F.lit("UNK").alias("SUB_EXCH_TYP_NM"),
    F.lit("N").alias("QHP_APTC_IN"),
    F.lit("N").alias("QHP_CSR_IN"),
    F.lit("2199-12-31").alias("MBR_EXCH_TERM_DT_SK"),
    F.lit("1753-01-01-00.00.00.000000").alias("SRC_SYS_LAST_UPDT_DTM"),
    F.lit("UNK").alias("EXCH_MBR_ID"),
    F.lit("UNK").alias("EXCH_POL_ID"),
    F.lit("UNK").alias("QHP_ID"),
    F.lit("UNK").alias("PAYMT_TRANS_ID"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("SUB_EXCH_CHAN_CD_SK"),
    F.lit(0).alias("SUB_EXCH_ENR_METH_CD_SK"),
    F.lit(0).alias("SUB_EXCH_TYP_CD_SK")
)

df_Funnel_64 = df_xfrm_BusinessLogic_OutABC.unionByName(df_xfrm_BusinessLogic_NA).unionByName(df_xfrm_BusinessLogic_UNK)

df_final = df_Funnel_64.select(
    F.col("MBR_EXCH_SK"),
    F.col("MBR_UNIQ_KEY"),
    F.rpad(F.col("MBR_EXCH_EFF_DT_SK"), 10, " ").alias("MBR_EXCH_EFF_DT_SK"),
    F.col("SRC_SYS_CD"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("MBR_SK"),
    F.col("QHP_SK"),
    F.col("SUB_EXCH_CHAN_CD"),
    F.col("SUB_EXCH_CHAN_NM"),
    F.col("SUB_EXCH_ENR_METH_CD"),
    F.col("SUB_EXCH_ENR_METH_NM"),
    F.col("SUB_EXCH_TYP_CD"),
    F.col("SUB_EXCH_TYP_NM"),
    F.rpad(F.col("QHP_APTC_IN"), 1, " ").alias("QHP_APTC_IN"),
    F.rpad(F.col("QHP_CSR_IN"), 1, " ").alias("QHP_CSR_IN"),
    F.rpad(F.col("MBR_EXCH_TERM_DT_SK"), 10, " ").alias("MBR_EXCH_TERM_DT_SK"),
    F.col("SRC_SYS_LAST_UPDT_DTM"),
    F.col("EXCH_MBR_ID"),
    F.col("EXCH_POL_ID"),
    F.col("QHP_ID"),
    F.col("PAYMT_TRANS_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("SUB_EXCH_CHAN_CD_SK"),
    F.col("SUB_EXCH_ENR_METH_CD_SK"),
    F.col("SUB_EXCH_TYP_CD_SK")
)

write_files(
    df_final,
    f"{adls_path}/load/MBR_EXCH_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)