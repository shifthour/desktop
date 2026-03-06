# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------       ------------------------------       --------------------
# MAGIC Aditya   Raju              06/19/2013               5114                              Create Load File for EDW Table PROV_NTWK_D                         EnterpriseWrhsDevl   Peter Marshall              8/27/2013      
# MAGIC Akhila Manickavelu   04/13/2016               5199                          Include Mapping for PROV_NTWK_ORIG_EFF_DT_SK                    EnterpriseDevl            Kalyan Neelam             2016-05-13

# MAGIC Write PROV_NTWK_D Data into a Sequential file for Load Job IdsEdwProv_Ntwk_CdDLoad.
# MAGIC Read all the Data from IDS PROV_NTWK_D Table; Pull records based on LAST_UPDT_RUN_CYC_EXCTN_SK.
# MAGIC Add Defaults and Null Handling.
# MAGIC Job Name: IdsEdwProvNtwkDExtr
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
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
IDSRunCycle = get_widget_value('IDSRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""SELECT
A.PROV_NTWK_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
A.NTWK_ID,
A.PROV_ID,
A.PROV_NTWK_PFX_ID,
A.EFF_DT_SK,
A.NTWK_SK,
A.PROV_AGMNT_SK,
A.PROV_SK,
A.PROV_NTWK_GNDR_ACPTD_CD_SK,
A.PROV_NTWK_RELSHP_TYP_CD_SK,
A.PROV_NTWK_TERM_RSN_CD_SK,
A.ACPTNG_MCAID_PATN_IN,
A.ACPTNG_MCARE_PATN_IN,
A.ACPTNG_PATN_IN,
A.AUTO_PCP_ASGMT_IN,
A.DIR_IN,
B.NTWK_SH_NM,
A.PCP_IN,
A.PROV_DIR_IN,
A.TERM_DT_SK,
A.MAX_PATN_AGE,
A.MAX_PATN_QTY,
A.MIN_PATN_AGE,
B.NTWK_NM,
C.PROV_AGMNT_ID
FROM {IDSOwner}.PROV_NTWK A
INNER JOIN {IDSOwner}.NTWK B ON A.NTWK_SK=B.NTWK_SK
INNER JOIN {IDSOwner}.PROV_AGMNT C ON A.PROV_AGMNT_SK=C.PROV_AGMNT_SK
LEFT OUTER JOIN {IDSOwner}.CD_MPPNG CD ON A.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
WHERE A.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSRunCycle}"""
df_PROV_NTWK_D = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

extract_query = f"""SELECT
A.NTWK_ID,
A.PROV_ID,
A.PROV_NTWK_PFX_ID,
A.PCP_IN,
min(A.EFF_DT_SK) as PROV_NTWK_ORIG_EFF_DT_SK
FROM {IDSOwner}.PROV_NTWK A
group by A.NTWK_ID, A.PROV_ID, A.PROV_NTWK_PFX_ID, A.PCP_IN
"""
df_PROV_NTWK = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

extract_query = f"""SELECT CD_MPPNG_SK,COALESCE(TRGT_CD,'UNK') TRGT_CD,COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM from {IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Cpy_cd_mpping = df_db2_CD_MPPNG_Extr

df_refGndrCd = df_Cpy_cd_mpping.select(
    F.col("CD_MPPNG_SK"),
    F.col("TRGT_CD"),
    F.col("TRGT_CD_NM")
)

df_refNtwkRelshp = df_Cpy_cd_mpping.select(
    F.col("CD_MPPNG_SK"),
    F.col("TRGT_CD"),
    F.col("TRGT_CD_NM")
)

df_refNtwkTermRsn = df_Cpy_cd_mpping.select(
    F.col("CD_MPPNG_SK"),
    F.col("TRGT_CD"),
    F.col("TRGT_CD_NM")
)

df_Lookup = (
    df_PROV_NTWK_D.alias("lnk_IdsEdwProvNtwkDExtr_InABC")
    .join(
        df_refGndrCd.alias("refGndrCd"),
        F.col("lnk_IdsEdwProvNtwkDExtr_InABC.PROV_NTWK_GNDR_ACPTD_CD_SK") == F.col("refGndrCd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_refNtwkRelshp.alias("refNtwkRelshp"),
        F.col("lnk_IdsEdwProvNtwkDExtr_InABC.PROV_NTWK_RELSHP_TYP_CD_SK") == F.col("refNtwkRelshp.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_refNtwkTermRsn.alias("refNtwkTermRsn"),
        F.col("lnk_IdsEdwProvNtwkDExtr_InABC.PROV_NTWK_TERM_RSN_CD_SK") == F.col("refNtwkTermRsn.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_PROV_NTWK.alias("Lnk_Provntwkorig"),
        (
            (F.col("lnk_IdsEdwProvNtwkDExtr_InABC.NTWK_ID") == F.col("Lnk_Provntwkorig.NTWK_ID"))
            & (F.col("lnk_IdsEdwProvNtwkDExtr_InABC.PROV_ID") == F.col("Lnk_Provntwkorig.PROV_ID"))
            & (F.col("lnk_IdsEdwProvNtwkDExtr_InABC.PROV_NTWK_PFX_ID") == F.col("Lnk_Provntwkorig.PROV_NTWK_PFX_ID"))
            & (F.col("lnk_IdsEdwProvNtwkDExtr_InABC.PCP_IN") == F.col("Lnk_Provntwkorig.PCP_IN"))
        ),
        "left"
    )
)

df_Lookup_out = df_Lookup.select(
    F.col("lnk_IdsEdwProvNtwkDExtr_InABC.PROV_NTWK_SK").alias("PROV_NTWK_SK"),
    F.col("lnk_IdsEdwProvNtwkDExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_IdsEdwProvNtwkDExtr_InABC.NTWK_ID").alias("NTWK_ID"),
    F.col("lnk_IdsEdwProvNtwkDExtr_InABC.PROV_ID").alias("PROV_ID"),
    F.col("lnk_IdsEdwProvNtwkDExtr_InABC.PROV_NTWK_PFX_ID").alias("PROV_NTWK_PFX_ID"),
    F.col("lnk_IdsEdwProvNtwkDExtr_InABC.EFF_DT_SK").alias("PROV_NTWK_EFF_DT_SK"),
    F.col("lnk_IdsEdwProvNtwkDExtr_InABC.NTWK_SK").alias("NTWK_SK"),
    F.col("lnk_IdsEdwProvNtwkDExtr_InABC.PROV_AGMNT_SK").alias("PROV_AGMNT_SK"),
    F.col("lnk_IdsEdwProvNtwkDExtr_InABC.PROV_SK").alias("PROV_SK"),
    F.col("lnk_IdsEdwProvNtwkDExtr_InABC.PROV_NTWK_GNDR_ACPTD_CD_SK").alias("PROV_NTWK_GNDR_ACPTD_CD_SK"),
    F.col("lnk_IdsEdwProvNtwkDExtr_InABC.PROV_NTWK_RELSHP_TYP_CD_SK").alias("PROV_NTWK_RELSHP_TYP_CD_SK"),
    F.col("lnk_IdsEdwProvNtwkDExtr_InABC.PROV_NTWK_TERM_RSN_CD_SK").alias("PROV_NTWK_TERM_RSN_CD_SK"),
    F.col("lnk_IdsEdwProvNtwkDExtr_InABC.ACPTNG_MCAID_PATN_IN").alias("PROV_NTWK_ACPTNG_MCAID_PATN_IN"),
    F.col("lnk_IdsEdwProvNtwkDExtr_InABC.ACPTNG_MCARE_PATN_IN").alias("PROV_NTWK_ACPTNG_MCARE_PATN_IN"),
    F.col("lnk_IdsEdwProvNtwkDExtr_InABC.ACPTNG_PATN_IN").alias("PROV_NTWK_ACPTNG_PATN_IN"),
    F.col("lnk_IdsEdwProvNtwkDExtr_InABC.AUTO_PCP_ASGMT_IN").alias("PROV_NTWK_AUTO_PCP_ASGMT_IN"),
    F.col("lnk_IdsEdwProvNtwkDExtr_InABC.DIR_IN").alias("PROV_NTWK_DIR_IN"),
    F.col("lnk_IdsEdwProvNtwkDExtr_InABC.PCP_IN").alias("PROV_NTWK_PCP_IN"),
    F.col("lnk_IdsEdwProvNtwkDExtr_InABC.PROV_DIR_IN").alias("PROV_NTWK_PROV_DIR_IN"),
    F.col("lnk_IdsEdwProvNtwkDExtr_InABC.TERM_DT_SK").alias("TERM_DT_SK"),
    F.col("lnk_IdsEdwProvNtwkDExtr_InABC.MAX_PATN_AGE").alias("PROV_NTWK_MAX_PATN_AGE"),
    F.col("lnk_IdsEdwProvNtwkDExtr_InABC.MAX_PATN_QTY").alias("PROV_NTWK_MAX_PATN_QTY"),
    F.col("lnk_IdsEdwProvNtwkDExtr_InABC.MIN_PATN_AGE").alias("PROV_NTWK_MIN_PATN_AGE"),
    F.col("lnk_IdsEdwProvNtwkDExtr_InABC.NTWK_NM").alias("NTWK_NM"),
    F.col("lnk_IdsEdwProvNtwkDExtr_InABC.PROV_AGMNT_ID").alias("PROV_AGMNT_ID"),
    F.col("lnk_IdsEdwProvNtwkDExtr_InABC.NTWK_SH_NM").alias("NTWK_SH_NM"),
    F.col("refGndrCd.TRGT_CD").alias("PROV_NTWK_GNDR_ACPTD_CD"),
    F.col("refGndrCd.TRGT_CD_NM").alias("PROV_NTWK_GNDR_ACPTD_NM"),
    F.col("refNtwkRelshp.TRGT_CD").alias("PROV_NTWK_RELSHP_TYP_CD"),
    F.col("refNtwkRelshp.TRGT_CD_NM").alias("PROV_NTWK_RELSHP_TYP_NM"),
    F.col("refNtwkTermRsn.TRGT_CD").alias("PROV_NTWK_TERM_RSN_CD"),
    F.col("refNtwkTermRsn.TRGT_CD_NM").alias("PROV_NTWK_TERM_RSN_NM"),
    F.col("Lnk_Provntwkorig.PROV_NTWK_ORIG_EFF_DT_SK").alias("PROV_NTWK_ORIG_EFF_DT_SK")
)

df_enriched = df_Lookup_out.select(
    F.col("PROV_NTWK_SK"),
    F.col("NTWK_ID"),
    F.col("PROV_ID"),
    F.col("PROV_NTWK_PFX_ID"),
    F.col("PROV_NTWK_PCP_IN"),
    F.col("PROV_NTWK_EFF_DT_SK"),
    F.when(F.col("SRC_SYS_CD").isNull(), 'UNK').otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("NTWK_SK"),
    F.col("PROV_SK"),
    F.col("PROV_AGMNT_SK"),
    F.when(F.col("PROV_NTWK_GNDR_ACPTD_CD").isNull(), 'UNK').otherwise(F.col("PROV_NTWK_GNDR_ACPTD_CD")).alias("PROV_NTWK_GNDR_ACPTD_CD"),
    F.when(F.col("PROV_NTWK_GNDR_ACPTD_NM").isNull(), 'UNKNOWN').otherwise(F.col("PROV_NTWK_GNDR_ACPTD_NM")).alias("PROV_NTWK_GNDR_ACPTD_NM"),
    F.when(F.col("PROV_NTWK_RELSHP_TYP_CD").isNull(), 'UNK').otherwise(F.col("PROV_NTWK_RELSHP_TYP_CD")).alias("PROV_NTWK_RELSHP_TYP_CD"),
    F.when(F.col("PROV_NTWK_RELSHP_TYP_NM").isNull(), 'UNKNOWN').otherwise(F.col("PROV_NTWK_RELSHP_TYP_NM")).alias("PROV_NTWK_RELSHP_TYP_NM"),
    F.when(F.col("PROV_NTWK_TERM_RSN_CD").isNull(), 'UNK').otherwise(F.col("PROV_NTWK_TERM_RSN_CD")).alias("PROV_NTWK_TERM_RSN_CD"),
    F.when(F.col("PROV_NTWK_TERM_RSN_NM").isNull(), 'UNKNOWN').otherwise(F.col("PROV_NTWK_TERM_RSN_NM")).alias("PROV_NTWK_TERM_RSN_NM"),
    F.col("PROV_NTWK_ACPTNG_MCAID_PATN_IN"),
    F.col("PROV_NTWK_ACPTNG_MCARE_PATN_IN"),
    F.col("PROV_NTWK_ACPTNG_PATN_IN"),
    F.col("PROV_NTWK_AUTO_PCP_ASGMT_IN"),
    F.col("PROV_NTWK_DIR_IN"),
    F.col("PROV_NTWK_PROV_DIR_IN"),
    F.when(F.col("PROV_NTWK_ORIG_EFF_DT_SK").isNull(), '').otherwise(F.col("PROV_NTWK_ORIG_EFF_DT_SK")).alias("PROV_NTWK_ORIG_EFF_DT_SK"),
    F.col("TERM_DT_SK").alias("PROV_NTWK_TERM_DT_SK"),
    F.col("PROV_NTWK_MIN_PATN_AGE"),
    F.col("PROV_NTWK_MAX_PATN_AGE"),
    F.col("PROV_NTWK_MAX_PATN_QTY"),
    F.col("NTWK_NM"),
    F.col("NTWK_SH_NM"),
    F.col("PROV_AGMNT_ID"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PROV_NTWK_GNDR_ACPTD_CD_SK"),
    F.col("PROV_NTWK_RELSHP_TYP_CD_SK"),
    F.col("PROV_NTWK_TERM_RSN_CD_SK")
)

df_final = df_enriched
df_final = df_final.withColumn("PROV_NTWK_PCP_IN", F.rpad(F.col("PROV_NTWK_PCP_IN"), 1, " "))
df_final = df_final.withColumn("PROV_NTWK_EFF_DT_SK", F.rpad(F.col("PROV_NTWK_EFF_DT_SK"), 10, " "))
df_final = df_final.withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
df_final = df_final.withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
df_final = df_final.withColumn("PROV_NTWK_ACPTNG_MCAID_PATN_IN", F.rpad(F.col("PROV_NTWK_ACPTNG_MCAID_PATN_IN"), 1, " "))
df_final = df_final.withColumn("PROV_NTWK_ACPTNG_MCARE_PATN_IN", F.rpad(F.col("PROV_NTWK_ACPTNG_MCARE_PATN_IN"), 1, " "))
df_final = df_final.withColumn("PROV_NTWK_ACPTNG_PATN_IN", F.rpad(F.col("PROV_NTWK_ACPTNG_PATN_IN"), 1, " "))
df_final = df_final.withColumn("PROV_NTWK_AUTO_PCP_ASGMT_IN", F.rpad(F.col("PROV_NTWK_AUTO_PCP_ASGMT_IN"), 1, " "))
df_final = df_final.withColumn("PROV_NTWK_DIR_IN", F.rpad(F.col("PROV_NTWK_DIR_IN"), 1, " "))
df_final = df_final.withColumn("PROV_NTWK_PROV_DIR_IN", F.rpad(F.col("PROV_NTWK_PROV_DIR_IN"), 1, " "))
df_final = df_final.withColumn("PROV_NTWK_ORIG_EFF_DT_SK", F.rpad(F.col("PROV_NTWK_ORIG_EFF_DT_SK"), 10, " "))
df_final = df_final.withColumn("PROV_NTWK_TERM_DT_SK", F.rpad(F.col("PROV_NTWK_TERM_DT_SK"), 10, " "))

df_final = df_final.select(
    "PROV_NTWK_SK",
    "NTWK_ID",
    "PROV_ID",
    "PROV_NTWK_PFX_ID",
    "PROV_NTWK_PCP_IN",
    "PROV_NTWK_EFF_DT_SK",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "NTWK_SK",
    "PROV_SK",
    "PROV_AGMNT_SK",
    "PROV_NTWK_GNDR_ACPTD_CD",
    "PROV_NTWK_GNDR_ACPTD_NM",
    "PROV_NTWK_RELSHP_TYP_CD",
    "PROV_NTWK_RELSHP_TYP_NM",
    "PROV_NTWK_TERM_RSN_CD",
    "PROV_NTWK_TERM_RSN_NM",
    "PROV_NTWK_ACPTNG_MCAID_PATN_IN",
    "PROV_NTWK_ACPTNG_MCARE_PATN_IN",
    "PROV_NTWK_ACPTNG_PATN_IN",
    "PROV_NTWK_AUTO_PCP_ASGMT_IN",
    "PROV_NTWK_DIR_IN",
    "PROV_NTWK_PROV_DIR_IN",
    "PROV_NTWK_ORIG_EFF_DT_SK",
    "PROV_NTWK_TERM_DT_SK",
    "PROV_NTWK_MIN_PATN_AGE",
    "PROV_NTWK_MAX_PATN_AGE",
    "PROV_NTWK_MAX_PATN_QTY",
    "NTWK_NM",
    "NTWK_SH_NM",
    "PROV_AGMNT_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PROV_NTWK_GNDR_ACPTD_CD_SK",
    "PROV_NTWK_RELSHP_TYP_CD_SK",
    "PROV_NTWK_TERM_RSN_CD_SK"
)

write_files(
    df_final,
    f"{adls_path}/load/PROV_NTWK_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)