# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                  Sharon Andrew    03/01/2006 - Originally Program
# MAGIC                  Sharon Andrew    07/06/2006  Renamed from EdwMbrRDSExtr to IdsMbrRDSExtrr.  
# MAGIC                 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                    PROJECT                       REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC 
# MAGIC Archana Palivela     07/16/2013        5114                              Originally Programmed                                                                         EnterpriseWhseDevl    Peter Marshall               9/26/2013

# MAGIC Job name: IdsEdwMbrRdsDExtr
# MAGIC 
# MAGIC 
# MAGIC Job Extract data from IDS table and apply Fkey and Code denormalization, create a load ready file.
# MAGIC Read data from source table MBR_RDS
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC GRP_SK
# MAGIC MBR_SK
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Write MBR_RDS_D Data into a Sequential file for Load Ready Job.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
IDSRunCycle = get_widget_value('IDSRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

query_db2_MBR_RDS_Extr = f"""SELECT 
MBR_RDS.MBR_RDS_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
MBR_RDS.MBR_UNIQ_KEY,
MBR_RDS.SUBMT_DT_SK,
MBR_RDS.PLN_YR_STRT_DT_SK,
MBR_RDS.SBSDY_PERD_EFF_DT_SK,
MBR_RDS.GRP_SK,
MBR_RDS.MBR_SK,
MBR_RDS.PLN_YR_END_DT_SK,
MBR_RDS.SBSDY_PERD_TERM_DT_SK
FROM {IDSOwner}.MBR_RDS MBR_RDS
LEFT JOIN {IDSOwner}.CD_MPPNG CD
ON MBR_RDS.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
WHERE MBR_RDS.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSRunCycle}
  AND MBR_RDS.MBR_RDS_SK <> 1
  AND MBR_RDS.MBR_RDS_SK <> 0
"""

df_db2_MBR_RDS_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_db2_MBR_RDS_Extr)
    .load()
)

query_db2_GRP_Extr = f"""SELECT 
GRP_SK,
GRP_ID,
GRP_NM
FROM {IDSOwner}.GRP
"""

df_db2_GRP_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_db2_GRP_Extr)
    .load()
)

query_DB2_MBR = f"""SELECT DISTINCT
MBR.MBR_SK,
MBR.MBR_UNIQ_KEY,
MBR.SUB_SK,
MBR.MBR_SFX_NO,
SUB.SUB_ID
FROM {IDSOwner}.MBR MBR,
     {IDSOwner}.SUB SUB,
     {IDSOwner}.MBR_RDS MBR_RDS
WHERE MBR_RDS.MBR_SK = MBR.MBR_SK
  AND MBR.SUB_SK = SUB.SUB_SK
GROUP BY 
MBR.MBR_SK,
MBR.MBR_UNIQ_KEY,
MBR.SUB_SK,
MBR.MBR_SFX_NO,
SUB.SUB_ID
"""

df_DB2_MBR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_DB2_MBR)
    .load()
)

df_lkp_Codes = (
    df_db2_MBR_RDS_Extr.alias("Ink_IdsEdwMbrRdsDExtrr_InABC")
    .join(
        df_db2_GRP_Extr.alias("Ref_GrpSk"),
        F.col("Ink_IdsEdwMbrRdsDExtrr_InABC.GRP_SK") == F.col("Ref_GrpSk.GRP_SK"),
        "left"
    )
    .join(
        df_DB2_MBR.alias("Ref_MbrSk"),
        F.col("Ink_IdsEdwMbrRdsDExtrr_InABC.MBR_SK") == F.col("Ref_MbrSk.MBR_SK"),
        "left"
    )
    .select(
        F.col("Ref_GrpSk.GRP_ID").alias("GRP_ID"),
        F.col("Ref_GrpSk.GRP_NM").alias("GRP_NM"),
        F.col("Ink_IdsEdwMbrRdsDExtrr_InABC.MBR_RDS_SK").alias("MBR_RDS_SK"),
        F.col("Ink_IdsEdwMbrRdsDExtrr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Ink_IdsEdwMbrRdsDExtrr_InABC.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("Ink_IdsEdwMbrRdsDExtrr_InABC.SUBMT_DT_SK").alias("MBR_RDS_SUBMT_DT_SK"),
        F.col("Ink_IdsEdwMbrRdsDExtrr_InABC.PLN_YR_STRT_DT_SK").alias("MBR_RDS_PLN_YR_STRT_DT_SK"),
        F.col("Ink_IdsEdwMbrRdsDExtrr_InABC.SBSDY_PERD_EFF_DT_SK").alias("MBR_RDS_PERD_EFF_DT_SK"),
        F.col("Ink_IdsEdwMbrRdsDExtrr_InABC.PLN_YR_END_DT_SK").alias("MBR_RDS_PLN_YR_END_DT_SK"),
        F.col("Ink_IdsEdwMbrRdsDExtrr_InABC.SBSDY_PERD_TERM_DT_SK").alias("MBR_RDS_PERD_TERM_DT_SK"),
        F.col("Ink_IdsEdwMbrRdsDExtrr_InABC.GRP_SK").alias("GRP_SK"),
        F.col("Ink_IdsEdwMbrRdsDExtrr_InABC.MBR_SK").alias("MBR_SK"),
        F.col("Ref_MbrSk.MBR_SFX_NO").alias("MBR_SFX_NO"),
        F.col("Ref_MbrSk.SUB_ID").alias("SUB_ID"),
        F.col("Ref_MbrSk.MBR_SK").alias("MBR_SK_SUB")
    )
)

df_lkp_Codes = (
    df_lkp_Codes
    .withColumn("MBR_RDS_SUBMT_DT_SK", F.rpad(F.col("MBR_RDS_SUBMT_DT_SK"), 10, " "))
    .withColumn("MBR_RDS_PLN_YR_STRT_DT_SK", F.rpad(F.col("MBR_RDS_PLN_YR_STRT_DT_SK"), 10, " "))
    .withColumn("MBR_RDS_PERD_EFF_DT_SK", F.rpad(F.col("MBR_RDS_PERD_EFF_DT_SK"), 10, " "))
    .withColumn("MBR_RDS_PLN_YR_END_DT_SK", F.rpad(F.col("MBR_RDS_PLN_YR_END_DT_SK"), 10, " "))
    .withColumn("MBR_RDS_PERD_TERM_DT_SK", F.rpad(F.col("MBR_RDS_PERD_TERM_DT_SK"), 10, " "))
    .withColumn("MBR_SFX_NO", F.rpad(F.col("MBR_SFX_NO"), 2, " "))
)

df_xmf_businessLogic = (
    df_lkp_Codes
    .withColumn(
        "svMbrId",
        F.concat(trim(F.col("SUB_ID")), trim(F.col("MBR_SFX_NO")))
    )
    .withColumn(
        "MbrCheck",
        F.when(F.col("MBR_SK_SUB") == 0, "UNK")
         .when(F.col("MBR_SK_SUB") == 1, "NA")
         .otherwise(F.col("svMbrId"))
    )
)

df_final = (
    df_xmf_businessLogic
    .withColumn("MBR_RDS_SK", F.col("MBR_RDS_SK"))
    .withColumn("SRC_SYS_CD", F.col("SRC_SYS_CD"))
    .withColumn("MBR_UNIQ_KEY", F.col("MBR_UNIQ_KEY"))
    .withColumn("MBR_RDS_SUBMT_DT_SK", F.col("MBR_RDS_SUBMT_DT_SK"))
    .withColumn("MBR_RDS_PLN_YR_STRT_DT_SK", F.col("MBR_RDS_PLN_YR_STRT_DT_SK"))
    .withColumn("MBR_RDS_PERD_EFF_DT_SK", F.col("MBR_RDS_PERD_EFF_DT_SK"))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.lit(EDWRunCycleDate))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.lit(EDWRunCycleDate))
    .withColumn("GRP_SK", F.col("GRP_SK"))
    .withColumn("MBR_SK", F.col("MBR_SK"))
    .withColumn("MBR_RDS_PLN_YR_END_DT_SK", F.col("MBR_RDS_PLN_YR_END_DT_SK"))
    .withColumn("MBR_RDS_PERD_TERM_DT_SK", F.col("MBR_RDS_PERD_TERM_DT_SK"))
    .withColumn("MBR_RDS_PLN_YR_CUR_RCRD_IN", F.lit("Y"))
    .withColumn(
        "GRP_ID",
        F.when(F.trim(F.col("GRP_ID")) == "", "UNK").otherwise(F.col("GRP_ID"))
    )
    .withColumn(
        "GRP_NM",
        F.when(F.trim(F.col("GRP_NM")) == "", "UNK").otherwise(F.col("GRP_NM"))
    )
    .withColumn(
        "MBR_ID",
        F.when(
            F.length(trim(F.col("MbrCheck"))) == 0,
            "UNK"
        ).otherwise(trim(F.col("MbrCheck")))
    )
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(EDWRunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(EDWRunCycle))
)

df_final = df_final.select(
    F.col("MBR_RDS_SK"),
    F.col("SRC_SYS_CD"),
    F.col("MBR_UNIQ_KEY"),
    F.rpad(F.col("MBR_RDS_SUBMT_DT_SK"), 10, " ").alias("MBR_RDS_SUBMT_DT_SK"),
    F.rpad(F.col("MBR_RDS_PLN_YR_STRT_DT_SK"), 10, " ").alias("MBR_RDS_PLN_YR_STRT_DT_SK"),
    F.rpad(F.col("MBR_RDS_PERD_EFF_DT_SK"), 10, " ").alias("MBR_RDS_PERD_EFF_DT_SK"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("GRP_SK"),
    F.col("MBR_SK"),
    F.rpad(F.col("MBR_RDS_PLN_YR_END_DT_SK"), 10, " ").alias("MBR_RDS_PLN_YR_END_DT_SK"),
    F.rpad(F.col("MBR_RDS_PERD_TERM_DT_SK"), 10, " ").alias("MBR_RDS_PERD_TERM_DT_SK"),
    F.rpad(F.col("MBR_RDS_PLN_YR_CUR_RCRD_IN"), 1, " ").alias("MBR_RDS_PLN_YR_CUR_RCRD_IN"),
    F.col("GRP_ID"),
    F.col("GRP_NM"),
    F.col("MBR_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

write_files(
    df_final,
    f"{adls_path}/load/MBR_RDS_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)