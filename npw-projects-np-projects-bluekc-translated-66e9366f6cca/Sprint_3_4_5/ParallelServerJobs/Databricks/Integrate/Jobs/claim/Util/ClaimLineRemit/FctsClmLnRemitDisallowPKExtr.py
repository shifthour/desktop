# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_2 09/22/09 15:32:09 Batch  15241_56279 PROMOTE bckcetl:31540 ids20 dsadm bls for sa
# MAGIC ^1_2 09/22/09 15:22:02 Batch  15241_55325 INIT bckcett:31540 testIDSnew dsadm bls for sa
# MAGIC ^1_1 09/22/09 09:19:44 Batch  15241_33586 INIT bckcett:31540 testIDSnew dsadm bls for sa
# MAGIC ^1_2 07/06/09 11:52:09 Batch  15163_42760 PROMOTE bckcett:31540 testIDSnew u150906 3833-RemitAlternateCharge_Sharon_testIDSnew             Maddy
# MAGIC ^1_2 07/06/09 11:37:10 Batch  15163_41931 INIT bckcett:31540 devlIDSnew u150906 3833-RemitAlternateCharge_Sharon_devlIDSnew                  Maddy
# MAGIC ^1_1 06/29/09 16:32:48 Batch  15156_59635 INIT bckcett:31540 devlIDSnew u150906 3833-RemitAlternateCharge_Sharon_devlIDSnew        Maddy
# MAGIC 
# MAGIC  Copyright 2008 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC hf_clm_ln_remit_dslw_pk_extr_uniq
# MAGIC 
# MAGIC 3;hf_clm_ln_remit_dslw_pk_extr_uniq;hf_clm_ln_remit_dslw_pk_extr_all;hf_clm_ln_remit_dsalw_pk_lkup
# MAGIC 
# MAGIC 1;hf_clm_remit_dsallow_dedup
# MAGIC 
# MAGIC JOB NAME:  FctsClmLnRemitDisallowPKExtr
# MAGIC CALLED BY:  FctsClmPrereqSeq
# MAGIC                
# MAGIC 
# MAGIC PROCESSING:   
# MAGIC          *  This job must be run after  
# MAGIC          *  UNIX file K_CLM_LN_REMIT_DSALLOW.dat is removed in Before-job to remove previous keys incase of earlier abend.  File writting is set to append.  
# MAGIC          *  Assigns / creates claim primary key surrogate keys for all records processed in current run.  Included are any adjusted to/from claims that will be foreign keyed later. 
# MAGIC          *  The primary key hash file hf_clm_ln_remit_dsallow  is the output of this job and is used by the following tables for keying
# MAGIC              CLM_LN_REMIT_DSALW
# MAGIC        
# MAGIC 
# MAGIC 2;hf_clm_ln_pk_lkup;hf_clm_pk_adj_tofrom
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 \(9)Project/Altiris #\(9)Change Description\(9)\(9)\(9)\(9)Development Project\(9)Code Reviewer\(9)Date Reviewed       
# MAGIC ------------------              --------------------     \(9)------------------------\(9)-----------------------------------------------------------------------\(9)--------------------------------\(9)-------------------------------\(9)----------------------------       
# MAGIC SANDREW \(9)2009-06-13\(9)RemitAltChrg          Original Programming                                               devlIDSnew                               Steph Goddard        07/01/2009
# MAGIC Prabhu ES               2022-02-26               S2S Remediation  MSSQL connection parameters added                    IntegrateDev5\(9)\(9)Ken Bradmon\(9)2022-06-11

# MAGIC This hashfile not used
# MAGIC Get SK for primary key on input record.  Key will be used in IdsClmLnRemitDsalwFkey.
# MAGIC Facets Claim Primary Key Process
# MAGIC This job loads primary key hash file with all keys used by tables with natural keys of source system code and claim ID
# MAGIC Load Claim Line Remit Disallow Key Hash File With Claims to Process
# MAGIC Extract all remit claim lines that will be processed using driver table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLnRemitDsalwLoadPK
# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


bcbs_secret_name = get_widget_value('bcbs_secret_name','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','FACETS')
SrcSysCdSK = get_widget_value('SrcSysCdSK','')
DriverTable = get_widget_value('DriverTable','')
CurrRunCycle = get_widget_value('CurrRunCycle','')

jdbc_url, jdbc_props = get_db_config(bcbs_secret_name)

extract_query_PD_MED_CLM_DTL = f"""
SELECT  DRIVER.CLM_ID,
        PD_MED_DTL.CDML_SEQ_NO CLM_LN_SEQ_NO,
        PD_MED_DTL.DISALLOW_TYPE CLM_LN_DSALW_TYP_CD,
        DRIVER.CLM_STS,
        DRIVER.CLCL_PAID_DT,
        DRIVER.CLCL_ID_ADJ_TO,
        DRIVER.CLCL_ID_ADJ_FROM,
        PD_MED_DTL.BYPASS_IN
FROM tempdb..{DriverTable} DRIVER,
     #$BCBSOwner#.PD_MED_CLM_DTL PD_MED_DTL
WHERE DRIVER.CLM_ID = PD_MED_DTL.CLCL_ID
"""

df_TMP_IDS_CLAIM_REMIT_DSALW_PD_MED_CLM_DTL = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_PD_MED_CLM_DTL)
    .load()
)

extract_query_PD_DNTL_CLM_DTL = f"""
SELECT  DRIVER.CLM_ID,
        DNTL_CLM_DTL.CDDL_SEQ_NO CLM_LN_SEQ_NO,
        DNTL_CLM_DTL.DISALLOW_TYPE CLM_LN_DSALW_TYP_CD,
        DRIVER.CLM_STS,
        DRIVER.CLCL_PAID_DT,
        DRIVER.CLCL_ID_ADJ_TO,
        DRIVER.CLCL_ID_ADJ_FROM,
        DNTL_CLM_DTL.BYPASS_IN
FROM tempdb..{DriverTable} DRIVER,
     #$BCBSOwner#.PD_DNTL_CLM_DTL DNTL_CLM_DTL
WHERE DRIVER.CLM_ID = DNTL_CLM_DTL.CLCL_ID
"""

df_TMP_IDS_CLAIM_REMIT_DSALW_PD_DNTL_CLM_DTL = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_PD_DNTL_CLM_DTL)
    .load()
)

extract_query_PD_MED_CLM_DTL_ALT_CHRG = f"""
SELECT  DRIVER.CLM_ID,
        PD_MED_DTL_ALT.CDML_SEQ_NO CLM_LN_SEQ_NO,
        PD_MED_DTL_ALT.DISALLOW_TYPE CLM_LN_DSALW_TYP_CD,
        DRIVER.CLM_STS,
        DRIVER.CLCL_PAID_DT,
        DRIVER.CLCL_ID_ADJ_TO,
        DRIVER.CLCL_ID_ADJ_FROM,
        PD_MED_DTL_ALT.BYPASS_IN
FROM tempdb..{DriverTable} DRIVER,
     #$BCBSOwner#.PD_MED_CLM_DTL_ALT_CHRG PD_MED_DTL_ALT
WHERE DRIVER.CLM_ID = PD_MED_DTL_ALT.CLCL_ID
"""

df_TMP_IDS_CLAIM_REMIT_DSALW_PD_MED_CLM_DTL_ALT_CHRG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_PD_MED_CLM_DTL_ALT_CHRG)
    .load()
)

df_Hashed_File_132_med_clm_dtl = dedup_sort(
    df_TMP_IDS_CLAIM_REMIT_DSALW_PD_MED_CLM_DTL,
    ["CLM_ID","CLM_LN_SEQ_NO","CLM_LN_DSALW_TYP_CD"],
    []
).select(
    "CLM_ID","CLM_LN_SEQ_NO","CLM_LN_DSALW_TYP_CD","CLM_STS","CLCL_PAID_DT","CLCL_ID_ADJ_TO","CLCL_ID_ADJ_FROM","BYPASS_IN"
)

df_Hashed_File_132_dntl_clm_dtl = dedup_sort(
    df_TMP_IDS_CLAIM_REMIT_DSALW_PD_DNTL_CLM_DTL,
    ["CLM_ID","CLM_LN_SEQ_NO","CLM_LN_DSALW_TYP_CD"],
    []
).select(
    "CLM_ID","CLM_LN_SEQ_NO","CLM_LN_DSALW_TYP_CD","CLM_STS","CLCL_PAID_DT","CLCL_ID_ADJ_TO","CLCL_ID_ADJ_FROM","BYPASS_IN"
)

df_Hashed_File_132_med_clm_dtl_atl_chrg = dedup_sort(
    df_TMP_IDS_CLAIM_REMIT_DSALW_PD_MED_CLM_DTL_ALT_CHRG,
    ["CLM_ID","CLM_LN_SEQ_NO","CLM_LN_DSALW_TYP_CD"],
    []
).select(
    "CLM_ID","CLM_LN_SEQ_NO","CLM_LN_DSALW_TYP_CD","CLM_STS","CLCL_PAID_DT","CLCL_ID_ADJ_TO","CLCL_ID_ADJ_FROM","BYPASS_IN"
)

df_LinkRemitData_Clms = (
    df_Hashed_File_132_med_clm_dtl
    .union(df_Hashed_File_132_dntl_clm_dtl)
    .union(df_Hashed_File_132_med_clm_dtl_atl_chrg)
)

df_hf_clm_ln_remit_dslw_pk_extr_uniq = dedup_sort(
    df_LinkRemitData_Clms,
    ["CLM_ID","CLM_LN_SEQ_NO","CLM_LN_DSALW_TYP_CD"],
    []
).select(
    "CLM_ID","CLM_LN_SEQ_NO","CLM_LN_DSALW_TYP_CD","CLM_STS","CLCL_PAID_DT","CLCL_ID_ADJ_TO","CLCL_ID_ADJ_FROM","BYPASS_IN"
)

df_hf_clm_ln_remit_dslw_pk_extr_uniq_stagevars = (
    df_hf_clm_ln_remit_dslw_pk_extr_uniq
    .withColumn("svClmId", strip_field(F.col("CLM_ID")))
    .withColumn("svDisallowType", trim(F.col("CLM_LN_DSALW_TYP_CD")))
    .withColumn(
        "svClmIDAdjFrom",
        F.when(
            F.length(trim(strip_field(F.col("CLCL_ID_ADJ_FROM")))) > 0,
            strip_field(F.col("CLCL_ID_ADJ_FROM")) + F.lit("R")
        ).otherwise("NA")
    )
    .withColumn(
        "svClmIDAdjTo",
        F.when(
            F.length(trim(strip_field(F.col("CLCL_ID_ADJ_TO")))) > 0,
            strip_field(F.col("CLCL_ID_ADJ_TO")) + F.lit("R")
        ).otherwise("NA")
    )
    .withColumn(
        "svByPassIn",
        F.when(
            F.length(trim(F.col("BYPASS_IN")))==0,
            F.lit("U")
        ).otherwise(trim(F.col("BYPASS_IN")))
    )
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSK))
    .withColumn("CLM_LN_SEQ_NO", F.col("CLM_LN_SEQ_NO"))
)

df_Copy_of_StripFields_Claim = df_hf_clm_ln_remit_dslw_pk_extr_uniq_stagevars.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("svClmId").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_NO"),
    F.col("svDisallowType").alias("CLM_LN_DSALW_TYP_CD"),
    F.col("svByPassIn").alias("BYPASS_IN"),
)
df_Copy_of_StripFields_Adjustments = df_hf_clm_ln_remit_dslw_pk_extr_uniq_stagevars.filter(
    F.col("CLM_STS")==F.lit("91")
).select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    (F.col("svClmId") + F.lit("R")).alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_NO"),
    F.col("svDisallowType").alias("CLM_LN_DSALW_TYP_CD"),
    F.col("svByPassIn").alias("BYPASS_IN"),
)
df_Copy_of_StripFields_Adj_from = df_hf_clm_ln_remit_dslw_pk_extr_uniq_stagevars.filter(
    F.col("svClmIDAdjFrom")!=F.lit("NA")
).select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("svClmIDAdjFrom").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_NO"),
    F.col("svDisallowType").alias("CLM_LN_DSALW_TYP_CD"),
    F.col("svByPassIn").alias("BYPASS_IN"),
)
df_Copy_of_StripFields_Adj_to = df_hf_clm_ln_remit_dslw_pk_extr_uniq_stagevars.filter(
    F.col("svClmIDAdjTo")!=F.lit("NA")
).select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("svClmIDAdjTo").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_NO"),
    F.col("svDisallowType").alias("CLM_LN_DSALW_TYP_CD"),
    F.col("svByPassIn").alias("BYPASS_IN"),
)

df_Link_Collector_148_Adj_to = (
    df_Copy_of_StripFields_Adj_to
    .withColumnRenamed("CLM_LN_NO","CLM_LN_SEQ_NO")
    .withColumnRenamed("BYPASS_IN","BYPS_IN")
)
df_Link_Collector_148_Adj_from = (
    df_Copy_of_StripFields_Adj_from
    .withColumnRenamed("CLM_LN_NO","CLM_LN_SEQ_NO")
    .withColumnRenamed("BYPASS_IN","BYPS_IN")
)
df_Link_Collector_148_Adjustments = (
    df_Copy_of_StripFields_Adjustments
    .withColumnRenamed("CLM_LN_NO","CLM_LN_SEQ_NO")
    .withColumnRenamed("BYPASS_IN","BYPS_IN")
)
df_Link_Collector_148_Claim = (
    df_Copy_of_StripFields_Claim
    .withColumnRenamed("CLM_LN_NO","CLM_LN_SEQ_NO")
    .withColumnRenamed("BYPASS_IN","BYPS_IN")
)

df_Link_Collector_148_all_clm_needed = (
    df_Link_Collector_148_Adj_to
    .union(df_Link_Collector_148_Adj_from)
    .union(df_Link_Collector_148_Adjustments)
    .union(df_Link_Collector_148_Claim)
)

df_hf_clm_ln_remit_pk_extr_all = dedup_sort(
    df_Link_Collector_148_all_clm_needed,
    ["SRC_SYS_CD_SK","CLM_ID","CLM_LN_SEQ_NO","CLM_LN_DSALW_TYP_CD","BYPS_IN"],
    []
).select("SRC_SYS_CD_SK","CLM_ID","CLM_LN_SEQ_NO","CLM_LN_DSALW_TYP_CD","BYPS_IN")

params_ClmLnRemitDsalwLoadPK = {
    "SrcSysCd": SrcSysCd,
    "SrcSysCdSK": SrcSysCdSK,
    "CurrRunCycle": CurrRunCycle,
    "IDSOwner": get_widget_value('IDSOwner','')
}
df_ClmLnRemitDsalwLoadPK = ClmLnRemitDsalwLoadPK(df_hf_clm_ln_remit_pk_extr_all, params_ClmLnRemitDsalwLoadPK)

df_hf_clm_ln_remit_dsalw_pk_lkup = df_ClmLnRemitDsalwLoadPK.select(
    "SRC_SYS_CD",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CLM_LN_DSALW_TYP_CD",
    "BYPS_IN",
    "CRT_RUN_CYC_EXCTN_SK",
    "CLM_LN_REMIT_DSALW_SK"
)

df_hf_clm_ln_remit_dsalw_pk_lkup = df_hf_clm_ln_remit_dsalw_pk_lkup.withColumn(
    "CLM_ID",
    F.rpad(F.col("CLM_ID"), <...>, " ")
).withColumn(
    "CLM_LN_DSALW_TYP_CD",
    F.rpad(F.col("CLM_LN_DSALW_TYP_CD"), <...>, " ")
).withColumn(
    "BYPS_IN",
    F.rpad(F.col("BYPS_IN"), 1, " ")
)

write_files(
    df_hf_clm_ln_remit_dsalw_pk_lkup,
    f"hf_clm_ln_remit_dsalw_pk_lkup.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)