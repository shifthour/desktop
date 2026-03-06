# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_5 09/22/09 15:32:09 Batch  15241_56279 PROMOTE bckcetl:31540 ids20 dsadm bls for sa
# MAGIC ^1_5 09/22/09 15:22:02 Batch  15241_55325 INIT bckcett:31540 testIDSnew dsadm bls for sa
# MAGIC ^1_4 09/22/09 09:19:44 Batch  15241_33586 INIT bckcett:31540 testIDSnew dsadm bls for sa
# MAGIC ^1_2 07/06/09 11:52:09 Batch  15163_42760 PROMOTE bckcett:31540 testIDSnew u150906 3833-RemitAlternateCharge_Sharon_testIDSnew             Maddy
# MAGIC ^1_2 07/06/09 11:37:10 Batch  15163_41931 INIT bckcett:31540 devlIDSnew u150906 3833-RemitAlternateCharge_Sharon_devlIDSnew                  Maddy
# MAGIC ^1_1 06/29/09 16:32:48 Batch  15156_59635 INIT bckcett:31540 devlIDSnew u150906 3833-RemitAlternateCharge_Sharon_devlIDSnew        Maddy
# MAGIC ^1_3 04/24/09 09:20:30 Batch  15090_33635 PROMOTE bckcetl ids20 dsadm bls for sa
# MAGIC ^1_3 04/24/09 09:14:25 Batch  15090_33296 INIT bckcett testIDS dsadm BLS FOR SA
# MAGIC ^1_2 03/10/09 11:34:35 Batch  15045_41695 PROMOTE bckcett devlIDS u10157 sa - Bringing ALL Claim code down from production
# MAGIC ^1_2 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
# MAGIC ^1_2 02/10/09 11:16:45 Batch  15017_40611 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 02/02/09 09:52:49 Batch  15009_35578 PROMOTE bckcetl ids20 dsadm bls for brent
# MAGIC ^1_1 01/26/09 13:03:15 Batch  15002_46999 INIT bckcett testIDS dsadm bls for bl
# MAGIC ^1_1 12/16/08 10:00:18 Batch  14961_36031 PROMOTE bckcett testIDS u03651 steph for Brent - claim primary keying
# MAGIC ^1_1 12/16/08 09:15:37 Batch  14961_33343 INIT bckcett devlIDS u03651 steffy
# MAGIC 
# MAGIC  Copyright 2008 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 3;hf_clm_ln_pk_lkup;hf_clm_pk_adj_tofrom;hf_clm_remit_dedup
# MAGIC 3;hf_clm_ln_pk_lkup;hf_clm_pk_adj_tofrom;hf_clm_remit_dedup3;hf_clm_ln_pk_lkup;hf_clm_pk_adj_tofrom;hf_clm_remit_dedup
# MAGIC 
# MAGIC 2;hf_clm_ln_pk_lkup;hf_clm_pk_adj_tofrom
# MAGIC 
# MAGIC hf_clm_ln_remit_pk_extr_med_ln;hf_clm_ln_remit_pk_extr_med_dlt;hf_clm_ln_remit_pk_extr_dntl_ln;hf_clm_ln_remit_pk_extr_dntl_dlt;hf_clm_ln_remit_pk_extr_alt_chrg_med_ln;hf_clm_ln_remit_pk_extr_alt_chrg_med_dtl;
# MAGIC hf_clm_ln_remit_pk_extr_all
# MAGIC hf_clm_ln_remit_pk_extr_uniq
# MAGIC 
# MAGIC JOB NAMES:  FctsClmLnRemitPKExtr
# MAGIC CALLED BY:  FctsClmPrereqSeq
# MAGIC                
# MAGIC 
# MAGIC PROCESSING:   
# MAGIC          *  This job must be run after FctsClmDriverBuild
# MAGIC          *  UNIX file K_CLM_LN.dat is removed in Before-job to remove previous keys incase of earlier abend.  File writting is set to append.  
# MAGIC          *  Assigns / creates claim primary key surrogate keys for all records processed in current run.  Included are any adjusted to/from claims that will be foreign keyed later. 
# MAGIC          *  The primary key hash file hf_clm_ln is the output of this job and is used by the following tables for keying
# MAGIC              CLM_LN
# MAGIC              CLM_LN_PCA
# MAGIC              CLM_LN_REMIT
# MAGIC              CLM_LN_CLNCL_EDIT
# MAGIC 
# MAGIC 
# MAGIC        
# MAGIC 
# MAGIC MODIFICATIONS:                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              Development       Code                          Date 
# MAGIC Developer          Date                 Project/Altiris #              Change Description                                                                                           Project                  Reviewer                  Reviewed       
# MAGIC ------------------        --------------------     ------------------------              -----------------------------------------------------------------------                                                  ----------------------      ---------------------------       ----------------------------   
# MAGIC Brent Leland      2008-07-30      3567 Primary Key            Original Programming                                                                                        devlIDS                 Steph Goddard        10/03/2008
# MAGIC 
# MAGIC SAndrew            2009-06-23      #3833 Remit Alt Chrg      Added two new BCBS tables which contains Alternate Remit amounts          devlIDSnew          Steph Goddard         07/01/2009
# MAGIC                                                                                                           to extract data from                          
# MAGIC                                                                                                 PD_MED_CLM_DTL_ALT_CHRG and PD_MED_CLN_LN_ALT_CHRG
# MAGIC                                                                                                Created hash files hf_clm_ln_remit_pk_extr_med_ln
# MAGIC                                                                                               hf_clm_ln_remit_pk_extr_med_dlt
# MAGIC                                                                                               hf_clm_ln_remit_pk_extr_dntl_ln
# MAGIC                                                                                               hf_clm_ln_remit_pk_extr_dntl_dlt
# MAGIC                                                                                               hf_clm_ln_remit_pk_extr_alt_chrg_med_ln
# MAGIC                                                                                               hf_clm_ln_remit_pk_extr_alt_chrg_med_dtl
# MAGIC SAndrew             2009-06-10         3833 Remit                 Moved from /claim/ClainLine/ClaimLnRemit to /claim/Util/ClaimLineRemit      devlIDSnew           Steph Goddard        07/01/2009
# MAGIC                                                       Alternate Chrg             Added two sql statements unioned with rest in TMP_IDS_CLM_REMT ODBC call for new Alternate Remit tables caused union to fail, caused complete restructure of the job.
# MAGIC                                                                                          Added check for Adjust To Claim IDs and Adjusting From Claim IDs before attaching on the R at end to prevent claims of "       R" and "NAR" 
# MAGIC Prabhu ES           2022-02-26       S2S Remediation         MSSQL connection parameters added                                                              IntegrateDev5          \(9)Ken Bradmon\(9)2022-06-11

# MAGIC This hashfile not used
# MAGIC Get SK for primary key on input record.  Key will be used in IdsClmLnRemitDsalwFkey.
# MAGIC Facets Claim Primary Key Process
# MAGIC This job loads primary key hash file with all keys used by tables with natural keys of source system code and claim ID
# MAGIC Load Claim LineKey Hash File With Claim Line Remit Claims to Process
# MAGIC Extract all remit claim lines that will be processed using driver table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Parameters
SrcSysCd = get_widget_value("SrcSysCd","")
SrcSysCdSK = get_widget_value("SrcSysCdSK","")
DriverTable = get_widget_value("DriverTable","")
CurrRunCycle = get_widget_value("CurrRunCycle","")
bcbs_secret_name = get_widget_value("bcbs_secret_name","")
ids_secret_name = get_widget_value("ids_secret_name","")
BCBSOwner = get_widget_value("BCBSOwner","")
IDSOwner = get_widget_value("IDSOwner","")

# Get JDBC config for BCBS
jdbc_url_bcbs, jdbc_props_bcbs = get_db_config(bcbs_secret_name)

# Read from ODBCConnector TMP_IDS_CLAIM_REMIT (6 output links)
query_pd_med_clm_ln = f"""
SELECT
  DRIVER.CLM_ID,
  BCBS_MED.CDML_SEQ_NO AS CLM_LN_SEQ_NO,
  DRIVER.CLM_STS,
  DRIVER.CLCL_PAID_DT,
  DRIVER.CLCL_ID_ADJ_TO,
  DRIVER.CLCL_ID_ADJ_FROM
FROM tempdb..{DriverTable} DRIVER
JOIN {BCBSOwner}.PD_MED_CLM_LN BCBS_MED
  ON DRIVER.CLM_ID = BCBS_MED.CLCL_ID
"""

df_PD_MED_CLM_LN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bcbs)
    .options(**jdbc_props_bcbs)
    .option("query", query_pd_med_clm_ln)
    .load()
)

query_pd_med_clm_dtl = f"""
SELECT
  DRIVER.CLM_ID,
  MED_CLM_DTL.CDML_SEQ_NO AS CLM_LN_SEQ_NO,
  DRIVER.CLM_STS,
  DRIVER.CLCL_PAID_DT,
  DRIVER.CLCL_ID_ADJ_TO,
  DRIVER.CLCL_ID_ADJ_FROM
FROM tempdb..{DriverTable} DRIVER
JOIN {BCBSOwner}.PD_MED_CLM_DTL MED_CLM_DTL
  ON DRIVER.CLM_ID = MED_CLM_DTL.CLCL_ID
"""

df_PD_MED_CLM_DTL = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bcbs)
    .options(**jdbc_props_bcbs)
    .option("query", query_pd_med_clm_dtl)
    .load()
)

query_pd_dntl_clm_ln = f"""
SELECT
  DRIVER.CLM_ID,
  BCBS_DNTL.CDDL_SEQ_NO AS CLM_LN_SEQ_NO,
  DRIVER.CLM_STS,
  DRIVER.CLCL_PAID_DT,
  DRIVER.CLCL_ID_ADJ_TO,
  DRIVER.CLCL_ID_ADJ_FROM
FROM tempdb..{DriverTable} DRIVER
JOIN {BCBSOwner}.PD_DNTL_CLM_LN BCBS_DNTL
  ON DRIVER.CLM_ID = BCBS_DNTL.CLCL_ID
"""

df_PD_DNTL_CLM_LN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bcbs)
    .options(**jdbc_props_bcbs)
    .option("query", query_pd_dntl_clm_ln)
    .load()
)

query_pd_dntl_clm_dtl = f"""
SELECT
  DRIVER.CLM_ID,
  DNTL_CLM_DTL.CDDL_SEQ_NO AS CLM_LN_SEQ_NO,
  DRIVER.CLM_STS,
  DRIVER.CLCL_PAID_DT,
  DRIVER.CLCL_ID_ADJ_TO,
  DRIVER.CLCL_ID_ADJ_FROM
FROM tempdb..{DriverTable} DRIVER
JOIN {BCBSOwner}.PD_DNTL_CLM_DTL DNTL_CLM_DTL
  ON DRIVER.CLM_ID = DNTL_CLM_DTL.CLCL_ID
"""

df_PD_DNTL_CLM_DTL = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bcbs)
    .options(**jdbc_props_bcbs)
    .option("query", query_pd_dntl_clm_dtl)
    .load()
)

query_pd_med_clm_dtl_alt_chrg = f"""
SELECT
  DRIVER.CLM_ID,
  MED_ALT_DTL.CDML_SEQ_NO AS CLM_LN_SEQ_NO,
  DRIVER.CLM_STS,
  DRIVER.CLCL_PAID_DT,
  DRIVER.CLCL_ID_ADJ_TO,
  DRIVER.CLCL_ID_ADJ_FROM
FROM tempdb..{DriverTable} DRIVER
JOIN {BCBSOwner}.PD_MED_CLM_DTL_ALT_CHRG MED_ALT_DTL
  ON DRIVER.CLM_ID = MED_ALT_DTL.CLCL_ID
"""

df_PD_MED_CLM_DTL_ALT_CHRG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bcbs)
    .options(**jdbc_props_bcbs)
    .option("query", query_pd_med_clm_dtl_alt_chrg)
    .load()
)

query_pd_med_clm_ln_alt_chrg = f"""
SELECT
  DRIVER.CLM_ID,
  MED_ALT_LN.CDML_SEQ_NO AS CLM_LN_SEQ_NO,
  DRIVER.CLM_STS,
  DRIVER.CLCL_PAID_DT,
  DRIVER.CLCL_ID_ADJ_TO,
  DRIVER.CLCL_ID_ADJ_FROM
FROM tempdb..{DriverTable} DRIVER
JOIN {BCBSOwner}.PD_MED_CLM_LN_ALT_CHRG MED_ALT_LN
  ON DRIVER.CLM_ID = MED_ALT_LN.CLCL_ID
"""

df_PD_MED_CLM_LN_ALT_CHRG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bcbs)
    .options(**jdbc_props_bcbs)
    .option("query", query_pd_med_clm_ln_alt_chrg)
    .load()
)

# Hashed_File_132 (Scenario A) - 6 input links => 6 output links
# We drop duplicates on ["CLM_ID","CLM_LN_SEQ_NO"] for each of the 6 DataFrames
df_PD_MED_CLM_LN_dedup = dedup_sort(
    df_PD_MED_CLM_LN,
    ["CLM_ID","CLM_LN_SEQ_NO"],
    []
)
df_PD_MED_CLM_DTL_dedup = dedup_sort(
    df_PD_MED_CLM_DTL,
    ["CLM_ID","CLM_LN_SEQ_NO"],
    []
)
df_PD_DNTL_CLM_LN_dedup = dedup_sort(
    df_PD_DNTL_CLM_LN,
    ["CLM_ID","CLM_LN_SEQ_NO"],
    []
)
df_PD_DNTL_CLM_DTL_dedup = dedup_sort(
    df_PD_DNTL_CLM_DTL,
    ["CLM_ID","CLM_LN_SEQ_NO"],
    []
)
df_PD_MED_CLM_DTL_ALT_CHRG_dedup = dedup_sort(
    df_PD_MED_CLM_DTL_ALT_CHRG,
    ["CLM_ID","CLM_LN_SEQ_NO"],
    []
)
df_PD_MED_CLM_LN_ALT_CHRG_dedup = dedup_sort(
    df_PD_MED_CLM_LN_ALT_CHRG,
    ["CLM_ID","CLM_LN_SEQ_NO"],
    []
)

# LinkRemitData (CCollector) unions these 6 flows
df_Clms = (
    df_PD_MED_CLM_LN_dedup.select(
        "CLM_ID",
        "CLM_LN_SEQ_NO",
        "CLM_STS",
        "CLCL_PAID_DT",
        "CLCL_ID_ADJ_TO",
        "CLCL_ID_ADJ_FROM"
    )
    .unionByName(
        df_PD_MED_CLM_DTL_dedup.select(
            "CLM_ID",
            "CLM_LN_SEQ_NO",
            "CLM_STS",
            "CLCL_PAID_DT",
            "CLCL_ID_ADJ_TO",
            "CLCL_ID_ADJ_FROM"
        )
    )
    .unionByName(
        df_PD_DNTL_CLM_LN_dedup.select(
            "CLM_ID",
            "CLM_LN_SEQ_NO",
            "CLM_STS",
            "CLCL_PAID_DT",
            "CLCL_ID_ADJ_TO",
            "CLCL_ID_ADJ_FROM"
        )
    )
    .unionByName(
        df_PD_DNTL_CLM_DTL_dedup.select(
            "CLM_ID",
            "CLM_LN_SEQ_NO",
            "CLM_STS",
            "CLCL_PAID_DT",
            "CLCL_ID_ADJ_TO",
            "CLCL_ID_ADJ_FROM"
        )
    )
    .unionByName(
        df_PD_MED_CLM_DTL_ALT_CHRG_dedup.select(
            "CLM_ID",
            "CLM_LN_SEQ_NO",
            "CLM_STS",
            "CLCL_PAID_DT",
            "CLCL_ID_ADJ_TO",
            "CLCL_ID_ADJ_FROM"
        )
    )
    .unionByName(
        df_PD_MED_CLM_LN_ALT_CHRG_dedup.select(
            "CLM_ID",
            "CLM_LN_SEQ_NO",
            "CLM_STS",
            "CLCL_PAID_DT",
            "CLCL_ID_ADJ_TO",
            "CLCL_ID_ADJ_FROM"
        )
    )
)

# hf_clm_ln_remit_pk_extr_uniq (Scenario A) -> dedup on [CLM_ID, CLM_LN_SEQ_NO]
df_Clms_dedup = dedup_sort(
    df_Clms,
    ["CLM_ID","CLM_LN_SEQ_NO"],
    []
)

# StripFields (CTransformerStage)
df_stripfields = (
    df_Clms_dedup
    .withColumn("temp_ClmId", strip_field(F.col("CLM_ID")))
    .withColumn("temp_AdjFrom", strip_field(F.col("CLCL_ID_ADJ_FROM")))
    .withColumn("temp_AdjTo", strip_field(F.col("CLCL_ID_ADJ_TO")))
    .withColumn(
        "svClmId",
        F.col("temp_ClmId")
    )
    .withColumn(
        "svClmIDAdjFrom",
        F.when(F.length(trim(F.col("temp_AdjFrom")))>0, F.concat(F.col("temp_AdjFrom"), F.lit("R"))).otherwise(F.lit("NA"))
    )
    .withColumn(
        "svClmIDAdjTo",
        F.when(F.length(trim(F.col("temp_AdjTo")))>0, F.concat(F.col("temp_AdjTo"), F.lit("R"))).otherwise(F.lit("NA"))
    )
)

windowSpec = Window.orderBy(F.monotonically_increasing_id())
df_stripfields_rn = df_stripfields.withColumn("rn", F.row_number().over(windowSpec))

# Claim link (no constraint)
df_Claim = df_stripfields.select(
    F.lit(SrcSysCdSK).alias("SRC_SYS_CD_SK"),
    F.col("svClmId").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_NO")
)

# Adjustments link (constraint: lnkClms.CLM_STS = '91')
df_Adjustments = df_stripfields.filter(F.col("CLM_STS") == '91').select(
    F.lit(SrcSysCdSK).alias("SRC_SYS_CD_SK"),
    F.concat(F.col("svClmId"), F.lit("R")).alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_NO")
)

# Adj_from link (constraint: svClmIDAdjFrom <> 'NA')
df_Adj_from = df_stripfields.filter(F.col("svClmIDAdjFrom") != 'NA').select(
    F.lit(SrcSysCdSK).alias("SRC_SYS_CD_SK"),
    F.col("svClmIDAdjFrom").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_NO")
)

# Adj_to link (constraint: svClmIDAdjTo <> 'NA')
df_Adj_to = df_stripfields.filter(F.col("svClmIDAdjTo") != 'NA').select(
    F.lit(SrcSysCdSK).alias("SRC_SYS_CD_SK"),
    F.col("svClmIDAdjTo").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_NO")
)

# UNK link (constraint: @INROWNUM = 1)
df_UNK = df_stripfields_rn.filter(F.col("rn") == 1).select(
    F.lit(SrcSysCdSK).alias("SRC_SYS_CD_SK"),
    F.lit("UNK").alias("CLM_ID"),
    F.lit(0).alias("CLM_LN_NO")
)

# NA link (constraint: @INROWNUM = 1)
df_NA = df_stripfields_rn.filter(F.col("rn") == 1).select(
    F.lit(SrcSysCdSK).alias("SRC_SYS_CD_SK"),
    F.lit("NA").alias("CLM_ID"),
    F.lit(0).alias("CLM_LN_NO")
)

# Link_Collector_148 (CCollector) unions these 6 flows
df_all_clm_needed = (
    df_Adj_to.unionByName(df_Adj_from)
    .unionByName(df_Adjustments)
    .unionByName(df_Claim)
    .unionByName(df_UNK)
    .unionByName(df_NA)
)

# hf_clm_ln_remit_pk_extr_all (Scenario A) -> dedup on [SRC_SYS_CD_SK, CLM_ID, CLM_LN_NO]
df_all_clm_needed_dedup = dedup_sort(
    df_all_clm_needed,
    ["SRC_SYS_CD_SK","CLM_ID","CLM_LN_NO"],
    []
)

# Rename columns to match next stage's schema usage
df_Transform = df_all_clm_needed_dedup.select(
    F.col("SRC_SYS_CD_SK").cast(T.IntegerType()).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_NO").cast(T.IntegerType()).alias("CLM_LN_SEQ_NO")
)

# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLnLoadPK
# COMMAND ----------

params_ClmLnLoadPK = {
    "SrcSysCd": SrcSysCd,
    "SrcSysCdSK": SrcSysCdSK,
    "CurrRunCycle": CurrRunCycle,
    "$IDSOwner": IDSOwner
}

df_PK_Clms = ClmLnLoadPK(df_Transform, params_ClmLnLoadPK)

# hf_clm_ln_pk_lkup (Scenario C)
df_PK_Clms_final = df_PK_Clms.select(
    F.col("SRC_SYS_CD"),
    F.rpad(F.col("CLM_ID"), 12, " ").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_LN_SK")
)

write_files(
    df_PK_Clms_final,
    "hf_clm_ln_pk_lkup.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)