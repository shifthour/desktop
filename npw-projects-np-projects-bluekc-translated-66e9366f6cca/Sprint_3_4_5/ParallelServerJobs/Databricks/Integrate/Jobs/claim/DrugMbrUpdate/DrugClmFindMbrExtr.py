# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_2 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
# MAGIC ^1_4 02/10/09 11:16:45 Batch  15017_40611 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_3 11/21/08 10:38:45 Batch  14936_38329 PROMOTE bckcetl ids20 dsadm bls for sa
# MAGIC ^1_3 11/21/08 10:35:35 Batch  14936_38139 INIT bckcett testIDSnew dsadm bls for sa
# MAGIC ^1_2 11/21/08 09:31:24 Batch  14936_34286 INIT bckcett testIDSnew dsadm bls for sa
# MAGIC ^1_1 11/20/08 08:35:42 Batch  14935_30947 PROMOTE bckcett testIDSnew u03651 steph for Sharon
# MAGIC ^1_1 11/20/08 08:30:03 Batch  14935_30606 INIT bckcett devlIDSnew u03651 steffy
# MAGIC ^1_1 01/28/08 09:39:22 Batch  14638_34767 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/09/08 13:32:13 Batch  14619_48738 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/28/07 10:13:19 Batch  14607_36806 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/24/07 17:16:00 Batch  14573_62165 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 10/18/07 10:33:23 Batch  14536_38008 PROMOTE bckcetl ids20 dsadm bls for sg
# MAGIC ^1_1 10/18/07 10:16:25 Batch  14536_36992 INIT bckcett testIDSnew dsadm bls for sg
# MAGIC ^1_1 10/08/07 12:38:41 Batch  14526_45526 PROMOTE bckcett testIDSnew u03651 steffy
# MAGIC ^1_1 10/08/07 11:44:07 Batch  14526_42251 INIT bckcett devlIDS30 u03651 steffy
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Â© Copyright 2007 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:  DrugClmMbrDataUpdtCntl
# MAGIC 
# MAGIC PROCESSING:  This job is run monthly to update any missing SK values that were not available when the initial claim came in.  This is run for both PCS and ARGUS claims.  The source is passed in to indicate which type of claims we are pulling.  This program will read the working table (W_DRUG_CLM) that has drug claims missing membership information.  It will then try to find the member data.  It will populate another working table, W_DRUG_ENR, with the member unique key and claim ID and also create a hash file with the member and claim information to be passed to the next program, DrugClmMbrDataUpdt
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      \(9)                Change Description                                                                        Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      \(9)                -----------------------------------------------------------------------                               --------------------------------       -------------------------------   ----------------------------       
# MAGIC Steph Goddard\(9)2007-10-01       TT4566 IAD Quarterly Release \(9)Original Programming                                                                       devlIDS30                       Brent Leland             10-08-2007             
# MAGIC SAndrew                 2008-11-18        TTR 390 fix                                    Added join to subscriber table to get the ALPHA_PFX_SK and      devlIDSnew                     Steph Goddard          11/19/2008
# MAGIC                                                                                                                  added it to hash file hf_drugclmupdt_mbrdata which is used in 
# MAGIC                                                                                                                  next program DrugClmMbrDataUpdt

# MAGIC Update missing member enrollment information from drug claims
# MAGIC Pull only the member enrollment information for the claims we are updating.  There are two SQL - one pulls by sub ID and the other by SSN.  Dedup, if any, is performed by the hash file
# MAGIC Hash file used in DrugClmMbrDataUpdt
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Source = get_widget_value('Source','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_mbr_data1 = """
SELECT
CLM.CLM_SK,
CLM.SRC_SYS_CD_SK,
CLM.CLM_ID,
CLM.SVC_STRT_DT_SK,
CLM.SUB_ID,
CLM.MBR_SFX_NO,
MBR.MBR_SK,
MBR.SUB_SK,
MBR.SUBGRP_SK,
MBR.BRTH_DT_SK,
MBR.MBR_UNIQ_KEY,
MBR.MBR_GNDR_CD_SK,
MBR.MBR_RELSHP_CD_SK,
MBR.FIRST_NM,
MBR.MIDINIT,
MBR.LAST_NM,
MBR.SSN,
DRVR.FILL_DT_SK,
SUB.ALPHA_PFX_SK
FROM {IDSOwner}.W_DRUG_CLM DRVR,
     {IDSOwner}.CLM CLM,
     {IDSOwner}.MBR MBR,
     {IDSOwner}.SUB SUB
WHERE DRVR.CLM_SK = CLM.CLM_SK
AND CLM.SUB_ID = SUB.SUB_ID
AND CLM.MBR_SFX_NO = MBR.MBR_SFX_NO
AND MBR.SSN <> ' '
AND LTRIM(RTRIM(CLM.SUB_ID)) <> ''
AND LTRIM(RTRIM(CLM.SUB_ID)) <> 'UNK'
AND LTRIM(RTRIM(SUB.SUB_ID)) <> 'UNK'
AND LTRIM(RTRIM(CLM.SUB_ID)) <> 'NA'
AND LTRIM(RTRIM(SUB.SUB_ID)) <> 'NA'
AND MBR.SUB_SK = SUB.SUB_SK
"""

df_LOOKUPS_mbr_data1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_mbr_data1.strip())
    .load()
)

extract_query_mbr_data2 = """
SELECT
CLM.CLM_SK,
CLM.SRC_SYS_CD_SK,
CLM.CLM_ID,
CLM.SVC_STRT_DT_SK,
CLM.SUB_ID,
CLM.MBR_SFX_NO,
MBR.MBR_SK,
MBR.SUB_SK,
MBR.SUBGRP_SK,
MBR.BRTH_DT_SK,
MBR.MBR_UNIQ_KEY,
MBR.MBR_GNDR_CD_SK,
MBR.MBR_RELSHP_CD_SK,
MBR.FIRST_NM,
MBR.MIDINIT,
MBR.LAST_NM,
MBR.SSN,
DRVR.FILL_DT_SK,
SUB.ALPHA_PFX_SK
FROM {IDSOwner}.W_DRUG_CLM DRVR,
     {IDSOwner}.CLM CLM,
     {IDSOwner}.MBR MBR,
     {IDSOwner}.SUB_ID_HIST HIST,
     {IDSOwner}.SUB SUB
WHERE DRVR.CLM_SK = CLM.CLM_SK
AND (CLM.SUB_ID = HIST.SUB_ID AND HIST.SUB_SK = SUB.SUB_SK)
AND CLM.MBR_SFX_NO = MBR.MBR_SFX_NO
AND SUB.SUB_SK = MBR.SUB_SK
"""

df_LOOKUPS_mbr_data2 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_mbr_data2.strip())
    .load()
)

df_hf_drugclm_out1 = df_LOOKUPS_mbr_data1.dropDuplicates([
    "CLM_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "SVC_STRT_DT_SK",
    "SUB_ID",
    "MBR_SFX_NO"
])

df_hf_drugclm_out2 = df_LOOKUPS_mbr_data2.dropDuplicates([
    "CLM_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "SVC_STRT_DT_SK",
    "SUB_ID",
    "MBR_SFX_NO"
])

df_mbr_out1 = df_hf_drugclm_out1.select(
    "CLM_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "SVC_STRT_DT_SK",
    F.col("SUB_ID").alias("CLM_SUB_ID"),
    "MBR_SFX_NO",
    "MBR_SK",
    "SUB_SK",
    "SUBGRP_SK",
    "BRTH_DT_SK",
    "MBR_UNIQ_KEY",
    "MBR_GNDR_CD_SK",
    "MBR_RELSHP_CD_SK",
    "FIRST_NM",
    "MIDINIT",
    "LAST_NM",
    "SSN",
    "FILL_DT_SK",
    "ALPHA_PFX_SK"
)

df_mbr_out2 = df_hf_drugclm_out2.select(
    "CLM_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "SVC_STRT_DT_SK",
    F.col("SUB_ID").alias("CLM_SUB_ID"),
    "MBR_SFX_NO",
    "MBR_SK",
    "SUB_SK",
    "SUBGRP_SK",
    "BRTH_DT_SK",
    "MBR_UNIQ_KEY",
    "MBR_GNDR_CD_SK",
    "MBR_RELSHP_CD_SK",
    "FIRST_NM",
    "MIDINIT",
    "LAST_NM",
    "SSN",
    "FILL_DT_SK",
    "ALPHA_PFX_SK"
)

df_collect = df_mbr_out1.unionByName(df_mbr_out2)
df_dedup_in = df_collect

df_hf_drugclm_mbr_dedup = df_dedup_in.dropDuplicates([
    "CLM_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "SVC_STRT_DT_SK",
    "CLM_SUB_ID",
    "MBR_SFX_NO"
])

df_dedup_out = df_hf_drugclm_mbr_dedup

df_trans_in = df_dedup_out
df_enriched = df_trans_in.withColumn(
    "MemberAge",
    F.when(F.col("BRTH_DT_SK").isNull(), F.lit(0)).otherwise(AGE.YRS(F.col("BRTH_DT_SK"), F.col("SVC_STRT_DT_SK"), F.lit("J")))
)

df_DrugOut = df_enriched.select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("FILL_DT_SK").alias("FILL_DT_SK"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY")
)

df_hf_mbr = df_enriched.select(
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("CLM_SUB_ID").alias("CLM_SUB_ID"),
    F.col("MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("SUB_SK").alias("SUB_SK"),
    F.col("SUBGRP_SK").alias("SUBGRP_SK"),
    F.col("BRTH_DT_SK").alias("BRTH_DT_SK"),
    F.col("MBR_GNDR_CD_SK").alias("MBR_GNDR_CD_SK"),
    F.col("MBR_RELSHP_CD_SK").alias("MBR_RELSHP_CD_SK"),
    F.col("FIRST_NM").alias("FIRST_NM"),
    F.col("MIDINIT").alias("MIDINIT"),
    F.col("LAST_NM").alias("LAST_NM"),
    F.col("SSN").alias("SSN"),
    F.col("MemberAge").alias("MBR_AGE"),
    F.col("ALPHA_PFX_SK").alias("ALPHA_PFX_SK")
)

df_W_DRUG_ENR = (
    df_DrugOut
    .withColumn("CLM_ID", rpad("CLM_ID", <...>, " "))
    .withColumn("FILL_DT_SK", rpad("FILL_DT_SK", 10, " "))
    .select("CLM_ID", "FILL_DT_SK", "MBR_UNIQ_KEY")
)

write_files(
    df_W_DRUG_ENR,
    f"{adls_path}/load/W_DRUG_ENR.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_hf_drugclmupdt_mbrdata_final = (
    df_hf_mbr
    .withColumn("CLM_SUB_ID", rpad("CLM_SUB_ID", 14, " "))
    .withColumn("MBR_SFX_NO", rpad("MBR_SFX_NO", 2, " "))
    .withColumn("BRTH_DT_SK", rpad("BRTH_DT_SK", 10, " "))
    .withColumn("MIDINIT", rpad("MIDINIT", 1, " "))
    .withColumn("FIRST_NM", rpad("FIRST_NM", <...>, " "))
    .withColumn("LAST_NM", rpad("LAST_NM", <...>, " "))
    .withColumn("SSN", rpad("SSN", <...>, " "))
    .select(
        "MBR_UNIQ_KEY",
        "CLM_SUB_ID",
        "MBR_SFX_NO",
        "MBR_SK",
        "SUB_SK",
        "SUBGRP_SK",
        "BRTH_DT_SK",
        "MBR_GNDR_CD_SK",
        "MBR_RELSHP_CD_SK",
        "FIRST_NM",
        "MIDINIT",
        "LAST_NM",
        "SSN",
        "MBR_AGE",
        "ALPHA_PFX_SK"
    )
)

write_files(
    df_hf_drugclmupdt_mbrdata_final,
    f"{adls_path}/hf_drugclmupdt_mbrdata.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)