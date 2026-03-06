# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_2 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
# MAGIC ^1_4 02/10/09 11:16:45 Batch  15017_40611 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_3 02/02/09 09:52:49 Batch  15009_35578 PROMOTE bckcetl ids20 dsadm bls for brent
# MAGIC ^1_3 01/26/09 13:03:15 Batch  15002_46999 INIT bckcett testIDS dsadm bls for bl
# MAGIC ^1_1 12/16/08 10:00:18 Batch  14961_36031 PROMOTE bckcett testIDS u03651 steph for Brent - claim primary keying
# MAGIC ^1_1 12/16/08 09:15:37 Batch  14961_33343 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_1 01/29/08 08:55:59 Batch  14639_32164 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/28/07 10:13:19 Batch  14607_36806 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/26/07 09:37:10 Batch  14575_34636 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 08/30/07 07:19:35 Batch  14487_26382 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 08/27/07 10:06:05 Batch  14484_36371 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_3 03/26/07 12:34:37 Batch  14330_45286 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 03/07/07 15:24:05 Batch  14311_55447 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_5 12/18/06 11:16:18 Batch  14232_40614 INIT bckcetl ids20 dsadm Backup for 12/18 install
# MAGIC ^1_4 04/17/06 10:35:58 Batch  13987_38166 PROMOTE bckcetl ids20 dsadm J. mahaffey
# MAGIC ^1_4 04/17/06 10:24:16 Batch  13987_37468 INIT bckcett testIDS30 dsadm J. Mahaffey for B. Leland
# MAGIC ^1_3 04/13/06 08:47:39 Batch  13983_31666 PROMOTE bckcett testIDS30 u08717 Brent
# MAGIC ^1_3 04/13/06 07:36:31 Batch  13983_27397 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_2 04/02/06 15:43:48 Batch  13972_56632 INIT bckcett devlIDS30 dcg01 steffy
# MAGIC ^1_3 03/29/06 15:32:39 Batch  13968_55966 INIT bckcett devlIDS30 dsadm Brent
# MAGIC ^1_2 03/29/06 14:54:04 Batch  13968_53650 INIT bckcett devlIDS30 dsadm Brent
# MAGIC ^1_1 03/29/06 05:18:35 Batch  13968_19121 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_1 03/23/06 14:55:53 Batch  13962_53758 INIT bckcett devlIDS30 dsadm Brent
# MAGIC ^1_1 03/20/06 13:57:04 Batch  13959_50244 INIT bckcett devlIDS30 dsadm Brent
# MAGIC ^1_1 01/06/06 12:42:54 Batch  13886_45778 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC 
# MAGIC ********************************************************************************
# MAGIC 
# MAGIC  Copyright 2005 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC JOB NAME: FctsClmLnHashFile3Extr
# MAGIC CALLED BY:  FctsClmPrereqSeq
# MAGIC 
# MAGIC DESCRIPTION:  One of three hash file extracts that must run in order (1,2,3)
# MAGIC 
# MAGIC \(9)Extract Claim Line Disallow Data extract from Facets, and load to hash files to be used in FctsClmLnTrns, FctsClmLnDntlTrns and FctsClmLnDsalwExtr
# MAGIC                 Extract rows from the disallow tables for W_CLM_LN_DSALW - pull all rows for claims on TMP_DRIVER
# MAGIC 
# MAGIC  SOURCE:
# MAGIC 
# MAGIC \(9)CMC_CDML_CL_LINE\(9) 
# MAGIC                 CMC_CDDL_CL_LINE
# MAGIC                 CMC_CDMD_LI_DISALL
# MAGIC                 CMC_CDDD_DNLI_DIS
# MAGIC                 CMC_CDOR_LI_OVR
# MAGIC                 CMC_CDDO_DNLI_OVR
# MAGIC                 CMC_CLCL_CLAIM
# MAGIC                 CMC_CLMI_MISC
# MAGIC                  CMC_CDCB_LI_COB
# MAGIC                 TMP_DRIVER
# MAGIC                  IDS CD_MPPNG
# MAGIC                  IDS DSALW_EXCD
# MAGIC 
# MAGIC 
# MAGIC  Hash Files
# MAGIC               used in FctsClmLnTrns, FctsClmLnDntlTrns, FctsClmLnDsalwExtr
# MAGIC 
# MAGIC                    hf_clm_ln_dsalw_tu
# MAGIC                    hf_clm_ln_dsalw_a
# MAGIC                    hf_clm_ln_dsalw_x
# MAGIC                    hf_clm_ln_dsalw_cdmd
# MAGIC                    hf_clm_ln_dsalw_cddd
# MAGIC                    hf_clm_ln_dsalw_cdml
# MAGIC                    hf_clm_ln_dsalw_cddl
# MAGIC                    hf_clm_ln_dsalw_cap
# MAGIC  
# MAGIC                   hf_clm_ln_dsalw_ln_amts - used in FctsClmLnMedExtr and FctsDntlClmLineExtr to calculate final disposition code
# MAGIC                                                              used in FctsClmLnMedTrns and FctsClmLnDntlTrns to determine provider write off, member responsibility and non member responsibility
# MAGIC 
# MAGIC             used and deleted in this job
# MAGIC                  hf_clm_ln_its_tu
# MAGIC                  hf_clm_ln_cdor_u
# MAGIC                  hf_clm_ln_cdor_at
# MAGIC                  hf_clm_ln_dsalw_cdmd_sum
# MAGIC                  hf_clm_ln_dsalw_cddd_sum
# MAGIC                  hf_clm_ln_hmo_retire_clms
# MAGIC                  hf_clm_ln_dsalw_prod
# MAGIC                    hf_clm_ln_dsalw_tu_tmp
# MAGIC                    hf_clm_ln_dsalw_a_tmp
# MAGIC                    hf_clm_ln_dsalw_x_tmp
# MAGIC                   hf_clm_ln_retire_clms
# MAGIC                   hf_clm_ln_dsalw_cob_clms
# MAGIC                
# MAGIC 10;hf_clm_ln_its_tu;hf_clm_ln_cdor_u;hf_clm_ln_cdor_at;hf_clm_ln_dsalw_cdmd_sum;hf_clm_ln_dsalw_cddd_sum;hf_clm_ln_dsalw_tu_tmp;hf_clm_ln_dsalw_a_tmp;hf_clm_ln_dsalw_x_tmp;hf_clm_ln_retire_clms;hf_clm_ln_dsalw_cob_clms
# MAGIC 
# MAGIC 
# MAGIC OUTPUTS:
# MAGIC 
# MAGIC \(9)W_CLM_LN_DSALW.dat to be loaded in IDS, must go through the delete process
# MAGIC  
# MAGIC                 IDS parameters
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer\(9)Date\(9)\(9)Project/Altiris #\(9)Change Description\(9)\(9)\(9)\(9)\(9)Development Project\(9)\(9)Code Reviewer\(9)Date Reviewed       
# MAGIC ====================================================================================================================================================================================
# MAGIC BJ Luce                  \(9)3/30/2006               \(9)\(9)\(9)initial program
# MAGIC BJ Luce                                                   \(9)\(9)\(9)sum of all disallows by clm/clm line by bypass indicator and 
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)responsibility code to  hf_clm_ln_dsalw_ln_amts 
# MAGIC Ralph Tucker    \(9)08/01/2008      \(9)3657\(9)\(9)Primary Key   Modified DSALW_EXCD ref link to a hash              \(9)devlIDS                        
# MAGIC                                                                                        \(9)\(9)reference created in FctsClmLnHashFile2Extr
# MAGIC Prabhu ES          \(9)2022-02-26       \(9)S2S Remediation\(9)MSSQL connection parameters added          \(9)\(9)IntegrateDev5\(9)\(9)Ken Bradmon\(9)2022-06-08

# MAGIC Facets Claim Line Hash File 3 Extract
# MAGIC Build hash files for dsalw rows to use in FctsClmLnTrns, FctsClmLnDntlTrns, and FctsClmLnDsalwExtr
# MAGIC Do not clear
# MAGIC                    hf_clm_ln_dsalw_tu
# MAGIC                    hf_clm_ln_dsalw_a
# MAGIC                    hf_clm_ln_dsalw_x
# MAGIC                    hf_clm_ln_dsalw_cdmd
# MAGIC                    hf_clm_ln_dsalw_cddd
# MAGIC                    hf_clm_ln_dsalw_cdml
# MAGIC                    hf_clm_ln_dsalw_cddl
# MAGIC Created in:
# MAGIC FctsClmLnHashFile2Extr
# MAGIC Build hash files for dsalw rows to use in 
# MAGIC FctsClmLnDsalwExtr
# MAGIC Do not clear
# MAGIC                    hf_clm_ln_dsalw_tu
# MAGIC                    hf_clm_ln_dsalw_a
# MAGIC                    hf_clm_ln_dsalw_x
# MAGIC                    hf_clm_ln_dsalw_cdmd
# MAGIC                    hf_clm_ln_dsalw_cddd
# MAGIC                    hf_clm_ln_dsalw_cdml
# MAGIC                    hf_clm_ln_dsalw_cddl
# MAGIC                    hf_clm_ln_dsalw_cap
# MAGIC 
# MAGIC Build hash file to use in FctsClmLnTrns, FctsClmLnDntlTrns, FctsClmLnMedExtr, FctsDntlClmLineExtr
# MAGIC 
# MAGIC                  hf_clm_ln_dsalw_ln_amts
# MAGIC These hashfiles are loaded in FctsClmLnHashFile1Extr
# MAGIC 
# MAGIC Also used in FctsClmLnHashFile2Extr
# MAGIC These hashfiles are loaded in FctsClmLnHashFile2Extr
# MAGIC This hashfile is loaded in FctsClmLnHashFile2Extr
# MAGIC This hashfile is loaded in FctsClmLnHashFile2Extr
# MAGIC sum of all disallows by clm/clm line by bypass indicator and responsibility code
# MAGIC 
# MAGIC hash file used in FctsClmLnMedExtr and FctsDntlClmLineExtr to calculate final disposition code
# MAGIC 
# MAGIC hash file used in FctsClmLnMedTrns and FctsClmLnDntlTrns to determine provider write off, member responsibility and non member responsibility
# MAGIC Hash Files created in FctsClmLnExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Parameters
Source = get_widget_value('Source','FACETS')
DriverTable = get_widget_value('DriverTable','')
RunCycle = get_widget_value('RunCycle','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')

# Read Hashed Files (Scenario C => read from corresponding Parquet)
df_cdmd_hash = spark.read.parquet(f"{adls_path}/hf_clm_ln_dsalw_cdmd.parquet")
df_cddd_hash = spark.read.parquet(f"{adls_path}/hf_clm_ln_dsalw_cddd.parquet")
df_hash_x_tmp = spark.read.parquet(f"{adls_path}/hf_clm_ln_dsalw_x_tmp.parquet")
df_hash_a_tmp = spark.read.parquet(f"{adls_path}/hf_clm_ln_dsalw_a_tmp.parquet")
df_hash_tu_tmp = spark.read.parquet(f"{adls_path}/hf_clm_ln_dsalw_tu_tmp.parquet")
df_cdmd_sum_hash = spark.read.parquet(f"{adls_path}/hf_clm_ln_dsalw_cdmd_sum.parquet")
df_sum_cddd_hash = spark.read.parquet(f"{adls_path}/hf_clm_ln_dsalw_cddd_sum.parquet")

# BuildOverrideHashFiles (CContainerStage) pass-through mappings
df_tu_cap = df_hash_tu_tmp
df_a_cap = df_hash_a_tmp
df_x_cap = df_hash_x_tmp
df_cdml_tu_lkup = df_cdmd_hash
df_cdml_a_lkup = df_cdmd_hash
df_cdml_x_lkup = df_cdmd_hash
df_cddl_tu_lkup = df_hash_tu_tmp
df_cddl_a_lkup = df_hash_a_tmp
df_cddl_x_lkup = df_hash_x_tmp
df_dsalw_a = df_hash_a_tmp
df_dsalw_x = df_hash_x_tmp
df_dsalw_tu = df_hash_tu_tmp

# BuildCddlCdmlHashFile (CContainerStage) pass-through mappings
df_cdml_cap = df_cdmd_hash
df_cddl_cap = df_hash_tu_tmp
df_dsalw_cdml = df_cdmd_hash
df_dsalw_cddl = df_hash_tu_tmp

# ContainerC114 (CContainerStage) pass-through mapping
df_dsalw_cap = df_cdmd_hash

# bring_em_together (CCollector) union of inputs
df_dsalw_cdmd = df_cdmd_hash
df_dsalw_cddd = df_cddd_hash

# All of these DataFrames have at least:
# "CLCL_ID", "CDML_SEQ_NO", "DSALW_AMT", "EXCD_RESP_CD", "BYPS_IN" 
# for the aggregator. Union them by the needed columns:
df_bring_em_together = (
    df_dsalw_cdmd.select("CLCL_ID","CDML_SEQ_NO","EXCD_RESP_CD","BYPS_IN","DSALW_AMT")
    .unionByName(df_dsalw_a.select("CLCL_ID","CDML_SEQ_NO","EXCD_RESP_CD","BYPS_IN","DSALW_AMT"))
    .unionByName(df_dsalw_tu.select("CLCL_ID","CDML_SEQ_NO","EXCD_RESP_CD","BYPS_IN","DSALW_AMT"))
    .unionByName(df_dsalw_cdml.select("CLCL_ID","CDML_SEQ_NO","EXCD_RESP_CD","BYPS_IN","DSALW_AMT"))
    .unionByName(df_dsalw_x.select("CLCL_ID","CDML_SEQ_NO","EXCD_RESP_CD","BYPS_IN","DSALW_AMT"))
    .unionByName(df_dsalw_cap.select("CLCL_ID","CDML_SEQ_NO","EXCD_RESP_CD","BYPS_IN","DSALW_AMT"))
    .unionByName(df_dsalw_cddd.select("CLCL_ID","CDML_SEQ_NO","EXCD_RESP_CD","BYPS_IN","DSALW_AMT"))
    .unionByName(df_dsalw_cddl.select("CLCL_ID","CDML_SEQ_NO","EXCD_RESP_CD","BYPS_IN","DSALW_AMT"))
)

# Aggregator_250
df_aggregator_250 = (
    df_bring_em_together
    .groupBy("CLCL_ID","CDML_SEQ_NO","EXCD_RESP_CD","BYPS_IN")
    .agg(F.sum("DSALW_AMT").alias("DSALW_AMT"))
)

# Transformer_251 with constraint DSLink252.DSALW_AMT > 0
df_transformer_251 = df_aggregator_250.filter("DSALW_AMT > 0")

# Final select (keep the same order and apply rpad for char/varchar columns with known or unknown lengths)
df_final = (
    df_transformer_251
    .select(
        "CLCL_ID",
        "CDML_SEQ_NO",
        "EXCD_RESP_CD",
        "BYPS_IN",
        "DSALW_AMT"
    )
    .withColumn("CLCL_ID", F.rpad(F.col("CLCL_ID"), 12, " "))
    .withColumn("BYPS_IN", F.rpad(F.col("BYPS_IN"), 1, " "))
    .withColumn("EXCD_RESP_CD", F.rpad(F.col("EXCD_RESP_CD"), <...>, " "))
)

# hf_clm_ln_dsalw_ln_amts (CHashedFileStage, scenario C => write parquet)
write_files(
    df_final,
    f"{adls_path}/hf_clm_ln_dsalw_ln_amts.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)