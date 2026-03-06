# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 08/31/07 14:39:46 Batch  14488_52789 INIT bckcetl edw10 dsadm dsadm
# MAGIC ^1_2 02/16/07 14:56:33 Batch  14292_53801 PROMOTE bckcetl edw10 dsadm Keith for Sharon
# MAGIC ^1_2 02/16/07 14:54:26 Batch  14292_53674 INIT bckcett testEDW10 dsadm Keith for Sharon
# MAGIC ^1_16 02/16/07 14:40:30 Batch  14292_52839 PROMOTE bckcett testEDW10 u10157 sa
# MAGIC ^1_16 02/16/07 14:38:18 Batch  14292_52701 PROMOTE bckcett testEDW10 u10157 sa
# MAGIC ^1_16 02/16/07 14:36:59 Batch  14292_52621 INIT bckcett devlEDW10 u10157 sa
# MAGIC ^1_15 02/16/07 14:34:02 Batch  14292_52444 INIT bckcett devlEDW10 u10157 sa
# MAGIC ^1_13 02/07/07 13:40:29 Batch  14283_49233 PROMOTE bckcett devlEDW10 u10157 sa
# MAGIC ^1_13 02/07/07 13:17:42 Batch  14283_47863 INIT bckcett testEDW10 u10157 sa
# MAGIC ^1_12 02/02/07 13:50:55 Batch  14278_49860 PROMOTE bckcett testEDW10 u10157 sa
# MAGIC ^1_12 02/02/07 13:50:15 Batch  14278_49817 PROMOTE bckcett devlEDW10 u10157 sa
# MAGIC ^1_12 02/02/07 13:49:27 Batch  14278_49768 INIT bckcetl edw10 dcg01 sa
# MAGIC ^1_11 02/01/07 15:06:16 Batch  14277_54378 INIT bckcetl edw10 dcg01 sa
# MAGIC ^1_10 02/01/07 13:32:45 Batch  14277_48774 INIT bckcetl edw10 dcg01 sa
# MAGIC ^1_9 01/31/07 16:48:40 Batch  14276_60522 INIT bckcetl edw10 dcg01 sa
# MAGIC ^1_8 01/30/07 16:40:20 Batch  14275_60023 PROMOTE bckcett edw10 u10157 sa
# MAGIC ^1_8 01/30/07 16:35:06 Batch  14275_59709 INIT bckcett testEDW10 u10157 SA
# MAGIC ^1_7 01/30/07 15:21:33 Batch  14275_55295 PROMOTE bckcett testEDW10 u10157 sa
# MAGIC ^1_7 01/30/07 15:19:35 Batch  14275_55179 INIT bckcetl edw10 dcg01 sa
# MAGIC ^1_6 01/29/07 13:06:32 Batch  14274_47199 PROMOTE bckcett edw10 u10157 sa
# MAGIC ^1_6 01/29/07 13:01:29 Batch  14274_46893 INIT bckcett testEDW10 u10157 sa
# MAGIC ^1_5 01/25/07 11:33:34 Batch  14270_41615 INIT bckcett testEDW10 u10157 sa
# MAGIC ^1_4 01/25/07 11:32:30 Batch  14270_41552 INIT bckcett testEDW10 u10157 sa
# MAGIC ^1_1 01/25/07 09:59:41 Batch  14270_35996 INIT bckcett testEDW10 dsadm bls
# MAGIC ^1_3 01/22/07 16:18:40 Batch  14267_58774 PROMOTE bckcett testEDW10 u10157 sa
# MAGIC ^1_3 01/22/07 16:13:54 Batch  14267_58435 INIT bckcett devlEDW10 u10157 sa
# MAGIC ^1_2 01/19/07 10:04:39 Batch  14264_36281 INIT bckcett devlEDW10 u10157 sa
# MAGIC ^1_1 11/10/06 14:26:19 Batch  14194_51981 INIT bckcett devlEDW10 u10157 SA
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     EdwPcaFExtract
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC       
# MAGIC 	        Pulls data from EDW tables to create the EDW PCA_F table. 
# MAGIC INPUTS:
# MAGIC 	
# MAGIC 	        Following EDW tables are used -
# MAGIC 
# MAGIC 		P_PCA
# MAGIC 		
# MAGIC   
# MAGIC HASH FILES:
# MAGIC    		NONE
# MAGIC             
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC 		
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                    There are 2 processes that affect data in this table.  This is the first process which loads claims sent to Empowered Benefits and runs weekly after the                                  completion of the job that popuates the P_PCA table.  The second process updates some fields based on a file received from Empowered Benefits.
# MAGIC 
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC 		Comma Delimited flat file created under /edw/<environment>/load/PCA_F.dat
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               
# MAGIC Date                 Developer                Change Description
# MAGIC ------------------      ----------------------------     --------------------------------------------------------------------------------------------------------
# MAGIC 2006-04-21      Suzanne Saylor          Original Programming.
# MAGIC 2007-01-10      Sharon Andrew          Added issue fixes.
# MAGIC 2007-01-26      Oliver Nielsen             Changed Paid Date Mapping to be 1753-01-01 if the source row has 'NA'

# MAGIC Check to see if the PCA claim aleady exists on the EDW PCA_F table.   Need to know to change claim vendor status code.
# MAGIC Pull claims from EDW CLM_F that are on the P_PCA table.   P_PCA table is populated in previous sequencer EdwPPcaSeq.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
RunCycle = get_widget_value('RunCycle','100')
RunCycleDate = get_widget_value('RunCycleDate','2007-01-03')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

extract_query_1 = (
    "SELECT pca.CLM_ID, pca.SRC_SYS_CD, pca.PCA_STTUS_CD, "
    "clm.CLS_SK, clm.GRP_SK, clm.MBR_SK, clm.PD_PROV_SK, clm.PROD_SK, clm.SUBGRP_SK, "
    "clm.CLM_PD_DT_SK, clm.CLM_PD_YR_MO_SK, clm.SUB_SK, pca.CLM_SVC_STRT_DT, pca.PCA_HLTH_RMBRMT_ACCT_AMT "
    f"FROM {EDWOwner}.P_PCA pca, {EDWOwner}.CLM_F clm "
    "WHERE clm.CLM_ID = pca.CLM_ID "
    "AND clm.SRC_SYS_CD = pca.SRC_SYS_CD "
    "AND pca.PCA_EXCL_IN = 'N'"
)

df_EDW_CLM_F = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_1)
    .load()
)

extract_query_2 = (
    "SELECT DISTINCT "
    "CLM_ID, SRC_SYS_CD, PCA_STTUS_CD, CRT_RUN_CYC_EXCTN_DT_SK, LAST_UPDT_RUN_CYC_EXCTN_DT_SK, "
    "PCA_SK, CLS_SK, GRP_SK, MBR_SK, PROD_SK, SUBGRP_SK, SUB_SK, PROV_SK, PCA_VNDR_STTUS_CD, "
    "PCA_FORC_SUB_PAY_IN, PCA_PD_DT, PCA_PD_YR_MO, PCA_SENT_TO_VNDR_DT, PCA_SENT_TO_VNDR_YR_MO, "
    "PCA_SVC_DT, PCA_SVC_YR_MO, PCA_VNDR_CHK_ISSUE_DT, PCA_VNDR_CHK_ISSUE_YR_MO, PCA_AVLBL_AMT, "
    "PCA_BNF_AMT, PCA_PROV_CHK_AMT, PCA_SUB_CHK_AMT, PCA_PROV_CHK_NO, PCA_SUB_CHK_NO, PCA_TPA_CTL_NO, "
    "CRT_RUN_CYC_EXCTN_SK, LAST_UPDT_RUN_CYC_EXCTN_SK "
    f"FROM {EDWOwner}.PCA_F "
    "WHERE PCA_STTUS_CD <> '50' "
    "AND PCA_STTUS_CD <> '51'"
)

df_EDW_PCA_F = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_2)
    .load()
)

df_joined = df_EDW_CLM_F.alias("Extract").join(
    df_EDW_PCA_F.alias("already_exists_PCA_F"),
    [
        F.col("Extract.CLM_ID") == F.col("already_exists_PCA_F.CLM_ID"),
        F.col("Extract.SRC_SYS_CD") == F.col("already_exists_PCA_F.SRC_SYS_CD"),
    ],
    "left"
)

df_new = df_joined.filter(F.isnull(F.col("already_exists_PCA_F.CLM_ID"))).select(
    F.lit(None).alias("PCA_SK"),
    trim(F.col("Extract.SRC_SYS_CD")).alias("SRC_SYS_CD"),
    trim(F.col("Extract.CLM_ID")).alias("CLM_ID"),
    trim(F.col("Extract.PCA_STTUS_CD")).alias("PCA_STTUS_CD"),
    F.lit(RunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(RunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("Extract.CLS_SK").alias("CLS_SK"),
    F.col("Extract.GRP_SK").alias("GRP_SK"),
    F.col("Extract.MBR_SK").alias("MBR_SK"),
    F.col("Extract.PROD_SK").alias("PROD_SK"),
    F.col("Extract.SUBGRP_SK").alias("SUBGRP_SK"),
    F.col("Extract.SUB_SK").alias("SUB_SK"),
    F.col("Extract.PD_PROV_SK").alias("PROV_SK"),
    F.when(
        (trim(F.col("Extract.PCA_STTUS_CD")) == "A02")
        | (trim(F.col("Extract.PCA_STTUS_CD")) == "A09"),
        F.lit("EXTR")
    )
    .when(trim(F.col("Extract.PCA_STTUS_CD")) == "A08", F.lit("NO EXTR"))
    .otherwise(F.lit("UNK"))
    .alias("PCA_VNDR_STTUS_CD"),
    F.lit("Y").alias("PCA_FORC_SUB_PAY_IN"),
    F.when(trim(F.col("Extract.CLM_PD_DT_SK")) == "NA", F.lit("1753-01-01"))
    .otherwise(trim(F.col("Extract.CLM_PD_DT_SK")))
    .alias("PCA_PD_DT"),
    F.when(F.length(trim(F.col("Extract.CLM_PD_YR_MO_SK"))) == 0, F.lit("UNK"))
    .otherwise(trim(F.col("Extract.CLM_PD_YR_MO_SK")))
    .alias("PCA_PD_YR_MO"),
    F.lit("2199-12-31").alias("PCA_SENT_TO_VNDR_DT"),
    F.lit("219912").alias("PCA_SENT_TO_VNDR_YR_MO"),
    F.when(
        F.length(trim(F.col("Extract.CLM_SVC_STRT_DT"))) > 0,
        F.col("Extract.CLM_SVC_STRT_DT"),
    ).otherwise(F.lit("1753-01-01")).alias("PCA_SVC_DT"),
    F.when(
        F.length(trim(F.col("Extract.CLM_SVC_STRT_DT"))) > 0,
        FORMAT_DATE(F.col("Extract.CLM_SVC_STRT_DT"), "DATE", "DATE", "CCYYMM"),
    ).otherwise(F.lit("175301")).alias("PCA_SVC_YR_MO"),
    F.lit("2199-12-31").alias("PCA_VNDR_CHK_ISSUE_DT"),
    F.lit("219912").alias("PCA_VNDR_CHK_ISSUE_YR_MO"),
    F.col("Extract.PCA_HLTH_RMBRMT_ACCT_AMT").alias("PCA_AVLBL_AMT"),
    F.lit(0).alias("PCA_BNF_AMT"),
    F.lit(0).alias("PCA_PROV_CHK_AMT"),
    F.lit(0).alias("PCA_SUB_CHK_AMT"),
    F.lit(0).alias("PCA_PROV_CHK_NO"),
    F.lit(0).alias("PCA_SUB_CHK_NO"),
    F.lit(0).alias("PCA_TPA_CTL_NO"),
    F.lit(RunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
)

df_update = df_joined.filter(F.isnull(F.col("already_exists_PCA_F.CLM_ID")) == False).select(
    F.col("already_exists_PCA_F.PCA_SK").alias("PCA_SK"),
    trim(F.col("already_exists_PCA_F.SRC_SYS_CD")).alias("SRC_SYS_CD"),
    trim(F.col("already_exists_PCA_F.CLM_ID")).alias("CLM_ID"),
    F.when(
        (trim(F.col("already_exists_PCA_F.PCA_STTUS_CD")) == "50")
        | (trim(F.col("already_exists_PCA_F.PCA_STTUS_CD")) == "51"),
        trim(F.col("already_exists_PCA_F.PCA_STTUS_CD"))
    ).otherwise(trim(F.col("Extract.PCA_STTUS_CD")))
    .alias("PCA_STTUS_CD"),
    F.col("already_exists_PCA_F.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(RunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("already_exists_PCA_F.CLS_SK").alias("CLS_SK"),
    F.col("already_exists_PCA_F.GRP_SK").alias("GRP_SK"),
    F.col("already_exists_PCA_F.MBR_SK").alias("MBR_SK"),
    F.col("already_exists_PCA_F.PROD_SK").alias("PROD_SK"),
    F.col("already_exists_PCA_F.SUBGRP_SK").alias("SUBGRP_SK"),
    F.col("already_exists_PCA_F.SUB_SK").alias("SUB_SK"),
    F.col("already_exists_PCA_F.PROV_SK").alias("PROV_SK"),
    trim(F.col("already_exists_PCA_F.PCA_VNDR_STTUS_CD")).alias("PCA_VNDR_STTUS_CD"),
    F.col("already_exists_PCA_F.PCA_FORC_SUB_PAY_IN").alias("PCA_FORC_SUB_PAY_IN"),
    F.when(trim(F.col("Extract.CLM_PD_DT_SK")) == "NA", F.lit("1753-01-01"))
    .otherwise(trim(F.col("Extract.CLM_PD_DT_SK")))
    .alias("PCA_PD_DT"),
    F.when(trim(F.col("Extract.CLM_PD_YR_MO_SK")) == "NA", F.lit("175301"))
    .otherwise(trim(F.col("Extract.CLM_PD_YR_MO_SK")))
    .alias("PCA_PD_YR_MO"),
    trim(F.col("already_exists_PCA_F.PCA_SENT_TO_VNDR_DT")).alias("PCA_SENT_TO_VNDR_DT"),
    F.col("already_exists_PCA_F.PCA_SENT_TO_VNDR_YR_MO").alias("PCA_SENT_TO_VNDR_YR_MO"),
    F.col("already_exists_PCA_F.PCA_SVC_DT").alias("PCA_SVC_DT"),
    F.col("already_exists_PCA_F.PCA_SVC_YR_MO").alias("PCA_SVC_YR_MO"),
    F.col("already_exists_PCA_F.PCA_VNDR_CHK_ISSUE_DT").alias("PCA_VNDR_CHK_ISSUE_DT"),
    F.col("already_exists_PCA_F.PCA_VNDR_CHK_ISSUE_YR_MO").alias("PCA_VNDR_CHK_ISSUE_YR_MO"),
    F.col("already_exists_PCA_F.PCA_AVLBL_AMT").alias("PCA_AVLBL_AMT"),
    F.col("already_exists_PCA_F.PCA_BNF_AMT").alias("PCA_BNF_AMT"),
    F.col("already_exists_PCA_F.PCA_PROV_CHK_AMT").alias("PCA_PROV_CHK_AMT"),
    F.col("already_exists_PCA_F.PCA_SUB_CHK_AMT").alias("PCA_SUB_CHK_AMT"),
    F.col("already_exists_PCA_F.PCA_PROV_CHK_NO").alias("PCA_PROV_CHK_NO"),
    F.col("already_exists_PCA_F.PCA_SUB_CHK_NO").alias("PCA_SUB_CHK_NO"),
    F.col("already_exists_PCA_F.PCA_TPA_CTL_NO").alias("PCA_TPA_CTL_NO"),
    F.col("already_exists_PCA_F.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
)

df_merged = df_new.unionByName(df_update)

df_merged = SurrogateKeyGen(df_merged,<DB sequence name>,'PCA_SK',<schema>,<secret_name>)

# Select final columns in the same order as specified by the last stage
df_final = df_merged.select(
    "PCA_SK",
    "SRC_SYS_CD",
    "CLM_ID",
    "PCA_STTUS_CD",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "CLS_SK",
    "GRP_SK",
    "MBR_SK",
    "PROD_SK",
    "SUBGRP_SK",
    "SUB_SK",
    "PROV_SK",
    "PCA_VNDR_STTUS_CD",
    "PCA_FORC_SUB_PAY_IN",
    "PCA_PD_DT",
    "PCA_PD_YR_MO",
    "PCA_SENT_TO_VNDR_DT",
    "PCA_SENT_TO_VNDR_YR_MO",
    "PCA_SVC_DT",
    "PCA_SVC_YR_MO",
    "PCA_VNDR_CHK_ISSUE_DT",
    "PCA_VNDR_CHK_ISSUE_YR_MO",
    "PCA_AVLBL_AMT",
    "PCA_BNF_AMT",
    "PCA_PROV_CHK_AMT",
    "PCA_SUB_CHK_AMT",
    "PCA_PROV_CHK_NO",
    "PCA_SUB_CHK_NO",
    "PCA_TPA_CTL_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK"
)

df_final = df_final.withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK", F.rpad("CRT_RUN_CYC_EXCTN_DT_SK", 10, " ")
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", 10, " ")
).withColumn(
    "PCA_FORC_SUB_PAY_IN", F.rpad("PCA_FORC_SUB_PAY_IN", 1, " ")
).withColumn(
    "PCA_PD_DT", F.rpad("PCA_PD_DT", 10, " ")
).withColumn(
    "PCA_PD_YR_MO", F.rpad("PCA_PD_YR_MO", 6, " ")
).withColumn(
    "PCA_SENT_TO_VNDR_DT", F.rpad("PCA_SENT_TO_VNDR_DT", 10, " ")
).withColumn(
    "PCA_SENT_TO_VNDR_YR_MO", F.rpad("PCA_SENT_TO_VNDR_YR_MO", 6, " ")
).withColumn(
    "PCA_SVC_DT", F.rpad("PCA_SVC_DT", 10, " ")
).withColumn(
    "PCA_SVC_YR_MO", F.rpad("PCA_SVC_YR_MO", 6, " ")
).withColumn(
    "PCA_VNDR_CHK_ISSUE_DT", F.rpad("PCA_VNDR_CHK_ISSUE_DT", 10, " ")
).withColumn(
    "PCA_VNDR_CHK_ISSUE_YR_MO", F.rpad("PCA_VNDR_CHK_ISSUE_YR_MO", 6, " ")
)

write_files(
    df_final,
    f"{adls_path}/load/PCA_F.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)