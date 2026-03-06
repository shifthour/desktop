# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_2 02/20/09 11:05:46 Batch  15027_39950 PROMOTE bckcetl ids20 dsadm bls for sa
# MAGIC ^1_2 02/20/09 10:48:45 Batch  15027_38927 INIT bckcett testIDS dsadm bls for sa
# MAGIC ^1_2 02/19/09 15:48:36 Batch  15026_56922 PROMOTE bckcett testIDS u03651 steph for Sharon
# MAGIC ^1_2 02/19/09 15:43:11 Batch  15026_56617 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_1 01/13/09 08:41:23 Batch  14989_31287 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_1 01/30/08 05:00:10 Batch  14640_18019 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 08/27/07 08:55:46 Batch  14484_32151 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 07/02/07 10:26:00 Batch  14428_37563 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_1 07/02/07 10:19:17 Batch  14428_37158 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_1 03/28/07 06:42:38 Batch  14332_24162 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 02/11/07 12:33:13 Batch  14287_45195 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_1 11/03/06 13:03:29 Batch  14187_47011 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_3 05/22/06 11:41:30 Batch  14022_42099 PROMOTE bckcetl ids20 dsadm Jim Hart
# MAGIC ^1_3 05/22/06 10:32:59 Batch  14022_37985 INIT bckcett testIDS30 dsadm Jim Hart
# MAGIC ^1_6 05/12/06 15:17:06 Batch  14012_55029 PROMOTE bckcett testIDS30 u10157 sa
# MAGIC ^1_6 05/12/06 15:03:19 Batch  14012_54207 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_5 05/11/06 16:20:33 Batch  14011_58841 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_4 05/11/06 12:36:55 Batch  14011_45419 PROMOTE bckcett devlIDS30 u10157 sa
# MAGIC ^1_4 05/11/06 12:31:29 Batch  14011_45092 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_1 05/02/06 13:04:24 Batch  14002_47066 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_2 01/26/06 07:54:36 Batch  13906_28483 INIT bckcetl ids20 dsadm Gina
# MAGIC ^1_1 01/24/06 14:49:28 Batch  13904_53375 PROMOTE bckcetl ids20 dsadm Gina Parr
# MAGIC ^1_1 01/24/06 14:47:17 Batch  13904_53242 INIT bckcett testIDS30 dsadm Gina Parr
# MAGIC ^1_2 01/24/06 14:36:07 Batch  13904_52574 INIT bckcett testIDS30 dsadm Gina Parr
# MAGIC ^1_1 01/24/06 14:21:30 Batch  13904_51695 INIT bckcett testIDS30 dsadm Gina Parr
# MAGIC ^1_2 12/23/05 11:39:15 Batch  13872_41979 PROMOTE bckcett testIDS30 u10913 Move Income Commission to test
# MAGIC ^1_2 12/23/05 11:12:01 Batch  13872_40337 INIT bckcett devlIDS30 u10913 Ollie Move Income/Commisions to test
# MAGIC ^1_1 12/23/05 10:39:46 Batch  13872_38400 INIT bckcett devlIDS30 u10913 Ollie Moving Income / Commission to Test
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsComsnBillRelFkey
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the primary key.   Preps the file for the foreign key lookup process that follows.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:  Primary Key output file
# MAGIC 	
# MAGIC   
# MAGIC HASH FILES:     hf_recycle
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  GetFkeyCodes
# MAGIC                             
# MAGIC PROCESSING:  Natural keys are converted to surrogate keys
# MAGIC 
# MAGIC OUTPUTS:   CLS table load file
# MAGIC                      hf_recycle - If recycling was used for this job records would be written to this hash file that had errors.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             Oliver Nielsen   - 10/20/2005  -  Originally programmed
# MAGIC             Tao Luo           - 10/20/2005 - Modified to IdsComsnBillRelFkey
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer               Date                         Change Description                                                                           Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------             ----------------------------     -----------------------------------------------------------------------                                   ----------------         ------------------------------------       ----------------------------      ----------------
# MAGIC Bhoomi Dasari     2008-09-19                   Added SrcSysCdSk parameter                                                          3567                 devlIDS                              Steph Goddard          09/22/2008
# MAGIC 
# MAGIC 
# MAGIC Goutham Kalidindi  2024-08-31              Part of facets release updated INCM_UNIQ_KEYfor from int to bigint.     US-625182               IntegrateDev3          Reddy Sanam           09/04/2024

# MAGIC Set all foreign surrogate keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC Read common record format file from extract job.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


TmpOutFile = get_widget_value('TmpOutFile','IdsComsnBillRelExtr.dat.fkey')
InFile = get_widget_value('InFile','IdsComsnBillRelExtr.dat.pkey')
Logging = get_widget_value('Logging','X')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

schema_IdsComsnBillRel = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("COMSN_BILL_REL_SK", IntegerType(), False),
    StructField("COMSN_BILL_REL_UNIQ_KEY", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("COCE_ID", StringType(), False),
    StructField("COAR_ID", StringType(), False),
    StructField("COAG_EFF_DT", StringType(), False),
    StructField("COSC_CALC_METH", StringType(), False),
    StructField("COBL_SOURCE_CD", StringType(), False),
    StructField("COBL_STS", IntegerType(), False),
    StructField("COMSN_BSS_AMT", DecimalType(38,10), False),
    StructField("INCM_AMT", DecimalType(38,10), False),
    StructField("BILL_CT", IntegerType(), False),
    StructField("COMSN_RELCALC_RQST_UNIQ_KEY", IntegerType(), False),
    StructField("INCM_UNIQ_KEY", IntegerType(), False),
    StructField("COMSN_PERD_NO", IntegerType(), False),
])

df_IdsComsnBillRel = (
    spark.read.format("csv")
    .option("header","false")
    .option("sep",",")
    .option("quote","\"")
    .schema(schema_IdsComsnBillRel)
    .load(f"{adls_path}/key/{InFile}")
)

df_Transformed = (
    df_IdsComsnBillRel
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("svAgnt", GetFkeyAgnt(F.col("SRC_SYS_CD"), F.col("COMSN_BILL_REL_SK"), F.col("COCE_ID"), F.lit(Logging)))
    .withColumn("svComsnAgmnt", GetFkeyComsnAgmnt(F.col("SRC_SYS_CD"), F.col("COMSN_BILL_REL_SK"), F.col("COAR_ID"), F.col("COAG_EFF_DT"), F.col("COCE_ID"), F.lit(Logging)))
    .withColumn("svComsnBillRelCalcMethCd", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("COMSN_BILL_REL_SK"), F.lit("COMMISSION BILLING RELATION CALC METHOD"), F.col("COSC_CALC_METH"), F.lit(Logging)))
    .withColumn("svComsnBillRelSrcCd", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("COMSN_BILL_REL_SK"), F.lit("COMMISSION BILLING RELATION SOURCE"), F.col("COBL_SOURCE_CD"), F.lit(Logging)))
    .withColumn("svComsnBillRelSttusCd", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("COMSN_BILL_REL_SK"), F.lit("COMMISSION BILLING RELATION STATUS"), F.col("COBL_STS"), F.lit(Logging)))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("COMSN_BILL_REL_SK")))
)

df_Fkey = df_Transformed.filter((F.col("ErrCount") == 0) | (F.col("PassThru") == 'Y'))
df_Fkey_out = df_Fkey.select(
    F.col("COMSN_BILL_REL_SK").alias("COMSN_BILL_REL_SK"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("COMSN_BILL_REL_UNIQ_KEY").alias("COMSN_BILL_REL_UNIQ_KEY"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(F.col("svAgnt").isNull(), F.lit(0)).otherwise(F.col("svAgnt")).alias("AGNT_SK"),
    F.when(F.col("svComsnAgmnt").isNull(), F.lit(0)).otherwise(F.col("svComsnAgmnt")).alias("COMSN_AGMNT_SK"),
    F.when(F.col("svComsnBillRelCalcMethCd").isNull(), F.lit(0)).otherwise(F.col("svComsnBillRelCalcMethCd")).alias("COMSN_BILL_REL_CALC_METH_CD_SK"),
    F.when(F.col("svComsnBillRelSrcCd").isNull(), F.lit(0)).otherwise(F.col("svComsnBillRelSrcCd")).alias("COMSN_BILL_REL_SRC_CD_SK"),
    F.when(F.col("svComsnBillRelSttusCd").isNull(), F.lit(0)).otherwise(F.col("svComsnBillRelSttusCd")).alias("COMSN_BILL_REL_STTUS_CD_SK"),
    F.col("COMSN_BSS_AMT").alias("COMSN_BSS_AMT"),
    F.col("INCM_AMT").alias("INCM_AMT"),
    F.col("BILL_CT").alias("BILL_CT"),
    F.col("COMSN_RELCALC_RQST_UNIQ_KEY").alias("COMSN_RELCALC_RQST_UNIQ_KEY"),
    F.col("INCM_UNIQ_KEY").alias("INCM_UNIQ_KEY"),
    F.col("COMSN_PERD_NO").alias("COMSN_PERD_NO")
)

df_Recycle = df_Transformed.filter(F.col("ErrCount") > 0)
df_Recycle_out = df_Recycle.select(
    GetRecycleKey(F.col("COMSN_BILL_REL_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("COMSN_BILL_REL_SK").alias("COMSN_BILL_REL_SK"),
    F.col("COMSN_BILL_REL_UNIQ_KEY").alias("COMSN_BILL_REL_UNIQ_KEY"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("COCE_ID").alias("COCE_ID"),
    F.col("COAR_ID").alias("COAR_ID"),
    F.col("COAG_EFF_DT").alias("COAG_EFF_DT"),
    F.col("COSC_CALC_METH").alias("COSC_CALC_METH"),
    F.col("COBL_SOURCE_CD").alias("COBL_SOURCE_CD"),
    F.col("COBL_STS").alias("COBL_STS"),
    F.col("COMSN_BSS_AMT").alias("COMSN_BSS_AMT"),
    F.col("INCM_AMT").alias("INCM_AMT"),
    F.col("BILL_CT").alias("BILL_CT"),
    F.col("COMSN_RELCALC_RQST_UNIQ_KEY").alias("COMSN_RELCALC_RQST_UNIQ_KEY"),
    F.col("INCM_UNIQ_KEY").alias("INCM_UNIQ_KEY"),
    F.col("COMSN_PERD_NO").alias("COMSN_PERD_NO")
)

df_Recycle_final = (
    df_Recycle_out
    .withColumn("INSRT_UPDT_CD", rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("SRC_SYS_CD", rpad(F.col("SRC_SYS_CD"), 100, " "))
    .withColumn("PRI_KEY_STRING", rpad(F.col("PRI_KEY_STRING"), 100, " "))
    .withColumn("COCE_ID", rpad(F.col("COCE_ID"), 12, " "))
    .withColumn("COAR_ID", rpad(F.col("COAR_ID"), 12, " "))
    .withColumn("COAG_EFF_DT", rpad(F.col("COAG_EFF_DT"), 10, " "))
    .withColumn("COSC_CALC_METH", rpad(F.col("COSC_CALC_METH"), 1, " "))
    .withColumn("COBL_SOURCE_CD", rpad(F.col("COBL_SOURCE_CD"), 1, " "))
)

recycle_cols_final = [
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "COMSN_BILL_REL_SK",
    "COMSN_BILL_REL_UNIQ_KEY",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "COCE_ID",
    "COAR_ID",
    "COAG_EFF_DT",
    "COSC_CALC_METH",
    "COBL_SOURCE_CD",
    "COBL_STS",
    "COMSN_BSS_AMT",
    "INCM_AMT",
    "BILL_CT",
    "COMSN_RELCALC_RQST_UNIQ_KEY",
    "INCM_UNIQ_KEY",
    "COMSN_PERD_NO"
]

df_Recycle_final = df_Recycle_final.select([F.col(c) for c in recycle_cols_final])

write_files(
    df_Recycle_final,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

windowSpec = Window.orderBy(F.lit(1))

df_DefaultUNK_1 = df_Transformed.withColumn("rownum", F.row_number().over(windowSpec))
df_DefaultUNK = df_DefaultUNK_1.filter(F.col("rownum") == 1).select(
    F.lit(0).alias("COMSN_BILL_REL_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit(0).alias("COMSN_BILL_REL_UNIQ_KEY"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("AGNT_SK"),
    F.lit(0).alias("COMSN_AGMNT_SK"),
    F.lit(0).alias("COMSN_BILL_REL_CALC_METH_CD_SK"),
    F.lit(0).alias("COMSN_BILL_REL_SRC_CD_SK"),
    F.lit(0).alias("COMSN_BILL_REL_STTUS_CD_SK"),
    F.lit(0).alias("COMSN_BSS_AMT"),
    F.lit(0).alias("INCM_AMT"),
    F.lit(0).alias("BILL_CT"),
    F.lit(0).alias("COMSN_RELCALC_RQST_UNIQ_KEY"),
    F.lit(0).alias("INCM_UNIQ_KEY"),
    F.lit(0).alias("COMSN_PERD_NO")
)

df_DefaultNA_1 = df_Transformed.withColumn("rownum", F.row_number().over(windowSpec))
df_DefaultNA = df_DefaultNA_1.filter(F.col("rownum") == 1).select(
    F.lit(1).alias("COMSN_BILL_REL_SK"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit(0).alias("COMSN_BILL_REL_UNIQ_KEY"),
    F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("AGNT_SK"),
    F.lit(1).alias("COMSN_AGMNT_SK"),
    F.lit(1).alias("COMSN_BILL_REL_CALC_METH_CD_SK"),
    F.lit(1).alias("COMSN_BILL_REL_SRC_CD_SK"),
    F.lit(1).alias("COMSN_BILL_REL_STTUS_CD_SK"),
    F.lit(0).alias("COMSN_BSS_AMT"),
    F.lit(0).alias("INCM_AMT"),
    F.lit(0).alias("BILL_CT"),
    F.lit(1).alias("COMSN_RELCALC_RQST_UNIQ_KEY"),
    F.lit(1).alias("INCM_UNIQ_KEY"),
    F.lit(0).alias("COMSN_PERD_NO")
)

collector_cols = [
    "COMSN_BILL_REL_SK",
    "SRC_SYS_CD_SK",
    "COMSN_BILL_REL_UNIQ_KEY",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "AGNT_SK",
    "COMSN_AGMNT_SK",
    "COMSN_BILL_REL_CALC_METH_CD_SK",
    "COMSN_BILL_REL_SRC_CD_SK",
    "COMSN_BILL_REL_STTUS_CD_SK",
    "COMSN_BSS_AMT",
    "INCM_AMT",
    "BILL_CT",
    "COMSN_RELCALC_RQST_UNIQ_KEY",
    "INCM_UNIQ_KEY",
    "COMSN_PERD_NO"
]

df_Collector_1 = df_Fkey_out.select([F.col(c) for c in collector_cols])
df_Collector_2 = df_DefaultUNK.select([F.col(c) for c in collector_cols])
df_Collector_3 = df_DefaultNA.select([F.col(c) for c in collector_cols])

df_Collector = df_Collector_1.unionByName(df_Collector_2).unionByName(df_Collector_3)

write_files(
    df_Collector,
    f"{adls_path}/load/{TmpOutFile}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)