# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_7 09/22/09 15:32:09 Batch  15241_56279 PROMOTE bckcetl:31540 ids20 dsadm bls for sa
# MAGIC ^1_7 09/22/09 15:22:02 Batch  15241_55325 INIT bckcett:31540 testIDSnew dsadm bls for sa
# MAGIC ^1_5 09/22/09 09:19:44 Batch  15241_33586 INIT bckcett:31540 testIDSnew dsadm bls for sa
# MAGIC ^1_2 07/06/09 11:52:09 Batch  15163_42760 PROMOTE bckcett:31540 testIDSnew u150906 3833-RemitAlternateCharge_Sharon_testIDSnew             Maddy
# MAGIC ^1_2 07/06/09 11:37:10 Batch  15163_41931 INIT bckcett:31540 devlIDSnew u150906 3833-RemitAlternateCharge_Sharon_devlIDSnew                  Maddy
# MAGIC ^1_1 06/29/09 11:44:53 Batch  15156_42345 INIT bckcett:31540 devlIDSnew u150906 3833-RemitAlternateChargeIDS_Sharon_devlIDSnew              Maddy
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
# MAGIC ^1_2 03/07/07 15:24:05 Batch  14311_55447 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_8 12/18/06 15:24:00 Batch  14232_55459 PROMOTE bckcetl ids20 dsadm Keith for Ralph install 12/18/2006
# MAGIC ^1_8 12/18/06 14:39:27 Batch  14232_52797 INIT bckcett testIDS30 dsadm Keith for Ralph CDHP Install 12182006
# MAGIC ^1_1 11/17/06 13:46:03 Batch  14201_49568 PROMOTE bckcett testIDS30 u06640 Ralph
# MAGIC ^1_1 11/17/06 11:12:19 Batch  14201_40344 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_3 09/29/06 07:52:17 Batch  14152_28340 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_2 08/22/06 09:38:57 Batch  14114_34741 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_6 04/17/06 10:35:58 Batch  13987_38166 PROMOTE bckcetl ids20 dsadm J. mahaffey
# MAGIC ^1_6 04/17/06 10:24:16 Batch  13987_37468 INIT bckcett testIDS30 dsadm J. Mahaffey for B. Leland
# MAGIC ^1_2 04/13/06 08:47:39 Batch  13983_31666 PROMOTE bckcett testIDS30 u08717 Brent
# MAGIC ^1_2 04/13/06 07:36:31 Batch  13983_27397 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_4 03/29/06 15:32:39 Batch  13968_55966 INIT bckcett devlIDS30 dsadm Brent
# MAGIC ^1_3 03/29/06 14:54:04 Batch  13968_53650 INIT bckcett devlIDS30 dsadm Brent
# MAGIC ^1_1 03/29/06 05:18:35 Batch  13968_19121 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_2 03/20/06 13:57:04 Batch  13959_50244 INIT bckcett devlIDS30 dsadm Brent
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsClmRemitHistFkey
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the primary key.   Preps the file for the foreign key lookup process that follows.
# MAGIC       
# MAGIC 
# MAGIC INPUTS: Primary Key output file.
# MAGIC 	
# MAGIC   
# MAGIC HASH FILES:     hf_recycle  
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                            
# MAGIC 
# MAGIC PROCESSING:
# MAGIC Product key lookup - 
# MAGIC                 GetFkeyProd( TRIM(  ClsPlnCrfIn.SRC_SYS_CD ),   ClsPlnCrfIn.CLS_PLN_SK,  TRIM (ClsPlnCrfIn.PROD_ID ),   "Y")
# MAGIC 
# MAGIC 
# MAGIC OUTPUTS: Load File for remit history table.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             Steph Goddard   04/2004  -   Originally Programmed
# MAGIC             Brent Leland       09/10/2004  -  Added default rows for UNK and NA
# MAGIC                                                            -  Added link partitioner
# MAGIC             Steph Goddard   09/13/2005    Added calculated payment amount indicator
# MAGIC             Steph Goddard   02/16/2006   sequencer changes
# MAGIC             Brent Leland       08/07/2006    Added current run cycle to output records.  Without the current run cycle, error recycle records would have an old run cycle and would be removed in delete process.
# MAGIC             Ralph Tucker    10/03/2006 Removed the following fields: CLM_REMIT_HIST-pAYMTOVRD_CD_SK
# MAGIC                                                                                                           CHK_PD_DT_SK
# MAGIC                                                                                                           CHK_NET_PAYMT_AMT
# MAGIC                                                                                                           CHK_NO
# MAGIC                                                                                                           CHK_SEQ_NO
# MAGIC                                                                                                           CHK_PAYE_NM
# MAGIC                                                                                                           CHK_PAYMT_REF_ID
# MAGIC                                                                                                           CLM_REMIT_HIST_PAYMTMETH_CD_SK
# MAGIC                                                              Added field:  PCA_PD_AMT          
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                                                Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                                                         --------------------------------       -------------------------------   ----------------------------       
# MAGIC Bhoomi Dasari         2008-08-12      3567(Primary Key)    Added new parameter SrcSysCdSk and passing it directly from source                  devlIDS                          Steph Goddard         08/18/2008
# MAGIC SAndrew                 2009-06-23      #3833 Remit Alt Chrg      Added two new fields to end of file and to shared container                             devlIDSnew                   Steph Goddard           07/01/2009
# MAGIC                                                                                                ALT_CHRG_IN and ALT_CHRG_PROV_WRTOFF_AMT

# MAGIC This hash file is written to by all claim foriegn key jobs.
# MAGIC Set all foreign surragote keys
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
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Parameter retrieval
Source = get_widget_value('Source','FACETS')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
InFile = get_widget_value('InFile','IdsClmRemitHistPKey.RemitHistTmp.RUNID')
Logging = get_widget_value('Logging','Y')

# Schema for ClmRemitHistCrf (CSeqFileStage)
schema_ClmRemitHistCrf = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType()),
    StructField("INSRT_UPDT_CD", StringType()),
    StructField("DISCARD_IN", StringType()),
    StructField("PASS_THRU_IN", StringType()),
    StructField("FIRST_RECYC_DT", TimestampType()),
    StructField("ERR_CT", IntegerType()),
    StructField("RECYCLE_CT", DecimalType(38,10)),
    StructField("SRC_SYS_CD", StringType()),
    StructField("PRI_KEY_STRING", StringType()),
    StructField("CLM_REMIT_HIST_SK", IntegerType()),
    StructField("SRC_SYS_CD_SK", IntegerType()),
    StructField("CLM_ID", StringType()),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType()),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType()),
    StructField("CLM_SK", IntegerType()),
    StructField("CALC_ACTL_PD_AMT_IN", StringType()),
    StructField("SUPRESS_EOB_IN", StringType()),
    StructField("SUPRESS_REMIT_IN", StringType()),
    StructField("ACTL_PD_AMT", DecimalType(38,10)),
    StructField("COB_PD_AMT", DecimalType(38,10)),
    StructField("COINS_AMT", DecimalType(38,10)),
    StructField("CNSD_CHRG_AMT", DecimalType(38,10)),
    StructField("COPAY_AMT", DecimalType(38,10)),
    StructField("DEDCT_AMT", DecimalType(38,10)),
    StructField("DSALW_AMT", DecimalType(38,10)),
    StructField("ER_COPAY_AMT", DecimalType(38,10)),
    StructField("INTRST_AMT", DecimalType(38,10)),
    StructField("NO_RESP_AMT", DecimalType(38,10)),
    StructField("PATN_RESP_AMT", DecimalType(38,10)),
    StructField("WRTOFF_AMT", DecimalType(38,10)),
    StructField("PCA_PAID_AMT", DecimalType(38,10)),
    StructField("ALT_CHRG_IN", StringType()),
    StructField("ALT_CHRG_PROV_WRTOFF_AMT", DecimalType(38,10))
])

# Read from ClmRemitHistCrf (CSeqFileStage)
df_ClmRemitHistCrf = (
    spark.read
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_ClmRemitHistCrf)
    .csv(f"{adls_path}/key/{InFile}")
)

# Enrich with Transformer logic (ForeignKey - CTransformerStage)
# Stage variables
df_enriched = (
    df_ClmRemitHistCrf
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("CLM_REMIT_HIST_SK")))
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("ClmSk", GetFkeyClm(F.col("SRC_SYS_CD"), F.col("CLM_REMIT_HIST_SK"), F.col("CLM_ID"), Logging))
    .withColumn("JobExecRecSK", F.when(F.col("ErrCount") > 0, GetRecycleKey(F.col("CLM_REMIT_HIST_SK"))).otherwise(F.lit(0)))
)

# Output link: ClmRemitHistOut1 (Constraint: ErrCount > 0 Or PassThru = 'Y')
df_ClmRemitHistOut1 = (
    df_enriched
    .filter((F.col("ErrCount") > 0) | (F.col("PassThru") == "Y"))
    .select(
        F.col("CLM_REMIT_HIST_SK").alias("CLM_REMIT_HIST_SK"),
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("ClmSk").alias("CLM_SK"),
        F.col("SUPRESS_EOB_IN").alias("SUPRESS_EOB_IN"),
        F.col("SUPRESS_REMIT_IN").alias("SUPRESS_REMIT_IN"),
        F.col("ACTL_PD_AMT").alias("ACTL_PD_AMT"),
        F.col("COB_PD_AMT").alias("COB_PD_AMT"),
        F.col("COINS_AMT").alias("COINS_AMT"),
        F.col("CNSD_CHRG_AMT").alias("CNSD_CHRG_AMT"),
        F.col("COPAY_AMT").alias("COPAY_AMT"),
        F.col("DEDCT_AMT").alias("DEDCT_AMT"),
        F.col("DSALW_AMT").alias("DSALW_AMT"),
        F.col("ER_COPAY_AMT").alias("ER_COPAY_AMT"),
        F.col("INTRST_AMT").alias("INTRST_AMT"),
        F.col("NO_RESP_AMT").alias("NO_RESP_AMT"),
        F.col("PATN_RESP_AMT").alias("PATN_RESP_AMT"),
        F.col("WRTOFF_AMT").alias("PROV_WRTOFF_AMT"),
        F.col("CALC_ACTL_PD_AMT_IN").alias("CALC_ACTL_PD_AMT_IN"),
        F.col("PCA_PAID_AMT").alias("PCA_PD_AMT"),
        F.col("ALT_CHRG_IN").alias("ALT_CHRG_IN"),
        F.col("ALT_CHRG_PROV_WRTOFF_AMT").alias("ALT_CHRG_PROV_WRTOFF_AMT")
    )
)

# Output link: Recycle (Constraint: ErrCount > 0)
df_Recycle = (
    df_enriched
    .filter(F.col("ErrCount") > 0)
    .select(
        F.col("JobExecRecSK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("CLM_REMIT_HIST_SK").alias("CLM_REMIT_HIST_SK"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("CLM_SK").alias("CLM_SK"),
        F.col("SUPRESS_EOB_IN").alias("SUPRESS_EOB_IN"),
        F.col("SUPRESS_REMIT_IN").alias("SUPRESS_REMIT_IN"),
        F.col("ACTL_PD_AMT").alias("ACTL_PD_AMT"),
        F.col("COB_PD_AMT").alias("COB_PD_AMT"),
        F.col("COINS_AMT").alias("COINS_AMT"),
        F.col("CNSD_CHRG_AMT").alias("CNSD_CHRG_AMT"),
        F.col("COPAY_AMT").alias("COPAY_AMT"),
        F.col("DEDCT_AMT").alias("DEDCT_AMT"),
        F.col("DSALW_AMT").alias("DSALW_AMT"),
        F.col("ER_COPAY_AMT").alias("ER_COPAY_AMT"),
        F.col("INTRST_AMT").alias("INTRST_AMT"),
        F.col("NO_RESP_AMT").alias("NO_RESP_AMT"),
        F.col("PATN_RESP_AMT").alias("PATN_RESP_AMT"),
        F.col("WRTOFF_AMT").alias("WRTOFF_AMT"),
        F.col("CALC_ACTL_PD_AMT_IN").alias("CALC_ACTL_PD_AMT_IN"),
        F.col("PCA_PAID_AMT").alias("PCA_PD_AMT"),
        F.col("ALT_CHRG_IN").alias("ALT_CHRG_IN"),
        F.col("ALT_CHRG_PROV_WRTOFF_AMT").alias("ALT_CHRG_PROV_WRTOFF_AMT")
    )
)

# Output link: Recycle_Clms (Constraint: ErrCount > 0)
df_ClaimRecycleClms = (
    df_enriched
    .filter(F.col("ErrCount") > 0)
    .select(
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("CLM_ID").alias("CLM_ID")
    )
)

# Write df_Recycle to hashed file "hf_recycle" => scenario C => write as parquet
write_files(
    df_Recycle,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# Write df_ClaimRecycleClms to hashed file "hf_claim_recycle_keys" => scenario C => write as parquet
write_files(
    df_ClaimRecycleClms,
    "hf_claim_recycle_keys.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# DefaultUNK (Constraint: @INROWNUM=1, one row with specified literals)
schema_Collector = StructType([
    StructField("CLM_REMIT_HIST_SK", IntegerType()),
    StructField("SRC_SYS_CD_SK", IntegerType()),
    StructField("CLM_ID", StringType()),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType()),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType()),
    StructField("CLM_SK", IntegerType()),
    StructField("SUPRESS_EOB_IN", StringType()),
    StructField("SUPRESS_REMIT_IN", StringType()),
    StructField("ACTL_PD_AMT", DecimalType(38,10)),
    StructField("COB_PD_AMT", DecimalType(38,10)),
    StructField("COINS_AMT", DecimalType(38,10)),
    StructField("CNSD_CHRG_AMT", DecimalType(38,10)),
    StructField("COPAY_AMT", DecimalType(38,10)),
    StructField("DEDCT_AMT", DecimalType(38,10)),
    StructField("DSALW_AMT", DecimalType(38,10)),
    StructField("ER_COPAY_AMT", DecimalType(38,10)),
    StructField("INTRST_AMT", DecimalType(38,10)),
    StructField("NO_RESP_AMT", DecimalType(38,10)),
    StructField("PATN_RESP_AMT", DecimalType(38,10)),
    StructField("PROV_WRTOFF_AMT", DecimalType(38,10)),
    StructField("CALC_ACTL_PD_AMT_IN", StringType()),
    StructField("PCA_PD_AMT", DecimalType(38,10)),
    StructField("ALT_CHRG_IN", StringType()),
    StructField("ALT_CHRG_PROV_WRTOFF_AMT", DecimalType(38,10))
])

row_UNK = (
    0, 0, "UNK", 0, 0, 0, "U", "U", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "U", 0, "U", 0
)
df_DefaultUNK = spark.createDataFrame([row_UNK], schema_Collector)

# DefaultNA (Constraint: @INROWNUM=1, one row with specified literals)
row_NA = (
    1, 1, "NA", 1, 1, 1, "X", "X", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "X", 0, "X", 0
)
df_DefaultNA = spark.createDataFrame([row_NA], schema_Collector)

# Collector (CCollector) => union the three inputs
df_collector = df_ClmRemitHistOut1.unionByName(df_DefaultUNK).unionByName(df_DefaultNA)

# Final select for output order
df_final = df_collector.select(
    "CLM_REMIT_HIST_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_SK",
    "SUPRESS_EOB_IN",
    "SUPRESS_REMIT_IN",
    "ACTL_PD_AMT",
    "COB_PD_AMT",
    "COINS_AMT",
    "CNSD_CHRG_AMT",
    "COPAY_AMT",
    "DEDCT_AMT",
    "DSALW_AMT",
    "ER_COPAY_AMT",
    "INTRST_AMT",
    "NO_RESP_AMT",
    "PATN_RESP_AMT",
    "PROV_WRTOFF_AMT",
    "CALC_ACTL_PD_AMT_IN",
    "PCA_PD_AMT",
    "ALT_CHRG_IN",
    "ALT_CHRG_PROV_WRTOFF_AMT"
)

# Apply rpad for char/varchar columns with known lengths
df_final = (
    df_final
    .withColumn("CLM_ID", rpad(F.col("CLM_ID"), 20, " "))
    .withColumn("SUPRESS_EOB_IN", rpad(F.col("SUPRESS_EOB_IN"), 1, " "))
    .withColumn("SUPRESS_REMIT_IN", rpad(F.col("SUPRESS_REMIT_IN"), 1, " "))
    .withColumn("CALC_ACTL_PD_AMT_IN", rpad(F.col("CALC_ACTL_PD_AMT_IN"), 1, " "))
    .withColumn("ALT_CHRG_IN", rpad(F.col("ALT_CHRG_IN"), 1, " "))
)

# ClmRemitHistFkey (CSeqFileStage) => write final
write_files(
    df_final,
    f"{adls_path}/load/CLM_REMIT_HIST.{Source}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)