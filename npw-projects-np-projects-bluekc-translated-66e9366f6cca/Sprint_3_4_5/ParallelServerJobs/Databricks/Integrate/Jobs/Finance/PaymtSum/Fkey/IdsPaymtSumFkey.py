# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_3 07/24/08 10:56:42 Batch  14816_39411 PROMOTE bckcetl ids20 dsadm rc for brent 
# MAGIC ^1_3 07/24/08 10:41:03 Batch  14816_38477 INIT bckcett testIDS dsadm rc fro brent 
# MAGIC ^1_4 07/22/08 08:32:54 Batch  14814_30778 PROMOTE bckcett testIDS u08717 Brent
# MAGIC ^1_4 07/22/08 08:21:54 Batch  14814_30118 INIT bckcett devlIDS u08717 Brent
# MAGIC ^1_3 07/21/08 12:30:50 Batch  14813_45058 INIT bckcett devlIDS u08717 brent
# MAGIC ^1_2 07/07/08 11:47:11 Batch  14799_42473 INIT bckcett devlIDS u11141 has
# MAGIC ^1_1 06/23/08 16:01:25 Batch  14785_57704 PROMOTE bckcett devlIDS u10913 O. Nielsen move from devlIDScur to devlIDS for B. Leland
# MAGIC ^1_1 06/23/08 15:24:04 Batch  14785_55472 INIT bckcett devlIDScur u10913 O. Nielsen move from devlIDSCUR to devlIDS for B. Leland
# MAGIC ^1_1 01/28/08 10:06:06 Batch  14638_36371 PROMOTE bckcett devlIDScur dsadm dsadm
# MAGIC ^1_1 01/28/08 09:39:22 Batch  14638_34767 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/27/07 09:46:56 Batch  14606_35219 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/24/07 09:35:50 Batch  14573_34558 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 08/27/07 08:55:46 Batch  14484_32151 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_3 03/27/07 13:27:29 Batch  14331_48452 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 01/10/07 13:53:12 Batch  14255_50002 PROMOTE bckcetl ids20 dsadm Keith for Ralph
# MAGIC ^1_2 01/10/07 13:47:36 Batch  14255_49665 INIT bckcett testIDS30 dsadm Keith for Ralph
# MAGIC ^1_2 01/04/07 09:49:52 Batch  14249_35398 PROMOTE bckcett testIDS30 u06640 Ralph
# MAGIC ^1_2 01/04/07 09:47:31 Batch  14249_35260 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_1 12/22/06 14:39:13 Batch  14236_52758 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_1 04/22/05 13:55:19 Batch  13627_50124 PROMOTE bckcetl VERSIONIDS dsadm Gina Parr
# MAGIC ^1_1 04/22/05 13:52:26 Batch  13627_49950 INIT bckcett testIDS30 dsadm Gina Parr
# MAGIC 
# MAGIC Â© Copyright 2008 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  <Sequencer Name>
# MAGIC                
# MAGIC 
# MAGIC PROCESSING:   Takes the file  from primary key data and does foreign key lookups.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Ralph Tucker           03/23/2005                                   Originally Programmed.
# MAGIC Ralph Tucker           12/03/2007                                   Moved LOB_SK field into natural key position under REF_ID
# MAGIC Brent Leland             02/15/2008     3567 Primary Key  Added error recycle hash file                                                        devlIDScur                      Steph Goddard          02/22/08
# MAGIC Brent Leland             07-21-2008      3567 Primary Key  Added SrcSysCdSK to input parameters to assign to records       devlIDS                           Steph Goddard          07/21/2008
# MAGIC                                                                                       instead of using GetFkeyCodes() routine.
# MAGIC                                                                                       Changed default values for UNK row columns 
# MAGIC                                                                                       PERD_END_DT_SK and PD_DT_SK to UNK.

# MAGIC Read common record format file.
# MAGIC Writing Sequential File to /load
# MAGIC Assign all foreign keys and create default rows for UNK and NA.
# MAGIC This hash file is written to by all payment summary foriegn key jobs.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType,
    DecimalType
)
from pyspark.sql import Window
import pyspark.sql.functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value("Logging","N")
SrcSysCdSK = get_widget_value("SrcSysCdSK","")

schema = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("CKPY_PAYEE_PR_ID", StringType(), False),
    StructField("PAYMT_SUM_SK", IntegerType(), False),
    StructField("PAYMT_SUM_ID", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LOBD_ID", StringType(), False),
    StructField("CKPY_PAYEE_TYPE", StringType(), False),
    StructField("CKPY_PYMT_TYPE", StringType(), False),
    StructField("CKPY_TYPE", StringType(), False),
    StructField("CKPY_COMB_IND", StringType(), False),
    StructField("CKPY_PAY_DT", StringType(), False),
    StructField("CKPY_PER_END_DT", StringType(), False),
    StructField("CKPY_DEDUCT_AMT", DecimalType(38,10), False),
    StructField("CKPY_NET_AMT", DecimalType(38,10), False),
    StructField("CKPY_ORIG_AMT", DecimalType(38,10), False),
    StructField("CKPY_CURR_CKCK_SEQ", IntegerType(), False)
])

df_IdsPaymtSumExtr = (
    spark.read.csv(
        path=f"{adls_path}/key/IdsPaymtSumExtr.PaymtSum.uniq",
        schema=schema,
        sep=",",
        quote="\"",
        header=False
    )
)

windowSpec = Window.orderBy(F.lit(1))
df_IdsPaymtSumExtr_singlePart = df_IdsPaymtSumExtr.withColumn("rownum", F.row_number().over(windowSpec))

df_PurgeTrn_base = (
    df_IdsPaymtSumExtr_singlePart
    .withColumn("svProv", GetFkeyProv(F.col("SRC_SYS_CD"), F.col("PAYMT_SUM_SK"), F.col("CKPY_PAYEE_PR_ID"), Logging))
    .withColumn("svLobCd", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("PAYMT_SUM_SK"), F.lit("CLAIM LINE LOB"), F.col("LOBD_ID"), Logging))
    .withColumn("svPayeTypCd", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("PAYMT_SUM_SK"), F.lit("PAYMENT SUMMARY PAYEE TYPE"), F.col("CKPY_PAYEE_TYPE"), Logging))
    .withColumn("svPaymtTypCd", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("PAYMT_SUM_SK"), F.lit("PAYMENT SUMMARY PAYMENT TYPE"), F.col("CKPY_PYMT_TYPE"), Logging))
    .withColumn("svPaymtSumTypCd", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("PAYMT_SUM_SK"), F.lit("PAYMENT SUMMARY TYPE"), F.col("CKPY_TYPE"), Logging))
    .withColumn("svPaidDt", GetFkeyDate(F.lit("IDS"), F.col("PAYMT_SUM_SK"), F.col("CKPY_PAY_DT"), Logging))
    .withColumn("svPerdEndDt", GetFkeyDate(F.lit("IDS"), F.col("PAYMT_SUM_SK"), F.col("CKPY_PER_END_DT"), Logging))
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("PAYMT_SUM_SK")))
)

df_PurgeTrn_PaymtSumkeyOut = df_PurgeTrn_base.filter(
    (F.col("ErrCount") == 0) | (F.col("PassThru") == 'Y')
).select(
    F.col("PAYMT_SUM_SK").alias("PAYMT_SUM_SK"),
    F.lit(SrcSysCdSK).alias("SRC_SYS_CD_SK"),
    F.col("PAYMT_SUM_ID").alias("PAYMT_REF_ID"),
    F.col("svLobCd").alias("PAYMT_SUM_LOB_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("svProv").alias("PD_PROV_SK"),
    F.col("svPayeTypCd").alias("PAYMT_SUM_PAYE_TYP_CD_SK"),
    F.col("svPaymtTypCd").alias("PAYMT_SUM_PAYMT_TYP_CD_SK"),
    F.col("svPaymtSumTypCd").alias("PAYMT_SUM_TYP_CD_SK"),
    F.col("CKPY_COMB_IND").alias("COMBND_CLM_PAYMT_IN"),
    F.col("svPaidDt").alias("PD_DT_SK"),
    F.col("svPerdEndDt").alias("PERD_END_DT_SK"),
    F.col("CKPY_DEDUCT_AMT").alias("DEDCT_AMT"),
    F.col("CKPY_NET_AMT").alias("NET_AMT"),
    F.col("CKPY_ORIG_AMT").alias("ORIG_SUM_AMT"),
    F.col("CKPY_CURR_CKCK_SEQ").alias("CUR_CHK_SEQ_NO")
)

df_PurgeTrn_lnkRecycle = df_PurgeTrn_base.filter(F.col("ErrCount") > 0).select(
    GetRecycleKey(F.col("PAYMT_SUM_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ErrCount").alias("ERR_CT"),
    (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("CKPY_PAYEE_PR_ID").alias("CKPY_PAYEE_PR_ID"),
    F.col("PAYMT_SUM_SK").alias("PAYMT_SUM_SK"),
    F.col("PAYMT_SUM_ID").alias("PAYMT_REF_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("LOBD_ID").alias("LOBD_ID"),
    F.col("CKPY_PAYEE_TYPE").alias("CKPY_PAYEE_TYPE"),
    F.col("CKPY_PYMT_TYPE").alias("CKPY_PYMT_TYPE"),
    F.col("CKPY_TYPE").alias("CKPY_TYPE"),
    F.col("CKPY_COMB_IND").alias("CKPY_COMB_IND"),
    F.col("CKPY_PAY_DT").alias("CKPY_PAY_DT"),
    F.col("CKPY_PER_END_DT").alias("CKPY_PER_END_DT"),
    F.col("CKPY_DEDUCT_AMT").alias("CKPY_DEDUCT_AMT"),
    F.col("CKPY_NET_AMT").alias("CKPY_NET_AMT"),
    F.col("CKPY_ORIG_AMT").alias("CKPY_ORIG_AMT"),
    F.col("CKPY_CURR_CKCK_SEQ").alias("CKPY_CURR_CKCK_SEQ")
)

df_PurgeTrn_Recycle_Keys = df_PurgeTrn_base.filter(F.col("ErrCount") > 0).select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PAYMT_SUM_ID").alias("PAYMT_REF_ID")
)

df_PurgeTrn_DefaultUNK = df_PurgeTrn_base.filter(F.col("rownum") == 1).select(
    F.lit(0).alias("PAYMT_SUM_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit("UNK").alias("PAYMT_REF_ID"),
    F.lit(0).alias("PAYMT_SUM_LOB_CD_SK"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("PD_PROV_SK"),
    F.lit(0).alias("PAYMT_SUM_PAYE_TYP_CD_SK"),
    F.lit(0).alias("PAYMT_SUM_PAYMT_TYP_CD_SK"),
    F.lit(0).alias("PAYMT_SUM_TYP_CD_SK"),
    F.lit("U").alias("COMBND_CLM_PAYMT_IN"),
    F.lit("UNK").alias("PD_DT_SK"),
    F.lit("UNK").alias("PERD_END_DT_SK"),
    F.lit(0).alias("DEDCT_AMT"),
    F.lit(0).alias("NET_AMT"),
    F.lit(0).alias("ORIG_SUM_AMT"),
    F.lit(0).alias("CUR_CHK_SEQ_NO")
)

df_PurgeTrn_DefaultNA = df_PurgeTrn_base.filter(F.col("rownum") == 1).select(
    F.lit(1).alias("PAYMT_SUM_SK"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit("NA").alias("PAYMT_REF_ID"),
    F.lit(1).alias("PAYMT_SUM_LOB_CD_SK"),
    F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("PD_PROV_SK"),
    F.lit(1).alias("PAYMT_SUM_PAYE_TYP_CD_SK"),
    F.lit(1).alias("PAYMT_SUM_PAYMT_TYP_CD_SK"),
    F.lit(1).alias("PAYMT_SUM_TYP_CD_SK"),
    F.lit("X").alias("COMBND_CLM_PAYMT_IN"),
    F.lit("NA").alias("PD_DT_SK"),
    F.lit("NA").alias("PERD_END_DT_SK"),
    F.lit(0).alias("DEDCT_AMT"),
    F.lit(0).alias("NET_AMT"),
    F.lit(0).alias("ORIG_SUM_AMT"),
    F.lit(0).alias("CUR_CHK_SEQ_NO")
)

write_files(
    df_PurgeTrn_lnkRecycle,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

write_files(
    df_PurgeTrn_Recycle_Keys,
    "hf_paymtsum_recycle_keys.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_Collector_1 = df_PurgeTrn_PaymtSumkeyOut.select(
    "PAYMT_SUM_SK",
    "SRC_SYS_CD_SK",
    "PAYMT_REF_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PD_PROV_SK",
    "PAYMT_SUM_LOB_CD_SK",
    "PAYMT_SUM_PAYE_TYP_CD_SK",
    "PAYMT_SUM_PAYMT_TYP_CD_SK",
    "PAYMT_SUM_TYP_CD_SK",
    "COMBND_CLM_PAYMT_IN",
    "PD_DT_SK",
    "PERD_END_DT_SK",
    "DEDCT_AMT",
    "NET_AMT",
    "ORIG_SUM_AMT",
    "CUR_CHK_SEQ_NO"
)

df_Collector_2 = df_PurgeTrn_DefaultUNK.select(
    "PAYMT_SUM_SK",
    "SRC_SYS_CD_SK",
    "PAYMT_REF_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PD_PROV_SK",
    "PAYMT_SUM_LOB_CD_SK",
    "PAYMT_SUM_PAYE_TYP_CD_SK",
    "PAYMT_SUM_PAYMT_TYP_CD_SK",
    "PAYMT_SUM_TYP_CD_SK",
    "COMBND_CLM_PAYMT_IN",
    "PD_DT_SK",
    "PERD_END_DT_SK",
    "DEDCT_AMT",
    "NET_AMT",
    "ORIG_SUM_AMT",
    "CUR_CHK_SEQ_NO"
)

df_Collector_3 = df_PurgeTrn_DefaultNA.select(
    "PAYMT_SUM_SK",
    "SRC_SYS_CD_SK",
    "PAYMT_REF_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PD_PROV_SK",
    "PAYMT_SUM_LOB_CD_SK",
    "PAYMT_SUM_PAYE_TYP_CD_SK",
    "PAYMT_SUM_PAYMT_TYP_CD_SK",
    "PAYMT_SUM_TYP_CD_SK",
    "COMBND_CLM_PAYMT_IN",
    "PD_DT_SK",
    "PERD_END_DT_SK",
    "DEDCT_AMT",
    "NET_AMT",
    "ORIG_SUM_AMT",
    "CUR_CHK_SEQ_NO"
)

df_Collector = df_Collector_1.unionByName(df_Collector_2).unionByName(df_Collector_3)

df_Collector_final = (
    df_Collector
    .withColumn("COMBND_CLM_PAYMT_IN", rpad(F.col("COMBND_CLM_PAYMT_IN"), 1, " "))
    .withColumn("PD_DT_SK", rpad(F.col("PD_DT_SK"), 10, " "))
    .withColumn("PERD_END_DT_SK", rpad(F.col("PERD_END_DT_SK"), 10, " "))
)

write_files(
    df_Collector_final,
    f"{adls_path}/load/PAYMT_SUM.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)