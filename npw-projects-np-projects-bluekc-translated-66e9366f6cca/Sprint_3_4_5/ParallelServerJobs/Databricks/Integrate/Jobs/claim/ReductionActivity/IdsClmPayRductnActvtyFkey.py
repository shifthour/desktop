# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:     IdsFctsClmLoad3Seq
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:  assign foreign (surrogate) keys to record
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------    
# MAGIC Steph Goddard        06/2004                                          Originally Programmed
# MAGIC Brent Leland            09/10/2004                                     Fixed default row values for UNK and NA
# MAGIC SAndrew                  08/08/2005                                     Facets 4.2 changes.   Added key field ACPR_SUB_TYP.  
# MAGIC                                                                                         impacts extract, CRF, primary and load.
# MAGIC BJ Luce                   10/18/2005                                    use If ErrCount > 0 Then 
# MAGIC                                                                                        GetRecycleKey( ClmRedActIn.PAYMT_RDUCTN_ACTVTY_SK) 
# MAGIC                                                                                        Else 0 for recycle key
# MAGIC Bhoomi Dasari         2008-07-09      3657(Primary Key)   Added SRC_SYS_CD field to the table                                            devlIDS                      Brent Leland              07-17-2008
# MAGIC Reddy Sanam         2020-10-09      295007                    In the transformer Foreign Keys,Changed derivation to 
# MAGIC                                                                                        these stage variables PaymtReductnSubTypSK,
# MAGIC                                                                                       PayRductnActEvtTypCdSk,                                                         
# MAGIC                                                                                       PayRductnActRsnCdSk  to pass
# MAGIC                                                                                        'FACETS' as the source system code for LUMERIS                      IntegrateDev2
# MAGIC 
# MAGIC Emran.Mohammad               2020-10-12                                              brought up to standards
# MAGIC Reddy Sanam    2021-10-25             US - 449722        Column length-RCVD_CHK_NO changed from char(8) to char(10)    IntegrateDev2              Goutham K                 11/2/2021

# MAGIC Read common record format file.
# MAGIC Writing Sequential File to /load
# MAGIC Assign foreign keys and create default records for unknown and not applicable.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DecimalType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value("Logging", "Y")
Source = get_widget_value("Source", "FACETS")
SrcSysCdSk = get_widget_value("SrcSysCdSk", "105859")
InFile = get_widget_value("InFile", "")

schema_ClmPayRductnActvtyExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38, 10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("PAYMT_RDUCTN_ACTVTY_SK", IntegerType(), False),
    StructField("PAYMT_RDUCTN_REF_ID", StringType(), False),
    StructField("PAYMT_RDUCTN_SUB_TYPE", StringType(), False),
    StructField("PAYMT_RDUCTN_ACTVTY_SEQ_NO", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("PAYMT_RDUCTN_ID", StringType(), False),
    StructField("USER_ID", StringType(), False),
    StructField("PAYMT_RDUCTN_ACT_EVT_TYP_CD", StringType(), False),
    StructField("PAYMT_RDUCTN_ACTVTY_RSN_CD", StringType(), False),
    StructField("ACCTG_PERD_END_DT", StringType(), False),
    StructField("CRT_DT", StringType(), False),
    StructField("RFND_RCVD_DT", StringType(), False),
    StructField("PAYMT_RDUCTN_ACTVTY_AMT", DecimalType(38, 10), False),
    StructField("PCA_RCVR_AMT", DecimalType(38, 10), False),
    StructField("PAYMT_REF_ID", StringType(), False),
    StructField("RCVD_CHK_NO", StringType(), False)
])

df_ClmPayRductnActvtyExtr = (
    spark.read
    .option("sep", ",")
    .option("header", "false")
    .option("quote", "\"")
    .schema(schema_ClmPayRductnActvtyExtr)
    .csv(f"{adls_path}/key/{InFile}")
)

df_Key = df_ClmPayRductnActvtyExtr

df_Transformed = (
    df_Key
    .withColumn("SvSrcSysCd",
        F.when(F.lit(Source) == F.lit("LUMERIS"), F.lit("FACETS")).otherwise(F.lit(Source))
    )
    .withColumn("PaymtReductn",
        GetFkeyPaymtRductn(
            F.col("SRC_SYS_CD"),
            F.col("PAYMT_RDUCTN_ACTVTY_SK"),
            F.col("PAYMT_RDUCTN_ID"),
            F.col("PAYMT_RDUCTN_SUB_TYPE"),
            Logging
        )
    )
    .withColumn("PaymtReductnSubTypSK",
        GetFkeyCodes(
            F.col("SvSrcSysCd"),
            F.col("PAYMT_RDUCTN_ACTVTY_SK"),
            "PAYMENT REDUCTION SUBTYPE",
            F.col("PAYMT_RDUCTN_SUB_TYPE"),
            Logging
        )
    )
    .withColumn("UserSK",
        GetFkeyAppUsr(
            F.col("SRC_SYS_CD"),
            F.col("PAYMT_RDUCTN_ACTVTY_SK"),
            F.col("USER_ID"),
            Logging
        )
    )
    .withColumn("PayRductnActEvtTypCdSk",
        GetFkeyCodes(
            F.col("SvSrcSysCd"),
            F.col("PAYMT_RDUCTN_ACTVTY_SK"),
            "PAYMENT REDUCTION ACTIVITY EVENT TYPE",
            F.col("PAYMT_RDUCTN_ACT_EVT_TYP_CD"),
            Logging
        )
    )
    .withColumn("PayRductnActRsnCdSk",
        GetFkeyCodes(
            F.col("SvSrcSysCd"),
            F.col("PAYMT_RDUCTN_ACTVTY_SK"),
            "PAYMENT REDUCTION ACTIVITY REASON",
            F.col("PAYMT_RDUCTN_ACTVTY_RSN_CD"),
            Logging
        )
    )
    .withColumn("AcctgPerdEndDtSk",
        GetFkeyDate("IDS", F.col("PAYMT_RDUCTN_ACTVTY_SK"), F.col("ACCTG_PERD_END_DT"), Logging)
    )
    .withColumn("CrtDtSk",
        GetFkeyDate("IDS", F.col("PAYMT_RDUCTN_ACTVTY_SK"), F.col("CRT_DT"), Logging)
    )
    .withColumn("RfndRcvdDtSk",
        GetFkeyDate("IDS", F.col("PAYMT_RDUCTN_ACTVTY_SK"), F.col("RFND_RCVD_DT"), Logging)
    )
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("PAYMT_RDUCTN_ACTVTY_SK")))
)

df_Fkey = df_Transformed.filter((F.col("ErrCount") == 0) | (F.col("PassThru") == "Y"))
df_recycle = df_Transformed.filter(F.col("ErrCount") > 0)
df_firstRow = df_Transformed.limit(1)
df_DefaultUNK = df_firstRow
df_DefaultNA = df_firstRow

df_Fkey_final = df_Fkey.select(
    F.col("PAYMT_RDUCTN_ACTVTY_SK").alias("PAYMT_RDUCTN_ACTVTY_SK"),
    F.lit(SrcSysCdSk).cast(IntegerType()).alias("SRC_SYS_CD_SK"),
    F.col("PAYMT_RDUCTN_REF_ID").alias("PAYMT_RDUCTN_REF_ID"),
    F.col("PaymtReductnSubTypSK").alias("PAYMT_RDUCTN_SUB_TYPE_SK"),
    F.col("PAYMT_RDUCTN_ACTVTY_SEQ_NO").alias("PAYMT_RDUCTN_ACTVTY_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PaymtReductn").alias("PAYMT_RDUCTN_SK"),
    F.col("UserSK").alias("USER_SK"),
    F.col("PayRductnActEvtTypCdSk").alias("PAYMT_RDUCTN_ACT_EVT_TYP_CD_SK"),
    F.col("PayRductnActRsnCdSk").alias("PAYMT_RDUCTN_ACTVTY_RSN_CD_SK"),
    F.col("AcctgPerdEndDtSk").alias("ACCTG_PERD_END_DT_SK"),
    F.col("CrtDtSk").alias("CRT_DT_SK"),
    F.col("RfndRcvdDtSk").alias("RFND_RCVD_DT_SK"),
    F.col("PAYMT_RDUCTN_ACTVTY_AMT").alias("PAYMT_RDUCTN_ACTVTY_AMT"),
    F.col("PCA_RCVR_AMT").alias("PCA_RCVR_AMT"),
    F.col("PAYMT_REF_ID").alias("PAYMT_REF_ID"),
    F.col("RCVD_CHK_NO").alias("RCVD_CHK_NO")
).withColumnRenamed("PAYMT_RDUCTN_SUB_TYPE_SK", "PAYMT_RDUCTN_SUBTYP_CD_SK")

df_recycle_final = df_recycle.select(
    GetRecycleKey(F.col("PAYMT_RDUCTN_ACTVTY_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ErrCount").alias("ERR_CT"),
    (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("PAYMT_RDUCTN_ACTVTY_SK").alias("PAYMT_RDUCTN_ACTVTY_SK"),
    F.col("PAYMT_RDUCTN_REF_ID").alias("PAYMT_RDUCTN_REF_ID"),
    F.col("PAYMT_RDUCTN_SUB_TYPE").alias("PAYMT_RDUCTN_SUB_TYPE"),
    F.col("PAYMT_RDUCTN_ACTVTY_SEQ_NO").alias("PAYMT_RDUCTN_ACTVTY_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PAYMT_RDUCTN_ID").alias("PAYMT_RDUCTN_ID"),
    F.col("USER_ID").alias("USER_ID"),
    F.col("PAYMT_RDUCTN_ACT_EVT_TYP_CD").alias("PAYMT_RDUCTN_ACT_EVT_TYP_CD"),
    F.col("PAYMT_RDUCTN_ACTVTY_RSN_CD").alias("PAYMT_RDUCTN_ACTVTY_RSN_CD"),
    F.col("ACCTG_PERD_END_DT").alias("ACCTG_PERD_END_DT"),
    F.col("CRT_DT").alias("CRT_DT"),
    F.col("RFND_RCVD_DT").alias("RFND_RCVD_DT"),
    F.col("PAYMT_RDUCTN_ACTVTY_AMT").alias("PAYMT_RDUCTN_ACTVTY_AMT"),
    F.col("PCA_RCVR_AMT").alias("PCA_RCVR_AMT"),
    F.col("PAYMT_REF_ID").alias("PAYMT_REF_ID"),
    F.col("RCVD_CHK_NO").alias("RCVD_CHK_NO")
)

write_files(
    df_recycle_final,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_DefaultUNK_final = df_DefaultUNK.select(
    F.lit(0).alias("PAYMT_RDUCTN_ACTVTY_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit("UNK").alias("PAYMT_RDUCTN_REF_ID"),
    F.lit(0).alias("PAYMT_RDUCTN_SUB_TYPE_SK"),
    F.lit(0).alias("PAYMT_RDUCTN_ACTVTY_SEQ_NO"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("PAYMT_RDUCTN_SK"),
    F.lit(0).alias("USER_SK"),
    F.lit(0).alias("PAYMT_RDUCTN_ACT_EVT_TYP_CD_SK"),
    F.lit(0).alias("PAYMT_RDUCTN_ACTVTY_RSN_CD_SK"),
    F.lit("UNK").alias("ACCTG_PERD_END_DT_SK"),
    F.lit("UNK").alias("CRT_DT_SK"),
    F.lit("UNK").alias("RFND_RCVD_DT_SK"),
    F.lit(0).alias("PAYMT_RDUCTN_ACTVTY_AMT"),
    F.lit(0).alias("PCA_RCVR_AMT"),
    F.lit("UNK").alias("PAYMT_REF_ID"),
    F.lit("UNK").alias("RCVD_CHK_NO")
).withColumnRenamed("PAYMT_RDUCTN_SUB_TYPE_SK", "PAYMT_RDUCTN_SUBTYP_CD_SK")

df_DefaultNA_final = df_DefaultNA.select(
    F.lit(1).alias("PAYMT_RDUCTN_ACTVTY_SK"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit("NA").alias("PAYMT_RDUCTN_REF_ID"),
    F.lit(1).alias("PAYMT_RDUCTN_SUB_TYPE_SK"),
    F.lit(0).alias("PAYMT_RDUCTN_ACTVTY_SEQ_NO"),
    F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("PAYMT_RDUCTN_SK"),
    F.lit(1).alias("USER_SK"),
    F.lit(1).alias("PAYMT_RDUCTN_ACT_EVT_TYP_CD_SK"),
    F.lit(1).alias("PAYMT_RDUCTN_ACTVTY_RSN_CD_SK"),
    F.lit("NA").alias("ACCTG_PERD_END_DT_SK"),
    F.lit("NA").alias("CRT_DT_SK"),
    F.lit("NA").alias("RFND_RCVD_DT_SK"),
    F.lit(0).alias("PAYMT_RDUCTN_ACTVTY_AMT"),
    F.lit(0).alias("PCA_RCVR_AMT"),
    F.lit("NA").alias("PAYMT_REF_ID"),
    F.lit("NA").alias("RCVD_CHK_NO")
).withColumnRenamed("PAYMT_RDUCTN_SUB_TYPE_SK", "PAYMT_RDUCTN_SUBTYP_CD_SK")

df_Collector = (
    df_DefaultUNK_final
    .unionByName(df_DefaultNA_final)
    .unionByName(df_Fkey_final)
)

df_Collector_final = (
    df_Collector
    .withColumn("PAYMT_RDUCTN_REF_ID", rpad(F.col("PAYMT_RDUCTN_REF_ID"), <...>, " "))
    .withColumn("PAYMT_REF_ID", rpad(F.col("PAYMT_REF_ID"), <...>, " "))
    .withColumn("ACCTG_PERD_END_DT_SK", rpad(F.col("ACCTG_PERD_END_DT_SK"), 10, " "))
    .withColumn("CRT_DT_SK", rpad(F.col("CRT_DT_SK"), 10, " "))
    .withColumn("RFND_RCVD_DT_SK", rpad(F.col("RFND_RCVD_DT_SK"), 10, " "))
    .withColumn("RCVD_CHK_NO", rpad(F.col("RCVD_CHK_NO"), 10, " "))
)

write_files(
    df_Collector_final.select(
        "PAYMT_RDUCTN_ACTVTY_SK",
        "SRC_SYS_CD_SK",
        "PAYMT_RDUCTN_REF_ID",
        "PAYMT_RDUCTN_SUBTYP_CD_SK",
        "PAYMT_RDUCTN_ACTVTY_SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "PAYMT_RDUCTN_SK",
        "USER_SK",
        "PAYMT_RDUCTN_ACT_EVT_TYP_CD_SK",
        "PAYMT_RDUCTN_ACTVTY_RSN_CD_SK",
        "ACCTG_PERD_END_DT_SK",
        "CRT_DT_SK",
        "RFND_RCVD_DT_SK",
        "PAYMT_RDUCTN_ACTVTY_AMT",
        "PCA_RCVR_AMT",
        "PAYMT_REF_ID",
        "RCVD_CHK_NO"
    ),
    f"{adls_path}/load/PAYMT_RDUCTN_ACTVTY.{Source}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)