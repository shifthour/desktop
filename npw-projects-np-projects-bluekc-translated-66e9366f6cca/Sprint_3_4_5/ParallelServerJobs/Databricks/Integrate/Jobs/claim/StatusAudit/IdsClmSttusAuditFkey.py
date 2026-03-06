# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsClmSttusAuditFkey
# MAGIC 
# MAGIC MODIFICATIONS:                                                                                                                                                           Development                                        Date 
# MAGIC Developer             Date                  Change Description                                                                         Project #           Project                     Code Reviewer    Reviewed  
# MAGIC ------------------------     ----------------------    -----------------------------------------------------------------------------------------------------   ----------------         -------------------------       --------------------------   --------------------
# MAGIC Steph Goddard     04/2004            Originally Programmed
# MAGIC Brent Leland         09/07/2004      Added Link Partitioner
# MAGIC                                                        Added default rows for UNK and NA
# MAGIC Brent Leland         10/09/2004      Added status timestamp field
# MAGIC Sharon Andrew     12/14/2004      Issue 1990 - when nasco, source the change reason code sk from the explanation table.
# MAGIC Steph Goddard     02/16/2006      Changes for sequencer
# MAGIC Bhoomi D              03/21/2008      Added one new field and renamed two fields                                  3255                 IDSCurDevl             Steph Goddard     03/31/2008
# MAGIC Hugh Sisson         07/15/2008      Corrected TRNSMSN_SRC_CD column name                               3567                 devlIDS                    Steph Goddard    08/12/2008
# MAGIC 
# MAGIC Emran.Mohammad               2020-10-12                                               brought up to standards

# MAGIC Read common record format file.
# MAGIC Writing Sequential File to /load
# MAGIC This hash file is written to by all claim foriegn key jobs.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DecimalType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value('InFile','FctsClmSttusAuditExtr.FctsClmSttusAudit.dat.20080325')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','105838')
Logging = get_widget_value('Logging','Y')

schema_ClmStatusCrf = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("CLM_STTUS_AUDIT_SK", IntegerType(), nullable=False),
    StructField("CLAIM_ID", StringType(), nullable=False),
    StructField("CLAIM_STATUS_AUDIT_SEQ_NO", IntegerType(), nullable=False),
    StructField("CRT_RUN_CYC_EXTCN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXTCN_SK", IntegerType(), nullable=False),
    StructField("CLAIM_SK", IntegerType(), nullable=False),
    StructField("CREATE_BY_USER_ID", StringType(), nullable=False),
    StructField("ROUTE_TO_USER_ID", StringType(), nullable=False),
    StructField("CLAIM_STATUS_CD", StringType(), nullable=False),
    StructField("CLAIM_STTUS_CHG_REASN", StringType(), nullable=False),
    StructField("CLST_STS_DT", StringType(), nullable=False),
    StructField("CLST_STS_DTM", TimestampType(), nullable=False),
    StructField("TRNSMSN_SRC_CD", StringType(), nullable=False)
])

df_ClmStatusCrf = (
    spark.read
    .option("header", "false")
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_ClmStatusCrf)
    .csv(f"{adls_path}/key/{InFile}")
)

df_ForeignKey = (
    df_ClmStatusCrf
    .withColumn(
        "ClmSK",
        GetFkeyClm(
            F.col("SRC_SYS_CD"),
            F.col("CLM_STTUS_AUDIT_SK"),
            F.col("CLAIM_ID"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "CreateByUserIdSK",
        GetFkeyAppUsr(
            F.col("SRC_SYS_CD"),
            F.col("CLM_STTUS_AUDIT_SK"),
            F.col("CREATE_BY_USER_ID"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "RouteToUserIdSK",
        GetFkeyAppUsr(
            F.col("SRC_SYS_CD"),
            F.col("CLM_STTUS_AUDIT_SK"),
            F.col("ROUTE_TO_USER_ID"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "ClmStatusCdSK",
        GetFkeyCodes(
            F.when(F.col("SRC_SYS_CD")=="LUMERIS", F.lit("FACETS")).otherwise(F.col("SRC_SYS_CD")),
            F.col("CLM_STTUS_AUDIT_SK"),
            F.lit("CLAIM STATUS"),
            F.col("CLAIM_STATUS_CD"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "ClmStatusChangeReasonSK",
        F.when(
            F.col("SRC_SYS_CD")=="LUMERIS",
            F.when(
                GetFkeyCodes(
                    F.lit("LUMERIS"),
                    F.col("CLM_STTUS_AUDIT_SK"),
                    F.lit("CLAIM STATUS REASON"),
                    F.col("CLAIM_STTUS_CHG_REASN"),
                    F.lit(Logging)
                )==F.lit(0),
                GetFkeyCodes(
                    F.lit("FACETS"),
                    F.col("CLM_STTUS_AUDIT_SK"),
                    F.lit("CLAIM STATUS REASON"),
                    F.col("CLAIM_STTUS_CHG_REASN"),
                    F.lit(Logging)
                )
            ).otherwise(
                GetFkeyCodes(
                    F.lit("LUMERIS"),
                    F.col("CLM_STTUS_AUDIT_SK"),
                    F.lit("CLAIM STATUS REASON"),
                    F.col("CLAIM_STTUS_CHG_REASN"),
                    F.lit(Logging)
                )
            )
        ).when(
            F.col("SRC_SYS_CD")=="NPS",
            GetFkeyExcd(
                F.col("SRC_SYS_CD"),
                F.col("CLM_STTUS_AUDIT_SK"),
                F.col("CLAIM_STTUS_CHG_REASN"),
                F.lit(Logging)
            )
        ).otherwise(
            GetFkeyCodes(
                F.col("SRC_SYS_CD"),
                F.col("CLM_STTUS_AUDIT_SK"),
                F.lit("CLAIM STATUS REASON"),
                F.col("CLAIM_STTUS_CHG_REASN"),
                F.lit(Logging)
            )
        )
    )
    .withColumn(
        "ClmStatusDateSK",
        GetFkeyDate(
            F.lit("IDS"),
            F.col("CLM_STTUS_AUDIT_SK"),
            F.substring(F.col("CLST_STS_DT"),1,10),
            F.lit(Logging)
        )
    )
    .withColumn(
        "svTransSttusCdSk",
        GetFkeyCodes(
            F.when(F.col("SRC_SYS_CD")=="LUMERIS", F.lit("FACETS")).otherwise(F.col("SRC_SYS_CD")),
            F.col("CLM_STTUS_AUDIT_SK"),
            F.lit("ITS CLAIM STATUS"),
            F.col("TRNSMSN_SRC_CD"),
            F.lit(Logging)
        )
    )
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("CLM_STTUS_AUDIT_SK")))
)

df_Output1 = (
    df_ForeignKey
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == "Y"))
    .select(
        F.col("CLM_STTUS_AUDIT_SK").alias("CLM_STTUS_AUDIT_SK"),
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.col("CLAIM_ID").alias("CLM_ID"),
        F.col("CLAIM_STATUS_AUDIT_SEQ_NO").alias("CLM_STTUS_AUDIT_SEQ_NO"),
        F.col("CRT_RUN_CYC_EXTCN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXTCN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("ClmSK").alias("CLM_SK"),
        F.col("CreateByUserIdSK").alias("CRT_BY_APP_USER_SK"),
        F.col("RouteToUserIdSK").alias("RTE_TO_APP_USER_SK"),
        F.col("ClmStatusCdSK").alias("CLM_STTUS_CD_SK"),
        F.col("ClmStatusChangeReasonSK").alias("CLM_STTUS_CHG_RSN_CD_SK"),
        F.col("svTransSttusCdSk").alias("TRNSMSN_SRC_CD_SK"),
        F.col("ClmStatusDateSK").alias("CLM_STTUS_DT_SK"),
        F.col("CLST_STS_DTM").alias("CLM_STTUS_DTM")
    )
)

df_Recycle1 = (
    df_ForeignKey
    .filter(F.col("ErrCount") > 0)
    .select(
        F.expr("GetRecycleKey(CLM_STTUS_AUDIT_SK)").alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("CLM_STTUS_AUDIT_SK").alias("CLM_STTUS_AUDIT_SK"),
        F.col("CLAIM_ID").alias("CLAIM_ID"),
        F.col("CLAIM_STATUS_AUDIT_SEQ_NO").alias("CLAIM_STATUS_AUDIT_SEQ_NO"),
        F.col("CRT_RUN_CYC_EXTCN_SK").alias("CRT_RUN_CYC_EXTCN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXTCN_SK").alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
        F.col("CLAIM_SK").alias("CLAIM_SK"),
        F.col("CREATE_BY_USER_ID").alias("CREATE_BY_USER_ID"),
        F.col("ROUTE_TO_USER_ID").alias("ROUTE_TO_USER_ID"),
        F.col("CLAIM_STATUS_CD").alias("CLAIM_STATUS_CD"),
        F.col("CLAIM_STTUS_CHG_REASN").alias("CLAIM_STTUS_CHG_REASN"),
        F.col("CLST_STS_DT").alias("CLST_STS_DT"),
        F.col("CLST_STS_DTM").alias("CLST_STS_DTM"),
        F.col("TRNSMSN_SRC_CD").alias("TRNSMSN_SRC_CD")
    )
)

df_Recycle_Clms = (
    df_ForeignKey
    .filter(F.col("ErrCount") > 0)
    .select(
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("CLAIM_ID").alias("CLM_ID")
    )
)

df_DefaultUNK = (
    df_ForeignKey.orderBy(F.lit(1)).limit(1)
    .select(
        F.lit(0).alias("CLM_STTUS_AUDIT_SK"),
        F.lit(0).alias("SRC_SYS_CD_SK"),
        F.lit("UNK").alias("CLM_ID"),
        F.lit(0).alias("CLM_STTUS_AUDIT_SEQ_NO"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("CLM_SK"),
        F.lit(0).alias("CRT_BY_APP_USER_SK"),
        F.lit(0).alias("RTE_TO_APP_USER_SK"),
        F.lit(0).alias("CLM_STTUS_CD_SK"),
        F.lit(0).alias("CLM_STTUS_CHG_RSN_CD_SK"),
        F.lit(0).alias("TRNSMSN_SRC_CD_SK"),
        F.lit("UNK").alias("CLM_STTUS_DT_SK"),
        F.lit("1753-01-01-00.00.00.000000").alias("CLM_STTUS_DTM")
    )
)

df_DefaulNA = (
    df_ForeignKey.orderBy(F.lit(1)).limit(1)
    .select(
        F.lit(1).alias("CLM_STTUS_AUDIT_SK"),
        F.lit(1).alias("SRC_SYS_CD_SK"),
        F.lit("NA").alias("CLM_ID"),
        F.lit(0).alias("CLM_STTUS_AUDIT_SEQ_NO"),
        F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("CLM_SK"),
        F.lit(1).alias("CRT_BY_APP_USER_SK"),
        F.lit(1).alias("RTE_TO_APP_USER_SK"),
        F.lit(1).alias("CLM_STTUS_CD_SK"),
        F.lit(1).alias("CLM_STTUS_CHG_RSN_CD_SK"),
        F.lit(1).alias("TRNSMSN_SRC_CD_SK"),
        F.lit("NA").alias("CLM_STTUS_DT_SK"),
        F.lit("1753-01-01-00.00.00.000000").alias("CLM_STTUS_DTM")
    )
)

write_files(
    df_Recycle1,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

write_files(
    df_Recycle_Clms,
    "hf_claim_recycle_keys.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_Collector = (
    df_Output1
    .unionByName(df_DefaultUNK)
    .unionByName(df_DefaulNA)
)

df_Collector_final = (
    df_Collector
    .withColumn("CLM_ID", rpad(F.col("CLM_ID"), 12, " "))
    .withColumn("CLM_STTUS_DT_SK", rpad(F.col("CLM_STTUS_DT_SK"), 10, " "))
    .select(
        "CLM_STTUS_AUDIT_SK",
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "CLM_STTUS_AUDIT_SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLM_SK",
        "CRT_BY_APP_USER_SK",
        "RTE_TO_APP_USER_SK",
        "CLM_STTUS_CD_SK",
        "CLM_STTUS_CHG_RSN_CD_SK",
        "TRNSMSN_SRC_CD_SK",
        "CLM_STTUS_DT_SK",
        "CLM_STTUS_DTM"
    )
)

write_files(
    df_Collector_final,
    f"{adls_path}/load/CLM_STTUS_AUDIT.{SrcSysCd}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)