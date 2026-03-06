# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007, 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer             Date                         Change Description                                                                                Project #                   Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------           ----------------------------     -----------------------------------------------------------------------                                       ----------------                 ------------------------------------       ----------------------------      ----------------
# MAGIC Parikshith Chada   01/25/2007             3028                          Originally Programmed                                                                           devlIDS30                           Steph Goddard            02/21/2007
# MAGIC Brent Leland          02/29/2008             Added error recycle                                                                              3567 Primary Key       devlIDScur                         Steph Goddard            05/06/2008 
# MAGIC                                                               Added foreign key lookup for APL_SK
# MAGIC              
# MAGIC Jag Yelavarthi     2015-06-19                Changed CLM_SK lookup from Hash file lookup to a                             TFS#1057                IntegrateDev1                       Kalyan Neelam       2015-06-23
# MAGIC                                                               Direct database lookup to K_CLM table. Reason for the 
# MAGIC                                                               change is that hf_clm does not hold full contents of
# MAGIC                                                               Claim records needed for reference lookup.
# MAGIC 
# MAGIC 
# MAGIC Akhila M           2018-03-14                1. Changed CUST_SVC_TASK lookup from Hash file lookup to a                  TFS#211664                IntegrateDev2             Kalyan Neelam      2018-03-14     
# MAGIC                                                               Direct database lookup to K_CUST_SVC_TASK table. Reason for the 
# MAGIC                                                               change is that hf_cust_svc_task does not hold full contents of
# MAGIC                                                                records needed for reference lookup.
# MAGIC 
# MAGIC                                                            2. Umsk logic has been updated - the Fkey routine is called only for
# MAGIC                                                                 typ_cd ='U'. If routine is called for other type codes, error logging 
# MAGIC                                                                 occurs due to fkey failure. SK value expected only for typ_cd='U'
# MAGIC 
# MAGIC                                                            3. svRelCustSvcSk logic has been updated - the Fkey routine is called 
# MAGIC                                                                  with error logging 'X'. The Fkey from that routine is not mandatory

# MAGIC Set all foreign surrogate keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC Cannot call the GfkeyCustsvcTask since the cust_svc_task load is incremental and the hash file has only incremental data. The Cust_svc_task_link is a full pull every time and has the tasks that are not there in the hf_cust_svc_task. so K table is used
# MAGIC The GfkeyCustsvcTask routine is used as well to get the new keys generated in the current run.
# MAGIC Read common record format file from extract job.
# MAGIC This hash file is written to by all customer service foriegn key jobs.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    TimestampType
)
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value('Logging','')
InFile = get_widget_value('InFile','')
RunID = get_widget_value('RunID','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_clm = f"SELECT SRC_SYS_CD_SK,CLM_ID,CLM_SK FROM {IDSOwner}.K_CLM"
df_K_CLM_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_clm)
    .load()
)

extract_query_cust_svc_task = f"SELECT SRC_SYS_CD_SK,CUST_SVC_ID,TASK_SEQ_NO,CUST_SVC_TASK_SK FROM {IDSOwner}.K_CUST_SVC_TASK"
df_K_Cust_Svc_Task_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_cust_svc_task)
    .load()
)

schema_IdsCustSvcTaskLink = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("CUST_SVC_TASK_LINK_SK", IntegerType(), False),
    StructField("CUST_SVC_ID", StringType(), False),
    StructField("TASK_SEQ_NO", IntegerType(), False),
    StructField("CUST_SVC_TASK_LINK_TYP_CD_SK", StringType(), False),
    StructField("LINK_RCRD_ID", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("APL_SK", StringType(), False),
    StructField("CLM_ID", StringType(), False),
    StructField("CUST_SVC_TASK_SK", StringType(), False),
    StructField("LAST_UPDT_USER_SK", StringType(), False),
    StructField("REL_CUST_SVC_SK", StringType(), False),
    StructField("UM_SK", StringType(), False),
    StructField("CUST_SVC_TASK_LINK_RSN_CD_SK", StringType(), False),
    StructField("LAST_UPDT_DTM", TimestampType(), False),
    StructField("LINK_DESC", StringType(), True)
])

df_IdsCustSvcTaskLink = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .schema(schema_IdsCustSvcTaskLink)
    .load(f"{adls_path}/key/{InFile}")
)

df_joined = (
    df_IdsCustSvcTaskLink.alias("Key")
    .join(
        df_K_CLM_Lkp.alias("Lnk_KClm_Lkp"),
        [
            F.col("Key.SrcSysCdSk") == F.col("Lnk_KClm_Lkp.SRC_SYS_CD_SK"),
            F.col("Key.CLM_ID") == F.col("Lnk_KClm_Lkp.CLM_ID")
        ],
        "left"
    )
    .join(
        df_K_Cust_Svc_Task_Lkp.alias("Lnk_KCustsvcTask_Lkp"),
        [
            F.col("Key.SrcSysCdSk") == F.col("Lnk_KCustsvcTask_Lkp.SRC_SYS_CD_SK"),
            F.col("Key.CUST_SVC_ID") == F.col("Lnk_KCustsvcTask_Lkp.CUST_SVC_ID"),
            F.col("Key.TASK_SEQ_NO") == F.col("Lnk_KCustsvcTask_Lkp.TASK_SEQ_NO")
        ],
        "left"
    )
)

df_enriched = (
    df_joined
    .withColumn("PassThru", F.col("Key.PASS_THRU_IN"))
    .withColumn(
        "svAplSk",
        GetFkeyApl(
            F.col("Key.SRC_SYS_CD"),
            F.col("Key.CUST_SVC_TASK_LINK_SK"),
            F.col("Key.APL_SK"),
            F.col("Logging")
        )
    )
    .withColumn(
        "svClmSk",
        F.when(F.col("Key.CLM_ID") == 'NA', F.lit(1))
        .otherwise(
            F.when(F.isnull(F.col("Lnk_KClm_Lkp.CLM_SK")), F.lit(0))
            .otherwise(F.col("Lnk_KClm_Lkp.CLM_SK"))
        )
    )
    .withColumn(
        "svUmSk",
        F.when(
            F.col("Key.CUST_SVC_TASK_LINK_TYP_CD_SK") == 'U',
            GetFkeyUm(
                F.col("Key.SRC_SYS_CD"),
                F.col("Key.CUST_SVC_TASK_LINK_SK"),
                F.col("Key.UM_SK"),
                F.col("Logging")
            )
        ).otherwise(F.lit(1))
    )
    .withColumn(
        "svLastUpdtUserSk",
        GetFkeyAppUsr(
            F.col("Key.SRC_SYS_CD"),
            F.col("Key.CUST_SVC_TASK_LINK_SK"),
            F.col("Key.LAST_UPDT_USER_SK"),
            F.col("Logging")
        )
    )
    .withColumn(
        "svRelCustSvcSk",
        GetFkeyCustSvc(
            F.col("Key.SRC_SYS_CD"),
            F.col("Key.CUST_SVC_TASK_LINK_SK"),
            F.col("Key.REL_CUST_SVC_SK"),
            F.lit("X")
        )
    )
    .withColumn(
        "svCustSvcTaskLinkRsnCdSk",
        GetFkeyCodes(
            F.col("Key.SRC_SYS_CD"),
            F.col("Key.CUST_SVC_TASK_LINK_SK"),
            F.lit("CUSTOMER SERVICE TASK LINK REASON"),
            F.col("Key.CUST_SVC_TASK_LINK_RSN_CD_SK"),
            F.col("Logging")
        )
    )
    .withColumn(
        "svCustSvcTaskLinkTypCdSk",
        GetFkeyCodes(
            F.col("Key.SRC_SYS_CD"),
            F.col("Key.CUST_SVC_TASK_LINK_SK"),
            F.lit("CUSTOMER SERVICE TASK LINK TYPE"),
            F.col("Key.CUST_SVC_TASK_LINK_TYP_CD_SK"),
            F.col("Logging")
        )
    )
    .withColumn(
        "ErrCount",
        GetFkeyErrorCnt(F.col("Key.CUST_SVC_TASK_LINK_SK"))
    )
)

df_Fkey = df_enriched.filter("(ErrCount = 0) OR (PassThru = 'Y')").select(
    F.col("Key.CUST_SVC_TASK_LINK_SK").alias("CUST_SVC_TASK_LINK_SK"),
    F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.col("Key.CUST_SVC_ID").alias("CUST_SVC_ID"),
    F.col("Key.TASK_SEQ_NO").alias("TASK_SEQ_NO"),
    F.col("svCustSvcTaskLinkTypCdSk").alias("CUST_SVC_TASK_LINK_TYP_CD_SK"),
    F.col("Key.LINK_RCRD_ID").alias("LINK_RCRD_ID"),
    F.col("Key.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Key.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("svAplSk").alias("APL_SK"),
    F.when(
        F.col("Key.SRC_SYS_CD") == 'NPS',
        F.when(
            (F.col("Key.CUST_SVC_TASK_LINK_TYP_CD_SK") == 'AWDDOCUMENT') | 
            (F.col("Key.CUST_SVC_TASK_LINK_TYP_CD_SK") == 'AWDINQUIRYDOCUMENT'),
            F.lit(1)
        ).otherwise(F.col("svClmSk"))
    ).otherwise(
        F.when(
            (
                (F.col("Key.CUST_SVC_TASK_LINK_TYP_CD_SK") == 'C') |
                (F.col("Key.CUST_SVC_TASK_LINK_TYP_CD_SK") == 'V')
            ) &
            (F.col("svClmSk") == 1) &
            (F.col("svRelCustSvcSk") == 1),
            F.lit(0)
        ).otherwise(
            F.when(
                (F.col("Key.CUST_SVC_TASK_LINK_TYP_CD_SK") == 'C') & (F.col("svClmSk") == 1),
                F.lit(1)
            ).otherwise(
                F.when(
                    F.col("Key.CUST_SVC_TASK_LINK_TYP_CD_SK") == 'C',
                    F.col("svClmSk")
                ).otherwise(
                    F.when(
                        (F.col("Key.CUST_SVC_TASK_LINK_TYP_CD_SK") == 'V') &
                        (F.col("svRelCustSvcSk") == 1),
                        F.col("svClmSk")
                    ).otherwise(
                        F.when(
                            (F.col("Key.CUST_SVC_TASK_LINK_TYP_CD_SK") == 'V') &
                            (F.col("svRelCustSvcSk") != 1),
                            F.lit(1)
                        ).otherwise(F.lit(1))
                    )
                )
            )
        )
    ).alias("CLM_SK"),
    F.when(
        F.isnan(F.col("Lnk_KCustsvcTask_Lkp.CUST_SVC_TASK_SK")) | F.isnull(F.col("Lnk_KCustsvcTask_Lkp.CUST_SVC_TASK_SK")),
        GetFkeyCustSvcTask(
            F.col("Key.SRC_SYS_CD"),
            F.col("Key.CUST_SVC_TASK_LINK_SK"),
            F.col("Key.CUST_SVC_ID"),
            F.col("Key.TASK_SEQ_NO"),
            F.col("Logging")
        )
    ).otherwise(F.col("Lnk_KCustsvcTask_Lkp.CUST_SVC_TASK_SK")).alias("CUST_SVC_TASK_SK"),
    F.col("svLastUpdtUserSk").alias("LAST_UPDT_USER_SK"),
    F.when(
        ((F.col("Key.CUST_SVC_TASK_LINK_TYP_CD_SK") == 'V') | (
            (F.col("Key.CUST_SVC_TASK_LINK_TYP_CD_SK") == 'C') &
            (F.col("svClmSk") == 1) &
            (F.col("svRelCustSvcSk") == 1)
        )),
        F.lit(0)
    ).otherwise(
        F.when(
            (F.col("Key.CUST_SVC_TASK_LINK_TYP_CD_SK") == 'V') & (F.col("svRelCustSvcSk") == 1),
            F.lit(1)
        ).otherwise(
            F.when(
                (F.col("Key.CUST_SVC_TASK_LINK_TYP_CD_SK") == 'V'),
                F.col("svRelCustSvcSk")
            ).otherwise(
                F.when(
                    (F.col("Key.CUST_SVC_TASK_LINK_TYP_CD_SK") == 'C') & (F.col("svClmSk") == 1),
                    F.col("svRelCustSvcSk")
                ).otherwise(
                    F.when(
                        (F.col("Key.CUST_SVC_TASK_LINK_TYP_CD_SK") == 'C') & (F.col("svClmSk") != 1),
                        F.lit(1)
                    ).otherwise(F.lit(1))
                )
            )
        )
    ).alias("REL_CUST_SVC_SK"),
    F.col("svUmSk").alias("UM_SK"),
    F.col("svCustSvcTaskLinkRsnCdSk").alias("CUST_SVC_TASK_LINK_RSN_CD_SK"),
    F.col("Key.LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    F.col("Key.LINK_DESC").alias("LINK_DESC")
)

df_Recycle = df_enriched.filter("ErrCount > 0").select(
    GetRecycleKey(F.col("Key.CUST_SVC_TASK_LINK_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("Key.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("Key.DISCARD_IN").alias("DISCARD_IN"),
    F.col("Key.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("Key.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ErrCount").alias("ERR_CT"),
    (F.col("Key.RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
    F.col("Key.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Key.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("Key.CUST_SVC_TASK_LINK_SK").alias("CUST_SVC_TASK_LINK_SK"),
    F.col("Key.CUST_SVC_ID").alias("CUST_SVC_ID"),
    F.col("Key.TASK_SEQ_NO").alias("TASK_SEQ_NO"),
    F.col("Key.CUST_SVC_TASK_LINK_TYP_CD_SK").alias("CUST_SVC_TASK_LINK_TYP_CD_SK"),
    F.col("Key.LINK_RCRD_ID").alias("LINK_RCRD_ID"),
    F.col("Key.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Key.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Key.APL_SK").alias("APL_SK"),
    F.col("Key.CLM_ID").alias("CLM_SK"),
    F.col("Key.CUST_SVC_TASK_SK").alias("CUST_SVC_TASK_SK"),
    F.col("Key.LAST_UPDT_USER_SK").alias("LAST_UPDT_USER_SK"),
    F.col("Key.REL_CUST_SVC_SK").alias("REL_CUST_SVC_SK"),
    F.col("Key.UM_SK").alias("UM_SK"),
    F.col("Key.CUST_SVC_TASK_LINK_RSN_CD_SK").alias("CUST_SVC_TASK_LINK_RSN_CD_SK"),
    F.col("Key.LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    F.col("Key.LINK_DESC").alias("LINK_DESC")
)

df_Recycle_Keys = df_enriched.filter("ErrCount > 0").select(
    F.col("Key.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Key.CUST_SVC_ID").alias("CUST_SVC_ID"),
    F.col("Key.TASK_SEQ_NO").alias("TASK_SEQ_NO")
)

# Prepare single-row default dataframes for the Collector

collector_schema = StructType([
    StructField("CUST_SVC_TASK_LINK_SK", IntegerType(), True),
    StructField("SRC_SYS_CD_SK", IntegerType(), True),
    StructField("CUST_SVC_ID", StringType(), True),
    StructField("TASK_SEQ_NO", IntegerType(), True),
    StructField("CUST_SVC_TASK_LINK_TYP_CD_SK", StringType(), True),
    StructField("LINK_RCRD_ID", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("APL_SK", StringType(), True),
    StructField("CLM_SK", IntegerType(), True),
    StructField("CUST_SVC_TASK_SK", StringType(), True),
    StructField("LAST_UPDT_USER_SK", StringType(), True),
    StructField("REL_CUST_SVC_SK", StringType(), True),
    StructField("UM_SK", StringType(), True),
    StructField("CUST_SVC_TASK_LINK_RSN_CD_SK", StringType(), True),
    StructField("LAST_UPDT_DTM", TimestampType(), True),
    StructField("LINK_DESC", StringType(), True)
])

df_DefaultUNK = spark.createDataFrame(
    [
        (
            0, 0, "UNK", 0, "0", "UNK", 0, 0, "0", 0,
            "0", "0", "0", "0", "0",
            F.to_timestamp(F.lit("1753-01-01 00:00:00"), "yyyy-MM-dd HH:mm:ss"),
            "UNK"
        )
    ],
    collector_schema
)

df_DefaultNA = spark.createDataFrame(
    [
        (
            1, 1, "NA", 1, "1", "NA", 1, 1, "1", 1,
            "1", "1", "1", "1", "1",
            F.to_timestamp(F.lit("1753-01-01 00:00:00"), "yyyy-MM-dd HH:mm:ss"),
            "NA"
        )
    ],
    collector_schema
)

df_Collector = (
    df_Fkey.select(
        "CUST_SVC_TASK_LINK_SK",
        "SRC_SYS_CD_SK",
        "CUST_SVC_ID",
        "TASK_SEQ_NO",
        "CUST_SVC_TASK_LINK_TYP_CD_SK",
        "LINK_RCRD_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "APL_SK",
        "CLM_SK",
        "CUST_SVC_TASK_SK",
        "LAST_UPDT_USER_SK",
        "REL_CUST_SVC_SK",
        "UM_SK",
        "CUST_SVC_TASK_LINK_RSN_CD_SK",
        "LAST_UPDT_DTM",
        "LINK_DESC"
    )
    .unionByName(df_DefaultUNK)
    .unionByName(df_DefaultNA)
)

# rpad for Collector output
df_Collector_final = df_Collector.select(
    F.col("CUST_SVC_TASK_LINK_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.rpad(F.col("CUST_SVC_ID"), 255, " ").alias("CUST_SVC_ID"),
    F.col("TASK_SEQ_NO"),
    F.rpad(F.col("CUST_SVC_TASK_LINK_TYP_CD_SK"), 255, " ").alias("CUST_SVC_TASK_LINK_TYP_CD_SK"),
    F.rpad(F.col("LINK_RCRD_ID"), 255, " ").alias("LINK_RCRD_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.col("APL_SK"), 255, " ").alias("APL_SK"),
    F.col("CLM_SK"),
    F.rpad(F.col("CUST_SVC_TASK_SK"), 255, " ").alias("CUST_SVC_TASK_SK"),
    F.rpad(F.col("LAST_UPDT_USER_SK"), 255, " ").alias("LAST_UPDT_USER_SK"),
    F.rpad(F.col("REL_CUST_SVC_SK"), 255, " ").alias("REL_CUST_SVC_SK"),
    F.rpad(F.col("UM_SK"), 255, " ").alias("UM_SK"),
    F.rpad(F.col("CUST_SVC_TASK_LINK_RSN_CD_SK"), 255, " ").alias("CUST_SVC_TASK_LINK_RSN_CD_SK"),
    F.col("LAST_UPDT_DTM"),
    F.rpad(F.col("LINK_DESC"), 255, " ").alias("LINK_DESC")
)

# Write the final collector output to a CSV file
write_files(
    df_Collector_final,
    f"{adls_path}/load/CUST_SVC_TASK_LINK.{SrcSysCd}.tmp",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# rpad for Recycle
df_Recycle_final = df_Recycle.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.rpad(F.col("SRC_SYS_CD"), 255, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("PRI_KEY_STRING"), 255, " ").alias("PRI_KEY_STRING"),
    F.col("CUST_SVC_TASK_LINK_SK"),
    F.rpad(F.col("CUST_SVC_ID"), 255, " ").alias("CUST_SVC_ID"),
    F.col("TASK_SEQ_NO"),
    F.rpad(F.col("CUST_SVC_TASK_LINK_TYP_CD_SK"), 255, " ").alias("CUST_SVC_TASK_LINK_TYP_CD_SK"),
    F.rpad(F.col("LINK_RCRD_ID"), 255, " ").alias("LINK_RCRD_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.col("APL_SK"), 255, " ").alias("APL_SK"),
    F.rpad(F.col("CLM_SK"), 255, " ").alias("CLM_SK"),
    F.rpad(F.col("CUST_SVC_TASK_SK"), 255, " ").alias("CUST_SVC_TASK_SK"),
    F.rpad(F.col("LAST_UPDT_USER_SK"), 255, " ").alias("LAST_UPDT_USER_SK"),
    F.rpad(F.col("REL_CUST_SVC_SK"), 255, " ").alias("REL_CUST_SVC_SK"),
    F.rpad(F.col("UM_SK"), 255, " ").alias("UM_SK"),
    F.rpad(F.col("CUST_SVC_TASK_LINK_RSN_CD_SK"), 255, " ").alias("CUST_SVC_TASK_LINK_RSN_CD_SK"),
    F.col("LAST_UPDT_DTM"),
    F.rpad(F.col("LINK_DESC"), 255, " ").alias("LINK_DESC")
)

write_files(
    df_Recycle_final,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# rpad for Recycle_Keys
df_Recycle_Keys_final = df_Recycle_Keys.select(
    F.rpad(F.col("SRC_SYS_CD"), 255, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("CUST_SVC_ID"), 255, " ").alias("CUST_SVC_ID"),
    F.col("TASK_SEQ_NO")
)

write_files(
    df_Recycle_Keys_final,
    "hf_custsvc_recycle_keys.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)