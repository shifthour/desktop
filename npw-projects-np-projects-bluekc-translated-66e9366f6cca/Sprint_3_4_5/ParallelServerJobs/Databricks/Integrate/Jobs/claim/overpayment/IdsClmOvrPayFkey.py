# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsClmAtchmntFkey
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the foreign keys.  Output is final table format for CLM_ATCHMT
# MAGIC       
# MAGIC 
# MAGIC INPUTS:   Common record format file from Primary key assignment job
# MAGIC 	
# MAGIC   
# MAGIC HASH FILES:     hf_recycle - used to keep records with an error in surrogate key assignment
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  none
# MAGIC                            
# MAGIC 
# MAGIC PROCESSING:  assign foreign (surrogate) keys to record
# MAGIC 
# MAGIC OUTPUTS:  file ready to load to CLM_OVRD table in IDS
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             SAndrew        08/2004         -  Originally Programmed
# MAGIC             Brent Leland  09/09/2004   -  Fixed default values for UNK and NA
# MAGIC             SAndrew        08/09/2005     Per FAcets 4.2  repaced field Payment_Reduction_SK to Payment-Reduction_Ref_id.  no more stage variable and cdma lookup.
# MAGIC             Tao Luo         12/20/2005   -  Added new NA condition.  If ORIG_PAYE_PROV_ID is not blank and ORIG_PAYE_SUB_CK equals zero, 
# MAGIC                                                             then ORIG_PAYE_SUB_SK and ORIG_PAYMT_MBR_SK is set to 1 (NA)
# MAGIC             Steph Goddard  2/16/2006   Changes for sequencer
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                                                Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                                                         --------------------------------       -------------------------------   ----------------------------       
# MAGIC Bhoomi Dasari         2008-07-28      3567(Primary Key)    Added new parameter SrcSysCdSk and passing it directly from source                  devlIDS                         Steph Goddrad           07/29/2008
# MAGIC 
# MAGIC Reddy Sanam         2020-10-09                                       changed stage variable ClmOverPaymentPayee to pass 'FACETS' 
# MAGIC                                                                                         when the source is LUMERIS
# MAGIC 
# MAGIC 
# MAGIC Emran.Mohammad               2020-10-12                                    brought up to standards

# MAGIC This hash file is written to by all claim foriegn key jobs.
# MAGIC Read common record format file from extract job.
# MAGIC Set all foreign surragote keys
# MAGIC Writing Sequential File to /load
# MAGIC Merge source data with default rows
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


Logging = get_widget_value('Logging','Y')
RunID = get_widget_value('RunID','')
Source = get_widget_value('Source','FACETS')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
InFile = get_widget_value('InFile','')

schema_ClmOvrPay = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("CLM_OVER_PAYMT_SK", IntegerType(), nullable=False),
    StructField("CLM_ID", StringType(), nullable=False),
    StructField("CLM_OVER_PAYMT_PAYE_IND", StringType(), nullable=False),
    StructField("LOB_NO", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("PAYMT_RDUCTN_ID", StringType(), nullable=False),
    StructField("ORIG_PAYE_PROV_ID", StringType(), nullable=False),
    StructField("ORIG_PAYE_SUB_CK", IntegerType(), nullable=False),
    StructField("ORIG_PAYMT_MBR_CK", IntegerType(), nullable=False),
    StructField("BYPS_AUTO_OVER_PAYMT_RDUCTN_IN", StringType(), nullable=False),
    StructField("ORIG_PCA_OVER_PAYMT_AMT", DecimalType(38,10), nullable=False),
    StructField("OVER_PAYMT_AMT", DecimalType(38,10), nullable=False)
])

df_ClmOvrPay = (
    spark.read
    .option("quote", "\"")
    .option("header", False)
    .option("delimiter", ",")
    .schema(schema_ClmOvrPay)
    .csv(f"{adls_path}/key/{InFile}")
)

df_foreignKey = (
    df_ClmOvrPay
    .withColumn(
        "ClmSk",
        GetFkeyClm(
            F.col("SRC_SYS_CD"),
            F.col("CLM_OVER_PAYMT_SK"),
            F.col("CLM_ID"),
            Logging
        )
    )
    .withColumn(
        "ClmOverPaymentPayee",
        GetFkeyCodes(
            F.when(F.col("SRC_SYS_CD") == "LUMERIS", F.lit("FACETS")).otherwise(F.col("SRC_SYS_CD")),
            F.col("CLM_OVER_PAYMT_SK"),
            F.lit("CLAIM OVER PAYMENT PAYEE"),
            F.col("CLM_OVER_PAYMT_PAYE_IND"),
            Logging
        )
    )
    .withColumn(
        "OrigPayeeProv",
        GetFkeyProv(
            F.col("SRC_SYS_CD"),
            F.col("CLM_OVER_PAYMT_SK"),
            F.col("ORIG_PAYE_PROV_ID"),
            Logging
        )
    )
    .withColumn(
        "OrigPayeSub",
        F.when(
            (F.col("ORIG_PAYE_PROV_ID").isNotNull()) & (F.col("ORIG_PAYE_SUB_CK") == 0),
            F.lit(1)
        ).otherwise(
            GetFkeySub(
                F.col("SRC_SYS_CD"),
                F.col("CLM_OVER_PAYMT_SK"),
                F.col("ORIG_PAYE_SUB_CK"),
                Logging
            )
        )
    )
    .withColumn(
        "OrigMbr",
        F.when(
            (F.col("ORIG_PAYE_PROV_ID").isNotNull()) & (F.col("ORIG_PAYMT_MBR_CK") == 0),
            F.lit(1)
        ).otherwise(
            GetFkeyMbr(
                F.col("SRC_SYS_CD"),
                F.col("CLM_OVER_PAYMT_SK"),
                F.col("ORIG_PAYMT_MBR_CK"),
                Logging
            )
        )
    )
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("CLM_OVER_PAYMT_SK")))
)

df_fkey = (
    df_foreignKey
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == 'Y'))
    .select(
        F.col("CLM_OVER_PAYMT_SK").alias("CLM_OVER_PAYMT_SK"),
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("ClmOverPaymentPayee").alias("CLM_OVER_PAYMT_PAYE_CD_SK"),
        F.col("LOB_NO").alias("LOB_NO"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("ClmSk").alias("CLM_SK"),
        F.col("OrigPayeeProv").alias("ORIG_PAYE_PROV_SK"),
        F.when(F.col("OrigPayeSub").isNull(), F.lit(0)).otherwise(F.col("OrigPayeSub")).alias("ORIG_PAYE_SUB_SK"),
        F.when(F.col("OrigMbr").isNull(), F.lit(0)).otherwise(F.col("OrigMbr")).alias("ORIG_PAYMT_MBR_SK"),
        F.col("BYPS_AUTO_OVER_PAYMT_RDUCTN_IN").alias("BYPS_AUTO_OVER_PAYMT_RDUCTN_IN"),
        F.col("ORIG_PCA_OVER_PAYMT_AMT").alias("ORIG_PCA_OVER_PAYMT_AMT"),
        F.col("OVER_PAYMT_AMT").alias("OVER_PAYMT_AMT"),
        F.col("PAYMT_RDUCTN_ID").alias("PAYMT_RDUCTN_REF_ID")
    )
)

df_recycle = (
    df_foreignKey
    .filter(F.col("ErrCount") > 0)
    .select(
        GetRecycleKey(F.col("CLM_OVER_PAYMT_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("CLM_OVER_PAYMT_SK").alias("CLM_OVER_PAYMT_SK"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("CLM_OVER_PAYMT_PAYE_IND").alias("CLM_OVER_PAYMT_PAYE_IND"),
        F.col("LOB_NO").alias("LOB_NO"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("PAYMT_RDUCTN_ID").alias("PAYMT_RDUCTN_ID"),
        F.col("ORIG_PAYE_PROV_ID").alias("ORIG_PAYE_PROV_ID"),
        F.col("ORIG_PAYE_SUB_CK").alias("ORIG_PAYE_SUB_CK"),
        F.col("ORIG_PAYMT_MBR_CK").alias("ORIG_PAYMT_MBR_CK"),
        F.col("BYPS_AUTO_OVER_PAYMT_RDUCTN_IN").alias("BYPS_AUTO_OVER_PAYMT_RDUCTN_IN"),
        F.col("ORIG_PCA_OVER_PAYMT_AMT").alias("ORIG_PCA_OVER_PAYMT_AMT"),
        F.col("OVER_PAYMT_AMT").alias("OVER_PAYMT_AMT")
    )
)

df_defaultUNK = (
    df_foreignKey
    .orderBy(F.lit(1))
    .limit(1)
    .select(
        F.lit(0).alias("CLM_OVER_PAYMT_SK"),
        F.lit(0).alias("SRC_SYS_CD_SK"),
        F.lit("UNK").alias("CLM_ID"),
        F.lit(0).alias("CLM_OVER_PAYMT_PAYE_CD_SK"),
        F.lit("UNK").alias("LOB_NO"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("CLM_SK"),
        F.lit(0).alias("ORIG_PAYE_PROV_SK"),
        F.lit(0).alias("ORIG_PAYE_SUB_SK"),
        F.lit(0).alias("ORIG_PAYMT_MBR_SK"),
        F.lit("U").alias("BYPS_AUTO_OVER_PAYMT_RDUCTN_IN"),
        F.lit(0).alias("ORIG_PCA_OVER_PAYMT_AMT"),
        F.lit(0).alias("OVER_PAYMT_AMT"),
        F.lit("UNK").alias("PAYMT_RDUCTN_REF_ID")
    )
)

df_defaultNA = (
    df_foreignKey
    .orderBy(F.lit(1))
    .limit(1)
    .select(
        F.lit(1).alias("CLM_OVER_PAYMT_SK"),
        F.lit(1).alias("SRC_SYS_CD_SK"),
        F.lit("NA").alias("CLM_ID"),
        F.lit(1).alias("CLM_OVER_PAYMT_PAYE_CD_SK"),
        F.lit("NA").alias("LOB_NO"),
        F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("CLM_SK"),
        F.lit(1).alias("ORIG_PAYE_PROV_SK"),
        F.lit(1).alias("ORIG_PAYE_SUB_SK"),
        F.lit(1).alias("ORIG_PAYMT_MBR_SK"),
        F.lit("X").alias("BYPS_AUTO_OVER_PAYMT_RDUCTN_IN"),
        F.lit(0).alias("ORIG_PCA_OVER_PAYMT_AMT"),
        F.lit(0).alias("OVER_PAYMT_AMT"),
        F.lit("NA").alias("PAYMT_RDUCTN_REF_ID")
    )
)

df_recycle_clms = (
    df_foreignKey
    .filter(F.col("ErrCount") > 0)
    .select(
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("CLM_ID").alias("CLM_ID")
    )
)

df_recycle_write = df_recycle.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_OVER_PAYMT_SK"),
    F.col("CLM_ID"),
    F.rpad(F.col("CLM_OVER_PAYMT_PAYE_IND"), 1, " ").alias("CLM_OVER_PAYMT_PAYE_IND"),
    F.rpad(F.col("LOB_NO"), 4, " ").alias("LOB_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.col("PAYMT_RDUCTN_ID"), 16, " ").alias("PAYMT_RDUCTN_ID"),
    F.rpad(F.col("ORIG_PAYE_PROV_ID"), 10, " ").alias("ORIG_PAYE_PROV_ID"),
    F.col("ORIG_PAYE_SUB_CK"),
    F.col("ORIG_PAYMT_MBR_CK"),
    F.rpad(F.col("BYPS_AUTO_OVER_PAYMT_RDUCTN_IN"), 1, " ").alias("BYPS_AUTO_OVER_PAYMT_RDUCTN_IN"),
    F.col("ORIG_PCA_OVER_PAYMT_AMT"),
    F.col("OVER_PAYMT_AMT")
)

write_files(
    df_recycle_write,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_claim_recycle_keys_write = df_recycle_clms.select(
    F.col("SRC_SYS_CD"),
    F.col("CLM_ID")
)

write_files(
    df_claim_recycle_keys_write,
    "hf_claim_recycle_keys.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_collector = (
    df_fkey.select(
        F.col("CLM_OVER_PAYMT_SK"),
        F.col("SRC_SYS_CD_SK"),
        F.col("CLM_ID"),
        F.col("CLM_OVER_PAYMT_PAYE_CD_SK"),
        F.col("LOB_NO"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("CLM_SK"),
        F.col("ORIG_PAYE_PROV_SK"),
        F.col("ORIG_PAYE_SUB_SK"),
        F.col("ORIG_PAYMT_MBR_SK"),
        F.col("BYPS_AUTO_OVER_PAYMT_RDUCTN_IN"),
        F.col("ORIG_PCA_OVER_PAYMT_AMT"),
        F.col("OVER_PAYMT_AMT"),
        F.col("PAYMT_RDUCTN_REF_ID")
    )
    .unionByName(
        df_defaultUNK.select(
            F.col("CLM_OVER_PAYMT_SK"),
            F.col("SRC_SYS_CD_SK"),
            F.col("CLM_ID"),
            F.col("CLM_OVER_PAYMT_PAYE_CD_SK"),
            F.col("LOB_NO"),
            F.col("CRT_RUN_CYC_EXCTN_SK"),
            F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("CLM_SK"),
            F.col("ORIG_PAYE_PROV_SK"),
            F.col("ORIG_PAYE_SUB_SK"),
            F.col("ORIG_PAYMT_MBR_SK"),
            F.col("BYPS_AUTO_OVER_PAYMT_RDUCTN_IN"),
            F.col("ORIG_PCA_OVER_PAYMT_AMT"),
            F.col("OVER_PAYMT_AMT"),
            F.col("PAYMT_RDUCTN_REF_ID")
        )
    )
    .unionByName(
        df_defaultNA.select(
            F.col("CLM_OVER_PAYMT_SK"),
            F.col("SRC_SYS_CD_SK"),
            F.col("CLM_ID"),
            F.col("CLM_OVER_PAYMT_PAYE_CD_SK"),
            F.col("LOB_NO"),
            F.col("CRT_RUN_CYC_EXCTN_SK"),
            F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("CLM_SK"),
            F.col("ORIG_PAYE_PROV_SK"),
            F.col("ORIG_PAYE_SUB_SK"),
            F.col("ORIG_PAYMT_MBR_SK"),
            F.col("BYPS_AUTO_OVER_PAYMT_RDUCTN_IN"),
            F.col("ORIG_PCA_OVER_PAYMT_AMT"),
            F.col("OVER_PAYMT_AMT"),
            F.col("PAYMT_RDUCTN_REF_ID")
        )
    )
)

df_collector_write = df_collector.select(
    F.col("CLM_OVER_PAYMT_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("CLM_OVER_PAYMT_PAYE_CD_SK"),
    F.rpad(F.col("LOB_NO"), 4, " ").alias("LOB_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_SK"),
    F.col("ORIG_PAYE_PROV_SK"),
    F.col("ORIG_PAYE_SUB_SK"),
    F.col("ORIG_PAYMT_MBR_SK"),
    F.rpad(F.col("BYPS_AUTO_OVER_PAYMT_RDUCTN_IN"), 1, " ").alias("BYPS_AUTO_OVER_PAYMT_RDUCTN_IN"),
    F.col("ORIG_PCA_OVER_PAYMT_AMT"),
    F.col("OVER_PAYMT_AMT"),
    F.col("PAYMT_RDUCTN_REF_ID")
)

write_files(
    df_collector_write,
    f"{adls_path}/load/CLM_OVER_PAYMT.{Source}.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)