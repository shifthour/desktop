# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY: IdsFctsClmLoad3Seq
# MAGIC                            
# MAGIC 
# MAGIC PROCESSING: Takes the sorted file and applies the foreign keys.  Output is final table format for CLM_ATCHMT
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------    
# MAGIC Sharon Andrew       08/2004                                          Originally Programmed
# MAGIC SAndrew                 08/08/2005                                    Facets 4.2 changes.   Added key field ACPR_SUB_TYP.  
# MAGIC                                                                                        impacts extract, CRF, primary and load.
# MAGIC Steph Goddard       02/16/2006                                   changes for sequencer
# MAGIC Bhoomi Dasari         2008-07-09      3657(Primary Key)   Added SRC_SYS_CD field to the table                                        devlIDS                          Brent Leland              07-14-2008
# MAGIC                                                                                        Changed stage variable name Status to svStatus due to 
# MAGIC                                                                                        reserved word usage in DataStage 8.1
# MAGIC Reddy Sanam        2020-10-09                                      created this stage variable -svSrcSysCd
# MAGIC                                                                                        to pass "FACETS" for 'LUMERIS'
# MAGIC 
# MAGIC 
# MAGIC Emran.Mohammad               2020-10-12                                             brought up to standards

# MAGIC Read common record format file.
# MAGIC Writing Sequential File to /load
# MAGIC Assign foreign keys and create default records for unknown and not applicable.
# MAGIC Create default rows for UNK and NA
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    TimestampType
)
from pyspark.sql.functions import (
    lit,
    when,
    col,
    rpad,
    row_number
)
from pyspark.sql import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Retrieve parameter values
Logging = get_widget_value('Logging', 'Y')
Source = get_widget_value('Source', 'FACETS')
SrcSysCdSk = get_widget_value('SrcSysCdSk', '105859')
InFile = get_widget_value('InFile', '')

# Define schema for ClmPayRductnCrf (CSeqFileStage)
schema_ClmPayRductnCrf = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(10, 0), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("PAYMT_RDUCTN_SK", IntegerType(), False),
    StructField("PAYMT_RDUCTN_REF_ID", StringType(), False),
    StructField("PAYMT_RDUCTN_SUBTYP_CD", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("PAYEE_PROVDER_ID", StringType(), False),
    StructField("USER_ID", IntegerType(), False),
    StructField("PAYMT_RDUCTN_EXCD_ID", IntegerType(), False),
    StructField("PAYMT_RDUCTN_PAYE_TYP_CD", StringType(), False),
    StructField("PAYMT_RDUCTN_PRM_TYP_CD", StringType(), False),
    StructField("PAYMT_RDUCTN_STTUS_CD", StringType(), False),
    StructField("PAYMT_RDUCTN_TYP_CD", StringType(), False),
    StructField("AUTO_PAYMT_RDUCTN_IN", StringType(), False),
    StructField("CRT_DT", StringType(), False),
    StructField("PLN_YR_DT", StringType(), False),
    StructField("ORIG_RDUCTN_AMT", DecimalType(38, 10), False),
    StructField("PCA_OVERPD_NET_AMT", DecimalType(38, 10), False),
    StructField("RCVD_AMT", DecimalType(38, 10), False),
    StructField("RCVRED_AMT", DecimalType(38, 10), False),
    StructField("REMN_NET_AMT", DecimalType(38, 10), False),
    StructField("WRT_OFF_AMT", DecimalType(38, 10), False),
    StructField("RDUCTN_DESC", StringType(), False),
    StructField("TAX_YR", StringType(), False)
])

# Read ClmPayRductnCrf
df_ClmPayRductnCrf = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .schema(schema_ClmPayRductnCrf)
    .load(f"{adls_path}/key/{InFile}")
)

# Enrich with stage variables (ForeignKey - CTransformerStage)
# Add svSrcSysCd
df_enriched = df_ClmPayRductnCrf.withColumn(
    "svSrcSysCd",
    when(col("SRC_SYS_CD") == "LUMERIS", lit("FACETS")).otherwise(col("SRC_SYS_CD"))
)

# Add other stage variables by direct user-defined function calls
df_enriched = df_enriched.withColumn(
    "PaymntRductnSubTypeCd",
    GetFkeyCodes(
        col("svSrcSysCd"),
        col("PAYMT_RDUCTN_SK"),
        lit("PAYMENT REDUCTION SUBTYPE"),
        col("PAYMT_RDUCTN_SUBTYP_CD"),
        lit(Logging)
    )
).withColumn(
    "PayeeProvider",
    GetFkeyProv(
        col("SRC_SYS_CD"),
        col("PAYMT_RDUCTN_SK"),
        col("PAYEE_PROVDER_ID"),
        lit(Logging)
    )
).withColumn(
    "User",
    GetFkeyAppUsr(
        col("SRC_SYS_CD"),
        col("PAYMT_RDUCTN_SK"),
        col("USER_ID"),
        lit(Logging)
    )
).withColumn(
    "Explanation",
    GetFkeyExcd(
        col("SRC_SYS_CD"),
        col("PAYMT_RDUCTN_SK"),
        col("PAYMT_RDUCTN_EXCD_ID"),
        lit(Logging)
    )
).withColumn(
    "PayeeType",
    GetFkeyCodes(
        col("svSrcSysCd"),
        col("PAYMT_RDUCTN_SK"),
        lit("PAYMENT REDUCTION PAYEE TYPE"),
        col("PAYMT_RDUCTN_PAYE_TYP_CD"),
        lit(Logging)
    )
).withColumn(
    "PremiumType",
    GetFkeyCodes(
        col("svSrcSysCd"),
        col("PAYMT_RDUCTN_SK"),
        lit("PAYMENT REDUCTION PREMIUM TYPE"),
        col("PAYMT_RDUCTN_PRM_TYP_CD"),
        lit(Logging)
    )
).withColumn(
    "svStatus",
    GetFkeyCodes(
        col("svSrcSysCd"),
        col("PAYMT_RDUCTN_SK"),
        lit("PAYMENT REDUCTION STATUS"),
        col("PAYMT_RDUCTN_STTUS_CD"),
        lit(Logging)
    )
).withColumn(
    "ReductionType",
    GetFkeyCodes(
        col("svSrcSysCd"),
        col("PAYMT_RDUCTN_SK"),
        lit("PAYMENT REDUCTION TYPE"),
        col("PAYMT_RDUCTN_TYP_CD"),
        lit(Logging)
    )
).withColumn(
    "CreateDate",
    GetFkeyDate(
        lit("IDS"),
        col("PAYMT_RDUCTN_SK"),
        col("CRT_DT"),
        lit(Logging)
    )
).withColumn(
    "PassThru",
    col("PASS_THRU_IN")
).withColumn(
    "ErrCount",
    GetFkeyErrorCnt(
        col("PAYMT_RDUCTN_SK")
    )
)

# Add parameter-based column for use in Pkey link
df_enriched = df_enriched.withColumn("SrcSysCdSk", lit(SrcSysCdSk))

# Derive df for "recycle" (ErrCount > 0)
df_for_recycle = (
    df_enriched
    .filter(col("ErrCount") > 0)
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", GetRecycleKey(col("PAYMT_RDUCTN_SK")))
    .withColumn("ERR_CT", col("ErrCount"))
    .withColumn("RECYCLE_CT", col("RECYCLE_CT") + lit(1))
    .select(
        col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD"),
        col("DISCARD_IN"),
        col("PASS_THRU_IN"),
        col("FIRST_RECYC_DT"),
        col("ERR_CT"),
        col("RECYCLE_CT"),
        col("SRC_SYS_CD"),
        col("PRI_KEY_STRING"),
        col("PAYMT_RDUCTN_SK"),
        col("PAYMT_RDUCTN_REF_ID"),
        col("PAYMT_RDUCTN_SUBTYP_CD"),
        col("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("PAYEE_PROVDER_ID"),
        col("USER_ID"),
        col("PAYMT_RDUCTN_EXCD_ID"),
        col("PAYMT_RDUCTN_PAYE_TYP_CD"),
        col("PAYMT_RDUCTN_PRM_TYP_CD"),
        col("PAYMT_RDUCTN_STTUS_CD"),
        col("PAYMT_RDUCTN_TYP_CD"),
        col("AUTO_PAYMT_RDUCTN_IN"),
        col("CRT_DT"),
        col("PLN_YR_DT"),
        col("ORIG_RDUCTN_AMT"),
        col("PCA_OVERPD_NET_AMT"),
        col("RCVD_AMT"),
        col("RCVRED_AMT"),
        col("REMN_NET_AMT"),
        col("WRT_OFF_AMT"),
        col("RDUCTN_DESC"),
        col("TAX_YR")
    )
)

# Write hashed file recycle to parquet (Scenario C)
write_files(
    df_for_recycle,
    f"{adls_path}/hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# Derive df_for_pkey (ErrCount=0 or PassThru=Y)
df_for_pkey = (
    df_enriched
    .filter((col("ErrCount") == 0) | (col("PassThru") == "Y"))
    .select(
        col("PAYMT_RDUCTN_SK").alias("PAYMT_RDUCTN_SK"),
        col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        col("PAYMT_RDUCTN_REF_ID").alias("PAYMT_RDUCTN_REF_ID"),
        col("PaymntRductnSubTypeCd").alias("PAYMT_RDUCTN_SUBTYP_CD_SK"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("PayeeProvider").alias("PAYE_PROV_SK"),
        col("User").alias("USER_SK"),
        col("Explanation").alias("PAYMT_RDUCTN_EXCD_SK"),
        col("PayeeType").alias("PAYMT_RDUCTN_PAYE_TYP_CD_SK"),
        col("PremiumType").alias("PAYMT_RDUCTN_PRM_TYP_CD_SK"),
        col("svStatus").alias("PAYMT_RDUCTN_STTUS_CD_SK"),
        col("ReductionType").alias("PAYMT_RDUCTN_TYP_CD_SK"),
        col("AUTO_PAYMT_RDUCTN_IN").alias("AUTO_PAYMT_RDUCTN_IN"),
        col("CreateDate").alias("CRT_DT_SK"),
        col("PLN_YR_DT").alias("PLN_YR_DT_SK"),
        col("ORIG_RDUCTN_AMT").alias("ORIG_RDUCTN_AMT"),
        col("PCA_OVERPD_NET_AMT").alias("PCA_OVERPD_NET_AMT"),
        col("RCVD_AMT").alias("RCVD_AMT"),
        col("RCVRED_AMT").alias("RCVRED_AMT"),
        col("REMN_NET_AMT").alias("REMN_NET_AMT"),
        col("WRT_OFF_AMT").alias("WRT_OFF_AMT"),
        col("RDUCTN_DESC").alias("RDUCTN_DESC"),
        col("TAX_YR").alias("TAX_YR")
    )
)

# Build DefaultUNK row (constraint: @INROWNUM = 1)
data_default_unk = [
    (
        0,  # PAYMT_RDUCTN_SK
        0,  # SRC_SYS_CD_SK
        "UNK",  # PAYMT_RDUCTN_REF_ID
        0,  # PAYMT_RDUCTN_SUBTYP_CD_SK
        0,  # CRT_RUN_CYC_EXCTN_SK
        0,  # LAST_UPDT_RUN_CYC_EXCTN_SK
        0,  # PAYE_PROV_SK
        0,  # USER_SK
        0,  # PAYMT_RDUCTN_EXCD_SK
        0,  # PAYMT_RDUCTN_PAYE_TYP_CD_SK
        0,  # PAYMT_RDUCTN_PRM_TYP_CD_SK
        0,  # PAYMT_RDUCTN_STTUS_CD_SK
        0,  # PAYMT_RDUCTN_TYP_CD_SK
        "U",  # AUTO_PAYMT_RDUCTN_IN
        "UNK",  # CRT_DT_SK
        "UNK",  # PLN_YR_DT_SK
        0,  # ORIG_RDUCTN_AMT
        0,  # PCA_OVERPD_NET_AMT
        0,  # RCVD_AMT
        0,  # RCVRED_AMT
        0,  # REMN_NET_AMT
        0,  # WRT_OFF_AMT
        "UNK",  # RDUCTN_DESC
        "UNK"  # TAX_YR
    )
]

schema_default_unk = StructType([
    StructField("PAYMT_RDUCTN_SK", IntegerType(), False),
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("PAYMT_RDUCTN_REF_ID", StringType(), False),
    StructField("PAYMT_RDUCTN_SUBTYP_CD_SK", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("PAYE_PROV_SK", IntegerType(), False),
    StructField("USER_SK", IntegerType(), False),
    StructField("PAYMT_RDUCTN_EXCD_SK", IntegerType(), False),
    StructField("PAYMT_RDUCTN_PAYE_TYP_CD_SK", IntegerType(), False),
    StructField("PAYMT_RDUCTN_PRM_TYP_CD_SK", IntegerType(), False),
    StructField("PAYMT_RDUCTN_STTUS_CD_SK", IntegerType(), False),
    StructField("PAYMT_RDUCTN_TYP_CD_SK", IntegerType(), False),
    StructField("AUTO_PAYMT_RDUCTN_IN", StringType(), False),
    StructField("CRT_DT_SK", StringType(), False),
    StructField("PLN_YR_DT_SK", StringType(), False),
    StructField("ORIG_RDUCTN_AMT", IntegerType(), False),
    StructField("PCA_OVERPD_NET_AMT", IntegerType(), False),
    StructField("RCVD_AMT", IntegerType(), False),
    StructField("RCVRED_AMT", IntegerType(), False),
    StructField("REMN_NET_AMT", IntegerType(), False),
    StructField("WRT_OFF_AMT", IntegerType(), False),
    StructField("RDUCTN_DESC", StringType(), False),
    StructField("TAX_YR", StringType(), False)
])

df_for_DefaultUNK = spark.createDataFrame(data_default_unk, schema_default_unk)

# Build DefaultNA row (constraint: @INROWNUM = 1)
data_default_na = [
    (
        1,   # PAYMT_RDUCTN_SK
        1,   # SRC_SYS_CD_SK
        "NA",  # PAYMT_RDUCTN_REF_ID
        1,   # PAYMT_RDUCTN_SUBTYP_CD_SK
        1,   # CRT_RUN_CYC_EXCTN_SK
        1,   # LAST_UPDT_RUN_CYC_EXCTN_SK
        1,   # PAYE_PROV_SK
        1,   # USER_SK
        1,   # PAYMT_RDUCTN_EXCD_SK
        1,   # PAYMT_RDUCTN_PAYE_TYP_CD_SK
        1,   # PAYMT_RDUCTN_PRM_TYP_CD_SK
        1,   # PAYMT_RDUCTN_STTUS_CD_SK
        1,   # PAYMT_RDUCTN_TYP_CD_SK
        "X", # AUTO_PAYMT_RDUCTN_IN
        "NA", # CRT_DT_SK
        "NA", # PLN_YR_DT_SK
        0,   # ORIG_RDUCTN_AMT
        0,   # PCA_OVERPD_NET_AMT
        0,   # RCVD_AMT
        0,   # RCVRED_AMT
        0,   # REMN_NET_AMT
        0,   # WRT_OFF_AMT
        "NA", # RDUCTN_DESC
        "NA"  # TAX_YR
    )
]

schema_default_na = StructType([
    StructField("PAYMT_RDUCTN_SK", IntegerType(), False),
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("PAYMT_RDUCTN_REF_ID", StringType(), False),
    StructField("PAYMT_RDUCTN_SUBTYP_CD_SK", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("PAYE_PROV_SK", IntegerType(), False),
    StructField("USER_SK", IntegerType(), False),
    StructField("PAYMT_RDUCTN_EXCD_SK", IntegerType(), False),
    StructField("PAYMT_RDUCTN_PAYE_TYP_CD_SK", IntegerType(), False),
    StructField("PAYMT_RDUCTN_PRM_TYP_CD_SK", IntegerType(), False),
    StructField("PAYMT_RDUCTN_STTUS_CD_SK", IntegerType(), False),
    StructField("PAYMT_RDUCTN_TYP_CD_SK", IntegerType(), False),
    StructField("AUTO_PAYMT_RDUCTN_IN", StringType(), False),
    StructField("CRT_DT_SK", StringType(), False),
    StructField("PLN_YR_DT_SK", StringType(), False),
    StructField("ORIG_RDUCTN_AMT", IntegerType(), False),
    StructField("PCA_OVERPD_NET_AMT", IntegerType(), False),
    StructField("RCVD_AMT", IntegerType(), False),
    StructField("RCVRED_AMT", IntegerType(), False),
    StructField("REMN_NET_AMT", IntegerType(), False),
    StructField("WRT_OFF_AMT", IntegerType(), False),
    StructField("RDUCTN_DESC", StringType(), False),
    StructField("TAX_YR", StringType(), False)
])

df_for_DefaultNA = spark.createDataFrame(data_default_na, schema_default_na)

# Collector: Union DefaultUNK, DefaultNA, Pkey
df_collector = (
    df_for_DefaultUNK
    .unionByName(df_for_DefaultNA)
    .unionByName(df_for_pkey)
)

# Apply rpad for char/varchar with known lengths in final schema
df_collector = df_collector.withColumn(
    "AUTO_PAYMT_RDUCTN_IN",
    rpad(col("AUTO_PAYMT_RDUCTN_IN"), 1, " ")
).withColumn(
    "CRT_DT_SK",
    rpad(col("CRT_DT_SK"), 10, " ")
).withColumn(
    "PLN_YR_DT_SK",
    rpad(col("PLN_YR_DT_SK"), 10, " ")
).withColumn(
    "TAX_YR",
    rpad(col("TAX_YR"), 4, " ")
)

# Final select for PAYMT_RDUCTN (CSeqFileStage)
df_final = df_collector.select(
    "PAYMT_RDUCTN_SK",
    "SRC_SYS_CD_SK",
    "PAYMT_RDUCTN_REF_ID",
    "PAYMT_RDUCTN_SUBTYP_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PAYE_PROV_SK",
    "USER_SK",
    "PAYMT_RDUCTN_EXCD_SK",
    "PAYMT_RDUCTN_PAYE_TYP_CD_SK",
    "PAYMT_RDUCTN_PRM_TYP_CD_SK",
    "PAYMT_RDUCTN_STTUS_CD_SK",
    "PAYMT_RDUCTN_TYP_CD_SK",
    "AUTO_PAYMT_RDUCTN_IN",
    "CRT_DT_SK",
    "PLN_YR_DT_SK",
    "ORIG_RDUCTN_AMT",
    "PCA_OVERPD_NET_AMT",
    "RCVD_AMT",
    "RCVRED_AMT",
    "REMN_NET_AMT",
    "WRT_OFF_AMT",
    "RDUCTN_DESC",
    "TAX_YR"
)

# Write PAYMT_RDUCTN.#Source#.dat
write_files(
    df_final,
    f"{adls_path}/load/PAYMT_RDUCTN.{Source}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)