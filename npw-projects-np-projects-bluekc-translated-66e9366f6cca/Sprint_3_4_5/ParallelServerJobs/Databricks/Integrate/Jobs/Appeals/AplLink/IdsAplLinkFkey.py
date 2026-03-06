# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC ***************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Takes the primary key file and applies the foreign keys.   
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer             Date                 Change Description                                                                          Project #           Development Project     Code Reviewer          Date Reviewed  
# MAGIC ------------------           ---------------------    -----------------------------------------------------------------------------------------------------    ----------------         ----------------------------------    ----------------------------      ----------------
# MAGIC Bhoomi Dasari      07/11/2007      Initial program                                                                                   3028                 devlIDS30                      Steph Goddard          8/23/07
# MAGIC Jag Yelavarthi      2015-06-17        Changed CLM_SK lookup from Hash file lookup to a                      TFS#1040        IntegrateDev1                Kalyan Neelam           2015-06-23
# MAGIC                                                        Direct database lookup to K_CLM table. Reason for the 
# MAGIC                                                        change is that hf_clm does not hold full contents of
# MAGIC                                                        Claim records needed for reference lookup.
# MAGIC Ravi Singh           2018-10- 11        Updated stage variables for 3rd party Vendor                                MTM-5841        IntegrateDev2	              Abhiram Dasarathy	2018-10-30
# MAGIC                                                        Evicore(MEDSLTNS) data to load to IDS
# MAGIC Ravi Singh           2018-10- 31        Updated stage variables for 3rd party Vendor                                MTM-5841        IntegrateDev2	              Kalyan Neelam          2018-11-12 
# MAGIC                                                        TELLIGEN data to load to IDS
# MAGIC Ravi Singh           2018-11- 28        Updated stage variables 'svClm' to popullate CLM_SK                  MTM-5841        IntegrateDev2                 Hugh Sisson             2018-11-30
# MAGIC                                                        for TELLIGEN data
# MAGIC 
# MAGIC Ravi Singh          2018-11- 30         Updated and validated code for 3rd party Vendor                         MTM-5841       IntegrateDev2              Kalyan Neelam             2018-12-10   
# MAGIC                                                        New Direction data to load to IDS

# MAGIC Set all foreign surrogate keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC CLM_ID lookup is done against K_CLM table. This acts as a direct database lookup to see if a CLM_SK is available for the incoming CLM_ID.
# MAGIC Read common record format file from extract job.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType,
    DecimalType
)
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Parameters
IDSOwner = get_widget_value('IDSOwner', '')
ids_secret_name = get_widget_value('ids_secret_name', '')
Logging = get_widget_value('Logging', '')
InFile = get_widget_value('InFile', '')

# ----------------------------------------------------------------------------
# Read from IdsAplLinkExtr (CSeqFileStage)
# ----------------------------------------------------------------------------
schema_IdsAplLinkExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38, 10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("APL_LINK_SK", IntegerType(), False),
    StructField("APL_ID", StringType(), False),
    StructField("APL_LINK_TYP_CD_SK", IntegerType(), False),
    StructField("APL_LINK_ID", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("APL_SK", IntegerType(), False),
    StructField("CASE_MGT_SK", IntegerType(), False),
    StructField("CLM_SK", IntegerType(), False),
    StructField("CUST_SVC_SK", IntegerType(), False),
    StructField("LAST_UPDT_USER_SK", IntegerType(), False),
    StructField("REL_APL_SK", IntegerType(), False),
    StructField("UM_SK", IntegerType(), False),
    StructField("APL_LINK_RSN_CD_SK", IntegerType(), False),
    StructField("LAST_UPDT_DTM", TimestampType(), False),
    StructField("APL_LINK_DESC", StringType(), True)
])

df_IdsAplLinkExtr = (
    spark.read
    .options(header="false", sep=",", quote='"')
    .schema(schema_IdsAplLinkExtr)
    .csv(f"{adls_path}/key/{InFile}")
)

# ----------------------------------------------------------------------------
# Read from K_CLM_Lkp (DB2Connector) - for lookup
# We read the entire table and apply the join logic in the Transformer
# ----------------------------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT SRC_SYS_CD_SK, CLM_ID, CLM_SK FROM {IDSOwner}.K_CLM"
df_K_CLM_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# ----------------------------------------------------------------------------
# ForeignKey (CTransformerStage)
# Join df_IdsAplLinkExtr (alias "Key") with df_K_CLM_Lkp (alias "Lnk_KClm_Lkp")
# using provided left-join conditions:
#   Lnk_KClm_Lkp.SRC_SYS_CD_SK = 1581
#   Lnk_KClm_Lkp.CLM_ID = Key.CLM_SK
# ----------------------------------------------------------------------------
df_joined = df_IdsAplLinkExtr.alias("Key").join(
    df_K_CLM_Lkp.alias("Lnk_KClm_Lkp"),
    ((F.col("Lnk_KClm_Lkp.SRC_SYS_CD_SK") == F.lit(1581)) &
     (F.col("Lnk_KClm_Lkp.CLM_ID") == F.col("Key.CLM_SK"))),
    "left"
)

# Add all columns from Key needed downstream so we can select them later
df_joined = (
    df_joined
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.col("Key.JOB_EXCTN_RCRD_ERR_SK"))
    .withColumn("INSRT_UPDT_CD", F.col("Key.INSRT_UPDT_CD"))
    .withColumn("DISCARD_IN", F.col("Key.DISCARD_IN"))
    .withColumn("PASS_THRU_IN", F.col("Key.PASS_THRU_IN"))
    .withColumn("FIRST_RECYC_DT", F.col("Key.FIRST_RECYC_DT"))
    .withColumn("ERR_CT", F.col("Key.ERR_CT"))
    .withColumn("RECYCLE_CT", F.col("Key.RECYCLE_CT"))
    .withColumn("SRC_SYS_CD", F.col("Key.SRC_SYS_CD"))
    .withColumn("PRI_KEY_STRING", F.col("Key.PRI_KEY_STRING"))
    .withColumn("KeyAPL_LINK_SK", F.col("Key.APL_LINK_SK"))
    .withColumn("KeyAPL_ID", F.col("Key.APL_ID"))
    .withColumn("KeyAPL_LINK_TYP_CD_SK", F.col("Key.APL_LINK_TYP_CD_SK"))
    .withColumn("KeyAPL_LINK_ID", F.col("Key.APL_LINK_ID"))
    .withColumn("KeyCRT_RUN_CYC_EXCTN_SK", F.col("Key.CRT_RUN_CYC_EXCTN_SK"))
    .withColumn("KeyLAST_UPDT_RUN_CYC_EXCTN_SK", F.col("Key.LAST_UPDT_RUN_CYC_EXCTN_SK"))
    .withColumn("KeyAPL_SK", F.col("Key.APL_SK"))
    .withColumn("KeyCASE_MGT_SK", F.col("Key.CASE_MGT_SK"))
    .withColumn("KeyCLM_SK", F.col("Key.CLM_SK"))
    .withColumn("KeyCUST_SVC_SK", F.col("Key.CUST_SVC_SK"))
    .withColumn("KeyLAST_UPDT_USER_SK", F.col("Key.LAST_UPDT_USER_SK"))
    .withColumn("KeyREL_APL_SK", F.col("Key.REL_APL_SK"))
    .withColumn("KeyUM_SK", F.col("Key.UM_SK"))
    .withColumn("KeyAPL_LINK_RSN_CD_SK", F.col("Key.APL_LINK_RSN_CD_SK"))
    .withColumn("KeyLAST_UPDT_DTM", F.col("Key.LAST_UPDT_DTM"))
    .withColumn("KeyAPL_LINK_DESC", F.col("Key.APL_LINK_DESC"))
)

# Create stage variables
df_enriched = (
    df_joined
    .withColumn(
        "SrcSysCdSk",
        GetFkeyCodes(
            F.lit("IDS"),
            F.col("KeyAPL_LINK_SK"),
            F.lit("SOURCE SYSTEM"),
            F.col("SRC_SYS_CD"),
            F.lit(Logging)
        )
    )
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn(
        "svAplSk",
        GetFkeyApl(
            F.col("SRC_SYS_CD"),
            F.col("KeyAPL_LINK_SK"),
            F.col("KeyAPL_SK"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "svLstUpdtUsrSk",
        F.when(
            (F.col("SRC_SYS_CD") == "MEDSLTNS") | (F.col("SRC_SYS_CD") == "NDBH"),
            F.col("KeyLAST_UPDT_USER_SK")
        ).otherwise(
            GetFkeyAppUsr(
                F.col("SRC_SYS_CD"),
                F.col("KeyAPL_LINK_SK"),
                F.col("KeyLAST_UPDT_USER_SK"),
                F.lit(Logging)
            )
        )
    )
    .withColumn(
        "svAplLnkTypCdSk",
        GetFkeyCodes(
            F.col("SRC_SYS_CD"),
            F.col("KeyAPL_LINK_SK"),
            F.lit("APPEAL LINK TYPE"),
            F.col("KeyAPL_LINK_TYP_CD_SK"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "svAplLnkRsnCdSk",
        GetFkeyCodes(
            F.col("SRC_SYS_CD"),
            F.col("KeyAPL_LINK_SK"),
            F.lit("APPEAL LINK REASON"),
            F.col("KeyAPL_LINK_RSN_CD_SK"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "svCsMgmt",
        F.when(
            F.col("SRC_SYS_CD") == "MEDSLTNS",
            F.col("KeyCASE_MGT_SK")
        ).otherwise(
            GetFkeyCaseMgt(
                F.col("SRC_SYS_CD"),
                F.col("KeyAPL_LINK_SK"),
                F.col("KeyCASE_MGT_SK"),
                F.lit(Logging)
            )
        )
    )
    .withColumn(
        "svClm",
        F.when(
            F.col("SRC_SYS_CD") == "TELLIGEN",
            F.col("KeyCLM_SK")
        ).otherwise(
            F.when(
                F.col("KeyCLM_SK") == "NA",
                F.lit(1)
            ).otherwise(
                F.when(
                    F.col("Lnk_KClm_Lkp.CLM_SK").isNull(),
                    F.lit(0)
                ).otherwise(
                    F.col("Lnk_KClm_Lkp.CLM_SK")
                )
            )
        )
    )
    .withColumn(
        "svCusSvc",
        GetFkeyCustSvc(
            F.col("SRC_SYS_CD"),
            F.col("KeyAPL_LINK_SK"),
            F.col("KeyCUST_SVC_SK"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "svRelApl",
        F.when(
            (F.col("SRC_SYS_CD") == "MEDSLTNS") | (F.col("SRC_SYS_CD") == "NDBH"),
            F.col("KeyREL_APL_SK")
        ).otherwise(
            GetFkeyApl(
                F.col("SRC_SYS_CD"),
                F.col("KeyAPL_LINK_SK"),
                F.col("KeyREL_APL_SK"),
                F.lit(Logging)
            )
        )
    )
    .withColumn(
        "svUm",
        F.when(
            F.col("SRC_SYS_CD") == "MEDSLTNS",
            F.col("KeyUM_SK")
        ).otherwise(
            GetFkeyUm(
                F.col("SRC_SYS_CD"),
                F.col("KeyAPL_LINK_SK"),
                F.col("KeyUM_SK"),
                F.lit(Logging)
            )
        )
    )
    .withColumn(
        "ErrCount",
        GetFkeyErrorCnt(
            F.col("KeyAPL_LINK_SK")
        )
    )
)

# Prepare final columns for the "Fkey" link constraint: ErrCount=0 OR PassThru='Y'
# Map them to the required names:
df_enriched = (
    df_enriched
    .withColumn("APL_LINK_SK", F.col("KeyAPL_LINK_SK"))
    .withColumn("SRC_SYS_CD_SK", F.col("SrcSysCdSk"))
    .withColumn("APL_ID", F.col("KeyAPL_ID"))
    .withColumn("APL_LINK_TYP_CD_SK", F.col("svAplLnkTypCdSk"))
    .withColumn("APL_LINK_ID", F.col("KeyAPL_LINK_ID"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.col("KeyCRT_RUN_CYC_EXCTN_SK"))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.col("KeyLAST_UPDT_RUN_CYC_EXCTN_SK"))
    .withColumn("APL_SK", F.col("svAplSk"))
    .withColumn("CASE_MGT_SK", F.col("svCsMgmt"))
    .withColumn("CLM_SK", F.col("svClm"))
    .withColumn("CUST_SVC_SK", F.col("svCusSvc"))
    .withColumn("LAST_UPDT_USER_SK", F.col("svLstUpdtUsrSk"))
    .withColumn("REL_APL_SK", F.col("svRelApl"))
    .withColumn("UM_SK", F.col("svUm"))
    .withColumn("APL_LINK_RSN_CD_SK", F.col("svAplLnkRsnCdSk"))
    .withColumn("LAST_UPDT_DTM", F.col("KeyLAST_UPDT_DTM"))
    .withColumn("APL_LINK_DESC", F.col("KeyAPL_LINK_DESC"))
)

df_fkey = df_enriched.filter(
    (F.col("ErrCount") == 0) | (F.col("PassThru") == "Y")
).select(
    "APL_LINK_SK",
    "SRC_SYS_CD_SK",
    "APL_ID",
    "APL_LINK_TYP_CD_SK",
    "APL_LINK_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "APL_SK",
    "CASE_MGT_SK",
    "CLM_SK",
    "CUST_SVC_SK",
    "LAST_UPDT_USER_SK",
    "REL_APL_SK",
    "UM_SK",
    "APL_LINK_RSN_CD_SK",
    "LAST_UPDT_DTM",
    "APL_LINK_DESC"
)

# ----------------------------------------------------------------------------
# Recycle link ("Recycle"): ErrCount > 0 => goes to hf_recycle (scenario C)
# ----------------------------------------------------------------------------
df_recycle = df_enriched.filter(F.col("ErrCount") > 0).select(
    GetRecycleKey(F.col("APL_LINK_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ErrCount").alias("ERR_CT"),
    (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("APL_LINK_SK").alias("APL_LINK_SK"),
    F.col("APL_ID").alias("APL_ID"),
    F.col("APL_LINK_TYP_CD_SK").alias("APL_LINK_TYP_CD_SK"),
    F.col("APL_LINK_ID").alias("APL_LINK_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("APL_SK").alias("APL_SK"),
    F.col("CASE_MGT_SK").alias("CASE_MGT_SK"),
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("CUST_SVC_SK").alias("CUST_SVC_SK"),
    F.col("LAST_UPDT_USER_SK").alias("LAST_UPDT_USER_SK"),
    F.col("REL_APL_SK").alias("REL_APL_SK"),
    F.col("UM_SK").alias("UM_SK"),
    F.col("APL_LINK_RSN_CD_SK").alias("APL_LINK_RSN_CD_SK"),
    F.col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    F.col("APL_LINK_DESC").alias("APL_LINK_DESC")
)

# Apply rpad for char/varchar columns in df_recycle
df_recycle = (
    df_recycle
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), 255, " "))
    .withColumn("PRI_KEY_STRING", F.rpad(F.col("PRI_KEY_STRING"), 255, " "))
    .withColumn("APL_ID", F.rpad(F.col("APL_ID"), 255, " "))
    .withColumn("APL_LINK_ID", F.rpad(F.col("APL_LINK_ID"), 255, " "))
    .withColumn("APL_LINK_DESC", F.rpad(F.col("APL_LINK_DESC"), 255, " "))
)

# Write the recycle output to Parquet (scenario C)
write_files(
    df_recycle,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

# ----------------------------------------------------------------------------
# DefaultUNK link ("@INROWNUM = 1")
# ----------------------------------------------------------------------------
df_first_unk = df_enriched.limit(1)
df_DefaultUNK = df_first_unk.select(
    F.lit(0).alias("APL_LINK_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit("UNK").alias("APL_ID"),
    F.lit(0).alias("APL_LINK_TYP_CD_SK"),
    F.lit("UNK").alias("APL_LINK_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("APL_SK"),
    F.lit(0).alias("CASE_MGT_SK"),
    F.lit(0).alias("CLM_SK"),
    F.lit(0).alias("CUST_SVC_SK"),
    F.lit(0).alias("LAST_UPDT_USER_SK"),
    F.lit(0).alias("REL_APL_SK"),
    F.lit(0).alias("UM_SK"),
    F.lit(0).alias("APL_LINK_RSN_CD_SK"),
    F.to_timestamp(F.lit("1753-01-01"), "yyyy-MM-dd").alias("LAST_UPDT_DTM"),
    F.lit("UNK").alias("APL_LINK_DESC")
)

# ----------------------------------------------------------------------------
# DefaultNA link ("@INROWNUM = 1")
# ----------------------------------------------------------------------------
df_first_na = df_enriched.limit(1)
df_DefaultNA = df_first_na.select(
    F.lit(1).alias("APL_LINK_SK"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit("NA").alias("APL_ID"),
    F.lit(1).alias("APL_LINK_TYP_CD_SK"),
    F.lit("NA").alias("APL_LINK_ID"),
    F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("APL_SK"),
    F.lit(1).alias("CASE_MGT_SK"),
    F.lit(1).alias("CLM_SK"),
    F.lit(1).alias("CUST_SVC_SK"),
    F.lit(1).alias("LAST_UPDT_USER_SK"),
    F.lit(1).alias("REL_APL_SK"),
    F.lit(1).alias("UM_SK"),
    F.lit(1).alias("APL_LINK_RSN_CD_SK"),
    F.to_timestamp(F.lit("1753-01-01"), "yyyy-MM-dd").alias("LAST_UPDT_DTM"),
    F.lit("NA").alias("APL_LINK_DESC")
)

# ----------------------------------------------------------------------------
# Collector (CCollector) - Union of Fkey, DefaultUNK, DefaultNA
# ----------------------------------------------------------------------------
df_collector = (
    df_fkey
    .unionByName(df_DefaultUNK)
    .unionByName(df_DefaultNA)
)

# Apply rpad for char/varchar columns in the final collector
df_collector = (
    df_collector
    .withColumn("APL_ID", F.rpad(F.col("APL_ID"), 255, " "))
    .withColumn("APL_LINK_ID", F.rpad(F.col("APL_LINK_ID"), 255, " "))
    .withColumn("APL_LINK_DESC", F.rpad(F.col("APL_LINK_DESC"), 255, " "))
)

df_collector = df_collector.select(
    "APL_LINK_SK",
    "SRC_SYS_CD_SK",
    "APL_ID",
    "APL_LINK_TYP_CD_SK",
    "APL_LINK_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "APL_SK",
    "CASE_MGT_SK",
    "CLM_SK",
    "CUST_SVC_SK",
    "LAST_UPDT_USER_SK",
    "REL_APL_SK",
    "UM_SK",
    "APL_LINK_RSN_CD_SK",
    "LAST_UPDT_DTM",
    "APL_LINK_DESC"
)

# ----------------------------------------------------------------------------
# APL_LINK (CSeqFileStage) - write to delimited file APL_LINK.dat
#   Directory is "load", so path -> f"{adls_path}/load/APL_LINK.dat"
# ----------------------------------------------------------------------------
write_files(
    df_collector,
    f"{adls_path}/load/APL_LINK.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)