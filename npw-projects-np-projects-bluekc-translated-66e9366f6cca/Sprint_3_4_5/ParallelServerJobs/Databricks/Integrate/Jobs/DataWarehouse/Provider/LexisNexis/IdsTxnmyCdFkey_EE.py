# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC 
# MAGIC Ravi Abburi            2017-11-30              5781                             Original Programming                                                                              IntegrateDev2         Kalyan Neelam             2018-01-31

# MAGIC IdsTxnmyCdFkey_EE
# MAGIC FKEY failures are written into this flat file.
# MAGIC This is a load ready file that will go into Load job
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
    DateType
)
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# PARAMETERS
SrcSysCd = get_widget_value('SrcSysCd','LEXISNEXIS')
IDSRunCycle = get_widget_value('IDSRunCycle','100')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunID = get_widget_value('RunID','100')
PrefixFkeyFailedFileName = get_widget_value('PrefixFkeyFailedFileName','FKeysFailure.IdsTxnmyCdCntl')
JobName = "IdsTxnmyCdFkey_EE"

# STAGE: seq_TXNMY_CD_Pkey (PxSequentialFile) - READ
schema_seq_TXNMY_CD_Pkey = StructType([
    StructField("TXNMY_CD_SK", IntegerType(), nullable=False),
    StructField("TXNMY_CD", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=True),
    StructField("EFF_DT", DateType(), nullable=True),
    StructField("DCTVTN_DT", DateType(), nullable=True),
    StructField("LAST_UPDT_DT", DateType(), nullable=True),
    StructField("TXNMY_DESC", StringType(), nullable=True),
    StructField("TXNMY_PROV_TYP_CD", StringType(), nullable=True),
    StructField("TXNMY_PROV_TYP_DESC", StringType(), nullable=True),
    StructField("TXNMY_CLS_CD", StringType(), nullable=True),
    StructField("TXNMY_CLS_DESC", StringType(), nullable=True),
    StructField("TXNMY_SPCLIZATION_CD", StringType(), nullable=True),
    StructField("TXNMY_SPCLIZATION_DESC", StringType(), nullable=True),
    StructField("CRT_SRC_SYS_CD", StringType(), nullable=True),
    StructField("LAST_UPDT_SRC_SYS_CD", StringType(), nullable=True),
    StructField("PROV_SPEC_CD", StringType(), nullable=True),
    StructField("PROV_FCLTY_TYP_CD", StringType(), nullable=True)
])
file_path_seq_TXNMY_CD_Pkey = f"{adls_path}/key/TXNMY_CD.{SrcSysCd}.pkey.{RunID}.dat"
df_seq_TXNMY_CD_Pkey = (
    spark.read
    .option("delimiter", ",")
    .option("quote", "^")
    .option("header", "false")
    .option("nullValue", None)
    .schema(schema_seq_TXNMY_CD_Pkey)
    .csv(file_path_seq_TXNMY_CD_Pkey)
)

# STAGE: db2_k_prov_spec_cd_Lkp (DB2ConnectorPX) - READ
extract_query_db2_k_prov_spec_cd_Lkp = f"SELECT PROV_SPEC_CD, PROV_SPEC_CD_SK FROM {IDSOwner}.K_PROV_SPEC_CD"
jdbc_url_db2_k_prov_spec_cd_Lkp, jdbc_props_db2_k_prov_spec_cd_Lkp = get_db_config(ids_secret_name)
df_db2_k_prov_spec_cd_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_k_prov_spec_cd_Lkp)
    .options(**jdbc_props_db2_k_prov_spec_cd_Lkp)
    .option("query", extract_query_db2_k_prov_spec_cd_Lkp)
    .load()
)

# STAGE: db2_K_FCLTY_TYP_CD_Lkp (DB2ConnectorPX) - READ
extract_query_db2_K_FCLTY_TYP_CD_Lkp = f"SELECT FCLTY_TYP_CD as PROV_FCLTY_TYP_CD, FCLTY_TYP_CD_SK FROM {IDSOwner}.K_FCLTY_TYP_CD"
jdbc_url_db2_K_FCLTY_TYP_CD_Lkp, jdbc_props_db2_K_FCLTY_TYP_CD_Lkp = get_db_config(ids_secret_name)
df_db2_K_FCLTY_TYP_CD_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_K_FCLTY_TYP_CD_Lkp)
    .options(**jdbc_props_db2_K_FCLTY_TYP_CD_Lkp)
    .option("query", extract_query_db2_K_FCLTY_TYP_CD_Lkp)
    .load()
)

# STAGE: Lkup_Fkey (PxLookup)
df_Lkup_Fkey = (
    df_seq_TXNMY_CD_Pkey.alias("lnk_TxnmyCd_EE_Lkup_In")
    .join(
        df_db2_K_FCLTY_TYP_CD_Lkp.alias("FcltyTypCdSk_lkp"),
        F.col("lnk_TxnmyCd_EE_Lkup_In.PROV_FCLTY_TYP_CD") == F.col("FcltyTypCdSk_lkp.PROV_FCLTY_TYP_CD"),
        "left"
    )
    .join(
        df_db2_k_prov_spec_cd_Lkp.alias("ProcSpecCdSk_lkp"),
        F.col("lnk_TxnmyCd_EE_Lkup_In.PROV_SPEC_CD") == F.col("ProcSpecCdSk_lkp.PROV_SPEC_CD"),
        "left"
    )
    .select(
        F.col("lnk_TxnmyCd_EE_Lkup_In.TXNMY_CD_SK").alias("TXNMY_CD_SK"),
        F.col("lnk_TxnmyCd_EE_Lkup_In.TXNMY_CD").alias("TXNMY_CD"),
        F.col("lnk_TxnmyCd_EE_Lkup_In.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_TxnmyCd_EE_Lkup_In.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("FcltyTypCdSk_lkp.FCLTY_TYP_CD_SK").alias("FCLTY_TYP_CD_SK"),
        F.col("ProcSpecCdSk_lkp.PROV_SPEC_CD_SK").alias("PROV_SPEC_CD_SK"),
        F.col("lnk_TxnmyCd_EE_Lkup_In.EFF_DT").alias("EFF_DT"),
        F.col("lnk_TxnmyCd_EE_Lkup_In.DCTVTN_DT").alias("DCTVTN_DT"),
        F.col("lnk_TxnmyCd_EE_Lkup_In.LAST_UPDT_DT").alias("LAST_UPDT_DT"),
        F.col("lnk_TxnmyCd_EE_Lkup_In.TXNMY_DESC").alias("TXNMY_DESC"),
        F.col("lnk_TxnmyCd_EE_Lkup_In.TXNMY_PROV_TYP_CD").alias("TXNMY_PROV_TYP_CD"),
        F.col("lnk_TxnmyCd_EE_Lkup_In.TXNMY_PROV_TYP_DESC").alias("TXNMY_PROV_TYP_DESC"),
        F.col("lnk_TxnmyCd_EE_Lkup_In.TXNMY_CLS_CD").alias("TXNMY_CLS_CD"),
        F.col("lnk_TxnmyCd_EE_Lkup_In.TXNMY_CLS_DESC").alias("TXNMY_CLS_DESC"),
        F.col("lnk_TxnmyCd_EE_Lkup_In.TXNMY_SPCLIZATION_CD").alias("TXNMY_SPCLIZATION_CD"),
        F.col("lnk_TxnmyCd_EE_Lkup_In.TXNMY_SPCLIZATION_DESC").alias("TXNMY_SPCLIZATION_DESC"),
        F.col("lnk_TxnmyCd_EE_Lkup_In.CRT_SRC_SYS_CD").alias("CRT_SRC_SYS_CD"),
        F.col("lnk_TxnmyCd_EE_Lkup_In.LAST_UPDT_SRC_SYS_CD").alias("LAST_UPDT_SRC_SYS_CD"),
        F.col("lnk_TxnmyCd_EE_Lkup_In.PROV_SPEC_CD").alias("PROV_SPEC_CD"),
        F.col("lnk_TxnmyCd_EE_Lkup_In.PROV_FCLTY_TYP_CD").alias("PROV_FCLTY_TYP_CD")
    )
)

# STAGE: xfm_CheckLkpResults (CTransformerStage)
df_xfm_CheckLkpResults = (
    df_Lkup_Fkey
    .withColumn("FCLTY_TYP_CD_SK_str", F.col("FCLTY_TYP_CD_SK").cast(StringType()))
    .withColumn("PROV_SPEC_CD_SK_str", F.col("PROV_SPEC_CD_SK").cast(StringType()))
    .withColumn(
        "svFcltyTypCdSkLkupCheck",
        F.when(
            F.col("FCLTY_TYP_CD_SK").isNull() &
            (F.col("FCLTY_TYP_CD_SK_str") != F.lit("NA")), 
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "svProcSpecCdSkLkupCheck",
        F.when(
            F.col("PROV_SPEC_CD_SK").isNull() &
            (F.col("PROV_SPEC_CD_SK_str") != F.lit("NA")),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
)

# Link: Lnk_Main (no constraint besides transformations)
df_xfm_CheckLkpResults_main = (
    df_xfm_CheckLkpResults
    .withColumn(
        "FCLTY_TYP_CD_SK",
        F.when(
            F.col("PROV_FCLTY_TYP_CD").isNull() | (F.col("PROV_FCLTY_TYP_CD") == F.lit("NA")),
            F.lit(1)
        ).when(
            F.col("FCLTY_TYP_CD_SK").isNull(),
            F.lit(0)
        ).otherwise(F.col("FCLTY_TYP_CD_SK"))
    )
    .withColumn(
        "PROV_SPEC_CD_SK",
        F.when(
            F.col("PROV_SPEC_CD").isNull() | (F.col("PROV_SPEC_CD") == F.lit("NA")),
            F.lit(1)
        ).when(
            F.col("PROV_SPEC_CD_SK").isNull(),
            F.lit(0)
        ).otherwise(F.col("PROV_SPEC_CD_SK"))
    )
    .select(
        F.col("TXNMY_CD_SK"),
        F.col("TXNMY_CD"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("FCLTY_TYP_CD_SK"),
        F.col("PROV_SPEC_CD_SK"),
        F.col("EFF_DT"),
        F.col("DCTVTN_DT"),
        F.col("LAST_UPDT_DT"),
        F.col("TXNMY_DESC"),
        F.col("TXNMY_PROV_TYP_CD"),
        F.col("TXNMY_PROV_TYP_DESC"),
        F.col("TXNMY_CLS_CD"),
        F.col("TXNMY_CLS_DESC"),
        F.col("TXNMY_SPCLIZATION_CD"),
        F.col("TXNMY_SPCLIZATION_DESC"),
        F.col("CRT_SRC_SYS_CD"),
        F.col("LAST_UPDT_SRC_SYS_CD")
    )
)

# Link: lnk_FcltyTypCdSkLkupFail (constraint = svFcltyTypCdSkLkupCheck = 'Y')
df_xfm_CheckLkpResults_fcltyFail = (
    df_xfm_CheckLkpResults
    .filter(F.col("svFcltyTypCdSkLkupCheck") == "Y")
    .select(
        F.col("TXNMY_CD_SK").alias("PRI_SK"),
        F.col("TXNMY_CD").alias("PRI_NAT_KEY_STRING"),
        F.col("CRT_SRC_SYS_CD").alias("SRC_SYS_CD_SK"),
        F.lit(JobName).alias("JOB_NM"),
        F.lit("FKLOOKUP").alias("ERROR_TYP"),
        F.lit("CD_MPPNG").alias("PHYSCL_FILE_NM"),
        F.concat(
            F.lit(SrcSysCd),
            F.lit(";"),
            F.lit("LEXISNEXIS;IDS;TXNMY_CD;TXNMY_CD;"),
            F.col("TXNMY_CD")
        ).alias("FRGN_NAT_KEY_STRING"),
        F.col("TXNMY_CD").alias("FIRST_RECYC_TS"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
    )
)

# Link: lnk_ProcSpecCdSkLkupFail (constraint = svProcSpecCdSkLkupCheck = 'Y')
df_xfm_CheckLkpResults_procspecFail = (
    df_xfm_CheckLkpResults
    .filter(F.col("svProcSpecCdSkLkupCheck") == "Y")
    .select(
        F.col("TXNMY_CD_SK").alias("PRI_SK"),
        F.col("TXNMY_CD").alias("PRI_NAT_KEY_STRING"),
        F.lit(SrcSysCd).alias("SRC_SYS_CD_SK"),
        F.lit(JobName).alias("JOB_NM"),
        F.lit("FKLOOKUP").alias("ERROR_TYP"),
        F.lit("PROV_SPEC_CD").alias("PHYSCL_FILE_NM"),
        F.concat(
            F.lit(SrcSysCd),
            F.lit(";"),
            F.col("PROV_SPEC_CD")
        ).alias("FRGN_NAT_KEY_STRING"),
        F.col("PROV_SPEC_CD").alias("FIRST_RECYC_TS"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
    )
)

# Link: Lnk_UNK (constraint = ((@INROWNUM-1)*@NUMPARTITIONS+@PARTITIONNUM+1)=1) => replicate by limit(1) 
df_xfm_CheckLkpResults_lnk_unk_source = df_xfm_CheckLkpResults.limit(1)
df_xfm_CheckLkpResults_lnk_unk = (
    df_xfm_CheckLkpResults_lnk_unk_source
    .select(
        F.lit(0).alias("TXNMY_CD_SK"),
        F.lit("UNK").alias("TXNMY_CD"),
        F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("FCLTY_TYP_CD_SK"),
        F.lit(0).alias("PROV_SPEC_CD_SK"),
        F.lit("1753-01-01").alias("EFF_DT"),
        F.lit("2199-12-31").alias("DCTVTN_DT"),
        F.lit("2199-12-31").alias("LAST_UPDT_DT"),
        F.lit("UNK").alias("TXNMY_DESC"),
        F.lit("UNK").alias("TXNMY_PROV_TYP_CD"),
        F.lit("UNK").alias("TXNMY_PROV_TYP_DESC"),
        F.lit("UNK").alias("TXNMY_CLS_CD"),
        F.lit("UNK").alias("TXNMY_CLS_DESC"),
        F.lit("UNK").alias("TXNMY_SPCLIZATION_CD"),
        F.lit("UNK").alias("TXNMY_SPCLIZATION_DESC"),
        F.lit(SrcSysCd).alias("CRT_SRC_SYS_CD"),
        F.lit(SrcSysCd).alias("LAST_UPDT_SRC_SYS_CD")
    )
)

# Link: Lnk_NA (constraint = ((@INROWNUM-1)*@NUMPARTITIONS+@PARTITIONNUM+1)=1) => replicate by limit(1)
df_xfm_CheckLkpResults_lnk_na_source = df_xfm_CheckLkpResults.limit(1)
df_xfm_CheckLkpResults_lnk_na = (
    df_xfm_CheckLkpResults_lnk_na_source
    .select(
        F.lit(1).alias("TXNMY_CD_SK"),
        F.lit("NA").alias("TXNMY_CD"),
        F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("FCLTY_TYP_CD_SK"),
        F.lit(1).alias("PROV_SPEC_CD_SK"),
        F.lit("1753-01-01").alias("EFF_DT"),
        F.lit("2199-12-31").alias("DCTVTN_DT"),
        F.lit("2199-12-31").alias("LAST_UPDT_DT"),
        F.lit("NA").alias("TXNMY_DESC"),
        F.lit("NA").alias("TXNMY_PROV_TYP_CD"),
        F.lit("NA").alias("TXNMY_PROV_TYP_DESC"),
        F.lit("NA").alias("TXNMY_CLS_CD"),
        F.lit("NA").alias("TXNMY_CLS_DESC"),
        F.lit("NA").alias("TXNMY_SPCLIZATION_CD"),
        F.lit("NA").alias("TXNMY_SPCLIZATION_DESC"),
        F.lit(SrcSysCd).alias("CRT_SRC_SYS_CD"),
        F.lit(SrcSysCd).alias("LAST_UPDT_SRC_SYS_CD")
    )
)

# STAGE: fnl_NA_UNK_Streams (PxFunnel)
df_fnl_NA_UNK_Streams = (
    df_xfm_CheckLkpResults_main
    .unionByName(df_xfm_CheckLkpResults_lnk_unk)
    .unionByName(df_xfm_CheckLkpResults_lnk_na)
)

# OUTPUT: seq_TXNMY_CD_FKey (PxSequentialFile) - WRITE
# Apply rpad for varchar columns (length unknown => use <...>)
df_seq_TXNMY_CD_FKey = (
    df_fnl_NA_UNK_Streams
    .withColumn("TXNMY_CD", F.rpad(F.col("TXNMY_CD"), <...>, " "))
    .withColumn("TXNMY_DESC", F.rpad(F.col("TXNMY_DESC"), <...>, " "))
    .withColumn("TXNMY_PROV_TYP_CD", F.rpad(F.col("TXNMY_PROV_TYP_CD"), <...>, " "))
    .withColumn("TXNMY_PROV_TYP_DESC", F.rpad(F.col("TXNMY_PROV_TYP_DESC"), <...>, " "))
    .withColumn("TXNMY_CLS_CD", F.rpad(F.col("TXNMY_CLS_CD"), <...>, " "))
    .withColumn("TXNMY_CLS_DESC", F.rpad(F.col("TXNMY_CLS_DESC"), <...>, " "))
    .withColumn("TXNMY_SPCLIZATION_CD", F.rpad(F.col("TXNMY_SPCLIZATION_CD"), <...>, " "))
    .withColumn("TXNMY_SPCLIZATION_DESC", F.rpad(F.col("TXNMY_SPCLIZATION_DESC"), <...>, " "))
    .withColumn("CRT_SRC_SYS_CD", F.rpad(F.col("CRT_SRC_SYS_CD"), <...>, " "))
    .withColumn("LAST_UPDT_SRC_SYS_CD", F.rpad(F.col("LAST_UPDT_SRC_SYS_CD"), <...>, " "))
    .select(
        "TXNMY_CD_SK",
        "TXNMY_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "FCLTY_TYP_CD_SK",
        "PROV_SPEC_CD_SK",
        "EFF_DT",
        "DCTVTN_DT",
        "LAST_UPDT_DT",
        "TXNMY_DESC",
        "TXNMY_PROV_TYP_CD",
        "TXNMY_PROV_TYP_DESC",
        "TXNMY_CLS_CD",
        "TXNMY_CLS_DESC",
        "TXNMY_SPCLIZATION_CD",
        "TXNMY_SPCLIZATION_DESC",
        "CRT_SRC_SYS_CD",
        "LAST_UPDT_SRC_SYS_CD"
    )
)
write_files(
    df_seq_TXNMY_CD_FKey,
    f"{adls_path}/load/TXNMY_CD.{SrcSysCd}.{RunID}.dat",
    ",",
    "overwrite",
    False,
    False,
    "^",
    None
)

# STAGE: Fnl_LkpFail (PxFunnel)
df_fnl_lkp_fail = (
    df_xfm_CheckLkpResults_fcltyFail
    .unionByName(df_xfm_CheckLkpResults_procspecFail)
)

# OUTPUT: Seq_FKeyFailedFile (PxSequentialFile) - WRITE
# Determine which columns might be string or not; apply rpad for suspected string columns using <...>
df_fnl_lkp_fail_write = (
    df_fnl_lkp_fail
    .withColumn("PRI_NAT_KEY_STRING", F.rpad(F.col("PRI_NAT_KEY_STRING"), <...>, " "))
    .withColumn("SRC_SYS_CD_SK", F.rpad(F.col("SRC_SYS_CD_SK"), <...>, " "))
    .withColumn("JOB_NM", F.rpad(F.col("JOB_NM"), <...>, " "))
    .withColumn("ERROR_TYP", F.rpad(F.col("ERROR_TYP"), <...>, " "))
    .withColumn("PHYSCL_FILE_NM", F.rpad(F.col("PHYSCL_FILE_NM"), <...>, " "))
    .withColumn("FRGN_NAT_KEY_STRING", F.rpad(F.col("FRGN_NAT_KEY_STRING"), <...>, " "))
    .withColumn("FIRST_RECYC_TS", F.rpad(F.col("FIRST_RECYC_TS"), <...>, " "))
    # PRI_SK, JOB_EXCTN_SK might be numeric, leaving as-is
    .select(
        "PRI_SK",
        "PRI_NAT_KEY_STRING",
        "SRC_SYS_CD_SK",
        "JOB_NM",
        "ERROR_TYP",
        "PHYSCL_FILE_NM",
        "FRGN_NAT_KEY_STRING",
        "FIRST_RECYC_TS",
        "JOB_EXCTN_SK"
    )
)
write_files(
    df_fnl_lkp_fail_write,
    f"{adls_path}/fkey_failures/{PrefixFkeyFailedFileName}.{JobName}.dat",
    ",",
    "overwrite",
    False,
    False,
    "^",
    None
)