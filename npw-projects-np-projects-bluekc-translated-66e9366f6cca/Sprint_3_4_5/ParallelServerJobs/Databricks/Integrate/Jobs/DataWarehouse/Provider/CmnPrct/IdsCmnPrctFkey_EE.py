# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC 
# MAGIC Pooja sunkara          2014-08-15              5345                             Original Programming                                                                        IntegrateWrhsDevl      Kalyan Neelam             2015-01-07
# MAGIC 
# MAGIC Kshema H K             2023-10-13             us 598277              Added  CULT_CPBLTY_NM (VarChar 50 Nullable) field                            IntegrateDevB          Jeyaprasanna               2023-10-18
# MAGIC                                                                                                                and mapped till Target

# MAGIC IdsCmnPrctFkey_EE
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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import pyspark.sql.functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


jobName = "IdsCmnPrctFkey_EE"

SrcSysCd = get_widget_value("SrcSysCd","FACETS")
IDSRunCycle = get_widget_value("IDSRunCycle","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
RunID = get_widget_value("RunID","")
PrefixFkeyFailedFileName = get_widget_value("PrefixFkeyFailedFileName","")

# -----------------------------------------------------------------------------
# Read seq_CMN_PRCT_Pkey (PxSequentialFile) from: f"{adls_path}/key/CMN_PRCT.{SrcSysCd}.pkey.{RunID}.dat"
# -----------------------------------------------------------------------------
seq_CMN_PRCT_Pkey_schema = StructType([
    StructField("PRI_NAT_KEY_STRING", StringType(), nullable=False),
    StructField("FIRST_RECYC_TS", TimestampType(), nullable=False),
    StructField("CMN_PRCT_SK", IntegerType(), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("CMN_PRCT_ID", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("SRC_SYS_CD_SK", IntegerType(), nullable=False),
    StructField("CMN_PRCT_GNDR_CD", StringType(), nullable=False),
    StructField("ACTV_IN", StringType(), nullable=False),
    StructField("BRTH_DT", StringType(), nullable=False),
    StructField("INIT_CRDTL_DT", StringType(), nullable=False),
    StructField("LAST_CRDTL_DT", StringType(), nullable=False),
    StructField("NEXT_CRDTL_DT", StringType(), nullable=False),
    StructField("FIRST_NM", StringType(), nullable=True),
    StructField("LAST_NM", StringType(), nullable=True),
    StructField("MIDINIT", StringType(), nullable=True),
    StructField("CMN_PRCT_TTL", StringType(), nullable=True),
    StructField("NTNL_PROV_ID", StringType(), nullable=False),
    StructField("SSN", StringType(), nullable=False),
    StructField("CULT_CPBLTY_NM", StringType(), nullable=True)
])

df_seq_CMN_PRCT_Pkey = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "^")
    .option("nullValue", None)
    .schema(seq_CMN_PRCT_Pkey_schema)
    .load(f"{adls_path}/key/CMN_PRCT.{SrcSysCd}.{RunID}.dat")
)

# -----------------------------------------------------------------------------
# Read db2_K_Clndr_Dt_BirthDt_Lkp (DB2ConnectorPX -> IDS)
# -----------------------------------------------------------------------------
ids_secret_name  # already set above
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_db2_K_Clndr_Dt_BirthDt_Lkp = f"""
SELECT DISTINCT
CLNDR_DT_SK
FROM {IDSOwner}.CLNDR_DT
WHERE CLNDR_DT_SK NOT IN ('INVALID','NA','NULL','UNK')
""".strip()

df_db2_K_Clndr_Dt_BirthDt_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_K_Clndr_Dt_BirthDt_Lkp)
    .load()
)

# -----------------------------------------------------------------------------
# Read db2_K_Clndr_Dt_InitCrtl_Lkp (DB2ConnectorPX -> IDS)
# -----------------------------------------------------------------------------
extract_query_db2_K_Clndr_Dt_InitCrtl_Lkp = f"""
SELECT DISTINCT
CLNDR_DT_SK
FROM {IDSOwner}.CLNDR_DT
WHERE CLNDR_DT_SK NOT IN ('INVALID','NA','NULL','UNK')
""".strip()

df_db2_K_Clndr_Dt_InitCrtl_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_K_Clndr_Dt_InitCrtl_Lkp)
    .load()
)

# -----------------------------------------------------------------------------
# Read db2_K_Clndr_Dt_LastCrdtl_Lkp (DB2ConnectorPX -> IDS)
# -----------------------------------------------------------------------------
extract_query_db2_K_Clndr_Dt_LastCrdtl_Lkp = f"""
SELECT DISTINCT
CLNDR_DT_SK
FROM {IDSOwner}.CLNDR_DT
WHERE CLNDR_DT_SK NOT IN ('INVALID','NA','NULL','UNK')
""".strip()

df_db2_K_Clndr_Dt_LastCrdtl_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_K_Clndr_Dt_LastCrdtl_Lkp)
    .load()
)

# -----------------------------------------------------------------------------
# Read db2_K_Clndr_Dt_NextCrdtlDt_Lkp (DB2ConnectorPX -> IDS)
# -----------------------------------------------------------------------------
extract_query_db2_K_Clndr_Dt_NextCrdtlDt_Lkp = f"""
SELECT DISTINCT
CLNDR_DT_SK
FROM {IDSOwner}.CLNDR_DT
WHERE CLNDR_DT_SK NOT IN ('INVALID','NA','NULL','UNK')
""".strip()

df_db2_K_Clndr_Dt_NextCrdtlDt_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_K_Clndr_Dt_NextCrdtlDt_Lkp)
    .load()
)

# -----------------------------------------------------------------------------
# Read ds_CD_MPPNG_LkpData (PxDataSet -> read from parquet)
# ds => translate to .parquet
# -----------------------------------------------------------------------------
df_ds_CD_MPPNG_LkpData = spark.read.parquet(f"{adls_path}/ds/CD_MPPNG.parquet")

# -----------------------------------------------------------------------------
# fltr_CdMppngData (PxFilter)
# -----------------------------------------------------------------------------
df_fltr_CdMppngData = df_ds_CD_MPPNG_LkpData.filter(
    (F.col("SRC_SYS_CD") == "FACETS")
    & (F.col("SRC_CLCTN_CD") == "FACETS DBO")
    & (F.col("TRGT_CLCTN_CD") == "IDS")
    & (F.col("SRC_DOMAIN_NM") == "GENDER")
    & (F.col("TRGT_DOMAIN_NM") == "GENDER")
).select(
    "CD_MPPNG_SK",
    "SRC_CD",
)

# -----------------------------------------------------------------------------
# Lkup_Fkey (PxLookup)
# Primary link: df_seq_CMN_PRCT_Pkey.alias("Lnk_IdsCmnPrctFkey_EE_InAbc")
# Lookup links:
#   df_fltr_CdMppngData.alias("ref_CmnPrctGndrCdSk") -> left join on CMN_PRCT_GNDR_CD = SRC_CD
#   df_db2_K_Clndr_Dt_BirthDt_Lkp.alias("Ref_BirthDtSk") -> left join on BRTH_DT = CLNDR_DT_SK
#   df_db2_K_Clndr_Dt_InitCrtl_Lkp.alias("ref_InitCrdtlDtSk") -> left join on INIT_CRDTL_DT = CLNDR_DT_SK
#   df_db2_K_Clndr_Dt_LastCrdtl_Lkp.alias("ref_LastCrdtlDtSk") -> left join on LAST_CRDTL_DT = CLNDR_DT_SK
#   df_db2_K_Clndr_Dt_NextCrdtlDt_Lkp.alias("ref_NextCrdtlDtSk") -> left join on NEXT_CRDTL_DT = CLNDR_DT_SK
# -----------------------------------------------------------------------------
df_Lkup_Fkey_joined = (
    df_seq_CMN_PRCT_Pkey.alias("Lnk_IdsCmnPrctFkey_EE_InAbc")
    .join(
        df_fltr_CdMppngData.alias("ref_CmnPrctGndrCdSk"),
        F.col("Lnk_IdsCmnPrctFkey_EE_InAbc.CMN_PRCT_GNDR_CD") == F.col("ref_CmnPrctGndrCdSk.SRC_CD"),
        "left",
    )
    .join(
        df_db2_K_Clndr_Dt_BirthDt_Lkp.alias("Ref_BirthDtSk"),
        F.col("Lnk_IdsCmnPrctFkey_EE_InAbc.BRTH_DT") == F.col("Ref_BirthDtSk.CLNDR_DT_SK"),
        "left",
    )
    .join(
        df_db2_K_Clndr_Dt_InitCrtl_Lkp.alias("ref_InitCrdtlDtSk"),
        F.col("Lnk_IdsCmnPrctFkey_EE_InAbc.INIT_CRDTL_DT") == F.col("ref_InitCrdtlDtSk.CLNDR_DT_SK"),
        "left",
    )
    .join(
        df_db2_K_Clndr_Dt_LastCrdtl_Lkp.alias("ref_LastCrdtlDtSk"),
        F.col("Lnk_IdsCmnPrctFkey_EE_InAbc.LAST_CRDTL_DT") == F.col("ref_LastCrdtlDtSk.CLNDR_DT_SK"),
        "left",
    )
    .join(
        df_db2_K_Clndr_Dt_NextCrdtlDt_Lkp.alias("ref_NextCrdtlDtSk"),
        F.col("Lnk_IdsCmnPrctFkey_EE_InAbc.NEXT_CRDTL_DT") == F.col("ref_NextCrdtlDtSk.CLNDR_DT_SK"),
        "left",
    )
)

df_Lkup_Fkey = df_Lkup_Fkey_joined.select(
    F.col("Lnk_IdsCmnPrctFkey_EE_InAbc.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("Lnk_IdsCmnPrctFkey_EE_InAbc.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("Lnk_IdsCmnPrctFkey_EE_InAbc.CMN_PRCT_SK").alias("CMN_PRCT_SK"),
    F.col("Lnk_IdsCmnPrctFkey_EE_InAbc.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Lnk_IdsCmnPrctFkey_EE_InAbc.CMN_PRCT_ID").alias("CMN_PRCT_ID"),
    F.col("Lnk_IdsCmnPrctFkey_EE_InAbc.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_IdsCmnPrctFkey_EE_InAbc.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_IdsCmnPrctFkey_EE_InAbc.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("Lnk_IdsCmnPrctFkey_EE_InAbc.CMN_PRCT_GNDR_CD").alias("CMN_PRCT_GNDR_CD"),
    F.col("Lnk_IdsCmnPrctFkey_EE_InAbc.ACTV_IN").alias("ACTV_IN"),
    F.col("Lnk_IdsCmnPrctFkey_EE_InAbc.BRTH_DT").alias("BRTH_DT"),
    F.col("Lnk_IdsCmnPrctFkey_EE_InAbc.INIT_CRDTL_DT").alias("INIT_CRDTL_DT"),
    F.col("Lnk_IdsCmnPrctFkey_EE_InAbc.LAST_CRDTL_DT").alias("LAST_CRDTL_DT"),
    F.col("Lnk_IdsCmnPrctFkey_EE_InAbc.NEXT_CRDTL_DT").alias("NEXT_CRDTL_DT"),
    F.col("Lnk_IdsCmnPrctFkey_EE_InAbc.FIRST_NM").alias("FIRST_NM"),
    F.col("Lnk_IdsCmnPrctFkey_EE_InAbc.LAST_NM").alias("LAST_NM"),
    F.col("Lnk_IdsCmnPrctFkey_EE_InAbc.MIDINIT").alias("MIDINIT"),
    F.col("Lnk_IdsCmnPrctFkey_EE_InAbc.CMN_PRCT_TTL").alias("CMN_PRCT_TTL"),
    F.col("Lnk_IdsCmnPrctFkey_EE_InAbc.NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    F.col("Lnk_IdsCmnPrctFkey_EE_InAbc.SSN").alias("SSN"),
    F.col("ref_CmnPrctGndrCdSk.CD_MPPNG_SK").alias("CMN_PRCT_GNDR_CD_SK"),
    F.col("Ref_BirthDtSk.CLNDR_DT_SK").alias("BRTH_DT_SK"),
    F.col("ref_InitCrdtlDtSk.CLNDR_DT_SK").alias("INIT_CRDTL_DT_SK"),
    F.col("ref_LastCrdtlDtSk.CLNDR_DT_SK").alias("LAST_CRDTL_DT_SK"),
    F.col("ref_NextCrdtlDtSk.CLNDR_DT_SK").alias("NEXT_CRDTL_DT_SK"),
    F.col("Lnk_IdsCmnPrctFkey_EE_InAbc.CULT_CPBLTY_NM").alias("CULT_CPBLTY_NM"),
)

# -----------------------------------------------------------------------------
# xfm_CheckLkpResults (CTransformerStage)
# Stage Variables => as columns, then multiple output links
# -----------------------------------------------------------------------------
df_xfm_CheckLkpResults_stagevars = (
    df_Lkup_Fkey
    .withColumn(
        "svCmnPrctGndrCdSkLkupchk",
        F.when(
            (F.col("CMN_PRCT_GNDR_CD_SK").isNull()) & (F.col("CMN_PRCT_GNDR_CD") != F.lit("NA")),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "svBirthDtSkLkupChk",
        F.when(
            (F.col("BRTH_DT_SK").isNull()) & (trim(F.col("BRTH_DT")) != F.lit("NA")),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "svNextCrdtlDtSkLkupChk",
        F.when(
            (F.col("NEXT_CRDTL_DT_SK").isNull()) & (trim(F.col("NEXT_CRDTL_DT")) != F.lit("NA")),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "svLastCrdtlDtSkLkupChk",
        F.when(
            (F.col("LAST_CRDTL_DT_SK").isNull()) & (trim(F.col("LAST_CRDTL_DT")) != F.lit("NA")),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "svInitCrdtlDtSkLkupChk",
        F.when(
            (F.col("INIT_CRDTL_DT_SK").isNull()) & (trim(F.col("INIT_CRDTL_DT")) != F.lit("NA")),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
)

# -- Lnk_Main (no constraint, all rows)
df_Lnk_Main = df_xfm_CheckLkpResults_stagevars.select(
    F.expr("CMN_PRCT_SK as CMN_PRCT_SK"),
    F.expr("CMN_PRCT_ID as CMN_PRCT_ID"),
    F.expr("CRT_RUN_CYC_EXCTN_SK as CRT_RUN_CYC_EXCTN_SK"),
    F.expr("LAST_UPDT_RUN_CYC_EXCTN_SK as LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.expr("SRC_SYS_CD_SK as SRC_SYS_CD_SK"),
    F.when(
        (F.col("CMN_PRCT_GNDR_CD") == "NA"), F.lit(1)
    ).when(
        (F.col("CMN_PRCT_GNDR_CD") == "UNK") | (F.col("CMN_PRCT_GNDR_CD_SK").isNull()), F.lit(0)
    ).otherwise(F.col("CMN_PRCT_GNDR_CD_SK")).alias("CMN_PRCT_GNDR_CD_SK"),
    F.expr("ACTV_IN as ACTV_IN"),
    F.when(
        F.col("BRTH_DT_SK").isNull(), F.lit("1753-01-01")
    ).otherwise(F.col("BRTH_DT_SK")).alias("BRTH_DT_SK"),
    F.when(
        F.col("LAST_CRDTL_DT_SK").isNull(), F.lit("1753-01-01")
    ).otherwise(F.col("LAST_CRDTL_DT_SK")).alias("LAST_CRDTL_DT_SK"),
    F.when(
        F.col("INIT_CRDTL_DT_SK").isNull(), F.lit("1753-01-01")
    ).otherwise(F.col("INIT_CRDTL_DT_SK")).alias("INIT_CRDTL_DT_SK"),
    F.when(
        F.col("NEXT_CRDTL_DT_SK").isNull(), F.lit("2199-12-31")
    ).otherwise(F.col("NEXT_CRDTL_DT_SK")).alias("NEXT_CRDTL_DT_SK"),
    F.expr("FIRST_NM as FIRST_NM"),
    F.expr("LAST_NM as LAST_NM"),
    F.expr("MIDINIT as MIDINIT"),
    F.expr("CMN_PRCT_TTL as CMN_PRCT_TTL"),
    F.expr("NTNL_PROV_ID as NTNL_PROV_ID"),
    F.expr("SSN as SSN"),
    F.expr("CULT_CPBLTY_NM as CULT_CPBLTY_NM"),
)

# -- Lnk_UNK constraint => ((@INROWNUM - 1) * @NUMPARTITIONS + @PARTITIONNUM + 1) = 1
df_Lnk_UNK = (
    df_xfm_CheckLkpResults_stagevars
    .filter(F.monotonically_increasing_id() == 0)  # emulate "only first row"
    .select(
        F.lit(0).alias("CMN_PRCT_SK"),
        F.lit("UNK").alias("CMN_PRCT_ID"),
        F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("SRC_SYS_CD_SK"),
        F.lit(0).alias("CMN_PRCT_GNDR_CD_SK"),
        F.lit("N").alias("ACTV_IN"),
        F.lit("1753-01-01").alias("BRTH_DT_SK"),
        F.lit("1753-01-01").alias("LAST_CRDTL_DT_SK"),
        F.lit("1753-01-01").alias("INIT_CRDTL_DT_SK"),
        F.lit("1753-01-01").alias("NEXT_CRDTL_DT_SK"),
        F.lit("UNK").alias("FIRST_NM"),
        F.lit("UNK").alias("LAST_NM"),
        F.lit(None).cast(StringType()).alias("MIDINIT"),
        F.lit(None).cast(StringType()).alias("CMN_PRCT_TTL"),
        F.lit("UNK").alias("NTNL_PROV_ID"),
        F.lit("").alias("SSN"),
        F.lit("UNK").alias("CULT_CPBLTY_NM"),
    )
)

# -- Lnk_NA constraint => same single-row condition
df_Lnk_NA = (
    df_xfm_CheckLkpResults_stagevars
    .filter(F.monotonically_increasing_id() == 0)  # emulate "only first row"
    .select(
        F.lit(1).alias("CMN_PRCT_SK"),
        F.lit("NA").alias("CMN_PRCT_ID"),
        F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("SRC_SYS_CD_SK"),
        F.lit(1).alias("CMN_PRCT_GNDR_CD_SK"),
        F.lit("N").alias("ACTV_IN"),
        F.lit("1753-01-01").alias("BRTH_DT_SK"),
        F.lit("1753-01-01").alias("LAST_CRDTL_DT_SK"),
        F.lit("1753-01-01").alias("INIT_CRDTL_DT_SK"),
        F.lit("1753-01-01").alias("NEXT_CRDTL_DT_SK"),
        F.lit("NA").alias("FIRST_NM"),
        F.lit("NA").alias("LAST_NM"),
        F.lit(None).cast(StringType()).alias("MIDINIT"),
        F.lit(None).cast(StringType()).alias("CMN_PRCT_TTL"),
        F.lit("NA").alias("NTNL_PROV_ID"),
        F.lit("").alias("SSN"),
        F.lit("NA").alias("CULT_CPBLTY_NM"),
    )
)

# -- LastCrdtlDtSkLkupFail => constraint => svLastCrdtlDtSkLkupChk = 'Y'
df_LastCrdtlDtSkLkupFail = (
    df_xfm_CheckLkpResults_stagevars
    .filter(F.col("svLastCrdtlDtSkLkupChk") == "Y")
    .select(
        F.col("CMN_PRCT_SK").alias("PRI_SK"),
        F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.lit("IdsCmnPrctFkey_EE").alias("JOB_NM"),
        F.lit("FKLOOKUP").alias("ERROR_TYP"),
        F.lit("CLNDR_DT").alias("PHYSCL_FILE_NM"),
        F.concat(F.col("SRC_SYS_CD"), F.lit(";"), F.col("LAST_CRDTL_DT")).alias("FRGN_NAT_KEY_STRING"),
        F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK"),
    )
)

# -- InitCrdtlDtSkLkupFail => svInitCrdtlDtSkLkupChk = 'Y'
df_InitCrdtlDtSkLkupFail = (
    df_xfm_CheckLkpResults_stagevars
    .filter(F.col("svInitCrdtlDtSkLkupChk") == "Y")
    .select(
        F.col("CMN_PRCT_SK").alias("PRI_SK"),
        F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.lit("IdsCmnPrctFkey_EE").alias("JOB_NM"),
        F.lit("FKLOOKUP").alias("ERROR_TYP"),
        F.lit("CLNDR_DT").alias("PHYSCL_FILE_NM"),
        F.concat(F.col("SRC_SYS_CD"), F.lit(";"), F.col("INIT_CRDTL_DT")).alias("FRGN_NAT_KEY_STRING"),
        F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK"),
    )
)

# -- NextCrdtlDtSkLkupFail => svNextCrdtlDtSkLkupChk = 'Y'
df_NextCrdtlDtSkLkupFail = (
    df_xfm_CheckLkpResults_stagevars
    .filter(F.col("svNextCrdtlDtSkLkupChk") == "Y")
    .select(
        F.col("CMN_PRCT_SK").alias("PRI_SK"),
        F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.lit("IdsCmnPrctFkey_EE").alias("JOB_NM"),
        F.lit("FKLOOKUP").alias("ERROR_TYP"),
        F.lit("CLNDR_DT").alias("PHYSCL_FILE_NM"),
        F.concat(F.col("SRC_SYS_CD"), F.lit(";"), F.col("NEXT_CRDTL_DT")).alias("FRGN_NAT_KEY_STRING"),
        F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK"),
    )
)

# -- BirthDtSkLkupFail => svBirthDtSkLkupChk = 'Y'
df_BirthDtSkLkupFail = (
    df_xfm_CheckLkpResults_stagevars
    .filter(F.col("svBirthDtSkLkupChk") == "Y")
    .select(
        F.col("CMN_PRCT_SK").alias("PRI_SK"),
        F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.lit("IdsCmnPrctFkey_EE").alias("JOB_NM"),
        F.lit("FKLOOKUP").alias("ERROR_TYP"),
        F.lit("CLNDR_DT").alias("PHYSCL_FILE_NM"),
        F.concat(F.col("SRC_SYS_CD"), F.lit(";"), F.col("BRTH_DT")).alias("FRGN_NAT_KEY_STRING"),
        F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK"),
    )
)

# -- CmnPrctGndrCdSkLkupFail => svCmnPrctGndrCdSkLkupchk = 'Y'
df_CmnPrctGndrCdSkLkupFail = (
    df_xfm_CheckLkpResults_stagevars
    .filter(F.col("svCmnPrctGndrCdSkLkupchk") == "Y")
    .select(
        F.col("CMN_PRCT_SK").alias("PRI_SK"),
        F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.lit("IdsCmnPrctFkey_EE").alias("JOB_NM"),
        F.lit("CDLOOKUP").alias("ERROR_TYP"),
        F.lit("CD_MPPNG").alias("PHYSCL_FILE_NM"),
        F.concat(
            F.col("SRC_SYS_CD"),
            F.lit(";"),
            F.lit("FACETS DB"),
            F.lit(";"),
            F.lit("IDS"),
            F.lit(";"),
            F.lit("GENDER"),
            F.lit(";"),
            F.lit("GENDER"),
            F.lit(";"),
            F.col("CMN_PRCT_GNDR_CD")
        ).alias("FRGN_NAT_KEY_STRING"),
        F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK"),
    )
)

# -----------------------------------------------------------------------------
# fnl_NA_UNK_Streams (PxFunnel) => unify df_Lnk_Main, df_Lnk_UNK, df_Lnk_NA
# -----------------------------------------------------------------------------
df_fnl_NA_UNK_Streams = (
    df_Lnk_Main.select(
        "CMN_PRCT_SK",
        "CMN_PRCT_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "SRC_SYS_CD_SK",
        "CMN_PRCT_GNDR_CD_SK",
        "ACTV_IN",
        "BRTH_DT_SK",
        "LAST_CRDTL_DT_SK",
        "INIT_CRDTL_DT_SK",
        "NEXT_CRDTL_DT_SK",
        "FIRST_NM",
        "LAST_NM",
        "MIDINIT",
        "CMN_PRCT_TTL",
        "NTNL_PROV_ID",
        "SSN",
        "CULT_CPBLTY_NM",
    )
    .unionByName(
        df_Lnk_UNK.select(
            "CMN_PRCT_SK",
            "CMN_PRCT_ID",
            "CRT_RUN_CYC_EXCTN_SK",
            "LAST_UPDT_RUN_CYC_EXCTN_SK",
            "SRC_SYS_CD_SK",
            "CMN_PRCT_GNDR_CD_SK",
            "ACTV_IN",
            "BRTH_DT_SK",
            "LAST_CRDTL_DT_SK",
            "INIT_CRDTL_DT_SK",
            "NEXT_CRDTL_DT_SK",
            "FIRST_NM",
            "LAST_NM",
            "MIDINIT",
            "CMN_PRCT_TTL",
            "NTNL_PROV_ID",
            "SSN",
            "CULT_CPBLTY_NM",
        )
    )
    .unionByName(
        df_Lnk_NA.select(
            "CMN_PRCT_SK",
            "CMN_PRCT_ID",
            "CRT_RUN_CYC_EXCTN_SK",
            "LAST_UPDT_RUN_CYC_EXCTN_SK",
            "SRC_SYS_CD_SK",
            "CMN_PRCT_GNDR_CD_SK",
            "ACTV_IN",
            "BRTH_DT_SK",
            "LAST_CRDTL_DT_SK",
            "INIT_CRDTL_DT_SK",
            "NEXT_CRDTL_DT_SK",
            "FIRST_NM",
            "LAST_NM",
            "MIDINIT",
            "CMN_PRCT_TTL",
            "NTNL_PROV_ID",
            "SSN",
            "CULT_CPBLTY_NM",
        )
    )
)

# -----------------------------------------------------------------------------
# seq_CMN_PRCT_FKey (PxSequentialFile) => write to: f"{adls_path}/load/CMN_PRCT.{SrcSysCd}.{RunID}.dat"
# Must rpad char/varchar columns
# -----------------------------------------------------------------------------
# Determine final schema for writing:
#   "CMN_PRCT_SK" (int - no rpad)
#   "CMN_PRCT_ID" (varchar -> rpad(..., <...>, " "))
#   "CRT_RUN_CYC_EXCTN_SK" (int)
#   "LAST_UPDT_RUN_CYC_EXCTN_SK" (int)
#   "SRC_SYS_CD_SK" (int)
#   "CMN_PRCT_GNDR_CD_SK" (int)
#   "ACTV_IN" (char(1) -> rpad(..., 1, " "))
#   "BRTH_DT_SK" (char(10) -> rpad(..., 10, " "))
#   "LAST_CRDTL_DT_SK" (char(10))
#   "INIT_CRDTL_DT_SK" (char(10))
#   "NEXT_CRDTL_DT_SK" (char(10))
#   "FIRST_NM" (varchar)
#   "LAST_NM" (varchar)
#   "MIDINIT" (char(1))
#   "CMN_PRCT_TTL" (varchar)
#   "NTNL_PROV_ID" (varchar)
#   "SSN" (varchar)
#   "CULT_CPBLTY_NM" (varchar)
df_seq_CMN_PRCT_FKey = (
    df_fnl_NA_UNK_Streams
    .select(
        F.col("CMN_PRCT_SK"),
        F.rpad(F.col("CMN_PRCT_ID"), <...>, " ").alias("CMN_PRCT_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("SRC_SYS_CD_SK"),
        F.col("CMN_PRCT_GNDR_CD_SK"),
        F.rpad(F.col("ACTV_IN"), 1, " ").alias("ACTV_IN"),
        F.rpad(F.col("BRTH_DT_SK"), 10, " ").alias("BRTH_DT_SK"),
        F.rpad(F.col("LAST_CRDTL_DT_SK"), 10, " ").alias("LAST_CRDTL_DT_SK"),
        F.rpad(F.col("INIT_CRDTL_DT_SK"), 10, " ").alias("INIT_CRDTL_DT_SK"),
        F.rpad(F.col("NEXT_CRDTL_DT_SK"), 10, " ").alias("NEXT_CRDTL_DT_SK"),
        F.rpad(F.col("FIRST_NM"), <...>, " ").alias("FIRST_NM"),
        F.rpad(F.col("LAST_NM"), <...>, " ").alias("LAST_NM"),
        F.rpad(F.col("MIDINIT"), 1, " ").alias("MIDINIT"),
        F.rpad(F.col("CMN_PRCT_TTL"), <...>, " ").alias("CMN_PRCT_TTL"),
        F.rpad(F.col("NTNL_PROV_ID"), <...>, " ").alias("NTNL_PROV_ID"),
        F.rpad(F.col("SSN"), <...>, " ").alias("SSN"),
        F.rpad(F.col("CULT_CPBLTY_NM"), <...>, " ").alias("CULT_CPBLTY_NM"),
    )
)

write_files(
    df_seq_CMN_PRCT_FKey,
    f"{adls_path}/load/CMN_PRCT.{SrcSysCd}.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

# -----------------------------------------------------------------------------
# Fnl_LkpFail (PxFunnel) => unify:
#   df_CmnPrctGndrCdSkLkupFail, df_BirthDtSkLkupFail,
#   df_NextCrdtlDtSkLkupFail, df_InitCrdtlDtSkLkupFail, df_LastCrdtlDtSkLkupFail
# -----------------------------------------------------------------------------
df_Fnl_LkpFail = (
    df_CmnPrctGndrCdSkLkupFail.select(
        "PRI_SK",
        "PRI_NAT_KEY_STRING",
        "SRC_SYS_CD_SK",
        "JOB_NM",
        "ERROR_TYP",
        "PHYSCL_FILE_NM",
        "FRGN_NAT_KEY_STRING",
        "FIRST_RECYC_TS",
        "JOB_EXCTN_SK",
    )
    .unionByName(
        df_BirthDtSkLkupFail.select(
            "PRI_SK",
            "PRI_NAT_KEY_STRING",
            "SRC_SYS_CD_SK",
            "JOB_NM",
            "ERROR_TYP",
            "PHYSCL_FILE_NM",
            "FRGN_NAT_KEY_STRING",
            "FIRST_RECYC_TS",
            "JOB_EXCTN_SK",
        )
    )
    .unionByName(
        df_NextCrdtlDtSkLkupFail.select(
            "PRI_SK",
            "PRI_NAT_KEY_STRING",
            "SRC_SYS_CD_SK",
            "JOB_NM",
            "ERROR_TYP",
            "PHYSCL_FILE_NM",
            "FRGN_NAT_KEY_STRING",
            "FIRST_RECYC_TS",
            "JOB_EXCTN_SK",
        )
    )
    .unionByName(
        df_InitCrdtlDtSkLkupFail.select(
            "PRI_SK",
            "PRI_NAT_KEY_STRING",
            "SRC_SYS_CD_SK",
            "JOB_NM",
            "ERROR_TYP",
            "PHYSCL_FILE_NM",
            "FRGN_NAT_KEY_STRING",
            "FIRST_RECYC_TS",
            "JOB_EXCTN_SK",
        )
    )
    .unionByName(
        df_LastCrdtlDtSkLkupFail.select(
            "PRI_SK",
            "PRI_NAT_KEY_STRING",
            "SRC_SYS_CD_SK",
            "JOB_NM",
            "ERROR_TYP",
            "PHYSCL_FILE_NM",
            "FRGN_NAT_KEY_STRING",
            "FIRST_RECYC_TS",
            "JOB_EXCTN_SK",
        )
    )
)

# -----------------------------------------------------------------------------
# Seq_FKeyFail (PxSequentialFile) => write to: f"{adls_path}/fkey_failures/{PrefixFkeyFailedFileName}.{jobName}.dat"
# Must rpad char/varchar columns as well
# -----------------------------------------------------------------------------
# The 9 columns:
#   PRI_SK (int)
#   PRI_NAT_KEY_STRING (varchar)
#   SRC_SYS_CD_SK (int)
#   JOB_NM (varchar)
#   ERROR_TYP (varchar)
#   PHYSCL_FILE_NM (varchar)
#   FRGN_NAT_KEY_STRING (varchar)
#   FIRST_RECYC_TS (timestamp?)
#   JOB_EXCTN_SK (int)
df_Seq_FKeyFail = df_Fnl_LkpFail.select(
    F.col("PRI_SK"),
    F.rpad(F.col("PRI_NAT_KEY_STRING"), <...>, " ").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK"),
    F.rpad(F.col("JOB_NM"), <...>, " ").alias("JOB_NM"),
    F.rpad(F.col("ERROR_TYP"), <...>, " ").alias("ERROR_TYP"),
    F.rpad(F.col("PHYSCL_FILE_NM"), <...>, " ").alias("PHYSCL_FILE_NM"),
    F.rpad(F.col("FRGN_NAT_KEY_STRING"), <...>, " ").alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS"),
    F.col("JOB_EXCTN_SK"),
)

write_files(
    df_Seq_FKeyFail,
    f"{adls_path}/fkey_failures/{PrefixFkeyFailedFileName}.{jobName}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)