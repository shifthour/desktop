# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #               Change Description                             Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Raja Gummadi                  03/04/2015      5460                                Originally Programmed                            IntegrateNewDevl        Kalyan Neelam             2015-03-06
# MAGIC Venkatesh Babu               2020-11-02                                            Added VRSN_ID                                       IntegrateDev2                Jeyaprasanna            2020-11-20

# MAGIC IdsIndvBeMaraRsltFkey_EE
# MAGIC This is a load ready file that will go into Load job
# MAGIC 11-06-2020  : Added VRSN_ID from source to target
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    IntegerType,
    DecimalType
)
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


SrcSysCd = get_widget_value("SrcSysCd","")
RunID = get_widget_value("RunID","")
PrefixFkeyFailedFileName = get_widget_value("PrefixFkeyFailedFileName","")

job_name = "CblTalnIdsIndvBeMaraRsltFkey"

# seqINDV_BE_MARA_RSLT_Pkey (PxSequentialFile) - Reading .dat
schema_seqINDV_BE_MARA_RSLT_Pkey = StructType([
    StructField("PRI_NAT_KEY_STRING", StringType(), nullable=False),
    StructField("FIRST_RECYC_TS", TimestampType(), nullable=False),
    StructField("INDV_BE_MARA_RSLT_SK", IntegerType(), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("INDV_BE_KEY", DecimalType(38,10), nullable=True),
    StructField("MDL_ID", StringType(), nullable=True),
    StructField("PRCS_YR_MO_SK", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("SRC_SYS_CD_SK", IntegerType(), nullable=False),
    StructField("GNDR", StringType(), nullable=True),
    StructField("PRIOR_CST_MED", DecimalType(38,10), nullable=True),
    StructField("PRIOR_CST_RX", DecimalType(38,10), nullable=True),
    StructField("PRIOR_CST_TOT", DecimalType(38,10), nullable=True),
    StructField("CATLCD", IntegerType(), nullable=True),
    StructField("UNCATLCD", IntegerType(), nullable=True),
    StructField("EXPSR_MO", IntegerType(), nullable=True),
    StructField("CATNDC", IntegerType(), nullable=True),
    StructField("UNCATNDC", IntegerType(), nullable=True),
    StructField("AGE", IntegerType(), nullable=True),
    StructField("ER_SCORE", DecimalType(38,10), nullable=True),
    StructField("IP_SCORE", DecimalType(38,10), nullable=True),
    StructField("MED_SCORE", DecimalType(38,10), nullable=True),
    StructField("OTHR", DecimalType(38,10), nullable=True),
    StructField("OP_SCORE", DecimalType(38,10), nullable=True),
    StructField("RX_SCORE", DecimalType(38,10), nullable=True),
    StructField("PHYS_SCORE", DecimalType(38,10), nullable=True),
    StructField("TOT_SCORE", DecimalType(38,10), nullable=True),
    StructField("VRSN_ID", StringType(), nullable=True)
])

df_seqINDV_BE_MARA_RSLT_Pkey = (
    spark.read.format("csv")
    .option("delimiter", ",")
    .option("quote", "^")
    .option("header", "false")
    .option("nullValue", None)
    .schema(schema_seqINDV_BE_MARA_RSLT_Pkey)
    .load(f"{adls_path}/key/INDV_BE_MARA_RSLT.{SrcSysCd}.pkey.{RunID}.dat")
)

# ds_CD_MPPNG_LkpData (PxDataSet) - Translate .ds to .parquet
df_ds_CD_MPPNG_LkpData = spark.read.parquet(f"{adls_path}/ds/CD_MPPNG.parquet")

# fltr_FilterData (PxFilter)
df_fltr_FilterData = (
    df_ds_CD_MPPNG_LkpData
    .filter(
        (F.col("SRC_SYS_CD") == "FACETS") &
        (F.col("SRC_CLCTN_CD") == "FACETS DBO") &
        (F.col("TRGT_CLCTN_CD") == "IDS") &
        (F.col("SRC_DOMAIN_NM") == "GENDER") &
        (F.col("TRGT_DOMAIN_NM") == "GENDER")
    )
    .select(
        F.col("SRC_CD").alias("SRC_CD"),
        F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
    )
)

# LkupFkey (PxLookup) - left join
df_LkupFkey_joined = (
    df_seqINDV_BE_MARA_RSLT_Pkey.alias("In")
    .join(
        df_fltr_FilterData.alias("lkup"),
        F.col("In.GNDR") == F.col("lkup.SRC_CD"),
        how="left"
    )
)

df_LkupFkey = df_LkupFkey_joined.select(
    F.col("In.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("In.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("In.INDV_BE_MARA_RSLT_SK").alias("INDV_BE_MARA_RSLT_SK"),
    F.col("In.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("In.INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("In.MDL_ID").alias("MDL_ID"),
    F.col("In.PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    F.col("In.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("In.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("In.GNDR").alias("GNDR"),
    F.col("In.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("In.PRIOR_CST_MED").alias("PRIOR_CST_MED"),
    F.col("In.PRIOR_CST_RX").alias("PRIOR_CST_RX"),
    F.col("In.PRIOR_CST_TOT").alias("PRIOR_CST_TOT"),
    F.col("In.CATLCD").alias("CATLCD"),
    F.col("In.UNCATLCD").alias("UNCATLCD"),
    F.col("In.EXPSR_MO").alias("EXPSR_MO"),
    F.col("In.CATNDC").alias("CATNDC"),
    F.col("In.UNCATNDC").alias("UNCATNDC"),
    F.col("In.AGE").alias("AGE"),
    F.col("In.ER_SCORE").alias("ER_SCORE"),
    F.col("In.IP_SCORE").alias("IP_SCORE"),
    F.col("In.MED_SCORE").alias("MED_SCORE"),
    F.col("In.OTHR").alias("OTHR"),
    F.col("In.OP_SCORE").alias("OP_SCORE"),
    F.col("In.RX_SCORE").alias("RX_SCORE"),
    F.col("In.PHYS_SCORE").alias("PHYS_SCORE"),
    F.col("In.TOT_SCORE").alias("TOT_SCORE"),
    F.col("lkup.CD_MPPNG_SK").alias("GNDR_CD_SK"),
    F.col("In.VRSN_ID").alias("VRSN_ID")
)

# xfm_CheckLkpResults (CTransformerStage)
df_xfm_CheckLkpResults = df_LkupFkey.withColumn(
    "svFkeyFail",
    F.when(F.col("GNDR_CD_SK").isNull(), F.lit("Y")).otherwise(F.lit("N"))
)

# Lnk_Main
df_Lnk_Main = df_xfm_CheckLkpResults.select(
    F.when(F.col("INDV_BE_MARA_RSLT_SK").isNotNull(), trim(F.col("INDV_BE_MARA_RSLT_SK"))).otherwise(F.lit("")).alias("INDV_BE_MARA_RSLT_SK"),
    F.col("INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("MDL_ID").alias("MDL_ID"),
    F.col("PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("GNDR_CD_SK").alias("GNDR_CD_SK"),
    F.col("PRIOR_CST_MED").alias("CST_MED_ALW_AMT"),
    F.col("PRIOR_CST_RX").alias("CST_PDX_ALW_AMT"),
    F.col("PRIOR_CST_TOT").alias("CST_TOT_ALW_AMT"),
    F.col("CATLCD").alias("DIAG_CD_CAT_CT"),
    F.col("UNCATLCD").alias("DIAG_CD_UNCAT_CT"),
    F.col("EXPSR_MO").alias("EXPSR_MO_CT"),
    F.col("CATNDC").alias("NDC_CAT_CT"),
    F.col("UNCATNDC").alias("NDC_UNCAT_CT"),
    F.col("AGE").alias("INDV_AGE_NO"),
    F.col("ER_SCORE").alias("ER_SCORE_NO"),
    F.col("IP_SCORE").alias("IP_SCORE_NO"),
    F.col("MED_SCORE").alias("MED_SCORE_NO"),
    F.col("OTHR").alias("OTHR_SVC_SCORE_NO"),
    F.col("OP_SCORE").alias("OP_SCORE_NO"),
    F.col("RX_SCORE").alias("PDX_SCORE_NO"),
    F.col("PHYS_SCORE").alias("PHYS_SVC_SCORE_NO"),
    F.col("TOT_SCORE").alias("TOT_SCORE_NO"),
    F.col("VRSN_ID").alias("VRSN_ID")
)

# Prepare a temp DF with row numbers for Lnk_UNK, Lnk_NA
w = Window.orderBy(F.monotonically_increasing_id())
df_temp = df_xfm_CheckLkpResults.withColumn("ROWNUM", F.row_number().over(w))

# Lnk_UNK
df_Lnk_UNK = df_temp.filter("ROWNUM=1").select(
    F.lit(0).alias("INDV_BE_MARA_RSLT_SK"),
    F.lit(0).alias("INDV_BE_KEY"),
    F.lit("UNK").alias("MDL_ID"),
    F.lit("175301").alias("PRCS_YR_MO_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("GNDR_CD_SK"),
    F.lit(0).alias("CST_MED_ALW_AMT"),
    F.lit(0).alias("CST_PDX_ALW_AMT"),
    F.lit(0).alias("CST_TOT_ALW_AMT"),
    F.lit(0).alias("DIAG_CD_CAT_CT"),
    F.lit(0).alias("DIAG_CD_UNCAT_CT"),
    F.lit(0).alias("EXPSR_MO_CT"),
    F.lit(0).alias("NDC_CAT_CT"),
    F.lit(0).alias("NDC_UNCAT_CT"),
    F.lit(0).alias("INDV_AGE_NO"),
    F.lit(0).alias("ER_SCORE_NO"),
    F.lit(0).alias("IP_SCORE_NO"),
    F.lit(0).alias("MED_SCORE_NO"),
    F.lit(0).alias("OTHR_SVC_SCORE_NO"),
    F.lit(0).alias("OP_SCORE_NO"),
    F.lit(0).alias("PDX_SCORE_NO"),
    F.lit(0).alias("PHYS_SVC_SCORE_NO"),
    F.lit(0).alias("TOT_SCORE_NO"),
    F.col("VRSN_ID").alias("VRSN_ID")
)

# Lnk_NA
df_Lnk_NA = df_temp.filter("ROWNUM=1").select(
    F.lit(1).alias("INDV_BE_MARA_RSLT_SK"),
    F.lit(0).alias("INDV_BE_KEY"),
    F.lit("NA").alias("MDL_ID"),
    F.lit("175301").alias("PRCS_YR_MO_SK"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("GNDR_CD_SK"),
    F.lit(0).alias("CST_MED_ALW_AMT"),
    F.lit(0).alias("CST_PDX_ALW_AMT"),
    F.lit(0).alias("CST_TOT_ALW_AMT"),
    F.lit(0).alias("DIAG_CD_CAT_CT"),
    F.lit(0).alias("DIAG_CD_UNCAT_CT"),
    F.lit(0).alias("EXPSR_MO_CT"),
    F.lit(0).alias("NDC_CAT_CT"),
    F.lit(0).alias("NDC_UNCAT_CT"),
    F.lit(0).alias("INDV_AGE_NO"),
    F.lit(0).alias("ER_SCORE_NO"),
    F.lit(0).alias("IP_SCORE_NO"),
    F.lit(0).alias("MED_SCORE_NO"),
    F.lit(0).alias("OTHR_SVC_SCORE_NO"),
    F.lit(0).alias("OP_SCORE_NO"),
    F.lit(0).alias("PDX_SCORE_NO"),
    F.lit(0).alias("PHYS_SVC_SCORE_NO"),
    F.lit(0).alias("TOT_SCORE_NO"),
    F.col("VRSN_ID").alias("VRSN_ID")
)

# lnk_AccumTypCdLkup_Fail (svFkeyFail = 'Y')
df_lnk_AccumTypCdLkup_Fail = (
    df_xfm_CheckLkpResults
    .filter(F.col("svFkeyFail") == "Y")
    .select(
        F.col("INDV_BE_MARA_RSLT_SK").alias("PRI_SK"),
        F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.lit(job_name).alias("JOB_NM"),
        F.lit("CDLOOKUP").alias("ERROR_TYP"),
        F.lit("CD_MPPNG").alias("PHYSCL_FILE_NM"),
        F.concat(
            F.col("SRC_SYS_CD"), F.lit(";"),
            F.lit("FACETS DBO"), F.lit(";"),
            F.lit("IDS"), F.lit(";"),
            F.lit("GENDER"), F.lit(";"),
            F.lit("GENDER"), F.lit(";"),
            F.col("GNDR")
        ).alias("FRGN_NAT_KEY_STRING"),
        F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
    )
)

# seq_FkeyFailedFile_csv (PxSequentialFile) - write .dat
write_files(
    df_lnk_AccumTypCdLkup_Fail,
    f"{adls_path}/fkey_failures/{PrefixFkeyFailedFileName}.{job_name}.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)

# fnl_NA_UNK_Streams (PxFunnel) - union
commonCols = [
    "INDV_BE_MARA_RSLT_SK",
    "INDV_BE_KEY",
    "MDL_ID",
    "PRCS_YR_MO_SK",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "GNDR_CD_SK",
    "CST_MED_ALW_AMT",
    "CST_PDX_ALW_AMT",
    "CST_TOT_ALW_AMT",
    "DIAG_CD_CAT_CT",
    "DIAG_CD_UNCAT_CT",
    "EXPSR_MO_CT",
    "NDC_CAT_CT",
    "NDC_UNCAT_CT",
    "INDV_AGE_NO",
    "ER_SCORE_NO",
    "IP_SCORE_NO",
    "MED_SCORE_NO",
    "OTHR_SVC_SCORE_NO",
    "OP_SCORE_NO",
    "PDX_SCORE_NO",
    "PHYS_SVC_SCORE_NO",
    "TOT_SCORE_NO",
    "VRSN_ID"
]

df_fnl_NA_UNK_Streams = (
    df_Lnk_Main.select(commonCols)
    .unionByName(df_Lnk_UNK.select(commonCols))
    .unionByName(df_Lnk_NA.select(commonCols))
)

# seq_INDV_BE_MARA_RSLT_Fkey (PxSequentialFile) - final write .dat
# Apply rpad for char or varchar columns in final output
df_fnl_out = (
    df_fnl_NA_UNK_Streams
    .withColumn("MDL_ID", F.rpad(F.col("MDL_ID"), <...>, " "))
    .withColumn("PRCS_YR_MO_SK", F.rpad(F.col("PRCS_YR_MO_SK"), 6, " "))
    .withColumn("VRSN_ID", F.rpad(F.col("VRSN_ID"), <...>, " "))
    .select(commonCols)
)

write_files(
    df_fnl_out,
    f"{adls_path}/load/INDV_BE_MARA_RSLT.{SrcSysCd}.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)