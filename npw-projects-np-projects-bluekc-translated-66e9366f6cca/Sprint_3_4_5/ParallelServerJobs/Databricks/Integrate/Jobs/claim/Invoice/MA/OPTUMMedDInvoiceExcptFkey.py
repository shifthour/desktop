# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY: OptumMedDDrugInvoiceCntl
# MAGIC 
# MAGIC PROCESSING:   Set foreign key values for OPTUMRX exception table.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                    Date                       Change Description                                         Project #                                                         Development Project          Code Reviewer               Date Reviewed  
# MAGIC ------------------           ----------------------------        -------------------------------------------------------------------      ----------------                                                       ------------------------------------       ----------------------------           ----------------
# MAGIC Velmani Kondappan    2020-10-20                  Initial Development                        6264 - PBM PHASE II - Government Programs           IntegrateDev2	Abhiram Dasarathy	2020-12-11

# MAGIC Load file for EDW table
# MAGIC OPTUMRX Invoice Exception Foreign Key
# MAGIC Load file for IDS table
# MAGIC Input file created in OPTUMRXInvoiceExcptTrans
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Parameter retrieval
param_EDWFilePath = get_widget_value('EDWFilePath','')
param_SrcSysCd = get_widget_value('SrcSysCd','OPTUMRX')
param_RunID = get_widget_value('RunID','')
param_RunCycle = get_widget_value('RunCycle','')
param_CurrentDate = get_widget_value('CurrentDate','')
param_Logging = get_widget_value('Logging', "'Y'")
param_IDSOwner = get_widget_value('IDSOwner','')
param_ids_secret_name = get_widget_value('ids_secret_name','')

# Schema definition for OPTUMRX_Invoice_Unmatched
schema_OPTUMRX_Invoice_Unmatched = StructType([
    StructField("ESI_INVC_EXCPT_SK", IntegerType(), nullable=False),
    StructField("SRC_SYS_CD_SK", IntegerType(), nullable=False),
    StructField("CLM_ID", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("FNCL_LOB_CD", StringType(), nullable=False),
    StructField("GRP_ID", StringType(), nullable=False),
    StructField("MBR_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("PROD_ID", StringType(), nullable=False),
    StructField("PD_DT_SK", StringType(), nullable=False),
    StructField("PRCS_DT_SK", StringType(), nullable=False),
    StructField("ACTL_PD_AMT", DecimalType(38,10), nullable=False),
    StructField("DT_FILLED", StringType(), nullable=False)
])

# Read from OPTUMRX_Invoice_Unmatched (CSeqFileStage)
df_OPTUMRX_Invoice_Unmatched = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .schema(schema_OPTUMRX_Invoice_Unmatched)
    .load(f"{adls_path}/key/OPTUMRX_Invoice_trans.dat.{param_RunID}")
)

# Prepare Transformer (Trns2) logic
df_stage2 = df_OPTUMRX_Invoice_Unmatched.withColumn(
    "SrcSysCd_param", F.lit(param_SrcSysCd)
)

df_stage2 = df_stage2.withColumn(
    "svSrcSysCd",
    F.when(
        (F.col("SrcSysCd_param").isNull()) | (F.length(trim(F.col("SrcSysCd_param"))) == 0),
        F.lit("UNK")
    )
    .when(F.trim(F.col("SrcSysCd_param")) == "PCT", F.lit("FACETS"))
    .when(F.trim(F.col("SrcSysCd_param")) == "WELLDYNERX", F.lit("FACETS"))
    .when(F.trim(F.col("SrcSysCd_param")) == "PCS", F.lit("FACETS"))
    .when(F.trim(F.col("SrcSysCd_param")) == "CAREMARK", F.lit("FACETS"))
    .when(F.trim(F.col("SrcSysCd_param")) == "ARGUS", F.lit("FACETS"))
    .when(F.trim(F.col("SrcSysCd_param")) == "EDC", F.lit("FACETS"))
    .when(F.trim(F.col("SrcSysCd_param")) == "OT@2", F.lit("FACETS"))
    .when(F.trim(F.col("SrcSysCd_param")) == "ADOL", F.lit("FACETS"))
    .when(F.trim(F.col("SrcSysCd_param")) == "MOHSAIC", F.lit("FACETS"))
    .when(F.trim(F.col("SrcSysCd_param")) == "CAREADVANCE", F.lit("FACETS"))
    .when(F.trim(F.col("SrcSysCd_param")) == "ESI", F.lit("FACETS"))
    .when(F.trim(F.col("SrcSysCd_param")) == "OPTUMRX", F.lit("FACETS"))
    .when(F.trim(F.col("SrcSysCd_param")) == "MCSOURCE", F.lit("FACETS"))
    .when(F.trim(F.col("SrcSysCd_param")) == "MCAID", F.lit("FACETS"))
    .when(F.trim(F.col("SrcSysCd_param")) == "MEDTRAK", F.lit("FACETS"))
    .when(F.trim(F.col("SrcSysCd_param")) == "BCBSSC", F.lit("FACETS"))
    .when(F.trim(F.col("SrcSysCd_param")) == "BCA", F.lit("FACETS"))
    .when(F.trim(F.col("SrcSysCd_param")) == "BCBSA", F.lit("FACETS"))
    .when(F.trim(F.col("SrcSysCd_param")) == "SAVRX", F.lit("FACETS"))
    .when(F.trim(F.col("SrcSysCd_param")) == "LDI", F.lit("FACETS"))
    .when(F.trim(F.col("SrcSysCd_param")) == "EYEMED", F.lit("FACETS"))
    .when(F.trim(F.col("SrcSysCd_param")) == "CVS", F.lit("FACETS"))
    .when(F.trim(F.col("SrcSysCd_param")) == "LUMERIS", F.lit("FACETS"))
    .when(F.trim(F.col("SrcSysCd_param")) == "MEDIMPACT", F.lit("FACETS"))
    .otherwise(F.col("SrcSysCd_param"))
)

df_stage2 = df_stage2.withColumn(
    "svGrp",
    GetFkeyGrp(
        F.col("svSrcSysCd"),
        F.col("ESI_INVC_EXCPT_SK"),
        F.col("GRP_ID"),
        F.lit(param_Logging)
    )
)

df_stage2 = df_stage2.withColumn(
    "svMbr",
    GetFkeyMbr(
        F.col("svSrcSysCd"),
        F.col("ESI_INVC_EXCPT_SK"),
        F.col("MBR_UNIQ_KEY"),
        F.lit(param_Logging)
    )
)

df_stage2 = df_stage2.withColumn(
    "svProd",
    GetFkeyProd(
        F.col("svSrcSysCd"),
        F.col("ESI_INVC_EXCPT_SK"),
        F.col("PROD_ID"),
        F.lit(param_Logging)
    )
)

df_stage2 = df_stage2.withColumn(
    "svPdDate",
    GetFkeyDate(
        F.lit("IDS"),
        F.col("ESI_INVC_EXCPT_SK"),
        F.col("PD_DT_SK"),
        F.lit(param_Logging)
    )
)

df_stage2 = df_stage2.withColumn(
    "svLOB",
    GetFkeyFnclLob(
        F.lit("PSI"),
        F.col("ESI_INVC_EXCPT_SK"),
        F.col("FNCL_LOB_CD"),
        F.lit(param_Logging)
    )
)

df_stage2 = df_stage2.withColumn(
    "svErrCnt",
    GetFkeyErrorCnt(F.col("ESI_INVC_EXCPT_SK"))
)

# Output link "IdsFile" (all rows)
df_IdsFile = df_stage2.select(
    F.col("ESI_INVC_EXCPT_SK").alias("ESI_INVC_EXCPT_SK"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("svLOB").alias("FNCL_LOB_SK"),
    F.col("svGrp").alias("GRP_SK"),
    F.col("svMbr").alias("MBR_SK"),
    F.col("svProd").alias("PROD_SK"),
    F.col("svPdDate").alias("PD_DT_SK"),
    F.col("PRCS_DT_SK").alias("PRCS_DT_SK"),
    F.col("ACTL_PD_AMT").alias("ACTL_PD_AMT")
)

df_IdsFile_out = (
    df_IdsFile
    .withColumn("PD_DT_SK", F.rpad(F.col("PD_DT_SK"), 10, " "))
    .withColumn("PRCS_DT_SK", F.rpad(F.col("PRCS_DT_SK"), 10, " "))
)

write_files(
    df_IdsFile_out,
    f"{adls_path}/load/OPTUMRX_INVC_EXCPT.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

# Output link "Recycle" (rows where svErrCnt > 0)
df_Recycle = (
    df_stage2
    .filter(F.col("svErrCnt") > 0)
    .select(
        F.expr("GetRecycleKey(ESI_INVC_EXCPT_SK)").alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("ESI_INVC_EXCPT_SK").alias("ESI_INVC_EXCPT_SK"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("FNCL_LOB_CD").alias("FNCL_LOB_CD"),
        F.col("GRP_ID").alias("GRP_ID"),
        F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY")
    )
)

df_Recycle_out = df_Recycle.withColumn("GRP_ID", F.rpad(F.col("GRP_ID"), 8, " "))

write_files(
    df_Recycle_out,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# Output link "EdwFile" (all rows)
df_EdwFile = df_stage2.select(
    F.col("ESI_INVC_EXCPT_SK").alias("ESI_INVC_EXCPT_SK"),
    F.col("svSrcSysCd").alias("SRC_SYS_CD"),
    F.col("CLM_ID").alias("CLM_ID"),
    current_date().alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    current_date().alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("svLOB").alias("FNCL_LOB_SK"),
    F.col("svGrp").alias("GRP_SK"),
    F.col("svMbr").alias("MBR_SK"),
    F.col("svProd").alias("PROD_SK"),
    F.col("ACTL_PD_AMT").alias("CLM_ACTL_PD_AMT"),
    F.col("svPdDate").alias("CLM_PD_DT_SK"),
    F.col("PRCS_DT_SK").alias("CLM_PRCS_DT_SK"),
    F.col("FNCL_LOB_CD").alias("FNCL_LOB_CD"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_EdwFile_out = (
    df_EdwFile
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("CLM_PD_DT_SK", F.rpad(F.col("CLM_PD_DT_SK"), 10, " "))
    .withColumn("CLM_PRCS_DT_SK", F.rpad(F.col("CLM_PRCS_DT_SK"), 10, " "))
    .withColumn("GRP_ID", F.rpad(F.col("GRP_ID"), 8, " "))
    .withColumn("PROD_ID", F.rpad(F.col("PROD_ID"), 18, " "))
)

write_files(
    df_EdwFile_out,
    f"{adls_path}/{param_EDWFilePath}/load/OPTUMRX_INVC_EXCPT_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)