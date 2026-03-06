# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Called from:  OPTUMACADrugInvoiceUpdateSeq
# MAGIC PROCESSING:   Set foreign key values for OPTUMRX exception table.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                    Date                       Change Description                                         Project #                           Development Project          Code Reviewer               Date Reviewed  
# MAGIC ------------------           ----------------------------        -------------------------------------------------------------------      ----------------                           ------------------------------------       ----------------------------           ----------------
# MAGIC Velmani Kondappan    2020-10-20      6264 - PBM PHASE II - Government Programs     Initial Development            IntegrateDev2                      Reddy Sanam                 2020-12-10

# MAGIC Load file for EDW table
# MAGIC OPTUMRX Invoice Exception Foreign Key: Called from OPTUMACADrugInvoiceUpdateSeq
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


# Define job parameters
EDWFilePath = get_widget_value('$EDWFilePath','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
RunCycle = get_widget_value('RunCycle','')
CurrentDateParam = get_widget_value('CurrentDate','')
Logging = get_widget_value('Logging','')
IDSOwner = get_widget_value('$IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# Helper to resolve file paths according to the rules
def resolve_path(directory, filename):
    lower_dir = directory.lower()
    if "landing" in lower_dir:
        # Replace only the first occurrence of "landing" if needed
        return directory.replace("landing", f"{adls_path_raw}/landing", 1) + f"/{filename}"
    elif "external" in lower_dir:
        # Replace only the first occurrence of "external" if needed
        return directory.replace("external", f"{adls_path_publish}/external", 1) + f"/{filename}"
    else:
        return f"{adls_path}/{directory}/{filename}"

################################################################################
# Stage: OPTUMRX_Invoice_Unmatched (CSeqFileStage) - READ
################################################################################
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
    StructField("DT_FILLED", StringType(), nullable=False)  # Not subsequently used in the Transformer outputs, but must not be skipped
])

input_directory_optumrx = "key"
input_filename_optumrx = f"OPTUMRX_Invoice_trans.dat.{RunID}"
full_path_optumrx = resolve_path(input_directory_optumrx, input_filename_optumrx)

df_OPTUMRX_Invoice_Unmatched = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", '"')
    .option("delimiter", ",")
    .schema(schema_OPTUMRX_Invoice_Unmatched)
    .load(full_path_optumrx)
)

################################################################################
# Stage: Trns2 (CTransformerStage)
################################################################################

# First, attach the parameter value as a column to handle references to "SrcSysCd"
df_Trans2_pre = df_OPTUMRX_Invoice_Unmatched.withColumn("SrcSysCdParam", F.lit(SrcSysCd))

facets_set = [
    'PCT','WELLDYNERX','PCS','CAREMARK','ARGUS','EDC','OT@2','ADOL','MOHSAIC','CAREADVANCE',
    'ESI','OPTUMRX','MCSOURCE','MCAID','MEDTRAK','BCBSSC','BCA','BCBSA','SAVRX','LDI','EYEMED',
    'CVS','LUMERIS','MEDIMPACT'
]

df_Trans2 = (
    df_Trans2_pre
    # svSrcSysCd
    .withColumn(
        "svSrcSysCd",
        F.when(
            F.col("SrcSysCdParam").isNull() | (F.length(trim(F.col("SrcSysCdParam"))) == 0),
            "UNK"
        )
        .when(trim(F.col("SrcSysCdParam")).isin(facets_set), "FACETS")
        .otherwise(F.col("SrcSysCdParam"))
    )
    # svGrp
    .withColumn(
        "svGrp",
        GetFkeyGrp(
            F.col("svSrcSysCd"),
            F.col("ESI_INVC_EXCPT_SK"),
            F.col("GRP_ID"),
            F.lit(Logging)
        )
    )
    # svMbr
    .withColumn(
        "svMbr",
        GetFkeyMbr(
            F.col("svSrcSysCd"),
            F.col("ESI_INVC_EXCPT_SK"),
            F.col("MBR_UNIQ_KEY"),
            F.lit(Logging)
        )
    )
    # svProd
    .withColumn(
        "svProd",
        GetFkeyProd(
            F.col("svSrcSysCd"),
            F.col("ESI_INVC_EXCPT_SK"),
            F.col("PROD_ID"),
            F.lit(Logging)
        )
    )
    # svPdDate
    .withColumn(
        "svPdDate",
        GetFkeyDate(
            F.lit("IDS"),
            F.col("ESI_INVC_EXCPT_SK"),
            F.col("PD_DT_SK"),
            F.lit(Logging)
        )
    )
    # svLOB
    .withColumn(
        "svLOB",
        GetFkeyFnclLob(
            F.lit("PSI"),
            F.col("ESI_INVC_EXCPT_SK"),
            F.col("FNCL_LOB_CD"),
            F.lit(Logging)
        )
    )
    # svErrCnt
    .withColumn(
        "svErrCnt",
        GetFkeyErrorCnt(
            F.col("ESI_INVC_EXCPT_SK")
        )
    )
)

###############################################################################
# Output link: IdsFile -> OPTUMRX_INVC_EXCPT
###############################################################################
df_IdsFile = df_Trans2.select(
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

# Apply rpad for char fields (PD_DT_SK and PRCS_DT_SK are char(10))
df_IdsFile = (
    df_IdsFile
    .withColumn("PD_DT_SK", rpad(F.col("PD_DT_SK"), 10, " "))
    .withColumn("PRCS_DT_SK", rpad(F.col("PRCS_DT_SK"), 10, " "))
)

###############################################################################
# Output link: Recycle -> hf_recycle (CHashedFileStage)
###############################################################################
df_Recycle = (
    df_Trans2
    .filter(F.col("svErrCnt") > 0)
    .select(
        GetRecycleKey(F.col("ESI_INVC_EXCPT_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
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

# rpad for GRP_ID which is char(8)
df_Recycle = df_Recycle.withColumn("GRP_ID", rpad(F.col("GRP_ID"), 8, " "))

###############################################################################
# Output link: EdwFile -> OPTUMRX_INVC_EXCPT_D
###############################################################################
df_EdwFile = df_Trans2.select(
    F.col("ESI_INVC_EXCPT_SK").alias("ESI_INVC_EXCPT_SK"),
    F.col("svSrcSysCd").alias("SRC_SYS_CD"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.lit(CurrentDateParam).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(CurrentDateParam).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
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

# rpad for char fields
df_EdwFile = (
    df_EdwFile
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("CLM_PD_DT_SK", rpad(F.col("CLM_PD_DT_SK"), 10, " "))
    .withColumn("CLM_PRCS_DT_SK", rpad(F.col("CLM_PRCS_DT_SK"), 10, " "))
    .withColumn("GRP_ID", rpad(F.col("GRP_ID"), 8, " "))
    .withColumn("PROD_ID", rpad(F.col("PROD_ID"), 18, " "))
)

################################################################################
# Stage: OPTUMRX_INVC_EXCPT (CSeqFileStage) - WRITE
################################################################################
sequence_out_directory_1 = "load"
sequence_out_filename_1 = "OPTUMRX_INVC_EXCPT.dat"
full_out_path_1 = resolve_path(sequence_out_directory_1, sequence_out_filename_1)

write_files(
    df_IdsFile,
    full_out_path_1,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

################################################################################
# Stage: hf_recycle (CHashedFileStage) - WRITE (Scenario C)
################################################################################
# Translate hashed file to parquet
hashedfile_path = f"{adls_path}/hf_recycle.parquet"
write_files(
    df_Recycle,
    hashedfile_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)

################################################################################
# Stage: OPTUMRX_INVC_EXCPT_D (CSeqFileStage) - WRITE
################################################################################
sequence_out_directory_2 = f"{EDWFilePath}/load"
sequence_out_filename_2 = "OPTUMRX_INVC_EXCPT_D.dat"

# Apply the same path-resolution logic to see if it contains "landing" or "external"
def resolve_edw_path(dir_val, fname_val):
    lower_dir = dir_val.lower()
    if "landing" in lower_dir:
        return dir_val.replace("landing", f"{adls_path_raw}/landing", 1) + f"/{fname_val}"
    elif "external" in lower_dir:
        return dir_val.replace("external", f"{adls_path_publish}/external", 1) + f"/{fname_val}"
    else:
        return f"{adls_path}/{dir_val}/{fname_val}"

full_out_path_2 = resolve_edw_path(sequence_out_directory_2, sequence_out_filename_2)

write_files(
    df_EdwFile,
    full_out_path_2,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)