# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_2 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
# MAGIC ^1_2 02/10/09 11:16:45 Batch  15017_40611 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 02/02/09 09:52:49 Batch  15009_35578 PROMOTE bckcetl ids20 dsadm bls for brent
# MAGIC ^1_1 01/26/09 13:03:15 Batch  15002_46999 INIT bckcett testIDS dsadm bls for bl
# MAGIC ^1_1 12/16/08 10:00:18 Batch  14961_36031 PROMOTE bckcett testIDS u03651 steph for Brent - claim primary keying
# MAGIC ^1_1 12/16/08 09:15:37 Batch  14961_33343 INIT bckcett devlIDS u03651 steffy
# MAGIC 
# MAGIC 
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:   Set foreign key values for ESI exception table.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                    Date                       Change Description                                                                                     Project #                  Development Project          Code Reviewer               Date Reviewed  
# MAGIC ------------------           ----------------------------        ----------------------------------------------------------------------------                                           ----------------               ------------------------------------       ----------------------------           ----------------
# MAGIC Brent Leland         11-25-2008                  Original programming                                                                                     3567 Primary Key    devlIDS

# MAGIC Load file for EDW table
# MAGIC ESI Invoice Exception Foreign Key
# MAGIC Load file for IDS table
# MAGIC Input file created in ESIInvoiceExcptTrans
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Parameters
EDWFilePath = get_widget_value('EDWFilePath','')
SrcSysCd = get_widget_value('SrcSysCd','ESI')
RunID = get_widget_value('RunID','100')
RunCycle = get_widget_value('RunCycle','100')
CurrentDate = get_widget_value('CurrentDate','')
Logging = get_widget_value('Logging','\'Y\'')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# Schema for ESI_Invoice_Unmatched
schema_esi_invoice_unmatched = StructType([
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

# Read ESI_Invoice_Unmatched
df_ESI_Invoice_Unmatched = (
    spark.read.format("csv")
    .option("quote", "\"")
    .option("delimiter", ",")
    .option("header", False)
    .schema(schema_esi_invoice_unmatched)
    .load(f"{adls_path}/key/ESI_Invoice_trans.dat.{RunID}")
)

# Transformer Trns2
df_trns2 = df_ESI_Invoice_Unmatched.withColumn("SrcSysCd", F.lit(SrcSysCd))

df_trns2 = df_trns2.withColumn(
    "svSrcSysCd",
    F.when(F.isnull("SrcSysCd"), F.lit("UNK"))
     .when(F.length(trim("SrcSysCd")) == 0, F.lit("UNK"))
     .when(
       trim("SrcSysCd").isin([
         "PCT","WELLDYNERX","PCS","CAREMARK","ARGUS","EDC","OT@2","ADOL","MOHSAIC","CAREADVANCE","ESI","OPTUMRX",
         "MCSOURCE","MCAID","MEDTRAK","BCBSSC","BCA","BCBSA","SAVRX","LDI","EYEMED","CVS","LUMERIS","MEDIMPACT"
       ]),
       F.lit("FACETS")
     )
     .otherwise(trim("SrcSysCd"))
)

df_trns2 = df_trns2.withColumn(
    "svGrp",
    GetFkeyGrp(
        F.col("svSrcSysCd"),
        F.col("ESI_INVC_EXCPT_SK"),
        F.col("GRP_ID"),
        F.lit(Logging)
    )
)

df_trns2 = df_trns2.withColumn(
    "svMbr",
    GetFkeyMbr(
        F.col("svSrcSysCd"),
        F.col("ESI_INVC_EXCPT_SK"),
        F.col("MBR_UNIQ_KEY"),
        F.lit(Logging)
    )
)

df_trns2 = df_trns2.withColumn(
    "svProd",
    GetFkeyProd(
        F.col("svSrcSysCd"),
        F.col("ESI_INVC_EXCPT_SK"),
        F.col("PROD_ID"),
        F.lit(Logging)
    )
)

df_trns2 = df_trns2.withColumn(
    "svPdDate",
    GetFkeyDate(
        F.lit("IDS"),
        F.col("ESI_INVC_EXCPT_SK"),
        F.col("PD_DT_SK"),
        F.lit(Logging)
    )
)

df_trns2 = df_trns2.withColumn(
    "svLOB",
    GetFkeyFnclLob(
        F.lit("PSI"),
        F.col("ESI_INVC_EXCPT_SK"),
        F.col("FNCL_LOB_CD"),
        F.lit(Logging)
    )
)

df_trns2 = df_trns2.withColumn(
    "svErrCnt",
    GetFkeyErrorCnt(
        F.col("ESI_INVC_EXCPT_SK")
    )
)

# Output link "IdsFile" -> ESI_INVC_EXCPT
df_idsfile = df_trns2.select(
    F.col("ESI_INVC_EXCPT_SK"),
    F.col("SRC_SYS_CD_SK"),
    rpad("CLM_ID", <...>, " ").alias("CLM_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("svLOB").alias("FNCL_LOB_SK"),
    F.col("svGrp").alias("GRP_SK"),
    F.col("svMbr").alias("MBR_SK"),
    F.col("svProd").alias("PROD_SK"),
    rpad("svPdDate", 10, " ").alias("PD_DT_SK"),
    rpad("PRCS_DT_SK", 10, " ").alias("PRCS_DT_SK"),
    F.col("ACTL_PD_AMT")
)

write_files(
    df_idsfile,
    f"{adls_path}/load/ESI_INVC_EXCPT.dat",
    ',',
    'overwrite',
    False,
    True,
    '\"',
    None
)

# Output link "Recycle" -> hf_recycle
df_recycle = df_trns2.filter(F.col("svErrCnt") > 0)
df_recycle = df_recycle.withColumn(
    "JOB_EXCTN_RCRD_ERR_SK",
    GetRecycleKey(F.col("ESI_INVC_EXCPT_SK"))
)

df_recycle_out = df_recycle.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "ESI_INVC_EXCPT_SK",
    "SRC_SYS_CD_SK",
    rpad("CLM_ID", <...>, " ").alias("CLM_ID"),
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    rpad("FNCL_LOB_CD", <...>, " ").alias("FNCL_LOB_CD"),
    rpad("GRP_ID", 8, " ").alias("GRP_ID"),
    "MBR_UNIQ_KEY"
)

write_files(
    df_recycle_out,
    f"{adls_path}/hf_recycle.parquet",
    ',',
    'overwrite',
    True,
    True,
    '\"',
    None
)

# Output link "EdwFile" -> ESI_INVC_EXCPT_D
df_edw = df_trns2.withColumn("CRT_RUN_CYC_EXCTN_DT_SK", current_date())
df_edw = df_edw.withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", current_date())

df_edw_out = df_edw.select(
    F.col("ESI_INVC_EXCPT_SK"),
    rpad("svSrcSysCd", <...>, " ").alias("SRC_SYS_CD"),
    rpad("CLM_ID", <...>, " ").alias("CLM_ID"),
    rpad("CRT_RUN_CYC_EXCTN_DT_SK", 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("svLOB").alias("FNCL_LOB_SK"),
    F.col("svGrp").alias("GRP_SK"),
    F.col("svMbr").alias("MBR_SK"),
    F.col("svProd").alias("PROD_SK"),
    F.col("ACTL_PD_AMT").alias("CLM_ACTL_PD_AMT"),
    rpad("svPdDate", 10, " ").alias("CLM_PD_DT_SK"),
    rpad("PRCS_DT_SK", 10, " ").alias("CLM_PRCS_DT_SK"),
    rpad("FNCL_LOB_CD", <...>, " ").alias("FNCL_LOB_CD"),
    rpad("GRP_ID", 8, " ").alias("GRP_ID"),
    F.col("MBR_UNIQ_KEY"),
    rpad("PROD_ID", 18, " ").alias("PROD_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

write_files(
    df_edw_out,
    f"{adls_path}/load/ESI_INVC_EXCPT_D.dat",
    ',',
    'overwrite',
    False,
    True,
    '\"',
    None
)