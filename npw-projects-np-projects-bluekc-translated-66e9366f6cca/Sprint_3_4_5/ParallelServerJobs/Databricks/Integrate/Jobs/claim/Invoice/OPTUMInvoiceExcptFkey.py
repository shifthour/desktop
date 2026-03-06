# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC COPYRIGHT 2019 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:   Set foreign key values for OPTUMRX exception table.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                    Date                       Change Description                                         Project #                           Development Project          Code Reviewer               Date Reviewed  
# MAGIC ------------------           ----------------------------        -------------------------------------------------------------------      ----------------                           ------------------------------------       ----------------------------           ----------------
# MAGIC Deepa Bajaj            10-28-2019                  Original programming                                      6131 - PBM Replacement  IntegrateDev2                     Kalyan Neelam                2019-11-27

# MAGIC Load file for EDW table
# MAGIC OPTUMRX Invoice Exception Foreign Key
# MAGIC Load file for IDS table
# MAGIC Input file created in OPTUMRXInvoiceExcptTrans
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


EDWFilePath = get_widget_value('EDWFilePath','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
RunCycle = get_widget_value('RunCycle','')
CurrentDate = get_widget_value('CurrentDate','')
Logging = get_widget_value('Logging','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

schema_OPTUMRX_Invoice_Unmatched = StructType([
    StructField("ESI_INVC_EXCPT_SK", IntegerType(), False),
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("CLM_ID", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("FNCL_LOB_CD", StringType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("MBR_UNIQ_KEY", IntegerType(), False),
    StructField("PROD_ID", StringType(), False),
    StructField("PD_DT_SK", StringType(), False),
    StructField("PRCS_DT_SK", StringType(), False),
    StructField("ACTL_PD_AMT", DecimalType(38,10), False),
    StructField("DT_FILLED", StringType(), False)
])

df_OPTUMRX_Invoice_Unmatched = (
    spark.read
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_OPTUMRX_Invoice_Unmatched)
    .csv(f"{adls_path}/key/OPTUMRX_Invoice_trans.dat.{RunID}")
)

temp_SrcSysCd = trim(SrcSysCd)
if not temp_SrcSysCd or temp_SrcSysCd == '':
    svSrcSysCd_value = "UNK"
elif temp_SrcSysCd in [
    "PCT","WELLDYNERX","PCS","CAREMARK","ARGUS","EDC","OT@2","ADOL","MOHSAIC",
    "CAREADVANCE","ESI","OPTUMRX","MCSOURCE","MCAID","MEDTRAK","BCBSSC","BCA",
    "BCBSA","SAVRX","LDI","EYEMED","CVS","LUMERIS","MEDIMPACT"
]:
    svSrcSysCd_value = "FACETS"
else:
    svSrcSysCd_value = temp_SrcSysCd

df_stagevar = (
    df_OPTUMRX_Invoice_Unmatched
    .withColumn("svSrcSysCd", F.lit(svSrcSysCd_value))
    .withColumn("svGrp", GetFkeyGrp(F.col("svSrcSysCd"), F.col("ESI_INVC_EXCPT_SK"), F.col("GRP_ID"), F.lit(Logging)))
    .withColumn("svMbr", GetFkeyMbr(F.col("svSrcSysCd"), F.col("ESI_INVC_EXCPT_SK"), F.col("MBR_UNIQ_KEY"), F.lit(Logging)))
    .withColumn("svProd", GetFkeyProd(F.col("svSrcSysCd"), F.col("ESI_INVC_EXCPT_SK"), F.col("PROD_ID"), F.lit(Logging)))
    .withColumn("svPdDate", GetFkeyDate(F.lit("IDS"), F.col("ESI_INVC_EXCPT_SK"), F.col("PD_DT_SK"), F.lit(Logging)))
    .withColumn("svLOB", GetFkeyFnclLob(F.lit("PSI"), F.col("ESI_INVC_EXCPT_SK"), F.col("FNCL_LOB_CD"), F.lit(Logging)))
    .withColumn("svErrCnt", GetFkeyErrorCnt(F.col("ESI_INVC_EXCPT_SK")))
)

df_IdsFile_unfiltered = df_stagevar
df_IdsFile = df_IdsFile_unfiltered.selectExpr(
    "ESI_INVC_EXCPT_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "svLOB as FNCL_LOB_SK",
    "svGrp as GRP_SK",
    "svMbr as MBR_SK",
    "svProd as PROD_SK",
    "svPdDate as PD_DT_SK",
    "PRCS_DT_SK",
    "ACTL_PD_AMT"
)

df_Recycle_raw = df_stagevar.filter(F.col("svErrCnt") > 0)
df_Recycle = df_Recycle_raw.selectExpr(
    "GetRecycleKey(ESI_INVC_EXCPT_SK) as JOB_EXCTN_RCRD_ERR_SK",
    "ESI_INVC_EXCPT_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "FNCL_LOB_CD",
    "GRP_ID",
    "MBR_UNIQ_KEY"
)

df_EdwFile = df_stagevar.selectExpr(
    "ESI_INVC_EXCPT_SK",
    f"'{SrcSysCd}' as SRC_SYS_CD",
    "CLM_ID",
    f"'{CurrentDate}' as CRT_RUN_CYC_EXCTN_DT_SK",
    f"'{CurrentDate}' as LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "svLOB as FNCL_LOB_SK",
    "svGrp as GRP_SK",
    "svMbr as MBR_SK",
    "svProd as PROD_SK",
    "ACTL_PD_AMT as CLM_ACTL_PD_AMT",
    "svPdDate as CLM_PD_DT_SK",
    "PRCS_DT_SK as CLM_PRCS_DT_SK",
    "FNCL_LOB_CD",
    "GRP_ID",
    "MBR_UNIQ_KEY",
    "PROD_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK"
)

df_IdsFile_out = (
    df_IdsFile
    .withColumn("PD_DT_SK", rpad(F.col("PD_DT_SK"), 10, " "))
    .withColumn("PRCS_DT_SK", rpad(F.col("PRCS_DT_SK"), 10, " "))
    .select(
        "ESI_INVC_EXCPT_SK",
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "FNCL_LOB_SK",
        "GRP_SK",
        "MBR_SK",
        "PROD_SK",
        "PD_DT_SK",
        "PRCS_DT_SK",
        "ACTL_PD_AMT"
    )
)

write_files(
    df_IdsFile_out,
    f"{adls_path}/load/OPTUMRX_INVC_EXCPT.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_Recycle_out = (
    df_Recycle
    .withColumn("GRP_ID", rpad(F.col("GRP_ID"), 8, " "))
    .select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "ESI_INVC_EXCPT_SK",
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "FNCL_LOB_CD",
        "GRP_ID",
        "MBR_UNIQ_KEY"
    )
)

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

df_EdwFile_out = (
    df_EdwFile
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("CLM_PD_DT_SK", rpad(F.col("CLM_PD_DT_SK"), 10, " "))
    .withColumn("CLM_PRCS_DT_SK", rpad(F.col("CLM_PRCS_DT_SK"), 10, " "))
    .withColumn("GRP_ID", rpad(F.col("GRP_ID"), 8, " "))
    .select(
        "ESI_INVC_EXCPT_SK",
        "SRC_SYS_CD",
        "CLM_ID",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "FNCL_LOB_SK",
        "GRP_SK",
        "MBR_SK",
        "PROD_SK",
        "CLM_ACTL_PD_AMT",
        "CLM_PD_DT_SK",
        "CLM_PRCS_DT_SK",
        "FNCL_LOB_CD",
        "GRP_ID",
        "MBR_UNIQ_KEY",
        "PROD_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK"
    )
)

write_files(
    df_EdwFile_out,
    f"{adls_path}/{EDWFilePath}/load/OPTUMRX_INVC_EXCPT_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)