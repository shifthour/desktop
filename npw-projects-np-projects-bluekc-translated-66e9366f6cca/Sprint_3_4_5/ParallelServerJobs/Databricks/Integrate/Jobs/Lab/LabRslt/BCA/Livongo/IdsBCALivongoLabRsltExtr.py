# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2021, 2022 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Job Name:  IdsBCALivongoLabRsltExtr
# MAGIC Called by: IdsBCALivongoLabRsltCntl
# MAGIC                    
# MAGIC 
# MAGIC Processing: Business rules are applied, K table logic is done through shared container.
# MAGIC                     
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:  Restore the source file before re-running the job
# MAGIC                     Previous Run Aborted: Restart, no other steps necessary     
# MAGIC 
# MAGIC Modifications:                        
# MAGIC \(9)\(9)\(9)\(9)\(9)Project/                                                                                                                             \(9)\(9)\(9)Code                   \(9)\(9)Date
# MAGIC Developer\(9)\(9)Date\(9)\(9)Altiris #\(9)\(9)Change Description                                                                                       \(9)\(9)Reviewer            \(9)\(9)Reviewed
# MAGIC ===============================================================================================================================================================================
# MAGIC Reddy Sanam\(9)\(9)2024-01-30\(9) US610374\(9)Original Programming.                                                                                                 Goutham Kalidindi                          2/26/2024                  \(9)            \(9)\(9)


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
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
    DecimalType
)
from pyspark.sql.functions import (
    col,
    lit,
    when,
    trim,
    upper,
    lag,
    concat_ws,
    monotonically_increasing_id,
    rpad
)
from pyspark.sql import Window
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../../shared_containers/PrimaryKey/LabRsltPK
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RowPassThru = get_widget_value('RowPassThru','Y')
CurrRunDt = get_widget_value('CurrRunDt','02-16-2024')
SrcSysCd = get_widget_value('SrcSysCd','BCA')
SrcSysCdSk = get_widget_value('SrcSysCdSk','1580')
RunId = get_widget_value('RunId','100')
CurrRunCycle = get_widget_value('CurrRunCycle','100')
SrcFileName = get_widget_value('SrcFileName','')

# --------------------------------------------------------------------------------
# Stage: BCALivongoLabFinalExtract (CSeqFileStage) - Reading from verified/BCA_LIVONGO_LAB_FORMATTED_LABFile.dat
# --------------------------------------------------------------------------------
schema_BCALivongoLabFinalExtract = StructType([
    StructField("FEP_MBR_ID", StringType(), True),
    StructField("CNTR_ID", StringType(), True),
    StructField("MBR_FIRST_NM", StringType(), True),
    StructField("MBR_MID_NM", StringType(), True),
    StructField("MBR_LAST_NM", StringType(), True),
    StructField("MBR_DOB", StringType(), True),
    StructField("RPTNG_PLN_CD", StringType(), True),
    StructField("CLM_ID", StringType(), True),
    StructField("CLM_LN_NO", StringType(), True),
    StructField("ORDER_PROV_ID", StringType(), True),
    StructField("ORDER_PROV_NPI", StringType(), True),
    StructField("SVC_DT", StringType(), True),
    StructField("PROC_CD", StringType(), True),
    StructField("PROC_CD_TYP", StringType(), True),
    StructField("TST_CD", StringType(), True),
    StructField("TST_CD_TYP", StringType(), True),
    StructField("LAB_VAL_NUM", DecimalType(10, 2), True),
    StructField("MEASURING_UNIT", StringType(), True),
    StructField("LAB_VAL_BINARY", StringType(), True),
    StructField("STTUS", DecimalType(10, 2), True),
    StructField("RSLT_TYP", StringType(), True),
    StructField("RSLT_VAL_FLAG", StringType(), True),
    StructField("TYP", StringType(), True),
    StructField("DATA_TYP", StringType(), True),
    StructField("SRC_TYP", StringType(), True),
    StructField("SRC_SYS_NM", StringType(), True),
    StructField("MBR_UNIQ_KEY", IntegerType(), False),
    StructField("MBR_SK", IntegerType(), False),
    StructField("PROV_SK", IntegerType(), False)
])

df_BCALivongoLabFinalExtract = (
    spark.read
    .option("header", "true")
    .option("sep", ",")
    .option("quote", "\u0000")
    .schema(schema_BCALivongoLabFinalExtract)
    .csv(f"{adls_path}/verified/BCA_LIVONGO_LAB_FORMATTED_LABFile.dat")
)

# --------------------------------------------------------------------------------
# Stage: Xfm_Source_System_cd (CTransformerStage)
# --------------------------------------------------------------------------------
df_Xfm_Source_System_cd = df_BCALivongoLabFinalExtract.select(
    col("FEP_MBR_ID").alias("FEP_MBR_ID"),
    col("CNTR_ID").alias("CNTR_ID"),
    col("MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    col("MBR_MID_NM").alias("MBR_MID_NM"),
    col("MBR_LAST_NM").alias("MBR_LAST_NM"),
    col("MBR_DOB").alias("MBR_DOB"),
    col("RPTNG_PLN_CD").alias("RPTNG_PLN_CD"),
    col("CLM_ID").alias("CLM_ID"),
    col("CLM_LN_NO").alias("CLM_LN_NO"),
    col("ORDER_PROV_ID").alias("ORDER_PROV_ID"),
    col("ORDER_PROV_NPI").alias("ORDER_PROV_NPI"),
    col("SVC_DT").alias("SVC_DT"),
    col("PROC_CD").alias("PROC_CD"),
    col("PROC_CD_TYP").alias("PROC_CD_TYP"),
    col("TST_CD").alias("TST_CD"),
    col("TST_CD_TYP").alias("TST_CD_TYP"),
    col("LAB_VAL_NUM").alias("LAB_VAL_NUM"),
    col("MEASURING_UNIT").alias("MEASURING_UNIT"),
    col("LAB_VAL_BINARY").alias("LAB_VAL_BINARY"),
    col("STTUS").alias("STTUS"),
    col("RSLT_TYP").alias("RSLT_TYP"),
    col("RSLT_VAL_FLAG").alias("RSLT_VAL_FLAG"),
    col("TYP").alias("TYP"),
    col("DATA_TYP").alias("DATA_TYP"),
    col("SRC_TYP").alias("SRC_TYP"),
    # Overwrite SRC_SYS_NM with a constant 'BCA'
    lit("BCA").alias("SRC_SYS_NM"),
    col("MBR_SK").alias("MBR_SK"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("PROV_SK").alias("PROV_SK")
)

# --------------------------------------------------------------------------------
# Stage: Business_Rules (CTransformerStage)
#   We replicate the stage-variable logic, including consecutive-duplicate checks.
# --------------------------------------------------------------------------------

# 1) Add a row id to preserve input order
df_temp = df_Xfm_Source_System_cd.withColumn("_row_id", monotonically_increasing_id())

# 2) Compute stage variables
df_temp = df_temp.withColumn(
    "svCPTCd",
    when(
        (col("PROC_CD").isNull()) | (trim(col("PROC_CD")) == ""),
        lit("NA")
    ).otherwise(upper(trim(col("PROC_CD"))))
).withColumn(
    "svProcTypCd",
    when(
        (col("PROC_CD_TYP").isNull()) | (trim(col("PROC_CD_TYP")) == ""),
        lit("NA")
    ).otherwise(upper(trim(col("PROC_CD_TYP"))))
).withColumn(
    "svLoincCd",
    when(
        (col("TST_CD").isNull()) | (trim(col("TST_CD")) == ""),
        lit("NA")
    ).otherwise(upper(trim(col("TST_CD"))))
).withColumn(
    "svRsltId",
    lit("NA")
).withColumn(
    "svPatnEncntrId",
    when(col("CLM_ID").isNull(), lit("")).otherwise(col("CLM_ID"))
).withColumn(
    "svTstRslt",
    col("LAB_VAL_NUM")
).withColumn(
    "svMbrUniqKey",
    col("MBR_UNIQ_KEY")
)

# Build svKey
df_temp = df_temp.withColumn(
    "svKey",
    when(col("MBR_UNIQ_KEY").isNull(), lit("UNK")).otherwise(col("MBR_UNIQ_KEY").cast(StringType()))
).cast(StringType())\
    .alias("svKey")  # intermediate
df_temp = df_temp.withColumn(
    "svKey",
    col("svKey") + lit(";")
    + when(col("SVC_DT").isNull(), lit("")).otherwise(col("SVC_DT")) + lit(";")
    + when(col("svCPTCd").isNull(), lit("")).otherwise(col("svCPTCd")) + lit(";")
    + lit("NA;")
    + when(col("svLoincCd").isNull(), lit("")).otherwise(col("svLoincCd")) + lit(";")
    + lit("NA;NA;")
    + when(col("svRsltId").isNull(), lit("")).otherwise(col("svRsltId")) + lit(";")
    + lit("NA;")
    + when(col("svPatnEncntrId").isNull(), lit("")).otherwise(col("svPatnEncntrId")) + lit(";")
    + when(lit(SrcSysCd).isNull(), lit("")).otherwise(lit(SrcSysCd))
)

# 3) Use a window to compare svKey with previous row's svKey
w = Window.orderBy("_row_id")
df_temp = df_temp.withColumn(
    "svPrevKey",
    lag(col("svKey"), 1).over(w)
).withColumn(
    "svDupChk",
    when(col("svKey") == col("svPrevKey"), lit("Y")).otherwise(lit("N"))
)

# 4) Filter ("Constraint") where svDupChk = 'N'
df_filtered = df_temp.filter(col("svDupChk") == "N")

# --------------------------------------------------------------------------------
# Output pin "AllCol"
# --------------------------------------------------------------------------------
df_BR_AllCol = df_filtered.select(
    when(col("svMbrUniqKey").isNull(), lit(0)).otherwise(col("svMbrUniqKey")).alias("MBR_UNIQ_KEY"),
    when(
        (col("SVC_DT").isNull()) | (trim(col("SVC_DT")) == ""),
        lit("NA")
    ).otherwise(upper(trim(col("SVC_DT")))).alias("SVC_DT_SK"),
    col("svCPTCd").alias("PROC_CD"),
    col("PROC_CD_TYP").alias("PROC_CD_TYP_CD"),
    col("svLoincCd").alias("LOINC_CD"),
    lit("NA").alias("DIAG_CD_1"),
    lit("NA").alias("DIAG_CD_TYP_CD_1"),
    col("svRsltId").alias("RSLT_ID"),
    lit("NA").alias("ORDER_TST_ID"),
    col("svPatnEncntrId").alias("PATN_ENCNTR_ID"),
    lit(SrcSysCd).alias("SRC_SYS_CD"),
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit("I").alias("INSRT_UPDT_CD"),
    col("svCPTCd").alias("SVRC_PROVIDED_CD_TRIMMED"),
    lit("N").alias("DISCARD_IN"),
    lit(RowPassThru).alias("PASS_THRU_IN"),
    lit(CurrRunDt).alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    # PRI_KEY_STRING
    when(col("MBR_UNIQ_KEY").isNull(), lit("UNK")).otherwise(col("MBR_UNIQ_KEY").cast(StringType()))
    .cast(StringType())
    + lit(";")
    + when(col("SVC_DT").isNull(), lit("")).otherwise(col("SVC_DT")) + lit(";")
    + when(col("svCPTCd").isNull(), lit("")).otherwise(col("svCPTCd")) + lit(";")
    + col("svProcTypCd") + lit(";")
    + when(col("svLoincCd").isNull(), lit("")).otherwise(col("svLoincCd")) + lit(";")
    + lit("NA;NA;")
    + when(col("svRsltId").isNull(), lit("")).otherwise(col("svRsltId")) + lit(";")
    + lit("NA;")
    + when(col("svPatnEncntrId").isNull(), lit("")).otherwise(col("svPatnEncntrId")) + lit(";")
    + when(lit(SrcSysCd).isNull(), lit("")).otherwise(lit(SrcSysCd))
    .alias("PRI_KEY_STRING"),
    lit("NA").alias("DIAG_CD_2"),
    lit("NA").alias("DIAG_CD_3"),
    lit("NA").alias("SVRC_PROV_ID"),
    lit("UNK").alias("NORM_RSLT_IN"),
    when(
        (col("SVC_DT").isNull()) | (trim(col("SVC_DT")) == "") | (trim(col("SVC_DT")) == "NA"),
        lit("1753-01-01")
    ).otherwise(col("SVC_DT")).alias("ORDER_DT_SK"),
    # SRC_SYS_EXTR_DT_SK
    concat_ws(
        "-",
        col("SrcFileName").substr( (lit("_").eqNullSafe(lit("_"))*0 + 1), 4),
        col("SrcFileName").substr( (lit("_").eqNullSafe(lit("_"))*0 + 5), 2),
        col("SrcFileName").substr( (lit("_").eqNullSafe(lit("_"))*0 + 7), 2)
    ).alias("SRC_SYS_EXTR_DT_SK"),
    col("svTstRslt").alias("NUM_RSLT_VAL"),
    lit(0).alias("RSLT_NORM_HI_VAL"),
    lit(0).alias("RSLT_NORM_LOW_VAL"),
    lit("UNK").alias("ORDER_TST_NM"),
    lit("UNK").alias("RSLT_DESC"),
    lit("NA").alias("RSLT_LONG_DESC_1"),
    lit("NA").alias("RSLT_LONG_DESC_2"),
    col("MEASURING_UNIT").alias("RSLT_MESR_UNIT_DESC"),
    lit("NA").alias("RSLT_RNG_DESC"),
    lit("NA").alias("SPCMN_ID"),
    when(
        (col("ORDER_PROV_ID").isNull()) | (trim(col("ORDER_PROV_ID")) == ""),
        lit("NA")
    ).otherwise(upper(trim(col("ORDER_PROV_ID")))).alias("SRC_SYS_ORDER_PROV_ID"),
    col("svCPTCd").alias("SRC_SYS_PROC_CD_TX"),
    lit(None).alias("TX_RSLT_VAL"),
    lit("NA").alias("ORDER_PROV_SRC_SYS_CD")
)

# --------------------------------------------------------------------------------
# Output pin "Transform"
# --------------------------------------------------------------------------------
df_BR_Transform = df_filtered.select(
    when(col("svMbrUniqKey").isNull(), lit(0)).otherwise(col("svMbrUniqKey")).alias("MBR_UNIQ_KEY"),
    when(
        (col("SVC_DT").isNull()) | (trim(col("SVC_DT")) == ""),
        lit("NA")
    ).otherwise(upper(trim(col("SVC_DT")))).alias("SVC_DT_SK"),
    col("svCPTCd").alias("PROC_CD"),
    col("PROC_CD_TYP").alias("PROC_CD_TYP_CD"),
    col("svLoincCd").alias("LOINC_CD"),
    lit("NA").alias("DIAG_CD_1"),
    lit("NA").alias("DIAG_CD_TYP_CD_1"),
    col("svRsltId").alias("RSLT_ID"),
    lit("NA").alias("ORDER_TST_ID"),
    col("svPatnEncntrId").alias("PATN_ENCNTR_ID"),
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK")
)

# --------------------------------------------------------------------------------
# Stage: LabRsltPKC114 (CContainerStage) referencing shared container "LabRsltPK"
# --------------------------------------------------------------------------------
params_LabRsltPKC114 = {
    "IDSOwner": IDSOwner,
    "CurrRunCycle": CurrRunCycle,
    "RowPassThru": RowPassThru,
    "CurrRunDt": CurrRunDt,
    "RunId": RunId,
    "SrcSysCd": SrcSysCd
}

df_LabRsltPKC114 = LabRsltPK(
    df_BR_Transform,
    df_BR_AllCol,
    params_LabRsltPKC114
)

# --------------------------------------------------------------------------------
# Stage: BCALivongoLab (CSeqFileStage) - Write file
# --------------------------------------------------------------------------------
# Select final columns in order, applying rpad for char/varchar columns
df_final = df_LabRsltPKC114.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("LAB_RSLT_SK"),
    col("MBR_UNIQ_KEY"),
    rpad(col("SVC_DT_SK"), 10, " ").alias("SVC_DT_SK"),
    rpad(col("PROC_CD"), 10, " ").alias("PROC_CD"),
    rpad(col("PROC_CD_TYP_CD"), 10, " ").alias("PROC_CD_TYP_CD"),
    rpad(col("LOINC_CD"), 20, " ").alias("LOINC_CD"),
    col("DIAG_CD_1"),
    col("DIAG_CD_TYP_CD_1"),
    col("RSLT_ID"),
    col("ORDER_TST_ID"),
    col("PATN_ENCNTR_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("DIAG_CD_2"),
    col("DIAG_CD_3"),
    col("SRVC_PROV_ID"),
    rpad(col("NORM_RSLT_IN"), 1, " ").alias("NORM_RSLT_IN"),
    rpad(col("ORDER_DT_SK"), 10, " ").alias("ORDER_DT_SK"),
    rpad(col("SRC_SYS_EXTR_DT_SK"), 10, " ").alias("SRC_SYS_EXTR_DT_SK"),
    col("NUM_RSLT_VAL"),
    col("RSLT_NORM_HI_VAL"),
    col("RSLT_NORM_LOW_VAL"),
    col("ORDER_TST_NM"),
    col("RSLT_DESC"),
    col("RSLT_LONG_DESC_1"),
    col("RSLT_LONG_DESC_2"),
    rpad(col("RSLT_MESR_UNIT_DESC"), 20, " ").alias("RSLT_MESR_UNIT_DESC"),
    col("RSLT_RNG_DESC"),
    col("SPCMN_ID"),
    rpad(col("SRC_SYS_ORDER_PROV_ID"), 100, " ").alias("SRC_SYS_ORDER_PROV_ID"),
    col("SRC_SYS_PROC_CD_TX"),
    col("TX_RSLT_VAL"),
    col("ORDER_PROV_SRC_SYS_CD")
)

write_files(
    df_final,
    f"{adls_path}/key/LabRsltExtr.BCA.LabRslt.dat.{RunId}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)