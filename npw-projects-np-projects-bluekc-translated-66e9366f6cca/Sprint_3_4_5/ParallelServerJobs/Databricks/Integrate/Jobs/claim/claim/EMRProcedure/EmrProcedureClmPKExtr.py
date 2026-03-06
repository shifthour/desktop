# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2018, 2021, 2022 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  EmrProcedureClmPKExtr
# MAGIC CALLED BY:  EmrProcedureClmLandSeq
# MAGIC                
# MAGIC 
# MAGIC PROCESSING:   
# MAGIC          *  Assigns / creates claim primary key surrogate keys for all records processed in current run.  
# MAGIC          *  The primary key hash file hf_clm is the output of this job and is used by the following tables for keying
# MAGIC              CLM
# MAGIC            
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer\(9)\(9)Date\(9)\(9)Project/Altiris #\(9)Change Description\(9)\(9)\(9)\(9)\(9)\(9)Development Project\(9)\(9)Code Reviewer\(9)Date Reviewed       
# MAGIC ==============================================================================================================================================================================================
# MAGIC Mrudula Kodali \(9)\(9)2021-02-22      \(9)US356530          \(9)Initial Programming                                             \(9)\(9)\(9)IntegrateDev2\(9)\(9)\(9)Jaideep Mankala\(9)03/03/2021 \(9)
# MAGIC Ken Bradmon\(9)\(9)2022-12-15\(9)us542805\(9)\(9)Added new columns: PROC_CD_CPT, \(9)\(9)\(9)\(9)IntegrateDev2\(9)\(9)\(9)Harsha Ravuri\(9)06/14/2023\(9)\(9)\(9)
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)PROC_CD_CPT_MOD_1, PROC_CD_CPT_MOD_2,\(9)\(9)\(9)IntegrateDev2\(9)\(9)
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)PROC_CD_CPTII_MOD_1, PROC_CD_CPTII_MOD_2, 
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)SNOMED, and CVX.

# MAGIC Job Name:  EmrProcedureClmPKExtr
# MAGIC Purpose:  EMR Procedure Claim SK Assignment
# MAGIC This job loads primary key hash file with all keys used by tables with natural keys of source system code and claim ID
# MAGIC Output required by container but not used here
# MAGIC Get SK for records without keys
# MAGIC This container is used in:
# MAGIC ESIClmInvoicePKExtr
# MAGIC ESIClmPKExtr
# MAGIC FctsClmPKExtr
# MAGIC FctsPcaClmPKExtr
# MAGIC MCSourceClmPKExtr
# MAGIC MedicaidClmPKExtr
# MAGIC NascoClmExtr
# MAGIC NascoClmPKExtr
# MAGIC PcsClmPKExtr
# MAGIC WellDyneClmPKExtr
# MAGIC MedtrakClmPKExtr
# MAGIC BCBSSCClmPKExtr
# MAGIC EYEMEDClmPKExtr
# MAGIC BCBSSCMedClmPKExtr
# MAGIC EmrProcedureClmPKExtr
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLoadPK
# COMMAND ----------

param_SrcSysCdSK = get_widget_value('SrcSysCdSK','')
param_CurrRunCycle = get_widget_value('CurrRunCycle','1')
param_RunID = get_widget_value('RunID','100')
param_$IDSOwner = get_widget_value('$IDSOwner','')
param_ids_secret_name = get_widget_value('ids_secret_name','')
param_InFile_F = get_widget_value('InFile_F','')
param_SrcSysCd = get_widget_value('SrcSysCd','')

schema_EmrProcedureClmLanding = StructType([
    StructField("CLM_ID", StringType(), True),
    StructField("POL_NO", StringType(), True),
    StructField("PATN_LAST_NM", StringType(), True),
    StructField("PATN_FIRST_NM", StringType(), True),
    StructField("PATN_MID_NM", StringType(), True),
    StructField("MBI", StringType(), True),
    StructField("DOB", StringType(), True),
    StructField("GNDR", StringType(), True),
    StructField("PROC_TYPE", StringType(), True),
    StructField("DT_OF_SVC", StringType(), True),
    StructField("RSLT_VAL", StringType(), True),
    StructField("RNDR_NTNL_PROV_ID", StringType(), True),
    StructField("RNDR_PROV_TYP", StringType(), True),
    StructField("PROC_CD_CPT", StringType(), True),
    StructField("PROC_CD_CPT_MOD_1", StringType(), True),
    StructField("PROC_CD_CPT_MOD_2", StringType(), True),
    StructField("PROC_CD_CPTII_MOD_1", StringType(), True),
    StructField("PROC_CD_CPTII_MOD_2", StringType(), True),
    StructField("SNOMED", StringType(), True),
    StructField("CVX", StringType(), True),
    StructField("SOURCE_ID", StringType(), True),
    StructField("MBR_UNIQ_KEY", StringType(), True),
    StructField("CLM_LN_SEQ", StringType(), True)
])

df_EmrClmLanding = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\u0000")
    .schema(schema_EmrProcedureClmLanding)
    .load(f"{adls_path}/verified/{param_InFile_F}")
)

df_Trans1 = df_EmrClmLanding.select(
    lit(param_SrcSysCdSK).cast(IntegerType()).alias("SRC_SYS_CD_SK"),
    col("CLM_ID").alias("CLM_ID")
)

df_dedup1 = dedup_sort(
    df_Trans1,
    ["SRC_SYS_CD_SK", "CLM_ID"],
    []
)

params_ClmLoadPK = {
    "SrcSysCd": param_SrcSysCd,
    "SrcSysCdSK": param_SrcSysCdSK,
    "CurrRunCycle": param_CurrRunCycle,
    "$IDSOwner": param_$IDSOwner
}
df_ClmLoadPK = ClmLoadPK(df_dedup1, params_ClmLoadPK)

df_final = df_ClmLoadPK.select(
    rpad(col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    rpad(col("CLM_ID"), <...>, " ").alias("CLM_ID"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("CLM_SK").alias("CLM_SK")
)

write_files(
    df_final,
    f"{adls_path}/hf_clm_pk_lkup.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)