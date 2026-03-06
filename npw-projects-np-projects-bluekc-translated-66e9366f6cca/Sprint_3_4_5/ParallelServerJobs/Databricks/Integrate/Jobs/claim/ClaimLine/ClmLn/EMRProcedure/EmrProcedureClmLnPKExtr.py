# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2018, 2022 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  EmrProcedureClmLnPKExtr
# MAGIC CALLED BY:  EmrProcedureClmLandSeq
# MAGIC                
# MAGIC 
# MAGIC PROCESSING:   
# MAGIC          *  Assigns / creates claim primary key surrogate keys for all records processed in current run. 
# MAGIC          *  The primary key hash file hf_clm_ln  is the output of this job and is used by the following tables for keying
# MAGIC              CLM_LN
# MAGIC              
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                                     \(9)Date                             \(9)Project/Altiris #\(9)Change Description\(9)\(9)\(9)\(9)\(9)\(9)\(9)Development Project\(9)Code Reviewer\(9)Date Reviewed       
# MAGIC ===========================================================================================================================================================================================
# MAGIC Mrudula Kodali\(9)\(9)\(9)2021-02-22     \(9)\(9)US356530              \(9)Initial Programming                                                     \(9)\(9)\(9)IntegrateDev2                   \(9)Jaideep Mankala        03/03/2021  \(9)
# MAGIC Ken Bradmon\(9)\(9)\(9)2022-12-15\(9)\(9)us542805\(9)\(9)Added new columns: PROC_CD_CPT, \(9)\(9)\(9)\(9)\(9)IntegrateDev2\(9)\(9)Harsha Ravuri\(9)   06/14/2023\(9)\(9)\(9)
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)PROC_CD_CPT_MOD_1, PROC_CD_CPT_MOD_2, \(9)\(9)\(9)IntegrateDev2\(9)\(9)
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)PROC_CD_CPTII_MOD_1, PROC_CD_CPTII_MOD_2, 
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)SNOMED, and CVX.

# MAGIC Get SK for primary key on input record
# MAGIC Job Name:  EmrProcedureClmLnPKExtr
# MAGIC Purpose:  EMR Procedure Claim LinePrimary Key Process
# MAGIC This job loads primary key hash file with all keys used by tables with natural keys of source system code claim ID and seq_no
# MAGIC Output required by container but not used here
# MAGIC This container is used in:
# MAGIC ESIClmLnPKExtr
# MAGIC FctsClmLnPKExtr
# MAGIC FctsClmLnRemitPKExtr
# MAGIC MCSourceClmLnPKExtr
# MAGIC MedicaidClmLnPKExtr
# MAGIC NascoClmLnPKExtr
# MAGIC PcsClmLnPKExtr
# MAGIC WellDyneClmLnPKExtr
# MAGIC MedtrakClmLnPKExtr
# MAGIC BCBSSCClmLnPKExtr
# MAGIC BCBSSCMedClmLnPKExtr
# MAGIC EYEMEDMedClmLnPKExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BinaryType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# Parameter retrieval
SrcSysCd = get_widget_value("SrcSysCd","")
SrcSysCdSK = get_widget_value("SrcSysCdSK","")
CurrRunCycle = get_widget_value("CurrRunCycle","")
RunID = get_widget_value("RunID","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")  # Added per requirement for $IDSOwner

# MAGIC %run ../../../../../../shared_containers/PrimaryKey/ClmLnLoadPK
# COMMAND ----------

# Stage: EmrProcedureEncClmLanding (CSeqFileStage)
schema_EmrProcedureEncClmLanding = StructType([
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
    StructField("SOURCE_ID", BinaryType(), True),
    StructField("MBR_UNIQ_KEY", StringType(), True),
    StructField("CLM_LN_SEQ", StringType(), True)
])

df_EmrProcedureEncClmLanding = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "\u0000")
    .schema(schema_EmrProcedureEncClmLanding)
    .load(f"{adls_path}/verified/" + get_widget_value("InFile_F",""))
)

# Stage: Trans1 (CTransformerStage)
df_Trans1 = df_EmrProcedureEncClmLanding.select(
    F.lit(F.when(F.lit(SrcSysCdSK) != "", SrcSysCdSK).otherwise("0").cast(IntegerType())).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.lit(1).alias("CLM_LN_SEQ_NO")
)

# Stage: hf_emrpro_clmlnpk_dedupe (CHashedFileStage) - Scenario A (Intermediate hashed file)
# Replace hashed file with dedup logic on key columns: SRC_SYS_CD_SK, CLM_ID, CLM_LN_SEQ_NO
df_dedup = df_Trans1.dropDuplicates(["SRC_SYS_CD_SK","CLM_ID","CLM_LN_SEQ_NO"])

# Stage: ClmLnLoadPK (CContainerStage)
container_params = {
    "SrcSysCd": SrcSysCd,
    "SrcSysCdSK": SrcSysCdSK,
    "CurrRunCycle": CurrRunCycle,
    "IDSOwner": IDSOwner
}
df_pk_clmsln = ClmLnLoadPK(df_dedup, container_params)

# Stage: hf_clm_ln_pk_lkup (CHashedFileStage) - Scenario C
# Final select with the same column order as the container output pins
df_final = df_pk_clmsln.select(
    "SRC_SYS_CD",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "CLM_LN_SK"
)

write_files(
    df_final,
    "hf_clm_ln_pk_lkup.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)