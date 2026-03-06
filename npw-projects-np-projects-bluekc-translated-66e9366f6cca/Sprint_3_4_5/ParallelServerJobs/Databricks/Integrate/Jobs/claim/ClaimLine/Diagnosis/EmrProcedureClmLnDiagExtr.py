# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2015, 2021, 2022 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:  EmrProcedureClmLnDiagExtr
# MAGIC CALLED BY:  EmrProcedureClmLnDiagExtr
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC     Reads the EMR Procedure file and runs through primary key using shared container ClmLnDiagPK
# MAGIC     
# MAGIC 
# MAGIC Developer 		Date		Project/Altiris #	Change Description							Development Project	Code Reviewer		Date Reviewed       
# MAGIC =============================================================================================================================================================================================
# MAGIC Vikas A              		2021-03-01	Risk Adjestment    	Initial Programming                                             				IntegrateDev2                 	Jaideep Mankala		03/04/2021        
# MAGIC Ken Bradmon		2022-12-15	us542805		Added new columns: PROC_CD_CPT, 					IntegrateDev1		Harsha Ravuri		06/14/2023		
# MAGIC 							PROC_CD_CPT_MOD_1, PROC_CD_CPT_2, 
# MAGIC 							PROC_CD_CPTII_MOD2, PROC_CD_CPTII_MOD2, 
# MAGIC 							SNOMED, and CVX.

# MAGIC This container is used in:
# MAGIC FctsClmLnDiagExtr
# MAGIC NascoClmLnDiagExtr
# MAGIC BCBSSCClmLnDiagExtr
# MAGIC EyeMedClmLnDiagExtr
# MAGIC EyeMedMAClmLnDiagExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Apply business logic
# MAGIC Assign primary surrogate key
# MAGIC Writing Sequential File to ../key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, when, lit, trim, rpad, regexp_replace
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------

CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrentDate = get_widget_value('CurrentDate','')
InFile_F = get_widget_value('InFile_F','')
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
query_IDS_Ext = f"SELECT TRIM(SRC_DOMAIN_TX) AS SRC_DOMAIN_TX, CASE WHEN TRIM(DOMAIN_ID)='DIAGNOSIS_CODE_ICD9' THEN 'ICD9' ELSE 'ICD10' END AS DOMAIN_ID_TXT, TRIM(TRGT_DOMAIN_TX) AS TRGT_DOMAIN_TX FROM {IDSOwner}.P_SRC_DOMAIN_TRNSLTN WHERE SRC_SYS_CD = 'PROVGRP'  AND DOMAIN_ID IN ('DIAGNOSIS_CODE_ICD9','DIAGNOSIS_CODE_ICD10')"
df_IDS_Ext = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_IDS_Ext)
    .load()
)
df_hf_emr_procedure_src_dmn_trans = dedup_sort(
    df_IDS_Ext,
    ["SRC_DOMAIN_TX","DOMAIN_ID_TXT"],
    []
)
schemaProcedure_Data = StructType([
    StructField("CLM_ID",StringType(),True),
    StructField("POL_NO",StringType(),True),
    StructField("PATN_LAST_NM",StringType(),True),
    StructField("PATN_FIRST_NM",StringType(),True),
    StructField("PATN_MID_NM",StringType(),True),
    StructField("MBI",StringType(),True),
    StructField("DOB",StringType(),True),
    StructField("GNDR",StringType(),True),
    StructField("PROC_TYPE",StringType(),True),
    StructField("DT_OF_SVC",StringType(),True),
    StructField("RSLT_VAL",StringType(),True),
    StructField("RNDR_NTNL_PROV_ID",StringType(),True),
    StructField("RNDR_PROV_TYP",StringType(),True),
    StructField("PROC_CD_CPT",StringType(),False),
    StructField("PROC_CD_CPT_MOD_1",StringType(),False),
    StructField("PROC_CD_CPT_MOD_2",StringType(),False),
    StructField("PROC_CD_CPTII_MOD_1",StringType(),False),
    StructField("PROC_CD_CPTII_MOD_2",StringType(),False),
    StructField("SNOMED",StringType(),False),
    StructField("CVX",StringType(),False),
    StructField("SOURCE_ID",StringType(),True),
    StructField("MBR_UNIQ_KEY",StringType(),True),
    StructField("CLM_LN_SEQ",StringType(),True)
])
df_Procedure_Data = (
    spark.read
    .option("header","false")
    .option("delimiter",",")
    .option("quote","\"")
    .schema(schemaProcedure_Data)
    .csv(f"{adls_path}/verified/{InFile_F}")
)
df_BusinessRules = df_Procedure_Data.select(
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit("I").alias("INSRT_UPDT_CD"),
    lit("N").alias("DISCARD_IN"),
    lit("Y").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    lit(SrcSysCd).alias("SRC_SYS_CD"),
    (lit(SrcSysCd) + lit(";") + col("CLM_ID") + lit(";") + lit("1")).alias("PRI_KEY_STRING"),
    lit(0).alias("CLM_DIAG_SK"),
    col("CLM_ID").alias("CLM_ID"),
    lit("1").alias("CLM_DIAG_ORDNL_CD"),
    lit(0).alias("CRT_RUN_CYC_EXTCN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
    lit("NA").alias("CLM_DIAG_POA_CD"),
    when((col("DT_OF_SVC").isNotNull()) & (col("DT_OF_SVC") < lit("20151001")), lit("ICD9"))
    .when((col("DT_OF_SVC").isNotNull()) & (col("DT_OF_SVC") >= lit("20151001")), lit("ICD10"))
    .otherwise(lit("UNK")).alias("DIAG_CD_TYP_CD"),
    trim(col("PROC_TYPE")).alias("PROC_TYPE"),
    col("CLM_LN_SEQ").alias("CLM_LN_SEQ")
)
df_Snapshot = df_BusinessRules.alias("TransformIt").join(
    df_hf_emr_procedure_src_dmn_trans.alias("Lkp_Src_Domain"),
    (
        trim(col("TransformIt.PROC_TYPE")) == col("Lkp_Src_Domain.SRC_DOMAIN_TX")
    )
    & (
        trim(col("TransformIt.DIAG_CD_TYP_CD")) == col("Lkp_Src_Domain.DOMAIN_ID_TXT")
    ),
    "left"
)
df_SnapshotAllCol = df_Snapshot.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("TransformIt.CLM_ID").alias("CLM_ID"),
    col("TransformIt.CLM_LN_SEQ").alias("CLM_LN_SEQ_NO"),
    col("TransformIt.CLM_DIAG_ORDNL_CD").alias("CLM_LN_DIAG_ORDNL_CD"),
    col("TransformIt.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("TransformIt.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    col("TransformIt.DISCARD_IN").alias("DISCARD_IN"),
    col("TransformIt.PASS_THRU_IN").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    col("TransformIt.ERR_CT").alias("ERR_CT"),
    col("TransformIt.RECYCLE_CT").alias("RECYCLE_CT"),
    col("TransformIt.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("TransformIt.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("TransformIt.CLM_DIAG_SK").alias("CLM_LN_DIAG_SK"),
    col("TransformIt.CRT_RUN_CYC_EXTCN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("TransformIt.LAST_UPDT_RUN_CYC_EXTCN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    when(col("Lkp_Src_Domain.TRGT_DOMAIN_TX").isNotNull(), regexp_replace(col("Lkp_Src_Domain.TRGT_DOMAIN_TX"), "\\.", "")).otherwise("NA").alias("DIAG_CD"),
    col("TransformIt.DIAG_CD_TYP_CD").alias("DIAG_CD_TYP_CD")
)
df_SnapshotTransform = df_Snapshot.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("TransformIt.CLM_ID").alias("CLM_ID"),
    col("TransformIt.CLM_LN_SEQ").alias("CLM_LN_SEQ_NO"),
    col("TransformIt.CLM_DIAG_ORDNL_CD").alias("CLM_LN_DIAG_ORDNL_CD")
)
# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLnDiagPK
# COMMAND ----------
params = {
    "DriverTable": "NA",
    "RunID": RunID,
    "CurrRunCycle": CurrRunCycle,
    "CurrDateTime": CurrentDate,
    "$FacetsDB": "NA",
    "$FacetsOwner": "NA",
    "SrcSysCd": SrcSysCd,
    "SrcSysCdSk": SrcSysCdSk,
    "$IDSOwner": IDSOwner
}
df_ClmLnDiagPK = ClmLnDiagPK(df_SnapshotAllCol, df_SnapshotTransform, params)
df_ClmLnDiagPK_final = df_ClmLnDiagPK.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"),10," ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"),1," ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"),1," ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("CLM_LN_DIAG_SK"),
    rpad(col("CLM_ID"),18," ").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO"),
    col("CLM_LN_DIAG_ORDNL_CD"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("DIAG_CD"),
    col("DIAG_CD_TYP_CD")
)
write_files(
    df_ClmLnDiagPK_final,
    f"{adls_path}/key/EmrProcedureClmLnDiagExtr.EmrProcedureClmLnDiag.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)