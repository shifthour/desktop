# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2015 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:  EmrProcedureClmDiagExtr
# MAGIC CALLED BY:  EmrProcedureClmExtrSeq
# MAGIC 
# MAGIC DESCRIPTION:      Reads the Format Extract file created from EmrProcedureClmExtrFormatData job and adds the Recycle fields and values for the SK if possible.
# MAGIC 
# MAGIC PROCESSING:   Output file is created with a temp. name.  After-Job Subroutine renames file after successful run.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer	Date		Project/Altiris #		Change Description							Development Project		Code Reviewer	Date Reviewed       
# MAGIC =============================================================================================================================================================================================== 
# MAGIC Vikas A		2021-02-24        	RA- EMR Procedure    	Initial Programming    						IntegrateDev2            		Jaideep Mankala 	03/04/2021         
# MAGIC Ken Bradmon	2022-12-15	us542805			Added 7 new columns: PROC_CD_CPT, 				IntegrateDev1			Harsha Ravuri	06/14/2023				
# MAGIC 							PROC_CD_CPT_MOD_1, PROC_CD_CPT_2, 		
# MAGIC 							PROC_CD_CPTII_MOD2, PROC_CD_CPTII_MOD2, 
# MAGIC 							SNOMED, and CVX.

# MAGIC Assign primary surrogate key
# MAGIC Writing Sequential File to ../key
# MAGIC Apply business logic
# MAGIC This container is used in:
# MAGIC FctsClmDiagExtr
# MAGIC NascoClmDiagExtr
# MAGIC BCBSSCClmDiagExtr
# MAGIC BCBSAClmDiagExtr
# MAGIC EyeMedClmExtr 
# MAGIC EyeMedMAClmDiagExtr
# MAGIC EmrProcedureClmDiagExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


CurrRunCycle = get_widget_value('CurrRunCycle','1')
RunID = get_widget_value('RunID','100')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrentDate = get_widget_value('CurrentDate','')
InFile_F = get_widget_value('InFile_F','')

schema_EMRClm_Landing = StructType([
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

df_EMRClm_Landing = (
    spark.read
    .option("delimiter", ",")
    .option("quote", "\"")
    .option("header", "false")
    .schema(schema_EMRClm_Landing)
    .csv(f"{adls_path}/verified/{InFile_F}")
)

df_BusinessRules = (
    df_EMRClm_Landing
    .withColumn(
        "svServDate",
        F.when(
            (F.col("DT_OF_SVC").isNotNull()) & (F.col("DT_OF_SVC") < F.lit("20151001")),
            F.lit("ICD9")
        ).when(
            (F.col("DT_OF_SVC").isNotNull()) & (F.col("DT_OF_SVC") >= F.lit("20151001")),
            F.lit("ICD10")
        ).otherwise(F.lit("UNK"))
    )
    .withColumn("ClmId", F.col("CLM_ID"))
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.lit("Y"))
    .withColumn("FIRST_RECYC_DT", current_date())
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
    .withColumn("PRI_KEY_STRING", F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("ClmId"), F.lit(";"), F.lit(1)))
    .withColumn("CLM_DIAG_SK", F.lit(0))
    .withColumn("CLM_DIAG_ORDNL_CD", F.lit("1"))
    .withColumn("CRT_RUN_CYC_EXTCN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXTCN_SK", F.lit(0))
    .withColumn("CLM_DIAG_POA_CD", F.lit("NA"))
    .withColumn("DIAG_CD_TYP_CD", F.col("svServDate"))
    .withColumn("PROC_TYPE", trim(F.col("PROC_TYPE")))
)

jdbc_url_IDS, jdbc_props_IDS = get_db_config(ids_secret_name)
extract_query_IDS = f"""
SELECT 
 TRIM(SRC_DOMAIN_TX) AS SRC_DOMAIN_TX,
 CASE WHEN TRIM(DOMAIN_ID)='DIAGNOSIS_CODE_ICD9' THEN 'ICD9' ELSE 'ICD10' END AS DOMAIN_ID_TXT,
 TRIM(TRGT_DOMAIN_TX) AS TRGT_DOMAIN_TX
FROM {IDSOwner}.P_SRC_DOMAIN_TRNSLTN
WHERE SRC_SYS_CD = 'PROVGRP' 
  AND DOMAIN_ID IN ('DIAGNOSIS_CODE_ICD9','DIAGNOSIS_CODE_ICD10')
"""

df_IDS_Ext = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", extract_query_IDS)
    .load()
)

df_hf_emr_procedure_clm_src_dmn_trans = df_IDS_Ext.dropDuplicates(["SRC_DOMAIN_TX","DOMAIN_ID_TXT"])

df_Snapshot = (
    df_BusinessRules.alias("TransformIt")
    .join(
        df_hf_emr_procedure_clm_src_dmn_trans.alias("Lkp_Src_Domain"),
        (
            trim(F.col("TransformIt.PROC_TYPE")) == F.col("Lkp_Src_Domain.SRC_DOMAIN_TX")
        )
        & (
            trim(F.col("TransformIt.DIAG_CD_TYP_CD")) == F.col("Lkp_Src_Domain.DOMAIN_ID_TXT")
        ),
        how="left"
    )
)

df_AllCol = df_Snapshot.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("TransformIt.CLM_ID").alias("CLM_ID"),
    F.col("TransformIt.CLM_DIAG_ORDNL_CD").alias("CLM_DIAG_ORDNL_CD"),
    F.col("TransformIt.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("TransformIt.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("TransformIt.DISCARD_IN").alias("DISCARD_IN"),
    F.col("TransformIt.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("TransformIt.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("TransformIt.ERR_CT").alias("ERR_CT"),
    F.col("TransformIt.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("TransformIt.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("TransformIt.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("TransformIt.CLM_DIAG_SK").alias("CLM_DIAG_SK"),
    F.col("TransformIt.CRT_RUN_CYC_EXTCN_SK").alias("CRT_RUN_CYC_EXTCN_SK"),
    F.col("TransformIt.LAST_UPDT_RUN_CYC_EXTCN_SK").alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
    F.when(
        F.col("Lkp_Src_Domain.TRGT_DOMAIN_TX").isNotNull(),
        F.regexp_replace(F.col("Lkp_Src_Domain.TRGT_DOMAIN_TX"), "\\.", "")
    ).otherwise(F.lit("NA")).alias("DIAG_CD"),
    F.col("TransformIt.CLM_DIAG_POA_CD").alias("CLM_DIAG_POA_CD"),
    F.col("TransformIt.DIAG_CD_TYP_CD").alias("DIAG_CD_TYP_CD")
)

df_Transform = df_Snapshot.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("TransformIt.CLM_ID").alias("CLM_ID"),
    F.col("TransformIt.CLM_DIAG_ORDNL_CD").alias("CLM_DIAG_ORDNL_CD")
)

# MAGIC %run ../../../../shared_containers/PrimaryKey/ClmDiagPK
# COMMAND ----------

params_ClmDiagPK = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "IDSOwner": IDSOwner
}

df_Key = ClmDiagPK(df_AllCol, df_Transform, params_ClmDiagPK)

df_EMRClmDiag_Final = (
    df_Key
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("CLM_DIAG_ORDNL_CD", F.rpad(F.col("CLM_DIAG_ORDNL_CD"), 2, " "))
    .withColumn("CLM_DIAG_POA_CD", F.rpad(F.col("CLM_DIAG_POA_CD"), 2, " "))
    .withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), <...>, " "))
    .withColumn("PRI_KEY_STRING", F.rpad(F.col("PRI_KEY_STRING"), <...>, " "))
    .withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), <...>, " "))
    .withColumn("DIAG_CD", F.rpad(F.col("DIAG_CD"), <...>, " "))
    .withColumn("DIAG_CD_TYP_CD", F.rpad(F.col("DIAG_CD_TYP_CD"), <...>, " "))
    .select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "CLM_DIAG_SK",
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "CLM_DIAG_ORDNL_CD",
        "CRT_RUN_CYC_EXTCN_SK",
        "LAST_UPDT_RUN_CYC_EXTCN_SK",
        "DIAG_CD",
        "CLM_DIAG_POA_CD",
        "DIAG_CD_TYP_CD"
    )
)

write_files(
    df_EMRClmDiag_Final,
    f"{adls_path}/key/EmrProcedureClmDiagExtr.EmrProcedureClmDiag.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)