# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY:  OpsDashboardClmExtrSeq
# MAGIC 
# MAGIC                            
# MAGIC PROCESSING:   Pulls the Claim Inventory information from OEC file which populates the Claim Inventory table
# MAGIC 
# MAGIC OEC is used as the source since this is the primary source of extract for this job. No default settings for Facets need to be used.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                                                Project/                            Change                                                                   Development 
# MAGIC Developer                          Date              Altiris #                            Description                                                              Project            Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ----------------------------------------------------------------------------    ---------------------  ---------------------------------   --------------------------   
# MAGIC Parikshith Chada              01/16/2007   3531                                Original Programming                                               devlIDS           Steph Goddard            03/27/2008
# MAGIC Hugh Sisson                     03/28/2008   3531                                Changed hash file name                                          devlIDS
# MAGIC Abhiram Dasarathy           07/27/2015    5407	                  Added Column ASG_USER_SK to the end of the file   EnterpriseDev1  Kalyan Neelam          2015-07-29

# MAGIC Pulling OEC Claim Inventory Data
# MAGIC Writing Sequential File to /key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StringType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmInvtryPkey
# COMMAND ----------

CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','')
InFile = get_widget_value('InFile','')
DriverTable = get_widget_value('DriverTable','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
FacetsDB = get_widget_value('FacetsDB','')

# Stage: k (CFF) - Treat as fixed-length file read
col_details_k = {
    "SCCF_SER_NUM": {"len": 17, "type": StringType()},
    "TRANS_ID": {"len": 2, "type": StringType()},
    "FLR": {"len": 2, "type": StringType()},
    "RCRD_SEQ": {"len": 2, "type": StringType()},
    "SEQ_NUM_READ": {"len": 10, "type": StringType()},
    "CLM_TYP": {"len": 2, "type": StringType()},
    "FLR_2": {"len": 20, "type": StringType()},
    "LOC_PLN_CNTRL_NUM": {"len": 17, "type": StringType()},
    "FLR_3": {"len": 191, "type": StringType()}
}
df_k = fixed_file_read_write(f"{adls_path}/{InFile}", col_details_k, "read")

# Stage: StripField (CTransformerStage)
df_StripField = (
    df_k
    .filter(F.col('SCCF_SER_NUM').cast('double').isNotNull())
    .withColumn('SCCF_SER_NUM', trim(F.col('SCCF_SER_NUM')))
    .select('SCCF_SER_NUM')
)

# Stage: hf_oec_clm_invtry_rmdup (CHashedFileStage) - Scenario A dedup on key [SCCF_SER_NUM]
df_hf_oec_clm_invtry_rmdup = df_StripField.dropDuplicates(['SCCF_SER_NUM'])

# Stage: BusinessRules (CTransformerStage)
df_BusinessRules = (
    df_hf_oec_clm_invtry_rmdup
    .filter(F.length(F.col('SCCF_SER_NUM')) > 0)
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.lit("Y"))
    .withColumn("FIRST_RECYC_DT", F.lit(CurrDate))
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit("OEC"))
    .withColumn("PRI_KEY_STRING", F.concat(F.lit("OEC;"), F.col("SCCF_SER_NUM")))
    .withColumn("CLM_INVTRY_SK", F.lit(0))
    .withColumn("CLM_INVTRY_KEY_ID", F.col("SCCF_SER_NUM"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("GRP_ID", F.lit("NA"))
    .withColumn("OPS_WORK_UNIT_ID", F.lit("ITSHost"))
    .withColumn("PDPD_ID", F.lit("NA"))
    .withColumn("PRPR_ID", F.lit("NA"))
    .withColumn("CLM_INVTRY_PEND_CAT_CD", F.lit("NA"))
    .withColumn("CLM_STTUS_CHG_RSN_CD", F.lit("NA"))
    .withColumn("CLST_STS", F.lit("NA"))
    .withColumn("CLCL_CL_SUB_TYPE", F.lit("NA"))
    .withColumn("CLCL_CL_TYPE", F.lit("NA"))
    .withColumn("INPT_DT_SK", F.lit(CurrDate))
    .withColumn("RCVD_DT_SK", F.lit(CurrDate))
    .withColumn("EXTR_DT_SK", F.lit(CurrDate))
    .withColumn("STTUS_DT_SK", F.lit(CurrDate))
    .withColumn("INVTRY_CT", F.lit(1))
    .withColumn("ASG_USER_SK", F.lit(1))
    .withColumn("WORK_ITEM_CT", F.lit(1))
)

# Stage: ClmInvtryPkey (CContainerStage)
params_ClmInvtryPkey = {
    "DriverTable": DriverTable,
    "CurrRunCycle": CurrRunCycle,
    "RunID": RunID,
    "CurrDate": CurrDate,
    "FacetsDB": FacetsDB,
    "FacetsOwner": FacetsOwner
}
df_ClmInvtryPkey = ClmInvtryPkey(df_BusinessRules, params_ClmInvtryPkey)

# Stage: OecClmInvtryExtr (CSeqFileStage) - Final select and write
df_final = df_ClmInvtryPkey.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_INVTRY_SK",
    "CLM_INVTRY_KEY_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "GRP_ID",
    "OPS_WORK_UNIT_ID",
    "PDPD_ID",
    "PRPR_ID",
    "CLM_INVTRY_PEND_CAT_CD",
    "CLM_STTUS_CHG_RSN_CD",
    "CLST_STS",
    "CLCL_CL_SUB_TYPE",
    "CLCL_CL_TYPE",
    "INPT_DT_SK",
    "RCVD_DT_SK",
    "EXTR_DT_SK",
    "STTUS_DT_SK",
    "INVTRY_CT",
    "ASG_USER_SK",
    "WORK_ITEM_CT"
)

df_final = (
    df_final
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("PDPD_ID", F.rpad(F.col("PDPD_ID"), 8, " "))
    .withColumn("PRPR_ID", F.rpad(F.col("PRPR_ID"), 12, " "))
    .withColumn("CLST_STS", F.rpad(F.col("CLST_STS"), 2, " "))
    .withColumn("CLCL_CL_SUB_TYPE", F.rpad(F.col("CLCL_CL_SUB_TYPE"), 1, " "))
    .withColumn("CLCL_CL_TYPE", F.rpad(F.col("CLCL_CL_TYPE"), 1, " "))
    .withColumn("INPT_DT_SK", F.rpad(F.col("INPT_DT_SK"), 10, " "))
    .withColumn("RCVD_DT_SK", F.rpad(F.col("RCVD_DT_SK"), 10, " "))
    .withColumn("EXTR_DT_SK", F.rpad(F.col("EXTR_DT_SK"), 10, " "))
    .withColumn("STTUS_DT_SK", F.rpad(F.col("STTUS_DT_SK"), 10, " "))
)

output_file_path = f"{adls_path}/key/OecClmInvtryExtr.ClmInvtry.dat.{RunID}"
write_files(
    df_final,
    output_file_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)