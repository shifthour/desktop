# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC ************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC ************************************************************************************
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     FctsClmLnPcaExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from CMC_CDHL_HSA_LINE to a landing file for the IDS
# MAGIC   Creates an output file in the verified directory for input to the IDS transform job.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC  
# MAGIC 09/28/2006   Raj             Completed the Job Mapping.Job ran successfully.
# MAGIC 09/28/2006   Michelle      Requested Raj to take off PDPD_ID="C"(4th digit) and dates of service >1/1/2007 requirement from business logic
# MAGIC 09/28/2006   Raj             Submitted test plan with Michelle
# MAGIC 09/28/2006   Michelle     The Facets Current data will be blown away on Friday and I would like to get the CLM_PCA and CLM_LN_PCA table loaded in IDS before this happens.  I 
# MAGIC                                          spoke with Crystal Harper and we will not get claim data back out in these tables for maybe a week or so.  
# MAGIC 09/28/2006   Raj             Loaded into IDS.
# MAGIC 10/02/2006   Michelle      Tested and found 8 reversal rows missing in the table.Requested Steph and Raj to find the rootcause.
# MAGIC 10/03/2006   Raj             Zero rows pulled from source when ran the job.Michelle requested Raj to wait until the data's are load in the table again(for a week)
# MAGIC 11/22/2006   Ralph Tucker  Added CDML_SEQ_NO to the PRI_KEY_STRING;  Rows were being dropped.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------    
# MAGIC Oliver Nielsen           08/20/2007    Balancing                 Added Balancing Snapshot File                                                 devlIDS30                        Steph Goddard        8/30/07
# MAGIC 
# MAGIC 
# MAGIC Parik                         2008-08-06      3567(Primary Key)  Added the new primary keying process                                       devlIDS                            Steph Goddard        08/12/2008
# MAGIC                                                                                        Added SQL join to driver table.
# MAGIC 
# MAGIC Abhiram Dasarathy	2015-05-26        TFS 7608	       Added logic to determine CLM_SK for Reversal CLM_ID            IntegrateNewDevl             Kalyan Neelam         2015-05-29
# MAGIC 					       to correspond to the CLM_SK on CLM table.
# MAGIC Prabhu ES               2022-02-28      S2S Remediation     MSSQL connection parameters added                                       IntegrateDev5                     Manasa Andru              2022-06-10

# MAGIC Balancing
# MAGIC CLM_LN_PCA
# MAGIC Extraction
# MAGIC Strip fields
# MAGIC Reversals
# MAGIC Business Logic
# MAGIC Data Collection
# MAGIC Primary Keying
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType, IntegerType, DecimalType, StructType, StructField
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
DriverTable = get_widget_value('DriverTable','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
Logging = get_widget_value('Logging','')
RunCycle = get_widget_value('RunCycle','')
TmpTblRunID = get_widget_value('TmpTblRunID','')
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLnPcaPK
# COMMAND ----------

df_hf_clm_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")
df_hf_clm_fcts_reversals = df_hf_clm_fcts_reversals.select(
    "CLCL_ID",
    "CLCL_CUR_STS",
    "CLCL_PAID_DT",
    "CLCL_ID_ADJ_TO",
    "CLCL_ID_ADJ_FROM"
)

df_clm_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")
df_clm_nasco_dup_bypass = df_clm_nasco_dup_bypass.select(
    "CLM_ID"
)

jdbc_url, jdbc_props = get_db_config(facets_secret_name)
extract_query = f"""
SELECT
  clm.CLCL_ID,
  clm.CDML_SEQ_NO,
  clm.CDHL_CONSIDER_AMT,
  clm.CDHL_NONCONSDR_AMT,
  clm.CDHL_DISALL_AMT,
  clm.EXCD_ID,
  clm.CDHL_PAID_AMT,
  clm.CDHL_SB_PYMT_AMT,
  clm.CDHL_PR_PYMT_AMT,
  clm.LOBD_ID,
  clm.CDHL_HSA_PCS_IND,
  clcl.CLCL_LOW_SVC_DT,
  clcl.PDPD_ID
FROM {FacetsOwner}.CMC_CDHL_HSA_LINE clm,
     {FacetsOwner}.CMC_CLCL_CLAIM clcl,
     tempdb..{DriverTable} drvr
WHERE clm.CLCL_ID = clcl.CLCL_ID
  AND clm.CLCL_ID = drvr.CLM_ID
"""
df_CMC_CDHL_HSA_LINE = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_stripfields = df_CMC_CDHL_HSA_LINE.select(
    strip_field(F.col("CLCL_ID")).alias("CLCL_ID"),
    F.col("CDML_SEQ_NO").alias("CDML_SEQ_NO"),
    F.col("CDHL_CONSIDER_AMT").alias("CDHL_CONSIDER_AMT"),
    F.col("CDHL_NONCONSDR_AMT").alias("CDHL_NONCONSDR_AMT"),
    F.col("CDHL_DISALL_AMT").alias("CDHL_DISALL_AMT"),
    strip_field(F.col("EXCD_ID")).alias("EXCD_ID"),
    F.col("CDHL_PAID_AMT").alias("CDHL_PAID_AMT"),
    F.col("CDHL_SB_PYMT_AMT").alias("CDHL_SB_PYMT_AMT"),
    F.col("CDHL_PR_PYMT_AMT").alias("CDHL_PR_PYMT_AMT"),
    strip_field(F.col("LOBD_ID")).alias("LOBD_ID"),
    strip_field(F.col("CDHL_HSA_PCS_IND")).alias("CDHL_HSA_PCS_IND"),
    F.col("CLCL_LOW_SVC_DT").alias("CLCL_LOW_SVC_DT"),
    strip_field(F.col("PDPD_ID")).alias("PDPD_ID")
)

df_bizlogic_lk = (
    df_stripfields.alias("Strip")
    .join(df_hf_clm_fcts_reversals.alias("fcts_reversals"), F.col("Strip.CLCL_ID") == F.col("fcts_reversals.CLCL_ID"), "left")
    .join(df_clm_nasco_dup_bypass.alias("nasco_dup_lkup"), F.col("Strip.CLCL_ID") == F.col("nasco_dup_lkup.CLM_ID"), "left")
)

df_bizlogic_regular = df_bizlogic_lk.filter("nasco_dup_lkup.CLM_ID IS NULL").select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("ROW_PASS_THRU"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.concat(F.lit("FACETS;"), trim(F.col("Strip.CLCL_ID")), F.lit(":"), F.col("Strip.CDML_SEQ_NO")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_LN_SK"),
    trim(F.col("Strip.CLCL_ID")).alias("CLM_ID"),
    F.col("Strip.CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Strip.CLCL_ID").alias("CLM_SK"),
    F.when(
        F.col("Strip.EXCD_ID").isNull() | (F.length(trim(F.col("Strip.EXCD_ID"))) == 0), 
        "NA"
    ).otherwise(trim(F.col("Strip.EXCD_ID"))).alias("DSALW_EXCD_SK"),
    F.when(
        F.col("Strip.LOBD_ID").isNull() | (F.length(trim(F.col("Strip.LOBD_ID"))) == 0),
        "NA"
    ).otherwise(trim(F.col("Strip.LOBD_ID"))).alias("CLM_LN_PCA_LOB_CD_SK"),
    F.when(
        F.col("Strip.CDHL_HSA_PCS_IND").isNull() | (F.length(trim(F.col("Strip.CDHL_HSA_PCS_IND"))) == 0),
        "NA"
    ).otherwise(trim(F.col("Strip.CDHL_HSA_PCS_IND"))).alias("CLM_LN_PCA_PRCS_CD_SK"),
    F.when(
        F.col("Strip.CDHL_CONSIDER_AMT").isNull() | (F.length(trim(F.col("Strip.CDHL_CONSIDER_AMT"))) == 0),
        F.lit(0)
    ).otherwise(trim(F.col("Strip.CDHL_CONSIDER_AMT"))).alias("CNSD_AMT"),
    F.when(
        F.col("Strip.CDHL_DISALL_AMT").isNull() | (F.length(trim(F.col("Strip.CDHL_DISALL_AMT"))) == 0),
        F.lit(0)
    ).otherwise(trim(F.col("Strip.CDHL_DISALL_AMT"))).alias("DSALW_AMT"),
    F.when(
        F.col("Strip.CDHL_NONCONSDR_AMT").isNull() | (F.length(trim(F.col("Strip.CDHL_NONCONSDR_AMT"))) == 0),
        F.lit(0)
    ).otherwise(trim(F.col("Strip.CDHL_NONCONSDR_AMT"))).alias("NONCNSD_AMT"),
    F.when(
        F.col("Strip.CDHL_PR_PYMT_AMT").isNull() | (F.length(trim(F.col("Strip.CDHL_PR_PYMT_AMT"))) == 0),
        F.lit(0)
    ).otherwise(trim(F.col("Strip.CDHL_PR_PYMT_AMT"))).alias("PROV_PD_AMT"),
    F.when(
        F.col("Strip.CDHL_SB_PYMT_AMT").isNull() | (F.length(trim(F.col("Strip.CDHL_SB_PYMT_AMT"))) == 0),
        F.lit(0)
    ).otherwise(trim(F.col("Strip.CDHL_SB_PYMT_AMT"))).alias("SUB_PD_AMT"),
    F.when(
        F.col("Strip.CDHL_PAID_AMT").isNull() | (F.length(trim(F.col("Strip.CDHL_PAID_AMT"))) == 0),
        F.lit(0)
    ).otherwise(trim(F.col("Strip.CDHL_PAID_AMT"))).alias("PD_AMT")
)

df_bizlogic_reversal = df_bizlogic_lk.filter(
    "(fcts_reversals.CLCL_ID IS NOT NULL) OR (fcts_reversals.CLCL_CUR_STS IN ('91','89'))"
).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("ROW_PASS_THRU"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.concat(F.lit("FACETS;"), trim(F.col("Strip.CLCL_ID")), F.lit("R;")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_LN_SK"),
    F.concat(trim(F.col("Strip.CLCL_ID")), F.lit("R")).alias("CLM_ID"),
    F.col("Strip.CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.concat(F.col("Strip.CLCL_ID"), F.lit("R")).alias("CLM_SK"),
    F.when(
        F.col("Strip.EXCD_ID").isNull() | (F.length(trim(F.col("Strip.EXCD_ID"))) == 0),
        "NA"
    ).otherwise(trim(F.col("Strip.EXCD_ID"))).alias("DSALW_EXCD_SK"),
    F.col("Strip.LOBD_ID").alias("CLM_LN_PCA_LOB_CD_SK"),
    F.col("Strip.CDHL_HSA_PCS_IND").alias("CLM_LN_PCA_PRCS_CD_SK"),
    -1 * F.when(
        (F.col("Strip.CDHL_CONSIDER_AMT").isNull()) | (F.length(trim(F.col("Strip.CDHL_CONSIDER_AMT"))) == 0) | (F.col("Strip.CDHL_CONSIDER_AMT").cast("double").isNull()),
        F.lit(0).cast("double")
    ).otherwise(F.col("Strip.CDHL_CONSIDER_AMT").cast("double")).alias("CNSD_AMT"),
    -1 * F.when(
        (F.col("Strip.CDHL_DISALL_AMT").isNull()) | (F.length(trim(F.col("Strip.CDHL_DISALL_AMT"))) == 0) | (F.col("Strip.CDHL_DISALL_AMT").cast("double").isNull()),
        F.lit(0).cast("double")
    ).otherwise(F.col("Strip.CDHL_DISALL_AMT").cast("double")).alias("DSALW_AMT"),
    -1 * F.when(
        (F.col("Strip.CDHL_NONCONSDR_AMT").isNull()) | (F.length(trim(F.col("Strip.CDHL_NONCONSDR_AMT"))) == 0) | (F.col("Strip.CDHL_NONCONSDR_AMT").cast("double").isNull()),
        F.lit(0).cast("double")
    ).otherwise(F.col("Strip.CDHL_NONCONSDR_AMT").cast("double")).alias("NONCNSD_AMT"),
    -1 * F.when(
        (F.col("Strip.CDHL_PR_PYMT_AMT").isNull()) | (F.length(trim(F.col("Strip.CDHL_PR_PYMT_AMT"))) == 0) | (F.col("Strip.CDHL_PR_PYMT_AMT").cast("double").isNull()),
        F.lit(0).cast("double")
    ).otherwise(F.col("Strip.CDHL_PR_PYMT_AMT").cast("double")).alias("PROV_PD_AMT"),
    -1 * F.when(
        (F.col("Strip.CDHL_SB_PYMT_AMT").isNull()) | (F.length(trim(F.col("Strip.CDHL_SB_PYMT_AMT"))) == 0) | (F.col("Strip.CDHL_SB_PYMT_AMT").cast("double").isNull()),
        F.lit(0).cast("double")
    ).otherwise(F.col("Strip.CDHL_SB_PYMT_AMT").cast("double")).alias("SUB_PD_AMT"),
    -1 * F.when(
        (F.col("Strip.CDHL_PAID_AMT").isNull()) | (F.length(trim(F.col("Strip.CDHL_PAID_AMT"))) == 0) | (F.col("Strip.CDHL_PAID_AMT").cast("double").isNull()),
        F.lit(0).cast("double")
    ).otherwise(F.col("Strip.CDHL_PAID_AMT").cast("double")).alias("PD_AMT")
)

df_link_collector = df_bizlogic_regular.unionByName(df_bizlogic_reversal)

df_snapshot_transform = df_link_collector.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("ROW_PASS_THRU").alias("ROW_PASS_THRU"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("CLM_LN_SK").alias("CLM_LN_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("DSALW_EXCD_SK").alias("DSALW_EXCD_SK"),
    F.col("CLM_LN_PCA_LOB_CD_SK").alias("CLM_LN_PCA_LOB_CD_SK"),
    F.col("CLM_LN_PCA_PRCS_CD_SK").alias("CLM_LN_PCA_PRCS_CD_SK"),
    F.col("CNSD_AMT").alias("CNSD_AMT"),
    F.col("DSALW_AMT").alias("DSALW_AMT"),
    F.col("NONCNSD_AMT").alias("NONCNSD_AMT"),
    F.col("PROV_PD_AMT").alias("PROV_PD_AMT"),
    F.col("SUB_PD_AMT").alias("SUB_PD_AMT"),
    F.col("PD_AMT").alias("PD_AMT")
)

df_snapshot_snapshot = df_link_collector.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("PD_AMT").alias("PD_AMT")
)

b_clm_ln_pca_path = f"{adls_path}/load/B_CLM_LN_PCA.FACETS.dat.{RunID}"
df_snapshot_snapshot_final = df_snapshot_snapshot.select(
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "PD_AMT"
)
write_files(
    df_snapshot_snapshot_final,
    b_clm_ln_pca_path,
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

params_ClmLnPcaPK = {
    "CurrRunCycle": CurrRunCycle
}
df_clmlnpcaPK = ClmLnPcaPK(df_snapshot_transform, params_ClmLnPcaPK)

df_fcts_clm_extr = df_clmlnpcaPK.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("ROW_PASS_THRU"), 1, " ").alias("ROW_PASS_THRU"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("CLM_LN_SK").alias("CLM_LN_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("DSALW_EXCD_SK").alias("DSALW_EXCD_SK"),
    F.col("CLM_LN_PCA_LOB_CD_SK").alias("CLM_LN_PCA_LOB_CD_SK"),
    F.col("CLM_LN_PCA_PRCS_CD_SK").alias("CLM_LN_PCA_PRCS_CD_SK"),
    F.col("CNSD_AMT").alias("CNSD_AMT"),
    F.col("DSALW_AMT").alias("DSALW_AMT"),
    F.col("NONCNSD_AMT").alias("NONCNSD_AMT"),
    F.col("PROV_PD_AMT").alias("PROV_PD_AMT"),
    F.col("SUB_PD_AMT").alias("SUB_PD_AMT"),
    F.col("PD_AMT").alias("PD_AMT")
)

fctsclmextr_path = f"{adls_path}/key/FctsClmLnPcaExtr.ClmLnPca.dat{RunID}"
write_files(
    df_fcts_clm_extr,
    fctsclmextr_path,
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)