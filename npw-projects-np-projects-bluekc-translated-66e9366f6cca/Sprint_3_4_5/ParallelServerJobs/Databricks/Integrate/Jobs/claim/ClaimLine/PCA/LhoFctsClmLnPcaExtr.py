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
# MAGIC CALLED BY:  LhoFctsClmOnDmdExtr1Seq
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
# MAGIC 
# MAGIC Christen Marshall     2020-08-10       US 262816            Created new version to do historical load for LHO                       IntegrateDev2          
# MAGIC Prabhu ES               2022-03-29       S2S                       MSSQL ODBC conn params added                                            IntegrateDev5	Ken Bradmon	2022-06-11

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
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Retrieve job parameters
DriverTable = get_widget_value('DriverTable','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
Logging = get_widget_value('Logging','Y')
RunCycle = get_widget_value('RunCycle','')
TmpTblRunID = get_widget_value('TmpTblRunID','')
CurrentDateParam = get_widget_value('CurrentDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
LhoFacetsStgOwner = get_widget_value('LhoFacetsStgOwner','')
LhoFacetsStgAcct = get_widget_value('LhoFacetsStgAcct','')
LhoFacetsStgPW = get_widget_value('LhoFacetsStgPW','')
LhoFacetsStgDSN = get_widget_value('LhoFacetsStgDSN','')
lhofacetsstg_secret_name = get_widget_value('lhofacetsstg_secret_name','')

# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLnPcaPK
# COMMAND ----------

# Read from hashed file hf_clm_fcts_reversals (Scenario C -> read parquet)
df_hf_clm_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")

# Read from hashed file hf_clm_nasco_dup_bypass (Scenario C -> read parquet)
df_clm_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")

# ODBCConnector stage: CMC_CDHL_HSA_LINE
jdbc_url, jdbc_props = get_db_config(lhofacetsstg_secret_name)
query_cmc_cdhl_hsa_line = (
    f"SELECT \n"
    f"clm.CLCL_ID,\n"
    f"clm.CDML_SEQ_NO,\n"
    f"clm.CDHL_CONSIDER_AMT,\n"
    f"clm.CDHL_NONCONSDR_AMT,\n"
    f"clm.CDHL_DISALL_AMT,\n"
    f"clm.EXCD_ID,\n"
    f"clm.CDHL_PAID_AMT,\n"
    f"clm.CDHL_SB_PYMT_AMT,\n"
    f"clm.CDHL_PR_PYMT_AMT,\n"
    f"clm.LOBD_ID,\n"
    f"clm.CDHL_HSA_PCS_IND,\n"
    f"clcl.CLCL_LOW_SVC_DT,\n"
    f"clcl.PDPD_ID\n"
    f"FROM {LhoFacetsStgOwner}.CMC_CDHL_HSA_LINE clm,\n"
    f"     {LhoFacetsStgOwner}.CMC_CLCL_CLAIM clcl,\n"
    f"     tempdb..{DriverTable}  drvr\n"
    f"WHERE clm.CLCL_ID = clcl.CLCL_ID\n"
    f"  AND clm.CLCL_ID = drvr.CLM_ID"
)
df_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_cmc_cdhl_hsa_line)
    .load()
)

# Transformer: StripFields
df_Strip = (
    df_Extract
    .withColumn("CLCL_ID", strip_field(F.col("CLCL_ID")))
    .withColumn("EXCD_ID", strip_field(F.col("EXCD_ID")))
    .withColumn("LOBD_ID", strip_field(F.col("LOBD_ID")))
    .withColumn("CDHL_HSA_PCS_IND", strip_field(F.col("CDHL_HSA_PCS_IND")))
    .withColumn("PDPD_ID", strip_field(F.col("PDPD_ID")))
    .select(
        F.col("CLCL_ID").alias("CLCL_ID"),
        F.col("CDML_SEQ_NO").alias("CDML_SEQ_NO"),
        F.col("CDHL_CONSIDER_AMT").alias("CDHL_CONSIDER_AMT"),
        F.col("CDHL_NONCONSDR_AMT").alias("CDHL_NONCONSDR_AMT"),
        F.col("CDHL_DISALL_AMT").alias("CDHL_DISALL_AMT"),
        F.col("EXCD_ID").alias("EXCD_ID"),
        F.col("CDHL_PAID_AMT").alias("CDHL_PAID_AMT"),
        F.col("CDHL_SB_PYMT_AMT").alias("CDHL_SB_PYMT_AMT"),
        F.col("CDHL_PR_PYMT_AMT").alias("CDHL_PR_PYMT_AMT"),
        F.col("LOBD_ID").alias("LOBD_ID"),
        F.col("CDHL_HSA_PCS_IND").alias("CDHL_HSA_PCS_IND"),
        F.col("CLCL_LOW_SVC_DT").alias("CLCL_LOW_SVC_DT"),
        F.col("PDPD_ID").alias("PDPD_ID")
    )
)

# BusinessLogic (CTransformerStage) with lookups on df_hf_clm_fcts_reversals and df_clm_nasco_dup_bypass
df_BusinessLogic_temp = (
    df_Strip.alias("Strip")
    .join(df_hf_clm_fcts_reversals.alias("fcts_reversals"),
          F.col("Strip.CLCL_ID") == F.col("fcts_reversals.CLCL_ID"), how="left")
    .join(df_clm_nasco_dup_bypass.alias("nasco_dup_lkup"),
          F.col("Strip.CLCL_ID") == F.col("nasco_dup_lkup.CLM_ID"), how="left")
    .withColumn("PassThru", F.lit("Y"))
    .withColumn("ClmId", trim(F.col("Strip.CLCL_ID")))
)

df_businesslogic_regular = df_BusinessLogic_temp.filter(
    F.isnull(F.col("nasco_dup_lkup.CLM_ID"))
)

df_businesslogic_reversal = df_BusinessLogic_temp.filter(
    (F.col("fcts_reversals.CLCL_ID").isNotNull())
    | (F.col("fcts_reversals.CLCL_CUR_STS") == F.lit("91"))
    | (F.col("fcts_reversals.CLCL_CUR_STS") == F.lit("89"))
)

# Columns for "Regular" link
df_regular = df_businesslogic_regular.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"), 
    rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
    rpad(F.col("PassThru"), 1, " ").alias("ROW_PASS_THRU"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("ClmId"), F.lit(":"), F.col("Strip.CDML_SEQ_NO")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_LN_SK"),
    rpad(F.col("ClmId"), 12, " ").alias("CLM_ID"),
    F.col("Strip.CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(F.col("Strip.CLCL_ID"), 12, " ").alias("CLM_SK"),
    F.when(
        F.col("Strip.EXCD_ID").isNull() | (trim(F.col("Strip.EXCD_ID")) == ""),
        F.lit("NA")
    ).otherwise(trim(F.col("Strip.EXCD_ID"))).alias("DSALW_EXCD_SK"),
    F.when(
        F.col("Strip.LOBD_ID").isNull() | (trim(F.col("Strip.LOBD_ID")) == ""),
        F.lit("NA")
    ).otherwise(rpad(trim(F.col("Strip.LOBD_ID")), 4, " ")).alias("CLM_LN_PCA_LOB_CD_SK"),
    F.when(
        F.col("Strip.CDHL_HSA_PCS_IND").isNull() | (trim(F.col("Strip.CDHL_HSA_PCS_IND")) == ""),
        F.lit("NA")
    ).otherwise(rpad(trim(F.col("Strip.CDHL_HSA_PCS_IND")), 1, " ")).alias("CLM_LN_PCA_PRCS_CD_SK"),
    F.when(
        F.col("Strip.CDHL_CONSIDER_AMT").isNull()
        | (trim(F.col("Strip.CDHL_CONSIDER_AMT")) == ""),
        F.lit("0")
    ).otherwise(trim(F.col("Strip.CDHL_CONSIDER_AMT"))).alias("CNSD_AMT"),
    F.when(
        F.col("Strip.CDHL_DISALL_AMT").isNull()
        | (trim(F.col("Strip.CDHL_DISALL_AMT")) == ""),
        F.lit("0")
    ).otherwise(trim(F.col("Strip.CDHL_DISALL_AMT"))).alias("DSALW_AMT"),
    F.when(
        F.col("Strip.CDHL_NONCONSDR_AMT").isNull()
        | (trim(F.col("Strip.CDHL_NONCONSDR_AMT")) == ""),
        F.lit("0")
    ).otherwise(trim(F.col("Strip.CDHL_NONCONSDR_AMT"))).alias("NONCNSD_AMT"),
    F.when(
        F.col("Strip.CDHL_PR_PYMT_AMT").isNull()
        | (trim(F.col("Strip.CDHL_PR_PYMT_AMT")) == ""),
        F.lit("0")
    ).otherwise(trim(F.col("Strip.CDHL_PR_PYMT_AMT"))).alias("PROV_PD_AMT"),
    F.when(
        F.col("Strip.CDHL_SB_PYMT_AMT").isNull()
        | (trim(F.col("Strip.CDHL_SB_PYMT_AMT")) == ""),
        F.lit("0")
    ).otherwise(trim(F.col("Strip.CDHL_SB_PYMT_AMT"))).alias("SUB_PD_AMT"),
    F.when(
        F.col("Strip.CDHL_PAID_AMT").isNull()
        | (trim(F.col("Strip.CDHL_PAID_AMT")) == ""),
        F.lit("0")
    ).otherwise(trim(F.col("Strip.CDHL_PAID_AMT"))).alias("PD_AMT")
)

# Columns for "Reversal" link
def neg_col(colStr):
    # Interprets the "Neg(...)" logic by converting string to float (when numeric) and multiplying by -1
    return (
        F.when(
            (F.col(colStr).isNull())
            | (trim(F.col(colStr)) == "")
            | (~F.col(colStr).rlike("^[0-9.+-]*$")),
            F.lit(0.0)
        )
        .otherwise(F.col(colStr).cast("double"))
        * -1
    )

df_reversal = df_businesslogic_reversal.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
    rpad(F.col("PassThru"), 1, " ").alias("ROW_PASS_THRU"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("ClmId"), F.lit("R;")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_LN_SK"),
    rpad(F.concat(F.col("ClmId"), F.lit("R")), 13, " ").alias("CLM_ID"),
    F.col("Strip.CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(F.concat(F.col("Strip.CLCL_ID"), F.lit("R")), 13, " ").alias("CLM_SK"),
    F.when(
        F.col("Strip.EXCD_ID").isNull() | (trim(F.col("Strip.EXCD_ID")) == ""),
        F.lit("NA")
    ).otherwise(trim(F.col("Strip.EXCD_ID"))).alias("DSALW_EXCD_SK"),
    # "CLM_LN_PCA_LOB_CD_SK" => no NA logic here in the job, just "Strip.LOBD_ID"
    rpad(F.col("Strip.LOBD_ID"), 4, " ").alias("CLM_LN_PCA_LOB_CD_SK"),
    rpad(F.col("Strip.CDHL_HSA_PCS_IND"), 1, " ").alias("CLM_LN_PCA_PRCS_CD_SK"),
    neg_col("Strip.CDHL_CONSIDER_AMT").alias("CNSD_AMT"),
    neg_col("Strip.CDHL_DISALL_AMT").alias("DSALW_AMT"),
    neg_col("Strip.CDHL_NONCONSDR_AMT").alias("NONCNSD_AMT"),
    neg_col("Strip.CDHL_PR_PYMT_AMT").alias("PROV_PD_AMT"),
    neg_col("Strip.CDHL_SB_PYMT_AMT").alias("SUB_PD_AMT"),
    neg_col("Strip.CDHL_PAID_AMT").alias("PD_AMT")
)

# Link Collector (CCollector) -> union
df_link_collector = df_regular.unionByName(df_reversal)

# SnapShot (CTransformerStage), primary link "Out"
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

# SnapShot (CTransformerStage), second output link "Snapshot" -> B_CLM_LN_PCA
df_B_CLM_LN_PCA = df_link_collector.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    # The job has CLM_ID declared as PK and originally from a hashed-file or char/varchar.  No length known -> use <...> to flag.
    rpad(F.col("CLM_ID"), <...>, " ").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("PD_AMT").alias("PD_AMT")
)

# Write the B_CLM_LN_PCA file (CSeqFileStage)
write_files(
    df_B_CLM_LN_PCA,
    f"{adls_path}/load/B_CLM_LN_PCA.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Container Stage: ClmLnPcaPK
params_ClmLnPcaPK = {
    "CurrRunCycle": CurrRunCycle
}
df_clmlnpcaPK = ClmLnPcaPK(df_snapshot_transform, params_ClmLnPcaPK)

# Final output to LhoFctsClmLnPcaExtr.LhoClmLnPca.dat#RunID#
# The container output columns (24). Apply rpad for known char columns:
df_LhoFctsClmLnPcaExtr = df_clmlnpcaPK.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(F.col("ROW_PASS_THRU"), 1, " ").alias("ROW_PASS_THRU"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_LN_SK"),
    # The job uses CLM_ID from a char/varchar source => length is uncertain -> use <...> to flag
    rpad(F.col("CLM_ID"), <...>, " ").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    # CLM_SK was from char(12), so rpad to 12
    rpad(F.col("CLM_SK"), 12, " ").alias("CLM_SK"),
    # DSALW_EXCD_SK from char(3)
    rpad(F.col("DSALW_EXCD_SK"), 3, " ").alias("DSALW_EXCD_SK"),
    # LOBD_ID was char(4)
    rpad(F.col("CLM_LN_PCA_LOB_CD_SK"), 4, " ").alias("CLM_LN_PCA_LOB_CD_SK"),
    # CDHL_HSA_PCS_IND was char(1)
    rpad(F.col("CLM_LN_PCA_PRCS_CD_SK"), 1, " ").alias("CLM_LN_PCA_PRCS_CD_SK"),
    F.col("CNSD_AMT"),
    F.col("DSALW_AMT"),
    F.col("NONCNSD_AMT"),
    F.col("PROV_PD_AMT"),
    F.col("SUB_PD_AMT"),
    F.col("PD_AMT")
)

write_files(
    df_LhoFctsClmLnPcaExtr,
    f"{adls_path}/key/LhoFctsClmLnPcaExtr.LhoClmLnPca.dat{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)