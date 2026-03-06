# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     FctsIncomeInvcExtr
# MAGIC CALLED BY:   FctsIncomeExtrSeq
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC     Pulls the Invoice information from Facets for Income 
# MAGIC       
# MAGIC    Date is used as a criteria for which records are pulled.   
# MAGIC 
# MAGIC INPUTS:
# MAGIC    CDS_INID_INVOICE
# MAGIC   
# MAGIC HASH FILES:  hf_invc
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   used the STRIP.FIELD to all of the character fields.
# MAGIC                   The DB2 timestamp value of the audit_ts is converted to a sybase timestamp with the transofrm  FORMAT.DATE
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   Output file is created with a temp. name. 
# MAGIC                   
# MAGIC                  This is pulls all records from a BeginDate supplied in parameters.
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Sequential file name is created in the job control ( FinalOutFile Parameter )
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC              Ralph Tucker  -  10/30/2005-   Originally Programmed
# MAGIC                SAndrew             12/12/2006   - Project 1756 - Added extraction critieria from CDS_INID_INVOICE for where  INID.AFAI_CK = 0          
# MAGIC                 Naren Garapaty - 04/2007 - Added Reversal Logic 
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              6/13/2007          3264                              Added Balancing process to the overall                 devlIDS30               Steph Goddard            09/15/2007
# MAGIC                                                                                                       job that takes a snapshot of the source data                        
# MAGIC 
# MAGIC Bhoomi Dasari                 09/26/2008        3567                             Added new primay key contianer and SrcSysCdsk  devlIDS                    Steph Goddard            10/03/2008          
# MAGIC                                                                                                        and SrcSysCd  
# MAGIC 
# MAGIC Manasa Andru                2016-03-15         TFS - 12308                 Added Trim function to the BILL_INVC_ID field in the   IntegrateDev2      Kalyan Neelam             2016-03-16
# MAGIC                                                                                                  svReversalInvcId Stage Variable in the ReversalLogic transformer.
# MAGIC 
# MAGIC 
# MAGIC Goutham Kalidindi  2021-03-24                   US258186              Changed Datatype length for field                                                IntegrateDev1      Kalyan Neelam        2021-03-31
# MAGIC                                                                                              BLIV_ID
# MAGIC                                                                                                char(12) to Varchar(15)
# MAGIC Prabhu ES              2022-03-02            S2S Remediation       MSSQL ODBC conn added and other param changes                   IntegrateDev5	Ken Bradmon	2022-06-13

# MAGIC Hash file hf_invc_allcol cleared
# MAGIC Pulls all Invoice rows from facets CDS_INID_INVOICE table matching data criteria.
# MAGIC Balancing snapshot of source table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/IncmInvcPK
# COMMAND ----------

facets_secret_name = get_widget_value('facets_secret_name','')
ids_secret_name = get_widget_value('ids_secret_name','')
TmpOutFile = get_widget_value('TmpOutFile','')
FacetsOwner = get_widget_value('FacetsOwner','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
DriverTable = get_widget_value('DriverTable','')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')
IDSOwner = get_widget_value('IDSOwner','')

# Read hashed file hf_invc_rebills as parquet (Scenario C)
df_hf_invc_rebills = spark.read.parquet(f"{adls_path}/hf_invc_rebills.parquet")

# Connect to database for CDS_INCT_COMPONENT
jdbc_url_cds_inct_component, jdbc_props_cds_inct_component = get_db_config(facets_secret_name)
query_cds_inct_component = """SELECT 
 INVC.BLIV_ID,
 INVC.BLEI_CK,
 INVC.BLBL_DUE_DT,
 INVC.BLBL_END_DT,
 INVC.BLIV_CREATE_DTM,
 INVC.BLBL_SPCL_BL_IND 
FROM tempdb..#DriverTable#  TmpInvc, 
#$FacetsOwner#.CDS_INID_INVOICE INVC
WHERE 
 TmpInvc.BILL_INVC_ID = INVC.BLIV_ID
"""
df_CDS_INCT_COMPONENT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_cds_inct_component)
    .options(**jdbc_props_cds_inct_component)
    .option("query", query_cds_inct_component)
    .load()
)

# StripField stage
df_strip = df_CDS_INCT_COMPONENT.select(
    F.regexp_replace(F.col("BLIV_ID"), "[\\x0A\\x0D\\x09]", "").alias("BLIV_ID"),
    F.col("BLEI_CK").alias("BLEI_CK"),
    F.col("BLBL_DUE_DT").alias("BLBL_DUE_DT"),
    F.col("BLBL_END_DT").alias("BLBL_END_DT"),
    F.col("BLIV_CREATE_DTM").alias("BLIV_CREATE_DTM"),
    F.regexp_replace(F.col("BLBL_SPCL_BL_IND"), "[\\x0A\\x0D\\x09]", "").alias("BLBL_SPCL_BL_IND")
)

# BusinessRules stage
df_bizrules = (
    df_strip
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.lit("Y"))
    .withColumn("FIRST_RECYC_DT", current_date())
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit("FACETS"))
    .withColumn("PRI_KEY_STRING", F.concat(F.lit("FACETS;"), trim(F.col("BLIV_ID"))))
    .withColumn("INVC_SK", F.lit(0))
    .withColumn("BILL_INVC_ID", trim(F.col("BLIV_ID")))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("BILL_ENTY", F.col("BLEI_CK"))
    .withColumn(
        "INVC_TYP_CD",
        F.when(
            F.col("BLBL_SPCL_BL_IND").isNull() | (F.length(F.col("BLBL_SPCL_BL_IND")) == 0),
            F.lit("NA")
        ).otherwise(F.col("BLBL_SPCL_BL_IND"))
    )
    .withColumn(
        "BILL_DUE_DT",
        F.when(
            F.col("BLBL_DUE_DT").isNull() | (F.length(F.col("BLBL_DUE_DT")) == 0),
            F.lit("12-31-9999")
        ).otherwise(F.col("BLBL_DUE_DT"))
    )
    .withColumn(
        "BILL_END_DT",
        F.when(
            F.col("BLBL_END_DT").isNull() | (F.length(F.col("BLBL_END_DT")) == 0),
            F.lit("12-31-9999")
        ).otherwise(F.col("BLBL_END_DT"))
    )
    .withColumn(
        "CRT_DT",
        F.when(
            F.col("BLIV_CREATE_DTM").isNull() | (F.length(F.col("BLIV_CREATE_DTM")) == 0),
            F.lit("1-1-1753")
        ).otherwise(F.col("BLIV_CREATE_DTM"))
    )
)

# ReversalLogic stage
# Left join with df_hf_invc_rebills as a lookup
df_join_rl = (
    df_bizrules.alias("ChkReversals")
    .join(
        df_hf_invc_rebills.alias("rebill_lkup"),
        (F.col("ChkReversals.BILL_INVC_ID") == F.col("rebill_lkup.BILL_INVC_ID")),
        how="left"
    )
)

# We create NonReversal
df_nonreversal = (
    df_join_rl.filter(F.col("rebill_lkup.BILL_INVC_ID").isNull())
    .select(
        F.col("ChkReversals.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("ChkReversals.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("ChkReversals.DISCARD_IN").alias("DISCARD_IN"),
        F.col("ChkReversals.PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("ChkReversals.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ChkReversals.ERR_CT").alias("ERR_CT"),
        F.col("ChkReversals.RECYCLE_CT").alias("RECYCLE_CT"),
        F.col("ChkReversals.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("ChkReversals.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("ChkReversals.INVC_SK").alias("INVC_SK"),
        F.col("ChkReversals.BILL_INVC_ID").alias("BILL_INVC_ID"),
        F.col("ChkReversals.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("ChkReversals.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("ChkReversals.BILL_ENTY").alias("BILL_ENTY"),
        F.col("ChkReversals.INVC_TYP_CD").alias("INVC_TYP_CD"),
        F.col("ChkReversals.BILL_DUE_DT").alias("BILL_DUE_DT"),
        F.col("ChkReversals.BILL_END_DT").alias("BILL_END_DT"),
        F.col("ChkReversals.CRT_DT").alias("CRT_DT"),
        F.when(F.col("rebill_lkup.BILL_INVC_ID").isNull(), F.lit("Y")).otherwise(F.lit("N")).alias("CUR_RCRD_IN")
    )
)

# We create Reversal
df_reversal = (
    df_join_rl.filter(F.col("rebill_lkup.BILL_INVC_ID").isNotNull())
    .select(
        F.col("ChkReversals.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("ChkReversals.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("ChkReversals.DISCARD_IN").alias("DISCARD_IN"),
        F.col("ChkReversals.PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("ChkReversals.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ChkReversals.ERR_CT").alias("ERR_CT"),
        F.col("ChkReversals.RECYCLE_CT").alias("RECYCLE_CT"),
        F.col("ChkReversals.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.concat(F.col("ChkReversals.PRI_KEY_STRING"), F.lit("R")).alias("PRI_KEY_STRING"),
        F.col("ChkReversals.INVC_SK").alias("INVC_SK"),
        F.concat(F.col("ChkReversals.BILL_INVC_ID"), F.lit("R")).alias("BILL_INVC_ID"),
        F.col("ChkReversals.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("ChkReversals.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("ChkReversals.BILL_ENTY").alias("BILL_ENTY"),
        F.col("ChkReversals.INVC_TYP_CD").alias("INVC_TYP_CD"),
        F.col("ChkReversals.BILL_DUE_DT").alias("BILL_DUE_DT"),
        F.col("ChkReversals.BILL_END_DT").alias("BILL_END_DT"),
        F.col("ChkReversals.CRT_DT").alias("CRT_DT"),
        F.lit("N").alias("CUR_RCRD_IN")
    )
)

# CollectData stage (union of NonReversal and Reversal)
df_collectdata = df_nonreversal.unionByName(df_reversal)

# CollectData -> Key (transform)
df_key_input = df_collectdata.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "INVC_SK",
    "BILL_INVC_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "BILL_ENTY",
    "INVC_TYP_CD",
    "BILL_DUE_DT",
    "BILL_END_DT",
    "CRT_DT",
    "CUR_RCRD_IN"
)

# Key stage
df_allcol = df_key_input.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("BILL_INVC_ID").alias("BILL_INVC_ID"),
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("INVC_SK").alias("INVC_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("BILL_ENTY").alias("BILL_ENTY"),
    F.col("INVC_TYP_CD").alias("INVC_TYP_CD"),
    F.col("BILL_DUE_DT").alias("BILL_DUE_DT"),
    F.col("BILL_END_DT").alias("BILL_END_DT"),
    F.col("CRT_DT").alias("CRT_DT"),
    F.col("CUR_RCRD_IN").alias("CUR_RCRD_IN")
)

df_transform = df_key_input.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("BILL_INVC_ID").alias("BILL_INVC_ID")
)

# Pass both into shared container IncmInvcPK
params_incmInvcPK = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "RunID": RunID,
    "CurrentDate": CurrDate,
    "IDSOwner": IDSOwner
}
df_incmInvcPK_output = IncmInvcPK(df_allcol, df_transform, params_incmInvcPK)

# IdsInvc (CSeqFileStage) -> final write
# Apply select with rpad for char/varchar columns
df_idsinvc_write = (
    df_incmInvcPK_output
    .select(
        F.col("JOB_EXCTN_RCRD_ERR_SK"),
        F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
        F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
        F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT"),
        F.col("ERR_CT"),
        F.col("RECYCLE_CT"),
        F.col("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING"),
        F.col("INVC_SK"),
        F.col("BILL_INVC_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("BILL_ENTY"),
        F.rpad(F.col("INVC_TYP_CD"), 10, " ").alias("INVC_TYP_CD"),
        F.rpad(F.col("BILL_DUE_DT"), 10, " ").alias("BILL_DUE_DT"),
        F.rpad(F.col("BILL_END_DT"), 10, " ").alias("BILL_END_DT"),
        F.rpad(F.col("CRT_DT"), 10, " ").alias("CRT_DT"),
        F.rpad(F.col("CUR_RCRD_IN"), 1, " ").alias("CUR_RCRD_IN")
    )
)

write_files(
    df_idsinvc_write,
    f"{adls_path}/key/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# hf_invc_rebills_rvrsl (CHashedFileStage) read from the same parquet
df_hf_invc_rebills_rvrsl = spark.read.parquet(f"{adls_path}/hf_invc_rebills.parquet")

# Facets_Source
jdbc_url_facets_source, jdbc_props_facets_source = get_db_config(facets_secret_name)
query_facets_source = """SELECT 
 INVC.BLIV_ID
FROM tempdb..#DriverTable#  TmpInvc, 
#$FacetsOwner#.CDS_INID_INVOICE INVC
WHERE 
 TmpInvc.BILL_INVC_ID = INVC.BLIV_ID
"""
df_facets_source = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets_source)
    .options(**jdbc_props_facets_source)
    .option("query", query_facets_source)
    .load()
)

# SnapshotReversalLogic
df_join_srl = (
    df_facets_source.alias("Snapshot")
    .join(
        df_hf_invc_rebills_rvrsl.alias("rebill_lkup"),
        (F.col("Snapshot.BLIV_ID") == F.col("rebill_lkup.BILL_INVC_ID")),
        how="left"
    )
)

df_snapshot_nonreversal = (
    df_join_srl.filter(F.col("rebill_lkup.BILL_INVC_ID").isNull())
    .select(
        F.trim(F.regexp_replace(F.col("Snapshot.BLIV_ID"), "[\\x0A\\x0D\\x09]", "")).alias("BILL_INVC_ID")
    )
)

df_snapshot_reversal = (
    df_join_srl.filter(F.col("rebill_lkup.BILL_INVC_ID").isNotNull())
    .select(
        F.concat(
            F.trim(F.regexp_replace(F.col("Snapshot.BLIV_ID"), "[\\x0A\\x0D\\x09]", "")),
            F.lit("R")
        ).alias("BILL_INVC_ID")
    )
)

df_collector2 = df_snapshot_nonreversal.unionByName(df_snapshot_reversal)

# Transform
df_transform_snapshot = df_collector2.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("BILL_INVC_ID").alias("BILL_INVC_ID")
)

# Snapshot_File
df_snapshot_file = df_transform_snapshot.select("SRC_SYS_CD_SK", "BILL_INVC_ID")

write_files(
    df_snapshot_file,
    f"{adls_path}/load/B_INVC.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)