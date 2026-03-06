# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     FctsSubAuditExtr
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Pulls data from Facets Audit CMC_SBSB_SUBSC table for loading to the IDS  SUB_AUDIT table
# MAGIC       
# MAGIC 
# MAGIC INPUTS:       Facets Audit 4.21 Membership Profile system
# MAGIC                       tables:  CMC_SBSB_SUBSC
# MAGIC 
# MAGIC 
# MAGIC HASH FILES:   hf_sub_audit
# MAGIC 
# MAGIC TRANSFORMS:   STRIP.FIELD
# MAGIC                              FORMAT.DATE
# MAGIC                            
# MAGIC PROCESSING:  Extract, transform, and primary keying for Membership subject area.
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS:   Sequential file
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC        
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date                    Project/Altiris #                     Change Description                                                                                   Development Project               Code Reviewer                   Date Reviewed
# MAGIC ----------------------------------      -------------------      -----------------------------------           ---------------------------------------------------------                                                             ----------------------------------              ---------------------------------           -------------------------
# MAGIC               Parikshith Chada   10/23/2006  -  Originally Programmed
# MAGIC Akhila Manickavelu          09/22/2016      5628-WorkersComp              Exclusion Criteria- P_SEL_PRCS_CRITR                                                                IntegrateDevl                 Kalyan Neelam                    2016-10-14
# MAGIC                                                                                                                   to remove wokers comp GRGR_ID's         
# MAGIC Prabhu ES                         02/24/2022     S2S Remediation                   MSSQL connection parameters added                                                                   IntegrateDev5		Ken Bradmon	2022-05-18

# MAGIC Strip Fields
# MAGIC All Update rows
# MAGIC All Delete and Insert Rows
# MAGIC Assign primary surrogate key
# MAGIC Apply business logic.
# MAGIC End of FACETS EXTRACTION JOB FROM CMC_SBSB_SUBSC
# MAGIC Writing Sequential File to ../key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, lag, length, coalesce, rpad, expr
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# Retrieve all required parameter values.
CurrRunCycle = get_widget_value("CurrRunCycle","")
RunID = get_widget_value("RunID","")
BeginDate = get_widget_value("BeginDate","")
EndDate = get_widget_value("EndDate","")
FacetsOwner = get_widget_value("FacetsOwner","")
BCBSOwner = get_widget_value("BCBSOwner","")

# Because we see $FacetsOwner and $BCBSOwner, we also retrieve their secret names:
facets_secret_name = get_widget_value("facets_secret_name","")
bcbs_secret_name = get_widget_value("bcbs_secret_name","")

# For the hashed-file scenario B, we assume IDS as the target schema:
ids_secret_name = get_widget_value("ids_secret_name","")

# ----------------------------------------------------------------------------
# Stage: hf_sub_audit_lookup (CHashedFileStage) - Scenario B
# ----------------------------------------------------------------------------
# Read from dummy table "IDS.dummy_hf_sub_audit" instead of the hashed file.
jdbc_url_hf_sub_audit, jdbc_props_hf_sub_audit = get_db_config(ids_secret_name)
extract_query_hf_sub_audit = "SELECT SRC_SYS_CD_SK, SUB_AUDIT_ROW_ID, SUB_AUDIT_SK, CRT_RUN_CYC_EXCTN_SK FROM IDS.dummy_hf_sub_audit"
df_hf_sub_audit_lookup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_hf_sub_audit)
    .options(**jdbc_props_hf_sub_audit)
    .option("query", extract_query_hf_sub_audit)
    .load()
)

# ----------------------------------------------------------------------------
# Stage: SubAuditExtr (ODBCConnector) reading from FacetsOwner, referencing BCBSOwner
# ----------------------------------------------------------------------------
jdbc_url_subaudit, jdbc_props_subaudit = get_db_config(facets_secret_name)

# Build the SQL query exactly as defined, replacing the parameter marks:
extract_query_subaudit = (
    f"SELECT SBSB.SBSB_CK,SBSB.HIST_ROW_ID,SBSB.HIST_CREATE_DTM,SBSB.HIST_USUS_ID,\n"
    f"SBSB.HIST_PHYS_ACT_CD,SBSB.HIST_IMAGE_CD,SBSB.TXN1_ROW_ID,SBSB.SBSB_LAST_NAME,\n"
    f"SBSB.SBSB_FIRST_NAME,SBSB.SBSB_MID_INIT,SBSB.SBSB_ORIG_EFF_DT,SBSB.SBSB_MCTR_STS,\n"
    f"SBSB.SBSB_HIRE_DT,SBSB.SBSB_RETIRE_DT,SBSB.SBSB_FI,SBSB.SBSB_RECD_DT,\n"
    f"SBSB.GRGR_CK \n"
    f"FROM {FacetsOwner}.CMC_SBSB_SUBSC SBSB\n"
    f"WHERE \n"
    f"SBSB.HIST_CREATE_DTM >= '{BeginDate}' \n"
    f"AND SBSB.HIST_CREATE_DTM <= '{EndDate}' \n"
    f"and  NOT EXISTS (SELECT DISTINCT\n"
    f"CMC_GRGR_GROUP.GRGR_CK \n"
    f"FROM \n"
    f"{BCBSOwner}.P_SEL_PRCS_CRITR P_SEL_PRCS_CRITR,\n"
    f"{FacetsOwner}.CMC_GRGR_GROUP CMC_GRGR_GROUP \n"
    f"WHERE P_SEL_PRCS_CRITR.SEL_PRCS_ID = 'WRHSINBEXCL'\n"
    f"and P_SEL_PRCS_CRITR.SEL_PRCS_ITEM_ID = 'WORKCOMP'\n"
    f"and P_SEL_PRCS_CRITR.SEL_PRCS_CRITR_CD = 'MEDPROC'\n"
    f"and CMC_GRGR_GROUP.GRGR_ID >= P_SEL_PRCS_CRITR.CRITR_VAL_FROM_TX\n"
    f"and CMC_GRGR_GROUP.GRGR_ID <= P_SEL_PRCS_CRITR.CRITR_VAL_THRU_TX and \n"
    f"SBSB.GRGR_CK=CMC_GRGR_GROUP.GRGR_CK)\n"
    f"ORDER BY SBSB.SBSB_CK,SBSB.TXN1_ROW_ID,SBSB.HIST_ROW_ID"
)

df_SubAuditExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_subaudit)
    .options(**jdbc_props_subaudit)
    .option("query", extract_query_subaudit)
    .load()
)

# ----------------------------------------------------------------------------
# Stage: StripFieldSbsb (CTransformerStage)
# ----------------------------------------------------------------------------
# The Convert(...) references are assumed to be "strip_field(...)"
df_Strip = df_SubAuditExtr.select(
    col("SBSB_CK").alias("SBSB_CK"),
    strip_field(col("HIST_ROW_ID")).alias("HIST_ROW_ID"),
    col("HIST_CREATE_DTM").alias("HIST_CREATE_DTM"),
    strip_field(col("HIST_USUS_ID")).alias("HIST_USUS_ID"),
    strip_field(col("HIST_PHYS_ACT_CD")).alias("HIST_PHYS_ACT_CD"),
    strip_field(col("HIST_IMAGE_CD")).alias("HIST_IMAGE_CD"),
    col("TXN1_ROW_ID").alias("TXN1_ROW_ID"),
    strip_field(col("SBSB_LAST_NAME")).alias("SBSB_LAST_NAME"),
    strip_field(col("SBSB_FIRST_NAME")).alias("SBSB_FIRST_NAME"),
    strip_field(col("SBSB_MID_INIT")).alias("SBSB_MID_INIT"),
    col("SBSB_ORIG_EFF_DT").alias("SBSB_ORIG_EFF_DT"),
    strip_field(col("SBSB_MCTR_STS")).alias("SBSB_MCTR_STS"),
    col("SBSB_HIRE_DT").alias("SBSB_HIRE_DT"),
    col("SBSB_RETIRE_DT").alias("SBSB_RETIRE_DT"),
    strip_field(col("SBSB_FI")).alias("SBSB_FI"),
    col("SBSB_RECD_DT").alias("SBSB_RECD_DT"),
    col("GRGR_CK").alias("GRGR_CK")
)

# ----------------------------------------------------------------------------
# Stage: Transformer_90 (CTransformerStage) => splits into InsDel (I or D) vs Update (U)
# ----------------------------------------------------------------------------
df_InsDel = df_Strip.filter("(HIST_PHYS_ACT_CD = 'I') or (HIST_PHYS_ACT_CD = 'D')")
df_InsDel = df_InsDel.select(
    col("SBSB_CK").alias("SBSB_CK"),
    col("HIST_ROW_ID").alias("HIST_ROW_ID"),
    col("HIST_CREATE_DTM").alias("HIST_CREATE_DTM"),
    col("HIST_USUS_ID").alias("HIST_USUS_ID"),
    col("HIST_PHYS_ACT_CD").alias("HIST_PHYS_ACT_CD"),
    col("HIST_IMAGE_CD").alias("HIST_IMAGE_CD"),
    col("TXN1_ROW_ID").alias("TXN1_ROW_ID"),
    col("SBSB_LAST_NAME").alias("SBSB_LAST_NAME"),
    col("SBSB_FIRST_NAME").alias("SBSB_FIRST_NAME"),
    col("SBSB_MID_INIT").alias("SBSB_MID_INIT"),
    col("SBSB_ORIG_EFF_DT").alias("SBSB_ORIG_EFF_DT"),
    col("SBSB_MCTR_STS").alias("SBSB_MCTR_STS"),
    col("SBSB_HIRE_DT").alias("SBSB_HIRE_DT"),
    col("SBSB_RETIRE_DT").alias("SBSB_RETIRE_DT"),
    col("SBSB_FI").alias("SBSB_FI"),
    col("SBSB_RECD_DT").alias("SBSB_RECD_DT"),
    col("GRGR_CK").alias("GRGR_CK")
)

df_Update = df_Strip.filter("HIST_PHYS_ACT_CD = 'U'")
df_Update = df_Update.select(
    col("SBSB_CK").alias("SBSB_CK"),
    col("HIST_ROW_ID").alias("HIST_ROW_ID"),
    col("HIST_CREATE_DTM").alias("HIST_CREATE_DTM"),
    col("HIST_USUS_ID").alias("HIST_USUS_ID"),
    col("HIST_PHYS_ACT_CD").alias("HIST_PHYS_ACT_CD"),
    col("HIST_IMAGE_CD").alias("HIST_IMAGE_CD"),
    col("TXN1_ROW_ID").alias("TXN1_ROW_ID"),
    col("SBSB_LAST_NAME").alias("SBSB_LAST_NAME"),
    col("SBSB_FIRST_NAME").alias("SBSB_FIRST_NAME"),
    col("SBSB_MID_INIT").alias("SBSB_MID_INIT"),
    col("SBSB_ORIG_EFF_DT").alias("SBSB_ORIG_EFF_DT"),
    col("SBSB_MCTR_STS").alias("SBSB_MCTR_STS"),
    col("SBSB_HIRE_DT").alias("SBSB_HIRE_DT"),
    col("SBSB_RETIRE_DT").alias("SBSB_RETIRE_DT"),
    col("SBSB_FI").alias("SBSB_FI"),
    col("SBSB_RECD_DT").alias("SBSB_RECD_DT"),
    col("GRGR_CK").alias("GRGR_CK")
)

# ----------------------------------------------------------------------------
# Stage: Transformer_127 (CTransformerStage) => logic with stage variables on df_Update
# ----------------------------------------------------------------------------
# We replicate row-by-row logic with a window approach. Since exact ordering is not specified,
# we assume an order by SBSB_CK, HIST_ROW_ID for demonstration. Stage variables compare current row
# to previous row's values:
w_127 = Window.orderBy("SBSB_CK", "HIST_ROW_ID")

df_127_vars = (
    df_Update
    .withColumn("svPrevSbsbLstName", lag(col("SBSB_LAST_NAME"), 1).over(w_127))
    .withColumn("svTempSbsbLstName", when(col("SBSB_LAST_NAME") == col("svPrevSbsbLstName"), lit("0")).otherwise(col("SBSB_LAST_NAME")))
    .withColumn("svPrevSbsbFstName", lag(col("SBSB_FIRST_NAME"), 1).over(w_127))
    .withColumn("svTempSbsbFstName", when(col("SBSB_FIRST_NAME") == col("svPrevSbsbFstName"), lit("0")).otherwise(col("SBSB_FIRST_NAME")))
    .withColumn("svPrevSbsbMidInit", lag(col("SBSB_MID_INIT"), 1).over(w_127))
    .withColumn("svTempSbsbMidInit", when(col("SBSB_MID_INIT") == col("svPrevSbsbMidInit"), lit("0")).otherwise(col("SBSB_MID_INIT")))
    .withColumn("svPrevOrigEffDt", lag(col("SBSB_ORIG_EFF_DT"), 1).over(w_127))
    .withColumn("svTempOrigEffDt", when(col("SBSB_ORIG_EFF_DT") == col("svPrevOrigEffDt"), lit("0")).otherwise(col("SBSB_ORIG_EFF_DT")))
    .withColumn("svPrevSbsbMctrSts", lag(col("SBSB_MCTR_STS"), 1).over(w_127))
    .withColumn("svTempSbsbMctrSts", when(col("SBSB_MCTR_STS") == col("svPrevSbsbMctrSts"), lit("0")).otherwise(col("SBSB_MCTR_STS")))
    .withColumn("svPrevSbsbHireDt", lag(col("SBSB_HIRE_DT"), 1).over(w_127))
    .withColumn("svTempSbsbHireDt", when(col("SBSB_HIRE_DT") == col("svPrevSbsbHireDt"), lit("0")).otherwise(col("SBSB_HIRE_DT")))
    .withColumn("svPrevSbsbRetireDt", lag(col("SBSB_RETIRE_DT"), 1).over(w_127))
    .withColumn("svTempSbsbRetireDt", when(col("SBSB_RETIRE_DT") == col("svPrevSbsbRetireDt"), lit("0")).otherwise(col("SBSB_RETIRE_DT")))
    .withColumn("svPrevSbsbFi", lag(col("SBSB_FI"), 1).over(w_127))
    .withColumn("svTempSbsbFi", when(col("SBSB_FI") == col("svPrevSbsbFi"), lit("0")).otherwise(col("SBSB_FI")))
    .withColumn("svCurrSbsbLstName", col("svTempSbsbLstName"))
    .withColumn("svCurrSbsbFstName", col("svTempSbsbFstName"))
    .withColumn("svCurrSbsbMidInit", col("svTempSbsbMidInit"))
    .withColumn("svCurrOrigEffDt", col("svTempOrigEffDt"))
    .withColumn("svCurrSbsbMctrSts", col("svTempSbsbMctrSts"))
    .withColumn("svCurrSbsbHireDt", col("svTempSbsbHireDt"))
    .withColumn("svCurrSbsbRetireDt", col("svTempSbsbRetireDt"))
    .withColumn("svCurrSbsbFi", col("svTempSbsbFi"))
)

# Apply the constraint: "Update.HIST_IMAGE_CD <> 'B' And (svCurrSbsbLstName <> 0 Or ... )"
df_127_filtered = df_127_vars.filter(
    (col("HIST_IMAGE_CD") != lit("B")) & (
        (col("svCurrSbsbLstName") != lit("0")) |
        (col("svCurrSbsbFstName") != lit("0")) |
        (col("svCurrSbsbMidInit") != lit("0")) |
        (col("svCurrOrigEffDt") != lit("0")) |
        (col("svCurrSbsbMctrSts") != lit("0")) |
        (col("svCurrSbsbHireDt") != lit("0")) |
        (col("svCurrSbsbRetireDt") != lit("0")) |
        (col("svCurrSbsbFi") != lit("0"))
    )
)

df_Trans127Out = df_127_filtered.select(
    col("SBSB_CK").alias("SBSB_CK"),
    col("HIST_ROW_ID").alias("HIST_ROW_ID"),
    col("HIST_CREATE_DTM").alias("HIST_CREATE_DTM"),
    col("HIST_USUS_ID").alias("HIST_USUS_ID"),
    col("HIST_PHYS_ACT_CD").alias("HIST_PHYS_ACT_CD"),
    col("HIST_IMAGE_CD").alias("HIST_IMAGE_CD"),
    col("TXN1_ROW_ID").alias("TXN1_ROW_ID"),
    col("SBSB_LAST_NAME").alias("SBSB_LAST_NAME"),
    col("SBSB_FIRST_NAME").alias("SBSB_FIRST_NAME"),
    col("SBSB_MID_INIT").alias("SBSB_MID_INIT"),
    col("SBSB_ORIG_EFF_DT").alias("SBSB_ORIG_EFF_DT"),
    col("SBSB_MCTR_STS").alias("SBSB_MCTR_STS"),
    col("SBSB_HIRE_DT").alias("SBSB_HIRE_DT"),
    col("SBSB_RETIRE_DT").alias("SBSB_RETIRE_DT"),
    col("SBSB_FI").alias("SBSB_FI"),
    col("SBSB_RECD_DT").alias("SBSB_RECD_DT"),
    col("GRGR_CK").alias("GRGR_CK")
)

# ----------------------------------------------------------------------------
# Stage: Transformer_130 (CTransformerStage) => simply pass InsDel to output
# ----------------------------------------------------------------------------
df_Transformer_130 = df_InsDel.select(
    col("SBSB_CK").alias("SBSB_CK"),
    col("HIST_ROW_ID").alias("HIST_ROW_ID"),
    col("HIST_CREATE_DTM").alias("HIST_CREATE_DTM"),
    col("HIST_USUS_ID").alias("HIST_USUS_ID"),
    col("HIST_PHYS_ACT_CD").alias("HIST_PHYS_ACT_CD"),
    col("HIST_IMAGE_CD").alias("HIST_IMAGE_CD"),
    col("TXN1_ROW_ID").alias("TXN1_ROW_ID"),
    col("SBSB_LAST_NAME").alias("SBSB_LAST_NAME"),
    col("SBSB_FIRST_NAME").alias("SBSB_FIRST_NAME"),
    col("SBSB_MID_INIT").alias("SBSB_MID_INIT"),
    col("SBSB_ORIG_EFF_DT").alias("SBSB_ORIG_EFF_DT"),
    col("SBSB_MCTR_STS").alias("SBSB_MCTR_STS"),
    col("SBSB_HIRE_DT").alias("SBSB_HIRE_DT"),
    col("SBSB_RETIRE_DT").alias("SBSB_RETIRE_DT"),
    col("SBSB_FI").alias("SBSB_FI"),
    col("SBSB_RECD_DT").alias("SBSB_RECD_DT"),
    col("GRGR_CK").alias("GRGR_CK")
)

# ----------------------------------------------------------------------------
# Stage: Link_Collector_105 (CCollector) => collects from Transformer_127 (DSLink103) + Transformer_130 (DSLink104)
# ----------------------------------------------------------------------------
df_Link_Collector_105 = df_Trans127Out.unionByName(df_Transformer_130, allowMissingColumns=False)

df_Keys = df_Link_Collector_105.select(
    col("SBSB_CK").alias("SBSB_CK"),
    col("HIST_ROW_ID").alias("HIST_ROW_ID"),
    col("HIST_CREATE_DTM").alias("HIST_CREATE_DTM"),
    col("HIST_USUS_ID").alias("HIST_USUS_ID"),
    col("HIST_PHYS_ACT_CD").alias("HIST_PHYS_ACT_CD"),
    col("HIST_IMAGE_CD").alias("HIST_IMAGE_CD"),
    col("TXN1_ROW_ID").alias("TXN1_ROW_ID"),
    col("SBSB_LAST_NAME").alias("SBSB_LAST_NAME"),
    col("SBSB_FIRST_NAME").alias("SBSB_FIRST_NAME"),
    col("SBSB_MID_INIT").alias("SBSB_MID_INIT"),
    col("SBSB_ORIG_EFF_DT").alias("SBSB_ORIG_EFF_DT"),
    col("SBSB_MCTR_STS").alias("SBSB_MCTR_STS"),
    col("SBSB_HIRE_DT").alias("SBSB_HIRE_DT"),
    col("SBSB_RETIRE_DT").alias("SBSB_RETIRE_DT"),
    col("SBSB_FI").alias("SBSB_FI"),
    col("SBSB_RECD_DT").alias("SBSB_RECD_DT"),
    col("GRGR_CK").alias("GRGR_CK")
)

# ----------------------------------------------------------------------------
# Stage: BusinessLogic (CTransformerStage)
# ----------------------------------------------------------------------------
# This stage sets a row pass thru variable 'Y', plus transforms. We treat "FORMAT.DATE(..., 'DB2TIMESTAMP')" as current_timestamp() or equivalent
# but instructions say we do not import that from pyspark, so we treat it as already available if it can be a custom routine. 
# However, the stage says “FORMAT.DATE(@DATE, "DATE", "CURRENT", "DB2TIMESTAMP")” => that was not in the JSON. 
# Actually the JSON has: FIRST_RECYC_DT => "FORMAT.DATE(@DATE, \"DATE\", \"CURRENT\", \"DB2TIMESTAMP\")"
# The guidelines say: "FORMAT.DATE(@DATE, ... 'SYBTIMESTAMP')" => let us translate to current_timestamp(). 
# So we do that:

df_BusinessLogic = df_Keys.select(
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    # INSRT_UPDT_CD (char(10))
    lit("I").alias("INSRT_UPDT_CD"),  
    # DISCARD_IN (char(1))
    lit("N").alias("DISCARD_IN"),
    # PASS_THRU_IN (char(1)) => row pass thru 'Y'
    lit("Y").alias("PASS_THRU_IN"),
    # FIRST_RECYC_DT => translate to current_timestamp()
    current_timestamp().alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    lit("FACETS").alias("SRC_SYS_CD"),
    # PRI_KEY_STRING => "FACETS" : ";" : Keys.HIST_ROW_ID
    expr("concat('FACETS',';', HIST_ROW_ID)").alias("PRI_KEY_STRING"),
    lit(0).alias("SUB_AUDIT_SK"),
    col("HIST_ROW_ID").alias("SUB_AUDIT_ROW_ID"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("HIST_USUS_ID").alias("SRC_SYS_CRT_USER_SK"),
    col("SBSB_CK").alias("SUB_SK"),
    when(
        (length(trim(col("HIST_PHYS_ACT_CD"))) == lit(0)) | col("HIST_PHYS_ACT_CD").isNull(), 
        lit("NA")
    ).otherwise(col("HIST_PHYS_ACT_CD")).alias("SUB_AUDIT_ACTN_CD_SK"),
    when(
        (length(trim(col("SBSB_FI"))) == lit(0)) | col("SBSB_FI").isNull(),
        lit("NA")
    ).otherwise(
        when(col("SBSB_FI") == lit("*"), lit("IDB")).otherwise(col("SBSB_FI"))
    ).alias("SUB_FMLY_CNTR_CD_SK"),
    when(col("GRGR_CK") == lit(38), lit("Y")).otherwise(lit("N")).alias("SCRD_IN"),
    expr("FORMAT.DATE(SBSB_ORIG_EFF_DT, 'SYBASE', 'TIMESTAMP', 'CCYY-MM-DD')").alias("ORIG_EFF_DT_SK"),
    expr("FORMAT.DATE(SBSB_RETIRE_DT, 'SYBASE', 'TIMESTAMP', 'CCYY-MM-DD')").alias("RETR_DT_SK"),
    expr("FORMAT.DATE(HIST_CREATE_DTM, 'SYBASE', 'TIMESTAMP', 'CCYY-MM-DD')").alias("SRC_SYS_CRT_DT_SK"),
    col("SBSB_CK").alias("SUB_UNIQ_KEY"),
    when(length(trim(col("SBSB_FIRST_NAME"))) == lit(0), expr("null")).otherwise(col("SBSB_FIRST_NAME")).alias("SUB_FIRST_NM"),
    when(length(trim(col("SBSB_MID_INIT"))) == lit(0), expr("null")).otherwise(col("SBSB_MID_INIT")).alias("SUB_MIDINIT"),
    when(length(trim(col("SBSB_LAST_NAME"))) == lit(0), expr("null")).otherwise(col("SBSB_LAST_NAME")).alias("SUB_LAST_NM"),
    expr("FORMAT.DATE(SBSB_HIRE_DT, 'SYBASE', 'TIMESTAMP', 'CCYY-MM-DD')").alias("SUB_AUDIT_HIRE_DT_TX"),
    expr("FORMAT.DATE(SBSB_RECD_DT, 'SYBASE', 'TIMESTAMP', 'CCYY-MM-DD')").alias("SUB_AUDIT_RCVD_DT_TX"),
    col("SBSB_MCTR_STS").alias("SUB_AUDIT_STTUS_TX")
).cache()  # optional cache for repeated usage

# ----------------------------------------------------------------------------
# Stage: PrimaryKey (CTransformerStage)
# ----------------------------------------------------------------------------
# 1) We have a left join with the dummy HF table (df_hf_sub_audit_lookup).
#    Join conditions: Transform.SRC_SYS_CD => lkup.SRC_SYS_CD_SK, Transform.SUB_AUDIT_ROW_ID => lkup.SUB_AUDIT_ROW_ID
#    Because it is a left join, we must do a real left join in Spark, not a cross join.
df_joined_primarykey = (
    df_BusinessLogic.alias("Transform")
    .join(
        df_hf_sub_audit_lookup.alias("lkup"),
        on=[
            col("Transform.SRC_SYS_CD") == col("lkup.SRC_SYS_CD_SK"),
            col("Transform.SUB_AUDIT_ROW_ID") == col("lkup.SUB_AUDIT_ROW_ID")
        ],
        how="left"
    )
)

# 2) KeyMgtGetNextValueConcurrent => SurrogateKeyGen for "SUB_AUDIT_SK".
#    We will generate a temporary surrogate column, then apply the logic:
df_enriched = df_joined_primarykey  # to apply SurrogateKeyGen
df_enriched = SurrogateKeyGen(df_enriched, <DB sequence name>, "tempSurKey", <schema>, <secret_name>)

# Now replicate the logic:
# SK = if IsNull(lkup.SUB_AUDIT_SK) then df_enriched.tempSurKey else lkup.SUB_AUDIT_SK
# NewCrtRunCycExtcnSk = if IsNull(lkup.SUB_AUDIT_SK) then CurrRunCycle else lkup.CRT_RUN_CYC_EXCTN_SK
df_with_sk = df_enriched.select(
    col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    col("Transform.DISCARD_IN").alias("DISCARD_IN"),
    col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
    col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("Transform.ERR_CT").alias("ERR_CT"),
    col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    when(col("lkup.SUB_AUDIT_SK").isNull(), col("tempSurKey")).otherwise(col("lkup.SUB_AUDIT_SK")).alias("SK"),
    col("Transform.SUB_AUDIT_ROW_ID").alias("SUB_AUDIT_ROW_ID"),
    when(col("lkup.SUB_AUDIT_SK").isNull(), lit(CurrRunCycle).cast("long")).otherwise(col("lkup.CRT_RUN_CYC_EXCTN_SK")).alias("NewCrtRunCycExtcnSk"),
    lit(CurrRunCycle).cast("long").alias("CurrRunCycleVal"),
    col("Transform.SRC_SYS_CRT_USER_SK").alias("SRC_SYS_CRT_USER_SK"),
    col("Transform.SUB_SK").alias("SUB_SK"),
    col("Transform.SUB_AUDIT_ACTN_CD_SK").alias("SUB_AUDIT_ACTN_CD_SK"),
    col("Transform.SUB_FMLY_CNTR_CD_SK").alias("SUB_FMLY_CNTR_CD_SK"),
    col("Transform.SCRD_IN").alias("SCRD_IN"),
    col("Transform.ORIG_EFF_DT_SK").alias("ORIG_EFF_DT_SK"),
    col("Transform.RETR_DT_SK").alias("RETR_DT_SK"),
    col("Transform.SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
    col("Transform.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    col("Transform.SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    col("Transform.SUB_MIDINIT").alias("SUB_MIDINIT"),
    col("Transform.SUB_LAST_NM").alias("SUB_LAST_NM"),
    col("Transform.SUB_AUDIT_HIRE_DT_TX").alias("SUB_AUDIT_HIRE_DT_TX"),
    col("Transform.SUB_AUDIT_RCVD_DT_TX").alias("SUB_AUDIT_RCVD_DT_TX"),
    col("Transform.SUB_AUDIT_STTUS_TX").alias("SUB_AUDIT_STTUS_TX"),
    col("lkup.SUB_AUDIT_SK").alias("lkupSubAuditSk"),  # keep for next constraint
    col("lkup.CRT_RUN_CYC_EXCTN_SK").alias("lkupCrtRunCyc")
)

# Split into two outputs:
# 2a) "Key" link => Always output
df_Key = df_with_sk.select(
    col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    col("DISCARD_IN").alias("DISCARD_IN"),
    col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("ERR_CT").alias("ERR_CT"),
    col("RECYCLE_CT").alias("RECYCLE_CT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("SK").alias("SUB_AUDIT_SK"),
    col("SUB_AUDIT_ROW_ID").alias("SUB_AUDIT_ROW_ID"),
    col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("CurrRunCycleVal").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("SRC_SYS_CRT_USER_SK").alias("SRC_SYS_CRT_USER_SK"),
    col("SUB_SK").alias("SUB_SK"),
    col("SUB_AUDIT_ACTN_CD_SK").alias("SUB_AUDIT_ACTN_CD_SK"),
    col("SUB_FMLY_CNTR_CD_SK").alias("SUB_FMLY_CNTR_CD_SK"),
    col("SCRD_IN").alias("SCRD_IN"),
    col("ORIG_EFF_DT_SK").alias("ORIG_EFF_DT_SK"),
    col("RETR_DT_SK").alias("RETR_DT_SK"),
    col("SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
    col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    col("SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    col("SUB_MIDINIT").alias("SUB_MIDINIT"),
    col("SUB_LAST_NM").alias("SUB_LAST_NM"),
    col("SUB_AUDIT_HIRE_DT_TX").alias("SUB_AUDIT_HIRE_DT_TX"),
    col("SUB_AUDIT_RCVD_DT_TX").alias("SUB_AUDIT_RCVD_DT_TX"),
    col("SUB_AUDIT_STTUS_TX").alias("SUB_AUDIT_STTUS_TX")
)

# 2b) "updt" link => Constraint: "IsNull(lkup.SUB_AUDIT_SK) = @TRUE"
df_updt = df_with_sk.filter(col("lkupSubAuditSk").isNull()).select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD_SK"),
    col("SUB_AUDIT_ROW_ID").alias("SUB_AUDIT_ROW_ID"),
    col("SK").alias("SUB_AUDIT_SK"),
    col("CurrRunCycleVal").alias("CRT_RUN_CYC_EXCTN_SK")
)

# ----------------------------------------------------------------------------
# Stage: hf_sub_audit (CHashedFileStage) - Scenario B => Write back to the same dummy table
# ----------------------------------------------------------------------------
# Because in DataStage this does an insert of new rows only, we do an upsert with "when matched then do nothing"
#   Key columns: SRC_SYS_CD_SK, SUB_AUDIT_ROW_ID
#   Insert columns: SRC_SYS_CD_SK, SUB_AUDIT_ROW_ID, SUB_AUDIT_SK, CRT_RUN_CYC_EXCTN_SK
# We'll create a staging table and then MERGE.
if df_updt.head(1):
    # Save df_updt to a STAGING table
    temp_table = "STAGING.FctsSubAuditExtr_hf_sub_audit_temp"
    execute_dml(f"DROP TABLE IF EXISTS {temp_table}", jdbc_url_hf_sub_audit, jdbc_props_hf_sub_audit)
    (
        df_updt.write
        .format("jdbc")
        .option("url", jdbc_url_hf_sub_audit)
        .options(**jdbc_props_hf_sub_audit)
        .option("dbtable", temp_table)
        .mode("overwrite")
        .save()
    )
    # Construct MERGE statement
    merge_sql = f"""
MERGE INTO IDS.dummy_hf_sub_audit AS T
USING {temp_table} AS S
ON 
    T.SRC_SYS_CD_SK = S.SRC_SYS_CD_SK
    AND T.SUB_AUDIT_ROW_ID = S.SUB_AUDIT_ROW_ID
WHEN MATCHED THEN
    UPDATE SET
      SRC_SYS_CD_SK = T.SRC_SYS_CD_SK
      -- do nothing effectively
WHEN NOT MATCHED THEN
    INSERT (SRC_SYS_CD_SK, SUB_AUDIT_ROW_ID, SUB_AUDIT_SK, CRT_RUN_CYC_EXCTN_SK)
    VALUES (S.SRC_SYS_CD_SK, S.SUB_AUDIT_ROW_ID, S.SUB_AUDIT_SK, S.CRT_RUN_CYC_EXCTN_SK);
"""
    execute_dml(merge_sql, jdbc_url_hf_sub_audit, jdbc_props_hf_sub_audit)

# ----------------------------------------------------------------------------
# Stage: IdsSubAuditExtr (CSeqFileStage) => writes the final output from df_Key to a .dat file
# ----------------------------------------------------------------------------
# The file path: "key/FctsSubAuditExtr.FctsSubAudit.dat.#RunID#"
# We use adls_path for "key":
file_path_IdsSubAuditExtr = f"{adls_path}/key/FctsSubAuditExtr.FctsSubAudit.dat.{RunID}"

# The final DataFrame must have the same columns in the same order, applying rpad for char/varchar columns:
# The schema from the "Key" link:
#  1) JOB_EXCTN_RCRD_ERR_SK
#  2) INSRT_UPDT_CD (char(10))
#  3) DISCARD_IN (char(1))
#  4) PASS_THRU_IN (char(1))
#  5) FIRST_RECYC_DT
#  6) ERR_CT
#  7) RECYCLE_CT
#  8) SRC_SYS_CD
#  9) PRI_KEY_STRING
# 10) SUB_AUDIT_SK
# 11) SUB_AUDIT_ROW_ID
# 12) CRT_RUN_CYC_EXCTN_SK
# 13) LAST_UPDT_RUN_CYC_EXCTN_SK
# 14) SRC_SYS_CRT_USER_SK
# 15) SUB_SK
# 16) SUB_AUDIT_ACTN_CD_SK
# 17) SUB_FMLY_CNTR_CD_SK
# 18) SCRD_IN (char(1))
# 19) ORIG_EFF_DT_SK (char(10))
# 20) RETR_DT_SK (char(10))
# 21) SRC_SYS_CRT_DT_SK (char(10))
# 22) SUB_UNIQ_KEY
# 23) SUB_FIRST_NM
# 24) SUB_MIDINIT (char(1))
# 25) SUB_LAST_NM
# 26) SUB_AUDIT_HIRE_DT_TX
# 27) SUB_AUDIT_RCVD_DT_TX
# 28) SUB_AUDIT_STTUS_TX

df_final = df_Key.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("SUB_AUDIT_SK"),
    col("SUB_AUDIT_ROW_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("SRC_SYS_CRT_USER_SK"),
    col("SUB_SK"),
    col("SUB_AUDIT_ACTN_CD_SK"),
    col("SUB_FMLY_CNTR_CD_SK"),
    rpad(col("SCRD_IN"), 1, " ").alias("SCRD_IN"),
    rpad(col("ORIG_EFF_DT_SK"), 10, " ").alias("ORIG_EFF_DT_SK"),
    rpad(col("RETR_DT_SK"), 10, " ").alias("RETR_DT_SK"),
    rpad(col("SRC_SYS_CRT_DT_SK"), 10, " ").alias("SRC_SYS_CRT_DT_SK"),
    col("SUB_UNIQ_KEY"),
    col("SUB_FIRST_NM"),
    rpad(col("SUB_MIDINIT"), 1, " ").alias("SUB_MIDINIT"),
    col("SUB_LAST_NM"),
    col("SUB_AUDIT_HIRE_DT_TX"),
    col("SUB_AUDIT_RCVD_DT_TX"),
    col("SUB_AUDIT_STTUS_TX")
)

write_files(
    df_final,
    file_path_IdsSubAuditExtr,
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote='"',
    nullValue=None
)