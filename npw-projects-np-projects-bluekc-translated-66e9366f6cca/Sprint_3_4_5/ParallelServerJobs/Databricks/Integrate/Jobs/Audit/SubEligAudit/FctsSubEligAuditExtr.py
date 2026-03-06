# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:     FctsSubEligAuditExtr
# MAGIC 
# MAGIC DESCRIPTION:   Pulls data from Facets Audit CMC_SBEL_ELIG_ENT table for loading to the IDS SUB_ELIG_AUDIT table
# MAGIC       
# MAGIC INPUTS:       Facets Audit 4.21 Membership Profile system
# MAGIC                       tables:  CMC_SBEL_ELIG_ENT
# MAGIC 
# MAGIC HASH FILES:  hf_sub_elig_audit
# MAGIC                       
# MAGIC TRANSFORMS:   STRIP.FIELD
# MAGIC                              FORMAT.DATE
# MAGIC                            
# MAGIC PROCESSING:  Extract, transform, and primary keying for Membership subject area.
# MAGIC   
# MAGIC OUTPUTS:   Sequential file
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC          
# MAGIC Developer                Date                 	Project/Altiris #	Change Description                                                                     Development Project      Code Reviewer          Date Reviewed      
# MAGIC ------------------              --------------------	------------------------	-----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------   
# MAGIC Parikshith Chada   10/26/2006  -  Originally Programmed
# MAGIC Kalyan Neelam         2011-11-04               TTR-456                Added new column CSPI_ID on end                                           IntegrateCurDevl             SAndrew                       2011-11-09
# MAGIC                                                                                                 Changed VOID_IND default value to 'N' from 'X', 
# MAGIC                                                                                                 Added default value = 'NA' to multiple fields, removed all the data elements.
# MAGIC 
# MAGIC Akhila  M           09/22/2016      5628-WorkersComp              Exclusion Criteria- P_SEL_PRCS_CRITR                                     IntegrateDevl                 Kalyan Neelam              2016-10-14
# MAGIC                                                                                                                   to remove wokers comp GRGR_ID's  
# MAGIC Prabhu ES         02/24/2022      S2S Remediation                  MSSQL connection parameters added                                         IntegrateDev5     	Ken Bradmon	2022-05-18

# MAGIC Strip Fields
# MAGIC All Update rows
# MAGIC All Delete and Insert Rows
# MAGIC Assign primary surrogate key
# MAGIC Apply business logic.
# MAGIC End of FACETS EXTRACTION JOB FROM CMC_SBEL_ELIG_ENT
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
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, rpad
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Retrieve all required parameter values
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
FacetsOwner = get_widget_value('FacetsOwner','')
bcbsOwner = get_widget_value('BCBSOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
bcbs_secret_name = get_widget_value('bcbs_secret_name','')
BeginDate = get_widget_value('BeginDate','')
EndDate = get_widget_value('EndDate','')
# Scenario B hashed-file read/write also implies an IDS or EDW database. 
# For illustration, assume IDS:
ids_secret_name = get_widget_value('ids_secret_name','')

# --------------------------------------------------------------------------------
# Read dummy table for hf_sub_elig_audit_lookup (Scenario B)
# --------------------------------------------------------------------------------
jdbc_url_hf_sub_elig_audit, jdbc_props_hf_sub_elig_audit = get_db_config(ids_secret_name)
query_hf_sub_elig_audit_lookup = "SELECT SRC_SYS_CD, SUB_ELIG_AUDIT_ROW_ID, SUB_ELIG_AUDIT_SK, CRT_RUN_CYC_EXCTN_SK FROM IDS.dummy_hf_sub_elig_audit"
df_hf_sub_elig_audit_lookup = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url_hf_sub_elig_audit)
    .options(**jdbc_props_hf_sub_elig_audit)
    .option("query", query_hf_sub_elig_audit_lookup)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: SubEligAuditExtr (ODBCConnector) reads from Facets connection
# --------------------------------------------------------------------------------
jdbc_url_sub_elig, jdbc_props_sub_elig = get_db_config(facets_secret_name)
extract_query = (
    f"SELECT SBEL.SBSB_CK,SBEL.SBEL_EFF_DT,SBEL.HIST_ROW_ID,SBEL.HIST_CREATE_DTM,SBEL.HIST_USUS_ID,"
    f"SBEL.HIST_PHYS_ACT_CD,SBEL.HIST_IMAGE_CD,SBEL.SBEL_ELIG_TYPE,SBEL.CSPD_CAT,SBEL.EXCD_ID,"
    f"SBEL.SBEL_VOID_IND,SBEL.TXN1_ROW_ID,SBEL.CSPI_ID,SBEL.SBEL_FI,SBEL.GRGR_CK "
    f"FROM {FacetsOwner}.CMC_SBEL_ELIG_ENT SBEL "
    f"WHERE SBEL.HIST_CREATE_DTM >= '{BeginDate}' "
    f"AND SBEL.HIST_CREATE_DTM <= '{EndDate}' "
    f"AND NOT EXISTS (SELECT DISTINCT CMC_GRGR_GROUP.GRGR_CK "
    f"FROM {bcbsOwner}.P_SEL_PRCS_CRITR P_SEL_PRCS_CRITR, {FacetsOwner}.CMC_GRGR_GROUP CMC_GRGR_GROUP "
    f"WHERE P_SEL_PRCS_CRITR.SEL_PRCS_ID = 'WRHSINBEXCL' "
    f"AND P_SEL_PRCS_CRITR.SEL_PRCS_ITEM_ID = 'WORKCOMP' "
    f"AND P_SEL_PRCS_CRITR.SEL_PRCS_CRITR_CD = 'MEDPROC' "
    f"AND CMC_GRGR_GROUP.GRGR_ID >= P_SEL_PRCS_CRITR.CRITR_VAL_FROM_TX "
    f"AND CMC_GRGR_GROUP.GRGR_ID <= P_SEL_PRCS_CRITR.CRITR_VAL_THRU_TX "
    f"AND SBEL.GRGR_CK=CMC_GRGR_GROUP.GRGR_CK) "
    f"ORDER BY SBEL.SBSB_CK, SBEL.TXN1_ROW_ID, SBEL.HIST_ROW_ID"
)
df_SubEligAuditExtr = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url_sub_elig)
    .options(**jdbc_props_sub_elig)
    .option("query", extract_query)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: StripFieldSbel (CTransformerStage)
# --------------------------------------------------------------------------------
# Replicating "Convert(CHAR(10):CHAR(13):CHAR(9), '', field)" with strip_field(...)
df_Strip = df_SubEligAuditExtr.select(
    col("SBSB_CK").alias("SBSB_CK"),
    col("SBEL_EFF_DT").alias("SBEL_EFF_DT"),
    strip_field(col("HIST_ROW_ID")).alias("HIST_ROW_ID"),
    col("HIST_CREATE_DTM").alias("HIST_CREATE_DTM"),
    strip_field(col("HIST_USUS_ID")).alias("HIST_USUS_ID"),
    strip_field(col("HIST_PHYS_ACT_CD")).alias("HIST_PHYS_ACT_CD"),
    strip_field(col("HIST_IMAGE_CD")).alias("HIST_IMAGE_CD"),
    strip_field(col("SBEL_ELIG_TYPE")).alias("SBEL_ELIG_TYPE"),
    strip_field(col("CSPD_CAT")).alias("CSPD_CAT"),
    strip_field(col("EXCD_ID")).alias("EXCD_ID"),
    strip_field(col("SBEL_VOID_IND")).alias("SBEL_VOID_IND"),
    col("TXN1_ROW_ID").alias("TXN1_ROW_ID"),
    strip_field(col("CSPI_ID")).alias("CSPI_ID"),
    strip_field(col("SBEL_FI")).alias("SBEL_FI")
)

# --------------------------------------------------------------------------------
# Stage: Transformer_90 (CTransformerStage) -> Outputs: InsDel, Update
# --------------------------------------------------------------------------------
# InsDel: constraint => Strip.HIST_PHYS_ACT_CD = 'I' Or 'D'
df_InsDel = (
    df_Strip
    .filter((col("HIST_PHYS_ACT_CD") == 'I') | (col("HIST_PHYS_ACT_CD") == 'D'))
    .select(
        col("SBSB_CK").alias("SBSB_CK"),
        col("SBEL_EFF_DT").alias("SBEL_EFF_DT"),
        col("HIST_ROW_ID").alias("HIST_ROW_ID"),
        col("HIST_CREATE_DTM").alias("HIST_CREATE_DTM"),
        rpad(col("HIST_USUS_ID"), 20, " ").alias("HIST_USUS_ID"),
        rpad(col("HIST_PHYS_ACT_CD"), 1, " ").alias("HIST_PHYS_ACT_CD"),
        rpad(col("HIST_IMAGE_CD"), 1, " ").alias("HIST_IMAGE_CD"),
        rpad(col("SBEL_ELIG_TYPE"), 2, " ").alias("SBEL_ELIG_TYPE"),
        rpad(col("CSPD_CAT"), 1, " ").alias("CSPD_CAT"),
        rpad(col("EXCD_ID"), 3, " ").alias("EXCD_ID"),
        rpad(col("SBEL_VOID_IND"), 1, " ").alias("SBEL_VOID_IND"),
        col("TXN1_ROW_ID").alias("TXN1_ROW_ID"),
        rpad(col("CSPI_ID"), 8, " ").alias("CSPI_ID"),
        rpad(col("SBEL_FI"), 1, " ").alias("SBEL_FI")
    )
)

# Update: constraint => Strip.HIST_PHYS_ACT_CD = 'U'
df_Update = (
    df_Strip
    .filter(col("HIST_PHYS_ACT_CD") == 'U')
    .select(
        col("SBSB_CK").alias("SBSB_CK"),
        col("SBEL_EFF_DT").alias("SBEL_EFF_DT"),
        col("HIST_ROW_ID").alias("HIST_ROW_ID"),
        col("HIST_CREATE_DTM").alias("HIST_CREATE_DTM"),
        rpad(col("HIST_USUS_ID"), 20, " ").alias("HIST_USUS_ID"),
        rpad(col("HIST_PHYS_ACT_CD"), 1, " ").alias("HIST_PHYS_ACT_CD"),
        rpad(col("HIST_IMAGE_CD"), 1, " ").alias("HIST_IMAGE_CD"),
        rpad(col("SBEL_ELIG_TYPE"), 2, " ").alias("SBEL_ELIG_TYPE"),
        rpad(col("CSPD_CAT"), 1, " ").alias("CSPD_CAT"),
        rpad(col("EXCD_ID"), 3, " ").alias("EXCD_ID"),
        rpad(col("SBEL_VOID_IND"), 1, " ").alias("SBEL_VOID_IND"),
        col("TXN1_ROW_ID").alias("TXN1_ROW_ID"),
        rpad(col("CSPI_ID"), 8, " ").alias("CSPI_ID"),
        rpad(col("SBEL_FI"), 1, " ").alias("SBEL_FI")
    )
)

# --------------------------------------------------------------------------------
# Stage: Transformer_127 (simulating row-by-row Stage Variables on df_Update)
# --------------------------------------------------------------------------------
# Attempting to replicate stage-variable logic with window functions.
# The job references previous row logic: "If current == previous then 0 else current."
# We define a window ordering by (SBSB_CK, SBEL_EFF_DT, HIST_ROW_ID) to approximate sequential processing.
window_127 = Window.orderBy("SBSB_CK", "SBEL_EFF_DT", "HIST_ROW_ID")

df_Update_127 = (
    df_Update
    # Create lag columns
    .withColumn("lag_SBEL_EFF_DT", F.lag(col("SBEL_EFF_DT"), 1).over(window_127))
    .withColumn("lag_SBEL_ELIG_TYPE", F.lag(col("SBEL_ELIG_TYPE"), 1).over(window_127))
    .withColumn("lag_CSPI_ID", F.lag(col("CSPI_ID"), 1).over(window_127))
    .withColumn("lag_SBEL_VOID_IND", F.lag(col("SBEL_VOID_IND"), 1).over(window_127))
    .withColumn("lag_SBEL_FI", F.lag(col("SBEL_FI"), 1).over(window_127))
    # Replicate "svTemp" logic
    .withColumn(
        "svTempSbelEfffDt",
        when(col("SBEL_EFF_DT") == col("lag_SBEL_EFF_DT"), F.lit(0)).otherwise(col("SBEL_EFF_DT"))
    )
    .withColumn(
        "svTempSbelEligType",
        when(col("SBEL_ELIG_TYPE") == col("lag_SBEL_ELIG_TYPE"), F.lit(0)).otherwise(col("SBEL_ELIG_TYPE"))
    )
    .withColumn(
        "svTempCspiId",
        when(col("CSPI_ID") == col("lag_CSPI_ID"), F.lit(0)).otherwise(col("CSPI_ID"))
    )
    .withColumn(
        "svTempSbelVoidInd",
        when(col("SBEL_VOID_IND") == col("lag_SBEL_VOID_IND"), F.lit(0)).otherwise(col("SBEL_VOID_IND"))
    )
    .withColumn(
        "svTempSbelFi",
        when(col("SBEL_FI") == col("lag_SBEL_FI"), F.lit(0)).otherwise(col("SBEL_FI"))
    )
    # "svCurr" columns mirror "svTemp"
    .withColumn("svCurrSbelEfffDt", col("svTempSbelEfffDt"))
    .withColumn("svCurrSbelEligType", col("svTempSbelEligType"))
    .withColumn("svCurrCspiId", col("svTempCspiId"))
    .withColumn("svCurrSbelVoidInd", col("svTempSbelVoidInd"))
    .withColumn("svCurrSbelFi", col("svTempSbelFi"))
    # Now apply the output constraint: HIST_IMAGE_CD <> 'B' AND (any svCurr != 0)
    .filter(
        (col("HIST_IMAGE_CD") != 'B') &
        (
            (col("svCurrSbelEfffDt") != 0) |
            (col("svCurrSbelEligType") != 0) |
            (col("svCurrCspiId") != 0) |
            (col("svCurrSbelVoidInd") != 0) |
            (col("svCurrSbelFi") != 0)
        )
    )
    # Finally select the columns from "Update" link definition in Transformer_127
    .select(
        col("SBSB_CK").alias("SBSB_CK"),
        col("SBEL_EFF_DT").alias("SBEL_EFF_DT"),
        col("HIST_ROW_ID").alias("HIST_ROW_ID"),
        col("HIST_CREATE_DTM").alias("HIST_CREATE_DTM"),
        col("HIST_USUS_ID").alias("HIST_USUS_ID"),
        col("HIST_PHYS_ACT_CD").alias("HIST_PHYS_ACT_CD"),
        col("HIST_IMAGE_CD").alias("HIST_IMAGE_CD"),
        col("SBEL_ELIG_TYPE").alias("SBEL_ELIG_TYPE"),
        col("CSPD_CAT").alias("CSPD_CAT"),
        col("EXCD_ID").alias("EXCD_ID"),
        col("SBEL_VOID_IND").alias("SBEL_VOID_IND"),
        col("TXN1_ROW_ID").alias("TXN1_ROW_ID"),
        col("CSPI_ID").alias("CSPI_ID"),
        col("SBEL_FI").alias("SBEL_FI")
    )
)

# --------------------------------------------------------------------------------
# Stage: Transformer_130 (CTransformerStage) -> direct pass from df_InsDel
# --------------------------------------------------------------------------------
df_InsDel_130 = df_InsDel.select(
    col("SBSB_CK").alias("SBSB_CK"),
    col("SBEL_EFF_DT").alias("SBEL_EFF_DT"),
    col("HIST_ROW_ID").alias("HIST_ROW_ID"),
    col("HIST_CREATE_DTM").alias("HIST_CREATE_DTM"),
    col("HIST_USUS_ID").alias("HIST_USUS_ID"),
    col("HIST_PHYS_ACT_CD").alias("HIST_PHYS_ACT_CD"),
    col("HIST_IMAGE_CD").alias("HIST_IMAGE_CD"),
    col("SBEL_ELIG_TYPE").alias("SBEL_ELIG_TYPE"),
    col("CSPD_CAT").alias("CSPD_CAT"),
    col("EXCD_ID").alias("EXCD_ID"),
    col("SBEL_VOID_IND").alias("SBEL_VOID_IND"),
    col("TXN1_ROW_ID").alias("TXN1_ROW_ID"),
    col("CSPI_ID").alias("CSPI_ID"),
    col("SBEL_FI").alias("SBEL_FI")
)

# --------------------------------------------------------------------------------
# Stage: Link_Collector_105 (CCollector) -> union of two inputs Round-Robin
# We will simply union the dataframes df_Update_127 and df_InsDel_130. 
# Round-Robin in DataStage is effectively a union for downstream usage.
# --------------------------------------------------------------------------------
df_Collector = df_Update_127.unionByName(df_InsDel_130, allowMissingColumns=False)

# --------------------------------------------------------------------------------
# Stage: BusinessLogic (CTransformerStage)
# --------------------------------------------------------------------------------
# The stage has a StageVariable 'RowPassThru' = 'Y'. We set PASS_THRU_IN from that.
# "FIRST_RECYC_DT" uses FORMAT.DATE(@DATE, "DATE", "CURRENT","DB2TIMESTAMP") => current_timestamp().
# For EXCD_SK and SRC_SYS_CRT_USER_SK type logic, we replicate the if-then from DataStage.
df_BusinessLogic = df_Collector.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
    rpad(F.lit("Y"), 1, " ").alias("PASS_THRU_IN"),
    current_timestamp().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    rpad(F.lit("FACETS"), <...>, " ").alias("SRC_SYS_CD"),  # Type is varchar with unknown length => use <...>
    F.concat_ws(";", F.lit("FACETS"), col("HIST_ROW_ID")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("SUB_ELIG_AUDIT_SK"),
    col("HIST_ROW_ID").alias("SUB_ELIG_AUDIT_ROW_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    when((F.length(F.trim(col("EXCD_ID"))) == 0) | col("EXCD_ID").isNull(), F.lit("NA")).otherwise(col("EXCD_ID")).alias("EXCD_SK"),
    when((F.trim(col("HIST_USUS_ID")) == "") | col("HIST_USUS_ID").isNull(), F.lit("NA")).otherwise(col("HIST_USUS_ID")).alias("SRC_SYS_CRT_USER_SK"),
    col("SBSB_CK").alias("SUB_SK"),
    when((F.length(F.trim(col("HIST_PHYS_ACT_CD"))) == 0) | col("HIST_PHYS_ACT_CD").isNull(), F.lit("NA")).otherwise(col("HIST_PHYS_ACT_CD")).alias("SUB_ELIG_AUDIT_ACTN_CD_SK"),
    when((F.length(F.trim(col("CSPD_CAT"))) == 0) | col("CSPD_CAT").isNull(), F.lit("NA")).otherwise(col("CSPD_CAT")).alias("SUB_ELIG_CLS_PROD_CAT_CD_SK"),
    when((F.length(F.trim(col("SBEL_ELIG_TYPE"))) == 0) | col("SBEL_ELIG_TYPE").isNull(), F.lit("NA")).otherwise(col("SBEL_ELIG_TYPE")).alias("SUB_ELIG_TYP_CD_SK"),
    rpad(when((F.length(F.trim(col("SBEL_VOID_IND"))) == 0) | (col("SBEL_VOID_IND") == ' '), F.lit("N")).otherwise(col("SBEL_VOID_IND")), 1, " ").alias("VOID_IN"),
    rpad(F.date_format(col("SBEL_EFF_DT"), "yyyy-MM-dd"), 10, " ").alias("EFF_DT_SK"),
    rpad(F.date_format(col("HIST_CREATE_DTM"), "yyyy-MM-dd"), 10, " ").alias("SRC_SYS_CRT_DT_SK"),
    when((F.trim(col("SBSB_CK")) == "") | col("SBSB_CK").isNull(), F.lit("UNK")).otherwise(col("SBSB_CK")).alias("SUB_UNIQ_KEY"),
    rpad(when((F.trim(col("CSPI_ID")) == "") | col("CSPI_ID").isNull(), F.lit("NA")).otherwise(col("CSPI_ID")), 8, " ").alias("CSPI_ID")
).cache()

# --------------------------------------------------------------------------------
# Stage: PrimaryKey (CTransformerStage) with left lookup to df_hf_sub_elig_audit_lookup
# --------------------------------------------------------------------------------
# Perform the left join on (SRC_SYS_CD, SUB_ELIG_AUDIT_ROW_ID). 
# Note that df_BusinessLogic has "SRC_SYS_CD" as a padded column with length <...>. 
# We will join on the unpadded version to match original logic,
# but the job strings might not match exactly. We replicate best efforts: 
# we interpret the join by trimming df_BusinessLogic.SRC_SYS_CD
# because df_hf_sub_elig_audit_lookup.SRC_SYS_CD is presumably not padded.

df_BusinessLogic_forjoin = df_BusinessLogic.withColumn("SRC_SYS_CD_JOIN", F.trim(col("SRC_SYS_CD"))).withColumn("SUB_ELIG_AUDIT_ROW_ID_JOIN", col("SUB_ELIG_AUDIT_ROW_ID"))

df_lookup = df_hf_sub_elig_audit_lookup.select(
    F.trim(col("SRC_SYS_CD")).alias("lkup_SRC_SYS_CD"),
    F.trim(col("SUB_ELIG_AUDIT_ROW_ID")).alias("lkup_SUB_ELIG_AUDIT_ROW_ID"),
    col("SUB_ELIG_AUDIT_SK").alias("lkup_SUB_ELIG_AUDIT_SK"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("lkup_CRT_RUN_CYC_EXCTN_SK")
)

df_PrimaryKey_joined = df_BusinessLogic_forjoin.join(
    df_lookup,
    on=[
        df_BusinessLogic_forjoin["SRC_SYS_CD_JOIN"] == df_lookup["lkup_SRC_SYS_CD"],
        df_BusinessLogic_forjoin["SUB_ELIG_AUDIT_ROW_ID_JOIN"] == df_lookup["lkup_SUB_ELIG_AUDIT_ROW_ID"]
    ],
    how="left"
)

# Now replicate the logic with stage variables:
# SK = if IsNull(lkup.SUB_ELIG_AUDIT_SK) then KeyMgtGetNextValueConcurrent("SUB_ELIG_AUDIT_SK") else lkup.SUB_ELIG_AUDIT_SK
# We must transform it into SurrogateKeyGen usage. 
# SurrogateKeyGen handles "fill in missing keys with a DB sequence" approach. 
# The job also sets "NewCrtRunCycExtcnSk" = if isNull(lkup_SUB_ELIG_AUDIT_SK) then CurrRunCycle else lkup_CRT_RUN_CYC_EXCTN_SK

df_enriched = df_PrimaryKey_joined.withColumn(
    "NewCrtRunCycExtcnSk",
    when(col("lkup_SUB_ELIG_AUDIT_SK").isNull(), lit(CurrRunCycle)).otherwise(col("lkup_CRT_RUN_CYC_EXCTN_SK"))
).withColumn(
    "SK_pre",
    when(col("lkup_SUB_ELIG_AUDIT_SK").isNull(), F.lit(None)).otherwise(col("lkup_SUB_ELIG_AUDIT_SK"))
)

# SurrogateKeyGen for "SUB_ELIG_AUDIT_SK"
# The first argument must be df_enriched; do not replace <DB sequence name>, <schema> or <secret_name>
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"SUB_ELIG_AUDIT_SK",<schema>,<secret_name>)

# Overwrite the logic that if lkup_SUB_ELIG_AUDIT_SK is not null, we keep that instead of the newly generated. 
# We'll coalesce the newly assigned "SUB_ELIG_AUDIT_SK" with "SK_pre".
df_enriched = df_enriched.withColumn(
    "SUB_ELIG_AUDIT_SK",
    F.when(col("lkup_SUB_ELIG_AUDIT_SK").isNotNull(), col("lkup_SUB_ELIG_AUDIT_SK")).otherwise(col("SUB_ELIG_AUDIT_SK"))
)

# Also "LAST_UPDT_RUN_CYC_EXCTN_SK" = CurrRunCycle
df_enriched = df_enriched.withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    lit(CurrRunCycle)
)

# Create final columns for each output pin
# Pin "Key" -> IdsSubEligAuditExtr
df_Key = df_enriched.select(
    col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    col("DISCARD_IN").alias("DISCARD_IN"),
    col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("ERR_CT").alias("ERR_CT"),
    col("RECYCLE_CT").alias("RECYCLE_CT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("SUB_ELIG_AUDIT_SK").alias("SUB_ELIG_AUDIT_SK"),
    col("SUB_ELIG_AUDIT_ROW_ID").alias("SUB_ELIG_AUDIT_ROW_ID"),
    col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("EXCD_SK").alias("EXCD_SK"),
    col("SRC_SYS_CRT_USER_SK").alias("SRC_SYS_CRT_USER_SK"),
    col("SUB_SK").alias("SUB_SK"),
    col("SUB_ELIG_AUDIT_ACTN_CD_SK").alias("SUB_ELIG_AUDIT_ACTN_CD_SK"),
    col("SUB_ELIG_CLS_PROD_CAT_CD_SK").alias("SUB_ELIG_CLS_PROD_CAT_CD_SK"),
    col("SUB_ELIG_TYP_CD_SK").alias("SUB_ELIG_TYP_CD_SK"),
    col("VOID_IN").alias("VOID_IN"),
    col("EFF_DT_SK").alias("EFF_DT_SK"),
    col("SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
    col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    col("CSPI_ID").alias("CSPI_ID")
)

# Pin "updt" -> filter: isNull(lkup_SUB_ELIG_AUDIT_SK) = true
# Then columns: SRC_SYS_CD, SUB_ELIG_AUDIT_ROW_ID, SUB_ELIG_AUDIT_SK, CRT_RUN_CYC_EXCTN_SK
df_updt = df_enriched.filter(col("lkup_SUB_ELIG_AUDIT_SK").isNull()).select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("SUB_ELIG_AUDIT_ROW_ID").alias("SUB_ELIG_AUDIT_ROW_ID"),
    col("SUB_ELIG_AUDIT_SK").alias("SUB_ELIG_AUDIT_SK"),
    lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK")
)

# --------------------------------------------------------------------------------
# Stage: hf_sub_elig_audit (Scenario B "write" to same hashed file)
# => Implement as upsert (but DataStage logic only inserts new records).
# --------------------------------------------------------------------------------
# 1) Write df_updt to a staging table, then 2) MERGE with WHEN NOT MATCHED THEN INSERT
jdbc_url_dummy, jdbc_props_dummy = get_db_config(ids_secret_name)
temp_table_name = "STAGING.FctsSubEligAuditExtr_hf_sub_elig_audit_temp"

# Drop if exists
drop_sql = f"DROP TABLE IF EXISTS {temp_table_name}"
execute_dml(drop_sql, jdbc_url_dummy, jdbc_props_dummy)

# Create the temporary table with df_updt
df_updt.write \
    .format("jdbc") \
    .option("url", jdbc_url_dummy) \
    .options(**jdbc_props_dummy) \
    .option("dbtable", temp_table_name) \
    .mode("append") \
    .save()

# Merge statement - only inserts if not matched
merge_sql = (
    f"MERGE INTO IDS.dummy_hf_sub_elig_audit AS T "
    f"USING {temp_table_name} AS S "
    f"ON T.SRC_SYS_CD = S.SRC_SYS_CD AND T.SUB_ELIG_AUDIT_ROW_ID = S.SUB_ELIG_AUDIT_ROW_ID "
    f"WHEN NOT MATCHED THEN "
    f"INSERT (SRC_SYS_CD, SUB_ELIG_AUDIT_ROW_ID, SUB_ELIG_AUDIT_SK, CRT_RUN_CYC_EXCTN_SK) "
    f"VALUES (S.SRC_SYS_CD, S.SUB_ELIG_AUDIT_ROW_ID, S.SUB_ELIG_AUDIT_SK, S.CRT_RUN_CYC_EXCTN_SK);"
)
execute_dml(merge_sql, jdbc_url_dummy, jdbc_props_dummy)

# --------------------------------------------------------------------------------
# Stage: IdsSubEligAuditExtr (CSeqFileStage)
# Write df_Key to the final delimited file
# File path pattern => "key/FctsSubEligAuditExtr.FctsSubEligAudit.dat.#RunID#"
# => no "landing" or "external" in path => use f"{adls_path}/key/..."
out_path_IdsSubEligAuditExtr = f"{adls_path}/key/FctsSubEligAuditExtr.FctsSubEligAudit.dat.{RunID}"

# The stage says no header, delimiter=",", quote="\"", overwrite
# Before writing, apply the final select with the same column order and rpad if char/varchar:
final_df_IdsSubEligAuditExtr = df_Key.select(
    rpad(col("JOB_EXCTN_RCRD_ERR_SK").cast("string"), <...>, " ").alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("ERR_CT").alias("ERR_CT"),
    col("RECYCLE_CT").alias("RECYCLE_CT"),
    rpad(col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),  # unknown length
    col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("SUB_ELIG_AUDIT_SK").alias("SUB_ELIG_AUDIT_SK"),
    col("SUB_ELIG_AUDIT_ROW_ID").alias("SUB_ELIG_AUDIT_ROW_ID"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("EXCD_SK"), <...>, " ").alias("EXCD_SK"),  # unknown char/varchar length
    rpad(col("SRC_SYS_CRT_USER_SK"), <...>, " ").alias("SRC_SYS_CRT_USER_SK"),
    col("SUB_SK").alias("SUB_SK"),
    rpad(col("SUB_ELIG_AUDIT_ACTN_CD_SK"), <...>, " ").alias("SUB_ELIG_AUDIT_ACTN_CD_SK"),
    rpad(col("SUB_ELIG_CLS_PROD_CAT_CD_SK"), <...>, " ").alias("SUB_ELIG_CLS_PROD_CAT_CD_SK"),
    rpad(col("SUB_ELIG_TYP_CD_SK"), <...>, " ").alias("SUB_ELIG_TYP_CD_SK"),
    rpad(col("VOID_IN"), 1, " ").alias("VOID_IN"),
    rpad(col("EFF_DT_SK"), 10, " ").alias("EFF_DT_SK"),
    rpad(col("SRC_SYS_CRT_DT_SK"), 10, " ").alias("SRC_SYS_CRT_DT_SK"),
    col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    rpad(col("CSPI_ID"), 8, " ").alias("CSPI_ID")
)

write_files(
    final_df_IdsSubEligAuditExtr,
    out_path_IdsSubEligAuditExtr,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)