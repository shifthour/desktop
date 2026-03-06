# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:     FctsMbrPcpAuditExtr
# MAGIC 
# MAGIC DESCRIPTION:   Pulls data from Facets Audit CMC_MEPR_PRIM_PROV table for loading to the IDS MBR_PCP_AUDIT table
# MAGIC       
# MAGIC INPUTS:       Facets Audit 4.21 Membership Profile system
# MAGIC                       tables:  CMC_MEPR_PRIM_PROV
# MAGIC 
# MAGIC HASH FILES:    hf_mbr_pcp_audit
# MAGIC                       
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
# MAGIC Developer                          Date                    Project/Altiris #                     Change Description                                                                                   Development Project               Code Reviewer                   Date Reviewed
# MAGIC ----------------------------------      -------------------      -----------------------------------           ---------------------------------------------------------                                                             ----------------------------------              ---------------------------------           -------------------------
# MAGIC Parikshith Chada   10/19/2006  -  Originally Programmed
# MAGIC Akhila Manickavelu          09/22/2016      5628-WorkersComp              Exclusion Criteria- P_SEL_PRCS_CRITR                                                                IntegrateDevl                 Kalyan Neelam                   2016-10-13
# MAGIC                                                                                                                   to remove wokers comp GRGR_ID's         
# MAGIC Prabhu ES                         2022-02-25       S2S Remediation                  MSSQL connection parameters added                                                                    IntegrateDev5	Ken Bradmon	2022-05-18

# MAGIC Strip Fields
# MAGIC All Update rows
# MAGIC All Delete and Insert Rows
# MAGIC Assign primary surrogate key
# MAGIC Apply business logic.
# MAGIC End of FACETS EXTRACTION JOB FROM CMC_MEPR_PRIM_PROV
# MAGIC Writing Sequential File to ../key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
FacetsOwner = get_widget_value('$FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
BeginDate = get_widget_value('BeginDate','')
BCBSOwner = get_widget_value('$BCBSOwner','')
bcbs_secret_name = get_widget_value('bcbs_secret_name','')
EndDate = get_widget_value('EndDate','')
ids_secret_name = get_widget_value('ids_secret_name','')

# CHashedFileStage hf_mbr_pcp_audit_lookup (Scenario B) - Read from dummy table
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
df_hf_mbr_pcp_audit_lookup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", "SELECT SRC_SYS_CD_SK, MBR_PCP_AUDIT_ROW_ID, MBR_PCP_AUDIT_SK, CRT_RUN_CYC_EXCTN_SK FROM dummy_hf_mbr_pcp_audit")
    .load()
)

# ODBCConnector MbrPcpAuditExtr
extract_query = f"""
SELECT MEPR.MEME_CK,
       MEPR.HIST_ROW_ID,
       MEPR.HIST_CREATE_DTM,
       MEPR.HIST_USUS_ID,
       MEPR.HIST_PHYS_ACT_CD,
       MEPR.HIST_IMAGE_CD,
       MEPR.TXN1_ROW_ID,
       MEPR.PRPR_ID,
       MEPR.GRGR_CK
FROM {FacetsOwner}.CMC_MEPR_PRIM_PROV MEPR
WHERE MEPR.HIST_CREATE_DTM >= '{BeginDate}'
  AND MEPR.HIST_CREATE_DTM <= '{EndDate}'
  AND NOT EXISTS (
    SELECT DISTINCT CMC_GRGR_GROUP.GRGR_CK
    FROM {BCBSOwner}.P_SEL_PRCS_CRITR P_SEL_PRCS_CRITR,
         {FacetsOwner}.CMC_GRGR_GROUP CMC_GRGR_GROUP
    WHERE P_SEL_PRCS_CRITR.SEL_PRCS_ID = 'WRHSINBEXCL'
      AND P_SEL_PRCS_CRITR.SEL_PRCS_ITEM_ID = 'WORKCOMP'
      AND P_SEL_PRCS_CRITR.SEL_PRCS_CRITR_CD = 'MEDPROC'
      AND CMC_GRGR_GROUP.GRGR_ID >= P_SEL_PRCS_CRITR.CRITR_VAL_FROM_TX
      AND CMC_GRGR_GROUP.GRGR_ID <= P_SEL_PRCS_CRITR.CRITR_VAL_THRU_TX
      AND MEPR.GRGR_CK = CMC_GRGR_GROUP.GRGR_CK
  )
ORDER BY MEPR.MEME_CK, MEPR.TXN1_ROW_ID, MEPR.HIST_ROW_ID
"""
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
df_MbrPcpAuditExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query)
    .load()
)

# CTransformerStage StripFieldMepr
df_StripFieldMepr = df_MbrPcpAuditExtr.select(
    F.col("MEME_CK").alias("MEME_CK"),
    strip_field(F.col("HIST_ROW_ID")).alias("HIST_ROW_ID"),
    F.col("HIST_CREATE_DTM").alias("HIST_CREATE_DTM"),
    strip_field(F.col("HIST_USUS_ID")).alias("HIST_USUS_ID"),
    strip_field(F.col("HIST_PHYS_ACT_CD")).alias("HIST_PHYS_ACT_CD"),
    strip_field(F.col("HIST_IMAGE_CD")).alias("HIST_IMAGE_CD"),
    F.col("TXN1_ROW_ID").alias("TXN1_ROW_ID"),
    strip_field(F.col("PRPR_ID")).alias("PRPR_ID"),
)

# CTransformerStage Transformer_90 (split into two outputs: InsDel, Update)
df_InsDel = df_StripFieldMepr.filter(
    (F.col("HIST_PHYS_ACT_CD") == "I") | (F.col("HIST_PHYS_ACT_CD") == "D")
)
df_Update = df_StripFieldMepr.filter(F.col("HIST_PHYS_ACT_CD") == "U")

# CTransformerStage Transformer_127 on df_Update (row-by-row variable logic)
# Replicating stage variable logic with window/lag, dropping rows if PRPR_ID is repeated
w_127 = Window.orderBy("HIST_ROW_ID")
df_Transformer_127 = (
    df_Update.withColumn("lagPRPR_ID", F.lag("PRPR_ID").over(w_127))
    .withColumn(
        "svTempPrprId",
        F.when(
            (F.col("PRPR_ID") == F.col("lagPRPR_ID")) & F.col("lagPRPR_ID").isNotNull(),
            F.lit(0),
        ).otherwise(F.col("PRPR_ID")),
    )
    .withColumn("svCurrPrprId", F.col("svTempPrprId"))
    .filter((F.col("HIST_IMAGE_CD") != "B") & (F.col("svCurrPrprId") != 0))
)

df_Transformer_127_final = df_Transformer_127.select(
    F.col("MEME_CK"),
    F.col("HIST_ROW_ID"),
    F.col("HIST_CREATE_DTM"),
    F.col("HIST_USUS_ID"),
    F.col("HIST_PHYS_ACT_CD"),
    F.col("HIST_IMAGE_CD"),
    F.col("TXN1_ROW_ID"),
    F.col("PRPR_ID"),
)

# CTransformerStage Transformer_130 on df_InsDel (no special logic, pass-through)
df_Transformer_130_final = df_InsDel.select(
    F.col("MEME_CK"),
    F.col("HIST_ROW_ID"),
    F.col("HIST_CREATE_DTM"),
    F.col("HIST_USUS_ID"),
    F.col("HIST_PHYS_ACT_CD"),
    F.col("HIST_IMAGE_CD"),
    F.col("TXN1_ROW_ID"),
    F.col("PRPR_ID"),
)

# CCollector Link_Collector_105 (union of Transformer_127_final and Transformer_130_final)
df_Link_Collector_105 = df_Transformer_127_final.unionByName(df_Transformer_130_final)

# CTransformerStage BusinessLogic
df_BusinessLogic = df_Link_Collector_105.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    current_timestamp().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.concat(F.lit("FACETS;"), F.col("HIST_ROW_ID")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("MBR_PCP_AUDIT_SK"),
    F.col("HIST_ROW_ID").alias("MBR_PCP_AUDIT_ROW_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MEME_CK").alias("MBR_SK"),
    F.when(
        (F.length(trim(F.col("PRPR_ID"))) == 0) | F.col("PRPR_ID").isNull(),
        F.lit("NA"),
    ).otherwise(F.col("PRPR_ID")).alias("PROV_SK"),
    F.col("HIST_USUS_ID").alias("SRC_SYS_CRT_USER_SK"),
    F.when(
        (F.length(trim(F.col("HIST_PHYS_ACT_CD"))) == 0)
        | (F.col("HIST_PHYS_ACT_CD").isNull()),
        F.lit("NA"),
    ).otherwise(F.col("HIST_PHYS_ACT_CD")).alias("MBR_PCP_AUDIT_ACTN_CD_SK"),
    F.date_format(F.col("HIST_CREATE_DTM"), "yyyy-MM-dd").alias("SRC_SYS_CRT_DT_SK"),
    F.col("MEME_CK").alias("MBR_UNIQ_KEY"),
)

# CTransformerStage PrimaryKey (join with df_hf_mbr_pcp_audit_lookup)
df_PrimaryKey_joined = df_BusinessLogic.alias("Transform").join(
    df_hf_mbr_pcp_audit_lookup.alias("lkup"),
    (
        (F.col("Transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD_SK"))
        & (F.col("Transform.MBR_PCP_AUDIT_ROW_ID") == F.col("lkup.MBR_PCP_AUDIT_ROW_ID"))
    ),
    how="left",
)

# Replicate stage variables SK, NewCrtRunCycExtcnSk
df_PrimaryKeyA = df_PrimaryKey_joined.withColumn(
    "SK",
    F.when(F.col("lkup.MBR_PCP_AUDIT_SK").isNull(), F.lit(None)).otherwise(F.col("lkup.MBR_PCP_AUDIT_SK")),
).withColumn(
    "NewCrtRunCycExtcnSk",
    F.when(F.col("lkup.MBR_PCP_AUDIT_SK").isNull(), F.lit(CurrRunCycle)).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK")),
)

df_enriched = df_PrimaryKeyA
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"SK",<schema>,<secret_name>)

# "Key" output (no constraint)
df_key = df_enriched.select(
    F.col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("Transform.DISCARD_IN").alias("DISCARD_IN"),
    F.col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("Transform.ERR_CT").alias("ERR_CT"),
    F.col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("SK").alias("MBR_PCP_AUDIT_SK"),
    F.col("Transform.MBR_PCP_AUDIT_ROW_ID").alias("MBR_PCP_AUDIT_ROW_ID"),
    F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.MBR_SK").alias("MBR_SK"),
    F.col("Transform.PROV_SK").alias("PROV_SK"),
    F.col("Transform.SRC_SYS_CRT_USER_SK").alias("SRC_SYS_CRT_USER_SK"),
    F.col("Transform.MBR_PCP_AUDIT_ACTN_CD_SK").alias("MBR_PCP_AUDIT_ACTN_CD_SK"),
    F.col("Transform.SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
    F.col("Transform.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
)

# "updt" output (constraint IsNull(lkup.MBR_PCP_AUDIT_SK))
df_updt = df_enriched.filter(F.col("lkup.MBR_PCP_AUDIT_SK").isNull()).select(
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD_SK"),
    F.col("Transform.MBR_PCP_AUDIT_ROW_ID").alias("MBR_PCP_AUDIT_ROW_ID"),
    F.col("SK").alias("MBR_PCP_AUDIT_SK"),
    F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
)

# CHashedFileStage hf_mbr_pcp_audit (Scenario B) - Write via MERGE into dummy table
temp_table_name = "STAGING.FctsMbrPcpAuditExtr_hf_mbr_pcp_audit_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_name}", jdbc_url, jdbc_props)
df_updt.write.format("jdbc").option("url", jdbc_url).options(**jdbc_props).option("dbtable", temp_table_name).mode("overwrite").save()

merge_sql = f"""
MERGE INTO dummy_hf_mbr_pcp_audit AS T
USING {temp_table_name} AS S
ON (T.SRC_SYS_CD_SK = S.SRC_SYS_CD_SK AND T.MBR_PCP_AUDIT_ROW_ID = S.MBR_PCP_AUDIT_ROW_ID)
WHEN MATCHED THEN
  UPDATE SET
    T.MBR_PCP_AUDIT_SK = T.MBR_PCP_AUDIT_SK
WHEN NOT MATCHED THEN
  INSERT (SRC_SYS_CD_SK, MBR_PCP_AUDIT_ROW_ID, MBR_PCP_AUDIT_SK, CRT_RUN_CYC_EXCTN_SK)
  VALUES (S.SRC_SYS_CD_SK, S.MBR_PCP_AUDIT_ROW_ID, S.MBR_PCP_AUDIT_SK, S.CRT_RUN_CYC_EXCTN_SK)
;
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

# CSeqFileStage IdsMbrPcpAuditExtr - final output
df_key_final = df_key.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("MBR_PCP_AUDIT_SK"),
    F.col("MBR_PCP_AUDIT_ROW_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MBR_SK"),
    F.col("PROV_SK"),
    F.rpad(F.col("SRC_SYS_CRT_USER_SK"), 20, " ").alias("SRC_SYS_CRT_USER_SK"),
    F.rpad(F.col("MBR_PCP_AUDIT_ACTN_CD_SK"), 1, " ").alias("MBR_PCP_AUDIT_ACTN_CD_SK"),
    F.rpad(F.col("SRC_SYS_CRT_DT_SK"), 10, " ").alias("SRC_SYS_CRT_DT_SK"),
    F.col("MBR_UNIQ_KEY"),
)

write_files(
    df_key_final,
    f"{adls_path}/key/FctsMbrPcpAuditExtr.FctsMbrPcpAudit.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)