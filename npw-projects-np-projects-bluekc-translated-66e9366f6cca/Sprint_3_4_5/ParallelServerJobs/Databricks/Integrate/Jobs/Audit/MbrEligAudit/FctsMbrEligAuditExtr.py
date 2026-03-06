# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:     FctsMbrEligAuditExtr
# MAGIC 
# MAGIC DESCRIPTION:   Pulls data from Facets Audit CMC_MEEL_ELIG_ENT table for loading to the IDS MBR_ELIG_AUDIT table
# MAGIC       
# MAGIC 
# MAGIC INPUTS:       Facets Audit 4.21 Membership Profile system
# MAGIC                       tables:  CMC_MEEL_ELIG_ENT
# MAGIC 
# MAGIC HASH FILES:   hf_mbr_elig_audit
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
# MAGIC Developer                Date                 	Project/Altiris #	Change Description                                                                     Development Project      Code Reviewer          Date Reviewed      
# MAGIC ------------------              --------------------	------------------------	-----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------   
# MAGIC Parikshith Chada   10/18/2006  -  Originally Programmed
# MAGIC Kalyan Neelam         2011-11-04               TTR-456                Added new column CSPI_ID on end                                           IntegrateCurDevl             SAndrew                  2011-11-09
# MAGIC                                                                                                 Changed VOID_IND default value to 'N' from 'X', 
# MAGIC                                                                                                 Added default value = 'NA' to multiple fields, removed all the data elements.
# MAGIC 
# MAGIC Akhila M          09/22/2016      5628-WorkersComp              Exclusion Criteria- P_SEL_PRCS_CRITR                                             IntegrateDevl           Kalyan Neelam         2016-10-13
# MAGIC                                                                                                                   to remove wokers comp GRGR_ID's     
# MAGIC Prabhu ES               2022-02-25              S2S Remediation     MSSQL connection parameters added                                       IntegrateDev5		Ken Bradmon	2022-05-18

# MAGIC Strip Fields
# MAGIC All Update rows
# MAGIC All Delete and Insert Rows
# MAGIC Assign primary surrogate key
# MAGIC Apply business logic.
# MAGIC End of FACETS EXTRACTION JOB FROM CMC_MEEL_ELIG_ENT
# MAGIC Writing Sequential File to ../key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, when, length, regexp_replace, lag, rpad
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


CurrRunCycle = get_widget_value("CurrRunCycle","")
RunID = get_widget_value("RunID","")
FacetsOwner = get_widget_value("FacetsOwner","")
facets_secret_name = get_widget_value("facets_secret_name","")
BCBSOwner = get_widget_value("BCBSOwner","")
bcbs_secret_name = get_widget_value("bcbs_secret_name","")
BeginDate = get_widget_value("BeginDate","")
EndDate = get_widget_value("EndDate","")
ids_secret_name = get_widget_value("ids_secret_name","")

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_hf_mbr_elig_audit_lookup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        "SELECT SRC_SYS_CD_SK, MBR_ELIG_AUDIT_ROW_ID, MBR_ELIG_AUDIT_SK, CRT_RUN_CYC_EXCTN_SK FROM IDS.dummy_hf_mbr_elig_audit"
    )
    .load()
)

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
extract_query_MbrEligAuditExtr = f"""
SELECT MEEL.MEME_CK,
       MEEL.MEEL_EFF_DT,
       MEEL.HIST_ROW_ID,
       MEEL.HIST_CREATE_DTM,
       MEEL.HIST_USUS_ID,
       MEEL.HIST_PHYS_ACT_CD,
       MEEL.HIST_IMAGE_CD,
       MEEL.TXN1_ROW_ID,
       MEEL.MEEL_ELIG_TYPE,
       MEEL.CSPD_CAT,
       MEEL.CSPI_ID,
       MEEL.EXCD_ID,
       MEEL.MEEL_VOID_IND,
       MEEL.GRGR_CK
FROM {FacetsOwner}.CMC_MEEL_ELIG_ENT MEEL
WHERE MEEL.HIST_CREATE_DTM >= '{BeginDate}'
  AND MEEL.HIST_CREATE_DTM <= '{EndDate}'
  AND NOT EXISTS (
      SELECT DISTINCT CMC_GRGR_GROUP.GRGR_CK
      FROM {BCBSOwner}.P_SEL_PRCS_CRITR P_SEL_PRCS_CRITR,
           {FacetsOwner}.CMC_GRGR_GROUP CMC_GRGR_GROUP
      WHERE P_SEL_PRCS_CRITR.SEL_PRCS_ID = 'WRHSINBEXCL'
        AND P_SEL_PRCS_CRITR.SEL_PRCS_ITEM_ID = 'WORKCOMP'
        AND P_SEL_PRCS_CRITR.SEL_PRCS_CRITR_CD = 'MEDPROC'
        AND CMC_GRGR_GROUP.GRGR_ID >= P_SEL_PRCS_CRITR.CRITR_VAL_FROM_TX
        AND CMC_GRGR_GROUP.GRGR_ID <= P_SEL_PRCS_CRITR.CRITR_VAL_THRU_TX
        AND MEEL.GRGR_CK = CMC_GRGR_GROUP.GRGR_CK
  )
ORDER BY MEEL.MEME_CK, MEEL.TXN1_ROW_ID, MEEL.HIST_ROW_ID
"""
df_MbrEligAuditExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_MbrEligAuditExtr)
    .load()
)

df_StripFieldMeel = df_MbrEligAuditExtr.select(
    col("MEME_CK").alias("MEME_CK"),
    col("MEEL_EFF_DT").alias("MEEL_EFF_DT"),
    regexp_replace(col("HIST_ROW_ID"), "[\n\r\t]", "").alias("HIST_ROW_ID"),
    col("HIST_CREATE_DTM").alias("HIST_CREATE_DTM"),
    regexp_replace(col("HIST_USUS_ID"), "[\n\r\t]", "").alias("HIST_USUS_ID"),
    regexp_replace(col("HIST_PHYS_ACT_CD"), "[\n\r\t]", "").alias("HIST_PHYS_ACT_CD"),
    regexp_replace(col("HIST_IMAGE_CD"), "[\n\r\t]", "").alias("HIST_IMAGE_CD"),
    col("TXN1_ROW_ID").alias("TXN1_ROW_ID"),
    regexp_replace(col("MEEL_ELIG_TYPE"), "[\n\r\t]", "").alias("MEEL_ELIG_TYPE"),
    regexp_replace(col("CSPD_CAT"), "[\n\r\t]", "").alias("CSPD_CAT"),
    regexp_replace(col("CSPI_ID"), "[\n\r\t]", "").alias("CSPI_ID"),
    regexp_replace(col("EXCD_ID"), "[\n\r\t]", "").alias("EXCD_ID"),
    regexp_replace(col("MEEL_VOID_IND"), "[\n\r\t]", "").alias("MEEL_VOID_IND")
)

df_InsDel = df_StripFieldMeel.filter((col("HIST_PHYS_ACT_CD") == "I") | (col("HIST_PHYS_ACT_CD") == "D"))
df_Update = df_StripFieldMeel.filter(col("HIST_PHYS_ACT_CD") == "U")

windowSpec_127 = Window.orderBy("MEME_CK", "MEEL_EFF_DT", "HIST_ROW_ID")
df_Transformer_127_stagevars = (
    df_Update
    .withColumn("prevMeelEffDt", lag("MEEL_EFF_DT", 1).over(windowSpec_127))
    .withColumn("prevMeelEligType", lag("MEEL_ELIG_TYPE", 1).over(windowSpec_127))
    .withColumn("prevCspiId", lag("CSPI_ID", 1).over(windowSpec_127))
    .withColumn("prevMeelVoidInd", lag("MEEL_VOID_IND", 1).over(windowSpec_127))
    .withColumn("tempMeelEffDt", when(col("MEEL_EFF_DT") == col("prevMeelEffDt"), lit(0)).otherwise(col("MEEL_EFF_DT")))
    .withColumn("tempMeelEligType", when(col("MEEL_ELIG_TYPE") == col("prevMeelEligType"), lit(0)).otherwise(col("MEEL_ELIG_TYPE")))
    .withColumn("tempCspiId", when(col("CSPI_ID") == col("prevCspiId"), lit(0)).otherwise(col("CSPI_ID")))
    .withColumn("tempMeelVoidInd", when(col("MEEL_VOID_IND") == col("prevMeelVoidInd"), lit(0)).otherwise(col("MEEL_VOID_IND")))
)

df_DSLink103 = df_Transformer_127_stagevars.filter(
    (col("HIST_IMAGE_CD") != "B") &
    (
      (col("tempMeelEffDt") != lit(0)) |
      (col("tempMeelEligType") != lit(0)) |
      (col("tempCspiId") != lit(0)) |
      (col("tempMeelVoidInd") != lit(0))
    )
).select(
    "MEME_CK",
    "MEEL_EFF_DT",
    "HIST_ROW_ID",
    "HIST_CREATE_DTM",
    "HIST_USUS_ID",
    "HIST_PHYS_ACT_CD",
    "HIST_IMAGE_CD",
    "TXN1_ROW_ID",
    "MEEL_ELIG_TYPE",
    "CSPD_CAT",
    "CSPI_ID",
    "EXCD_ID",
    "MEEL_VOID_IND"
)

df_DSLink104 = df_InsDel.select(
    "MEME_CK",
    "MEEL_EFF_DT",
    "HIST_ROW_ID",
    "HIST_CREATE_DTM",
    "HIST_USUS_ID",
    "HIST_PHYS_ACT_CD",
    "HIST_IMAGE_CD",
    "TXN1_ROW_ID",
    "MEEL_ELIG_TYPE",
    "CSPD_CAT",
    "CSPI_ID",
    "EXCD_ID",
    "MEEL_VOID_IND"
)

df_Keys = df_DSLink103.unionByName(df_DSLink104)

df_BusinessLogic = (
    df_Keys
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", lit(0))
    .withColumn("INSRT_UPDT_CD", lit("I"))
    .withColumn("DISCARD_IN", lit("N"))
    .withColumn("PASS_THRU_IN", lit("Y"))
    .withColumn("FIRST_RECYC_DT", current_timestamp())
    .withColumn("ERR_CT", lit(0))
    .withColumn("RECYCLE_CT", lit(0))
    .withColumn("SRC_SYS_CD", lit("FACETS"))
    .withColumn("PRI_KEY_STRING", lit("FACETS").concat(lit(";")).concat(col("HIST_ROW_ID")))
    .withColumn("MBR_ELIG_AUDIT_SK", lit(0))
    .withColumn("MBR_ELIG_AUDIT_ROW_ID", col("HIST_ROW_ID"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(0))
    .withColumn("EXCD_SK", when((col("EXCD_ID").isNull()) | (trim(col("EXCD_ID")) == ""), lit("NA")).otherwise(col("EXCD_ID")))
    .withColumn("MBR_SK", col("MEME_CK"))
    .withColumn("SRC_SYS_CRT_USER_SK", when((col("HIST_USUS_ID").isNull()) | (trim(col("HIST_USUS_ID")) == ""), lit("NA")).otherwise(col("HIST_USUS_ID")))
    .withColumn("MBR_ELIG_AUDIT_ACTN_CD_SK", when((col("HIST_PHYS_ACT_CD").isNull()) | (trim(col("HIST_PHYS_ACT_CD")) == ""), lit("NA")).otherwise(col("HIST_PHYS_ACT_CD")))
    .withColumn("MBR_ELIG_CLS_PROD_CAT_CD_SK", when((col("CSPD_CAT").isNull()) | (trim(col("CSPD_CAT")) == ""), lit("NA")).otherwise(col("CSPD_CAT")))
    .withColumn("MBR_ELIG_TYP_CD_SK", when((col("MEEL_ELIG_TYPE").isNull()) | (trim(col("MEEL_ELIG_TYPE")) == ""), lit("NA")).otherwise(col("MEEL_ELIG_TYPE")))
    .withColumn("VOID_IN", when((col("MEEL_VOID_IND").isNull()) | (trim(col("MEEL_VOID_IND")) == ""), lit("N")).otherwise(col("MEEL_VOID_IND")))
    .withColumn("EFF_DT_SK", col("MEEL_EFF_DT").cast("string"))
    .withColumn("SRC_SYS_CRT_DT_SK", col("HIST_CREATE_DTM").cast("string"))
    .withColumn("MBR_UNIQ_KEY", when((col("MEME_CK").isNull()) | (trim(col("MEME_CK")) == ""), lit("NA")).otherwise(col("MEME_CK")))
    .withColumn("CSPI_ID", when((col("CSPI_ID").isNull()) | (trim(col("CSPI_ID")) == ""), lit("NA")).otherwise(col("CSPI_ID")))
)

df_Transform = df_BusinessLogic.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "MBR_ELIG_AUDIT_SK",
    "MBR_ELIG_AUDIT_ROW_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "EXCD_SK",
    "MBR_SK",
    "SRC_SYS_CRT_USER_SK",
    "MBR_ELIG_AUDIT_ACTN_CD_SK",
    "MBR_ELIG_CLS_PROD_CAT_CD_SK",
    "MBR_ELIG_TYP_CD_SK",
    "VOID_IN",
    "EFF_DT_SK",
    "SRC_SYS_CRT_DT_SK",
    "MBR_UNIQ_KEY",
    "CSPI_ID"
)

df_enriched = (
    df_Transform.alias("Transform")
    .join(
        df_hf_mbr_elig_audit_lookup.alias("lkup"),
        (
            (col("Transform.SRC_SYS_CD") == col("lkup.SRC_SYS_CD_SK"))
            & (col("Transform.MBR_ELIG_AUDIT_ROW_ID") == col("lkup.MBR_ELIG_AUDIT_ROW_ID"))
        ),
        "left"
    )
    .select(
        col("Transform.JOB_EXCTN_RCRD_ERR_SK"),
        col("Transform.INSRT_UPDT_CD"),
        col("Transform.DISCARD_IN"),
        col("Transform.PASS_THRU_IN"),
        col("Transform.FIRST_RECYC_DT"),
        col("Transform.ERR_CT"),
        col("Transform.RECYCLE_CT"),
        col("Transform.SRC_SYS_CD"),
        col("Transform.PRI_KEY_STRING"),
        col("lkup.MBR_ELIG_AUDIT_SK").alias("lkup_MBR_ELIG_AUDIT_SK"),
        col("lkup.CRT_RUN_CYC_EXCTN_SK").alias("lkup_CRT_RUN_CYC_EXCTN_SK"),
        col("Transform.MBR_ELIG_AUDIT_ROW_ID"),
        col("Transform.MBR_ELIG_AUDIT_SK"),
        col("Transform.CRT_RUN_CYC_EXCTN_SK"),
        col("Transform.LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("Transform.EXCD_SK"),
        col("Transform.MBR_SK"),
        col("Transform.SRC_SYS_CRT_USER_SK"),
        col("Transform.MBR_ELIG_AUDIT_ACTN_CD_SK"),
        col("Transform.MBR_ELIG_CLS_PROD_CAT_CD_SK"),
        col("Transform.MBR_ELIG_TYP_CD_SK"),
        col("Transform.VOID_IN"),
        col("Transform.EFF_DT_SK"),
        col("Transform.SRC_SYS_CRT_DT_SK"),
        col("Transform.MBR_UNIQ_KEY"),
        col("Transform.CSPI_ID")
    )
    .withColumn(
        "SK",
        when(col("lkup_MBR_ELIG_AUDIT_SK").isNull(), lit(None)).otherwise(col("lkup_MBR_ELIG_AUDIT_SK"))
    )
    .withColumn(
        "NewCrtRunCycExtcnSk",
        when(col("lkup_MBR_ELIG_AUDIT_SK").isNull(), lit(CurrRunCycle)).otherwise(col("lkup_CRT_RUN_CYC_EXCTN_SK"))
    )
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(CurrRunCycle))
)

df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"SK",<schema>,<secret_name>)

df_Key = df_enriched.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD"),
    col("DISCARD_IN"),
    col("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("SK").alias("MBR_ELIG_AUDIT_SK"),
    col("MBR_ELIG_AUDIT_ROW_ID"),
    col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("EXCD_SK"),
    col("MBR_SK"),
    col("SRC_SYS_CRT_USER_SK"),
    col("MBR_ELIG_AUDIT_ACTN_CD_SK"),
    col("MBR_ELIG_CLS_PROD_CAT_CD_SK"),
    col("MBR_ELIG_TYP_CD_SK"),
    col("VOID_IN"),
    col("EFF_DT_SK"),
    col("SRC_SYS_CRT_DT_SK"),
    col("MBR_UNIQ_KEY"),
    col("CSPI_ID")
)

df_updt = df_enriched.filter(col("lkup_MBR_ELIG_AUDIT_SK").isNull()).select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD_SK"),
    col("MBR_ELIG_AUDIT_ROW_ID"),
    col("SK").alias("MBR_ELIG_AUDIT_SK"),
    lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK")
)

drop_temp_sql = "DROP TABLE IF EXISTS STAGING.FctsMbrEligAuditExtr_hf_mbr_elig_audit_temp"
execute_dml(drop_temp_sql, jdbc_url_ids, jdbc_props_ids)

df_updt.write \
    .format("jdbc") \
    .option("url", jdbc_url_ids) \
    .options(**jdbc_props_ids) \
    .option("dbtable", "STAGING.FctsMbrEligAuditExtr_hf_mbr_elig_audit_temp") \
    .mode("append") \
    .save()

merge_sql = """
MERGE INTO IDS.dummy_hf_mbr_elig_audit AS T
USING STAGING.FctsMbrEligAuditExtr_hf_mbr_elig_audit_temp AS S
ON T.SRC_SYS_CD_SK = S.SRC_SYS_CD_SK
   AND T.MBR_ELIG_AUDIT_ROW_ID = S.MBR_ELIG_AUDIT_ROW_ID
WHEN NOT MATCHED THEN
  INSERT (SRC_SYS_CD_SK, MBR_ELIG_AUDIT_ROW_ID, MBR_ELIG_AUDIT_SK, CRT_RUN_CYC_EXCTN_SK)
  VALUES (S.SRC_SYS_CD_SK, S.MBR_ELIG_AUDIT_ROW_ID, S.MBR_ELIG_AUDIT_SK, S.CRT_RUN_CYC_EXCTN_SK);
"""
execute_dml(merge_sql, jdbc_url_ids, jdbc_props_ids)

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
    col("MBR_ELIG_AUDIT_SK"),
    col("MBR_ELIG_AUDIT_ROW_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("EXCD_SK"), 3, " ").alias("EXCD_SK"),
    col("MBR_SK"),
    rpad(col("SRC_SYS_CRT_USER_SK"), 20, " ").alias("SRC_SYS_CRT_USER_SK"),
    rpad(col("MBR_ELIG_AUDIT_ACTN_CD_SK"), 1, " ").alias("MBR_ELIG_AUDIT_ACTN_CD_SK"),
    rpad(col("MBR_ELIG_CLS_PROD_CAT_CD_SK"), 1, " ").alias("MBR_ELIG_CLS_PROD_CAT_CD_SK"),
    rpad(col("MBR_ELIG_TYP_CD_SK"), 2, " ").alias("MBR_ELIG_TYP_CD_SK"),
    rpad(col("VOID_IN"), 1, " ").alias("VOID_IN"),
    rpad(col("EFF_DT_SK"), 10, " ").alias("EFF_DT_SK"),
    rpad(col("SRC_SYS_CRT_DT_SK"), 10, " ").alias("SRC_SYS_CRT_DT_SK"),
    col("MBR_UNIQ_KEY"),
    rpad(col("CSPI_ID"), 8, " ").alias("CSPI_ID")
)

file_path_idsMbrEligAuditExtr = f"{adls_path}/key/FctsMbrEligAuditExtr.FctsMbrEligAudit.dat.{RunID}"
write_files(
    df_final,
    file_path_idsMbrEligAuditExtr,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)