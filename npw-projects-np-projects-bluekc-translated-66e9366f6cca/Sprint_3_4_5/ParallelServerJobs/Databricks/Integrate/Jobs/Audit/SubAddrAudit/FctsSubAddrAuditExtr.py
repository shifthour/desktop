# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:     FctsSubAddrAuditExtr
# MAGIC 
# MAGIC DESCRIPTION:   Pulls data from Facets Audit CMC_SBAD_ADDR table for loading to the IDS SUB_ADDR_AUDIT table
# MAGIC       
# MAGIC 
# MAGIC INPUTS:       Facets Audit 4.21 Membership Profile system
# MAGIC                       tables:  CMC_SBAD_ADDR
# MAGIC 
# MAGIC HASH FILES:   hf_sub_addr_audit
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
# MAGIC 
# MAGIC Developer                          Date                    Project/Altiris #                     Change Description                                                                                   Development Project               Code Reviewer                   Date Reviewed
# MAGIC ----------------------------------      -------------------      -----------------------------------           ---------------------------------------------------------                                                             ----------------------------------              ---------------------------------           -------------------------
# MAGIC                  Parikshith Chada   10/20/2006  -  Originally Programmed
# MAGIC Akhila Manickavelu          09/22/2016      5628-WorkersComp              Exclusion Criteria- P_SEL_PRCS_CRITR                                                                IntegrateDevl                Kalyan Neelam                     2016-10-14
# MAGIC                                                                                                                   to remove wokers comp GRGR_ID's 
# MAGIC Prabhu ES                        02/25/2022       S2S Remediation                  MSSQL connection parameters added                                                                   IntegrateDev5                        Ken Bradmon	2022-05-18

# MAGIC Strip Fields
# MAGIC All Update rows
# MAGIC All Delete and Insert Rows
# MAGIC Assign primary surrogate key
# MAGIC Apply business logic.
# MAGIC End of FACETS EXTRACTION JOB FROM CMC_SBAD_ADDR
# MAGIC Writing Sequential File to ../key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

CurrRunCycle = get_widget_value("CurrRunCycle", "")
RunID = get_widget_value("RunID", "")
FacetsOwner = get_widget_value("$FacetsOwner", "")
BeginDate = get_widget_value("BeginDate", "")
EndDate = get_widget_value("EndDate", "")
BCBSOwner = get_widget_value("$BCBSOwner", "")
facets_secret_name = get_widget_value("facets_secret_name", "")
bcbs_secret_name = get_widget_value("bcbs_secret_name", "")
ids_secret_name = get_widget_value("ids_secret_name", "")

# Read from the dummy table for hf_sub_addr_audit_lookup (Scenario B)
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_hf_sub_addr_audit = (
    "SELECT SRC_SYS_CD_SK, SUB_ADDR_AUDIT_ROW_ID, SUB_ADDR_AUDIT_SK, CRT_RUN_CYC_EXCTN_SK "
    "FROM dummy_hf_sub_addr_audit"
)
df_hf_sub_addr_audit_lookup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_hf_sub_addr_audit)
    .load()
)

# Read from SubAddrAuditExtr (ODBCConnector) for Facets
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
extract_query_subaddrauditextr = (
    "SELECT SBAD.SBSB_CK,SBAD.SBAD_TYPE,SBAD.HIST_ROW_ID,SBAD.HIST_CREATE_DTM,SBAD.HIST_USUS_ID,SBAD.HIST_PHYS_ACT_CD,"
    "SBAD.HIST_IMAGE_CD,SBAD.TXN1_ROW_ID,SBAD.SBAD_ADDR1,SBAD.SBAD_ADDR2,SBAD.SBAD_ADDR3,SBAD.SBAD_CITY,SBAD.SBAD_STATE,"
    "SBAD.SBAD_ZIP,SBAD.SBAD_COUNTY,SBAD.SBAD_CTRY_CD,SBAD.SBAD_PHONE,SBAD.SBAD_PHONE_EXT,SBAD.SBAD_EMAIL,SBAD.GRGR_CK "
    "FROM " + FacetsOwner + ".CMC_SBAD_ADDR SBAD "
    "WHERE SBAD.HIST_CREATE_DTM >= '" + BeginDate + "' "
    "AND SBAD.HIST_CREATE_DTM <= '" + EndDate + "' "
    "AND NOT EXISTS (SELECT DISTINCT CMC_GRGR_GROUP.GRGR_CK "
    "FROM " + BCBSOwner + ".P_SEL_PRCS_CRITR P_SEL_PRCS_CRITR, "
    + FacetsOwner + ".CMC_GRGR_GROUP CMC_GRGR_GROUP "
    "WHERE P_SEL_PRCS_CRITR.SEL_PRCS_ID = 'WRHSINBEXCL' "
    "and P_SEL_PRCS_CRITR.SEL_PRCS_ITEM_ID = 'WORKCOMP' "
    "and P_SEL_PRCS_CRITR.SEL_PRCS_CRITR_CD = 'MEDPROC' "
    "and CMC_GRGR_GROUP.GRGR_ID >= P_SEL_PRCS_CRITR.CRITR_VAL_FROM_TX "
    "and CMC_GRGR_GROUP.GRGR_ID <= P_SEL_PRCS_CRITR.CRITR_VAL_THRU_TX "
    "and SBAD.GRGR_CK=CMC_GRGR_GROUP.GRGR_CK) "
    "ORDER BY SBAD.SBSB_CK,SBAD.TXN1_ROW_ID,SBAD.HIST_ROW_ID"
)
df_SubAddrAuditExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_subaddrauditextr)
    .load()
)

# StripFieldSbad (Transformer)
df_StripFieldSbad = df_SubAddrAuditExtr.select(
    F.col("SBSB_CK").alias("SBSB_CK"),
    F.expr("Convert(CHAR(10) : CHAR(13) : CHAR(9), '', SBAD_TYPE)").alias("SBAD_TYPE"),
    F.expr("Convert(CHAR(10) : CHAR(13) : CHAR(9), '', HIST_ROW_ID)").alias("HIST_ROW_ID"),
    F.col("HIST_CREATE_DTM").alias("HIST_CREATE_DTM"),
    F.expr("Convert(CHAR(10) : CHAR(13) : CHAR(9), '', HIST_USUS_ID)").alias("HIST_USUS_ID"),
    F.expr("Convert(CHAR(10) : CHAR(13) : CHAR(9), '', HIST_PHYS_ACT_CD)").alias("HIST_PHYS_ACT_CD"),
    F.expr("Convert(CHAR(10) : CHAR(13) : CHAR(9), '', HIST_IMAGE_CD)").alias("HIST_IMAGE_CD"),
    F.col("TXN1_ROW_ID").alias("TXN1_ROW_ID"),
    F.expr("Convert(CHAR(10) : CHAR(13) : CHAR(9), '', SBAD_ADDR1)").alias("SBAD_ADDR1"),
    F.expr("Convert(CHAR(10) : CHAR(13) : CHAR(9), '', SBAD_ADDR2)").alias("SBAD_ADDR2"),
    F.expr("Convert(CHAR(10) : CHAR(13) : CHAR(9), '', SBAD_ADDR3)").alias("SBAD_ADDR3"),
    F.expr("Convert(CHAR(10) : CHAR(13) : CHAR(9), '', SBAD_CITY)").alias("SBAD_CITY"),
    F.expr("Convert(CHAR(10) : CHAR(13) : CHAR(9), '', SBAD_STATE)").alias("SBAD_STATE"),
    F.expr("Convert(CHAR(10) : CHAR(13) : CHAR(9), '', SBAD_ZIP)").alias("SBAD_ZIP"),
    F.expr("Convert(CHAR(10) : CHAR(13) : CHAR(9), '', SBAD_COUNTY)").alias("SBAD_COUNTY"),
    F.expr("Convert(CHAR(10) : CHAR(13) : CHAR(9), '', SBAD_CTRY_CD)").alias("SBAD_CTRY_CD"),
    F.expr("Convert(CHAR(10) : CHAR(13) : CHAR(9), '', SBAD_PHONE)").alias("SBAD_PHONE"),
    F.expr("Convert(CHAR(10) : CHAR(13) : CHAR(9), '', SBAD_PHONE_EXT)").alias("SBAD_PHONE_EXT"),
    F.expr("Convert(CHAR(10) : CHAR(13) : CHAR(9), '', SBAD_EMAIL)").alias("SBAD_EMAIL"),
)

# Transformer_90: Split into InsDel and Update
df_Transformer_90_InsDel = df_StripFieldSbad.filter(
    (F.col("HIST_PHYS_ACT_CD") == "I") | (F.col("HIST_PHYS_ACT_CD") == "D")
).select(
    F.col("SBSB_CK").alias("SBSB_CK"),
    F.col("SBAD_TYPE").alias("SBAD_TYPE"),
    F.col("HIST_ROW_ID").alias("HIST_ROW_ID"),
    F.col("HIST_CREATE_DTM").alias("HIST_CREATE_DTM"),
    F.col("HIST_USUS_ID").alias("HIST_USUS_ID"),
    F.col("HIST_PHYS_ACT_CD").alias("HIST_PHYS_ACT_CD"),
    F.col("HIST_IMAGE_CD").alias("HIST_IMAGE_CD"),
    F.col("TXN1_ROW_ID").alias("TXN1_ROW_ID"),
    F.col("SBAD_ADDR1").alias("SBAD_ADDR1"),
    F.col("SBAD_ADDR2").alias("SBAD_ADDR2"),
    F.col("SBAD_ADDR3").alias("SBAD_ADDR3"),
    F.col("SBAD_CITY").alias("SBAD_CITY"),
    F.col("SBAD_STATE").alias("SBAD_STATE"),
    F.col("SBAD_ZIP").alias("SBAD_ZIP"),
    F.col("SBAD_COUNTY").alias("SBAD_COUNTY"),
    F.col("SBAD_CTRY_CD").alias("SBAD_CTRY_CD"),
    F.col("SBAD_PHONE").alias("SBAD_PHONE"),
    F.col("SBAD_PHONE_EXT").alias("SBAD_PHONE_EXT"),
    F.col("SBAD_EMAIL").alias("SBAD_EMAIL"),
)

df_Transformer_90_Update = df_StripFieldSbad.filter(
    (F.col("HIST_PHYS_ACT_CD") == "U")
).select(
    F.col("SBSB_CK").alias("SBSB_CK"),
    F.col("SBAD_TYPE").alias("SBAD_TYPE"),
    F.col("HIST_ROW_ID").alias("HIST_ROW_ID"),
    F.col("HIST_CREATE_DTM").alias("HIST_CREATE_DTM"),
    F.col("HIST_USUS_ID").alias("HIST_USUS_ID"),
    F.col("HIST_PHYS_ACT_CD").alias("HIST_PHYS_ACT_CD"),
    F.col("HIST_IMAGE_CD").alias("HIST_IMAGE_CD"),
    F.col("TXN1_ROW_ID").alias("TXN1_ROW_ID"),
    F.col("SBAD_ADDR1").alias("SBAD_ADDR1"),
    F.col("SBAD_ADDR2").alias("SBAD_ADDR2"),
    F.col("SBAD_ADDR3").alias("SBAD_ADDR3"),
    F.col("SBAD_CITY").alias("SBAD_CITY"),
    F.col("SBAD_STATE").alias("SBAD_STATE"),
    F.col("SBAD_ZIP").alias("SBAD_ZIP"),
    F.col("SBAD_COUNTY").alias("SBAD_COUNTY"),
    F.col("SBAD_CTRY_CD").alias("SBAD_CTRY_CD"),
    F.col("SBAD_PHONE").alias("SBAD_PHONE"),
    F.col("SBAD_PHONE_EXT").alias("SBAD_PHONE_EXT"),
    F.col("SBAD_EMAIL").alias("SBAD_EMAIL"),
)

# Transformer_127: Simulate row-by-row stage variable logic
# (Assume ordering by SBSB_CK, SBAD_TYPE, HIST_ROW_ID for demonstration)
w_127 = Window.partitionBy().orderBy("SBSB_CK", "SBAD_TYPE", "HIST_ROW_ID")

df_Transformer_127_temp = (
    df_Transformer_90_Update
    .withColumn("svTempSbadAddr1",
        F.when(F.col("SBAD_ADDR1") == F.lag("SBAD_ADDR1", 1).over(w_127), F.lit("0")).otherwise(F.col("SBAD_ADDR1"))
    )
    .withColumn("svTempSbadAddr2",
        F.when(F.col("SBAD_ADDR2") == F.lag("SBAD_ADDR2", 1).over(w_127), F.lit("0")).otherwise(F.col("SBAD_ADDR2"))
    )
    .withColumn("svTempSbadAddr3",
        F.when(F.col("SBAD_ADDR3") == F.lag("SBAD_ADDR3", 1).over(w_127), F.lit("0")).otherwise(F.col("SBAD_ADDR3"))
    )
    .withColumn("svTempSbadCity",
        F.when(F.col("SBAD_CITY") == F.lag("SBAD_CITY", 1).over(w_127), F.lit("0")).otherwise(F.col("SBAD_CITY"))
    )
    .withColumn("svTempSbadState",
        F.when(F.col("SBAD_STATE") == F.lag("SBAD_STATE", 1).over(w_127), F.lit("0")).otherwise(F.col("SBAD_STATE"))
    )
    .withColumn("svTempSbadZip",
        F.when(F.col("SBAD_ZIP") == F.lag("SBAD_ZIP", 1).over(w_127), F.lit("0")).otherwise(F.col("SBAD_ZIP"))
    )
    .withColumn("svTempSbadCountry",
        F.when(F.col("SBAD_COUNTY") == F.lag("SBAD_COUNTY", 1).over(w_127), F.lit("0")).otherwise(F.col("SBAD_COUNTY"))
    )
    .withColumn("svTempSbadCtryCd",
        F.when(F.col("SBAD_CTRY_CD") == F.lag("SBAD_CTRY_CD", 1).over(w_127), F.lit("0")).otherwise(F.col("SBAD_CTRY_CD"))
    )
    .withColumn("svTempSbadPhone",
        F.when(F.col("SBAD_PHONE") == F.lag("SBAD_PHONE", 1).over(w_127), F.lit("0")).otherwise(F.col("SBAD_PHONE"))
    )
    .withColumn("svTempSbadPhoneExt",
        F.when(F.col("SBAD_PHONE_EXT") == F.lag("SBAD_PHONE_EXT", 1).over(w_127), F.lit("0")).otherwise(F.col("SBAD_PHONE_EXT"))
    )
    .withColumn("svTempSbadEmail",
        F.when(F.col("SBAD_EMAIL") == F.lag("SBAD_EMAIL", 1).over(w_127), F.lit("0")).otherwise(F.col("SBAD_EMAIL"))
    )
)

df_Transformer_127_out = df_Transformer_127_temp.filter(
    (F.col("HIST_IMAGE_CD") != "B") &
    (
      (F.col("svTempSbadAddr1") != "0") |
      (F.col("svTempSbadAddr2") != "0") |
      (F.col("svTempSbadAddr3") != "0") |
      (F.col("svTempSbadCity") != "0") |
      (F.col("svTempSbadState") != "0") |
      (F.col("svTempSbadZip") != "0") |
      (F.col("svTempSbadCountry") != "0") |
      (F.col("svTempSbadCtryCd") != "0") |
      (F.col("svTempSbadPhone") != "0") |
      (F.col("svTempSbadPhoneExt") != "0") |
      (F.col("svTempSbadEmail") != "0")
    )
).select(
    F.col("SBSB_CK").alias("SBSB_CK"),
    F.col("SBAD_TYPE").alias("SBAD_TYPE"),
    F.col("HIST_ROW_ID").alias("HIST_ROW_ID"),
    F.col("HIST_CREATE_DTM").alias("HIST_CREATE_DTM"),
    F.col("HIST_USUS_ID").alias("HIST_USUS_ID"),
    F.col("HIST_PHYS_ACT_CD").alias("HIST_PHYS_ACT_CD"),
    F.col("HIST_IMAGE_CD").alias("HIST_IMAGE_CD"),
    F.col("TXN1_ROW_ID").alias("TXN1_ROW_ID"),
    F.col("SBAD_ADDR1").alias("SBAD_ADDR1"),
    F.col("SBAD_ADDR2").alias("SBAD_ADDR2"),
    F.col("SBAD_ADDR3").alias("SBAD_ADDR3"),
    F.col("SBAD_CITY").alias("SBAD_CITY"),
    F.col("SBAD_STATE").alias("SBAD_STATE"),
    F.col("SBAD_ZIP").alias("SBAD_ZIP"),
    F.col("SBAD_COUNTY").alias("SBAD_COUNTY"),
    F.col("SBAD_CTRY_CD").alias("SBAD_CTRY_CD"),
    F.col("SBAD_PHONE").alias("SBAD_PHONE"),
    F.col("SBAD_PHONE_EXT").alias("SBAD_PHONE_EXT"),
    F.col("SBAD_EMAIL").alias("SBAD_EMAIL"),
)

# Transformer_130 from df_Transformer_90_InsDel (no extra logic, pass all columns)
df_Transformer_130 = df_Transformer_90_InsDel.select(
    F.col("SBSB_CK").alias("SBSB_CK"),
    F.col("SBAD_TYPE").alias("SBAD_TYPE"),
    F.col("HIST_ROW_ID").alias("HIST_ROW_ID"),
    F.col("HIST_CREATE_DTM").alias("HIST_CREATE_DTM"),
    F.col("HIST_USUS_ID").alias("HIST_USUS_ID"),
    F.col("HIST_PHYS_ACT_CD").alias("HIST_PHYS_ACT_CD"),
    F.col("HIST_IMAGE_CD").alias("HIST_IMAGE_CD"),
    F.col("TXN1_ROW_ID").alias("TXN1_ROW_ID"),
    F.col("SBAD_ADDR1").alias("SBAD_ADDR1"),
    F.col("SBAD_ADDR2").alias("SBAD_ADDR2"),
    F.col("SBAD_ADDR3").alias("SBAD_ADDR3"),
    F.col("SBAD_CITY").alias("SBAD_CITY"),
    F.col("SBAD_STATE").alias("SBAD_STATE"),
    F.col("SBAD_ZIP").alias("SBAD_ZIP"),
    F.col("SBAD_COUNTY").alias("SBAD_COUNTY"),
    F.col("SBAD_CTRY_CD").alias("SBAD_CTRY_CD"),
    F.col("SBAD_PHONE").alias("SBAD_PHONE"),
    F.col("SBAD_PHONE_EXT").alias("SBAD_PHONE_EXT"),
    F.col("SBAD_EMAIL").alias("SBAD_EMAIL"),
)

# Link_Collector_105 (union of Transformer_127_out and Transformer_130)
df_Link_Collector_105 = df_Transformer_127_out.unionByName(df_Transformer_130, allowMissingColumns=True)

# BusinessLogic (Transformer) - set pass-through and other columns
df_BusinessLogic_temp = df_Link_Collector_105.withColumn("RowPassThru", F.lit("Y"))
df_BusinessLogic = df_BusinessLogic_temp.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.col("RowPassThru").alias("PASS_THRU_IN"),
    current_timestamp().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.concat(F.lit("FACETS"), F.lit(";"), F.col("HIST_ROW_ID")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("SUB_ADDR_AUDIT_SK"),
    F.col("HIST_ROW_ID").alias("SUB_ADDR_AUDIT_ROW_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("HIST_USUS_ID").alias("SRC_SYS_CRT_USER_SK"),
    F.col("SBSB_CK").alias("SUB_SK"),
    F.col("HIST_PHYS_ACT_CD").alias("SUB_ADDR_AUDIT_ACTN_CD_SK"),
    F.col("SBAD_TYPE").alias("SUB_ADDR_CD_SK"),
    F.expr("FORMAT.DATE(HIST_CREATE_DTM, 'SYBASE', 'TIMESTAMP', 'CCYY-MM-DD')").alias("SRC_SYS_CRT_DT_SK"),
    F.col("SBSB_CK").alias("SUB_UNIQ_KEY"),
    F.when(F.col("SBAD_ADDR1") == F.lit(" "), F.lit(None)).otherwise(F.col("SBAD_ADDR1")).alias("ADDR_LN_1"),
    F.when(F.col("SBAD_ADDR2") == F.lit(" "), F.lit(None)).otherwise(F.col("SBAD_ADDR2")).alias("ADDR_LN_2"),
    F.when(F.col("SBAD_ADDR3") == F.lit(" "), F.lit(None)).otherwise(F.col("SBAD_ADDR3")).alias("ADDR_LN_3"),
    F.when(F.col("SBAD_CITY") == F.lit(" "), F.lit(None)).otherwise(F.col("SBAD_CITY")).alias("CITY_NM"),
    F.when((F.length(F.trim(F.col("SBAD_STATE"))) == 0) | (F.col("SBAD_STATE").isNull()), F.lit("NA")).otherwise(F.col("SBAD_STATE")).alias("SUB_ADDR_ST_CD_SK"),
    F.when(F.col("SBAD_ZIP") == F.lit(" "), F.lit(None)).otherwise(F.col("SBAD_ZIP")).alias("POSTAL_CD"),
    F.when(F.col("SBAD_COUNTY") == F.lit(" "), F.lit(None)).otherwise(F.col("SBAD_COUNTY")).alias("CNTY_NM"),
    F.when((F.length(F.trim(F.col("SBAD_CTRY_CD"))) == 0) | (F.col("SBAD_CTRY_CD").isNull()), F.lit("NA")).otherwise(F.col("SBAD_CTRY_CD")).alias("SUB_ADDR_CTRY_CD_SK"),
    F.when(F.col("SBAD_PHONE") == F.lit(" "), F.lit(None)).otherwise(F.col("SBAD_PHONE")).alias("PHN_NO"),
)

# PrimaryKey (Transformer) with a left join to df_hf_sub_addr_audit_lookup
df_PrimaryKey_temp = (
    df_BusinessLogic.alias("Transform")
    .join(
        df_hf_sub_addr_audit_lookup.alias("lkup"),
        [
            F.col("Transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD_SK"),
            F.col("Transform.SUB_ADDR_AUDIT_ROW_ID") == F.col("lkup.SUB_ADDR_AUDIT_ROW_ID"),
        ],
        how="left"
    )
)

# Apply logic: SUB_ADDR_AUDIT_SK => if lkup isNull => None else lkup.SUB_ADDR_AUDIT_SK
df_PrimaryKey_interim = df_PrimaryKey_temp.select(
    F.col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("Transform.DISCARD_IN").alias("DISCARD_IN"),
    F.col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("Transform.ERR_CT").alias("ERR_CT"),
    F.col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.when(F.col("lkup.SUB_ADDR_AUDIT_SK").isNull(), F.lit(None)).otherwise(F.col("lkup.SUB_ADDR_AUDIT_SK")).alias("SUB_ADDR_AUDIT_SK"),
    F.col("Transform.SUB_ADDR_AUDIT_ROW_ID").alias("SUB_ADDR_AUDIT_ROW_ID"),
    F.when(F.col("lkup.SUB_ADDR_AUDIT_SK").isNull(), F.col(CurrRunCycle)).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.SRC_SYS_CRT_USER_SK").alias("SRC_SYS_CRT_USER_SK"),
    F.col("Transform.SUB_SK").alias("SUB_SK"),
    F.col("Transform.SUB_ADDR_AUDIT_ACTN_CD_SK").alias("SUB_ADDR_AUDIT_ACTN_CD_SK"),
    F.col("Transform.SUB_ADDR_CD_SK").alias("SUB_ADDR_CD_SK"),
    F.col("Transform.SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
    F.col("Transform.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("Transform.ADDR_LN_1").alias("ADDR_LN_1"),
    F.col("Transform.ADDR_LN_2").alias("ADDR_LN_2"),
    F.col("Transform.ADDR_LN_3").alias("ADDR_LN_3"),
    F.col("Transform.CITY_NM").alias("CITY_NM"),
    F.col("Transform.SUB_ADDR_ST_CD_SK").alias("SUB_ADDR_ST_CD_SK"),
    F.col("Transform.POSTAL_CD").alias("POSTAL_CD"),
    F.col("Transform.CNTY_NM").alias("CNTY_NM"),
    F.col("Transform.SUB_ADDR_CTRY_CD_SK").alias("SUB_ADDR_CTRY_CD_SK"),
    F.col("Transform.PHN_NO").alias("PHN_NO"),
    F.col("lkup.SUB_ADDR_AUDIT_SK").alias("lkup_SUB_ADDR_AUDIT_SK")  # to help filter later
)

# Now apply SurrogateKeyGen for "SUB_ADDR_AUDIT_SK" where it's null
df_enriched = df_PrimaryKey_interim
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"SUB_ADDR_AUDIT_SK",<schema>,<secret_name>)

# Split out the rows that need to be "insert" into the dummy table (IsNull(lkup.SUB_ADDR_AUDIT_SK))
df_updt = df_enriched.filter(F.col("lkup_SUB_ADDR_AUDIT_SK").isNull()).select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD_SK"),
    F.col("SUB_ADDR_AUDIT_ROW_ID").alias("SUB_ADDR_AUDIT_ROW_ID"),
    F.col("SUB_ADDR_AUDIT_SK").alias("SUB_ADDR_AUDIT_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
)

# Merge into dummy_hf_sub_addr_audit
jdbc_url_ids_merge, jdbc_props_ids_merge = get_db_config(ids_secret_name)
temp_table_name = "STAGING.FctsSubAddrAuditExtr_hf_sub_addr_audit_temp"
execute_dml("DROP TABLE IF EXISTS " + temp_table_name, jdbc_url_ids_merge, jdbc_props_ids_merge)

df_updt.write \
    .format("jdbc") \
    .option("url", jdbc_url_ids_merge) \
    .options(**jdbc_props_ids_merge) \
    .option("dbtable", temp_table_name) \
    .mode("overwrite") \
    .save()

merge_sql = (
  "MERGE INTO dummy_hf_sub_addr_audit AS T "
  "USING " + temp_table_name + " AS S "
  "ON (T.SRC_SYS_CD_SK = S.SRC_SYS_CD_SK AND T.SUB_ADDR_AUDIT_ROW_ID = S.SUB_ADDR_AUDIT_ROW_ID) "
  "WHEN MATCHED THEN "
  "  UPDATE SET T.SUB_ADDR_AUDIT_SK = S.SUB_ADDR_AUDIT_SK, T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK "
  "WHEN NOT MATCHED THEN "
  "  INSERT (SRC_SYS_CD_SK, SUB_ADDR_AUDIT_ROW_ID, SUB_ADDR_AUDIT_SK, CRT_RUN_CYC_EXCTN_SK) "
  "  VALUES (S.SRC_SYS_CD_SK, S.SUB_ADDR_AUDIT_ROW_ID, S.SUB_ADDR_AUDIT_SK, S.CRT_RUN_CYC_EXCTN_SK);"
)
execute_dml(merge_sql, jdbc_url_ids_merge, jdbc_props_ids_merge)

# Final output to IdsSubAddrAuditExtr (CSeqFileStage)
# Select columns in final order from df_enriched where it corresponds to the "Key" link
df_final = df_enriched.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "SUB_ADDR_AUDIT_SK",
    "SUB_ADDR_AUDIT_ROW_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "SRC_SYS_CRT_USER_SK",
    "SUB_SK",
    "SUB_ADDR_AUDIT_ACTN_CD_SK",
    "SUB_ADDR_CD_SK",
    "SRC_SYS_CRT_DT_SK",
    "SUB_UNIQ_KEY",
    "ADDR_LN_1",
    "ADDR_LN_2",
    "ADDR_LN_3",
    "CITY_NM",
    "SUB_ADDR_ST_CD_SK",
    "POSTAL_CD",
    "CNTY_NM",
    "SUB_ADDR_CTRY_CD_SK",
    "PHN_NO"
)

# Apply rpad for columns with char types in final output: INSRT_UPDT_CD char(10), DISCARD_IN char(1), PASS_THRU_IN char(1), SRC_SYS_CRT_DT_SK char(10)
df_final_padded = df_final.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("SUB_ADDR_AUDIT_SK"),
    F.col("SUB_ADDR_AUDIT_ROW_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("SRC_SYS_CRT_USER_SK"),
    F.col("SUB_SK"),
    F.col("SUB_ADDR_AUDIT_ACTN_CD_SK"),
    F.col("SUB_ADDR_CD_SK"),
    F.rpad(F.col("SRC_SYS_CRT_DT_SK"), 10, " ").alias("SRC_SYS_CRT_DT_SK"),
    F.col("SUB_UNIQ_KEY"),
    F.col("ADDR_LN_1"),
    F.col("ADDR_LN_2"),
    F.col("ADDR_LN_3"),
    F.col("CITY_NM"),
    F.col("SUB_ADDR_ST_CD_SK"),
    F.col("POSTAL_CD"),
    F.col("CNTY_NM"),
    F.col("SUB_ADDR_CTRY_CD_SK"),
    F.col("PHN_NO"),
)

out_file_path = f"{adls_path}/key/FctsSubAddrAuditExtr.FctsSubAddrAudit.dat.{RunID}"
write_files(
    df_final_padded,
    out_file_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)