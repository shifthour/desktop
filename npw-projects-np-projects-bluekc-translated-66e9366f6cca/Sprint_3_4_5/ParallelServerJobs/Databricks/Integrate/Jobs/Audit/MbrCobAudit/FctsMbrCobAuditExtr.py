# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:     FctsMbrCobAuditExtr
# MAGIC 
# MAGIC DESCRIPTION:   Pulls data from Facets Audit CMC_MECB_COB table for loading to the IDS MBR_COB_AUDIT table
# MAGIC       
# MAGIC INPUTS:       Facets Audit 4.21 Membership Profile system
# MAGIC                       tables:  CMC_MECB_COB 
# MAGIC 
# MAGIC HASH FILES:   hf_mbr_cob_audit
# MAGIC                       
# MAGIC TRANSFORMS:   STRIP.FIELD
# MAGIC                              FORMAT.DATE
# MAGIC                            
# MAGIC PROCESSING:  Extract, transform, and primary keying for Membership subject area.
# MAGIC   
# MAGIC OUTPUTS:   Sequential file
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Parikshith Chada   10/25/2006  -  Originally Programmed
# MAGIC O. Nielsen                7/25/2008          Facets 4.5.1         changed OTHR_CAR_POL_ID to varchar(40)                           devlIDSnew                    Steph Goddard          08/25/2008
# MAGIC 
# MAGIC Akhila Manickavelu  09/22/2016      5628-WorkersComp              Exclusion Criteria- P_SEL_PRCS_CRITR                    IntegrateDevl                 Kalyan Neelam            2016-10-13
# MAGIC                                                                                                                   to remove wokers comp GRGR_ID's 
# MAGIC Prabhu ES                02/25/2022       S2S Remediation     MSSQL connection parameters added                                     IntegrateDev5                   Ken Bradmon	2022-05-18

# MAGIC Strip Fields
# MAGIC All Update rows
# MAGIC All Delete and Insert Rows
# MAGIC Assign primary surrogate key
# MAGIC Apply business logic.
# MAGIC End of FACETS EXTRACTION JOB FROM CMC_MECB_COB
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
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
BCBSOwner = get_widget_value('BCBSOwner','')
bcbs_secret_name = get_widget_value('bcbs_secret_name','')
BeginDate = get_widget_value('BeginDate','')
EndDate = get_widget_value('EndDate','')

ids_secret_name = get_widget_value('ids_secret_name','')  # Assuming we'll store the "dummy_hf_mbr_cob_audit" in IDS

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)

extract_query_MbrCobAuditExtr = f"""
SELECT
  MECB.MEME_CK,
  MECB.MECB_INSUR_TYPE,
  MECB.MECB_INSUR_ORDER,
  MECB.MECB_EFF_DT,
  MECB.HIST_ROW_ID,
  MECB.HIST_CREATE_DTM,
  MECB.HIST_USUS_ID,
  MECB.HIST_PHYS_ACT_CD,
  MECB.HIST_IMAGE_CD,
  MECB.TXN1_ROW_ID,
  MECB.MECB_TERM_DT,
  MECB.MECB_MCTR_TRSN,
  MECB.MCRE_ID,
  MECB.MECB_POLICY_ID,
  MECB.MECB_LAST_VER_DT,
  MECB.MECB_LAST_VER_NAME,
  MECB.MECB_MCTR_VMTH,
  MECB.MECB_LOI_START_DT,
  MECB.GRGR_CK
FROM {FacetsOwner}.CMC_MECB_COB MECB
WHERE
  MECB.HIST_CREATE_DTM >= '{BeginDate}'
  AND MECB.HIST_CREATE_DTM <= '{EndDate}'
  AND NOT EXISTS (
    SELECT DISTINCT
      CMC_GRGR_GROUP.GRGR_CK
    FROM
      {bcbsOwner}.P_SEL_PRCS_CRITR P_SEL_PRCS_CRITR,
      {FacetsOwner}.CMC_GRGR_GROUP CMC_GRGR_GROUP
    WHERE
      P_SEL_PRCS_CRITR.SEL_PRCS_ID = 'WRHSINBEXCL'
      AND P_SEL_PRCS_CRITR.SEL_PRCS_ITEM_ID = 'WORKCOMP'
      AND P_SEL_PRCS_CRITR.SEL_PRCS_CRITR_CD = 'MEDPROC'
      AND CMC_GRGR_GROUP.GRGR_ID >= P_SEL_PRCS_CRITR.CRITR_VAL_FROM_TX
      AND CMC_GRGR_GROUP.GRGR_ID <= P_SEL_PRCS_CRITR.CRITR_VAL_THRU_TX
      AND MECB.GRGR_CK=CMC_GRGR_GROUP.GRGR_CK
  )
ORDER BY MECB.MEME_CK, MECB.TXN1_ROW_ID, MECB.HIST_ROW_ID
"""

df_MbrCobAuditExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_MbrCobAuditExtr)
    .load()
)

df_StripFieldMecb = df_MbrCobAuditExtr.select(
    F.col("MEME_CK").alias("MEME_CK"),
    F.when(
        (F.length(strip_field(F.col("MECB_INSUR_TYPE"))) == 0) | (strip_field(F.col("MECB_INSUR_TYPE")) == ""), 
        strip_field(F.col("MECB_INSUR_TYPE"))
    ).otherwise(strip_field(F.col("MECB_INSUR_TYPE"))).alias("MECB_INSUR_TYPE"),
    F.when(
        (F.length(strip_field(F.col("MECB_INSUR_ORDER"))) == 0) | (strip_field(F.col("MECB_INSUR_ORDER")) == ""), 
        strip_field(F.col("MECB_INSUR_ORDER"))
    ).otherwise(strip_field(F.col("MECB_INSUR_ORDER"))).alias("MECB_INSUR_ORDER"),
    F.col("MECB_EFF_DT").alias("MECB_EFF_DT"),
    strip_field(F.col("HIST_ROW_ID")).alias("HIST_ROW_ID"),
    F.col("HIST_CREATE_DTM").alias("HIST_CREATE_DTM"),
    strip_field(F.col("HIST_USUS_ID")).alias("HIST_USUS_ID"),
    strip_field(F.col("HIST_PHYS_ACT_CD")).alias("HIST_PHYS_ACT_CD"),
    strip_field(F.col("HIST_IMAGE_CD")).alias("HIST_IMAGE_CD"),
    F.col("TXN1_ROW_ID").alias("TXN1_ROW_ID"),
    F.col("MECB_TERM_DT").alias("MECB_TERM_DT"),
    strip_field(F.col("MECB_MCTR_TRSN")).alias("MECB_MCTR_TRSN"),
    strip_field(F.col("MCRE_ID")).alias("MCRE_ID"),
    F.when(
        (F.length(strip_field(F.col("MECB_POLICY_ID"))) == 0) | (strip_field(F.col("MECB_POLICY_ID")) == ""),
        " "
    ).otherwise(strip_field(F.col("MECB_POLICY_ID"))).alias("MECB_POLICY_ID"),
    F.col("MECB_LAST_VER_DT").alias("MECB_LAST_VER_DT"),
    strip_field(F.col("MECB_LAST_VER_NAME")).alias("MECB_LAST_VER_NAME"),
    strip_field(F.col("MECB_MCTR_VMTH")).alias("MECB_MCTR_VMTH"),
    F.col("MECB_LOI_START_DT").alias("MECB_LOI_START_DT")
)

df_InsDel = df_StripFieldMecb.filter(
    (F.col("HIST_PHYS_ACT_CD") == "I") | (F.col("HIST_PHYS_ACT_CD") == "D")
).select(
    F.col("MEME_CK"),
    F.col("MECB_INSUR_TYPE"),
    F.col("MECB_INSUR_ORDER"),
    F.col("MECB_EFF_DT"),
    F.col("HIST_ROW_ID"),
    F.col("HIST_CREATE_DTM"),
    F.col("HIST_USUS_ID"),
    F.col("HIST_PHYS_ACT_CD"),
    F.col("HIST_IMAGE_CD"),
    F.col("TXN1_ROW_ID"),
    F.col("MECB_TERM_DT"),
    F.col("MECB_MCTR_TRSN"),
    F.col("MCRE_ID"),
    F.col("MECB_POLICY_ID"),
    F.col("MECB_LAST_VER_DT"),
    F.col("MECB_LAST_VER_NAME"),
    F.col("MECB_MCTR_VMTH"),
    F.col("MECB_LOI_START_DT")
)

df_Update = df_StripFieldMecb.filter(
    F.col("HIST_PHYS_ACT_CD") == "U"
).select(
    F.col("MEME_CK"),
    F.col("MECB_INSUR_TYPE"),
    F.col("MECB_INSUR_ORDER"),
    F.col("MECB_EFF_DT"),
    F.col("HIST_ROW_ID"),
    F.col("HIST_CREATE_DTM"),
    F.col("HIST_USUS_ID"),
    F.col("HIST_PHYS_ACT_CD"),
    F.col("HIST_IMAGE_CD"),
    F.col("TXN1_ROW_ID"),
    F.col("MECB_TERM_DT"),
    F.col("MECB_MCTR_TRSN"),
    F.col("MCRE_ID"),
    F.col("MECB_POLICY_ID"),
    F.col("MECB_LAST_VER_DT"),
    F.col("MECB_LAST_VER_NAME"),
    F.col("MECB_MCTR_VMTH"),
    F.col("MECB_LOI_START_DT")
)

window_upd = Window.orderBy(
    F.lit(1)
)

df_HIST_IMAGE_CD = df_Update\
.withColumn("prev_MECB_INSUR_TYPE", F.lag("MECB_INSUR_TYPE", 1).over(window_upd))\
.withColumn("svTempMecbInsurType", F.when(F.col("MECB_INSUR_TYPE")==F.col("prev_MECB_INSUR_TYPE"), "0").otherwise(F.col("MECB_INSUR_TYPE")))\
.withColumn("svCurrMecbInsurType", F.col("svTempMecbInsurType"))\
.withColumn("prev_MECB_INSUR_ORDER", F.lag("MECB_INSUR_ORDER", 1).over(window_upd))\
.withColumn("svTempMecbInsurOrder", F.when(F.col("MECB_INSUR_ORDER")==F.col("prev_MECB_INSUR_ORDER"), "0").otherwise(F.col("MECB_INSUR_ORDER")))\
.withColumn("svCurrMecbInsurOrder", F.col("svTempMecbInsurOrder"))\
.withColumn("prev_MECB_EFF_DT", F.lag("MECB_EFF_DT", 1).over(window_upd))\
.withColumn("svTempMecbEffDt", F.when(F.col("MECB_EFF_DT")==F.col("prev_MECB_EFF_DT"), "0").otherwise(F.col("MECB_EFF_DT")))\
.withColumn("svCurrMecbEffDt", F.col("svTempMecbEffDt"))\
.withColumn("prev_MECB_TERM_DT", F.lag("MECB_TERM_DT", 1).over(window_upd))\
.withColumn("svTempMecbTermDt", F.when(F.col("MECB_TERM_DT")==F.col("prev_MECB_TERM_DT"), "0").otherwise(F.col("MECB_TERM_DT")))\
.withColumn("svCurrMecbTermDt", F.col("svTempMecbTermDt"))\
.withColumn("prev_MCRE_ID", F.lag("MCRE_ID", 1).over(window_upd))\
.withColumn("svTempMecrId", F.when(F.col("MCRE_ID")==F.col("prev_MCRE_ID"), "0").otherwise(F.col("MCRE_ID")))\
.withColumn("svCurrMcreId", F.col("svTempMecrId"))\
.withColumn("prev_MECB_POLICY_ID", F.lag("MECB_POLICY_ID", 1).over(window_upd))\
.withColumn("svTempMecbPolicyId", F.when(F.col("MECB_POLICY_ID")==F.col("prev_MECB_POLICY_ID"), "0").otherwise(F.col("MECB_POLICY_ID")))\
.withColumn("svCurrMecbPolicyId", F.col("svTempMecbPolicyId"))\
.withColumn("prev_MECB_LAST_VER_NAME", F.lag("MECB_LAST_VER_NAME", 1).over(window_upd))\
.withColumn("svTempMecbLastVerName", F.when(F.col("MECB_LAST_VER_NAME")==F.col("prev_MECB_LAST_VER_NAME"), "0").otherwise(F.col("MECB_LAST_VER_NAME")))\
.withColumn("svCurrMecbLastVerName", F.col("svTempMecbLastVerName"))\
.withColumn("prev_MECB_MCTR_VMTH", F.lag("MECB_MCTR_VMTH", 1).over(window_upd))\
.withColumn("svTempMecbMctrVmth", F.when(F.col("MECB_MCTR_VMTH")==F.col("prev_MECB_MCTR_VMTH"), "0").otherwise(F.col("MECB_MCTR_VMTH")))\
.withColumn("svCurrMecbMctrVmth", F.col("svTempMecbMctrVmth"))\
.withColumn("prev_MECB_LOI_START_DT", F.lag("MECB_LOI_START_DT", 1).over(window_upd))\
.withColumn("svTempMecbLoiStartDt", F.when(F.col("MECB_LOI_START_DT")==F.col("prev_MECB_LOI_START_DT"), "0").otherwise(F.col("MECB_LOI_START_DT")))\
.withColumn("svCurrMecbLoiStartDt", F.col("svTempMecbLoiStartDt"))

df_Merge1 = df_HIST_IMAGE_CD.filter(
    (F.col("HIST_IMAGE_CD") != "B") &
    (
        (F.col("svCurrMecbInsurType") != "0") |
        (F.col("svCurrMecbInsurOrder") != "0") |
        (F.col("svCurrMecbEffDt") != "0") |
        (F.col("svCurrMecbTermDt") != "0") |
        (F.col("svCurrMcreId") != "0") |
        (F.col("svCurrMecbPolicyId") != "0") |
        (F.col("svCurrMecbLastVerName") != "0") |
        (F.col("svCurrMecbMctrVmth") != "0") |
        (F.col("svCurrMecbLoiStartDt") != "0")
    )
).select(
    F.col("MEME_CK"),
    F.col("MECB_INSUR_TYPE"),
    F.col("MECB_INSUR_ORDER"),
    F.col("MECB_EFF_DT"),
    F.col("HIST_ROW_ID"),
    F.col("HIST_CREATE_DTM"),
    F.col("HIST_USUS_ID"),
    F.col("HIST_PHYS_ACT_CD"),
    F.col("HIST_IMAGE_CD"),
    F.col("TXN1_ROW_ID"),
    F.col("MECB_TERM_DT"),
    F.col("MECB_MCTR_TRSN"),
    F.col("MCRE_ID"),
    F.col("MECB_POLICY_ID"),
    F.col("MECB_LAST_VER_DT"),
    F.col("MECB_LAST_VER_NAME"),
    F.col("MECB_MCTR_VMTH"),
    F.col("MECB_LOI_START_DT")
)

df_Transformer4 = df_InsDel.select(
    F.col("MEME_CK"),
    F.col("MECB_INSUR_TYPE"),
    F.col("MECB_INSUR_ORDER"),
    F.col("MECB_EFF_DT"),
    F.col("HIST_ROW_ID"),
    F.col("HIST_CREATE_DTM"),
    F.col("HIST_USUS_ID"),
    F.col("HIST_PHYS_ACT_CD"),
    F.col("HIST_IMAGE_CD"),
    F.col("TXN1_ROW_ID"),
    F.col("MECB_TERM_DT"),
    F.col("MECB_MCTR_TRSN"),
    F.col("MCRE_ID"),
    F.col("MECB_POLICY_ID"),
    F.col("MECB_LAST_VER_DT"),
    F.col("MECB_LAST_VER_NAME"),
    F.col("MECB_MCTR_VMTH"),
    F.col("MECB_LOI_START_DT")
)

df_LinkCollector = df_Merge1.union(df_Transformer4).select(
    F.col("MEME_CK").alias("MEME_CK"),
    F.col("MECB_INSUR_TYPE").alias("MECB_INSUR_TYPE"),
    F.col("MECB_INSUR_ORDER").alias("MECB_INSUR_ORDER"),
    F.col("MECB_EFF_DT").alias("MECB_EFF_DT"),
    F.col("HIST_ROW_ID").alias("HIST_ROW_ID"),
    F.col("HIST_CREATE_DTM").alias("HIST_CREATE_DTM"),
    F.col("HIST_USUS_ID").alias("HIST_USUS_ID"),
    F.col("HIST_PHYS_ACT_CD").alias("HIST_PHYS_ACT_CD"),
    F.col("HIST_IMAGE_CD").alias("HIST_IMAGE_CD"),
    F.col("TXN1_ROW_ID").alias("TXN1_ROW_ID"),
    F.col("MECB_TERM_DT").alias("MECB_TERM_DT"),
    F.col("MECB_MCTR_TRSN").alias("MECB_MCTR_TRSN"),
    F.col("MCRE_ID").alias("MCRE_ID"),
    F.col("MECB_POLICY_ID").alias("MECB_POLICY_ID"),
    F.col("MECB_LAST_VER_DT").alias("MECB_LAST_VER_DT"),
    F.col("MECB_LAST_VER_NAME").alias("MECB_LAST_VER_NAME"),
    F.col("MECB_MCTR_VMTH").alias("MECB_MCTR_VMTH"),
    F.col("MECB_LOI_START_DT").alias("MECB_LOI_START_DT")
)

df_BusinessLogic = df_LinkCollector.withColumn("RowPassThru", F.lit("Y")).select(
    F.lit("0").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    current_timestamp().alias("FIRST_RECYC_DT"),
    F.lit("0").alias("ERR_CT"),
    F.lit("0").alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    (F.lit("FACETS") + F.lit(";") + F.col("HIST_ROW_ID")).alias("PRI_KEY_STRING"),
    F.lit("0").alias("MBR_COB_AUDIT_SK"),
    F.col("HIST_ROW_ID").alias("MBR_COB_AUDIT_ROW_ID"),
    F.lit("0").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit("0").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MEME_CK").alias("MBR_SK"),
    F.col("HIST_USUS_ID").alias("SRC_SYS_CRT_USER_SK"),
    F.when((F.length(trim(F.col("HIST_PHYS_ACT_CD")))==0)|F.col("HIST_PHYS_ACT_CD").isNull(),"NA").otherwise(F.col("HIST_PHYS_ACT_CD")).alias("MBR_COB_AUDIT_ACTN_CD_SK"),
    F.when((F.length(trim(F.col("MECB_MCTR_VMTH")))==0)|F.col("MECB_MCTR_VMTH").isNull(),"NA").otherwise(F.col("MECB_MCTR_VMTH")).alias("MBR_COB_LAST_VER_METH_CD_SK"),
    F.when((F.length(trim(F.col("MCRE_ID")))==0)|F.col("MCRE_ID").isNull(),"NA").otherwise(F.col("MCRE_ID")).alias("MBR_COB_OTHR_CAR_ID_CD_SK"),
    F.when((F.length(trim(F.col("MECB_INSUR_ORDER")))==0)|F.col("MECB_INSUR_ORDER").isNull(),"NA").otherwise(F.col("MECB_INSUR_ORDER")).alias("MBR_COB_PAYMT_PRTY_CD_SK"),
    F.when((F.length(trim(F.col("MECB_MCTR_TRSN")))==0)|F.col("MECB_MCTR_TRSN").isNull(),"NA").otherwise(F.col("MECB_MCTR_TRSN")).alias("MBR_COB_TERM_RSN_CD_SK"),
    F.when((F.length(trim(F.col("MECB_INSUR_TYPE")))==0)|F.col("MECB_INSUR_TYPE").isNull(),"NA").otherwise(F.col("MECB_INSUR_TYPE")).alias("MBR_COB_TYP_CD_SK"),
    F.col("MEME_CK").alias("MBR_UNIQ_KEY"),
    F.date_format(F.col("MECB_LAST_VER_DT"), "yyyy-MM-dd HH:mm:ss").alias("COB_LTR_TRGR_DT_SK"),
    F.date_format(F.col("MECB_EFF_DT"), "yyyy-MM-dd HH:mm:ss").alias("EFF_DT_SK"),
    F.date_format(F.col("MECB_LOI_START_DT"), "yyyy-MM-dd HH:mm:ss").alias("LACK_OF_COB_INFO_STRT_DT_SK"),
    F.date_format(F.col("HIST_CREATE_DTM"), "yyyy-MM-dd HH:mm:ss").alias("SRC_SYS_CRT_DT_SK"),
    F.date_format(F.col("MECB_TERM_DT"), "yyyy-MM-dd HH:mm:ss").alias("TERM_DT_SK"),
    F.when(F.length(trim(F.col("MECB_LAST_VER_NAME")))==0, "NA").otherwise(F.col("MECB_LAST_VER_NAME")).alias("LAST_VERIFIER_TX"),
    F.col("MECB_POLICY_ID").alias("OTHR_CAR_POL_ID")
)

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

df_dummy_hf_mbr_cob_audit = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("dbtable","IDS.dummy_hf_mbr_cob_audit")
    .load()
)

cond = [
    df_BusinessLogic["SRC_SYS_CD"] == df_dummy_hf_mbr_cob_audit["SRC_SYS_CD_SK"],
    df_BusinessLogic["MBR_COB_AUDIT_ROW_ID"] == df_dummy_hf_mbr_cob_audit["MBR_COB_AUDIT_ROW_ID"]
]

df_PrimaryKey_joined = df_BusinessLogic.alias("Transform").join(
    df_dummy_hf_mbr_cob_audit.alias("lkup"),
    on=cond,
    how="left"
)

df_PrimaryKey = df_PrimaryKey_joined.select(
    F.col("Transform.*"),
    F.col("lkup.MBR_COB_AUDIT_SK").alias("lkup_MBR_COB_AUDIT_SK"),
    F.col("lkup.CRT_RUN_CYC_EXCTN_SK").alias("lkup_CRT_RUN_CYC_EXCTN_SK")
)

df_PrimaryKey_withLogic = df_PrimaryKey.withColumn(
    "SK_col",
    F.when(F.col("lkup_MBR_COB_AUDIT_SK").isNull(), F.lit(None)).otherwise(F.col("lkup_MBR_COB_AUDIT_SK"))
).withColumn(
    "NewCrtRunCycExtcnSk_col",
    F.when(F.col("lkup_MBR_COB_AUDIT_SK").isNull(), F.col("CurrRunCycle")).otherwise(F.col("lkup_CRT_RUN_CYC_EXCTN_SK"))
).withColumn(
    "CurrRunCycle", F.lit(CurrRunCycle)
)

df_enriched = df_PrimaryKey_withLogic
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"SK_col",<schema>,<secret_name>)

df_Key = df_enriched.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("SK_col").alias("MBR_COB_AUDIT_SK"),
    F.col("MBR_COB_AUDIT_ROW_ID").alias("MBR_COB_AUDIT_ROW_ID"),
    F.col("NewCrtRunCycExtcnSk_col").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("CurrRunCycle").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("SRC_SYS_CRT_USER_SK").alias("SRC_SYS_CRT_USER_SK"),
    F.col("MBR_COB_AUDIT_ACTN_CD_SK").alias("MBR_COB_AUDIT_ACTN_CD_SK"),
    F.col("MBR_COB_LAST_VER_METH_CD_SK").alias("MBR_COB_LAST_VER_METH_CD_SK"),
    F.col("MBR_COB_OTHR_CAR_ID_CD_SK").alias("MBR_COB_OTHR_CAR_ID_CD_SK"),
    F.col("MBR_COB_PAYMT_PRTY_CD_SK").alias("MBR_COB_PAYMT_PRTY_CD_SK"),
    F.col("MBR_COB_TERM_RSN_CD_SK").alias("MBR_COB_TERM_RSN_CD_SK"),
    F.col("MBR_COB_TYP_CD_SK").alias("MBR_COB_TYP_CD_SK"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("COB_LTR_TRGR_DT_SK").alias("COB_LTR_TRGR_DT_SK"),
    F.col("EFF_DT_SK").alias("EFF_DT_SK"),
    F.col("LACK_OF_COB_INFO_STRT_DT_SK").alias("LACK_OF_COB_INFO_STRT_DT_SK"),
    F.col("SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
    F.col("TERM_DT_SK").alias("TERM_DT_SK"),
    F.col("LAST_VERIFIER_TX").alias("LAST_VERIFIER_TX"),
    F.col("OTHR_CAR_POL_ID").alias("OTHR_CAR_POL_ID")
)

df_updt = df_enriched.filter(F.col("lkup_MBR_COB_AUDIT_SK").isNull()).select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD_SK"),
    F.col("MBR_COB_AUDIT_ROW_ID").alias("MBR_COB_AUDIT_ROW_ID"),
    F.col("SK_col").alias("MBR_COB_AUDIT_SK"),
    F.col("CurrRunCycle").alias("CRT_RUN_CYC_EXCTN_SK")
)

spark.sql("DROP TABLE IF EXISTS STAGING.FctsMbrCobAuditExtr_hf_mbr_cob_audit_temp")
df_updt.write \
    .format("jdbc") \
    .option("url", jdbc_url_ids) \
    .options(**jdbc_props_ids) \
    .option("dbtable", "STAGING.FctsMbrCobAuditExtr_hf_mbr_cob_audit_temp") \
    .mode("overwrite") \
    .save()

merge_sql_hf = """
MERGE INTO IDS.dummy_hf_mbr_cob_audit AS T
USING STAGING.FctsMbrCobAuditExtr_hf_mbr_cob_audit_temp AS S
ON T.SRC_SYS_CD_SK = S.SRC_SYS_CD_SK
   AND T.MBR_COB_AUDIT_ROW_ID = S.MBR_COB_AUDIT_ROW_ID
WHEN MATCHED THEN UPDATE SET
  T.MBR_COB_AUDIT_SK = S.MBR_COB_AUDIT_SK,
  T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK
WHEN NOT MATCHED THEN
  INSERT (SRC_SYS_CD_SK, MBR_COB_AUDIT_ROW_ID, MBR_COB_AUDIT_SK, CRT_RUN_CYC_EXCTN_SK)
  VALUES (S.SRC_SYS_CD_SK, S.MBR_COB_AUDIT_ROW_ID, S.MBR_COB_AUDIT_SK, S.CRT_RUN_CYC_EXCTN_SK);
"""

execute_dml(merge_sql_hf, jdbc_url_ids, jdbc_props_ids)

df_final = df_Key.select(
    rpad(F.col("JOB_EXCTN_RCRD_ERR_SK").cast(StringType()), 1, " ").alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(F.col("INSRT_UPDT_CD").cast(StringType()), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(F.col("DISCARD_IN").cast(StringType()), 1, " ").alias("DISCARD_IN"),
    rpad(F.col("PASS_THRU_IN").cast(StringType()), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    rpad(F.col("ERR_CT").cast(StringType()), 1, " ").alias("ERR_CT"),
    rpad(F.col("RECYCLE_CT").cast(StringType()), 1, " ").alias("RECYCLE_CT"),
    rpad(F.col("SRC_SYS_CD").cast(StringType()), 6, " ").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    rpad(F.col("MBR_COB_AUDIT_SK").cast(StringType()), 10, " ").alias("MBR_COB_AUDIT_SK"),
    rpad(F.col("MBR_COB_AUDIT_ROW_ID").cast(StringType()), 12, " ").alias("MBR_COB_AUDIT_ROW_ID"),
    rpad(F.col("CRT_RUN_CYC_EXCTN_SK").cast(StringType()), 10, " ").alias("CRT_RUN_CYC_EXCTN_SK"),
    rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").cast(StringType()), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(F.col("MBR_SK").cast(StringType()), 10, " ").alias("MBR_SK"),
    rpad(F.col("SRC_SYS_CRT_USER_SK").cast(StringType()), 20, " ").alias("SRC_SYS_CRT_USER_SK"),
    rpad(F.col("MBR_COB_AUDIT_ACTN_CD_SK").cast(StringType()), 3, " ").alias("MBR_COB_AUDIT_ACTN_CD_SK"),
    rpad(F.col("MBR_COB_LAST_VER_METH_CD_SK").cast(StringType()), 4, " ").alias("MBR_COB_LAST_VER_METH_CD_SK"),
    rpad(F.col("MBR_COB_OTHR_CAR_ID_CD_SK").cast(StringType()), 9, " ").alias("MBR_COB_OTHR_CAR_ID_CD_SK"),
    rpad(F.col("MBR_COB_PAYMT_PRTY_CD_SK").cast(StringType()), 3, " ").alias("MBR_COB_PAYMT_PRTY_CD_SK"),
    rpad(F.col("MBR_COB_TERM_RSN_CD_SK").cast(StringType()), 4, " ").alias("MBR_COB_TERM_RSN_CD_SK"),
    rpad(F.col("MBR_COB_TYP_CD_SK").cast(StringType()), 3, " ").alias("MBR_COB_TYP_CD_SK"),
    rpad(F.col("MBR_UNIQ_KEY").cast(StringType()), 10, " ").alias("MBR_UNIQ_KEY"),
    rpad(F.col("COB_LTR_TRGR_DT_SK").cast(StringType()), 19, " ").alias("COB_LTR_TRGR_DT_SK"),
    rpad(F.col("EFF_DT_SK").cast(StringType()), 19, " ").alias("EFF_DT_SK"),
    rpad(F.col("LACK_OF_COB_INFO_STRT_DT_SK").cast(StringType()), 19, " ").alias("LACK_OF_COB_INFO_STRT_DT_SK"),
    rpad(F.col("SRC_SYS_CRT_DT_SK").cast(StringType()), 19, " ").alias("SRC_SYS_CRT_DT_SK"),
    rpad(F.col("TERM_DT_SK").cast(StringType()), 19, " ").alias("TERM_DT_SK"),
    rpad(F.col("LAST_VERIFIER_TX").cast(StringType()), 30, " ").alias("LAST_VERIFIER_TX"),
    F.col("OTHR_CAR_POL_ID")
)

write_files(
    df_final,
    f"{adls_path}/key/FctsMbrCobAuditExtr.FctsMbrCobAudit.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)