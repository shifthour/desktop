# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:     FctsMbrAuditExtr
# MAGIC 
# MAGIC DESCRIPTION:   Pulls data from Facets Audit CMC_MEME_MEMBER table for loading to the IDS MBR_AUDIT table
# MAGIC       
# MAGIC 
# MAGIC INPUTS:       Facets Audit 4.21 Membership Profile system
# MAGIC                       tables:  CMC_MEME_MEMBER
# MAGIC 
# MAGIC HASH FILES:    hf_mbr_audit
# MAGIC                       
# MAGIC TRANSFORMS:   STRIP.FIELD
# MAGIC                              FORMAT.DATE
# MAGIC                            
# MAGIC PROCESSING:  Extract, transform, and primary keying for Membership subject area.
# MAGIC   
# MAGIC OUTPUTS:   Sequential file
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               Parikshith Chada   10/24/2006  -  Originally Programmed
# MAGIC       
# MAGIC Developer                          Date                    Project/Altiris #                     Change Description                                                                                   Development Project               Code Reviewer                   Date Reviewed
# MAGIC ----------------------------------      -------------------      -----------------------------------           ---------------------------------------------------------                                                             ----------------------------------              ---------------------------------           -------------------------
# MAGIC    
# MAGIC Akhila Manickavelu          09/22/2016      5628-WorkersComp              Exclusion Criteria- P_SEL_PRCS_CRITR                                                                IntegrateDevl                 Kalyan Neelam                    2016-10-13
# MAGIC                                                                                                                   to remove wokers comp GRGR_ID's          
# MAGIC Prabhu ES                        02/24/2022       S2S Remediation                  MSSQL connection parameters added                                                                    IntegrateDev5	Ken Bradmon	2022-05-18

# MAGIC Strip Fields
# MAGIC All Update rows
# MAGIC All Delete and Insert Rows
# MAGIC Assign primary surrogate key
# MAGIC Apply business logic.
# MAGIC End of FACETS EXTRACTION JOB FROM CMC_MEME_MEMBER
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
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
BeginDate = get_widget_value('BeginDate','')
EndDate = get_widget_value('EndDate','')
BCBSOwner = get_widget_value('BCBSOwner','')
bcbs_secret_name = get_widget_value('bcbs_secret_name','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url_hf_mbr_audit, jdbc_props_hf_mbr_audit = get_db_config(ids_secret_name)
df_hf_mbr_audit_lookup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_hf_mbr_audit)
    .options(**jdbc_props_hf_mbr_audit)
    .option("query", "SELECT SRC_SYS_CD_SK, MBR_AUDIT_ROW_ID, MBR_AUDIT_SK, CRT_RUN_CYC_EXCTN_SK FROM IDS.dummy_hf_mbr_audit")
    .load()
)

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
extract_query = (
    f"SELECT MEME.MEME_CK, MEME.HIST_ROW_ID, MEME.HIST_CREATE_DTM, MEME.HIST_USUS_ID, MEME.HIST_PHYS_ACT_CD, "
    f"MEME.HIST_IMAGE_CD, MEME.TXN1_ROW_ID, MEME.MEME_ID_NAME, MEME.MEME_LAST_NAME, MEME.MEME_FIRST_NAME, "
    f"MEME.MEME_MID_INIT, MEME.MEME_TITLE, MEME.MEME_SSN, MEME.MEME_SEX, MEME.MEME_BIRTH_DT, MEME.MEME_PREX_EFF_DT, "
    f"MEME.GRGR_CK "
    f"FROM {FacetsOwner}.CMC_MEME_MEMBER MEME "
    f"WHERE MEME.HIST_CREATE_DTM >= '{BeginDate}' "
    f"AND MEME.HIST_CREATE_DTM <= '{EndDate}' "
    f"AND NOT EXISTS (SELECT DISTINCT CMC_GRGR_GROUP.GRGR_CK "
    f"FROM {BCBSOwner}.P_SEL_PRCS_CRITR P_SEL_PRCS_CRITR, "
    f"{FacetsOwner}.CMC_GRGR_GROUP CMC_GRGR_GROUP "
    f"WHERE P_SEL_PRCS_CRITR.SEL_PRCS_ID = 'WRHSINBEXCL' "
    f"AND P_SEL_PRCS_CRITR.SEL_PRCS_ITEM_ID = 'WORKCOMP' "
    f"AND P_SEL_PRCS_CRITR.SEL_PRCS_CRITR_CD = 'MEDPROC' "
    f"AND CMC_GRGR_GROUP.GRGR_ID >= P_SEL_PRCS_CRITR.CRITR_VAL_FROM_TX "
    f"AND CMC_GRGR_GROUP.GRGR_ID <= P_SEL_PRCS_CRITR.CRITR_VAL_THRU_TX "
    f"AND MEME.GRGR_CK=CMC_GRGR_GROUP.GRGR_CK) "
    f"ORDER BY MEME.MEME_CK, MEME.TXN1_ROW_ID, MEME.HIST_ROW_ID"
)

df_MbrAuditExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query)
    .load()
)

df_StripFieldMeme = df_MbrAuditExtr.select(
    F.col("MEME_CK").alias("MEME_CK"),
    strip_field(F.col("HIST_ROW_ID")).alias("HIST_ROW_ID"),
    F.col("HIST_CREATE_DTM").alias("HIST_CREATE_DTM"),
    strip_field(F.col("HIST_USUS_ID")).alias("HIST_USUS_ID"),
    strip_field(F.col("HIST_PHYS_ACT_CD")).alias("HIST_PHYS_ACT_CD"),
    strip_field(F.col("HIST_IMAGE_CD")).alias("HIST_IMAGE_CD"),
    F.col("TXN1_ROW_ID").alias("TXN1_ROW_ID"),
    strip_field(F.col("MEME_ID_NAME")).alias("MEME_ID_NAME"),
    strip_field(F.col("MEME_LAST_NAME")).alias("MEME_LAST_NAME"),
    strip_field(F.col("MEME_FIRST_NAME")).alias("MEME_FIRST_NAME"),
    strip_field(F.col("MEME_MID_INIT")).alias("MEME_MID_INIT"),
    strip_field(F.col("MEME_TITLE")).alias("MEME_TITLE"),
    strip_field(F.col("MEME_SSN")).alias("MEME_SSN"),
    strip_field(F.col("MEME_SEX")).alias("MEME_SEX"),
    F.col("MEME_BIRTH_DT").alias("MEME_BIRTH_DT"),
    F.col("MEME_PREX_EFF_DT").alias("MEME_PREX_EFF_DT"),
    F.col("GRGR_CK").alias("GRGR_CK")
)

df_InsDel = df_StripFieldMeme.filter(
    (F.col("HIST_PHYS_ACT_CD") == 'I') | (F.col("HIST_PHYS_ACT_CD") == 'D')
)

df_Update = df_StripFieldMeme.filter(
    F.col("HIST_PHYS_ACT_CD") == 'U'
)

df_Transformer_130 = df_InsDel

windowSpec_127 = Window.orderBy(F.monotonically_increasing_id())

df_Transformer_127_pre = (
    df_Update
    .withColumn("PrevMemeIdName", F.lag(F.col("MEME_ID_NAME"), 1).over(windowSpec_127))
    .withColumn("svTempMemeIdName", F.when(F.col("MEME_ID_NAME") == F.col("PrevMemeIdName"), F.lit("0")).otherwise(F.col("MEME_ID_NAME")))
    .withColumn("PrevMemeLstName", F.lag(F.col("MEME_LAST_NAME"), 1).over(windowSpec_127))
    .withColumn("svTempMemeLstName", F.when(F.col("MEME_LAST_NAME") == F.col("PrevMemeLstName"), F.lit("0")).otherwise(F.col("MEME_LAST_NAME")))
    .withColumn("PrevMemeFstName", F.lag(F.col("MEME_FIRST_NAME"), 1).over(windowSpec_127))
    .withColumn("svTempMemeFstName", F.when(F.col("MEME_FIRST_NAME") == F.col("PrevMemeFstName"), F.lit("0")).otherwise(F.col("MEME_FIRST_NAME")))
    .withColumn("PrevMemeMidInit", F.lag(F.col("MEME_MID_INIT"), 1).over(windowSpec_127))
    .withColumn("svTempMemeMidInit", F.when(F.col("MEME_MID_INIT") == F.col("PrevMemeMidInit"), F.lit("0")).otherwise(F.col("MEME_MID_INIT")))
    .withColumn("PrevMemeTitle", F.lag(F.col("MEME_TITLE"), 1).over(windowSpec_127))
    .withColumn("svTempMemeTitle", F.when(F.col("MEME_TITLE") == F.col("PrevMemeTitle"), F.lit("0")).otherwise(F.col("MEME_TITLE")))
    .withColumn("svCurrMemeIdName", F.col("svTempMemeIdName"))
    .withColumn("svCurrMemeLstName", F.col("svTempMemeLstName"))
    .withColumn("svCurrMemeFstName", F.col("svTempMemeFstName"))
    .withColumn("svCurrMemeMidInit", F.col("svTempMemeMidInit"))
    .withColumn("svCurrMemeTitle", F.col("svTempMemeTitle"))
)

df_Transformer_127 = df_Transformer_127_pre.filter(
    (F.col("HIST_IMAGE_CD") != 'B') & (
        (F.col("svCurrMemeIdName") != '0') |
        (F.col("svCurrMemeLstName") != '0') |
        (F.col("svCurrMemeFstName") != '0') |
        (F.col("svCurrMemeMidInit") != '0') |
        (F.col("svCurrMemeTitle") != '0')
    )
)

df_Transformer_127_out = df_Transformer_127.select(
    F.col("MEME_CK").alias("MEME_CK"),
    F.col("HIST_ROW_ID").alias("HIST_ROW_ID"),
    F.col("HIST_CREATE_DTM").alias("HIST_CREATE_DTM"),
    F.col("HIST_USUS_ID").alias("HIST_USUS_ID"),
    F.col("HIST_PHYS_ACT_CD").alias("HIST_PHYS_ACT_CD"),
    F.col("HIST_IMAGE_CD").alias("HIST_IMAGE_CD"),
    F.col("TXN1_ROW_ID").alias("TXN1_ROW_ID"),
    F.col("MEME_ID_NAME").alias("MEME_ID_NAME"),
    F.col("MEME_LAST_NAME").alias("MEME_LAST_NAME"),
    F.col("MEME_FIRST_NAME").alias("MEME_FIRST_NAME"),
    F.col("MEME_MID_INIT").alias("MEME_MID_INIT"),
    F.col("MEME_TITLE").alias("MEME_TITLE"),
    F.col("MEME_SSN").alias("MEME_SSN"),
    F.col("MEME_SEX").alias("MEME_SEX"),
    F.col("MEME_BIRTH_DT").alias("MEME_BIRTH_DT"),
    F.col("MEME_PREX_EFF_DT").alias("MEME_PREX_EFF_DT"),
    F.col("GRGR_CK").alias("GRGR_CK")
)

df_Transformer_130_out = df_Transformer_130.select(
    F.col("MEME_CK").alias("MEME_CK"),
    F.col("HIST_ROW_ID").alias("HIST_ROW_ID"),
    F.col("HIST_CREATE_DTM").alias("HIST_CREATE_DTM"),
    F.col("HIST_USUS_ID").alias("HIST_USUS_ID"),
    F.col("HIST_PHYS_ACT_CD").alias("HIST_PHYS_ACT_CD"),
    F.col("HIST_IMAGE_CD").alias("HIST_IMAGE_CD"),
    F.col("TXN1_ROW_ID").alias("TXN1_ROW_ID"),
    F.col("MEME_ID_NAME").alias("MEME_ID_NAME"),
    F.col("MEME_LAST_NAME").alias("MEME_LAST_NAME"),
    F.col("MEME_FIRST_NAME").alias("MEME_FIRST_NAME"),
    F.col("MEME_MID_INIT").alias("MEME_MID_INIT"),
    F.col("MEME_TITLE").alias("MEME_TITLE"),
    F.col("MEME_SSN").alias("MEME_SSN"),
    F.col("MEME_SEX").alias("MEME_SEX"),
    F.col("MEME_BIRTH_DT").alias("MEME_BIRTH_DT"),
    F.col("MEME_PREX_EFF_DT").alias("MEME_PREX_EFF_DT"),
    F.col("GRGR_CK").alias("GRGR_CK")
)

df_Link_Collector_105 = df_Transformer_127_out.union(df_Transformer_130_out)

df_BusinessLogic = df_Link_Collector_105.withColumn("RowPassThru", F.lit("Y"))

df_BusinessLogic_out = df_BusinessLogic.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.col("RowPassThru").alias("PASS_THRU_IN"),
    current_timestamp().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.concat(F.lit("FACETS"), F.lit(";"), F.col("HIST_ROW_ID")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("MBR_AUDIT_SK"),
    F.col("HIST_ROW_ID").alias("MBR_AUDIT_ROW_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MEME_CK").alias("MBR_SK"),
    F.col("HIST_USUS_ID").alias("SRC_SYS_CRT_USER_SK"),
    F.col("HIST_PHYS_ACT_CD").alias("MBR_AUDIT_ACTN_CD_SK"),
    F.col("MEME_SEX").alias("MBR_GNDR_CD_SK"),
    F.when(F.col("GRGR_CK") == 38, F.lit("Y")).otherwise(F.lit("N")).alias("SCRD_IN"),
    F.date_format(F.col("MEME_BIRTH_DT"), "yyyy-MM-dd").alias("BRTH_DT_SK"),
    F.date_format(F.col("MEME_PREX_EFF_DT"), "yyyy-MM-dd").alias("PREX_COND_EFF_DT_SK"),
    F.date_format(F.col("HIST_CREATE_DTM"), "yyyy-MM-dd").alias("SRC_SYS_CRT_DT_SK"),
    F.col("MEME_CK").alias("MBR_UNIQ_KEY"),
    F.when(F.col("MEME_FIRST_NAME") == F.lit(' '), F.lit(None)).otherwise(F.col("MEME_FIRST_NAME")).alias("FIRST_NM"),
    F.when(F.col("MEME_MID_INIT") == F.lit(' '), F.lit(None)).otherwise(F.col("MEME_MID_INIT")).alias("MIDINIT"),
    F.when(F.col("MEME_LAST_NAME") == F.lit(' '), F.lit(None)).otherwise(F.col("MEME_LAST_NAME")).alias("LAST_NM"),
    F.when((F.length(trim(F.col("MEME_SSN"))) == 0) | (F.col("MEME_SSN").isNull()), F.lit("NA")).otherwise(F.col("MEME_SSN")).alias("SSN")
)

df_PrimaryKey_join = df_BusinessLogic_out.alias("Transform").join(
    df_hf_mbr_audit_lookup.alias("lkup"),
    [
        F.col("Transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD_SK"),
        F.col("Transform.MBR_AUDIT_ROW_ID") == F.col("lkup.MBR_AUDIT_ROW_ID")
    ],
    "left"
)

df_PrimaryKey_temp = df_PrimaryKey_join.select(
    F.col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("Transform.DISCARD_IN").alias("DISCARD_IN"),
    F.col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("Transform.ERR_CT").alias("ERR_CT"),
    F.col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("lkup.MBR_AUDIT_SK").alias("lkup_MBR_AUDIT_SK"),
    F.col("Transform.MBR_AUDIT_ROW_ID").alias("MBR_AUDIT_ROW_ID"),
    F.col("lkup.CRT_RUN_CYC_EXCTN_SK").alias("lkup_CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("param_CurrRunCycle"),
    F.col("Transform.MBR_SK").alias("MBR_SK"),
    F.col("Transform.SRC_SYS_CRT_USER_SK").alias("SRC_SYS_CRT_USER_SK"),
    F.col("Transform.MBR_AUDIT_ACTN_CD_SK").alias("MBR_AUDIT_ACTN_CD_SK"),
    F.col("Transform.MBR_GNDR_CD_SK").alias("MBR_GNDR_CD_SK"),
    F.col("Transform.SCRD_IN").alias("SCRD_IN"),
    F.col("Transform.BRTH_DT_SK").alias("BRTH_DT_SK"),
    F.col("Transform.PREX_COND_EFF_DT_SK").alias("PREX_COND_EFF_DT_SK"),
    F.col("Transform.SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
    F.col("Transform.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("Transform.FIRST_NM").alias("FIRST_NM"),
    F.col("Transform.MIDINIT").alias("MIDINIT"),
    F.col("Transform.LAST_NM").alias("LAST_NM"),
    F.col("Transform.SSN").alias("SSN")
).withColumn(
    "MBR_AUDIT_SK",
    F.col("lkup_MBR_AUDIT_SK")
).withColumn(
    "CRT_RUN_CYC_EXCTN_SK",
    F.when(F.col("lkup_MBR_AUDIT_SK").isNull(), F.col("param_CurrRunCycle")).otherwise(F.col("lkup_CRT_RUN_CYC_EXCTN_SK"))
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    F.col("param_CurrRunCycle")
)

df_enriched = df_PrimaryKey_temp

df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"MBR_AUDIT_SK",<schema>,<secret_name>)

df_PrimaryKey = df_enriched.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD"),
    F.col("DISCARD_IN"),
    F.col("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("MBR_AUDIT_SK"),
    F.col("MBR_AUDIT_ROW_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MBR_SK"),
    F.col("SRC_SYS_CRT_USER_SK"),
    F.col("MBR_AUDIT_ACTN_CD_SK"),
    F.col("MBR_GNDR_CD_SK"),
    F.col("SCRD_IN"),
    F.col("BRTH_DT_SK"),
    F.col("PREX_COND_EFF_DT_SK"),
    F.col("SRC_SYS_CRT_DT_SK"),
    F.col("MBR_UNIQ_KEY"),
    F.col("FIRST_NM"),
    F.col("MIDINIT"),
    F.col("LAST_NM"),
    F.col("SSN")
)

df_updt = df_enriched.filter(F.col("lkup_MBR_AUDIT_SK").isNull()).select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD_SK"),
    F.col("MBR_AUDIT_ROW_ID").alias("MBR_AUDIT_ROW_ID"),
    F.col("MBR_AUDIT_SK").alias("MBR_AUDIT_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK")
)

temp_table_name = "STAGING.FctsMbrAuditExtr_hf_mbr_audit_temp"
drop_sql = f"DROP TABLE IF EXISTS {temp_table_name}"
execute_dml(drop_sql, jdbc_url_hf_mbr_audit, jdbc_props_hf_mbr_audit)

df_updt.write.jdbc(
    url=jdbc_url_hf_mbr_audit,
    table=temp_table_name,
    mode="overwrite",
    properties=jdbc_props_hf_mbr_audit
)

merge_sql = (
    f"MERGE INTO IDS.dummy_hf_mbr_audit AS T "
    f"USING {temp_table_name} AS S "
    f"ON T.SRC_SYS_CD_SK = S.SRC_SYS_CD_SK AND T.MBR_AUDIT_ROW_ID = S.MBR_AUDIT_ROW_ID "
    f"WHEN MATCHED THEN UPDATE SET T.MBR_AUDIT_SK = S.MBR_AUDIT_SK, T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK "
    f"WHEN NOT MATCHED THEN INSERT (SRC_SYS_CD_SK, MBR_AUDIT_ROW_ID, MBR_AUDIT_SK, CRT_RUN_CYC_EXCTN_SK) "
    f"VALUES (S.SRC_SYS_CD_SK, S.MBR_AUDIT_ROW_ID, S.MBR_AUDIT_SK, S.CRT_RUN_CYC_EXCTN_SK);"
)
execute_dml(merge_sql, jdbc_url_hf_mbr_audit, jdbc_props_hf_mbr_audit)

df_IdsMbrAuditExtr_pre = df_PrimaryKey.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("MBR_AUDIT_SK"),
    F.col("MBR_AUDIT_ROW_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MBR_SK"),
    F.col("SRC_SYS_CRT_USER_SK"),
    F.col("MBR_AUDIT_ACTN_CD_SK"),
    F.col("MBR_GNDR_CD_SK"),
    F.rpad(F.col("SCRD_IN"), 1, " ").alias("SCRD_IN"),
    F.rpad(F.col("BRTH_DT_SK"), 10, " ").alias("BRTH_DT_SK"),
    F.rpad(F.col("PREX_COND_EFF_DT_SK"), 10, " ").alias("PREX_COND_EFF_DT_SK"),
    F.rpad(F.col("SRC_SYS_CRT_DT_SK"), 10, " ").alias("SRC_SYS_CRT_DT_SK"),
    F.col("MBR_UNIQ_KEY"),
    F.col("FIRST_NM"),
    F.rpad(F.col("MIDINIT"), 1, " ").alias("MIDINIT"),
    F.col("LAST_NM"),
    F.col("SSN")
)

df_IdsMbrAuditExtr = df_IdsMbrAuditExtr_pre

write_files(
    df_IdsMbrAuditExtr,
    f"{adls_path}/key/FctsMbrAuditExtr.FctsMbrAudit.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)