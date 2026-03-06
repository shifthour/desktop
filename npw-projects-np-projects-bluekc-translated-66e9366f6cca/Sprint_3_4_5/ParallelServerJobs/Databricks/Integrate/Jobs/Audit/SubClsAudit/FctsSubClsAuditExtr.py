# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:     FctsSubClsAuditExtr
# MAGIC 
# MAGIC DESCRIPTION:   Pulls data from Facets Audit CMC_SBCS_CLASS table for loading to the IDS SUB_CLS_AUDIT table
# MAGIC       
# MAGIC 
# MAGIC INPUTS:       Facets Audit 4.21 Membership Profile system
# MAGIC                       tables:  CMC_SBCS_CLASS
# MAGIC 
# MAGIC HASH FILES:   hf_sub_cls_audit
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
# MAGIC 
# MAGIC Developer                          Date                    Project/Altiris #                     Change Description                                                                                   Development Project               Code Reviewer                   Date Reviewed
# MAGIC ----------------------------------      -------------------      -----------------------------------           ---------------------------------------------------------                                                             ----------------------------------              ---------------------------------           -------------------------
# MAGIC               Parikshith Chada   10/24/2006  -  Originally Programmed
# MAGIC               Steph Goddard   3/28/2007         Changed hard-coded facets_repl.dbo to #$FacetsDB#.dbo - 
# MAGIC                                                                      the CMC_SBCS_CLASS table needs to come from Audit, but CMC_GRGR_GROUP does not 
# MAGIC                                                                      therefore #$FacetsOwner# points to the audit tables
# MAGIC Akhila Manickavelu          09/22/2016      5628-WorkersComp              Exclusion Criteria- P_SEL_PRCS_CRITR                                                                IntegrateDevl                 Kalyan Neelam                     2016-10-13
# MAGIC                                                                                                                   to remove wokers comp GRGR_ID's             
# MAGIC Prabhu ES                        02/24/2022       S2S Remediation                   MSSQL connection parameters added                                                                   IntegrateDev5	Ken Bradmon	2022-05-18

# MAGIC All Update rows
# MAGIC All Delete and Insert Rows
# MAGIC Assign primary surrogate key
# MAGIC Apply business logic.
# MAGIC End of FACETS EXTRACTION JOB FROM CMC_SBCS_CLASS
# MAGIC the CMC_SBCS_CLASS table needs to come from Audit, but CMC_GRGR_GROUP does not therefore #$FacetsOwner# points to the audit tables and CMC_GRGR_GROUP is prefaced by #$FacetsDB#.dbo
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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
FacetsOwner = get_widget_value('$FacetsOwner','')
BeginDate = get_widget_value('BeginDate','')
EndDate = get_widget_value('EndDate','')
BCBSOwner = get_widget_value('$BCBSOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
bcbs_secret_name = get_widget_value('bcbs_secret_name','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
query_facets = (
    "SELECT cscl.SBSB_CK,\n"
    "cscl.SBCS_EFF_DT,\n"
    "cscl.HIST_ROW_ID,\n"
    "cscl.HIST_CREATE_DTM,\n"
    "cscl.HIST_USUS_ID,\n"
    "cscl.HIST_PHYS_ACT_CD,\n"
    "cscl.HIST_IMAGE_CD,\n"
    "cscl.TXN1_ROW_ID,\n"
    "cscl.SBCS_TERM_DT,\n"
    "cscl.CSCS_ID,\n"
    "cggp.GRGR_ID,\n"
    "cggp.GRGR_CK\n"
    "FROM \n"
    + FacetsOwner
    + ".CMC_SBCS_CLASS cscl,\n"
    + FacetsOwner
    + ".CMC_GRGR_GROUP cggp\n"
    "WHERE \n"
    "cscl.HIST_CREATE_DTM >= '"
    + BeginDate
    + "' \n"
    "AND cscl.HIST_CREATE_DTM <= '"
    + EndDate
    + "' \n"
    "AND cggp.GRGR_CK = cscl.GRGR_CK\n"
    "and  NOT EXISTS (SELECT DISTINCT\n"
    "CMC_GRGR_GROUP.GRGR_CK \n"
    "FROM \n"
    + BCBSOwner
    + ".P_SEL_PRCS_CRITR P_SEL_PRCS_CRITR,\n"
    + FacetsOwner
    + ".CMC_GRGR_GROUP CMC_GRGR_GROUP \n"
    "WHERE P_SEL_PRCS_CRITR.SEL_PRCS_ID = 'WRHSINBEXCL'\n"
    "and P_SEL_PRCS_CRITR.SEL_PRCS_ITEM_ID = 'WORKCOMP'\n"
    "and P_SEL_PRCS_CRITR.SEL_PRCS_CRITR_CD = 'MEDPROC'\n"
    "and CMC_GRGR_GROUP.GRGR_ID >= P_SEL_PRCS_CRITR.CRITR_VAL_FROM_TX\n"
    "and CMC_GRGR_GROUP.GRGR_ID <= P_SEL_PRCS_CRITR.CRITR_VAL_THRU_TX and \n"
    "cggp.GRGR_CK=CMC_GRGR_GROUP.GRGR_CK)\n\n"
    "ORDER BY cscl.SBSB_CK,cscl.TXN1_ROW_ID,cscl.HIST_ROW_ID"
)
df_SubClsAuditExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_facets)
    .load()
)

df_StripFieldSbcs = (
    df_SubClsAuditExtr
    .withColumn("SBSB_CK", F.col("SBSB_CK"))
    .withColumn("SBCS_EFF_DT", F.col("SBCS_EFF_DT"))
    .withColumn("HIST_ROW_ID", strip_field(F.col("HIST_ROW_ID")))
    .withColumn("HIST_CREATE_DTM", F.col("HIST_CREATE_DTM"))
    .withColumn("HIST_USUS_ID", strip_field(F.col("HIST_USUS_ID")))
    .withColumn("HIST_PHYS_ACT_CD", strip_field(F.col("HIST_PHYS_ACT_CD")))
    .withColumn("HIST_IMAGE_CD", strip_field(F.col("HIST_IMAGE_CD")))
    .withColumn("TXN1_ROW_ID", F.col("TXN1_ROW_ID"))
    .withColumn("SBCS_TERM_DT", F.col("SBCS_TERM_DT"))
    .withColumn("CSCS_ID", strip_field(F.col("CSCS_ID")))
    .withColumn("GRGR_ID", strip_field(F.col("GRGR_ID")))
)

df_InsDel = df_StripFieldSbcs.filter(
    (F.col("HIST_PHYS_ACT_CD") == "I") | (F.col("HIST_PHYS_ACT_CD") == "D")
)
df_Update = df_StripFieldSbcs.filter(F.col("HIST_PHYS_ACT_CD") == "U")

w_127 = Window.orderBy(F.monotonically_increasing_id())

df_Transformer_127_step = (
    df_Update
    .withColumn("_row", F.monotonically_increasing_id())
    .withColumn("svPrevSbcsEffDt", F.lag("SBCS_EFF_DT", 1).over(w_127))
    .withColumn("svTempSbcsEffDt", F.when(F.col("SBCS_EFF_DT") == F.col("svPrevSbcsEffDt"), F.lit(0)).otherwise(F.col("SBCS_EFF_DT")))
    .withColumn("svCurrSbcsEffDt", F.col("svTempSbcsEffDt"))
    .withColumn("svPrevSbcsTermDt", F.lag("SBCS_TERM_DT", 1).over(w_127))
    .withColumn("svTempSbcsTermDt", F.when(F.col("SBCS_TERM_DT") == F.col("svPrevSbcsTermDt"), F.lit(0)).otherwise(F.col("SBCS_TERM_DT")))
    .withColumn("svCurrSbcsTermDt", F.col("svTempSbcsTermDt"))
    .withColumn("svPrevCscsId", F.lag("CSCS_ID", 1).over(w_127))
    .withColumn("svTempCscsId", F.when(F.col("CSCS_ID") == F.col("svPrevCscsId"), F.lit(0)).otherwise(F.col("CSCS_ID")))
    .withColumn("svCurrCscsId", F.col("svTempCscsId"))
)

df_Transformer_127 = df_Transformer_127_step.filter(
    (F.col("HIST_IMAGE_CD") != "B")
    & (
        (F.col("svCurrSbcsEffDt") != 0)
        | (F.col("svCurrSbcsTermDt") != 0)
        | (F.col("svCurrCscsId") != 0)
    )
).drop("_row","svPrevSbcsEffDt","svTempSbcsEffDt","svPrevSbcsTermDt","svTempSbcsTermDt","svPrevCscsId","svTempCscsId","svCurrSbcsEffDt","svCurrSbcsTermDt","svCurrCscsId")

df_Transformer_127 = df_Transformer_127.select(
    "SBSB_CK",
    "SBCS_EFF_DT",
    "HIST_ROW_ID",
    "HIST_CREATE_DTM",
    "HIST_USUS_ID",
    "HIST_PHYS_ACT_CD",
    "HIST_IMAGE_CD",
    "TXN1_ROW_ID",
    "SBCS_TERM_DT",
    "CSCS_ID",
    "GRGR_ID"
)

df_Transformer_130 = df_InsDel.select(
    "SBSB_CK",
    "SBCS_EFF_DT",
    "HIST_ROW_ID",
    "HIST_CREATE_DTM",
    "HIST_USUS_ID",
    "HIST_PHYS_ACT_CD",
    "HIST_IMAGE_CD",
    "TXN1_ROW_ID",
    "SBCS_TERM_DT",
    "CSCS_ID",
    "GRGR_ID"
)

df_Link_Collector_105 = df_Transformer_127.unionByName(df_Transformer_130)

df_BusinessLogic = df_Link_Collector_105.withColumn("RowPassThru", F.lit("Y"))

df_BusinessLogic = df_BusinessLogic.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.col("RowPassThru").alias("PASS_THRU_IN"),
    current_timestamp().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.concat(F.lit("FACETS"), F.lit(";"), F.col("HIST_ROW_ID")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("SUB_CLS_AUDIT_SK"),
    F.col("HIST_ROW_ID").alias("SUB_CLS_AUDIT_ROW_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CSCS_ID").alias("CLS_SK"),
    F.col("HIST_USUS_ID").alias("SRC_SYS_CRT_USER_SK"),
    F.col("SBSB_CK").alias("SUB_SK"),
    F.when(
        (F.length(trim(F.col("HIST_PHYS_ACT_CD"))) == 0) | (F.col("HIST_PHYS_ACT_CD").isNull()),
        F.lit("NA")
    ).otherwise(F.col("HIST_PHYS_ACT_CD")).alias("SUB_CLS_AUDIT_ACTN_CD_SK"),
    F.date_format(F.col("HIST_CREATE_DTM"), "yyyy-MM-dd").alias("SRC_SYS_CRT_DT_SK"),
    F.col("SBSB_CK").alias("SUB_UNIQ_KEY"),
    F.date_format(F.col("SBCS_EFF_DT"), "yyyy-MM-dd").alias("SUB_CLS_AUDIT_EFF_DT"),
    F.date_format(F.col("SBCS_TERM_DT"), "yyyy-MM-dd").alias("SUB_CLS_AUDIT_TERM_DT"),
    F.col("GRGR_ID").alias("GRGR_ID")
)

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_hf_sub_cls_audit_lookup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", "SELECT SRC_SYS_CD_SK, SUB_CLS_AUDIT_ROW_ID, SUB_CLS_AUDIT_SK, CRT_RUN_CYC_EXCTN_SK FROM dummy_hf_sub_cls_audit")
    .load()
)

df_PrimaryKeyJoined = (
    df_BusinessLogic.alias("Transform")
    .join(
        df_hf_sub_cls_audit_lookup.alias("lkup"),
        (F.col("Transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD_SK"))
        & (F.col("Transform.SUB_CLS_AUDIT_ROW_ID") == F.col("lkup.SUB_CLS_AUDIT_ROW_ID")),
        "left"
    )
)

df_enriched = df_PrimaryKeyJoined

df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"SUB_CLS_AUDIT_SK",<schema>,<secret_name>)

df_enriched = df_enriched.withColumn(
    "CRT_RUN_CYC_EXCTN_SK",
    F.when(F.col("lkup.SUB_CLS_AUDIT_SK").isNull(), F.lit(CurrRunCycle)).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK"))
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    F.lit(CurrRunCycle)
)

df_Key = df_enriched.select(
    F.col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("Transform.INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("Transform.DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("Transform.PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("Transform.ERR_CT").alias("ERR_CT"),
    F.col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("SUB_CLS_AUDIT_SK").alias("SUB_CLS_AUDIT_SK"),
    F.col("Transform.SUB_CLS_AUDIT_ROW_ID").alias("SUB_CLS_AUDIT_ROW_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.CLS_SK").alias("CLS_SK"),
    F.col("Transform.SRC_SYS_CRT_USER_SK").alias("SRC_SYS_CRT_USER_SK"),
    F.col("Transform.SUB_SK").alias("SUB_SK"),
    F.col("Transform.SUB_CLS_AUDIT_ACTN_CD_SK").alias("SUB_CLS_AUDIT_ACTN_CD_SK"),
    F.rpad(F.col("Transform.SRC_SYS_CRT_DT_SK"), 10, " ").alias("SRC_SYS_CRT_DT_SK"),
    F.col("Transform.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("Transform.SUB_CLS_AUDIT_EFF_DT").alias("SUB_CLS_AUDIT_EFF_DT"),
    F.col("Transform.SUB_CLS_AUDIT_TERM_DT").alias("SUB_CLS_AUDIT_TERM_DT"),
    F.rpad(F.col("Transform.GRGR_ID"), 8, " ").alias("GRGR_ID")
)

df_updt_insert = df_enriched.filter(F.col("lkup.SUB_CLS_AUDIT_SK").isNull()).select(
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD_SK"),
    F.col("Transform.SUB_CLS_AUDIT_ROW_ID").alias("SUB_CLS_AUDIT_ROW_ID"),
    F.col("SUB_CLS_AUDIT_SK").alias("SUB_CLS_AUDIT_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK")
)

spark.sql("DROP TABLE IF EXISTS STAGING.FctsSubClsAuditExtr_hf_sub_cls_audit_temp")
(
    df_updt_insert.write.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("dbtable", "STAGING.FctsSubClsAuditExtr_hf_sub_cls_audit_temp")
    .mode("overwrite")
    .save()
)

merge_sql_hf_sub_cls_audit = (
    "MERGE dummy_hf_sub_cls_audit AS T "
    "USING STAGING.FctsSubClsAuditExtr_hf_sub_cls_audit_temp AS S "
    "ON T.SRC_SYS_CD_SK = S.SRC_SYS_CD_SK AND T.SUB_CLS_AUDIT_ROW_ID = S.SUB_CLS_AUDIT_ROW_ID "
    "WHEN NOT MATCHED THEN INSERT (SRC_SYS_CD_SK, SUB_CLS_AUDIT_ROW_ID, SUB_CLS_AUDIT_SK, CRT_RUN_CYC_EXCTN_SK) "
    "VALUES (S.SRC_SYS_CD_SK, S.SUB_CLS_AUDIT_ROW_ID, S.SUB_CLS_AUDIT_SK, S.CRT_RUN_CYC_EXCTN_SK);"
)

execute_dml(merge_sql_hf_sub_cls_audit, jdbc_url_ids, jdbc_props_ids)

file_path_IdsSubClsAuditExtr = f"{adls_path}/key/FctsSubClsAuditExtr.FctsSubClsAudit.dat.{RunID}"
write_files(
    df_key,
    file_path_IdsSubClsAuditExtr,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)