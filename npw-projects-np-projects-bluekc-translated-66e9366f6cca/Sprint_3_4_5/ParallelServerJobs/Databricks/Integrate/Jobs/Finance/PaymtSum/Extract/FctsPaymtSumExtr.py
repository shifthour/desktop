# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005, 2006, 2007, 2008  BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY:  FctsPaymtSumExtrSeq
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:
# MAGIC   Pulls data from CMC_CKPY_PAYEE_SUM  to a landing file for the IDS.    Last run date subtracted by three gives pull date.  Allows for replication problems and avoids having to store and process to and from dates, also simplifies the extract process. 
# MAGIC   
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC              Ralph Tucker    03/10/2004-   Originally Programmed
# MAGIC              Ralph Tucker    04/25/2006 -  Combined extr, trns, and pkey jobs.
# MAGIC              Ralph Tucker    01/03/2007 -  Added PAYMT_SUM_LOB_CD_SK to the natural key, changed Pkey.
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                                    Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                              ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              5/10/2007          3264                          Added Balancing process to the overall                         devlIDS30               Steph Goddard            09/15/2007
# MAGIC                                                                                                       job that takes a snapshot of the source data  
# MAGIC Brent Leland                    02-13-2008     3567 Primary Key           Changed primary key from hash file to DB2 table            devlIDScur               Steph Goddard           02/22/2008
# MAGIC                                                                                                      Changed Facets SQL to only pull using driver table                
# MAGIC Brent Leland                    07-21-2008     3567 Primary Key           Requirement modification to PAYMT_SUM_TYP_CD    devlIDS                    Steph Goddard           07/21/2008
# MAGIC                                                                                                       having space considered a valid code and not
# MAGIC                                                                                                       to be defaulted to NA.
# MAGIC 
# MAGIC Manasa Andru                08-08-2014      TFS - 9548                     Changed the default value in the CKPY_COMB_IND    IntegrteNewDevl       Kalyan Neelam                   2014-08-08
# MAGIC                                                                                                            to 'N' in the BusinessRules Transformer.
# MAGIC Prabhu ES                      02-24-2022      S2S Remediation            MSSQL connection parameters added                          IntegrateDev5	Harsha Ravuri	06-10-2022

# MAGIC Pulling Facets Payment Summary Data
# MAGIC Container hash file cleared by after-job subroutine
# MAGIC Strip Carriage Return, Line Feed, and  Tab
# MAGIC Apply business logic
# MAGIC Writing Sequential File to ../key
# MAGIC Balancing snapshot of source table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, when, length, regexp_replace, date_format, to_date, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------

jdbc_url_facets, jdbc_props_facets = None, None

TableTimeStamp = get_widget_value('TableTimeStamp','9999')
CurrRunCycle = get_widget_value('CurrRunCycle','100')
CurrDate = get_widget_value('CurrDate','2008-07-21')
SrcSysCdSk = get_widget_value('SrcSysCdSk','105838')
RunID = get_widget_value('RunID','9999')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
FacetsDB = get_widget_value('FacetsDB','')

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)

extract_query_CMC_CKPY_PAYEE_SUM = """SELECT 
P.CKPY_REF_ID,
P.CKPY_TYPE,
P.CKPY_PAY_DT,
P.LOBD_ID,
P.CKPY_PAYEE_PR_ID,
P.CKPY_PAYEE_CK,
P.CKPY_PAYEE_TYPE,
P.CKPY_PYMT_TYPE,
P.CKPY_COMB_IND,
P.CKPY_PER_END_DT,
P.CKPY_ORIG_AMT,
P.CKPY_DEDUCT_AMT,
P.CKPY_NET_AMT,
P.CKPY_CURR_CKCK_SEQ,
P.CKPY_LOCK_TOKEN,
P.ATXR_SOURCE_ID 
FROM """ + FacetsOwner + """.CMC_CKPY_PAYEE_SUM P,
     tempdb..TMP_IDS_PMTSUM H
WHERE P.CKPY_REF_ID = H.CKPY_REF_ID
"""

df_CMC_CKPY_PAYEE_SUM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_CMC_CKPY_PAYEE_SUM)
    .load()
)

df_StripField = df_CMC_CKPY_PAYEE_SUM.select(
    col("CKPY_REF_ID").alias("CKPY_REF_ID"),
    regexp_replace(col("CKPY_TYPE"), "[\n\r\t]", "").alias("CKPY_TYPE"),
    col("CKPY_PAY_DT").alias("CKPY_PAY_DT"),
    regexp_replace(col("LOBD_ID"), "[\n\r\t]", "").alias("LOBD_ID"),
    regexp_replace(col("CKPY_PAYEE_PR_ID"), "[\n\r\t]", "").alias("CKPY_PAYEE_PR_ID"),
    col("CKPY_PAYEE_CK").alias("CKPY_PAYEE_CK"),
    regexp_replace(col("CKPY_PAYEE_TYPE"), "[\n\r\t]", "").alias("CKPY_PAYEE_TYPE"),
    regexp_replace(col("CKPY_PYMT_TYPE"), "[\n\r\t]", "").alias("CKPY_PYMT_TYPE"),
    regexp_replace(col("CKPY_COMB_IND"), "[\n\r\t]", "").alias("CKPY_COMB_IND"),
    col("CKPY_PER_END_DT").alias("CKPY_PER_END_DT"),
    col("CKPY_ORIG_AMT").alias("CKPY_ORIG_AMT"),
    col("CKPY_DEDUCT_AMT").alias("CKPY_DEDUCT_AMT"),
    col("CKPY_NET_AMT").alias("CKPY_NET_AMT"),
    col("CKPY_CURR_CKCK_SEQ").alias("CKPY_CURR_CKCK_SEQ"),
    col("CKPY_LOCK_TOKEN").alias("CKPY_LOCK_TOKEN"),
    col("ATXR_SOURCE_ID").alias("ATXR_SOURCE_ID")
)

df_BusinessRules = (
    df_StripField
    .withColumn("RowPassThru", lit("Y"))
    .withColumn("svProv", when(length(trim(col("CKPY_PAYEE_PR_ID"))) == 0, lit("0")).otherwise(trim(col("CKPY_PAYEE_PR_ID"))))
    .withColumn("svProv2", GetFkeyProv("FACETS", lit(1), col("svProv"), lit("X")))
    .withColumn("svLobCdSk", GetFkeyCodes("FACETS", lit(1), lit("CLAIM LINE LOB"), trim(col("LOBD_ID")), lit("X")))
    .withColumn("svLobCd", trim(col("LOBD_ID")))
    .withColumn("svRefID", trim(col("CKPY_REF_ID")))
)

df_BusinessRules_AllCol = df_BusinessRules.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("svRefID").alias("CKPY_REF_ID"),
    col("svLobCd").alias("LOBD_ID"),
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit("I").alias("INSRT_UPDT_CD"),
    lit("N").alias("DISCARD_IN"),
    col("RowPassThru").alias("PASS_THRU_IN"),
    lit(CurrDate).alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    lit("FACETS").alias("SRC_SYS_CD"),
    (lit("FACETS")
     .concat(lit(";"))
     .concat(col("svRefID"))
     .concat(lit(";"))
     .concat(col("svLobCd"))).alias("PRI_KEY_STRING"),
    date_format(to_date(col("CKPY_PAY_DT"), "yyyy-MM-dd"), "yyyy-MM-dd").alias("CKPY_PAY_DT"),
    when(col("svProv2") == 0, "NA").otherwise(trim(col("CKPY_PAYEE_PR_ID"))).alias("CKPY_PAYEE_PR_ID"),
    when(length(trim(col("CKPY_PAYEE_TYPE"))) == 0, "NA").otherwise(trim(col("CKPY_PAYEE_TYPE"))).alias("CKPY_PAYEE_TYPE"),
    when(col("CKPY_PYMT_TYPE").isNull(), "NA")
    .otherwise(
        when(length(trim(col("CKPY_PYMT_TYPE"))) == 0, lit(" "))
        .otherwise(trim(col("CKPY_PYMT_TYPE")))
    ).alias("CKPY_PYMT_TYPE"),
    when(length(trim(col("CKPY_TYPE"))) == 0, "NA").otherwise(trim(col("CKPY_TYPE"))).alias("CKPY_TYPE"),
    when(length(trim(col("CKPY_COMB_IND"))) == 0, "N").otherwise(trim(col("CKPY_COMB_IND"))).alias("CKPY_COMB_IND"),
    date_format(to_date(col("CKPY_PER_END_DT"), "yyyy-MM-dd"), "yyyy-MM-dd").alias("CKPY_PER_END_DT"),
    col("CKPY_ORIG_AMT").alias("CKPY_ORIG_AMT"),
    col("CKPY_DEDUCT_AMT").alias("CKPY_DEDUCT_AMT"),
    col("CKPY_NET_AMT").alias("CKPY_NET_AMT"),
    col("CKPY_CURR_CKCK_SEQ").alias("CKPY_CURR_CKCK_SEQ"),
    col("svLobCdSk").alias("LOB_CD_SK")
)

df_BusinessRules_Transform = df_BusinessRules.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("svRefID").alias("PAYMT_REF_ID"),
    col("svLobCd").alias("PAYMT_SUM_LOB_CD")
)

# MAGIC %run ../../../../../shared_containers/PrimaryKey/PaymtSumPK
# COMMAND ----------

params_PaymtSumPK = {
    "TableTimeStamp": TableTimeStamp,
    "CurrRunCycle": CurrRunCycle,
    "CurrDate": CurrDate,
    "SrcSysCdSk": SrcSysCdSk,
    "RunID": RunID,
    "IDSOwner": IDSOwner,
    "FacetsDB": FacetsDB,
    "FacetsOwner": FacetsOwner
}

df_IdsPaymtSumPkey = PaymtSumPK(df_BusinessRules_Transform, df_BusinessRules_AllCol, params_PaymtSumPK)

df_final_IdsPaymtSumPkey = df_IdsPaymtSumPkey.select(
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    rpad(col("CKPY_PAYEE_PR_ID"), 12, " ").alias("CKPY_PAYEE_PR_ID"),
    col("PAYMT_SUM_SK"),
    col("PAYMT_SUM_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("LOBD_ID"), 4, " ").alias("LOBD_ID"),
    rpad(col("CKPY_PAYEE_TYPE"), 10, " ").alias("CKPY_PAYEE_TYPE"),
    rpad(col("CKPY_PYMT_TYPE"), 10, " ").alias("CKPY_PYMT_TYPE"),
    rpad(col("CKPY_TYPE"), 2, " ").alias("CKPY_TYPE"),
    rpad(col("CKPY_COMB_IND"), 1, " ").alias("CKPY_COMB_IND"),
    rpad(col("CKPY_PAY_DT"), 10, " ").alias("CKPY_PAY_DT"),
    rpad(col("CKPY_PER_END_DT"), 10, " ").alias("CKPY_PER_END_DT"),
    col("CKPY_DEDUCT_AMT"),
    col("CKPY_NET_AMT"),
    col("CKPY_ORIG_AMT"),
    col("CKPY_CURR_CKCK_SEQ")
)

write_files(
    df_final_IdsPaymtSumPkey,
    f"{adls_path}/key/IdsPaymtSumPkey.PaymtSum.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

extract_query_Facets_Source = """SELECT 
P.CKPY_REF_ID,
P.LOBD_ID,
P.CKPY_NET_AMT
FROM """ + FacetsOwner + """.CMC_CKPY_PAYEE_SUM P,
     tempdb..TMP_IDS_PMTSUM H
WHERE P.CKPY_REF_ID = H.CKPY_REF_ID
"""

df_Facets_Source = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_Facets_Source)
    .load()
)

df_Transform = (
    df_Facets_Source
    .withColumn("svPaymtSumLobCdSk", GetFkeyCodes("FACETS", lit(100), lit("CLAIM LINE LOB"), trim(col("LOBD_ID")), lit("X")))
)

df_Snapshot_File = df_Transform.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    regexp_replace(trim(col("CKPY_REF_ID")), "[\n\r\t]", "").alias("PAYMT_REF_ID"),
    col("svPaymtSumLobCdSk").alias("PAYMT_SUM_LOB_CD_SK"),
    col("CKPY_NET_AMT").alias("NET_AMT")
)

write_files(
    df_Snapshot_File,
    f"{adls_path}/load/B_PAYMT_SUM.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)