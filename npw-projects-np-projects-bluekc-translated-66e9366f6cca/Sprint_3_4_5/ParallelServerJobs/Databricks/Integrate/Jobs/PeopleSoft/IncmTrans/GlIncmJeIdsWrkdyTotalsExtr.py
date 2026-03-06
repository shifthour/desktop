# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC JOB NAME:  GlIncmJeIdsWrkdyTotalsExtr
# MAGIC Called by : GLJeIdsWrkdyTotalsExtrSeq
# MAGIC 
# MAGIC 
# MAGIC Process Description: This job will extract the monthly GL Income detail transactions, join to the IDS crosswalk to assign the correct FNCL LOB and load to JRNL_ENTRY totals output file that will be the source                                   of the Finance monthly Journal Entry total report that will be used for balancing
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                               Project/                                                                                                                        Code                  Date
# MAGIC Developer                    Date                   Altiris #     Change Description                                                                                     Reviewer            Reviewed
# MAGIC -----------------------              -------------------    -------------   -----------------------------------------------------------------------------------------------------------------   -------------------------  -------------------
# MAGIC Tim Sieg                         05/30/2018       5879          Original Programming                                                                             Kalyan Neelam   2018-06-12
# MAGIC Prabhu ES                     2022-03-14         S2S           MSSQL ODBC conn params added - IntegrateDev5	Ken Bradmon	2022-06-13

# MAGIC This extract is required for Finance to balance the results of the Income JE process that extracts data from SYBASE and IDS and creates the monthly Workday JE file
# MAGIC Extract current GL Income JE details
# MAGIC Extract current list of LOB codes
# MAGIC Create GL Journal Entries using Final LOB from IDS crosswalk
# MAGIC Aggregate all GL Journal Entries to one line for each GL account transaction
# MAGIC Create GL Journal entries with Debit and Credit amounts
# MAGIC Summarized GL Journal Entry file to be source of Crystal reprot for Finance
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
BCBSFINOwner = get_widget_value('BCBSFINOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
bcbsfin_secret_name = get_widget_value('bcbsfin_secret_name','')
Trans_JeNme = get_widget_value('Trans_JeNme','')
Environment = get_widget_value('Environment','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_ids = f"""
SELECT A.LGCY_FNCL_LOB_CD AS LGCY_FNCL_LOB_CD,
       A.GL_FNCL_LOB_CD AS GL_FNCL_LOB_CD,
       SUBSTR(A.GL_FNCL_LOB_CD,5,4) AS FINALLOB
FROM {IDSOwner}.JRNL_ENTRY_TRANS_CRSWALK A,
     (
       SELECT SRC_SYS_CD,
              LGCY_FNCL_LOB_CD,
              MAX(JRNL_ENTRY_TRANS_CRSWALK_SK) AS MXSK,
              MAX(LAST_UPDT_RUN_CYC_EXCTN_SK) AS MXUPDTSK
       FROM {IDSOwner}.JRNL_ENTRY_TRANS_CRSWALK
       WHERE FNCL_LOB_CD NOT IN ('UNK', 'NA')
       GROUP BY SRC_SYS_CD,
                LGCY_FNCL_LOB_CD
     ) MX
WHERE A.JRNL_ENTRY_TRANS_CRSWALK_SK = MX.MXSK
  AND A.LAST_UPDT_RUN_CYC_EXCTN_SK = MX.MXUPDTSK
"""
df_DB2_JRNL_ENTRY_TRANS_CRSWALK = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_ids)
    .load()
)

jdbc_url_bcbsfin, jdbc_props_bcbsfin = get_db_config(bcbsfin_secret_name)
extract_query_bcbsfin = f"""
SELECT BUSINESS_UNIT_GL,
       TRANSACTION_LINE,
       ACCOUNTING_DT,
       WD_ACCT_NO,
       CASE WHEN DR_CR_CD = 'CR' THEN CAST((MONETARY_AMOUNT * -1) AS DECIMAL(19,2))
            ELSE CAST(MONETARY_AMOUNT AS DECIMAL(19,2))
       END AS GL_AMT,
       WD_CC_ID,
       WD_RVNU_CAT_ID,
       WD_SPEND_CAT_ID,
       WD_AFFIL_ID,
       WD_CUST_ID,
       MAP_IN,
       APPL_JRNL_ID,
       CASE WHEN JRNL_LN_REF = ' ' THEN '0000'
            ELSE SUBSTRING(JRNL_LN_REF,7,4)
       END AS GL_DTL_LOB
FROM {BCBSFINOwner}.GL_INCOME_DTL_BCP
WHERE MAP_IN = 'Y'
  AND MONETARY_AMOUNT <> 0.00
"""
df_Src_GL_INCOME_DTL_BCP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bcbsfin)
    .options(**jdbc_props_bcbsfin)
    .option("query", extract_query_bcbsfin)
    .load()
)

df_JeCrrntLob = (
    df_Src_GL_INCOME_DTL_BCP.alias("GlJeLines")
    .join(
        df_DB2_JRNL_ENTRY_TRANS_CRSWALK.alias("IdsLobCds"),
        F.col("GlJeLines.GL_DTL_LOB") == F.col("IdsLobCds.LGCY_FNCL_LOB_CD"),
        "left"
    )
    .select(
        F.rpad(F.col("GlJeLines.BUSINESS_UNIT_GL"), 5, " ").alias("BUSINESS_UNIT_GL"),
        F.col("GlJeLines.TRANSACTION_LINE").alias("TRANSACTION_LINE"),
        F.col("GlJeLines.ACCOUNTING_DT").alias("ACCOUNTING_DT"),
        F.col("GlJeLines.WD_ACCT_NO").alias("WD_ACCT_NO"),
        F.col("GlJeLines.GL_AMT").alias("GL_AMT"),
        F.col("GlJeLines.WD_CC_ID").alias("WD_CC_ID"),
        F.col("GlJeLines.WD_RVNU_CAT_ID").alias("WD_RVNU_CAT_ID"),
        F.col("GlJeLines.WD_SPEND_CAT_ID").alias("WD_SPEND_CAT_ID"),
        F.col("GlJeLines.WD_AFFIL_ID").alias("WD_AFFIL_ID"),
        F.col("GlJeLines.WD_CUST_ID").alias("WD_CUST_ID"),
        F.rpad(F.col("GlJeLines.MAP_IN"), 1, " ").alias("MAP_IN"),
        F.rpad(F.col("GlJeLines.APPL_JRNL_ID"), 10, " ").alias("APPL_JRNL_ID"),
        F.rpad(F.col("GlJeLines.GL_DTL_LOB"), 10, " ").alias("GL_DTL_LOB"),
        F.rpad(F.col("IdsLobCds.FINALLOB"), 4, " ").alias("FINALLOB")
    )
)

df_GlJeLob = df_JeCrrntLob.select(
    F.rpad(F.col("BUSINESS_UNIT_GL"), 5, " ").alias("BUSINESS_UNIT_GL"),
    F.col("TRANSACTION_LINE").alias("TRANSACTION_LINE"),
    F.col("ACCOUNTING_DT").alias("ACCOUNTING_DT"),
    F.col("WD_ACCT_NO").alias("WD_ACCT_NO"),
    F.col("GL_AMT").alias("GL_AMT"),
    F.col("WD_CC_ID").alias("WD_CC_ID"),
    F.col("WD_RVNU_CAT_ID").alias("WD_RVNU_CAT_ID"),
    F.col("WD_SPEND_CAT_ID").alias("WD_SPEND_CAT_ID"),
    F.col("WD_AFFIL_ID").alias("WD_AFFIL_ID"),
    F.col("WD_CUST_ID").alias("WD_CUST_ID"),
    F.rpad(F.col("MAP_IN"), 1, " ").alias("MAP_IN"),
    F.rpad(F.col("APPL_JRNL_ID"), 10, " ").alias("APPL_JRNL_ID"),
    F.rpad(
        F.when(F.length(F.col("FINALLOB")) == 0, trim(F.col("GL_DTL_LOB")))
         .otherwise(trim(F.col("FINALLOB"))),
        4,
        " "
    ).alias("FINALLOB")
)

df_JeLnSum_agg = df_GlJeLob.groupBy(
    "BUSINESS_UNIT_GL",
    "ACCOUNTING_DT",
    "WD_ACCT_NO",
    "WD_CC_ID",
    "WD_RVNU_CAT_ID",
    "WD_SPEND_CAT_ID",
    "WD_AFFIL_ID",
    "WD_CUST_ID",
    "FINALLOB",
    "APPL_JRNL_ID"
).agg(F.sum(F.col("GL_AMT")).alias("GL_AMT_SUM"))

df_JeLnSum = df_JeLnSum_agg.select(
    F.rpad(F.col("BUSINESS_UNIT_GL"), 5, " ").alias("BUSINESS_UNIT_GL"),
    F.col("ACCOUNTING_DT").alias("ACCOUNTING_DT"),
    F.col("WD_ACCT_NO").alias("WD_ACCT_NO"),
    F.col("WD_CC_ID").alias("WD_CC_ID"),
    F.col("WD_RVNU_CAT_ID").alias("WD_RVNU_CAT_ID"),
    F.col("WD_SPEND_CAT_ID").alias("WD_SPEND_CAT_ID"),
    F.col("WD_AFFIL_ID").alias("WD_AFFIL_ID"),
    F.col("WD_CUST_ID").alias("WD_CUST_ID"),
    F.rpad(F.col("FINALLOB"), 20, " ").alias("FINALLOB"),
    F.rpad(F.col("APPL_JRNL_ID"), 10, " ").alias("APPL_JRNL_ID"),
    F.col("GL_AMT_SUM").alias("GL_AMT_SUM")
)

df_FnlJeTtl_pre = df_JeLnSum.filter(F.col("GL_AMT_SUM") != 0.0)

df_FnlJeTtl = df_FnlJeTtl_pre.select(
    F.rpad(F.col("BUSINESS_UNIT_GL"), 5, " ").alias("BUSINESS_UNIT_GL"),
    F.col("ACCOUNTING_DT").alias("ACCOUNTING_DT"),
    F.when(F.col("WD_ACCT_NO").isNull(), " ").otherwise(F.col("WD_ACCT_NO")).alias("WD_ACCT_NO"),
    F.when(F.col("WD_CC_ID").isNull(), " ").otherwise(F.col("WD_CC_ID")).alias("WD_CC_ID"),
    F.when(F.col("WD_RVNU_CAT_ID").isNull(), " ").otherwise(F.col("WD_RVNU_CAT_ID")).alias("WD_RVNU_CAT_ID"),
    F.when(F.col("WD_SPEND_CAT_ID").isNull(), " ").otherwise(F.col("WD_SPEND_CAT_ID")).alias("WD_SPEND_CAT_ID"),
    F.when(F.col("WD_AFFIL_ID").isNull(), " ").otherwise(F.col("WD_AFFIL_ID")).alias("WD_AFFIL_ID"),
    F.when(F.col("WD_CUST_ID").isNull(), " ").otherwise(F.col("WD_CUST_ID")).alias("WD_CUST_ID"),
    F.rpad(F.col("FINALLOB"), 20, " ").alias("FINALLOB"),
    F.rpad(F.col("APPL_JRNL_ID"), 10, " ").alias("APPL_JRNL_ID"),
    F.when(F.col("GL_AMT_SUM").isNull(), 0.0)
     .when(F.col("GL_AMT_SUM") > 0.0, F.col("GL_AMT_SUM"))
     .otherwise(F.lit(0.0))
     .alias("GL_AMT_SUM_DR"),
    F.when(F.col("GL_AMT_SUM").isNull(), 0.0)
     .when(F.col("GL_AMT_SUM") < 0.0, F.col("GL_AMT_SUM"))
     .otherwise(F.lit(0.0))
     .alias("GL_AMT_SUM_CR")
)

final_file_path = f"{adls_path_publish}/{Environment}_JRNL_ENTRY_{Trans_JeNme}_TOTALS.txt"
write_files(
    df_FnlJeTtl,
    final_file_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)