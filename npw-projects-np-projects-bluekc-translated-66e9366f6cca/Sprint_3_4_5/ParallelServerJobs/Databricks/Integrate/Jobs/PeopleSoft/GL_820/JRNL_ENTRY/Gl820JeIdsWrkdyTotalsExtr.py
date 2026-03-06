# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC JOB NAME:  Gl820JeIdsWrkdyTotalsExtr
# MAGIC Called by : GLJeIdsWrkdyTotalsExtrSeq
# MAGIC 
# MAGIC 
# MAGIC Process Description: This job will extract the monthly GL 820 detail transactions, join to the IDS crosswalk to assign the correct FNCL LOB and load to JRNL_ENTRY totals output file that will be the source                                  
# MAGIC of the Finance monthly Journal Entry total report that will be used for balancing
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                               Project/                                                                                                                        Code                  Date
# MAGIC Developer                    Date                   Altiris #     Change Description                                                                                     Reviewer            Reviewed
# MAGIC -----------------------              -------------------    -------------   -----------------------------------------------------------------------------------------------------------------   -------------------------  -------------------
# MAGIC Tim Sieg                         05/30/2018       5879          Original Programming                                                                             Kalyan Neelam   2018-06-06
# MAGIC Prabhu ES                      2022-03-14        S2S            MSSQL ODBC conn params added - IntegrateDev5	Ken Bradmon	2022-06-13

# MAGIC This extract is required for Finance to balance the results of the 820 JE process that extracts data from SYBASE and IDS and creates the monthly Workday JE file
# MAGIC Extract current GL 820 JE details
# MAGIC Extract current list of LOB codes
# MAGIC Create GL Journal Entries using Final LOB from IDS crosswalk
# MAGIC Aggregate all GL Journal Entries to one line for each GL account transaction
# MAGIC Create GL Journal entries with Debit and Credit amounts
# MAGIC Summarized GL Journal Entry file to be source of Crystal reprot for Finance
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import *
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
BCBSFINOwner = get_widget_value('BCBSFINOwner','')
bcbsfin_secret_name = get_widget_value('bcbsfin_secret_name','')
EndDt = get_widget_value('EndDt','')
Trans_JeNme = get_widget_value('Trans_JeNme','')
Environment = get_widget_value('Environment','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_ids = f"""SELECT A.LGCY_FNCL_LOB_CD  AS LGCY_FNCL_LOB_CD,
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
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_ids)
    .load()
)
df_IdsLobCds = df_DB2_JRNL_ENTRY_TRANS_CRSWALK

jdbc_url_bcbs, jdbc_props_bcbs = get_db_config(bcbsfin_secret_name)
extract_query_bcbs = f"""SELECT BUSINESS_UNIT_GL,
       TRANSACTION_LINE,
       ACCOUNTING_DT,
       WD_ACCT_NO,
       CASE WHEN DR_CR_CD = 'CR' THEN CAST((MONETARY_AMOUNT*-1) AS DECIMAL(19,2))
            ELSE CAST(MONETARY_AMOUNT AS DECIMAL(19,2)) END AS GL_AMT,
       WD_CC_ID,
       WD_RVNU_CAT_ID,
       WD_SPEND_CAT_ID,
       WD_AFFIL_ID,
       WD_CUST_ID,
       MAP_IN,
       APPL_JRNL_ID,
       CASE WHEN JRNL_LN_REF = ' ' THEN '0000'
            ELSE SUBSTRING(JRNL_LN_REF,7,4) END AS GL_DTL_LOB
  FROM {BCBSFINOwner}.GL_ON_EXCH_FED_PYMT_DTL
 WHERE SNAP_ACT_DT = '{EndDt}'
   AND MAP_IN = 'Y'
   AND MONETARY_AMOUNT <> 0.00
"""
df_Src_GL_ON_EXCH_FED_PYMT_DTL = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bcbs)
    .options(**jdbc_props_bcbs)
    .option("query", extract_query_bcbs)
    .load()
)
df_GlJeLines = df_Src_GL_ON_EXCH_FED_PYMT_DTL

df_JeCrrntLob = (
    df_GlJeLines.alias("GlJeLines")
    .join(
        df_IdsLobCds.alias("IdsLobCds"),
        F.col("GlJeLines.GL_DTL_LOB") == F.col("IdsLobCds.LGCY_FNCL_LOB_CD"),
        "left",
    )
    .select(
        F.col("GlJeLines.BUSINESS_UNIT_GL").alias("BUSINESS_UNIT_GL"),
        F.col("GlJeLines.TRANSACTION_LINE").alias("TRANSACTION_LINE"),
        F.col("GlJeLines.ACCOUNTING_DT").alias("ACCOUNTING_DT"),
        F.col("GlJeLines.WD_ACCT_NO").alias("WD_ACCT_NO"),
        F.col("GlJeLines.GL_AMT").alias("GL_AMT"),
        F.col("GlJeLines.WD_CC_ID").alias("WD_CC_ID"),
        F.col("GlJeLines.WD_RVNU_CAT_ID").alias("WD_RVNU_CAT_ID"),
        F.col("GlJeLines.WD_SPEND_CAT_ID").alias("WD_SPEND_CAT_ID"),
        F.col("GlJeLines.WD_AFFIL_ID").alias("WD_AFFIL_ID"),
        F.col("GlJeLines.WD_CUST_ID").alias("WD_CUST_ID"),
        F.col("GlJeLines.MAP_IN").alias("MAP_IN"),
        F.col("GlJeLines.APPL_JRNL_ID").alias("APPL_JRNL_ID"),
        F.col("GlJeLines.GL_DTL_LOB").alias("GL_DTL_LOB"),
        F.col("IdsLobCds.FINALLOB").alias("FINALLOB"),
    )
)

df_GlJeLob = df_JeCrrntLob.alias("JeCrrntLob").select(
    F.col("JeCrrntLob.BUSINESS_UNIT_GL").alias("BUSINESS_UNIT_GL"),
    F.col("JeCrrntLob.TRANSACTION_LINE").alias("TRANSACTION_LINE"),
    F.col("JeCrrntLob.ACCOUNTING_DT").alias("ACCOUNTING_DT"),
    F.col("JeCrrntLob.WD_ACCT_NO").alias("WD_ACCT_NO"),
    F.col("JeCrrntLob.GL_AMT").alias("GL_AMT"),
    F.col("JeCrrntLob.WD_CC_ID").alias("WD_CC_ID"),
    F.col("JeCrrntLob.WD_RVNU_CAT_ID").alias("WD_RVNU_CAT_ID"),
    F.col("JeCrrntLob.WD_SPEND_CAT_ID").alias("WD_SPEND_CAT_ID"),
    F.col("JeCrrntLob.WD_AFFIL_ID").alias("WD_AFFIL_ID"),
    F.col("JeCrrntLob.WD_CUST_ID").alias("WD_CUST_ID"),
    F.col("JeCrrntLob.MAP_IN").alias("MAP_IN"),
    F.col("JeCrrntLob.APPL_JRNL_ID").alias("APPL_JRNL_ID"),
    F.when(
        F.length(F.col("JeCrrntLob.FINALLOB")) == 0,
        trim(F.col("JeCrrntLob.GL_DTL_LOB"))
    )
    .otherwise(trim(F.col("JeCrrntLob.FINALLOB")))
    .alias("FINALLOB"),
)

df_JeLnSum = (
    df_GlJeLob.groupBy(
        "BUSINESS_UNIT_GL",
        "ACCOUNTING_DT",
        "WD_ACCT_NO",
        "WD_CC_ID",
        "WD_RVNU_CAT_ID",
        "WD_SPEND_CAT_ID",
        "WD_AFFIL_ID",
        "WD_CUST_ID",
        "FINALLOB",
        "APPL_JRNL_ID",
    )
    .agg(F.sum("GL_AMT").alias("GL_AMT_SUM"))
    .select(
        F.col("BUSINESS_UNIT_GL"),
        F.col("ACCOUNTING_DT"),
        F.col("WD_ACCT_NO"),
        F.col("WD_CC_ID"),
        F.col("WD_RVNU_CAT_ID"),
        F.col("WD_SPEND_CAT_ID"),
        F.col("WD_AFFIL_ID"),
        F.col("WD_CUST_ID"),
        F.col("FINALLOB"),
        F.col("APPL_JRNL_ID"),
        F.col("GL_AMT_SUM"),
    )
)

df_FnlJeTtl = (
    df_JeLnSum.filter(F.col("GL_AMT_SUM") != 0.00)
    .select(
        F.col("BUSINESS_UNIT_GL").alias("BUSINESS_UNIT_GL"),
        F.col("ACCOUNTING_DT").alias("ACCOUNTING_DT"),
        F.when(F.col("WD_ACCT_NO").isNull(), F.lit(" ")).otherwise(F.col("WD_ACCT_NO")).alias("WD_ACCT_NO"),
        F.when(F.col("WD_CC_ID").isNull(), F.lit(" ")).otherwise(F.col("WD_CC_ID")).alias("WD_CC_ID"),
        F.when(F.col("WD_RVNU_CAT_ID").isNull(), F.lit(" ")).otherwise(F.col("WD_RVNU_CAT_ID")).alias("WD_RVNU_CAT_ID"),
        F.when(F.col("WD_SPEND_CAT_ID").isNull(), F.lit(" ")).otherwise(F.col("WD_SPEND_CAT_ID")).alias("WD_SPEND_CAT_ID"),
        F.when(F.col("WD_AFFIL_ID").isNull(), F.lit(" ")).otherwise(F.col("WD_AFFIL_ID")).alias("WD_AFFIL_ID"),
        F.when(F.col("WD_CUST_ID").isNull(), F.lit(" ")).otherwise(F.col("WD_CUST_ID")).alias("WD_CUST_ID"),
        F.col("FINALLOB").alias("FINALLOB"),
        F.col("APPL_JRNL_ID").alias("APPL_JRNL_ID"),
        F.when(F.col("GL_AMT_SUM").isNull(), F.lit(0.00))
         .otherwise(F.when(F.col("GL_AMT_SUM") > 0.00, F.col("GL_AMT_SUM")).otherwise(F.lit(0.00)))
         .alias("GL_AMT_SUM_DR"),
        F.when(F.col("GL_AMT_SUM").isNull(), F.lit(0.00))
         .otherwise(F.when(F.col("GL_AMT_SUM") < 0.00, F.col("GL_AMT_SUM")).otherwise(F.lit(0.00)))
         .alias("GL_AMT_SUM_CR"),
    )
)

df_final = df_FnlJeTtl.select(
    rpad(F.col("BUSINESS_UNIT_GL"), 5, " ").alias("BUSINESS_UNIT_GL"),
    F.col("ACCOUNTING_DT").alias("ACCOUNTING_DT"),
    F.col("WD_ACCT_NO").alias("WD_ACCT_NO"),
    F.col("WD_CC_ID").alias("WD_CC_ID"),
    F.col("WD_RVNU_CAT_ID").alias("WD_RVNU_CAT_ID"),
    F.col("WD_SPEND_CAT_ID").alias("WD_SPEND_CAT_ID"),
    F.col("WD_AFFIL_ID").alias("WD_AFFIL_ID"),
    F.col("WD_CUST_ID").alias("WD_CUST_ID"),
    rpad(F.col("FINALLOB"), 20, " ").alias("FINALLOB"),
    rpad(F.col("APPL_JRNL_ID"), 10, " ").alias("APPL_JRNL_ID"),
    F.col("GL_AMT_SUM_DR").alias("GL_AMT_SUM_DR"),
    F.col("GL_AMT_SUM_CR").alias("GL_AMT_SUM_CR"),
)

write_files(
    df_final,
    f"{adls_path_publish}/external/{Environment}_JRNL_ENTRY_{Trans_JeNme}_TOTALS.txt",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=True,
    quote="\"",
    nullValue=None
)