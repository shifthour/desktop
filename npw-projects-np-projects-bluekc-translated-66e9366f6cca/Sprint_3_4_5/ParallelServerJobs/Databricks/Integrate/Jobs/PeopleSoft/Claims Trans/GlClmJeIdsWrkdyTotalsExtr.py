# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC JOB NAME:  GlClmJeIdsWrkdyTotalsExtr
# MAGIC Called by : GLJeIdsWrkdyTotalsExtrSeq
# MAGIC 
# MAGIC 
# MAGIC Process Description: This job will extract the monthly GL Claim detail transactions, join to the IDS crosswalk to assign the correct FNCL LOB and load to JRNL_ENTRY totals output file that will be the source                                   of the Finance monthly Journal Entry total report that will be used for balancing
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                               Project/                                                                                                                                                 Code                  Date
# MAGIC Developer                    Date                   Altiris #     Change Description                                                                                                             Reviewer            Reviewed
# MAGIC -----------------------              -------------------    -------------   -----------------------------------------------------------------------------------------------------------------                               -------------------------  -------------------
# MAGIC Tim Sieg                         05/30/2018       5879          Original Programming                                                                                                         Kalyan Neelam    2018-06-06
# MAGIC Prabhu ES                     2022-03-14         S2S           MSSQL ODBC conn params added                                                           IntegrateDev5  Jeyaprasanna     2022-06-11
# MAGIC Tim Sieg                        2022-03-29       S2S             Updating GL_DTL_LOB type to Varchar in Src_GL_CLAIM_DTL_BCP    IntegrateDev5  Jeyaprasanna     2022-06-11

# MAGIC This extract is required for Finance to balance the results of the Claim JE process that extracts data from SYBASE and IDS and creates the monthly Workday JE file
# MAGIC Extract current GL Claim JE details
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
from pyspark.sql.functions import col, when, lit, length, sum as sum_, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


BCBSFINOwner = get_widget_value('BCBSFINOwner','')
bcbsfin_secret_name = get_widget_value('bcbsfin_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
Trans_JeNme = get_widget_value('Trans_JeNme','')
Environment = get_widget_value('Environment','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_DB2_JRNL_ENTRY_TRANS_CRSWALK = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"SELECT A.LGCY_FNCL_LOB_CD AS LGCY_FNCL_LOB_CD, A.GL_FNCL_LOB_CD AS GL_FNCL_LOB_CD, SUBSTR(A.GL_FNCL_LOB_CD,5,4) AS FINALLOB "
        f"FROM {IDSOwner}.JRNL_ENTRY_TRANS_CRSWALK A, "
        f"(SELECT SRC_SYS_CD, LGCY_FNCL_LOB_CD, MAX(JRNL_ENTRY_TRANS_CRSWALK_SK) AS MXSK, MAX(LAST_UPDT_RUN_CYC_EXCTN_SK) AS MXUPDTSK "
        f"FROM {IDSOwner}.JRNL_ENTRY_TRANS_CRSWALK "
        f"WHERE FNCL_LOB_CD NOT IN ('UNK', 'NA') "
        f"GROUP BY SRC_SYS_CD, LGCY_FNCL_LOB_CD) MX "
        f"WHERE A.JRNL_ENTRY_TRANS_CRSWALK_SK = MX.MXSK "
        f"AND A.LAST_UPDT_RUN_CYC_EXCTN_SK = MX.MXUPDTSK"
    )
    .load()
)

jdbc_url_bcbsfin, jdbc_props_bcbsfin = get_db_config(bcbsfin_secret_name)
df_Src_GL_CLAIM_DTL_BCP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bcbsfin)
    .options(**jdbc_props_bcbsfin)
    .option(
        "query",
        f"SELECT BUSINESS_UNIT_GL, TRANSACTION_LINE, ACCOUNTING_DT, WD_ACCT_NO, "
        f"CASE WHEN DR_CR_CD = 'CR' THEN CAST((MONETARY_AMOUNT*-1) AS DECIMAL(19,2)) ELSE CAST(MONETARY_AMOUNT AS DECIMAL(19,2)) END AS GL_AMT, "
        f"WD_CC_ID, WD_RVNU_CAT_ID, WD_SPEND_CAT_ID, WD_AFFIL_ID, WD_CUST_ID, MAP_IN, APPL_JRNL_ID, "
        f"CASE WHEN JRNL_LN_REF = ' ' THEN '0000' ELSE SUBSTRING(JRNL_LN_REF,7,4) END AS GL_DTL_LOB "
        f"FROM {BCBSFINOwner}.GL_CLAIM_DTL_BCP "
        f"WHERE MAP_IN = 'Y' "
        f"AND MONETARY_AMOUNT <> 0.00 "
        f"AND SNAP_SRC_CD <> 'NDB'"
    )
    .load()
)

df_Lookup_IDS_LOB_CD = (
    df_Src_GL_CLAIM_DTL_BCP.alias("gl")
    .join(
        df_DB2_JRNL_ENTRY_TRANS_CRSWALK.alias("lob"),
        on=[col("gl.GL_DTL_LOB") == col("lob.LGCY_FNCL_LOB_CD")],
        how="left"
    )
)

df_JeCrrntLob = df_Lookup_IDS_LOB_CD.select(
    col("gl.BUSINESS_UNIT_GL").alias("BUSINESS_UNIT_GL"),
    col("gl.TRANSACTION_LINE").alias("TRANSACTION_LINE"),
    col("gl.ACCOUNTING_DT").alias("ACCOUNTING_DT"),
    col("gl.WD_ACCT_NO").alias("WD_ACCT_NO"),
    col("gl.GL_AMT").alias("GL_AMT"),
    col("gl.WD_CC_ID").alias("WD_CC_ID"),
    col("gl.WD_RVNU_CAT_ID").alias("WD_RVNU_CAT_ID"),
    col("gl.WD_SPEND_CAT_ID").alias("WD_SPEND_CAT_ID"),
    col("gl.WD_AFFIL_ID").alias("WD_AFFIL_ID"),
    col("gl.WD_CUST_ID").alias("WD_CUST_ID"),
    col("gl.MAP_IN").alias("MAP_IN"),
    col("gl.APPL_JRNL_ID").alias("APPL_JRNL_ID"),
    col("gl.GL_DTL_LOB").alias("GL_DTL_LOB"),
    col("lob.FINALLOB").alias("FINALLOB")
)

df_Trns_GL_JE_CURNT_LOB = df_JeCrrntLob.select(
    col("BUSINESS_UNIT_GL").alias("BUSINESS_UNIT_GL"),
    col("TRANSACTION_LINE").alias("TRANSACTION_LINE"),
    col("ACCOUNTING_DT").alias("ACCOUNTING_DT"),
    col("WD_ACCT_NO").alias("WD_ACCT_NO"),
    col("GL_AMT").alias("GL_AMT"),
    col("WD_CC_ID").alias("WD_CC_ID"),
    col("WD_RVNU_CAT_ID").alias("WD_RVNU_CAT_ID"),
    col("WD_SPEND_CAT_ID").alias("WD_SPEND_CAT_ID"),
    col("WD_AFFIL_ID").alias("WD_AFFIL_ID"),
    col("WD_CUST_ID").alias("WD_CUST_ID"),
    col("MAP_IN").alias("MAP_IN"),
    col("APPL_JRNL_ID").alias("APPL_JRNL_ID"),
    when(length(col("FINALLOB")) == 0, trim(col("GL_DTL_LOB"))).otherwise(trim(col("FINALLOB"))).alias("FINALLOB")
)

df_Aggr_GL_JE_LINE = (
    df_Trns_GL_JE_CURNT_LOB.groupBy(
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
    )
    .agg(sum_(col("GL_AMT")).alias("GL_AMT_SUM"))
    .select(
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
        "GL_AMT_SUM"
    )
)

df_Trns_GL_JE_LINE_DRCR_AMTS = (
    df_Aggr_GL_JE_LINE.filter(col("GL_AMT_SUM") != 0.0)
    .select(
        col("BUSINESS_UNIT_GL").alias("BUSINESS_UNIT_GL"),
        col("ACCOUNTING_DT").alias("ACCOUNTING_DT"),
        when(col("WD_ACCT_NO").isNull(), lit(" ")).otherwise(col("WD_ACCT_NO")).alias("WD_ACCT_NO"),
        when(col("WD_CC_ID").isNull(), lit(" ")).otherwise(col("WD_CC_ID")).alias("WD_CC_ID"),
        when(col("WD_RVNU_CAT_ID").isNull(), lit(" ")).otherwise(col("WD_RVNU_CAT_ID")).alias("WD_RVNU_CAT_ID"),
        when(col("WD_SPEND_CAT_ID").isNull(), lit(" ")).otherwise(col("WD_SPEND_CAT_ID")).alias("WD_SPEND_CAT_ID"),
        when(col("WD_AFFIL_ID").isNull(), lit(" ")).otherwise(col("WD_AFFIL_ID")).alias("WD_AFFIL_ID"),
        when(col("WD_CUST_ID").isNull(), lit(" ")).otherwise(col("WD_CUST_ID")).alias("WD_CUST_ID"),
        col("FINALLOB").alias("FINALLOB"),
        col("APPL_JRNL_ID").alias("APPL_JRNL_ID"),
        when(col("GL_AMT_SUM").isNull(), lit(0.0))
        .when(col("GL_AMT_SUM") > 0, col("GL_AMT_SUM"))
        .otherwise(lit(0.0))
        .alias("GL_AMT_SUM_DR"),
        when(col("GL_AMT_SUM").isNull(), lit(0.0))
        .when(col("GL_AMT_SUM") < 0, col("GL_AMT_SUM"))
        .otherwise(lit(0.0))
        .alias("GL_AMT_SUM_CR")
    )
)

df_final = df_Trns_GL_JE_LINE_DRCR_AMTS.select(
    rpad(col("BUSINESS_UNIT_GL"), 5, " ").alias("BUSINESS_UNIT_GL"),
    col("ACCOUNTING_DT").alias("ACCOUNTING_DT"),
    col("WD_ACCT_NO").alias("WD_ACCT_NO"),
    col("WD_CC_ID").alias("WD_CC_ID"),
    col("WD_RVNU_CAT_ID").alias("WD_RVNU_CAT_ID"),
    col("WD_SPEND_CAT_ID").alias("WD_SPEND_CAT_ID"),
    col("WD_AFFIL_ID").alias("WD_AFFIL_ID"),
    col("WD_CUST_ID").alias("WD_CUST_ID"),
    rpad(col("FINALLOB"), 20, " ").alias("FINALLOB"),
    rpad(col("APPL_JRNL_ID"), 10, " ").alias("APPL_JRNL_ID"),
    col("GL_AMT_SUM_DR").alias("GL_AMT_SUM_DR"),
    col("GL_AMT_SUM_CR").alias("GL_AMT_SUM_CR")
)

write_files(
    df_final,
    f"{adls_path_publish}/external/{Environment}_JRNL_ENTRY_{Trans_JeNme}_TOTALS.txt",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)