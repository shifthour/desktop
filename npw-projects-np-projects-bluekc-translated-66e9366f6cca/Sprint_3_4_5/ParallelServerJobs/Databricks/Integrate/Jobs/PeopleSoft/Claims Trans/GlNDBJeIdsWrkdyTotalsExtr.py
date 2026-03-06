# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Called by : GLJeIdsWrkdyTotalsExtrSeq
# MAGIC 
# MAGIC 
# MAGIC Process Description: This job will extract the monthly GL NDB detail transactions, join to the IDS crosswalk to assign the correct FNCL LOB and load to JRNL_ENTRY totals output file that will be the source                                   of the Finance monthly Journal Entry total report that will be used for balancing
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                               Project/                                                                                                                        Code                  Date
# MAGIC Developer                    Date                   Altiris #     Change Description                                                                                     Reviewer            Reviewed
# MAGIC -----------------------              -------------------    -------------   -----------------------------------------------------------------------------------------------------------------   -------------------------  -------------------
# MAGIC Tim Sieg                         05/30/2018       5879          Original Programming                                                                             Kalyan Neelam   2018-06-06
# MAGIC Prabhu ES                     2022-06-15         S2S           MSSQL ODBC conn params added              IntegrateDev5

# MAGIC This extract is required for Finance to balance the results of the NewDirections JE process that extracts data from SYBASE and IDS and creates the monthly Workday JE file
# MAGIC Extract current GL NDB JE details
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
from pyspark.sql import DataFrame
from pyspark.sql import functions as sx
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

BCBSFINOwner = get_widget_value("BCBSFINOwner","")
bcbsfin_secret_name = get_widget_value("bcbsfin_secret_name","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
Trans_JeNme = get_widget_value("Trans_JeNme","")
Environment = get_widget_value("Environment","")

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_DB2_JRNL_ENTRY_TRANS_CRSWALK = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT A.LGCY_FNCL_LOB_CD  AS LGCY_FNCL_LOB_CD,
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
        f"""
SELECT BUSINESS_UNIT_GL, 
       TRANSACTION_LINE, 
       ACCOUNTING_DT, 
       WD_ACCT_NO, 
       CASE WHEN DR_CR_CD = 'CR' THEN CAST((TX_AMT*-1) AS DECIMAL(19,2))
            ELSE CAST(TX_AMT AS DECIMAL(19,2))
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
  FROM {BCBSFINOwner}.GL_CLAIM_DTL_BCP
 WHERE MAP_IN = 'Y'
   AND MONETARY_AMOUNT <> 0.00
   AND SNAP_SRC_CD = 'NDB'
"""
    )
    .load()
)

df_lookup_ids_lob_cd = (
    df_Src_GL_CLAIM_DTL_BCP.alias("GlJeLines")
    .join(
        df_DB2_JRNL_ENTRY_TRANS_CRSWALK.alias("IdsLobCds"),
        sx.col("GlJeLines.GL_DTL_LOB") == sx.col("IdsLobCds.LGCY_FNCL_LOB_CD"),
        "left",
    )
    .select(
        sx.col("GlJeLines.BUSINESS_UNIT_GL").alias("BUSINESS_UNIT_GL"),
        sx.col("GlJeLines.TRANSACTION_LINE").alias("TRANSACTION_LINE"),
        sx.col("GlJeLines.ACCOUNTING_DT").alias("ACCOUNTING_DT"),
        sx.col("GlJeLines.WD_ACCT_NO").alias("WD_ACCT_NO"),
        sx.col("GlJeLines.GL_AMT").alias("GL_AMT"),
        sx.col("GlJeLines.WD_CC_ID").alias("WD_CC_ID"),
        sx.col("GlJeLines.WD_RVNU_CAT_ID").alias("WD_RVNU_CAT_ID"),
        sx.col("GlJeLines.WD_SPEND_CAT_ID").alias("WD_SPEND_CAT_ID"),
        sx.col("GlJeLines.WD_AFFIL_ID").alias("WD_AFFIL_ID"),
        sx.col("GlJeLines.WD_CUST_ID").alias("WD_CUST_ID"),
        sx.col("GlJeLines.MAP_IN").alias("MAP_IN"),
        sx.col("GlJeLines.APPL_JRNL_ID").alias("APPL_JRNL_ID"),
        sx.col("GlJeLines.GL_DTL_LOB").alias("GL_DTL_LOB"),
        sx.col("IdsLobCds.FINALLOB").alias("FINALLOB"),
    )
)

df_trns_gl_je_curnt_lob = (
    df_lookup_ids_lob_cd.alias("JeCrrntLob")
    .withColumn("BUSINESS_UNIT_GL", sx.col("JeCrrntLob.BUSINESS_UNIT_GL"))
    .withColumn("TRANSACTION_LINE", sx.col("JeCrrntLob.TRANSACTION_LINE"))
    .withColumn("ACCOUNTING_DT", sx.col("JeCrrntLob.ACCOUNTING_DT"))
    .withColumn("WD_ACCT_NO", sx.col("JeCrrntLob.WD_ACCT_NO"))
    .withColumn("GL_AMT", sx.col("JeCrrntLob.GL_AMT"))
    .withColumn("WD_CC_ID", sx.col("JeCrrntLob.WD_CC_ID"))
    .withColumn("WD_RVNU_CAT_ID", sx.col("JeCrrntLob.WD_RVNU_CAT_ID"))
    .withColumn("WD_SPEND_CAT_ID", sx.col("JeCrrntLob.WD_SPEND_CAT_ID"))
    .withColumn("WD_AFFIL_ID", sx.col("JeCrrntLob.WD_AFFIL_ID"))
    .withColumn("WD_CUST_ID", sx.col("JeCrrntLob.WD_CUST_ID"))
    .withColumn("MAP_IN", sx.col("JeCrrntLob.MAP_IN"))
    .withColumn("APPL_JRNL_ID", sx.col("JeCrrntLob.APPL_JRNL_ID"))
    .withColumn(
        "FINALLOB",
        sx.when(
            sx.length(sx.col("JeCrrntLob.FINALLOB")) == 0,
            trim(sx.col("JeCrrntLob.GL_DTL_LOB")),
        ).otherwise(trim(sx.col("JeCrrntLob.FINALLOB"))),
    )
)

df_aggr_gl_je_line = (
    df_trns_gl_je_curnt_lob.alias("GlJeLob")
    .groupBy(
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
    .agg(sx.sum("GL_AMT").alias("GL_AMT_SUM"))
)

df_trns_gl_je_line_drcr_amts = (
    df_aggr_gl_je_line.alias("JeLnSum")
    .filter(sx.col("JeLnSum.GL_AMT_SUM") != 0)
    .withColumn(
        "WD_ACCT_NO",
        sx.when(sx.col("JeLnSum.WD_ACCT_NO").isNull(), sx.lit(" "))
        .otherwise(sx.col("JeLnSum.WD_ACCT_NO")),
    )
    .withColumn(
        "WD_CC_ID",
        sx.when(sx.col("JeLnSum.WD_CC_ID").isNull(), sx.lit(" "))
        .otherwise(sx.col("JeLnSum.WD_CC_ID")),
    )
    .withColumn(
        "WD_RVNU_CAT_ID",
        sx.when(sx.col("JeLnSum.WD_RVNU_CAT_ID").isNull(), sx.lit(" "))
        .otherwise(sx.col("JeLnSum.WD_RVNU_CAT_ID")),
    )
    .withColumn(
        "WD_SPEND_CAT_ID",
        sx.when(sx.col("JeLnSum.WD_SPEND_CAT_ID").isNull(), sx.lit(" "))
        .otherwise(sx.col("JeLnSum.WD_SPEND_CAT_ID")),
    )
    .withColumn(
        "WD_AFFIL_ID",
        sx.when(sx.col("JeLnSum.WD_AFFIL_ID").isNull(), sx.lit(" "))
        .otherwise(sx.col("JeLnSum.WD_AFFIL_ID")),
    )
    .withColumn(
        "WD_CUST_ID",
        sx.when(sx.col("JeLnSum.WD_CUST_ID").isNull(), sx.lit(" "))
        .otherwise(sx.col("JeLnSum.WD_CUST_ID")),
    )
    .withColumn(
        "GL_AMT_SUM_DR",
        sx.when(sx.col("JeLnSum.GL_AMT_SUM").isNull(), sx.lit(0.00))
        .when(sx.col("JeLnSum.GL_AMT_SUM") > 0, sx.col("JeLnSum.GL_AMT_SUM"))
        .otherwise(sx.lit(0.00)),
    )
    .withColumn(
        "GL_AMT_SUM_CR",
        sx.when(sx.col("JeLnSum.GL_AMT_SUM").isNull(), sx.lit(0.00))
        .when(sx.col("JeLnSum.GL_AMT_SUM") < 0, sx.col("JeLnSum.GL_AMT_SUM"))
        .otherwise(sx.lit(0.00)),
    )
)

df_final = (
    df_trns_gl_je_line_drcr_amts.select(
        sx.col("BUSINESS_UNIT_GL"),
        "ACCOUNTING_DT",
        "WD_ACCT_NO",
        "WD_CC_ID",
        "WD_RVNU_CAT_ID",
        "WD_SPEND_CAT_ID",
        "WD_AFFIL_ID",
        "WD_CUST_ID",
        sx.col("FINALLOB"),
        sx.col("APPL_JRNL_ID"),
        "GL_AMT_SUM_DR",
        "GL_AMT_SUM_CR",
    )
    .withColumn("BUSINESS_UNIT_GL", sx.rpad(sx.col("BUSINESS_UNIT_GL"), 5, " "))
    .withColumn("FINALLOB", sx.rpad(sx.col("FINALLOB"), 20, " "))
    .withColumn("APPL_JRNL_ID", sx.rpad(sx.col("APPL_JRNL_ID"), 10, " "))
)

write_files(
    df_final,
    f"{adls_path_publish}/external/{Environment}_JRNL_ENTRY_{Trans_JeNme}_TOTALS.txt",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote='"',
    nullValue=None,
)