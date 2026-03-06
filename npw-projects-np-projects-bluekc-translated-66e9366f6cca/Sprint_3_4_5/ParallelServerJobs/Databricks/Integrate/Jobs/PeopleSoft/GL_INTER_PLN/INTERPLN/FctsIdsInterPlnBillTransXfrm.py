# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Called By:FctsBcbsExtInterPlnBillLoadSeq
# MAGIC 
# MAGIC 
# MAGIC Process Description: Extract Data from (GL_INTER_PLN_BILL_DTL) and Load it to IDS 
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                               Project/                                                                                                                        Code                  Date
# MAGIC Developer                    Date                   Altiris #     Change Description                                                                                     Reviewer            Reviewed
# MAGIC -----------------------              -------------------    -------------   -----------------------------------------------------------------------------------------------------------------   -------------------------  -------------------
# MAGIC sudheer champati       10/12/2015       5212       Originally Programmed                                                                                 Kalyan Neelam    2015-12-31

# MAGIC All the Businees rules are applied in this Extract before generating the Pimaray key to Load to the FinalTable.
# MAGIC The Datastet is used in the FctsIdsOnExchFedPaymtTransPkey job and is deleted at the CTRL level.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
RunIDTimeStamp = get_widget_value('RunIDTimeStamp','')
RUNDATE = get_widget_value('RUNDATE','')

schema_ds_INTER_PLN_BILL_Extr = StructType([
    StructField("GL_INTER_PLAN_BILL_DTL_CK", StringType(), True),
    StructField("LOBD_ID", StringType(), True),
    StructField("PDBL_ACCT_CAT", StringType(), True),
    StructField("CRME_PAY_FROM_DT", StringType(), True),
    StructField("CRME_PAY_THRU_DT", StringType(), True),
    StructField("CRME_PYMT_METH_IND", StringType(), True),
    StructField("GRGR_ID", StringType(), True),
    StructField("ACCOUNTING_DT", StringType(), True),
    StructField("MONETARY_AMOUNT", StringType(), True),
    StructField("BILL_DTL_SRL_NO", StringType(), True)
])

df_ds_INTER_PLN_BILL_Extr = (
    spark.read.schema(schema_ds_INTER_PLN_BILL_Extr)
    .parquet(f"{adls_path}/ds/INTER_PLN_BILL_TRANS.{SrcSysCd}.extr.{RunID}.parquet")
    .select(
        "GL_INTER_PLAN_BILL_DTL_CK",
        "LOBD_ID",
        "PDBL_ACCT_CAT",
        "CRME_PAY_FROM_DT",
        "CRME_PAY_THRU_DT",
        "CRME_PYMT_METH_IND",
        "GRGR_ID",
        "ACCOUNTING_DT",
        "MONETARY_AMOUNT",
        "BILL_DTL_SRL_NO"
    )
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_ids = f"""
SELECT DISTINCT
 PLAN.INTER_PLN_BILL_DTL_ID,
 PLAN.MBR_SK,
 MAX(PROD.PROD_ID) AS PROD_ID,
 MAX(ENR.TERM_DT_SK) AS TERM_DT_SK
FROM
 {IDSOwner}.INTER_PLN_BILL_DTL PLAN,
 {IDSOwner}.MBR MBR,
 {IDSOwner}.MBR_ENR ENR,
 {IDSOwner}.PROD PROD,
 {IDSOwner}.CD_MPPNG MPPNG
WHERE
 PLAN.MBR_SK = MBR.MBR_SK
 AND MBR.MBR_SK = ENR.MBR_SK
 AND ENR.PROD_SK = PROD.PROD_SK
 AND ENR.ELIG_IN = 'Y'
 AND ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = MPPNG.CD_MPPNG_SK
 AND MPPNG.TRGT_CD = 'MED'
GROUP BY
 PLAN.INTER_PLN_BILL_DTL_ID,
 PLAN.MBR_SK
"""

df_IDS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_ids)
    .load()
)

df_Lookup_6 = (
    df_ds_INTER_PLN_BILL_Extr.alias("lnk_FctsIdsInterPlnXfrm")
    .join(
        df_IDS.alias("IDS_SRC"),
        F.col("lnk_FctsIdsInterPlnXfrm.BILL_DTL_SRL_NO") == F.col("IDS_SRC.INTER_PLN_BILL_DTL_ID"),
        how="left"
    )
    .select(
        F.col("lnk_FctsIdsInterPlnXfrm.GL_INTER_PLAN_BILL_DTL_CK").alias("GL_INTER_PLAN_BILL_DTL_CK"),
        F.col("lnk_FctsIdsInterPlnXfrm.LOBD_ID").alias("LOBD_ID"),
        F.col("lnk_FctsIdsInterPlnXfrm.PDBL_ACCT_CAT").alias("PDBL_ACCT_CAT"),
        F.col("lnk_FctsIdsInterPlnXfrm.CRME_PAY_FROM_DT").alias("CRME_PAY_FROM_DT"),
        F.col("lnk_FctsIdsInterPlnXfrm.CRME_PAY_THRU_DT").alias("CRME_PAY_THRU_DT"),
        F.col("lnk_FctsIdsInterPlnXfrm.CRME_PYMT_METH_IND").alias("CRME_PYMT_METH_IND"),
        F.col("lnk_FctsIdsInterPlnXfrm.GRGR_ID").alias("GRGR_ID"),
        F.col("lnk_FctsIdsInterPlnXfrm.ACCOUNTING_DT").alias("ACCOUNTING_DT"),
        F.col("lnk_FctsIdsInterPlnXfrm.MONETARY_AMOUNT").alias("MONETARY_AMOUNT"),
        F.col("lnk_FctsIdsInterPlnXfrm.BILL_DTL_SRL_NO").alias("BILL_DTL_SRL_NO"),
        F.col("IDS_SRC.PROD_ID").alias("PDPD_ID"),
        F.col("IDS_SRC.MBR_SK").alias("MBR_SK")
    )
)

df_StripField = (
    df_Lookup_6
    .select(
        F.col("GL_INTER_PLAN_BILL_DTL_CK").alias("GL_INTER_PLAN_BILL_DTL_CK"),
        TrimLeadingTrailing(STRIP.FIELD.EE(F.col("LOBD_ID"))).alias("LOBD_ID"),
        TrimLeadingTrailing(STRIP.FIELD.EE(F.col("PDBL_ACCT_CAT"))).alias("PDBL_ACCT_CAT"),
        F.col("CRME_PAY_FROM_DT").alias("CRME_PAY_FROM_DT"),
        F.col("CRME_PAY_THRU_DT").alias("CRME_PAY_THRU_DT"),
        TrimLeadingTrailing(STRIP.FIELD.EE(F.col("CRME_PYMT_METH_IND"))).alias("CRME_PYMT_METH_IND"),
        TrimLeadingTrailing(STRIP.FIELD.EE(F.col("GRGR_ID"))).alias("GRGR_ID"),
        F.col("ACCOUNTING_DT").alias("ACCOUNTING_DT"),
        F.col("MONETARY_AMOUNT").alias("MONETARY_AMOUNT"),
        TrimLeadingTrailing(STRIP.FIELD.EE(F.col("BILL_DTL_SRL_NO"))).alias("BILL_DTL_SRL_NO"),
        TrimLeadingTrailing(STRIP.FIELD.EE(F.col("PDPD_ID"))).alias("PDPD_ID"),
        F.col("MBR_SK").alias("MBR_SK")
    )
)

df_Xfrm_BusinessRules = (
    df_StripField
    .withColumn(
        "svACCTGDT",
        FORMAT.DATE.EE(
            F.col("ACCOUNTING_DT"),
            "SYBASE",
            "TIMESTAMP",
            "DATE"
        )
    )
    .withColumn(
        "PRI_NAT_KEY_STRING",
        F.concat(
            F.col("SrcSysCd"),
            F.lit(";"),
            F.col("GL_INTER_PLAN_BILL_DTL_CK"),
            F.lit(";"),
            F.col("svACCTGDT")
        )
    )
    .withColumn("FIRST_RECYC_TS", F.col("RunIDTimeStamp"))
    .withColumn("INTER_PLN_BILL_TRANS_CK", F.col("GL_INTER_PLAN_BILL_DTL_CK"))
    .withColumn("ACCTG_DT_SK", F.col("ACCOUNTING_DT"))
    .withColumn("SRC_SYS_CD_SK", F.col("SrcSysCdSk"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("INTER_PLN_BILL_TRANS_LOB_CD_SK", F.lit("TBD"))
    .withColumn("INTER_PLN_BILL_TRANS_PAYMT_METH_CD_SK", F.lit("TBD"))
    .withColumn("PD_FROM_DT_SK", F.col("CRME_PAY_FROM_DT"))
    .withColumn("PD_THRU_DT_SK", F.col("CRME_PAY_THRU_DT"))
    .withColumn("CALC_FUND_AMT", StringToDecimal(F.col("MONETARY_AMOUNT")))
    .withColumn("FUND_RATE_AMT", StringToDecimal(F.col("MONETARY_AMOUNT")))
    .withColumn("SRC_SYS_CD", F.lit("BCBSA"))
    .withColumn("LOBD_ID", F.col("LOBD_ID"))
    .withColumn("CRME_PYMT_METH_IND", F.col("CRME_PYMT_METH_IND"))
    .withColumn("BILL_DTL_SRL_NO", F.col("BILL_DTL_SRL_NO"))
    .withColumn("PDPD_ID", F.col("PDPD_ID"))
    .withColumn("MBR_SK", F.col("MBR_SK"))
    .withColumn("GRGR_ID", F.col("GRGR_ID"))
    .withColumn("PDBL_ACCT_CAT", F.col("PDBL_ACCT_CAT"))
)

df_Xfrm_BusinessRules_final = df_Xfrm_BusinessRules.select(
    "PRI_NAT_KEY_STRING",
    "FIRST_RECYC_TS",
    "INTER_PLN_BILL_TRANS_CK",
    "ACCTG_DT_SK",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "INTER_PLN_BILL_TRANS_LOB_CD_SK",
    "INTER_PLN_BILL_TRANS_PAYMT_METH_CD_SK",
    "PD_FROM_DT_SK",
    "PD_THRU_DT_SK",
    "CALC_FUND_AMT",
    "FUND_RATE_AMT",
    "SRC_SYS_CD",
    "LOBD_ID",
    "CRME_PYMT_METH_IND",
    "BILL_DTL_SRL_NO",
    "PDPD_ID",
    "MBR_SK",
    "GRGR_ID",
    "PDBL_ACCT_CAT"
)

write_files(
    df_Xfrm_BusinessRules_final,
    f"{adls_path}/ds/INTER_PLN_BILL_TRANS.{SrcSysCd}.xfrm.{RunID}.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)