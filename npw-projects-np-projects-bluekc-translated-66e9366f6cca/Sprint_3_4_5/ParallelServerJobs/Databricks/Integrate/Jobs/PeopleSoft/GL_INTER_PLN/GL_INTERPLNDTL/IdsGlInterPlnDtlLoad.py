# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC JOB NAME:  IdsGlInterPlnDtlLoad
# MAGIC CALLED BY: FctsGLInterPlnBillDtlLoadCntl
# MAGIC 
# MAGIC PROCESSING: This Job Loads Data from Sequential File created in Extract Job and loads SYBASE Table GL_INTER_PLAN_BILL_DTL
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                      Date                 	Project/Altiris #      Change Description                                                   Development Project	Code Reviewer             Date Reviewed       
# MAGIC ------------------                --------------------     	------------------------      -----------------------------------------------------------------------    	--------------------------------	-------------------------------      ---------------------------       
# MAGIC Tim Sieg                      03-07-2018	  5599		Original Programming			IntegrateDev2         	               Jaideep Mankala            03/19/2018
# MAGIC Prabhu ES                   03-10-2022         S2S Remediation      MSSQL ODBC conn params added                        IntegrateDev5		Ken Bradmon	2022-06-13

# MAGIC Job to load the Target SYBASE table GL_INTER_PLAN_BILL_DTL
# MAGIC 
# MAGIC Job Name:
# MAGIC IdsGlInterPlnDtlLoad
# MAGIC Read the Load ready file created in Extract job
# MAGIC Load file created in IdsGlInterPlnDtlExtr job will be loaded into the target SYBASE table GL_INTER_PLAN_BILL_DTL
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, TimestampType
from pyspark.sql.functions import rpad, col
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


BCBSFINOwner = get_widget_value('BCBSFINOwner','')
bcbsfin_secret_name = get_widget_value('bcbsfin_secret_name','')

schema_Seq_GlInterPlnBillDtl = StructType([
    StructField("LOBD_ID", StringType(), False),
    StructField("PDBL_ACCT_CAT", StringType(), False),
    StructField("SNAP_SRC_CD", StringType(), False),
    StructField("FA_SUB_SRC_CD", StringType(), False),
    StructField("MAP_FLD_1_CD", StringType(), False),
    StructField("MAP_FLD_2_CD", StringType(), False),
    StructField("MAP_FLD_3_CD", StringType(), False),
    StructField("MAP_FLD_4_CD", StringType(), False),
    StructField("MAP_EFF_DT", TimestampType(), False),
    StructField("HOST_PLN_ID", StringType(), False),
    StructField("BILL_DTL_SRL_NO", StringType(), False),
    StructField("TX_AMT", DecimalType(38,10), True),
    StructField("SNAP_ACT_DT", TimestampType(), True),
    StructField("CRME_FUND_AMT", DecimalType(38,10), True),
    StructField("CRME_PAY_FROM_DT", TimestampType(), False),
    StructField("CRME_PAY_THRU_DT", TimestampType(), False),
    StructField("CRME_PYMT_METH_IND", StringType(), False),
    StructField("DR_CR_CD", StringType(), False),
    StructField("PAGR_ID", StringType(), True),
    StructField("GRGR_ID", StringType(), True),
    StructField("MAP_IN", StringType(), False),
    StructField("BUSINESS_UNIT", StringType(), False),
    StructField("TRANSACTION_ID", StringType(), False),
    StructField("TRANSACTION_LINE", IntegerType(), False),
    StructField("ACCOUNTING_DT", TimestampType(), False),
    StructField("APPL_JRNL_ID", StringType(), False),
    StructField("BUSINESS_UNIT_GL", StringType(), False),
    StructField("ACCOUNT", StringType(), False),
    StructField("DEPTID", StringType(), False),
    StructField("OPERATING_UNIT", StringType(), False),
    StructField("PRODUCT", StringType(), False),
    StructField("AFFILIATE", StringType(), False),
    StructField("CHARTFIELD1", StringType(), False),
    StructField("CHARTFIELD2", StringType(), False),
    StructField("CHARTFIELD3", StringType(), False),
    StructField("PROJECT_ID", StringType(), False),
    StructField("CURRENCY_CD", StringType(), False),
    StructField("RT_TYPE", StringType(), False),
    StructField("MONETARY_AMOUNT", DecimalType(38,10), False),
    StructField("FOREIGN_AMOUNT", DecimalType(38,10), False),
    StructField("JRNL_LN_REF", StringType(), False),
    StructField("LINE_DESCR", StringType(), False),
    StructField("GL_DISTRIB_STATUS", StringType(), False),
    StructField("WD_LOB_CD", StringType(), True),
    StructField("WD_ACCT_DTM", TimestampType(), True),
    StructField("WD_ACCT_NO", StringType(), True),
    StructField("WD_CC_ID", StringType(), True),
    StructField("WD_DNR_ID", StringType(), True),
    StructField("WD_AFFIL_ID", StringType(), True),
    StructField("WD_CUST_ID", StringType(), True),
    StructField("WD_JRNL_TYP_CD", StringType(), True),
    StructField("WD_JRNL_DESC", StringType(), True),
    StructField("WD_BANK_ACCT_NM", StringType(), True),
    StructField("WD_RVNU_CAT_ID", StringType(), True),
    StructField("WD_SPEND_CAT_ID", StringType(), True)
])

df_Seq_GlInterPlnBillDtl = (
    spark.read
    .option("header", "true")
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_Seq_GlInterPlnBillDtl)
    .csv(f"{adls_path}/load/GlInterPlnBillDtl.dat")
)

df_cpyforBuffer = df_Seq_GlInterPlnBillDtl.select(
    col("LOBD_ID").alias("LOBD_ID"),
    col("PDBL_ACCT_CAT").alias("PDBL_ACCT_CAT"),
    col("SNAP_SRC_CD").alias("SNAP_SRC_CD"),
    col("FA_SUB_SRC_CD").alias("FA_SUB_SRC_CD"),
    col("MAP_FLD_1_CD").alias("MAP_FLD_1_CD"),
    col("MAP_FLD_2_CD").alias("MAP_FLD_2_CD"),
    col("MAP_FLD_3_CD").alias("MAP_FLD_3_CD"),
    col("MAP_FLD_4_CD").alias("MAP_FLD_4_CD"),
    col("MAP_EFF_DT").alias("MAP_EFF_DT"),
    col("HOST_PLN_ID").alias("HOST_PLN_ID"),
    col("BILL_DTL_SRL_NO").alias("BILL_DTL_SRL_NO"),
    col("TX_AMT").alias("TX_AMT"),
    col("SNAP_ACT_DT").alias("SNAP_ACT_DT"),
    col("CRME_FUND_AMT").alias("CRME_FUND_AMT"),
    col("CRME_PAY_FROM_DT").alias("CRME_PAY_FROM_DT"),
    col("CRME_PAY_THRU_DT").alias("CRME_PAY_THRU_DT"),
    col("CRME_PYMT_METH_IND").alias("CRME_PYMT_METH_IND"),
    col("DR_CR_CD").alias("DR_CR_CD"),
    col("PAGR_ID").alias("PAGR_ID"),
    col("GRGR_ID").alias("GRGR_ID"),
    col("MAP_IN").alias("MAP_IN"),
    col("BUSINESS_UNIT").alias("BUSINESS_UNIT"),
    col("TRANSACTION_ID").alias("TRANSACTION_ID"),
    col("TRANSACTION_LINE").alias("TRANSACTION_LINE"),
    col("ACCOUNTING_DT").alias("ACCOUNTING_DT"),
    col("APPL_JRNL_ID").alias("APPL_JRNL_ID"),
    col("BUSINESS_UNIT_GL").alias("BUSINESS_UNIT_GL"),
    col("ACCOUNT").alias("ACCOUNT"),
    col("DEPTID").alias("DEPTID"),
    col("OPERATING_UNIT").alias("OPERATING_UNIT"),
    col("PRODUCT").alias("PRODUCT"),
    col("AFFILIATE").alias("AFFILIATE"),
    col("CHARTFIELD1").alias("CHARTFIELD1"),
    col("CHARTFIELD2").alias("CHARTFIELD2"),
    col("CHARTFIELD3").alias("CHARTFIELD3"),
    col("PROJECT_ID").alias("PROJECT_ID"),
    col("CURRENCY_CD").alias("CURRENCY_CD"),
    col("RT_TYPE").alias("RT_TYPE"),
    col("MONETARY_AMOUNT").alias("MONETARY_AMOUNT"),
    col("FOREIGN_AMOUNT").alias("FOREIGN_AMOUNT"),
    col("JRNL_LN_REF").alias("JRNL_LN_REF"),
    col("LINE_DESCR").alias("LINE_DESCR"),
    col("GL_DISTRIB_STATUS").alias("GL_DISTRIB_STATUS"),
    col("WD_LOB_CD").alias("WD_LOB_CD"),
    col("WD_ACCT_DTM").alias("WD_ACCT_DTM"),
    col("WD_ACCT_NO").alias("WD_ACCT_NO"),
    col("WD_CC_ID").alias("WD_CC_ID"),
    col("WD_DNR_ID").alias("WD_DNR_ID"),
    col("WD_AFFIL_ID").alias("WD_AFFIL_ID"),
    col("WD_CUST_ID").alias("WD_CUST_ID"),
    col("WD_JRNL_TYP_CD").alias("WD_JRNL_TYP_CD"),
    col("WD_JRNL_DESC").alias("WD_JRNL_DESC"),
    col("WD_BANK_ACCT_NM").alias("WD_BANK_ACCT_NM"),
    col("WD_RVNU_CAT_ID").alias("WD_RVNU_CAT_ID"),
    col("WD_SPEND_CAT_ID").alias("WD_SPEND_CAT_ID")
)

df_GL_INTER_PLN_BILL_DTL = df_cpyforBuffer.select(
    rpad("LOBD_ID", 4, " ").alias("LOBD_ID"),
    rpad("PDBL_ACCT_CAT", 4, " ").alias("PDBL_ACCT_CAT"),
    rpad("SNAP_SRC_CD", 3, " ").alias("SNAP_SRC_CD"),
    rpad("FA_SUB_SRC_CD", 5, " ").alias("FA_SUB_SRC_CD"),
    rpad("MAP_FLD_1_CD", 4, " ").alias("MAP_FLD_1_CD"),
    rpad("MAP_FLD_2_CD", 4, " ").alias("MAP_FLD_2_CD"),
    rpad("MAP_FLD_3_CD", 4, " ").alias("MAP_FLD_3_CD"),
    rpad("MAP_FLD_4_CD", 4, " ").alias("MAP_FLD_4_CD"),
    col("MAP_EFF_DT"),
    rpad("HOST_PLN_ID", 4, " ").alias("HOST_PLN_ID"),
    rpad("BILL_DTL_SRL_NO", <...>, " ").alias("BILL_DTL_SRL_NO"),
    col("TX_AMT"),
    col("SNAP_ACT_DT"),
    col("CRME_FUND_AMT"),
    col("CRME_PAY_FROM_DT"),
    col("CRME_PAY_THRU_DT"),
    rpad("CRME_PYMT_METH_IND", 1, " ").alias("CRME_PYMT_METH_IND"),
    rpad("DR_CR_CD", 2, " ").alias("DR_CR_CD"),
    rpad("PAGR_ID", <...>, " ").alias("PAGR_ID"),
    rpad("GRGR_ID", <...>, " ").alias("GRGR_ID"),
    rpad("MAP_IN", 1, " ").alias("MAP_IN"),
    rpad("BUSINESS_UNIT", 5, " ").alias("BUSINESS_UNIT"),
    rpad("TRANSACTION_ID", 10, " ").alias("TRANSACTION_ID"),
    col("TRANSACTION_LINE"),
    col("ACCOUNTING_DT"),
    rpad("APPL_JRNL_ID", 10, " ").alias("APPL_JRNL_ID"),
    rpad("BUSINESS_UNIT_GL", 5, " ").alias("BUSINESS_UNIT_GL"),
    rpad("ACCOUNT", 10, " ").alias("ACCOUNT"),
    rpad("DEPTID", 10, " ").alias("DEPTID"),
    rpad("OPERATING_UNIT", 8, " ").alias("OPERATING_UNIT"),
    rpad("PRODUCT", 6, " ").alias("PRODUCT"),
    rpad("AFFILIATE", 5, " ").alias("AFFILIATE"),
    rpad("CHARTFIELD1", 10, " ").alias("CHARTFIELD1"),
    rpad("CHARTFIELD2", 10, " ").alias("CHARTFIELD2"),
    rpad("CHARTFIELD3", 10, " ").alias("CHARTFIELD3"),
    rpad("PROJECT_ID", 15, " ").alias("PROJECT_ID"),
    rpad("CURRENCY_CD", 3, " ").alias("CURRENCY_CD"),
    rpad("RT_TYPE", 5, " ").alias("RT_TYPE"),
    col("MONETARY_AMOUNT"),
    col("FOREIGN_AMOUNT"),
    rpad("JRNL_LN_REF", 10, " ").alias("JRNL_LN_REF"),
    rpad("LINE_DESCR", 30, " ").alias("LINE_DESCR"),
    rpad("GL_DISTRIB_STATUS", 1, " ").alias("GL_DISTRIB_STATUS"),
    rpad("WD_LOB_CD", <...>, " ").alias("WD_LOB_CD"),
    col("WD_ACCT_DTM"),
    rpad("WD_ACCT_NO", <...>, " ").alias("WD_ACCT_NO"),
    rpad("WD_CC_ID", <...>, " ").alias("WD_CC_ID"),
    rpad("WD_DNR_ID", <...>, " ").alias("WD_DNR_ID"),
    rpad("WD_AFFIL_ID", <...>, " ").alias("WD_AFFIL_ID"),
    rpad("WD_CUST_ID", <...>, " ").alias("WD_CUST_ID"),
    rpad("WD_JRNL_TYP_CD", <...>, " ").alias("WD_JRNL_TYP_CD"),
    rpad("WD_JRNL_DESC", <...>, " ").alias("WD_JRNL_DESC"),
    rpad("WD_BANK_ACCT_NM", <...>, " ").alias("WD_BANK_ACCT_NM"),
    rpad("WD_RVNU_CAT_ID", <...>, " ").alias("WD_RVNU_CAT_ID"),
    rpad("WD_SPEND_CAT_ID", <...>, " ").alias("WD_SPEND_CAT_ID")
)

jdbc_url, jdbc_props = get_db_config(bcbsfin_secret_name)

execute_dml(
    "DROP TABLE IF EXISTS STAGING.IdsGlInterPlnDtlLoad_GL_INTER_PLN_BILL_DTL_temp",
    jdbc_url,
    jdbc_props
)

df_GL_INTER_PLN_BILL_DTL.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.IdsGlInterPlnDtlLoad_GL_INTER_PLN_BILL_DTL_temp") \
    .mode("append") \
    .save()

merge_sql = f"""
MERGE INTO {BCBSFINOwner}.GL_INTER_PLAN_BILL_DTL AS T
USING STAGING.IdsGlInterPlnDtlLoad_GL_INTER_PLN_BILL_DTL_temp AS S
ON (1=0)
WHEN NOT MATCHED THEN
INSERT (
LOBD_ID,
PDBL_ACCT_CAT,
SNAP_SRC_CD,
FA_SUB_SRC_CD,
MAP_FLD_1_CD,
MAP_FLD_2_CD,
MAP_FLD_3_CD,
MAP_FLD_4_CD,
MAP_EFF_DT,
HOST_PLN_ID,
BILL_DTL_SRL_NO,
TX_AMT,
SNAP_ACT_DT,
CRME_FUND_AMT,
CRME_PAY_FROM_DT,
CRME_PAY_THRU_DT,
CRME_PYMT_METH_IND,
DR_CR_CD,
PAGR_ID,
GRGR_ID,
MAP_IN,
BUSINESS_UNIT,
TRANSACTION_ID,
TRANSACTION_LINE,
ACCOUNTING_DT,
APPL_JRNL_ID,
BUSINESS_UNIT_GL,
ACCOUNT,
DEPTID,
OPERATING_UNIT,
PRODUCT,
AFFILIATE,
CHARTFIELD1,
CHARTFIELD2,
CHARTFIELD3,
PROJECT_ID,
CURRENCY_CD,
RT_TYPE,
MONETARY_AMOUNT,
FOREIGN_AMOUNT,
JRNL_LN_REF,
LINE_DESCR,
GL_DISTRIB_STATUS,
WD_LOB_CD,
WD_ACCT_DTM,
WD_ACCT_NO,
WD_CC_ID,
WD_DNR_ID,
WD_AFFIL_ID,
WD_CUST_ID,
WD_JRNL_TYP_CD,
WD_JRNL_DESC,
WD_BANK_ACCT_NM,
WD_RVNU_CAT_ID,
WD_SPEND_CAT_ID
)
VALUES (
S.LOBD_ID,
S.PDBL_ACCT_CAT,
S.SNAP_SRC_CD,
S.FA_SUB_SRC_CD,
S.MAP_FLD_1_CD,
S.MAP_FLD_2_CD,
S.MAP_FLD_3_CD,
S.MAP_FLD_4_CD,
S.MAP_EFF_DT,
S.HOST_PLN_ID,
S.BILL_DTL_SRL_NO,
S.TX_AMT,
S.SNAP_ACT_DT,
S.CRME_FUND_AMT,
S.CRME_PAY_FROM_DT,
S.CRME_PAY_THRU_DT,
S.CRME_PYMT_METH_IND,
S.DR_CR_CD,
S.PAGR_ID,
S.GRGR_ID,
S.MAP_IN,
S.BUSINESS_UNIT,
S.TRANSACTION_ID,
S.TRANSACTION_LINE,
S.ACCOUNTING_DT,
S.APPL_JRNL_ID,
S.BUSINESS_UNIT_GL,
S.ACCOUNT,
S.DEPTID,
S.OPERATING_UNIT,
S.PRODUCT,
S.AFFILIATE,
S.CHARTFIELD1,
S.CHARTFIELD2,
S.CHARTFIELD3,
S.PROJECT_ID,
S.CURRENCY_CD,
S.RT_TYPE,
S.MONETARY_AMOUNT,
S.FOREIGN_AMOUNT,
S.JRNL_LN_REF,
S.LINE_DESCR,
S.GL_DISTRIB_STATUS,
S.WD_LOB_CD,
S.WD_ACCT_DTM,
S.WD_ACCT_NO,
S.WD_CC_ID,
S.WD_DNR_ID,
S.WD_AFFIL_ID,
S.WD_CUST_ID,
S.WD_JRNL_TYP_CD,
S.WD_JRNL_DESC,
S.WD_BANK_ACCT_NM,
S.WD_RVNU_CAT_ID,
S.WD_SPEND_CAT_ID
);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)