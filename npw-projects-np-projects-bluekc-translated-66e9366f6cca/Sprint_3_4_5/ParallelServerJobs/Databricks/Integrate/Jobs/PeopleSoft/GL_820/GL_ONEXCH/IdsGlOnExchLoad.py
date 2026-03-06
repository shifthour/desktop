# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC JOB NAME:  IdsGlOnExchLoad
# MAGIC CALLED BY: FctsGLOnExchFedPymtLoadCntl
# MAGIC 
# MAGIC PROCESSING:  This Job Loads Data from Sequential File created in Extract Job and loads SYBASE Table GL_ON_EXCH_FED_PYMT_DTL
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                      Date                 	Project/Altiris #      Change Description                                                   Development Project	Code Reviewer             Date Reviewed       
# MAGIC ------------------                --------------------     	------------------------      -----------------------------------------------------------------------    	--------------------------------	-------------------------------      ---------------------------       
# MAGIC Tim Sieg                      03-07-2018	  5599		Original Programming			IntegrateDev2                         Jaideep Mankala          03/19/2018        
# MAGIC Razia F                  2022-03-30            S2S                        MSSQL ODBC conn params added                         IntegrateDev5	Ken Bradmon	2022-06-13

# MAGIC Job to load the Target SYBASE table GL_ON_EXCH_FED_PYMT_DTL
# MAGIC 
# MAGIC Job Name:
# MAGIC IdsGlOnExchLoad
# MAGIC Read the Load ready file created in Extract job
# MAGIC Load file created in IdsGlOnExchExtr job will be loaded into the target SYBASE table GL_ON_EXCH_FED_PYMT_DTL
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DecimalType,
    IntegerType,
    DateType
)
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


bcbsfin_owner = get_widget_value('BCBSFINOwner','')
bcbsfin_secret_name = get_widget_value('bcbsfin_secret_name','')

jdbc_url, jdbc_props = get_db_config(bcbsfin_secret_name)

schema_Seq_GlOnExchFedPymtDtl = StructType([
    StructField("LOBD_ID", StringType(), nullable=False),
    StructField("PDBL_ACCT_CAT", StringType(), nullable=False),
    StructField("SNAP_SRC_CD", StringType(), nullable=False),
    StructField("FA_SUB_SRC_CD", StringType(), nullable=False),
    StructField("MAP_FLD_1_CD", StringType(), nullable=False),
    StructField("MAP_FLD_2_CD", StringType(), nullable=False),
    StructField("MAP_FLD_3_CD", StringType(), nullable=False),
    StructField("MAP_FLD_4_CD", StringType(), nullable=False),
    StructField("MAP_EFF_DT", TimestampType(), nullable=False),
    StructField("CMS_ENR_PAYMT_DTL_CK", DecimalType(38,10), nullable=False),
    StructField("EXCH_MBR_ID", StringType(), nullable=False),
    StructField("GRGR_ID", StringType(), nullable=False),
    StructField("GL_CAT_CD", StringType(), nullable=False),
    StructField("FCTS_SUB_ID", StringType(), nullable=False),
    StructField("TX_AMT", DecimalType(38,10), nullable=False),
    StructField("SNAP_ACT_DT", TimestampType(), nullable=False),
    StructField("DR_CR_CD", StringType(), nullable=False),
    StructField("PAYMT_SUBMT_DT", DateType(), nullable=False),
    StructField("PDPD_ID", StringType(), nullable=False),
    StructField("PDBL_ID", StringType(), nullable=False),
    StructField("BLEI_CK", IntegerType(), nullable=False),
    StructField("BLBL_DUE_DT", TimestampType(), nullable=False),
    StructField("COV_STRT_DT", TimestampType(), nullable=False),
    StructField("COV_END_DT", TimestampType(), nullable=False),
    StructField("PAYMT_COV_YRMO", StringType(), nullable=False),
    StructField("BLAC_CREATE_DTM", TimestampType(), nullable=False),
    StructField("GRGR_NAME", StringType(), nullable=False),
    StructField("SGSG_ID", StringType(), nullable=False),
    StructField("QHP_ID", StringType(), nullable=False),
    StructField("ST_CD", StringType(), nullable=False),
    StructField("RVSED_ACTG_CAT_CD", StringType(), nullable=False),
    StructField("MAP_IN", StringType(), nullable=False),
    StructField("BUSINESS_UNIT", StringType(), nullable=False),
    StructField("TRANSACTION_ID", StringType(), nullable=False),
    StructField("TRANSACTION_LINE", IntegerType(), nullable=False),
    StructField("ACCOUNTING_DT", TimestampType(), nullable=False),
    StructField("APPL_JRNL_ID", StringType(), nullable=False),
    StructField("BUSINESS_UNIT_GL", StringType(), nullable=False),
    StructField("ACCOUNT", StringType(), nullable=False),
    StructField("DEPTID", StringType(), nullable=False),
    StructField("OPERATING_UNIT", StringType(), nullable=False),
    StructField("PRODUCT", StringType(), nullable=False),
    StructField("AFFILIATE", StringType(), nullable=False),
    StructField("CHARTFIELD1", StringType(), nullable=False),
    StructField("CHARTFIELD2", StringType(), nullable=False),
    StructField("CHARTFIELD3", StringType(), nullable=False),
    StructField("PROJECT_ID", StringType(), nullable=False),
    StructField("CURRENCY_CD", StringType(), nullable=False),
    StructField("RT_TYPE", StringType(), nullable=False),
    StructField("MONETARY_AMOUNT", DecimalType(38,10), nullable=False),
    StructField("FOREIGN_AMOUNT", DecimalType(38,10), nullable=False),
    StructField("JRNL_LN_REF", StringType(), nullable=False),
    StructField("LINE_DESCR", StringType(), nullable=False),
    StructField("GL_DISTRIB_STATUS", StringType(), nullable=False),
    StructField("CMS_ENR_PAYMT_AMT_SEQ_NO", DecimalType(38,10), nullable=True),
    StructField("WD_LOB_CD", StringType(), nullable=True),
    StructField("WD_ACCT_DTM", TimestampType(), nullable=True),
    StructField("WD_ACCT_NO", StringType(), nullable=True),
    StructField("WD_CC_ID", StringType(), nullable=True),
    StructField("WD_DNR_ID", StringType(), nullable=True),
    StructField("WD_AFFIL_ID", StringType(), nullable=True),
    StructField("WD_CUST_ID", StringType(), nullable=True),
    StructField("WD_JRNL_TYP_CD", StringType(), nullable=True),
    StructField("WD_JRNL_DESC", StringType(), nullable=True),
    StructField("WD_BANK_ACCT_NM", StringType(), nullable=True),
    StructField("WD_RVNU_CAT_ID", StringType(), nullable=True),
    StructField("WD_SPEND_CAT_ID", StringType(), nullable=True)
])

df_Seq_GlOnExchFedPymtDtl = (
    spark.read.format("csv")
    .schema(schema_Seq_GlOnExchFedPymtDtl)
    .option("header", "true")
    .option("delimiter", ",")
    .option("quote", "\"")
    .load(f"{adls_path}/load/GlOnExchFedPymtDtl.dat")
)

df_cpyforBuffer = df_Seq_GlOnExchFedPymtDtl.select(
    rpad(col("LOBD_ID"), 4, " ").alias("LOBD_ID"),
    rpad(col("PDBL_ACCT_CAT"), 4, " ").alias("PDBL_ACCT_CAT"),
    rpad(col("SNAP_SRC_CD"), 3, " ").alias("SNAP_SRC_CD"),
    rpad(col("FA_SUB_SRC_CD"), 5, " ").alias("FA_SUB_SRC_CD"),
    rpad(col("MAP_FLD_1_CD"), 4, " ").alias("MAP_FLD_1_CD"),
    rpad(col("MAP_FLD_2_CD"), 4, " ").alias("MAP_FLD_2_CD"),
    rpad(col("MAP_FLD_3_CD"), 4, " ").alias("MAP_FLD_3_CD"),
    rpad(col("MAP_FLD_4_CD"), 4, " ").alias("MAP_FLD_4_CD"),
    col("MAP_EFF_DT").alias("MAP_EFF_DT"),
    col("CMS_ENR_PAYMT_DTL_CK").alias("CMS_ENR_PAYMT_DTL_CK"),
    col("EXCH_MBR_ID").alias("EXCH_MBR_ID"),
    rpad(col("GRGR_ID"), 8, " ").alias("GRGR_ID"),
    rpad(col("GL_CAT_CD"), 4, " ").alias("GL_CAT_CD"),
    col("FCTS_SUB_ID").alias("FCTS_SUB_ID"),
    col("TX_AMT").alias("TX_AMT"),
    col("SNAP_ACT_DT").alias("SNAP_ACT_DT"),
    rpad(col("DR_CR_CD"), 2, " ").alias("DR_CR_CD"),
    col("PAYMT_SUBMT_DT").alias("PAYMT_SUBMT_DT"),
    rpad(col("PDPD_ID"), 8, " ").alias("PDPD_ID"),
    rpad(col("PDBL_ID"), 4, " ").alias("PDBL_ID"),
    col("BLEI_CK").alias("BLEI_CK"),
    col("BLBL_DUE_DT").alias("BLBL_DUE_DT"),
    col("COV_STRT_DT").alias("COV_STRT_DT"),
    col("COV_END_DT").alias("COV_END_DT"),
    rpad(col("PAYMT_COV_YRMO"), 6, " ").alias("PAYMT_COV_YRMO"),
    col("BLAC_CREATE_DTM").alias("BLAC_CREATE_DTM"),
    rpad(col("GRGR_NAME"), 50, " ").alias("GRGR_NAME"),
    rpad(col("SGSG_ID"), 4, " ").alias("SGSG_ID"),
    col("QHP_ID").alias("QHP_ID"),
    col("ST_CD").alias("ST_CD"),
    col("RVSED_ACTG_CAT_CD").alias("RVSED_ACTG_CAT_CD"),
    rpad(col("MAP_IN"), 1, " ").alias("MAP_IN"),
    rpad(col("BUSINESS_UNIT"), 5, " ").alias("BUSINESS_UNIT"),
    rpad(col("TRANSACTION_ID"), 10, " ").alias("TRANSACTION_ID"),
    col("TRANSACTION_LINE").alias("TRANSACTION_LINE"),
    col("ACCOUNTING_DT").alias("ACCOUNTING_DT"),
    rpad(col("APPL_JRNL_ID"), 10, " ").alias("APPL_JRNL_ID"),
    rpad(col("BUSINESS_UNIT_GL"), 5, " ").alias("BUSINESS_UNIT_GL"),
    rpad(col("ACCOUNT"), 10, " ").alias("ACCOUNT"),
    rpad(col("DEPTID"), 10, " ").alias("DEPTID"),
    rpad(col("OPERATING_UNIT"), 8, " ").alias("OPERATING_UNIT"),
    rpad(col("PRODUCT"), 6, " ").alias("PRODUCT"),
    rpad(col("AFFILIATE"), 5, " ").alias("AFFILIATE"),
    rpad(col("CHARTFIELD1"), 10, " ").alias("CHARTFIELD1"),
    rpad(col("CHARTFIELD2"), 10, " ").alias("CHARTFIELD2"),
    rpad(col("CHARTFIELD3"), 10, " ").alias("CHARTFIELD3"),
    rpad(col("PROJECT_ID"), 15, " ").alias("PROJECT_ID"),
    rpad(col("CURRENCY_CD"), 3, " ").alias("CURRENCY_CD"),
    rpad(col("RT_TYPE"), 5, " ").alias("RT_TYPE"),
    col("MONETARY_AMOUNT").alias("MONETARY_AMOUNT"),
    col("FOREIGN_AMOUNT").alias("FOREIGN_AMOUNT"),
    rpad(col("JRNL_LN_REF"), 10, " ").alias("JRNL_LN_REF"),
    rpad(col("LINE_DESCR"), 30, " ").alias("LINE_DESCR"),
    rpad(col("GL_DISTRIB_STATUS"), 1, " ").alias("GL_DISTRIB_STATUS"),
    col("CMS_ENR_PAYMT_AMT_SEQ_NO").alias("CMS_ENR_PAYMT_AMT_SEQ_NO"),
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

df_GL_ON_EXCH_FED_PYMT_DTL = df_cpyforBuffer

df_GL_ON_EXCH_FED_PYMT_DTL_final = df_GL_ON_EXCH_FED_PYMT_DTL.select(
    rpad(col("LOBD_ID"), 4, " ").alias("LOBD_ID"),
    rpad(col("PDBL_ACCT_CAT"), 4, " ").alias("PDBL_ACCT_CAT"),
    rpad(col("SNAP_SRC_CD"), 3, " ").alias("SNAP_SRC_CD"),
    rpad(col("FA_SUB_SRC_CD"), 5, " ").alias("FA_SUB_SRC_CD"),
    rpad(col("MAP_FLD_1_CD"), 4, " ").alias("MAP_FLD_1_CD"),
    rpad(col("MAP_FLD_2_CD"), 4, " ").alias("MAP_FLD_2_CD"),
    rpad(col("MAP_FLD_3_CD"), 4, " ").alias("MAP_FLD_3_CD"),
    rpad(col("MAP_FLD_4_CD"), 4, " ").alias("MAP_FLD_4_CD"),
    col("MAP_EFF_DT").alias("MAP_EFF_DT"),
    col("CMS_ENR_PAYMT_DTL_CK").alias("CMS_ENR_PAYMT_DTL_CK"),
    col("EXCH_MBR_ID").alias("EXCH_MBR_ID"),
    rpad(col("GRGR_ID"), 8, " ").alias("GRGR_ID"),
    rpad(col("GL_CAT_CD"), 4, " ").alias("GL_CAT_CD"),
    col("FCTS_SUB_ID").alias("FCTS_SUB_ID"),
    col("TX_AMT").alias("TX_AMT"),
    col("SNAP_ACT_DT").alias("SNAP_ACT_DT"),
    rpad(col("DR_CR_CD"), 2, " ").alias("DR_CR_CD"),
    col("PAYMT_SUBMT_DT").alias("PAYMT_SUBMT_DT"),
    rpad(col("PDPD_ID"), 8, " ").alias("PDPD_ID"),
    rpad(col("PDBL_ID"), 4, " ").alias("PDBL_ID"),
    col("BLEI_CK").alias("BLEI_CK"),
    col("BLBL_DUE_DT").alias("BLBL_DUE_DT"),
    col("COV_STRT_DT").alias("COV_STRT_DT"),
    col("COV_END_DT").alias("COV_END_DT"),
    rpad(col("PAYMT_COV_YRMO"), 6, " ").alias("PAYMT_COV_YRMO"),
    col("BLAC_CREATE_DTM").alias("BLAC_CREATE_DTM"),
    rpad(col("GRGR_NAME"), 50, " ").alias("GRGR_NAME"),
    rpad(col("SGSG_ID"), 4, " ").alias("SGSG_ID"),
    col("QHP_ID").alias("QHP_ID"),
    col("ST_CD").alias("ST_CD"),
    col("RVSED_ACTG_CAT_CD").alias("RVSED_ACTG_CAT_CD"),
    rpad(col("MAP_IN"), 1, " ").alias("MAP_IN"),
    rpad(col("BUSINESS_UNIT"), 5, " ").alias("BUSINESS_UNIT"),
    rpad(col("TRANSACTION_ID"), 10, " ").alias("TRANSACTION_ID"),
    col("TRANSACTION_LINE").alias("TRANSACTION_LINE"),
    col("ACCOUNTING_DT").alias("ACCOUNTING_DT"),
    rpad(col("APPL_JRNL_ID"), 10, " ").alias("APPL_JRNL_ID"),
    rpad(col("BUSINESS_UNIT_GL"), 5, " ").alias("BUSINESS_UNIT_GL"),
    rpad(col("ACCOUNT"), 10, " ").alias("ACCOUNT"),
    rpad(col("DEPTID"), 10, " ").alias("DEPTID"),
    rpad(col("OPERATING_UNIT"), 8, " ").alias("OPERATING_UNIT"),
    rpad(col("PRODUCT"), 6, " ").alias("PRODUCT"),
    rpad(col("AFFILIATE"), 5, " ").alias("AFFILIATE"),
    rpad(col("CHARTFIELD1"), 10, " ").alias("CHARTFIELD1"),
    rpad(col("CHARTFIELD2"), 10, " ").alias("CHARTFIELD2"),
    rpad(col("CHARTFIELD3"), 10, " ").alias("CHARTFIELD3"),
    rpad(col("PROJECT_ID"), 15, " ").alias("PROJECT_ID"),
    rpad(col("CURRENCY_CD"), 3, " ").alias("CURRENCY_CD"),
    rpad(col("RT_TYPE"), 5, " ").alias("RT_TYPE"),
    col("MONETARY_AMOUNT").alias("MONETARY_AMOUNT"),
    col("FOREIGN_AMOUNT").alias("FOREIGN_AMOUNT"),
    rpad(col("JRNL_LN_REF"), 10, " ").alias("JRNL_LN_REF"),
    rpad(col("LINE_DESCR"), 30, " ").alias("LINE_DESCR"),
    rpad(col("GL_DISTRIB_STATUS"), 1, " ").alias("GL_DISTRIB_STATUS"),
    col("CMS_ENR_PAYMT_AMT_SEQ_NO").alias("CMS_ENR_PAYMT_AMT_SEQ_NO"),
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

execute_dml(
    f"DROP TABLE IF EXISTS STAGING.IdsGlOnExchLoad_GL_ON_EXCH_FED_PYMT_DTL_temp",
    jdbc_url,
    jdbc_props
)

df_GL_ON_EXCH_FED_PYMT_DTL_final.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.IdsGlOnExchLoad_GL_ON_EXCH_FED_PYMT_DTL_temp") \
    .mode("overwrite") \
    .save()

merge_sql = """
MERGE INTO {owner}.GL_ON_EXCH_FED_PYMT_DTL AS T
USING STAGING.IdsGlOnExchLoad_GL_ON_EXCH_FED_PYMT_DTL_temp AS S
ON 1=0
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
    CMS_ENR_PAYMT_DTL_CK,
    EXCH_MBR_ID,
    GRGR_ID,
    GL_CAT_CD,
    FCTS_SUB_ID,
    TX_AMT,
    SNAP_ACT_DT,
    DR_CR_CD,
    PAYMT_SUBMT_DT,
    PDPD_ID,
    PDBL_ID,
    BLEI_CK,
    BLBL_DUE_DT,
    COV_STRT_DT,
    COV_END_DT,
    PAYMT_COV_YRMO,
    BLAC_CREATE_DTM,
    GRGR_NAME,
    SGSG_ID,
    QHP_ID,
    ST_CD,
    RVSED_ACTG_CAT_CD,
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
    CMS_ENR_PAYMT_AMT_SEQ_NO,
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
    S.CMS_ENR_PAYMT_DTL_CK,
    S.EXCH_MBR_ID,
    S.GRGR_ID,
    S.GL_CAT_CD,
    S.FCTS_SUB_ID,
    S.TX_AMT,
    S.SNAP_ACT_DT,
    S.DR_CR_CD,
    S.PAYMT_SUBMT_DT,
    S.PDPD_ID,
    S.PDBL_ID,
    S.BLEI_CK,
    S.BLBL_DUE_DT,
    S.COV_STRT_DT,
    S.COV_END_DT,
    S.PAYMT_COV_YRMO,
    S.BLAC_CREATE_DTM,
    S.GRGR_NAME,
    S.SGSG_ID,
    S.QHP_ID,
    S.ST_CD,
    S.RVSED_ACTG_CAT_CD,
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
    S.CMS_ENR_PAYMT_AMT_SEQ_NO,
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
""".format(owner=bcbsfin_owner)

execute_dml(merge_sql, jdbc_url, jdbc_props)