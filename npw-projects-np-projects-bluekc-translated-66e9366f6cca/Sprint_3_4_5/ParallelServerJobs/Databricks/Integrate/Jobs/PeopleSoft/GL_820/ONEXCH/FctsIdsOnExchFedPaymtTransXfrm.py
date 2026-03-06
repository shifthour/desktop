# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Called By:FctsBcbsExtOnExchExtrLoadSeq
# MAGIC 
# MAGIC 
# MAGIC Process Description: Extract Data from (GL_ON_EXCH_FED_PYMT_DTL) and Load it to IDS 
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                               Project/                                                                                                                        Code                  Date
# MAGIC Developer                    Date                   Altiris #     Change Description                                                                                     Reviewer            Reviewed
# MAGIC -----------------------              -------------------    -------------   -----------------------------------------------------------------------------------------------------------------   -------------------------  -------------------
# MAGIC sudheer champati        06/10/2014       5128       Originally Programmed                                                                                  Kalyan Neelam  2015-06-17
# MAGIC 
# MAGIC kailashnath Jadhav      17/12/2015       5128       Updated derivation for attribute                                                                   Jag Yelavarthi     2015-12-23
# MAGIC 
# MAGIC                                                                               RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD_SK   
# MAGIC 
# MAGIC Aishwarya                     29/12/2015        5128     CD_MPG query modified - SUBSTR on SRC_CD and added remove        Jag Yelavarthi     2015-12-30
# MAGIC                                                                              duplicates on SRC_CD
# MAGIC 
# MAGIC Aishwarya                     11/01/2016        5128     Trim applied on FA_SUB_SRC_CD                                                            Jag Yelavarthi       2016-01-11

# MAGIC All the Businees rules are applied in this Extract before generating the Pimaray key to Load to the FinalTable.
# MAGIC The Datastet is used in the FctsIdsOnExchFedPaymtTransPkey job and is deleted at the CTRL level.
# MAGIC Datastet generated in job FctsIdsOnExchFedPaymtTransExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as pF
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
RunIDTimeStamp = get_widget_value('RunIDTimeStamp','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""SELECT DISTINCT SRC_CD, CD_MPPNG_SK, SRC_CLCTN_CD, SRC_SYS_CD, TRGT_CLCTN_CD, TRGT_DOMAIN_NM
FROM {IDSOwner}.CD_MPPNG
Where
SRC_CLCTN_CD = 'CMS' AND
SRC_SYS_CD = 'CMS' AND
TRGT_CLCTN_CD = 'IDS' AND
TRGT_DOMAIN_NM = 'ENROLLMENT PAYMENT TYPE'
"""

df_DB2_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_ds_ONEX_Extr = spark.read.parquet(f"{adls_path}/ds/ON_EXCH_FED_PYMT_TRANS.{SrcSysCd}.extr.{RunID}.parquet")

df_StripField = df_ds_ONEX_Extr.select(
    pF.col("GL_ON_EXCH_FED_PYMT_DTL_CK").alias("GL_ON_EXCH_FED_PYMT_DTL_CK"),
    pF.col("ACCOUNTING_DT").alias("ACCOUNTING_DT"),
    pF.col("PDBL_ACCT_CAT").alias("PDBL_ACCT_CAT"),
    pF.col("FA_SUB_SRC_CD").alias("FA_SUB_SRC_CD"),
    pF.col("EXCH_MBR_ID").alias("EXCH_MBR_ID"),
    pF.col("FCTS_SUB_ID").alias("FCTS_SUB_ID"),
    pF.col("COV_STRT_DT").alias("COV_STRT_DT"),
    pF.col("COV_END_DT").alias("COV_END_DT"),
    pF.col("GRGR_ID").alias("GRGR_ID"),
    pF.col("GRGR_NAME").alias("GRGR_NAME"),
    pF.col("PDPD_ID").alias("PDPD_ID")
)

df_StripField = (
    df_StripField
    .withColumn("GL_ON_EXCH_FED_PYMT_DTL_CK", strip_field(pF.col("GL_ON_EXCH_FED_PYMT_DTL_CK")))
    .withColumn("PDBL_ACCT_CAT", trim(strip_field(pF.col("PDBL_ACCT_CAT"))))
    .withColumn("FA_SUB_SRC_CD", trim(strip_field(pF.col("FA_SUB_SRC_CD"))))
    .withColumn("EXCH_MBR_ID", trim(strip_field(pF.col("EXCH_MBR_ID"))))
    .withColumn("FCTS_SUB_ID", trim(strip_field(pF.col("FCTS_SUB_ID"))))
    .withColumn("GRGR_ID", trim(strip_field(pF.col("GRGR_ID"))))
    .withColumn("PDPD_ID", trim(strip_field(pF.col("PDPD_ID"))))
    .withColumn("GRGR_NAME", trim(strip_field(pF.col("GRGR_NAME"))))
)

df_Lkp_FA_SUB_SRC_CD = df_StripField.alias("lnk_FA_SUB_SRC_CD_out").join(
    df_DB2_CD_MPPNG.alias("LnkFrom_CD_MPPNG"),
    (pF.col("lnk_FA_SUB_SRC_CD_out.FA_SUB_SRC_CD") == pF.col("LnkFrom_CD_MPPNG.SRC_CD"))
    & (pF.col("lnk_FA_SUB_SRC_CD_out.FA_SUB_SRC_CD") == pF.col("LnkFrom_CD_MPPNG.CD_MPPNG_SK")),
    "left"
)

df_Lkp_FA_SUB_SRC_CD = df_Lkp_FA_SUB_SRC_CD.select(
    pF.col("lnk_FA_SUB_SRC_CD_out.GL_ON_EXCH_FED_PYMT_DTL_CK").alias("GL_ON_EXCH_FED_PYMT_DTL_CK"),
    pF.col("lnk_FA_SUB_SRC_CD_out.ACCOUNTING_DT").alias("ACCOUNTING_DT"),
    pF.col("lnk_FA_SUB_SRC_CD_out.PDBL_ACCT_CAT").alias("PDBL_ACCT_CAT"),
    pF.col("LnkFrom_CD_MPPNG.CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    pF.col("lnk_FA_SUB_SRC_CD_out.EXCH_MBR_ID").alias("EXCH_MBR_ID"),
    pF.col("lnk_FA_SUB_SRC_CD_out.FCTS_SUB_ID").alias("FCTS_SUB_ID"),
    pF.col("lnk_FA_SUB_SRC_CD_out.COV_STRT_DT").alias("COV_STRT_DT"),
    pF.col("lnk_FA_SUB_SRC_CD_out.COV_END_DT").alias("COV_END_DT"),
    pF.col("lnk_FA_SUB_SRC_CD_out.GRGR_ID").alias("GRGR_ID"),
    pF.col("lnk_FA_SUB_SRC_CD_out.PDPD_ID").alias("PDPD_ID"),
    pF.col("lnk_FA_SUB_SRC_CD_out.GRGR_NAME").alias("GRGR_NAME")
)

df_Xfrm_BusinessRules = (
    df_Lkp_FA_SUB_SRC_CD
    .withColumn("svACCTGDT", format_date_ee(pF.col("ACCOUNTING_DT"), pF.lit("SYBASE"), pF.lit("TIMESTAMP"), pF.lit("DATE")))
    .select(
        pF.concat_ws(";", pF.lit(SrcSysCd), pF.col("GL_ON_EXCH_FED_PYMT_DTL_CK"), pF.col("svACCTGDT")).alias("PRI_NAT_KEY_STRING"),
        pF.lit(RunIDTimeStamp).alias("FIRST_RECYC_TS"),
        strip_field(pF.col("GL_ON_EXCH_FED_PYMT_DTL_CK")).alias("ON_EXCH_FED_PAYMT_TRANS_CK"),
        pF.col("ACCOUNTING_DT").alias("ACCTG_DT_SK"),
        pF.lit(SrcSysCdSk).alias("SOURCE_SYSTEM_CODE_SK"),
        pF.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        pF.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        pF.lit("FNCL_LOB_SK").alias("FNCL_LOB_SK"),
        pF.lit("GRP_SK").alias("GRP_SK"),
        pF.lit("PROD_SK").alias("PROD_SK"),
        pF.when(pF.col("CD_MPPNG_SK").isNull(), pF.lit(0)).otherwise(pF.col("CD_MPPNG_SK")).alias("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD_SK"),
        pF.col("COV_STRT_DT").alias("COV_STRT_DT_SK"),
        pF.col("COV_END_DT").alias("COV_END_DT_SK"),
        pF.col("PDBL_ACCT_CAT").alias("PROD_LOB_NO"),
        pF.col("EXCH_MBR_ID").alias("EXCH_MBR_ID"),
        pF.col("FCTS_SUB_ID").alias("SUB_ID"),
        pF.col("GRGR_ID").alias("GRGR_ID"),
        pF.col("PDPD_ID").alias("PDPD_ID"),
        pF.lit("FACETS").alias("SRC_SYS_CD"),
        pF.col("GRGR_NAME").alias("GRGR_NAME")
    )
)

df_final = (
    df_Xfrm_BusinessRules
    .withColumn("ACCTG_DT_SK", pF.rpad(pF.col("ACCTG_DT_SK"), 10, " "))
    .withColumn("COV_STRT_DT_SK", pF.rpad(pF.col("COV_STRT_DT_SK"), 10, " "))
    .withColumn("COV_END_DT_SK", pF.rpad(pF.col("COV_END_DT_SK"), 10, " "))
    .withColumn("PROD_LOB_NO", pF.rpad(pF.col("PROD_LOB_NO"), 4, " "))
    .withColumn("GRGR_ID", pF.rpad(pF.col("GRGR_ID"), 8, " "))
    .withColumn("PDPD_ID", pF.rpad(pF.col("PDPD_ID"), 8, " "))
    .select(
        "PRI_NAT_KEY_STRING",
        "FIRST_RECYC_TS",
        "ON_EXCH_FED_PAYMT_TRANS_CK",
        "ACCTG_DT_SK",
        "SOURCE_SYSTEM_CODE_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "FNCL_LOB_SK",
        "GRP_SK",
        "PROD_SK",
        "RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD_SK",
        "COV_STRT_DT_SK",
        "COV_END_DT_SK",
        "PROD_LOB_NO",
        "EXCH_MBR_ID",
        "SUB_ID",
        "GRGR_ID",
        "PDPD_ID",
        "SRC_SYS_CD",
        "GRGR_NAME"
    )
)

write_files(
    df_final,
    f"{adls_path}/ds/ON_EXCH_FED_PYMT_TRANS.{SrcSysCd}.xfrm.{RunID}.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)