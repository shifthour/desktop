# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC CALLED BY: IdsMaMMRRcvdIncmSbsdyDtlCntl
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   As a part of the MA/ACA in-sourcing, this process will load the Monthly Membership (MMR) file from CMS.  This file contains monthly subsidy data at the beneficiary/member level.  Data needs to be loaded to IDS table, RCVD_INCM_SUBSDY_DTL to be included in the in the GL process for reconciliation and finanical reporting. 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Sai Madhu Yelakkayala   11/02/2020         US-243707                     Originally Programmed                           ids_dev2                     Jeyaprasanna             2020-12-15

# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC A concious effort need to be made to evaluate which one of the duplicate set row to drop is a case by case basis decision.
# MAGIC Table K_RCVD_INCM_SBSDY_DTL.Insert new SKEY rows generated as part of this run. This is a database insert operation.
# MAGIC PKEY out file for FKEY job consumption
# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC Left Outer Join Here
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
RunId = get_widget_value('RunId','')
ids_secret_name = get_widget_value('ids_secret_name','')

df_ds_RcvdIncmSbsdyDtl_Xfm = spark.read.parquet(f"{adls_path}/ds/RCVD_INCM_SBSDY_DTL_{RunId}.parquet")

df_cpy_MultiStreams = df_ds_RcvdIncmSbsdyDtl_Xfm

df_lnkFullDataJnIn = df_cpy_MultiStreams.select(
    F.col("CMS_ENR_PAYMT_UNIQ_KEY").alias("CMS_ENR_PAYMT_UNIQ_KEY"),
    F.col("CMS_ENR_PAYMT_AMT_SEQ_NO").alias("CMS_ENR_PAYMT_AMT_SEQ_NO"),
    F.col("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD").alias("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("RCVD_INCM_SBSDY_ACCT_ACTVTY_CD").alias("RCVD_INCM_SBSDY_ACCT_ACTVTY_CD"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("BILL_ENTY_SK").alias("BILL_ENTY_SK"),
    F.col("CLS_PLN_SK").alias("CLS_PLN_SK"),
    F.col("FEE_DSCNT_SK").alias("FEE_DSCNT_SK"),
    F.col("FNCL_LOB_SK").alias("FNCL_LOB_SK"),
    F.col("PROD_SK").alias("PROD_SK"),
    F.col("QHP_SK").alias("QHP_SK"),
    F.col("RCVD_INCM_SBSDY_ACCT_ACTVTY_CD_SK").alias("RCVD_INCM_SBSDY_ACCT_ACTVTY_CD_SK"),
    F.col("RCVD_INCM_SBSDY_ACCT_TYP_CD_SK").alias("RCVD_INCM_SBSDY_ACCT_TYP_CD_SK"),
    F.col("RCVD_INCM_SBSDY_ACTV_SRC_CD_SK").alias("RCVD_INCM_SBSDY_ACTV_SRC_CD_SK"),
    F.col("RCVD_INCM_SBSDY_ACTV_TYP_CD_SK").alias("RCVD_INCM_SBSDY_ACTV_TYP_CD_SK"),
    F.col("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD_SK").alias("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD_SK"),
    F.col("RCVD_INCM_SBSDY_LOB_CD_SK").alias("RCVD_INCM_SBSDY_LOB_CD_SK"),
    F.col("FIRST_YR_IN").alias("FIRST_YR_IN"),
    F.col("BILL_DUE_DT_SK").alias("BILL_DUE_DT_SK"),
    F.col("CRT_DT_SK").alias("CRT_DT_SK"),
    F.col("POSTED_DT_SK").alias("POSTED_DT_SK"),
    F.col("ERN_INCM_AMT").alias("ERN_INCM_AMT"),
    F.col("RVNU_RCVD_AMT").alias("RVNU_RCVD_AMT"),
    F.col("BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
    F.col("BILL_GRP_BILL_ENTY_UNIQ_KEY").alias("BILL_GRP_BILL_ENTY_UNIQ_KEY"),
    F.col("EXCH_MBR_ID").alias("EXCH_MBR_ID"),
    F.col("EXCH_SUB_ID").alias("EXCH_SUB_ID"),
    F.col("EXCH_POL_ID").alias("EXCH_POL_ID"),
    F.col("GL_NO").alias("GL_NO"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("PROD_BILL_CMPNT_ID").alias("PROD_BILL_CMPNT_ID"),
    F.col("QHP_ID").alias("QHP_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

df_rdp_in = df_cpy_MultiStreams.select(
    F.col("CMS_ENR_PAYMT_UNIQ_KEY").alias("CMS_ENR_PAYMT_UNIQ_KEY"),
    F.col("CMS_ENR_PAYMT_AMT_SEQ_NO").alias("CMS_ENR_PAYMT_AMT_SEQ_NO"),
    F.col("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD").alias("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD"),
    F.col("RCVD_INCM_SBSDY_ACCT_ACTVTY_CD").alias("RCVD_INCM_SBSDY_ACCT_ACTVTY_CD"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

ids_jdbc_url, ids_jdbc_props = get_db_config(ids_secret_name)
extract_query_db2_K_RCVD_INCM_SBSDY_DTL_In = (
    "select CMS_ENR_PAYMT_UNIQ_KEY, CMS_ENR_PAYMT_AMT_SEQ_NO, "
    "RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD, RCVD_INCM_SBSDY_ACCT_ACTVTY_CD, "
    "SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, RCVD_INCM_SBSDY_DTL_SK "
    f"from {IDSOwner}.K_RCVD_INCM_SBSDY_DTL"
)
df_db2_K_RCVD_INCM_SBSDY_DTL_In = (
    spark.read.format("jdbc")
    .option("url", ids_jdbc_url)
    .options(**ids_jdbc_props)
    .option("query", extract_query_db2_K_RCVD_INCM_SBSDY_DTL_In)
    .load()
)

df_Rdp_Naturalkeys_dedup = dedup_sort(
    df_rdp_in,
    ["CMS_ENR_PAYMT_UNIQ_KEY","CMS_ENR_PAYMT_AMT_SEQ_NO","RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD","RCVD_INCM_SBSDY_ACCT_ACTVTY_CD","SRC_SYS_CD"],
    []
)
df_Rdp_Naturalkeys = df_Rdp_Naturalkeys_dedup.select(
    F.col("CMS_ENR_PAYMT_UNIQ_KEY").alias("CMS_ENR_PAYMT_UNIQ_KEY"),
    F.col("CMS_ENR_PAYMT_AMT_SEQ_NO").alias("CMS_ENR_PAYMT_AMT_SEQ_NO"),
    F.col("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD").alias("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD"),
    F.col("RCVD_INCM_SBSDY_ACCT_ACTVTY_CD").alias("RCVD_INCM_SBSDY_ACCT_ACTVTY_CD"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

df_jn_RcvdIncm = df_Rdp_Naturalkeys.alias("lnkRemDupDataOut").join(
    df_db2_K_RCVD_INCM_SBSDY_DTL_In.alias("lnkKRcvdIncmSbsdyDtl"),
    on=[
        F.col("lnkRemDupDataOut.CMS_ENR_PAYMT_UNIQ_KEY") == F.col("lnkKRcvdIncmSbsdyDtl.CMS_ENR_PAYMT_UNIQ_KEY"),
        F.col("lnkRemDupDataOut.CMS_ENR_PAYMT_AMT_SEQ_NO") == F.col("lnkKRcvdIncmSbsdyDtl.CMS_ENR_PAYMT_AMT_SEQ_NO"),
        F.col("lnkRemDupDataOut.RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD") == F.col("lnkKRcvdIncmSbsdyDtl.RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD"),
        F.col("lnkRemDupDataOut.RCVD_INCM_SBSDY_ACCT_ACTVTY_CD") == F.col("lnkKRcvdIncmSbsdyDtl.RCVD_INCM_SBSDY_ACCT_ACTVTY_CD"),
        F.col("lnkRemDupDataOut.SRC_SYS_CD") == F.col("lnkKRcvdIncmSbsdyDtl.SRC_SYS_CD")
    ],
    how="left"
)

df_lnk_RCVD_JoinOut = df_jn_RcvdIncm.select(
    F.col("lnkRemDupDataOut.CMS_ENR_PAYMT_UNIQ_KEY").alias("CMS_ENR_PAYMT_UNIQ_KEY"),
    F.col("lnkRemDupDataOut.CMS_ENR_PAYMT_AMT_SEQ_NO").alias("CMS_ENR_PAYMT_AMT_SEQ_NO"),
    F.col("lnkRemDupDataOut.RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD").alias("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD"),
    F.col("lnkRemDupDataOut.RCVD_INCM_SBSDY_ACCT_ACTVTY_CD").alias("RCVD_INCM_SBSDY_ACCT_ACTVTY_CD"),
    F.col("lnkRemDupDataOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnkKRcvdIncmSbsdyDtl.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnkKRcvdIncmSbsdyDtl.RCVD_INCM_SBSDY_DTL_SK").alias("RCVD_INCM_SBSDY_DTL_SK")
)

df_enriched = df_lnk_RCVD_JoinOut.withColumn(
    "isnull_rcvd_incm_sbsdy_dtl_sk",
    F.col("RCVD_INCM_SBSDY_DTL_SK").isNull()
).withColumn(
    "svRunCycle",
    F.when(F.col("RCVD_INCM_SBSDY_DTL_SK").isNull(), F.lit(IDSRunCycle))
     .otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
)

df_enriched = SurrogateKeyGen(
    df_enriched,
    <DB sequence name>,
    "RCVD_INCM_SBSDY_DTL_SK",
    <schema>,
    <secret_name>
)

df_lnk_KRcvdIncmSubsdyDtl = df_enriched.filter(
    F.col("isnull_rcvd_incm_sbsdy_dtl_sk")
).select(
    F.col("CMS_ENR_PAYMT_UNIQ_KEY").alias("CMS_ENR_PAYMT_UNIQ_KEY"),
    F.col("CMS_ENR_PAYMT_AMT_SEQ_NO").alias("CMS_ENR_PAYMT_AMT_SEQ_NO"),
    F.col("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD").alias("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD"),
    F.col("RCVD_INCM_SBSDY_ACCT_ACTVTY_CD").alias("RCVD_INCM_SBSDY_ACCT_ACTVTY_CD"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("svRunCycle").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("RCVD_INCM_SBSDY_DTL_SK").alias("RCVD_INCM_SBSDY_DTL_SK")
)

df_lnkPKEYxfmOut = df_enriched.select(
    F.col("CMS_ENR_PAYMT_UNIQ_KEY").alias("CMS_ENR_PAYMT_UNIQ_KEY"),
    F.col("CMS_ENR_PAYMT_AMT_SEQ_NO").alias("CMS_ENR_PAYMT_AMT_SEQ_NO"),
    F.col("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD").alias("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD"),
    F.col("RCVD_INCM_SBSDY_ACCT_ACTVTY_CD").alias("RCVD_INCM_SBSDY_ACCT_ACTVTY_CD"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("svRunCycle").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK_1"),
    F.col("RCVD_INCM_SBSDY_DTL_SK").alias("RCVD_INCM_SBSDY_DTL_SK")
)

df_lnk_KRcvdIncmSubsdyDtl_temp_table = "STAGING.IdsRcvdIncmSbsdyDtlPkey_db2_K_RCVD_INCM_SBSDY_DTL_Load_temp"

spark.sql(f"DROP TABLE IF EXISTS {df_lnk_KRcvdIncmSubsdyDtl_temp_table}")

(
    df_lnk_KRcvdIncmSubsdyDtl.write.format("jdbc")
    .option("url", ids_jdbc_url)
    .options(**ids_jdbc_props)
    .option("dbtable", df_lnk_KRcvdIncmSubsdyDtl_temp_table)
    .mode("overwrite")
    .save()
)

pk_cols = [
    "T.CMS_ENR_PAYMT_UNIQ_KEY","T.CMS_ENR_PAYMT_AMT_SEQ_NO",
    "T.RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD","T.RCVD_INCM_SBSDY_ACCT_ACTVTY_CD","T.SRC_SYS_CD"
]

merge_sql_db2_K_RCVD_INCM_SBSDY_DTL_Load = (
    f"MERGE INTO {IDSOwner}.K_RCVD_INCM_SBSDY_DTL AS T "
    f"USING {df_lnk_KRcvdIncmSubsdyDtl_temp_table} AS S "
    "ON ("
    "T.CMS_ENR_PAYMT_UNIQ_KEY=S.CMS_ENR_PAYMT_UNIQ_KEY AND "
    "T.CMS_ENR_PAYMT_AMT_SEQ_NO=S.CMS_ENR_PAYMT_AMT_SEQ_NO AND "
    "T.RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD=S.RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD AND "
    "T.RCVD_INCM_SBSDY_ACCT_ACTVTY_CD=S.RCVD_INCM_SBSDY_ACCT_ACTVTY_CD AND "
    "T.SRC_SYS_CD=S.SRC_SYS_CD"
    ") "
    "WHEN NOT MATCHED THEN INSERT ( "
    "CMS_ENR_PAYMT_UNIQ_KEY, CMS_ENR_PAYMT_AMT_SEQ_NO, RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD, "
    "RCVD_INCM_SBSDY_ACCT_ACTVTY_CD, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, RCVD_INCM_SBSDY_DTL_SK "
    ") VALUES ( "
    "S.CMS_ENR_PAYMT_UNIQ_KEY, S.CMS_ENR_PAYMT_AMT_SEQ_NO, S.RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD, "
    "S.RCVD_INCM_SBSDY_ACCT_ACTVTY_CD, S.SRC_SYS_CD, S.CRT_RUN_CYC_EXCTN_SK, S.RCVD_INCM_SBSDY_DTL_SK "
    ");"
)

execute_dml(merge_sql_db2_K_RCVD_INCM_SBSDY_DTL_Load, ids_jdbc_url, ids_jdbc_props)

df_jn_PKEYs_intermediate = df_lnkFullDataJnIn.alias("lnkFullDataJnIn").join(
    df_lnkPKEYxfmOut.alias("lnkPKEYxfmOut"),
    on=[
        F.col("lnkFullDataJnIn.CMS_ENR_PAYMT_UNIQ_KEY") == F.col("lnkPKEYxfmOut.CMS_ENR_PAYMT_UNIQ_KEY"),
        F.col("lnkFullDataJnIn.CMS_ENR_PAYMT_AMT_SEQ_NO") == F.col("lnkPKEYxfmOut.CMS_ENR_PAYMT_AMT_SEQ_NO"),
        F.col("lnkFullDataJnIn.RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD") == F.col("lnkPKEYxfmOut.RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD"),
        F.col("lnkFullDataJnIn.RCVD_INCM_SBSDY_ACCT_ACTVTY_CD") == F.col("lnkPKEYxfmOut.RCVD_INCM_SBSDY_ACCT_ACTVTY_CD"),
        F.col("lnkFullDataJnIn.SRC_SYS_CD").alias("ignore") == F.col("lnkPKEYxfmOut.SRC_SYS_CD")
    ],
    how="inner"
)

df_seq_RCVD_INCM_SBSDY_DTL_PKEY = df_jn_PKEYs_intermediate.select(
    F.col("lnkPKEYxfmOut.RCVD_INCM_SBSDY_DTL_SK").alias("RCVD_INCM_SBSDY_DTL_SK"),
    F.col("lnkFullDataJnIn.CMS_ENR_PAYMT_UNIQ_KEY").alias("CMS_ENR_PAYMT_UNIQ_KEY"),
    F.col("lnkFullDataJnIn.CMS_ENR_PAYMT_AMT_SEQ_NO").alias("CMS_ENR_PAYMT_AMT_SEQ_NO"),
    F.col("lnkFullDataJnIn.RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD").alias("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD"),
    F.col("lnkFullDataJnIn.RCVD_INCM_SBSDY_ACCT_ACTVTY_CD").alias("RCVD_INCM_SBSDY_ACCT_ACTVTY_CD"),
    F.col("lnkFullDataJnIn.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnkPKEYxfmOut.LAST_UPDT_RUN_CYC_EXCTN_SK_1").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnkFullDataJnIn.BILL_ENTY_SK").alias("BILL_ENTY_SK"),
    F.col("lnkFullDataJnIn.CLS_PLN_SK").alias("CLS_PLN_SK"),
    F.col("lnkFullDataJnIn.FEE_DSCNT_SK").alias("FEE_DSCNT_SK"),
    F.col("lnkFullDataJnIn.FNCL_LOB_SK").alias("FNCL_LOB_SK"),
    F.col("lnkFullDataJnIn.PROD_SK").alias("PROD_SK"),
    F.col("lnkFullDataJnIn.QHP_SK").alias("QHP_SK"),
    F.col("lnkFullDataJnIn.RCVD_INCM_SBSDY_ACCT_ACTVTY_CD_SK").alias("RCVD_INCM_SBSDY_ACCT_ACTVTY_CD_SK"),
    F.col("lnkFullDataJnIn.RCVD_INCM_SBSDY_ACCT_TYP_CD_SK").alias("RCVD_INCM_SBSDY_ACCT_TYP_CD_SK"),
    F.col("lnkFullDataJnIn.RCVD_INCM_SBSDY_ACTV_SRC_CD_SK").alias("RCVD_INCM_SBSDY_ACTV_SRC_CD_SK"),
    F.col("lnkFullDataJnIn.RCVD_INCM_SBSDY_ACTV_TYP_CD_SK").alias("RCVD_INCM_SBSDY_ACTV_TYP_CD_SK"),
    F.col("lnkFullDataJnIn.RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD_SK").alias("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD_SK"),
    F.col("lnkFullDataJnIn.RCVD_INCM_SBSDY_LOB_CD_SK").alias("RCVD_INCM_SBSDY_LOB_CD_SK"),
    rpad(F.col("lnkFullDataJnIn.FIRST_YR_IN"), 1, " ").alias("FIRST_YR_IN"),
    rpad(F.col("lnkFullDataJnIn.BILL_DUE_DT_SK"), 10, " ").alias("BILL_DUE_DT_SK"),
    rpad(F.col("lnkFullDataJnIn.CRT_DT_SK"), 10, " ").alias("CRT_DT_SK"),
    rpad(F.col("lnkFullDataJnIn.POSTED_DT_SK"), 10, " ").alias("POSTED_DT_SK"),
    F.col("lnkFullDataJnIn.ERN_INCM_AMT").alias("ERN_INCM_AMT"),
    F.col("lnkFullDataJnIn.RVNU_RCVD_AMT").alias("RVNU_RCVD_AMT"),
    F.col("lnkFullDataJnIn.BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
    F.col("lnkFullDataJnIn.BILL_GRP_BILL_ENTY_UNIQ_KEY").alias("BILL_GRP_BILL_ENTY_UNIQ_KEY"),
    F.col("lnkFullDataJnIn.EXCH_MBR_ID").alias("EXCH_MBR_ID"),
    F.col("lnkFullDataJnIn.EXCH_SUB_ID").alias("EXCH_SUB_ID"),
    F.col("lnkFullDataJnIn.EXCH_POL_ID").alias("EXCH_POL_ID"),
    F.col("lnkFullDataJnIn.GL_NO").alias("GL_NO"),
    rpad(F.col("lnkFullDataJnIn.PROD_ID"), 8, " ").alias("PROD_ID"),
    F.col("lnkFullDataJnIn.PROD_BILL_CMPNT_ID").alias("PROD_BILL_CMPNT_ID"),
    F.col("lnkFullDataJnIn.QHP_ID").alias("QHP_ID")
)

write_files(
    df_seq_RCVD_INCM_SBSDY_DTL_PKEY,
    f"{adls_path}/load/RCVD_INCM_SBSDY_DTL_{RunId}.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=True,
    quote="\"",
    nullValue=None
)