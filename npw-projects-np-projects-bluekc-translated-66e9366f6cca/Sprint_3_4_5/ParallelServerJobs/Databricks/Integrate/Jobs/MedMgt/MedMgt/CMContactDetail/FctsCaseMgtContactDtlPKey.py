# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Called By: FctsMedMgtCntl
# MAGIC 
# MAGIC 
# MAGIC Process : Case Mgt Status Pkey process which is processed and loaded to CASE_MGT_CNTCT_DTL_LOG table
# MAGIC 
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                              		  Project/                                                                                                    	Code                       Date
# MAGIC Developer         	Date             	 Altiris #     	Change Description                                                   	Reviewer                Reviewed
# MAGIC -----------------------  	-------------------   	-------------   	-----------------------------------------------------------------------------   	-------------------------      -------------------
# MAGIC Akash Parsha        2019-09-03                 US140165              Case Mgt Contact PKey Process         			Jaideep Mankala   10/10/2019

# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC Insert Only the Newly generated Pkeys into the K Table
# MAGIC Read K_CASE_MGT_CNTCT_DTL_LOGTable to pull the Natural Keys and the Skey.
# MAGIC Inner Join primary key info with table info
# MAGIC Left Join on Natural Keys
# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC JobName: IdsProdQhpPkey
# MAGIC 
# MAGIC Strip field function and Tranforamtion rules are applied on the data Extracted from SBC
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, when, isnull, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
SrcSysCd = get_widget_value('SrcSysCd','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
RunID = get_widget_value('RunID','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_db2_K_CASE_MGT_CONTACT_DTL_in = f"""
SELECT 
CASE_MGT_ID,
CNTCT_SEQ_NO,
SRC_SYS_CD,
LOG_SEQ_NO,
CRT_RUN_CYC_EXCTN_SK,
CASE_MGT_CNTCT_DTL_LOG_SK
from {IDSOwner}.K_CASE_MGT_CNTCT_DTL_LOG
"""
df_db2_K_CASE_MGT_CONTACT_DTL_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_K_CASE_MGT_CONTACT_DTL_in.strip())
    .load()
)

df_CMCS_CONTACT_DTL_LOGExtr = spark.read.parquet(
    f"{adls_path}/ds/CASE_MGT_CNTCT_DTL_LOG.{SrcSysCd}.extr.{RunID}.parquet"
)

df_Cp_Pk_in = df_CMCS_CONTACT_DTL_LOGExtr

df_Cp_Pk_out_CP_Out = df_Cp_Pk_in.select(
    col("PRI_NAT_KEY_STRING"),
    col("FIRST_RECYC_TS"),
    col("CASE_MGT_ID"),
    col("CNTCT_SEQ_NO"),
    col("LOG_SEQ_NO"),
    col("LOG_DTM"),
    col("LOG_SUM_TX"),
    col("CMLG_MCTR_CALL"),
    col("CMLG_METHOD"),
    col("CMLG_USID"),
    col("SRC_SYS_CD")
)

df_Cp_Pk_out_lnk_Transforms_Out = df_Cp_Pk_in.select(
    col("CASE_MGT_ID"),
    col("CNTCT_SEQ_NO"),
    col("LOG_SEQ_NO"),
    col("SRC_SYS_CD")
)

df_rdup_Natural_Keys = dedup_sort(
    df_Cp_Pk_out_lnk_Transforms_Out,
    ["CASE_MGT_ID", "LOG_SEQ_NO", "CNTCT_SEQ_NO", "SRC_SYS_CD"],
    []
)

df_jn_CMCT = (
    df_rdup_Natural_Keys.alias("lnk_Natural_Keys_out")
    .join(
        df_db2_K_CASE_MGT_CONTACT_DTL_in.alias("LnkCaseMgtContactDtlPk"),
        (
            (col("lnk_Natural_Keys_out.CASE_MGT_ID") == col("LnkCaseMgtContactDtlPk.CASE_MGT_ID")) &
            (col("lnk_Natural_Keys_out.CNTCT_SEQ_NO") == col("LnkCaseMgtContactDtlPk.CNTCT_SEQ_NO")) &
            (col("lnk_Natural_Keys_out.LOG_SEQ_NO") == col("LnkCaseMgtContactDtlPk.LOG_SEQ_NO")) &
            (col("lnk_Natural_Keys_out.SRC_SYS_CD") == col("LnkCaseMgtContactDtlPk.SRC_SYS_CD"))
        ),
        how="left"
    )
    .select(
        col("lnk_Natural_Keys_out.CASE_MGT_ID").alias("CASE_MGT_ID"),
        col("lnk_Natural_Keys_out.CNTCT_SEQ_NO").alias("CNTCT_SEQ_NO"),
        col("lnk_Natural_Keys_out.LOG_SEQ_NO").alias("LOG_SEQ_NO"),
        col("lnk_Natural_Keys_out.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("LnkCaseMgtContactDtlPk.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LnkCaseMgtContactDtlPk.CASE_MGT_CNTCT_DTL_LOG_SK").alias("CASE_MGT_CNTCT_DTL_LOG_SK")
    )
)

df_jn_CMCT_withFlag = df_jn_CMCT.select(
    "*",
    isnull(col("CASE_MGT_CNTCT_DTL_LOG_SK")).alias("original_sk_null")
)

df_xfrm_PKEYgen_all = df_jn_CMCT_withFlag.select(
    col("CASE_MGT_ID"),
    col("CNTCT_SEQ_NO"),
    col("LOG_SEQ_NO"),
    col("SRC_SYS_CD"),
    when(
        isnull(col("CASE_MGT_CNTCT_DTL_LOG_SK")), 
        lit(IDSRunCycle)
    ).otherwise(col("CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CASE_MGT_CNTCT_DTL_LOG_SK"),
    col("original_sk_null")
)

df_enriched = df_xfrm_PKEYgen_all
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'CASE_MGT_CNTCT_DTL_LOG_SK',<schema>,<secret_name>)

df_lnk_Pkey_out = df_enriched.select(
    col("CASE_MGT_ID"),
    col("CNTCT_SEQ_NO"),
    col("LOG_SEQ_NO"),
    col("SRC_SYS_CD"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CASE_MGT_CNTCT_DTL_LOG_SK"),
    col("original_sk_null")
)

df_lnk_KCmSt_New = df_lnk_Pkey_out.filter("original_sk_null = true").select(
    col("CASE_MGT_ID"),
    col("CNTCT_SEQ_NO"),
    col("LOG_SEQ_NO"),
    col("SRC_SYS_CD"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("CASE_MGT_CNTCT_DTL_LOG_SK")
)

df_DB2_K_CASE_MGT_CONTACT_DTL_Load = df_lnk_KCmSt_New

execute_dml(
    "DROP TABLE IF EXISTS STAGING.FctsCaseMgtContactDtlPKey_DB2_K_CASE_MGT_CONTACT_DTL_Load_temp",
    jdbc_url,
    jdbc_props
)

df_DB2_K_CASE_MGT_CONTACT_DTL_Load.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.FctsCaseMgtContactDtlPKey_DB2_K_CASE_MGT_CONTACT_DTL_Load_temp") \
    .mode("overwrite") \
    .save()

merge_sql_DB2_K_CASE_MGT_CONTACT_DTL_Load = f"""
MERGE INTO {IDSOwner}.K_CASE_MGT_CNTCT_DTL_LOG AS T
USING STAGING.FctsCaseMgtContactDtlPKey_DB2_K_CASE_MGT_CONTACT_DTL_Load_temp AS S
ON (
    T.CASE_MGT_ID = S.CASE_MGT_ID
    AND T.CNTCT_SEQ_NO = S.CNTCT_SEQ_NO
    AND T.LOG_SEQ_NO = S.LOG_SEQ_NO
    AND T.SRC_SYS_CD = S.SRC_SYS_CD
)
WHEN MATCHED THEN UPDATE SET
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
    T.CASE_MGT_CNTCT_DTL_LOG_SK = S.CASE_MGT_CNTCT_DTL_LOG_SK
WHEN NOT MATCHED THEN INSERT
(
    CASE_MGT_ID,
    CNTCT_SEQ_NO,
    LOG_SEQ_NO,
    SRC_SYS_CD,
    CRT_RUN_CYC_EXCTN_SK,
    CASE_MGT_CNTCT_DTL_LOG_SK
)
VALUES
(
    S.CASE_MGT_ID,
    S.CNTCT_SEQ_NO,
    S.LOG_SEQ_NO,
    S.SRC_SYS_CD,
    S.CRT_RUN_CYC_EXCTN_SK,
    S.CASE_MGT_CNTCT_DTL_LOG_SK
);
"""
execute_dml(merge_sql_DB2_K_CASE_MGT_CONTACT_DTL_Load, jdbc_url, jdbc_props)

df_lnk_Pkey_out_forJoin = df_lnk_Pkey_out.select(
    col("CASE_MGT_ID"),
    col("CNTCT_SEQ_NO"),
    col("LOG_SEQ_NO"),
    col("SRC_SYS_CD"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CASE_MGT_CNTCT_DTL_LOG_SK")
)

df_Cp_Pk_out_CP_Out_forJoin = df_Cp_Pk_out_CP_Out.select(
    col("PRI_NAT_KEY_STRING"),
    col("FIRST_RECYC_TS"),
    col("CASE_MGT_ID"),
    col("CNTCT_SEQ_NO"),
    col("LOG_SEQ_NO"),
    col("LOG_DTM"),
    col("LOG_SUM_TX"),
    col("CMLG_MCTR_CALL"),
    col("CMLG_METHOD"),
    col("CMLG_USID"),
    col("SRC_SYS_CD")
)

df_jn_PKey = (
    df_lnk_Pkey_out_forJoin.alias("lnk_Pkey_out")
    .join(
        df_Cp_Pk_out_CP_Out_forJoin.alias("CP_Out"),
        (
            (col("lnk_Pkey_out.CASE_MGT_ID") == col("CP_Out.CASE_MGT_ID")) &
            (col("lnk_Pkey_out.CNTCT_SEQ_NO") == col("CP_Out.CNTCT_SEQ_NO")) &
            (col("lnk_Pkey_out.LOG_SEQ_NO") == col("CP_Out.LOG_SEQ_NO")) &
            (col("lnk_Pkey_out.SRC_SYS_CD") == col("CP_Out.SRC_SYS_CD"))
        ),
        how="inner"
    )
    .select(
        col("lnk_Pkey_out.CASE_MGT_ID").alias("CASE_MGT_ID"),
        col("lnk_Pkey_out.CNTCT_SEQ_NO").alias("CNTCT_SEQ_NO"),
        col("lnk_Pkey_out.LOG_SEQ_NO").alias("LOG_SEQ_NO"),
        col("lnk_Pkey_out.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnk_Pkey_out.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("lnk_Pkey_out.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("lnk_Pkey_out.CASE_MGT_CNTCT_DTL_LOG_SK").alias("CASE_MGT_CNTCT_DTL_LOG_SK"),
        col("CP_Out.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        col("CP_Out.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        col("CP_Out.LOG_DTM").alias("LOG_DTM"),
        col("CP_Out.LOG_SUM_TX").alias("LOG_SUM_TX"),
        col("CP_Out.CMLG_MCTR_CALL").alias("CMLG_MCTR_CALL"),
        col("CP_Out.CMLG_METHOD").alias("CMLG_METHOD"),
        col("CP_Out.CMLG_USID").alias("CMLG_USID")
    )
)

df_final_write = df_jn_PKey.select(
    col("CASE_MGT_ID"),
    col("CNTCT_SEQ_NO"),
    col("LOG_SEQ_NO"),
    rpad(col("SRC_SYS_CD"), 50, " ").alias("SRC_SYS_CD"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CASE_MGT_CNTCT_DTL_LOG_SK"),
    rpad(col("PRI_NAT_KEY_STRING"), 50, " ").alias("PRI_NAT_KEY_STRING"),
    rpad(col("FIRST_RECYC_TS"), 50, " ").alias("FIRST_RECYC_TS"),
    rpad(col("LOG_DTM"), 50, " ").alias("LOG_DTM"),
    rpad(col("LOG_SUM_TX"), 50, " ").alias("LOG_SUM_TX"),
    rpad(col("CMLG_MCTR_CALL"), 50, " ").alias("CMLG_MCTR_CALL"),
    rpad(col("CMLG_METHOD"), 50, " ").alias("CMLG_METHOD"),
    rpad(col("CMLG_USID"), 50, " ").alias("CMLG_USID")
)

write_files(
    df_final_write,
    f"{adls_path}/key/CASE_MGT_CNT_DTL_LOG.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)