# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                2/6/2007           Cust Svc/3028               Originally Programmed                     devlEDW10                  Steph Goddard             02/21/2007
# MAGIC Ralph Tucker                 2/21/2007      15                                       Added default of 'NA' for DOC_ID per   devlEDW10 
# MAGIC                                                                                                           CDMA requirements.
# MAGIC Bhupinder kaur                11/21/2013        5114                          Rewrite in parallel                             EnterpriseWhrsDevl            Jag Yelavarthi              2014-01-29

# MAGIC Code SK lookups for Denormalization
# MAGIC Write CUST_SVC_TASK_DOC_D Data into a Sequential file for Load Job IdsEdwCustSvcTaskDocDLoad.
# MAGIC Read the  IDS table : CUST_SVC_TASK_CSTM_DTL
# MAGIC EDW Customer Service Task Doc  extract from IDS based on Driver Table - W_CUST_SVC_DRVR
# MAGIC Add Defaults and Null Handling.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Job Name: IdsEdwCustSvcTaskDocDExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_db2_CUST_SVC_TASK_DOC_D_in = f"""
SELECT DISTINCT
CST_DTL.CUST_SVC_TASK_CSTM_DTL_SK,
CST_DTL.SRC_SYS_CD_SK,
CST_DTL.CUST_SVC_ID,
CST_DTL.TASK_SEQ_NO,
CST_DTL.CSTM_DTL_SEQ_NO,
CST_DTL.CRT_RUN_CYC_EXCTN_SK,
CST_DTL.CRT_BY_USER_SK,
CST_DTL.CUST_SVC_TASK_SK,
CST_DTL.LAST_UPDT_USER_SK,
CST_DTL.CRT_DTM,
CST_DTL.LAST_UPDT_DTM,
CST_DTL.CUST_SVC_TASK_CSTM_DTL_CD_SK,
CST_DTL.CSTM_DTL_DESC,
CST_DTL.CSTM_DTL_UNIQ_ID
FROM
{IDSOwner}.CUST_SVC_TASK_CSTM_DTL CST_DTL,
{IDSOwner}.CD_MPPNG CD_MPPNG,
{IDSOwner}.W_CUST_SVC_DRVR DRVR
WHERE
CST_DTL.CUST_SVC_TASK_CSTM_DTL_CD_SK = CD_MPPNG.CD_MPPNG_SK
AND CD_MPPNG.TRGT_CD = 'DOCID'
AND CST_DTL.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK
AND CST_DTL.CUST_SVC_ID = DRVR.CUST_SVC_ID
"""
df_db2_CUST_SVC_TASK_DOC_D_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CUST_SVC_TASK_DOC_D_in)
    .load()
)

extract_query_db2_CD_MPPNG_in = f"""
SELECT
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG_in)
    .load()
)

df_cpy_Cd_Mppng = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

extract_query_db2_APP_USER_in = f"""
SELECT
USER_SK,
USER_ID
FROM {IDSOwner}.APP_USER
"""
df_db2_APP_USER_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_APP_USER_in)
    .load()
)

df_cpy_App_User = df_db2_APP_USER_in.select(
    F.col("USER_SK").alias("USER_SK"),
    F.col("USER_ID").alias("USER_ID")
)

df_lkp_Codes = (
    df_db2_CUST_SVC_TASK_DOC_D_in.alias("lnk_IdsEdwCustSvcTaskDocDExtr_InABC")
    .join(
        df_cpy_Cd_Mppng.alias("ref_SrcSysCd"),
        F.col("lnk_IdsEdwCustSvcTaskDocDExtr_InABC.SRC_SYS_CD_SK") == F.col("ref_SrcSysCd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_Cd_Mppng.alias("ref_CustSvcTaskCstmDtlCd"),
        F.col("lnk_IdsEdwCustSvcTaskDocDExtr_InABC.CUST_SVC_TASK_CSTM_DTL_CD_SK") == F.col("ref_CustSvcTaskCstmDtlCd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_App_User.alias("ref_LastupdtUser"),
        F.col("lnk_IdsEdwCustSvcTaskDocDExtr_InABC.LAST_UPDT_USER_SK") == F.col("ref_LastupdtUser.USER_SK"),
        "left"
    )
    .join(
        df_cpy_App_User.alias("ref_CrtByUser"),
        F.col("lnk_IdsEdwCustSvcTaskDocDExtr_InABC.CRT_BY_USER_SK") == F.col("ref_CrtByUser.USER_SK"),
        "left"
    )
    .select(
        F.col("lnk_IdsEdwCustSvcTaskDocDExtr_InABC.CUST_SVC_TASK_CSTM_DTL_SK").alias("CUST_SVC_TASK_CSTM_DTL_SK"),
        F.col("lnk_IdsEdwCustSvcTaskDocDExtr_InABC.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("lnk_IdsEdwCustSvcTaskDocDExtr_InABC.CUST_SVC_ID").alias("CUST_SVC_ID"),
        F.col("lnk_IdsEdwCustSvcTaskDocDExtr_InABC.TASK_SEQ_NO").alias("TASK_SEQ_NO"),
        F.col("lnk_IdsEdwCustSvcTaskDocDExtr_InABC.CSTM_DTL_SEQ_NO").alias("CSTM_DTL_SEQ_NO"),
        F.col("lnk_IdsEdwCustSvcTaskDocDExtr_InABC.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_IdsEdwCustSvcTaskDocDExtr_InABC.CRT_BY_USER_SK").alias("CRT_BY_USER_SK"),
        F.col("lnk_IdsEdwCustSvcTaskDocDExtr_InABC.CUST_SVC_TASK_SK").alias("CUST_SVC_TASK_SK"),
        F.col("lnk_IdsEdwCustSvcTaskDocDExtr_InABC.LAST_UPDT_USER_SK").alias("LAST_UPDT_USER_SK"),
        F.col("lnk_IdsEdwCustSvcTaskDocDExtr_InABC.CRT_DTM").alias("CRT_DTM"),
        F.col("lnk_IdsEdwCustSvcTaskDocDExtr_InABC.LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
        F.col("lnk_IdsEdwCustSvcTaskDocDExtr_InABC.CUST_SVC_TASK_CSTM_DTL_CD_SK").alias("CUST_SVC_TASK_CSTM_DTL_CD_SK"),
        F.col("lnk_IdsEdwCustSvcTaskDocDExtr_InABC.CSTM_DTL_DESC").alias("CSTM_DTL_DESC"),
        F.col("lnk_IdsEdwCustSvcTaskDocDExtr_InABC.CSTM_DTL_UNIQ_ID").alias("CSTM_DTL_UNIQ_ID"),
        F.col("ref_SrcSysCd.TRGT_CD").alias("SRC_SYS_CD"),
        F.col("ref_CustSvcTaskCstmDtlCd.TRGT_CD").alias("TRGT_CD_1"),
        F.col("ref_CustSvcTaskCstmDtlCd.TRGT_CD_NM").alias("TRGT_CD_NM"),
        F.col("ref_LastupdtUser.USER_ID").alias("USER_ID_Last_updt_User"),
        F.col("ref_CrtByUser.USER_ID").alias("USER_ID_Crt_By_User")
    )
)

df_lnk_FullData = (
    df_lkp_Codes
    .filter(
        (F.col("CUST_SVC_TASK_CSTM_DTL_SK") != 0)
        & (F.col("CUST_SVC_TASK_CSTM_DTL_SK") != 1)
    )
    .select(
        F.col("CUST_SVC_TASK_CSTM_DTL_SK").alias("CUST_SVC_TASK_DOC_SK"),
        F.when(
            F.col("SRC_SYS_CD").isNull() | (F.length(trim(F.col("SRC_SYS_CD"))) == 0),
            F.lit("NA")
        ).otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
        F.col("CUST_SVC_ID").alias("CUST_SVC_ID"),
        F.col("TASK_SEQ_NO").alias("CUST_SVC_TASK_SEQ_NO"),
        F.col("CSTM_DTL_SEQ_NO").alias("CUST_SVC_TASK_DOC_SEQ_NO"),
        F.when(
            F.col("TRGT_CD_1").isNull() | (F.length(trim(F.col("TRGT_CD_1"))) == 0),
            F.lit("NA")
        ).otherwise(F.col("TRGT_CD_1")).alias("CUST_SVC_TASK_CSTM_DTL_CD"),
        F.col("CSTM_DTL_UNIQ_ID").alias("CUST_SVC_TASK_CSTM_DTL_UNIQ_ID"),
        F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("CRT_BY_USER_SK").alias("CRT_BY_USER_SK"),
        F.col("CUST_SVC_TASK_SK").alias("CUST_SVC_TASK_SK"),
        F.col("LAST_UPDT_USER_SK").alias("LAST_UPDT_USER_SK"),
        F.when(
            F.col("USER_ID_Crt_By_User").isNull() | (F.length(trim(F.col("USER_ID_Crt_By_User"))) == 0),
            F.lit("NA")
        ).otherwise(F.col("USER_ID_Crt_By_User")).alias("CRT_BY_USER_ID"),
        F.col("CRT_DTM").alias("CUST_SVC_TASK_CSTM_DTL_CRT_DTM"),
        F.col("LAST_UPDT_DTM").alias("CS_TASK_CSTM_DTL_LAST_UPDT_DTM"),
        F.when(
            F.col("TRGT_CD_NM").isNull() | (F.length(trim(F.col("TRGT_CD_NM"))) == 0),
            F.lit("NA")
        ).otherwise(F.col("TRGT_CD_NM")).alias("CUST_SVC_TASK_CSTM_DTL_NM"),
        F.when(
            F.length(trim(F.col("CSTM_DTL_DESC"))) == 0,
            F.lit("NA")
        ).otherwise(F.substring(trim(F.col("CSTM_DTL_DESC")), 1, 20)).alias("DOC_ID"),
        F.when(
            F.col("USER_ID_Last_updt_User").isNull() | (F.length(trim(F.col("USER_ID_Last_updt_User"))) == 0),
            F.lit("NA")
        ).otherwise(F.col("USER_ID_Last_updt_User")).alias("LAST_UPDT_USER_ID"),
        F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("CUST_SVC_TASK_CSTM_DTL_CD_SK").alias("CUST_SVC_TASK_CSTM_DTL_CD_SK")
    )
)

df_lnk_NA = spark.createDataFrame(
    [
        (
            1,
            "NA",
            "NA",
            0,
            0,
            "NA",
            "1753-01-01 00:00:00",
            "1753-01-01",
            "1753-01-01",
            1,
            1,
            1,
            "NA",
            "1753-01-01 00:00:00",
            "1753-01-01 00:00:00",
            "NA",
            "NA",
            "NA",
            100,
            100,
            1
        )
    ],
    [
        "CUST_SVC_TASK_DOC_SK",
        "SRC_SYS_CD",
        "CUST_SVC_ID",
        "CUST_SVC_TASK_SEQ_NO",
        "CUST_SVC_TASK_DOC_SEQ_NO",
        "CUST_SVC_TASK_CSTM_DTL_CD",
        "CUST_SVC_TASK_CSTM_DTL_UNIQ_ID",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "CRT_BY_USER_SK",
        "CUST_SVC_TASK_SK",
        "LAST_UPDT_USER_SK",
        "CRT_BY_USER_ID",
        "CUST_SVC_TASK_CSTM_DTL_CRT_DTM",
        "CS_TASK_CSTM_DTL_LAST_UPDT_DTM",
        "CUST_SVC_TASK_CSTM_DTL_NM",
        "DOC_ID",
        "LAST_UPDT_USER_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CUST_SVC_TASK_CSTM_DTL_CD_SK"
    ]
)

df_lnk_UNK = spark.createDataFrame(
    [
        (
            0,
            "UNK",
            "UNK",
            0,
            0,
            "UNK",
            "1753-01-01 00:00:00",
            "1753-01-01",
            "1753-01-01",
            0,
            0,
            0,
            "UNK",
            "1753-01-01 00:00:00",
            "1753-01-01 00:00:00",
            "UNK",
            "UNK",
            "UNK",
            100,
            100,
            0
        )
    ],
    [
        "CUST_SVC_TASK_DOC_SK",
        "SRC_SYS_CD",
        "CUST_SVC_ID",
        "CUST_SVC_TASK_SEQ_NO",
        "CUST_SVC_TASK_DOC_SEQ_NO",
        "CUST_SVC_TASK_CSTM_DTL_CD",
        "CUST_SVC_TASK_CSTM_DTL_UNIQ_ID",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "CRT_BY_USER_SK",
        "CUST_SVC_TASK_SK",
        "LAST_UPDT_USER_SK",
        "CRT_BY_USER_ID",
        "CUST_SVC_TASK_CSTM_DTL_CRT_DTM",
        "CS_TASK_CSTM_DTL_LAST_UPDT_DTM",
        "CUST_SVC_TASK_CSTM_DTL_NM",
        "DOC_ID",
        "LAST_UPDT_USER_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CUST_SVC_TASK_CSTM_DTL_CD_SK"
    ]
)

df_fnl_CustSvcTaskDocD = (
    df_lnk_FullData
    .unionByName(df_lnk_NA)
    .unionByName(df_lnk_UNK)
)

final_columns = [
    "CUST_SVC_TASK_DOC_SK",
    "SRC_SYS_CD",
    "CUST_SVC_ID",
    "CUST_SVC_TASK_SEQ_NO",
    "CUST_SVC_TASK_DOC_SEQ_NO",
    "CUST_SVC_TASK_CSTM_DTL_CD",
    "CUST_SVC_TASK_CSTM_DTL_UNIQ_ID",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "CRT_BY_USER_SK",
    "CUST_SVC_TASK_SK",
    "LAST_UPDT_USER_SK",
    "CRT_BY_USER_ID",
    "CUST_SVC_TASK_CSTM_DTL_CRT_DTM",
    "CS_TASK_CSTM_DTL_LAST_UPDT_DTM",
    "CUST_SVC_TASK_CSTM_DTL_NM",
    "DOC_ID",
    "LAST_UPDT_USER_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CUST_SVC_TASK_CSTM_DTL_CD_SK"
]

df_fnl_CustSvcTaskDocD = df_fnl_CustSvcTaskDocD.select(
    F.col("CUST_SVC_TASK_DOC_SK"),
    F.col("SRC_SYS_CD"),
    F.col("CUST_SVC_ID"),
    F.col("CUST_SVC_TASK_SEQ_NO"),
    F.col("CUST_SVC_TASK_DOC_SEQ_NO"),
    F.col("CUST_SVC_TASK_CSTM_DTL_CD"),
    F.col("CUST_SVC_TASK_CSTM_DTL_UNIQ_ID"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("CRT_BY_USER_SK"),
    F.col("CUST_SVC_TASK_SK"),
    F.col("LAST_UPDT_USER_SK"),
    F.rpad(F.col("CRT_BY_USER_ID"), 10, " ").alias("CRT_BY_USER_ID"),
    F.col("CUST_SVC_TASK_CSTM_DTL_CRT_DTM"),
    F.col("CS_TASK_CSTM_DTL_LAST_UPDT_DTM"),
    F.col("CUST_SVC_TASK_CSTM_DTL_NM"),
    F.col("DOC_ID"),
    F.rpad(F.col("LAST_UPDT_USER_ID"), 10, " ").alias("LAST_UPDT_USER_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CUST_SVC_TASK_CSTM_DTL_CD_SK")
)

write_files(
    df_fnl_CustSvcTaskDocD,
    f"{adls_path}/load/CUST_SVC_TASK_DOC_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)