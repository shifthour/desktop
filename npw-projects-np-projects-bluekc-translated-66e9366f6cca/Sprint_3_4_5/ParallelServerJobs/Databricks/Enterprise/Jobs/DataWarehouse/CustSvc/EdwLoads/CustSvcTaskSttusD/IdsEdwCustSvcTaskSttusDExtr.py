# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #                  Change Description                             Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------      ---------------------------------------------                   ----------------------------------     ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               2/1/2007        3028                             Originally Programmed                                 devlEDW10                   Steph Goddard           02/23/2007
# MAGIC Kalyan Neelam               2010-01-29       TTR - 604                       Added new field RTE_TO_GRP_ID           EnterpriseWrhsDevl       Steph Goddard           01/30/2010
# MAGIC 
# MAGIC Bhupinder kaur                12/23/2013        5114                            Rewrite in Parallel                                      EnterpriseWhrsDevl       Jag Yelavarthi             2014-01-29

# MAGIC Code SK lookups for Denormalization
# MAGIC Write CUST_SVC_TASK_STTUS_D Data into a Sequential file for Load Job IdsEdwCustSvcTaskSttusDLoad.
# MAGIC Read the  IDS table : CUST_SVC_TASK_STTUS 
# MAGIC W_CUST_SVC_DRVR
# MAGIC Add Defaults and Null Handling.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Job Name: IdsEdwCustSvcTaskSttusDExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F, types as T
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = (
    f"SELECT CST_STTUS.CUST_SVC_TASK_STTUS_SK,\n"
    f"CST_STTUS.SRC_SYS_CD_SK,\n"
    f"CST_STTUS.CUST_SVC_ID,\n"
    f"CST_STTUS.TASK_SEQ_NO,\n"
    f"CST_STTUS.STTUS_SEQ_NO,\n"
    f"CST_STTUS.CRT_RUN_CYC_EXCTN_SK,\n"
    f"CST_STTUS.LAST_UPDT_RUN_CYC_EXCTN_SK,\n"
    f"CST_STTUS.CUST_SVC_TASK_SK,\n"
    f"CST_STTUS.CRT_BY_USER_SK,\n"
    f"CST_STTUS.CUST_SVC_TASK_STTUS_CD_SK,\n"
    f"CST_STTUS.RTE_TO_USER_SK,\n"
    f"CST_STTUS.CUST_SVC_TASK_STTUS_RSN_CD_SK,\n"
    f"CST_STTUS.STTUS_DTM,\n"
    f"CST_STTUS.RTE_TO_GRP_ID\n"
    f" FROM {IDSOwner}.CUST_SVC_TASK_STTUS CST_STTUS, {IDSOwner}.W_CUST_SVC_DRVR DRVR\n"
    f"WHERE CST_STTUS.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK\n"
    f"AND CST_STTUS.CUST_SVC_ID = DRVR.CUST_SVC_ID"
)
df_db2_CUST_SVC_TASK_STTUS_D_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

extract_query = (
    f"SELECT CD_MPPNG_SK,COALESCE(TRGT_CD,'UNK') TRGT_CD,COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM "
    f"from {IDSOwner}.CD_MPPNG"
)
df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_cpy_Cd_Mppng_ref_SrcSysCd = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_cpy_Cd_Mppng_ref_CustSvcTskSttusCdLkup = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_cpy_Cd_Mppng_ref_CustSvcTskSttusRsnCdLkup = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

extract_query = f"SELECT USER_SK, USER_ID FROM {IDSOwner}.APP_USER"
df_db2_APP_USER_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_cpy_App_User_ref_RteToUserLookup = df_db2_APP_USER_in.select(
    F.col("USER_SK").alias("USER_SK"),
    F.col("USER_ID").alias("USER_ID")
)
df_cpy_App_User_ref_CrtByUser = df_db2_APP_USER_in.select(
    F.col("USER_SK").alias("USER_SK"),
    F.col("USER_ID").alias("USER_ID")
)

df_lkp_Codes = (
    df_db2_CUST_SVC_TASK_STTUS_D_in.alias("lnk_IdsEdwCustSvcTaskSttusDExtr_InABC")
    .join(
        df_cpy_Cd_Mppng_ref_SrcSysCd.alias("ref_SrcSysCd"),
        F.col("lnk_IdsEdwCustSvcTaskSttusDExtr_InABC.SRC_SYS_CD_SK") == F.col("ref_SrcSysCd.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_cpy_Cd_Mppng_ref_CustSvcTskSttusCdLkup.alias("ref_CustSvcTskSttusCdLkup"),
        F.col("lnk_IdsEdwCustSvcTaskSttusDExtr_InABC.CUST_SVC_TASK_STTUS_CD_SK") == F.col("ref_CustSvcTskSttusCdLkup.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_cpy_App_User_ref_RteToUserLookup.alias("ref_RteToUserLookup"),
        F.col("lnk_IdsEdwCustSvcTaskSttusDExtr_InABC.RTE_TO_USER_SK") == F.col("ref_RteToUserLookup.USER_SK"),
        "left",
    )
    .join(
        df_cpy_App_User_ref_CrtByUser.alias("ref_CrtByUser"),
        F.col("lnk_IdsEdwCustSvcTaskSttusDExtr_InABC.CRT_BY_USER_SK") == F.col("ref_CrtByUser.USER_SK"),
        "left",
    )
    .join(
        df_cpy_Cd_Mppng_ref_CustSvcTskSttusRsnCdLkup.alias("ref_CustSvcTskSttusRsnCdLkup"),
        F.col("lnk_IdsEdwCustSvcTaskSttusDExtr_InABC.CUST_SVC_TASK_STTUS_RSN_CD_SK") == F.col("ref_CustSvcTskSttusRsnCdLkup.CD_MPPNG_SK"),
        "left",
    )
    .select(
        F.col("lnk_IdsEdwCustSvcTaskSttusDExtr_InABC.CUST_SVC_TASK_STTUS_SK").alias("CUST_SVC_TASK_STTUS_SK"),
        F.col("lnk_IdsEdwCustSvcTaskSttusDExtr_InABC.CUST_SVC_ID").alias("CUST_SVC_ID"),
        F.col("lnk_IdsEdwCustSvcTaskSttusDExtr_InABC.TASK_SEQ_NO").alias("TASK_SEQ_NO"),
        F.col("lnk_IdsEdwCustSvcTaskSttusDExtr_InABC.STTUS_SEQ_NO").alias("STTUS_SEQ_NO"),
        F.col("lnk_IdsEdwCustSvcTaskSttusDExtr_InABC.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_IdsEdwCustSvcTaskSttusDExtr_InABC.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_IdsEdwCustSvcTaskSttusDExtr_InABC.CRT_BY_USER_SK").alias("CRT_BY_USER_SK"),
        F.col("lnk_IdsEdwCustSvcTaskSttusDExtr_InABC.CUST_SVC_TASK_SK").alias("CUST_SVC_TASK_SK"),
        F.col("lnk_IdsEdwCustSvcTaskSttusDExtr_InABC.RTE_TO_USER_SK").alias("RTE_TO_USER_SK"),
        F.col("lnk_IdsEdwCustSvcTaskSttusDExtr_InABC.CUST_SVC_TASK_STTUS_CD_SK").alias("CUST_SVC_TASK_STTUS_CD_SK"),
        F.col("lnk_IdsEdwCustSvcTaskSttusDExtr_InABC.CUST_SVC_TASK_STTUS_RSN_CD_SK").alias("CUST_SVC_TASK_STTUS_RSN_CD_SK"),
        F.col("lnk_IdsEdwCustSvcTaskSttusDExtr_InABC.STTUS_DTM").alias("STTUS_DTM"),
        F.col("lnk_IdsEdwCustSvcTaskSttusDExtr_InABC.RTE_TO_GRP_ID").alias("RTE_TO_GRP_ID"),
        F.col("ref_SrcSysCd.TRGT_CD").alias("SRC_SYS_CD"),
        F.col("ref_CustSvcTskSttusCdLkup.TRGT_CD").alias("STTUS_CD"),
        F.col("ref_CustSvcTskSttusCdLkup.TRGT_CD_NM").alias("STTUS_CD_NM"),
        F.col("ref_RteToUserLookup.USER_ID").alias("RTE_USER_ID"),
        F.col("ref_CrtByUser.USER_ID").alias("CRT_BY_USER_ID"),
        F.col("ref_CustSvcTskSttusRsnCdLkup.TRGT_CD").alias("RSN_CD"),
        F.col("ref_CustSvcTskSttusRsnCdLkup.TRGT_CD_NM").alias("RSN_CD_NM"),
    )
)

df_lnk_FullData = (
    df_lkp_Codes.filter("(CUST_SVC_TASK_STTUS_SK != 0) AND (CUST_SVC_TASK_STTUS_SK != 1)")
    .select(
        F.col("CUST_SVC_TASK_STTUS_SK").alias("CUST_SVC_TASK_STTUS_SK"),
        F.when(
            F.col("SRC_SYS_CD").isNull() | (F.length(trim(F.col("SRC_SYS_CD"))) == 0),
            F.lit("NA")
        ).otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
        F.col("CUST_SVC_ID").alias("CUST_SVC_ID"),
        F.col("TASK_SEQ_NO").alias("CUST_SVC_TASK_SEQ_NO"),
        F.col("STTUS_SEQ_NO").alias("CUST_SVC_TASK_STTUS_SEQ_NO"),
        F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("CRT_BY_USER_SK").alias("CRT_BY_USER_SK"),
        F.col("CUST_SVC_TASK_SK").alias("CUST_SVC_TASK_SK"),
        F.col("RTE_TO_USER_SK").alias("RTE_TO_USER_SK"),
        F.when(
            F.col("CRT_BY_USER_ID").isNull() | (F.length(trim(F.col("CRT_BY_USER_ID"))) == 0),
            F.lit("NA")
        ).otherwise(F.col("CRT_BY_USER_ID")).alias("CRT_BY_USER_ID"),
        F.when(
            F.col("STTUS_CD").isNull() | (F.length(trim(F.col("STTUS_CD"))) == 0),
            F.lit("NA")
        ).otherwise(F.col("STTUS_CD")).alias("CUST_SVC_TASK_STTUS_CD"),
        F.when(
            F.col("STTUS_CD_NM").isNull() | (F.length(trim(F.col("STTUS_CD_NM"))) == 0),
            F.lit("NA")
        ).otherwise(F.col("STTUS_CD_NM")).alias("CUST_SVC_TASK_STTUS_NM"),
        F.col("STTUS_DTM").alias("CUST_SVC_TASK_STTUS_DTM"),
        F.when(
            F.col("RSN_CD").isNull() | (F.length(trim(F.col("RSN_CD"))) == 0),
            F.lit("NA")
        ).otherwise(F.col("RSN_CD")).alias("CUST_SVC_TASK_STTUS_RSN_CD"),
        F.when(
            F.col("RSN_CD_NM").isNull() | (F.length(trim(F.col("RSN_CD_NM"))) == 0),
            F.lit("NA")
        ).otherwise(F.col("RSN_CD_NM")).alias("CUST_SVC_TASK_STTUS_RSN_NM"),
        F.when(
            F.col("RTE_USER_ID").isNull() | (F.length(trim(F.col("RTE_USER_ID"))) == 0),
            F.lit("NA")
        ).otherwise(F.col("RTE_USER_ID")).alias("RTE_TO_USER_ID"),
        F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("CUST_SVC_TASK_STTUS_CD_SK").alias("CUST_SVC_TASK_STTUS_CD_SK"),
        F.col("CUST_SVC_TASK_STTUS_RSN_CD_SK").alias("CUST_SVC_TASK_STTUS_RSN_CD_SK"),
        F.col("RTE_TO_GRP_ID").alias("RTE_TO_GRP_ID"),
    )
)

df_lnk_NA_schema = T.StructType([
    T.StructField("CUST_SVC_TASK_STTUS_SK", T.IntegerType(), True),
    T.StructField("SRC_SYS_CD", T.StringType(), True),
    T.StructField("CUST_SVC_ID", T.StringType(), True),
    T.StructField("CUST_SVC_TASK_SEQ_NO", T.IntegerType(), True),
    T.StructField("CUST_SVC_TASK_STTUS_SEQ_NO", T.IntegerType(), True),
    T.StructField("CRT_RUN_CYC_EXCTN_DT_SK", T.StringType(), True),
    T.StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", T.StringType(), True),
    T.StructField("CRT_BY_USER_SK", T.IntegerType(), True),
    T.StructField("CUST_SVC_TASK_SK", T.IntegerType(), True),
    T.StructField("RTE_TO_USER_SK", T.IntegerType(), True),
    T.StructField("CRT_BY_USER_ID", T.StringType(), True),
    T.StructField("CUST_SVC_TASK_STTUS_CD", T.StringType(), True),
    T.StructField("CUST_SVC_TASK_STTUS_NM", T.StringType(), True),
    T.StructField("CUST_SVC_TASK_STTUS_DTM", T.StringType(), True),
    T.StructField("CUST_SVC_TASK_STTUS_RSN_CD", T.StringType(), True),
    T.StructField("CUST_SVC_TASK_STTUS_RSN_NM", T.StringType(), True),
    T.StructField("RTE_TO_USER_ID", T.StringType(), True),
    T.StructField("CRT_RUN_CYC_EXCTN_SK", T.IntegerType(), True),
    T.StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", T.IntegerType(), True),
    T.StructField("CUST_SVC_TASK_STTUS_CD_SK", T.IntegerType(), True),
    T.StructField("CUST_SVC_TASK_STTUS_RSN_CD_SK", T.IntegerType(), True),
    T.StructField("RTE_TO_GRP_ID", T.StringType(), True),
])
df_lnk_NA = spark.createDataFrame(
    [
        (
            1,
            "NA",
            "NA",
            0,
            0,
            "1753-01-01",
            "1753-01-01",
            1,
            1,
            1,
            "NA",
            "NA",
            "NA",
            "1753-01-01 00:00:00",
            "NA",
            "NA",
            "NA",
            100,
            100,
            1,
            1,
            "NA",
        )
    ],
    schema=df_lnk_NA_schema
)

df_lnk_UNK_schema = T.StructType([
    T.StructField("CUST_SVC_TASK_STTUS_SK", T.IntegerType(), True),
    T.StructField("SRC_SYS_CD", T.StringType(), True),
    T.StructField("CUST_SVC_ID", T.StringType(), True),
    T.StructField("CUST_SVC_TASK_SEQ_NO", T.IntegerType(), True),
    T.StructField("CUST_SVC_TASK_STTUS_SEQ_NO", T.IntegerType(), True),
    T.StructField("CRT_RUN_CYC_EXCTN_DT_SK", T.StringType(), True),
    T.StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", T.StringType(), True),
    T.StructField("CRT_BY_USER_SK", T.IntegerType(), True),
    T.StructField("CUST_SVC_TASK_SK", T.IntegerType(), True),
    T.StructField("RTE_TO_USER_SK", T.IntegerType(), True),
    T.StructField("CRT_BY_USER_ID", T.StringType(), True),
    T.StructField("CUST_SVC_TASK_STTUS_CD", T.StringType(), True),
    T.StructField("CUST_SVC_TASK_STTUS_NM", T.StringType(), True),
    T.StructField("CUST_SVC_TASK_STTUS_DTM", T.StringType(), True),
    T.StructField("CUST_SVC_TASK_STTUS_RSN_CD", T.StringType(), True),
    T.StructField("CUST_SVC_TASK_STTUS_RSN_NM", T.StringType(), True),
    T.StructField("RTE_TO_USER_ID", T.StringType(), True),
    T.StructField("CRT_RUN_CYC_EXCTN_SK", T.IntegerType(), True),
    T.StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", T.IntegerType(), True),
    T.StructField("CUST_SVC_TASK_STTUS_CD_SK", T.IntegerType(), True),
    T.StructField("CUST_SVC_TASK_STTUS_RSN_CD_SK", T.IntegerType(), True),
    T.StructField("RTE_TO_GRP_ID", T.StringType(), True),
])
df_lnk_UNK = spark.createDataFrame(
    [
        (
            0,
            "UNK",
            "UNK",
            0,
            0,
            "1753-01-01",
            "1753-01-01",
            0,
            0,
            0,
            "UNK",
            "UNK",
            "UNK",
            "1753-01-01 00:00:00",
            "UNK",
            "UNK",
            "UNK",
            100,
            100,
            0,
            0,
            "UNK",
        )
    ],
    schema=df_lnk_UNK_schema
)

df_Fnl_CustSvcTaskSttusD = (
    df_lnk_FullData.unionByName(df_lnk_NA)
    .unionByName(df_lnk_UNK)
)

df_final = df_Fnl_CustSvcTaskSttusD.select(
    F.col("CUST_SVC_TASK_STTUS_SK").alias("CUST_SVC_TASK_STTUS_SK"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CUST_SVC_ID").alias("CUST_SVC_ID"),
    F.col("CUST_SVC_TASK_SEQ_NO").alias("CUST_SVC_TASK_SEQ_NO"),
    F.col("CUST_SVC_TASK_STTUS_SEQ_NO").alias("CUST_SVC_TASK_STTUS_SEQ_NO"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("CRT_BY_USER_SK").alias("CRT_BY_USER_SK"),
    F.col("CUST_SVC_TASK_SK").alias("CUST_SVC_TASK_SK"),
    F.col("RTE_TO_USER_SK").alias("RTE_TO_USER_SK"),
    F.rpad(F.col("CRT_BY_USER_ID"), 10, " ").alias("CRT_BY_USER_ID"),
    F.col("CUST_SVC_TASK_STTUS_CD").alias("CUST_SVC_TASK_STTUS_CD"),
    F.col("CUST_SVC_TASK_STTUS_NM").alias("CUST_SVC_TASK_STTUS_NM"),
    F.col("CUST_SVC_TASK_STTUS_DTM").alias("CUST_SVC_TASK_STTUS_DTM"),
    F.col("CUST_SVC_TASK_STTUS_RSN_CD").alias("CUST_SVC_TASK_STTUS_RSN_CD"),
    F.col("CUST_SVC_TASK_STTUS_RSN_NM").alias("CUST_SVC_TASK_STTUS_RSN_NM"),
    F.rpad(F.col("RTE_TO_USER_ID"), 10, " ").alias("RTE_TO_USER_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CUST_SVC_TASK_STTUS_CD_SK").alias("CUST_SVC_TASK_STTUS_CD_SK"),
    F.col("CUST_SVC_TASK_STTUS_RSN_CD_SK").alias("CUST_SVC_TASK_STTUS_RSN_CD_SK"),
    F.col("RTE_TO_GRP_ID").alias("RTE_TO_GRP_ID"),
)

write_files(
    df_final,
    f"{adls_path}/load/CUST_SVC_TASK_STTUS_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)