# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                2/8/2007           Cust Svc/3028               Originally Programmed                          devlEDW10               Steph Goddard             02/21/2007
# MAGIC Judy Reynolds                2010-11-18        TTR_883                      Modified to assign default values to    EnterpriseWrhsDevl      Steph Goddard             11/22/2010
# MAGIC                                                                                                       datetime and date fields             
# MAGIC Bhupinder kaur                12/19/2013        5114                           Rewrite in parallel                                EnterpriseWhrsDevl       Jag Yelavarthi              2014-01-29

# MAGIC Code SK lookups for Denormalization
# MAGIC Write CUST_SVC_TASK_BCKDT_D Data into a Sequential file for Load Job IdsEdwCustSvcTaskBckdtDLoad.
# MAGIC Read the  IDS table : CUST_SVC_TASK_CSTM_DTL
# MAGIC W_CUST_SVC_DRVR
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
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

jdbc_url_db2_CUST_SVC_TASK_BCKDT_D_in, jdbc_props_db2_CUST_SVC_TASK_BCKDT_D_in = get_db_config(ids_secret_name)
query_db2_CUST_SVC_TASK_BCKDT_D_in = f"""SELECT 
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
CST_DTL.CSTM_DTL_UNIQ_ID,
CST_DTL.CSTM_DTL_DT_1_SK
FROM 
{IDSOwner}.CUST_SVC_TASK_CSTM_DTL CST_DTL, 
{IDSOwner}.CD_MPPNG CD_MPPNG,
{IDSOwner}.W_CUST_SVC_DRVR DRVR
WHERE 
CST_DTL.CUST_SVC_TASK_CSTM_DTL_CD_SK = CD_MPPNG.CD_MPPNG_SK
AND CD_MPPNG.TRGT_CD = 'BCKDT'
AND CST_DTL.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK
AND CST_DTL.CUST_SVC_ID = DRVR.CUST_SVC_ID
"""
df_db2_CUST_SVC_TASK_BCKDT_D_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_CUST_SVC_TASK_BckdtD_in)
    .options(**jdbc_props_db2_CUST_SVC_TASK_BckdtD_in)
    .option("query", query_db2_CUST_SVC_TASK_BCKdtD_in)
    .load()
)

jdbc_url_db2_CD_MPPNG_in, jdbc_props_db2_CD_MPPNG_in = get_db_config(ids_secret_name)
query_db2_CD_MPPNG_in = f"""SELECT CD_MPPNG_SK,COALESCE(TRGT_CD,'UNK') TRGT_CD,COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM from {IDSOwner}.CD_MPPNG"""
df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_CD_MPPNG_in)
    .options(**jdbc_props_db2_CD_MPPNG_in)
    .option("query", query_db2_CD_MPPNG_in)
    .load()
)

df_cpy_Cd_Mppng = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK"),
    F.col("TRGT_CD"),
    F.col("TRGT_CD_NM")
)

jdbc_url_db2_APP_USER_in, jdbc_props_db2_APP_USER_in = get_db_config(ids_secret_name)
query_db2_APP_USER_in = f"""SELECT
USER_SK,
USER_ID
FROM {IDSOwner}.APP_USER
"""
df_db2_APP_USER_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_APP_USER_in)
    .options(**jdbc_props_db2_APP_USER_in)
    .option("query", query_db2_APP_USER_in)
    .load()
)

df_cpy_App_User = df_db2_APP_USER_in.select(
    F.col("USER_SK"),
    F.col("USER_ID")
)

df_lkp_Codes = (
    df_db2_CUST_SVC_TASK_BCKDT_D_in.alias("prim")
    .join(
        df_cpy_Cd_Mppng.alias("ref_SrcSysCd"),
        F.col("prim.SRC_SYS_CD_SK") == F.col("ref_SrcSysCd.CD_MPPNG_SK"),
        how="left"
    )
    .join(
        df_cpy_Cd_Mppng.alias("ref_CustSvcTaskCstmDtlCd"),
        F.col("prim.CUST_SVC_TASK_CSTM_DTL_CD_SK") == F.col("ref_CustSvcTaskCstmDtlCd.CD_MPPNG_SK"),
        how="left"
    )
    .join(
        df_cpy_App_User.alias("ref_LastupdtUser"),
        F.col("prim.LAST_UPDT_USER_SK") == F.col("ref_LastupdtUser.USER_SK"),
        how="left"
    )
    .join(
        df_cpy_App_User.alias("ref_CrtByUser"),
        F.col("prim.CRT_BY_USER_SK") == F.col("ref_CrtByUser.USER_SK"),
        how="left"
    )
    .select(
        F.col("prim.CUST_SVC_TASK_CSTM_DTL_SK").alias("CUST_SVC_TASK_CSTM_DTL_SK"),
        F.col("prim.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("prim.CUST_SVC_ID").alias("CUST_SVC_ID"),
        F.col("prim.TASK_SEQ_NO").alias("TASK_SEQ_NO"),
        F.col("prim.CSTM_DTL_SEQ_NO").alias("CSTM_DTL_SEQ_NO"),
        F.col("prim.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("prim.CRT_BY_USER_SK").alias("CRT_BY_USER_SK"),
        F.col("prim.CUST_SVC_TASK_SK").alias("CUST_SVC_TASK_SK"),
        F.col("prim.LAST_UPDT_USER_SK").alias("LAST_UPDT_USER_SK"),
        F.col("prim.CRT_DTM").alias("CRT_DTM"),
        F.col("prim.LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
        F.col("prim.CUST_SVC_TASK_CSTM_DTL_CD_SK").alias("CUST_SVC_TASK_CSTM_DTL_CD_SK"),
        F.col("prim.CSTM_DTL_DESC").alias("CSTM_DTL_DESC"),
        F.col("prim.CSTM_DTL_UNIQ_ID").alias("CSTM_DTL_UNIQ_ID"),
        F.col("ref_SrcSysCd.TRGT_CD").alias("SRC_SYS_CD"),
        F.col("ref_CustSvcTaskCstmDtlCd.TRGT_CD").alias("TRGT_CD_1"),
        F.col("ref_CustSvcTaskCstmDtlCd.TRGT_CD_NM").alias("TRGT_CD_NM"),
        F.col("ref_LastupdtUser.USER_ID").alias("USER_ID_Last_updt_User"),
        F.col("ref_CrtByUser.USER_ID").alias("USER_ID_Crt_By_User"),
        F.col("prim.CSTM_DTL_DT_1_SK").alias("CSTM_DTL_DT_1_SK")
    )
)

df_xfrm_BusinessLogic_lnk_FullData = (
    df_lkp_Codes
    .filter(
        (F.col("CUST_SVC_TASK_CSTM_DTL_SK") != 0)
        & (F.col("CUST_SVC_TASK_CSTM_DTL_SK") != 1)
    )
    .withColumn(
        "CUST_SVC_TASK_BCKDT_SK",
        F.col("CUST_SVC_TASK_CSTM_DTL_SK")
    )
    .withColumn(
        "SRC_SYS_CD",
        F.when(
            F.col("SRC_SYS_CD").isNull() | (F.length(trim("SRC_SYS_CD")) == 0),
            F.lit("NA")
        ).otherwise(F.col("SRC_SYS_CD"))
    )
    .withColumn("CUST_SVC_ID", F.col("CUST_SVC_ID"))
    .withColumn("CUST_SVC_TASK_SEQ_NO", F.col("TASK_SEQ_NO"))
    .withColumn("CUST_SVC_TASK_BCKDT_SEQ_NO", F.col("CSTM_DTL_SEQ_NO"))
    .withColumn(
        "CUST_SVC_TASK_CSTM_DTL_CD",
        F.when(
            F.col("TRGT_CD_1").isNull() | (F.length(trim("TRGT_CD_1")) == 0),
            F.lit("NA")
        ).otherwise(F.col("TRGT_CD_1"))
    )
    .withColumn(
        "CUST_SVC_TASK_CSTM_DTL_UNIQ_ID",
        F.when(
            F.col("CSTM_DTL_UNIQ_ID").isNull()
            | (F.length(trim("CSTM_DTL_UNIQ_ID")) == 0)
            | (trim("CSTM_DTL_UNIQ_ID") == F.lit("UNK")),
            F.lit("1753-01-01 00:00:00.000000")
        ).otherwise(F.col("CSTM_DTL_UNIQ_ID"))
    )
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.lit(EDWRunCycleDate))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.lit(EDWRunCycleDate))
    .withColumn("CRT_BY_USER_SK", F.col("CRT_BY_USER_SK"))
    .withColumn("CUST_SVC_TASK_SK", F.col("CUST_SVC_TASK_SK"))
    .withColumn("LAST_UPDT_USER_SK", F.col("LAST_UPDT_USER_SK"))
    .withColumn(
        "BCKDT_DT",
        F.when(
            F.col("CSTM_DTL_DT_1_SK").isNull()
            | (F.length(trim("CSTM_DTL_DT_1_SK")) == 0)
            | (trim("CSTM_DTL_DT_1_SK") == F.lit("UNK")),
            F.lit("1753-01-01 00:00:00.000000")
        ).otherwise(
            FORMAT.DATE.EE(
                F.col("CSTM_DTL_DT_1_SK"),
                'DATE',
                'DATE',
                'DB2TIMESTAMP'
            )
        )
    )
    .withColumn(
        "CRT_BY_USER_ID",
        F.when(
            F.col("USER_ID_Crt_By_User").isNull()
            | (F.length(trim("USER_ID_Crt_By_User")) == 0),
            F.lit("NA")
        ).otherwise(F.col("USER_ID_Crt_By_User"))
    )
    .withColumn(
        "CUST_SVC_TASK_CSTM_DTL_CRT_DTM",
        F.when(
            F.col("CRT_DTM").isNull()
            | (F.length(trim("CRT_DTM")) == 0)
            | (trim("CRT_DTM") == F.lit("UNK")),
            F.lit("1753-01-01 00:00:00.000000")
        ).otherwise(F.col("CRT_DTM"))
    )
    .withColumn(
        "CS_TASK_CSTM_DTL_LAST_UPDT_DTM",
        F.when(
            F.col("LAST_UPDT_DTM").isNull()
            | (F.length(trim("LAST_UPDT_DTM")) == 0)
            | (trim("LAST_UPDT_DTM") == F.lit("UNK")),
            F.lit("1753-01-01 00:00:00.000000")
        ).otherwise(F.col("LAST_UPDT_DTM"))
    )
    .withColumn(
        "CUST_SVC_TASK_CSTM_DTL_NM",
        F.when(
            F.col("TRGT_CD_NM").isNull()
            | (F.length(trim("TRGT_CD_NM")) == 0),
            F.lit("NA")
        ).otherwise(F.col("TRGT_CD_NM"))
    )
    .withColumn(
        "LAST_UPDT_USER_ID",
        F.when(
            F.col("USER_ID_Last_updt_User").isNull()
            | (F.length(trim("USER_ID_Last_updt_User")) == 0),
            F.lit("NA")
        ).otherwise(F.col("USER_ID_Last_updt_User"))
    )
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(EDWRunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(EDWRunCycle))
    .withColumn("CUST_SVC_TASK_CSTM_DTL_CD_SK", F.col("CUST_SVC_TASK_CSTM_DTL_CD_SK"))
    .select(
        "CUST_SVC_TASK_BCKDT_SK",
        "SRC_SYS_CD",
        "CUST_SVC_ID",
        "CUST_SVC_TASK_SEQ_NO",
        "CUST_SVC_TASK_BCKDT_SEQ_NO",
        "CUST_SVC_TASK_CSTM_DTL_CD",
        "CUST_SVC_TASK_CSTM_DTL_UNIQ_ID",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "CRT_BY_USER_SK",
        "CUST_SVC_TASK_SK",
        "LAST_UPDT_USER_SK",
        "BCKDT_DT",
        "CRT_BY_USER_ID",
        "CUST_SVC_TASK_CSTM_DTL_CRT_DTM",
        "CS_TASK_CSTM_DTL_LAST_UPDT_DTM",
        "CUST_SVC_TASK_CSTM_DTL_NM",
        "LAST_UPDT_USER_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CUST_SVC_TASK_CSTM_DTL_CD_SK"
    )
)

schema_xfrm_BusinessLogic = StructType([
    StructField("CUST_SVC_TASK_BCKDT_SK", IntegerType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CUST_SVC_ID", StringType(), True),
    StructField("CUST_SVC_TASK_SEQ_NO", IntegerType(), True),
    StructField("CUST_SVC_TASK_BCKDT_SEQ_NO", IntegerType(), True),
    StructField("CUST_SVC_TASK_CSTM_DTL_CD", StringType(), True),
    StructField("CUST_SVC_TASK_CSTM_DTL_UNIQ_ID", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("CRT_BY_USER_SK", IntegerType(), True),
    StructField("CUST_SVC_TASK_SK", IntegerType(), True),
    StructField("LAST_UPDT_USER_SK", IntegerType(), True),
    StructField("BCKDT_DT", StringType(), True),
    StructField("CRT_BY_USER_ID", StringType(), True),
    StructField("CUST_SVC_TASK_CSTM_DTL_CRT_DTM", StringType(), True),
    StructField("CS_TASK_CSTM_DTL_LAST_UPDT_DTM", StringType(), True),
    StructField("CUST_SVC_TASK_CSTM_DTL_NM", StringType(), True),
    StructField("LAST_UPDT_USER_ID", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("CUST_SVC_TASK_CSTM_DTL_CD_SK", IntegerType(), True)
])

df_xfrm_BusinessLogic_lnk_NA = spark.createDataFrame(
    data=[(
        1,
        'NA',
        'NA',
        0,
        0,
        'NA',
        '1753-01-01 00:00:00',
        '1753-01-01',
        '1753-01-01',
        1,
        1,
        1,
        '1753-01-01 00:00:00',
        'NA',
        '1753-01-01 00:00:00',
        '1753-01-01 00:00:00',
        'NA',
        'NA',
        100,
        100,
        1
    )],
    schema=schema_xfrm_BusinessLogic
)

df_xfrm_BusinessLogic_lnk_UNK = spark.createDataFrame(
    data=[(
        0,
        'UNK',
        'UNK',
        0,
        0,
        'UNK',
        '1753-01-01 00:00:00',
        '1753-01-01',
        '1753-01-01',
        0,
        0,
        0,
        '1753-01-01 00:00:00',
        'UNK',
        '1753-01-01 00:00:00',
        '1753-01-01 00:00:00',
        'UNK',
        'UNK',
        100,
        100,
        0
    )],
    schema=schema_xfrm_BusinessLogic
)

df_fnl_CustSvcTaskBckdtD = (
    df_xfrm_BusinessLogic_lnk_FullData
    .unionByName(df_xfrm_BusinessLogic_lnk_NA)
    .unionByName(df_xfrm_BusinessLogic_lnk_UNK)
)

df_fnl_CustSvcTaskBckdtD_final = (
    df_fnl_CustSvcTaskBckdtD
    .select(
        F.col("CUST_SVC_TASK_BCKDT_SK"),
        F.col("SRC_SYS_CD"),
        F.col("CUST_SVC_ID"),
        F.col("CUST_SVC_TASK_SEQ_NO"),
        F.col("CUST_SVC_TASK_BCKDT_SEQ_NO"),
        F.col("CUST_SVC_TASK_CSTM_DTL_CD"),
        F.col("CUST_SVC_TASK_CSTM_DTL_UNIQ_ID"),
        rpad("CRT_RUN_CYC_EXCTN_DT_SK", 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        rpad("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("CRT_BY_USER_SK"),
        F.col("CUST_SVC_TASK_SK"),
        F.col("LAST_UPDT_USER_SK"),
        F.col("BCKDT_DT"),
        rpad("CRT_BY_USER_ID", 10, " ").alias("CRT_BY_USER_ID"),
        F.col("CUST_SVC_TASK_CSTM_DTL_CRT_DTM"),
        F.col("CS_TASK_CSTM_DTL_LAST_UPDT_DTM"),
        F.col("CUST_SVC_TASK_CSTM_DTL_NM"),
        rpad("LAST_UPDT_USER_ID", 10, " ").alias("LAST_UPDT_USER_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("CUST_SVC_TASK_CSTM_DTL_CD_SK")
    )
)

write_files(
    df_fnl_CustSvcTaskBckdtD_final,
    f"{adls_path}/load/CUST_SVC_TASK_BCKDT_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)