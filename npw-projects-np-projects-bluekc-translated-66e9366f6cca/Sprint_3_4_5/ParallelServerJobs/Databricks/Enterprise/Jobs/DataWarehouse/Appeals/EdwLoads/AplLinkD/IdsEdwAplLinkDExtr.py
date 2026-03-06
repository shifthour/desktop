# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:  Extracting data from APL_LINK
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                2/08/2007           Appeals/3028              Originally Programmed                        devlEDW10                  Steph Goddard               09/15/2007
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:  
# MAGIC                                                                                                                                                                                                                                         DATASTAGE                             CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                                 ENVIRONMENT                        REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        ---------------------------------------------------------                               --------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Balkarn Gill              11/07/2013        5114                              Create Load File for EDW Table APL_LINK_D                             EnterpriseWhseDevl                Jag Yelavarthi              2014-01-15
# MAGIC 
# MAGIC Ravi Singh              12/07/2018          MTM- 5841               Added in parameter and soruce query for extract NDBH,                  EnterpriseDev2                     Kalyan Neelam             2018-12-10 
# MAGIC                                                                                               Evicore(MEDSLTNS) and Telligen Appeal process data  
# MAGIC Praneeth Kakarla   21/02/2019          93883                       In the db2_APL_LINK_in stage removed the WHERE clause           EnterpriseDev2                     Kalyan Neelam             2019-02-22 
# MAGIC                                                                                          criteria that use the run cycle numbers in the IDS table from the SQL query

# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Job Name:IdsEdwAplLinkDExtr
# MAGIC Read from source table APL_LNK.  Apply Run Cycle filters
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC APL_LINK_TYP_CD_SK,
# MAGIC APL_LINK_RSN_CD_SK
# MAGIC Write APL_LINK_D Data into a Sequential file for Load Job IdsEdwAplLinkDLoad.
# MAGIC Add Defaults and Null Handling.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')
CurrRunCycle = get_widget_value('CurrRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

df_db2_APL_LINK_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", """SELECT 
APL_LINK.APL_LINK_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
APL_LINK.APL_ID,
APL_LINK.APL_LINK_TYP_CD_SK,
APL_LINK.APL_LINK_ID,
APL_LINK.CRT_RUN_CYC_EXCTN_SK,
APL_LINK.LAST_UPDT_RUN_CYC_EXCTN_SK,
APL_LINK.CASE_MGT_SK,
APL_LINK.CLM_SK,
APL_LINK.CUST_SVC_SK,
APL_LINK.LAST_UPDT_USER_SK,
APL_LINK.APL_SK,
APL_LINK.REL_APL_SK,
APL_LINK.UM_SK,
APL_LINK.APL_LINK_RSN_CD_SK,
APL_LINK.LAST_UPDT_DTM,
APL_LINK.APL_LINK_DESC 
FROM #$IDSOwner#.APL_LINK APL_LINK 
LEFT JOIN #$IDSOwner#.CD_MPPNG CD
ON APL_LINK .SRC_SYS_CD_SK = CD.CD_MPPNG_SK
;""")
    .load()
)

df_db2_APP_USER_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"""SELECT 
APP_USER.USER_SK,
APP_USER.USER_ID 
FROM #$IDSOwner#.APP_USER APP_USER;""")
    .load()
)

df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"""SELECT CD_MPPNG_SK,COALESCE(TRGT_CD,'UNK') TRGT_CD,COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM from #$IDSOwner#.CD_MPPNG""")
    .load()
)

df_Ref_AplLinkRsnCd_Lkp = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_AplLinkTypCd_Lkp = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_IdsEdwAplLinkDExtr_InAbc = df_db2_APL_LINK_in.alias("lnk_IdsEdwAplLinkDExtr_InAbc")
df_Ref_AplLinkTypCd_Lkp_ = df_Ref_AplLinkTypCd_Lkp.alias("Ref_AplLinkTypCd_Lkp")
df_Ref_AplLinkRsnCd_Lkp_ = df_Ref_AplLinkRsnCd_Lkp.alias("Ref_AplLinkRsnCd_Lkp")
df_lkp_UserSk_ref = df_db2_APP_USER_in.alias("lkp_UserSk_ref")

df_joined = (
    df_IdsEdwAplLinkDExtr_InAbc
    .join(
        df_Ref_AplLinkTypCd_Lkp_,
        F.col("lnk_IdsEdwAplLinkDExtr_InAbc.APL_LINK_TYP_CD_SK") == F.col("Ref_AplLinkTypCd_Lkp.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Ref_AplLinkRsnCd_Lkp_,
        F.col("lnk_IdsEdwAplLinkDExtr_InAbc.APL_LINK_RSN_CD_SK") == F.col("Ref_AplLinkRsnCd_Lkp.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_lkp_UserSk_ref,
        F.col("lnk_IdsEdwAplLinkDExtr_InAbc.LAST_UPDT_USER_SK") == F.col("lkp_UserSk_ref.USER_SK"),
        "left"
    )
)

df_lkp_Codes = df_joined.select(
    F.col("lnk_IdsEdwAplLinkDExtr_InAbc.APL_LINK_SK").alias("APL_LINK_SK"),
    F.col("lnk_IdsEdwAplLinkDExtr_InAbc.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_IdsEdwAplLinkDExtr_InAbc.APL_ID").alias("APL_ID"),
    F.col("Ref_AplLinkTypCd_Lkp.TRGT_CD").alias("APL_LINK_TYP_CD"),
    F.col("lnk_IdsEdwAplLinkDExtr_InAbc.APL_LINK_ID").alias("APL_LINK_ID"),
    F.col("lnk_IdsEdwAplLinkDExtr_InAbc.CASE_MGT_SK").alias("CASE_MGT_SK"),
    F.col("lnk_IdsEdwAplLinkDExtr_InAbc.CLM_SK").alias("CLM_SK"),
    F.col("lnk_IdsEdwAplLinkDExtr_InAbc.CUST_SVC_SK").alias("CUST_SVC_SK"),
    F.col("lnk_IdsEdwAplLinkDExtr_InAbc.LAST_UPDT_USER_SK").alias("LAST_UPDT_USER_SK"),
    F.col("lnk_IdsEdwAplLinkDExtr_InAbc.APL_SK").alias("APL_SK"),
    F.col("lnk_IdsEdwAplLinkDExtr_InAbc.REL_APL_SK").alias("REL_APL_SK"),
    F.col("lnk_IdsEdwAplLinkDExtr_InAbc.UM_SK").alias("UM_SK"),
    F.col("lnk_IdsEdwAplLinkDExtr_InAbc.APL_LINK_DESC").alias("APL_LINK_DESC"),
    F.col("Ref_AplLinkRsnCd_Lkp.TRGT_CD").alias("APL_LINK_RSN_CD"),
    F.col("Ref_AplLinkRsnCd_Lkp.TRGT_CD_NM").alias("APL_LINK_RSN_NM"),
    F.col("Ref_AplLinkTypCd_Lkp.TRGT_CD_NM").alias("APL_LINK_TYP_NM"),
    F.col("lnk_IdsEdwAplLinkDExtr_InAbc.LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    F.col("lkp_UserSk_ref.USER_ID").alias("USER_ID"),
    F.col("lnk_IdsEdwAplLinkDExtr_InAbc.APL_LINK_RSN_CD_SK").alias("APL_LINK_RSN_CD_SK"),
    F.col("lnk_IdsEdwAplLinkDExtr_InAbc.APL_LINK_TYP_CD_SK").alias("APL_LINK_TYP_CD_SK")
)

df_main = (
    df_lkp_Codes
    .filter((F.col("APL_LINK_SK") != 0) & (F.col("APL_LINK_SK") != 1))
    .select(
        F.col("APL_LINK_SK").alias("APL_LINK_SK"),
        F.when(F.trim(F.col("SRC_SYS_CD")) == "", F.lit("NA"))
         .otherwise(F.col("SRC_SYS_CD"))
         .alias("SRC_SYS_CD"),
        F.col("APL_ID").alias("APL_ID"),
        F.when(
            F.col("APL_LINK_TYP_CD").isNull() |
            (F.length(F.trim(F.col("APL_LINK_TYP_CD"))) == 0),
            F.lit("NA")
        ).otherwise(F.col("APL_LINK_TYP_CD")).alias("APL_LINK_TYP_CD"),
        F.col("APL_LINK_ID").alias("APL_LINK_ID"),
        F.lit(CurrRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(CurrRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("APL_SK").alias("APL_SK"),
        F.col("CASE_MGT_SK").alias("CASE_MGT_SK"),
        F.col("CLM_SK").alias("CLM_SK"),
        F.col("CUST_SVC_SK").alias("CUST_SVC_SK"),
        F.col("LAST_UPDT_USER_SK").alias("LAST_UPDT_USER_SK"),
        F.col("REL_APL_SK").alias("REL_APL_SK"),
        F.col("UM_SK").alias("UM_SK"),
        F.col("APL_LINK_DESC").alias("APL_LINK_DESC"),
        F.when(
            F.col("APL_LINK_RSN_CD").isNull() |
            (F.length(F.trim(F.col("APL_LINK_RSN_CD"))) == 0),
            F.lit("NA")
        ).otherwise(F.col("APL_LINK_RSN_CD")).alias("APL_LINK_RSN_CD"),
        F.when(
            F.col("APL_LINK_RSN_NM").isNull() |
            (F.length(F.trim(F.col("APL_LINK_RSN_NM"))) == 0),
            F.lit("NA")
        ).otherwise(F.col("APL_LINK_RSN_NM")).alias("APL_LINK_RSN_NM"),
        F.when(
            F.col("APL_LINK_TYP_NM").isNull() |
            (F.length(F.trim(F.col("APL_LINK_TYP_NM"))) == 0),
            F.lit("NA")
        ).otherwise(F.col("APL_LINK_TYP_NM")).alias("APL_LINK_TYP_NM"),
        F.substring(F.col("LAST_UPDT_DTM"), 1, 10).alias("LAST_UPDT_DT_SK"),
        F.when(
            F.col("USER_ID").isNull() |
            (F.length(F.trim(F.col("USER_ID"))) == 0),
            F.lit("NA")
        ).otherwise(F.col("USER_ID")).alias("LAST_UPDT_USER_ID"),
        F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("APL_LINK_RSN_CD_SK").alias("APL_LINK_RSN_CD_SK"),
        F.col("APL_LINK_TYP_CD_SK").alias("APL_LINK_TYP_CD_SK")
    )
)

schema_na_unk = StructType([
    StructField("APL_LINK_SK", IntegerType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("APL_ID", StringType(), True),
    StructField("APL_LINK_TYP_CD", StringType(), True),
    StructField("APL_LINK_ID", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("APL_SK", IntegerType(), True),
    StructField("CASE_MGT_SK", IntegerType(), True),
    StructField("CLM_SK", IntegerType(), True),
    StructField("CUST_SVC_SK", IntegerType(), True),
    StructField("LAST_UPDT_USER_SK", IntegerType(), True),
    StructField("REL_APL_SK", IntegerType(), True),
    StructField("UM_SK", IntegerType(), True),
    StructField("APL_LINK_DESC", StringType(), True),
    StructField("APL_LINK_RSN_CD", StringType(), True),
    StructField("APL_LINK_RSN_NM", StringType(), True),
    StructField("APL_LINK_TYP_NM", StringType(), True),
    StructField("LAST_UPDT_DT_SK", StringType(), True),
    StructField("LAST_UPDT_USER_ID", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("APL_LINK_RSN_CD_SK", IntegerType(), True),
    StructField("APL_LINK_TYP_CD_SK", IntegerType(), True)
])

df_NA = spark.createDataFrame([
    (
        1, 'NA', 'NA', 'NA', 'NA',
        '1753-01-01', '1753-01-01',
        1, 1, 1, 1, 1, 1, 1,
        None, 'NA', 'NA', 'NA',
        '1753-01-01', 'NA',
        100, 100,
        1, 1
    )
], schema_na_unk)

df_UNK = spark.createDataFrame([
    (
        0, 'UNK', 'UNK', 'UNK', 'UNK',
        '1753-01-01', '1753-01-01',
        0, 0, 0, 0, 0, 0, 0,
        None, 'UNK', 'UNK', 'UNK',
        '1753-01-01', 'UNK',
        100, 100,
        0, 0
    )
], schema_na_unk)

df_fnl_dataLinks = df_NA.unionByName(df_main).unionByName(df_UNK)

df_fnl_dataLinks = (
    df_fnl_dataLinks
    .withColumn("USER_ID", F.rpad(F.col("LAST_UPDT_USER_ID"), 10, " "))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_DT_SK", F.rpad(F.col("LAST_UPDT_DT_SK"), 10, " "))
    .select(
        "APL_LINK_SK",
        "SRC_SYS_CD",
        "APL_ID",
        "APL_LINK_TYP_CD",
        "APL_LINK_ID",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "APL_SK",
        "CASE_MGT_SK",
        "CLM_SK",
        "CUST_SVC_SK",
        "LAST_UPDT_USER_SK",
        "REL_APL_SK",
        "UM_SK",
        "APL_LINK_DESC",
        "APL_LINK_RSN_CD",
        "APL_LINK_RSN_NM",
        "APL_LINK_TYP_NM",
        "LAST_UPDT_DT_SK",
        "USER_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "APL_LINK_RSN_CD_SK",
        "APL_LINK_TYP_CD_SK"
    )
)

write_files(
    df_fnl_dataLinks,
    f"{adls_path}/load/APL_LINK_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)