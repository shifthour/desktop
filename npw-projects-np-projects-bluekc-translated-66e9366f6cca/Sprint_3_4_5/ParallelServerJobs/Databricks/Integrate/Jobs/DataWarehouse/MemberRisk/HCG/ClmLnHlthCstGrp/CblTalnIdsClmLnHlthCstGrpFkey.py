# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #               Change Description                             Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Raja Gummadi                  2015-04-22     5460                                Originally Programmed                                 IntegrateNewDevl          Kalyan Neelam             2015-04-23
# MAGIC Raja Gummadi                  2015-05-12     5460                                Deleted 32 Columns and lookups                IntegrateNewDevl          Kalyan Neelam             2015-05-15
# MAGIC Raja Gummadi                  2016-08-22      TFS-12593                     Added FCLTY_CASE_ID column                 IntegrateDev1               Kalyan Neelam             2016-08-24
# MAGIC 
# MAGIC Manasa Andru                  2020-04-09       US - 191510                Changed the FKey error file write mode        IntegrateDev2	         Jaideep Mankala       04/09/2020
# MAGIC                                                                                                                         to 'Append' from 'Overwrite'
# MAGIC 
# MAGIC Razia                                 2021-12-02           US416687                 added SRS_SYS_CD" as part of lookup key           IntegrateDev2       Goutham K                 12/3/2021
# MAGIC 
# MAGIC 
# MAGIC Goutham Kalidindi             2023-05-14         US-583538               Replace Sparse lookup with JOIN Stage                 IntegrateDev1           Reddy Sanam              05/17/2023
# MAGIC                                                                                                      for performace improvement

# MAGIC MR_LN_SVC_CAT lookup
# MAGIC CLM_LN lookup
# MAGIC This is a load ready file that will go into Load job
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DecimalType
from pyspark.sql.functions import col, lit, when, concat, rpad
from pyspark.sql import DataFrame
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner', '')
ids_secret_name = get_widget_value('ids_secret_name', '')
SrcSysCd = get_widget_value('SrcSysCd', '')
RunID = get_widget_value('RunID', '')
PrefixFkeyFailedFileName = get_widget_value('PrefixFkeyFailedFileName', '')

DSJobName = "CblTalnIdsClmLnHlthCstGrpFkey"

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_K_CLM_LN = f"SELECT CLM_ID AS CLM_ID, CLM_LN_SEQ_NO AS CLM_LN_SEQ_NO, SRC_SYS_CD_SK as CLM_SRC_SYS_CD_SK, CLM_LN_SK FROM {IDSOwner}.K_CLM_LN"
df_K_CLM_LN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_K_CLM_LN)
    .load()
)

schema_seqCLM_LN_HLTH_CST_GRP_Pkey = StructType([
    StructField("PRI_NAT_KEY_STRING", StringType(), False),
    StructField("FIRST_RECYC_TS", TimestampType(), False),
    StructField("CLM_LN_HLTH_CST_GRP_SK", IntegerType(), True),
    StructField("CLM_ID", StringType(), False),
    StructField("CLM_LN_SEQ_NO", IntegerType(), False),
    StructField("CLM_SRC_SYS_CD", StringType(), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("CASE_ADM_ID", StringType(), False),
    StructField("MR_LN", StringType(), False),
    StructField("MR_LN_DTL", StringType(), False),
    StructField("MR_LN_CASE", StringType(), False),
    StructField("MR_CASES_ADM", IntegerType(), True),
    StructField("MR_UNIT_DAYS", DecimalType(38,10), True),
    StructField("MR_PROC", IntegerType(), True),
    StructField("FCLTY_CASE_ID", StringType(), True)
])

df_seqCLM_LN_HLTH_CST_GRP_Pkey = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "^")
    .option("delimiter", ",")
    .option("nullValue", None)
    .schema(schema_seqCLM_LN_HLTH_CST_GRP_Pkey)
    .load(f"{adls_path}/key/CLM_LN_HLTH_CST_GRP.{SrcSysCd}.pkey.{RunID}.dat")
)

extract_query_db2_K_MR_LN = (
    f"SELECT K.MR_LN_ID, K.MR_LN_SVC_CAT_SK, K.SRC_SYS_CD "
    f"FROM {IDSOwner}.K_MR_LN_SVC_CAT K "
    f"INNER JOIN (SELECT M.MR_LN_ID, MAX(M.EFF_DT_SK) AS MAX_EFF_DT_SK, M.SRC_SYS_CD "
    f"            FROM {IDSOwner}.K_MR_LN_SVC_CAT M "
    f"            GROUP BY M.MR_LN_ID, M.SRC_SYS_CD) MR "
    f"ON K.MR_LN_ID = MR.MR_LN_ID AND K.EFF_DT_SK = MR.MAX_EFF_DT_SK AND MR.SRC_SYS_CD = K.SRC_SYS_CD"
)
df_db2_K_MR_LN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_K_MR_LN)
    .load()
)

df_ds_CD_MPPNG_LkpData = spark.read.parquet(f"{adls_path}/ds/CD_MPPNG.parquet")

df_fltr_FilterData = df_ds_CD_MPPNG_LkpData.filter(
    (col("SRC_SYS_CD") == "IDS") &
    (col("SRC_CLCTN_CD") == "IDS") &
    (col("TRGT_CLCTN_CD") == "IDS") &
    (col("SRC_DOMAIN_NM") == "SOURCE SYSTEM") &
    (col("TRGT_DOMAIN_NM") == "SOURCE SYSTEM")
)

df_ClmSrcLkup = df_fltr_FilterData.select(
    col("SRC_CD").alias("SRC_CD"),
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

df_LkupFkey = (
    df_seqCLM_LN_HLTH_CST_GRP_Pkey.alias("PkeyIn")
    .join(
        df_ClmSrcLkup.alias("ClmSrcLkup"),
        col("PkeyIn.CLM_SRC_SYS_CD") == col("ClmSrcLkup.SRC_CD"),
        "left"
    )
    .join(
        df_db2_K_MR_LN.alias("MrLnSvcLkup"),
        (col("PkeyIn.MR_LN") == col("MrLnSvcLkup.MR_LN_ID")) &
        (col("PkeyIn.SRC_SYS_CD") == col("MrLnSvcLkup.SRC_SYS_CD")),
        "left"
    )
    .select(
        col("PkeyIn.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        col("PkeyIn.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        col("PkeyIn.CLM_LN_HLTH_CST_GRP_SK").alias("CLM_LN_HLTH_CST_GRP_SK"),
        col("PkeyIn.CLM_ID").alias("CLM_ID"),
        col("PkeyIn.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        col("PkeyIn.CLM_SRC_SYS_CD").alias("CLM_SRC_SYS_CD"),
        col("PkeyIn.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PkeyIn.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("PkeyIn.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("PkeyIn.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        col("PkeyIn.CASE_ADM_ID").alias("CASE_ADM_ID"),
        col("PkeyIn.MR_LN").alias("MR_LN"),
        col("PkeyIn.MR_LN_DTL").alias("MR_LN_DTL"),
        col("PkeyIn.MR_LN_CASE").alias("MR_LN_CASE"),
        col("PkeyIn.MR_CASES_ADM").alias("MR_CASES_ADM"),
        col("PkeyIn.MR_UNIT_DAYS").alias("MR_UNIT_DAYS"),
        col("PkeyIn.MR_PROC").alias("MR_PROC"),
        col("ClmSrcLkup.CD_MPPNG_SK").alias("CLM_SRC_SYS_CD_SK"),
        col("MrLnSvcLkup.MR_LN_SVC_CAT_SK").alias("MR_LN_SVC_CAT_SK"),
        col("PkeyIn.FCLTY_CASE_ID").alias("FCLTY_CASE_ID")
    )
)

df_Join_71 = (
    df_LkupFkey.alias("In")
    .join(
        df_K_CLM_LN.alias("clmlkup"),
        (
            (col("In.CLM_ID") == col("clmlkup.CLM_ID")) &
            (col("In.CLM_LN_SEQ_NO") == col("clmlkup.CLM_LN_SEQ_NO")) &
            (col("In.CLM_SRC_SYS_CD_SK") == col("clmlkup.CLM_SRC_SYS_CD_SK"))
        ),
        "left"
    )
    .select(
        col("In.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        col("In.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        col("In.CLM_LN_HLTH_CST_GRP_SK").alias("CLM_LN_HLTH_CST_GRP_SK"),
        col("In.CLM_ID").alias("CLM_ID"),
        col("In.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        col("In.CLM_SRC_SYS_CD").alias("CLM_SRC_SYS_CD"),
        col("In.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("In.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("In.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("In.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        col("In.CASE_ADM_ID").alias("CASE_ADM_ID"),
        col("In.MR_LN").alias("MR_LN"),
        col("In.MR_LN_DTL").alias("MR_LN_DTL"),
        col("In.MR_LN_CASE").alias("MR_LN_CASE"),
        col("In.MR_CASES_ADM").alias("MR_CASES_ADM"),
        col("In.MR_UNIT_DAYS").alias("MR_UNIT_DAYS"),
        col("In.MR_PROC").alias("MR_PROC"),
        col("In.CLM_SRC_SYS_CD_SK").alias("CLM_SRC_SYS_CD_SK"),
        col("In.MR_LN_SVC_CAT_SK").alias("MR_LN_SVC_CAT_SK"),
        col("clmlkup.CLM_LN_SK").alias("CLM_LN_SK"),
        col("In.FCLTY_CASE_ID").alias("FCLTY_CASE_ID")
    )
)

df_xfm_CheckLkpResults = df_Join_71.select(
    col("*"),
    when(col("CLM_SRC_SYS_CD_SK").isNull(), lit("Y")).otherwise(lit("N")).alias("svFkeyClmSrcSysCdCheck"),
    when(col("MR_LN_SVC_CAT_SK").isNull(), lit("Y")).otherwise(lit("N")).alias("svFkeyMrLnSvcCatCheck"),
    when(col("CLM_LN_SK").isNull(), lit("Y")).otherwise(lit("N")).alias("svFkeyClmLnCheck"),
    col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING")
)

df_Lnk_Main = df_xfm_CheckLkpResults.select(
    when(col("CLM_LN_HLTH_CST_GRP_SK").isNull(), lit(None)).otherwise(col("CLM_LN_HLTH_CST_GRP_SK")).alias("CLM_LN_HLTH_CST_GRP_SK"),
    col("CLM_ID").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    when(col("CLM_SRC_SYS_CD_SK").isNull(), lit(0)).otherwise(col("CLM_SRC_SYS_CD_SK")).alias("CLM_SRC_SYS_CD_SK"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    when(col("CLM_LN_SK").isNull(), lit(0)).otherwise(col("CLM_LN_SK")).alias("CLM_LN_SK"),
    when(col("MR_LN_SVC_CAT_SK").isNull(), lit(0)).otherwise(col("MR_LN_SVC_CAT_SK")).alias("MR_LN_SVC_CAT_SK"),
    col("MR_CASES_ADM").alias("MR_CASE_ADM_CT"),
    col("MR_PROC").alias("MR_PROC_CT"),
    col("MR_UNIT_DAYS").alias("MR_UNIT_DAY_CT"),
    col("CASE_ADM_ID").alias("CASE_ADM_ID"),
    col("MR_LN").alias("MR_LN_ID"),
    col("MR_LN_CASE").alias("MR_LN_CASE_ID"),
    col("MR_LN_DTL").alias("MR_LN_DTL_ID"),
    col("FCLTY_CASE_ID").alias("FCLTY_CASE_ID")
)

df_Lnk_UNK = df_xfm_CheckLkpResults.limit(1).select(
    lit(0).alias("CLM_LN_HLTH_CST_GRP_SK"),
    lit("UNK").alias("CLM_ID"),
    lit(0).alias("CLM_LN_SEQ_NO"),
    lit(0).alias("CLM_SRC_SYS_CD_SK"),
    lit(0).alias("SRC_SYS_CD_SK"),
    lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("CLM_LN_SK"),
    lit(0).alias("MR_LN_SVC_CAT_SK"),
    lit(0).alias("MR_CASE_ADM_CT"),
    lit(0).alias("MR_PROC_CT"),
    lit(0).alias("MR_UNIT_DAY_CT"),
    lit("UNK").alias("CASE_ADM_ID"),
    lit("UNK").alias("MR_LN_ID"),
    lit("UNK").alias("MR_LN_CASE_ID"),
    lit("UNK").alias("MR_LN_DTL_ID"),
    lit("UNK").alias("FCLTY_CASE_ID")
)

df_Lnk_NA = df_xfm_CheckLkpResults.limit(1).select(
    lit(1).alias("CLM_LN_HLTH_CST_GRP_SK"),
    lit("NA").alias("CLM_ID"),
    lit(0).alias("CLM_LN_SEQ_NO"),
    lit(1).alias("CLM_SRC_SYS_CD_SK"),
    lit(1).alias("SRC_SYS_CD_SK"),
    lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("CLM_LN_SK"),
    lit(1).alias("MR_LN_SVC_CAT_SK"),
    lit(0).alias("MR_CASE_ADM_CT"),
    lit(0).alias("MR_PROC_CT"),
    lit(0).alias("MR_UNIT_DAY_CT"),
    lit("NA").alias("CASE_ADM_ID"),
    lit("NA").alias("MR_LN_ID"),
    lit("NA").alias("MR_LN_CASE_ID"),
    lit("NA").alias("MR_LN_DTL_ID"),
    lit("NA").alias("FCLTY_CASE_ID")
)

df_SrcSysFail = df_xfm_CheckLkpResults.filter(col("svFkeyClmSrcSysCdCheck") == "Y").select(
    col("CLM_LN_HLTH_CST_GRP_SK").alias("PRI_SK"),
    col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    lit(DSJobName).alias("JOB_NM"),
    lit("CDLOOKUP - CLM_SRC_SYS_CD").alias("ERROR_TYP"),
    lit("CD_MPPNG").alias("PHYSCL_FILE_NM"),
    concat(
        lit("IDS"), lit(";"), lit("IDS"), lit(";"), lit("IDS"), lit(";"),
        lit("SOURCE SYSTEM"), lit(";"), lit("SOURCE SYSTEM"), lit(";"),
        col("CLM_SRC_SYS_CD")
    ).alias("FRGN_NAT_KEY_STRING"),
    col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_MrLnFail = df_xfm_CheckLkpResults.filter(col("svFkeyMrLnSvcCatCheck") == "Y").select(
    col("CLM_LN_HLTH_CST_GRP_SK").alias("PRI_SK"),
    col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    lit(DSJobName).alias("JOB_NM"),
    lit("FKEYLOOKUP - MR_LN_SVC_CAT").alias("ERROR_TYP"),
    lit("MR_LN_SVC_CAT").alias("PHYSCL_FILE_NM"),
    col("MR_LN").alias("FRGN_NAT_KEY_STRING"),
    col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_ClmLnFail = df_xfm_CheckLkpResults.filter(col("svFkeyClmLnCheck") == "Y").select(
    col("CLM_LN_HLTH_CST_GRP_SK").alias("PRI_SK"),
    col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    lit(DSJobName).alias("JOB_NM"),
    lit("FKEYLOOKUP - CLM_LN_SK").alias("ERROR_TYP"),
    lit("K_CLM_LN").alias("PHYSCL_FILE_NM"),
    col("CLM_ID").alias("FRGN_NAT_KEY_STRING"),
    col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_Funnel_56 = df_SrcSysFail.unionByName(df_MrLnFail).unionByName(df_ClmLnFail)

df_final_fkeyFail = df_Funnel_56.select(
    col("PRI_SK"),
    rpad(col("PRI_NAT_KEY_STRING"), <...>, " ").alias("PRI_NAT_KEY_STRING"),
    col("SRC_SYS_CD_SK"),
    rpad(col("JOB_NM"), <...>, " ").alias("JOB_NM"),
    rpad(col("ERROR_TYP"), <...>, " ").alias("ERROR_TYP"),
    rpad(col("PHYSCL_FILE_NM"), <...>, " ").alias("PHYSCL_FILE_NM"),
    rpad(col("FRGN_NAT_KEY_STRING"), <...>, " ").alias("FRGN_NAT_KEY_STRING"),
    col("FIRST_RECYC_TS"),
    col("JOB_EXCTN_SK")
)

write_files(
    df_final_fkeyFail,
    f"{adls_path}/fkey_failures/{PrefixFkeyFailedFileName}.{DSJobName}.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

df_fnl_NA_UNK_Streams = df_Lnk_Main.unionByName(df_Lnk_UNK).unionByName(df_Lnk_NA)

df_final_fnl_NA_UNK_Streams = df_fnl_NA_UNK_Streams.select(
    col("CLM_LN_HLTH_CST_GRP_SK"),
    rpad(col("CLM_ID"), <...>, " ").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO"),
    col("CLM_SRC_SYS_CD_SK"),
    col("SRC_SYS_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CLM_LN_SK"),
    col("MR_LN_SVC_CAT_SK"),
    col("MR_CASE_ADM_CT"),
    col("MR_PROC_CT"),
    col("MR_UNIT_DAY_CT"),
    rpad(col("CASE_ADM_ID"), <...>, " ").alias("CASE_ADM_ID"),
    rpad(col("MR_LN_ID"), <...>, " ").alias("MR_LN_ID"),
    rpad(col("MR_LN_CASE_ID"), <...>, " ").alias("MR_LN_CASE_ID"),
    rpad(col("MR_LN_DTL_ID"), <...>, " ").alias("MR_LN_DTL_ID"),
    rpad(col("FCLTY_CASE_ID"), <...>, " ").alias("FCLTY_CASE_ID")
)

write_files(
    df_final_fnl_NA_UNK_Streams,
    f"{adls_path}/load/CLM_LN_HLTH_CST_GRP.{SrcSysCd}.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)