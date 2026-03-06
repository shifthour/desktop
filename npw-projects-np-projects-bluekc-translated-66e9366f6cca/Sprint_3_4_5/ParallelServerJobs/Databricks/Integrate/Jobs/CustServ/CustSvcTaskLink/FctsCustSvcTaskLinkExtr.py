# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #                    Change Description                                       Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   --------------------------------              ---------------------------------------------------------              ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               1/25/2007                3028                         Originally Programmed                                      devlIDS30                    Steph Goddard             02/21/2007
# MAGIC              
# MAGIC Parikshith Chada              06/22/2007              3264                         Added balancing process to the overall job       devlIDS30                    Steph Goddrad             09/14/2007
# MAGIC Ralph Tucker                   12/30/2007    15                                        Added Hit List Processing                                 devlIDS30                    Steph Goddard             01/09/2008
# MAGIC Ralph Tucker                   1/15/2008      15                                        Changed driver table name                               devlIDS                        Steph Goddard             01/17/2008
# MAGIC Ralph Tucker                   1/24/2008      15                                        Took out driver table for full Facets pull            devlIDS                        Steph Goddard             01/30/2008
# MAGIC                                                                                                             to accomadate Load/Replace
# MAGIC Brent Leland                    03/04/2008    3567 Primary Key                 Changed to use new primary key process        devlIDScur                    Steph Goddard            05/06/2008
# MAGIC 
# MAGIC Manasa Andru                 2013-09-30      TTR - 1056                          Corrected the input value in the After-Job        IntegrateNewDevl         Kalyan Neelam             2013-10-03
# MAGIC                                                                                                         subroutine and corrected the job for standards.
# MAGIC 
# MAGIC Manasa Andru                 2013-10-17      TTR - 1230                      Updated APL_SK in the Snapshot transformer   IntegrateNewDevl       Bhoomi Dasari              10/23/2013
# MAGIC                                                                                                                  and Upcased CSLI_TYPE field.
# MAGIC Prabhu ES                       2022-03-01      S2S Remediation             MSSQL connection parameters added               IntegrateDev5                Kalyan Neelam            2022-06-09

# MAGIC This is a full extract from Facets and  replace into the IDS because the Facets keys can change
# MAGIC Container hash file cleared by after-job subroutine
# MAGIC Strip Carriage Return, Line Feed, and Tab.
# MAGIC Extract Facets Customer Service Task Link Data
# MAGIC Apply business logic
# MAGIC Balancing snapshot of source table
# MAGIC Writing Sequential File to /pkey
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


CurrRunCycle = get_widget_value('CurrRunCycle','100')
RunID = get_widget_value('RunID','99')
CurrDate = get_widget_value('CurrDate','2008-03-31')
SrcSysCdSk = get_widget_value('SrcSysCdSk','72686')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# MAGIC %run ../../../../shared_containers/PrimaryKey/CustSvcTaskLinkPK
# COMMAND ----------

jdbc_url, jdbc_props = get_db_config(facets_secret_name)
extract_query = f"""
SELECT
    task.CSSC_ID,
    task.CSTK_SEQ_NO,
    link.CSLI_TYPE,
    link.CSLI_ID,
    link.CSLI_MCTR_REAS,
    link.CSLI_DESC,
    link.CSLI_LAST_UPD_DTM,
    link.CSLI_LAST_UPD_USID
FROM
    {FacetsOwner}.CMC_CSTK_TASK task,
    {FacetsOwner}.CMC_CSLI_LINK link
WHERE
    task.CSSC_ID=link.CSSC_ID
    AND task.CSTK_SEQ_NO=link.CSTK_SEQ_NO
"""
df_FctsCustSvcTaskLinkExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_StripFields = (
    df_FctsCustSvcTaskLinkExtr
    .withColumn("CSSC_ID", UpCase(Convert("CHAR(10) : CHAR(13) : CHAR(9)", "", F.col("CSSC_ID"))))
    .withColumn("CSTK_SEQ_NO", F.col("CSTK_SEQ_NO"))
    .withColumn("CSLI_TYPE", UpCase(Convert("CHAR(10) : CHAR(13) : CHAR(9)", "", F.col("CSLI_TYPE"))))
    .withColumn("CSLI_ID", UpCase(Convert("CHAR(10) : CHAR(13) : CHAR(9)", "", F.col("CSLI_ID"))))
    .withColumn("CSLI_MCTR_REAS", Convert("CHAR(10) : CHAR(13) : CHAR(9)", "", F.col("CSLI_MCTR_REAS")))
    .withColumn("CSLI_DESC", UpCase(Convert("CHAR(10) : CHAR(13) : CHAR(9)", "", F.col("CSLI_DESC"))))
    .withColumn("CSLI_LAST_UPD_USID", Convert("CHAR(10) : CHAR(13) : CHAR(9)", "", F.col("CSLI_LAST_UPD_USID")))
    .withColumn("CSLI_LAST_UPD_DTM", F.col("CSLI_LAST_UPD_DTM"))
)

df_hf_cust_svc_task_link_dedup = df_StripFields.dropDuplicates(["CSSC_ID","CSTK_SEQ_NO","CSLI_TYPE","CSLI_ID"])

df_BusinessRules_vars = (
    df_hf_cust_svc_task_link_dedup
    .withColumn(
        "RowPassThru",
        F.lit("Y")
    )
    .withColumn(
        "svSrcSysCd",
        F.lit("FACETS")
    )
    .withColumn(
        "svCsscId",
        trim(F.col("CSSC_ID"))
    )
    .withColumn(
        "svLnkTypCd",
        F.when(
            F.col("CSLI_TYPE").isNull() | (trim(F.col("CSLI_TYPE")) == ""),
            F.lit("NA")
        ).otherwise(trim(F.col("CSLI_TYPE")))
    )
    .withColumn(
        "svLnkRcrdId",
        F.when(
            F.col("CSLI_ID").isNull() | (trim(F.col("CSLI_ID")) == ""),
            F.lit("NA")
        ).otherwise(trim(F.col("CSLI_ID")))
    )
    .withColumn(
        "svCsliId",
        F.when(
            F.col("CSLI_ID").isNull() | (trim(F.col("CSLI_ID")) == ""),
            F.lit("NA")
        ).otherwise(trim(F.col("CSLI_ID")))
    )
)

df_BusinessRules_AllCol = (
    df_BusinessRules_vars
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))
    .withColumn("CUST_SVC_ID", F.col("svCsscId"))
    .withColumn("TASK_SEQ_NO", F.col("CSTK_SEQ_NO"))
    .withColumn("CUST_SVC_TASK_LINK_TYP_CD", F.col("svLnkTypCd"))
    .withColumn("LINK_RCRD_ID", F.col("svLnkRcrdId"))
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.col("RowPassThru"))
    .withColumn("FIRST_RECYC_DT", F.lit(CurrDate))
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.col("svSrcSysCd"))
    .withColumn(
        "PRI_KEY_STRING",
        F.concat_ws(
            ";",
            F.col("svSrcSysCd"),
            F.col("svCsscId"),
            F.col("CSTK_SEQ_NO"),
            F.col("svLnkTypCd"),
            F.col("svLnkRcrdId")
        )
    )
    .withColumn("CUST_SVC_TASK_LINK_SK", F.lit(0))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn(
        "APL_ID",
        F.when(
            trim(F.col("CSLI_TYPE")) == "A",
            trim(F.col("CSLI_ID"))
        ).otherwise(F.lit("NA"))
    )
    .withColumn("CLM_ID", F.col("svCsliId"))
    .withColumn("CUST_SVC_TASK_ID", F.col("CSTK_SEQ_NO"))
    .withColumn(
        "LAST_UPDT_USER_ID",
        F.when(
            F.col("CSLI_LAST_UPD_USID").isNull() | (trim(F.col("CSLI_LAST_UPD_USID")) == ""),
            F.lit("NA")
        ).otherwise(trim(F.col("CSLI_LAST_UPD_USID")))
    )
    .withColumn("REL_CUST_SVC_ID", F.col("svCsliId"))
    .withColumn("UM_ID", F.col("svCsliId"))
    .withColumn(
        "CUST_SVC_TASK_LINK_RSN_CD",
        F.when(
            F.col("CSLI_MCTR_REAS").isNull() | (trim(F.col("CSLI_MCTR_REAS")) == ""),
            F.lit("NA")
        ).otherwise(trim(F.col("CSLI_MCTR_REAS")))
    )
    .withColumn(
        "LAST_UPDT_DTM",
        FORMAT.DATE(F.col("CSLI_LAST_UPD_DTM"), "SYBASE", "TIMESTAMP", "DB2Timestamp")
    )
    .withColumn(
        "LINK_DESC",
        F.when(
            F.col("CSLI_DESC").isNull() | (trim(F.col("CSLI_DESC")) == ""),
            F.lit(None)
        ).otherwise(trim(F.col("CSLI_DESC")))
    )
)

df_BusinessRules_Transform = (
    df_BusinessRules_vars
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))
    .withColumn("CUST_SVC_ID", F.col("svCsscId"))
    .withColumn("TASK_SEQ_NO", F.col("CSTK_SEQ_NO"))
    .withColumn("CUST_SVC_TASK_LINK_TYP_CD", F.col("svLnkTypCd"))
    .withColumn("LINK_RCRD_ID", F.col("svLnkRcrdId"))
)

params_CustSvcTaskLinkPK = {
    "CurrRunCycle": CurrRunCycle,
    "RunID": RunID,
    "CurrDate": CurrDate,
    "SrcSysCdSk": SrcSysCdSk,
    "SrcSysCd": "FACETS",
    "IDSOwner": IDSOwner
}
df_CustSvcTaskLinkPK_Key = CustSvcTaskLinkPK(df_BusinessRules_AllCol, df_BusinessRules_Transform, params_CustSvcTaskLinkPK)

df_IdsCustSvcTaskLink = (
    df_CustSvcTaskLinkPK_Key
    .select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "CUST_SVC_TASK_LINK_SK",
        "CUST_SVC_ID",
        "TASK_SEQ_NO",
        "CUST_SVC_TASK_LINK_TYP_CD",
        "LINK_RCRD_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "APL_ID",
        "CLM_ID",
        "CUST_SVC_TASK_ID",
        "LAST_UPDT_USER_ID",
        "REL_CUST_SVC_ID",
        "UM_ID",
        "CUST_SVC_TASK_LINK_RSN_CD",
        "LAST_UPDT_DTM",
        "LINK_DESC"
    )
    .withColumn("INSRT_UPDT_CD", rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(F.col("PASS_THRU_IN"), 1, " "))
)

write_files(
    df_IdsCustSvcTaskLink,
    f"{adls_path}/key/IdsCustSvcTaskLinkExtr.CSTaskLink.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
snapshot_query = f"""
SELECT
    task.CSSC_ID,
    task.CSTK_SEQ_NO,
    link.CSLI_TYPE,
    link.CSLI_ID
FROM
    {FacetsOwner}.CMC_CSTK_TASK task,
    {FacetsOwner}.CMC_CSLI_LINK link
WHERE
    task.CSSC_ID=link.CSSC_ID
    AND task.CSTK_SEQ_NO=link.CSTK_SEQ_NO
"""
df_Facets_Source = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", snapshot_query)
    .load()
)

df_Transform_vars = (
    df_Facets_Source
    .withColumn(
        "svCustSvcTaskLinkTypCdSk",
        GetFkeyCodes(
            "FACETS",
            F.lit(100),
            "CUSTOMER SERVICE TASK LINK TYPE",
            trim(Convert("CHAR(10) : CHAR(13) : CHAR(9)", "", F.col("CSLI_TYPE"))),
            "X"
        )
    )
)

df_Transform_out = (
    df_Transform_vars
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))
    .withColumn("CUST_SVC_ID", trim(UpCase(Convert("CHAR(10) : CHAR(13) : CHAR(9)", "", F.col("CSSC_ID")))))
    .withColumn("TASK_SEQ_NO", F.col("CSTK_SEQ_NO"))
    .withColumn(
        "CUST_SVC_TASK_LINK_TYP_CD_SK",
        F.when(
            F.col("CSLI_TYPE").isNull() | (trim(F.col("CSLI_TYPE")) == ""),
            F.lit(1)
        ).otherwise(F.col("svCustSvcTaskLinkTypCdSk"))
    )
    .withColumn(
        "LINK_RCRD_ID",
        F.when(
            F.col("CSLI_ID").isNull() | (trim(F.col("CSLI_ID")) == ""),
            F.lit("NA")
        ).otherwise(trim(UpCase(Convert("CHAR(10) : CHAR(13) : CHAR(9)", "", F.col("CSLI_ID")))))
    )
    .withColumn(
        "APL_SK",
        F.when(
            trim(UpCase(F.col("CSLI_TYPE"))) == "A",
            GetFkeyApl("FACETS", F.lit(1), trim(UpCase(F.col("CSLI_ID"))), "X")
        ).otherwise(F.lit(1))
    )
)

df_Snapshot_File = df_Transform_out.select(
    "SRC_SYS_CD_SK",
    "CUST_SVC_ID",
    "TASK_SEQ_NO",
    "CUST_SVC_TASK_LINK_TYP_CD_SK",
    "LINK_RCRD_ID",
    "APL_SK"
)

write_files(
    df_Snapshot_File,
    f"{adls_path}/load/B_CUST_SVC_TASK_LINK.FACETS.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)