# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_4 10/21/08 10:40:52 Batch  14905_38463 PROMOTE bckcetl ids20 dsadm rc for brent  
# MAGIC ^1_4 10/21/08 10:17:42 Batch  14905_37069 INIT bckcett testIDS dsadm rc for brent 
# MAGIC ^1_1 10/20/08 12:51:44 Batch  14904_46312 PROMOTE bckcett testIDS u08717 Brent
# MAGIC ^1_1 10/20/08 10:48:46 Batch  14904_38931 INIT bckcett devlIDS u08717 Brent
# MAGIC ^1_1 08/01/08 10:50:09 Batch  14824_39011 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_7 07/23/08 15:42:49 Batch  14815_56575 PROMOTE bckcett devlIDS u08717 Brent
# MAGIC ^1_7 07/23/08 15:08:26 Batch  14815_54510 INIT bckcett devlIDScur u08717 Brent
# MAGIC ^1_6 05/12/08 08:17:54 Batch  14743_29878 INIT bckcett devlIDScur u08717 Brent
# MAGIC ^1_3 02/21/08 10:42:22 Batch  14662_38644 PROMOTE bckcetl ids20 dsadm bls for rt
# MAGIC ^1_3 02/21/08 10:33:14 Batch  14662_38014 INIT bckcett testIDS dsadm bls for rt
# MAGIC ^1_5 02/20/08 12:35:05 Batch  14661_45313 PROMOTE bckcett testIDS u03651 steph for Ralph - 2nd try!
# MAGIC ^1_5 02/20/08 12:30:38 Batch  14661_45042 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_4 02/20/08 12:24:32 Batch  14661_44675 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_1 02/01/08 09:35:42 Batch  14642_34546 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_1 01/03/08 07:55:47 Batch  14613_29071 PROMOTE bckcett devlIDS u06640 Ralph
# MAGIC ^1_1 01/03/08 07:43:28 Batch  14613_27814 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_1 11/01/07 16:40:40 Batch  14550_60068 PROMOTE bckcetl ids20 dsadm rc for brent
# MAGIC ^1_1 11/01/07 16:19:50 Batch  14550_58802 INIT bckcett testIDS30 dsadm rc for brent
# MAGIC ^1_2 10/29/07 15:58:44 Batch  14547_57529 PROMOTE bckcett testIDS30 u08717 Brent
# MAGIC ^1_2 10/29/07 15:35:38 Batch  14547_56143 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_1 10/29/07 14:48:31 Batch  14547_53315 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_4 06/07/07 11:21:23 Batch  14403_40889 PROMOTE bckcetl ids20 dsadm bls for rt
# MAGIC ^1_4 06/07/07 11:18:00 Batch  14403_40684 INIT bckcett testIDS30 dsadm bls for rt
# MAGIC ^1_3 06/07/07 10:48:58 Batch  14403_38948 PROMOTE bckcett testIDS30 u06640 Ralph
# MAGIC ^1_3 06/07/07 10:48:04 Batch  14403_38889 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_2 06/07/07 10:35:41 Batch  14403_38146 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_1 06/04/07 13:21:59 Batch  14400_48124 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_3 05/08/07 14:04:19 Batch  14373_50664 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_2 04/06/07 14:05:51 Batch  14341_50754 INIT bckcett devlIDS30 dsadm dsadm
# MAGIC ^1_1 04/05/07 13:31:14 Batch  14340_48677 INIT bckcett devlIDS30 dsadm dsadm
# MAGIC ^1_2 04/04/07 08:38:14 Batch  14339_31097 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_1 03/30/07 10:52:52 Batch  14334_39177 INIT bckcett devlIDS30 u03651 steffy
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007, 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:       
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                   Project/Altiris #               Change Description                                  Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------     -----------------------------------    ---------------------------------------------------------             ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               1/31/2007              3028                              Originally Programmed                            devlIDS30                    Steph Goddard             02/21/2007
# MAGIC Parik                               06/24/2007          3264                         Added balancing process to the overall job     devlIDS30                    Steph Goddard             09/14/2007
# MAGIC Ralph Tucker                 12/28/2007          15                             Added Hit List Processing                                devlIDS30                    Steph Goddard             01/09/2008
# MAGIC Ralph Tucker                 1/15/2008             15                            Changed driver table name                              devlIDS                        Steph Goddard             01/17/2008
# MAGIC                                                                                                      Changed to use Shared Container on Pkey  
# MAGIC Brent Leland                   02/26/2008      3567 Primary Key         Changed to use new primary key process         devlIDScur                  Steph Goddard             05/06/2008
# MAGIC Kalyan Neelam               2010-01-29         TTR - 604                   Added new field RTE_TO_GRP_ID                IntegrateWrhsDevl        Steph Goddard            01/30/2010
# MAGIC Prabhu ES                      2022-03-01       S2S Remediation          MSSQL connection parameters added            IntegrateDev5

# MAGIC Strip Fields
# MAGIC Apply business logic.
# MAGIC Writing Sequential File to ../key
# MAGIC Assign primary surrogate key
# MAGIC Container hash file cleared by after-job subroutine
# MAGIC Balancing snapshot of source table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/CustSvcTaskSttusPK
# COMMAND ----------

CurrRunCycle = get_widget_value('CurrRunCycle','120')
RunID = get_widget_value('RunID','20061108856')
CurrDate = get_widget_value('CurrDate','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
facets_secret_name = get_widget_value('facets_secret_name','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)

extract_query_facets_source = """
SELECT 
 sttus.CSSC_ID,
 sttus.CSTK_SEQ_NO,
 sttus.CSTS_SEQ_NO
FROM 
 #$FacetsOwner#.CMC_CSTS_STATUS sttus,
 #$FacetsOwner#.CMC_CSTK_TASK task,
 tempdb..TMP_IDS_CUST_SVC_DRVR DRVR
WHERE 
 sttus.CSSC_ID=task.CSSC_ID
 AND sttus.CSTK_SEQ_NO=task.CSTK_SEQ_NO
 AND task.CSSC_ID = DRVR.CSSC_ID
 AND task.CSTK_SEQ_NO = DRVR.CSTK_SEQ_NO
"""

df_Facets_Source = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_facets_source)
    .load()
)

df_Transform = (
    df_Facets_Source
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))
    .withColumn("CUST_SVC_ID", trim(strip_field(F.col("CSSC_ID"))))
    .withColumn("TASK_SEQ_NO", F.col("CSTK_SEQ_NO"))
    .withColumn("STTUS_SEQ_NO", F.col("CSTS_SEQ_NO"))
    .select(
        "SRC_SYS_CD_SK",
        "CUST_SVC_ID",
        "TASK_SEQ_NO",
        "STTUS_SEQ_NO"
    )
)

write_files(
    df_Transform.select(
        "SRC_SYS_CD_SK",
        "CUST_SVC_ID",
        "TASK_SEQ_NO",
        "STTUS_SEQ_NO"
    ),
    f"{adls_path}/load/B_CUST_SVC_TASK_STTUS.FACETS.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

extract_query_fcts_1 = """
SELECT 
 sttus.CSSC_ID,
 sttus.CSTK_SEQ_NO,
 sttus.CSTS_SEQ_NO,
 sttus.CSTS_USID,
 sttus.CSTS_ASSIGN_USID,
 sttus.CSTS_STS,
 sttus.CSTS_STS_DTM,
 sttus.CSTS_ASSIGN_UDEPT
FROM 
 #$FacetsOwner#.CMC_CSTS_STATUS sttus,
 #$FacetsOwner#.CMC_CSTK_TASK task,
 tempdb..TMP_IDS_CUST_SVC_DRVR DRVR
WHERE 
 sttus.CSSC_ID=task.CSSC_ID
 AND sttus.CSTK_SEQ_NO=task.CSTK_SEQ_NO
 AND task.CSSC_ID = DRVR.CSSC_ID
 AND task.CSTK_SEQ_NO = DRVR.CSTK_SEQ_NO
"""

extract_query_fcts_2 = """
SELECT 
 CSTK.CSSC_ID,
 CSTK.CSTK_SEQ_NO,
 CSTK.CSTK_LAST_UPD_DTM,
 CSTK.CSTK_MCTR_REAS
FROM 
 #$FacetsOwner#.CMC_CSTK_TASK CSTK,
 tempdb..TMP_IDS_CUST_SVC_DRVR DRVR
WHERE 
 CSTK.CSSC_ID = DRVR.CSSC_ID
 AND CSTK.CSTK_SEQ_NO = DRVR.CSTK_SEQ_NO
"""

df_FctsCustSvcTaskSttusExtr_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_fcts_1)
    .load()
)

df_FctsCustSvcTaskSttusExtr_TaskRsnExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_fcts_2)
    .load()
)

df_task_rsn_lkup = dedup_sort(
    df_FctsCustSvcTaskSttusExtr_TaskRsnExtr,
    ["CSSC_ID", "CSTK_SEQ_NO", "CSTK_LAST_UPD_DTM"],
    []
).select(
    "CSSC_ID",
    "CSTK_SEQ_NO",
    "CSTK_LAST_UPD_DTM",
    "CSTK_MCTR_REAS"
)

df_StripField_pre = (
    df_FctsCustSvcTaskSttusExtr_Extract.alias("Extract")
    .join(
        df_task_rsn_lkup.alias("TaskRsnLoad"),
        on=[
            F.col("Extract.CSSC_ID") == F.col("TaskRsnLoad.CSSC_ID"),
            F.col("Extract.CSTK_SEQ_NO") == F.col("TaskRsnLoad.CSTK_SEQ_NO"),
            F.col("Extract.CSTS_STS_DTM") == F.col("TaskRsnLoad.CSTK_LAST_UPD_DTM")
        ],
        how="left"
    )
)

df_StripField = df_StripField_pre.select(
    trim(strip_field(F.col("Extract.CSSC_ID"))).alias("CSSC_ID"),
    F.col("Extract.CSTK_SEQ_NO").alias("CSTK_SEQ_NO"),
    F.col("Extract.CSTS_SEQ_NO").alias("CSTS_SEQ_NO"),
    trim(strip_field(F.col("Extract.CSTS_USID"))).alias("CSTS_USID"),
    trim(strip_field(F.col("Extract.CSTS_ASSIGN_USID"))).alias("CSTS_ASSIGN_USID"),
    trim(strip_field(F.col("Extract.CSTS_STS"))).alias("CSTS_STS"),
    F.col("Extract.CSTS_STS_DTM").alias("CSTS_STS_DTM"),
    F.when(
        F.col("TaskRsnLoad.CSTK_MCTR_REAS").isNull() | (F.length(trim(F.col("TaskRsnLoad.CSTK_MCTR_REAS"))) == 0),
        F.lit("NA")
    ).otherwise(trim(strip_field(F.col("TaskRsnLoad.CSTK_MCTR_REAS")))).alias("CSTK_MCTR_REAS"),
    F.when(
        F.col("Extract.CSTS_ASSIGN_UDEPT").isNull() | (F.length(trim(F.col("Extract.CSTS_ASSIGN_UDEPT"))) == 0),
        F.lit("NA")
    ).otherwise(trim(F.col("Extract.CSTS_ASSIGN_UDEPT"))).alias("CSTS_ASSIGN_UDEPT")
)

df_hf_cust_svc_task_sttus_dedup = dedup_sort(
    df_StripField,
    ["CSSC_ID", "CSTK_SEQ_NO", "CSTS_SEQ_NO"],
    []
).select(
    "CSSC_ID",
    "CSTK_SEQ_NO",
    "CSTS_SEQ_NO",
    "CSTS_USID",
    "CSTS_ASSIGN_USID",
    "CSTS_STS",
    "CSTS_STS_DTM",
    "CSTK_MCTR_REAS",
    "CSTS_ASSIGN_UDEPT"
)

df_BusinessLogic_pre = (
    df_hf_cust_svc_task_sttus_dedup
    .withColumn("RowPassThru", F.lit("Y"))
    .withColumn("svCustSvcId", trim(F.col("CSSC_ID")))
)

df_BusinessLogic_AllCol = df_BusinessLogic_pre.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("svCustSvcId").alias("CUST_SVC_ID"),
    F.col("CSTK_SEQ_NO").alias("TASK_SEQ_NO"),
    F.col("CSTS_SEQ_NO").alias("STTUS_SEQ_NO"),
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.col("RowPassThru").alias("PASS_THRU_IN"),
    F.lit(CurrDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.concat(
        F.lit("FACETS;"),
        F.col("svCustSvcId"),
        F.lit(";"),
        F.col("CSTK_SEQ_NO").cast("string"),
        F.lit(";"),
        F.col("CSTS_SEQ_NO").cast("string")
    ).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CUST_SVC_TASK_STTUS_SK"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(
        F.col("CSTS_USID").isNull() | (F.length(trim(F.col("CSTS_USID"))) == 0),
        F.lit("NA")
    ).otherwise(trim(F.col("CSTS_USID"))).alias("CRT_BY_USER_SK"),
    F.lit(0).alias("CUST_SVC_TASK_SK"),
    F.when(
        F.col("CSTS_ASSIGN_USID").isNull() | (F.length(trim(F.col("CSTS_ASSIGN_USID"))) == 0),
        F.lit("NA")
    ).otherwise(trim(F.col("CSTS_ASSIGN_USID"))).alias("RTE_TO_USER_SK"),
    F.when(
        F.col("CSTS_STS").isNull() | (F.length(trim(F.col("CSTS_STS"))) == 0),
        F.lit("NA")
    ).otherwise(trim(F.col("CSTS_STS"))).alias("CUST_SVC_TASK_STTUS_CD"),
    F.when(
        F.col("CSTK_MCTR_REAS").isNull() | (F.length(trim(F.col("CSTK_MCTR_REAS"))) == 0),
        F.lit("NA")
    ).otherwise(trim(F.col("CSTK_MCTR_REAS"))).alias("CUST_SVC_TASK_STTUS_RSN_CD"),
    F.col("CSTS_STS_DTM").alias("STTUS_DTM"),
    F.col("CSTS_ASSIGN_UDEPT").alias("RTE_TO_GRP_ID")
)

df_BusinessLogic_Transform = df_BusinessLogic_pre.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("svCustSvcId").alias("CUST_SVC_ID"),
    F.col("CSTK_SEQ_NO").alias("TASK_SEQ_NO"),
    F.col("CSTS_SEQ_NO").alias("STTUS_SEQ_NO")
)

params_CustSvcTaskSttusPK = {
    "CurrRunCycle": CurrRunCycle,
    "RunID": RunID,
    "CurrDate": CurrDate,
    "SrcSysCdSk": SrcSysCdSk,
    "SrcSysCd": "FACETS",
    "IDSOwner": get_widget_value('IDSOwner','')
}

df_CustSvcTaskSttusPK = CustSvcTaskSttusPK(df_BusinessLogic_AllCol, df_BusinessLogic_Transform, params_CustSvcTaskSttusPK)

df_final = df_CustSvcTaskSttusPK.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CUST_SVC_TASK_STTUS_SK"),
    F.rpad(F.col("CUST_SVC_ID"), 12, " ").alias("CUST_SVC_ID"),
    F.col("TASK_SEQ_NO"),
    F.col("STTUS_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.col("CRT_BY_USER_SK"), 10, " ").alias("CRT_BY_USER_SK"),
    F.col("CUST_SVC_TASK_SK"),
    F.rpad(F.col("RTE_TO_USER_SK"), 10, " ").alias("RTE_TO_USER_SK"),
    F.rpad(F.col("CUST_SVC_TASK_STTUS_CD"), 2, " ").alias("CUST_SVC_TASK_STTUS_CD"),
    F.rpad(F.col("CUST_SVC_TASK_STTUS_RSN_CD"), 4, " ").alias("CUST_SVC_TASK_STTUS_RSN_CD"),
    F.col("STTUS_DTM"),
    F.col("RTE_TO_GRP_ID")
)

write_files(
    df_final,
    f"{adls_path}/key/IdsCustSvcTaskSttusExtr.CSTaskSttus.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)