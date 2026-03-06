# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2010 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Pulls data from MBR_COMM and loads in to MBR_COMM_D table.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer               Date                         Change Description                                                                           Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------             ----------------------------     -----------------------------------------------------------------------                                   ----------------         ------------------------------------       ----------------------------      ----------------
# MAGIC Bhoomi Dasari       2010-09-28                Initial programming                                                                             3035                  EnterpriseCurDevl            Steph Goddard          10/04/2010
# MAGIC 
# MAGIC Kalyan Neelam     2013-05-09                Added CRM run cycle, changes table layout                                4841 - Digital Comm   EnterpriseNewDevl        Bhoomi Dasari            5/12/2013                
# MAGIC 
# MAGIC Archana Palivela    08/09/2013              Originally Programmed  (In Parallel)                                                    5114                  EnterpriseWrhsDevl
# MAGIC 
# MAGIC Pooja Sunkara       02-14-2014               Changed the Extract SQL filter condition 
# MAGIC                                                                MBR_COMM.LAST_UPDT_RUN_CYC_EXCTN_SK >=                    5114                EnterpriseWrhsDevl          Bhoomi Dasari            2/14/2014
# MAGIC                                                                     #IdsMbrCommRunCycle#
# MAGIC 
# MAGIC Michael Harmon      2016-11-14             Added 3 new email address fields from MBR_COMM and stage         5541                  Enterprise\\Dev1              Kalyan Neelam            2016-11-17
# MAGIC                                                               variable to determine priority of which email address is populated
# MAGIC                                                               in MBR_COMM_D
# MAGIC 
# MAGIC Madhavan B         2017-03-24              Added 3 new email address fields OTHR_EMAIL_ADDR,                   ENHC0011137      EnterpriseDev2              Kalyan Neelam            2017-03-28
# MAGIC                                                              FCTS_EMAIL_ADDR and WEB_RGSTRN_EMAIL_ADDR
# MAGIC                                                               to MBR_COMM_D
# MAGIC 
# MAGIC Praneeth K          2022-01-25                Added 5 new Member fields                                                            469607                   EnterpriseDev2             Jeyaprasanna                2022-01-31
# MAGIC 
# MAGIC Vamsi Aripaka     2023-12-21                 Added new field MBR_OPTNL_PHN_CALL_IN                           US 607496               EnterpriseDev1             Jeyaprasanna                2024-01-17
# MAGIC                                                                from source to target.
# MAGIC 
# MAGIC Ediga Maruthi          2024-06-18             Removed default 'N' values in xmf_businessLogic stage and allowed nulls for the fileds   621348   EnterpriseDev2     Jeyaprasanna    2024-06-24        
# MAGIC                                                                MBR_EDUC_EMAIL_IN,MBR_EDUC_POSTAL_IN,MBR_RQRD_EMAIL_IN,
# MAGIC                                                                MBR_RQRD_POSTAL_IN,MBR_GNRC_SMS_IN,MBR_OPTNL_PHN_CALL_IN
# MAGIC                                                                from source to target.

# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Write MBR_COMM_D Data into a Sequential file for Load Ready Job.
# MAGIC Job name: IdsEdwMbrCommDExtr
# MAGIC 
# MAGIC 
# MAGIC Job Extract data from IDS table and apply Fkey and Code denormalization, create a load ready file.
# MAGIC Read data from source table 
# MAGIC MBR_COMM
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC MYCLM_EOB_PRFRNC_CD_SK
# MAGIC RCRD_STTUS_CD_SK
# MAGIC STTUS_RSN_CD_SK
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
CrmMbrCommRunCycle = get_widget_value("CrmMbrCommRunCycle","")
IdsMbrCommRunCycle = get_widget_value("IdsMbrCommRunCycle","")
EDWRunCycle = get_widget_value("EDWRunCycle","")
EDWRunCycleDate = get_widget_value("EDWRunCycleDate","")

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

db2_mbr_comm_query = f"""
SELECT 
MBR_COMM.MBR_COMM_SK,
MBR_COMM.MBR_UNIQ_KEY,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
MBR_COMM.LAST_UPDT_RUN_CYC_EXCTN_SK,
MBR_COMM.MBR_SK,
MBR_COMM.MYCLM_EOB_PRFRNC_CD_SK,
MBR_COMM.RCRD_STTUS_CD_SK,
MBR_COMM.STTUS_RSN_CD_SK,
MBR_COMM.AUTO_PAY_NTFCTN_IN,
MBR_COMM.BLUELOOP_PARTCPN_IN,
MBR_COMM.HLTH_WELNS_DO_NOT_SEND_IN,
MBR_COMM.HLTH_WELNS_EMAIL_IN,
MBR_COMM.HLTH_WELNS_POSTAL_IN,
MBR_COMM.HLTH_WELNS_SMS_IN,
MBR_COMM.MBR_PRFRNCS_SET_IN,
MBR_COMM.MYBLUEKC_ELTRNC_BILL_EMAIL_IN,
MBR_COMM.MYBLUEKC_ELTRNC_BILL_SMS_IN,
MBR_COMM.MYBLUEKC_PRT_BILL_EMAIL_IN,
MBR_COMM.MYBLUEKC_PRT_BILL_POSTAL_IN,
MBR_COMM.MYBLUEKC_PRT_BILL_SMS_IN,
MBR_COMM.MYCLM_EMAIL_IN,
MBR_COMM.MYCLM_POSTAL_IN,
MBR_COMM.MYCLM_SMS_IN,
MBR_COMM.MYPLN_INFO_EMAIL_IN,
MBR_COMM.MYPLN_INFO_POSTAL_IN,
MBR_COMM.MYPLN_INFO_SMS_IN,
MBR_COMM.PROD_SVC_DO_NOT_SEND_IN,
MBR_COMM.PROD_SVC_EMAIL_IN,
MBR_COMM.PROD_SVC_POSTAL_IN,
MBR_COMM.PROD_SVC_SMS_IN,
MBR_COMM.SRC_SYS_UPDT_DTM,
MBR_COMM.CELL_PHN_NO,
MBR_COMM.EMAIL_ADDR,
MBR_COMM.OTHR_EMAIL_ADDR,
MBR_COMM.FCTS_EMAIL_ADDR,
MBR_COMM.WEB_RGSTRN_EMAIL_ADDR,
MBR_COMM.MBR_EDUC_EMAIL_IN,
MBR_COMM.MBR_EDUC_POSTAL_IN,
MBR_COMM.MBR_RQRD_EMAIL_IN,
MBR_COMM.MBR_RQRD_POSTAL_IN,
MBR_COMM.MBR_GNRC_SMS_IN,
MBR_COMM.MBR_OPTNL_PHN_CALL_IN
FROM {IDSOwner}.MBR_COMM MBR_COMM
LEFT JOIN {IDSOwner}.CD_MPPNG CD
ON MBR_COMM.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
WHERE 
MBR_COMM.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
AND
(
  (CD.TRGT_CD = 'WEBUSERINFO' AND MBR_COMM.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IdsMbrCommRunCycle})
  OR
  (CD.TRGT_CD = 'CRM' AND MBR_COMM.LAST_UPDT_RUN_CYC_EXCTN_SK >= {CrmMbrCommRunCycle})
)
"""

df_DB2_MBR_COMM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", db2_mbr_comm_query)
    .load()
)

db2_cd_mppng_query = f"""
SELECT 
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""

df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", db2_cd_mppng_query)
    .load()
)

df_cpy_cd_mppng = df_db2_CD_MPPNG_Extr

df_lkp_FKeys = (
    df_DB2_MBR_COMM.alias("Lnk_IdsEdwMbrCommDExtr_InABC")
    .join(
        df_cpy_cd_mppng.alias("RefMyclmEobPrfrnc"),
        F.col("Lnk_IdsEdwMbrCommDExtr_InABC.MYCLM_EOB_PRFRNC_CD_SK") == F.col("RefMyclmEobPrfrnc.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_cpy_cd_mppng.alias("RefRcrdSttud"),
        F.col("Lnk_IdsEdwMbrCommDExtr_InABC.RCRD_STTUS_CD_SK") == F.col("RefRcrdSttud.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_cpy_cd_mppng.alias("RefSttusRsn"),
        F.col("Lnk_IdsEdwMbrCommDExtr_InABC.STTUS_RSN_CD_SK") == F.col("RefSttusRsn.CD_MPPNG_SK"),
        "left",
    )
    .select(
        F.col("Lnk_IdsEdwMbrCommDExtr_InABC.MBR_COMM_SK").alias("MBR_COMM_SK"),
        F.col("Lnk_IdsEdwMbrCommDExtr_InABC.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY_S"),
        F.col("Lnk_IdsEdwMbrCommDExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Lnk_IdsEdwMbrCommDExtr_InABC.MBR_SK").alias("MBR_SK"),
        F.col("RefMyclmEobPrfrnc.TRGT_CD").alias("MYCLM_EOB_PRFRNC_CD"),
        F.col("RefMyclmEobPrfrnc.TRGT_CD_NM").alias("MYCLM_EOB_PRFRNC_NM"),
        F.col("RefRcrdSttud.TRGT_CD").alias("RCRD_STTUS_CD"),
        F.col("RefRcrdSttud.TRGT_CD_NM").alias("RCRD_STTUS_NM"),
        F.col("RefSttusRsn.TRGT_CD").alias("STTUS_RSN_CD"),
        F.col("RefSttusRsn.TRGT_CD_NM").alias("STTUS_RSN_NM"),
        F.col("Lnk_IdsEdwMbrCommDExtr_InABC.AUTO_PAY_NTFCTN_IN").alias("AUTO_PAY_NTFCTN_IN"),
        F.col("Lnk_IdsEdwMbrCommDExtr_InABC.BLUELOOP_PARTCPN_IN").alias("BLUELOOP_PARTCPN_IN"),
        F.col("Lnk_IdsEdwMbrCommDExtr_InABC.HLTH_WELNS_DO_NOT_SEND_IN").alias("HLTH_WELNS_DO_NOT_SEND_IN"),
        F.col("Lnk_IdsEdwMbrCommDExtr_InABC.HLTH_WELNS_EMAIL_IN").alias("HLTH_WELNS_EMAIL_IN"),
        F.col("Lnk_IdsEdwMbrCommDExtr_InABC.HLTH_WELNS_POSTAL_IN").alias("HLTH_WELNS_POSTAL_IN"),
        F.col("Lnk_IdsEdwMbrCommDExtr_InABC.HLTH_WELNS_SMS_IN").alias("HLTH_WELNS_SMS_IN"),
        F.col("Lnk_IdsEdwMbrCommDExtr_InABC.MBR_PRFRNCS_SET_IN").alias("MBR_PRFRNCS_SET_IN"),
        F.col("Lnk_IdsEdwMbrCommDExtr_InABC.MYBLUEKC_ELTRNC_BILL_EMAIL_IN").alias("MYBLUEKC_ELTRNC_BILL_EMAIL_IN"),
        F.col("Lnk_IdsEdwMbrCommDExtr_InABC.MYBLUEKC_ELTRNC_BILL_SMS_IN").alias("MYBLUEKC_ELTRNC_BILL_SMS_IN"),
        F.col("Lnk_IdsEdwMbrCommDExtr_InABC.MYBLUEKC_PRT_BILL_EMAIL_IN").alias("MYBLUEKC_PRT_BILL_EMAIL_IN"),
        F.col("Lnk_IdsEdwMbrCommDExtr_InABC.MYBLUEKC_PRT_BILL_POSTAL_IN").alias("MYBLUEKC_PRT_BILL_POSTAL_IN"),
        F.col("Lnk_IdsEdwMbrCommDExtr_InABC.MYBLUEKC_PRT_BILL_SMS_IN").alias("MYBLUEKC_PRT_BILL_SMS_IN"),
        F.col("Lnk_IdsEdwMbrCommDExtr_InABC.MYCLM_EMAIL_IN").alias("MYCLM_EMAIL_IN"),
        F.col("Lnk_IdsEdwMbrCommDExtr_InABC.MYCLM_POSTAL_IN").alias("MYCLM_POSTAL_IN"),
        F.col("Lnk_IdsEdwMbrCommDExtr_InABC.MYCLM_SMS_IN").alias("MYCLM_SMS_IN"),
        F.col("Lnk_IdsEdwMbrCommDExtr_InABC.MYPLN_INFO_EMAIL_IN").alias("MYPLN_INFO_EMAIL_IN"),
        F.col("Lnk_IdsEdwMbrCommDExtr_InABC.MYPLN_INFO_POSTAL_IN").alias("MYPLN_INFO_POSTAL_IN"),
        F.col("Lnk_IdsEdwMbrCommDExtr_InABC.MYPLN_INFO_SMS_IN").alias("MYPLN_INFO_SMS_IN"),
        F.col("Lnk_IdsEdwMbrCommDExtr_InABC.PROD_SVC_DO_NOT_SEND_IN").alias("PROD_SVC_DO_NOT_SEND_IN"),
        F.col("Lnk_IdsEdwMbrCommDExtr_InABC.PROD_SVC_EMAIL_IN").alias("PROD_SVC_EMAIL_IN"),
        F.col("Lnk_IdsEdwMbrCommDExtr_InABC.PROD_SVC_POSTAL_IN").alias("PROD_SVC_POSTAL_IN"),
        F.col("Lnk_IdsEdwMbrCommDExtr_InABC.PROD_SVC_SMS_IN").alias("PROD_SVC_SMS_IN"),
        F.col("Lnk_IdsEdwMbrCommDExtr_InABC.SRC_SYS_UPDT_DTM").alias("SRC_SYS_UPDT_DTM"),
        F.col("Lnk_IdsEdwMbrCommDExtr_InABC.CELL_PHN_NO").alias("CELL_PHN_NO"),
        F.col("Lnk_IdsEdwMbrCommDExtr_InABC.EMAIL_ADDR").alias("EMAIL_ADDR"),
        F.col("Lnk_IdsEdwMbrCommDExtr_InABC.MYCLM_EOB_PRFRNC_CD_SK").alias("MYCLM_EOB_PRFRNC_CD_SK"),
        F.col("Lnk_IdsEdwMbrCommDExtr_InABC.RCRD_STTUS_CD_SK").alias("RCRD_STTUS_CD_SK"),
        F.col("Lnk_IdsEdwMbrCommDExtr_InABC.STTUS_RSN_CD_SK").alias("STTUS_RSN_CD_SK"),
        F.col("Lnk_IdsEdwMbrCommDExtr_InABC.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("Lnk_IdsEdwMbrCommDExtr_InABC.OTHR_EMAIL_ADDR").alias("OTHR_EMAIL_ADDR"),
        F.col("Lnk_IdsEdwMbrCommDExtr_InABC.FCTS_EMAIL_ADDR").alias("FCTS_EMAIL_ADDR"),
        F.col("Lnk_IdsEdwMbrCommDExtr_InABC.WEB_RGSTRN_EMAIL_ADDR").alias("WEB_RGSTRN_EMAIL_ADDR"),
        F.col("Lnk_IdsEdwMbrCommDExtr_InABC.MBR_EDUC_EMAIL_IN").alias("MBR_EDUC_EMAIL_IN"),
        F.col("Lnk_IdsEdwMbrCommDExtr_InABC.MBR_EDUC_POSTAL_IN").alias("MBR_EDUC_POSTAL_IN"),
        F.col("Lnk_IdsEdwMbrCommDExtr_InABC.MBR_RQRD_EMAIL_IN").alias("MBR_RQRD_EMAIL_IN"),
        F.col("Lnk_IdsEdwMbrCommDExtr_InABC.MBR_RQRD_POSTAL_IN").alias("MBR_RQRD_POSTAL_IN"),
        F.col("Lnk_IdsEdwMbrCommDExtr_InABC.MBR_GNRC_SMS_IN").alias("MBR_GNRC_SMS_IN"),
        F.col("Lnk_IdsEdwMbrCommDExtr_InABC.MBR_OPTNL_PHN_CALL_IN").alias("MBR_OPTNL_PHN_CALL_IN"),
    )
)

df_xfm_stage_var = (
    df_lkp_FKeys.withColumn(
        "svEMAILADDR",
        F.when(
            (F.col("EMAIL_ADDR") != "0")
            & (F.col("EMAIL_ADDR").isNotNull())
            & (F.length(trim("EMAIL_ADDR")) != 0)
            & (~F.col("EMAIL_ADDR").isin("NA", "UNK")),
            F.col("EMAIL_ADDR"),
        )
        .when(
            (F.col("WEB_RGSTRN_EMAIL_ADDR") != "0")
            & (F.col("WEB_RGSTRN_EMAIL_ADDR").isNotNull())
            & (F.length(trim("WEB_RGSTRN_EMAIL_ADDR")) != 0)
            & (~F.col("WEB_RGSTRN_EMAIL_ADDR").isin("NA", "UNK")),
            F.col("WEB_RGSTRN_EMAIL_ADDR"),
        )
        .when(
            (F.col("FCTS_EMAIL_ADDR") != "0")
            & (F.col("FCTS_EMAIL_ADDR").isNotNull())
            & (F.length(trim("FCTS_EMAIL_ADDR")) != 0)
            & (~F.col("FCTS_EMAIL_ADDR").isin("NA", "UNK")),
            F.col("FCTS_EMAIL_ADDR"),
        )
        .when(
            (F.col("OTHR_EMAIL_ADDR") != "0")
            & (F.col("OTHR_EMAIL_ADDR").isNotNull())
            & (F.length(trim("OTHR_EMAIL_ADDR")) != 0)
            & (~F.col("OTHR_EMAIL_ADDR").isin("NA", "UNK")),
            F.col("OTHR_EMAIL_ADDR"),
        )
        .otherwise(""),
    )
)

df_Lnk_Xfm_Out = (
    df_xfm_stage_var.filter((F.col("MBR_COMM_SK") != 0) & (F.col("MBR_COMM_SK") != 1))
    .select(
        F.col("MBR_COMM_SK").alias("MBR_COMM_SK"),
        F.col("MBR_UNIQ_KEY_S").alias("MBR_UNIQ_KEY"),
        F.when(F.trim(F.col("SRC_SYS_CD")) == "", "UNK").otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
        F.col("MBR_SK").alias("MBR_SK"),
        F.when(F.trim(F.col("MYCLM_EOB_PRFRNC_CD")) == "", "UNK").otherwise(F.col("MYCLM_EOB_PRFRNC_CD")).alias("MYCLM_EOB_PRFRNC_CD"),
        F.when(F.trim(F.col("MYCLM_EOB_PRFRNC_NM")) == "", "UNK").otherwise(F.col("MYCLM_EOB_PRFRNC_NM")).alias("MYCLM_EOB_PRFRNC_NM"),
        F.when(F.trim(F.col("RCRD_STTUS_CD")) == "", "UNK").otherwise(F.col("RCRD_STTUS_CD")).alias("RCRD_STTUS_CD"),
        F.when(F.trim(F.col("RCRD_STTUS_NM")) == "", "UNK").otherwise(F.col("RCRD_STTUS_NM")).alias("RCRD_STTUS_NM"),
        F.when(F.trim(F.col("STTUS_RSN_CD")) == "", "UNK").otherwise(F.col("STTUS_RSN_CD")).alias("STTUS_RSN_CD"),
        F.when(F.trim(F.col("RCRD_STTUS_NM")) == "", "UNK").otherwise(F.col("RCRD_STTUS_NM")).alias("STTUS_RSN_NM"),
        F.col("AUTO_PAY_NTFCTN_IN").alias("AUTO_PAY_NTFCTN_IN"),
        F.col("BLUELOOP_PARTCPN_IN").alias("BLUELOOP_PARTCPN_IN"),
        F.col("HLTH_WELNS_DO_NOT_SEND_IN").alias("HLTH_WELNS_DO_NOT_SEND_IN"),
        F.col("HLTH_WELNS_EMAIL_IN").alias("HLTH_WELNS_EMAIL_IN"),
        F.col("HLTH_WELNS_POSTAL_IN").alias("HLTH_WELNS_POSTAL_IN"),
        F.col("HLTH_WELNS_SMS_IN").alias("HLTH_WELNS_SMS_IN"),
        F.col("MBR_PRFRNCS_SET_IN").alias("MBR_PRFRNCS_SET_IN"),
        F.col("MYBLUEKC_ELTRNC_BILL_EMAIL_IN").alias("MYBLUEKC_ELTRNC_BILL_EMAIL_IN"),
        F.col("MYBLUEKC_ELTRNC_BILL_SMS_IN").alias("MYBLUEKC_ELTRNC_BILL_SMS_IN"),
        F.col("MYBLUEKC_PRT_BILL_EMAIL_IN").alias("MYBLUEKC_PRT_BILL_EMAIL_IN"),
        F.col("MYBLUEKC_PRT_BILL_POSTAL_IN").alias("MYBLUEKC_PRT_BILL_POSTAL_IN"),
        F.col("MYBLUEKC_PRT_BILL_SMS_IN").alias("MYBLUEKC_PRT_BILL_SMS_IN"),
        F.col("MYCLM_EMAIL_IN").alias("MYCLM_EMAIL_IN"),
        F.col("MYCLM_POSTAL_IN").alias("MYCLM_POSTAL_IN"),
        F.col("MYCLM_SMS_IN").alias("MYCLM_SMS_IN"),
        F.col("MYPLN_INFO_EMAIL_IN").alias("MYPLN_INFO_EMAIL_IN"),
        F.col("MYPLN_INFO_POSTAL_IN").alias("MYPLN_INFO_POSTAL_IN"),
        F.col("MYPLN_INFO_SMS_IN").alias("MYPLN_INFO_SMS_IN"),
        F.col("PROD_SVC_DO_NOT_SEND_IN").alias("PROD_SVC_DO_NOT_SEND_IN"),
        F.col("PROD_SVC_EMAIL_IN").alias("PROD_SVC_EMAIL_IN"),
        F.col("PROD_SVC_POSTAL_IN").alias("PROD_SVC_POSTAL_IN"),
        F.col("PROD_SVC_SMS_IN").alias("PROD_SVC_SMS_IN"),
        F.col("SRC_SYS_UPDT_DTM").alias("SRC_SYS_UPDT_DTM"),
        F.when(F.col("CELL_PHN_NO").isNull(), "UNK").otherwise(F.col("CELL_PHN_NO")).alias("CELL_PHN_NO"),
        F.col("svEMAILADDR").alias("EMAIL_ADDR"),
        F.col("MYCLM_EOB_PRFRNC_CD_SK").alias("MYCLM_EOB_PRFRNC_CD_SK"),
        F.col("RCRD_STTUS_CD_SK").alias("RCRD_STTUS_CD_SK"),
        F.col("STTUS_RSN_CD_SK").alias("STTUS_RSN_CD_SK"),
        F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("OTHR_EMAIL_ADDR").alias("OTHR_EMAIL_ADDR"),
        F.col("FCTS_EMAIL_ADDR").alias("FCTS_EMAIL_ADDR"),
        F.col("WEB_RGSTRN_EMAIL_ADDR").alias("WEB_RGSTRN_EMAIL_ADDR"),
        F.col("MBR_EDUC_EMAIL_IN").alias("MBR_EDUC_EMAIL_IN"),
        F.col("MBR_EDUC_POSTAL_IN").alias("MBR_EDUC_POSTAL_IN"),
        F.col("MBR_RQRD_EMAIL_IN").alias("MBR_RQRD_EMAIL_IN"),
        F.col("MBR_RQRD_POSTAL_IN").alias("MBR_RQRD_POSTAL_IN"),
        F.col("MBR_GNRC_SMS_IN").alias("MBR_GNRC_SMS_IN"),
        F.col("MBR_OPTNL_PHN_CALL_IN").alias("MBR_OPTNL_PHN_CALL_IN"),
    )
)

df_UNK_ROW = spark.createDataFrame(
    [
        (
            0,
            0,
            "UNK",
            0,
            "UNK",
            "UNK",
            "UNK",
            "UNK",
            "UNK",
            "UNK",
            "N",
            "N",
            "N",
            "N",
            "N",
            "N",
            "N",
            "N",
            "N",
            "N",
            "N",
            "N",
            "N",
            "N",
            "N",
            "N",
            "N",
            "N",
            "N",
            "N",
            "1753-01-01 00:00:00 ",
            "UNK",
            "UNK",
            0,
            0,
            0,
            "1753-01-01",
            EDWRunCycleDate,
            100,
            EDWRunCycle,
            0,
            "UNK",
            "UNK",
            "UNK",
            "",
            "",
            "",
            "",
            "",
            "",
        )
    ],
    [
        "MBR_COMM_SK",
        "MBR_UNIQ_KEY",
        "SRC_SYS_CD",
        "MBR_SK",
        "MYCLM_EOB_PRFRNC_CD",
        "MYCLM_EOB_PRFRNC_NM",
        "RCRD_STTUS_CD",
        "RCRD_STTUS_NM",
        "STTUS_RSN_CD",
        "STTUS_RSN_NM",
        "AUTO_PAY_NTFCTN_IN",
        "BLUELOOP_PARTCPN_IN",
        "HLTH_WELNS_DO_NOT_SEND_IN",
        "HLTH_WELNS_EMAIL_IN",
        "HLTH_WELNS_POSTAL_IN",
        "HLTH_WELNS_SMS_IN",
        "MBR_PRFRNCS_SET_IN",
        "MYBLUEKC_ELTRNC_BILL_EMAIL_IN",
        "MYBLUEKC_ELTRNC_BILL_SMS_IN",
        "MYBLUEKC_PRT_BILL_EMAIL_IN",
        "MYBLUEKC_PRT_BILL_POSTAL_IN",
        "MYBLUEKC_PRT_BILL_SMS_IN",
        "MYCLM_EMAIL_IN",
        "MYCLM_POSTAL_IN",
        "MYCLM_SMS_IN",
        "MYPLN_INFO_EMAIL_IN",
        "MYPLN_INFO_POSTAL_IN",
        "MYPLN_INFO_SMS_IN",
        "PROD_SVC_DO_NOT_SEND_IN",
        "PROD_SVC_EMAIL_IN",
        "PROD_SVC_POSTAL_IN",
        "PROD_SVC_SMS_IN",
        "SRC_SYS_UPDT_DTM",
        "CELL_PHN_NO",
        "EMAIL_ADDR",
        "MYCLM_EOB_PRFRNC_CD_SK",
        "RCRD_STTUS_CD_SK",
        "STTUS_RSN_CD_SK",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "IDS_LAST_UPDT_RUN_CYC_EXCTN_SK",
        "OTHR_EMAIL_ADDR",
        "FCTS_EMAIL_ADDR",
        "WEB_RGSTRN_EMAIL_ADDR",
        "MBR_EDUC_EMAIL_IN",
        "MBR_EDUC_POSTAL_IN",
        "MBR_RQRD_EMAIL_IN",
        "MBR_RQRD_POSTAL_IN",
        "MBR_GNRC_SMS_IN",
        "MBR_OPTNL_PHN_CALL_IN",
    ],
)

df_NA_ROW = spark.createDataFrame(
    [
        (
            1,
            0,
            "NA",
            1,
            "NA",
            "NA",
            "NA",
            "NA",
            "NA",
            "NA",
            "N",
            "N",
            "N",
            "N",
            "N",
            "N",
            "N",
            "N",
            "N",
            "N",
            "N",
            "N",
            "N",
            "N",
            "N",
            "N",
            "N",
            "N",
            "N",
            "N",
            "N",
            "N",
            "1753-01-01 00:00:00",
            "NA",
            "NA",
            1,
            1,
            1,
            "1753-01-01",
            EDWRunCycleDate,
            100,
            EDWRunCycle,
            0,
            "NA",
            "NA",
            "NA",
            "",
            "",
            "",
            "",
            "",
            "",
        )
    ],
    [
        "MBR_COMM_SK",
        "MBR_UNIQ_KEY",
        "SRC_SYS_CD",
        "MBR_SK",
        "MYCLM_EOB_PRFRNC_CD",
        "MYCLM_EOB_PRFRNC_NM",
        "RCRD_STTUS_CD",
        "RCRD_STTUS_NM",
        "STTUS_RSN_CD",
        "STTUS_RSN_NM",
        "AUTO_PAY_NTFCTN_IN",
        "BLUELOOP_PARTCPN_IN",
        "HLTH_WELNS_DO_NOT_SEND_IN",
        "HLTH_WELNS_EMAIL_IN",
        "HLTH_WELNS_POSTAL_IN",
        "HLTH_WELNS_SMS_IN",
        "MBR_PRFRNCS_SET_IN",
        "MYBLUEKC_ELTRNC_BILL_EMAIL_IN",
        "MYBLUEKC_ELTRNC_BILL_SMS_IN",
        "MYBLUEKC_PRT_BILL_EMAIL_IN",
        "MYBLUEKC_PRT_BILL_POSTAL_IN",
        "MYBLUEKC_PRT_BILL_SMS_IN",
        "MYCLM_EMAIL_IN",
        "MYCLM_POSTAL_IN",
        "MYCLM_SMS_IN",
        "MYPLN_INFO_EMAIL_IN",
        "MYPLN_INFO_POSTAL_IN",
        "MYPLN_INFO_SMS_IN",
        "PROD_SVC_DO_NOT_SEND_IN",
        "PROD_SVC_EMAIL_IN",
        "PROD_SVC_POSTAL_IN",
        "PROD_SVC_SMS_IN",
        "SRC_SYS_UPDT_DTM",
        "CELL_PHN_NO",
        "EMAIL_ADDR",
        "MYCLM_EOB_PRFRNC_CD_SK",
        "RCRD_STTUS_CD_SK",
        "STTUS_RSN_CD_SK",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "IDS_LAST_UPDT_RUN_CYC_EXCTN_SK",
        "OTHR_EMAIL_ADDR",
        "FCTS_EMAIL_ADDR",
        "WEB_RGSTRN_EMAIL_ADDR",
        "MBR_EDUC_EMAIL_IN",
        "MBR_EDUC_POSTAL_IN",
        "MBR_RQRD_EMAIL_IN",
        "MBR_RQRD_POSTAL_IN",
        "MBR_GNRC_SMS_IN",
        "MBR_OPTNL_PHN_CALL_IN",
    ],
)

df_funnel = (
    df_Lnk_Xfm_Out.unionByName(df_NA_ROW)
    .unionByName(df_UNK_ROW)
)

final_cols = [
    ("MBR_COMM_SK", None),
    ("MBR_UNIQ_KEY", None),
    ("SRC_SYS_CD", None),
    ("CRT_RUN_CYC_EXCTN_DT_SK", 10),
    ("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", 10),
    ("MBR_SK", None),
    ("MYCLM_EOB_PRFRNC_CD", None),
    ("MYCLM_EOB_PRFRNC_NM", None),
    ("RCRD_STTUS_CD", None),
    ("RCRD_STTUS_NM", None),
    ("STTUS_RSN_CD", None),
    ("STTUS_RSN_NM", None),
    ("AUTO_PAY_NTFCTN_IN", 1),
    ("BLUELOOP_PARTCPN_IN", 1),
    ("HLTH_WELNS_DO_NOT_SEND_IN", 1),
    ("HLTH_WELNS_EMAIL_IN", 1),
    ("HLTH_WELNS_POSTAL_IN", 1),
    ("HLTH_WELNS_SMS_IN", 1),
    ("MBR_PRFRNCS_SET_IN", 1),
    ("MYBLUEKC_ELTRNC_BILL_EMAIL_IN", 1),
    ("MYBLUEKC_ELTRNC_BILL_SMS_IN", 1),
    ("MYBLUEKC_PRT_BILL_EMAIL_IN", 1),
    ("MYBLUEKC_PRT_BILL_POSTAL_IN", 1),
    ("MYBLUEKC_PRT_BILL_SMS_IN", 1),
    ("MYCLM_EMAIL_IN", 1),
    ("MYCLM_POSTAL_IN", 1),
    ("MYCLM_SMS_IN", 1),
    ("MYPLN_INFO_EMAIL_IN", 1),
    ("MYPLN_INFO_POSTAL_IN", 1),
    ("MYPLN_INFO_SMS_IN", 1),
    ("PROD_SVC_DO_NOT_SEND_IN", 1),
    ("PROD_SVC_EMAIL_IN", 1),
    ("PROD_SVC_POSTAL_IN", 1),
    ("PROD_SVC_SMS_IN", 1),
    ("SRC_SYS_UPDT_DTM", None),
    ("CELL_PHN_NO", None),
    ("EMAIL_ADDR", None),
    ("CRT_RUN_CYC_EXCTN_SK", None),
    ("LAST_UPDT_RUN_CYC_EXCTN_SK", None),
    ("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK", None),
    ("MYCLM_EOB_PRFRNC_CD_SK", None),
    ("RCRD_STTUS_CD_SK", None),
    ("STTUS_RSN_CD_SK", None),
    ("OTHR_EMAIL_ADDR", None),
    ("FCTS_EMAIL_ADDR", None),
    ("WEB_RGSTRN_EMAIL_ADDR", None),
    ("MBR_EDUC_EMAIL_IN", 1),
    ("MBR_EDUC_POSTAL_IN", 1),
    ("MBR_RQRD_EMAIL_IN", 1),
    ("MBR_RQRD_POSTAL_IN", 1),
    ("MBR_GNRC_SMS_IN", 1),
    ("MBR_OPTNL_PHN_CALL_IN", 1),
]

df_final = df_funnel
for col_name, char_len in final_cols:
    if char_len is not None:
        df_final = df_final.withColumn(col_name, F.rpad(F.col(col_name), char_len, " "))

df_final = df_final.select([F.col(col_name) for col_name, _ in final_cols])

write_files(
    df_final,
    f"{adls_path}/load/MBR_COMM_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)