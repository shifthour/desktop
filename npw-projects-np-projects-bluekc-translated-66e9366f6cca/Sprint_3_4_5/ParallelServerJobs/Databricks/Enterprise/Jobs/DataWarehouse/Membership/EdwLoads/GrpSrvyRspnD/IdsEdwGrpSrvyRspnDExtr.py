# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     EdwGrpSrvyExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from ids to create GRP_SRVY_RSPN_D table.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	IDS:  GRP_ATCHMT
# MAGIC                          
# MAGIC                 EDW: GRP_SRVY_RSPN_D   
# MAGIC                         
# MAGIC   
# MAGIC HASH FILES:
# MAGIC                 hf_cdma_codes         - Load the CDMA codes to get cd a nd cd_nm from cd_sk
# MAGIC                 
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   TRIM all the fields that are extracted or used in lookups
# MAGIC                  
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   All records are extracted and the table is rebuilt each day.
# MAGIC                   This program will extract and load all records with a last survey response indicator (current indicator) of "N"
# MAGIC                    EdwGrpSrvySetCur will set the current record indicator after the file from this program is loaded.
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                   GRP_SRVY_RSPN_D.dat ready to load to EDW
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               
# MAGIC               Steph Goddard 08/17/2005  Original program
# MAGIC               Suzanne Saylor  04/12/2006 - Added RunCycle parameter, set LAST_RUN_CYC_EXCTN_SK to RunCycle
# MAGIC               Sharon Andrew   07/06/2006      Renamed from EdwSrvyExtr to IdsGrpSrvyExtr.  
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Ralph Tucker          2009-03-04        3660 - MSP          Added new selection criteria to SQL                                            devlIDScur                      Steph Goddard           03/04/2009
# MAGIC 
# MAGIC Pooja Sunkara         07/16/2013        5114                   Converted job from server to parallel version.                              EnterpriseWrhsDevl         Peter Marshall             9/4/2013

# MAGIC Read data from source table GRP_ATCHMT.
# MAGIC Extracts all data from IDS reference table CD_MPPNG.
# MAGIC 
# MAGIC Code SK lookups for Denormalization
# MAGIC Lookup Keys:
# MAGIC GRP_REL_ENTY_TERM_RSN_CD_SK
# MAGIC GRP_REL_ENTY_TYP_CD_SK
# MAGIC GRP_REL_ENTY_CAT_CD_SK
# MAGIC SRC_SYS_CD_SK
# MAGIC Add CRT_RUN_CYC_EXCTN_DT_SK and LAST_UPDT_RUN_CYC_EXCTN_DT_SK
# MAGIC Write GRP_SRVY_RSPN_D Data into a Sequential file for Load Ready Job.
# MAGIC This job creates a file ready to load to GRP_SRVY_RSPN_D table.  However, the current indicator is not set here.  The table must be loaded first, then EdwGrpSrvySetCur will select the current record.
# MAGIC End of IDS Group Survey Extract
# MAGIC Job name:
# MAGIC IdsEdwGrpSrvyRspnDExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
IDSRunCycle = get_widget_value('IDSRunCycle','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

extract_query_db2_CD_MPPNG1_in = f"""
SELECT
  SRC_DRVD_LKUP_VAL,
  COALESCE(TRGT_CD,'UNK') TRGT_CD
FROM {IDSOwner}.CD_MPPNG CD
WHERE TRGT_DOMAIN_NM = 'GROUP SURVEY TYPE'
  AND TRGT_SRC_SYS_CD = 'EDW'
"""

df_db2_CD_MPPNG1_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CD_MPPNG1_in)
    .load()
)

extract_query_db2_GRP_ATCHMT_in = f"""
SELECT
  GRP_ATCHMT_SK,
  COALESCE(CD.TRGT_CD,' ') SRC_SYS_CD,
  GRP_ID,
  GRP_ATCHMT_ID,
  GRP_ATCHMT_DTM,
  GRP_ATCHMT_LAST_UPDT_DTM,
  GRP_SK,
  ATCHMT_NO_1,
  ATCHMT_TX_1,
  GRP_ATCHMT_DESC
FROM {IDSOwner}.GRP_ATCHMT GRP_ATCHMT
LEFT JOIN {IDSOwner}.CD_MPPNG CD
  ON GRP_ATCHMT.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
WHERE substr(GRP_ATCHMT_ID,3,2) in ('MP','MS','NR','OV','E0','E1','E2')
  AND GRP_ATCHMT.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSRunCycle}
"""

df_db2_GRP_ATCHMT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_GRP_ATCHMT_in)
    .load()
)

df_xfrm_businessLogicSubStr = df_db2_GRP_ATCHMT_in.select(
    col("GRP_ATCHMT_SK").alias("GRP_ATCHMT_SK"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("GRP_ID").alias("GRP_ID"),
    col("GRP_ATCHMT_ID").alias("GRP_ATCHMT_ID"),
    col("GRP_ATCHMT_DTM").alias("GRP_ATCHMT_DTM"),
    col("GRP_ATCHMT_LAST_UPDT_DTM").alias("GRP_ATCHMT_LAST_UPDT_DTM"),
    col("GRP_ATCHMT_ID").substr(3, 2).alias("GRP_ATCHMT_ID32"),
    col("GRP_SK").alias("GRP_SK"),
    col("ATCHMT_NO_1").alias("ATCHMT_NO_1"),
    col("ATCHMT_TX_1").alias("ATCHMT_TX_1"),
    col("GRP_ATCHMT_DESC").alias("GRP_ATCHMT_DESC")
)

df_lkp_CdmaCodes = (
    df_xfrm_businessLogicSubStr.alias("lkp_CdmaCode_InABC")
    .join(
        df_db2_CD_MPPNG1_in.alias("ref_SrvyTyp"),
        col("lkp_CdmaCode_InABC.GRP_ATCHMT_ID32") == col("ref_SrvyTyp.SRC_DRVD_LKUP_VAL"),
        "left"
    )
    .select(
        col("lkp_CdmaCode_InABC.GRP_ATCHMT_SK").alias("GRP_ATCHMT_SK"),
        col("lkp_CdmaCode_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lkp_CdmaCode_InABC.GRP_ATCHMT_LAST_UPDT_DTM").alias("GRP_ATCHMT_LAST_UPDT_DTM"),
        col("lkp_CdmaCode_InABC.GRP_ID").alias("GRP_ID"),
        col("lkp_CdmaCode_InABC.GRP_ATCHMT_ID").alias("GRP_ATCHMT_ID"),
        col("lkp_CdmaCode_InABC.GRP_ATCHMT_DTM").alias("GRP_ATCHMT_DTM"),
        col("lkp_CdmaCode_InABC.GRP_SK").alias("GRP_SK"),
        col("lkp_CdmaCode_InABC.ATCHMT_NO_1").alias("ATCHMT_NO_1"),
        col("lkp_CdmaCode_InABC.ATCHMT_TX_1").alias("ATCHMT_TX_1"),
        col("lkp_CdmaCode_InABC.GRP_ATCHMT_DESC").alias("GRP_ATCHMT_DESC"),
        col("ref_SrvyTyp.TRGT_CD").alias("SRVY_TYP_CD")
    )
)

df_xfrm_businessLogic_Main = (
    df_lkp_CdmaCodes
    .filter((col("GRP_ATCHMT_SK") != 0) & (col("GRP_ATCHMT_SK") != 1))
    .select(
        col("GRP_ATCHMT_SK").alias("GRP_SRVY_RSPN_SK"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("GRP_ID").alias("GRP_ID"),
        col("GRP_ATCHMT_LAST_UPDT_DTM").alias("GRP_SRVY_RSPN_DTM"),
        col("GRP_ATCHMT_ID").substr(1, 2).alias("SRVY_2_DGT_YR"),
        when(col("SRVY_TYP_CD").isNull(), lit(" ")).otherwise(col("SRVY_TYP_CD")).alias("SRVY_TYP_CD"),
        lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        col("GRP_SK").alias("GRP_SK"),
        col("GRP_ATCHMT_ID").alias("GRP_ATCHMT_ID"),
        col("ATCHMT_NO_1").alias("GRP_SRVY_RSPN_GRP_SIZE_NO"),
        col("GRP_ATCHMT_DESC").alias("GRP_SRVY_RSPN_SRVY_TYP_NM"),
        lit("N").alias("LAST_SRVY_RSPN_IN"),
        lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("GRP_ATCHMT_SK").alias("GRP_ATCHMT_SK")
    )
)

df_xfrm_businessLogic_UNK = spark.createDataFrame(
    [
        (
            0,
            "UNK",
            "UNK",
            "1753-01-01 00:00:00",
            "UNK",
            "UNK",
            "1753-01-01",
            EDWRunCycleDate,
            0,
            "UNK",
            0,
            "UNK",
            "N",
            100,
            EDWRunCycle,
            0
        )
    ],
    [
        "GRP_SRVY_RSPN_SK",
        "SRC_SYS_CD",
        "GRP_ID",
        "GRP_SRVY_RSPN_DTM",
        "SRVY_2_DGT_YR",
        "SRVY_TYP_CD",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "GRP_SK",
        "GRP_ATCHMT_ID",
        "GRP_SRVY_RSPN_GRP_SIZE_NO",
        "GRP_SRVY_RSPN_SRVY_TYP_NM",
        "LAST_SRVY_RSPN_IN",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "GRP_ATCHMT_SK"
    ]
)

df_xfrm_businessLogic_NA = spark.createDataFrame(
    [
        (
            1,
            "NA",
            "NA",
            "1753-01-01 00:00:00",
            "NA",
            "NA",
            "1753-01-01",
            EDWRunCycleDate,
            1,
            "NA",
            0,
            "NA",
            "N",
            100,
            EDWRunCycle,
            1
        )
    ],
    [
        "GRP_SRVY_RSPN_SK",
        "SRC_SYS_CD",
        "GRP_ID",
        "GRP_SRVY_RSPN_DTM",
        "SRVY_2_DGT_YR",
        "SRVY_TYP_CD",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "GRP_SK",
        "GRP_ATCHMT_ID",
        "GRP_SRVY_RSPN_GRP_SIZE_NO",
        "GRP_SRVY_RSPN_SRVY_TYP_NM",
        "LAST_SRVY_RSPN_IN",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "GRP_ATCHMT_SK"
    ]
)

df_fnl_dataLinks = df_xfrm_businessLogic_Main.unionByName(df_xfrm_businessLogic_UNK).unionByName(df_xfrm_businessLogic_NA)

df_fnl_dataLinks_out = df_fnl_dataLinks.select(
    col("GRP_SRVY_RSPN_SK"),
    col("SRC_SYS_CD"),
    col("GRP_ID"),
    col("GRP_SRVY_RSPN_DTM"),
    rpad(col("SRVY_2_DGT_YR"), 2, " ").alias("SRVY_2_DGT_YR"),
    col("SRVY_TYP_CD"),
    rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("GRP_SK"),
    col("GRP_ATCHMT_ID"),
    col("GRP_SRVY_RSPN_GRP_SIZE_NO"),
    col("GRP_SRVY_RSPN_SRVY_TYP_NM"),
    rpad(col("LAST_SRVY_RSPN_IN"), 1, " ").alias("LAST_SRVY_RSPN_IN"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("GRP_ATCHMT_SK")
)

write_files(
    df_fnl_dataLinks_out,
    f"{adls_path}/load/GRP_SRVY_RSPN_D.dat",
    ",",
    "overwrite",
    False,
    False,
    "^",
    None
)