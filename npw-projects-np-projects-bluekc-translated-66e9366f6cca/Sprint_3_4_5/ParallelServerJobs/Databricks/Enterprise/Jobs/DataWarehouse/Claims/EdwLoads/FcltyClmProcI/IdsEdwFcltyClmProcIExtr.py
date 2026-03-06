# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 1;hf_fcltyclm_proc_cd_trgt_nm
# MAGIC 
# MAGIC JOB NAME:     EdwFcltyClmProcIntExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from Edw and Ids to compare and load Edw
# MAGIC   Loads the data from EDW and IDS into two hash files for subsequent comparison.
# MAGIC   The comparison will determine rows that have changed and whether to generate a new history record.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	EDW - FCLTY_CLM_PROC_I
# MAGIC                             W_EDW_ETL_DRVR
# MAGIC                 IDS -   FCLTY_CLM_PROC 
# MAGIC                            PROC_CD
# MAGIC                            W_EDW_ETL_DRVR
# MAGIC                            CLM
# MAGIC 
# MAGIC   
# MAGIC HASH FILES:
# MAGIC                 hf_cdma_codes         - Load the CDMA codes to get cd a nd cd_nm from cd_sk
# MAGIC                 hf_FCLTY_CLM_PROC_I_edw - edw output file
# MAGIC                 hf_FCLTY_CLM_PROC_ids - ids output file
# MAGIC                
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   TRIM all the fields that are extracted or used in lookups
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   IDS - read from source, lookup all code SK values and get the natural codes
# MAGIC                   EDW - Read from source and dump to hf. Do nothing but trim the fields.
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Each of the stream output hash files
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               Tom Harrocks 08/02/2004-   Originally Programmed
# MAGIC               Steph Goddard 06/21/2005  Get claim sk for output file - it was putting wrong SK in clm_sk field
# MAGIC               Brent leland     03/24/2006    Changed parameters to evironment variables.
# MAGIC                                                              Took out trim() on SK values.
# MAGIC               Brent Leland    05/11/2006    Removed trim() on codes and IDs
# MAGIC               BhoomiD  - 04/10/2007 Added new field FCLTY_CLM_PROC_CD_PRI_MOD_TX, a direct mapping from FCLTY_CLM_PROC.PROC_CD_MOD_TX.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer               Date                 Project/Altiris #         Change Description                                                                                                          Development Project      Code Reviewer          Date Reviewed       
# MAGIC -----------------------        --------------------     ------------------------        -----------------------------------------------------------------------------------------------------------------                         --------------------------------       -------------------------------   ----------------------------       
# MAGIC Rick Henry             2012-05-08        4896                       Added FCLTY_CLM_PROC_CD_TYP_CD_SK                                                                 EnterpriseNewDevl           SAndrew                   2012-05-18
# MAGIC                                                                                                                                                                                                                    
# MAGIC DEVELOPER                         DATE                 PROJECT                                                DESCRIPTION                                            DATASTAGE                                            CODE                            REVIEW
# MAGIC                                                                                                                                                                                                               ENVIRONMENT                                       REVIEWER                   DATE                                              
# MAGIC ------------------------------------------   ----------------------      ------------------------------------------------                ---------------------------------------------------------------     ---------------------------------------------------              ------------------------------          --------------------
# MAGIC Balkarn                                10/08/2013                          5114                                       Original Programming                                   EnterpriseWrhsDevl                                  Peter Marshall                  12/20/2013
# MAGIC                                                                                                                                           (Server to Parallel Conversion)

# MAGIC This is Source extract data from an IDS table and apply Code denormalization, create a load ready file.
# MAGIC 
# MAGIC Job Name:
# MAGIC IdsEdwFcltyClmProcIExtr
# MAGIC 
# MAGIC Table:
# MAGIC FCLTY_CLM_PROC_I
# MAGIC Read from source table FCLTY_CLM_PROC , PROC_CD, CLM and W_EDW_ETL_DRVR
# MAGIC This is a load ready file conforms to the target table structure.
# MAGIC 
# MAGIC Write DRG_D Data into a Sequential file for Load Job.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Add Defaults and Null Handling
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC 
# MAGIC 1) PROC_CD_SK
# MAGIC 2) FCLTY_CLM_PROC_ORDNL_CD_SK
# MAGIC Lookup on Reference PROC_CD_TRGT_FCLTYINT
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Assume SparkSession is already defined as spark.
# Assume all helper functions and variables (adls_path, adls_path_raw, adls_path_publish, get_widget_value, get_db_config, execute_dml, write_files, trim, current_timestamp, current_date, SurrogateKeyGen, etc.) are already available in the namespace.

# Parameters
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')

# --------------------------------------------------------------------------------
# Stage: db2_FCLTY_CLM_PROC_in (DB2ConnectorPX)
# --------------------------------------------------------------------------------

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_db2_FCLTY_CLM_PROC_in = f"""
SELECT 
  cp.FCLTY_CLM_PROC_SK,
  COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
  cp.SRC_SYS_CD_SK,
  cp.CLM_ID,
  cp.PROC_DT_SK,
  p.PROC_CD,
  p.PROC_CD_DESC,
  cp.FCLTY_CLM_SK,
  cp.PROC_CD_SK,
  cp.FCLTY_CLM_PROC_ORDNL_CD_SK,
  cl.CLM_SK,
  cp.PROC_CD_MOD_TX
FROM {IDSOwner}.FCLTY_CLM_PROC cp
LEFT JOIN {IDSOwner}.CD_MPPNG CD
  ON cp.SRC_SYS_CD_SK = CD.CD_MPPNG_SK,
     {IDSOwner}.PROC_CD p,
     {IDSOwner}.W_EDW_ETL_DRVR dr,
     {IDSOwner}.CLM cl
WHERE cp.PROC_CD_SK = p.PROC_CD_SK
  AND cp.SRC_SYS_CD_SK = dr.SRC_SYS_CD_SK
  AND cp.CLM_ID = dr.CLM_ID
  AND cp.CLM_ID = cl.CLM_ID
"""
df_db2_FCLTY_CLM_PROC_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_FCLTY_CLM_PROC_in)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: db2_CD_MPPNG_Extr (DB2ConnectorPX)
# --------------------------------------------------------------------------------

extract_query_db2_CD_MPPNG_Extr = f"""
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
    .option("query", extract_query_db2_CD_MPPNG_Extr)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: cpy (PxCopy)
# --------------------------------------------------------------------------------

df_cpy_Ref_FcltyClmProcCdTypeLkup = df_db2_CD_MPPNG_Extr.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_cpy_Ref_FcltyClmProcOrdnlLkup = df_db2_CD_MPPNG_Extr.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

# --------------------------------------------------------------------------------
# Stage: db2_PROC_CD (DB2ConnectorPX)
# --------------------------------------------------------------------------------

extract_query_db2_PROC_CD = f"""
SELECT
  PROC_CD_SK,
  PROC_CD,
  PROC_CD_TYP_CD_SK,
  PROC_CD_TYP_CD
FROM {IDSOwner}.PROC_CD
"""
df_db2_PROC_CD = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_PROC_CD)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: lkp_ProcCD (PxLookup)
# --------------------------------------------------------------------------------

df_lkp_ProcCD = (
    df_db2_FCLTY_CLM_PROC_in.alias("lnk_IdsEdwFcltyClmProcExtr_InABC")
    .join(
        df_db2_PROC_CD.alias("Ref_ProcCDLkp"),
        on=[
            col("lnk_IdsEdwFcltyClmProcExtr_InABC.PROC_CD_SK")
            == col("Ref_ProcCDLkp.PROC_CD_SK")
        ],
        how="left",
    )
    .select(
        col("lnk_IdsEdwFcltyClmProcExtr_InABC.FCLTY_CLM_PROC_SK").alias("FCLTY_CLM_PROC_SK"),
        col("lnk_IdsEdwFcltyClmProcExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnk_IdsEdwFcltyClmProcExtr_InABC.CLM_ID").alias("CLM_ID"),
        col("lnk_IdsEdwFcltyClmProcExtr_InABC.FCLTY_CLM_PROC_ORDNL_CD_SK").alias("FCLTY_CLM_PROC_ORDNL_CD_SK"),
        col("lnk_IdsEdwFcltyClmProcExtr_InABC.CLM_SK").alias("CLM_SK"),
        col("lnk_IdsEdwFcltyClmProcExtr_InABC.FCLTY_CLM_SK").alias("FCLTY_CLM_SK"),
        col("lnk_IdsEdwFcltyClmProcExtr_InABC.PROC_CD_SK").alias("FCLTY_CLM_PROC_CD_SK"),
        col("lnk_IdsEdwFcltyClmProcExtr_InABC.PROC_DT_SK").alias("FCLTY_CLM_PROC_DT_SK"),
        col("lnk_IdsEdwFcltyClmProcExtr_InABC.PROC_CD").alias("FCLTY_CLM_PROC_CD"),
        col("lnk_IdsEdwFcltyClmProcExtr_InABC.PROC_CD_DESC").alias("PROC_CD_DESC"),
        col("lnk_IdsEdwFcltyClmProcExtr_InABC.PROC_CD_MOD_TX").alias("FCLTY_CLM_PROC_CD_MOD_TX"),
        col("Ref_ProcCDLkp.PROC_CD_TYP_CD_SK").alias("PROC_CD_TYP_CD_SK"),
        col("Ref_ProcCDLkp.PROC_CD_TYP_CD").alias("FCLTY_CLM_PROC_CD_TYP_CD"),
    )
)

# --------------------------------------------------------------------------------
# Stage: lkp_Codes (PxLookup)
# --------------------------------------------------------------------------------

df_lkp_Codes = (
    df_lkp_ProcCD.alias("lnk_CodesLkpData_out")
    .join(
        df_cpy_Ref_FcltyClmProcOrdnlLkup.alias("Ref_FcltyClmProcOrdnlLkup"),
        on=[
            col("lnk_CodesLkpData_out.FCLTY_CLM_PROC_ORDNL_CD_SK")
            == col("Ref_FcltyClmProcOrdnlLkup.CD_MPPNG_SK")
        ],
        how="left",
    )
    .join(
        df_cpy_Ref_FcltyClmProcCdTypeLkup.alias("Ref_FcltyClmProcCdTypeLkup"),
        on=[
            col("lnk_CodesLkpData_out.PROC_CD_TYP_CD_SK")
            == col("Ref_FcltyClmProcCdTypeLkup.CD_MPPNG_SK")
        ],
        how="left",
    )
    .select(
        col("lnk_CodesLkpData_out.FCLTY_CLM_PROC_SK").alias("FCLTY_CLM_PROC_SK"),
        col("lnk_CodesLkpData_out.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnk_CodesLkpData_out.CLM_ID").alias("CLM_ID"),
        col("Ref_FcltyClmProcOrdnlLkup.TRGT_CD").alias("FCLTY_CLM_PROC_ORDNL_CD"),
        col("lnk_CodesLkpData_out.CLM_SK").alias("CLM_SK"),
        col("lnk_CodesLkpData_out.FCLTY_CLM_SK").alias("FCLTY_CLM_SK"),
        col("lnk_CodesLkpData_out.FCLTY_CLM_PROC_CD_SK").alias("FCLTY_CLM_PROC_CD_SK"),
        col("lnk_CodesLkpData_out.FCLTY_CLM_PROC_DT_SK").alias("FCLTY_CLM_PROC_DT_SK"),
        col("lnk_CodesLkpData_out.FCLTY_CLM_PROC_CD").alias("FCLTY_CLM_PROC_CD"),
        col("lnk_CodesLkpData_out.PROC_CD_DESC").alias("FCLTY_CLM_PROC_DESC"),
        col("lnk_CodesLkpData_out.FCLTY_CLM_PROC_CD_TYP_CD").alias("FCLTY_CLM_PROC_CD_TYP_CD"),
        col("Ref_FcltyClmProcCdTypeLkup.TRGT_CD_NM").alias("FCLTY_CLM_PROC_CD_TYP_NM"),
        col("Ref_FcltyClmProcOrdnlLkup.TRGT_CD_NM").alias("FCLTY_CLM_PROC_ORDNL_NM"),
        col("lnk_CodesLkpData_out.FCLTY_CLM_PROC_CD_MOD_TX").alias("FCLTY_CLM_PROC_CD_MOD_TX"),
        col("lnk_CodesLkpData_out.FCLTY_CLM_PROC_ORDNL_CD_SK").alias("FCLTY_CLM_PROC_ORDNL_CD_SK"),
        col("lnk_CodesLkpData_out.PROC_CD_TYP_CD_SK").alias("FCLTY_CLM_PROC_CD_TYP_CD_SK"),
    )
)

# --------------------------------------------------------------------------------
# Stage: xfm_BusinessLogic (CTransformerStage)
# --------------------------------------------------------------------------------

# Output link 1: FcltyClmProcIMainExtr
df_xfm_BusinessLogic_FcltyClmProcIMainExtr = (
    df_lkp_Codes.filter(
        (col("FCLTY_CLM_PROC_SK") != 0) & (col("FCLTY_CLM_PROC_SK") != 1)
    )
    .select(
        col("FCLTY_CLM_PROC_SK").alias("FCLTY_CLM_PROC_SK"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("CLM_ID").alias("CLM_ID"),
        col("FCLTY_CLM_PROC_ORDNL_CD").alias("FCLTY_CLM_PROC_ORDNL_CD"),
        lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        col("CLM_SK").alias("CLM_SK"),
        col("FCLTY_CLM_SK").alias("FCLTY_CLM_SK"),
        col("FCLTY_CLM_PROC_CD_SK").alias("FCLTY_CLM_PROC_CD_SK"),
        col("FCLTY_CLM_PROC_DT_SK").alias("FCLTY_CLM_PROC_DT_SK"),
        col("FCLTY_CLM_PROC_CD").alias("FCLTY_CLM_PROC_CD"),
        col("FCLTY_CLM_PROC_DESC").alias("FCLTY_CLM_PROC_DESC"),
        col("FCLTY_CLM_PROC_ORDNL_NM").alias("FCLTY_CLM_PROC_ORDNL_NM"),
        col("FCLTY_CLM_PROC_CD_MOD_TX").alias("FCLTY_CLM_PROC_CD_PRI_MOD_TX"),
        lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("FCLTY_CLM_PROC_ORDNL_CD_SK").alias("FCLTY_CLM_PROC_ORDNL_CD_SK"),
        when(trim(col("FCLTY_CLM_PROC_CD_TYP_CD_SK")) == "", lit(0))
        .otherwise(col("FCLTY_CLM_PROC_CD_TYP_CD_SK"))
        .alias("FCLTY_CLM_PROC_CD_TYP_CD_SK"),
        col("FCLTY_CLM_PROC_CD_TYP_CD").alias("FCLTY_CLM_PROC_CD_TYP_CD"),
        col("FCLTY_CLM_PROC_CD_TYP_NM").alias("FCLTY_CLM_PROC_CD_TYP_NM"),
    )
)

# Output link 2: UNK (one row of constants)
df_xfm_BusinessLogic_UNK = df_lkp_Codes.limit(1).select(
    lit(0).alias("FCLTY_CLM_PROC_SK"),
    lit("UNK").alias("SRC_SYS_CD"),
    lit("UNK").alias("CLM_ID"),
    lit("UNK").alias("FCLTY_CLM_PROC_ORDNL_CD"),
    lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    lit(0).alias("CLM_SK"),
    lit(0).alias("FCLTY_CLM_SK"),
    lit(0).alias("FCLTY_CLM_PROC_CD_SK"),
    lit("1753-01-01").alias("FCLTY_CLM_PROC_DT_SK"),
    lit("UNK").alias("FCLTY_CLM_PROC_CD"),
    lit("").alias("FCLTY_CLM_PROC_DESC"),
    lit("UNK").alias("FCLTY_CLM_PROC_ORDNL_NM"),
    lit("").alias("FCLTY_CLM_PROC_CD_PRI_MOD_TX"),
    lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("FCLTY_CLM_PROC_ORDNL_CD_SK"),
    lit(0).alias("FCLTY_CLM_PROC_CD_TYP_CD_SK"),
    lit("UNK").alias("FCLTY_CLM_PROC_CD_TYP_CD"),
    lit("UNK").alias("FCLTY_CLM_PROC_CD_TYP_NM"),
)

# Output link 3: NA (one row of constants)
df_xfm_BusinessLogic_NA = df_lkp_Codes.limit(1).select(
    lit(1).alias("FCLTY_CLM_PROC_SK"),
    lit("NA").alias("SRC_SYS_CD"),
    lit("NA").alias("CLM_ID"),
    lit("NA").alias("FCLTY_CLM_PROC_ORDNL_CD"),
    lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    lit(1).alias("CLM_SK"),
    lit(1).alias("FCLTY_CLM_SK"),
    lit(1).alias("FCLTY_CLM_PROC_CD_SK"),
    lit("1753-01-01").alias("FCLTY_CLM_PROC_DT_SK"),
    lit("NA").alias("FCLTY_CLM_PROC_CD"),
    lit("").alias("FCLTY_CLM_PROC_DESC"),
    lit("NA").alias("FCLTY_CLM_PROC_ORDNL_NM"),
    lit("").alias("FCLTY_CLM_PROC_CD_PRI_MOD_TX"),
    lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("FCLTY_CLM_PROC_ORDNL_CD_SK"),
    lit(1).alias("FCLTY_CLM_PROC_CD_TYP_CD_SK"),
    lit("NA").alias("FCLTY_CLM_PROC_CD_TYP_CD"),
    lit("NA").alias("FCLTY_CLM_PROC_CD_TYP_NM"),
)

# --------------------------------------------------------------------------------
# Stage: Funnel_26 (PxFunnel)
# --------------------------------------------------------------------------------

df_Funnel_26 = (
    df_xfm_BusinessLogic_FcltyClmProcIMainExtr.select(
        "FCLTY_CLM_PROC_SK",
        "SRC_SYS_CD",
        "CLM_ID",
        "FCLTY_CLM_PROC_ORDNL_CD",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "FCLTY_CLM_PROC_DT_SK",
        "FCLTY_CLM_PROC_CD",
        "FCLTY_CLM_PROC_DESC",
        "FCLTY_CLM_PROC_CD_PRI_MOD_TX",
        "FCLTY_CLM_PROC_ORDNL_NM",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLM_SK",
        "FCLTY_CLM_SK",
        "FCLTY_CLM_PROC_CD_SK",
        "FCLTY_CLM_PROC_ORDNL_CD_SK",
        "FCLTY_CLM_PROC_CD_TYP_CD_SK",
        "FCLTY_CLM_PROC_CD_TYP_CD",
        "FCLTY_CLM_PROC_CD_TYP_NM",
    )
    .union(
        df_xfm_BusinessLogic_UNK.select(
            "FCLTY_CLM_PROC_SK",
            "SRC_SYS_CD",
            "CLM_ID",
            "FCLTY_CLM_PROC_ORDNL_CD",
            "CRT_RUN_CYC_EXCTN_DT_SK",
            "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
            "FCLTY_CLM_PROC_DT_SK",
            "FCLTY_CLM_PROC_CD",
            "FCLTY_CLM_PROC_DESC",
            "FCLTY_CLM_PROC_CD_PRI_MOD_TX",
            "FCLTY_CLM_PROC_ORDNL_NM",
            "CRT_RUN_CYC_EXCTN_SK",
            "LAST_UPDT_RUN_CYC_EXCTN_SK",
            "CLM_SK",
            "FCLTY_CLM_SK",
            "FCLTY_CLM_PROC_CD_SK",
            "FCLTY_CLM_PROC_ORDNL_CD_SK",
            "FCLTY_CLM_PROC_CD_TYP_CD_SK",
            "FCLTY_CLM_PROC_CD_TYP_CD",
            "FCLTY_CLM_PROC_CD_TYP_NM",
        )
    )
    .union(
        df_xfm_BusinessLogic_NA.select(
            "FCLTY_CLM_PROC_SK",
            "SRC_SYS_CD",
            "CLM_ID",
            "FCLTY_CLM_PROC_ORDNL_CD",
            "CRT_RUN_CYC_EXCTN_DT_SK",
            "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
            "FCLTY_CLM_PROC_DT_SK",
            "FCLTY_CLM_PROC_CD",
            "FCLTY_CLM_PROC_DESC",
            "FCLTY_CLM_PROC_CD_PRI_MOD_TX",
            "FCLTY_CLM_PROC_ORDNL_NM",
            "CRT_RUN_CYC_EXCTN_SK",
            "LAST_UPDT_RUN_CYC_EXCTN_SK",
            "CLM_SK",
            "FCLTY_CLM_SK",
            "FCLTY_CLM_PROC_CD_SK",
            "FCLTY_CLM_PROC_ORDNL_CD_SK",
            "FCLTY_CLM_PROC_CD_TYP_CD_SK",
            "FCLTY_CLM_PROC_CD_TYP_CD",
            "FCLTY_CLM_PROC_CD_TYP_NM",
        )
    )
)

# --------------------------------------------------------------------------------
# Stage: seq_FCLTY_CLM_PROC_I_csv_load (PxSequentialFile)
# --------------------------------------------------------------------------------
# Apply rpad for char fields (length in the final stage):
df_final = df_Funnel_26.select(
    col("FCLTY_CLM_PROC_SK"),
    col("SRC_SYS_CD"),
    col("CLM_ID"),
    col("FCLTY_CLM_PROC_ORDNL_CD"),
    rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    rpad(col("FCLTY_CLM_PROC_DT_SK"), 10, " ").alias("FCLTY_CLM_PROC_DT_SK"),
    col("FCLTY_CLM_PROC_CD"),
    col("FCLTY_CLM_PROC_DESC"),
    rpad(col("FCLTY_CLM_PROC_CD_PRI_MOD_TX"), 2, " ").alias("FCLTY_CLM_PROC_CD_PRI_MOD_TX"),
    col("FCLTY_CLM_PROC_ORDNL_NM"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CLM_SK"),
    col("FCLTY_CLM_SK"),
    col("FCLTY_CLM_PROC_CD_SK"),
    col("FCLTY_CLM_PROC_ORDNL_CD_SK"),
    col("FCLTY_CLM_PROC_CD_TYP_CD_SK"),
    col("FCLTY_CLM_PROC_CD_TYP_CD"),
    col("FCLTY_CLM_PROC_CD_TYP_NM"),
)

write_files(
    df_final,
    f"{adls_path}/load/FCLTY_CLM_PROC_I.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)