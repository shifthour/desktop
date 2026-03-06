# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsClmMartClmProcExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from IDS to create claim data mart
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	IDS - FCLTY_CLM_PROC
# MAGIC                 IDS - P_CLM_EXTR
# MAGIC                 IDS - CD_MPPNG
# MAGIC                 IDS - PROC_CD              
# MAGIC 
# MAGIC   
# MAGIC HASH FILES:
# MAGIC                 hf_clmlnproc_mppng - hash file for sk lookup
# MAGIC                
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   IDS - read from source, lookup all code SK values and get the natural codes
# MAGIC                  
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     direct update of CLM_DM_CLM_PROC
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               Steph Goddard   03/22/2005 -   Originally Programmed
# MAGIC               BJ Luce              09/29/2005 - change SQL parameters to ClmMart for job sequencer
# MAGIC               Brent Leland     04/03/2006    Changed to use evironment parameters
# MAGIC                                                               Changed name of driver table
# MAGIC               Brent Leland     04/06/2006    Added run cycle parameter for new table field LAST_UPDT_RUN_CYC_NO
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date                    Project/Altiris #                     Change Description                                                       Development Project               Code Reviewer                      Date Reviewed
# MAGIC ---------------------------------      -------------------      -----------------------------------           ---------------------------------------------------------                                 ----------------------------------              ---------------------------------               -------------------------
# MAGIC Steph Goddard             03/22/2005 -                                                    Originally Programmed
# MAGIC BJ Luce                       09/29/2005 -                                                     Change SQL parameters to ClmMart for job sequencer
# MAGIC  Brent Leland               04/03/2006                                                       Changed to use evironment parameters
# MAGIC                                                                                                               Changed name of driver table
# MAGIC Brent Leland                04/06/2006                                                      Added run cycle parameter for new table field 
# MAGIC                                                                                                             LAST_UPDT_RUN_CYC_NO 
# MAGIC 
# MAGIC Nagesh Bandi              09/17/2013              5114                                 Original Programming(Server to Parallel)                             IntegrateWrhsDevl

# MAGIC Write CLM_DM_CLM_LN_PROC Data into a Sequential file for Load Job IdsDmClmMartClmProcLoad.
# MAGIC Add Defaults and Null Handling.
# MAGIC Job Name: 
# MAGIC 
# MAGIC IdsDmClmMartClmProcExtr
# MAGIC Read all the Data from IDS FCLTY_CLM_PROC Table;
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, lit
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Parameters
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')

# ---------------------------------------------------------------------------------------------------------------------
# db2_FCLTY_CLM_PROC_Extr (DB2ConnectorPX)
jdbc_url_db2_FCLTY_CLM_PROC_Extr, jdbc_props_db2_FCLTY_CLM_PROC_Extr = get_db_config(ids_secret_name)
extract_query_db2_FCLTY_CLM_PROC_Extr = (
    "SELECT \n"
    "PROC.CLM_ID,\n"
    "COALESCE (CD1.TRGT_CD, ' ') AS SRC_SYS_CD,\n"
    "FCLTY_CLM_PROC_ORDNL_CD_SK,\n"
    "PROC_CD,\n"
    "PROC_CD_CAT_CD_SK,\n"
    "PROC_CD_DESC \n"
    "FROM \n"
    f"{IDSOwner}.FCLTY_CLM_PROC PROC\n"
    f"JOIN {IDSOwner}.W_WEBDM_ETL_DRVR EXTR \n"
    "ON PROC.SRC_SYS_CD_SK = EXTR.SRC_SYS_CD_SK AND PROC.CLM_ID = EXTR.CLM_ID\n"
    f"JOIN {IDSOwner}.PROC_CD CD \n"
    "ON PROC.PROC_CD_SK = CD.PROC_CD_SK  \n"
    f"LEFT JOIN {IDSOwner}.CD_MPPNG CD1  \n"
    "ON  PROC.SRC_SYS_CD_SK  = CD1.CD_MPPNG_SK"
)
df_db2_FCLTY_CLM_PROC_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_FCLTY_CLM_PROC_Extr)
    .options(**jdbc_props_db2_FCLTY_CLM_PROC_Extr)
    .option("query", extract_query_db2_FCLTY_CLM_PROC_Extr)
    .load()
)

# ---------------------------------------------------------------------------------------------------------------------
# db2_CD_MPPNG_in (DB2ConnectorPX)
jdbc_url_db2_CD_MPPNG_in, jdbc_props_db2_CD_MPPNG_in = get_db_config(ids_secret_name)
extract_query_db2_CD_MPPNG_in = (
    "SELECT \n"
    "CD_MPPNG_SK,\n"
    "COALESCE(TRGT_CD,'UNK') TRGT_CD,\n"
    "COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM \n"
    f"FROM {IDSOwner}.CD_MPPNG"
)
df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_CD_MPPNG_in)
    .options(**jdbc_props_db2_CD_MPPNG_in)
    .option("query", extract_query_db2_CD_MPPNG_in)
    .load()
)

# ---------------------------------------------------------------------------------------------------------------------
# cp_ProcCd (PxCopy)
df_cp_ProcCd = df_db2_CD_MPPNG_in

df_ref_CLM_ProcOrdnnlCd_lkp = df_cp_ProcCd.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_ref_ProcCatCd_lkp = df_cp_ProcCd.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

# ---------------------------------------------------------------------------------------------------------------------
# lkp_Codes (PxLookup)
df_lkp_Codes_Primary = df_db2_FCLTY_CLM_PROC_Extr.alias("lnk_IdsDmClmMartClmProcExtr_InABC")

df_lkp_Codes_join1 = df_lkp_Codes_Primary.join(
    df_ref_ProcCatCd_lkp.alias("ref_ProcCatCd_lkp"),
    col("lnk_IdsDmClmMartClmProcExtr_InABC.FCLTY_CLM_PROC_ORDNL_CD_SK") == col("ref_ProcCatCd_lkp.CD_MPPNG_SK"),
    "left"
)

df_lkp_Codes_joinfinal = df_lkp_Codes_join1.join(
    df_ref_CLM_ProcOrdnnlCd_lkp.alias("ref_CLM_ProcOrdnnlCd_lkp"),
    col("lnk_IdsDmClmMartClmProcExtr_InABC.PROC_CD_CAT_CD_SK") == col("ref_CLM_ProcOrdnnlCd_lkp.CD_MPPNG_SK"),
    "left"
)

df_lkp_Codes_DataOut = df_lkp_Codes_joinfinal.select(
    col("lnk_IdsDmClmMartClmProcExtr_InABC.CLM_ID").alias("CLM_ID"),
    col("lnk_IdsDmClmMartClmProcExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnk_IdsDmClmMartClmProcExtr_InABC.FCLTY_CLM_PROC_ORDNL_CD_SK").alias("FCLTY_CLM_PROC_ORDNL_CD_SK"),
    col("lnk_IdsDmClmMartClmProcExtr_InABC.PROC_CD").alias("PROC_CD"),
    col("lnk_IdsDmClmMartClmProcExtr_InABC.PROC_CD_CAT_CD_SK").alias("PROC_CD_CAT_CD_SK"),
    col("lnk_IdsDmClmMartClmProcExtr_InABC.PROC_CD_DESC").alias("PROC_CD_DESC"),
    col("ref_ProcCatCd_lkp.TRGT_CD").alias("CLM_PROC_ORDNL_CD"),
    col("ref_CLM_ProcOrdnnlCd_lkp.TRGT_CD").alias("PROC_CAT_CD")
)

# ---------------------------------------------------------------------------------------------------------------------
# xfrm_BusinessLogic (CTransformerStage)
df_xfrm_BusinessLogic = (
    df_lkp_Codes_DataOut
    .withColumn(
        "SRC_SYS_CD",
        when(trim(col("SRC_SYS_CD")) == "", lit(" ")).otherwise(col("SRC_SYS_CD"))
    )
    .withColumn(
        "CLM_PROC_ORDNL_CD",
        when(trim(col("CLM_PROC_ORDNL_CD")) == "", lit(" ")).otherwise(col("CLM_PROC_ORDNL_CD"))
    )
    .withColumn(
        "PROC_CAT_CD",
        when(trim(col("PROC_CAT_CD")) == "", lit(" ")).otherwise(col("PROC_CAT_CD"))
    )
    .withColumn("LAST_UPDT_RUN_CYC_NO", lit(CurrRunCycle))
    .select(
        "SRC_SYS_CD",
        "CLM_ID",
        "CLM_PROC_ORDNL_CD",
        "PROC_CD",
        "PROC_CAT_CD",
        "PROC_CD_DESC",
        "LAST_UPDT_RUN_CYC_NO"
    )
)

# ---------------------------------------------------------------------------------------------------------------------
# seq_CLM_DM_CLM_PROC_csv_load (PxSequentialFile)
write_files(
    df_xfrm_BusinessLogic,
    f"{adls_path}/load/CLM_DM_CLM_PROC.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)