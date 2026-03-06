# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     EdwComsnScheduleExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from ids fee discount table COMSN_SCHD and loads to EDW
# MAGIC   Does not keep history.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	IDS:     COMSN_SCHD
# MAGIC                 EDW:  COMSN_SCHD_D                        
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   TRIM all the fields that are extracted or used in lookups
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   IDS - read from records from the source that have a run cycle greater than the BeginCycle, lookup all code SK values and get the natural codes
# MAGIC  
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                   An EDW load file.   Since no history, then the output is a load file to update the EDW table.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               Sharon Andrew 11/07/2005  - Originally Programmed
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                       Change Description                                               Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -------------------------------------------------- ----------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Ralph Tucker                  2012-08-17      4873 - Commissions Reporting  Added new field; updated to latest standards          IntegrateNewDevl        Kalyan Neelam              2012-11-06
# MAGIC 
# MAGIC 
# MAGIC Raj Mangalampally         11/29/2013               5114                               Original Programming                                                  EnterpriseWrhsDevl    Jag Yelavarthi               2013-12-26   
# MAGIC                                                                                                             (Server to Parallel Conv)

# MAGIC Write COMSN_SCHD_D Data into a Sequential file for Load Job IdsEdwComsnSchdDLoad.
# MAGIC Read all the Data from IDS COMSN_SCHD Table and join on CD_MPPNG table to bring SRC_SYS_CD ; 
# MAGIC Pull records based on LAST_UPDT_RUN_CYC_EXCTN_SK.
# MAGIC Add Defaults and Null Handling.
# MAGIC Job Name: IdsEdwComsnSchdDExtr
# MAGIC This Job will extract records from COMSN_SCHD from IDS and applies code Denormalization
# MAGIC Lookup Keys
# MAGIC 1COMSN_SCHD_CALC_METH_CD_SK
# MAGIC 2)COMSN_MTHDLGY_TYP_CD_SK
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.functions import col, lit, when, rpad
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Retrieve parameter values
BeginCycle = get_widget_value('BeginCycle','')
EdwRunCycleDate = get_widget_value('EdwRunCycleDate','')
IDSOwner = get_widget_value('IDSOwner','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
ids_secret_name = get_widget_value('ids_secret_name','')

# --------------------------------------------------------------------------------
# Stage: db2_COMSN_SCHD_D_in (DB2ConnectorPX)
# --------------------------------------------------------------------------------
db2_COMSN_SCHD_D_in_url, db2_COMSN_SCHD_D_in_props = get_db_config(ids_secret_name)
extract_query_db2_COMSN_SCHD_D_in = (
    f"SELECT \n"
    f"COMSN_SCHD.COMSN_SCHD_SK,\n"
    f"COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,\n"
    f"COMSN_SCHD.SRC_SYS_CD_SK,\n"
    f"COMSN_SCHD.COMSN_SCHD_ID,\n"
    f"COMSN_SCHD.COMSN_SCHD_CALC_METH_CD_SK,\n"
    f"COMSN_SCHD.SCHD_DESC,\n"
    f"COMSN_SCHD.COMSN_MTHDLGY_TYP_CD_SK \n\n"
    f"FROM {IDSOwner}.COMSN_SCHD COMSN_SCHD \n"
    f"LEFT JOIN {IDSOwner}.CD_MPPNG CD\n"
    f"ON COMSN_SCHD.SRC_SYS_CD_SK = CD.CD_MPPNG_SK\n"
    f"WHERE \n"
    f"COMSN_SCHD.LAST_UPDT_RUN_CYC_EXCTN_SK >= {BeginCycle}"
)
df_db2_COMSN_SCHD_D_in = (
    spark.read.format("jdbc")
    .option("url", db2_COMSN_SCHD_D_in_url)
    .options(**db2_COMSN_SCHD_D_in_props)
    .option("query", extract_query_db2_COMSN_SCHD_D_in)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: db2_CD_MPPNG_Extr (DB2ConnectorPX)
# --------------------------------------------------------------------------------
db2_CD_MPPNG_Extr_url, db2_CD_MPPNG_Extr_props = get_db_config(ids_secret_name)
extract_query_db2_CD_MPPNG_Extr = (
    f"SELECT CD_MPPNG_SK,COALESCE(TRGT_CD,'UNK') TRGT_CD,COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM "
    f"from {IDSOwner}.CD_MPPNG"
)
df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", db2_CD_MPPNG_Extr_url)
    .options(**db2_CD_MPPNG_Extr_props)
    .option("query", extract_query_db2_CD_MPPNG_Extr)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: cpy_cd_mppng (PxCopy)
# --------------------------------------------------------------------------------
df_Ref_CalcMethodCd_Lkup = df_db2_CD_MPPNG_Extr.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_Ref_ComsnMthdglyTypCd_Lkup = df_db2_CD_MPPNG_Extr.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

# --------------------------------------------------------------------------------
# Stage: lkp_Codes (PxLookup)
# --------------------------------------------------------------------------------
df_lkp_Codes_temp = (
    df_db2_COMSN_SCHD_D_in.alias("lnk_IdsEdwComsnSchdDExtr_InABC")
    .join(
        df_Ref_ComsnMthdglyTypCd_Lkup.alias("Ref_ComsnMthdglyTypCd_Lkup"),
        col("lnk_IdsEdwComsnSchdDExtr_InABC.COMSN_MTHDLGY_TYP_CD_SK")
        == col("Ref_ComsnMthdglyTypCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Ref_CalcMethodCd_Lkup.alias("Ref_CalcMethodCd_Lkup"),
        col("lnk_IdsEdwComsnSchdDExtr_InABC.COMSN_SCHD_CALC_METH_CD_SK")
        == col("Ref_CalcMethodCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
)

df_lkp_Codes = df_lkp_Codes_temp.select(
    col("lnk_IdsEdwComsnSchdDExtr_InABC.COMSN_SCHD_SK").alias("COMSN_SCHD_SK"),
    col("lnk_IdsEdwComsnSchdDExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnk_IdsEdwComsnSchdDExtr_InABC.COMSN_SCHD_ID").alias("COMSN_SCHD_ID"),
    col("Ref_CalcMethodCd_Lkup.TRGT_CD").alias("COMSN_SCHD_CALC_METH_CD"),
    col("Ref_CalcMethodCd_Lkup.TRGT_CD_NM").alias("COMSN_SCHD_CALC_METH_NM"),
    col("lnk_IdsEdwComsnSchdDExtr_InABC.SCHD_DESC").alias("COMSN_SCHD_DESC"),
    col("lnk_IdsEdwComsnSchdDExtr_InABC.COMSN_SCHD_CALC_METH_CD_SK").alias("COMSN_SCHD_CALC_METH_CD_SK"),
    col("lnk_IdsEdwComsnSchdDExtr_InABC.COMSN_MTHDLGY_TYP_CD_SK").alias("COMSN_MTHDLGY_TYP_CD_SK"),
    col("Ref_ComsnMthdglyTypCd_Lkup.TRGT_CD").alias("COMSN_MTHDLGY_TYP_CD"),
    col("Ref_ComsnMthdglyTypCd_Lkup.TRGT_CD_NM").alias("COMSN_MTHDLGY_TYP_NM")
)

# --------------------------------------------------------------------------------
# Stage: xfrm_BusinessLogic (CTransformerStage)
#   - Three output links: lnk_IdsEdwComsnSchdDMainData_Out, lnk_UNK, lnk_NA
# --------------------------------------------------------------------------------
# Input DF
df_xfrm_BusinessLogic_in = df_lkp_Codes

# 1) lnk_IdsEdwComsnSchdDMainData_Out => constraint => COMSN_SCHD_SK <> 0 AND COMSN_SCHD_SK <> 1
df_lnk_IdsEdwComsnSchdDMainData_Out = (
    df_xfrm_BusinessLogic_in.filter((col("COMSN_SCHD_SK") != 0) & (col("COMSN_SCHD_SK") != 1))
    .select(
        col("COMSN_SCHD_SK").alias("COMSN_SCHD_SK"),
        when(trim(col("SRC_SYS_CD")) == "", lit("UNK")).otherwise(col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
        col("COMSN_SCHD_ID").alias("COMSN_SCHD_ID"),
        lit(EdwRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        lit(EdwRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        when(trim(col("COMSN_SCHD_CALC_METH_CD")) == "", lit("UNK"))
            .otherwise(col("COMSN_SCHD_CALC_METH_CD")).alias("COMSN_SCHD_CALC_METH_CD"),
        when(trim(col("COMSN_SCHD_CALC_METH_NM")) == "", lit("UNK"))
            .otherwise(col("COMSN_SCHD_CALC_METH_NM")).alias("COMSN_SCHD_CALC_METH_NM"),
        col("COMSN_SCHD_DESC").alias("COMSN_SCHD_DESC"),
        lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("COMSN_SCHD_CALC_METH_CD_SK").alias("COMSN_SCHD_CALC_METH_CD_SK"),
        col("COMSN_MTHDLGY_TYP_CD_SK").alias("COMSN_MTHDLGY_TYP_CD_SK"),
        when(trim(col("COMSN_MTHDLGY_TYP_CD")) == "", lit("UNK"))
            .otherwise(col("COMSN_MTHDLGY_TYP_CD")).alias("COMSN_MTHDLGY_TYP_CD"),
        when(trim(col("COMSN_MTHDLGY_TYP_NM")) == "", lit("UNKNOWN"))
            .otherwise(col("COMSN_MTHDLGY_TYP_NM")).alias("COMSN_MTHDLGY_TYP_NM")
    )
)

# 2) lnk_UNK => constraint => "((@INROWNUM - 1) * @NUMPARTITIONS + @PARTITIONNUM + 1) = 1"
#    This creates a single synthetic row.
df_lnk_UNK = spark.createDataFrame(
    [
        (
            0,      # COMSN_SCHD_SK
            'UNK',  # SRC_SYS_CD
            'UNK',  # COMSN_SCHD_ID
            '1753-01-01', # CRT_RUN_CYC_EXCTN_DT_SK
            '1753-01-01', # LAST_UPDT_RUN_CYC_EXCTN_DT_SK
            'UNK',  # COMSN_SCHD_CALC_METH_CD
            'UNK',  # COMSN_SCHD_CALC_METH_NM
            '',     # COMSN_SCHD_DESC
            100,    # CRT_RUN_CYC_EXCTN_SK
            100,    # LAST_UPDT_RUN_CYC_EXCTN_SK
            0,      # COMSN_SCHD_CALC_METH_CD_SK
            0,      # COMSN_MTHDLGY_TYP_CD_SK
            'UNK',  # COMSN_MTHDLGY_TYP_CD
            'UNK'   # COMSN_MTHDLGY_TYP_NM
        )
    ],
    schema=[
        "COMSN_SCHD_SK", "SRC_SYS_CD", "COMSN_SCHD_ID",
        "CRT_RUN_CYC_EXCTN_DT_SK", "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "COMSN_SCHD_CALC_METH_CD", "COMSN_SCHD_CALC_METH_NM", "COMSN_SCHD_DESC",
        "CRT_RUN_CYC_EXCTN_SK", "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "COMSN_SCHD_CALC_METH_CD_SK", "COMSN_MTHDLGY_TYP_CD_SK",
        "COMSN_MTHDLGY_TYP_CD", "COMSN_MTHDLGY_TYP_NM"
    ]
)

# 3) lnk_NA => constraint => "((@INROWNUM - 1) * @NUMPARTITIONS + @PARTITIONNUM + 1) = 1"
#    Another single synthetic row.
df_lnk_NA = spark.createDataFrame(
    [
        (
            1,       # COMSN_SCHD_SK
            'NA',    # SRC_SYS_CD
            'NA',    # COMSN_SCHD_ID
            '1753-01-01', # CRT_RUN_CYC_EXCTN_DT_SK
            '1753-01-01', # LAST_UPDT_RUN_CYC_EXCTN_DT_SK
            'NA',    # COMSN_SCHD_CALC_METH_CD
            'NA',    # COMSN_SCHD_CALC_METH_NM
            '',      # COMSN_SCHD_DESC
            100,     # CRT_RUN_CYC_EXCTN_SK
            100,     # LAST_UPDT_RUN_CYC_EXCTN_SK
            1,       # COMSN_SCHD_CALC_METH_CD_SK
            1,       # COMSN_MTHDLGY_TYP_CD_SK
            'NA',    # COMSN_MTHDLGY_TYP_CD
            'NA'     # COMSN_MTHDLGY_TYP_NM
        )
    ],
    schema=[
        "COMSN_SCHD_SK", "SRC_SYS_CD", "COMSN_SCHD_ID",
        "CRT_RUN_CYC_EXCTN_DT_SK", "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "COMSN_SCHD_CALC_METH_CD", "COMSN_SCHD_CALC_METH_NM", "COMSN_SCHD_DESC",
        "CRT_RUN_CYC_EXCTN_SK", "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "COMSN_SCHD_CALC_METH_CD_SK", "COMSN_MTHDLGY_TYP_CD_SK",
        "COMSN_MTHDLGY_TYP_CD", "COMSN_MTHDLGY_TYP_NM"
    ]
)

# --------------------------------------------------------------------------------
# Stage: fnl_UNK_NA (PxFunnel)
#   Funnel the three dataframes together
# --------------------------------------------------------------------------------
# Union them (all columns must match exactly in the same order)
df_fnl_UNK_NA = (
    df_lnk_IdsEdwComsnSchdDMainData_Out
    .unionByName(df_lnk_UNK)
    .unionByName(df_lnk_NA)
)

# --------------------------------------------------------------------------------
# Stage: seq_COMSN_SCHD_D_csv_load (PxSequentialFile)
#   Write to file COMSN_SCHD_D.dat (no header, quoteChar="^", delimiter=",", nullValue=None)
# --------------------------------------------------------------------------------
# For columns with declared char(10), apply rpad. Others remain as is.
df_seq_COMSN_SCHD_D_csv_load = df_fnl_UNK_NA.select(
    col("COMSN_SCHD_SK"),
    col("SRC_SYS_CD"),
    col("COMSN_SCHD_ID"),
    rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("COMSN_SCHD_CALC_METH_CD"),
    col("COMSN_SCHD_CALC_METH_NM"),
    col("COMSN_SCHD_DESC"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("COMSN_SCHD_CALC_METH_CD_SK"),
    col("COMSN_MTHDLGY_TYP_CD_SK"),
    col("COMSN_MTHDLGY_TYP_CD"),
    col("COMSN_MTHDLGY_TYP_NM")
)

write_files(
    df_seq_COMSN_SCHD_D_csv_load,
    f"{adls_path}/load/COMSN_SCHD_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)