# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     EdwComsnMnlAdjExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from ids table COMSN_MNL_ADJ and loads to EDW
# MAGIC   Does not keep history.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	IDS:     COMSN_MNL_ADJ
# MAGIC                 EDW:  COMSN_ MNL_ADJ_F                        
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
# MAGIC               Oliver Nielsen 11/09/2005  - Originally Programmed
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                 Date              Project/Altiris #     Change Description                                                                                            DS Proj                             Reviewer            Dt Reviewed
# MAGIC ---------------------------      -------------------   -------------------------    ---------------------------------------------------------------------------------------------------                        -------------------                      -------------------         -------------------------   
# MAGIC Raj Mangalampally   12/25/2013          5114               Original Programming                                                                                         EnterpriseWrhsDevl    
# MAGIC                                                                                     (Server to Parallel Conversion)

# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC COMSN_MNL_ADJ_PAYMT_TYP_CD_SK
# MAGIC COMSN_MNL_ADJ_LOB_CD_SK
# MAGIC COMSN_MNL_ADJ_RSN_CD_SK
# MAGIC COMSN_MNL_ADJ_STTUS_CD_SK
# MAGIC Write COMSN_MNL_ADJ_F.dat Data into a Sequential file for Load Job IdsEdwComsnMnlAdjLoad.
# MAGIC Read all the Data from IDS COMMISSIONS Table COMSN_MNL_ADJ; 
# MAGIC Pull records based on LAST_UPDT_RUN_CYC_EXCTN_SK.
# MAGIC Add Defaults and Null Handling.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Job Name: IdsEdwComsnMnlAdjExtr
# MAGIC Read all the Data from IDS AGNT and COMSN_ARGMT Tables
# MAGIC Lookup Keys:
# MAGIC 
# MAGIC AGNT_SK
# MAGIC AGNT_ID
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, StructType, StructField
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# MAGIC %run ../../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
BeginCycle = get_widget_value('BeginCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
RunID = get_widget_value('RunID','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

# db2_CD_MPPNG_in
extract_query_db2_CD_MPPNG_in = f"SELECT CD_MPPNG_SK, COALESCE(TRGT_CD,'UNK') TRGT_CD, COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM from {IDSOwner}.CD_MPPNG"
df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CD_MPPNG_in)
    .load()
)

# cpy_cd_mppng (PxCopy split into 4 outputs)
df_cpy_cd_mppng_Ref_RsnCd_Lkup = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_cpy_cd_mppng_Ref_Sttus_Lkup = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_cpy_cd_mppng_Ref_LobCd_Lkup = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_cpy_cd_mppng_Ref_PaymtTyp_Lkup = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

# db2_COMSN_MNL_ADJ_in
extract_query_db2_COMSN_MNL_ADJ_in = f"""
SELECT 
CMA.COMSN_MNL_ADJ_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
CMA.AGNT_ID,
CMA.SEQ_NO,
CMA.AGNT_SK,
COMSN_ARGMT_SK,
CMA.PD_AGNT_SK,
CMA.COMSN_MNL_ADJ_LOB_CD_SK,
CMA.COMSN_MNL_ADJ_PAYMT_TYP_CD_SK,
CMA.COMSN_MNL_ADJ_RSN_CD_SK,
CMA.COMSN_MNL_ADJ_STTUS_CD_SK,
CMA.PD_DT_SK,
CMA.ADJ_AMT,
CMA.ADJ_DESC,
CMA.COMSN_AGMNT_EFF_DT_SK
FROM {IDSOwner}.COMSN_MNL_ADJ CMA
LEFT JOIN {IDSOwner}.CD_MPPNG CD
ON CMA.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
WHERE 
CMA.LAST_UPDT_RUN_CYC_EXCTN_SK >= {BeginCycle}
"""
df_db2_COMSN_MNL_ADJ_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_COMSN_MNL_ADJ_in)
    .load()
)

# db2_COMSN_ARGMT_in
extract_query_db2_COMSN_ARGMT_in = f"SELECT COMSN_ARGMT_SK, COMSN_ARGMT_ID FROM {IDSOwner}.COMSN_ARGMT"
df_db2_COMSN_ARGMT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_COMSN_ARGMT_in)
    .load()
)

# db2_AGNT_in
extract_query_db2_AGNT_in = f"SELECT AGNT_SK, AGNT_ID FROM {IDSOwner}.AGNT"
df_db2_AGNT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_AGNT_in)
    .load()
)

# cpy_AGNT_Data
df_cpy_AGNT_Data_Ref_AgntLkup = df_db2_AGNT_in.select(
    F.col("AGNT_SK").alias("AGNT_SK"),
    F.col("AGNT_ID").alias("AGNT_ID")
)
df_cpy_AGNT_Data_Ref_PaidAgntLkup = df_db2_AGNT_in.select(
    F.col("AGNT_SK").alias("AGNT_SK"),
    F.col("AGNT_ID").alias("AGNT_ID")
)

# lkp_Codes1 (PxLookup)
df_lkp_Codes1 = (
    df_db2_COMSN_MNL_ADJ_in.alias("lnk_IdsEdwComsnMnlAdjFExtr_InABC")
    .join(
        df_db2_COMSN_ARGMT_in.alias("Ref_ComsnArgmt_Lkup"),
        F.col("lnk_IdsEdwComsnMnlAdjFExtr_InABC.COMSN_ARGMT_SK") == F.col("Ref_ComsnArgmt_Lkup.COMSN_ARGMT_SK"),
        "left"
    )
    .join(
        df_cpy_AGNT_Data_Ref_AgntLkup.alias("Ref_AgntLkup"),
        F.col("lnk_IdsEdwComsnMnlAdjFExtr_InABC.AGNT_SK") == F.col("Ref_AgntLkup.AGNT_SK"),
        "left"
    )
    .join(
        df_cpy_AGNT_Data_Ref_PaidAgntLkup.alias("Ref_PaidAgntLkup"),
        F.col("lnk_IdsEdwComsnMnlAdjFExtr_InABC.PD_AGNT_SK") == F.col("Ref_PaidAgntLkup.AGNT_SK"),
        "left"
    )
    .select(
        F.col("lnk_IdsEdwComsnMnlAdjFExtr_InABC.COMSN_MNL_ADJ_SK").alias("COMSN_MNL_ADJ_SK"),
        F.col("lnk_IdsEdwComsnMnlAdjFExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_IdsEdwComsnMnlAdjFExtr_InABC.AGNT_ID").alias("AGNT_ID"),
        F.col("lnk_IdsEdwComsnMnlAdjFExtr_InABC.SEQ_NO").alias("SEQ_NO"),
        F.col("lnk_IdsEdwComsnMnlAdjFExtr_InABC.AGNT_SK").alias("AGNT_SK"),
        F.col("lnk_IdsEdwComsnMnlAdjFExtr_InABC.PD_DT_SK").alias("PD_DT_SK"),
        F.col("lnk_IdsEdwComsnMnlAdjFExtr_InABC.COMSN_MNL_ADJ_LOB_CD_SK").alias("COMSN_MNL_ADJ_LOB_CD_SK"),
        F.col("lnk_IdsEdwComsnMnlAdjFExtr_InABC.COMSN_MNL_ADJ_PAYMT_TYP_CD_SK").alias("COMSN_MNL_ADJ_PAYMT_TYP_CD_SK"),
        F.col("lnk_IdsEdwComsnMnlAdjFExtr_InABC.COMSN_MNL_ADJ_RSN_CD_SK").alias("COMSN_MNL_ADJ_RSN_CD_SK"),
        F.col("lnk_IdsEdwComsnMnlAdjFExtr_InABC.COMSN_MNL_ADJ_STTUS_CD_SK").alias("COMSN_MNL_ADJ_STTUS_CD_SK"),
        F.col("lnk_IdsEdwComsnMnlAdjFExtr_InABC.COMSN_AGMNT_EFF_DT_SK").alias("COMSN_AGMNT_EFF_DT_SK"),
        F.col("lnk_IdsEdwComsnMnlAdjFExtr_InABC.PD_AGNT_SK").alias("PD_AGNT_SK"),
        F.col("lnk_IdsEdwComsnMnlAdjFExtr_InABC.ADJ_AMT").alias("ADJ_AMT"),
        F.col("lnk_IdsEdwComsnMnlAdjFExtr_InABC.ADJ_DESC").alias("ADJ_DESC"),
        F.col("Ref_PaidAgntLkup.AGNT_ID").alias("AGNT_ID_PAID"),
        F.col("Ref_ComsnArgmt_Lkup.COMSN_ARGMT_ID").alias("COMSN_ARGMT_ID")
    )
)

# lkp_Codes (PxLookup)
df_lkp_Codes = (
    df_lkp_Codes1.alias("lnk_AgntData_Out")
    .join(
        df_cpy_cd_mppng_Ref_RsnCd_Lkup.alias("Ref_RsnCd_Lkup"),
        F.col("lnk_AgntData_Out.COMSN_MNL_ADJ_RSN_CD_SK") == F.col("Ref_RsnCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_cd_mppng_Ref_Sttus_Lkup.alias("Ref_Sttus_Lkup"),
        F.col("lnk_AgntData_Out.COMSN_MNL_ADJ_STTUS_CD_SK") == F.col("Ref_Sttus_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_cd_mppng_Ref_LobCd_Lkup.alias("Ref_LobCd_Lkup"),
        F.col("lnk_AgntData_Out.COMSN_MNL_ADJ_LOB_CD_SK") == F.col("Ref_LobCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_cd_mppng_Ref_PaymtTyp_Lkup.alias("Ref_PaymtTyp_Lkup"),
        F.col("lnk_AgntData_Out.COMSN_MNL_ADJ_PAYMT_TYP_CD_SK") == F.col("Ref_PaymtTyp_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .select(
        F.col("lnk_AgntData_Out.COMSN_MNL_ADJ_SK").alias("COMSN_MNL_ADJ_SK"),
        F.col("lnk_AgntData_Out.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_AgntData_Out.AGNT_ID").alias("AGNT_ID"),
        F.col("lnk_AgntData_Out.SEQ_NO").alias("COMSN_MNL_ADJ_SEQ_NO"),
        F.col("lnk_AgntData_Out.PD_DT_SK").alias("COMSN_MNL_ADJ_PD_DT_SK"),
        F.col("lnk_AgntData_Out.AGNT_SK").alias("AGNT_SK"),
        F.col("lnk_AgntData_Out.PD_AGNT_SK").alias("PD_AGNT_SK"),
        F.col("Ref_LobCd_Lkup.TRGT_CD").alias("COMSN_MNL_ADJ_LOB_CD"),
        F.col("Ref_LobCd_Lkup.TRGT_CD_NM").alias("COMSN_MNL_ADJ_LOB_NM"),
        F.col("Ref_PaymtTyp_Lkup.TRGT_CD").alias("COMSN_MNL_ADJ_PAYMT_TYP_CD"),
        F.col("Ref_PaymtTyp_Lkup.TRGT_CD_NM").alias("COMSN_MNL_ADJ_PAYMT_TYP_NM"),
        F.col("Ref_RsnCd_Lkup.TRGT_CD").alias("COMSN_MNL_ADJ_RSN_CD"),
        F.col("Ref_RsnCd_Lkup.TRGT_CD_NM").alias("COMSN_MNL_ADJ_RSN_NM"),
        F.col("Ref_Sttus_Lkup.TRGT_CD").alias("COMSN_MNL_ADJ_STTUS_CD"),
        F.col("Ref_Sttus_Lkup.TRGT_CD_NM").alias("COMSN_MNL_ADJ_STTUS_NM"),
        F.col("lnk_AgntData_Out.COMSN_AGMNT_EFF_DT_SK").alias("COMSN_AGMNT_EFF_DT_SK"),
        F.col("lnk_AgntData_Out.ADJ_AMT").alias("COMSN_MNL_ADJ_AMT"),
        F.col("lnk_AgntData_Out.AGNT_ID_PAID").alias("AGNT_ID_PAID"),
        F.col("lnk_AgntData_Out.COMSN_ARGMT_ID").alias("COMSN_ARGMT_ID"),
        F.col("lnk_AgntData_Out.ADJ_DESC").alias("COMSN_MNL_ADJ_DESC"),
        F.col("lnk_AgntData_Out.COMSN_MNL_ADJ_LOB_CD_SK").alias("COMSN_MNL_ADJ_LOB_CD_SK"),
        F.col("lnk_AgntData_Out.COMSN_MNL_ADJ_PAYMT_TYP_CD_SK").alias("COMSN_MNL_ADJ_PAYMT_TYP_CD_SK"),
        F.col("lnk_AgntData_Out.COMSN_MNL_ADJ_RSN_CD_SK").alias("COMSN_MNL_ADJ_RSN_CD_SK"),
        F.col("lnk_AgntData_Out.COMSN_MNL_ADJ_STTUS_CD_SK").alias("COMSN_MNL_ADJ_STTUS_CD_SK")
    )
)

# xfrm_BusinessLogic (CTransformerStage)
# Link: lnk_IdsEdwComsnMnladjMaiData_Out
df_xfrm_BusinessLogic_main = (
    df_lkp_Codes
    .filter(
        (F.col("COMSN_MNL_ADJ_SK") != 0) & (F.col("COMSN_MNL_ADJ_SK") != 1)
    )
    .select(
        F.col("COMSN_MNL_ADJ_SK").alias("COMSN_MNL_ADJ_SK"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("AGNT_ID").alias("AGNT_ID"),
        F.col("COMSN_MNL_ADJ_SEQ_NO").alias("COMSN_MNL_ADJ_SEQ_NO"),
        F.col("COMSN_MNL_ADJ_PD_DT_SK").alias("COMSN_MNL_ADJ_PD_DT_SK"),
        F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("AGNT_SK").alias("AGNT_SK"),
        F.col("PD_AGNT_SK").alias("PD_AGNT_SK"),
        F.col("COMSN_MNL_ADJ_LOB_CD").alias("COMSN_MNL_ADJ_LOB_CD"),
        F.col("COMSN_MNL_ADJ_LOB_NM").alias("COMSN_MNL_ADJ_LOB_NM"),
        F.col("COMSN_MNL_ADJ_PAYMT_TYP_CD").alias("COMSN_MNL_ADJ_PAYMT_TYP_CD"),
        F.col("COMSN_MNL_ADJ_PAYMT_TYP_NM").alias("COMSN_MNL_ADJ_PAYMT_TYP_NM"),
        F.col("COMSN_MNL_ADJ_RSN_CD").alias("COMSN_MNL_ADJ_RSN_CD"),
        F.col("COMSN_MNL_ADJ_RSN_NM").alias("COMSN_MNL_ADJ_RSN_NM"),
        F.col("COMSN_MNL_ADJ_STTUS_CD").alias("COMSN_MNL_ADJ_STTUS_CD"),
        F.col("COMSN_MNL_ADJ_STTUS_NM").alias("COMSN_MNL_ADJ_STTUS_NM"),
        F.when(
            (trim(F.col("COMSN_MNL_ADJ_RSN_CD")) == F.lit("BON")) &
            (F.substring(F.col("COMSN_MNL_ADJ_DESC"),1,24) == F.lit("BlueMax Bonus Payment - ")),
            F.lit("Y")
        ).otherwise(F.lit("N")).alias("BM_IN"),
        F.col("COMSN_AGMNT_EFF_DT_SK").alias("COMSN_AGMNT_EFF_DT_SK"),
        F.col("COMSN_MNL_ADJ_AMT").alias("COMSN_MNL_ADJ_AMT"),
        F.col("COMSN_ARGMT_ID").alias("COMSN_ARGMT_ID"),
        F.col("COMSN_MNL_ADJ_DESC").alias("COMSN_MNL_ADJ_DESC"),
        F.col("AGNT_ID_PAID").alias("PD_AGNT_ID"),
        F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("COMSN_MNL_ADJ_LOB_CD_SK").alias("COMSN_MNL_ADJ_LOB_CD_SK"),
        F.col("COMSN_MNL_ADJ_PAYMT_TYP_CD_SK").alias("COMSN_MNL_ADJ_PAYMT_TYP_CD_SK"),
        F.col("COMSN_MNL_ADJ_RSN_CD_SK").alias("COMSN_MNL_ADJ_RSN_CD_SK"),
        F.col("COMSN_MNL_ADJ_STTUS_CD_SK").alias("COMSN_MNL_ADJ_STTUS_CD_SK")
    )
)

# Link: NA => single-row with literal expressions
df_xfrm_BusinessLogic_NA = spark.range(1).select(
    F.lit(1).alias("COMSN_MNL_ADJ_SK"),
    F.lit("NA").alias("SRC_SYS_CD"),
    F.lit("NA").alias("AGNT_ID"),
    F.lit(0).alias("COMSN_MNL_ADJ_SEQ_NO"),
    F.lit("1753-01-01").alias("COMSN_MNL_ADJ_PD_DT_SK"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(1).alias("AGNT_SK"),
    F.lit(1).alias("PD_AGNT_SK"),
    F.lit("NA").alias("COMSN_MNL_ADJ_LOB_CD"),
    F.lit("NA").alias("COMSN_MNL_ADJ_LOB_NM"),
    F.lit("NA").alias("COMSN_MNL_ADJ_PAYMT_TYP_CD"),
    F.lit("NA").alias("COMSN_MNL_ADJ_PAYMT_TYP_NM"),
    F.lit("NA").alias("COMSN_MNL_ADJ_RSN_CD"),
    F.lit("NA").alias("COMSN_MNL_ADJ_RSN_NM"),
    F.lit("NA").alias("COMSN_MNL_ADJ_STTUS_CD"),
    F.lit("NA").alias("COMSN_MNL_ADJ_STTUS_NM"),
    F.lit("N").alias("BM_IN"),
    F.lit("1753-01-01").alias("COMSN_AGMNT_EFF_DT_SK"),
    F.lit(0).alias("COMSN_MNL_ADJ_AMT"),
    F.lit("NA").alias("COMSN_ARGMT_ID"),
    F.lit("").alias("COMSN_MNL_ADJ_DESC"),
    F.lit("NA").alias("PD_AGNT_ID"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("COMSN_MNL_ADJ_LOB_CD_SK"),
    F.lit(1).alias("COMSN_MNL_ADJ_PAYMT_TYP_CD_SK"),
    F.lit(1).alias("COMSN_MNL_ADJ_RSN_CD_SK"),
    F.lit(1).alias("COMSN_MNL_ADJ_STTUS_CD_SK")
)

# Link: UNK => single-row with literal expressions
df_xfrm_BusinessLogic_UNK = spark.range(1).select(
    F.lit(0).alias("COMSN_MNL_ADJ_SK"),
    F.lit("UNK").alias("SRC_SYS_CD"),
    F.lit("UNK").alias("AGNT_ID"),
    F.lit(0).alias("COMSN_MNL_ADJ_SEQ_NO"),
    F.lit("1753-01-01").alias("COMSN_MNL_ADJ_PD_DT_SK"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(0).alias("AGNT_SK"),
    F.lit(0).alias("PD_AGNT_SK"),
    F.lit("UNK").alias("COMSN_MNL_ADJ_LOB_CD"),
    F.lit("UNK").alias("COMSN_MNL_ADJ_LOB_NM"),
    F.lit("UNK").alias("COMSN_MNL_ADJ_PAYMT_TYP_CD"),
    F.lit("UNK").alias("COMSN_MNL_ADJ_PAYMT_TYP_NM"),
    F.lit("UNK").alias("COMSN_MNL_ADJ_RSN_CD"),
    F.lit("UNK").alias("COMSN_MNL_ADJ_RSN_NM"),
    F.lit("UNK").alias("COMSN_MNL_ADJ_STTUS_CD"),
    F.lit("UNK").alias("COMSN_MNL_ADJ_STTUS_NM"),
    F.lit("N").alias("BM_IN"),
    F.lit("1753-01-01").alias("COMSN_AGMNT_EFF_DT_SK"),
    F.lit(0).alias("COMSN_MNL_ADJ_AMT"),
    F.lit("UNK").alias("COMSN_ARGMT_ID"),
    F.lit("").alias("COMSN_MNL_ADJ_DESC"),
    F.lit("UNK").alias("PD_AGNT_ID"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("COMSN_MNL_ADJ_LOB_CD_SK"),
    F.lit(0).alias("COMSN_MNL_ADJ_PAYMT_TYP_CD_SK"),
    F.lit(0).alias("COMSN_MNL_ADJ_RSN_CD_SK"),
    F.lit(0).alias("COMSN_MNL_ADJ_STTUS_CD_SK")
)

# fnl_NA_UNK (PxFunnel)
df_fnl_NA_UNK = (
    df_xfrm_BusinessLogic_main
    .unionByName(df_xfrm_BusinessLogic_NA)
    .unionByName(df_xfrm_BusinessLogic_UNK)
)

# seq_COMSN_MNL_ADJ_F_csv_load (PxSequentialFile)
# Before writing, apply rpad for all char/varchar columns with known lengths, then select in final column order
df_seq_COMSN_MNL_ADJ_F_csv_load = df_fnl_NA_UNK \
    .withColumn("COMSN_MNL_ADJ_PD_DT_SK", F.rpad(F.col("COMSN_MNL_ADJ_PD_DT_SK"), 10, " ")) \
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")) \
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")) \
    .withColumn("COMSN_AGMNT_EFF_DT_SK", F.rpad(F.col("COMSN_AGMNT_EFF_DT_SK"), 10, " ")) \
    .withColumn("BM_IN", F.rpad(F.col("BM_IN"), 1, " ")) \
    .select(
        "COMSN_MNL_ADJ_SK",
        "SRC_SYS_CD",
        "AGNT_ID",
        "COMSN_MNL_ADJ_SEQ_NO",
        "COMSN_MNL_ADJ_PD_DT_SK",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "AGNT_SK",
        "PD_AGNT_SK",
        "COMSN_MNL_ADJ_LOB_CD",
        "COMSN_MNL_ADJ_LOB_NM",
        "COMSN_MNL_ADJ_PAYMT_TYP_CD",
        "COMSN_MNL_ADJ_PAYMT_TYP_NM",
        "COMSN_MNL_ADJ_RSN_CD",
        "COMSN_MNL_ADJ_RSN_NM",
        "COMSN_MNL_ADJ_STTUS_CD",
        "COMSN_MNL_ADJ_STTUS_NM",
        "BM_IN",
        "COMSN_AGMNT_EFF_DT_SK",
        "COMSN_MNL_ADJ_AMT",
        "COMSN_ARGMT_ID",
        "COMSN_MNL_ADJ_DESC",
        "PD_AGNT_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "COMSN_MNL_ADJ_LOB_CD_SK",
        "COMSN_MNL_ADJ_PAYMT_TYP_CD_SK",
        "COMSN_MNL_ADJ_RSN_CD_SK",
        "COMSN_MNL_ADJ_STTUS_CD_SK"
    )

write_files(
    df_seq_COMSN_MNL_ADJ_F_csv_load,
    f"{adls_path}/load/COMSN_MNL_ADJ_F.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)