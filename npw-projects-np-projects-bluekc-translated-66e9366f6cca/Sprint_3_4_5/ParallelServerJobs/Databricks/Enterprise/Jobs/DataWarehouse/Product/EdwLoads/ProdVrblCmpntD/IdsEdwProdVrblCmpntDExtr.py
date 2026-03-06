# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC JOB NAME:     EdwProdVrblCmpntDExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC 
# MAGIC   Pulls data from IDS table PROD_VRBL_CMPNT into EDW table PROD_VRBL_CMPNT_D
# MAGIC       
# MAGIC INPUTS:  PROD_VRBL_CMPNT
# MAGIC 
# MAGIC TRANSFORMS: TRIM all the fields that are extracted or used in lookups
# MAGIC                            
# MAGIC PROCESSING: IDS - read from source, lookup all code SK values from CodeMapping hash file and get the natural codes
# MAGIC   
# MAGIC OUTPUTS:  Loading into a sequential file
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             Parikshith Chada   11/22/2006  ---    Originally Programmed
# MAGIC 
# MAGIC 
# MAGIC Developer                Date                 	Project/Altiris #	                 Change Description	         Development Project	Code Reviewer	Date Reviewed       
# MAGIC ------------------              --------------------     	------------------------	                 ---------------------------------------------------     --------------------------------	-------------------------------	----------------------------  
# MAGIC Parikshith Chada     11/22/2006  ---        Originally Programmed
# MAGIC      
# MAGIC Pete Cundy              2009-09-16	3556 Change Data Capture 	 Modified SQL                                   devlIDSnew                    Steph Goddard       09/23/2009
# MAGIC 
# MAGIC Srikanth Mettpalli      05/30/2013             5114                                        Original Programming                       EnterpriseWrhsDevl        Jag Yelavarthi         2013-08-11
# MAGIC                                                                                                                  (Server to Parallel Conversion)

# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC CD_MPPNG_SK   PVC_TYP_CD_SK
# MAGIC PVC_PRI_LOB_RESP_CD_SK
# MAGIC Write PROD_VRBL_CMPNT Data into a Sequential file for Load Job IdsEdwProdVrblCmpntDLoad.
# MAGIC Read all the Data from IDS PROD_VRBL_CMPNT Table; Pull records based on LAST_UPDT_RUN_CYC_EXCTN_SK
# MAGIC Add Defaults and Null Handling.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Job Name: IdsEdwProdVrblCmpntDExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, when, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
IDSRunCycle = get_widget_value('IDSRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

sql_db2_PROD_VRBL_CMPNT_in = f"""
SELECT
PROD_VRBL_CMPNT.PVC_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
PROD_VRBL_CMPNT.PROD_ID,
PROD_VRBL_CMPNT.TIER_NO,
PROD_VRBL_CMPNT.PVC_TYP_CD_SK,
PROD_VRBL_CMPNT.EFF_DT_SK,
PROD_VRBL_CMPNT.SEQ_NO,
PROD_VRBL_CMPNT.CRT_RUN_CYC_EXCTN_SK,
PROD_VRBL_CMPNT.LAST_UPDT_RUN_CYC_EXCTN_SK,
PROD_VRBL_CMPNT.PROD_SK,
PROD_VRBL_CMPNT.PVC_PRI_LOB_RESP_CD_SK,
PROD_VRBL_CMPNT.TERM_DT_SK,
PROD_VRBL_CMPNT.IN_NTWK_PROV_BNF_LVL_IN,
PROD_VRBL_CMPNT.NONPAR_PROV_BNF_LVL_IN,
PROD_VRBL_CMPNT.PAR_PROV_BNF_LVL_IN,
PROD_VRBL_CMPNT.PREAUTH_NOT_RQRD_IN,
PROD_VRBL_CMPNT.PREAUTH_OBTN_IN,
PROD_VRBL_CMPNT.PREAUTH_VLTN_IN,
PROD_VRBL_CMPNT.PCP_BNF_LVL_IN,
PROD_VRBL_CMPNT.RFRL_NOT_RQRD_IN,
PROD_VRBL_CMPNT.RFRL_OBTN_IN,
PROD_VRBL_CMPNT.RFRL_VLTN_IN,
PROD_VRBL_CMPNT.DEDCT_CMPNT_ID,
PROD_VRBL_CMPNT.DNTL_CAT_PAYMT_CMPNT_ID,
PROD_VRBL_CMPNT.DNTL_PROC_PAYMT_CMPNT_ID,
PROD_VRBL_CMPNT.LMT_CMPNT_ID,
PROD_VRBL_CMPNT.SVC_PAYMT_CMPNT_ID
FROM {IDSOwner}.PROD_VRBL_CMPNT PROD_VRBL_CMPNT
LEFT JOIN {IDSOwner}.CD_MPPNG CD
ON PROD_VRBL_CMPNT.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
WHERE PROD_VRBL_CMPNT.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSRunCycle}
"""

df_db2_PROD_VRBL_CMPNT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", sql_db2_PROD_VRBL_CMPNT_in)
    .load()
)

sql_db2_CD_MPPNG_in = f"""
SELECT
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""

df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", sql_db2_CD_MPPNG_in)
    .load()
)

df_cpy_cd_mppng = df_db2_CD_MPPNG_in.select(
    col("CD_MPPNG_SK"),
    col("TRGT_CD"),
    col("TRGT_CD_NM")
)

df_Ref_PvcTypeCdLkup = df_cpy_cd_mppng.select(
    "CD_MPPNG_SK",
    "TRGT_CD",
    "TRGT_CD_NM"
)

df_Ref_PvcPriLobRespCdLkup = df_cpy_cd_mppng.select(
    "CD_MPPNG_SK",
    "TRGT_CD",
    "TRGT_CD_NM"
)

df_lkp_Codes = (
    df_db2_PROD_VRBL_CMPNT_in.alias("inABC")
    .join(
        df_Ref_PvcTypeCdLkup.alias("Ref_PvcTypeCdLkup"),
        col("inABC.PVC_TYP_CD_SK") == col("Ref_PvcTypeCdLkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Ref_PvcPriLobRespCdLkup.alias("Ref_PvcPriLobRespCdLkup"),
        col("inABC.PVC_PRI_LOB_RESP_CD_SK") == col("Ref_PvcPriLobRespCdLkup.CD_MPPNG_SK"),
        "left"
    )
    .select(
        col("inABC.PVC_SK"),
        col("inABC.SRC_SYS_CD"),
        col("inABC.PROD_ID"),
        col("inABC.TIER_NO").alias("PVC_TIER_NO"),
        col("Ref_PvcTypeCdLkup.TRGT_CD").alias("PVC_TYP_CD"),
        col("inABC.EFF_DT_SK").alias("PVC_EFF_DT_SK"),
        col("inABC.SEQ_NO").alias("PVC_SEQ_NO"),
        col("inABC.PROD_SK"),
        col("inABC.DEDCT_CMPNT_ID").alias("PVC_DEDCT_CMPNT_ID"),
        col("inABC.DNTL_CAT_PAYMT_CMPNT_ID").alias("PVC_DNTL_CAT_PAYMT_CMPNT_ID"),
        col("inABC.DNTL_PROC_PAYMT_CMPNT_ID").alias("PVC_DNTL_PROC_PAYMT_CMPNT_ID"),
        col("inABC.IN_NTWK_PROV_BNF_LVL_IN").alias("PVC_IN_NTWK_PROV_BNF_LVL_IN"),
        col("inABC.LMT_CMPNT_ID").alias("PVC_LMT_CMPNT_ID"),
        col("inABC.NONPAR_PROV_BNF_LVL_IN").alias("PVC_NONPAR_PROV_BNF_LVL_IN"),
        col("inABC.PAR_PROV_BNF_LVL_IN").alias("PVC_PAR_PROV_BNF_LVL_IN"),
        col("inABC.PCP_BNF_LVL_IN").alias("PVC_PCP_BNF_LVL_IN"),
        col("inABC.PREAUTH_NOT_RQRD_IN").alias("PVC_PREAUTH_NOT_RQRD_IN"),
        col("inABC.PREAUTH_OBTN_IN").alias("PVC_PREAUTH_OBTN_IN"),
        col("inABC.PREAUTH_VLTN_IN").alias("PVC_PREAUTH_VLTN_IN"),
        col("Ref_PvcPriLobRespCdLkup.TRGT_CD").alias("PVC_PRI_LOB_RESP_CD"),
        col("Ref_PvcPriLobRespCdLkup.TRGT_CD_NM").alias("PVC_PRI_LOB_RESP_NM"),
        col("inABC.RFRL_NOT_RQRD_IN").alias("PVC_RFRL_NOT_RQRD_IN"),
        col("inABC.RFRL_OBTN_IN").alias("PVC_RFRL_OBTN_IN"),
        col("inABC.RFRL_VLTN_IN").alias("PVC_RFRL_VLTN_IN"),
        col("inABC.SVC_PAYMT_CMPNT_ID").alias("PVC_SVC_PAYMT_CMPNT_ID"),
        col("inABC.TERM_DT_SK").alias("PVC_TERM_DT_SK"),
        col("Ref_PvcTypeCdLkup.TRGT_CD_NM").alias("PVC_TYP_NM"),
        col("inABC.PVC_PRI_LOB_RESP_CD_SK"),
        col("inABC.PVC_TYP_CD_SK")
    )
)

df_xfrm_BusinessLogic = (
    df_lkp_Codes
    .withColumn("SRC_SYS_CD", when(trim(col("SRC_SYS_CD")) == '', lit('UNK')).otherwise(col("SRC_SYS_CD")))
    .withColumn("PVC_TYP_CD", when(trim(col("PVC_TYP_CD")) == '', lit('UNK')).otherwise(col("PVC_TYP_CD")))
    .withColumn("PVC_PRI_LOB_RESP_CD", when(trim(col("PVC_PRI_LOB_RESP_CD")) == '', lit('UNK')).otherwise(col("PVC_PRI_LOB_RESP_CD")))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", lit(EDWRunCycleDate))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", lit(EDWRunCycleDate))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(EDWRunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(EDWRunCycle))
)

df_final = df_xfrm_BusinessLogic.select(
    col("PVC_SK"),
    col("SRC_SYS_CD"),
    rpad(col("PROD_ID"), 8, " ").alias("PROD_ID"),
    col("PVC_TIER_NO"),
    col("PVC_TYP_CD"),
    rpad(col("PVC_EFF_DT_SK"), 10, " ").alias("PVC_EFF_DT_SK"),
    col("PVC_SEQ_NO"),
    rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("PROD_SK"),
    col("PVC_DEDCT_CMPNT_ID"),
    col("PVC_DNTL_CAT_PAYMT_CMPNT_ID"),
    col("PVC_DNTL_PROC_PAYMT_CMPNT_ID"),
    rpad(col("PVC_IN_NTWK_PROV_BNF_LVL_IN"), 1, " ").alias("PVC_IN_NTWK_PROV_BNF_LVL_IN"),
    col("PVC_LMT_CMPNT_ID"),
    rpad(col("PVC_NONPAR_PROV_BNF_LVL_IN"), 1, " ").alias("PVC_NONPAR_PROV_BNF_LVL_IN"),
    rpad(col("PVC_PAR_PROV_BNF_LVL_IN"), 1, " ").alias("PVC_PAR_PROV_BNF_LVL_IN"),
    rpad(col("PVC_PCP_BNF_LVL_IN"), 1, " ").alias("PVC_PCP_BNF_LVL_IN"),
    rpad(col("PVC_PREAUTH_NOT_RQRD_IN"), 1, " ").alias("PVC_PREAUTH_NOT_RQRD_IN"),
    rpad(col("PVC_PREAUTH_OBTN_IN"), 1, " ").alias("PVC_PREAUTH_OBTN_IN"),
    rpad(col("PVC_PREAUTH_VLTN_IN"), 1, " ").alias("PVC_PREAUTH_VLTN_IN"),
    col("PVC_PRI_LOB_RESP_CD"),
    col("PVC_PRI_LOB_RESP_NM"),
    rpad(col("PVC_RFRL_NOT_RQRD_IN"), 1, " ").alias("PVC_RFRL_NOT_RQRD_IN"),
    rpad(col("PVC_RFRL_OBTN_IN"), 1, " ").alias("PVC_RFRL_OBTN_IN"),
    rpad(col("PVC_RFRL_VLTN_IN"), 1, " ").alias("PVC_RFRL_VLTN_IN"),
    col("PVC_SVC_PAYMT_CMPNT_ID"),
    rpad(col("PVC_TERM_DT_SK"), 10, " ").alias("PVC_TERM_DT_SK"),
    col("PVC_TYP_NM"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("PVC_PRI_LOB_RESP_CD_SK"),
    col("PVC_TYP_CD_SK")
)

write_files(
    df_final,
    f"{adls_path}/load/PROD_VRBL_CMPNT_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)