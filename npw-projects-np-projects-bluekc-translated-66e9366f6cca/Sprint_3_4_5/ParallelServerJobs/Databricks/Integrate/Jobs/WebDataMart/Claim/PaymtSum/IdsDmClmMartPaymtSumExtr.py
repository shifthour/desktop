# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsClmMartPaySumExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from IDS to create claim data mart
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	IDS - CLM_REMIT_HIST
# MAGIC                 IDS - PAYMT_SUM           
# MAGIC                 IDS - W_WEBDM_ETL_DRVR
# MAGIC   
# MAGIC HASH FILES:
# MAGIC                 hf_clm_cd_mppng - hash file for sk lookup
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
# MAGIC                    direct update of CLM_DM_PAYMT_SUM
# MAGIC 
# MAGIC 
# MAGIC      
# MAGIC               Hugh Sisson     01/03/2007    Moved PAYMT_SUM_LOB_CD to be part of the natural key
# MAGIC               
# MAGIC 
# MAGIC Developer                          Date                    Project/Altiris #                     Change Description                                                       Development Project               Code Reviewer                      Date Reviewed
# MAGIC ---------------------------------      -------------------      -----------------------------------           ---------------------------------------------------------                                 ----------------------------------              ---------------------------------               -------------------------
# MAGIC Ralph Tucker                03/23/2005                                                      Originally Programmed
# MAGIC 
# MAGIC BJ Luce                         08/29/2005                                                     change SQL parameters to ClmMart for job sequence
# MAGIC 
# MAGIC Brent Leland                  04/03/2006                                                     Changed to use evironment parameters
# MAGIC   
# MAGIC                                                                                                               Changed name of driver table
# MAGIC 
# MAGIC Brent Leland                  04/06/2006                                                     Added run cycle parameter for new table field 
# MAGIC                                                                                                               LAST_UPDT_RUN_CYC_NO
# MAGIC 
# MAGIC Hugh Sisson                  01/03/2007                                                     Moved PAYMT_SUM_LOB_CD to be part of the natural key
# MAGIC   
# MAGIC Nagesh Bandi                09/27/2013              5114                                 Original Programming(Server to Parallel)                             IntegrateWrhsDevl

# MAGIC Write CLM_DM_PAYMT_SUM Data into a Sequential file for Load Job IdsDmClmMartPaymtSumLoad.
# MAGIC Read all the Data from IDS PAYMT_SUM Table;
# MAGIC Add Defaults and Null Handling.
# MAGIC Job Name: 
# MAGIC 
# MAGIC IdsDmClmMartPaymtSumExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, when, to_timestamp, lit, rpad, trim
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------

jdbc_url = None
jdbc_props = None

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_db2_CLM_LN_Extr = """SELECT distinct 
 COALESCE(cd.TRGT_CD, 'UNK') AS SRC_SYS_CD,
 ps.PAYMT_REF_ID,
 ps.PAYMT_SUM_LOB_CD_SK,
 pr.PROV_ID,
 ps.PAYMT_SUM_PAYE_TYP_CD_SK,
 ps.PAYMT_SUM_PAYMT_TYP_CD_SK,
 ps.PAYMT_SUM_TYP_CD_SK,
 ps.COMBND_CLM_PAYMT_IN,
 ps.PD_DT_SK,
 ps.PERD_END_DT_SK,
 ps.DEDCT_AMT,
 ps.NET_AMT,
 ps.ORIG_SUM_AMT,
 ps.CUR_CHK_SEQ_NO 
FROM {0}.CLM_CHK cc
JOIN {0}.W_WEBDM_ETL_DRVR ex 
    ON cc.SRC_SYS_CD_SK = ex.SRC_SYS_CD_SK AND cc.CLM_ID = ex.CLM_ID
JOIN {0}.PAYMT_SUM ps 
    ON cc.CHK_PAYMT_REF_ID = ps.PAYMT_REF_ID
JOIN {0}.PROV pr 
    ON ps.PD_PROV_SK = pr.PROV_SK
LEFT JOIN {0}.CD_MPPNG cd 
    ON ex.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
""".format(IDSOwner)
df_db2_CLM_LN_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CLM_LN_Extr)
    .load()
)

extract_query_db2_CD_MPPNG_in = """SELECT 
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM {0}.CD_MPPNG  
""".format(IDSOwner)
df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG_in)
    .load()
)

df_Cpy_Cd_Mppng = df_db2_CD_MPPNG_in

df_retTypCd = df_Cpy_Cd_Mppng.alias("retTypCd")
df_retPaymtTypCd = df_Cpy_Cd_Mppng.alias("retPaymtTypCd")
df_retLob = df_Cpy_Cd_Mppng.alias("retLob")
df_retPayeTypCd = df_Cpy_Cd_Mppng.alias("retPayeTypCd")

df_lkp_Codes = df_db2_CLM_LN_Extr.alias("lnk_IdsDmClmMartPaymtSumExtr_InABC") \
    .join(
        df_retPaymtTypCd,
        col("lnk_IdsDmClmMartPaymtSumExtr_InABC.PAYMT_SUM_PAYMT_TYP_CD_SK") == col("retPaymtTypCd.CD_MPPNG_SK"),
        "left"
    ) \
    .join(
        df_retLob,
        col("lnk_IdsDmClmMartPaymtSumExtr_InABC.PAYMT_SUM_LOB_CD_SK") == col("retLob.CD_MPPNG_SK"),
        "left"
    ) \
    .join(
        df_retTypCd,
        col("lnk_IdsDmClmMartPaymtSumExtr_InABC.PAYMT_SUM_TYP_CD_SK") == col("retTypCd.CD_MPPNG_SK"),
        "left"
    ) \
    .join(
        df_retPayeTypCd,
        col("lnk_IdsDmClmMartPaymtSumExtr_InABC.PAYMT_SUM_PAYE_TYP_CD_SK") == col("retPayeTypCd.CD_MPPNG_SK"),
        "left"
    ) \
    .select(
        col("lnk_IdsDmClmMartPaymtSumExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnk_IdsDmClmMartPaymtSumExtr_InABC.PAYMT_REF_ID").alias("PAYMT_REF_ID"),
        col("lnk_IdsDmClmMartPaymtSumExtr_InABC.PROV_ID").alias("PROV_ID"),
        col("lnk_IdsDmClmMartPaymtSumExtr_InABC.COMBND_CLM_PAYMT_IN").alias("COMBND_CLM_PAYMT_IN"),
        col("lnk_IdsDmClmMartPaymtSumExtr_InABC.PD_DT_SK").alias("PD_DT_SK"),
        col("lnk_IdsDmClmMartPaymtSumExtr_InABC.PERD_END_DT_SK").alias("PERD_END_DT_SK"),
        col("lnk_IdsDmClmMartPaymtSumExtr_InABC.DEDCT_AMT").alias("DEDCT_AMT"),
        col("lnk_IdsDmClmMartPaymtSumExtr_InABC.NET_AMT").alias("NET_AMT"),
        col("lnk_IdsDmClmMartPaymtSumExtr_InABC.ORIG_SUM_AMT").alias("ORIG_SUM_AMT"),
        col("lnk_IdsDmClmMartPaymtSumExtr_InABC.CUR_CHK_SEQ_NO").alias("CUR_CHK_SEQ_NO"),
        col("retPaymtTypCd.TRGT_CD").alias("PAYMT_SUM_PAYMT_TYP_CD"),
        col("retLob.TRGT_CD").alias("PAYMT_SUM_LOB_CD"),
        col("retTypCd.TRGT_CD").alias("PAYMT_SUM_TYP_CD"),
        col("retPayeTypCd.TRGT_CD").alias("PAYMT_SUM_PAYE_TYP_CD")
    )

df_xfrm_BusinessLogic = df_lkp_Codes \
    .withColumn(
        "SRC_SYS_CD",
        when(trim(col("SRC_SYS_CD")) == "", "UNK").otherwise(col("SRC_SYS_CD"))
    ).withColumn(
        "PAYMT_SUM_LOB_CD",
        when(trim(col("PAYMT_SUM_LOB_CD")) == "", "UNK").otherwise(col("PAYMT_SUM_LOB_CD"))
    ).withColumn(
        "PAYMT_SUM_PAYE_TYP_CD",
        when(trim(col("PAYMT_SUM_PAYE_TYP_CD")) == "", "UNK").otherwise(col("PAYMT_SUM_PAYE_TYP_CD"))
    ).withColumn(
        "PAYMT_SUM_PAYMT_TYP_CD",
        when(trim(col("PAYMT_SUM_PAYMT_TYP_CD")) == "", "UNK").otherwise(col("PAYMT_SUM_PAYMT_TYP_CD"))
    ).withColumn(
        "PAYMT_SUM_TYP_CD",
        when(trim(col("PAYMT_SUM_TYP_CD")) == "", "UNK").otherwise(col("PAYMT_SUM_TYP_CD"))
    ).withColumn(
        "PD_DT",
        when(
            (trim(col("PD_DT_SK")) == "") | (trim(col("PD_DT_SK")) == "NA") | (trim(col("PD_DT_SK")) == "UNK"),
            lit(None)
        ).otherwise(to_timestamp(col("PD_DT_SK"), "yyyy-MM-dd"))
    ).withColumn(
        "PERD_END_DT",
        when(
            (trim(col("PERD_END_DT_SK")) == "") | (trim(col("PERD_END_DT_SK")) == "NA") | (trim(col("PERD_END_DT_SK")) == "UNK"),
            lit("2199-12-31 23:23:59.999").cast("timestamp")
        ).otherwise(to_timestamp(col("PERD_END_DT_SK"), "yyyy-MM-dd"))
    ).withColumn(
        "PROV_PD_PROV_ID",
        col("PROV_ID")
    ).withColumn(
        "PAYMT_SUM_COMBND_CLM_PAYMT_IN",
        col("COMBND_CLM_PAYMT_IN")
    ).withColumn(
        "LAST_UPDT_RUN_CYC_NO",
        lit(CurrRunCycle)
    )

df_final = df_xfrm_BusinessLogic.select(
    col("SRC_SYS_CD"),
    col("PAYMT_REF_ID"),
    col("PAYMT_SUM_LOB_CD"),
    col("PROV_PD_PROV_ID"),
    col("PAYMT_SUM_PAYE_TYP_CD"),
    col("PAYMT_SUM_PAYMT_TYP_CD"),
    col("PAYMT_SUM_TYP_CD"),
    rpad(col("PAYMT_SUM_COMBND_CLM_PAYMT_IN"), 1, " ").alias("PAYMT_SUM_COMBND_CLM_PAYMT_IN"),
    col("PD_DT"),
    col("PERD_END_DT"),
    col("DEDCT_AMT"),
    col("NET_AMT"),
    col("ORIG_SUM_AMT"),
    col("CUR_CHK_SEQ_NO"),
    col("LAST_UPDT_RUN_CYC_NO")
)

write_files(
    df_final,
    f"{adls_path}/load/CLM_DM_PAYMT_SUM.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)