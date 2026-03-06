# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC Â© Copyright 2007 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  Fcts claim line savings sequencer
# MAGIC                
# MAGIC 
# MAGIC PROCESSING:   Extracts data from IDS claim, claim line, and claim line disallow tables to create a claim line savings table.  This program will create a driver table with information
# MAGIC                            from the IDS tables.  This extract must be run after the claim tables are loaded and in the Facets claim extract process, as the data on this table will end up in EDW.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Steph Goddard         4/25/2007       HDMS                   New Program                                                                              devlIDS30                       Brent Leland              05/15/2007                                            
# MAGIC                         
# MAGIC Manasa Andru          2014/05/12     TFS - 1271            Added logic in the Extract SQL to extract only                            IntegrateNewDevl            Kalyan Neelam           2014-05-16
# MAGIC                                                                                  ACPTD values for the CLM_LN_FINL_DISP_CD_SK field.
# MAGIC Shanmugam A \(9) 2017-03-02         5321               SQL in stage 'IDS' will be aliased to match with stage meta data      IntegrateDev2                 Jag Yelavarthi           2017-03-07
# MAGIC \(9)

# MAGIC This program extracts records from IDS to create a driver table of records which will create CLM_LN_SAV records..!!!
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = """
SELECT DISTINCT CLM.CLM_SK,
       CLM.SRC_SYS_CD_SK,
       CLM.CLM_FINL_DISP_CD_SK,
       CLM.CLM_STTUS_CD_SK,
       CLM.CLM_TYP_CD_SK, 
       CLM.PD_DT_SK,
       CLM_LN.CLM_LN_SK,
       CLM_LN.CLM_LN_FINL_DISP_CD_SK,
       CLM_LN.CAP_LN_IN,
       CLM_LN_DSALW.CLM_LN_DSALW_SK,
       CLM_LN_DSALW.CLM_ID,
       CLM_LN_DSALW.CLM_LN_SEQ_NO,
       CLM_LN_DSALW.CLM_LN_DSALW_EXCD_SK,
       CLM_LN_DSALW.DSALW_AMT AS DSALW_DSALW_AMT,
       CLM_LN_DSALW.CLM_LN_DSALW_TYP_CAT_CD_SK,
       DSALW_EXCD.BYPS_IN,
       DSALW_EXCD.EXCD_ID,
       CLM_LN_DSALW.CLM_LN_DSALW_TYP_CD_SK,
       DSALW_EXCD.EXCD_RESP_CD_SK,
       CLM.CLM_NTWK_STTUS_CD_SK,
       CLM_LN.DSALW_AMT AS DSALW_AMT,
       CD_STTUS.TRGT_CD AS STATUS_SRC_CD,
       CD_CLM_TYP.TRGT_CD AS TYP_SRC_CD
FROM #$IDSOwner#.CLM CLM, 
     #$IDSOwner#.CLM_LN CLM_LN, 
     #$IDSOwner#.CLM_LN_DSALW CLM_LN_DSALW,
     #$IDSOwner#.DSALW_EXCD DSALW_EXCD,
     #$IDSOwner#.CD_MPPNG CD_SRC_SYS,
     #$IDSOwner#.CD_MPPNG CD_FINL_DISP,
     #$IDSOwner#.CD_MPPNG CD_STTUS,
     #$IDSOwner#.CD_MPPNG CD_CLM_TYP 
WHERE CLM.LAST_UPDT_RUN_CYC_EXCTN_SK = #ExtrRunCycle# and 
      CLM.CLM_SK = CLM_LN.CLM_SK and 
      CLM_LN.CLM_LN_SK = CLM_LN_DSALW.CLM_LN_SK and
      CLM_LN_DSALW.CLM_LN_DSALW_EXCD_SK = DSALW_EXCD.EXCD_SK and
      DSALW_EXCD.EFF_DT_SK <= CLM.PD_DT_SK and
      CLM.PD_DT_SK <= DSALW_EXCD.TERM_DT_SK and
      CLM.SRC_SYS_CD_SK = CD_SRC_SYS.CD_MPPNG_SK and
      CD_SRC_SYS.TRGT_CD = 'FACETS' and
      CLM.CLM_FINL_DISP_CD_SK = CD_FINL_DISP.CD_MPPNG_SK and
      CD_FINL_DISP.TRGT_CD = 'ACPTD' and
      CLM.CLM_TYP_CD_SK = CD_CLM_TYP.CD_MPPNG_SK and
      CD_CLM_TYP.TRGT_CD in ('MED','DNTL') and
      CLM.CLM_STTUS_CD_SK = CD_STTUS.CD_MPPNG_SK and
      CD_STTUS.TRGT_CD in ('A02','A08','A09') and
      CLM_LN.CLM_LN_FINL_DISP_CD_SK = CD_FINL_DISP.CD_MPPNG_SK and
      CD_FINL_DISP.TRGT_CD = 'ACPTD'
"""

df_IDS_extract_data = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

codes_query = """
SELECT CD_MPPNG.CD_MPPNG_SK as CD_MPPNG_SK,
       CD_MPPNG.SRC_CD as SRC_CD,
       CD_MPPNG.SRC_CD_NM as SRC_CD_NM,
       CD_MPPNG.TRGT_CD as TRGT_CD
FROM #$IDSOwner#.CD_MPPNG CD_MPPNG
"""

df_IDS_codes = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", codes_query)
    .load()
)

df_hf_ids_clm_ln_sav_cd_mppng = dedup_sort(df_IDS_codes, ["CD_MPPNG_SK"], [])

df_transformer_1_joined = (
    df_IDS_extract_data.alias("extract_data")
    .join(
        df_hf_ids_clm_ln_sav_cd_mppng.alias("DsalwTypCdSrc"),
        F.col("extract_data.CLM_LN_DSALW_TYP_CD_SK") == F.col("DsalwTypCdSrc.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_hf_ids_clm_ln_sav_cd_mppng.alias("RespCdSrc"),
        F.col("extract_data.EXCD_RESP_CD_SK") == F.col("RespCdSrc.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_hf_ids_clm_ln_sav_cd_mppng.alias("NtwkSttusCdSrc"),
        F.col("extract_data.CLM_NTWK_STTUS_CD_SK") == F.col("NtwkSttusCdSrc.CD_MPPNG_SK"),
        "left"
    )
)

df_transformer_1 = (
    df_transformer_1_joined
    .withColumn(
        "NtwkStatSrcCd",
        F.when(F.col("NtwkSttusCdSrc.SRC_CD").isNull(), F.lit("UNK")).otherwise(F.col("NtwkSttusCdSrc.SRC_CD"))
    )
    .withColumn(
        "RespSrcCd",
        F.when(F.col("RespCdSrc.SRC_CD").isNull(), F.lit("UNK")).otherwise(F.col("RespCdSrc.SRC_CD"))
    )
)

df_W_CLM_LN_SAV_DRVR = df_transformer_1.select(
    F.col("extract_data.CLM_ID").alias("CLM_ID"),
    F.col("extract_data.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.when(F.col("DsalwTypCdSrc.SRC_CD").isNull(), F.lit("UNK")).otherwise(F.col("DsalwTypCdSrc.SRC_CD")).alias("CLM_LN_DSALW_TYP_SRC_CD"),
    F.rpad(F.col("extract_data.PD_DT_SK"), 10, " ").alias("PD_DT_SK"),
    F.lit(" ").alias("CLM_FINL_DISP_CD"),
    F.col("extract_data.CLM_SK").alias("CLM_SK"),
    F.col("extract_data.CLM_LN_SK").alias("CLM_LN_SK"),
    F.lit(" ").alias("CLM_LN_FINL_DISP_CD"),
    F.col("extract_data.CLM_LN_DSALW_SK").alias("CLM_LN_DSALW_SK"),
    F.col("extract_data.DSALW_DSALW_AMT").alias("CLM_LN_DSALW_AMT"),
    F.when(F.col("DsalwTypCdSrc.TRGT_CD").isNull(), F.lit("UNK")).otherwise(F.col("DsalwTypCdSrc.TRGT_CD")).alias("CLM_LN_DSALW_TYP_TRGT_CD"),
    F.col("extract_data.EXCD_ID").alias("CLM_LN_DSALW_EXCD"),
    F.rpad(F.col("extract_data.BYPS_IN"), 1, " ").alias("BYPS_IN"),
    F.when(
        F.col("RespSrcCd").eqNullSafe("O"),
        F.when(
            F.col("NtwkStatSrcCd").isin("I", "P"),
            F.lit("P")
        ).otherwise(
            F.when(
                F.col("NtwkStatSrcCd").eqNullSafe("O"),
                F.lit("M")
            ).otherwise(F.lit("U"))
        )
    ).otherwise(F.col("RespSrcCd")).alias("RESP_SRC_CD"),
    F.lit(" ").alias("FINL_RESP_SRC_CD"),
    F.col("NtwkStatSrcCd").alias("CLM_NTWK_STTUS_SRC_CD"),
    F.col("extract_data.CLM_STTUS_CD_SK").alias("CLM_STTUS_CD_SK"),
    F.lit(" ").alias("CLS_CD"),
    F.col("extract_data.STATUS_SRC_CD").alias("CLM_STTUS_CD"),
    F.col("extract_data.TYP_SRC_CD").alias("CLM_TYP_CD"),
    F.rpad(F.col("extract_data.CAP_LN_IN"), 1, " ").alias("CAP_LN_IN")
)

write_files(
    df_W_CLM_LN_SAV_DRVR,
    f"{adls_path}/load/W_CLM_LN_SAV_DRVR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)