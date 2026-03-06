# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC Â© Copyright 2007 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     IdsClmMartExtrLoadSeq
# MAGIC 
# MAGIC Processing:
# MAGIC                     Extract and load claim line remit disallow data based on driver table W_WEBDM_ETL_DRVR
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                      
# MAGIC Developer                          Date                    Project/Altiris #                     Change Description                                                       Development Project               Code Reviewer                      Date Reviewed
# MAGIC ---------------------------------      -------------------      -----------------------------------           ---------------------------------------------------------                                 ----------------------------------              ---------------------------------               -------------------------
# MAGIC Parik                              08/04/2008            3057(Web claims)               Originally Programmed                                                      devlIDSnew                            Steph Goddard                        08/11/2008
# MAGIC   
# MAGIC Nagesh Bandi              09/10/2013              5114                                 Original Programming(Server to Parallel)                             IntegrateWrhsDevl       
# MAGIC 
# MAGIC Jag Yelavarthi              2014-04-01              TFS#8426                            Corrected derivation for EXCD_DESC                             IntegrateNewDevl                   Bhoomi Dasari                          4/3/2014

# MAGIC Write CLM_DM_CLM_LN_REMIT_DSALW Data into a Sequential file for Load Job IdsDmClmDmClmLnRemitDsalwLoad.
# MAGIC Read all the Data from IDS CLM_LN_ALT_CHRG_REMIT Table;
# MAGIC Add Defaults and Null Handling.
# MAGIC Job Name: 
# MAGIC 
# MAGIC IdsDmClmDmClmLnRemitDsalwExtr
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC 
# MAGIC CLM_LN_REMIT_DSALW_EXCD_SK
# MAGIC CLM_LN_DSALW_TYP_CD_SK
# MAGIC CLM_LN_RMT_DSW_EXCD_RESP_CD_SK
# MAGIC Code SK lookups to pull SUM(CLM_LN_REMIT_DSALW_AMT) for Min(CLM_LN_SEQ_NO)
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC 
# MAGIC SRC_SYS_CD,
# MAGIC CLM_ID,
# MAGIC CLM_LN_REMIT_DSALW_EXCD_SK,
# MAGIC CLM_LN_RMT_DSW_EXCD_RESP_CD_SK,
# MAGIC min (RemitDsalw.CLM_LN_SEQ_NO)
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

extract_query_db2_ExcdExtr = f"""
SELECT
  EXCD.EXCD_SK,
  EXCD.EXCD_ID,
  EXCD.EXCD_LONG_TX1,
  EXCD.EXCD_LONG_TX2
FROM {IDSOwner}.EXCD EXCD
"""
df_db2_ExcdExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_ExcdExtr)
    .load()
)

extract_query_db2_CLM_LN_REMIT_DSALW_PRoutput_Extr = f"""
SELECT
  cd.TRGT_CD AS SRC_SYS_CD,
  RemitDsalw.CLM_ID,
  RemitDsalw.CLM_LN_DSALW_TYP_CD_SK,
  RemitDsalw.CLM_LN_REMIT_DSALW_EXCD_SK,
  RemitDsalw.CLM_LN_RMT_DSW_EXCD_RESP_CD_SK,
  RemitDsalw.CLM_LN_SEQ_NO,
  RemitDsalw.REMIT_DSALW_AMT AS CLM_LN_REMIT_DSALW_AMT
FROM {IDSOwner}.CLM_LN_REMIT_DSALW RemitDsalw
JOIN {IDSOwner}.W_WEBDM_ETL_DRVR Extr
  ON Extr.SRC_SYS_CD_SK = RemitDsalw.SRC_SYS_CD_SK
  AND Extr.CLM_ID = RemitDsalw.CLM_ID
JOIN {IDSOwner}.CLM Clm
  ON RemitDsalw.CLM_ID = Clm.CLM_ID
JOIN {IDSOwner}.CD_MPPNG Mppng
  ON Clm.CLM_SUBTYP_CD_SK = Mppng.CD_MPPNG_SK
  AND Mppng.TRGT_CD = 'PR'
LEFT JOIN {IDSOwner}.CD_MPPNG cd
  ON RemitDsalw.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
"""
df_db2_CLM_LN_REMIT_DSALW_PRoutput_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CLM_LN_REMIT_DSALW_PRoutput_Extr)
    .load()
)

extract_query_db2_CD_MPPNG_in = f"""
SELECT
  CD_MPPNG_SK,
  COALESCE(TRGT_CD,'UNK') AS TRGT_CD,
  COALESCE(TRGT_CD_NM,'UNK') AS TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CD_MPPNG_in)
    .load()
)

df_Cpy_Cd_Mppng_refClmLnRmtDswExcdRespCd = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Cpy_Cd_Mppng_refClmLnDsalwTypCd = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

extract_query_db2_CLM_LN_REMIT_DSALW_Extr = f"""
SELECT
  cd.TRGT_CD AS SRC_SYS_CD,
  RemitDsalw.CLM_ID,
  RemitDsalw.CLM_LN_DSALW_TYP_CD_SK,
  RemitDsalw.CLM_LN_REMIT_DSALW_EXCD_SK,
  RemitDsalw.CLM_LN_RMT_DSW_EXCD_RESP_CD_SK,
  MIN(RemitDsalw.CLM_LN_SEQ_NO) AS CLM_LN_SEQ_NO
FROM {IDSOwner}.CLM_LN_REMIT_DSALW RemitDsalw
JOIN {IDSOwner}.W_WEBDM_ETL_DRVR Extr
  ON Extr.SRC_SYS_CD_SK = RemitDsalw.SRC_SYS_CD_SK
  AND Extr.CLM_ID = RemitDsalw.CLM_ID
JOIN {IDSOwner}.CLM Clm
  ON RemitDsalw.CLM_ID = Clm.CLM_ID
JOIN {IDSOwner}.CD_MPPNG Mppng
  ON Clm.CLM_SUBTYP_CD_SK = Mppng.CD_MPPNG_SK
  AND Mppng.TRGT_CD IN ('IP','OP')
LEFT JOIN {IDSOwner}.CD_MPPNG cd
  ON RemitDsalw.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
GROUP BY
  cd.TRGT_CD,
  RemitDsalw.CLM_ID,
  RemitDsalw.CLM_LN_DSALW_TYP_CD_SK,
  RemitDsalw.CLM_LN_REMIT_DSALW_EXCD_SK,
  RemitDsalw.CLM_LN_RMT_DSW_EXCD_RESP_CD_SK
"""
df_db2_CLM_LN_REMIT_DSALW_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CLM_LN_REMIT_DSALW_Extr)
    .load()
)

extract_query_db2_CLM_LN_REMIT_DSALW_min_Extr = f"""
SELECT
  cd.TRGT_CD AS SRC_SYS_CD,
  RemitDsalw.CLM_ID,
  RemitDsalw.CLM_LN_REMIT_DSALW_EXCD_SK,
  RemitDsalw.CLM_LN_RMT_DSW_EXCD_RESP_CD_SK,
  MIN(RemitDsalw.CLM_LN_SEQ_NO) AS CLM_LN_SEQ_NO,
  CAST(SUM(RemitDsalw.REMIT_DSALW_AMT) AS DECIMAL(13,2)) AS CLM_LN_REMIT_DSALW_AMT
FROM {IDSOwner}.CLM_LN_REMIT_DSALW RemitDsalw
JOIN {IDSOwner}.W_WEBDM_ETL_DRVR Extr
  ON Extr.SRC_SYS_CD_SK = RemitDsalw.SRC_SYS_CD_SK
  AND Extr.CLM_ID = RemitDsalw.CLM_ID
JOIN {IDSOwner}.CLM Clm
  ON RemitDsalw.CLM_ID = Clm.CLM_ID
JOIN {IDSOwner}.CD_MPPNG Mppng
  ON Clm.CLM_SUBTYP_CD_SK = Mppng.CD_MPPNG_SK
  AND Mppng.TRGT_CD IN ('IP','OP')
LEFT JOIN {IDSOwner}.CD_MPPNG cd
  ON RemitDsalw.SRC_SYS_CD_SK = cd.CD_MPPNG_SK
GROUP BY
  RemitDsalw.CLM_ID,
  RemitDsalw.CLM_LN_REMIT_DSALW_EXCD_SK,
  RemitDsalw.CLM_LN_RMT_DSW_EXCD_RESP_CD_SK,
  cd.TRGT_CD
"""
df_db2_CLM_LN_REMIT_DSALW_min_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CLM_LN_REMIT_DSALW_min_Extr)
    .load()
)

df_Remit_Dsalw_Amt_lkp = (
    df_db2_CLM_LN_REMIT_DSALW_Extr.alias("lnk_IdsDmClmDmClmLnRemitDsalwExtr_InABC")
    .join(
        df_db2_CLM_LN_REMIT_DSALW_min_Extr.alias("refRemitDsalwAmt_lkp"),
        [
            F.col("lnk_IdsDmClmDmClmLnRemitDsalwExtr_InABC.SRC_SYS_CD") == F.col("refRemitDsalwAmt_lkp.SRC_SYS_CD"),
            F.col("lnk_IdsDmClmDmClmLnRemitDsalwExtr_InABC.CLM_ID") == F.col("refRemitDsalwAmt_lkp.CLM_ID"),
            F.col("lnk_IdsDmClmDmClmLnRemitDsalwExtr_InABC.CLM_LN_REMIT_DSALW_EXCD_SK") == F.col("refRemitDsalwAmt_lkp.CLM_LN_REMIT_DSALW_EXCD_SK"),
            F.col("lnk_IdsDmClmDmClmLnRemitDsalwExtr_InABC.CLM_LN_RMT_DSW_EXCD_RESP_CD_SK") == F.col("refRemitDsalwAmt_lkp.CLM_LN_RMT_DSW_EXCD_RESP_CD_SK"),
            F.col("lnk_IdsDmClmDmClmLnRemitDsalwExtr_InABC.CLM_LN_SEQ_NO") == F.col("refRemitDsalwAmt_lkp.CLM_LN_SEQ_NO")
        ],
        how="left"
    )
    .select(
        F.col("lnk_IdsDmClmDmClmLnRemitDsalwExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_IdsDmClmDmClmLnRemitDsalwExtr_InABC.CLM_ID").alias("CLM_ID"),
        F.col("lnk_IdsDmClmDmClmLnRemitDsalwExtr_InABC.CLM_LN_DSALW_TYP_CD_SK").alias("CLM_LN_DSALW_TYP_CD_SK"),
        F.col("lnk_IdsDmClmDmClmLnRemitDsalwExtr_InABC.CLM_LN_REMIT_DSALW_EXCD_SK").alias("CLM_LN_REMIT_DSALW_EXCD_SK"),
        F.col("refRemitDsalwAmt_lkp.CLM_LN_RMT_DSW_EXCD_RESP_CD_SK").alias("CLM_LN_RMT_DSW_EXCD_RESP_CD_SK"),
        F.col("lnk_IdsDmClmDmClmLnRemitDsalwExtr_InABC.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("refRemitDsalwAmt_lkp.CLM_LN_REMIT_DSALW_AMT").alias("CLM_LN_REMIT_DSALW_AMT"),
        F.col("refRemitDsalwAmt_lkp.CLM_ID").alias("REMIT_CLM_ID_1")
    )
)

df_Copy_of_xfrm_BusinessLogic = (
    df_Remit_Dsalw_Amt_lkp
    .filter(F.col("REMIT_CLM_ID_1").isNotNull())
    .select(
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("CLM_LN_DSALW_TYP_CD_SK").alias("CLM_LN_DSALW_TYP_CD_SK"),
        F.col("CLM_LN_REMIT_DSALW_EXCD_SK").alias("CLM_LN_REMIT_DSALW_EXCD_SK"),
        F.when(F.col("CLM_LN_RMT_DSW_EXCD_RESP_CD_SK").isNull(), F.lit(0))
         .otherwise(F.col("CLM_LN_RMT_DSW_EXCD_RESP_CD_SK"))
         .alias("CLM_LN_RMT_DSW_EXCD_RESP_CD_SK"),
        F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.when(F.col("CLM_LN_REMIT_DSALW_AMT").isNull(), F.lit(0))
         .otherwise(F.col("CLM_LN_REMIT_DSALW_AMT"))
         .alias("CLM_LN_REMIT_DSALW_AMT")
    )
)

df_fnl_dataLinks = (
    df_db2_CLM_LN_REMIT_DSALW_PRoutput_Extr.select(
        "SRC_SYS_CD",
        "CLM_ID",
        "CLM_LN_DSALW_TYP_CD_SK",
        "CLM_LN_REMIT_DSALW_EXCD_SK",
        "CLM_LN_RMT_DSW_EXCD_RESP_CD_SK",
        "CLM_LN_SEQ_NO",
        "CLM_LN_REMIT_DSALW_AMT"
    )
    .unionByName(
        df_Copy_of_xfrm_BusinessLogic.select(
            "SRC_SYS_CD",
            "CLM_ID",
            "CLM_LN_DSALW_TYP_CD_SK",
            "CLM_LN_REMIT_DSALW_EXCD_SK",
            "CLM_LN_RMT_DSW_EXCD_RESP_CD_SK",
            "CLM_LN_SEQ_NO",
            "CLM_LN_REMIT_DSALW_AMT"
        )
    )
)

df_lkp_Codes = (
    df_fnl_dataLinks.alias("fnl_dataLinks_out")
    .join(
        df_db2_ExcdExtr.alias("refExcdLkup"),
        F.col("fnl_dataLinks_out.CLM_LN_REMIT_DSALW_EXCD_SK") == F.col("refExcdLkup.EXCD_SK"),
        how="left"
    )
    .join(
        df_Cpy_Cd_Mppng_refClmLnDsalwTypCd.alias("refClmLnDsalwTypCd"),
        F.col("fnl_dataLinks_out.CLM_LN_DSALW_TYP_CD_SK") == F.col("refClmLnDsalwTypCd.CD_MPPNG_SK"),
        how="left"
    )
    .join(
        df_Cpy_Cd_Mppng_refClmLnRmtDswExcdRespCd.alias("refClmLnRmtDswExcdRespCd"),
        F.col("fnl_dataLinks_out.CLM_LN_RMT_DSW_EXCD_RESP_CD_SK") == F.col("refClmLnRmtDswExcdRespCd.CD_MPPNG_SK"),
        how="left"
    )
    .select(
        F.col("fnl_dataLinks_out.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("fnl_dataLinks_out.CLM_ID").alias("CLM_ID"),
        F.col("fnl_dataLinks_out.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("fnl_dataLinks_out.CLM_LN_REMIT_DSALW_AMT").alias("CLM_LN_REMIT_DSALW_AMT"),
        F.col("refClmLnDsalwTypCd.TRGT_CD").alias("CLM_LN_DSALW_TYP_CD"),
        F.col("refClmLnRmtDswExcdRespCd.TRGT_CD").alias("CLM_LN_RMT_DSALW_EXCD_RESP_CD"),
        F.col("refClmLnRmtDswExcdRespCd.TRGT_CD_NM").alias("CLM_LN_RMT_DSALW_EXCD_RESP_NM"),
        F.col("refExcdLkup.EXCD_SK").alias("EXCD_SK"),
        F.col("refExcdLkup.EXCD_ID").alias("EXCD_ID"),
        F.col("refExcdLkup.EXCD_LONG_TX1").alias("EXCD_LONG_TX1"),
        F.col("refExcdLkup.EXCD_LONG_TX2").alias("EXCD_LONG_TX2")
    )
)

df_xfrm_BusinessLogic = (
    df_lkp_Codes
    .select(
        F.when(trim(F.col("SRC_SYS_CD")) == "", F.lit(" ")).otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.when(trim(F.col("CLM_LN_DSALW_TYP_CD")) == "", F.lit(" ")).otherwise(F.col("CLM_LN_DSALW_TYP_CD")).alias("CLM_LN_DSALW_TYP_CD"),
        F.col("CLM_LN_REMIT_DSALW_AMT").alias("CLM_LN_REMIT_DSALW_AMT"),
        F.when(F.col("EXCD_SK") == 0, F.lit(" ")).otherwise(F.col("EXCD_ID")).alias("CLM_LN_REMIT_DSALW_EXCD_ID"),
        F.when(
            F.col("EXCD_SK") == 0,
            F.lit("UNK")
        ).otherwise(
            F.when(
                F.length(trim(F.when(F.col("EXCD_LONG_TX1").isNotNull(), F.col("EXCD_LONG_TX1")).otherwise(F.lit("")))) == 0,
                F.when(
                    F.length(trim(F.when(F.col("EXCD_LONG_TX2").isNotNull(), F.col("EXCD_LONG_TX2")).otherwise(F.lit("")))) == 0,
                    F.lit(" ")
                ).otherwise(
                    trim(F.when(F.col("EXCD_LONG_TX2").isNotNull(), F.col("EXCD_LONG_TX2")).otherwise(F.lit("")))
                )
            ).otherwise(
                F.when(
                    F.length(trim(F.when(F.col("EXCD_LONG_TX2").isNotNull(), F.col("EXCD_LONG_TX2")).otherwise(F.lit("")))) == 0,
                    trim(F.when(F.col("EXCD_LONG_TX1").isNotNull(), F.col("EXCD_LONG_TX1")).otherwise(F.lit("")))
                ).otherwise(
                    F.concat(
                        trim(F.when(F.col("EXCD_LONG_TX1").isNotNull(), F.col("EXCD_LONG_TX1")).otherwise(F.lit(""))),
                        F.lit(" "),
                        trim(F.when(F.col("EXCD_LONG_TX2").isNotNull(), F.col("EXCD_LONG_TX2")).otherwise(F.lit("")))
                    )
                )
            )
        ).alias("CLM_LN_REMIT_DSALW_EXCD_DESC"),
        F.when(trim(F.col("CLM_LN_RMT_DSALW_EXCD_RESP_CD")) == "", F.lit(" ")).otherwise(F.col("CLM_LN_RMT_DSALW_EXCD_RESP_CD")).alias("CLM_LN_RMT_DSALW_EXCD_RESP_CD"),
        F.when(trim(F.col("CLM_LN_RMT_DSALW_EXCD_RESP_NM")) == "", F.lit(" ")).otherwise(F.col("CLM_LN_RMT_DSALW_EXCD_RESP_NM")).alias("CLM_LN_RMT_DSALW_EXCD_RESP_NM"),
        F.lit("CurrRunCycle").alias("LAST_UPDT_RUN_CYC_NO")
    )
)

df_final = (
    df_xfrm_BusinessLogic
    .select(
        F.col("SRC_SYS_CD"),
        F.col("CLM_ID"),
        F.col("CLM_LN_SEQ_NO"),
        F.col("CLM_LN_DSALW_TYP_CD"),
        F.col("CLM_LN_REMIT_DSALW_AMT"),
        rpad(F.col("CLM_LN_REMIT_DSALW_EXCD_ID"), 4, " ").alias("CLM_LN_REMIT_DSALW_EXCD_ID"),
        F.col("CLM_LN_REMIT_DSALW_EXCD_DESC"),
        F.col("CLM_LN_RMT_DSALW_EXCD_RESP_CD"),
        F.col("CLM_LN_RMT_DSALW_EXCD_RESP_NM"),
        F.col("LAST_UPDT_RUN_CYC_NO")
    )
)

write_files(
    df_final,
    f"{adls_path}/load/CLM_DM_CLM_LN_REMIT_DSALW.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)