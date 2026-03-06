# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC Â© Copyright 2009 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     IdsClmMartExtrLoadSeq
# MAGIC 
# MAGIC Processing:
# MAGIC                     Extract and load claim line remit disallow data based on driver table W_WEBDM_ETL_DRVR
# MAGIC 
# MAGIC Modifications:                        
# MAGIC   Modifications:                        
# MAGIC 
# MAGIC Developer                          Date                    Project/Altiris #                     Change Description                                                       Development Project               Code Reviewer                      Date Reviewed
# MAGIC ---------------------------------      -------------------      -----------------------------------           ---------------------------------------------------------                                 ----------------------------------              ---------------------------------               -------------------------
# MAGIC SAndrew                      2009-06-04           3833 Alt Chrg Remit                Originally Programmed                                                       devlIDSnew                            Steph Goddard                        07/01/2009
# MAGIC   
# MAGIC Nagesh Bandi              07/03/2013              5114                                 Original Programming(Server to Parallel)                             IntegrateWrhsDevl                   Jag Yelavarthi                         2013-12-01
# MAGIC 
# MAGIC Jag Yelavarthi              2014-04-01              TFS#8426                            Corrected derivation for EXCD_DESC                             IntegrateNewDevl                   Bhoomi Dasari                          4/3/2014
# MAGIC 
# MAGIC Ediga Maruthi              2025-04-29             US 647845                       Added EXCD_ID_FOR_SORTING (Varchar 20) column in
# MAGIC                                                                                                             xfrm_BusinessLogic stage. Added DedupOnExcdId stage 
# MAGIC                                                                                                             to remove the duplicates.Added annotation at 
# MAGIC                                                                                                             DedupOnExcdId stage.                                                          IntegrateDev2                       Jeyaprasanna                           2025-05-06

# MAGIC Possible
# MAGIC EXCD_ID values
# MAGIC as of 2025-04-24:
# MAGIC 
# MAGIC DIS
# MAGIC fi5
# MAGIC P60
# MAGIC P61
# MAGIC PDC
# MAGIC PSA
# MAGIC PXN
# MAGIC WZJ
# MAGIC XF6
# MAGIC YK3
# MAGIC YQY
# MAGIC YRE
# MAGIC YRF
# MAGIC YRK
# MAGIC Write CLM_DM_CLM_LN_ALT_CHRG Data into a Sequential file for Load Job IdsDmClmDmClmLnAltChrgLoad.
# MAGIC Read all the Data from IDS CLM_LN_ALT_CHRG_REMIT Table;
# MAGIC Add Defaults and Null Handling.
# MAGIC Job Name: 
# MAGIC 
# MAGIC IdsDmClmDmClmLnAltChrgExtr
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
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# Parameters
IDSOwner = get_widget_value('IDSOwner','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
ids_secret_name = get_widget_value('ids_secret_name','')

# Obtain JDBC configuration
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# Read from "db2_ExcdExtr"
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
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_ExcdExtr)
    .load()
)

# Read from "db2_CLM_LN_ALT_CHRG_REMIT_DSALW_PRoutput_Extr"
extract_query_db2_CLM_LN_ALT_CHRG_REMIT_DSALW_PRoutput_Extr = f"""
SELECT 
cd.TRGT_CD AS SRC_SYS_CD,
RemitDsalw.CLM_ID,
RemitDsalw.CLM_LN_DSALW_TYP_CD_SK,
RemitDsalw.CLM_LN_REMIT_DSALW_EXCD_SK,
RemitDsalw.CLM_LN_RMT_DSW_EXCD_RESP_CD_SK,
RemitDsalw.CLM_LN_SEQ_NO,
RemitDsalw.REMIT_DSALW_AMT AS CLM_LN_REMIT_DSALW_AMT
FROM 
{IDSOwner}.CLM_LN_ALT_CHRG_REMIT_DSALW RemitDsalw
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
df_db2_CLM_LN_ALT_CHRG_REMIT_DSALW_PRoutput_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CLM_LN_ALT_CHRG_REMIT_DSALW_PRoutput_Extr)
    .load()
)

# Read from "db2_CD_MPPNG_in"
extract_query_db2_CD_MPPNG_in = f"""
SELECT 
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') AS TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') AS TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG_in)
    .load()
)

# Copy stage "Cpy_Cd_Mppng" - produce two outputs from df_db2_CD_MPPNG_in
df_refClmLnRmtDswExcdRespCd = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK"),
    F.col("TRGT_CD"),
    F.col("TRGT_CD_NM")
)

df_refClmLnDsalwTypCd = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK"),
    F.col("TRGT_CD"),
    F.col("TRGT_CD_NM")
)

# Read from "db2_CLM_LN_ALT_CHRG_REMIT_DSALW_Extr"
extract_query_db2_CLM_LN_ALT_CHRG_REMIT_DSALW_Extr = f"""
SELECT 
cd.TRGT_CD AS SRC_SYS_CD,
RemitDsalw.CLM_ID,
RemitDsalw.CLM_LN_DSALW_TYP_CD_SK,
RemitDsalw.CLM_LN_REMIT_DSALW_EXCD_SK,
RemitDsalw.CLM_LN_RMT_DSW_EXCD_RESP_CD_SK,
MIN(RemitDsalw.CLM_LN_SEQ_NO) AS CLM_LN_SEQ_NO
FROM 
{IDSOwner}.CLM_LN_ALT_CHRG_REMIT_DSALW RemitDsalw
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
df_db2_CLM_LN_ALT_CHRG_REMIT_DSALW_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CLM_LN_ALT_CHRG_REMIT_DSALW_Extr)
    .load()
)

# Read from "db2_CLM_LN_ALT_CHRG_REMIT_DSALW_min_Extr"
extract_query_db2_CLM_LN_ALT_CHRG_REMIT_DSALW_min_Extr = f"""
SELECT 
cd.TRGT_CD AS SRC_SYS_CD,
RemitDsalw.CLM_ID,
RemitDsalw.CLM_LN_REMIT_DSALW_EXCD_SK,
RemitDsalw.CLM_LN_RMT_DSW_EXCD_RESP_CD_SK,
MIN(RemitDsalw.CLM_LN_SEQ_NO) AS CLM_LN_SEQ_NO,
CAST(SUM(RemitDsalw.REMIT_DSALW_AMT) AS DECIMAL(13,2)) AS CLM_LN_REMIT_DSALW_AMT
FROM 
{IDSOwner}.CLM_LN_ALT_CHRG_REMIT_DSALW RemitDsalw
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
df_db2_CLM_LN_ALT_CHRG_REMIT_DSALW_min_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CLM_LN_ALT_CHRG_REMIT_DSALW_min_Extr)
    .load()
)

# Lookup "Remit_Dsalw_Amt_lkp" - left join
df_Remit_Dsalw_Amt_lkp = (
    df_db2_CLM_LN_ALT_CHRG_REMIT_DSALW_Extr.alias("primary")
    .join(
        df_db2_CLM_LN_ALT_CHRG_REMIT_DSALW_min_Extr.alias("lkp"),
        (
            (F.col("primary.SRC_SYS_CD") == F.col("lkp.SRC_SYS_CD")) &
            (F.col("primary.CLM_ID") == F.col("lkp.CLM_ID")) &
            (F.col("primary.CLM_LN_REMIT_DSALW_EXCD_SK") == F.col("lkp.CLM_LN_REMIT_DSALW_EXCD_SK")) &
            (F.col("primary.CLM_LN_RMT_DSW_EXCD_RESP_CD_SK") == F.col("lkp.CLM_LN_RMT_DSW_EXCD_RESP_CD_SK")) &
            (F.col("primary.CLM_LN_SEQ_NO") == F.col("lkp.CLM_LN_SEQ_NO"))
        ),
        how="left"
    )
    .select(
        F.col("primary.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("primary.CLM_ID").alias("CLM_ID"),
        F.col("primary.CLM_LN_DSALW_TYP_CD_SK").alias("CLM_LN_DSALW_TYP_CD_SK"),
        F.col("primary.CLM_LN_REMIT_DSALW_EXCD_SK").alias("CLM_LN_REMIT_DSALW_EXCD_SK"),
        F.col("lkp.CLM_LN_RMT_DSW_EXCD_RESP_CD_SK").alias("CLM_LN_RMT_DSW_EXCD_RESP_CD_SK"),
        F.col("primary.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("lkp.CLM_LN_REMIT_DSALW_AMT").alias("CLM_LN_REMIT_DSALW_AMT"),
        F.col("lkp.CLM_ID").alias("REMIT_CLM_ID_1")
    )
)

# xfrm_BusinessLogic1
df_xfrm_BusinessLogic1 = (
    df_Remit_Dsalw_Amt_lkp
    .filter(F.col("REMIT_CLM_ID_1").isNotNull())
    .withColumn(
        "CLM_LN_RMT_DSW_EXCD_RESP_CD_SK",
        F.when(F.col("CLM_LN_RMT_DSW_EXCD_RESP_CD_SK").isNull(), F.lit(0))
         .otherwise(F.col("CLM_LN_RMT_DSW_EXCD_RESP_CD_SK"))
    )
    .withColumn(
        "CLM_LN_REMIT_DSALW_AMT",
        F.when(F.col("CLM_LN_REMIT_DSALW_AMT").isNull(), F.lit(0))
         .otherwise(F.col("CLM_LN_REMIT_DSALW_AMT"))
    )
    .select(
        F.col("SRC_SYS_CD"),
        F.col("CLM_ID"),
        F.col("CLM_LN_DSALW_TYP_CD_SK"),
        F.col("CLM_LN_REMIT_DSALW_EXCD_SK"),
        F.col("CLM_LN_RMT_DSW_EXCD_RESP_CD_SK"),
        F.col("CLM_LN_SEQ_NO"),
        F.col("CLM_LN_REMIT_DSALW_AMT")
    )
)

# Funnel "fnl_dataLinks" - combine df_db2_CLM_LN_ALT_CHRG_REMIT_DSALW_PRoutput_Extr and df_xfrm_BusinessLogic1
df_fnl_dataLinks = (
    df_db2_CLM_LN_ALT_CHRG_REMIT_DSALW_PRoutput_Extr.select(
        F.col("SRC_SYS_CD"),
        F.col("CLM_ID"),
        F.col("CLM_LN_DSALW_TYP_CD_SK"),
        F.col("CLM_LN_REMIT_DSALW_EXCD_SK"),
        F.col("CLM_LN_RMT_DSW_EXCD_RESP_CD_SK"),
        F.col("CLM_LN_SEQ_NO"),
        F.col("CLM_LN_REMIT_DSALW_AMT")
    )
    .unionByName(
        df_xfrm_BusinessLogic1.select(
            F.col("SRC_SYS_CD"),
            F.col("CLM_ID"),
            F.col("CLM_LN_DSALW_TYP_CD_SK"),
            F.col("CLM_LN_REMIT_DSALW_EXCD_SK"),
            F.col("CLM_LN_RMT_DSW_EXCD_RESP_CD_SK"),
            F.col("CLM_LN_SEQ_NO"),
            F.col("CLM_LN_REMIT_DSALW_AMT")
        )
    )
)

# Lookup "lkp_Codes" with three left joins:
#    1) refExcdLkup => df_db2_ExcdExtr
#    2) refClmLnDsalwTypCd => df_refClmLnDsalwTypCd
#    3) refClmLnRmtDswExcdRespCd => df_refClmLnRmtDswExcdRespCd
df_lkp_join_1 = (
    df_fnl_dataLinks.alias("p")
    .join(
        df_db2_ExcdExtr.alias("e"),
        F.col("p.CLM_LN_REMIT_DSALW_EXCD_SK") == F.col("e.EXCD_SK"),
        how="left"
    )
)

df_lkp_join_2 = (
    df_lkp_join_1.alias("p")
    .join(
        df_refClmLnDsalwTypCd.alias("d"),
        F.col("p.CLM_LN_DSALW_TYP_CD_SK") == F.col("d.CD_MPPNG_SK"),
        how="left"
    )
)

df_lkp_join_3 = (
    df_lkp_join_2.alias("p")
    .join(
        df_refClmLnRmtDswExcdRespCd.alias("r"),
        F.col("p.CLM_LN_RMT_DSW_EXCD_RESP_CD_SK") == F.col("r.CD_MPPNG_SK"),
        how="left"
    )
)

df_lkp_Codes = df_lkp_join_3.select(
    F.col("p.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("p.CLM_ID").alias("CLM_ID"),
    F.col("p.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("p.CLM_LN_REMIT_DSALW_AMT").alias("CLM_LN_REMIT_DSALW_AMT"),
    F.col("d.TRGT_CD").alias("CLM_LN_DSALW_TYP_CD"),
    F.col("r.TRGT_CD").alias("CLM_LN_RMT_DSALW_EXCD_RESP_CD"),
    F.col("r.TRGT_CD_NM").alias("CLM_LN_RMT_DSALW_EXCD_RESP_NM"),
    F.col("e.EXCD_SK").alias("EXCD_SK"),
    F.col("e.EXCD_ID").alias("EXCD_ID"),
    F.col("e.EXCD_LONG_TX1").alias("EXCD_LONG_TX1"),
    F.col("e.EXCD_LONG_TX2").alias("EXCD_LONG_TX2")
)

# xfrm_BusinessLogic
# Stage variable svExcdId => If EXCD_SK=0 then " " else EXCD_ID
df_xfrm_BusinessLogic = (
    df_lkp_Codes
    .withColumn(
        "svExcdId",
        F.when(F.col("EXCD_SK") == 0, F.lit(" ")).otherwise(F.col("EXCD_ID"))
    )
    .withColumn(
        "SRC_SYS_CD",
        F.when(F.trim(F.col("SRC_SYS_CD")) == "", F.lit(" ")).otherwise(F.col("SRC_SYS_CD"))
    )
    .withColumn(
        "CLM_LN_DSALW_TYP_CD",
        F.when(F.trim(F.col("CLM_LN_DSALW_TYP_CD")) == "", F.lit(" ")).otherwise(F.col("CLM_LN_DSALW_TYP_CD"))
    )
    .withColumn(
        "CLM_LN_REMIT_DSALW_EXCD_ID",
        F.col("svExcdId")
    )
    .withColumn(
        "CLM_LN_REMIT_DSALW_EXCD_DESC",
        F.when(
            F.col("EXCD_SK") == 0,
            F.lit(" ")
        ).otherwise(
            F.when(
                F.length(F.trim(F.coalesce(F.col("EXCD_LONG_TX1"), F.lit("")))) == 0,
                F.when(
                    F.length(F.trim(F.coalesce(F.col("EXCD_LONG_TX2"), F.lit("")))) == 0,
                    F.lit(" ")
                ).otherwise(
                    F.trim(F.coalesce(F.col("EXCD_LONG_TX2"), F.lit("")))
                )
            ).otherwise(
                F.when(
                    F.length(F.trim(F.coalesce(F.col("EXCD_LONG_TX2"), F.lit("")))) == 0,
                    F.trim(F.coalesce(F.col("EXCD_LONG_TX1"), F.lit("")))
                ).otherwise(
                    F.concat(
                        F.trim(F.coalesce(F.col("EXCD_LONG_TX1"), F.lit(""))),
                        F.lit(" "),
                        F.trim(F.coalesce(F.col("EXCD_LONG_TX2"), F.lit("")))
                    )
                )
            )
        )
    )
    .withColumn(
        "CLM_LN_RMT_DSALW_EXCD_RESP_CD",
        F.when(F.trim(F.col("CLM_LN_RMT_DSALW_EXCD_RESP_CD")) == "", F.lit(" ")).otherwise(F.col("CLM_LN_RMT_DSALW_EXCD_RESP_CD"))
    )
    .withColumn(
        "CLM_LN_RMT_DSALW_EXCD_RESP_NM",
        F.when(F.trim(F.col("CLM_LN_RMT_DSALW_EXCD_RESP_NM")) == "", F.lit(" ")).otherwise(F.col("CLM_LN_RMT_DSALW_EXCD_RESP_NM"))
    )
    .withColumn(
        "LAST_UPDT_RUN_CYC_NO",
        F.lit(CurrRunCycle)
    )
    .withColumn(
        "EXCD_ID_FOR_SORTING",
        F.when(F.col("svExcdId") == "YK3 ", F.lit("ZZZ")).otherwise(F.col("svExcdId"))
    )
    .select(
        F.col("SRC_SYS_CD"),
        F.col("CLM_ID"),
        F.col("CLM_LN_SEQ_NO"),
        F.col("CLM_LN_DSALW_TYP_CD"),
        F.col("CLM_LN_REMIT_DSALW_AMT"),
        F.col("CLM_LN_REMIT_DSALW_EXCD_ID"),
        F.col("CLM_LN_REMIT_DSALW_EXCD_DESC"),
        F.col("CLM_LN_RMT_DSALW_EXCD_RESP_CD"),
        F.col("CLM_LN_RMT_DSALW_EXCD_RESP_NM"),
        F.col("LAST_UPDT_RUN_CYC_NO"),
        F.col("EXCD_ID_FOR_SORTING")
    )
)

# DedupOnExcdId => PxRemDup
df_DedupOnExcdId = dedup_sort(
    df_xfrm_BusinessLogic,
    partition_cols=["SRC_SYS_CD","CLM_ID","CLM_LN_SEQ_NO","CLM_LN_DSALW_TYP_CD"],
    sort_cols=[
        ("SRC_SYS_CD","A"),
        ("CLM_ID","A"),
        ("CLM_LN_SEQ_NO","A"),
        ("CLM_LN_DSALW_TYP_CD","A"),
        ("EXCD_ID_FOR_SORTING","A")
    ]
)

df_final = df_DedupOnExcdId.select(
    F.col("SRC_SYS_CD"),
    F.col("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("CLM_LN_DSALW_TYP_CD"),
    F.col("CLM_LN_REMIT_DSALW_AMT"),
    F.col("CLM_LN_REMIT_DSALW_EXCD_ID"),
    F.col("CLM_LN_REMIT_DSALW_EXCD_DESC"),
    F.col("CLM_LN_RMT_DSALW_EXCD_RESP_CD"),
    F.col("CLM_LN_RMT_DSALW_EXCD_RESP_NM"),
    F.col("LAST_UPDT_RUN_CYC_NO")
)

# Apply rpad for columns of type char/varchar as specified (EXCD_ID was char(4), mapped to CLM_LN_REMIT_DSALW_EXCD_ID)
df_final = df_final.withColumn(
    "CLM_LN_REMIT_DSALW_EXCD_ID",
    F.rpad(F.col("CLM_LN_REMIT_DSALW_EXCD_ID"), 4, " ")
)

# Write to sequential file
write_files(
    df_final,
    f"{adls_path}/load/CLM_DM_CLM_LN_ALT_CHRG_REMIT_DSALW.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)