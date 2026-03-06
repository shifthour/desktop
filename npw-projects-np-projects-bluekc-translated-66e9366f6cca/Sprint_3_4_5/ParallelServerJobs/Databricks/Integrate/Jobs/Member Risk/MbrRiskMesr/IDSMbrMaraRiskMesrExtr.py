# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2010 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY : IdsMARAMbrRiskCntl
# MAGIC 
# MAGIC PROCESSING: Extract MARA_RSLT data and prepare to load into sequential file
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                            Date               Project/Altiris #               Change Description                                                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------         -------------------   -----------------------------------    ---------------------------------------------------------                                     ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Krishnakanth Manivannan  2016-11-01     30001                             Original Programming                                                            IntegrateDev2                Jag Yelavarthi             2016-11-16
# MAGIC Krishnakanth Manivannan  2017-02-17     30001                             Modified to drive from 
# MAGIC                                                                                                          MBR_PCP_ATTRBTN table                                                 IntegrateDev1              Kalyan Neelam              2017-02-20
# MAGIC Krishnakanth Manivannan  2017-03-03     30001                             Modified to job to do a lookup in                                           IntegrateDev1              Kalyan Neelam              2017-03-03
# MAGIC                                                                                                           the table MPI_INDV_BE_CRSWALK  
# MAGIC                                                                                                          for the INDV_BE_KEY which do not
# MAGIC                                                                                                          match with INDV_MARA_RSLT table
# MAGIC Krishnakanth Manivannan  2017-07-20     BreakFix                         HealthLuman will not be sending                                           IntegrateDev1
# MAGIC                                                                                                          the MARA Data to BlueKC for the month
# MAGIC                                                                                                          from June to September 2017. Mara data
# MAGIC                                                                                                          was modified to extract 201705 month always
# MAGIC Krishnakanth Manivannan  2017-10-17     30001                            Removed the hard coded YearMo and added logic to           IntegrateDev2              Kalyan Neelam             2017-10-17
# MAGIC                                                                                                         handle (i) no MARA record (ii) 0 BE_KEY record 
# MAGIC                                                                                                            (iii) xwlk table lookup failure.
# MAGIC Manasa Andru\(9)           2019-06-17     101595                         Modified SQL in MBR_PCP_ATTRBTN_MARA_RSLT_ex    IntegrateDev1              Jaideep Mankala          06/17/2019
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)        adding TRGT_DOMAIN_NM = 'SOURCE SYSTEM'
# MAGIC 
# MAGIC Manasa Andru                      2020-11-24     US - 242088              Added Date Range Conditions in the extract stages                    IntegrateDev2               Jaideep Mankala           11/24/2020        
# MAGIC 
# MAGIC Deepak Kumar                   2022-03-26      US-502041                 Uused SRC_SYS_CD_SK ='#SrcSysCdSk#'  parameter in SQL  IntegrateDevB           Goutham Kalidindi             3/23/2022
# MAGIC 
# MAGIC Reddy Sanam                    2023-02-02      US574919                  In the stage "MARA_RSLT_ex" and "MBR_PCP_ATTRBTN_     IntegrateDev2
# MAGIC                                                                                                       MARA_RSLT_ex"   
# MAGIC                                                                                                      changed model id to include both ('CXCONLAG0','CXXPLNCON')
# MAGIC 
# MAGIC Reddy Sanam                  2023-04-11      US574919                In the stage "risk_score_rng_default" query is changed to                IntegrateDev1            Goutham Kalidindi            4/11/2023
# MAGIC                                                                                                   include effective dates check in the where clause
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)

# MAGIC Extract all the MARA Record and its respective RISK_CAT_ID for the given #CurrentRunMonth#.
# MAGIC i. Get the Previous INDV_BE_KEY from MPI_INDV_BE_CRSWALK table if the INDV_BE_KEY changes for a member
# MAGIC ii. Populate with the existing INDV_BE_KEY from INDV_BE_MARA_RSLT table if there is no match in MPI_INDV_BE_CRSWALK table.
# MAGIC Extract the records from INDV_BE_MARA_RSLT, MBR_PCP_ATTRBTN, CD_MPPNG, format the data with respect to the MBR_RISK_MESR table structure and load it into the seq file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
# MAGIC %run ../../../../shared_containers/PrimaryKey/MbrRiskMesrPK
# COMMAND ----------

from pyspark.sql.functions import col, lit, when, rpad, isnull
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
RunDate = get_widget_value('RunDate','')
CurrentRunMonth = get_widget_value('CurrentRunMonth','')
CurrentAttrbtnMonth = get_widget_value('CurrentAttrbtnMonth','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
RunYrMo = get_widget_value('RunYrMo','')
CurrRunDate = get_widget_value('CurrRunDate','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_1 = f"""
SELECT
    ATTR_MARA_RSLT.INDV_BE_MARA_RSLT_SK,
    ATTR_MARA_RSLT.TOT_SCORE_NO,
    ATTR_MARA_RSLT.PRCS_YR_MO_SK,
    ATTR_MARA_RSLT.MBR_UNIQ_KEY,
    ATTR_MARA_RSLT.INDV_BE_KEY,
    COALESCE(RNG.RISK_MTHDLGY_CD,'UNK') AS RISK_MTHDLGY_CD,
    COALESCE(RNG.RISK_MTHDLGY_TYP_CD,'UNK') AS RISK_MTHDLGY_TYP_CD,
    COALESCE(RNG.RISK_CAT_ID,'UNK') AS RISK_CAT_ID,
    RNG.RISK_CAT_SCORE_RNG_EFF_DT_SK,
    RNG.RISK_CAT_SCORE_RNG_TERM_DT_SK
FROM (
    SELECT
        COALESCE(RSLT.INDV_BE_MARA_RSLT_SK,0) AS INDV_BE_MARA_RSLT_SK,
        RSLT.TOT_SCORE_NO,
        COALESCE(RSLT.PRCS_YR_MO_SK,'{CurrentRunMonth}') AS PRCS_YR_MO_SK,
        ATTR.MBR_UNIQ_KEY,
        ATTR.INDV_BE_KEY
    FROM {IDSOwner}.MBR_PCP_ATTRBTN ATTR
    LEFT JOIN (
        SELECT
            INDV_BE_MARA_RSLT_SK,
            TOT_SCORE_NO,
            PRCS_YR_MO_SK,
            INDV_BE_KEY
        FROM {IDSOwner}.INDV_BE_MARA_RSLT
        WHERE
            MDL_ID in ('CXXPLNCON')
            AND SRC_SYS_CD_SK = '{SrcSysCdSk}'
            AND PRCS_YR_MO_SK='{CurrentRunMonth}'
    ) RSLT
    ON ATTR.INDV_BE_KEY=RSLT.INDV_BE_KEY
    WHERE
        ATTR.SRC_SYS_CD_SK = (
            SELECT CD_MPPNG_SK
            FROM {IDSOwner}.CD_MPPNG
            WHERE SRC_DOMAIN_NM='SOURCE SYSTEM'
              AND SRC_CD='BCBSKC'
              AND TRGT_DOMAIN_NM ='SOURCE SYSTEM'
        )
        AND VARCHAR_FORMAT(ATTR.ROW_EFF_DT_SK,'YYYYMM')='{CurrentAttrbtnMonth}'
) ATTR_MARA_RSLT
LEFT JOIN {IDSOwner}.P_RISK_CAT_SCORE_RNG RNG
ON RNG.SRC_SYS_CD='UWS'
AND RNG.RISK_CAT_LOW_SCORE_NO<=ATTR_MARA_RSLT.TOT_SCORE_NO
AND RNG.RISK_CAT_HI_SCORE_NO>=ATTR_MARA_RSLT.TOT_SCORE_NO
AND RISK_CAT_SCORE_RNG_EFF_DT_SK  <= '{CurrRunDate}'
AND RISK_CAT_SCORE_RNG_TERM_DT_SK >= '{CurrRunDate}'
"""

df_MBR_PCP_ATTRBTN_MARA_RSLT_ex = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_1)
    .load()
)

df_attrbtn_mara_score = (
    df_MBR_PCP_ATTRBTN_MARA_RSLT_ex
    .filter((col("INDV_BE_MARA_RSLT_SK") != 0) & (col("INDV_BE_KEY") != 0))
    .select(
        col("INDV_BE_MARA_RSLT_SK").alias("INDV_BE_MARA_RSLT_SK"),
        col("TOT_SCORE_NO").alias("TOT_SCORE_NO"),
        rpad(col("PRCS_YR_MO_SK"), 6, " ").alias("PRCS_YR_MO_SK"),
        col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        lit(SrcSysCd).alias("SRC_SYS_CD"),
        col("RISK_MTHDLGY_CD").alias("RISK_MTHDLGY_CD"),
        col("RISK_MTHDLGY_TYP_CD").alias("RISK_MTHDLGY_TYP_CD"),
        col("RISK_CAT_ID").alias("RISK_CAT_ID")
    )
)

df_attrbtn_no_mara_score_valid_be_key = (
    df_MBR_PCP_ATTRBTN_MARA_RSLT_ex
    .filter((col("INDV_BE_MARA_RSLT_SK") == 0) & (col("INDV_BE_KEY") != 0))
    .select(
        col("INDV_BE_KEY").alias("INDV_BE_KEY"),
        col("INDV_BE_MARA_RSLT_SK").alias("INDV_BE_MARA_RSLT_SK"),
        col("TOT_SCORE_NO").alias("TOT_SCORE_NO"),
        rpad(col("PRCS_YR_MO_SK"), 6, " ").alias("PRCS_YR_MO_SK"),
        col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        lit(SrcSysCd).alias("SRC_SYS_CD"),
        lit(SrcSysCdSk).alias("SRC_SYS_CD_SK")
    )
)

df_attrbtn_mara_score_0_be_key = (
    df_MBR_PCP_ATTRBTN_MARA_RSLT_ex
    .filter(col("INDV_BE_KEY") == 0)
    .select(
        col("INDV_BE_MARA_RSLT_SK").alias("INDV_BE_MARA_RSLT_SK"),
        col("TOT_SCORE_NO").alias("TOT_SCORE_NO"),
        rpad(col("PRCS_YR_MO_SK"), 6, " ").alias("PRCS_YR_MO_SK"),
        col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        col("INDV_BE_KEY").alias("INDV_BE_KEY"),
        lit("Y").alias("BEKEY_0_IND"),
        lit(SrcSysCd).alias("SRC_SYS_CD"),
        lit(SrcSysCdSk).alias("SRC_SYS_CD_SK")
    )
)

extract_query_2 = f"""
SELECT
    'Y' AS BEKEY_0_IND,
    RISK_CAT_ID,
    RISK_MTHDLGY_CD,
    RISK_MTHDLGY_TYP_CD,
    NULL AS RISK_CAT_LOW_SCORE_NO
FROM
    {IDSOwner}.P_RISK_CAT_SCORE_RNG
WHERE
    RISK_CAT_LOW_SCORE_NO = (
        SELECT
            MIN(RISK_CAT_LOW_SCORE_NO)
        FROM
            {IDSOwner}.P_RISK_CAT_SCORE_RNG
        WHERE
            SRC_SYS_CD='UWS'
            AND RISK_CAT_SCORE_RNG_EFF_DT_SK <= '{CurrRunDate}'
            AND RISK_CAT_SCORE_RNG_TERM_DT_SK >= '{CurrRunDate}'
    )
    AND RISK_CAT_SCORE_RNG_EFF_DT_SK <= '{CurrRunDate}'
    AND RISK_CAT_SCORE_RNG_TERM_DT_SK >= '{CurrRunDate}'
"""

df_risk_score_rng_default = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_2)
    .load()
)

df_hf_risk_score_rng_min_score_default_temp = dedup_sort(
    df_risk_score_rng_default,
    partition_cols=["BEKEY_0_IND"],
    sort_cols=[]
)

df_hf_risk_score_rng_min_score_default_1 = df_hf_risk_score_rng_min_score_default_temp.select(
    col("BEKEY_0_IND").alias("BEKEY_0_IND"),
    col("RISK_CAT_ID").alias("RISK_CAT_ID"),
    col("RISK_MTHDLGY_CD").alias("RISK_MTHDLGY_CD"),
    col("RISK_MTHDLGY_TYP_CD").alias("RISK_MTHDLGY_TYP_CD"),
    col("RISK_CAT_LOW_SCORE_NO").cast(StringType()).alias("RISK_CAT_LOW_SCORE_NO")
)

df_hf_risk_score_rng_min_score_default_2 = df_hf_risk_score_rng_min_score_default_temp.select(
    rpad(col("BEKEY_0_IND"),1," ").alias("BEKEY_0_IND"),
    col("RISK_CAT_ID").alias("RISK_CAT_ID"),
    col("RISK_MTHDLGY_CD").alias("RISK_MTHDLGY_CD"),
    col("RISK_MTHDLGY_TYP_CD").alias("RISK_MTHDLGY_TYP_CD"),
    col("RISK_CAT_LOW_SCORE_NO").cast(DecimalType(38,10)).alias("RISK_CAT_LOW_SCORE_NO")
)

extract_query_3 = f"""
WITH FILE_SENT_AND_ATTR_DATES (FILE_SENT_DT, ATTR_RUN_DT) AS
(
SELECT X.FILE_SENT_DT, Y.ATTR_RUN_DT
FROM (
    SELECT
        MAX(STRT_DT) AS FILE_SENT_DT,
        '1' AS IND
    FROM {IDSOwner}.P_RUN_CYC
    WHERE
        SRC_SYS_CD='EDW'
        AND TRGT_SYS_CD='{SrcSysCd}'
        AND SUBJ_CD='CLAIM'
        AND VARCHAR_FORMAT(STRT_DT,'YYYYMM')='{RunYrMo}'
) X
INNER JOIN (
    SELECT MAX(RUNCYC.STRT_DT) AS ATTR_RUN_DT, '1' AS IND
    FROM {IDSOwner}.P_RUN_CYC RUNCYC
    INNER JOIN {IDSOwner}.MBR_PCP_ATTRBTN ATTR
      ON ATTR.LAST_UPDT_RUN_CYC_EXCTN_SK = RUNCYC.RUN_CYC_NO
    INNER JOIN {IDSOwner}.CD_MPPNG MPG
      ON MPG.TRGT_CD = RUNCYC.SRC_SYS_CD
      AND ATTR.SRC_SYS_CD_SK = MPG.CD_MPPNG_SK
    WHERE
        VARCHAR_FORMAT(ATTR.ROW_EFF_DT_SK,'YYYYMM')='{CurrentAttrbtnMonth}'
        AND RUNCYC.SRC_SYS_CD='BCBSKC'
        AND RUNCYC.TRGT_SYS_CD='IDS'
        AND RUNCYC.SUBJ_CD='PCMH'
) Y
ON X.IND = Y.IND
)
SELECT
    XWLK1.PREV_INDV_BE_KEY,
    XWLK1.MBR_UNIQ_KEY,
    XWLK2.MAX_INDV_BE_KEY
FROM {IDSOwner}.MPI_INDV_BE_CRSWALK XWLK1
INNER JOIN (
    SELECT
        XW1.MBR_UNIQ_KEY,
        XW1.NEW_INDV_BE_KEY AS MAX_INDV_BE_KEY,
        RANK() OVER (PARTITION BY XW1.MBR_UNIQ_KEY ORDER BY XW1.PRCS_RUN_DTM DESC) AS RANK_ID,
        XW1.PRCS_RUN_DTM
    FROM {IDSOwner}.MPI_INDV_BE_CRSWALK XW1, FILE_SENT_AND_ATTR_DATES FSA1
    WHERE
        XW1.PRCS_RUN_DT_SK >= FSA1.FILE_SENT_DT
        AND XW1.PRCS_RUN_DT_SK <= FSA1.ATTR_RUN_DT
) XWLK2
ON XWLK2.MBR_UNIQ_KEY=XWLK1.MBR_UNIQ_KEY
INNER JOIN FILE_SENT_AND_ATTR_DATES FSA2
ON XWLK1.PRCS_RUN_DT_SK >= FSA2.FILE_SENT_DT
AND XWLK1.PRCS_RUN_DT_SK <= FSA2.ATTR_RUN_DT
WHERE
XWLK2.RANK_ID=1
AND XWLK1.PREV_INDV_BE_KEY <> 0
"""

df_MPI_XWALK_ex = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_3)
    .load()
)

df_hf_ids_xwlk_lkup = dedup_sort(
    df_MPI_XWALK_ex,
    partition_cols=["PREV_INDV_BE_KEY"],
    sort_cols=[]
).select(
    col("PREV_INDV_BE_KEY").alias("PREV_INDV_BE_KEY"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("MAX_INDV_BE_KEY").alias("MAX_NEW_INDV_BE_KEY")
)

extract_query_4 = f"""
SELECT
    MARA.INDV_BE_KEY,
    MARA.INDV_BE_MARA_RSLT_SK,
    MARA.TOT_SCORE_NO AS RISK_MTHDLGY_TYP_SCORE_NO,
    MARA.PRCS_YR_MO_SK,
    COALESCE(RNG.RISK_MTHDLGY_CD,'UNK') AS RISK_MTHDLGY_CD,
    COALESCE(RNG.RISK_MTHDLGY_TYP_CD,'UNK') AS RISK_MTHDLGY_TYP_CD,
    COALESCE(RNG.RISK_CAT_ID,'UNK') AS RISK_CAT_ID
FROM {IDSOwner}.INDV_BE_MARA_RSLT MARA
LEFT JOIN {IDSOwner}.P_RISK_CAT_SCORE_RNG RNG
ON RNG.SRC_SYS_CD='UWS'
AND RNG.RISK_CAT_LOW_SCORE_NO<=MARA.TOT_SCORE_NO
AND RNG.RISK_CAT_HI_SCORE_NO>=MARA.TOT_SCORE_NO
AND RNG.RISK_CAT_SCORE_RNG_EFF_DT_SK <= '{CurrRunDate}'
AND RNG.RISK_CAT_SCORE_RNG_TERM_DT_SK >= '{CurrRunDate}'
WHERE
MARA.PRCS_YR_MO_SK = '{CurrentRunMonth}'
AND MARA.MDL_ID IN ('CXXPLNCON')
AND MARA.SRC_SYS_CD_SK = '{SrcSysCdSk}'
"""

df_MARA_RSLT_ex = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_4)
    .load()
)

df_hf_mara_rslt_lkup = dedup_sort(
    df_MARA_RSLT_ex,
    partition_cols=["INDV_BE_KEY"],
    sort_cols=[]
).select(
    col("INDV_BE_KEY").alias("INDV_BE_KEY"),
    col("INDV_BE_MARA_RSLT_SK").alias("INDV_BE_MARA_RSLT_SK"),
    col("RISK_MTHDLGY_TYP_SCORE_NO").alias("RISK_MTHDLGY_TYP_SCORE_NO"),
    col("PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    col("RISK_MTHDLGY_CD").alias("RISK_MTHDLGY_CD"),
    col("RISK_MTHDLGY_TYP_CD").alias("RISK_MTHDLGY_TYP_CD"),
    col("RISK_CAT_ID").alias("RISK_CAT_ID")
)

df_be_key_synchronization_input = (
    df_hf_mara_rslt_lkup.alias("mara_rslt_be_key")
    .join(
        df_hf_ids_xwlk_lkup.alias("xwlk_be_key_lkup"),
        on=[df_hf_mara_rslt_lkup.INDV_BE_KEY == df_hf_ids_xwlk_lkup.PREV_INDV_BE_KEY],
        how="left"
    )
)

df_be_key_synchronization = df_be_key_synchronization_input.select(
    when(isnull(col("xwlk_be_key_lkup.MAX_NEW_INDV_BE_KEY")), col("mara_rslt_be_key.INDV_BE_KEY"))
        .otherwise(col("xwlk_be_key_lkup.MAX_NEW_INDV_BE_KEY"))
        .alias("INDV_BE_KEY"),
    col("mara_rslt_be_key.INDV_BE_MARA_RSLT_SK").alias("INDV_BE_MARA_RSLT_SK"),
    col("mara_rslt_be_key.RISK_MTHDLGY_TYP_SCORE_NO").alias("RISK_MTHDLGY_TYP_SCORE_NO"),
    col("mara_rslt_be_key.PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    col("mara_rslt_be_key.RISK_MTHDLGY_TYP_CD").alias("RISK_MTHDLGY_TYP_CD"),
    col("mara_rslt_be_key.RISK_MTHDLGY_CD").alias("RISK_MTHDLGY_CD"),
    col("mara_rslt_be_key.RISK_CAT_ID").alias("RISK_CAT_ID")
)

df_hf_mara_rslt_sync_be_key = dedup_sort(
    df_be_key_synchronization,
    partition_cols=["INDV_BE_KEY"],
    sort_cols=[]
).select(
    col("INDV_BE_KEY").alias("INDV_BE_KEY"),
    col("INDV_BE_MARA_RSLT_SK").alias("INDV_BE_MARA_RSLT_SK"),
    col("RISK_MTHDLGY_TYP_SCORE_NO").alias("RISK_MTHDLGY_TYP_SCORE_NO"),
    col("PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    col("RISK_MTHDLGY_TYP_CD").alias("RISK_MTHDLGY_TYP_CD"),
    col("RISK_MTHDLGY_CD").alias("RISK_MTHDLGY_CD"),
    col("RISK_CAT_ID").alias("RISK_CAT_ID")
)

df_synchronized_be_key_lkup_input = (
    df_attrbtn_no_mara_score_valid_be_key.alias("attrbtn_no_mara_score_valid_be_key")
    .join(
        df_hf_mara_rslt_sync_be_key.alias("mara_rslt_synchronized_be_key_lkup"),
        on=[
            df_attrbtn_no_mara_score_valid_be_key.INDV_BE_KEY == df_hf_mara_rslt_sync_be_key.INDV_BE_KEY,
            F.lit("201702") == df_hf_mara_rslt_sync_be_key.PRCS_YR_MO_SK
        ],
        how="left"
    )
)

df_synchronized_be_key_lkup = df_synchronized_be_key_lkup_input.select(
    when(
        (isnull(col("mara_rslt_synchronized_be_key_lkup.INDV_BE_KEY"))) |
        (col("mara_rslt_synchronized_be_key_lkup.INDV_BE_KEY") == 0),
        lit("Y")
    )
    .otherwise(lit("N"))
    .alias("BE_KEY_0_IND"),
    when(isnull(col("mara_rslt_synchronized_be_key_lkup.INDV_BE_KEY")),
         col("attrbtn_no_mara_score_valid_be_key.INDV_BE_MARA_RSLT_SK"))
    .otherwise(col("mara_rslt_synchronized_be_key_lkup.INDV_BE_MARA_RSLT_SK"))
    .alias("INDV_BE_MARA_RSLT_SK"),
    col("mara_rslt_synchronized_be_key_lkup.RISK_MTHDLGY_TYP_SCORE_NO").alias("RISK_MTHDLGY_TYP_SCORE_NO"),
    rpad(col("attrbtn_no_mara_score_valid_be_key.PRCS_YR_MO_SK"), 6, " ").alias("PRCS_YR_MO_SK"),
    col("attrbtn_no_mara_score_valid_be_key.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    lit(SrcSysCd).alias("SRC_SYS_CD"),
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("mara_rslt_synchronized_be_key_lkup.RISK_MTHDLGY_CD").alias("RISK_MTHDLGY_CD"),
    col("mara_rslt_synchronized_be_key_lkup.RISK_MTHDLGY_TYP_CD").alias("RISK_MTHDLGY_TYP_CD"),
    col("mara_rslt_synchronized_be_key_lkup.RISK_CAT_ID").alias("RISK_CAT_ID"),
    col("attrbtn_no_mara_score_valid_be_key.INDV_BE_KEY").alias("INDV_BE_KEY")
)

df_assign_default_val_0_be_key_input = (
    df_attrbtn_mara_score_0_be_key.alias("attrbtn_mara_score_0_be_key")
    .join(
        df_hf_risk_score_rng_min_score_default_1.alias("be_key_0_default_risk_score_1"),
        on=[df_attrbtn_mara_score_0_be_key.BEKEY_0_IND == df_hf_risk_score_rng_min_score_default_1.BEKEY_0_IND],
        how="left"
    )
)

df_assign_default_val_0_be_key = df_assign_default_val_0_be_key_input.select(
    col("attrbtn_mara_score_0_be_key.INDV_BE_MARA_RSLT_SK").alias("INDV_BE_MARA_RSLT_SK"),
    col("be_key_0_default_risk_score_1.RISK_CAT_LOW_SCORE_NO").alias("TOT_SCORE_NO"),
    rpad(col("attrbtn_mara_score_0_be_key.PRCS_YR_MO_SK"), 6, " ").alias("PRCS_YR_MO_SK"),
    col("attrbtn_mara_score_0_be_key.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    lit(SrcSysCd).alias("SRC_SYS_CD"),
    col("be_key_0_default_risk_score_1.RISK_MTHDLGY_CD").alias("RISK_MTHDLGY_CD"),
    col("be_key_0_default_risk_score_1.RISK_MTHDLGY_TYP_CD").alias("RISK_MTHDLGY_TYP_CD"),
    col("be_key_0_default_risk_score_1.RISK_CAT_ID").alias("RISK_CAT_ID")
)

df_assign_default_val_no_mara_rslt_input = (
    df_synchronized_be_key_lkup.alias("attrbtn_mara_rslt_be_key_synchronized")
    .join(
        df_hf_risk_score_rng_min_score_default_2.alias("be_key_0_default_risk_score_2"),
        on=[df_synchronized_be_key_lkup.BE_KEY_0_IND == df_hf_risk_score_rng_min_score_default_2.BEKEY_0_IND],
        how="left"
    )
)

df_assign_default_val_no_mara_rslt = df_assign_default_val_no_mara_rslt_input.select(
    col("attrbtn_mara_rslt_be_key_synchronized.INDV_BE_MARA_RSLT_SK").alias("INDV_BE_MARA_RSLT_SK"),
    when(
        col("attrbtn_mara_rslt_be_key_synchronized.BE_KEY_0_IND") == "Y",
        col("be_key_0_default_risk_score_2.RISK_CAT_LOW_SCORE_NO")
    )
    .otherwise(col("attrbtn_mara_rslt_be_key_synchronized.RISK_MTHDLGY_TYP_SCORE_NO"))
    .alias("TOT_SCORE_NO"),
    rpad(col("attrbtn_mara_rslt_be_key_synchronized.PRCS_YR_MO_SK"), 6, " ").alias("PRCS_YR_MO_SK"),
    col("attrbtn_mara_rslt_be_key_synchronized.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    lit(SrcSysCd).alias("SRC_SYS_CD"),
    when(
        col("attrbtn_mara_rslt_be_key_synchronized.BE_KEY_0_IND") == "Y",
        col("be_key_0_default_risk_score_2.RISK_MTHDLGY_CD")
    )
    .otherwise(col("attrbtn_mara_rslt_be_key_synchronized.RISK_MTHDLGY_CD"))
    .alias("RISK_MTHDLGY_CD"),
    when(
        col("attrbtn_mara_rslt_be_key_synchronized.BE_KEY_0_IND") == "Y",
        col("be_key_0_default_risk_score_2.RISK_MTHDLGY_TYP_CD")
    )
    .otherwise(col("attrbtn_mara_rslt_be_key_synchronized.RISK_MTHDLGY_TYP_CD"))
    .alias("RISK_MTHDLGY_TYP_CD"),
    when(
        col("attrbtn_mara_rslt_be_key_synchronized.BE_KEY_0_IND") == "Y",
        col("be_key_0_default_risk_score_2.RISK_CAT_ID")
    )
    .otherwise(col("attrbtn_mara_rslt_be_key_synchronized.RISK_CAT_ID"))
    .alias("RISK_CAT_ID")
)

common_cols = [
    "INDV_BE_MARA_RSLT_SK", "TOT_SCORE_NO", "PRCS_YR_MO_SK",
    "MBR_UNIQ_KEY", "SRC_SYS_CD_SK", "SRC_SYS_CD",
    "RISK_MTHDLGY_CD", "RISK_MTHDLGY_TYP_CD", "RISK_CAT_ID"
]

df_combine_all_rec = (
    df_attrbtn_mara_score.select(common_cols)
    .unionByName(df_assign_default_val_0_be_key.select(common_cols))
    .unionByName(df_assign_default_val_no_mara_rslt.select(common_cols))
)

df_format_MBR_RISK_MESR = df_combine_all_rec.select(
    col("INDV_BE_MARA_RSLT_SK").alias("INDV_BE_MARA_RSLT_SK"),
    col("TOT_SCORE_NO").alias("TOT_SCORE_NO"),
    col("PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("RISK_MTHDLGY_CD").alias("RISK_MTHDLGY_CD"),
    col("RISK_MTHDLGY_TYP_CD").alias("RISK_MTHDLGY_TYP_CD"),
    col("RISK_CAT_ID").alias("RISK_CAT_ID")
)

df_format_MBR_RISK_MESR_allcol = df_format_MBR_RISK_MESR.select(
    rpad(col("SRC_SYS_CD_SK"),0,"").alias("SRC_SYS_CD_SK"),
    rpad(col("MBR_UNIQ_KEY"),0,"").alias("MBR_UNIQ_KEY"),
    rpad(trim(col("RISK_CAT_ID")),0,"").alias("RISK_CAT_ID"),
    rpad(col("PRCS_YR_MO_SK"),6," ").alias("PRCS_YR_MO_SK"),
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(lit("I"),10," ").alias("INSRT_UPDT_CD"),
    rpad(lit("N"),1," ").alias("DISCARD_IN"),
    rpad(lit("Y"),1," ").alias("PASS_THRU_IN"),
    lit("DSJobStartTimestamp").alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.concat(col("SRC_SYS_CD"), F.lit(";"), col("MBR_UNIQ_KEY"), F.lit(";"), col("RISK_CAT_ID"), F.lit(";"), col("PRCS_YR_MO_SK")).alias("PRI_KEY_STRING"),
    lit(0).alias("MBR_RISK_MESR_SK"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("MBR_UNIQ_KEY").alias("MBR_CK"),
    lit(1).alias("MBR_MED_MESRS_CD"),
    lit(1).alias("RISK_CAT_SK"),
    lit(0).alias("FTR_RELTV_RISK_NO"),
    lit(0).alias("RISK_SVRTY_CD"),
    rpad(lit("UNK"),10," ").alias("RISK_IDNT_DT"),
    lit(0).alias("MCSRC_RISK_LVL_NO"),
    lit(0).alias("MDCSN_RISK_SCORE_NO"),
    lit(0).alias("MDCSN_HLTH_STTUS_MESR_NO"),
    lit("UNK").alias("CRG_ID"),
    lit("UNK").alias("CRG_DESC"),
    lit("UNK").alias("AGG_CRG_BASE_3_ID"),
    lit("UNK").alias("AGG_CRG_BASE_3_DESC"),
    lit(0).alias("CRG_WT"),
    lit("UNK").alias("CRG_MDL_ID"),
    lit("UNK").alias("CRG_VRSN_ID"),
    col("INDV_BE_MARA_RSLT_SK").alias("INDV_BE_MARA_RSLT_SK"),
    col("RISK_MTHDLGY_CD").alias("RISK_MTHDLGY_CD"),
    col("RISK_MTHDLGY_TYP_CD").alias("RISK_MTHDLGY_TYP_CD"),
    col("TOT_SCORE_NO").alias("RISK_MTHDLGY_TYP_SCORE_NO")
)

df_format_MBR_RISK_MESR_transform = df_format_MBR_RISK_MESR.select(
    rpad(col("SRC_SYS_CD_SK"),0,"").alias("SRC_SYS_CD_SK"),
    rpad(col("MBR_UNIQ_KEY"),0,"").alias("MBR_UNIQ_KEY"),
    col("RISK_CAT_ID").alias("RISK_CAT_ID"),
    rpad(col("PRCS_YR_MO_SK"),6," ").alias("PRCS_YR_MO_SK")
)

params_container = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "RunID": RunID,
    "CurrentDate": RunDate,
    "IDSOwner": IDSOwner
}

df_seq_MBR_RISK_MESR = MbrRiskMesrPK(df_format_MBR_RISK_MESR_transform, df_format_MBR_RISK_MESR_allcol, params_container)

write_files(
    df_seq_MBR_RISK_MESR,
    f"{adls_path}/key/CobaltTalonMbrRiskMesrExtr.MbrRiskMesr.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)