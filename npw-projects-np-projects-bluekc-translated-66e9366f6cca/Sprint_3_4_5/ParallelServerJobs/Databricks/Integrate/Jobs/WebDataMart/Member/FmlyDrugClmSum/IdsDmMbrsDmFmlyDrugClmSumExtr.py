# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:    IdsMbrshDmExtrSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This pulls data from IDS Drug Claim and sums up values based on SUB_UNIQ_KEY
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                   Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------               ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              03/10/2008         3121                            Originally Programmed                                devlIDScur                     Steph Goddard             04/14/2008  
# MAGIC 
# MAGIC Parikshith Chada              04/29/2008         3121                            Some structural job changes in lookup       devlIDScur                     Steph Goddard             05/02/2008
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Shiva Devagiri          08/06/2013        5114                            Original Programming(Server to Parallel)                                                 EnterpriseWhseDevl  Peter Marshall               10/21/2013

# MAGIC Read from source table GRP AND SUB AND MBR AND CLM AND DRUG_CLM from IDS.
# MAGIC Job Name: IdsDmMbrsDmFmlyDrugClmSumExtr
# MAGIC Write MBRSHP_DM_MBR_FMLY_DRUG_CLM_SUM Data into a Sequential file for Load Job IdsDmMbrshDmFmlyDrugClmSumLoad
# MAGIC Add Defaults and Null Handling.
# MAGIC Write MBRSHP_DM_GRP_REL_EDI Data into a Sequential file for Load Job IdsDmGrpRelEdiLoad.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


ids_secret_name = get_widget_value('ids_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
CurrDate = get_widget_value('CurrDate','')
BeginDate = get_widget_value('BeginDate','')
EndDate = get_widget_value('EndDate','')

jdbc_url_db2_FMLY_DRUG_CLM_SUM_in_Main_Extr, jdbc_props_db2_FMLY_DRUG_CLM_SUM_in_Main_Extr = get_db_config(ids_secret_name)
extract_query_db2_FMLY_DRUG_CLM_SUM_in_Main_Extr = f"""SELECT distinct
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
SUB.SUB_UNIQ_KEY

FROM 
{IDSOwner}.DRUG_CLM DRUG_CLM,
{IDSOwner}.CLM CLM,
{IDSOwner}.SUB SUB,
{IDSOwner}.GRP GRP,
{IDSOwner}.CD_MPPNG MPPNG ,
{IDSOwner}.CD_MPPNG CD

WHERE CLM.PD_DT_SK >= '{BeginDate}'
AND CLM.PD_DT_SK <= '{EndDate}'
AND CLM.CLM_STTUS_CD_SK = MPPNG.CD_MPPNG_SK
AND MPPNG.TRGT_CD = 'A02'
AND DRUG_CLM.CLM_ID = CLM.CLM_ID
AND DRUG_CLM.SRC_SYS_CD_SK = CLM.SRC_SYS_CD_SK
AND CLM.SUB_SK = SUB.SUB_SK
AND SUB.GRP_SK = GRP.GRP_SK
AND GRP.BNF_CST_MDLER_IN = 'Y'
AND DRUG_CLM.SRC_SYS_CD_SK = CD.CD_MPPNG_SK

ORDER BY
SUB.SUB_UNIQ_KEY
"""
df_db2_FMLY_DRUG_CLM_SUM_in_Main_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_FMLY_DRUG_CLM_SUM_in_Main_Extr)
    .options(**jdbc_props_db2_FMLY_DRUG_CLM_SUM_in_Main_Extr)
    .option("query", extract_query_db2_FMLY_DRUG_CLM_SUM_in_Main_Extr)
    .load()
)

jdbc_url_db2_CONF_FMLY_MBRCT_In_Extr, jdbc_props_db2_CONF_FMLY_MBRCT_In_Extr = get_db_config(ids_secret_name)
extract_query_db2_CONF_FMLY_MBRCT_In_Extr = f"""SELECT
SUB.SUB_UNIQ_KEY,
MBR.MBR_SK

FROM 
({IDSOwner}.GRP AS GRP INNER JOIN (((( {IDSOwner}.SUB AS SUB INNER JOIN {IDSOwner}.MBR AS MBR ON SUB.SUB_SK = MBR.SUB_SK)
INNER JOIN {IDSOwner}.CLM AS CLM ON MBR.MBR_SK = CLM.MBR_SK) INNER JOIN {IDSOwner}.DRUG_CLM AS DRUG ON (CLM.CLM_ID = DRUG.CLM_ID)
AND (CLM.SRC_SYS_CD_SK = DRUG.SRC_SYS_CD_SK))
INNER JOIN {IDSOwner}.CD_MPPNG AS MPPNG ON CLM.CLM_STTUS_CD_SK = MPPNG.CD_MPPNG_SK) ON GRP.GRP_SK = SUB.GRP_SK)

WHERE
GRP.BNF_CST_MDLER_IN = 'Y'
AND MBR.CONF_COMM_IN = 'Y'
AND CLM.PD_DT_SK >= '{BeginDate}' AND CLM.PD_DT_SK <= '{EndDate}'
AND MPPNG.TRGT_CD = 'A02'
"""
df_db2_CONF_FMLY_MBRCT_In_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_CONF_FMLY_MBRCT_In_Extr)
    .options(**jdbc_props_db2_CONF_FMLY_MBRCT_In_Extr)
    .option("query", extract_query_db2_CONF_FMLY_MBRCT_In_Extr)
    .load()
)

df_Aggr_ConfFmlyCt = (
    df_db2_CONF_FMLY_MBRCT_In_Extr
    .groupBy("SUB_UNIQ_KEY")
    .agg(F.count("*").alias("CONF_COMM_CT"))
)

jdbc_url_db2_FMLY_DRUG_CLM_SUM_in, jdbc_props_db2_FMLY_DRUG_CLM_SUM_in = get_db_config(ids_secret_name)
extract_query_db2_FMLY_DRUG_CLM_SUM_in = f"""SELECT
SUB.SUB_UNIQ_KEY,
CLM.CLM_CT,
DRUG.MNTN_IN,
MPPNG2.TRGT_CD

FROM 
({IDSOwner}.GRP AS GRP INNER JOIN ((((( {IDSOwner}.SUB AS SUB INNER JOIN {IDSOwner}.MBR AS MBR ON SUB.SUB_SK = MBR.SUB_SK)
INNER JOIN {IDSOwner}.CLM AS CLM ON MBR.MBR_SK = CLM.MBR_SK) INNER JOIN {IDSOwner}.DRUG_CLM AS DRUG ON (CLM.CLM_ID = DRUG.CLM_ID)
AND (CLM.SRC_SYS_CD_SK = DRUG.SRC_SYS_CD_SK)) INNER JOIN {IDSOwner}.CD_MPPNG AS MPPNG2 ON DRUG.DRUG_CLM_TIER_CD_SK = MPPNG2.CD_MPPNG_SK)
INNER JOIN {IDSOwner}.CD_MPPNG AS MPPNG1 ON CLM.CLM_STTUS_CD_SK = MPPNG1.CD_MPPNG_SK) ON GRP.GRP_SK = SUB.GRP_SK)

WHERE
CLM.PD_DT_SK >= '{BeginDate}' AND CLM.PD_DT_SK <= '{EndDate}'
AND MBR.CONF_COMM_IN = 'N'
AND MPPNG2.TRGT_CD IN ('TIER1','TIER2','TIER3')
AND MPPNG1.TRGT_CD = 'A02'
AND GRP.BNF_CST_MDLER_IN = 'Y'

ORDER BY
SUB.SUB_UNIQ_KEY
"""
df_db2_FMLY_DRUG_CLM_SUM_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_FMLY_DRUG_CLM_SUM_in)
    .options(**jdbc_props_db2_FMLY_DRUG_CLM_SUM_in)
    .option("query", extract_query_db2_FMLY_DRUG_CLM_SUM_in)
    .load()
)

df_MntnTier1CtExtr = (
    df_db2_FMLY_DRUG_CLM_SUM_in
    .where((trim(F.col("MNTN_IN")) == 'Y') & (trim(F.col("TRGT_CD")) == 'TIER1'))
    .select("SUB_UNIQ_KEY", "CLM_CT")
)

df_MntnTier2CtExtr = (
    df_db2_FMLY_DRUG_CLM_SUM_in
    .where((trim(F.col("MNTN_IN")) == 'Y') & (trim(F.col("TRGT_CD")) == 'TIER2'))
    .select("SUB_UNIQ_KEY", "CLM_CT")
)

df_MntnTier3CtExtr = (
    df_db2_FMLY_DRUG_CLM_SUM_in
    .where((trim(F.col("MNTN_IN")) == 'Y') & (trim(F.col("TRGT_CD")) == 'TIER3'))
    .select("SUB_UNIQ_KEY", "CLM_CT")
)

df_NonMntnTier1CtExtr = (
    df_db2_FMLY_DRUG_CLM_SUM_in
    .where((trim(F.col("MNTN_IN")) != 'Y') & (trim(F.col("TRGT_CD")) == 'TIER1'))
    .select("SUB_UNIQ_KEY", "CLM_CT")
)

df_NonMntnTier2CtExtr = (
    df_db2_FMLY_DRUG_CLM_SUM_in
    .where((trim(F.col("MNTN_IN")) != 'Y') & (trim(F.col("TRGT_CD")) == 'TIER2'))
    .select("SUB_UNIQ_KEY", "CLM_CT")
)

df_NonMntnTier3CtExtr = (
    df_db2_FMLY_DRUG_CLM_SUM_in
    .where((trim(F.col("MNTN_IN")) != 'Y') & (trim(F.col("TRGT_CD")) == 'TIER3'))
    .select("SUB_UNIQ_KEY", "CLM_CT")
)

df_Aggr_MntnTier3 = (
    df_MntnTier3CtExtr
    .groupBy("SUB_UNIQ_KEY")
    .agg(F.sum("CLM_CT").alias("MNTN_TIER3_CT"))
)

df_Aggr_MntnTier1 = (
    df_MntnTier1CtExtr
    .groupBy("SUB_UNIQ_KEY")
    .agg(F.sum("CLM_CT").alias("MNTN_TIER1_CT"))
)

df_Aggr_NonMntnTier3 = (
    df_NonMntnTier3CtExtr
    .groupBy("SUB_UNIQ_KEY")
    .agg(F.sum("CLM_CT").alias("NON_MNTN_TIER3_CT"))
)

df_Aggr_NonMntnTier2 = (
    df_NonMntnTier2CtExtr
    .groupBy("SUB_UNIQ_KEY")
    .agg(F.sum("CLM_CT").alias("NON_MNTN_TIER2_CT"))
)

df_Aggr_NonMntnTier1 = (
    df_NonMntnTier1CtExtr
    .groupBy("SUB_UNIQ_KEY")
    .agg(F.sum("CLM_CT").alias("NON_MNTN_TIER1_CT"))
)

df_Aggr_MntnTier2 = (
    df_MntnTier2CtExtr
    .groupBy("SUB_UNIQ_KEY")
    .agg(F.sum("CLM_CT").alias("MNTN_TIER2_CT"))
)

df_Join_Ips = (
    df_db2_FMLY_DRUG_CLM_SUM_in_Main_Extr.alias("Extract")
    .join(df_Aggr_MntnTier1.alias("Agtn_MntnTier1Ctt"),
          F.col("Extract.SUB_UNIQ_KEY") == F.col("Agtn_MntnTier1Ctt.SUB_UNIQ_KEY"), "left")
    .join(df_Aggr_MntnTier2.alias("Agtn_MntnTier2Ctt"),
          F.col("Extract.SUB_UNIQ_KEY") == F.col("Agtn_MntnTier2Ctt.SUB_UNIQ_KEY"), "left")
    .join(df_Aggr_MntnTier3.alias("Agtn_MntnTier3Ctt"),
          F.col("Extract.SUB_UNIQ_KEY") == F.col("Agtn_MntnTier3Ctt.SUB_UNIQ_KEY"), "left")
    .join(df_Aggr_NonMntnTier1.alias("Agtn_NonMntnTier1Ctt"),
          F.col("Extract.SUB_UNIQ_KEY") == F.col("Agtn_NonMntnTier1Ctt.SUB_UNIQ_KEY"), "left")
    .join(df_Aggr_NonMntnTier2.alias("Agtn_NonMntnTier2Ctt"),
          F.col("Extract.SUB_UNIQ_KEY") == F.col("Agtn_NonMntnTier2Ctt.SUB_UNIQ_KEY"), "left")
    .join(df_Aggr_NonMntnTier3.alias("Agtn_NonMntnTier3Ctt"),
          F.col("Extract.SUB_UNIQ_KEY") == F.col("Agtn_NonMntnTier3Ctt.SUB_UNIQ_KEY"), "left")
    .join(df_Aggr_ConfFmlyCt.alias("AgrConfFmlyCt"),
          F.col("Extract.SUB_UNIQ_KEY") == F.col("AgrConfFmlyCt.SUB_UNIQ_KEY"), "left")
    .select(
        F.col("Extract.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Extract.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        F.col("Agtn_MntnTier1Ctt.MNTN_TIER1_CT").alias("MNTN_TIER1_CT"),
        F.col("Agtn_MntnTier2Ctt.MNTN_TIER2_CT").alias("MNTN_TIER2_CT"),
        F.col("Agtn_MntnTier3Ctt.MNTN_TIER3_CT").alias("MNTN_TIER3_CT"),
        F.col("Agtn_NonMntnTier1Ctt.NON_MNTN_TIER1_CT").alias("NON_MNTN_TIER1_CT"),
        F.col("Agtn_NonMntnTier2Ctt.NON_MNTN_TIER2_CT").alias("NON_MNTN_TIER2_CT"),
        F.col("Agtn_NonMntnTier3Ctt.NON_MNTN_TIER3_CT").alias("NON_MNTN_TIER3_CT"),
        F.col("AgrConfFmlyCt.CONF_COMM_CT").alias("CONF_COMM_CT")
    )
)

df_Xfm_Transformer = df_Join_Ips.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.when(F.col("CONF_COMM_CT").isNull(), F.lit(0))
     .otherwise(F.col("CONF_COMM_CT").cast(IntegerType()))
     .alias("CONF_FMLY_MBR_CT"),
    F.when(F.col("MNTN_TIER1_CT").isNull(), F.lit(0))
     .otherwise(F.col("MNTN_TIER1_CT").cast(IntegerType()))
     .alias("MNTN_TIER_1_CT"),
    F.when(F.col("MNTN_TIER2_CT").isNull(), F.lit(0))
     .otherwise(F.col("MNTN_TIER2_CT").cast(IntegerType()))
     .alias("MNTN_TIER_2_CT"),
    F.when(F.col("MNTN_TIER3_CT").isNull(), F.lit(0))
     .otherwise(F.col("MNTN_TIER3_CT").cast(IntegerType()))
     .alias("MNTN_TIER_3_CT"),
    F.when(F.col("NON_MNTN_TIER1_CT").isNull(), F.lit(0))
     .otherwise(F.col("NON_MNTN_TIER1_CT").cast(IntegerType()))
     .alias("NON_MNTN_TIER_1_CT"),
    F.when(F.col("NON_MNTN_TIER2_CT").isNull(), F.lit(0))
     .otherwise(F.col("NON_MNTN_TIER2_CT").cast(IntegerType()))
     .alias("NON_MNTN_TIER_2_CT"),
    F.when(F.col("NON_MNTN_TIER3_CT").isNull(), F.lit(0))
     .otherwise(F.col("NON_MNTN_TIER3_CT").cast(IntegerType()))
     .alias("NON_MNTN_TIER_3_CT"),
    F.to_timestamp(F.lit(EndDate), "yyyy-MM-dd").alias("LAST_PD_DT"),
    F.to_timestamp(F.lit(CurrDate), "yyyy-MM-dd").alias("LAST_UPDT_DT"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_NO")
)

write_files(
    df_Xfm_Transformer.select(
        "SRC_SYS_CD",
        "SUB_UNIQ_KEY",
        "CONF_FMLY_MBR_CT",
        "MNTN_TIER_1_CT",
        "MNTN_TIER_2_CT",
        "MNTN_TIER_3_CT",
        "NON_MNTN_TIER_1_CT",
        "NON_MNTN_TIER_2_CT",
        "NON_MNTN_TIER_3_CT",
        "LAST_PD_DT",
        "LAST_UPDT_DT",
        "LAST_UPDT_RUN_CYC_NO"
    ),
    f"{adls_path}/load/MBRSH_DM_FMLY_DRUG_CLM_SUM.dat",
    delimiter=",",
    mode="overwrite",
    is_parqruet=False,
    header=False,
    quote="^",
    nullValue=None
)