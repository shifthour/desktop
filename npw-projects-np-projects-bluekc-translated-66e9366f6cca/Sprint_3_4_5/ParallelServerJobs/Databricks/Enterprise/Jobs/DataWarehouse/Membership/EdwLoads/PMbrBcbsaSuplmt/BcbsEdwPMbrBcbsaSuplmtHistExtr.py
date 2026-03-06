# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC CALLED BY: BcbsIdsPMbrBcbsaSuplmtCntl
# MAGIC 
# MAGIC PROCESSING: Extracts last updated data from BCBS database table MBR_BCBSA_SUPLMT and creates a load file for IDS table P_MBR_BCBSA_SUPLMT
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                                                                Date                         Change Description                                                       Project #                                                 Development Project          Code Reviewer          Date Reviewed  
# MAGIC -----------------------                                                     ----------------------------     ----------------------------------------------------------------------------            ----------------                                                ------------------------------------       ----------------------------      -------------------------
# MAGIC Karthik Chintalapani                                                08/20/2014            Originally Programmed                                                       5212                                                         IntegrateNewDevl           Kalyan Neelam           2015-03-26
# MAGIC      
# MAGIC 
# MAGIC 
# MAGIC K Chintalapani                                                         2016-03-09           Added and 2 new columns                                                  5212                                                                                                Kalyan Neelam           2016-03-13
# MAGIC                                                                                                              PRCS_CYC_YR_MO_SK 
# MAGIC                                                                                                            and PDX_CARVE_OUT_SUBMSN_CD                                                                                      
# MAGIC 
# MAGIC Madhavan B                                                           2017-07-26        Added 4 new fields (PRIOR_CONSIS_MBR_ID,                   5658                                                                                                 Kalyan Neelam           2017-10-02
# MAGIC                                                                                                          CONSIS_MBR_ID_CYC_CHG, PRIOR_MMI,
# MAGIC                                                                                                          MMI_CYC_CHG) to the end of the source and target.
# MAGIC 
# MAGIC Manasa Andru                                                         2018-08-06        Added 5 new fields - PROV_PLN_CD, PROD_ID, PROV_NO   5726 - Ntnl Pgms                                EnterpriseDev2                Kalyan Neelam          2018-11-13
# MAGIC                                                                                                        PROV_NO_SFX, SEQ_NO at the end of the file and maped from source.  
# MAGIC 
# MAGIC Rojarani Karnati                                                         2021-10-19          Added new column BCBSA_HI_PRFRMNC_NTWK_IN       US378619                                            EnterpriseDev2                Raja Gummadi         2021-10-26 
# MAGIC 
# MAGIC Anoop Nair                                                            2022-06-15          Added FACETS DSN                                                                S2S Remediation                                  EnterpriseDev5

# MAGIC Job name: BcbsIdsPMbrBsbsaSuplmtExtr
# MAGIC Strip Carriage Return, Line Feed, and  Tab
# MAGIC Apply business logic
# MAGIC As this is a P- table and doesnt have any Pkey or Fkey processes the extract and Xfrm steps are done in one single job.
# MAGIC Write P_MBR_BCBSA_SUPLMT
# MAGIC Extract from facets BCBSA_MBR_SUPLMT
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, when, lit, rpad, coalesce
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Parameters from Job
BCBSOwner = get_widget_value('BCBSOwner','')
bcbs_secret_name = get_widget_value('bcbs_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
LastRunDt = get_widget_value('LastRunDt','')
CurrDate = get_widget_value('CurrDate','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
SrcSysCd = get_widget_value('SrcSysCd','')

# --------------------------------------------------------------------------------
# Stage: db2_MBR (DB2ConnectorPX)
# --------------------------------------------------------------------------------
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_db2_MBR = f"""
SELECT
  MBR_UNIQ_KEY,
  MBR_SK
FROM {IDSOwner}.MBR
WHERE
  MBR_UNIQ_KEY <> 0
  AND
  MBR_UNIQ_KEY <> 1
"""
df_db2_MBR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_MBR.strip())
    .load()
)

# --------------------------------------------------------------------------------
# Stage: fac_BCBSA_MBR_SUPLMT_In (ODBCConnectorPX)
# --------------------------------------------------------------------------------
jdbc_url_bcbs, jdbc_props_bcbs = get_db_config(bcbs_secret_name)
extract_query_fac_BCBSA_MBR_SUPLMT_In = f"""
SELECT BCBSA_HOME_PLN_ID,
       BCBSA_HOME_PLN_CONSIS_MBR_ID,
       BCBSA_ITS_SUB_ID,
       BCBSA_HOME_PLN_MBR_ID,
       BCBSA_COV_BEG_DT,
       BCBSA_COV_END_DT,
       BCBSA_HOME_PLN_PROD_ID,
       MBR_ACTVTY_IN,
       BCBSA_VOID_IN,
       BCBSA_MBR_PARTCPN_CD,
       BCBSA_MMI_ID,
       COALESCE(SBSB_CK,0) as SBSB_CK,
       COALESCE(MEME_CK,0) as MEME_CK,
       COALESCE(SBSB_ID, ' ') as SBSB_ID,
       COALESCE(BCBSA_MBR_PFX_TTL, ' ') as BCBSA_MBR_PFX_TTL,
       BCBSA_MBR_FIRST_NM,
       BCBSA_MBR_MIDINIT,
       BCBSA_MBR_LAST_NM,
       COALESCE(BCBSA_MBR_SFX_TTL, ' ') as BCBSA_MBR_SFX_TTL,
       BCBSA_MBR_GNDR_CD,
       BCBSA_MBR_BRTH_DT,
       COALESCE(BCBSA_MBR_PRI_ST_ADDR_1, ' ') as BCBSA_MBR_PRI_ST_ADDR_1,
       COALESCE(BCBSA_MBR_PRI_ST_ADDR_2, ' ') as BCBSA_MBR_PRI_ST_ADDR_2,
       COALESCE(BCBSA_MBR_PRI_CITY_NM, ' ') as BCBSA_MBR_PRI_CITY_NM,
       COALESCE(BCBSA_MBR_PRI_ST_CD, ' ') as BCBSA_MBR_PRI_ST_CD,
       COALESCE(BCBSA_MBR_PRI_ZIP_CD, ' ') as BCBSA_MBR_PRI_ZIP_CD,
       COALESCE(BCBSA_MBR_PRI_ZIP_CD_4, ' ') as BCBSA_MBR_PRI_ZIP_CD_4,
       COALESCE(BCBSA_MBR_PRI_PHN_NO, ' ') as BCBSA_MBR_PRI_PHN_NO,
       COALESCE(BCBSA_MBR_PRI_EMAIL_ADDR, ' ') as BCBSA_MBR_PRI_EMAIL_ADDR,
       COALESCE(BCBSA_MBR_SEC_ST_ADDR_1, ' ') as BCBSA_MBR_SEC_ST_ADDR_1,
       COALESCE(BCBSA_MBR_SEC_ST_ADDR_2, ' ') as BCBSA_MBR_SEC_ST_ADDR_2,
       COALESCE(BCBSA_MBR_SEC_CITY_NM, ' ') as BCBSA_MBR_SEC_CITY_NM,
       COALESCE(BCBSA_MBR_SEC_ST_CD, ' ') as BCBSA_MBR_SEC_ST_CD,
       COALESCE(BCBSA_MBR_SEC_ZIP_CD, ' ') as BCBSA_MBR_SEC_ZIP_CD,
       COALESCE(BCBSA_MBR_SEC_ZIP_CD_4, ' ') as BCBSA_MBR_SEC_ZIP_CD_4,
       BCBSA_MBR_RELSHP_CD,
       COALESCE(BCBSA_MBR_ALPHA_PFX_CD, ' ') as BCBSA_MBR_ALPHA_PFX_CD,
       COALESCE(BCBSA_PROD_CAT_CD, ' ') as BCBSA_PROD_CAT_CD,
       COALESCE(BCBSA_MBR_CONF_CD, ' ') as BCBSA_MBR_CONF_CD,
       COALESCE(BCBSA_CNTR_TYP_CD, ' ') as BCBSA_CNTR_TYP_CD,
       COALESCE(BCBSA_HOST_PLN_OVRD_CD, ' ') as BCBSA_HOST_PLN_OVRD_CD,
       COALESCE(BCBSA_MBR_MED_COB_CD, ' ') as BCBSA_MBR_MED_COB_CD,
       BCBSA_HOST_PLN_CD,
       BCBSA_HOME_PLN_CORP_PLN_CD,
       BCBSA_HOST_PLN_CORP_PLN_CD,
       PRCS_CYC_YR_MO,
       PDX_CARVE_OUT_SUBMSN_CD,
       PRIOR_HOME_PLN_CONSIS_MBR_ID,
       HOME_PLN_CMI_CHG_YR_MO,
       PRIOR_BCBSA_MMI_ID,
       BCBSA_MMI_ID_CHG_YR_MO,
       PROV_PLN_CD,
       PROD_ID,
       PROV_NO,
       PROV_NO_SFX,
       SEQ_NO,
       BCBSA_HI_PRFRMNC_NTWK_IN
FROM {BCBSOwner}.BCBSA_MBR_SUPLMT
WHERE
  MBR_ACTVTY_IN = 'Y'
"""
df_fac_BCBSA_MBR_SUPLMT_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bcbs)
    .options(**jdbc_props_bcbs)
    .option("query", extract_query_fac_BCBSA_MBR_SUPLMT_In.strip())
    .load()
)

# --------------------------------------------------------------------------------
# Stage: StripField (CTransformerStage)
# --------------------------------------------------------------------------------
df_StripField = (
    df_fac_BCBSA_MBR_SUPLMT_In
    .withColumn("BCBSA_HOME_PLN_ID", strip_field(col("BCBSA_HOME_PLN_ID")))
    .withColumn("BCBSA_HOME_PLN_CONSIS_MBR_ID", strip_field(col("BCBSA_HOME_PLN_CONSIS_MBR_ID")))
    .withColumn("BCBSA_ITS_SUB_ID", strip_field(col("BCBSA_ITS_SUB_ID")))
    .withColumn("BCBSA_HOME_PLN_MBR_ID", strip_field(col("BCBSA_HOME_PLN_MBR_ID")))
    .withColumn("BCBSA_COV_BEG_DT", col("BCBSA_COV_BEG_DT").cast("date"))
    .withColumn("BCBSA_COV_END_DT", col("BCBSA_COV_END_DT").cast("date"))
    .withColumn("BCBSA_HOME_PLN_PROD_ID", strip_field(col("BCBSA_HOME_PLN_PROD_ID")))
    .withColumn("MBR_ACTVTY_IN", strip_field(col("MBR_ACTVTY_IN")))
    .withColumn("BCBSA_VOID_IN", strip_field(col("BCBSA_VOID_IN")))
    .withColumn("BCBSA_MBR_PARTCPN_CD", strip_field(col("BCBSA_MBR_PARTCPN_CD")))
    .withColumn("BCBSA_MMI_ID", strip_field(col("BCBSA_MMI_ID")))
    .withColumn("SBSB_CK", when(col("SBSB_CK").isNull(), lit(0)).otherwise(col("SBSB_CK")))
    .withColumn("MBR_UNIQ_KEY", when(col("MEME_CK").isNull(), lit(0)).otherwise(col("MEME_CK")))
    .withColumn("SBSB_ID", when(strip_field(col("SBSB_ID")) == '', lit(' ')).otherwise(strip_field(col("SBSB_ID"))))
    .withColumn("BCBSA_MBR_PFX_TTL", when(strip_field(col("BCBSA_MBR_PFX_TTL")) == '', lit(' ')).otherwise(strip_field(col("BCBSA_MBR_PFX_TTL"))))
    .withColumn("BCBSA_MBR_FIRST_NM", strip_field(col("BCBSA_MBR_FIRST_NM")))
    .withColumn("BCBSA_MBR_MIDINIT", strip_field(col("BCBSA_MBR_MIDINIT")))
    .withColumn("BCBSA_MBR_LAST_NM", strip_field(col("BCBSA_MBR_LAST_NM")))
    .withColumn("BCBSA_MBR_SFX_TTL", when(strip_field(col("BCBSA_MBR_SFX_TTL")) == '', lit(' ')).otherwise(strip_field(col("BCBSA_MBR_SFX_TTL"))))
    .withColumn("BCBSA_MBR_GNDR_CD", when(strip_field(col("BCBSA_MBR_GNDR_CD")) == '', lit(' ')).otherwise(strip_field(col("BCBSA_MBR_GNDR_CD"))))
    .withColumn("BCBSA_MBR_BRTH_DT", col("BCBSA_MBR_BRTH_DT").cast("date"))
    .withColumn("BCBSA_MBR_PRI_ST_ADDR_1", when(strip_field(col("BCBSA_MBR_PRI_ST_ADDR_1")) == '', lit(' ')).otherwise(strip_field(col("BCBSA_MBR_PRI_ST_ADDR_1"))))
    .withColumn("BCBSA_MBR_PRI_ST_ADDR_2", when(strip_field(col("BCBSA_MBR_PRI_ST_ADDR_2")) == '', lit(' ')).otherwise(strip_field(col("BCBSA_MBR_PRI_ST_ADDR_2"))))
    .withColumn("BCBSA_MBR_PRI_CITY_NM", when(strip_field(col("BCBSA_MBR_PRI_CITY_NM")) == '', lit(' ')).otherwise(strip_field(col("BCBSA_MBR_PRI_CITY_NM"))))
    .withColumn("BCBSA_MBR_PRI_ST_CD", when(strip_field(col("BCBSA_MBR_PRI_ST_CD")) == '', lit(' ')).otherwise(strip_field(col("BCBSA_MBR_PRI_ST_CD"))))
    .withColumn("BCBSA_MBR_PRI_ZIP_CD", when(strip_field(col("BCBSA_MBR_PRI_ZIP_CD")) == '', lit(' ')).otherwise(strip_field(col("BCBSA_MBR_PRI_ZIP_CD"))))
    .withColumn("BCBSA_MBR_PRI_ZIP_CD_4", when(strip_field(col("BCBSA_MBR_PRI_ZIP_CD_4")) == '', lit(' ')).otherwise(strip_field(col("BCBSA_MBR_PRI_ZIP_CD_4"))))
    .withColumn("BCBSA_MBR_PRI_PHN_NO", when(strip_field(col("BCBSA_MBR_PRI_PHN_NO")) == '', lit(' ')).otherwise(strip_field(col("BCBSA_MBR_PRI_PHN_NO"))))
    .withColumn("BCBSA_MBR_PRI_EMAIL_ADDR", when(strip_field(col("BCBSA_MBR_PRI_EMAIL_ADDR")) == '', lit(' ')).otherwise(strip_field(col("BCBSA_MBR_PRI_EMAIL_ADDR"))))
    .withColumn("BCBSA_MBR_SEC_ST_ADDR_1", when(strip_field(col("BCBSA_MBR_SEC_ST_ADDR_1")) == '', lit(' ')).otherwise(strip_field(col("BCBSA_MBR_SEC_ST_ADDR_1"))))
    .withColumn("BCBSA_MBR_SEC_ST_ADDR_2", when(strip_field(col("BCBSA_MBR_SEC_ST_ADDR_2")) == '', lit(' ')).otherwise(strip_field(col("BCBSA_MBR_SEC_ST_ADDR_2"))))
    .withColumn("BCBSA_MBR_SEC_CITY_NM", when(strip_field(col("BCBSA_MBR_SEC_CITY_NM")) == '', lit(' ')).otherwise(strip_field(col("BCBSA_MBR_SEC_CITY_NM"))))
    .withColumn("BCBSA_MBR_SEC_ST_CD", when(strip_field(col("BCBSA_MBR_SEC_ST_CD")) == '', lit(' ')).otherwise(strip_field(col("BCBSA_MBR_SEC_ST_CD"))))
    .withColumn("BCBSA_MBR_SEC_ZIP_CD", when(strip_field(col("BCBSA_MBR_SEC_ZIP_CD")) == '', lit(' ')).otherwise(strip_field(col("BCBSA_MBR_SEC_ZIP_CD"))))
    .withColumn("BCBSA_MBR_SEC_ZIP_CD_4", when(strip_field(col("BCBSA_MBR_SEC_ZIP_CD_4")) == '', lit(' ')).otherwise(strip_field(col("BCBSA_MBR_SEC_ZIP_CD_4"))))
    .withColumn("BCBSA_MBR_RELSHP_CD", strip_field(col("BCBSA_MBR_RELSHP_CD")))
    .withColumn("BCBSA_MBR_ALPHA_PFX_CD", when(strip_field(col("BCBSA_MBR_ALPHA_PFX_CD")) == '', lit(' ')).otherwise(strip_field(col("BCBSA_MBR_ALPHA_PFX_CD"))))
    .withColumn("BCBSA_PROD_CAT_CD", when(strip_field(col("BCBSA_PROD_CAT_CD")) == '', lit(' ')).otherwise(strip_field(col("BCBSA_PROD_CAT_CD"))))
    .withColumn("BCBSA_MBR_CONF_CD", when(strip_field(col("BCBSA_MBR_CONF_CD")) == '', lit(' ')).otherwise(strip_field(col("BCBSA_MBR_CONF_CD"))))
    .withColumn("BCBSA_CNTR_TYP_CD", when(strip_field(col("BCBSA_CNTR_TYP_CD")) == '', lit(' ')).otherwise(strip_field(col("BCBSA_CNTR_TYP_CD"))))
    .withColumn("BCBSA_HOST_PLN_OVRD_CD", when(strip_field(col("BCBSA_HOST_PLN_OVRD_CD")) == '', lit(' ')).otherwise(strip_field(col("BCBSA_HOST_PLN_OVRD_CD"))))
    .withColumn("BCBSA_MBR_MED_COB_CD", when(strip_field(col("BCBSA_MBR_MED_COB_CD")) == '', lit(' ')).otherwise(strip_field(col("BCBSA_MBR_MED_COB_CD"))))
    .withColumn("BCBSA_HOST_PLN_CD", strip_field(col("BCBSA_HOST_PLN_CD")))
    .withColumn("BCBSA_HOME_PLN_CORP_PLN_CD", strip_field(col("BCBSA_HOME_PLN_CORP_PLN_CD")))
    .withColumn("BCBSA_HOST_PLN_CORP_PLN_CD", strip_field(col("BCBSA_HOST_PLN_CORP_PLN_CD")))
    .withColumn("PRCS_CYC_YR_MO", strip_field(col("PRCS_CYC_YR_MO")))
    .withColumn("PDX_CARVE_OUT_SUBMSN_CD", strip_field(col("PDX_CARVE_OUT_SUBMSN_CD")))
    .withColumn("PRIOR_HOME_PLN_CONSIS_MBR_ID", strip_field(col("PRIOR_HOME_PLN_CONSIS_MBR_ID")))
    .withColumn("HOME_PLN_CONSIS_MBR_ID_CHG_YR", strip_field(col("HOME_PLN_CMI_CHG_YR_MO")))
    .withColumn("PRIOR_BCBSA_MMI_ID", strip_field(col("PRIOR_BCBSA_MMI_ID")))
    .withColumn("BCBSA_MMI_ID_CHG_YR_MO", strip_field(col("BCBSA_MMI_ID_CHG_YR_MO")))
    .withColumn("PROV_PLN_CD", col("PROV_PLN_CD"))
    .withColumn("PROD_ID", col("PROD_ID"))
    .withColumn("PROV_NO", col("PROV_NO"))
    .withColumn("PROV_NO_SFX", col("PROV_NO_SFX"))
    .withColumn("SEQ_NO", col("SEQ_NO"))
    .withColumn("BCBSA_HI_PRFRMNC_NTWK_IN", col("BCBSA_HI_PRFRMNC_NTWK_IN"))
)

# --------------------------------------------------------------------------------
# Stage: Jn_Mbr (PxJoin, leftouterjoin on MBR_UNIQ_KEY)
# --------------------------------------------------------------------------------
df_Jn_Mbr = df_StripField.alias("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean").join(
    df_db2_MBR.alias("lnk_Extr"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.MBR_UNIQ_KEY") == col("lnk_Extr.MBR_UNIQ_KEY"),
    "left"
)

df_lnk_PMbrSuplmtBnfOut1 = df_Jn_Mbr.select(
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("lnk_Extr.MBR_SK").alias("MBR_SK"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_HOME_PLN_ID").alias("BCBSA_HOME_PLN_ID"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_HOME_PLN_CONSIS_MBR_ID").alias("BCBSA_HOME_PLN_CONSIS_MBR_ID"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_ITS_SUB_ID").alias("BCBSA_ITS_SUB_ID"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_HOME_PLN_MBR_ID").alias("BCBSA_HOME_PLN_MBR_ID"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_COV_BEG_DT").alias("BCBSA_COV_BEG_DT"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_COV_END_DT").alias("BCBSA_COV_END_DT"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_HOME_PLN_PROD_ID").alias("BCBSA_HOME_PLN_PROD_ID"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.MBR_ACTVTY_IN").alias("MBR_ACTVTY_IN"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_VOID_IN").alias("BCBSA_VOID_IN"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_MBR_PARTCPN_CD").alias("BCBSA_MBR_PARTCPN_CD"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_MMI_ID").alias("BCBSA_MMI_ID"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.SBSB_CK").alias("SBSB_CK"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.SBSB_ID").alias("SBSB_ID"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_MBR_PFX_TTL").alias("BCBSA_MBR_PFX_TTL"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_MBR_FIRST_NM").alias("BCBSA_MBR_FIRST_NM"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_MBR_MIDINIT").alias("BCBSA_MBR_MIDINIT"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_MBR_LAST_NM").alias("BCBSA_MBR_LAST_NM"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_MBR_SFX_TTL").alias("BCBSA_MBR_SFX_TTL"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_MBR_GNDR_CD").alias("BCBSA_MBR_GNDR_CD"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_MBR_BRTH_DT").alias("BCBSA_MBR_BRTH_DT"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_MBR_PRI_ST_ADDR_1").alias("BCBSA_MBR_PRI_ST_ADDR_1"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_MBR_PRI_ST_ADDR_2").alias("BCBSA_MBR_PRI_ST_ADDR_2"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_MBR_PRI_CITY_NM").alias("BCBSA_MBR_PRI_CITY_NM"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_MBR_PRI_ST_CD").alias("BCBSA_MBR_PRI_ST_CD"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_MBR_PRI_ZIP_CD").alias("BCBSA_MBR_PRI_ZIP_CD"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_MBR_PRI_ZIP_CD_4").alias("BCBSA_MBR_PRI_ZIP_CD_4"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_MBR_PRI_PHN_NO").alias("BCBSA_MBR_PRI_PHN_NO"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_MBR_PRI_EMAIL_ADDR").alias("BCBSA_MBR_PRI_EMAIL_ADDR"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_MBR_SEC_ST_ADDR_1").alias("BCBSA_MBR_SEC_ST_ADDR_1"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_MBR_SEC_ST_ADDR_2").alias("BCBSA_MBR_SEC_ST_ADDR_2"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_MBR_SEC_CITY_NM").alias("BCBSA_MBR_SEC_CITY_NM"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_MBR_SEC_ST_CD").alias("BCBSA_MBR_SEC_ST_CD"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_MBR_SEC_ZIP_CD").alias("BCBSA_MBR_SEC_ZIP_CD"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_MBR_SEC_ZIP_CD_4").alias("BCBSA_MBR_SEC_ZIP_CD_4"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_MBR_RELSHP_CD").alias("BCBSA_MBR_RELSHP_CD"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_MBR_ALPHA_PFX_CD").alias("BCBSA_MBR_ALPHA_PFX_CD"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_PROD_CAT_CD").alias("BCBSA_PROD_CAT_CD"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_MBR_CONF_CD").alias("BCBSA_MBR_CONF_CD"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_CNTR_TYP_CD").alias("BCBSA_CNTR_TYP_CD"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_HOST_PLN_OVRD_CD").alias("BCBSA_HOST_PLN_OVRD_CD"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_MBR_MED_COB_CD").alias("BCBSA_MBR_MED_COB_CD"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_HOST_PLN_CD").alias("BCBSA_HOST_PLN_CD"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_HOME_PLN_CORP_PLN_CD").alias("BCBSA_HOME_PLN_CORP_PLN_CD"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_HOST_PLN_CORP_PLN_CD").alias("BCBSA_HOST_PLN_CORP_PLN_CD"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.PRCS_CYC_YR_MO").alias("PRCS_CYC_YR_MO"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.PDX_CARVE_OUT_SUBMSN_CD").alias("PDX_CARVE_OUT_SUBMSN_CD"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.PRIOR_HOME_PLN_CONSIS_MBR_ID").alias("PRIOR_HOME_PLN_CONSIS_MBR_ID"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.HOME_PLN_CONSIS_MBR_ID_CHG_YR").alias("HOME_PLN_CONSIS_MBR_ID_CHG_YR"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.PRIOR_BCBSA_MMI_ID").alias("PRIOR_BCBSA_MMI_ID"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_MMI_ID_CHG_YR_MO").alias("BCBSA_MMI_ID_CHG_YR_MO"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.PROV_PLN_CD").alias("PROV_PLN_CD"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.PROD_ID").alias("PROD_ID"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.PROV_NO").alias("PROV_NO"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.PROV_NO_SFX").alias("PROV_NO_SFX"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.SEQ_NO").alias("SEQ_NO"),
    col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_HI_PRFRMNC_NTWK_IN").alias("BCBSA_HI_PRFRMNC_NTWK_IN")
)

# --------------------------------------------------------------------------------
# Stage: xfm_BusinessLogic (CTransformerStage)
# --------------------------------------------------------------------------------
df_xfm_BusinessLogic = (
    df_lnk_PMbrSuplmtBnfOut1
    .withColumn("BCBSA_HOME_PLN_ID", col("BCBSA_HOME_PLN_ID"))
    .withColumn("BCBSA_CONSIS_MBR_ID", col("BCBSA_HOME_PLN_CONSIS_MBR_ID"))
    .withColumn("BCBSA_ITS_SUB_ID", col("BCBSA_ITS_SUB_ID"))
    .withColumn("BCBSA_HOME_PLN_MBR_ID", col("BCBSA_HOME_PLN_MBR_ID"))
    .withColumn("BCBSA_COV_BEG_DT", col("BCBSA_COV_BEG_DT"))
    .withColumn("BCBSA_COV_END_DT", col("BCBSA_COV_END_DT"))
    .withColumn("BCBSA_HOME_PLN_PROD_ID", col("BCBSA_HOME_PLN_PROD_ID"))
    .withColumn("SRC_SYS_CD", lit(SrcSysCd))
    .withColumn("PRCS_CYC_YR_MO_SK", col("PRCS_CYC_YR_MO"))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", lit(CurrDate))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", lit(CurrDate))
    .withColumn("BCBSA_MBR_ACTVTY_IN", col("MBR_ACTVTY_IN"))
    .withColumn("BCBSA_VOID_IN", col("BCBSA_VOID_IN"))
    .withColumn("BCBSA_MBR_PARTCPN_CD", col("BCBSA_MBR_PARTCPN_CD"))
    .withColumn("BCBSA_MMI_ID", col("BCBSA_MMI_ID"))
    .withColumn("SUB_UNIQ_KEY", when(col("SBSB_CK") == '', lit(0)).otherwise(col("SBSB_CK")))
    .withColumn("MBR_UNIQ_KEY", when(col("MBR_SK") == '', lit(0)).otherwise(col("MBR_UNIQ_KEY")))
    .withColumn("MBR_SK", when(col("MBR_SK") == '', lit(0)).otherwise(col("MBR_SK")))
    .withColumn("SUB_ID", col("SBSB_ID"))
    .withColumn("BCBSA_MBR_PFX_TTL", col("BCBSA_MBR_PFX_TTL"))
    .withColumn("BCBSA_MBR_FIRST_NM", col("BCBSA_MBR_FIRST_NM"))
    .withColumn("BCBSA_MBR_MIDINIT", col("BCBSA_MBR_MIDINIT"))
    .withColumn("BCBSA_MBR_LAST_NM", col("BCBSA_MBR_LAST_NM"))
    .withColumn("BCBSA_MBR_SFX_TTL", col("BCBSA_MBR_SFX_TTL"))
    .withColumn("BCBSA_MBR_GNDR_CD", col("BCBSA_MBR_GNDR_CD"))
    .withColumn("BCBSA_MBR_BRTH_DT", col("BCBSA_MBR_BRTH_DT"))
    .withColumn("BCBSA_MBR_RELSHP_CD", col("BCBSA_MBR_RELSHP_CD"))
    .withColumn("BCBSA_MBR_PRI_ST_ADDR_1", col("BCBSA_MBR_PRI_ST_ADDR_1"))
    .withColumn("BCBSA_MBR_PRI_ST_ADDR_2", col("BCBSA_MBR_PRI_ST_ADDR_2"))
    .withColumn("BCBSA_MBR_PRI_CITY_NM", col("BCBSA_MBR_PRI_CITY_NM"))
    .withColumn("BCBSA_MBR_PRI_ST_CD", col("BCBSA_MBR_PRI_ST_CD"))
    .withColumn("BCBSA_MBR_PRI_ZIP_CD", col("BCBSA_MBR_PRI_ZIP_CD"))
    .withColumn("BCBSA_MBR_PRI_ZIP_CD_4", col("BCBSA_MBR_PRI_ZIP_CD_4"))
    .withColumn("BCBSA_MBR_PRI_PHN_NO", col("BCBSA_MBR_PRI_PHN_NO"))
    .withColumn("BCBSA_MBR_PRI_EMAIL_ADDR", col("BCBSA_MBR_PRI_EMAIL_ADDR"))
    .withColumn("BCBSA_MBR_SEC_ST_ADDR_1", col("BCBSA_MBR_SEC_ST_ADDR_1"))
    .withColumn("BCBSA_MBR_SEC_ST_ADDR_2", col("BCBSA_MBR_SEC_ST_ADDR_2"))
    .withColumn("BCBSA_MBR_SEC_CITY_NM", col("BCBSA_MBR_SEC_CITY_NM"))
    .withColumn("BCBSA_MBR_SEC_ST_CD", col("BCBSA_MBR_SEC_ST_CD"))
    .withColumn("BCBSA_MBR_SEC_ZIP_CD", col("BCBSA_MBR_SEC_ZIP_CD"))
    .withColumn("BCBSA_MBR_SEC_ZIP_CD_4", col("BCBSA_MBR_SEC_ZIP_CD_4"))
    .withColumn("BCBSA_HOST_PLN_CD", col("BCBSA_HOST_PLN_CD"))
    .withColumn("BCBSA_HOME_PLN_CORP_PLN_CD", col("BCBSA_HOME_PLN_CORP_PLN_CD"))
    .withColumn("BCBSA_HOST_PLN_CORP_PLN_CD", col("BCBSA_HOST_PLN_CORP_PLN_CD"))
    .withColumn("BCBSA_MBR_ALPHA_PFX_CD", col("BCBSA_MBR_ALPHA_PFX_CD"))
    .withColumn("BCBSA_PROD_CAT_CD", col("BCBSA_PROD_CAT_CD"))
    .withColumn("BCBSA_MBR_CONF_CD", col("BCBSA_MBR_CONF_CD"))
    .withColumn("BCBSA_CNTR_TYP_CD", col("BCBSA_CNTR_TYP_CD"))
    .withColumn("BCBSA_HOST_PLN_OVRD_CD", col("BCBSA_HOST_PLN_OVRD_CD"))
    .withColumn("BCBSA_MBR_MED_COB_CD", col("BCBSA_MBR_MED_COB_CD"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(CurrRunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(CurrRunCycle))
    .withColumn("PDX_CARVE_OUT_SUBMSN_CD", col("PDX_CARVE_OUT_SUBMSN_CD"))
    .withColumn("PRIOR_HOME_PLN_CONSIS_MBR_ID", col("PRIOR_HOME_PLN_CONSIS_MBR_ID"))
    .withColumn("HOME_PLN_CONSIS_MBR_ID_CHG_YR_MO", col("HOME_PLN_CONSIS_MBR_ID_CHG_YR"))
    .withColumn("PRIOR_BCBSA_MMI_ID", col("PRIOR_BCBSA_MMI_ID"))
    .withColumn("BCBSA_MMI_ID_CHG_YR_MO", col("BCBSA_MMI_ID_CHG_YR_MO"))
    .withColumn("PROV_PLN_CD", col("PROV_PLN_CD"))
    .withColumn("PROD_ID", col("PROD_ID"))
    .withColumn("PROV_NO", col("PROV_NO"))
    .withColumn("PROV_NO_SFX", col("PROV_NO_SFX"))
    .withColumn("SEQ_NO", col("SEQ_NO"))
    .withColumn("BCBSA_HI_PRFRMNC_NTWK_IN", col("BCBSA_HI_PRFRMNC_NTWK_IN"))
)

# --------------------------------------------------------------------------------
# Stage: Seq_P_MBR_BCBSA_SUPLMT (PxSequentialFile)
# Write file to: adls_path/load/P_MBR_BCBSA_SUPLMT_HIST.dat
# --------------------------------------------------------------------------------
df_final = df_xfm_BusinessLogic.select(
    rpad(col("BCBSA_HOME_PLN_ID"), 3, " ").alias("BCBSA_HOME_PLN_ID"),
    rpad(col("BCBSA_CONSIS_MBR_ID"), 22, " ").alias("BCBSA_CONSIS_MBR_ID"),
    rpad(col("BCBSA_ITS_SUB_ID"), 17, " ").alias("BCBSA_ITS_SUB_ID"),
    rpad(col("BCBSA_HOME_PLN_MBR_ID"), 22, " ").alias("BCBSA_HOME_PLN_MBR_ID"),
    col("BCBSA_COV_BEG_DT").alias("BCBSA_COV_BEG_DT"),
    col("BCBSA_COV_END_DT").alias("BCBSA_COV_END_DT"),
    rpad(col("BCBSA_HOME_PLN_PROD_ID"), 15, " ").alias("BCBSA_HOME_PLN_PROD_ID"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    rpad(col("PRCS_CYC_YR_MO_SK"), 6, " ").alias("PRCS_CYC_YR_MO_SK"),
    rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    rpad(col("BCBSA_MBR_ACTVTY_IN"), 1, " ").alias("BCBSA_MBR_ACTVTY_IN"),
    rpad(col("BCBSA_VOID_IN"), 1, " ").alias("BCBSA_VOID_IN"),
    rpad(col("BCBSA_MBR_PARTCPN_CD"), 1, " ").alias("BCBSA_MBR_PARTCPN_CD"),
    rpad(col("BCBSA_MMI_ID"), 22, " ").alias("BCBSA_MMI_ID"),
    col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("MBR_SK").alias("MBR_SK"),
    rpad(col("SUB_ID"), 9, " ").alias("SUB_ID"),
    rpad(col("BCBSA_MBR_PFX_TTL"), 20, " ").alias("BCBSA_MBR_PFX_TTL"),
    rpad(col("BCBSA_MBR_FIRST_NM"), 70, " ").alias("BCBSA_MBR_FIRST_NM"),
    rpad(col("BCBSA_MBR_MIDINIT"), 2, " ").alias("BCBSA_MBR_MIDINIT"),
    rpad(col("BCBSA_MBR_LAST_NM"), 150, " ").alias("BCBSA_MBR_LAST_NM"),
    rpad(col("BCBSA_MBR_SFX_TTL"), 20, " ").alias("BCBSA_MBR_SFX_TTL"),
    rpad(col("BCBSA_MBR_GNDR_CD"), 1, " ").alias("BCBSA_MBR_GNDR_CD"),
    rpad(col("BCBSA_MBR_BRTH_DT"), 10, " ").alias("BCBSA_MBR_BRTH_DT"),
    rpad(col("BCBSA_MBR_RELSHP_CD"), 2, " ").alias("BCBSA_MBR_RELSHP_CD"),
    rpad(col("BCBSA_MBR_PRI_ST_ADDR_1"), 70, " ").alias("BCBSA_MBR_PRI_ST_ADDR_1"),
    rpad(col("BCBSA_MBR_PRI_ST_ADDR_2"), 70, " ").alias("BCBSA_MBR_PRI_ST_ADDR_2"),
    rpad(col("BCBSA_MBR_PRI_CITY_NM"), 35, " ").alias("BCBSA_MBR_PRI_CITY_NM"),
    rpad(col("BCBSA_MBR_PRI_ST_CD"), 2, " ").alias("BCBSA_MBR_PRI_ST_CD"),
    rpad(col("BCBSA_MBR_PRI_ZIP_CD"), 5, " ").alias("BCBSA_MBR_PRI_ZIP_CD"),
    rpad(col("BCBSA_MBR_PRI_ZIP_CD_4"), 4, " ").alias("BCBSA_MBR_PRI_ZIP_CD_4"),
    rpad(col("BCBSA_MBR_PRI_PHN_NO"), 10, " ").alias("BCBSA_MBR_PRI_PHN_NO"),
    rpad(col("BCBSA_MBR_PRI_EMAIL_ADDR"), 70, " ").alias("BCBSA_MBR_PRI_EMAIL_ADDR"),
    rpad(col("BCBSA_MBR_SEC_ST_ADDR_1"), 70, " ").alias("BCBSA_MBR_SEC_ST_ADDR_1"),
    rpad(col("BCBSA_MBR_SEC_ST_ADDR_2"), 70, " ").alias("BCBSA_MBR_SEC_ST_ADDR_2"),
    rpad(col("BCBSA_MBR_SEC_CITY_NM"), 35, " ").alias("BCBSA_MBR_SEC_CITY_NM"),
    rpad(col("BCBSA_MBR_SEC_ST_CD"), 2, " ").alias("BCBSA_MBR_SEC_ST_CD"),
    rpad(col("BCBSA_MBR_SEC_ZIP_CD"), 5, " ").alias("BCBSA_MBR_SEC_ZIP_CD"),
    rpad(col("BCBSA_MBR_SEC_ZIP_CD_4"), 4, " ").alias("BCBSA_MBR_SEC_ZIP_CD_4"),
    rpad(col("BCBSA_HOST_PLN_CD"), 3, " ").alias("BCBSA_HOST_PLN_CD"),
    rpad(col("BCBSA_HOME_PLN_CORP_PLN_CD"), 3, " ").alias("BCBSA_HOME_PLN_CORP_PLN_CD"),
    rpad(col("BCBSA_HOST_PLN_CORP_PLN_CD"), 3, " ").alias("BCBSA_HOST_PLN_CORP_PLN_CD"),
    rpad(col("BCBSA_MBR_ALPHA_PFX_CD"), 3, " ").alias("BCBSA_MBR_ALPHA_PFX_CD"),
    rpad(col("BCBSA_PROD_CAT_CD"), 3, " ").alias("BCBSA_PROD_CAT_CD"),
    rpad(col("BCBSA_MBR_CONF_CD"), 3, " ").alias("BCBSA_MBR_CONF_CD"),
    rpad(col("BCBSA_CNTR_TYP_CD"), 10, " ").alias("BCBSA_CNTR_TYP_CD"),
    rpad(col("BCBSA_HOST_PLN_OVRD_CD"), 3, " ").alias("BCBSA_HOST_PLN_OVRD_CD"),
    rpad(col("BCBSA_MBR_MED_COB_CD"), 1, " ").alias("BCBSA_MBR_MED_COB_CD"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("PDX_CARVE_OUT_SUBMSN_CD"), 1, " ").alias("PDX_CARVE_OUT_SUBMSN_CD"),
    rpad(col("PRIOR_HOME_PLN_CONSIS_MBR_ID"), 22, " ").alias("PRIOR_HOME_PLN_CONSIS_MBR_ID"),
    rpad(col("HOME_PLN_CONSIS_MBR_ID_CHG_YR_MO"), 6, " ").alias("HOME_PLN_CONSIS_MBR_ID_CHG_YR_MO"),
    rpad(col("PRIOR_BCBSA_MMI_ID"), 22, " ").alias("PRIOR_BCBSA_MMI_ID"),
    rpad(col("BCBSA_MMI_ID_CHG_YR_MO"), 6, " ").alias("BCBSA_MMI_ID_CHG_YR_MO"),
    rpad(col("PROV_PLN_CD"), 3, " ").alias("PROV_PLN_CD"),
    rpad(col("PROD_ID"), 4, " ").alias("PROD_ID"),
    rpad(col("PROV_NO"), 13, " ").alias("PROV_NO"),
    rpad(col("PROV_NO_SFX"), 2, " ").alias("PROV_NO_SFX"),
    rpad(col("SEQ_NO"), 2, " ").alias("SEQ_NO"),
    rpad(col("BCBSA_HI_PRFRMNC_NTWK_IN"), 1, " ").alias("BCBSA_HI_PRFRMNC_NTWK_IN")
)

write_files(
    df_final,
    f"{adls_path}/load/P_MBR_BCBSA_SUPLMT_HIST.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)