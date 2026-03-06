# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC Â© Copyright 2009 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:  EdwDMProvDirSeq
# MAGIC 
# MAGIC PROCESSING:   Retrieves the data from EDW PROV_NTWK_D table and loads into the Web Provider Directory database
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 \(9)Project/Altiris #      \(9)Change Description                                        \(9)Development Project      Code Reviewer          Date Reviewed       
# MAGIC ---------------------------    --------------------     \(9)------------------------      \(9)-----------------------------------------------------------------------         --------------------------------       -------------------------------   ----------------------------       
# MAGIC Bhoomi Dasari\(9)2009-05-29\(9)3500             \(9)Original Programming\(9)\(9)\(9)devlEDWnew                  Steph Goddard          06/03/2009 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                               DATASTAGE                          CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                    ENVIRONMENT                     REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -------------------------------------------------------------------------------------------------------  ------------------------------                ------------------------------       --------------------
# MAGIC Balkarrn Gill              06/20/2013        5114                              Create Load File for DM Table PROV_DIR_DM_PROV                   EnterpriseWrhsDevl                 Peter Marshall                9/3/2013
# MAGIC 
# MAGIC Jag Yelavarthi          2015-05-13         #5401                             Derivation for CMN_PRCT_MIDINIT is changed to default to          EnterpriseNewDevl                 Jag Yelavarthi                2015-05-14
# MAGIC                                                                                                    a space in case there is no match from CMN_PRCT table               
# MAGIC 
# MAGIC Mohan Karnati          2019-04-16      87422 NP                         Adding PROV_TXNMY_CD in db2_PROV_D_2in 
# MAGIC                                                                                                              and db2_PROV_D_1stage                                                      EnterpriseDev1                      Jaideep Mankala           04/24/2019

# MAGIC Write PROV_D Data into a Sequential file for Load Job EdwDmProvDirDmProvtLoad.
# MAGIC Read all the Data from EDW PROV_D, PDX_NTWK_D and NTWK_D Table; Pull
# MAGIC Add Defaults and Null Handling.
# MAGIC Job Name: EdwDmProvDirDmProvtExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, StructType, StructField
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


# Obtain parameter values
EDWOwner = get_widget_value('EDWOwner', '')
edw_secret_name = get_widget_value('edw_secret_name', '')
CurrDate = get_widget_value('CurrDate', '')
LastUpdtDt = get_widget_value('LastUpdtDt', '')

# Prepare JDBC configuration for EDW
jdbc_url, jdbc_props = get_db_config(edw_secret_name)

# --------------------------------------------------------------------------------
# Stage: db2_Cmn_Prct_d_Extr (DB2ConnectorPX - EDW)
# --------------------------------------------------------------------------------
extract_query_db2_Cmn_Prct_d_Extr = f"""SELECT distinct
CMN_PRCT_D.CMN_PRCT_SK,
CMN_PRCT_D.CMN_PRCT_BRTH_DT_SK,
CMN_PRCT_D.CMN_PRCT_FIRST_NM,
CMN_PRCT_D.CMN_PRCT_MIDINIT,
CMN_PRCT_D.CMN_PRCT_LAST_NM,
CMN_PRCT_D.CMN_PRCT_GNDR_CD,
CMN_PRCT_D.CMN_PRCT_GNDR_NM,
CMN_PRCT_D.CMN_PRCT_LAST_CRDTL_DT_SK,
CMN_PRCT_D.CMN_PRCT_SSN,
CMN_PRCT_D.CMN_PRCT_TTL,
CMN_PRCT_D.CMN_PRCT_ID
FROM {EDWOwner}.CMN_PRCT_D CMN_PRCT_D
"""
df_db2_Cmn_Prct_d_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_Cmn_Prct_d_Extr)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: db2_ENTY_RGSTRN_D_Extr (DB2ConnectorPX - EDW)
# --------------------------------------------------------------------------------
extract_query_db2_ENTY_RGSTRN_D_Extr = f"""SELECT distinct
ENTY_RGSTRN_D.PROV_SK,
ENTY_RGSTRN_D.ENTY_RGSTRN_TYP_CD
FROM {EDWOwner}.ENTY_RGSTRN_D ENTY_RGSTRN_D
WHERE 
ENTY_RGSTRN_D.ENTY_RGSTRN_TYP_CD = 'LEAPFROG'
"""
df_db2_ENTY_RGSTRN_D_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_ENTY_RGSTRN_D_Extr)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: db2_PROV_D_1in (DB2ConnectorPX - EDW)
# --------------------------------------------------------------------------------
extract_query_db2_PROV_D_1in = f"""SELECT distinct
PROV_D.PROV_ID,
PROV_D.SRC_SYS_CD,
PROV_D.ACTV_IN,
PROV_D.CMN_PRCT_ID,
PROV_D.CNTGS_CNTY_PROV_IN,
PROV_D.ITS_HOME_GNRC_PROV_IN,
PROV_D.LOCAL_BCBSKC_PROV_IN,
PROV_D.PAR_PROV_IN,
PROV_D.PROV_ADDR_ID,
PROV_D.PROV_BILL_SVC_ID,
PROV_D.PROV_BILL_SVC_NM,
PROV_D.PROV_ENTY_CD,
PROV_D.PROV_ENTY_NM,
PROV_D.PROV_FCLTY_TYP_CD,
PROV_D.PROV_FCLTY_TYP_NM,
PROV_D.PROV_NM,
PROV_D.PROV_NTNL_PROV_ID,
PROV_D.PROV_PRCTC_TYP_CD,
PROV_D.PROV_PRCTC_TYP_NM,
PROV_D.PROV_REL_GRP_PROV_ID,
PROV_D.PROV_REL_GRP_PROV_NM,
PROV_D.PROV_REL_IPA_PROV_ID,
PROV_D.PROV_REL_IPA_PROV_NM,
PROV_D.PROV_SPEC_CD,
PROV_D.PROV_SPEC_NM,
PROV_D.PROV_TAX_ID,
PROV_D.PROV_TYP_CD,
PROV_D.PROV_TYP_NM,
PROV_D.CMN_PRCT_SK,
PROV_D.PROV_SK,
PROV_D.PROV_TXNMY_CD
FROM
{EDWOwner}.PROV_D PROV_D,
{EDWOwner}.PROV_NTWK_D B,
{EDWOwner}.NTWK_D NTWK_D
WHERE
PROV_D.PROV_ID = B.PROV_ID
AND B.NTWK_ID = NTWK_D.NTWK_ID
AND B.SRC_SYS_CD = 'FACETS'
AND NTWK_D.NTWK_DIR_CD IN ('ALLDIR', 'WEBDIR')
AND B.PROV_NTWK_DIR_IN = 'Y'
AND B.PROV_NTWK_EFF_DT_SK <= '{CurrDate}'
AND B.PROV_NTWK_TERM_DT_SK > '{CurrDate}'
AND PROV_D.PROV_TERM_DT_SK > '{CurrDate}'
"""
df_db2_PROV_D_1in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_PROV_D_1in)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: db2_PROV_D_2in (DB2ConnectorPX - EDW)
# --------------------------------------------------------------------------------
extract_query_db2_PROV_D_2in = f"""SELECT distinct
PROV_D.PROV_ID,
PROV_D.SRC_SYS_CD,
PROV_D.ACTV_IN,
PROV_D.CMN_PRCT_ID,
PROV_D.CNTGS_CNTY_PROV_IN,
PROV_D.ITS_HOME_GNRC_PROV_IN,
PROV_D.LOCAL_BCBSKC_PROV_IN,
PROV_D.PAR_PROV_IN,
PROV_D.PROV_ADDR_ID,
PROV_D.PROV_BILL_SVC_ID,
PROV_D.PROV_BILL_SVC_NM,
PROV_D.PROV_ENTY_CD,
PROV_D.PROV_ENTY_NM,
PROV_D.PROV_FCLTY_TYP_CD,
PROV_D.PROV_FCLTY_TYP_NM,
PROV_D.PROV_NM,
PROV_D.PROV_NTNL_PROV_ID,
PROV_D.PROV_PRCTC_TYP_CD,
PROV_D.PROV_PRCTC_TYP_NM,
PROV_D.PROV_REL_GRP_PROV_ID,
PROV_D.PROV_REL_GRP_PROV_NM,
PROV_D.PROV_REL_IPA_PROV_ID,
PROV_D.PROV_REL_IPA_PROV_NM,
PROV_D.PROV_SPEC_CD,
PROV_D.PROV_SPEC_NM,
PROV_D.PROV_TAX_ID,
PROV_D.PROV_TYP_CD,
PROV_D.PROV_TYP_NM,
PROV_D.CMN_PRCT_SK,
PROV_D.PROV_SK,
PROV_D.PROV_TXNMY_CD
FROM
{EDWOwner}.PROV_D PROV_D,
{EDWOwner}.PDX_NTWK_D D
WHERE
D.PROV_SK = PROV_D.PROV_SK
AND PROV_D.SRC_SYS_CD = 'NABP'
AND D.PDX_NTWK_CD IN ('ESI0021', 'ESI9031')
AND D.PDX_NTWK_TERM_DT_SK = '9999-12-31'
AND D.PDX_NTWK_DIR_IN = 'Y'
AND D.PDX_NTWK_SK NOT IN (0,1)
"""
df_db2_PROV_D_2in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_PROV_D_2in)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: Prov_Funnel (PxFunnel)
# --------------------------------------------------------------------------------
df_Prov_Funnel_1 = df_db2_PROV_D_1in.select(
    F.col("PROV_SK"),
    F.col("PROV_ID"),
    F.col("SRC_SYS_CD"),
    F.col("ACTV_IN"),
    F.col("CMN_PRCT_ID"),
    F.col("CNTGS_CNTY_PROV_IN"),
    F.col("ITS_HOME_GNRC_PROV_IN"),
    F.col("LOCAL_BCBSKC_PROV_IN"),
    F.col("PAR_PROV_IN"),
    F.col("PROV_ADDR_ID"),
    F.col("PROV_BILL_SVC_ID"),
    F.col("PROV_BILL_SVC_NM"),
    F.col("PROV_ENTY_CD"),
    F.col("PROV_ENTY_NM"),
    F.col("PROV_FCLTY_TYP_CD"),
    F.col("PROV_FCLTY_TYP_NM"),
    F.col("PROV_NM"),
    F.col("PROV_NTNL_PROV_ID"),
    F.col("PROV_PRCTC_TYP_CD"),
    F.col("PROV_PRCTC_TYP_NM"),
    F.col("PROV_REL_GRP_PROV_ID"),
    F.col("PROV_REL_GRP_PROV_NM"),
    F.col("PROV_REL_IPA_PROV_ID"),
    F.col("PROV_REL_IPA_PROV_NM"),
    F.col("PROV_SPEC_CD"),
    F.col("PROV_SPEC_NM"),
    F.col("PROV_TAX_ID"),
    F.col("PROV_TYP_CD"),
    F.col("PROV_TYP_NM"),
    F.col("CMN_PRCT_SK"),
    F.col("PROV_TXNMY_CD"),
)

df_Prov_Funnel_2 = df_db2_PROV_D_2in.select(
    F.col("PROV_SK"),
    F.col("PROV_ID"),
    F.col("SRC_SYS_CD"),
    F.col("ACTV_IN"),
    F.col("CMN_PRCT_ID"),
    F.col("CNTGS_CNTY_PROV_IN"),
    F.col("ITS_HOME_GNRC_PROV_IN"),
    F.col("LOCAL_BCBSKC_PROV_IN"),
    F.col("PAR_PROV_IN"),
    F.col("PROV_ADDR_ID"),
    F.col("PROV_BILL_SVC_ID"),
    F.col("PROV_BILL_SVC_NM"),
    F.col("PROV_ENTY_CD"),
    F.col("PROV_ENTY_NM"),
    F.col("PROV_FCLTY_TYP_CD"),
    F.col("PROV_FCLTY_TYP_NM"),
    F.col("PROV_NM"),
    F.col("PROV_NTNL_PROV_ID"),
    F.col("PROV_PRCTC_TYP_CD"),
    F.col("PROV_PRCTC_TYP_NM"),
    F.col("PROV_REL_GRP_PROV_ID"),
    F.col("PROV_REL_GRP_PROV_NM"),
    F.col("PROV_REL_IPA_PROV_ID"),
    F.col("PROV_REL_IPA_PROV_NM"),
    F.col("PROV_SPEC_CD"),
    F.col("PROV_SPEC_NM"),
    F.col("PROV_TAX_ID"),
    F.col("PROV_TYP_CD"),
    F.col("PROV_TYP_NM"),
    F.col("CMN_PRCT_SK"),
    F.col("PROV_TXNMY_CD"),
)

df_Prov_Funnel = df_Prov_Funnel_1.unionByName(df_Prov_Funnel_2)

# --------------------------------------------------------------------------------
# Stage: lkp_Cmn_Prct_d (PxLookup)
# --------------------------------------------------------------------------------
df_lkp_Cmn_Prct_d_temp = (
    df_Prov_Funnel.alias("PF")
    .join(
        df_db2_Cmn_Prct_d_Extr.alias("CP"),
        F.col("PF.CMN_PRCT_SK") == F.col("CP.CMN_PRCT_SK"),
        "left",
    )
    .join(
        df_db2_ENTY_RGSTRN_D_Extr.alias("EN"),
        F.col("PF.PROV_SK") == F.col("EN.PROV_SK"),
        "left",
    )
)

df_lkp_Cmn_Prct_d = df_lkp_Cmn_Prct_d_temp.select(
    F.col("PF.PROV_ID").alias("PROV_ID"),
    F.col("PF.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CP.CMN_PRCT_GNDR_CD").alias("CMN_PRCT_GNDR_CD"),
    F.col("CP.CMN_PRCT_GNDR_NM").alias("CMN_PRCT_GNDR_NM"),
    F.col("PF.PROV_FCLTY_TYP_CD").alias("FCLTY_TYP_CD"),
    F.col("PF.PROV_FCLTY_TYP_NM").alias("FCLTY_TYP_NM"),
    F.col("PF.PROV_BILL_SVC_ID").alias("PROV_BILL_SVC_ID"),
    F.col("PF.PROV_BILL_SVC_NM").alias("PROV_BILL_SVC_NM"),
    F.col("PF.PROV_ENTY_CD").alias("PROV_ENTY_CD"),
    F.col("PF.PROV_ENTY_NM").alias("PROV_ENTY_NM"),
    F.col("PF.PROV_PRCTC_TYP_CD").alias("PROV_PRCTC_TYP_CD"),
    F.col("PF.PROV_PRCTC_TYP_NM").alias("PROV_PRCTC_TYP_NM"),
    F.col("PF.PROV_REL_GRP_PROV_ID").alias("PROV_REL_GRP_PROV_ID"),
    F.col("PF.PROV_REL_GRP_PROV_NM").alias("PROV_REL_GRP_PROV_NM"),
    F.col("PF.PROV_REL_IPA_PROV_ID").alias("PROV_REL_IPA_PROV_ID"),
    F.col("PF.PROV_REL_IPA_PROV_NM").alias("PROV_REL_IPA_PROV_NM"),
    F.col("PF.PROV_SPEC_CD").alias("PROV_SPEC_CD"),
    F.col("PF.PROV_SPEC_NM").alias("PROV_SPEC_NM"),
    F.col("PF.PROV_TYP_CD").alias("PROV_TYP_CD"),
    F.col("PF.PROV_TYP_NM").alias("PROV_TYP_NM"),
    F.col("PF.ACTV_IN").alias("ACTV_IN"),
    F.col("PF.CNTGS_CNTY_PROV_IN").alias("CNTGS_CNTY_PROV_IN"),
    F.col("PF.ITS_HOME_GNRC_PROV_IN").alias("ITS_HOME_GNRC_PROV_IN"),
    F.col("PF.LOCAL_BCBSKC_PROV_IN").alias("LOCAL_BCBSKC_PROV_IN"),
    F.col("PF.PAR_PROV_IN").alias("PAR_PROV_IN"),
    F.col("CP.CMN_PRCT_BRTH_DT_SK").alias("CMN_PRCT_BRTH_DT"),
    F.col("CP.CMN_PRCT_LAST_CRDTL_DT_SK").alias("CMN_PRCT_LAST_CRDTL_DT"),
    F.col("CP.CMN_PRCT_FIRST_NM").alias("CMN_PRCT_FIRST_NM"),
    F.col("CP.CMN_PRCT_MIDINIT").alias("CMN_PRCT_MIDINIT"),
    F.col("CP.CMN_PRCT_LAST_NM").alias("CMN_PRCT_LAST_NM"),
    F.col("PF.CMN_PRCT_ID").alias("CMN_PRCT_ID"),
    F.col("CP.CMN_PRCT_SSN").alias("CMN_PRCT_SSN"),
    F.col("CP.CMN_PRCT_TTL").alias("CMN_PRCT_TTL"),
    F.col("PF.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.col("PF.PROV_NM").alias("PROV_NM"),
    F.col("PF.PROV_NTNL_PROV_ID").alias("PROV_NPI"),
    F.col("PF.PROV_TAX_ID").alias("PROV_TAX_ID"),
    F.col("EN.ENTY_RGSTRN_TYP_CD").alias("LEAPFROG_IN"),
    F.col("CP.CMN_PRCT_SK").alias("CMN_PRCT_SK"),
    F.col("PF.PROV_TXNMY_CD").alias("PROV_TXNMY_CD"),
)

# --------------------------------------------------------------------------------
# Stage: xfrm_BusinessLogic (CTransformerStage)
# --------------------------------------------------------------------------------
df_xfrm_BusinessLogic = df_lkp_Cmn_Prct_d

df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "CMN_PRCT_GNDR_CD",
    F.when(
        (F.col("CMN_PRCT_SK").isNull())
        | (F.col("CMN_PRCT_GNDR_CD") == "UNK")
        | (F.col("CMN_PRCT_GNDR_CD") == "NA"),
        F.lit(" "),
    ).otherwise(F.col("CMN_PRCT_GNDR_CD")),
)

df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "CMN_PRCT_GNDR_NM",
    F.when(
        (F.col("CMN_PRCT_SK").isNull())
        | (F.col("CMN_PRCT_GNDR_NM") == "UNK")
        | (F.col("CMN_PRCT_GNDR_NM") == "NA"),
        F.lit(" "),
    ).otherwise(F.col("CMN_PRCT_GNDR_NM")),
)

df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "LEAPFROG_IN",
    F.when(
        (F.col("LEAPFROG_IN").isNull()) | (F.col("LEAPFROG_IN") == ""),
        F.lit("N"),
    ).otherwise(F.lit("Y")),
)

df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "CMN_PRCT_BRTH_DT",
    F.when(
        (F.col("CMN_PRCT_SK").isNull())
        | (trim(F.col("CMN_PRCT_BRTH_DT")) == "NA")
        | (trim(F.col("CMN_PRCT_BRTH_DT")) == "UNK")
        | (trim(F.col("CMN_PRCT_BRTH_DT")) == ""),
        FORMAT_DATE_EE(
            F.lit("1753-01-01"), F.lit("DATE"), F.lit("DATE"), F.lit("SYBTIMESTAMP")
        ),
    ).otherwise(
        FORMAT_DATE_EE(
            F.col("CMN_PRCT_BRTH_DT"), F.lit("DATE"), F.lit("DATE"), F.lit("SYBTIMESTAMP")
        )
    ),
)

df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "CMN_PRCT_LAST_CRDTL_DT",
    F.when(
        (F.col("CMN_PRCT_SK").isNull())
        | (trim(F.col("CMN_PRCT_LAST_CRDTL_DT")) == "NA")
        | (trim(F.col("CMN_PRCT_LAST_CRDTL_DT")) == "UNK")
        | (trim(F.col("CMN_PRCT_LAST_CRDTL_DT")) == ""),
        FORMAT_DATE_EE(
            F.lit("1753-01-01"), F.lit("DATE"), F.lit("DATE"), F.lit("SYBTIMESTAMP")
        ),
    ).otherwise(
        FORMAT_DATE_EE(
            F.col("CMN_PRCT_LAST_CRDTL_DT"), F.lit("DATE"), F.lit("DATE"), F.lit("SYBTIMESTAMP")
        )
    ),
)

df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "CMN_PRCT_FIRST_NM",
    F.when(
        (F.col("CMN_PRCT_SK").isNull())
        | (F.col("CMN_PRCT_FIRST_NM") == "NA")
        | (F.col("CMN_PRCT_FIRST_NM") == "UNK"),
        F.lit(" "),
    ).otherwise(F.col("CMN_PRCT_FIRST_NM")),
)

df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "CMN_PRCT_MIDINIT",
    F.when(
        (F.col("CMN_PRCT_SK").isNull())
        | (F.col("CMN_PRCT_MIDINIT").isNull())
        | (trim(F.col("CMN_PRCT_MIDINIT")) == "")
        | (F.col("CMN_PRCT_MIDINIT") == "UNK")
        | (F.col("CMN_PRCT_MIDINIT") == "NA"),
        F.lit(" "),
    ).otherwise(F.col("CMN_PRCT_MIDINIT")),
)

df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "CMN_PRCT_LAST_NM",
    F.when(
        (F.col("CMN_PRCT_SK").isNull())
        | (F.col("CMN_PRCT_LAST_NM") == "UNK")
        | (F.col("CMN_PRCT_LAST_NM") == "NA"),
        F.lit(" "),
    ).otherwise(F.col("CMN_PRCT_LAST_NM")),
)

df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "CMN_PRCT_ID",
    F.when(F.col("CMN_PRCT_SK").isNull(), F.lit(" ")).otherwise(F.col("CMN_PRCT_ID")),
)

df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "CMN_PRCT_SSN",
    F.when(
        (F.col("CMN_PRCT_SK") == "UNK") | (F.col("CMN_PRCT_SSN") == "NA"),
        F.lit(" "),
    ).otherwise(F.col("CMN_PRCT_SSN")),
)

df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "CMN_PRCT_TTL",
    F.when(
        F.col("CMN_PRCT_SK").isNull(),
        F.lit(" "),
    )
    .when(
        F.col("CMN_PRCT_TTL").isNull(),
        F.lit(" "),
    )
    .when(
        (F.col("CMN_PRCT_TTL") == "NA") | (F.col("CMN_PRCT_TTL") == "UNK"),
        F.lit(" "),
    )
    .otherwise(F.col("CMN_PRCT_TTL")),
)

df_xfrm_BusinessLogic = df_xfrm_BusinessLogic.withColumn(
    "LAST_UPDT_DT",
    StringToTimestamp(F.lit(LastUpdtDt), "%yyyy-%mm-%dd")
)

# --------------------------------------------------------------------------------
# Stage: seq_PROV_DIR_DM_PROV_csv_load (PxSequentialFile)
# --------------------------------------------------------------------------------
# Final select with column order and rpad for char/varchar columns
df_final = df_xfrm_BusinessLogic.select(
    F.col("PROV_ID").alias("PROV_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CMN_PRCT_GNDR_CD").alias("CMN_PRCT_GNDR_CD"),
    F.col("CMN_PRCT_GNDR_NM").alias("CMN_PRCT_GNDR_NM"),
    F.col("FCLTY_TYP_CD").alias("FCLTY_TYP_CD"),
    F.rpad(F.col("FCLTY_TYP_NM"), 55, " ").alias("FCLTY_TYP_NM"),
    F.col("PROV_BILL_SVC_ID").alias("PROV_BILL_SVC_ID"),
    F.col("PROV_BILL_SVC_NM").alias("PROV_BILL_SVC_NM"),
    F.col("PROV_ENTY_CD").alias("PROV_ENTY_CD"),
    F.col("PROV_ENTY_NM").alias("PROV_ENTY_NM"),
    F.col("PROV_PRCTC_TYP_CD").alias("PROV_PRCTC_TYP_CD"),
    F.col("PROV_PRCTC_TYP_NM").alias("PROV_PRCTC_TYP_NM"),
    F.col("PROV_REL_GRP_PROV_ID").alias("PROV_REL_GRP_PROV_ID"),
    F.col("PROV_REL_GRP_PROV_NM").alias("PROV_REL_GRP_PROV_NM"),
    F.col("PROV_REL_IPA_PROV_ID").alias("PROV_REL_IPA_PROV_ID"),
    F.col("PROV_REL_IPA_PROV_NM").alias("PROV_REL_IPA_PROV_NM"),
    F.col("PROV_SPEC_CD").alias("PROV_SPEC_CD"),
    F.col("PROV_SPEC_NM").alias("PROV_SPEC_NM"),
    F.col("PROV_TYP_CD").alias("PROV_TYP_CD"),
    F.col("PROV_TYP_NM").alias("PROV_TYP_NM"),
    F.rpad(F.col("ACTV_IN"), 1, " ").alias("ACTV_IN"),
    F.rpad(F.col("CNTGS_CNTY_PROV_IN"), 1, " ").alias("CNTGS_CNTY_PROV_IN"),
    F.rpad(F.col("ITS_HOME_GNRC_PROV_IN"), 1, " ").alias("ITS_HOME_GNRC_PROV_IN"),
    F.rpad(F.col("LEAPFROG_IN"), 1, " ").alias("LEAPFROG_IN"),
    F.rpad(F.col("LOCAL_BCBSKC_PROV_IN"), 1, " ").alias("LOCAL_BCBSKC_PROV_IN"),
    F.rpad(F.col("PAR_PROV_IN"), 1, " ").alias("PAR_PROV_IN"),
    F.rpad(F.col("CMN_PRCT_BRTH_DT"), 10, " ").alias("CMN_PRCT_BRTH_DT"),
    F.rpad(F.col("CMN_PRCT_LAST_CRDTL_DT"), 10, " ").alias("CMN_PRCT_LAST_CRDTL_DT"),
    F.col("CMN_PRCT_FIRST_NM").alias("CMN_PRCT_FIRST_NM"),
    F.rpad(F.col("CMN_PRCT_MIDINIT"), 1, " ").alias("CMN_PRCT_MIDINIT"),
    F.col("CMN_PRCT_LAST_NM").alias("CMN_PRCT_LAST_NM"),
    F.col("CMN_PRCT_ID").alias("CMN_PRCT_ID"),
    F.col("CMN_PRCT_SSN").alias("CMN_PRCT_SSN"),
    F.rpad(F.col("CMN_PRCT_TTL"), 10, " ").alias("CMN_PRCT_TTL"),
    F.col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.col("PROV_NM").alias("PROV_NM"),
    F.col("PROV_NPI").alias("PROV_NPI"),
    F.col("PROV_TAX_ID").alias("PROV_TAX_ID"),
    F.col("LAST_UPDT_DT").alias("LAST_UPDT_DT"),
    F.col("PROV_TXNMY_CD").alias("PROV_TXNMY_CD"),
)

write_files(
    df_final,
    f"{adls_path}/load/PROV_DIR_DM_PROV.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None,
)