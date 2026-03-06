# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                    Tao Luo:  Original Programming - 02/22/2006
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #                                   Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------                                   -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Tracy Davis             4/17/2008        Web Realignment IAD (3500)            Added new fields:                                                                        devlIDSNew                    Steph Goddard          05/18/2009
# MAGIC                                                                                                                            GRP_ENR_TYP_CD
# MAGIC                                                                                                                            GRP_ENR_TYP_NM
# MAGIC                                                                                                                            GRP_WEB_RSTRCT_IN
# MAGIC                                                                                                                            GRP_CUR_ANNV_DT
# MAGIC                                                                                                                            GRP_NEXT_ANNV_DT
# MAGIC                                                                                                                            GRP_REINST_DT
# MAGIC                                                                                                                            GRP_RNWL_DT
# MAGIC Ralph Tucker          6/11/2009       3500 - Web Realign                           Added new fields                                                                          devlIDSnew                     Steph Goddard           06/22/2009
# MAGIC 
# MAGIC Kimberly Doty           08/19/2009     4113 Member 360                             Added new fields                                                                          devlIDSnew                      Steph Goddard           08/31/2009
# MAGIC                                                                                                                          GRP_STTUS_CD
# MAGIC                                                                                                                          GRP_STTUS_NM
# MAGIC SAndrew               2009-10-01          4113 Mbr360                                  renamed folder its under, removed parms LastUpdtDt                   devlIDSnew                     Steph Goddard         10/17/2009
# MAGIC 
# MAGIC Kimberly Doty        01/25/2010        4044 Blue Renew                           Added fields: GRP_TERM_RSN_CD and 
# MAGIC                                                                                                                                      GRP_TERM_RSN_NM                                            IntegrateCurDevl                Steph Goddard          01/30/2010
# MAGIC Kalyan Neelam     2010-10-29          3346                                              Added two new fields on end -                                                       IntegrateNewDevl               Steph Goddard          11/04/2010
# MAGIC                                                                                                             GRP_MKTNG_TERR_ID,  
# MAGIC                                                                                                              IDS_LAST_UPDT_RUN_CYC_EXCTN_SK
# MAGIC 
# MAGIC Bhupinder Kaur     2013-07-25       5114                            Create Load File for DM Table MBRSH_DM_GRP                                            IntegrateWrhsDevl                      Jag Yelavarthi           2013-10-24
# MAGIC Kalyan Neelam     2015-11-04          5212                          Added Hosted_Grp_Lookup to drop Host group records                                   IntegrateDev1                  Bhoomi Dasari           11/5/2015
# MAGIC 
# MAGIC 
# MAGIC Karthik Chintalapani    2016-04-19     5212                      Changed the keys in the query in db2_Hosted_GRP_In stage                               IntegrateDev1                Kalyan Neelam            2016-04-19
# MAGIC                                                                                         to G.PRNT_GRP_SK = PG.PRNT_GRP_SK

# MAGIC Drop Hosted Groups
# MAGIC Write MBRSH_DM_GRP Data into a Sequential file for Load Job IdsDmMbrshDmGrpLoad.
# MAGIC Add Defaults and Null Handling.
# MAGIC Code SK lookups for Denormalization
# MAGIC Lookup Keys:
# MAGIC GRP_ID
# MAGIC MKTNG_TERR_SK
# MAGIC GRP_ST_CD_SK
# MAGIC GRP_EDI_ACCT_VNDR_CD_SK
# MAGIC GRP_STTUS_CD_SK
# MAGIC GRP_MKT_SIZE_CAT_CD_SK
# MAGIC GRP_TERM_RSN_CD_SK
# MAGIC Job Name: IdsDmMbrshDmGrpExtr
# MAGIC Read from source table GRP from IDS.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Retrieve job parameters
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
UWSOwner = get_widget_value('UWSOwner','')
CurrRunCycle = get_widget_value('CurrRunCycle','')

# Retrieve DB connection info for IDS
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

# Because there is an ODBC stage with no specified database secret, define a generic one:
odbc_secret_name = get_widget_value('odbc_secret_name','')
jdbc_url_odbc, jdbc_props_odbc = get_db_config(odbc_secret_name)

# ----------------------------------------------------------------------------
# Stage: db2_Hosted_GRP_In (DB2ConnectorPX, reading from IDS)
# ----------------------------------------------------------------------------
extract_query_db2_Hosted_GRP_In = f"""
SELECT distinct
G.GRP_ID
FROM {IDSOwner}.GRP G,
     {IDSOwner}.PRNT_GRP PG
WHERE
    G.PRNT_GRP_SK = PG.PRNT_GRP_SK
    AND PG.CLNT_ID <> 'HS'
    AND G.GRP_ID NOT LIKE '90%'
"""
df_db2_Hosted_GRP_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_Hosted_GRP_In)
    .load()
)

# ----------------------------------------------------------------------------
# Stage: dm_WEB_GRP_RSTRCT_in (ODBCConnectorPX)
# ----------------------------------------------------------------------------
extract_query_dm_WEB_GRP_RSTRCT_in = """
SELECT
GRP_ID
FROM  WEB_GRP_RSTRCT
"""
df_dm_WEB_GRP_RSTRCT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_odbc)
    .options(**jdbc_props_odbc)
    .option("query", extract_query_dm_WEB_GRP_RSTRCT_in)
    .load()
)

# ----------------------------------------------------------------------------
# Stage: db2_MKTNG_TERR_In (DB2ConnectorPX, reading from IDS)
# ----------------------------------------------------------------------------
extract_query_db2_MKTNG_TERR_In = f"""
SELECT
MKTNG_TERR.MKTNG_TERR_SK,
MKTNG_TERR.MKTNG_TERR_ID
FROM {IDSOwner}.MKTNG_TERR MKTNG_TERR
"""
df_db2_MKTNG_TERR_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_MKTNG_TERR_In)
    .load()
)

# ----------------------------------------------------------------------------
# Stage: db2_GRP_in (DB2ConnectorPX, reading from IDS)
# ----------------------------------------------------------------------------
extract_query_db2_GRP_in = f"""
SELECT
G.GRP_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
G.GRP_ID,
G.CRT_RUN_CYC_EXCTN_SK,
G.LAST_UPDT_RUN_CYC_EXCTN_SK,
G.CLNT_SK,
G.MKTNG_TERR_SK,
G.PRNT_GRP_SK,
G.GRP_BILL_LVL_CD_SK,
G.GRP_BUS_SUB_CAT_SH_NM_CD_SK,
G.GRP_EDI_ACCT_VNDR_CD_SK,
G.GRP_MKT_SIZE_CAT_CD_SK,
G.GRP_STTUS_CD_SK,
G.GRP_TERM_RSN_CD_SK,
G.BNF_CST_MDLER_IN,
G.CAP_IN,
G.DP_IN,
G.MULTI_OPT_IN,
G.WEB_PHYS_BNF_IN,
G.CUR_ANNV_DT_SK,
G.NEXT_ANNV_DT_SK,
G.ORIG_EFF_DT_SK,
G.REINST_DT_SK,
G.RNWL_DT_MO_DAY,
G.RNWL_DT_SK,
G.TERM_DT_SK,
G.GRP_UNIQ_KEY,
G.TOT_CNTR_CT,
G.TOT_ELIG_EMPL_CT,
G.TOT_EMPL_CT,
G.CNTCT_LAST_NM,
G.CNTCT_FIRST_NM,
G.CNTCT_MIDINIT,
G.CNTCT_TTL,
G.GRP_NM,
G.ADDR_LN_1,
G.ADDR_LN_2,
G.ADDR_LN_3,
G.CITY_NM,
G.GRP_ST_CD_SK,
G.POSTAL_CD,
G.CNTY_NM,
G.PHN_NO,
G.PHNEXT_NO,
G.FAX_NO,
G.FAX_EXT_NO,
G.EMAIL_ADDR_TX,
G.TAX_ID_NO,
G.SRC_SYS_LAST_UPDT_DT_SK,
G.SRC_SYS_LAST_UPDT_USER_SK
FROM {IDSOwner}.GRP G
LEFT JOIN {IDSOwner}.CD_MPPNG CD
    ON G.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
WHERE G.GRP_SK NOT IN (0, 1)
"""
df_db2_GRP_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_GRP_in)
    .load()
)

# ----------------------------------------------------------------------------
# Stage: db2_CD_MPPNG_Extr (DB2ConnectorPX, reading from IDS)
# ----------------------------------------------------------------------------
extract_query_db2_CD_MPPNG_Extr = f"""
SELECT
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
from {IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CD_MPPNG_Extr)
    .load()
)

# ----------------------------------------------------------------------------
# Stage: Cpy_Mppng_Cd (PxCopy)
#  This generates multiple output links, each selecting specific columns
# ----------------------------------------------------------------------------
df_Ref_Grp_Mrkt_Size_Lkp = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_Grp_Edi_Acct_Vndr_Lkp = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_Grp_Sttus_Lkp = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_Grp_Term_Rsn_Lkp = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_Grp_St_Cd_Lkp = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

# ----------------------------------------------------------------------------
# Stage: lkp_Codes (PxLookup)
#   Primary link: df_db2_GRP_in
#   Lookup links: 
#       df_Ref_Grp_St_Cd_Lkp            (left join on GRP_ST_CD_SK = CD_MPPNG_SK)
#       df_Ref_Grp_Edi_Acct_Vndr_Lkp    (left join on GRP_EDI_ACCT_VNDR_CD_SK = CD_MPPNG_SK)
#       df_dm_WEB_GRP_RSTRCT_in         (left join with no join condition in the DS job,
#                                        but we use GRP_ID for a meaningful join)
#       df_Ref_Grp_Mrkt_Size_Lkp        (left join on GRP_MKT_SIZE_CAT_CD_SK = CD_MPPNG_SK)
#       df_Ref_Grp_Sttus_Lkp            (left join on GRP_STTUS_CD_SK = CD_MPPNG_SK)
#       df_Ref_Grp_Term_Rsn_Lkp         (left join on GRP_TERM_RSN_CD_SK = CD_MPPNG_SK)
#       df_db2_MKTNG_TERR_In            (left join on MKTNG_TERR_SK = MKTNG_TERR_SK)
# ----------------------------------------------------------------------------
df_lkp_Codes_intermediate = (
    df_db2_GRP_in.alias("lnk_IdsDmMbrshDmGrpExtr_InAbc")
    .join(
        df_Ref_Grp_St_Cd_Lkp.alias("Ref_Grp_St_Cd_Lkp"),
        F.col("lnk_IdsDmMbrshDmGrpExtr_InAbc.GRP_ST_CD_SK") == F.col("Ref_Grp_St_Cd_Lkp.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Ref_Grp_Edi_Acct_Vndr_Lkp.alias("Ref_Grp_Edi_Acct_Vndr_Lkp"),
        F.col("lnk_IdsDmMbrshDmGrpExtr_InAbc.GRP_EDI_ACCT_VNDR_CD_SK") == F.col("Ref_Grp_Edi_Acct_Vndr_Lkp.CD_MPPNG_SK"),
        "left"
    )
    # Interpreting the no-join-condition reference link as matching on GRP_ID
    .join(
        df_dm_WEB_GRP_RSTRCT_in.alias("lnk_WEB_GRP_RSTRCT_In"),
        F.col("lnk_IdsDmMbrshDmGrpExtr_InAbc.GRP_ID") == F.col("lnk_WEB_GRP_RSTRCT_In.GRP_ID"),
        "left"
    )
    .join(
        df_Ref_Grp_Mrkt_Size_Lkp.alias("Ref_Grp_Mrkt_Size_Lkp"),
        F.col("lnk_IdsDmMbrshDmGrpExtr_InAbc.GRP_MKT_SIZE_CAT_CD_SK") == F.col("Ref_Grp_Mrkt_Size_Lkp.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Ref_Grp_Sttus_Lkp.alias("Ref_Grp_Sttus_Lkp"),
        F.col("lnk_IdsDmMbrshDmGrpExtr_InAbc.GRP_STTUS_CD_SK") == F.col("Ref_Grp_Sttus_Lkp.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Ref_Grp_Term_Rsn_Lkp.alias("Ref_Grp_Term_Rsn_Lkp"),
        F.col("lnk_IdsDmMbrshDmGrpExtr_InAbc.GRP_TERM_RSN_CD_SK") == F.col("Ref_Grp_Term_Rsn_Lkp.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_db2_MKTNG_TERR_In.alias("ref_MktngTerr_Lkp"),
        F.col("lnk_IdsDmMbrshDmGrpExtr_InAbc.MKTNG_TERR_SK") == F.col("ref_MktngTerr_Lkp.MKTNG_TERR_SK"),
        "left"
    )
)

# Now select the columns for the output link from lkp_Codes
df_lkp_Codes = df_lkp_Codes_intermediate.select(
    F.col("lnk_IdsDmMbrshDmGrpExtr_InAbc.GRP_SK").alias("GRP_SK"),
    F.col("lnk_IdsDmMbrshDmGrpExtr_InAbc.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_IdsDmMbrshDmGrpExtr_InAbc.GRP_ID").alias("GRP_ID"),
    F.col("lnk_IdsDmMbrshDmGrpExtr_InAbc.ORIG_EFF_DT_SK").alias("GRP_ORIG_EFF_DT_SK"),
    F.col("lnk_IdsDmMbrshDmGrpExtr_InAbc.TERM_DT_SK").alias("GRP_TERM_DT_SK"),
    F.col("lnk_IdsDmMbrshDmGrpExtr_InAbc.GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
    F.col("lnk_IdsDmMbrshDmGrpExtr_InAbc.GRP_NM").alias("GRP_NM"),
    F.col("lnk_IdsDmMbrshDmGrpExtr_InAbc.ADDR_LN_1").alias("ADDR_LN_1"),
    F.col("lnk_IdsDmMbrshDmGrpExtr_InAbc.ADDR_LN_2").alias("ADDR_LN_2"),
    F.col("lnk_IdsDmMbrshDmGrpExtr_InAbc.ADDR_LN_3").alias("ADDR_LN_3"),
    F.col("lnk_IdsDmMbrshDmGrpExtr_InAbc.CITY_NM").alias("GRP_CITY_NM"),
    F.col("Ref_Grp_St_Cd_Lkp.TRGT_CD").alias("GRP_ST_CD"),
    F.col("lnk_IdsDmMbrshDmGrpExtr_InAbc.POSTAL_CD").alias("GRP_POSTAL_CD"),
    F.col("lnk_IdsDmMbrshDmGrpExtr_InAbc.CNTY_NM").alias("GRP_CNTY_NM"),
    F.col("lnk_IdsDmMbrshDmGrpExtr_InAbc.PHN_NO").alias("GRP_PHN_NO"),
    F.col("lnk_IdsDmMbrshDmGrpExtr_InAbc.PHNEXT_NO").alias("GRP_PHN_EXT_NO"),
    F.col("lnk_IdsDmMbrshDmGrpExtr_InAbc.FAX_NO").alias("GRP_FAX_NO"),
    F.col("lnk_IdsDmMbrshDmGrpExtr_InAbc.FAX_EXT_NO").alias("GRP_FAX_EXT_NO"),
    F.col("lnk_IdsDmMbrshDmGrpExtr_InAbc.EMAIL_ADDR_TX").alias("GRP_EMAIL_ADDR_TX"),
    F.col("Ref_Grp_Edi_Acct_Vndr_Lkp.TRGT_CD").alias("GRP_EDI_ACCT_VNDR_CD"),
    F.col("Ref_Grp_Edi_Acct_Vndr_Lkp.TRGT_CD_NM").alias("GRP_EDI_ACCT_VNDR_NM"),
    F.col("Ref_Grp_Mrkt_Size_Lkp.TRGT_CD").alias("GRP_MKT_SIZE_CAT_CD"),
    F.col("Ref_Grp_Mrkt_Size_Lkp.TRGT_CD_NM").alias("GRP_MKT_SIZE_CAT_NM"),
    F.col("lnk_WEB_GRP_RSTRCT_In.GRP_ID").alias("GRP_WEB_RSTRT_IN"),
    F.col("lnk_IdsDmMbrshDmGrpExtr_InAbc.CUR_ANNV_DT_SK").alias("GRP_CUR_ANNV_DT_SK"),
    F.col("lnk_IdsDmMbrshDmGrpExtr_InAbc.NEXT_ANNV_DT_SK").alias("GRP_NEXT_ANNV_DT_SK"),
    F.col("lnk_IdsDmMbrshDmGrpExtr_InAbc.REINST_DT_SK").alias("GRP_REINST_DT_SK"),
    F.col("lnk_IdsDmMbrshDmGrpExtr_InAbc.RNWL_DT_SK").alias("GRP_RNWL_DT_SK"),
    F.col("Ref_Grp_Sttus_Lkp.TRGT_CD").alias("GRP_STTUS_CD"),
    F.col("Ref_Grp_Sttus_Lkp.TRGT_CD_NM").alias("GRP_STTUS_NM"),
    F.col("Ref_Grp_Term_Rsn_Lkp.TRGT_CD").alias("GRP_TERM_CD"),
    F.col("Ref_Grp_Term_Rsn_Lkp.TRGT_CD_NM").alias("GRP_TERM_NM"),
    F.col("ref_MktngTerr_Lkp.MKTNG_TERR_ID").alias("GRP_MKTNG_TERR_ID"),
    F.col("lnk_IdsDmMbrshDmGrpExtr_InAbc.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_IdsDmMbrshDmGrpExtr_InAbc.GRP_TERM_RSN_CD_SK").alias("GRP_TERM_RSN_CD_SK")
)

# ----------------------------------------------------------------------------
# Stage: Hosted_Grp_Lookup (PxLookup)
#   Primary link: df_lkp_Codes
#   Lookup link: df_db2_Hosted_GRP_In (inner join on GRP_ID)
# ----------------------------------------------------------------------------
df_Hosted_Grp_Lookup_intermediate = (
    df_lkp_Codes.alias("lnk_CodesLkpDataOut1")
    .join(
        df_db2_Hosted_GRP_In.alias("Host_Grp_Id_lookup"),
        F.col("lnk_CodesLkpDataOut1.GRP_ID") == F.col("Host_Grp_Id_lookup.GRP_ID"),
        "inner"
    )
)

df_Hosted_Grp_Lookup = df_Hosted_Grp_Lookup_intermediate.select(
    F.col("lnk_CodesLkpDataOut1.GRP_SK").alias("GRP_SK"),
    F.col("lnk_CodesLkpDataOut1.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_CodesLkpDataOut1.GRP_ID").alias("GRP_ID"),
    F.col("lnk_CodesLkpDataOut1.GRP_ORIG_EFF_DT_SK").alias("GRP_ORIG_EFF_DT_SK"),
    F.col("lnk_CodesLkpDataOut1.GRP_TERM_DT_SK").alias("GRP_TERM_DT_SK"),
    F.col("lnk_CodesLkpDataOut1.GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
    F.col("lnk_CodesLkpDataOut1.GRP_NM").alias("GRP_NM"),
    F.col("lnk_CodesLkpDataOut1.ADDR_LN_1").alias("ADDR_LN_1"),
    F.col("lnk_CodesLkpDataOut1.ADDR_LN_2").alias("ADDR_LN_2"),
    F.col("lnk_CodesLkpDataOut1.ADDR_LN_3").alias("ADDR_LN_3"),
    F.col("lnk_CodesLkpDataOut1.GRP_CITY_NM").alias("GRP_CITY_NM"),
    F.col("lnk_CodesLkpDataOut1.GRP_ST_CD").alias("GRP_ST_CD"),
    F.col("lnk_CodesLkpDataOut1.GRP_POSTAL_CD").alias("GRP_POSTAL_CD"),
    F.col("lnk_CodesLkpDataOut1.GRP_CNTY_NM").alias("GRP_CNTY_NM"),
    F.col("lnk_CodesLkpDataOut1.GRP_PHN_NO").alias("GRP_PHN_NO"),
    F.col("lnk_CodesLkpDataOut1.GRP_PHN_EXT_NO").alias("GRP_PHN_EXT_NO"),
    F.col("lnk_CodesLkpDataOut1.GRP_FAX_NO").alias("GRP_FAX_NO"),
    F.col("lnk_CodesLkpDataOut1.GRP_FAX_EXT_NO").alias("GRP_FAX_EXT_NO"),
    F.col("lnk_CodesLkpDataOut1.GRP_EMAIL_ADDR_TX").alias("GRP_EMAIL_ADDR_TX"),
    F.col("lnk_CodesLkpDataOut1.GRP_EDI_ACCT_VNDR_CD").alias("GRP_EDI_ACCT_VNDR_CD"),
    F.col("lnk_CodesLkpDataOut1.GRP_EDI_ACCT_VNDR_NM").alias("GRP_EDI_ACCT_VNDR_NM"),
    F.col("lnk_CodesLkpDataOut1.GRP_MKT_SIZE_CAT_CD").alias("GRP_MKT_SIZE_CAT_CD"),
    F.col("lnk_CodesLkpDataOut1.GRP_MKT_SIZE_CAT_NM").alias("GRP_MKT_SIZE_CAT_NM"),
    F.col("lnk_CodesLkpDataOut1.GRP_WEB_RSTRT_IN").alias("GRP_WEB_RSTRT_IN"),
    F.col("lnk_CodesLkpDataOut1.GRP_CUR_ANNV_DT_SK").alias("GRP_CUR_ANNV_DT_SK"),
    F.col("lnk_CodesLkpDataOut1.GRP_NEXT_ANNV_DT_SK").alias("GRP_NEXT_ANNV_DT_SK"),
    F.col("lnk_CodesLkpDataOut1.GRP_REINST_DT_SK").alias("GRP_REINST_DT_SK"),
    F.col("lnk_CodesLkpDataOut1.GRP_RNWL_DT_SK").alias("GRP_RNWL_DT_SK"),
    F.col("lnk_CodesLkpDataOut1.GRP_STTUS_CD").alias("GRP_STTUS_CD"),
    F.col("lnk_CodesLkpDataOut1.GRP_STTUS_NM").alias("GRP_STTUS_NM"),
    F.col("lnk_CodesLkpDataOut1.GRP_TERM_CD").alias("GRP_TERM_CD"),
    F.col("lnk_CodesLkpDataOut1.GRP_TERM_NM").alias("GRP_TERM_NM"),
    F.col("lnk_CodesLkpDataOut1.GRP_MKTNG_TERR_ID").alias("GRP_MKTNG_TERR_ID"),
    F.col("lnk_CodesLkpDataOut1.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_CodesLkpDataOut1.GRP_TERM_RSN_CD_SK").alias("GRP_TERM_RSN_CD_SK")
)

# ----------------------------------------------------------------------------
# Stage: xfrm_BusinessLogic (CTransformerStage)
# ----------------------------------------------------------------------------
# Stage variable:
svDefaultDate = '1753-01-01 00:00:00'

df_xfrm_BusinessLogic_intermediate = df_Hosted_Grp_Lookup.withColumn(
    "SRC_SYS_CD",
    F.col("SRC_SYS_CD")
).withColumn(
    "GRP_ID",
    F.col("GRP_ID")
).withColumn(
    "GRP_ORIG_EFF_DT",
    F.to_timestamp(F.col("GRP_ORIG_EFF_DT_SK"), "yyyy-MM-dd")
).withColumn(
    "GRP_TERM_DT",
    F.to_timestamp(F.col("GRP_TERM_DT_SK"), "yyyy-MM-dd")
).withColumn(
    "GRP_UNIQ_KEY",
    F.col("GRP_UNIQ_KEY")
).withColumn(
    "GRP_NM",
    F.when(F.col("GRP_NM").isNull(), F.lit(" ")).otherwise(F.col("GRP_NM"))
).withColumn(
    "GRP_ADDR_LN_1",
    F.col("ADDR_LN_1")
).withColumn(
    "GRP_ADDR_LN_2",
    F.col("ADDR_LN_2")
).withColumn(
    "GRP_ADDR_LN_3",
    F.col("ADDR_LN_3")
).withColumn(
    "GRP_CITY_NM",
    F.col("GRP_CITY_NM")
).withColumn(
    "GRP_ST_CD",
    F.when(F.col("GRP_ST_CD").isNull(), F.lit("UNK ")).otherwise(F.col("GRP_ST_CD"))
).withColumn(
    "GRP_POSTAL_CD",
    F.col("GRP_POSTAL_CD")
).withColumn(
    "GRP_CNTY_NM",
    F.col("GRP_CNTY_NM")
).withColumn(
    "GRP_PHN_NO",
    F.col("GRP_PHN_NO")
).withColumn(
    "GRP_PHN_EXT_NO",
    F.col("GRP_PHN_EXT_NO")
).withColumn(
    "GRP_FAX_NO",
    F.col("GRP_FAX_NO")
).withColumn(
    "GRP_FAX_EXT_NO",
    F.col("GRP_FAX_EXT_NO")
).withColumn(
    "GRP_EMAIL_ADDR",
    # Replicating "Trim(lnk_CodesLkpDataOut.GRP_EMAIL_ADDR_TX, '^', 'A')" with a best-effort removal of '^'
    F.regexp_replace(F.col("GRP_EMAIL_ADDR_TX"), r'(^\^+|\^+$)', '')
).withColumn(
    "LAST_UPDT_RUN_CYC_NO",
    F.lit(CurrRunCycle)
).withColumn(
    "GRP_EDI_ACCT_VNDR_CD",
    F.when(
        F.col("GRP_EDI_ACCT_VNDR_CD").isNull() | (F.trim(F.col("GRP_EDI_ACCT_VNDR_CD")) == ""),
        F.lit("UNK ")
    ).otherwise(F.col("GRP_EDI_ACCT_VNDR_CD"))
).withColumn(
    "GRP_EDI_ACCT_VNDR_NM",
    F.when(
        F.col("GRP_EDI_ACCT_VNDR_NM").isNull() | (F.trim(F.col("GRP_EDI_ACCT_VNDR_NM")) == ""),
        F.lit("UNK ")
    ).otherwise(F.col("GRP_EDI_ACCT_VNDR_NM"))
).withColumn(
    "GRP_MKT_SIZE_CAT_CD",
    F.when(
        F.col("GRP_MKT_SIZE_CAT_CD").isNull() | (F.trim(F.col("GRP_MKT_SIZE_CAT_CD")) == ""),
        F.lit("UNK")
    ).otherwise(F.col("GRP_MKT_SIZE_CAT_CD"))
).withColumn(
    "GRP_MKT_SIZE_CAT_NM",
    F.when(
        F.col("GRP_MKT_SIZE_CAT_NM").isNull() | (F.trim(F.col("GRP_MKT_SIZE_CAT_NM")) == ""),
        F.lit("UNK")
    ).otherwise(F.col("GRP_MKT_SIZE_CAT_NM"))
).withColumn(
    "GRP_WEB_RSTRCT_IN",
    F.when(
        F.col("GRP_WEB_RSTRT_IN").isNull() | (F.col("GRP_WEB_RSTRT_IN") == ""),
        F.lit("N")
    ).otherwise(F.lit("Y"))
).withColumn(
    "GRP_CUR_ANNV_DT",
    F.when(
        (F.trim(F.col("GRP_CUR_ANNV_DT_SK")) == "UNK") | (F.trim(F.col("GRP_CUR_ANNV_DT_SK")) == "NA"),
        F.lit(svDefaultDate)
    ).otherwise(F.to_timestamp(F.col("GRP_CUR_ANNV_DT_SK"), "yyyy-MM-dd"))
).withColumn(
    "GRP_NEXT_ANNV_DT",
    F.when(
        (F.trim(F.col("GRP_NEXT_ANNV_DT_SK")) == "UNK") | (F.trim(F.col("GRP_NEXT_ANNV_DT_SK")) == "NA"),
        F.lit(svDefaultDate)
    ).otherwise(F.to_timestamp(F.col("GRP_NEXT_ANNV_DT_SK"), "yyyy-MM-dd"))
).withColumn(
    "GRP_REINST_DT",
    F.when(
        (F.trim(F.col("GRP_REINST_DT_SK")) == "UNK") | (F.trim(F.col("GRP_REINST_DT_SK")) == "NA"),
        F.lit(svDefaultDate)
    ).otherwise(F.to_timestamp(F.col("GRP_REINST_DT_SK"), "yyyy-MM-dd"))
).withColumn(
    "GRP_RNWL_DT",
    F.when(
        (F.trim(F.col("GRP_RNWL_DT_SK")) == "UNK") | (F.trim(F.col("GRP_RNWL_DT_SK")) == "NA"),
        F.lit(svDefaultDate)
    ).otherwise(F.to_timestamp(F.col("GRP_RNWL_DT_SK"), "yyyy-MM-dd"))
).withColumn(
    "GRP_STTUS_CD",
    F.when(F.col("GRP_STTUS_CD").isNull(), F.lit(None)).otherwise(F.col("GRP_STTUS_CD"))
).withColumn(
    "GRP_STTUS_NM",
    F.when(F.col("GRP_STTUS_NM").isNull(), F.lit(None)).otherwise(F.col("GRP_STTUS_NM"))
).withColumn(
    "GRP_TERM_RSN_CD",
    F.when(
        (F.col("GRP_TERM_RSN_CD_SK") == 0) | (F.col("GRP_TERM_RSN_CD_SK") == 1),
        F.lit(None)
    ).otherwise(F.col("GRP_TERM_CD"))
).withColumn(
    "GRP_TERM_RSN_NM",
    F.when(
        (F.col("GRP_TERM_RSN_CD_SK") == 0) | (F.col("GRP_TERM_RSN_CD_SK") == 1),
        F.lit(None)
    ).otherwise(F.col("GRP_TERM_NM"))
).withColumn(
    "GRP_MKTNG_TERR_ID",
    F.when(F.col("GRP_MKTNG_TERR_ID").isNull(), F.lit("")).otherwise(F.col("GRP_MKTNG_TERR_ID"))
).withColumn(
    "IDS_LAST_UPDT_RUN_CYC_EXCTN_SK",
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# Now apply rpad for columns that have "char" definitions in the final output.
df_xfrm_BusinessLogic = (
    df_xfrm_BusinessLogic_intermediate
    .withColumn("GRP_PHN_EXT_NO", F.rpad(F.col("GRP_PHN_EXT_NO"), 5, " "))
    .withColumn("GRP_FAX_EXT_NO", F.rpad(F.col("GRP_FAX_EXT_NO"), 5, " "))
    .withColumn("GRP_WEB_RSTRCT_IN", F.rpad(F.col("GRP_WEB_RSTRCT_IN"), 1, " "))
)

# Select final columns in DataStage order
df_seq_MBRSH_DM_GRP_csv_load = df_xfrm_BusinessLogic.select(
    "SRC_SYS_CD",
    "GRP_ID",
    "GRP_ORIG_EFF_DT",
    "GRP_TERM_DT",
    "GRP_UNIQ_KEY",
    "GRP_NM",
    "GRP_ADDR_LN_1",
    "GRP_ADDR_LN_2",
    "GRP_ADDR_LN_3",
    "GRP_CITY_NM",
    "GRP_ST_CD",
    "GRP_POSTAL_CD",
    "GRP_CNTY_NM",
    "GRP_PHN_NO",
    "GRP_PHN_EXT_NO",
    "GRP_FAX_NO",
    "GRP_FAX_EXT_NO",
    "GRP_EMAIL_ADDR",
    "LAST_UPDT_RUN_CYC_NO",
    "GRP_EDI_ACCT_VNDR_CD",
    "GRP_EDI_ACCT_VNDR_NM",
    "GRP_MKT_SIZE_CAT_CD",
    "GRP_MKT_SIZE_CAT_NM",
    "GRP_WEB_RSTRCT_IN",
    "GRP_CUR_ANNV_DT",
    "GRP_NEXT_ANNV_DT",
    "GRP_REINST_DT",
    "GRP_RNWL_DT",
    "GRP_STTUS_CD",
    "GRP_STTUS_NM",
    "GRP_TERM_RSN_CD",
    "GRP_TERM_RSN_NM",
    "GRP_MKTNG_TERR_ID",
    "IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"
)

# ----------------------------------------------------------------------------
# Stage: seq_MBRSH_DM_GRP_csv_load (PxSequentialFile)
#   Write a delimited file with no header, "^" as quote, overwrite mode
# ----------------------------------------------------------------------------
write_files(
    df_seq_MBRSH_DM_GRP_csv_load,
    f"{adls_path}/load/MBRSH_DM_GRP.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)