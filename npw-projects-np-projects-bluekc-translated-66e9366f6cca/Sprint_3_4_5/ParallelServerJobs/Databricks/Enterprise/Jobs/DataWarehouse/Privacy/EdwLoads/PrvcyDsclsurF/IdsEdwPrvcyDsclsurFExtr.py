# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                                                                       Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------                                             ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                4/3/2007           CDS Sunset/3279          Originally Programmed                                                                  devlEDW10                   Steph Goddard            4/6/07
# MAGIC SAndrew                         2010-04-09        Production Abend          If all records on EDW already, empty load file - abned                                               
# MAGIC Steph Goddard               7/15/10             TTR-630                        changed field name PRVCY_EXTRNL_ENTY_CLS_TYP_CD_S  EnterpriseNewDevl       SAndrew                     2010-10-01
# MAGIC                                                                                                         to PRVCY_EXTL_ENTY_CLS_TYP_CD_SK       
# MAGIC 
# MAGIC Bhoomi Dasari                2/14/2013         TTR-1534                     Updated address fields from length of 40 to 80                              EnterpriseNewDevl          Kalyan Neelam            2013-03-01
# MAGIC 
# MAGIC 
# MAGIC Srikanth Mettpalli             2013-10-01          5114                          Original Programming                                                                      EnterpriseWrhsDevl     Peter Marshall              1/17/2014       
# MAGIC                                                                                                       (Server to Parallel Conversion)      
# MAGIC 
# MAGIC Manasa Andru                2015-04-02       TFS - 1309 and 1310      Added logic to filter the 'NA' record in the extract                          EnterpriseNewDevl         Kalyan Neelam             2015-04-03
# MAGIC                                                                                                                      SQL of all the DB2 stages.
# MAGIC 
# MAGIC Venkatesh Babu             20210723         US402345                      Derivation for PRVCY_RECPNT_EXT_ENTY_ZIP_CD_4             EnterpriseDev1              Reddy Sanam             2021-07-28
# MAGIC                                                                                                       PRVCY_RECPNT_EXT_ENTY_ZIP_CD_5 both changed 
# MAGIC                                                                                                          as POSTAL_CD is not nullable.
# MAGIC                                                                                                            Removed db/lookup to pass Histroic data
# MAGIC 
# MAGIC Vamsi Aripaka                2023-01-05        US 571302                    Updated the join key column in Stage Lkp_PrvcyExtrnl                   EnterpriseDevB/Dev2   Jeyaprasanna             2023-01-09
# MAGIC 
# MAGIC Revathi Boojireddy         2023-03-06          US 578370                Added db2_PRVCY_DSCLSUR_F stage and updated logic in          EnterpriseDevB/Dev2   Jeyaprasanna            2023-03-13
# MAGIC                                                                                                      xfrm_BusinessLogic transformer stage for columns 
# MAGIC                                                                                                     PRVCY_DSCLSUR_ASSOC_EXTR_DT_SK and   
# MAGIC                                                                                        .             PRVCY_DSCLSUR_ASSOC_EXTR_IN

# MAGIC Job Name: IdsEdwPrvcyDsclsurFExtr
# MAGIC Write PRVCY_DSCLSUR_F Data into a Sequential file for Load Job IdsEdwPrvcyDsclsurFLoad.
# MAGIC Add Defaults and Null Handling.
# MAGIC Code SK lookups for Denormalization
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Business Logic - Only lets through records we have not seen yet in EDW.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
EDWOwner = get_widget_value('EDWOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
edw_secret_name = get_widget_value('edw_secret_name','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')
CurrRunCycle = get_widget_value('CurrRunCycle','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)

extract_query = f"SELECT PRSN.PRVCY_EXTRNL_ENTY_UNIQ_KEY,PRSN.FIRST_NM,PRSN.MIDINIT,PRSN.LAST_NM FROM {IDSOwner}.PRVCY_EXTRNL_PRSN PRSN;"
df_db2_PrvcyExtrnlPrsn_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query)
    .load()
)

extract_query = f"SELECT ORG.PRVCY_EXTRNL_ENTY_UNIQ_KEY,ORG.ORG_NM FROM {IDSOwner}.PRVCY_EXTRNL_ORG ORG WHERE ORG.ORG_NM <> 'NA';"
df_db2_PrvcyExtrnlOrg_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query)
    .load()
)

extract_query = f"SELECT MBR_D.MBR_SK, MBR_D.MBR_ID FROM {EDWOwner}.MBR_D MBR_D;"
df_db2_Mbr_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", extract_query)
    .load()
)

extract_query = f"SELECT MBR.PRVCY_EXTRNL_MBR_SK, MBR.PRVCY_EXTRNL_MBR_ID FROM {EDWOwner}.PRVCY_EXTRNL_MBR_D MBR;"
df_db2_PrvcyExtrnlMbr_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", extract_query)
    .load()
)

extract_query = f"SELECT CD_MPPNG_SK,COALESCE(TRGT_CD,'UNK') TRGT_CD,COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM FROM {IDSOwner}.CD_MPPNG"
df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query)
    .load()
)

df_cpy_cd_mppng_Ref_PrvcyMbrSrcCd_Lkup = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_cpy_cd_mppng_Ref_PrvcyDsclCatCd1_Lkup = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_cpy_cd_mppng_Ref_PrvcyDsclCatCd2_Lkup = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_cpy_cd_mppng_Ref_PrvcyDsclCatCd3_Lkup = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_cpy_cd_mppng_Ref_PrvcyDsclCatCd4_Lkup = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_cpy_cd_mppng_Ref_PrvcyExtrnlEntyAddrCd_Lkup = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_cpy_cd_mppng_Ref_PrvcyDsclRsnCd_Lkup = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_cpy_cd_mppng_Ref_PrvcyEntyClsTypCd_Lkup = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_cpy_cd_mppng_Ref_PrvcyDsclMethCd_Lkup = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

extract_query = f"""
SELECT
DSCLSUR.PRVCY_DSCLSUR_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
DSCLSUR.PRVCY_MBR_UNIQ_KEY,
DSCLSUR.SEQ_NO,
DSCLSUR.PRVCY_MBR_SRC_CD_SK,
DSCLSUR.PRVCY_DSCLSUR_ENTY_SK,
DSCLSUR.MBR_SK,
DSCLSUR.PRVCY_EXTRNL_MBR_SK,
DSCLSUR.PRVCY_RECPNT_EXTRNL_ENTY_SK,
DSCLSUR.PRVCY_DSCLSUR_CAT_1_CD_SK,
DSCLSUR.PRVCY_DSCLSUR_CAT_2_CD_SK,
DSCLSUR.PRVCY_DSCLSUR_CAT_3_CD_SK,
DSCLSUR.PRVCY_DSCLSUR_CAT_4_CD_SK,
DSCLSUR.PRVCY_DSCLSUR_METH_CD_SK,
DSCLSUR.PRVCY_DSCLSUR_RSN_CD_SK,
DSCLSUR.CRT_DT_SK,
DSCLSUR.DSCLSUR_DT_SK,
DSCLSUR.DSCLSUR_DESC,
DSCLSUR.SRC_SYS_LAST_UPDT_DT_SK,
DSCLSUR.SRC_SYS_LAST_UPDT_USER_SK
FROM {IDSOwner}.PRVCY_DSCLSUR DSCLSUR
LEFT JOIN {IDSOwner}.CD_MPPNG CD
ON DSCLSUR.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
"""
df_db2_Prvcy_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query)
    .load()
)

extract_query = f"""
SELECT
ENTY.PRVCY_EXTRNL_ENTY_SK,
ENTY.PRVCY_EXTL_ENTY_CLS_TYP_CD_SK,
ENTY.PRVCY_EXTRNL_ENTY_UNIQ_KEY
FROM {IDSOwner}.PRVCY_EXTRNL_ENTY ENTY
WHERE ENTY.PRVCY_EXTRNL_ENTY_SK <> 1
"""
df_db2_PrvcyExtrnl_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query)
    .load()
)

extract_query = f"""
SELECT
ADDR.PRVCY_EXTRNL_ENTY_UNIQ_KEY,
ADDR.ADDR_LN_1,
ADDR.ADDR_LN_2,
ADDR.ADDR_LN_3,
ADDR.CITY_NM,
ADDR.PRVCY_EXTL_ENTY_ADDR_ST_CD_SK,
ADDR.POSTAL_CD,
ADDR.PRVCY_EXTRNL_ENTY_ADDR_SK
FROM {IDSOwner}.PRVCY_EXTRNL_ENTY_ADDR ADDR
WHERE ADDR.PRVCY_EXTRNL_ENTY_ADDR_SK <> 1
ORDER BY (-1) * (ADDR.PRVCY_EXTRNL_ENTY_ADDR_SK)
"""
df_db2_PrvcyExtrnlEntyAddr_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query)
    .load()
)

df_rdp_Addr_dedup = dedup_sort(
    df_db2_PrvcyExtrnlEntyAddr_in,
    ["PRVCY_EXTRNL_ENTY_UNIQ_KEY"],
    [("PRVCY_EXTRNL_ENTY_ADDR_SK","D")]
).drop("PRVCY_EXTRNL_ENTY_ADDR_SK")

df_rdp_Addr = df_rdp_Addr_dedup.select(
    F.col("PRVCY_EXTRNL_ENTY_UNIQ_KEY").alias("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
    F.col("ADDR_LN_1").alias("ADDR_LN_1"),
    F.col("ADDR_LN_2").alias("ADDR_LN_2"),
    F.col("ADDR_LN_3").alias("ADDR_LN_3"),
    F.col("CITY_NM").alias("CITY_NM"),
    F.col("PRVCY_EXTL_ENTY_ADDR_ST_CD_SK").alias("PRVCY_EXTL_ENTY_ADDR_ST_CD_SK"),
    F.col("POSTAL_CD").alias("POSTAL_CD")
)

extract_query = f"""
SELECT
DSCLSURF.PRVCY_DSCLSUR_SK,
DSCLSURF.PRVCY_DSCLSUR_ASSOC_EXTR_DT_SK,
DSCLSURF.PRVCY_DSCLSUR_ASSOC_EXTR_IN
FROM {EDWOwner}.PRVCY_DSCLSUR_F DSCLSURF
"""
df_db2_PRVCY_DSCLSUR_F = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", extract_query)
    .load()
)

df_Lkp_PrvcyExtrnl_join1 = df_db2_Prvcy_in.alias("lnk_IdsEdwPrvcyDsclsurFExtr_InABC") \
    .join(
        df_db2_PrvcyExtrnl_in.alias("Ref_PrvcyExtrnl_Lkup"),
        on=[F.col("lnk_IdsEdwPrvcyDsclsurFExtr_InABC.PRVCY_RECPNT_EXTRNL_ENTY_SK")
            ==F.col("Ref_PrvcyExtrnl_Lkup.PRVCY_EXTRNL_ENTY_SK")],
        how="left"
    ) \
    .join(
        df_db2_PRVCY_DSCLSUR_F.alias("Ref_PRVCY_DSCLSUR_F"),
        on=[F.col("lnk_IdsEdwPrvcyDsclsurFExtr_InABC.PRVCY_DSCLSUR_SK")
            ==F.col("Ref_PRVCY_DSCLSUR_F.PRVCY_DSCLSUR_SK")],
        how="left"
    ) \
    .select(
        F.col("lnk_IdsEdwPrvcyDsclsurFExtr_InABC.PRVCY_DSCLSUR_SK").alias("PRVCY_DSCLSUR_SK"),
        F.col("lnk_IdsEdwPrvcyDsclsurFExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_IdsEdwPrvcyDsclsurFExtr_InABC.PRVCY_MBR_UNIQ_KEY").alias("PRVCY_MBR_UNIQ_KEY"),
        F.col("lnk_IdsEdwPrvcyDsclsurFExtr_InABC.SEQ_NO").alias("SEQ_NO"),
        F.col("lnk_IdsEdwPrvcyDsclsurFExtr_InABC.PRVCY_MBR_SRC_CD_SK").alias("PRVCY_MBR_SRC_CD_SK"),
        F.col("lnk_IdsEdwPrvcyDsclsurFExtr_InABC.PRVCY_DSCLSUR_ENTY_SK").alias("PRVCY_DSCLSUR_ENTY_SK"),
        F.col("lnk_IdsEdwPrvcyDsclsurFExtr_InABC.MBR_SK").alias("MBR_SK"),
        F.col("lnk_IdsEdwPrvcyDsclsurFExtr_InABC.PRVCY_EXTRNL_MBR_SK").alias("PRVCY_EXTRNL_MBR_SK"),
        F.col("lnk_IdsEdwPrvcyDsclsurFExtr_InABC.PRVCY_RECPNT_EXTRNL_ENTY_SK").alias("PRVCY_RECPNT_EXTRNL_ENTY_SK"),
        F.col("lnk_IdsEdwPrvcyDsclsurFExtr_InABC.PRVCY_DSCLSUR_CAT_1_CD_SK").alias("PRVCY_DSCLSUR_CAT_1_CD_SK"),
        F.col("lnk_IdsEdwPrvcyDsclsurFExtr_InABC.PRVCY_DSCLSUR_CAT_2_CD_SK").alias("PRVCY_DSCLSUR_CAT_2_CD_SK"),
        F.col("lnk_IdsEdwPrvcyDsclsurFExtr_InABC.PRVCY_DSCLSUR_CAT_3_CD_SK").alias("PRVCY_DSCLSUR_CAT_3_CD_SK"),
        F.col("lnk_IdsEdwPrvcyDsclsurFExtr_InABC.PRVCY_DSCLSUR_CAT_4_CD_SK").alias("PRVCY_DSCLSUR_CAT_4_CD_SK"),
        F.col("lnk_IdsEdwPrvcyDsclsurFExtr_InABC.PRVCY_DSCLSUR_METH_CD_SK").alias("PRVCY_DSCLSUR_METH_CD_SK"),
        F.col("lnk_IdsEdwPrvcyDsclsurFExtr_InABC.PRVCY_DSCLSUR_RSN_CD_SK").alias("PRVCY_DSCLSUR_RSN_CD_SK"),
        F.col("lnk_IdsEdwPrvcyDsclsurFExtr_InABC.CRT_DT_SK").alias("CRT_DT_SK"),
        F.col("lnk_IdsEdwPrvcyDsclsurFExtr_InABC.DSCLSUR_DT_SK").alias("DSCLSUR_DT_SK"),
        F.col("lnk_IdsEdwPrvcyDsclsurFExtr_InABC.DSCLSUR_DESC").alias("DSCLSUR_DESC"),
        F.col("lnk_IdsEdwPrvcyDsclsurFExtr_InABC.SRC_SYS_LAST_UPDT_DT_SK").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        F.col("lnk_IdsEdwPrvcyDsclsurFExtr_InABC.SRC_SYS_LAST_UPDT_USER_SK").alias("SRC_SYS_LAST_UPDT_USER_SK"),
        F.col("Ref_PrvcyExtrnl_Lkup.PRVCY_EXTL_ENTY_CLS_TYP_CD_SK").alias("PRVCY_EXTL_ENTY_CLS_TYP_CD_SK"),
        F.col("Ref_PrvcyExtrnl_Lkup.PRVCY_EXTRNL_ENTY_UNIQ_KEY").alias("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
        F.col("Ref_PRVCY_DSCLSUR_F.PRVCY_DSCLSUR_ASSOC_EXTR_DT_SK").alias("PRVCY_DSCLSUR_ASSOC_EXTR_DT_SK"),
        F.col("Ref_PRVCY_DSCLSUR_F.PRVCY_DSCLSUR_ASSOC_EXTR_IN").alias("PRVCY_DSCLSUR_ASSOC_EXTR_IN")
    )

df_Lkp_Prvcy_Addr = df_Lkp_PrvcyExtrnl_join1.alias("Lnk_Lkp1_Data") \
    .join(
        df_rdp_Addr.alias("Ref_PrvcyExtrnlEntyAddr_Lkup"),
        on=[F.col("Lnk_Lkp1_Data.PRVCY_EXTRNL_ENTY_UNIQ_KEY")
            ==F.col("Ref_PrvcyExtrnlEntyAddr_Lkup.PRVCY_EXTRNL_ENTY_UNIQ_KEY")],
        how="left"
    ) \
    .select(
        F.col("Lnk_Lkp1_Data.PRVCY_DSCLSUR_SK").alias("PRVCY_DSCLSUR_SK"),
        F.col("Lnk_Lkp1_Data.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Lnk_Lkp1_Data.PRVCY_MBR_UNIQ_KEY").alias("PRVCY_MBR_UNIQ_KEY"),
        F.col("Lnk_Lkp1_Data.SEQ_NO").alias("SEQ_NO"),
        F.col("Lnk_Lkp1_Data.PRVCY_MBR_SRC_CD_SK").alias("PRVCY_MBR_SRC_CD_SK"),
        F.col("Lnk_Lkp1_Data.PRVCY_DSCLSUR_ENTY_SK").alias("PRVCY_DSCLSUR_ENTY_SK"),
        F.col("Lnk_Lkp1_Data.MBR_SK").alias("MBR_SK"),
        F.col("Lnk_Lkp1_Data.PRVCY_EXTRNL_MBR_SK").alias("PRVCY_EXTRNL_MBR_SK"),
        F.col("Lnk_Lkp1_Data.PRVCY_RECPNT_EXTRNL_ENTY_SK").alias("PRVCY_RECPNT_EXTRNL_ENTY_SK"),
        F.col("Lnk_Lkp1_Data.PRVCY_DSCLSUR_CAT_1_CD_SK").alias("PRVCY_DSCLSUR_CAT_1_CD_SK"),
        F.col("Lnk_Lkp1_Data.PRVCY_DSCLSUR_CAT_2_CD_SK").alias("PRVCY_DSCLSUR_CAT_2_CD_SK"),
        F.col("Lnk_Lkp1_Data.PRVCY_DSCLSUR_CAT_3_CD_SK").alias("PRVCY_DSCLSUR_CAT_3_CD_SK"),
        F.col("Lnk_Lkp1_Data.PRVCY_DSCLSUR_CAT_4_CD_SK").alias("PRVCY_DSCLSUR_CAT_4_CD_SK"),
        F.col("Lnk_Lkp1_Data.PRVCY_DSCLSUR_METH_CD_SK").alias("PRVCY_DSCLSUR_METH_CD_SK"),
        F.col("Lnk_Lkp1_Data.PRVCY_DSCLSUR_RSN_CD_SK").alias("PRVCY_DSCLSUR_RSN_CD_SK"),
        F.col("Lnk_Lkp1_Data.CRT_DT_SK").alias("CRT_DT_SK"),
        F.col("Lnk_Lkp1_Data.DSCLSUR_DT_SK").alias("DSCLSUR_DT_SK"),
        F.col("Lnk_Lkp1_Data.DSCLSUR_DESC").alias("DSCLSUR_DESC"),
        F.col("Lnk_Lkp1_Data.SRC_SYS_LAST_UPDT_DT_SK").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        F.col("Lnk_Lkp1_Data.SRC_SYS_LAST_UPDT_USER_SK").alias("SRC_SYS_LAST_UPDT_USER_SK"),
        F.col("Lnk_Lkp1_Data.PRVCY_EXTL_ENTY_CLS_TYP_CD_SK").alias("PRVCY_EXTL_ENTY_CLS_TYP_CD_SK"),
        F.col("Lnk_Lkp1_Data.PRVCY_EXTRNL_ENTY_UNIQ_KEY").alias("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
        F.col("Ref_PrvcyExtrnlEntyAddr_Lkup.ADDR_LN_1").alias("ADDR_LN_1"),
        F.col("Ref_PrvcyExtrnlEntyAddr_Lkup.ADDR_LN_2").alias("ADDR_LN_2"),
        F.col("Ref_PrvcyExtrnlEntyAddr_Lkup.ADDR_LN_3").alias("ADDR_LN_3"),
        F.col("Ref_PrvcyExtrnlEntyAddr_Lkup.CITY_NM").alias("CITY_NM"),
        F.col("Ref_PrvcyExtrnlEntyAddr_Lkup.PRVCY_EXTL_ENTY_ADDR_ST_CD_SK").alias("PRVCY_EXTL_ENTY_ADDR_ST_CD_SK"),
        F.col("Ref_PrvcyExtrnlEntyAddr_Lkup.POSTAL_CD").alias("POSTAL_CD"),
        F.col("Lnk_Lkp1_Data.PRVCY_DSCLSUR_ASSOC_EXTR_DT_SK").alias("PRVCY_DSCLSUR_ASSOC_EXTR_DT_SK"),
        F.col("Lnk_Lkp1_Data.PRVCY_DSCLSUR_ASSOC_EXTR_IN").alias("PRVCY_DSCLSUR_ASSOC_EXTR_IN")
    )

df_lkp_Codes_bp = df_Lkp_Prvcy_Addr.alias("Lnk_Extract_Data")

df_lkp_Codes_joined = (
    df_lkp_Codes_bp
    .join(df_cpy_cd_mppng_Ref_PrvcyExtrnlEntyAddrCd_Lkup.alias("Ref_PrvcyExtrnlEntyAddrCd_Lkup"),
          on=[F.col("Lnk_Extract_Data.PRVCY_EXTL_ENTY_ADDR_ST_CD_SK")==F.col("Ref_PrvcyExtrnlEntyAddrCd_Lkup.CD_MPPNG_SK")],
          how="left")
    .join(df_cpy_cd_mppng_Ref_PrvcyDsclRsnCd_Lkup.alias("Ref_PrvcyDsclRsnCd_Lkup"),
          on=[F.col("Lnk_Extract_Data.PRVCY_DSCLSUR_RSN_CD_SK")==F.col("Ref_PrvcyDsclRsnCd_Lkup.CD_MPPNG_SK")],
          how="left")
    .join(df_cpy_cd_mppng_Ref_PrvcyDsclMethCd_Lkup.alias("Ref_PrvcyDsclMethCd_Lkup"),
          on=[F.col("Lnk_Extract_Data.PRVCY_DSCLSUR_METH_CD_SK")==F.col("Ref_PrvcyDsclMethCd_Lkup.CD_MPPNG_SK")],
          how="left")
    .join(df_cpy_cd_mppng_Ref_PrvcyDsclCatCd4_Lkup.alias("Ref_PrvcyDsclCatCd4_Lkup"),
          on=[F.col("Lnk_Extract_Data.PRVCY_DSCLSUR_CAT_4_CD_SK")==F.col("Ref_PrvcyDsclCatCd4_Lkup.CD_MPPNG_SK")],
          how="left")
    .join(df_cpy_cd_mppng_Ref_PrvcyDsclCatCd3_Lkup.alias("Ref_PrvcyDsclCatCd3_Lkup"),
          on=[F.col("Lnk_Extract_Data.PRVCY_DSCLSUR_CAT_3_CD_SK")==F.col("Ref_PrvcyDsclCatCd3_Lkup.CD_MPPNG_SK")],
          how="left")
    .join(df_cpy_cd_mppng_Ref_PrvcyDsclCatCd2_Lkup.alias("Ref_PrvcyDsclCatCd2_Lkup"),
          on=[F.col("Lnk_Extract_Data.PRVCY_DSCLSUR_CAT_2_CD_SK")==F.col("Ref_PrvcyDsclCatCd2_Lkup.CD_MPPNG_SK")],
          how="left")
    .join(df_cpy_cd_mppng_Ref_PrvcyDsclCatCd1_Lkup.alias("Ref_PrvcyDsclCatCd1_Lkup"),
          on=[F.col("Lnk_Extract_Data.PRVCY_DSCLSUR_CAT_1_CD_SK")==F.col("Ref_PrvcyDsclCatCd1_Lkup.CD_MPPNG_SK")],
          how="left")
    .join(df_cpy_cd_mppng_Ref_PrvcyMbrSrcCd_Lkup.alias("Ref_PrvcyMbrSrcCd_Lkup"),
          on=[F.col("Lnk_Extract_Data.PRVCY_MBR_SRC_CD_SK")==F.col("Ref_PrvcyMbrSrcCd_Lkup.CD_MPPNG_SK")],
          how="left")
    .join(df_db2_Mbr_in.alias("Ref_PrvcyMbrCd_Lkup"),
          on=[F.col("Lnk_Extract_Data.MBR_SK")==F.col("Ref_PrvcyMbrCd_Lkup.MBR_SK")],
          how="left")
    .join(df_db2_PrvcyExtrnlMbr_in.alias("Ref_PrvcyExtrnlMbrCd_Lkup"),
          on=[F.col("Lnk_Extract_Data.PRVCY_EXTRNL_MBR_SK")==F.col("Ref_PrvcyExtrnlMbrCd_Lkup.PRVCY_EXTRNL_MBR_SK")],
          how="left")
    .join(df_cpy_cd_mppng_Ref_PrvcyEntyClsTypCd_Lkup.alias("Ref_PrvcyEntyClsTypCd_Lkup"),
          on=[F.col("Lnk_Extract_Data.PRVCY_EXTL_ENTY_CLS_TYP_CD_SK")==F.col("Ref_PrvcyEntyClsTypCd_Lkup.CD_MPPNG_SK")],
          how="left")
    .join(df_db2_PrvcyExtrnlPrsn_in.alias("Ref_PrvcyExtrnlPrsn_Lkup"),
          on=[F.col("Lnk_Extract_Data.PRVCY_EXTRNL_ENTY_UNIQ_KEY")==F.col("Ref_PrvcyExtrnlPrsn_Lkup.PRVCY_EXTRNL_ENTY_UNIQ_KEY")],
          how="left")
    .join(df_db2_PrvcyExtrnlOrg_in.alias("Ref_PrvcyExtrnlOrg_Lkup"),
          on=[F.col("Lnk_Extract_Data.PRVCY_EXTRNL_ENTY_UNIQ_KEY")==F.col("Ref_PrvcyExtrnlOrg_Lkup.PRVCY_EXTRNL_ENTY_UNIQ_KEY")],
          how="left")
)

df_lnk_CodesLkpData_out = df_lkp_Codes_joined.select(
    F.col("Lnk_Extract_Data.PRVCY_DSCLSUR_SK").alias("PRVCY_DSCLSUR_SK"),
    F.col("Lnk_Extract_Data.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Lnk_Extract_Data.PRVCY_MBR_UNIQ_KEY").alias("PRVCY_MBR_UNIQ_KEY"),
    F.col("Lnk_Extract_Data.SEQ_NO").alias("SEQ_NO"),
    F.col("Lnk_Extract_Data.PRVCY_MBR_SRC_CD_SK").alias("PRVCY_MBR_SRC_CD_SK"),
    F.col("Lnk_Extract_Data.PRVCY_DSCLSUR_ENTY_SK").alias("PRVCY_DSCLSUR_ENTY_SK"),
    F.col("Lnk_Extract_Data.MBR_SK").alias("MBR_SK"),
    F.col("Lnk_Extract_Data.PRVCY_EXTRNL_MBR_SK").alias("PRVCY_EXTRNL_MBR_SK"),
    F.col("Lnk_Extract_Data.PRVCY_RECPNT_EXTRNL_ENTY_SK").alias("PRVCY_RECPNT_EXTRNL_ENTY_SK"),
    F.col("Lnk_Extract_Data.PRVCY_DSCLSUR_CAT_1_CD_SK").alias("PRVCY_DSCLSUR_CAT_1_CD_SK"),
    F.col("Lnk_Extract_Data.PRVCY_DSCLSUR_CAT_2_CD_SK").alias("PRVCY_DSCLSUR_CAT_2_CD_SK"),
    F.col("Lnk_Extract_Data.PRVCY_DSCLSUR_CAT_3_CD_SK").alias("PRVCY_DSCLSUR_CAT_3_CD_SK"),
    F.col("Lnk_Extract_Data.PRVCY_DSCLSUR_CAT_4_CD_SK").alias("PRVCY_DSCLSUR_CAT_4_CD_SK"),
    F.col("Lnk_Extract_Data.PRVCY_DSCLSUR_METH_CD_SK").alias("PRVCY_DSCLSUR_METH_CD_SK"),
    F.col("Lnk_Extract_Data.PRVCY_DSCLSUR_RSN_CD_SK").alias("PRVCY_DSCLSUR_RSN_CD_SK"),
    F.col("Lnk_Extract_Data.CRT_DT_SK").alias("CRT_DT_SK"),
    F.col("Lnk_Extract_Data.DSCLSUR_DT_SK").alias("DSCLSUR_DT_SK"),
    F.col("Lnk_Extract_Data.DSCLSUR_DESC").alias("DSCLSUR_DESC"),
    F.col("Lnk_Extract_Data.SRC_SYS_LAST_UPDT_DT_SK").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    F.col("Lnk_Extract_Data.SRC_SYS_LAST_UPDT_USER_SK").alias("SRC_SYS_LAST_UPDT_USER_SK"),
    F.col("Lnk_Extract_Data.PRVCY_EXTL_ENTY_CLS_TYP_CD_SK").alias("PRVCY_EXTL_ENTY_CLS_TYP_CD_SK"),
    F.col("Lnk_Extract_Data.PRVCY_EXTRNL_ENTY_UNIQ_KEY").alias("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
    F.col("Lnk_Extract_Data.ADDR_LN_1").alias("ADDR_LN_1"),
    F.col("Lnk_Extract_Data.ADDR_LN_2").alias("ADDR_LN_2"),
    F.col("Lnk_Extract_Data.ADDR_LN_3").alias("ADDR_LN_3"),
    F.col("Lnk_Extract_Data.CITY_NM").alias("CITY_NM"),
    F.col("Lnk_Extract_Data.PRVCY_EXTL_ENTY_ADDR_ST_CD_SK").alias("PRVCY_EXTL_ENTY_ADDR_ST_CD_SK"),
    F.col("Lnk_Extract_Data.POSTAL_CD").alias("POSTAL_CD"),
    F.col("Ref_PrvcyExtrnlEntyAddrCd_Lkup.TRGT_CD").alias("TRGT_CD_Addr"),
    F.col("Ref_PrvcyExtrnlEntyAddrCd_Lkup.TRGT_CD_NM").alias("TRGT_CD_NM_Addr"),
    F.col("Ref_PrvcyDsclCatCd1_Lkup.TRGT_CD").alias("PRVCY_DSCLSUR_CAT_1_CD"),
    F.col("Ref_PrvcyDsclCatCd1_Lkup.TRGT_CD_NM").alias("PRVCY_DSCLSUR_CAT_1_NM"),
    F.col("Ref_PrvcyDsclCatCd2_Lkup.TRGT_CD").alias("PRVCY_DSCLSUR_CAT_2_CD"),
    F.col("Ref_PrvcyDsclCatCd2_Lkup.TRGT_CD_NM").alias("PRVCY_DSCLSUR_CAT_2_NM"),
    F.col("Ref_PrvcyDsclCatCd3_Lkup.TRGT_CD").alias("PRVCY_DSCLSUR_CAT_3_CD"),
    F.col("Ref_PrvcyDsclCatCd3_Lkup.TRGT_CD_NM").alias("PRVCY_DSCLSUR_CAT_3_NM"),
    F.col("Ref_PrvcyDsclCatCd4_Lkup.TRGT_CD").alias("PRVCY_DSCLSUR_CAT_4_CD"),
    F.col("Ref_PrvcyDsclCatCd4_Lkup.TRGT_CD_NM").alias("PRVCY_DSCLSUR_CAT_4_NM"),
    F.col("Ref_PrvcyDsclMethCd_Lkup.TRGT_CD").alias("PRVCY_DSCLSUR_METH_CD"),
    F.col("Ref_PrvcyDsclMethCd_Lkup.TRGT_CD_NM").alias("PRVCY_DSCLSUR_METH_NM"),
    F.col("Ref_PrvcyDsclRsnCd_Lkup.TRGT_CD").alias("PRVCY_DSCLSUR_RSN_CD"),
    F.col("Ref_PrvcyDsclRsnCd_Lkup.TRGT_CD_NM").alias("PRVCY_DSCLSUR_RSN_NM"),
    F.col("Ref_PrvcyMbrSrcCd_Lkup.TRGT_CD").alias("TRGT_CD_MbrSRC"),
    F.col("Ref_PrvcyMbrSrcCd_Lkup.TRGT_CD_NM").alias("TRGT_CD_NM_MbrSRC"),
    F.col("Ref_PrvcyMbrCd_Lkup.MBR_ID").alias("MBR_ID"),
    F.col("Ref_PrvcyExtrnlMbrCd_Lkup.PRVCY_EXTRNL_MBR_ID").alias("PRVCY_EXTRNL_MBR_ID"),
    F.col("Ref_PrvcyEntyClsTypCd_Lkup.TRGT_CD").alias("TRGT_CD_Cls"),
    F.col("Ref_PrvcyExtrnlPrsn_Lkup.FIRST_NM").alias("FIRST_NM"),
    F.col("Ref_PrvcyExtrnlPrsn_Lkup.MIDINIT").alias("MIDINIT"),
    F.col("Ref_PrvcyExtrnlPrsn_Lkup.LAST_NM").alias("LAST_NM"),
    F.col("Ref_PrvcyExtrnlOrg_Lkup.ORG_NM").alias("ORG_NM"),
    F.col("Lnk_Extract_Data.PRVCY_DSCLSUR_ASSOC_EXTR_DT_SK").alias("PRVCY_DSCLSUR_ASSOC_EXTR_DT_SK"),
    F.col("Lnk_Extract_Data.PRVCY_DSCLSUR_ASSOC_EXTR_IN").alias("PRVCY_DSCLSUR_ASSOC_EXTR_IN")
)

df_xfrm_in = df_lnk_CodesLkpData_out

df_lnk_xfm_Data = df_xfrm_in.filter(
    (F.col("PRVCY_DSCLSUR_SK")!=1) & (F.col("PRVCY_DSCLSUR_SK")!=0)
)

df_lnk_xfm_Data_sel = df_lnk_xfm_Data.select(
    F.col("PRVCY_DSCLSUR_SK").alias("PRVCY_DSCLSUR_SK"),
    F.when(
        (F.col("SRC_SYS_CD")=='')|(F.col("SRC_SYS_CD").isNull()),
        F.lit('NA')
    ).otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    F.col("PRVCY_MBR_UNIQ_KEY").alias("PRVCY_MBR_UNIQ_KEY"),
    F.col("SEQ_NO").alias("PRVCY_DSCLSUR_SEQ_NO"),
    F.when(
        (F.col("TRGT_CD_MbrSRC").isNull())|(F.length(trim(F.col("TRGT_CD_MbrSRC")))==0),
        F.lit('NA')
    ).otherwise(F.col("TRGT_CD_MbrSRC")).alias("PRVCY_MBR_SRC_CD"),
    F.lit(CurrRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(CurrRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("PRVCY_DSCLSUR_ENTY_SK").alias("PRVCY_DSCLSUR_ENTY_SK"),
    F.col("PRVCY_EXTRNL_MBR_SK").alias("PRVCY_EXTRNL_MBR_SK"),
    F.col("PRVCY_RECPNT_EXTRNL_ENTY_SK").alias("PRVCY_RECPNT_EXTRNL_ENTY_SK"),
    F.when(
        (F.col("PRVCY_DSCLSUR_ASSOC_EXTR_DT_SK").isNull())|(F.length(trim(F.col("PRVCY_DSCLSUR_ASSOC_EXTR_DT_SK")))==0),
        F.lit('NA')
    ).otherwise(F.col("PRVCY_DSCLSUR_ASSOC_EXTR_DT_SK")).alias("PRVCY_DSCLSUR_ASSOC_EXTR_DT_SK"),
    F.when(
        (F.col("PRVCY_DSCLSUR_ASSOC_EXTR_IN").isNull())|(F.length(trim(F.col("PRVCY_DSCLSUR_ASSOC_EXTR_IN")))==0),
        F.lit('N')
    ).otherwise(F.col("PRVCY_DSCLSUR_ASSOC_EXTR_IN")).alias("PRVCY_DSCLSUR_ASSOC_EXTR_IN"),
    F.when(
        (F.col("PRVCY_DSCLSUR_CAT_1_CD").isNull())|(F.length(trim(F.col("PRVCY_DSCLSUR_CAT_1_CD")))==0),
        F.lit('NA')
    ).otherwise(F.col("PRVCY_DSCLSUR_CAT_1_CD")).alias("PRVCY_DSCLSUR_CAT_1_CD"),
    F.when(
        (F.col("PRVCY_DSCLSUR_CAT_1_NM").isNull())|(F.length(trim(F.col("PRVCY_DSCLSUR_CAT_1_NM")))==0),
        F.lit('NA')
    ).otherwise(F.col("PRVCY_DSCLSUR_CAT_1_NM")).alias("PRVCY_DSCLSUR_CAT_1_NM"),
    F.when(
        (F.col("PRVCY_DSCLSUR_CAT_2_CD").isNull())|(F.length(trim(F.col("PRVCY_DSCLSUR_CAT_2_CD")))==0),
        F.lit('NA')
    ).otherwise(F.col("PRVCY_DSCLSUR_CAT_2_CD")).alias("PRVCY_DSCLSUR_CAT_2_CD"),
    F.when(
        (F.col("PRVCY_DSCLSUR_CAT_2_NM").isNull())|(F.length(trim(F.col("PRVCY_DSCLSUR_CAT_2_NM")))==0),
        F.lit('NA')
    ).otherwise(F.col("PRVCY_DSCLSUR_CAT_2_NM")).alias("PRVCY_DSCLSUR_CAT_2_NM"),
    F.when(
        (F.col("PRVCY_DSCLSUR_CAT_3_CD").isNull())|(F.length(trim(F.col("PRVCY_DSCLSUR_CAT_3_CD")))==0),
        F.lit('NA')
    ).otherwise(F.col("PRVCY_DSCLSUR_CAT_3_CD")).alias("PRVCY_DSCLSUR_CAT_3_CD"),
    F.when(
        (F.col("PRVCY_DSCLSUR_CAT_3_NM").isNull())|(F.length(trim(F.col("PRVCY_DSCLSUR_CAT_3_NM")))==0),
        F.lit('NA')
    ).otherwise(F.col("PRVCY_DSCLSUR_CAT_3_NM")).alias("PRVCY_DSCLSUR_CAT_3_NM"),
    F.when(
        (F.col("PRVCY_DSCLSUR_CAT_4_CD").isNull())|(F.length(trim(F.col("PRVCY_DSCLSUR_CAT_4_CD")))==0),
        F.lit('NA')
    ).otherwise(F.col("PRVCY_DSCLSUR_CAT_4_CD")).alias("PRVCY_DSCLSUR_CAT_4_CD"),
    F.when(
        (F.col("PRVCY_DSCLSUR_CAT_4_NM").isNull())|(F.length(trim(F.col("PRVCY_DSCLSUR_CAT_4_NM")))==0),
        F.lit('NA')
    ).otherwise(F.col("PRVCY_DSCLSUR_CAT_4_NM")).alias("PRVCY_DSCLSUR_CAT_4_NM"),
    F.col("CRT_DT_SK").alias("PRVCY_DSCLSUR_CRT_DT_SK"),
    F.col("DSCLSUR_DT_SK").alias("PRVCY_DSCLSUR_DT_SK"),
    F.col("DSCLSUR_DESC").alias("PRVCY_DSCLSUR_DESC"),
    F.when(
        (F.col("PRVCY_DSCLSUR_METH_CD").isNull())|(F.length(trim(F.col("PRVCY_DSCLSUR_METH_CD")))==0),
        F.lit('NA')
    ).otherwise(F.col("PRVCY_DSCLSUR_METH_CD")).alias("PRVCY_DSCLSUR_METH_CD"),
    F.when(
        (F.col("PRVCY_DSCLSUR_METH_NM").isNull())|(F.length(trim(F.col("PRVCY_DSCLSUR_METH_NM")))==0),
        F.lit('NA')
    ).otherwise(F.col("PRVCY_DSCLSUR_METH_NM")).alias("PRVCY_DSCLSUR_METH_NM"),
    F.when(
        (F.col("PRVCY_DSCLSUR_RSN_CD").isNull())|(F.length(trim(F.col("PRVCY_DSCLSUR_RSN_CD")))==0),
        F.lit('NA')
    ).otherwise(F.col("PRVCY_DSCLSUR_RSN_CD")).alias("PRVCY_DSCLSUR_RSN_CD"),
    F.when(
        (F.col("PRVCY_DSCLSUR_RSN_NM").isNull())|(F.length(trim(F.col("PRVCY_DSCLSUR_RSN_NM")))==0),
        F.lit('NA')
    ).otherwise(F.col("PRVCY_DSCLSUR_RSN_NM")).alias("PRVCY_DSCLSUR_RSN_NM"),
    F.when(
        (F.col("TRGT_CD_MbrSRC")==F.lit('FACETS')) & (F.col("MBR_ID").isNotNull()),
        F.col("MBR_ID")
    ).when(
        (F.col("TRGT_CD_MbrSRC")!=F.lit('FACETS')) & (F.col("PRVCY_EXTRNL_MBR_ID").isNotNull()),
        F.col("PRVCY_EXTRNL_MBR_ID")
    ).otherwise(F.lit('NA')).alias("PRVCY_MBR_ID"),
    F.when(
        (F.col("TRGT_CD_NM_MbrSRC").isNull())|(F.length(trim(F.col("TRGT_CD_NM_MbrSRC")))==0),
        F.lit('NA')
    ).otherwise(F.col("TRGT_CD_NM_MbrSRC")).alias("PRVCY_MBR_SRC_NM"),
    F.when(
        F.col("TRGT_CD_Cls")==F.lit('PRSN'),
        F.when(F.col("FIRST_NM").isNull(),F.lit('')).otherwise(F.col("FIRST_NM")) \
         + F.when(F.col("MIDINIT").isNull(),F.lit('')).otherwise(F.col("MIDINIT")) \
         + F.when(F.col("LAST_NM").isNull(),F.lit('')).otherwise(F.col("LAST_NM"))
    ).when(
        F.col("TRGT_CD_Cls")==F.lit('ORG'),
        F.when(F.col("ORG_NM").isNull(),F.lit('')).otherwise(F.col("ORG_NM"))
    ).otherwise(F.lit('')).alias("PRVCY_RECPNT_EXT_ENTY_FULL_NM"),
    F.when(
        (F.col("ADDR_LN_1").isNull())|(F.length(trim(F.col("ADDR_LN_1")))==0),
        F.lit('')
    ).otherwise(F.col("ADDR_LN_1")).alias("PRVCY_RECPNT_EXT_ENTY_ADDR_LN1"),
    F.when(
        (F.col("ADDR_LN_2").isNull())|(F.length(trim(F.col("ADDR_LN_2")))==0),
        F.lit('')
    ).otherwise(F.col("ADDR_LN_2")).alias("PRVCY_RECPNT_EXT_ENTY_ADDR_LN2"),
    F.when(
        (F.col("ADDR_LN_3").isNull())|(F.length(trim(F.col("ADDR_LN_3")))==0),
        F.lit('')
    ).otherwise(F.col("ADDR_LN_3")).alias("PRVCY_RECPNT_EXT_ENTY_ADDR_LN3"),
    F.when(
        (F.col("CITY_NM").isNull())|(F.length(trim(F.col("CITY_NM")))==0),
        F.lit('')
    ).otherwise(F.col("CITY_NM")).alias("PRVCY_RECPNT_EXT_ENTY_CITY_NM"),
    F.when(
        (F.length(trim(F.when(F.col("POSTAL_CD").isNotNull(),F.col("POSTAL_CD")).otherwise(F.lit(''))))==0),
        F.lit('     ')
    ).when(
        (F.col("TRGT_CD_Addr")!=F.lit('1')) & (F.col("TRGT_CD_Addr")!=F.lit('0')) & (F.instr(F.col("POSTAL_CD"),'1N0N')>0),
        F.col("POSTAL_CD").substr(F.lit(1),F.lit(5))
    ).otherwise(F.lit('     ')).alias("PRVCY_RECPNT_EXT_ENTY_ZIP_CD_5"),
    F.when(
        (F.length(trim(F.when(F.col("POSTAL_CD").isNotNull(),F.col("POSTAL_CD")).otherwise(F.lit(''))))==0),
        F.lit('    ')
    ).when(
        (F.col("TRGT_CD_Addr")!=F.lit('1')) & (F.col("TRGT_CD_Addr")!=F.lit('0')) 
        & (F.instr(F.col("POSTAL_CD"),'1N0N')>0)
        & (F.instr(F.col("POSTAL_CD").substr(F.lit(6),F.lit(4)),'1N0N')>0),
        F.col("POSTAL_CD").substr(F.lit(6),F.lit(4))
    ).when(
        (F.col("TRGT_CD_Addr")!=F.lit('1')) & (F.col("TRGT_CD_Addr")!=F.lit('0'))
        & (F.instr(F.col("POSTAL_CD"),'1N0N')>0)
        & (F.instr(F.col("POSTAL_CD").substr(F.lit(7),F.lit(4)),'1N0N')>0),
        F.col("POSTAL_CD").substr(F.lit(7),F.lit(4))
    ).otherwise(F.lit('    ')).alias("PRVCY_RECPNT_EXT_ENTY_ZIP_CD_4"),
    F.when(
        (F.col("TRGT_CD_Addr").isNull())|(F.length(trim(F.col("TRGT_CD_Addr")))==0),
        F.lit('UNK')
    ).otherwise(F.col("TRGT_CD_Addr")).alias("PRVCY_RECPNT_EXT_ENTY_ST_CD"),
    F.when(
        (F.col("TRGT_CD_NM_Addr").isNull())|(F.length(trim(F.col("TRGT_CD_NM_Addr")))==0),
        F.lit('UNK')
    ).otherwise(F.col("TRGT_CD_NM_Addr")).alias("PRVCY_RECPNT_EXT_ENTY_ST_NM"),
    F.col("SRC_SYS_LAST_UPDT_DT_SK").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    F.col("SRC_SYS_LAST_UPDT_USER_SK").alias("SRC_SYS_LAST_UPDT_USER_SK"),
    F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PRVCY_MBR_SRC_CD_SK").alias("PRVCY_MBR_SRC_CD_SK"),
    F.col("PRVCY_DSCLSUR_CAT_1_CD_SK").alias("PRVCY_DSCLSUR_CAT_1_CD_SK"),
    F.col("PRVCY_DSCLSUR_CAT_2_CD_SK").alias("PRVCY_DSCLSUR_CAT_2_CD_SK"),
    F.col("PRVCY_DSCLSUR_CAT_3_CD_SK").alias("PRVCY_DSCLSUR_CAT_3_CD_SK"),
    F.col("PRVCY_DSCLSUR_CAT_4_CD_SK").alias("PRVCY_DSCLSUR_CAT_4_CD_SK"),
    F.col("PRVCY_DSCLSUR_METH_CD_SK").alias("PRVCY_DSCLSUR_METH_CD_SK"),
    F.col("PRVCY_DSCLSUR_RSN_CD_SK").alias("PRVCY_DSCLSUR_RSN_CD_SK")
)

df_lnk_NA_out_firstRow = df_xfrm_in.limit(1)
df_lnk_NA_out = df_lnk_NA_out_firstRow.select(
    F.lit(1).alias("PRVCY_DSCLSUR_SK"),
    F.lit('NA').alias("SRC_SYS_CD"),
    F.lit(1).alias("PRVCY_MBR_UNIQ_KEY"),
    F.lit(0).alias("PRVCY_DSCLSUR_SEQ_NO"),
    F.lit('NA').alias("PRVCY_MBR_SRC_CD"),
    F.lit('1753-01-01').alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit('1753-01-01').alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(1).alias("MBR_SK"),
    F.lit(1).alias("PRVCY_DSCLSUR_ENTY_SK"),
    F.lit(1).alias("PRVCY_EXTRNL_MBR_SK"),
    F.lit(1).alias("PRVCY_RECPNT_EXTRNL_ENTY_SK"),
    F.lit('1753-01-01').alias("PRVCY_DSCLSUR_ASSOC_EXTR_DT_SK"),
    F.lit('N').alias("PRVCY_DSCLSUR_ASSOC_EXTR_IN"),
    F.lit('NA').alias("PRVCY_DSCLSUR_CAT_1_CD"),
    F.lit('NA').alias("PRVCY_DSCLSUR_CAT_1_NM"),
    F.lit('NA').alias("PRVCY_DSCLSUR_CAT_2_CD"),
    F.lit('NA').alias("PRVCY_DSCLSUR_CAT_2_NM"),
    F.lit('NA').alias("PRVCY_DSCLSUR_CAT_3_CD"),
    F.lit('NA').alias("PRVCY_DSCLSUR_CAT_3_NM"),
    F.lit('NA').alias("PRVCY_DSCLSUR_CAT_4_CD"),
    F.lit('NA').alias("PRVCY_DSCLSUR_CAT_4_NM"),
    F.lit('1753-01-01').alias("PRVCY_DSCLSUR_CRT_DT_SK"),
    F.lit('1753-01-01').alias("PRVCY_DSCLSUR_DT_SK"),
    F.lit(' ').alias("PRVCY_DSCLSUR_DESC"),
    F.lit('NA').alias("PRVCY_DSCLSUR_METH_CD"),
    F.lit('NA').alias("PRVCY_DSCLSUR_METH_NM"),
    F.lit('NA').alias("PRVCY_DSCLSUR_RSN_CD"),
    F.lit('NA').alias("PRVCY_DSCLSUR_RSN_NM"),
    F.lit('NA').alias("PRVCY_MBR_ID"),
    F.lit('NA').alias("PRVCY_MBR_SRC_NM"),
    F.lit('NA').alias("PRVCY_RECPNT_EXT_ENTY_FULL_NM"),
    F.lit(' ').alias("PRVCY_RECPNT_EXT_ENTY_ADDR_LN1"),
    F.lit(' ').alias("PRVCY_RECPNT_EXT_ENTY_ADDR_LN2"),
    F.lit(' ').alias("PRVCY_RECPNT_EXT_ENTY_ADDR_LN3"),
    F.lit('NA').alias("PRVCY_RECPNT_EXT_ENTY_CITY_NM"),
    F.lit(' ').alias("PRVCY_RECPNT_EXT_ENTY_ZIP_CD_5"),
    F.lit(' ').alias("PRVCY_RECPNT_EXT_ENTY_ZIP_CD_4"),
    F.lit('NA').alias("PRVCY_RECPNT_EXT_ENTY_ST_CD"),
    F.lit('NA').alias("PRVCY_RECPNT_EXT_ENTY_ST_NM"),
    F.lit('1753-01-01').alias("SRC_SYS_LAST_UPDT_DT_SK"),
    F.lit(1).alias("SRC_SYS_LAST_UPDT_USER_SK"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("PRVCY_MBR_SRC_CD_SK"),
    F.lit(1).alias("PRVCY_DSCLSUR_CAT_1_CD_SK"),
    F.lit(1).alias("PRVCY_DSCLSUR_CAT_2_CD_SK"),
    F.lit(1).alias("PRVCY_DSCLSUR_CAT_3_CD_SK"),
    F.lit(1).alias("PRVCY_DSCLSUR_CAT_4_CD_SK"),
    F.lit(1).alias("PRVCY_DSCLSUR_METH_CD_SK"),
    F.lit(1).alias("PRVCY_DSCLSUR_RSN_CD_SK")
)

df_lnk_UNK_out_firstRow = df_xfrm_in.limit(1)
df_lnk_UNK_out = df_lnk_UNK_out_firstRow.select(
    F.lit(0).alias("PRVCY_DSCLSUR_SK"),
    F.lit('UNK').alias("SRC_SYS_CD"),
    F.lit(0).alias("PRVCY_MBR_UNIQ_KEY"),
    F.lit(0).alias("PRVCY_DSCLSUR_SEQ_NO"),
    F.lit('UNK').alias("PRVCY_MBR_SRC_CD"),
    F.lit('1753-01-01').alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit('1753-01-01').alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(0).alias("MBR_SK"),
    F.lit(0).alias("PRVCY_DSCLSUR_ENTY_SK"),
    F.lit(0).alias("PRVCY_EXTRNL_MBR_SK"),
    F.lit(0).alias("PRVCY_RECPNT_EXTRNL_ENTY_SK"),
    F.lit('1753-01-01').alias("PRVCY_DSCLSUR_ASSOC_EXTR_DT_SK"),
    F.lit('N').alias("PRVCY_DSCLSUR_ASSOC_EXTR_IN"),
    F.lit('UNK').alias("PRVCY_DSCLSUR_CAT_1_CD"),
    F.lit('UNK').alias("PRVCY_DSCLSUR_CAT_1_NM"),
    F.lit('UNK').alias("PRVCY_DSCLSUR_CAT_2_CD"),
    F.lit('UNK').alias("PRVCY_DSCLSUR_CAT_2_NM"),
    F.lit('UNK').alias("PRVCY_DSCLSUR_CAT_3_CD"),
    F.lit('UNK').alias("PRVCY_DSCLSUR_CAT_3_NM"),
    F.lit('UNK').alias("PRVCY_DSCLSUR_CAT_4_CD"),
    F.lit('UNK').alias("PRVCY_DSCLSUR_CAT_4_NM"),
    F.lit('1753-01-01').alias("PRVCY_DSCLSUR_CRT_DT_SK"),
    F.lit('1753-01-01').alias("PRVCY_DSCLSUR_DT_SK"),
    F.lit(' ').alias("PRVCY_DSCLSUR_DESC"),
    F.lit('UNK').alias("PRVCY_DSCLSUR_METH_CD"),
    F.lit('UNK').alias("PRVCY_DSCLSUR_METH_NM"),
    F.lit('UNK').alias("PRVCY_DSCLSUR_RSN_CD"),
    F.lit('UNK').alias("PRVCY_DSCLSUR_RSN_NM"),
    F.lit('UNK').alias("PRVCY_MBR_ID"),
    F.lit('UNK').alias("PRVCY_MBR_SRC_NM"),
    F.lit('UNK').alias("PRVCY_RECPNT_EXT_ENTY_FULL_NM"),
    F.lit(' ').alias("PRVCY_RECPNT_EXT_ENTY_ADDR_LN1"),
    F.lit(' ').alias("PRVCY_RECPNT_EXT_ENTY_ADDR_LN2"),
    F.lit(' ').alias("PRVCY_RECPNT_EXT_ENTY_ADDR_LN3"),
    F.lit('UNK').alias("PRVCY_RECPNT_EXT_ENTY_CITY_NM"),
    F.lit(' ').alias("PRVCY_RECPNT_EXT_ENTY_ZIP_CD_5"),
    F.lit(' ').alias("PRVCY_RECPNT_EXT_ENTY_ZIP_CD_4"),
    F.lit('UNK').alias("PRVCY_RECPNT_EXT_ENTY_ST_CD"),
    F.lit('UNK').alias("PRVCY_RECPNT_EXT_ENTY_ST_NM"),
    F.lit('1753-01-01').alias("SRC_SYS_LAST_UPDT_DT_SK"),
    F.lit(0).alias("SRC_SYS_LAST_UPDT_USER_SK"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("PRVCY_MBR_SRC_CD_SK"),
    F.lit(0).alias("PRVCY_DSCLSUR_CAT_1_CD_SK"),
    F.lit(0).alias("PRVCY_DSCLSUR_CAT_2_CD_SK"),
    F.lit(0).alias("PRVCY_DSCLSUR_CAT_3_CD_SK"),
    F.lit(0).alias("PRVCY_DSCLSUR_CAT_4_CD_SK"),
    F.lit(0).alias("PRVCY_DSCLSUR_METH_CD_SK"),
    F.lit(0).alias("PRVCY_DSCLSUR_RSN_CD_SK")
)

df_lnk_xfm_all = df_lnk_NA_out.unionByName(df_lnk_UNK_out).unionByName(df_lnk_xfm_Data_sel)

df_fnl_Data = df_lnk_xfm_all.select(
    "PRVCY_DSCLSUR_SK",
    "SRC_SYS_CD",
    "PRVCY_MBR_UNIQ_KEY",
    "PRVCY_DSCLSUR_SEQ_NO",
    "PRVCY_MBR_SRC_CD",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "MBR_SK",
    "PRVCY_DSCLSUR_ENTY_SK",
    "PRVCY_EXTRNL_MBR_SK",
    "PRVCY_RECPNT_EXTRNL_ENTY_SK",
    "PRVCY_DSCLSUR_ASSOC_EXTR_DT_SK",
    "PRVCY_DSCLSUR_ASSOC_EXTR_IN",
    "PRVCY_DSCLSUR_CAT_1_CD",
    "PRVCY_DSCLSUR_CAT_1_NM",
    "PRVCY_DSCLSUR_CAT_2_CD",
    "PRVCY_DSCLSUR_CAT_2_NM",
    "PRVCY_DSCLSUR_CAT_3_CD",
    "PRVCY_DSCLSUR_CAT_3_NM",
    "PRVCY_DSCLSUR_CAT_4_CD",
    "PRVCY_DSCLSUR_CAT_4_NM",
    "PRVCY_DSCLSUR_CRT_DT_SK",
    "PRVCY_DSCLSUR_DT_SK",
    "PRVCY_DSCLSUR_DESC",
    "PRVCY_DSCLSUR_METH_CD",
    "PRVCY_DSCLSUR_METH_NM",
    "PRVCY_DSCLSUR_RSN_CD",
    "PRVCY_DSCLSUR_RSN_NM",
    "PRVCY_MBR_ID",
    "PRVCY_MBR_SRC_NM",
    "PRVCY_RECPNT_EXT_ENTY_FULL_NM",
    "PRVCY_RECPNT_EXT_ENTY_ADDR_LN1",
    "PRVCY_RECPNT_EXT_ENTY_ADDR_LN2",
    "PRVCY_RECPNT_EXT_ENTY_ADDR_LN3",
    "PRVCY_RECPNT_EXT_ENTY_CITY_NM",
    "PRVCY_RECPNT_EXT_ENTY_ZIP_CD_5",
    "PRVCY_RECPNT_EXT_ENTY_ZIP_CD_4",
    "PRVCY_RECPNT_EXT_ENTY_ST_CD",
    "PRVCY_RECPNT_EXT_ENTY_ST_NM",
    "SRC_SYS_LAST_UPDT_DT_SK",
    "SRC_SYS_LAST_UPDT_USER_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PRVCY_MBR_SRC_CD_SK",
    "PRVCY_DSCLSUR_CAT_1_CD_SK",
    "PRVCY_DSCLSUR_CAT_2_CD_SK",
    "PRVCY_DSCLSUR_CAT_3_CD_SK",
    "PRVCY_DSCLSUR_CAT_4_CD_SK",
    "PRVCY_DSCLSUR_METH_CD_SK",
    "PRVCY_DSCLSUR_RSN_CD_SK"
)

df_fnl_Data_padded = df_fnl_Data \
.withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")) \
.withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")) \
.withColumn("PRVCY_DSCLSUR_ASSOC_EXTR_DT_SK", F.rpad(F.col("PRVCY_DSCLSUR_ASSOC_EXTR_DT_SK"), 10, " ")) \
.withColumn("PRVCY_DSCLSUR_ASSOC_EXTR_IN", F.rpad(F.col("PRVCY_DSCLSUR_ASSOC_EXTR_IN"), 1, " ")) \
.withColumn("PRVCY_DSCLSUR_CRT_DT_SK", F.rpad(F.col("PRVCY_DSCLSUR_CRT_DT_SK"), 10, " ")) \
.withColumn("PRVCY_DSCLSUR_DT_SK", F.rpad(F.col("PRVCY_DSCLSUR_DT_SK"), 10, " ")) \
.withColumn("PRVCY_RECPNT_EXT_ENTY_ZIP_CD_5", F.rpad(F.col("PRVCY_RECPNT_EXT_ENTY_ZIP_CD_5"), 5, " ")) \
.withColumn("PRVCY_RECPNT_EXT_ENTY_ZIP_CD_4", F.rpad(F.col("PRVCY_RECPNT_EXT_ENTY_ZIP_CD_4"), 4, " ")) \
.withColumn("SRC_SYS_LAST_UPDT_DT_SK", F.rpad(F.col("SRC_SYS_LAST_UPDT_DT_SK"), 10, " "))

write_files(
    df_fnl_Data_padded,
    f"{adls_path}/load/PRVCY_DSCLSUR_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)