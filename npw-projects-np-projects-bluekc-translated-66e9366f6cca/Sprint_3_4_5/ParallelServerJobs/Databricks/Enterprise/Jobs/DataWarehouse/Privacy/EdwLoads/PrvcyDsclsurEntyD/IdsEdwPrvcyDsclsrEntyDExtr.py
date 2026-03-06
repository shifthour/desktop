# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Naren Garapaty               4/2/2007           FHP                             Originally Programmed                                devlEDW10          
# MAGIC Steph Goddard                7/15/10           TTR-630                        Changed field name -                         EnterpriseNewDevl            SAndrew                   2010-10-01
# MAGIC                                                                                                      PRVCY_EXTRNL_ENTY_CLS_TYP_CD_S to PRVCY_EXTL_ENTY_CLS_TYP_CD_SK      
# MAGIC   
# MAGIC Bhoomi Dasari                 2/14/2013       TTR-1534                    Updated three Address fields to             EnterpriseNewDevl           Kalyan Neelam           2013-03-01
# MAGIC                                                                                                     length from 40 to 80             
# MAGIC 
# MAGIC 
# MAGIC DEVELOPER                         DATE                 PROJECT                                                DESCRIPTION                                            DATASTAGE                                            CODE                            REVIEW
# MAGIC                                                                                                                                                                                                               ENVIRONMENT                                       REVIEWER                   DATE                                              
# MAGIC ------------------------------------------   ----------------------      ------------------------------------------------                ---------------------------------------------------------------     ---------------------------------------------------              ------------------------------          --------------------
# MAGIC Balkarn                                12/17/2013                          5114                                       Original Programming                                   EnterpriseWrhsDevl
# MAGIC                                                                                                                                           (Server to Parallel Conversion)
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Archana Palivela               02/03/2014                              5114                              Added logic to join PRVCY_DSCLSUR_ENTY           EnterpriseWrhsDevl                         Bhoomi Dasari             2/4/2013
# MAGIC                                                                                                                                          to PRVCY_EXTRNL_ENTY_ADDR
# MAGIC 
# MAGIC Manasa Andru                   2015-04-02                     TFS - 1309 and 1310              Added logic to filter the 'NA' record in the extract        EnterpriseNewDevl                            Kalyan Neelam                  2015-04-03
# MAGIC                                                                                                                                    SQL of all the DB2 stages.

# MAGIC Job Name:
# MAGIC IdsEdwPrvcyDsclsrEntyDExtr
# MAGIC Read from source table FCLTY_CLM_PROC , PROC_CD, CLM and W_EDW_ETL_DRVR
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC 
# MAGIC 1) PROC_CD_SK
# MAGIC 2) FCLTY_CLM_PROC_ORDNL_CD_SK
# MAGIC Add Defaults and Null Handling.
# MAGIC Write PRVCY_DSCLSUR_ENTY_DData into a Sequential file for Load Job IdsEdwPrvcyDsclsrEntyDLoad
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, when, lit, row_number, monotonically_increasing_id, rpad, trim
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# MAGIC %run ../../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

ids_secret_name = get_widget_value('ids_secret_name', '')
IDSOwner = get_widget_value('IDSOwner', '')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate', '')
CurrRunCycle = get_widget_value('CurrRunCycle', '')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

# --------------------- db2_CD_MPPNG_Extr ---------------------
extract_query_db2_CD_MPPNG_Extr = """
SELECT CD_MPPNG_SK,COALESCE(TRGT_CD,'UNK') TRGT_CD,COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
from #$IDSOwner#.CD_MPPNG
""".replace("#$IDSOwner#", f"{IDSOwner}")
df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CD_MPPNG_Extr)
    .load()
)

# --------------------- cpy (PxCopy) ---------------------
df_Ref_PrvcyEntyClsTypCdLkup = df_db2_CD_MPPNG_Extr.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_Ref_PrvcyDsclTypCdLkup = df_db2_CD_MPPNG_Extr.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_Ref_PrvcyDsclCatCdLkup = df_db2_CD_MPPNG_Extr.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_Ref_PrvcyExtrnlEntyAddrCdLkup = df_db2_CD_MPPNG_Extr.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

# --------------------- db2_PRVCY_EXTRNL_PRSN ---------------------
extract_query_db2_PRVCY_EXTRNL_PRSN = """
SELECT distinct
PRSN.PRVCY_EXTRNL_ENTY_UNIQ_KEY,
PRSN.FIRST_NM,
PRSN.MIDINIT,
PRSN.LAST_NM
FROM #$IDSOwner#.PRVCY_EXTRNL_PRSN PRSN;
""".replace("#$IDSOwner#", f"{IDSOwner}")
df_db2_PRVCY_EXTRNL_PRSN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_PRVCY_EXTRNL_PRSN)
    .load()
)

# --------------------- rdp_Per (PxRemDup) ---------------------
df_rdp_Per = dedup_sort(
    df_db2_PRVCY_EXTRNL_PRSN,
    ["PRVCY_EXTRNL_ENTY_UNIQ_KEY"],
    [("PRVCY_EXTRNL_ENTY_UNIQ_KEY", "A")]
)

# --------------------- db2_PRVCY_EXTRNL_ORG ---------------------
extract_query_db2_PRVCY_EXTRNL_ORG = f"""
SELECT distinct
ORG.PRVCY_EXTRNL_ENTY_UNIQ_KEY,
ORG.ORG_NM
FROM
{IDSOwner}.PRVCY_EXTRNL_ORG ORG
WHERE
ORG.PRVCY_EXTRNL_ENTY_SK <> 1
"""
df_db2_PRVCY_EXTRNL_ORG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_PRVCY_EXTRNL_ORG)
    .load()
)

# --------------------- rdp_OrgCd (PxRemDup) ---------------------
df_rdp_OrgCd = dedup_sort(
    df_db2_PRVCY_EXTRNL_ORG,
    ["PRVCY_EXTRNL_ENTY_UNIQ_KEY"],
    [("PRVCY_EXTRNL_ENTY_UNIQ_KEY", "A")]
)

# --------------------- db2_PRVCY_DSCLSUR_ENTY_in ---------------------
extract_query_db2_PRVCY_DSCLSUR_ENTY_in = f"""
SELECT distinct
DSCLSUR.PRVCY_DSCLSUR_ENTY_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
DSCLSUR.PRVCY_DSCLSUR_ENTY_UNIQ_KEY,
DSCLSUR.CRT_RUN_CYC_EXCTN_SK,
DSCLSUR.LAST_UPDT_RUN_CYC_EXCTN_SK,
DSCLSUR.PRVCY_DSCLSUR_ENTY_CAT_CD_SK,
DSCLSUR.PRVCY_DSCLSUR_ENTY_TYP_CD_SK,
DSCLSUR.PRVCY_DSCLSUR_ENTY_ID,
DSCLSUR.SRC_SYS_LAST_UPDT_DT_SK,
DSCLSUR.SRC_SYS_LAST_UPDT_USER_SK
FROM
{IDSOwner}.PRVCY_DSCLSUR_ENTY DSCLSUR
LEFT JOIN  {IDSOwner}.CD_MPPNG CD
ON DSCLSUR.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
AND DSCLSUR.PRVCY_DSCLSUR_ENTY_SK <> 1
"""
df_db2_PRVCY_DSCLSUR_ENTY_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_PRVCY_DSCLSUR_ENTY_in)
    .load()
)

# --------------------- db2_PRVCY_EXTRNL_ENTY_ADDR ---------------------
extract_query_db2_PRVCY_EXTRNL_ENTY_ADDR = f"""
SELECT
ADDR.PRVCY_EXTRNL_ENTY_UNIQ_KEY,
ADDR.ADDR_LN_1,
ADDR.ADDR_LN_2,
ADDR.ADDR_LN_3,
ADDR.CITY_NM,
ADDR.PRVCY_EXTL_ENTY_ADDR_ST_CD_SK,
ADDR.POSTAL_CD,
ADDR.CNTY_NM,
MPPNG.TRGT_CD
FROM
{IDSOwner}.PRVCY_EXTRNL_ENTY_ADDR ADDR,
{IDSOwner}.CD_MPPNG MPPNG,
{IDSOwner}.PRVCY_DSCLSUR_ENTY ENTY
WHERE
ADDR.PRVCY_EXTL_ENTY_ADDR_TYP_CD_SK = MPPNG.CD_MPPNG_SK
and  ENTY.SRC_SYS_CD_SK = ADDR.SRC_SYS_CD_SK
and  ADDR.PRVCY_EXTRNL_ENTY_UNIQ_KEY = ENTY.PRVCY_DSCLSUR_ENTY_UNIQ_KEY
and  MPPNG.TRGT_CD not in ( 'UNK' , 'NA')
and  ADDR.PRVCY_EXTRNL_ENTY_ADDR_SK <> 1
"""
df_db2_PRVCY_EXTRNL_ENTY_ADDR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_PRVCY_EXTRNL_ENTY_ADDR)
    .load()
)

# --------------------- xfm_logic (CTransformerStage) ---------------------
# Implement the constraint logic for TRGT_CD:
# "if Trim(TRGT_CD)='HOME' and Trim(TRGT_CD)='WORK' then 'WORK' else if 'HOME' then 'HOME' else if 'WORK' then 'WORK' else original"
df_xfm_logic_pre = df_db2_PRVCY_EXTRNL_ENTY_ADDR.withColumn(
    "TRGT_CD_transformed",
    when(
        (trim(col("TRGT_CD")) == "HOME") & (trim(col("TRGT_CD")) == "WORK"),
        "WORK"
    ).when(
        trim(col("TRGT_CD")) == "HOME",
        "HOME"
    ).when(
        trim(col("TRGT_CD")) == "WORK",
        "WORK"
    ).otherwise(col("TRGT_CD"))
)

df_xfm_logic = df_xfm_logic_pre.select(
    col("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
    col("ADDR_LN_1"),
    col("ADDR_LN_2"),
    col("ADDR_LN_3"),
    col("CITY_NM"),
    col("PRVCY_EXTL_ENTY_ADDR_ST_CD_SK"),
    col("POSTAL_CD"),
    col("CNTY_NM"),
    col("TRGT_CD_transformed").alias("TRGT_CD")
)

# --------------------- Remove_Duplicates_81 (PxRemDup) ---------------------
# Deduplicate by PRVCY_EXTRNL_ENTY_UNIQ_KEY, sort by TRGT_CD desc
df_rdp_81_sorted = df_xfm_logic.withColumn(
    "_sort_key_1", trim(col("PRVCY_EXTRNL_ENTY_UNIQ_KEY"))
).withColumn(
    "_sort_key_2", trim(col("TRGT_CD"))
)

window_81 = Window.partitionBy("_sort_key_1").orderBy(col("_sort_key_2").desc())
df_rdp_81 = df_rdp_81_sorted.withColumn("row_num", row_number().over(window_81)).filter(col("row_num") == 1)

df_Remove_Duplicates_81 = df_rdp_81.select(
    col("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
    col("ADDR_LN_1"),
    col("ADDR_LN_2"),
    col("ADDR_LN_3"),
    col("CITY_NM"),
    col("PRVCY_EXTL_ENTY_ADDR_ST_CD_SK"),
    col("POSTAL_CD"),
    col("CNTY_NM")
)

# --------------------- db2_PRVCY_EXTRNL_ENTY ---------------------
extract_query_db2_PRVCY_EXTRNL_ENTY = f"""
SELECT distinct
ENTY.PRVCY_EXTRNL_ENTY_UNIQ_KEY,
ENTY.PRVCY_EXTL_ENTY_CLS_TYP_CD_SK
FROM
{IDSOwner}.PRVCY_EXTRNL_ENTY ENTY
where
ENTY.PRVCY_EXTRNL_ENTY_SK <> 1
"""
df_db2_PRVCY_EXTRNL_ENTY = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_PRVCY_EXTRNL_ENTY)
    .load()
)

# --------------------- rdp_Cd (PxRemDup) ---------------------
df_rdp_Cd = dedup_sort(
    df_db2_PRVCY_EXTRNL_ENTY,
    ["PRVCY_EXTRNL_ENTY_UNIQ_KEY"],
    [("PRVCY_EXTRNL_ENTY_UNIQ_KEY", "A")]
)

# --------------------- lkp_ExtPrv (PxLookup) ---------------------
# Primary link: df_db2_PRVCY_DSCLSUR_ENTY_in
# Lookup link: df_rdp_Cd (left join on PRVCY_DSCLSUR_ENTY_UNIQ_KEY = PRVCY_EXTRNL_ENTY_UNIQ_KEY)
df_lkp_ExtPrv = df_db2_PRVCY_DSCLSUR_ENTY_in.alias("lnk_IdsEdwPrvcyDsclsrEntyDExtr_InABC").join(
    df_rdp_Cd.alias("Ref_PrvcyExtrnlLkup"),
    on=[
        col("lnk_IdsEdwPrvcyDsclsrEntyDExtr_InABC.PRVCY_DSCLSUR_ENTY_UNIQ_KEY")
        == col("Ref_PrvcyExtrnlLkup.PRVCY_EXTRNL_ENTY_UNIQ_KEY")
    ],
    how="left"
).select(
    col("lnk_IdsEdwPrvcyDsclsrEntyDExtr_InABC.PRVCY_DSCLSUR_ENTY_SK").alias("PRVCY_DSCLSUR_ENTY_SK"),
    col("lnk_IdsEdwPrvcyDsclsrEntyDExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnk_IdsEdwPrvcyDsclsrEntyDExtr_InABC.PRVCY_DSCLSUR_ENTY_UNIQ_KEY").alias("PRVCY_DSCLSUR_ENTY_UNIQ_KEY"),
    col("lnk_IdsEdwPrvcyDsclsrEntyDExtr_InABC.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("lnk_IdsEdwPrvcyDsclsrEntyDExtr_InABC.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("lnk_IdsEdwPrvcyDsclsrEntyDExtr_InABC.PRVCY_DSCLSUR_ENTY_CAT_CD_SK").alias("PRVCY_DSCLSUR_ENTY_CAT_CD_SK"),
    col("lnk_IdsEdwPrvcyDsclsrEntyDExtr_InABC.PRVCY_DSCLSUR_ENTY_TYP_CD_SK").alias("PRVCY_DSCLSUR_ENTY_TYP_CD_SK"),
    col("lnk_IdsEdwPrvcyDsclsrEntyDExtr_InABC.PRVCY_DSCLSUR_ENTY_ID").alias("PRVCY_DSCLSUR_ENTY_ID"),
    col("lnk_IdsEdwPrvcyDsclsrEntyDExtr_InABC.SRC_SYS_LAST_UPDT_DT_SK").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    col("lnk_IdsEdwPrvcyDsclsrEntyDExtr_InABC.SRC_SYS_LAST_UPDT_USER_SK").alias("SRC_SYS_LAST_UPDT_USER_SK"),
    col("Ref_PrvcyExtrnlLkup.PRVCY_EXTL_ENTY_CLS_TYP_CD_SK").alias("PRVCY_EXTL_ENTY_CLS_TYP_CD_SK"),
    col("Ref_PrvcyExtrnlLkup.PRVCY_EXTRNL_ENTY_UNIQ_KEY").alias("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
)

# --------------------- xfm (CTransformerStage) ---------------------
df_xfm = df_lkp_ExtPrv.select(
    col("PRVCY_DSCLSUR_ENTY_SK"),
    col("SRC_SYS_CD"),
    col("PRVCY_DSCLSUR_ENTY_UNIQ_KEY"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("PRVCY_DSCLSUR_ENTY_CAT_CD_SK"),
    col("PRVCY_DSCLSUR_ENTY_TYP_CD_SK"),
    col("PRVCY_DSCLSUR_ENTY_ID"),
    col("SRC_SYS_LAST_UPDT_DT_SK"),
    col("SRC_SYS_LAST_UPDT_USER_SK"),
    when(
        (col("PRVCY_EXTRNL_ENTY_UNIQ_KEY").isNull()) | (col("PRVCY_EXTRNL_ENTY_UNIQ_KEY") == lit(0)),
        lit(0)
    ).otherwise(col("PRVCY_EXTL_ENTY_CLS_TYP_CD_SK")).alias("PRVCY_EXTL_ENTY_CLS_TYP_CD_SK"),
    col("PRVCY_EXTRNL_ENTY_UNIQ_KEY").alias("PRVCY_EXTRNL_ENTY_UNIQ_KEY")
)

# --------------------- lkp_Prv (PxLookup) ---------------------
# Primary link: df_xfm
# Lookup link: df_Remove_Duplicates_81 (left join on PRVCY_DSCLSUR_ENTY_UNIQ_KEY = PRVCY_EXTRNL_ENTY_UNIQ_KEY)
df_lkp_Prv = df_xfm.alias("xfm_o").join(
    df_Remove_Duplicates_81.alias("DSLink82"),
    on=[
        col("xfm_o.PRVCY_DSCLSUR_ENTY_UNIQ_KEY") == col("DSLink82.PRVCY_EXTRNL_ENTY_UNIQ_KEY")
    ],
    how="left"
).select(
    col("xfm_o.PRVCY_DSCLSUR_ENTY_SK").alias("PRVCY_DSCLSUR_ENTY_SK"),
    col("xfm_o.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("xfm_o.PRVCY_DSCLSUR_ENTY_UNIQ_KEY").alias("PRVCY_DSCLSUR_ENTY_UNIQ_KEY"),
    col("xfm_o.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("xfm_o.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("xfm_o.PRVCY_DSCLSUR_ENTY_CAT_CD_SK").alias("PRVCY_DSCLSUR_ENTY_CAT_CD_SK"),
    col("xfm_o.PRVCY_DSCLSUR_ENTY_TYP_CD_SK").alias("PRVCY_DSCLSUR_ENTY_TYP_CD_SK"),
    col("xfm_o.PRVCY_DSCLSUR_ENTY_ID").alias("PRVCY_DSCLSUR_ENTY_ID"),
    col("xfm_o.SRC_SYS_LAST_UPDT_DT_SK").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    col("xfm_o.SRC_SYS_LAST_UPDT_USER_SK").alias("SRC_SYS_LAST_UPDT_USER_SK"),
    col("xfm_o.PRVCY_EXTL_ENTY_CLS_TYP_CD_SK").alias("PRVCY_EXTL_ENTY_CLS_TYP_CD_SK"),
    col("xfm_o.PRVCY_EXTRNL_ENTY_UNIQ_KEY").alias("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
    col("DSLink82.ADDR_LN_1").alias("ADDR_LN_1"),
    col("DSLink82.ADDR_LN_2").alias("ADDR_LN_2"),
    col("DSLink82.ADDR_LN_3").alias("ADDR_LN_3"),
    col("DSLink82.CITY_NM").alias("CITY_NM"),
    col("DSLink82.PRVCY_EXTL_ENTY_ADDR_ST_CD_SK").alias("PRVCY_EXTL_ENTY_ADDR_ST_CD_SK"),
    col("DSLink82.POSTAL_CD").alias("POSTAL_CD"),
    col("DSLink82.CNTY_NM").alias("CNTY_NM")
)

# --------------------- lkp_Codes (PxLookup) ---------------------
# Multiple left joins in one go:
df_lkp_Codes = (
    df_lkp_Prv.alias("xfm_out")
    # Lookup: Ref_PrvcyDsclTypCdLkup
    .join(
        df_Ref_PrvcyDsclTypCdLkup.alias("Ref_PrvcyDsclTypCdLkup"),
        on=[col("xfm_out.PRVCY_DSCLSUR_ENTY_TYP_CD_SK") == col("Ref_PrvcyDsclTypCdLkup.CD_MPPNG_SK")],
        how="left"
    )
    # Lookup: Ref_PrvcyDsclCatCdLkup
    .join(
        df_Ref_PrvcyDsclCatCdLkup.alias("Ref_PrvcyDsclCatCdLkup"),
        on=[col("xfm_out.PRVCY_DSCLSUR_ENTY_CAT_CD_SK") == col("Ref_PrvcyDsclCatCdLkup.CD_MPPNG_SK")],
        how="left"
    )
    # Lookup: Ref_PrvcyExtrnlEntyAddrCdLkup
    .join(
        df_Ref_PrvcyExtrnlEntyAddrCdLkup.alias("Ref_PrvcyExtrnlEntyAddrCdLkup"),
        on=[col("xfm_out.PRVCY_EXTL_ENTY_ADDR_ST_CD_SK") == col("Ref_PrvcyExtrnlEntyAddrCdLkup.CD_MPPNG_SK")],
        how="left"
    )
    # Lookup: Ref_PrvcyEntyClsTypCdLkup
    .join(
        df_Ref_PrvcyEntyClsTypCdLkup.alias("Ref_PrvcyEntyClsTypCdLkup"),
        on=[col("xfm_out.PRVCY_EXTL_ENTY_CLS_TYP_CD_SK") == col("Ref_PrvcyEntyClsTypCdLkup.CD_MPPNG_SK")],
        how="left"
    )
    # Lookup: df_rdp_Per
    .join(
        df_rdp_Per.alias("Ref_PrvcyExtrnlPrsnLkup"),
        on=[col("xfm_out.PRVCY_EXTRNL_ENTY_UNIQ_KEY") == col("Ref_PrvcyExtrnlPrsnLkup.PRVCY_EXTRNL_ENTY_UNIQ_KEY")],
        how="left"
    )
    # Lookup: df_rdp_OrgCd
    .join(
        df_rdp_OrgCd.alias("Ref_PrvcyExtrnlOrgLkup"),
        on=[col("xfm_out.PRVCY_EXTRNL_ENTY_UNIQ_KEY") == col("Ref_PrvcyExtrnlOrgLkup.PRVCY_EXTRNL_ENTY_UNIQ_KEY")],
        how="left"
    )
    .select(
        col("xfm_out.PRVCY_DSCLSUR_ENTY_SK").alias("PRVCY_DSCLSUR_ENTY_SK"),
        col("xfm_out.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("xfm_out.PRVCY_DSCLSUR_ENTY_UNIQ_KEY").alias("PRVCY_DSCLSUR_ENTY_UNIQ_KEY"),
        col("xfm_out.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("xfm_out.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("xfm_out.PRVCY_DSCLSUR_ENTY_CAT_CD_SK").alias("PRVCY_DSCLSUR_ENTY_CAT_CD_SK"),
        col("xfm_out.PRVCY_DSCLSUR_ENTY_TYP_CD_SK").alias("PRVCY_DSCLSUR_ENTY_TYP_CD_SK"),
        col("xfm_out.PRVCY_DSCLSUR_ENTY_ID").alias("PRVCY_DSCLSUR_ENTY_ID"),
        col("xfm_out.SRC_SYS_LAST_UPDT_DT_SK").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        col("xfm_out.SRC_SYS_LAST_UPDT_USER_SK").alias("SRC_SYS_LAST_UPDT_USER_SK"),
        col("xfm_out.PRVCY_EXTL_ENTY_CLS_TYP_CD_SK").alias("PRVCY_EXTL_ENTY_CLS_TYP_CD_SK"),
        col("xfm_out.PRVCY_EXTRNL_ENTY_UNIQ_KEY").alias("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
        col("xfm_out.ADDR_LN_1").alias("ADDR_LN_1"),
        col("xfm_out.ADDR_LN_2").alias("ADDR_LN_2"),
        col("xfm_out.ADDR_LN_3").alias("ADDR_LN_3"),
        col("xfm_out.CITY_NM").alias("CITY_NM"),
        col("xfm_out.PRVCY_EXTL_ENTY_ADDR_ST_CD_SK").alias("PRVCY_EXTL_ENTY_ADDR_ST_CD_SK"),
        col("xfm_out.POSTAL_CD").alias("POSTAL_CD"),
        col("xfm_out.CNTY_NM").alias("CNTY_NM"),
        col("Ref_PrvcyExtrnlPrsnLkup.PRVCY_EXTRNL_ENTY_UNIQ_KEY").alias("PRVCY_EXTRNL_ENTY_UNIQ_KEY_1"),
        col("Ref_PrvcyExtrnlPrsnLkup.FIRST_NM").alias("FIRST_NM"),
        col("Ref_PrvcyExtrnlPrsnLkup.MIDINIT").alias("MIDINIT"),
        col("Ref_PrvcyExtrnlPrsnLkup.LAST_NM").alias("LAST_NM"),
        col("Ref_PrvcyExtrnlOrgLkup.PRVCY_EXTRNL_ENTY_UNIQ_KEY").alias("PRVCY_EXTRNL_ENTY_UNIQ_KEY_2"),
        col("Ref_PrvcyExtrnlOrgLkup.ORG_NM").alias("ORG_NM"),
        col("Ref_PrvcyEntyClsTypCdLkup.TRGT_CD").alias("TRGT_CD"),
        col("Ref_PrvcyExtrnlEntyAddrCdLkup.TRGT_CD").alias("ADDR_TRGT_CD"),
        col("Ref_PrvcyExtrnlEntyAddrCdLkup.TRGT_CD_NM").alias("ADDR_TRGT_CD_NM"),
        col("Ref_PrvcyDsclTypCdLkup.CD_MPPNG_SK").alias("TYP_CD_MPPNG_SK"),
        col("Ref_PrvcyDsclTypCdLkup.TRGT_CD").alias("TYP_TRGT_CD"),
        col("Ref_PrvcyDsclTypCdLkup.TRGT_CD_NM").alias("TYP_TRGT_CD_NM"),
        col("Ref_PrvcyDsclCatCdLkup.CD_MPPNG_SK").alias("CAT_CD_MPPNG_SK"),
        col("Ref_PrvcyDsclCatCdLkup.TRGT_CD").alias("CAT_TRGT_CD"),
        col("Ref_PrvcyDsclCatCdLkup.TRGT_CD_NM").alias("CAT_TRGT_CD_NM"),
    )
)

# --------------------- xfm_BusinessLogic (CTransformerStage) ---------------------
# We have three output links from xfm_BusinessLogic: lnk_Main, NA, UNK
# 1) lnk_Main => filter: PRVCY_DSCLSUR_ENTY_SK<>0 and <>1
df_xfm_BusinessLogic_lnk_Main = (
    df_lkp_Codes
    .filter((col("PRVCY_DSCLSUR_ENTY_SK") != 0) & (col("PRVCY_DSCLSUR_ENTY_SK") != 1))
    .select(
        col("PRVCY_DSCLSUR_ENTY_SK"),
        col("SRC_SYS_CD"),
        col("PRVCY_DSCLSUR_ENTY_UNIQ_KEY"),
        lit(CurrRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        lit(CurrRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        col("PRVCY_DSCLSUR_ENTY_ID"),
        when(
            (trim(col("TRGT_CD")) == "PRSN") & (
                (col("PRVCY_EXTRNL_ENTY_UNIQ_KEY_1").isNull()) | (length(col("PRVCY_EXTRNL_ENTY_UNIQ_KEY_1")) == 0)
            ),
            lit("")
        ).when(
            trim(col("TRGT_CD")) == "PRSN",
            col("FIRST_NM").cast("string")
            + col("MIDINIT").cast("string")
            + col("LAST_NM").cast("string")
        ).when(
            (trim(col("TRGT_CD")) == "ORG") & (
                (col("PRVCY_EXTRNL_ENTY_UNIQ_KEY_2").isNull()) | (length(col("PRVCY_EXTRNL_ENTY_UNIQ_KEY_2")) == 0)
            ),
            lit("")
        ).when(
            trim(col("TRGT_CD")) == "ORG",
            col("ORG_NM")
        ).otherwise(lit("")).alias("PRVCY_DSCLSUR_ENTY_FULL_NM"),
        when(
            (col("ADDR_LN_1").isNull()) | (length(trim(col("ADDR_LN_1"))) == 0),
            lit("")
        ).otherwise(col("ADDR_LN_1")).alias("PRVCY_RECPNT_EXT_ENTY_ADDR_LN1"),
        when(
            (col("ADDR_LN_2").isNull()) | (length(trim(col("ADDR_LN_2"))) == 0),
            lit("")
        ).otherwise(col("ADDR_LN_2")).alias("PRVCY_RECPNT_EXT_ENTY_ADDR_LN2"),
        when(
            (col("ADDR_LN_3").isNull()) | (length(trim(col("ADDR_LN_3"))) == 0),
            lit("")
        ).otherwise(col("ADDR_LN_3")).alias("PRVCY_RECPNT_EXT_ENTY_ADDR_LN3"),
        when(
            (col("CITY_NM").isNull()) | (length(trim(col("CITY_NM"))) == 0),
            lit("")
        ).otherwise(col("CITY_NM")).alias("PRVCY_RECPNT_EXT_ENTY_CITY_NM"),
        when(
            trim(col("POSTAL_CD")).rlike("1N0N"),
            lit("")
        ).otherwise(col("POSTAL_CD").substr(1,5)).alias("PRVCY_RECPNT_EXT_ENTY_ZIP_CD_5"),
        when(
            trim(col("POSTAL_CD")).rlike("1N0N") & (col("POSTAL_CD").substr(6,4).rlike("1N0N")),
            col("POSTAL_CD").substr(6,4)
        ).when(
            trim(col("POSTAL_CD")).rlike("1N0N") & (col("POSTAL_CD").substr(7,3).rlike("1N0N")),
            col("POSTAL_CD").substr(7,3)
        ).otherwise(col("POSTAL_CD").substr(6,4)).alias("PRVCY_RECPNT_EXT_ENTY_ZIP_CD_4"),
        when(
            (length(trim(col("ADDR_TRGT_CD"))) == 0) | (col("ADDR_TRGT_CD").isNull()),
            lit("UNK")
        ).otherwise(col("ADDR_TRGT_CD")).alias("PRVCY_RECPNT_EXT_ENTY_ST_CD"),
        when(
            (length(trim(col("ADDR_TRGT_CD_NM"))) == 0) | (col("ADDR_TRGT_CD_NM").isNull()),
            lit("UNK")
        ).otherwise(col("ADDR_TRGT_CD_NM")).alias("PRVCY_RECPNT_EXT_ENTY_ST_NM"),
        when(
            (col("CNTY_NM").isNull()) | (length(trim(col("CNTY_NM"))) == 0),
            lit("")
        ).otherwise(col("CNTY_NM")).alias("PRVCY_DSCLSUR_ENTY_CNTY_NM"),
        lit(" ").alias("PRVCY_DSCLSUR_ENTY_AGNT_ID"),
        when(
            (col("CAT_CD_MPPNG_SK").isNull()) | (col("CAT_CD_MPPNG_SK") == lit("")),
            lit("UNK")
        ).otherwise(col("CAT_TRGT_CD")).alias("PRVCY_DSCLSUR_ENTY_CAT_CD"),
        when(
            (col("CAT_CD_MPPNG_SK").isNull()) | (col("CAT_CD_MPPNG_SK") == lit("")),
            lit("UNK")
        ).otherwise(col("CAT_TRGT_CD_NM")).alias("PRVCY_DSCLSUR_ENTY_CAT_NM"),
        when(
            (col("PRVCY_DSCLSUR_ENTY_ID") == lit("BCKC"))
            | (col("PRVCY_DSCLSUR_ENTY_ID") == lit("BCK00000000")),
            lit("740")
        ).otherwise(lit("   ")).alias("PRVCY_DSCLSUR_ENTY_PLN_CD_TX"),
        when(
            (col("TYP_CD_MPPNG_SK").isNull()) | (col("TYP_CD_MPPNG_SK") == lit("")),
            lit("UNK")
        ).otherwise(col("TYP_TRGT_CD")).alias("PRVCY_DSCLSUR_ENTY_TYP_CD"),
        when(
            (col("TYP_CD_MPPNG_SK").isNull()) | (col("TYP_CD_MPPNG_SK") == lit("")),
            lit("UNK")
        ).otherwise(col("TYP_TRGT_CD_NM")).alias("PRVCY_DSCLSUR_ENTY_TYP_NM"),
        lit(" ").alias("PRVCY_DSCLSUR_ENTY_USE_IN"),
        col("SRC_SYS_LAST_UPDT_DT_SK").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        col("SRC_SYS_LAST_UPDT_USER_SK").alias("SRC_SYS_LAST_UPDT_USER_SK"),
        lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("PRVCY_DSCLSUR_ENTY_CAT_CD_SK"),
        col("PRVCY_DSCLSUR_ENTY_TYP_CD_SK"),
    )
)

# 2) NA => constraint => "((@INROWNUM - 1)*@NUMPARTITIONS + @PARTITIONNUM + 1) = 1" 
#    This produces exactly one row with the given "WhereExpression" values.
#    We'll emulate row-1 logic by picking the first row of df_lkp_Codes, then overwrite columns with constants.
window_na = Window.orderBy(monotonically_increasing_id())
df_one_row_na = df_lkp_Codes.withColumn("_row_num", row_number().over(window_na)).filter(col("_row_num") == 1).limit(1)

df_NA = df_one_row_na.select(
    lit(1).alias("PRVCY_DSCLSUR_ENTY_SK"),  # will be overwritten next
    lit("NA").alias("SRC_SYS_CD"),          # will be overwritten next
    lit(1).alias("PRVCY_DSCLSUR_ENTY_UNIQ_KEY"),
    lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    lit("NA").alias("PRVCY_DSCLSUR_ENTY_ID"),
    lit("NA").alias("PRVCY_DSCLSUR_ENTY_FULL_NM"),
    lit("").alias("PRVCY_RECPNT_EXT_ENTY_ADDR_LN1"),
    lit("").alias("PRVCY_RECPNT_EXT_ENTY_ADDR_LN2"),
    lit("").alias("PRVCY_RECPNT_EXT_ENTY_ADDR_LN3"),
    lit("NA").alias("PRVCY_RECPNT_EXT_ENTY_CITY_NM"),
    lit("").alias("PRVCY_RECPNT_EXT_ENTY_ZIP_CD_5"),
    lit("").alias("PRVCY_RECPNT_EXT_ENTY_ZIP_CD_4"),
    lit("NA").alias("PRVCY_RECPNT_EXT_ENTY_ST_CD"),
    lit("NA").alias("PRVCY_RECPNT_EXT_ENTY_ST_NM"),
    lit("NA").alias("PRVCY_DSCLSUR_ENTY_CNTY_NM"),
    lit("NA").alias("PRVCY_DSCLSUR_ENTY_AGNT_ID"),
    lit("NA").alias("PRVCY_DSCLSUR_ENTY_CAT_CD"),
    lit("NA").alias("PRVCY_DSCLSUR_ENTY_CAT_NM"),
    lit("").alias("PRVCY_DSCLSUR_ENTY_PLN_CD_TX"),
    lit("NA").alias("PRVCY_DSCLSUR_ENTY_TYP_CD"),
    lit("NA").alias("PRVCY_DSCLSUR_ENTY_TYP_NM"),
    lit("N").alias("PRVCY_DSCLSUR_ENTY_USE_IN"),
    lit("1753-01-01").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    lit(1).alias("SRC_SYS_LAST_UPDT_USER_SK"),
    lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("PRVCY_DSCLSUR_ENTY_CAT_CD_SK"),
    lit(1).alias("PRVCY_DSCLSUR_ENTY_TYP_CD_SK"),
)

# 3) UNK => same partition logic => one row with these constants
df_one_row_unk = df_lkp_Codes.withColumn("_row_num", row_number().over(window_na)).filter(col("_row_num") == 1).limit(1)

df_UNK = df_one_row_unk.select(
    lit(0).alias("PRVCY_DSCLSUR_ENTY_SK"),
    lit("UNK").alias("SRC_SYS_CD"),
    lit(0).alias("PRVCY_DSCLSUR_ENTY_UNIQ_KEY"),
    lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    lit("UNK").alias("PRVCY_DSCLSUR_ENTY_ID"),
    lit("UNK").alias("PRVCY_DSCLSUR_ENTY_FULL_NM"),
    lit("").alias("PRVCY_RECPNT_EXT_ENTY_ADDR_LN1"),
    lit("").alias("PRVCY_RECPNT_EXT_ENTY_ADDR_LN2"),
    lit("").alias("PRVCY_RECPNT_EXT_ENTY_ADDR_LN3"),
    lit("UNK").alias("PRVCY_RECPNT_EXT_ENTY_CITY_NM"),
    lit("").alias("PRVCY_RECPNT_EXT_ENTY_ZIP_CD_5"),
    lit("").alias("PRVCY_RECPNT_EXT_ENTY_ZIP_CD_4"),
    lit("UNK").alias("PRVCY_RECPNT_EXT_ENTY_ST_CD"),
    lit("UNK").alias("PRVCY_RECPNT_EXT_ENTY_ST_NM"),
    lit("UNK").alias("PRVCY_DSCLSUR_ENTY_CNTY_NM"),
    lit("UNK").alias("PRVCY_DSCLSUR_ENTY_AGNT_ID"),
    lit("UNK").alias("PRVCY_DSCLSUR_ENTY_CAT_CD"),
    lit("UNK").alias("PRVCY_DSCLSUR_ENTY_CAT_NM"),
    lit("").alias("PRVCY_DSCLSUR_ENTY_PLN_CD_TX"),
    lit("UNK").alias("PRVCY_DSCLSUR_ENTY_TYP_CD"),
    lit("UNK").alias("PRVCY_DSCLSUR_ENTY_TYP_NM"),
    lit("N").alias("PRVCY_DSCLSUR_ENTY_USE_IN"),
    lit("1753-01-01").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    lit(0).alias("SRC_SYS_LAST_UPDT_USER_SK"),
    lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("PRVCY_DSCLSUR_ENTY_CAT_CD_SK"),
    lit(0).alias("PRVCY_DSCLSUR_ENTY_TYP_CD_SK"),
)

# --------------------- fnl_dataLinks (PxFunnel) ---------------------
df_fnl_dataLinks = df_NA.unionByName(df_xfm_BusinessLogic_lnk_Main).unionByName(df_UNK)

# --------------------- Final - apply rpad for char columns before writing ---------------------
df_fnl_dataLinks_rpad = (
    df_fnl_dataLinks
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("PRVCY_RECPNT_EXT_ENTY_ZIP_CD_5", rpad(col("PRVCY_RECPNT_EXT_ENTY_ZIP_CD_5"), 5, " "))
    .withColumn("PRVCY_RECPNT_EXT_ENTY_ZIP_CD_4", rpad(col("PRVCY_RECPNT_EXT_ENTY_ZIP_CD_4"), 4, " "))
    .withColumn("PRVCY_DSCLSUR_ENTY_USE_IN", rpad(col("PRVCY_DSCLSUR_ENTY_USE_IN"), 1, " "))
    .withColumn("SRC_SYS_LAST_UPDT_DT_SK", rpad(col("SRC_SYS_LAST_UPDT_DT_SK"), 10, " "))
)

# Maintain column order exactly as the funnel's output:
final_cols = [
    "PRVCY_DSCLSUR_ENTY_SK",
    "SRC_SYS_CD",
    "PRVCY_DSCLSUR_ENTY_UNIQ_KEY",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "PRVCY_DSCLSUR_ENTY_ID",
    "PRVCY_DSCLSUR_ENTY_FULL_NM",
    "PRVCY_RECPNT_EXT_ENTY_ADDR_LN1",
    "PRVCY_RECPNT_EXT_ENTY_ADDR_LN2",
    "PRVCY_RECPNT_EXT_ENTY_ADDR_LN3",
    "PRVCY_RECPNT_EXT_ENTY_CITY_NM",
    "PRVCY_RECPNT_EXT_ENTY_ZIP_CD_5",
    "PRVCY_RECPNT_EXT_ENTY_ZIP_CD_4",
    "PRVCY_RECPNT_EXT_ENTY_ST_CD",
    "PRVCY_RECPNT_EXT_ENTY_ST_NM",
    "PRVCY_DSCLSUR_ENTY_CNTY_NM",
    "PRVCY_DSCLSUR_ENTY_AGNT_ID",
    "PRVCY_DSCLSUR_ENTY_CAT_CD",
    "PRVCY_DSCLSUR_ENTY_CAT_NM",
    "PRVCY_DSCLSUR_ENTY_PLN_CD_TX",
    "PRVCY_DSCLSUR_ENTY_TYP_CD",
    "PRVCY_DSCLSUR_ENTY_TYP_NM",
    "PRVCY_DSCLSUR_ENTY_USE_IN",
    "SRC_SYS_LAST_UPDT_DT_SK",
    "SRC_SYS_LAST_UPDT_USER_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PRVCY_DSCLSUR_ENTY_CAT_CD_SK",
    "PRVCY_DSCLSUR_ENTY_TYP_CD_SK"
]
df_final = df_fnl_dataLinks_rpad.select(final_cols)

# --------------------- seq_PRVCY_DSCLSUR_ENTY_D_Load (PxSequentialFile) ---------------------
write_files(
    df_final,
    f"{adls_path}/load/PRVCY_DSCLSUR_ENTY_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)