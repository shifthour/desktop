# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC               Oliver Nielsen  09/09/2005    Originally Programmed
# MAGIC               Steph Goddard 04/18/2006   Checked for NA or UNK default rows before setting license number
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                        DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                             ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        --------------------------------------------------------------------------------------               -------------------------------    ------------------------------    ----------------------
# MAGIC Bhupinder Kaur            06/05/2013      5114                               Create Load File for EDW Table CMN_PRCT_D                    EnterpriseWhseDevl             Jag Yelavarthi          2013-09-01
# MAGIC 
# MAGIC Revathi BoojiReddy    12/10/2023     US  598278                     Added new column CULT_CPBLTY_NM  to source query      EnterpriseDevB              Jeyaprasanna              2023-10-18
# MAGIC                                                                                                     and propagated till target

# MAGIC DB2 Queries are taking the max of LIC_SEQ_NO
# MAGIC Write CMN_PRCT Data into a Sequential file for Load Job IdsEdwProvCmnPrctDLoad.
# MAGIC Read from IDS source table:
# MAGIC CMN_PRCT 
# MAGIC 
# MAGIC Run Cycle filters is applied to get just the needed rows forward
# MAGIC Job: IdsEdwProvCmnPrctDExtr
# MAGIC Add Defaults and Null Handling.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Code SK lookups for Denormalization
# MAGIC Lookup Keys:
# MAGIC CMN_PRCT_GNDR_CD_SK
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, lit, when, length, rpad, trim as pyspark_trim
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------

# MAGIC %run ../../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

extract_query_db2_CD_MPPNG_in = f"""SELECT
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CD_MPPNG_in)
    .load()
)

extract_query_db2_CMN_PRCT_D_Extr = f"""SELECT distinct
A.CMN_PRCT_SK,
A.SRC_SYS_CD_SK,
A.CMN_PRCT_ID,
A.CMN_PRCT_GNDR_CD_SK,
A.ACTV_IN,
A.BRTH_DT_SK,
A.INIT_CRDTL_DT_SK,
A.LAST_CRDTL_DT_SK,
A.NEXT_CRDTL_DT_SK,
A.FIRST_NM,
A.LAST_NM,
A.MIDINIT,
A.CMN_PRCT_TTL,
A.NTNL_PROV_ID,
A.SSN,
COALESCE(CM.TRGT_CD,'UNK') as SRC_SYS_CD,
A.CULT_CPBLTY_NM
FROM {IDSOwner}.CMN_PRCT A
LEFT OUTER JOIN {IDSOwner}.CD_MPPNG CM 
ON A.SRC_SYS_CD_SK=CM.CD_MPPNG_SK
"""
df_db2_CMN_PRCT_D_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CMN_PRCT_D_Extr)
    .load()
)

extract_query_db2_CMN_PRCT_CRDTL_SPEC_in = f"""select
distinct(CMN_PRCT_SK),
CRDTL_SPEC_DESC
FROM {IDSOwner}.CMN_PRCT_CRDTL_SPEC
WHERE CRDTL_SPEC_SEQ_NO=1
order by CRDTL_SPEC_DESC
"""
df_db2_CMN_PRCT_CRDTL_SPEC_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CMN_PRCT_CRDTL_SPEC_in)
    .load()
)

extract_query_db2_ENTY_LIC_Mo_in = f"""SELECT
 A.ENTY_LIC_SK,
 B.TRGT_CD,
 A.LIC_NO
FROM {IDSOwner}.ENTY_LIC A, {IDSOwner}.CD_MPPNG B,
(
  SELECT A.ENTY_LIC_SK, B.TRGT_CD, MAX(A.LIC_SEQ_NO) LIC_SEQ_NO
  FROM {IDSOwner}.ENTY_LIC A, {IDSOwner}.CD_MPPNG B
  WHERE A.ENTY_LIC_ST_CD_SK=B.CD_MPPNG_SK
  AND A.EFF_DT_SK <= '{EDWRunCycleDate}'
  AND A.TERM_DT_SK >= '{EDWRunCycleDate}'
  AND B.TRGT_CD='MO'
  GROUP BY A.ENTY_LIC_SK,B.TRGT_CD
)INNER_TAB
WHERE A.ENTY_LIC_ST_CD_SK=B.CD_MPPNG_SK
AND A.EFF_DT_SK <= '{EDWRunCycleDate}'
AND A.TERM_DT_SK >= '{EDWRunCycleDate}'
AND B.TRGT_CD='MO'
AND A.LIC_SEQ_NO=INNER_TAB.LIC_SEQ_NO
AND B.TRGT_CD=INNER_TAB.TRGT_CD
AND A.ENTY_LIC_SK=INNER_TAB.ENTY_LIC_SK
"""
df_db2_ENTY_LIC_Mo_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_ENTY_LIC_Mo_in)
    .load()
)

extract_query_db2_ENTY_LIC_Ks_in = f"""SELECT
 A.ENTY_LIC_SK,
 B.TRGT_CD,
 A.LIC_NO,
 A.EFF_DT_SK,
 A.LIC_SEQ_NO
FROM {IDSOwner}.ENTY_LIC A, {IDSOwner}.CD_MPPNG B,
(
  SELECT A.ENTY_LIC_SK, B.TRGT_CD, MAX(A.LIC_SEQ_NO) LIC_SEQ_NO
  FROM {IDSOwner}.ENTY_LIC A, {IDSOwner}.CD_MPPNG B
  WHERE A.ENTY_LIC_ST_CD_SK=B.CD_MPPNG_SK
  AND A.EFF_DT_SK <= '{EDWRunCycleDate}'
  AND A.TERM_DT_SK >= '{EDWRunCycleDate}'
  AND B.TRGT_CD='KS'
  GROUP BY A.ENTY_LIC_SK,B.TRGT_CD
)INNER_TAB
WHERE A.ENTY_LIC_ST_CD_SK=B.CD_MPPNG_SK
AND A.EFF_DT_SK <= '{EDWRunCycleDate}'
AND A.TERM_DT_SK >= '{EDWRunCycleDate}'
AND B.TRGT_CD='KS'
AND A.LIC_SEQ_NO=INNER_TAB.LIC_SEQ_NO
AND B.TRGT_CD=INNER_TAB.TRGT_CD
AND A.ENTY_LIC_SK=INNER_TAB.ENTY_LIC_SK
"""
df_db2_ENTY_LIC_Ks_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_ENTY_LIC_Ks_in)
    .load()
)

df_lkp_FKeys = (
    df_db2_CMN_PRCT_D_Extr.alias("lnk_IdsProvCmnPrctExtr_InABC")
    .join(
        df_db2_CMN_PRCT_CRDTL_SPEC_in.alias("Ref_CmnSpec"),
        col("lnk_IdsProvCmnPrctExtr_InABC.CMN_PRCT_SK") == col("Ref_CmnSpec.CMN_PRCT_SK"),
        "left"
    )
    .join(
        df_db2_ENTY_LIC_Mo_in.alias("Ref_EntyLicMO"),
        col("lnk_IdsProvCmnPrctExtr_InABC.CMN_PRCT_SK") == col("Ref_EntyLicMO.ENTY_LIC_SK"),
        "left"
    )
    .join(
        df_db2_ENTY_LIC_Ks_in.alias("Ref_EntyLicKS"),
        col("lnk_IdsProvCmnPrctExtr_InABC.CMN_PRCT_SK") == col("Ref_EntyLicKS.ENTY_LIC_SK"),
        "left"
    )
    .select(
        col("lnk_IdsProvCmnPrctExtr_InABC.CMN_PRCT_SK").alias("CMN_PRCT_SK"),
        col("lnk_IdsProvCmnPrctExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnk_IdsProvCmnPrctExtr_InABC.CMN_PRCT_ID").alias("CMN_PRCT_ID"),
        col("lnk_IdsProvCmnPrctExtr_InABC.CMN_PRCT_GNDR_CD_SK").alias("CMN_PRCT_GNDR_CD_SK"),
        col("lnk_IdsProvCmnPrctExtr_InABC.ACTV_IN").alias("ACTV_IN"),
        col("lnk_IdsProvCmnPrctExtr_InABC.BRTH_DT_SK").alias("BRTH_DT_SK"),
        col("lnk_IdsProvCmnPrctExtr_InABC.INIT_CRDTL_DT_SK").alias("INIT_CRDTL_DT_SK"),
        col("lnk_IdsProvCmnPrctExtr_InABC.LAST_CRDTL_DT_SK").alias("LAST_CRDTL_DT_SK"),
        col("lnk_IdsProvCmnPrctExtr_InABC.NEXT_CRDTL_DT_SK").alias("NEXT_CRDTL_DT_SK"),
        col("lnk_IdsProvCmnPrctExtr_InABC.FIRST_NM").alias("FIRST_NM"),
        col("lnk_IdsProvCmnPrctExtr_InABC.LAST_NM").alias("LAST_NM"),
        col("lnk_IdsProvCmnPrctExtr_InABC.MIDINIT").alias("MIDINIT"),
        col("lnk_IdsProvCmnPrctExtr_InABC.CMN_PRCT_TTL").alias("CMN_PRCT_TTL"),
        col("lnk_IdsProvCmnPrctExtr_InABC.NTNL_PROV_ID").alias("NTNL_PROV_ID"),
        col("lnk_IdsProvCmnPrctExtr_InABC.SSN").alias("SSN"),
        col("Ref_EntyLicMO.TRGT_CD").alias("ENTY_LIC_ST_CD_MO"),
        col("Ref_EntyLicMO.LIC_NO").alias("LIC_NO_MO"),
        col("Ref_CmnSpec.CRDTL_SPEC_DESC").alias("CRDTL_SPEC_DESC"),
        col("Ref_EntyLicKS.TRGT_CD").alias("ENTY_LIC_ST_CD_KS"),
        col("Ref_EntyLicKS.LIC_NO").alias("LIC_NO_KS"),
        col("lnk_IdsProvCmnPrctExtr_InABC.CULT_CPBLTY_NM").alias("CULT_CPBLTY_NM")
    )
)

df_lkp_Codes = (
    df_lkp_FKeys.alias("lnkFKeyLkpData_Out")
    .join(
        df_db2_CD_MPPNG_in.alias("Ref_GndrCd"),
        col("lnkFKeyLkpData_Out.CMN_PRCT_GNDR_CD_SK") == col("Ref_GndrCd.CD_MPPNG_SK"),
        "left"
    )
    .select(
        col("lnkFKeyLkpData_Out.CMN_PRCT_SK").alias("CMN_PRCT_SK"),
        col("lnkFKeyLkpData_Out.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnkFKeyLkpData_Out.CMN_PRCT_ID").alias("CMN_PRCT_ID"),
        col("lnkFKeyLkpData_Out.CMN_PRCT_GNDR_CD_SK").alias("CMN_PRCT_GNDR_CD_SK"),
        col("lnkFKeyLkpData_Out.ACTV_IN").alias("ACTV_IN"),
        col("lnkFKeyLkpData_Out.BRTH_DT_SK").alias("BRTH_DT_SK"),
        col("lnkFKeyLkpData_Out.INIT_CRDTL_DT_SK").alias("INIT_CRDTL_DT_SK"),
        col("lnkFKeyLkpData_Out.LAST_CRDTL_DT_SK").alias("LAST_CRDTL_DT_SK"),
        col("lnkFKeyLkpData_Out.NEXT_CRDTL_DT_SK").alias("NEXT_CRDTL_DT_SK"),
        col("lnkFKeyLkpData_Out.FIRST_NM").alias("FIRST_NM"),
        col("lnkFKeyLkpData_Out.LAST_NM").alias("LAST_NM"),
        col("lnkFKeyLkpData_Out.MIDINIT").alias("MIDINIT"),
        col("Ref_GndrCd.TRGT_CD").alias("GNDR_CD"),
        col("Ref_GndrCd.TRGT_CD_NM").alias("GNDR_NM"),
        col("lnkFKeyLkpData_Out.CMN_PRCT_TTL").alias("CMN_PRCT_TTL"),
        col("lnkFKeyLkpData_Out.NTNL_PROV_ID").alias("NTNL_PROV_ID"),
        col("lnkFKeyLkpData_Out.SSN").alias("SSN"),
        col("lnkFKeyLkpData_Out.ENTY_LIC_ST_CD_MO").alias("ENTY_LIC_ST_CD_MO"),
        col("lnkFKeyLkpData_Out.LIC_NO_MO").alias("LIC_NO_MO"),
        col("lnkFKeyLkpData_Out.CRDTL_SPEC_DESC").alias("CRDTL_SPEC_DESC"),
        col("lnkFKeyLkpData_Out.ENTY_LIC_ST_CD_KS").alias("ENTY_LIC_ST_CD_KS"),
        col("lnkFKeyLkpData_Out.LIC_NO_KS").alias("LIC_NO_KS"),
        col("lnkFKeyLkpData_Out.CULT_CPBLTY_NM").alias("CULT_CPBLTY_NM")
    )
)

df_xfrm_BusinessLogic = (
    df_lkp_Codes
    .withColumn("CMN_PRCT_SK", col("CMN_PRCT_SK"))
    .withColumn("SRC_SYS_CD",
        when(
            col("SRC_SYS_CD").isNull() | (pyspark_trim(col("SRC_SYS_CD")) == ""),
            lit("UNK")
        ).otherwise(col("SRC_SYS_CD"))
    )
    .withColumn("CMN_PRCT_ID", col("CMN_PRCT_ID"))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", lit(EDWRunCycleDate))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", lit(EDWRunCycleDate))
    .withColumn("CMN_PRCT_ACTV_IN", col("ACTV_IN"))
    .withColumn("CMN_PRCT_BRTH_DT_SK", col("BRTH_DT_SK"))
    .withColumn("CMN_PRCT_CRDTL_SPEC_DESC", col("CRDTL_SPEC_DESC"))
    .withColumn("CMN_PRCT_FIRST_NM", col("FIRST_NM"))
    .withColumn(
        "CMN_PRCT_MIDINIT",
        when(
            pyspark_trim(
                when(col("MIDINIT").isNull(), lit("")).otherwise(col("MIDINIT")))
            == ',',
            None
        ).otherwise(col("MIDINIT"))
    )
    .withColumn("CMN_PRCT_LAST_NM", col("LAST_NM"))
    .withColumn("CMN_PRCT_GNDR_CD",
        when(
            col("GNDR_CD").isNull() | (pyspark_trim(col("GNDR_CD")) == ""),
            lit("UNK")
        ).otherwise(col("GNDR_CD"))
    )
    .withColumn("CMN_PRCT_GNDR_NM", col("GNDR_NM"))
    .withColumn("CMN_PRCT_INIT_CRDTL_DT_SK", col("INIT_CRDTL_DT_SK"))
    .withColumn("CMN_PRCT_KS_LIC_ID",
        when(
            col("LIC_NO_KS").isNull() | (pyspark_trim(col("LIC_NO_KS")) == ""),
            lit("NA")
        ).otherwise(col("LIC_NO_KS"))
    )
    .withColumn("CMN_PRCT_LAST_CRDTL_DT_SK", col("LAST_CRDTL_DT_SK"))
    .withColumn("CMN_PRCT_MO_LIC_ID",
        when(
            col("LIC_NO_MO").isNull() | (pyspark_trim(col("LIC_NO_MO")) == ""),
            lit("NA")
        ).otherwise(col("LIC_NO_MO"))
    )
    .withColumn("CMN_PRCT_NTNL_PROV_ID", col("NTNL_PROV_ID"))
    .withColumn("CMN_PRCT_NEXT_CRDTL_DT_SK", col("NEXT_CRDTL_DT_SK"))
    .withColumn("CMN_PRCT_SSN", col("SSN"))
    .withColumn("CMN_PRCT_TTL", col("CMN_PRCT_TTL"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(EDWRunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(EDWRunCycle))
    .withColumn("CMN_PRCT_GNDR_CD_SK", col("CMN_PRCT_GNDR_CD_SK"))
    .withColumn("CULT_CPBLTY_NM", col("CULT_CPBLTY_NM"))
)

df_final = df_xfrm_BusinessLogic.select(
    col("CMN_PRCT_SK"),
    col("SRC_SYS_CD"),
    col("CMN_PRCT_ID"),
    rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    rpad(col("CMN_PRCT_ACTV_IN"), 1, " ").alias("CMN_PRCT_ACTV_IN"),
    rpad(col("CMN_PRCT_BRTH_DT_SK"), 10, " ").alias("CMN_PRCT_BRTH_DT_SK"),
    col("CMN_PRCT_CRDTL_SPEC_DESC"),
    col("CMN_PRCT_FIRST_NM"),
    rpad(col("CMN_PRCT_MIDINIT"), 1, " ").alias("CMN_PRCT_MIDINIT"),
    col("CMN_PRCT_LAST_NM"),
    col("CMN_PRCT_GNDR_CD"),
    col("CMN_PRCT_GNDR_NM"),
    rpad(col("CMN_PRCT_INIT_CRDTL_DT_SK"), 10, " ").alias("CMN_PRCT_INIT_CRDTL_DT_SK"),
    col("CMN_PRCT_KS_LIC_ID"),
    rpad(col("CMN_PRCT_LAST_CRDTL_DT_SK"), 10, " ").alias("CMN_PRCT_LAST_CRDTL_DT_SK"),
    col("CMN_PRCT_MO_LIC_ID"),
    col("CMN_PRCT_NTNL_PROV_ID"),
    rpad(col("CMN_PRCT_NEXT_CRDTL_DT_SK"), 10, " ").alias("CMN_PRCT_NEXT_CRDTL_DT_SK"),
    col("CMN_PRCT_SSN"),
    col("CMN_PRCT_TTL"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CMN_PRCT_GNDR_CD_SK"),
    col("CULT_CPBLTY_NM")
)

write_files(
    df_final,
    f"{adls_path}/load/CMN_PRCT_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)