# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2010 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  FctsIncomeInvcCmpntCtExtr
# MAGIC Called by:
# MAGIC                     FctsIncomeExtrSeq
# MAGIC 
# MAGIC Processing:
# MAGIC                     Pull data from FACETS CDS_INCT_COMPONENT using the driver table created in the Income/Driver/FctsIncomeTempLoad job.
# MAGIC                     Data will be used to populated the IDS INVC_CMPNT_CT table which is the primary source for the EDW INVC_CMPNT_CT_F table.
# MAGIC 
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:    Restart, no other steps necessary
# MAGIC                     Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                  Project/                                                                                                                       Code                   Date
# MAGIC Developer           Date              Altiris #     Change Description                                                                                     Reviewer            Reviewed
# MAGIC -------------------------  -------------------   -------------   -----------------------------------------------------------------------------------------------------------------   -------------------------  -------------------
# MAGIC Hugh Sisson       2010-09-13    3346        Original program                                                                                           Steph Goddard   09/21/2010
# MAGIC 
# MAGIC Goutham Kalidindi  2021-03-24  258186      Changed Datatype length for field                                                         IntegrateDev1   Kalyan Neelam    2021-03-31
# MAGIC                                                                         BLIV_ID
# MAGIC                                                                           char(12) to Varchar(15)
# MAGIC Prabhu ES           2022-03-02   S2S          MSSQL ODBC conn added and other param changes                            IntegrateDev5\(9)Ken Bradmon\(9)2022-06-15

# MAGIC The hf_invc_cmpnt_ct_allcol hashed file created in the IncomeInvcCmpntCtPK is cleared at the end of this job
# MAGIC Pulls all Invoice rows from facets CDS_INSB_SB_DETAIL table matching data criteria.
# MAGIC Target code lookup - 
# MAGIC Natural key contains 3 integrated code values that require a lookup based on the source code values
# MAGIC Hashed file hf_invc_rebills is created by the FctsPreReqHashFiles job and is cleared by the ClearHash stage in FctsCleanupSeq
# MAGIC Assign primary surrogate key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, isnull, rpad, concat_ws
from pyspark.sql.types import IntegerType, StringType, DecimalType, DateType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


DriverTable = get_widget_value("DriverTable","TMP_IDS_INCOME")
SrcSysCd = get_widget_value("SrcSysCd","FACETS")
SrcSysCdSk = get_widget_value("SrcSysCdSk","1581")
RunID = get_widget_value("RunID","")
CurrDate = get_widget_value("CurrDate","")
CurrRunCycle = get_widget_value("CurrRunCycle","")
FacetsOwner = get_widget_value("FacetsOwner","")
facets_secret_name = get_widget_value("facets_secret_name","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")

# MAGIC %run ../../../../../shared_containers/PrimaryKey/IncomeInvcCmpntCtPK
# COMMAND ----------

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)

df_hf_invc_rebills = spark.read.parquet(f"{adls_path}/hf_invc_rebills.parquet").select("BILL_INVC_ID")

extract_query_CD_MPPNG = f"""
SELECT 
  CD_MPPNG.TRGT_DOMAIN_NM,
  CD_MPPNG.SRC_CD,
  CD_MPPNG.TRGT_CD
FROM {IDSOwner}.CD_MPPNG CD_MPPNG
WHERE CD_MPPNG.TRGT_DOMAIN_NM IN (
  'COMMISSION DETAIL DISPOSITION',
  'INVOICE SUBSCRIBER PREMIUM TYPE',
  'INVOICE COMPONENT COUNT SOURCE'
)
"""
dfCD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_CD_MPPNG)
    .load()
)
df_hf_invc_cmpnt_ct_trgt_codes = dedup_sort(dfCD_MPPNG, ["TRGT_DOMAIN_NM", "SRC_CD"], [])

extract_query_CDS_INCT_COMPONENT = f"""
SELECT DISTINCT
  INCT.BLIV_ID,
  INCT.CSPI_ID,
  INCT.PDPD_ID,
  INCT.PDBL_ID,
  INCT.BLCT_DISP_CD,
  INCT.BLCT_PREM_TYPE,
  INCT.BLCT_SOURCE,
  INCT.BLCT_BILLED_AMT,
  INCT.BLCT_LVS_SB,
  INCT.BLCT_LVS_DEP,
  INID.BLEI_CK,
  INID.BLBL_DUE_DT
FROM tempdb..{DriverTable} TmpInvc,
     {FacetsOwner}.CDS_INCT_COMPONENT INCT,
     {FacetsOwner}.CDS_INID_INVOICE INID
WHERE TmpInvc.BILL_INVC_ID = INCT.BLIV_ID
  AND INCT.BLIV_ID = INID.BLIV_ID
"""
df_CDS_INCT_COMPONENT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_CDS_INCT_COMPONENT)
    .load()
)

df_strip = (
    df_CDS_INCT_COMPONENT
    .withColumn("BLIV_ID", trim(strip_field(col("BLIV_ID"))))
    .withColumn("CSPI_ID", trim(strip_field(col("CSPI_ID"))))
    .withColumn("PDPD_ID", trim(strip_field(col("PDPD_ID"))))
    .withColumn("PDBL_ID", trim(strip_field(col("PDBL_ID"))))
    .withColumn("BLCT_DISP_CD", col("BLCT_DISP_CD"))
    .withColumn("BLCT_PREM_TYPE", col("BLCT_PREM_TYPE"))
    .withColumn("BLCT_SOURCE", col("BLCT_SOURCE"))
    .withColumn("BLCT_BILLED_AMT", col("BLCT_BILLED_AMT"))
    .withColumn("BLCT_LVS_SB", col("BLCT_LVS_SB"))
    .withColumn("BLCT_LVS_DEP", col("BLCT_LVS_DEP"))
    .withColumn("BLEI_CK", col("BLEI_CK"))
    .withColumn("BLBL_DUE_DT", col("BLBL_DUE_DT"))
)

df_businessrules_pre = df_strip.withColumn(
    "svBillDueDt",
    when(isnull(col("BLBL_DUE_DT")), lit("")).otherwise(col("BLBL_DUE_DT"))
)

df_disp = df_hf_invc_cmpnt_ct_trgt_codes.filter(
    (col("TRGT_DOMAIN_NM") == "COMMISSION DETAIL DISPOSITION")
).select(col("SRC_CD").alias("disp_SRC_CD"), col("TRGT_CD").alias("disp_TRGT_CD"))

df_prmtyp = df_hf_invc_cmpnt_ct_trgt_codes.filter(
    (col("TRGT_DOMAIN_NM") == "INVOICE SUBSCRIBER PREMIUM TYPE")
).select(col("SRC_CD").alias("prmtyp_SRC_CD"), col("TRGT_CD").alias("prmtyp_TRGT_CD"))

df_src = df_hf_invc_cmpnt_ct_trgt_codes.filter(
    (col("TRGT_DOMAIN_NM") == "INVOICE COMPONENT COUNT SOURCE")
).select(col("SRC_CD").alias("src_SRC_CD"), col("TRGT_CD").alias("src_TRGT_CD"))

execute_dml(f"DROP TABLE IF EXISTS STAGING.FctsIncomeInvcCmpntCtExtr_PROD_BILL_CMPNT_temp", jdbc_url_ids, jdbc_props_ids)
temp_table_name = "STAGING.FctsIncomeInvcCmpntCtExtr_PROD_BILL_CMPNT_temp"
df_businessrules_pre.write.jdbc(
    temp_table_name,
    mode="overwrite",
    properties=jdbc_props_ids,
    url=jdbc_url_ids
)
prod_cmpnt_query = f"""
SELECT PC.SRC_SYS_CD_SK,
       PC.PROD_ID,
       PC.PROD_CMPNT_EFF_DT_SK,
       PC.PROD_CMPNT_TERM_DT_SK,
       PC.PROD_CMPNT_PFX_ID,
       ST.svBillDueDt as ST_svBillDueDt,
       ST.BLIV_ID as ST_BLIV_ID,
       ST.CSPI_ID as ST_CSPI_ID,
       ST.PDPD_ID as ST_PDPD_ID,
       ST.PDBL_ID as ST_PDBL_ID,
       ST.BLCT_DISP_CD as ST_BLCT_DISP_CD,
       ST.BLCT_PREM_TYPE as ST_BLCT_PREM_TYPE,
       ST.BLCT_SOURCE as ST_BLCT_SOURCE,
       ST.BLCT_BILLED_AMT as ST_BLCT_BILLED_AMT,
       ST.BLCT_LVS_SB as ST_BLCT_LVS_SB,
       ST.BLCT_LVS_DEP as ST_BLCT_LVS_DEP,
       ST.BLEI_CK as ST_BLEI_CK,
       ST.BLBL_DUE_DT as ST_BLBL_DUE_DT
FROM {IDSOwner}.PROD_CMPNT PC
JOIN {IDSOwner}.CD_MPPNG CM
  ON PC.PROD_CMPNT_TYP_CD_SK = CM.CD_MPPNG_SK
  AND CM.TRGT_CD = 'PDBL'
JOIN {temp_table_name} ST
  ON PC.SRC_SYS_CD_SK = {SrcSysCdSk}
  AND PC.PROD_ID = ST.PDPD_ID
  AND PC.PROD_CMPNT_EFF_DT_SK <= ST.svBillDueDt
  AND PC.PROD_CMPNT_TERM_DT_SK >= ST.svBillDueDt
"""
df_prod_cmpnt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", prod_cmpnt_query)
    .load()
)

df_br_disp_join = df_businessrules_pre.alias("br").join(
    df_disp.alias("dsp"), col("br.BLCT_DISP_CD") == col("dsp.disp_SRC_CD"), "left"
)
df_br_prmtyp_join = df_br_disp_join.alias("br").join(
    df_prmtyp.alias("pt"), col("br.BLCT_PREM_TYPE") == col("pt.prmtyp_SRC_CD"), "left"
)
df_br_src_join = df_br_prmtyp_join.alias("br").join(
    df_src.alias("sc"), col("br.BLCT_SOURCE") == col("sc.src_SRC_CD"), "left"
)

df_prod_cmpnt_sel = df_prod_cmpnt.select(
    "ST_BLIV_ID",
    "ST_CSPI_ID",
    "ST_PDPD_ID",
    "ST_PDBL_ID",
    "ST_BLCT_DISP_CD",
    "ST_BLCT_PREM_TYPE",
    "ST_BLCT_SOURCE",
    "ST_BLCT_BILLED_AMT",
    "ST_BLCT_LVS_SB",
    "ST_BLCT_LVS_DEP",
    "ST_BLEI_CK",
    "ST_BLBL_DUE_DT",
    col("PROD_CMPNT_PFX_ID").alias("pcp_PROD_CMPNT_PFX_ID")
)

df_br_fulljoin = df_br_src_join.alias("br").join(
    df_prod_cmpnt_sel.alias("pc"),
    (
        (col("br.BLIV_ID") == col("pc.ST_BLIV_ID")) &
        (col("br.CSPI_ID") == col("pc.ST_CSPI_ID")) &
        (col("br.PDPD_ID") == col("pc.ST_PDPD_ID")) &
        (col("br.PDBL_ID") == col("pc.ST_PDBL_ID"))
    ),
    "left"
)

df_businessrules = df_br_fulljoin.select(
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit("I").alias("INSRT_UPDT_CD"),
    lit("N").alias("DISCARD_IN"),
    lit("Y").alias("PASS_THRU_IN"),
    col("CurrDate").alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    col("SrcSysCd").alias("SRC_SYS_CD"),
    concat_ws(";", col("br.BLIV_ID"), col("br.CSPI_ID"), col("br.PDPD_ID"), col("br.PDBL_ID"),
              when(isnull(col("dsp.disp_TRGT_CD")), lit("UNK")).otherwise(col("dsp.disp_TRGT_CD")),
              when(isnull(col("pt.prmtyp_TRGT_CD")), lit("UNK")).otherwise(col("pt.prmtyp_TRGT_CD")),
              when(isnull(col("sc.src_TRGT_CD")), lit("UNK")).otherwise(col("sc.src_TRGT_CD")),
              col("SrcSysCd")
    ).alias("PRI_KEY_STRING"),
    lit(0).alias("INVC_CMPNT_CT_SK"),
    col("br.BLIV_ID").alias("BILL_INVC_ID"),
    col("br.CSPI_ID").alias("CLS_PLN_ID"),
    col("br.PDPD_ID").alias("PROD_ID"),
    col("br.PDBL_ID").alias("PROD_BILL_CMPNT_ID"),
    when(isnull(col("dsp.disp_TRGT_CD")), lit("UNK")).otherwise(col("dsp.disp_TRGT_CD")).alias("INVC_CMPNT_CT_DISP_CD"),
    when(isnull(col("pt.prmtyp_TRGT_CD")), lit("UNK")).otherwise(col("pt.prmtyp_TRGT_CD")).alias("INVC_CMPNT_CT_PRM_TYP_CD"),
    when(isnull(col("sc.src_TRGT_CD")), lit("UNK")).otherwise(col("sc.src_TRGT_CD")).alias("INVC_CMPNT_CT_SRC_CD"),
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("br.svBillDueDt").alias("BILL_DUE_DT_SK"),
    col("br.BLCT_BILLED_AMT").alias("BILL_AMT"),
    col("br.BLEI_CK").alias("BILL_ENTY_UNIQ_KEY"),
    col("br.BLCT_LVS_DEP").alias("DPNDT_LVS_CT"),
    col("br.BLCT_LVS_SB").alias("SUB_LVS_CT"),
    col("br.BLCT_DISP_CD").alias("BLCT_DISP_CD"),
    col("br.BLCT_PREM_TYPE").alias("BLCT_PREM_TYPE"),
    col("br.BLCT_SOURCE").alias("BLCT_SOURCE"),
    when(isnull(col("pc.pcP_PROD_CMPNT_PFX_ID")), lit("UNK"))
       .otherwise(col("pc.pcP_PROD_CMPNT_PFX_ID"))
       .alias("PROD_CMPNT_PFX_ID")
)

df_reversal_logic_join = df_businessrules.alias("ChkReversals").join(
    df_hf_invc_rebills.alias("rebill_lkup"),
    col("ChkReversals.BILL_INVC_ID") == col("rebill_lkup.BILL_INVC_ID"),
    "left"
)

df_nonreversal = df_reversal_logic_join.filter(isnull(col("rebill_lkup.BILL_INVC_ID"))).select(
    col("ChkReversals.JOB_EXCTN_RCRD_ERR_SK"),
    col("ChkReversals.INSRT_UPDT_CD"),
    col("ChkReversals.DISCARD_IN"),
    col("ChkReversals.PASS_THRU_IN"),
    col("ChkReversals.FIRST_RECYC_DT"),
    col("ChkReversals.ERR_CT"),
    col("ChkReversals.RECYCLE_CT"),
    col("ChkReversals.SRC_SYS_CD"),
    col("ChkReversals.PRI_KEY_STRING"),
    col("ChkReversals.INVC_CMPNT_CT_SK"),
    col("ChkReversals.BILL_INVC_ID"),
    col("ChkReversals.CLS_PLN_ID"),
    col("ChkReversals.PROD_ID"),
    col("ChkReversals.PROD_BILL_CMPNT_ID"),
    col("ChkReversals.INVC_CMPNT_CT_DISP_CD"),
    col("ChkReversals.INVC_CMPNT_CT_PRM_TYP_CD"),
    col("ChkReversals.INVC_CMPNT_CT_SRC_CD"),
    col("ChkReversals.SRC_SYS_CD_SK"),
    col("ChkReversals.CRT_RUN_CYC_EXCTN_SK"),
    col("ChkReversals.LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("ChkReversals.BILL_DUE_DT_SK"),
    col("ChkReversals.BILL_AMT"),
    col("ChkReversals.BILL_ENTY_UNIQ_KEY"),
    col("ChkReversals.DPNDT_LVS_CT"),
    col("ChkReversals.SUB_LVS_CT"),
    col("ChkReversals.BLCT_DISP_CD"),
    col("ChkReversals.BLCT_PREM_TYPE"),
    col("ChkReversals.BLCT_SOURCE"),
    col("ChkReversals.PROD_CMPNT_PFX_ID")
)

df_reversal = df_reversal_logic_join.filter(~isnull(col("rebill_lkup.BILL_INVC_ID"))).select(
    col("ChkReversals.JOB_EXCTN_RCRD_ERR_SK"),
    col("ChkReversals.INSRT_UPDT_CD"),
    col("ChkReversals.DISCARD_IN"),
    col("ChkReversals.PASS_THRU_IN"),
    col("ChkReversals.FIRST_RECYC_DT"),
    col("ChkReversals.ERR_CT"),
    col("ChkReversals.RECYCLE_CT"),
    col("ChkReversals.SRC_SYS_CD"),
    col("ChkReversals.svReversalsString").alias("PRI_KEY_STRING"),
    col("ChkReversals.INVC_CMPNT_CT_SK"),
    concat_ws("", col("ChkReversals.BILL_INVC_ID"), lit("R")).alias("BILL_INVC_ID"),
    col("ChkReversals.CLS_PLN_ID"),
    col("ChkReversals.PROD_ID"),
    col("ChkReversals.PROD_BILL_CMPNT_ID"),
    col("ChkReversals.INVC_CMPNT_CT_DISP_CD"),
    col("ChkReversals.INVC_CMPNT_CT_PRM_TYP_CD"),
    col("ChkReversals.INVC_CMPNT_CT_SRC_CD"),
    col("ChkReversals.SRC_SYS_CD_SK"),
    col("ChkReversals.CRT_RUN_CYC_EXCTN_SK"),
    col("ChkReversals.LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("ChkReversals.BILL_DUE_DT_SK"),
    (-1 * col("ChkReversals.BILL_AMT")).alias("BILL_AMT"),
    col("ChkReversals.BILL_ENTY_UNIQ_KEY"),
    (-1 * col("ChkReversals.DPNDT_LVS_CT")).alias("DPNDT_LVS_CT"),
    (-1 * col("ChkReversals.SUB_LVS_CT")).alias("SUB_LVS_CT"),
    col("ChkReversals.BLCT_DISP_CD"),
    col("ChkReversals.BLCT_PREM_TYPE"),
    col("ChkReversals.BLCT_SOURCE"),
    col("ChkReversals.PROD_CMPNT_PFX_ID")
)

df_collected = df_nonreversal.unionByName(df_reversal)

df_key = df_collected.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "INVC_CMPNT_CT_SK",
    "BILL_INVC_ID",
    "CLS_PLN_ID",
    "PROD_ID",
    "PROD_BILL_CMPNT_ID",
    "INVC_CMPNT_CT_DISP_CD",
    "INVC_CMPNT_CT_PRM_TYP_CD",
    "INVC_CMPNT_CT_SRC_CD",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "BILL_DUE_DT_SK",
    "BILL_AMT",
    "BILL_ENTY_UNIQ_KEY",
    "DPNDT_LVS_CT",
    "SUB_LVS_CT",
    "BLCT_DISP_CD",
    "BLCT_PREM_TYPE",
    "BLCT_SOURCE",
    "PROD_CMPNT_PFX_ID"
)

df_key_Transform = df_key.select(
    col("BILL_INVC_ID").alias("BILL_INVC_ID"),
    col("CLS_PLN_ID").alias("CLS_PLN_ID"),
    col("PROD_ID").alias("PROD_ID"),
    col("PROD_BILL_CMPNT_ID").alias("PROD_BILL_CMPNT_ID"),
    col("INVC_CMPNT_CT_DISP_CD").alias("INVC_CMPNT_CT_DISP_CD"),
    col("INVC_CMPNT_CT_PRM_TYP_CD").alias("INVC_CMPNT_CT_PRM_TYP_CD"),
    col("INVC_CMPNT_CT_SRC_CD").alias("INVC_CMPNT_CT_SRC_CD"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK")
)

df_key_AllCol = df_key.select(
    col("BILL_INVC_ID").alias("BILL_INVC_ID"),
    col("CLS_PLN_ID").alias("CLS_PLN_ID"),
    col("PROD_ID").alias("PROD_ID"),
    col("PROD_BILL_CMPNT_ID").alias("PROD_BILL_CMPNT_ID"),
    col("INVC_CMPNT_CT_DISP_CD").alias("INVC_CMPNT_CT_DISP_CD"),
    col("INVC_CMPNT_CT_PRM_TYP_CD").alias("INVC_CMPNT_CT_PRM_TYP_CD"),
    col("INVC_CMPNT_CT_SRC_CD").alias("INVC_CMPNT_CT_SRC_CD"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    col("DISCARD_IN").alias("DISCARD_IN"),
    col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("ERR_CT").alias("ERR_CT"),
    col("RECYCLE_CT").alias("RECYCLE_CT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("INVC_CMPNT_CT_SK").alias("INVC_CMPNT_CT_SK"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("BILL_DUE_DT_SK").alias("BILL_DUE_DT_SK"),
    col("BILL_AMT").alias("BILL_AMT"),
    col("BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
    col("DPNDT_LVS_CT").alias("DPNDT_LVS_CT"),
    col("SUB_LVS_CT").alias("SUB_LVS_CT"),
    col("BLCT_DISP_CD").alias("BLCT_DISP_CD"),
    col("BLCT_PREM_TYPE").alias("BLCT_PREM_TYPE"),
    col("BLCT_SOURCE").alias("BLCT_SOURCE"),
    col("PROD_CMPNT_PFX_ID").alias("PROD_CMPNT_PFX_ID")
)

params_IncomeInvcCmpntCtPK = {
    "DriverTable": DriverTable,
    "SrcSysCd": SrcSysCd,
    "SrcSysCdSk": SrcSysCdSk,
    "RunID": RunID,
    "CurrDate": CurrDate,
    "CurrRunCycle": CurrRunCycle,
    "$FacetsDB": "",
    "$FacetsOwner": FacetsOwner,
    "$IDSOwner": IDSOwner
}
df_container_out = IncomeInvcCmpntCtPK(df_key_AllCol, df_key_Transform, params_IncomeInvcCmpntCtPK)

df_final = df_container_out.select(
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("INVC_CMPNT_CT_SK"),
    col("BILL_INVC_ID"),
    col("CLS_PLN_ID"),
    col("PROD_ID"),
    col("PROD_BILL_CMPNT_ID"),
    col("INVC_CMPNT_CT_DISP_CD"),
    col("INVC_CMPNT_CT_PRM_TYP_CD"),
    col("INVC_CMPNT_CT_SRC_CD"),
    col("SRC_SYS_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("BILL_DUE_DT_SK"), 10, " ").alias("BILL_DUE_DT_SK"),
    col("BILL_AMT"),
    col("BILL_ENTY_UNIQ_KEY"),
    col("DPNDT_LVS_CT"),
    col("SUB_LVS_CT"),
    rpad(col("BLCT_DISP_CD"), 1, " ").alias("BLCT_DISP_CD"),
    rpad(col("BLCT_PREM_TYPE"), 1, " ").alias("BLCT_PREM_TYPE"),
    rpad(col("BLCT_SOURCE"), 1, " ").alias("BLCT_SOURCE"),
    col("PROD_CMPNT_PFX_ID")
)

write_files(
    df_final,
    f"{adls_path}/key/FctsInvcCmpntCtExtr.InvcCmpntCt.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)