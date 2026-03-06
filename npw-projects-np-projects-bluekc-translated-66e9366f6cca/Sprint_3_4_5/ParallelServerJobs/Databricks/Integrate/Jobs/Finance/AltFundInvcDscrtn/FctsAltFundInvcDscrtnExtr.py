# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2007 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  FctsAltFundCntl
# MAGIC                
# MAGIC 
# MAGIC PROCESSING:   
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                    Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ----------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Ralph Tucker              2007-08-21                                      Original Programming.                                                                   devlIDS30                     Steph Goddard           09/28/2007
# MAGIC                         
# MAGIC 
# MAGIC Parikshith Chada        10/04/2007            3259               Added Balancing extract process to the overall job                        devlIDS30 
# MAGIC 
# MAGIC Goutham Kalidindi       2021-03-24            358186         Changed Datatype length for field BLIV_ID                                                                              Kalyan Neelam           2021-03-31
# MAGIC                                                                                          char(12) to Varchar(15)
# MAGIC Prabhu ES                  2022-02-24                                    S2S Remediation     MSSQL connection parameters added            IntegrateDev5               Kalyan Neelam           2022-06-13

# MAGIC Pulls all Invoice Discretionary rows from facets CDS_INDI_DISCRETN table matching data criteria.
# MAGIC Balancing snapshot of source table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StructType, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Parameters
TmpOutFile = get_widget_value('TmpOutFile','FctsAltFundInvcDscrtnExtr.AltFundInvcDscrtnExtr.dat')
CurrRunCycle = get_widget_value('CurrRunCycle','666')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunID = get_widget_value('RunID','20070827')
StartDate = get_widget_value('StartDate','2007-01-01')
FirstRecycleDt = get_widget_value('FirstRecycleDt','2007-10-01 00:00:00.00000')

# --------------------------------------------------------------------------------
# Stage: hf_alt_fund_invc_dscrtn_lkup (CHashedFileStage) - Scenario B (Read step)
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_hf_alt_fund_invc_dscrtn = """
SELECT
  SRC_SYS_CD,
  ALT_FUND_INVC_DSCRTN_ID,
  BLDI_SEQ_NO,
  CRT_RUN_CYC_EXCTN_SK,
  ALT_FUND_INVC_DSCRTN_SK
FROM IDS.dummy_hf_alt_fund_invc_dscrtn
"""
df_hf_alt_fund_invc_dscrtn_lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_hf_alt_fund_invc_dscrtn)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: CDS_INDI_DISCRETN (ODBCConnector)
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
extract_query_CDS_INDI_DISCRETN = f"""
SELECT
  INDI.BLIV_ID,
  INDI.BLDI_SEQ_NO,
  INDI.BLDI_DESC,
  INDI.BLDI_EXTRACONT_AMT AS BLDI_EXTRA_CNTR_AMT,
  INDI.BLDI_UPDATE_DTM,
  INID.BLBL_DUE_DT,
  INDI.CSPI_ID,
  INDI.PDPD_ID,
  INDI.PDBL_ID,
  INDI.GRGR_ID,
  INDI.SGSG_ID,
  INID.SBSB_CK,
  INID.BLEI_CK,
  INID.BLBL_END_DT
FROM {FacetsOwner}.CDS_INID_INVOICE INID,
     {FacetsOwner}.CDS_INDI_DISCRETN INDI
WHERE INID.BLIV_ID = INDI.BLIV_ID
  AND INID.AFAI_CK > 0
  AND INID.BLIV_CREATE_DTM >= '{StartDate}'
"""
df_CDS_INDI_DISCRETN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_CDS_INDI_DISCRETN)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: CMC_BLDI_DISCRETN (ODBCConnector)
df_CMC_BLDI_DISCRETN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("dbtable", f"{FacetsOwner}.CMC_BLDI_DISCRETN")
    .load()
)

# --------------------------------------------------------------------------------
# Stage: StripField (CTransformerStage)
# Two input links: "Extract" (primary) from df_CDS_INDI_DISCRETN, "Amt_excepttion" (lookup) from df_CMC_BLDI_DISCRETN with left join, no conditions (cross join).
df_strip_join = df_CDS_INDI_DISCRETN.alias("Extract").join(
    df_CMC_BLDI_DISCRETN.alias("Amt_excepttion"),
    on=None,
    how="left"
)

# Derive stage variables:
df_strip_vars = (
    df_strip_join
    .withColumn("svCertExtr", ParseCert(F.col("Extract.BLDI_DESC")))
    .withColumn("svBLDIYear", FORMAT_DATE(F.col("Extract.BLDI_UPDATE_DTM"), F.lit("SYBASE"), F.lit("Timestamp"), F.lit("CCYY-MM-DD")))
    .withColumn("svDueDt", FORMAT_DATE(F.col("Extract.BLBL_DUE_DT"), F.lit("DATE"), F.lit("DATE"), F.lit("CCYYMMDD")))
    .withColumn(
        "svDueDt2",
        F.concat(
            F.substring(F.col("svDueDt"), 5, 2),
            F.lit('-'),
            F.substring(F.col("svDueDt"), 7, 2),
            F.lit('-'),
            F.substring(F.col("svDueDt"), 1, 4)
        )
    )
    .withColumn("svBegDt", ParseDate(F.col("Extract.BLDI_DESC"), F.col("svBLDIYear")))
    .withColumn(
        "svBegDt2",
        F.when(F.col("svBegDt") == "?", F.col("svDueDt2")).otherwise(F.col("svBegDt"))
    )
    .withColumn(
        "svParseMths",
        F.when(ParseMthsCDHP(F.col("Extract.BLDI_DESC")) == 0, F.lit(1)).otherwise(ParseMthsCDHP(F.col("Extract.BLDI_DESC")))
    )
    .withColumn(
        "svMths",
        F.when(F.col("svParseMths") == "?", "1").otherwise(F.col("svParseMths"))
    )
    .withColumn(
        "svMths2",
        F.when(F.col("svMths") < F.lit(0), F.lit(1)).otherwise(F.col("svMths"))
    )
    .withColumn("svParsedDesc", Parse4ShDescCDHP(F.col("Extract.BLDI_DESC")))
    .withColumn("svPrsnId", Parse4PrsnId(F.col("Extract.BLDI_DESC")))
    .withColumn(
        "svTotalAmts",
        (F.col("Amt_excepttion.BLDI_ASO_AMT") + F.col("Amt_excepttion.BLDI_SSL_AMT") + F.col("Amt_excepttion.BLDI_HSA_AMT"))
    )
)

# Produce output columns for link "Strip"
df_Strip = df_strip_vars.select(
    strip_field(F.col("Extract.BLIV_ID")).alias("BLIV_ID"),
    F.col("Extract.BLDI_SEQ_NO").alias("BLDI_SEQ_NO"),
    strip_field(F.col("Extract.BLDI_DESC")).alias("BLDI_DESC"),
    strip_field(F.col("Extract.CSPI_ID")).alias("CSPI_ID"),
    strip_field(F.col("Extract.PDPD_ID")).alias("PDPD_ID"),
    strip_field(F.col("Extract.PDBL_ID")).alias("PDBL_ID"),
    strip_field(F.col("Extract.GRGR_ID")).alias("GRGR_ID"),
    strip_field(F.col("Extract.SGSG_ID")).alias("SGSG_ID"),
    F.when(F.col("Extract.BLDI_EXTRA_CNTR_AMT") == 0, F.col("svTotalAmts")).otherwise(F.col("Extract.BLDI_EXTRA_CNTR_AMT")).alias("BLDI_EXTRA_CNTR_AMT"),
    F.col("svMths").alias("MO_QTY"),
    strip_field(F.col("svParsedDesc")).alias("SH_DESC"),
    F.col("Extract.SBSB_CK").alias("SBSB_CK"),
    F.lit("UNK").alias("CSCS_id"),
    F.when(F.length(trim(F.col("svPrsnId"))) == 0, F.lit(None)).otherwise(trim(F.col("svPrsnId"))).alias("DSCRTN_PRSN_ID_TX"),
    F.col("svBegDt2").alias("INVC_DSCRTN_BEG_DT"),
    F.col("svDueDt2").alias("INVC_DSCRTN_DUE_DT"),
    F.col("Extract.BLBL_DUE_DT").alias("BLBL_DUE_DT"),
    F.col("Extract.BLEI_CK").alias("BLEI_CK")
)

# --------------------------------------------------------------------------------
# Stage: BusinessRules (CTransformerStage)
df_business_vars = (
    df_Strip
    .withColumn(
        "svBegDt",
        FORMAT_DATE(F.col("INVC_DSCRTN_BEG_DT"), F.lit("DATE"), F.lit("MM/DD/CCYY"), F.lit("CCYY-MM-DD"))
    )
    .withColumn(
        "svEndDt",
        FIND_DATE(F.col("svBegDt"), F.col("MO_QTY") - F.lit(1), F.lit("M"), F.lit("X"), F.lit("CCYY-MM-DD"))
    )
    .withColumn(
        "svEndDt2",
        FIND_DATE(F.col("svEndDt"), F.lit(0), F.lit("M"), F.lit("L"), F.lit("CCYY-MM-DD"))
    )
)

# Constraint: "Strip.MO_QTY <> '?'"
df_BusinessRules_filtered = df_business_vars.filter(F.col("MO_QTY") != "?")

# Output columns for link "Transform"
df_BusinessRules = df_BusinessRules_filtered.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.col("FirstRecycleDt").alias("FIRST_RECYC_DT"),  # uses parameter
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.concat(F.lit("FACETS"), F.lit(";"), trim(F.col("BLIV_ID")), F.lit(";"), F.col("BLDI_SEQ_NO")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("INVC_SK"),
    trim(F.col("BLIV_ID")).alias("BILL_INVC_ID"),
    F.col("BLDI_SEQ_NO").alias("BLDI_SEQ_NO"),
    F.col("CSCS_id").alias("CLS_ID"),
    F.col("CSPI_ID").alias("CLS_PLN_ID"),
    F.col("GRGR_ID").alias("GRGR_ID"),
    F.when(F.length(trim(F.col("PDPD_ID"))) == 0, F.lit("NA")).otherwise(F.col("PDPD_ID")).alias("PROD_ID"),
    F.col("PDBL_ID").alias("PROD_BILL_CMPNT_ID"),
    F.col("SGSG_ID").alias("SUBGRP_ID"),
    F.col("BLDI_EXTRA_CNTR_AMT").alias("BLDI_EXTRA_CNTR_AMT"),
    F.when(F.col("MO_QTY") == "?", F.lit(1))
     .when(F.col("MO_QTY") == 0, F.lit(1))
     .when(F.col("MO_QTY") < 0, F.lit(1))
     .when(F.col("MO_QTY") > 72, F.lit(1))
     .otherwise(F.col("MO_QTY")).alias("DSCRTN_MO_QTY"),
    F.when(F.length(trim(F.col("BLDI_DESC"))) == 0, F.lit(None))
     .otherwise(UpCase(F.col("BLDI_DESC"))).alias("DSCRTN_DESC"),
    F.when(F.length(trim(F.col("SH_DESC"))) == 0, F.lit(None))
     .otherwise(F.col("SH_DESC")).alias("DSCRTN_SH_DESC"),
    F.col("SBSB_CK").alias("SBSB_CK"),
    F.when(F.length(trim(F.col("DSCRTN_PRSN_ID_TX"))) == 0, F.lit(None))
     .when(F.length(trim(F.col("DSCRTN_PRSN_ID_TX"))) > 9, F.lit(None))
     .otherwise(trim(F.col("DSCRTN_PRSN_ID_TX"))).alias("DSCRTN_PRSN_ID_TX"),
    FORMAT_DATE(F.col("INVC_DSCRTN_BEG_DT"), F.lit("DATE"), F.lit("MM/DD/CCYY"), F.lit("CCYY-MM-DD")).alias("INVC_DSCRTN_BEG_DT"),
    F.col("svEndDt2").alias("INVC_DSCRTN_END_DT")
)

# --------------------------------------------------------------------------------
# Stage: PrimaryKey (CTransformerStage)
# Join with df_hf_alt_fund_invc_dscrtn_lkup on:
#   Transform.SRC_SYS_CD = lkup.SRC_SYS_CD
#   Transform.BILL_INVC_ID = lkup.ALT_FUND_INVC_DSCRTN_ID
#   Transform.BLDI_SEQ_NO = lkup.BLDI_SEQ_NO

df_primarykey_join = (
    df_BusinessRules.alias("Transform")
    .join(
        df_hf_alt_fund_invc_dscrtn_lkup.alias("lkup"),
        on=[
            F.col("Transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD"),
            F.col("Transform.BILL_INVC_ID") == F.col("lkup.ALT_FUND_INVC_DSCRTN_ID"),
            F.col("Transform.BLDI_SEQ_NO") == F.col("lkup.BLDI_SEQ_NO")
        ],
        how="left"
    )
)

# Add columns replicating stage-variable logic:
df_primarykey_cols = (
    df_primarykey_join
    .withColumn(
        "ALT_FUND_INVC_DSCRTN_SK",
        F.col("lkup.ALT_FUND_INVC_DSCRTN_SK")
    )
    .withColumn(
        "CRT_RUN_CYC_EXCTN_SK",
        F.when(F.col("lkup.ALT_FUND_INVC_DSCRTN_SK").isNull(), F.lit(CurrRunCycle))
         .otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK"))
    )
    .withColumn("Transform.JOB_EXCTN_RCRD_ERR_SK", F.col("Transform.JOB_EXCTN_RCRD_ERR_SK"))
    .withColumn("Transform.INSRT_UPDT_CD", F.col("Transform.INSRT_UPDT_CD"))
    .withColumn("Transform.DISCARD_IN", F.col("Transform.DISCARD_IN"))
    .withColumn("Transform.PASS_THRU_IN", F.col("Transform.PASS_THRU_IN"))
    .withColumn("Transform.FIRST_RECYC_DT", F.col("Transform.FIRST_RECYC_DT"))
    .withColumn("Transform.ERR_CT", F.col("Transform.ERR_CT"))
    .withColumn("Transform.RECYCLE_CT", F.col("Transform.RECYCLE_CT"))
    .withColumn("Transform.SRC_SYS_CD", F.col("Transform.SRC_SYS_CD"))
    .withColumn("Transform.PRI_KEY_STRING", F.col("Transform.PRI_KEY_STRING"))
    .withColumn("Transform.BILL_INVC_ID", F.col("Transform.BILL_INVC_ID"))
    .withColumn("Transform.BLDI_SEQ_NO", F.col("Transform.BLDI_SEQ_NO"))
    .withColumn("CurrRunCycle", F.lit(CurrRunCycle))
    .withColumn("Transform.LAST_UPDT_RUN_CYC_EXCTN_SK", F.col("Transform.LAST_UPDT_RUN_CYC_EXCTN_SK"))
    .withColumn("Transform.INVC_SK", F.col("Transform.INVC_SK"))
    .withColumn("Transform.GRGR_ID", F.col("Transform.GRGR_ID"))
    .withColumn("Transform.SUBGRP_ID", F.col("Transform.SUBGRP_ID"))
    .withColumn("Transform.INVC_DSCRTN_BEG_DT", F.col("Transform.INVC_DSCRTN_BEG_DT"))
    .withColumn("Transform.INVC_DSCRTN_END_DT", F.col("Transform.INVC_DSCRTN_END_DT"))
    .withColumn("Transform.BLDI_EXTRA_CNTR_AMT", F.col("Transform.BLDI_EXTRA_CNTR_AMT"))
    .withColumn("Transform.DSCRTN_MO_QTY", F.col("Transform.DSCRTN_MO_QTY"))
    .withColumn("Transform.DSCRTN_DESC", F.col("Transform.DSCRTN_DESC"))
    .withColumn("Transform.DSCRTN_PRSN_ID_TX", F.col("Transform.DSCRTN_PRSN_ID_TX"))
    .withColumn("Transform.DSCRTN_SH_DESC", F.col("Transform.DSCRTN_SH_DESC"))
    .withColumn("Transform.PROD_ID", F.col("Transform.PROD_ID"))
    .withColumn("Transform.PROD_BILL_CMPNT_ID", F.col("Transform.PROD_BILL_CMPNT_ID"))
    .withColumn("Transform.CLS_ID", F.col("Transform.CLS_ID"))
    .withColumn("Transform.CLS_PLN_ID", F.col("Transform.CLS_PLN_ID"))
)

# Rename to df_enriched and apply SurrogateKeyGen for column ALT_FUND_INVC_DSCRTN_SK
df_enriched = df_primarykey_cols
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"ALT_FUND_INVC_DSCRTN_SK",<schema>,<secret_name>)

# Output link "Key" => "IdsAltFundInvcDscrtn" (all rows)
df_Key = df_enriched.select(
    F.col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("Transform.DISCARD_IN").alias("DISCARD_IN"),
    F.col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("Transform.ERR_CT").alias("ERR_CT"),
    F.col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("ALT_FUND_INVC_DSCRTN_SK").alias("ALT_FUND_INVC_DSCRTN_SK"),
    F.col("Transform.BILL_INVC_ID").alias("ALT_FUND_INVC_ID"),
    F.col("Transform.BLDI_SEQ_NO").alias("SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("CurrRunCycle").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.BILL_INVC_ID").alias("ALT_FUND_INVC_SK"),
    F.col("Transform.GRGR_ID").alias("GRP_ID"),
    F.col("Transform.SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("Transform.INVC_DSCRTN_BEG_DT").alias("INVC_DSCRTN_BEG_DT_SK"),
    F.col("Transform.INVC_DSCRTN_END_DT").alias("INVC_DSCRTN_END_DT_SK"),
    F.col("Transform.BLDI_EXTRA_CNTR_AMT").alias("EXTRA_CNTR_AMT"),
    F.col("Transform.DSCRTN_MO_QTY").alias("DSCRTN_MO_QTY"),
    F.col("Transform.DSCRTN_DESC").alias("DSCRTN_DESC"),
    F.col("Transform.DSCRTN_PRSN_ID_TX").alias("DSCRTN_PRSN_ID_TX"),
    F.col("Transform.DSCRTN_SH_DESC").alias("DSCRTN_SH_DESC")
)

# Apply rpad for char/varchar columns in final output to file
df_IdsAltFundInvcDscrtn = (
    df_Key
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("INVC_DSCRTN_BEG_DT_SK", F.rpad(F.col("INVC_DSCRTN_BEG_DT_SK"), 10, " "))
    .withColumn("INVC_DSCRTN_END_DT_SK", F.rpad(F.col("INVC_DSCRTN_END_DT_SK"), 10, " "))
    .withColumn("DSCRTN_DESC", F.rpad(F.col("DSCRTN_DESC"), 70, " "))
    .withColumn("DSCRTN_SH_DESC", F.rpad(F.col("DSCRTN_SH_DESC"), 80, " "))
)

# Write to file (CSeqFileStage) - "IdsAltFundInvcDscrtn"
out_file_path_ids_alt = f"{adls_path}/key/{TmpOutFile}"
write_files(
    df_IdsAltFundInvcDscrtn,
    out_file_path_ids_alt,
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Output link "updt" => "hf_alt_fund_invc_dscrtn" => scenario B (write step)
df_updt = df_enriched.filter(F.col("lkup.ALT_FUND_INVC_DSCRTN_ID").isNull()).select(
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.BILL_INVC_ID").alias("ALT_FUND_INVC_DSCRTN_ID"),
    F.col("Transform.BLDI_SEQ_NO").alias("BLDI_SEQ_NO"),
    F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("ALT_FUND_INVC_DSCRTN_SK").alias("ALT_FUND_INVC_DSCRTN_SK")
)

# Write/merge to dummy table: "IDS.dummy_hf_alt_fund_invc_dscrtn"
temp_table_name_updt = "STAGING.FctsAltFundInvcDscrtnExtr_hf_alt_fund_invc_dscrtn_temp"
merge_sql_updt = f"""
MERGE INTO IDS.dummy_hf_alt_fund_invc_dscrtn AS T
USING {temp_table_name_updt} AS S
   ON T.SRC_SYS_CD = S.SRC_SYS_CD
  AND T.ALT_FUND_INVC_DSCRTN_ID = S.ALT_FUND_INVC_DSCRTN_ID
  AND T.BLDI_SEQ_NO = S.BLDI_SEQ_NO
WHEN MATCHED THEN
  UPDATE SET
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
    T.ALT_FUND_INVC_DSCRTN_SK = S.ALT_FUND_INVC_DSCRTN_SK
WHEN NOT MATCHED THEN
  INSERT (
    SRC_SYS_CD,
    ALT_FUND_INVC_DSCRTN_ID,
    BLDI_SEQ_NO,
    CRT_RUN_CYC_EXCTN_SK,
    ALT_FUND_INVC_DSCRTN_SK
  )
  VALUES (
    S.SRC_SYS_CD,
    S.ALT_FUND_INVC_DSCRTN_ID,
    S.BLDI_SEQ_NO,
    S.CRT_RUN_CYC_EXCTN_SK,
    S.ALT_FUND_INVC_DSCRTN_SK
  )
;
"""

execute_dml(f"DROP TABLE IF EXISTS {temp_table_name_updt}", jdbc_url_ids, jdbc_props_ids)
df_updt.write \
    .format("jdbc") \
    .option("url", jdbc_url_ids) \
    .options(**jdbc_props_ids) \
    .option("dbtable", temp_table_name_updt) \
    .mode("overwrite") \
    .save()
execute_dml(merge_sql_updt, jdbc_url_ids, jdbc_props_ids)

# --------------------------------------------------------------------------------
# Stage: Facets_Source (ODBCConnector)
extract_query_Facets_Source = f"""
SELECT
  INDI.BLIV_ID,
  INDI.BLDI_SEQ_NO
FROM {FacetsOwner}.CDS_INDI_DISCRETN INDI,
     {FacetsOwner}.CDS_INID_INVOICE INID
WHERE
  INDI.BLIV_ID = INID.BLIV_ID
  AND INID.AFAI_CK > 0
  AND INID.BLIV_CREATE_DTM >= '{StartDate}'
"""
df_Facets_Source = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_Facets_Source)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: Transform (CTransformerStage)
df_Facets_Transform_vars = (
    df_Facets_Source
    .withColumn("svSrcSysCdSk", GetFkeyCodes(F.lit("IDS"), F.lit(1), F.lit("SOURCE SYSTEM"), F.lit("FACETS"), F.lit("X")))
)

df_Facets_Transform = df_Facets_Transform_vars.select(
    F.col("svSrcSysCdSk").alias("SRC_SYS_CD_SK"),
    trim(strip_field(F.col("BLIV_ID"))).alias("ALT_FUND_INVC_ID"),
    F.col("BLDI_SEQ_NO").alias("SEQ_NO")
)

# --------------------------------------------------------------------------------
# Stage: Snapshot_File (CSeqFileStage) -> "load/B_ALT_FUND_INVC_DSCRTN.dat"
# The job does not specify exact char/varchar for these columns, so only rpad if needed is unknown. We keep as-is.
file_path_snapshot = f"{adls_path}/load/B_ALT_FUND_INVC_DSCRTN.dat"
write_files(
    df_Facets_Transform,
    file_path_snapshot,
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)