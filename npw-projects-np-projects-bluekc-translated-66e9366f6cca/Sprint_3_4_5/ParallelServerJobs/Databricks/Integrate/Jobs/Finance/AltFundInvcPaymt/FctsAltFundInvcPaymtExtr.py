# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2007, 2009 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     FactAltFundExtrSeq
# MAGIC 
# MAGIC Processing:
# MAGIC                     *
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:    Restart, no other steps necessary
# MAGIC                     Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                     Project/                                                                                                                           Code                   Date
# MAGIC Developer              Date              Altiris #         Change Description                                                                                     Reviewer            Reviewed
# MAGIC ----------------------------  -------------------   -----------------   -----------------------------------------------------------------------------------------------------------------   -------------------------  -------------------
# MAGIC Naren Garapaty     09/20/2007   3259            Initial program                                                                                              Steph Goddard   09/27/2007
# MAGIC Parikshith Chada   10/08/2007   3259            Added Balancing extract process to the overall job                                     
# MAGIC Hugh Sisson          03/30/2009   188315        Added steps to use Claim Primary Key table for foreign key lookups           Steph Goddard   04/06/2009
# MAGIC 
# MAGIC Hugh Sisson          2016-06-07    TFS12538   Change SEQ_NO from SmallInt to Integer                                                   Jag Yelavarthi     2016-06-08
# MAGIC 
# MAGIC Goutham Kalidindi  2021-03-24 358186    Changed Datatype length for field BLIV_ID                                                         Kalyan Neelam    2021-03-31
# MAGIC                                                                   char(12) to Varchar(15)
# MAGIC Prabhu ES              2022-02-24                         S2S Remediation MSSQL connection parameters added                          Kalyan Neelam    2022-06-13

# MAGIC Pulling FACETS Data
# MAGIC Assign primary surrogate key
# MAGIC Strip Carriage Return, Line Feed, and  Tab
# MAGIC Apply business logic
# MAGIC Writing Sequential File to ../key
# MAGIC Balancing snapshot of source table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Obtain parameter values
TmpOutFile = get_widget_value('TmpOutFile','IdsAltFundInvcPaymtPkey.AltFundInvcPaymtTmp.dat')
CurrRunCycle = get_widget_value('CurrRunCycle','1')
FirstRecycleDt = get_widget_value('FirstRecycleDt','2007-09-26')
SrcSysCdSk = get_widget_value('SrcSysCdSk','0')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
ids_secret_name = get_widget_value('ids_secret_name','')

# Read from FACETS ODBCConnector (StageName: FACETS)
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
facets_query = f"""
SELECT
A.BLIV_ID,
A.MEME_CK,
A.AFCP_PER_NO,
A.CSPI_ID,
A.PDPD_ID,
A.CLCL_ID,
A.SBSB_CK,
A.INPS_SEQ_NO,
A.BLPS_FNDG_FROM_DT,
A.BLPS_FNDG_THRU_DT,
A.GRGR_CK,
A.SGSG_CK,
A.SBSB_ID,
A.BLPS_HSA_AMT,
B.SGSG_ID,
C.GRGR_ID
FROM 
{FacetsOwner}.CDS_INPS_PYMT_DTL A,
{FacetsOwner}.CMC_SGSG_SUB_GROUP B,
{FacetsOwner}.CMC_GRGR_GROUP C
WHERE
A.SGSG_CK=B.SGSG_CK
AND B.GRGR_CK=C.GRGR_CK
"""
df_FACETS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", facets_query)
    .load()
)

# Transformer: StripField (StageName: StripField)
df_StripField = (
    df_FACETS
    .withColumn("BLIV_ID", strip_field(F.col("BLIV_ID")))
    .withColumn("MEME_CK", F.col("MEME_CK"))
    .withColumn("AFCP_PER_NO", F.col("AFCP_PER_NO"))
    .withColumn("CSPI_ID", strip_field(F.col("CSPI_ID")))
    .withColumn("PDPD_ID", strip_field(F.col("PDPD_ID")))
    .withColumn("CLCL_ID", strip_field(F.col("CLCL_ID")))
    .withColumn("SBSB_CK", F.col("SBSB_CK"))
    .withColumn("INPS_SEQ_NO", F.col("INPS_SEQ_NO"))
    .withColumn("BLPS_FNDG_FROM_DT", F.col("BLPS_FNDG_FROM_DT"))
    .withColumn("BLPS_FNDG_THRU_DT", F.col("BLPS_FNDG_THRU_DT"))
    .withColumn("GRGR_CK", F.col("GRGR_CK"))
    .withColumn("SGSG_CK", F.col("SGSG_CK"))
    .withColumn("SBSB_ID", strip_field(F.col("SBSB_ID")))
    .withColumn("BLPS_HSA_AMT", F.col("BLPS_HSA_AMT"))
    .withColumn("SGSG_ID", strip_field(F.col("SGSG_ID")))
    .withColumn("GRGR_ID", strip_field(F.col("GRGR_ID")))
)

# Transformer: BusinessRules (StageName: BusinessRules)
df_BusinessRules = (
    df_StripField
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.lit("Y"))
    .withColumn("FIRST_RECYC_DT", F.col("FirstRecycleDt"))  # from parameter
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit("FACETS"))
    .withColumn(
        "PRI_KEY_STRING",
        F.concat_ws(
            ";",
            F.lit("FACETS"),
            F.col("BLIV_ID"),
            F.col("MEME_CK"),
            F.col("AFCP_PER_NO"),
            F.col("CSPI_ID"),
            F.col("PDPD_ID"),
            F.col("CLCL_ID"),
            F.col("SBSB_CK"),
            F.col("INPS_SEQ_NO")
        )
    )
    .withColumn("ALT_FUND_INVC_PAYMT_SK", F.lit(0))
    .withColumn("ALT_FUND_INVC_ID", F.col("BLIV_ID"))
    .withColumn("MBR_UNIQ_KEY", F.col("MEME_CK"))
    .withColumn("ALT_FUND_CNTR_PERD_NO", F.col("AFCP_PER_NO"))
    .withColumn("CLS_PLN_ID", F.col("CSPI_ID"))
    .withColumn("PROD_ID", F.col("PDPD_ID"))
    .withColumn("CLM_ID", F.col("CLCL_ID"))
    .withColumn("SUB_UNIQ_KEY", F.col("SBSB_CK"))
    .withColumn("SEQ_NO", F.col("INPS_SEQ_NO"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("ALT_FUND_INVC_SK", F.col("BLIV_ID"))
    .withColumn("CLM", F.col("CLCL_ID"))
    .withColumn("CLS_PLN", F.col("CSPI_ID"))
    .withColumn("GRP", trim(F.col("GRGR_CK")))
    .withColumn("MBR", F.col("MEME_CK"))
    .withColumn("PROD", F.col("PDPD_ID"))
    .withColumn("SUBGRP", F.lit(0))
    .withColumn("SUB", trim(F.col("SBSB_ID")))
    .withColumn("FUND_FROM_DT", F.col("BLPS_FNDG_FROM_DT"))
    .withColumn("FUND_THRU_DT", F.col("BLPS_FNDG_THRU_DT"))
    .withColumn(
        "PCA_AMT",
        F.when(F.isnull("BLPS_HSA_AMT"), F.lit(0))
         .when(F.length("BLPS_HSA_AMT") == 0, F.lit(0))
         .otherwise(F.col("BLPS_HSA_AMT"))
    )
    .withColumn("SGSG_ID", F.col("SGSG_ID"))
    .withColumn("GRGR_ID", F.col("GRGR_ID"))
)

# Scenario B for hashed file: "hf_alt_fund_invc_paymt". 
# We read from an IDS-based dummy table "dummy_hf_alt_fund_invc_paymt" with the same columns.
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", """
SELECT
SRC_SYS_CD,
ALT_FUND_INVC_ID,
MBR_UNIQ_KEY,
ALT_FUND_CNTR_PERD_NO,
CLS_PLN_ID,
PROD_ID,
CLM_ID,
SUB_UNIQ_KEY,
SEQ_NO,
CRT_RUN_CYC_EXCTN_SK,
ALT_FUND_INVC_PAYMT_SK
FROM IDS.dummy_hf_alt_fund_invc_paymt
""")
    .load()
)

# Transformer: PkeyTrn (StageName: PkeyTrn) with left join to df_lkup
join_cond = [
    df_BusinessRules["SRC_SYS_CD"] == df_lkup["SRC_SYS_CD"],
    df_BusinessRules["ALT_FUND_INVC_ID"] == df_lkup["ALT_FUND_INVC_ID"],
    df_BusinessRules["MBR_UNIQ_KEY"] == df_lkup["MBR_UNIQ_KEY"],
    df_BusinessRules["ALT_FUND_CNTR_PERD_NO"] == df_lkup["ALT_FUND_CNTR_PERD_NO"],
    df_BusinessRules["CLS_PLN_ID"] == df_lkup["CLS_PLN_ID"],
    df_BusinessRules["PROD_ID"] == df_lkup["PROD_ID"],
    df_BusinessRules["CLM_ID"] == df_lkup["CLM_ID"],
    df_BusinessRules["SUB_UNIQ_KEY"] == df_lkup["SUB_UNIQ_KEY"],
    df_BusinessRules["SEQ_NO"] == df_lkup["SEQ_NO"]
]

df_join = df_BusinessRules.alias("Transform").join(df_lkup.alias("lkup"), join_cond, how="left")

# According to the derivation: 
# ALT_FUND_INVC_PAYMT_SK => if lkup is null => KeyMgtGetNextValueConcurrent("IDS_SK"), else lkup
# We apply the SurrogateKeyGen approach to fill missing ALT_FUND_INVC_PAYMT_SK

df_enriched = (
    df_join
    .withColumn(
        "ALT_FUND_INVC_PAYMT_SK",
        F.when(F.isnull("lkup.ALT_FUND_INVC_PAYMT_SK"), F.lit(None))
         .otherwise(F.col("lkup.ALT_FUND_INVC_PAYMT_SK"))
    )
    .withColumn(
        "CRT_RUN_CYC_EXCTN_SK",
        F.when(F.isnull("lkup.ALT_FUND_INVC_PAYMT_SK"), F.col("CurrRunCycle").cast(IntegerType()))
         .otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK"))
    )
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.col("CurrRunCycle").cast(IntegerType()))
    .withColumn("SK", F.col("ALT_FUND_INVC_PAYMT_SK"))  # We'll finalize after SurrogateKeyGen
    .withColumn("NewCrtRunCycExtcnSk", F.col("CRT_RUN_CYC_EXCTN_SK"))
    .withColumn("SrcSysCdSk", F.col("SrcSysCdSk"))
)

# Now call SurrogateKeyGen to populate ALT_FUND_INVC_PAYMT_SK if null
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"ALT_FUND_INVC_PAYMT_SK",<schema>,<secret_name>)

# Now define columns for the "Key" link (IdsAltFundInvcPaymt).
# The DataStage job sets "SK" into ALT_FUND_INVC_PAYMT_SK for the final flow, 
# plus CurrRunCycle into LAST_UPDT_RUN_CYC_EXCTN_SK, 
# plus the condition-based CRT_RUN_CYC_EXCTN_SK from "NewCrtRunCycExtcnSk".
df_IdsAltFundInvcPaymt = df_enriched.select(
    F.col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("Transform.DISCARD_IN").alias("DISCARD_IN"),
    F.col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("Transform.ERR_CT").alias("ERR_CT"),
    F.col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("SK").alias("ALT_FUND_INVC_PAYMT_SK"),
    F.col("Transform.ALT_FUND_INVC_ID").alias("ALT_FUND_INVC_ID"),
    F.col("Transform.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("Transform.ALT_FUND_CNTR_PERD_NO").alias("ALT_FUND_CNTR_PERD_NO"),
    F.col("Transform.CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.col("Transform.PROD_ID").alias("PROD_ID"),
    F.col("Transform.CLM_ID").alias("CLM_ID"),
    F.col("Transform.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("Transform.SEQ_NO").alias("SEQ_NO"),
    F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("CurrRunCycle").cast(IntegerType()).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.ALT_FUND_INVC_SK").alias("ALT_FUND_INVC_SK"),
    F.col("Transform.CLM").alias("CLM"),
    F.col("Transform.CLS_PLN").alias("CLS_PLN"),
    F.col("Transform.GRP").alias("GRP"),
    F.col("Transform.MBR").alias("MBR"),
    F.col("Transform.PROD").alias("PROD"),
    F.col("Transform.SUBGRP").alias("SUBGRP"),
    F.col("Transform.SUB").alias("SUB"),
    F.col("Transform.FUND_FROM_DT").alias("FUND_FROM_DT"),
    F.col("Transform.FUND_THRU_DT").alias("FUND_THRU_DT"),
    F.col("Transform.PCA_AMT").alias("PCA_AMT"),
    F.col("Transform.SGSG_ID").alias("SGSG_ID"),
    F.col("Transform.GRGR_ID").alias("GRGR_ID")
)

# Create the final ordered DF for IdsAltFundInvcPaymt, applying rpad for char/varchar columns
df_IdsAltFundInvcPaymt_final = (
    df_IdsAltFundInvcPaymt
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("FUND_FROM_DT", F.rpad(F.col("FUND_FROM_DT"), 10, " "))
    .withColumn("FUND_THRU_DT", F.rpad(F.col("FUND_THRU_DT"), 10, " "))
    .withColumn("SGSG_ID", F.rpad(F.col("SGSG_ID"), 4, " "))
    .withColumn("GRGR_ID", F.rpad(F.col("GRGR_ID"), 8, " "))
    .select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "ALT_FUND_INVC_PAYMT_SK",
        "ALT_FUND_INVC_ID",
        "MBR_UNIQ_KEY",
        "ALT_FUND_CNTR_PERD_NO",
        "CLS_PLN_ID",
        "PROD_ID",
        "CLM_ID",
        "SUB_UNIQ_KEY",
        "SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "ALT_FUND_INVC_SK",
        "CLM",
        "CLS_PLN",
        "GRP",
        "MBR",
        "PROD",
        "SUBGRP",
        "SUB",
        "FUND_FROM_DT",
        "FUND_THRU_DT",
        "PCA_AMT",
        "SGSG_ID",
        "GRGR_ID"
    )
)

# Write the final DataFrame to file (IdsAltFundInvcPaymt)
write_files(
    df_IdsAltFundInvcPaymt_final,
    f"{adls_path}/key/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# The "updt" link: insert rows into dummy table if ALT_FUND_INVC_PAYMT_SK was null (IsNull lkup => @TRUE)
df_updt = df_enriched.filter(F.isnull("lkup.ALT_FUND_INVC_PAYMT_SK")).select(
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.ALT_FUND_INVC_ID").alias("ALT_FUND_INVC_ID"),
    F.col("Transform.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("Transform.ALT_FUND_CNTR_PERD_NO").alias("ALT_FUND_CNTR_PERD_NO"),
    F.col("Transform.CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.col("Transform.PROD_ID").alias("PROD_ID"),
    F.col("Transform.CLM_ID").alias("CLM_ID"),
    F.col("Transform.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("Transform.SEQ_NO").alias("SEQ_NO"),
    F.col("CurrRunCycle").cast(IntegerType()).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("SK").alias("ALT_FUND_INVC_PAYMT_SK")
)

# Merge into IDS.dummy_hf_alt_fund_invc_paymt
# Create a staging table STAGING.FctsAltFundInvcPaymtExtr_hf_alt_fund_invc_paymt_updt_temp
execute_dml(
    f"DROP TABLE IF EXISTS STAGING.FctsAltFundInvcPaymtExtr_hf_alt_fund_invc_paymt_updt_temp",
    jdbc_url_ids,
    jdbc_props_ids
)

# Write df_updt to staging table
df_updt.write \
    .format("jdbc") \
    .option("url", jdbc_url_ids) \
    .options(**jdbc_props_ids) \
    .option("dbtable", "STAGING.FctsAltFundInvcPaymtExtr_hf_alt_fund_invc_paymt_updt_temp") \
    .mode("overwrite") \
    .save()

# Perform the merge: matched => do nothing, not matched => insert
merge_sql_updt = """
MERGE INTO IDS.dummy_hf_alt_fund_invc_paymt AS T
USING STAGING.FctsAltFundInvcPaymtExtr_hf_alt_fund_invc_paymt_updt_temp AS S
ON
    T.SRC_SYS_CD = S.SRC_SYS_CD AND
    T.ALT_FUND_INVC_ID = S.ALT_FUND_INVC_ID AND
    T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY AND
    T.ALT_FUND_CNTR_PERD_NO = S.ALT_FUND_CNTR_PERD_NO AND
    T.CLS_PLN_ID = S.CLS_PLN_ID AND
    T.PROD_ID = S.PROD_ID AND
    T.CLM_ID = S.CLM_ID AND
    T.SUB_UNIQ_KEY = S.SUB_UNIQ_KEY AND
    T.SEQ_NO = S.SEQ_NO
WHEN NOT MATCHED THEN
  INSERT (
    SRC_SYS_CD,
    ALT_FUND_INVC_ID,
    MBR_UNIQ_KEY,
    ALT_FUND_CNTR_PERD_NO,
    CLS_PLN_ID,
    PROD_ID,
    CLM_ID,
    SUB_UNIQ_KEY,
    SEQ_NO,
    CRT_RUN_CYC_EXCTN_SK,
    ALT_FUND_INVC_PAYMT_SK
  )
  VALUES (
    S.SRC_SYS_CD,
    S.ALT_FUND_INVC_ID,
    S.MBR_UNIQ_KEY,
    S.ALT_FUND_CNTR_PERD_NO,
    S.CLS_PLN_ID,
    S.PROD_ID,
    S.CLM_ID,
    S.SUB_UNIQ_KEY,
    S.SEQ_NO,
    S.CRT_RUN_CYC_EXCTN_SK,
    S.ALT_FUND_INVC_PAYMT_SK
  );
"""
execute_dml(merge_sql_updt, jdbc_url_ids, jdbc_props_ids)

# "Clm" link -> W_ALT_FUND_INVC_PAYMT
df_Clm = df_enriched.select(
    F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.col("Transform.CLM_ID").alias("CLM_ID")
)
write_files(
    df_Clm,
    f"{adls_path}/load/W_ALT_FUND_INVC_PAYMT.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# Facets_Source ODBCConnector (StageName: Facets_Source)
df_FacetsSource = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", f"""
SELECT 
A.BLIV_ID,
A.MEME_CK,
A.AFCP_PER_NO,
A.CSPI_ID,
A.PDPD_ID,
A.CLCL_ID,
A.SBSB_CK,
A.INPS_SEQ_NO
FROM 
{FacetsOwner}.CDS_INPS_PYMT_DTL A,
{FacetsOwner}.CMC_SGSG_SUB_GROUP B,
{FacetsOwner}.CMC_GRGR_GROUP C
WHERE
A.SGSG_CK=B.SGSG_CK
AND B.GRGR_CK=C.GRGR_CK
""")
    .load()
)

# Transformer: Transform (StageName: Transform)
# Stage variable: svSrcSysCdSk = "GetFkeyCodes("IDS",1,"SOURCE SYSTEM","FACETS","X")", assume it returns integer
df_TransformSnapshot = (
    df_FacetsSource
    .withColumn("SRC_SYS_CD_SK", F.lit(-1))  # we place a placeholder, then override below
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk).cast(IntegerType()))
    .withColumn("ALT_FUND_INVC_ID", trim(strip_field(F.col("BLIV_ID"))))
    .withColumn("MBR_UNIQ_KEY", F.col("MEME_CK"))
    .withColumn("ALT_FUND_CNTR_PERD_NO", F.col("AFCP_PER_NO"))
    .withColumn("CLS_PLN_ID", trim(strip_field(F.col("CSPI_ID"))))
    .withColumn("PROD_ID", trim(strip_field(F.col("PDPD_ID"))))
    .withColumn("CLM_ID", trim(strip_field(F.col("CLCL_ID"))))
    .withColumn("SUB_UNIQ_KEY", F.col("SBSB_CK"))
    .withColumn("SEQ_NO", F.col("INPS_SEQ_NO"))
)

# Snapshot_File
df_Snapshot_File = df_TransformSnapshot.select(
    "SRC_SYS_CD_SK",
    "ALT_FUND_INVC_ID",
    "MBR_UNIQ_KEY",
    "ALT_FUND_CNTR_PERD_NO",
    "CLS_PLN_ID",
    "PROD_ID",
    "CLM_ID",
    "SUB_UNIQ_KEY",
    "SEQ_NO"
)
write_files(
    df_Snapshot_File,
    f"{adls_path}/load/B_ALT_FUND_INVC_PAYMT.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)