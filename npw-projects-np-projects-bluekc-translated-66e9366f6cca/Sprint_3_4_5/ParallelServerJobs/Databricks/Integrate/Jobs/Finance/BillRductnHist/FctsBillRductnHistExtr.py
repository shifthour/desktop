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
# MAGIC                                                     Project/                                                                                                                       Code                   Date
# MAGIC Developer              Date              Altiris #     Change Description                                                                                     Reviewer            Reviewed
# MAGIC ----------------------------  -------------------   -------------   -----------------------------------------------------------------------------------------------------------------   -------------------------  -------------------
# MAGIC Naren Garapaty     09/20/2007   3259        Initial program                                                                                              Steph Goddard          09/27/2007
# MAGIC Parikshith Chada   10/08/2007   3259        Added Balancing extract process to the overall job                                     
# MAGIC Hugh Sisson          03/30/2009   188315    Added steps to use Claim Primary Key table for foreign key lookups            Steph Goddard     04/06/2009 
# MAGIC 
# MAGIC Goutham Kalidindi  2021-03-24   358186    Changed Datatype length for field BLIV_ID                                                   Kalyan Neelam      2021-03-31
# MAGIC                                                                   char(12) to Varchar(15)
# MAGIC Prabhu ES              2022-02-24                   S2S Remediation-MSSQL connection parameters added                            Kalyan Neelam      2022-06-13

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
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


FACETSOwner = get_widget_value("FacetsOwner", "")
facets_secret_name = get_widget_value("facets_secret_name", "")
TmpOutFile = get_widget_value("TmpOutFile", "IdsBillRductnHistPkey.BillRductnHistTmp.dat")
CurrRunCycle = get_widget_value("CurrRunCycle", "1")
FirstRecycleDt = get_widget_value("FirstRecycleDt", "2007-09-26")
SrcSysCdSk = get_widget_value("SrcSysCdSk", "")

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)

jdbc_url_dummy, jdbc_props_dummy = get_db_config(facets_secret_name)
extract_query_hf_bill_rductn_hist = (
    "SELECT SRC_SYS_CD, MBR_UNIQ_KEY, BILL_ENTY_UNIQ_KEY, ALT_FUND_CNTR_PERD_NO, FUND_FROM_DT, SEQ_NO, "
    "CRT_RUN_CYC_EXCTN_SK, BILL_RDUCTN_HIST_SK FROM dummy_hf_bill_rductn_hist"
)
df_hf_bill_rductn_hist = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_dummy)
    .options(**jdbc_props_dummy)
    .option("query", extract_query_hf_bill_rductn_hist)
    .load()
)

select_Extract = f"""
SELECT
A.MEME_CK,
A.BLEI_CK,
A.AFCP_PER_NO,
A.BLRH_FNDG_FROM_DT,
A.BLRH_SEQ_NO,
A.BLRH_FNDG_THRU_DT,
A.GRGR_CK,
A.SGSG_CK,
A.CSPI_ID,
A.PDPD_ID,
A.BLRH_ORIG_CLCL_ID,
A.BLRH_HSA_AMT,
B.SGSG_ID,
C.GRGR_ID
FROM {FACETSOwner}.CMC_BLRH_RED_HIST A,
     {FACETSOwner}.CMC_SGSG_SUB_GROUP B,
     {FACETSOwner}.CMC_GRGR_GROUP C
WHERE
A.SGSG_CK=B.SGSG_CK AND
B.GRGR_CK=C.GRGR_CK
"""

df_FACETS_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", select_Extract)
    .load()
)

select_BlivId = f"""
SELECT
A.MEME_CK,
A.BLEI_CK,
A.AFCP_PER_NO,
A.BLRH_FNDG_FROM_DT,
A.BLRH_SEQ_NO,
B.BLIV_ID
FROM
{FACETSOwner}.CMC_BLRH_RED_HIST A,
{FACETSOwner}.CDS_INID_INVOICE B
WHERE
A.BLEI_CK=B.BLEI_CK AND
A.BLRH_FNDG_FROM_DT=B.BLBL_FNDG_FROM_DT AND
A.BLRH_FNDG_THRU_DT=B.BLBL_FNDG_THRU_DT AND
B.BLEI_BILL_LEVEL='A' AND
B.AFAI_CK>0 AND
B.BLIV_CREATE_DTM>'2006-12-31'
"""

df_FACETS_BlivId = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", select_BlivId)
    .load()
)

df_hf_brdh_bliv_id = df_FACETS_BlivId.dropDuplicates(
    ["MEME_CK", "BLEI_CK", "AFCP_PER_NO", "BLRH_FNDG_FROM_DT", "BLRH_SEQ_NO"]
)

df_StripField_join = df_FACETS_Extract.alias("Extract").join(
    df_hf_brdh_bliv_id.alias("BlivIdLkup"),
    on=[
        F.col("Extract.MEME_CK") == F.col("BlivIdLkup.MEME_CK"),
        F.col("Extract.BLEI_CK") == F.col("BlivIdLkup.BLEI_CK"),
        F.col("Extract.AFCP_PER_NO") == F.col("BlivIdLkup.AFCP_PER_NO"),
        F.col("Extract.BLRH_FNDG_FROM_DT") == F.col("BlivIdLkup.BLRH_FNDG_FROM_DT"),
        F.col("Extract.BLRH_SEQ_NO") == F.col("BlivIdLkup.BLRH_SEQ_NO"),
    ],
    how="left",
)
df_StripField = (
    df_StripField_join
    .withColumn("MEME_CK", F.col("Extract.MEME_CK"))
    .withColumn("BLEI_CK", F.col("Extract.BLEI_CK"))
    .withColumn("AFCP_PER_NO", F.col("Extract.AFCP_PER_NO"))
    .withColumn(
        "BLRH_FNDG_FROM_DT",
        FORMAT.DATE(
            F.col("Extract.BLRH_FNDG_FROM_DT"),
            F.lit("SYBASE"),
            F.lit("TIMESTAMP"),
            F.lit("CCYY-MM-DD"),
        ),
    )
    .withColumn("BLRH_SEQ_NO", F.col("Extract.BLRH_SEQ_NO"))
    .withColumn(
        "BLRH_FNDG_THRU_DT",
        FORMAT.DATE(
            F.col("Extract.BLRH_FNDG_THRU_DT"),
            F.lit("SYBASE"),
            F.lit("TIMESTAMP"),
            F.lit("CCYY-MM-DD"),
        ),
    )
    .withColumn("GRGR_CK", F.col("Extract.GRGR_CK"))
    .withColumn("SGSG_CK", F.col("Extract.SGSG_CK"))
    .withColumn(
        "CSPI_ID",
        Convert(F.lit("CHAR(10) : CHAR(13) : CHAR(9)"), F.lit(""), F.col("Extract.CSPI_ID")),
    )
    .withColumn(
        "PDPD_ID",
        Convert(F.lit("CHAR(10) : CHAR(13) : CHAR(9)"), F.lit(""), F.col("Extract.PDPD_ID")),
    )
    .withColumn(
        "BLRH_ORIG_CLCL_ID",
        Convert(
            F.lit("CHAR(10) : CHAR(13) : CHAR(9)"), F.lit(""), F.col("Extract.BLRH_ORIG_CLCL_ID")
        ),
    )
    .withColumn("BLRH_HSA_AMT", F.col("Extract.BLRH_HSA_AMT"))
    .withColumn(
        "SGSG_ID",
        Convert(F.lit("CHAR(10) : CHAR(13) : CHAR(9)"), F.lit(""), F.col("Extract.SGSG_ID")),
    )
    .withColumn(
        "GRGR_ID",
        Convert(F.lit("CHAR(10) : CHAR(13) : CHAR(9)"), F.lit(""), F.col("Extract.GRGR_ID")),
    )
    .withColumn(
        "BLIV_ID",
        F.when(F.isnull(F.col("BlivIdLkup.BLIV_ID")), F.lit("UNK")).otherwise(
            Convert(F.lit("CHAR(10) : CHAR(13) : CHAR(9)"), F.lit(""), F.col("BlivIdLkup.BLIV_ID"))
        ),
    )
)

df_BusinessRules = (
    df_StripField
    .withColumn("RowPassThru", F.lit("Y"))
    .withColumn("svSrcSysCd", F.lit("FACETS"))
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.col("RowPassThru"))
    .withColumn("FIRST_RECYC_DT", F.lit(FirstRecycleDt))
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.col("svSrcSysCd"))
    .withColumn(
        "PRI_KEY_STRING",
        F.concat_ws(
            ";",
            F.col("svSrcSysCd"),
            F.col("MEME_CK"),
            F.col("BLEI_CK"),
            F.col("AFCP_PER_NO"),
            F.col("BLRH_FNDG_FROM_DT"),
            F.col("BLRH_SEQ_NO"),
        ),
    )
    .withColumn("BILL_RDUCTN_HIST_SK", F.lit(0))
    .withColumn("MBR_UNIQ_KEY", F.col("MEME_CK"))
    .withColumn("BILL_ENTY_UNIQ_KEY", F.col("BLEI_CK"))
    .withColumn("ALT_FUND_CNTR_PERD_NO", F.col("AFCP_PER_NO"))
    .withColumn("FUND_FROM_DT", F.col("BLRH_FNDG_FROM_DT"))
    .withColumn("SEQ_NO", F.col("BLRH_SEQ_NO"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("ALT_FUND_INVC", trim(F.col("BLIV_ID")))
    .withColumn("BILL_ENTY", F.col("BLEI_CK"))
    .withColumn("CLS_PLN", trim(F.col("CSPI_ID")))
    .withColumn("GRP", F.col("GRGR_CK"))
    .withColumn("MBR", F.col("MEME_CK"))
    .withColumn("CLM", trim(F.col("BLRH_ORIG_CLCL_ID")))
    .withColumn("PROD", trim(F.col("PDPD_ID")))
    .withColumn("SUBGRP", F.lit(0))
    .withColumn("FUND_THRU_DT", F.col("BLRH_FNDG_THRU_DT"))
    .withColumn(
        "PCA_AMT",
        F.when(
            F.isnull(F.col("BLRH_HSA_AMT")).__or__(F.length(F.col("BLRH_HSA_AMT")) == 0),
            F.lit(0),
        ).otherwise(-1 * F.col("BLRH_HSA_AMT")),
    )
    .withColumn("CLM_ID", trim(F.col("BLRH_ORIG_CLCL_ID")))
    .withColumn("SGSG_ID", trim(F.col("SGSG_ID")))
    .withColumn("GRGR_ID", trim(F.col("GRGR_ID")))
)

df_PkeyTrn_join = df_BusinessRules.alias("Transform").join(
    df_hf_bill_rductn_hist.alias("lkup"),
    on=[
        F.col("Transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD"),
        F.col("Transform.MBR_UNIQ_KEY") == F.col("lkup.MBR_UNIQ_KEY"),
        F.col("Transform.BILL_ENTY_UNIQ_KEY") == F.col("lkup.BILL_ENTY_UNIQ_KEY"),
        F.col("Transform.ALT_FUND_CNTR_PERD_NO") == F.col("lkup.ALT_FUND_CNTR_PERD_NO"),
        F.col("Transform.FUND_FROM_DT") == F.col("lkup.FUND_FROM_DT"),
        F.col("Transform.SEQ_NO") == F.col("lkup.SEQ_NO"),
    ],
    how="left",
)

df_enriched = df_PkeyTrn_join.withColumnRenamed("lkup.BILL_RDUCTN_HIST_SK", "lkup_BILL_RDUCTN_HIST_SK").withColumnRenamed("lkup.CRT_RUN_CYC_EXCTN_SK", "lkup_CRT_RUN_CYC_EXCTN_SK")

df_enriched = SurrogateKeyGen(
    df_enriched,
    <DB sequence name>,
    "BILL_RDUCTN_HIST_SK",
    <schema>,
    <secret_name>
)

df_Key = (
    df_enriched
    .withColumn(
        "SK",
        F.when(
            F.isnull(F.col("lkup_BILL_RDUCTN_HIST_SK")), F.col("BILL_RDUCTN_HIST_SK")
        ).otherwise(F.col("lkup_BILL_RDUCTN_HIST_SK")),
    )
    .withColumn(
        "NewCrtRunCycExtcnSk",
        F.when(
            F.isnull(F.col("lkup_BILL_RDUCTN_HIST_SK")),
            F.lit(CurrRunCycle),
        ).otherwise(F.col("lkup_CRT_RUN_CYC_EXCTN_SK")),
    )
)

df_Key_final = (
    df_Key
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.col("Transform.JOB_EXCTN_RCRD_ERR_SK"))
    .withColumn("INSRT_UPDT_CD", F.col("Transform.INSRT_UPDT_CD"))
    .withColumn("DISCARD_IN", F.col("Transform.DISCARD_IN"))
    .withColumn("PASS_THRU_IN", F.col("Transform.PASS_THRU_IN"))
    .withColumn("FIRST_RECYC_DT", F.col("Transform.FIRST_RECYC_DT"))
    .withColumn("ERR_CT", F.col("Transform.ERR_CT"))
    .withColumn("RECYCLE_CT", F.col("Transform.RECYCLE_CT"))
    .withColumn("SRC_SYS_CD", F.col("Transform.SRC_SYS_CD"))
    .withColumn("PRI_KEY_STRING", F.col("Transform.PRI_KEY_STRING"))
    .withColumn("BILL_RDUCTN_HIST_SK", F.col("SK"))
    .withColumn("MBR_UNIQ_KEY", F.col("Transform.MBR_UNIQ_KEY"))
    .withColumn("BILL_ENTY_UNIQ_KEY", F.col("Transform.BILL_ENTY_UNIQ_KEY"))
    .withColumn("ALT_FUND_CNTR_PERD_NO", F.col("Transform.ALT_FUND_CNTR_PERD_NO"))
    .withColumn("FUND_FROM_DT", F.col("Transform.FUND_FROM_DT"))
    .withColumn("SEQ_NO", F.col("Transform.SEQ_NO"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.col("NewCrtRunCycExtcnSk"))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle))
    .withColumn("ALT_FUND_INVC", F.col("Transform.ALT_FUND_INVC"))
    .withColumn("BILL_ENTY", F.col("Transform.BILL_ENTY"))
    .withColumn("CLS_PLN", F.col("Transform.CLS_PLN"))
    .withColumn("GRP", F.col("Transform.GRP"))
    .withColumn("MBR", F.col("Transform.MBR"))
    .withColumn("CLM", F.col("Transform.CLM"))
    .withColumn("PROD", F.col("Transform.PROD"))
    .withColumn("SUBGRP", F.col("Transform.SUBGRP"))
    .withColumn("FUND_THRU_DT", F.col("Transform.FUND_THRU_DT"))
    .withColumn("PCA_AMT", F.col("Transform.PCA_AMT"))
    .withColumn("CLM_ID", F.col("Transform.CLM_ID"))
    .withColumn("SGSG_ID", F.col("Transform.SGSG_ID"))
    .withColumn("GRGR_ID", F.col("Transform.GRGR_ID"))
)

df_hf_bill_rductn_hist_updt = df_Key.filter(F.isnull(F.col("lkup_BILL_RDUCTN_HIST_SK"))).select(
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("Transform.BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
    F.col("Transform.ALT_FUND_CNTR_PERD_NO").alias("ALT_FUND_CNTR_PERD_NO"),
    F.col("Transform.FUND_FROM_DT").alias("FUND_FROM_DT"),
    F.col("Transform.SEQ_NO").alias("SEQ_NO"),
    F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("SK").alias("BILL_RDUCTN_HIST_SK"),
)

execute_dml(
    f"DROP TABLE IF EXISTS STAGING.FctsBillRductnHistExtr_hf_bill_rductn_hist_updt_temp",
    jdbc_url_dummy,
    jdbc_props_dummy
)

df_hf_bill_rductn_hist_updt.write.jdbc(
    url=jdbc_url_dummy,
    table="STAGING.FctsBillRductnHistExtr_hf_bill_rductn_hist_updt_temp",
    mode="overwrite",
    properties=jdbc_props_dummy,
)

merge_sql_updt = """
MERGE dummy_hf_bill_rductn_hist AS T
USING STAGING.FctsBillRductnHistExtr_hf_bill_rductn_hist_updt_temp AS S
ON 
    T.SRC_SYS_CD = S.SRC_SYS_CD AND
    T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY AND
    T.BILL_ENTY_UNIQ_KEY = S.BILL_ENTY_UNIQ_KEY AND
    T.ALT_FUND_CNTR_PERD_NO = S.ALT_FUND_CNTR_PERD_NO AND
    T.FUND_FROM_DT = S.FUND_FROM_DT AND
    T.SEQ_NO = S.SEQ_NO
WHEN MATCHED THEN
  UPDATE SET 
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
    T.BILL_RDUCTN_HIST_SK = S.BILL_RDUCTN_HIST_SK
WHEN NOT MATCHED THEN
  INSERT (
    SRC_SYS_CD,
    MBR_UNIQ_KEY,
    BILL_ENTY_UNIQ_KEY,
    ALT_FUND_CNTR_PERD_NO,
    FUND_FROM_DT,
    SEQ_NO,
    CRT_RUN_CYC_EXCTN_SK,
    BILL_RDUCTN_HIST_SK
  )
  VALUES (
    S.SRC_SYS_CD,
    S.MBR_UNIQ_KEY,
    S.BILL_ENTY_UNIQ_KEY,
    S.ALT_FUND_CNTR_PERD_NO,
    S.FUND_FROM_DT,
    S.SEQ_NO,
    S.CRT_RUN_CYC_EXCTN_SK,
    S.BILL_RDUCTN_HIST_SK
  );
"""

execute_dml(merge_sql_updt, jdbc_url_dummy, jdbc_props_dummy)

df_IdsBillRductnHist = df_Key_final.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "BILL_RDUCTN_HIST_SK",
    "MBR_UNIQ_KEY",
    "BILL_ENTY_UNIQ_KEY",
    "ALT_FUND_CNTR_PERD_NO",
    "FUND_FROM_DT",
    "SEQ_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "ALT_FUND_INVC",
    "BILL_ENTY",
    "CLS_PLN",
    "GRP",
    "MBR",
    "CLM",
    "PROD",
    "SUBGRP",
    "FUND_THRU_DT",
    "PCA_AMT",
    "CLM_ID",
    "SGSG_ID",
    "GRGR_ID",
)

write_files(
    df_IdsBillRductnHist,
    f"{adls_path}/key/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None,
)

df_W_BILL_RDUCTN_HIST = df_Key.filter(True).select(
    F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.col("Transform.CLM_ID").alias("CLM_ID"),
).dropna(subset=["SRC_SYS_CD_SK","CLM_ID"])

write_files(
    df_W_BILL_RDUCTN_HIST,
    f"{adls_path}/load/W_BILL_RDUCTN_HIST.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None,
)

select_Facets_Source = f"""
SELECT
A.MEME_CK,
A.BLEI_CK,
A.AFCP_PER_NO,
A.BLRH_FNDG_FROM_DT,
A.BLRH_SEQ_NO
FROM
{FACETSOwner}.CMC_BLRH_RED_HIST  A,
{FACETSOwner}.CMC_SGSG_SUB_GROUP B,
{FACETSOwner}.CMC_GRGR_GROUP C
WHERE
A.SGSG_CK=B.SGSG_CK AND
B.GRGR_CK=C.GRGR_CK
"""

df_Facets_Source = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", select_Facets_Source)
    .load()
)

df_Transform = (
    df_Facets_Source
    .withColumn(
        "svSrcSysCdSk",
        GetFkeyCodes(F.lit("IDS"), F.lit(1), F.lit("SOURCE SYSTEM"), F.lit("FACETS"), F.lit("X")),
    )
    .withColumn(
        "svBlrhFndgFromDt",
        FORMAT.DATE(F.col("BLRH_FNDG_FROM_DT"), F.lit("SYBASE"), F.lit("TIMESTAMP"), F.lit("CCYY-MM-DD")),
    )
    .withColumn(
        "svFundFromDtSk",
        GetFkeyDate(F.lit("IDS"), F.lit(100), F.col("svBlrhFndgFromDt"), F.lit("X")),
    )
)

df_Snapshot_File = df_Transform.select(
    F.col("svSrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.col("MEME_CK").alias("MBR_UNIQ_KEY"),
    F.col("BLEI_CK").alias("BILL_ENTY_UNIQ_KEY"),
    F.col("AFCP_PER_NO").alias("ALT_FUND_CNTR_PERD_NO"),
    F.col("svFundFromDtSk").cast("char(10)").alias("FUND_FROM_DT_SK"),
    F.col("BLRH_SEQ_NO").alias("SEQ_NO"),
)

write_files(
    df_Snapshot_File,
    f"{adls_path}/load/B_BILL_RDUCTN_HIST.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None,
)