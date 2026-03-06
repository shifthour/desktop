# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   
# MAGIC PROCESSING:   .
# MAGIC       
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                                  Date                         Change Description                                                        Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------                           ----------------------------     -----------------------------------------------------------------------                  ----------------         ------------------------------------       ----------------------------      ----------------
# MAGIC Naren Garapaty                      09/20/2007                Initial program                                                                    3259                       devlIDS30                Steph Goddard          09/27/2007       
# MAGIC 
# MAGIC 
# MAGIC Parikshith Chada                   10/04/2007               Added Balancing extract process to the overall job              3259                      devlIDS30        
# MAGIC 
# MAGIC 
# MAGIC Goutham Kalidindi                  2021-03-24                Changed Datatype length for field BLIV_ID                      358186                                                        Kalyan Neelam         2021-03-31
# MAGIC                                                                                   char(12) to Varchar(15)
# MAGIC Prabhu ES                            2022-02-24                  S2S Remediation - MSSQL conn param added              IntegrateDev5                                              Kalyan Neelam         2022-06-13

# MAGIC Pulling CDS_INVOICE Data
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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


BCBSOwner = get_widget_value('BCBSOwner','')
bcbs_secret_name = get_widget_value('bcbs_secret_name','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
ids_secret_name = get_widget_value('ids_secret_name','')
TmpOutFile = get_widget_value('TmpOutFile','')
StartDate = get_widget_value('StartDate','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
FirstRecycleDt = get_widget_value('FirstRecycleDt','')

jdbc_url_bcbs, jdbc_props_bcbs = get_db_config(bcbs_secret_name)
df_BCBS_FUND_DT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bcbs)
    .options(**jdbc_props_bcbs)
    .option("query", f"SELECT BLBL_FNDG_THRU_DT, BLBL_FNDG_FROM_DT, BLBL_CREATE_DT, BLBL_DUE_DT FROM {BCBSOwner}.BCBS_FUND_DT")
    .load()
)

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
df_CDS_INID_INVOICE = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", f"""SELECT
P.BLIV_ID,
P.BLDF_MCTR_STMT,
P.BLEI_CK,
P.BLBL_DUE_DT,
P.BLBL_END_DT,
P.BLIV_CREATE_DTM,
P.BLBL_BILLED_AMT,
P.INID_OUTSTAND_BAL,
P.BLEI_NET_DUE,
P.INID_PYMT_TYPE,
P.BLBL_FNDG_FROM_DT,
P.BLBL_FNDG_THRU_DT,
P.AFAI_CK
FROM {FacetsOwner}.CDS_INID_INVOICE P
WHERE
P.BLIV_CREATE_DTM>='{StartDate}' AND
P.AFAI_CK<>0""")
    .load()
)

df_Facets_Source = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", f"""SELECT 
CDS_INID_INVOICE.BLIV_ID,
CDS_INID_INVOICE.BLBL_DUE_DT,
CDS_INID_INVOICE.BLBL_END_DT,
CDS_INID_INVOICE.BLBL_BILLED_AMT,
CDS_INID_INVOICE.INID_OUTSTAND_BAL,
CDS_INID_INVOICE.BLEI_NET_DUE
FROM {FacetsOwner}.CDS_INID_INVOICE CDS_INID_INVOICE
WHERE CDS_INID_INVOICE.BLIV_CREATE_DTM>='{StartDate}'
AND CDS_INID_INVOICE.AFAI_CK<>0""")
    .load()
)

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_hf_alt_fund_invc = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", "SELECT SRC_SYS_CD, ALT_FUND_INVC_ID, CRT_RUN_CYC_EXCTN_SK, ALT_FUND_INVC_SK FROM dummy_hf_alt_fund_invc")
    .load()
)

df_BCBS_FUND_DT_dedup = dedup_sort(
    df_BCBS_FUND_DT,
    ["BLBL_FNDG_THRU_DT"],
    [("BLBL_FNDG_THRU_DT","A")]
)

df_Strip_pre = df_CDS_INID_INVOICE.alias("E").join(
    df_BCBS_FUND_DT_dedup.alias("B"),
    (F.col("E.BLBL_FNDG_THRU_DT") == F.col("B.BLBL_FNDG_THRU_DT")) &
    (F.col("E.BLBL_FNDG_FROM_DT") == F.col("B.BLBL_FNDG_FROM_DT")),
    how="left"
)

df_Strip = (
    df_Strip_pre
    .withColumn("BLIV_ID", strip_field(F.col("E.BLIV_ID")))
    .withColumn("BLDF_MCTR_STMT", strip_field(F.col("E.BLDF_MCTR_STMT")))
    .withColumn("BLEI_CK", F.col("E.BLEI_CK"))
    .withColumn("BLBL_DUE_DT", F.when(F.col("E.BLBL_DUE_DT").isNull(), F.lit("-1"))
                .otherwise(F.date_format(F.col("E.BLBL_DUE_DT"), "yyyy-MM-dd")))
    .withColumn("BLBL_END_DT", F.when(F.col("E.BLBL_END_DT").isNull(), F.lit("-1"))
                .otherwise(F.date_format(F.col("E.BLBL_END_DT"), "yyyy-MM-dd")))
    .withColumn("BLIV_CREATE_DTM", F.when(F.col("E.BLIV_CREATE_DTM").isNull(), F.lit("-1"))
                .otherwise(F.date_format(F.col("E.BLIV_CREATE_DTM"), "yyyy-MM-dd")))
    .withColumn("BLBL_BILLED_AMT", F.col("E.BLBL_BILLED_AMT"))
    .withColumn("INID_OUTSTAND_BAL", F.col("E.INID_OUTSTAND_BAL"))
    .withColumn("BLEI_NET_DUE", F.col("E.BLEI_NET_DUE"))
    .withColumn("INID_PYMT_TYPE", strip_field(F.col("E.INID_PYMT_TYPE")))
    .withColumn("BLBL_FNDG_FROM_DT", F.when(F.col("E.BLBL_FNDG_FROM_DT").isNull(), F.lit("-1"))
                .otherwise(F.date_format(F.col("E.BLBL_FNDG_FROM_DT"), "yyyy-MM-dd")))
    .withColumn("BLBL_FNDG_THRU_DT", F.when(F.col("E.BLBL_FNDG_THRU_DT").isNull(), F.lit("-1"))
                .otherwise(F.date_format(F.col("E.BLBL_FNDG_THRU_DT"), "yyyy-MM-dd")))
    .withColumn("AFAI_CK", F.col("E.AFAI_CK"))
    .withColumn("BLBL_CREATE_DT", F.when(F.col("B.BLBL_CREATE_DT").isNull(), F.lit("-1"))
                .otherwise(F.date_format(F.col("B.BLBL_CREATE_DT"), "yyyy-MM-dd")))
    .withColumn("BLBL_DUE_DT_1", F.when(F.col("B.BLBL_DUE_DT").isNull(), F.lit("-1"))
                .otherwise(F.date_format(F.col("B.BLBL_DUE_DT"), "yyyy-MM-dd")))
    .select(
        "BLIV_ID",
        "BLDF_MCTR_STMT",
        "BLEI_CK",
        "BLBL_DUE_DT",
        "BLBL_END_DT",
        "BLIV_CREATE_DTM",
        "BLBL_BILLED_AMT",
        "INID_OUTSTAND_BAL",
        "BLEI_NET_DUE",
        "INID_PYMT_TYPE",
        "BLBL_FNDG_FROM_DT",
        "BLBL_FNDG_THRU_DT",
        "AFAI_CK",
        "BLBL_CREATE_DT",
        "BLBL_DUE_DT_1"
    )
)

df_BusinessRules = (
    df_Strip
    .withColumn("RowPassThru", F.lit("Y"))
    .withColumn("svBlivId", trim(F.col("BLIV_ID")))
    .withColumn("svSrcSysCd", F.lit("FACETS"))
    .withColumn("svBldfMctrStmt", trim(F.col("BLDF_MCTR_STMT")))
    .withColumn("svInidPymtType", trim(F.col("INID_PYMT_TYPE")))
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.col("RowPassThru"))
    .withColumn("FIRST_RECYC_DT", F.lit(FirstRecycleDt))
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.col("svSrcSysCd"))
    .withColumn("PRI_KEY_STRING", F.concat(F.col("svSrcSysCd"), F.lit(" ;"), F.col("svBlivId")))
    .withColumn("ALT_FUND_INVC_SK", F.lit(0))
    .withColumn("ALT_FUND_INVC_ID", F.col("svBlivId"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("ALT_FUND", F.col("AFAI_CK"))
    .withColumn("BILL_ENTY", F.col("BLEI_CK"))
    .withColumn("ALT_FUND_INVC_STYLE_CD",
                F.when((F.col("svBldfMctrStmt").isNull()) | (F.length(F.col("svBldfMctrStmt")) == 0), F.lit("NA"))
                .otherwise(F.col("svBldfMctrStmt")))
    .withColumn("ALT_FUND_INVC_PAYMT_CD",
                F.when((F.col("svInidPymtType").isNull()) | (F.length(F.col("svInidPymtType")) == 0), F.lit("NA"))
                .otherwise(F.col("svInidPymtType")))
    .withColumn("BCBS_BILL_DT", F.col("BLBL_CREATE_DT"))
    .withColumn("BCBS_DUE_DT", F.col("BLBL_DUE_DT_1"))
    .withColumn("BILL_DUE_DT", F.col("BLBL_DUE_DT"))
    .withColumn("BILL_END_DT", F.col("BLBL_END_DT"))
    .withColumn("CRT_DT", F.col("BLIV_CREATE_DTM"))
    .withColumn("FUND_FROM_DT", F.col("BLBL_FNDG_FROM_DT"))
    .withColumn("FUND_THRU_DT", F.col("BLBL_FNDG_THRU_DT"))
    .withColumn("BILL_AMT",
                F.when((F.col("BLBL_BILLED_AMT").isNull()) | (F.length(F.col("BLBL_BILLED_AMT").cast(StringType())) == 0), F.lit(0))
                .otherwise(F.col("BLBL_BILLED_AMT")))
    .withColumn("NET_DUE_AMT",
                F.when((F.col("BLEI_NET_DUE").isNull()) | (F.length(F.col("BLEI_NET_DUE").cast(StringType())) == 0), F.lit(0))
                .otherwise(F.col("BLEI_NET_DUE")))
    .withColumn("OUTSTND_BAL_AMT",
                F.when((F.col("INID_OUTSTAND_BAL").isNull()) | (F.length(F.col("INID_OUTSTAND_BAL").cast(StringType())) == 0), F.lit(0))
                .otherwise(F.col("INID_OUTSTAND_BAL")))
)

df_join_pkey = df_BusinessRules.alias("T").join(
    df_hf_alt_fund_invc.alias("L"),
    [
        F.col("T.SRC_SYS_CD") == F.col("L.SRC_SYS_CD"),
        F.col("T.ALT_FUND_INVC_ID") == F.col("L.ALT_FUND_INVC_ID")
    ],
    how="left"
)

df_join_pkey = df_join_pkey.withColumn(
    "TMP_ALT_FUND_INVC_SK",
    F.col("L.ALT_FUND_INVC_SK").cast(IntegerType())
).withColumn(
    "TMP_CRT_RUN_CYC_EXCTN_SK",
    F.when(F.col("L.ALT_FUND_INVC_SK").isNull(), F.lit(CurrRunCycle))
    .otherwise(F.col("L.CRT_RUN_CYC_EXCTN_SK"))
)

df_join_pkey = df_join_pkey.withColumn("ALT_FUND_INVC_SK", F.col("TMP_ALT_FUND_INVC_SK"))
df_join_pkey = SurrogateKeyGen(df_join_pkey,<DB sequence name>,"ALT_FUND_INVC_SK",<schema>,<secret_name>)
df_join_pkey = df_join_pkey.withColumn(
    "ALT_FUND_INVC_SK",
    F.when(F.col("TMP_ALT_FUND_INVC_SK").isNull(), F.col("ALT_FUND_INVC_SK")).otherwise(F.col("TMP_ALT_FUND_INVC_SK"))
).withColumn(
    "CRT_RUN_CYC_EXCTN_SK",
    F.col("TMP_CRT_RUN_CYC_EXCTN_SK")
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    F.lit(CurrRunCycle).cast(IntegerType())
)

df_Key = df_join_pkey.select(
    "T.JOB_EXCTN_RCRD_ERR_SK",
    "T.INSRT_UPDT_CD",
    "T.DISCARD_IN",
    "T.PASS_THRU_IN",
    "T.FIRST_RECYC_DT",
    "T.ERR_CT",
    "T.RECYCLE_CT",
    "T.SRC_SYS_CD",
    "T.PRI_KEY_STRING",
    "ALT_FUND_INVC_SK",
    "T.ALT_FUND_INVC_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "T.ALT_FUND",
    "T.BILL_ENTY",
    "T.ALT_FUND_INVC_STYLE_CD",
    "T.ALT_FUND_INVC_PAYMT_CD",
    "T.BCBS_BILL_DT",
    "T.BCBS_DUE_DT",
    "T.BILL_DUE_DT",
    "T.BILL_END_DT",
    "T.CRT_DT",
    "T.FUND_FROM_DT",
    "T.FUND_THRU_DT",
    "T.BILL_AMT",
    "T.NET_DUE_AMT",
    "T.OUTSTND_BAL_AMT"
)

df_Key_final = (
    df_Key
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("BCBS_BILL_DT", F.rpad(F.col("BCBS_BILL_DT"), 10, " "))
    .withColumn("BCBS_DUE_DT", F.rpad(F.col("BCBS_DUE_DT"), 10, " "))
    .withColumn("BILL_DUE_DT", F.rpad(F.col("BILL_DUE_DT"), 10, " "))
    .withColumn("BILL_END_DT", F.rpad(F.col("BILL_END_DT"), 10, " "))
    .withColumn("CRT_DT", F.rpad(F.col("CRT_DT"), 10, " "))
    .withColumn("FUND_FROM_DT", F.rpad(F.col("FUND_FROM_DT"), 10, " "))
    .withColumn("FUND_THRU_DT", F.rpad(F.col("FUND_THRU_DT"), 10, " "))
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
        "ALT_FUND_INVC_SK",
        "ALT_FUND_INVC_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "ALT_FUND",
        "BILL_ENTY",
        "ALT_FUND_INVC_STYLE_CD",
        "ALT_FUND_INVC_PAYMT_CD",
        "BCBS_BILL_DT",
        "BCBS_DUE_DT",
        "BILL_DUE_DT",
        "BILL_END_DT",
        "CRT_DT",
        "FUND_FROM_DT",
        "FUND_THRU_DT",
        "BILL_AMT",
        "NET_DUE_AMT",
        "OUTSTND_BAL_AMT"
    )
)

df_updt = df_join_pkey.filter(F.col("TMP_ALT_FUND_INVC_SK").isNull()).select(
    F.col("T.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("T.ALT_FUND_INVC_ID").alias("ALT_FUND_INVC_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("ALT_FUND_INVC_SK").alias("ALT_FUND_INVC_SK")
)

df_updt.createOrReplaceTempView("__df_updt_tempview__")

spark.sql("DROP TABLE IF EXISTS STAGING.FctsAltFundInvc_hf_alt_fund_invc_updt_temp")

(
    df_updt.write
    .format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("dbtable", "STAGING.FctsAltFundInvc_hf_alt_fund_invc_updt_temp")
    .mode("overwrite")
    .save()
)

merge_sql = """
MERGE INTO dummy_hf_alt_fund_invc AS T
USING STAGING.FctsAltFundInvc_hf_alt_fund_invc_updt_temp AS S
ON (T.SRC_SYS_CD = S.SRC_SYS_CD AND T.ALT_FUND_INVC_ID = S.ALT_FUND_INVC_ID)
WHEN NOT MATCHED THEN
  INSERT (SRC_SYS_CD, ALT_FUND_INVC_ID, CRT_RUN_CYC_EXCTN_SK, ALT_FUND_INVC_SK)
  VALUES (S.SRC_SYS_CD, S.ALT_FUND_INVC_ID, S.CRT_RUN_CYC_EXCTN_SK, S.ALT_FUND_INVC_SK);
"""

execute_dml(merge_sql, jdbc_url_ids, jdbc_props_ids)

write_files(
    df_Key_final,
    f"{adls_path}/key/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_Transform = (
    df_Facets_Source
    .withColumn("svSrcSysCdSk", GetFkeyCodes("IDS", F.lit(1), "SOURCE SYSTEM", "FACETS", "X"))
    .withColumn("svBlblDueDt", F.when(F.col("BLBL_DUE_DT").isNull(), F.lit("-1"))
                .otherwise(F.date_format(F.col("BLBL_DUE_DT"), "yyyy-MM-dd")))
    .withColumn("svBlblEndDt", F.when(F.col("BLBL_END_DT").isNull(), F.lit("-1"))
                .otherwise(F.date_format(F.col("BLBL_END_DT"), "yyyy-MM-dd")))
    .withColumn("svBillDueDtSk", GetFkeyDate("IDS", F.lit(100), F.col("svBlblDueDt"), "X"))
    .withColumn("svBillEndDtSk", GetFkeyDate("IDS", F.lit(102), F.col("svBlblEndDt"), "X"))
)

df_Snapshot = df_Transform.select(
    F.col("svSrcSysCdSk").alias("SRC_SYS_CD_SK"),
    trim(strip_field(F.col("BLIV_ID"))).alias("ALT_FUND_INVC_ID"),
    F.col("svBillDueDtSk").alias("BILL_DUE_DT_SK"),
    F.col("svBillEndDtSk").alias("BILL_END_DT_SK"),
    F.col("BLBL_BILLED_AMT").alias("BILL_AMT"),
    F.col("BLEI_NET_DUE").alias("NET_DUE_AMT"),
    F.col("INID_OUTSTAND_BAL").alias("OUTSTND_BAL_AMT")
)

df_Snapshot_final = (
    df_Snapshot
    .withColumn("BILL_DUE_DT_SK", F.rpad(F.col("BILL_DUE_DT_SK"), 10, " "))
    .withColumn("BILL_END_DT_SK", F.rpad(F.col("BILL_END_DT_SK"), 10, " "))
    .select(
        "SRC_SYS_CD_SK",
        "ALT_FUND_INVC_ID",
        "BILL_DUE_DT_SK",
        "BILL_END_DT_SK",
        "BILL_AMT",
        "NET_DUE_AMT",
        "OUTSTND_BAL_AMT"
    )
)

write_files(
    df_Snapshot_final,
    f"{adls_path}/load/B_ALT_FUND_INVC.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)