# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2010, 2022 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  FctsBillSumExtr
# MAGIC Called by:  FctsIncomeExtrSeq
# MAGIC 
# MAGIC Processing:
# MAGIC                     Pull data from FACETS CMC_BLBL_BILL_SUMM using the driver table created in the Income/Driver/FctsIncomeTempLoad job.
# MAGIC                     Data will be used to populated the IDS BILL_SUM table which is the primary source for the EDW BILL_SUM_F table.
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:    If necessary, repopulate the driver table
# MAGIC                     Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                  Project/                                                                                                                       Code                   Date
# MAGIC Developer           Date              Altiris #     Change Description                                                                                     Reviewer            Reviewed
# MAGIC -------------------------  -------------------   -------------   -----------------------------------------------------------------------------------------------------------------   -------------------------  -------------------
# MAGIC Hugh Sisson       2010-05-25    3346        Original program                                                                                           Steph Goddard   09/21/2010
# MAGIC Raja Gummadi    2014-02-19    5127        Added 3 new columns to the Job                                                                   Kalyan Neelam    2014-02-19
# MAGIC                                                                  BLBL_SUBSIDY_AMT_NVL
# MAGIC                                                                  BLBL_APTC_PAID_STS_NVL
# MAGIC                                                                  BLBL_APTC_DLNQ_DT_NVL
# MAGIC Goutham Kalidindi  2021-03-24 358186    Changed Datatype length for field BLBL_BLIV_ID_LAST                             Kalyan Neelam    2021-03-31
# MAGIC                                                                   char(12) to Varchar(15)
# MAGIC Prabhu ES         2022-03-02  S2S             MSSQL ODBC conn added and other param changes  IntegrateDev5\(9)\(9)Ken Bradmon\(9)2022-06-13

# MAGIC Pulling FACETS Data
# MAGIC The hf_bill_sum_allcol hashed file created in the BillSummNewPK is cleared at the end of this job
# MAGIC Assign primary surrogate key
# MAGIC Strip Carriage Return, Line Feed, and Tab characters.
# MAGIC Trim leading and trailing spaces.
# MAGIC Apply business logic
# MAGIC Writing Sequential File to ../key
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
from pyspark.sql.types import StringType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


facets_secret_name = get_widget_value('facets_secret_name','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
CurrRunDt = get_widget_value('CurrRunDt','')
TmpOutFile = get_widget_value('TmpOutFile','')
DriverTable = get_widget_value('DriverTable','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
FacetsOwner = get_widget_value('FacetsOwner','')
IDSOwner = get_widget_value('IDSOwner','')

jdbc_url, jdbc_props = get_db_config(facets_secret_name)

extract_query = """SELECT DISTINCT
     BLBL.BLEI_CK,
     BLBL.BLBL_DUE_DT,
     BLBL.BLBL_END_DT,
     BLBL.BLBL_CREATE_DTM,
     BLBL.BLBL_DLNQ_DT,
     BLBL.BLBL_PREM_UPD_DTM,
     BLBL.BLBL_LST_ALLOC_DTM,
     BLBL.BLBL_RECON_DTM,
     BLBL.BLBL_BILLED_AMT,
     BLBL.BLBL_RCVD_AMT,
     BLBL.BLBL_PAID_STS,
     BLBL.BLBL_DAYS_BILLED,
     BLBL.BLBL_PRORATE_FCTR,
     BLBL.BLBL_PRORATE_IND,
     BLBL.BLBL_SPCL_BL_IND,
     BLBL.BLBL_BLIV_ID_LAST,
     BLBL.BLBL_TYPE,
     BLBL.BLBL_NO_ACTIVE_SB,
     BLBL.BLBL_SUBSIDY_AMT_NVL,
     BLBL.BLBL_APTC_PAID_STS_NVL,
     BLBL.BLBL_APTC_DLNQ_DT_NVL
FROM 
     tempdb..{DriverTable} TmpInvc,
     {FacetsOwner}.CMC_BLBL_BILL_SUMM BLBL
WHERE
     TmpInvc.BILL_INVC_ID = BLBL.BLBL_BLIV_ID_LAST
""".format(DriverTable=DriverTable, FacetsOwner=FacetsOwner)

df_CMC_BLBL_BILL_SUMM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Strip = df_CMC_BLBL_BILL_SUMM.select(
    F.col("BLEI_CK").alias("BLEI_CK"),
    F.col("BLBL_DUE_DT").alias("BLBL_DUE_DT"),
    F.col("BLBL_END_DT").alias("BLBL_END_DT"),
    F.col("BLBL_CREATE_DTM").alias("BLBL_CREATE_DTM"),
    F.col("BLBL_DLNQ_DT").alias("BLBL_DLNQ_DT"),
    F.col("BLBL_PREM_UPD_DTM").alias("BLBL_PREM_UPD_DTM"),
    F.col("BLBL_LST_ALLOC_DTM").alias("BLBL_LST_ALLOC_DTM"),
    F.col("BLBL_RECON_DTM").alias("BLBL_RECON_DTM"),
    F.col("BLBL_BILLED_AMT").alias("BLBL_BILLED_AMT"),
    F.col("BLBL_RCVD_AMT").alias("BLBL_RCVD_AMT"),
    strip_field(trim(F.col("BLBL_PAID_STS"))).alias("BLBL_PAID_STS"),
    F.col("BLBL_DAYS_BILLED").alias("BLBL_DAYS_BILLED"),
    F.col("BLBL_PRORATE_FCTR").alias("BLBL_PRORATE_FCTR"),
    strip_field(trim(F.col("BLBL_PRORATE_IND"))).alias("BLBL_PRORATE_IND"),
    strip_field(trim(F.col("BLBL_SPCL_BL_IND"))).alias("BLBL_SPCL_BL_IND"),
    strip_field(trim(F.col("BLBL_BLIV_ID_LAST"))).alias("BLBL_BLIV_ID_LAST"),
    strip_field(trim(F.col("BLBL_TYPE"))).alias("BLBL_TYPE"),
    F.col("BLBL_NO_ACTIVE_SB").alias("BLBL_NO_ACTIVE_SB"),
    strip_field(trim(F.col("BLBL_SUBSIDY_AMT_NVL"))).alias("BLBL_SUBSIDY_AMT_NVL"),
    strip_field(trim(F.col("BLBL_APTC_PAID_STS_NVL"))).alias("BLBL_APTC_PAID_STS_NVL"),
    strip_field(trim(F.col("BLBL_APTC_DLNQ_DT_NVL"))).alias("BLBL_APTC_DLNQ_DT_NVL")
)

df_BusinessRules = (
    df_Strip
    .withColumn("RowPassThru", F.lit("Y"))
    .withColumn(
        "svBillDueDt",
        F.date_format(
            F.to_timestamp(F.col("BLBL_DUE_DT"), "yyyy-MM-dd HH:mm:ss"),
            "yyyy-MM-dd"
        )
    )
)

df_BusinessRulesKey = (
    df_BusinessRules
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.col("RowPassThru"))
    .withColumn("FIRST_RECYC_DT", F.lit(CurrRunDt))
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
    .withColumn(
        "PRI_KEY_STRING",
        F.concat(
            F.lit(SrcSysCd),
            F.lit(";"),
            F.col("BLEI_CK"),
            F.lit(";"),
            F.col("svBillDueDt")
        )
    )
    .withColumn("BILL_ENTY_UNIQ_KEY", F.col("BLEI_CK"))
    .withColumn("BILL_DUE_DT_SK", F.col("svBillDueDt"))
    .withColumn("BILL_ENTY", F.col("BLEI_CK"))
    .withColumn("LAST_BILL_INVC", F.col("BLBL_BLIV_ID_LAST"))
    .withColumn(
        "BILL_SUM_BILL_TYP_CD",
        F.when(F.length(F.col("BLBL_TYPE")) == 0, F.lit("NA")).otherwise(F.col("BLBL_TYPE"))
    )
    .withColumn(
        "BILL_SUM_BILL_PD_STTUS_CD",
        F.when(F.length(F.col("BLBL_PAID_STS")) == 0, F.lit("NA")).otherwise(F.col("BLBL_PAID_STS"))
    )
    .withColumn(
        "BILL_SUM_SPCL_BILL_CD",
        F.when(F.length(F.col("BLBL_SPCL_BL_IND")) == 0, F.lit("NA")).otherwise(F.col("BLBL_SPCL_BL_IND"))
    )
    .withColumn(
        "PRORT_IN",
        F.when(F.length(F.col("BLBL_PRORATE_IND")) == 0, F.lit("N")).otherwise(F.col("BLBL_PRORATE_IND"))
    )
    .withColumn(
        "CRT_DTM",
        F.when(
            F.length(F.col("BLBL_CREATE_DTM")) == 0,
            F.lit("1753-01-01-00.00.00.000000")
        ).otherwise(
            F.date_format(
                F.to_timestamp(F.col("BLBL_CREATE_DTM"), "yyyy-MM-dd HH:mm:ss"),
                "yyyy-MM-dd-HH.mm.ss.SSSSSS"
            )
        )
    )
    .withColumn(
        "DLQNCY_DT_SK",
        F.when(
            F.length(F.col("BLBL_DLNQ_DT")) == 0,
            F.lit("1753-01-01")
        ).otherwise(
            F.date_format(F.to_timestamp(F.col("BLBL_DLNQ_DT"), "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd")
        )
    )
    .withColumn(
        "END_DT_SK",
        F.when(
            F.length(F.col("BLBL_END_DT")) == 0,
            F.lit("2199-12-31")
        ).otherwise(
            F.date_format(F.to_timestamp(F.col("BLBL_END_DT"), "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd")
        )
    )
    .withColumn(
        "LAST_ALLOC_DT_SK",
        F.when(
            F.length(F.col("BLBL_LST_ALLOC_DTM")) == 0,
            F.lit("1753-01-01")
        ).otherwise(
            F.date_format(F.to_timestamp(F.col("BLBL_LST_ALLOC_DTM"), "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd")
        )
    )
    .withColumn(
        "PRM_UPDT_DTM",
        F.when(
            F.length(F.col("BLBL_PREM_UPD_DTM")) == 0,
            F.lit("1753-01-01-00.00.00.000000")
        ).otherwise(
            F.date_format(
                F.to_timestamp(F.col("BLBL_PREM_UPD_DTM"), "yyyy-MM-dd HH:mm:ss"),
                "yyyy-MM-dd-HH.mm.ss.SSSSSS"
            )
        )
    )
    .withColumn(
        "RECON_DT_SK",
        F.when(
            F.length(F.col("BLBL_RECON_DTM")) == 0,
            F.lit("1753-01-01")
        ).otherwise(
            F.date_format(F.to_timestamp(F.col("BLBL_RECON_DTM"), "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd")
        )
    )
    .withColumn(
        "BILL_AMT",
        F.when(
            (F.length(F.col("BLBL_BILLED_AMT")) == 0),
            F.lit("0.00")
        ).otherwise(F.col("BLBL_BILLED_AMT"))
    )
    .withColumn(
        "RCVD_AMT",
        F.when(
            (F.length(F.col("BLBL_RCVD_AMT")) == 0),
            F.lit("0.00")
        ).otherwise(F.col("BLBL_RCVD_AMT"))
    )
    .withColumn(
        "ACTV_SUBS_NO",
        F.when(
            (F.length(F.col("BLBL_NO_ACTIVE_SB")) == 0),
            F.lit("0")
        ).otherwise(F.col("BLBL_NO_ACTIVE_SB"))
    )
    .withColumn(
        "BILL_DAYS_NO",
        F.when(
            (F.length(F.col("BLBL_DAYS_BILLED")) == 0),
            F.lit("0")
        ).otherwise(F.col("BLBL_DAYS_BILLED"))
    )
    .withColumn(
        "PRORT_FCTR",
        F.when(
            F.col("BLBL_PRORATE_IND") == "N",
            F.lit("1000000")
        ).otherwise(F.col("BLBL_PRORATE_FCTR"))
    )
    .withColumn(
        "LAST_BILL_INVC_ID",
        F.when(
            F.length(F.col("BLBL_TYPE")) == 0,
            F.lit("NA")
        ).otherwise(F.col("BLBL_BLIV_ID_LAST"))
    )
    .withColumn(
        "BLBL_SUBSIDY_AMT_NVL",
        F.when(
            F.col("BLBL_SUBSIDY_AMT_NVL").isNull() | (F.length(F.col("BLBL_SUBSIDY_AMT_NVL")) == 0),
            F.lit("0.00")
        ).otherwise(F.col("BLBL_SUBSIDY_AMT_NVL"))
    )
    .withColumn(
        "BLBL_APTC_PAID_STS_NVL",
        F.when(
            F.col("BLBL_APTC_PAID_STS_NVL").isNull() | (F.length(F.col("BLBL_APTC_PAID_STS_NVL")) == 0),
            F.lit("NA")
        ).otherwise(F.col("BLBL_APTC_PAID_STS_NVL"))
    )
    .withColumn(
        "BLBL_APTC_DLNQ_DT_NVL",
        F.when(
            F.col("BLBL_APTC_DLNQ_DT_NVL").isNull() | (F.length(F.col("BLBL_APTC_DLNQ_DT_NVL")) == 0),
            F.lit("1753-01-01")
        ).otherwise(F.substring(F.col("BLBL_APTC_DLNQ_DT_NVL"), 1, 10))
    )
)

df_KeyAllCol = df_BusinessRulesKey.select(
    F.col("BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
    F.col("BILL_DUE_DT_SK").alias("BILL_DUE_DT_SK"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("BILL_ENTY").alias("BILL_ENTY"),
    F.col("LAST_BILL_INVC").alias("LAST_BILL_INVC"),
    F.col("BILL_SUM_BILL_TYP_CD").alias("BILL_SUM_BILL_TYP_CD"),
    F.col("BILL_SUM_BILL_PD_STTUS_CD").alias("BILL_SUM_BILL_PD_STTUS_CD"),
    F.col("BILL_SUM_SPCL_BILL_CD").alias("BILL_SUM_SPCL_BILL_CD"),
    F.col("PRORT_IN").alias("PRORT_IN"),
    F.col("CRT_DTM").alias("CRT_DTM"),
    F.col("DLQNCY_DT_SK").alias("DLQNCY_DT_SK"),
    F.col("END_DT_SK").alias("END_DT_SK"),
    F.col("LAST_ALLOC_DT_SK").alias("LAST_ALLOC_DT_SK"),
    F.col("PRM_UPDT_DTM").alias("PRM_UPDT_DTM"),
    F.col("RECON_DT_SK").alias("RECON_DT_SK"),
    F.col("BILL_AMT").alias("BILL_AMT"),
    F.col("RCVD_AMT").alias("RCVD_AMT"),
    F.col("ACTV_SUBS_NO").alias("ACTV_SUBS_NO"),
    F.col("BILL_DAYS_NO").alias("BILL_DAYS_NO"),
    F.col("PRORT_FCTR").alias("PRORT_FCTR"),
    F.col("LAST_BILL_INVC_ID").alias("LAST_BILL_INVC_ID"),
    F.col("BLBL_SUBSIDY_AMT_NVL").alias("BLBL_SUBSIDY_AMT_NVL"),
    F.col("BLBL_APTC_PAID_STS_NVL").alias("BLBL_APTC_PAID_STS_NVL"),
    F.col("BLBL_APTC_DLNQ_DT_NVL").alias("BLBL_APTC_DLNQ_DT_NVL")
)

df_KeyTransform = df_BusinessRulesKey.select(
    F.col("BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
    F.col("BILL_DUE_DT_SK").alias("BILL_DUE_DT_SK"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK")
)

# MAGIC %run ../../../../shared_containers/PrimaryKey/BillSummNewPK
# COMMAND ----------

params = {
    "CurrRunCycle": CurrRunCycle,
    "CurrRunDt": CurrRunDt,
    "TmpOutFile": TmpOutFile,
    "DriverTable": DriverTable,
    "SrcSysCd": SrcSysCd,
    "SrcSysCdSk": SrcSysCdSk,
    "$FacetsDB": "",
    "$FacetsOwner": FacetsOwner,
    "$IDSOwner": IDSOwner
}
df_BillSummNewPK = BillSummNewPK(df_KeyAllCol, df_KeyTransform, params)

df_BillSumCrf = df_BillSummNewPK.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("BILL_SUM_SK"),
    F.col("BILL_ENTY_UNIQ_KEY"),
    F.rpad(F.col("BILL_DUE_DT_SK"), 10, " ").alias("BILL_DUE_DT_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("BILL_ENTY"),
    F.col("LAST_BILL_INVC"),
    F.rpad(F.col("BILL_SUM_BILL_TYP_CD"), 2, " ").alias("BILL_SUM_BILL_TYP_CD"),
    F.rpad(F.col("BILL_SUM_BILL_PD_STTUS_CD"), 2, " ").alias("BILL_SUM_BILL_PD_STTUS_CD"),
    F.rpad(F.col("BILL_SUM_SPCL_BILL_CD"), 2, " ").alias("BILL_SUM_SPCL_BILL_CD"),
    F.rpad(F.col("PRORT_IN"), 1, " ").alias("PRORT_IN"),
    F.col("CRT_DTM"),
    F.rpad(F.col("DLQNCY_DT_SK"), 10, " ").alias("DLQNCY_DT_SK"),
    F.rpad(F.col("END_DT_SK"), 10, " ").alias("END_DT_SK"),
    F.rpad(F.col("LAST_ALLOC_DT_SK"), 10, " ").alias("LAST_ALLOC_DT_SK"),
    F.col("PRM_UPDT_DTM"),
    F.rpad(F.col("RECON_DT_SK"), 10, " ").alias("RECON_DT_SK"),
    F.col("BILL_AMT"),
    F.col("RCVD_AMT"),
    F.col("ACTV_SUBS_NO"),
    F.col("BILL_DAYS_NO"),
    F.col("PRORT_FCTR"),
    F.col("LAST_BILL_INVC_ID"),
    F.col("BLBL_SUBSIDY_AMT_NVL"),
    F.rpad(F.col("BLBL_APTC_PAID_STS_NVL"), 1, " ").alias("BLBL_APTC_PAID_STS_NVL"),
    F.col("BLBL_APTC_DLNQ_DT_NVL")
)

write_files(
    df_BillSumCrf,
    f"{adls_path}/key/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)