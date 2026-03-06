# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC Copyright 2021, 2022 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Job Name:  CommonIdsLabRsltExtr
# MAGIC Called by: IdsAllLabRsltCntl
# MAGIC                    
# MAGIC 
# MAGIC Processing: Business rules are applied, K table logic is done through shared container.
# MAGIC                     
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:  Restore the source file before re-running the job
# MAGIC                     Previous Run Aborted: Restart, no other steps necessary     
# MAGIC 
# MAGIC Modifications:                        
# MAGIC \(9)\(9)\(9)\(9)\(9)Project/                                                                                                                             \(9)\(9)\(9)Code                   \(9)\(9)Date
# MAGIC Developer\(9)\(9)Date\(9)\(9)Altiris #\(9)\(9)Change Description                                                                                       \(9)\(9)Reviewer            \(9)\(9)Reviewed
# MAGIC ===============================================================================================================================================================================
# MAGIC Lakshmi Devagiri    \(9)\(9)2021-02-21\(9) US278143 \(9)Original Programming.                                              \(9)            \(9)\(9)Kalyan Neelam        \(9)\(9)2021-02-23          
# MAGIC Ken Bradmon\(9)\(9)2022-10-12\(9)us554528\(9)\(9)Update transformation for ORDER_DT_SK and 
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)SRC_SYS_EXTR_DT_SK in the "Business_Rules" stage.  They should\(9)Harsha Ravuri\(9)\(9)2022-11-02 
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)never have been hard-coded to use the value "1".
# MAGIC 
# MAGIC Deepika C                              2025-05-01              US 649263              Removed stage variable svTstRslt and directly mapped                                IntegrateDev1          Jeyaprasanna       2025-05-02
# MAGIC                                                                                                              RSLT_VAL_NUM to NUM_RSLT_VAL column in Business_Rules stage.


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame, functions as F, types as T, Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/LabRsltPK
# COMMAND ----------

InputFile = get_widget_value('InputFile','')
RowPassThru = get_widget_value('RowPassThru','')
CurrRunDt = get_widget_value('CurrRunDt','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
RunId = get_widget_value('RunId','')
CurrRunCycle = get_widget_value('CurrRunCycle','')

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

schema_LhoLabFinalExtract = T.StructType([
    T.StructField("RSLT_DT", T.StringType(), True),
    T.StructField("ACESION_NO", T.StringType(), True),
    T.StructField("LAB_CD", T.StringType(), True),
    T.StructField("PATN_LAST_NM", T.StringType(), True),
    T.StructField("PATN_FIRST_NM", T.StringType(), True),
    T.StructField("PATN_MID_NM", T.StringType(), True),
    T.StructField("DOB", T.StringType(), True),
    T.StructField("PATN_AGE", T.StringType(), True),
    T.StructField("GNDR", T.StringType(), True),
    T.StructField("VNDR_BILL_ID", T.StringType(), True),
    T.StructField("POL_NO", T.StringType(), True),
    T.StructField("ORDER_NTNL_PROV_ID", T.StringType(), True),
    T.StructField("ORDER_ACCT_NM", T.StringType(), True),
    T.StructField("RNDR_NTNL_PROV_ID", T.StringType(), True),
    T.StructField("ICD_VRSN", T.StringType(), True),
    T.StructField("DIAG_CD", T.StringType(), True),
    T.StructField("DIAG_CD_2", T.StringType(), True),
    T.StructField("DIAG_CD_3", T.StringType(), True),
    T.StructField("ORDER_NM", T.StringType(), True),
    T.StructField("LOCAL_ORDER_CD", T.StringType(), True),
    T.StructField("LOINC_CD", T.StringType(), True),
    T.StructField("RSLT_NM", T.StringType(), True),
    T.StructField("LOCAL_RSLT_CD", T.StringType(), True),
    T.StructField("RSLT_VAL_NUM", T.StringType(), True),
    T.StructField("RSLT_VAL_TEXT", T.StringType(), True),
    T.StructField("RSLT_UNIT", T.StringType(), True),
    T.StructField("ABNORM_FLAG", T.StringType(), True),
    T.StructField("MRN_ID", T.StringType(), True),
    T.StructField("SOURCE_ID", T.StringType(), True),
    T.StructField("MBR_UNIQ_KEY", T.StringType(), True),
    T.StructField("PROV_ID", T.StringType(), True)
])

df_LhoLabFinalExtract = (
    spark.read.csv(
        path=f"{adls_path}/verified/{InputFile}",
        schema=schema_LhoLabFinalExtract,
        sep=",",
        header=True,
        quote="\u0000"
    )
)

df_Xfm_Source_System_cd = df_LhoLabFinalExtract.select(
    F.col("RSLT_DT").alias("RSLT_DT"),
    F.col("ACESION_NO").alias("ACESION_NO"),
    F.col("LAB_CD").alias("LAB_CD"),
    F.col("PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("PATN_MID_NM").alias("PATN_MID_NM"),
    F.col("DOB").alias("DOB"),
    F.col("PATN_AGE").alias("PATN_AGE"),
    F.col("GNDR").alias("GNDR"),
    F.col("VNDR_BILL_ID").alias("VNDR_BILL_ID"),
    F.col("POL_NO").alias("POL_NO"),
    F.col("ORDER_NTNL_PROV_ID").alias("ORDER_NTNL_PROV_ID"),
    F.col("ORDER_ACCT_NM").alias("ORDER_ACCT_NM"),
    F.col("RNDR_NTNL_PROV_ID").alias("RNDR_NTNL_PROV_ID"),
    F.col("ICD_VRSN").alias("ICD_VRSN"),
    F.col("DIAG_CD").alias("DIAG_CD"),
    F.col("DIAG_CD_2").alias("DIAG_CD_2"),
    F.col("DIAG_CD_3").alias("DIAG_CD_3"),
    F.col("ORDER_NM").alias("ORDER_NM"),
    F.col("LOCAL_ORDER_CD").alias("LOCAL_ORDER_CD"),
    F.col("LOINC_CD").alias("LOINC_CD"),
    F.col("RSLT_NM").alias("RSLT_NM"),
    F.col("LOCAL_RSLT_CD").alias("LOCAL_RSLT_CD"),
    F.col("RSLT_VAL_NUM").alias("RSLT_VAL_NUM"),
    F.col("RSLT_VAL_TEXT").alias("RSLT_VAL_TEXT"),
    F.col("RSLT_UNIT").alias("RSLT_UNIT"),
    F.col("ABNORM_FLAG").alias("ABNORM_FLAG"),
    F.col("MRN_ID").alias("MRN_ID"),
    F.col("SOURCE_ID").alias("SOURCE_ID"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("PROV_ID").alias("PROV_ID")
)

df_Business_Rules_basesv = (
    df_Xfm_Source_System_cd
    .withColumn("svCPTCd", F.lit("NA"))
    .withColumn(
        "svLoincCd",
        F.when(
            F.col("LOINC_CD").isNull() | (F.length(F.trim(F.col("LOINC_CD"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("LOINC_CD"))))
    )
    .withColumn("svRsltId", F.lit("NA"))
    .withColumn(
        "svPatnEncntrId",
        F.when(
            F.col("ACESION_NO").isNull() | (F.length(F.trim(F.col("ACESION_NO"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("ACESION_NO"))))
    )
)

df_Business_Rules_basesv = df_Business_Rules_basesv.withColumn(
    "svKey",
    F.concat_ws(
        ":",
        F.when(F.col("MBR_UNIQ_KEY").isNull(), F.lit("")).otherwise(F.col("MBR_UNIQ_KEY")),
        F.when(F.col("RSLT_DT").isNull(), F.lit("")).otherwise(F.col("RSLT_DT")),
        F.when(F.col("svCPTCd").isNull(), F.lit("")).otherwise(F.col("svCPTCd")),
        F.lit("CPT4"),
        F.when(F.col("svLoincCd").isNull(), F.lit("")).otherwise(F.col("svLoincCd")),
        F.lit("NA"),
        F.lit("NA"),
        F.when(F.col("svRsltId").isNull(), F.lit("")).otherwise(F.col("svRsltId")),
        F.lit("NA"),
        F.when(F.col("svPatnEncntrId").isNull(), F.lit("")).otherwise(F.col("svPatnEncntrId")),
        F.when(F.col("SOURCE_ID").isNull(), F.lit("")).otherwise(F.col("SOURCE_ID"))
    )
)

df_Business_Rules_windowed = (
    df_Business_Rules_basesv
    .withColumn("row_id", F.monotonically_increasing_id())
)

w = Window.orderBy("row_id")

df_Business_Rules_windowed = (
    df_Business_Rules_windowed
    .withColumn("svPrevKey", F.lag("svKey", 1).over(w))
    .withColumn("svDupChk", F.when(F.col("svKey") == F.col("svPrevKey"), F.lit("Y")).otherwise(F.lit("N")))
    .withColumn("svMbrUniqKey", F.col("MBR_UNIQ_KEY"))
)

df_Business_Rules_final = df_Business_Rules_windowed.filter(F.col("svDupChk") == "N")

df_Business_Rules_AllCol = (
    df_Business_Rules_final
    .select(
        F.when(F.col("svMbrUniqKey").isNull(), F.lit("0")).otherwise(F.col("svMbrUniqKey")).alias("MBR_UNIQ_KEY"),
        F.when(
            F.col("RSLT_DT").isNull() | (F.length(F.trim(F.col("RSLT_DT"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("RSLT_DT")))).alias("SVC_DT_SK"),
        F.col("svCPTCd").alias("PROC_CD"),
        F.lit("NA").alias("PROC_CD_TYP_CD"),
        F.col("svLoincCd").alias("LOINC_CD"),
        F.when(
            F.col("DIAG_CD").isNull() | (F.length(F.trim(F.col("DIAG_CD"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("DIAG_CD")))).alias("DIAG_CD_1"),
        F.when(
            F.col("ICD_VRSN").isNull() | (F.length(F.trim(F.col("ICD_VRSN"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("ICD_VRSN")))).alias("DIAG_CD_TYP_CD_1"),
        F.col("svRsltId").alias("RSLT_ID"),
        F.when(
            F.col("LOCAL_ORDER_CD").isNull() | (F.length(F.trim(F.col("LOCAL_ORDER_CD"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("LOCAL_ORDER_CD")))).alias("ORDER_TST_ID"),
        F.col("svPatnEncntrId").alias("PATN_ENCNTR_ID"),
        F.col("SOURCE_ID").alias("SRC_SYS_CD"),
        F.lit("0").alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.col("svCPTCd").alias("SVRC_PROVIDED_CD_TRIMMED"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit(RowPassThru).alias("PASS_THRU_IN"),
        F.lit(CurrRunDt).alias("FIRST_RECYC_DT"),
        F.lit("0").alias("ERR_CT"),
        F.lit("0").alias("RECYCLE_CT"),
        F.concat_ws(
            ";",
            F.when(F.col("MBR_UNIQ_KEY").isNull(), F.lit("UNK"))
             .otherwise(
                F.when(F.col("MBR_UNIQ_KEY").isNull(), F.lit("")).otherwise(F.col("MBR_UNIQ_KEY"))
             ),
            F.when(F.col("RSLT_DT").isNull(), F.lit("")).otherwise(F.col("RSLT_DT")),
            F.when(F.col("svCPTCd").isNull(), F.lit("")).otherwise(F.col("svCPTCd")),
            F.lit("CPT4"),
            F.when(F.col("svLoincCd").isNull(), F.lit("")).otherwise(F.col("svLoincCd")),
            F.lit("NA"),
            F.lit("NA"),
            F.when(F.col("svRsltId").isNull(), F.lit("")).otherwise(F.col("svRsltId")),
            F.lit("NA"),
            F.when(F.col("svPatnEncntrId").isNull(), F.lit("")).otherwise(F.col("svPatnEncntrId")),
            F.when(F.col("SOURCE_ID").isNull(), F.lit("")).otherwise(F.col("SOURCE_ID"))
        ).alias("PRI_KEY_STRING"),
        F.when(
            F.col("DIAG_CD_2").isNull() | (F.length(F.trim(F.col("DIAG_CD_2"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("DIAG_CD_2")))).alias("DIAG_CD_2"),
        F.when(
            F.col("DIAG_CD_3").isNull() | (F.length(F.trim(F.col("DIAG_CD_3"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("DIAG_CD_3")))).alias("DIAG_CD_3"),
        F.when(
            F.col("PROV_ID").isNull() | (F.length(F.trim(F.col("PROV_ID"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("PROV_ID")))).alias("SVRC_PROV_ID"),
        F.when(
            F.col("MBR_UNIQ_KEY").isNull(),
            F.lit("NA")
        ).otherwise(F.trim(F.col("ABNORM_FLAG"))).alias("NORM_RSLT_IN"),
        F.when(
            F.col("RSLT_DT").isNull(),
            F.lit("1753-01-01")
        ).otherwise(
            F.when(
                F.trim(F.col("RSLT_DT")) == F.lit("NA"),
                F.lit("1753-01-01")
            ).otherwise(F.col("RSLT_DT"))
        ).alias("ORDER_DT_SK"),
        F.lit(CurrRunDt).alias("SRC_SYS_EXTR_DT_SK"),
        F.when(
            (F.col("RSLT_VAL_NUM").cast("double").isNull()) | (F.col("RSLT_VAL_NUM").cast("double") == 0.0),
            F.lit(None)
        ).otherwise(F.col("RSLT_VAL_NUM")).alias("NUM_RSLT_VAL"),
        F.lit("").alias("RSLT_NORM_HI_VAL"),
        F.lit("").alias("RSLT_NORM_LOW_VAL"),
        F.when(
            F.col("ORDER_NM").isNull() | (F.length(F.trim(F.col("ORDER_NM"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("ORDER_NM")))).alias("ORDER_TST_NM"),
        F.when(
            F.col("RSLT_NM").isNull() | (F.length(F.trim(F.col("RSLT_NM"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("RSLT_NM")))).alias("RSLT_DESC"),
        F.lit("NA").alias("RSLT_LONG_DESC_1"),
        F.lit("NA").alias("RSLT_LONG_DESC_2"),
        F.when(
            F.col("RSLT_UNIT").isNull(),
            F.lit("NA")
        ).otherwise(F.trim(F.col("RSLT_UNIT"))).alias("RSLT_MESR_UNIT_DESC"),
        F.lit("NA").alias("RSLT_RNG_DESC"),
        F.lit("NA").alias("SPCMN_ID"),
        F.when(
            F.col("ORDER_NTNL_PROV_ID").isNull() | (F.length(F.trim(F.col("ORDER_NTNL_PROV_ID"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("ORDER_NTNL_PROV_ID")))).alias("SRC_SYS_ORDER_PROV_ID"),
        F.col("svCPTCd").alias("SRC_SYS_PROC_CD_TX"),
        F.trim(F.col("RSLT_VAL_TEXT")).alias("TX_RSLT_VAL"),
        F.lit("FACETS").alias("ORDER_PROV_SRC_SYS_CD")
    )
)

df_Business_Rules_Transform = (
    df_Business_Rules_final
    .select(
        F.when(F.col("svMbrUniqKey").isNull(), F.lit("0")).otherwise(F.col("svMbrUniqKey")).alias("MBR_UNIQ_KEY"),
        F.when(
            F.col("RSLT_DT").isNull() | (F.length(F.trim(F.col("RSLT_DT"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("RSLT_DT")))).alias("SVC_DT_SK"),
        F.col("svCPTCd").alias("PROC_CD"),
        F.lit("NA").alias("PROC_CD_TYP_CD"),
        F.col("svLoincCd").alias("LOINC_CD"),
        F.when(
            F.col("DIAG_CD").isNull() | (F.length(F.trim(F.col("DIAG_CD"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("DIAG_CD")))).alias("DIAG_CD_1"),
        F.when(
            F.col("ICD_VRSN").isNull() | (F.length(F.trim(F.col("ICD_VRSN"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("ICD_VRSN")))).alias("DIAG_CD_TYP_CD_1"),
        F.col("svRsltId").alias("RSLT_ID"),
        F.when(
            F.col("LOCAL_ORDER_CD").isNull() | (F.length(F.trim(F.col("LOCAL_ORDER_CD"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("LOCAL_ORDER_CD")))).alias("ORDER_TST_ID"),
        F.col("svPatnEncntrId").alias("PATN_ENCNTR_ID"),
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK")
    )
)

params_LabRsltPKC114 = {
    "IDSOwner": IDSOwner,
    "CurrRunCycle": CurrRunCycle,
    "RowPassThru": RowPassThru,
    "CurrRunDt": CurrRunDt,
    "RunId": RunId,
    "SrcSysCd": SrcSysCd
}

df_LabRsltPKC114 = LabRsltPK(df_Business_Rules_Transform, df_Business_Rules_AllCol, params_LabRsltPKC114)

df_LhoLabRslt = df_LabRsltPKC114.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "LAB_RSLT_SK",
    "MBR_UNIQ_KEY",
    "SVC_DT_SK",
    "PROC_CD",
    "PROC_CD_TYP_CD",
    "LOINC_CD",
    "DIAG_CD_1",
    "DIAG_CD_TYP_CD_1",
    "RSLT_ID",
    "ORDER_TST_ID",
    "PATN_ENCNTR_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "DIAG_CD_2",
    "DIAG_CD_3",
    "SRVC_PROV_ID",
    "NORM_RSLT_IN",
    "ORDER_DT_SK",
    "SRC_SYS_EXTR_DT_SK",
    "NUM_RSLT_VAL",
    "RSLT_NORM_HI_VAL",
    "RSLT_NORM_LOW_VAL",
    "ORDER_TST_NM",
    "RSLT_DESC",
    "RSLT_LONG_DESC_1",
    "RSLT_LONG_DESC_2",
    "RSLT_MESR_UNIT_DESC",
    "RSLT_RNG_DESC",
    "SPCMN_ID",
    "SRC_SYS_ORDER_PROV_ID",
    "SRC_SYS_PROC_CD_TX",
    "TX_RSLT_VAL",
    "ORDER_PROV_SRC_SYS_CD"
).withColumn(
    "INSRT_UPDT_CD",
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ")
).withColumn(
    "DISCARD_IN",
    F.rpad(F.col("DISCARD_IN"), 1, " ")
).withColumn(
    "PASS_THRU_IN",
    F.rpad(F.col("PASS_THRU_IN"), 1, " ")
).withColumn(
    "SVC_DT_SK",
    F.rpad(F.col("SVC_DT_SK"), 10, " ")
).withColumn(
    "NORM_RSLT_IN",
    F.rpad(F.col("NORM_RSLT_IN"), 1, " ")
).withColumn(
    "ORDER_DT_SK",
    F.rpad(F.col("ORDER_DT_SK"), 10, " ")
).withColumn(
    "SRC_SYS_EXTR_DT_SK",
    F.rpad(F.col("SRC_SYS_EXTR_DT_SK"), 10, " ")
)

write_files(
    df_LhoLabRslt,
    f"{adls_path}/key/LabRsltExtr.LabRslt.dat.{RunId}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)