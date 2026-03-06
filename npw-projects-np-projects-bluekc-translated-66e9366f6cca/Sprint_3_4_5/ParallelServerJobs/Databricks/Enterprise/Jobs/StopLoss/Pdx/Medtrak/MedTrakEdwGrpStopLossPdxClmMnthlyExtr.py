# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2008 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     MedTrakEdwGrpStopLossPdxClmWeeklyCntl
# MAGIC 
# MAGIC PROCESSING : Extract Rx Claim from MedTrak- WeeklyRxControlFileTotals to Load Edw target table GRP_STOP_LOSS_PDX_CLM_RCVD_MNTHLY_SUM
# MAGIC 
# MAGIC Modifications:                        
# MAGIC  Developer                          Date               Project/Altiris #                      Change Description                                                          Development Project     Code Reviewer     Date Reviewed
# MAGIC ---------------------------------------    -------------------    -----------------------------------------     -------------------------------------------------------------------------------------    ----------------------------------    -------------------------    -------------------------   
# MAGIC Kaushik Kapoor                  2017-04-02     5828- Stop Loss-MedTrak     Original Development                                                       EnterpriseDev2              Kalyan Neelam     2018-04-20
# MAGIC Kaushik Kapoor                  2018-09-10     5828                                      Fixed the Grand Total Logic                                             EnterpriseDev2              Hugh Sisson         2018-09-19

# MAGIC MedTrak Edw Stop Loss PDX Claim Extr
# MAGIC The file is PIPE delimited.and received from MedTrak South Carolina for the process of PDX Claim load to EDW
# MAGIC get the field CRT_RUN_CYC_EXCTN_SK from 
# MAGIC EDW GRP_STOP_LOSS_PDX_CLM_RCVD_MNTHLY_SUM
# MAGIC If CovNo or GrpBase is blank then Fill it with Previous Value
# MAGIC Aggregate amount field based on below fields to Generate Group Total Record
# MAGIC 1) FILE_DT_SK
# MAGIC 2) SRC_SYS_CD
# MAGIC 3) GRP_ID
# MAGIC The load  file is used by the MedTrakEdwStopLossPdxClmWeeklyLoad job to load EDW DB GRP_STOP_LOSS_PDX_CLM_RCVD_MNTHLY_SUM Table
# MAGIC Get GRP_ID from P_PBM_GRP_XREF table
# MAGIC set 'GROUP TOTAL' for RCRD_TYP_NM
# MAGIC set 'GROUP TOTAL' for RCRD_TYP_NM
# MAGIC Null Handling GRP_ID
# MAGIC Aggregate amount field for 
# MAGIC GRP_ID ='UNK' based on FILE_DT_SK, GRP_ID fields to Generate Group Total Record
# MAGIC Remove Dups based on
# MAGIC 1) FILE_DT_SK
# MAGIC 2) RCRD_TYP_NM  
# MAGIC 3) SRC_SYS_GRP_ID
# MAGIC Remove Dups based on PBM_GRP_ID
# MAGIC Remove Dups based on
# MAGIC 1) FILE_DT_SK
# MAGIC 2) GRP_ID
# MAGIC Remove Dups based on
# MAGIC 1) FILE_DT_SK
# MAGIC 2) GRP_ID
# MAGIC Remove Dups based on
# MAGIC 1) FILE_DT_SK
# MAGIC 2) GRP_ID
# MAGIC Remove Dups based on
# MAGIC 1) FILE_DT_SK
# MAGIC 2) GRP_ID
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType
)
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
RunCycleDt = get_widget_value('RunCycleDt','')
InFileName = get_widget_value('InFileName','')

# 1) PpbmGrpXref (DB2ConnectorPX - IDS)
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_ids = (
    f"SELECT DISTINCT PBM_GRP_ID, GRP_ID "
    f"FROM {IDSOwner}.P_PBM_GRP_XREF "
    f"WHERE SRC_SYS_CD = 'MEDTRAK' "
    f"AND '{RunCycleDt}' BETWEEN CAST(EFF_DT AS DATE) AND CAST(TERM_DT AS DATE)"
)
df_PpbmGrpXref = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_ids)
    .load()
)

# 2) RmvDupe_PbmGrpXref (PxRemDup)
df_rmvdPbmGrpXref = dedup_sort(
    df_PpbmGrpXref,
    partition_cols=["PBM_GRP_ID"],
    sort_cols=[("PBM_GRP_ID","A")]
).select(
    F.col("PBM_GRP_ID"),
    F.col("GRP_ID")
)

# 3) MedTrakPdxClm (DB2ConnectorPX - EDW)
jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)
extract_query_edw = (
    f"SELECT DISTINCT FILE_DT_SK, SRC_SYS_GRP_ID, RCRD_TYP_NM, CRT_RUN_CYC_EXCTN_DT_SK "
    f"FROM {EDWOwner}.GRP_STOP_LOSS_PDX_CLM_RCVD_MNTHLY_SUM "
    f"WHERE SRC_SYS_CD = 'MEDTRAK'"
)
df_MedTrakPdxClm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", extract_query_edw)
    .load()
)

# 4) RmvDup_PdxClm (PxRemDup)
df_rmvdPdxClm = dedup_sort(
    df_MedTrakPdxClm,
    partition_cols=["FILE_DT_SK","RCRD_TYP_NM","SRC_SYS_GRP_ID"],
    sort_cols=[("FILE_DT_SK","A"),("RCRD_TYP_NM","A"),("SRC_SYS_GRP_ID","A")]
).select(
    F.col("FILE_DT_SK"),
    F.col("SRC_SYS_GRP_ID"),
    F.col("RCRD_TYP_NM"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK")
)

# 5) MedTrakPDXFile (PxSequentialFile) - Reading a pipe-delimited file with header
schema_MedTrakPDXFile = StructType([
    StructField("FILE_DT_SK", StringType(), True),
    StructField("COV_NO", StringType(), True),
    StructField("SC_GRP_BASE", StringType(), True),
    StructField("TOT_RCRD_CT", IntegerType(), True),
    StructField("TOT_INGR_CST_AMT", DecimalType(38,10), True),
    StructField("TOT_DRUG_ADM_FEE_AMT", DecimalType(38,10), True),
    StructField("TOT_COPAY_AMT", DecimalType(38,10), True),
    StructField("TOT_PD_AMT", DecimalType(38,10), True),
    StructField("TOT_QTY_CT", DecimalType(38,10), True),
])
df_MedTrakPDXFile = (
    spark.read
    .option("header", True)
    .option("delimiter", "|")
    .schema(schema_MedTrakPDXFile)
    .csv(f"{adls_path}/load/{InFileName}")
)

# 6) Xfm_fillCovNo_GrpBase (CTransformerStage) - replicate stage variable logic with window
df_temp_trans = df_MedTrakPDXFile.withColumn("row_id", F.monotonically_increasing_id())
w = Window.orderBy("row_id")
df_temp_trans = df_temp_trans \
    .withColumn("svCurrCovNo", F.col("COV_NO")) \
    .withColumn("svPrevCovNo", F.lag("svCurrCovNo").over(w)) \
    .withColumn("svCurrGrpBase", F.col("SC_GRP_BASE")) \
    .withColumn("svPrevGrpBase", F.lag("svCurrGrpBase").over(w)) \
    .withColumn(
        "svRecTypNm",
        F.when(
            (trim(F.col("COV_NO")) == "") & (trim(F.col("SC_GRP_BASE")) == ""),
            F.lit("GRAND TOTAL")
        ).otherwise(F.lit("Group RX"))
    )

df_Xfm_fillCovNo_GrpBase = df_temp_trans.select(
    F.when(
        (trim(F.col("COV_NO")) == "") & (trim(F.col("SC_GRP_BASE")) == ""),
        F.lit("999999")
    ).when(
        trim(F.col("COV_NO")) == "",
        F.col("svPrevCovNo")
    ).otherwise(F.col("COV_NO")).alias("COV_NO"),
    F.when(
        (trim(F.col("COV_NO")) == "") & (trim(F.col("SC_GRP_BASE")) == ""),
        F.lit("999999")
    ).when(
        trim(F.col("SC_GRP_BASE")) == "",
        F.col("svPrevGrpBase")
    ).otherwise(F.col("SC_GRP_BASE")).alias("SC_GRP_BASE"),
    F.col("FILE_DT_SK").alias("FILE_DT_SK"),
    F.when(
        (F.expr("LEFT(svRecTypNm, 5)") == F.lit("Group")) &
        (trim(F.col("SC_GRP_BASE")) == ""),
        F.col("svPrevGrpBase")
    ).when(
        (F.expr("LEFT(svRecTypNm, 5)") == F.lit("Group")) &
        (trim(F.col("SC_GRP_BASE")) != ""),
        F.col("svCurrGrpBase")
    ).when(
        (trim(F.col("SC_GRP_BASE")) == "") & (trim(F.col("COV_NO")) == ""),
        F.lit("999999")
    ).otherwise(F.lit("")).alias("SRC_SYS_GRP_ID"),
    F.when(
        (trim(F.col("COV_NO")) == "") & (trim(F.col("SC_GRP_BASE")) == ""),
        F.lit("GRAND TOTAL")
    ).otherwise(F.lit("Group RX")).alias("RCRD_TYP_NM"),
    F.lit("MEDTRAK").alias("SRC_SYS_CD"),
    F.col("TOT_RCRD_CT").alias("TOT_RCRD_CT"),
    F.col("TOT_INGR_CST_AMT").alias("TOT_INGR_CST_AMT"),
    F.col("TOT_DRUG_ADM_FEE_AMT").alias("TOT_DRUG_ADM_FEE_AMT"),
    F.col("TOT_COPAY_AMT").alias("TOT_COPAY_AMT"),
    F.col("TOT_PD_AMT").alias("TOT_PD_AMT"),
    F.col("TOT_QTY_CT").alias("TOT_QTY_CT"),
)

# 7) Lkp_MedTrak (PxLookup) - left joins to RmvDupe_PbmGrpXref and RmvDup_PdxClm
df_Lkp_MedTrak = (
    df_Xfm_fillCovNo_GrpBase.alias("Lnk_medtrak_FilledInp")
    .join(
        df_rmvdPbmGrpXref.alias("Lnk_GrpXref"),
        F.col("Lnk_medtrak_FilledInp.SC_GRP_BASE") == F.col("Lnk_GrpXref.PBM_GRP_ID"),
        "left"
    )
    .join(
        df_rmvdPdxClm.alias("Lnk_PdxClm"),
        (
            (F.col("Lnk_medtrak_FilledInp.FILE_DT_SK") == F.col("Lnk_PdxClm.FILE_DT_SK")) &
            (F.col("Lnk_medtrak_FilledInp.SRC_SYS_GRP_ID") == F.col("Lnk_PdxClm.SRC_SYS_GRP_ID")) &
            (F.col("Lnk_medtrak_FilledInp.RCRD_TYP_NM") == F.col("Lnk_PdxClm.RCRD_TYP_NM"))
        ),
        "left"
    )
).select(
    F.col("Lnk_medtrak_FilledInp.COV_NO").alias("COV_NO"),
    F.col("Lnk_medtrak_FilledInp.SC_GRP_BASE").alias("SC_GRP_BASE"),
    F.col("Lnk_medtrak_FilledInp.FILE_DT_SK").alias("FILE_DT_SK"),
    F.col("Lnk_medtrak_FilledInp.SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    F.col("Lnk_medtrak_FilledInp.RCRD_TYP_NM").alias("RCRD_TYP_NM"),
    F.col("Lnk_medtrak_FilledInp.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Lnk_GrpXref.GRP_ID").alias("GRP_ID"),
    F.col("Lnk_PdxClm.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("Lnk_medtrak_FilledInp.TOT_RCRD_CT").alias("TOT_RCRD_CT"),
    F.col("Lnk_medtrak_FilledInp.TOT_INGR_CST_AMT").alias("TOT_INGR_CST_AMT"),
    F.col("Lnk_medtrak_FilledInp.TOT_DRUG_ADM_FEE_AMT").alias("TOT_DRUG_ADM_FEE_AMT"),
    F.col("Lnk_medtrak_FilledInp.TOT_COPAY_AMT").alias("TOT_COPAY_AMT"),
    F.col("Lnk_medtrak_FilledInp.TOT_PD_AMT").alias("TOT_PD_AMT"),
    F.col("Lnk_medtrak_FilledInp.TOT_QTY_CT").alias("TOT_QTY_CT"),
)

# 8) Xfm_EnrichGrpID (CTransformerStage)
df_Xfm_EnrichGrpID = df_Lkp_MedTrak.select(
    F.col("COV_NO").alias("COV_NO"),
    F.col("SC_GRP_BASE").alias("SC_GRP_BASE"),
    F.col("FILE_DT_SK").alias("FILE_DT_SK"),
    F.col("SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    F.col("RCRD_TYP_NM").alias("RCRD_TYP_NM"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.when(
        trim(F.col("GRP_ID")) == "",
        F.lit("UNK")
    ).otherwise(F.col("GRP_ID")).alias("GRP_ID"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("TOT_RCRD_CT").alias("TOT_RCRD_CT"),
    F.col("TOT_INGR_CST_AMT").alias("TOT_INGR_CST_AMT"),
    F.col("TOT_DRUG_ADM_FEE_AMT").alias("TOT_DRUG_ADM_FEE_AMT"),
    F.col("TOT_COPAY_AMT").alias("TOT_COPAY_AMT"),
    F.col("TOT_PD_AMT").alias("TOT_PD_AMT"),
    F.col("TOT_QTY_CT").alias("TOT_QTY_CT"),
)

# 9) Flt_GrpID (PxFilter) => output link 0 => GRP_ID <> 'UNK' AND RCRD_TYP_NM <> 'GRAND TOTAL'
#                             output link 1 => GRP_ID = 'UNK' AND RCRD_TYP_NM <> 'GRAND TOTAL'
#                             output link 2 => RCRD_TYP_NM = 'GRAND TOTAL'
df_Flt_GrpID_0 = df_Xfm_EnrichGrpID.filter(
    (F.col("GRP_ID") != "UNK") & (F.col("RCRD_TYP_NM") != "GRAND TOTAL")
)
df_Flt_GrpID_1 = df_Xfm_EnrichGrpID.filter(
    (F.col("GRP_ID") == "UNK") & (F.col("RCRD_TYP_NM") != "GRAND TOTAL")
)
df_Flt_GrpID_2 = df_Xfm_EnrichGrpID.filter(
    (F.col("RCRD_TYP_NM") == "GRAND TOTAL")
)

# 10) Cpy_GrpIdNotUNK (PxCopy) => input df_Flt_GrpID_0
df_Cpy_GrpIdNotUNK = df_Flt_GrpID_0.select(
    F.col("COV_NO").alias("COV_NO"),
    F.col("SC_GRP_BASE").alias("SC_GRP_BASE"),
    F.col("FILE_DT_SK").alias("FILE_DT_SK"),
    F.col("SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    F.col("RCRD_TYP_NM").alias("RCRD_TYP_NM"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("TOT_RCRD_CT").alias("TOT_RCRD_CT"),
    F.col("TOT_INGR_CST_AMT").alias("TOT_INGR_CST_AMT"),
    F.col("TOT_DRUG_ADM_FEE_AMT").alias("TOT_DRUG_ADM_FEE_AMT"),
    F.col("TOT_COPAY_AMT").alias("TOT_COPAY_AMT"),
    F.col("TOT_PD_AMT").alias("TOT_PD_AMT"),
    F.col("TOT_QTY_CT").alias("TOT_QTY_CT"),
)

#  Cpy_GrpIdNotUNK => multiple outputs:
#     "AggGrpIdNotUNK" -> used by "AggAmt_GrpIdNotUNK"
#     "JoinGrpIdNotUNK" -> used by "Funnel_All"
#     "Lnk_getFirstRow" -> used by "Sort_MaxGrpBase_NotGrpID"
df_Cpy_GrpIdNotUNK_Agg = df_Cpy_GrpIdNotUNK
df_Cpy_GrpIdNotUNK_Join = df_Cpy_GrpIdNotUNK
df_Cpy_GrpIdNotUNK_FirstRow = df_Cpy_GrpIdNotUNK.select(
    F.col("COV_NO"),
    F.col("SC_GRP_BASE"),
    F.col("FILE_DT_SK"),
    F.col("SRC_SYS_GRP_ID"),
    F.col("SRC_SYS_CD"),
    F.col("GRP_ID"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK"),
)

# 11) Cpy_GrpIdUNK (PxCopy) => input df_Flt_GrpID_1
df_Cpy_GrpIdUNK = df_Flt_GrpID_1.select(
    F.col("COV_NO").alias("COV_NO"),
    F.col("SC_GRP_BASE").alias("SC_GRP_BASE"),
    F.col("FILE_DT_SK").alias("FILE_DT_SK"),
    F.col("SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    F.col("RCRD_TYP_NM").alias("RCRD_TYP_NM"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("TOT_RCRD_CT").alias("TOT_RCRD_CT"),
    F.col("TOT_INGR_CST_AMT").alias("TOT_INGR_CST_AMT"),
    F.col("TOT_DRUG_ADM_FEE_AMT").alias("TOT_DRUG_ADM_FEE_AMT"),
    F.col("TOT_COPAY_AMT").alias("TOT_COPAY_AMT"),
    F.col("TOT_PD_AMT").alias("TOT_PD_AMT"),
    F.col("TOT_QTY_CT").alias("TOT_QTY_CT"),
)
#   "AggGrpIdUNK" => used by "AggAmts"
#   "JoinGrpIdUNK" => used by "Funnel_All"
#   "Lnk_getFirstRow" => used by "Sort_MaxGrpBase"
df_Cpy_GrpIdUNK_Agg = df_Cpy_GrpIdUNK
df_Cpy_GrpIdUNK_Join = df_Cpy_GrpIdUNK
df_Cpy_GrpIdUNK_FirstRow = df_Cpy_GrpIdUNK.select(
    F.col("COV_NO"),
    F.col("FILE_DT_SK"),
    F.col("SC_GRP_BASE"),
    F.col("SRC_SYS_GRP_ID"),
    F.col("SRC_SYS_CD"),
    F.col("GRP_ID"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK"),
)

# 12) Sort_MaxGrpBase (PxSort) => sorts df_Cpy_GrpIdUNK_FirstRow by FILE_DT_SK, GRP_ID, SC_GRP_BASE
df_Sort_MaxGrpBase = df_Cpy_GrpIdUNK_FirstRow.withColumn("row_sort", F.struct(
    F.col("FILE_DT_SK"),
    F.col("GRP_ID"),
    F.col("SC_GRP_BASE")
))
df_Sort_MaxGrpBase = df_Sort_MaxGrpBase.orderBy("row_sort")
df_Sort_MaxGrpBase = df_Sort_MaxGrpBase.select(
    F.col("COV_NO").alias("COV_NO"),
    F.col("FILE_DT_SK").alias("FILE_DT_SK"),
    F.col("SC_GRP_BASE").alias("SC_GRP_BASE"),
    F.col("SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
)

# 13) RmvDup_GrpID_UNK (PxRemDup) => partition_cols=["FILE_DT_SK","GRP_ID"], keep first
df_RmvDup_GrpID_UNK = dedup_sort(
    df_Sort_MaxGrpBase,
    partition_cols=["FILE_DT_SK","GRP_ID"],
    sort_cols=[("FILE_DT_SK","A"),("GRP_ID","A")]
).select(
    F.col("COV_NO"),
    F.col("FILE_DT_SK"),
    F.col("SC_GRP_BASE"),
    F.col("SRC_SYS_GRP_ID"),
    F.col("SRC_SYS_CD"),
    F.col("GRP_ID"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK")
)

# 14) Sort_MaxGrpBase_NotGrpID => sorts df_Cpy_GrpIdNotUNK_FirstRow
df_Sort_MaxGrpBase_NotGrpID = df_Cpy_GrpIdNotUNK_FirstRow.withColumn("row_sort", F.struct(
    F.col("FILE_DT_SK"),
    F.col("GRP_ID"),
    F.col("SC_GRP_BASE")
))
df_Sort_MaxGrpBase_NotGrpID = df_Sort_MaxGrpBase_NotGrpID.orderBy("row_sort")
df_Sort_MaxGrpBase_NotGrpID = df_Sort_MaxGrpBase_NotGrpID.select(
    F.col("COV_NO").alias("COV_NO"),
    F.col("FILE_DT_SK").alias("FILE_DT_SK"),
    F.col("SC_GRP_BASE").alias("SC_GRP_BASE"),
    F.col("SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
)

# 15) RmvDup_GrpID_NotUNK
df_RmvDup_GrpID_NotUNK = dedup_sort(
    df_Sort_MaxGrpBase_NotGrpID,
    partition_cols=["FILE_DT_SK","GRP_ID"],
    sort_cols=[("FILE_DT_SK","A"),("GRP_ID","A")]
).select(
    F.col("COV_NO"),
    F.col("FILE_DT_SK"),
    F.col("SC_GRP_BASE"),
    F.col("SRC_SYS_GRP_ID"),
    F.col("SRC_SYS_CD"),
    F.col("GRP_ID"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK")
)

# 16) AggAmts (PxAggregator) => input df_Cpy_GrpIdUNK_Agg => group by FILE_DT_SK, GRP_ID => sum TOT_RCRD_CT,...
df_AggAmts = df_Cpy_GrpIdUNK_Agg.groupBy("FILE_DT_SK","GRP_ID").agg(
    F.sum("TOT_RCRD_CT").alias("TOT_RCRD_CT"),
    F.sum("TOT_INGR_CST_AMT").alias("TOT_INGR_CST_AMT"),
    F.sum("TOT_DRUG_ADM_FEE_AMT").alias("TOT_DRUG_ADM_FEE_AMT"),
    F.sum("TOT_COPAY_AMT").alias("TOT_COPAY_AMT"),
    F.sum("TOT_PD_AMT").alias("TOT_PD_AMT"),
    F.sum("TOT_QTY_CT").alias("TOT_QTY_CT")
)

# 17) RmvDup_UnkGrpID => dedup on FILE_DT_SK, GRP_ID
df_RmvDup_UnkGrpID = dedup_sort(
    df_AggAmts,
    partition_cols=["FILE_DT_SK","GRP_ID"],
    sort_cols=[("FILE_DT_SK","A"),("GRP_ID","A")]
).select(
    F.col("FILE_DT_SK"),
    F.col("GRP_ID"),
    F.col("TOT_RCRD_CT"),
    F.col("TOT_INGR_CST_AMT"),
    F.col("TOT_DRUG_ADM_FEE_AMT"),
    F.col("TOT_COPAY_AMT"),
    F.col("TOT_PD_AMT"),
    F.col("TOT_QTY_CT"),
)

# 18) AddCol_RecTyeNm (CTransformerStage)
df_AddCol_RecTyeNm = df_RmvDup_UnkGrpID.select(
    F.col("FILE_DT_SK").alias("FILE_DT_SK"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.lit("GROUP TOTAL").alias("RCRD_TYP_NM"),
    F.col("TOT_RCRD_CT").alias("TOT_RCRD_CT"),
    F.col("TOT_INGR_CST_AMT").alias("TOT_INGR_CST_AMT"),
    F.col("TOT_DRUG_ADM_FEE_AMT").alias("TOT_DRUG_ADM_FEE_AMT"),
    F.col("TOT_COPAY_AMT").alias("TOT_COPAY_AMT"),
    F.col("TOT_PD_AMT").alias("TOT_PD_AMT"),
    F.col("TOT_QTY_CT").alias("TOT_QTY_CT"),
)

# 19) Lkp_GrpIdUNK => primary link: df_AddCol_RecTyeNm, lookup link: df_RmvDup_GrpID_UNK
df_Lkp_GrpIdUNK = df_AddCol_RecTyeNm.alias("LkpAggGrpIdUNK").join(
    df_RmvDup_GrpID_UNK.alias("LkpGrpIdUNK"),
    (
        (F.col("LkpAggGrpIdUNK.FILE_DT_SK") == F.col("LkpGrpIdUNK.FILE_DT_SK")) &
        (F.col("LkpAggGrpIdUNK.GRP_ID") == F.col("LkpGrpIdUNK.GRP_ID"))
    ),
    "left"
).select(
    F.col("LkpGrpIdUNK.COV_NO").alias("COV_NO"),
    F.col("LkpGrpIdUNK.SC_GRP_BASE").alias("SC_GRP_BASE"),
    F.col("LkpGrpIdUNK.FILE_DT_SK").alias("FILE_DT_SK"),
    F.col("LkpGrpIdUNK.SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    F.col("LkpAggGrpIdUNK.RCRD_TYP_NM").alias("RCRD_TYP_NM"),
    F.col("LkpGrpIdUNK.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("LkpGrpIdUNK.GRP_ID").alias("GRP_ID"),
    F.col("LkpGrpIdUNK.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("LkpAggGrpIdUNK.TOT_RCRD_CT").alias("TOT_RCRD_CT"),
    F.col("LkpAggGrpIdUNK.TOT_INGR_CST_AMT").alias("TOT_INGR_CST_AMT"),
    F.col("LkpAggGrpIdUNK.TOT_DRUG_ADM_FEE_AMT").alias("TOT_DRUG_ADM_FEE_AMT"),
    F.col("LkpAggGrpIdUNK.TOT_COPAY_AMT").alias("TOT_COPAY_AMT"),
    F.col("LkpAggGrpIdUNK.TOT_PD_AMT").alias("TOT_PD_AMT"),
    F.col("LkpAggGrpIdUNK.TOT_QTY_CT").alias("TOT_QTY_CT"),
)

# 20) AggAmt_GrpIdNotUNK => input df_Cpy_GrpIdNotUNK_Agg => group by FILE_DT_SK, GRP_ID => sum
df_AggAmt_GrpIdNotUNK = df_Cpy_GrpIdNotUNK_Agg.groupBy("FILE_DT_SK","GRP_ID").agg(
    F.sum("TOT_RCRD_CT").alias("TOT_RCRD_CT"),
    F.sum("TOT_INGR_CST_AMT").alias("TOT_INGR_CST_AMT"),
    F.sum("TOT_DRUG_ADM_FEE_AMT").alias("TOT_DRUG_ADM_FEE_AMT"),
    F.sum("TOT_COPAY_AMT").alias("TOT_COPAY_AMT"),
    F.sum("TOT_PD_AMT").alias("TOT_PD_AMT"),
    F.sum("TOT_QTY_CT").alias("TOT_QTY_CT"),
)

# 21) RmvDup_GrpID => dedup on FILE_DT_SK, GRP_ID
df_RmvDup_GrpID = dedup_sort(
    df_AggAmt_GrpIdNotUNK,
    partition_cols=["FILE_DT_SK","GRP_ID"],
    sort_cols=[("FILE_DT_SK","A"),("GRP_ID","A")]
).select(
    F.col("FILE_DT_SK"),
    F.col("GRP_ID"),
    F.col("TOT_RCRD_CT"),
    F.col("TOT_INGR_CST_AMT"),
    F.col("TOT_DRUG_ADM_FEE_AMT"),
    F.col("TOT_COPAY_AMT"),
    F.col("TOT_PD_AMT"),
    F.col("TOT_QTY_CT"),
)

# 22) Xfm_AddCol
df_Xfm_AddCol = df_RmvDup_GrpID.select(
    F.col("FILE_DT_SK").alias("FILE_DT_SK"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.lit("MEDTRAK").alias("SRC_SYS_CD"),
    F.lit("GROUP TOTAL").alias("RCRD_TYP_NM"),
    F.when(
        (F.col("TOT_RCRD_CT").isNull()) | (F.col("TOT_RCRD_CT") == 0),
        F.lit(0)
    ).otherwise(F.col("TOT_RCRD_CT")).alias("TOT_RCRD_CT"),
    F.when(
        (F.col("TOT_INGR_CST_AMT").isNull()) | (F.col("TOT_INGR_CST_AMT") == 0),
        F.lit(0)
    ).otherwise(F.col("TOT_INGR_CST_AMT")).alias("TOT_INGR_CST_AMT"),
    F.when(
        (F.col("TOT_DRUG_ADM_FEE_AMT").isNull()) | (F.col("TOT_DRUG_ADM_FEE_AMT") == 0),
        F.lit(0)
    ).otherwise(F.col("TOT_DRUG_ADM_FEE_AMT")).alias("TOT_DRUG_ADM_FEE_AMT"),
    F.when(
        (F.col("TOT_COPAY_AMT").isNull()) | (F.col("TOT_COPAY_AMT") == 0),
        F.lit(0)
    ).otherwise(F.col("TOT_COPAY_AMT")).alias("TOT_COPAY_AMT"),
    F.when(
        (F.col("TOT_PD_AMT").isNull()) | (F.col("TOT_PD_AMT") == 0),
        F.lit(0)
    ).otherwise(F.col("TOT_PD_AMT")).alias("TOT_PD_AMT"),
    F.when(
        (F.col("TOT_QTY_CT").isNull()) | (F.col("TOT_QTY_CT") == 0),
        F.lit(0)
    ).otherwise(F.col("TOT_QTY_CT")).alias("TOT_QTY_CT"),
)

# 23) Lkp_AggGrpIdNotUNK => primary link: df_Xfm_AddCol, lookup link: df_RmvDup_GrpID_NotUNK
df_Lkp_AggGrpIdNotUNK = df_Xfm_AddCol.alias("LkpAggGrpIdNotUNK").join(
    df_RmvDup_GrpID_NotUNK.alias("Lnk_MaxGrpBaseLkp"),
    (
        (F.col("LkpAggGrpIdNotUNK.FILE_DT_SK") == F.col("Lnk_MaxGrpBaseLkp.FILE_DT_SK")) &
        (F.col("LkpAggGrpIdNotUNK.GRP_ID") == F.col("Lnk_MaxGrpBaseLkp.GRP_ID"))
    ),
    "left"
).select(
    F.col("Lnk_MaxGrpBaseLkp.COV_NO").alias("COV_NO"),
    F.col("Lnk_MaxGrpBaseLkp.SC_GRP_BASE").alias("SC_GRP_BASE"),
    F.col("LkpAggGrpIdNotUNK.FILE_DT_SK").alias("FILE_DT_SK"),
    F.col("Lnk_MaxGrpBaseLkp.SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    F.col("LkpAggGrpIdNotUNK.RCRD_TYP_NM").alias("RCRD_TYP_NM"),
    F.col("LkpAggGrpIdNotUNK.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("LkpAggGrpIdNotUNK.GRP_ID").alias("GRP_ID"),
    F.col("Lnk_MaxGrpBaseLkp.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("LkpAggGrpIdNotUNK.TOT_RCRD_CT").alias("TOT_RCRD_CT"),
    F.col("LkpAggGrpIdNotUNK.TOT_INGR_CST_AMT").alias("TOT_INGR_CST_AMT"),
    F.col("LkpAggGrpIdNotUNK.TOT_DRUG_ADM_FEE_AMT").alias("TOT_DRUG_ADM_FEE_AMT"),
    F.col("LkpAggGrpIdNotUNK.TOT_COPAY_AMT").alias("TOT_COPAY_AMT"),
    F.col("LkpAggGrpIdNotUNK.TOT_PD_AMT").alias("TOT_PD_AMT"),
    F.col("LkpAggGrpIdNotUNK.TOT_QTY_CT").alias("TOT_QTY_CT"),
)

# 24) Funnel_All => combine:
#    a) df_Lkp_GrpIdUNK -> alias JoinAggGrpIdUNK
#    b) df_Lkp_AggGrpIdNotUNK -> alias JoinAggGrpIdNotUNK
#    c) df_Cpy_GrpIdNotUNK_Join -> alias JoinGrpIdNotUNK
#    d) df_Cpy_GrpIdUNK_Join -> alias JoinGrpIdUNK
#    e) df_Flt_GrpID_2 -> alias GrndTot
# We funnel them. We'll do a unionAll-like approach in Spark.
df_Funnel_JoinAggGrpIdUNK = df_Lkp_GrpIdUNK.select(
    F.col("COV_NO").alias("COV_NO"),
    F.col("SC_GRP_BASE").alias("SC_GRP_BASE"),
    F.col("FILE_DT_SK").alias("FILE_DT_SK"),
    F.col("SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    F.col("RCRD_TYP_NM").alias("RCRD_TYP_NM"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("TOT_RCRD_CT").alias("TOT_RCRD_CT"),
    F.col("TOT_INGR_CST_AMT").alias("TOT_INGR_CST_AMT"),
    F.col("TOT_DRUG_ADM_FEE_AMT").alias("TOT_DRUG_ADM_FEE_AMT"),
    F.col("TOT_COPAY_AMT").alias("TOT_COPAY_AMT"),
    F.col("TOT_PD_AMT").alias("TOT_PD_AMT"),
    F.col("TOT_QTY_CT").alias("TOT_QTY_CT"),
)
df_Funnel_JoinAggGrpIdNotUNK = df_Lkp_AggGrpIdNotUNK.select(
    F.col("COV_NO").alias("COV_NO"),
    F.col("SC_GRP_BASE").alias("SC_GRP_BASE"),
    F.col("FILE_DT_SK").alias("FILE_DT_SK"),
    F.col("SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    F.col("RCRD_TYP_NM").alias("RCRD_TYP_NM"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("TOT_RCRD_CT").alias("TOT_RCRD_CT"),
    F.col("TOT_INGR_CST_AMT").alias("TOT_INGR_CST_AMT"),
    F.col("TOT_DRUG_ADM_FEE_AMT").alias("TOT_DRUG_ADM_FEE_AMT"),
    F.col("TOT_COPAY_AMT").alias("TOT_COPAY_AMT"),
    F.col("TOT_PD_AMT").alias("TOT_PD_AMT"),
    F.col("TOT_QTY_CT").alias("TOT_QTY_CT"),
)
df_Funnel_JoinGrpIdNotUNK = df_Cpy_GrpIdNotUNK_Join.select(
    F.col("COV_NO").alias("COV_NO"),
    F.col("SC_GRP_BASE").alias("SC_GRP_BASE"),
    F.col("FILE_DT_SK").alias("FILE_DT_SK"),
    F.col("SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    F.col("RCRD_TYP_NM").alias("RCRD_TYP_NM"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("TOT_RCRD_CT").alias("TOT_RCRD_CT"),
    F.col("TOT_INGR_CST_AMT").alias("TOT_INGR_CST_AMT"),
    F.col("TOT_DRUG_ADM_FEE_AMT").alias("TOT_DRUG_ADM_FEE_AMT"),
    F.col("TOT_COPAY_AMT").alias("TOT_COPAY_AMT"),
    F.col("TOT_PD_AMT").alias("TOT_PD_AMT"),
    F.col("TOT_QTY_CT").alias("TOT_QTY_CT"),
)
df_Funnel_JoinGrpIdUNK = df_Cpy_GrpIdUNK_Join.select(
    F.col("COV_NO").alias("COV_NO"),
    F.col("SC_GRP_BASE").alias("SC_GRP_BASE"),
    F.col("FILE_DT_SK").alias("FILE_DT_SK"),
    F.col("SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    F.col("RCRD_TYP_NM").alias("RCRD_TYP_NM"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("TOT_RCRD_CT").alias("TOT_RCRD_CT"),
    F.col("TOT_INGR_CST_AMT").alias("TOT_INGR_CST_AMT"),
    F.col("TOT_DRUG_ADM_FEE_AMT").alias("TOT_DRUG_ADM_FEE_AMT"),
    F.col("TOT_COPAY_AMT").alias("TOT_COPAY_AMT"),
    F.col("TOT_PD_AMT").alias("TOT_PD_AMT"),
    F.col("TOT_QTY_CT").alias("TOT_QTY_CT"),
)
df_Funnel_GrndTot = df_Flt_GrpID_2.select(
    F.col("COV_NO").alias("COV_NO"),
    F.col("SC_GRP_BASE").alias("SC_GRP_BASE"),
    F.col("FILE_DT_SK").alias("FILE_DT_SK"),
    F.col("SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    F.col("RCRD_TYP_NM").alias("RCRD_TYP_NM"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("TOT_RCRD_CT").alias("TOT_RCRD_CT"),
    F.col("TOT_INGR_CST_AMT").alias("TOT_INGR_CST_AMT"),
    F.col("TOT_DRUG_ADM_FEE_AMT").alias("TOT_DRUG_ADM_FEE_AMT"),
    F.col("TOT_COPAY_AMT").alias("TOT_COPAY_AMT"),
    F.col("TOT_PD_AMT").alias("TOT_PD_AMT"),
    F.col("TOT_QTY_CT").alias("TOT_QTY_CT"),
)

df_Funnel_All = df_Funnel_JoinAggGrpIdUNK.unionByName(df_Funnel_JoinAggGrpIdNotUNK)\
    .unionByName(df_Funnel_JoinGrpIdNotUNK)\
    .unionByName(df_Funnel_JoinGrpIdUNK)\
    .unionByName(df_Funnel_GrndTot)

# 25) Xfm_BusinessLogic (CTransformerStage)
df_Xfm_BusinessLogic = df_Funnel_All.select(
    F.when(
        trim(F.col("FILE_DT_SK")) == "",
        F.lit("")
    ).otherwise(F.col("FILE_DT_SK")).alias("FILE_DT_SK"),
    F.when(
        trim(F.col("SRC_SYS_GRP_ID")) == "",
        F.lit("")
    ).otherwise(
        F.when(
            F.col("RCRD_TYP_NM") == F.lit("GRAND TOTAL"),
            F.lit("999999")
        ).otherwise(F.col("SRC_SYS_GRP_ID"))
    ).alias("SRC_SYS_GRP_ID"),
    F.when(
        trim(F.col("RCRD_TYP_NM")) == "",
        F.lit("")
    ).otherwise(F.col("RCRD_TYP_NM")).alias("RCRD_TYP_NM"),
    F.when(
        trim(F.col("SRC_SYS_CD")) == "",
        F.lit("")
    ).otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    F.when(
        trim(F.col("CRT_RUN_CYC_EXCTN_DT_SK")) == "",
        F.col("RunCycleDt")
    ).otherwise(F.col("CRT_RUN_CYC_EXCTN_DT_SK")).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.when(
        trim(F.col("RunCycleDt")) == "",
        F.lit("")
    ).otherwise(F.col("RunCycleDt")).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.when(
        F.col("RCRD_TYP_NM") == F.lit("GRAND TOTAL"),
        F.lit("")
    ).otherwise(
        F.when(
            trim(F.col("GRP_ID")) == "",
            F.lit("")
        ).otherwise(F.col("GRP_ID"))
    ).alias("GRP_ID"),
    F.when(
        F.expr("LEFT(RCRD_TYP_NM, 5)") == F.lit("GRAND"),
        F.lit("999999")
    ).otherwise(
        F.when(
            trim(F.col("GRP_ID")) == "",
            F.lit("")
        ).otherwise(F.col("GRP_ID"))
    ).alias("SRC_SYS_COV_NO"),
    F.when(
        (F.col("TOT_RCRD_CT").isNull()) | (F.col("TOT_RCRD_CT") == 0),
        F.lit(0)
    ).otherwise(F.col("TOT_RCRD_CT")).alias("RCVD_TOT_RCRD_CT"),
    F.when(
        (F.col("TOT_INGR_CST_AMT").isNull()) | (F.col("TOT_INGR_CST_AMT") == 0),
        F.lit(0)
    ).otherwise(F.col("TOT_INGR_CST_AMT")).alias("RCVD_TOT_INGR_CST_AMT"),
    F.when(
        (F.col("TOT_DRUG_ADM_FEE_AMT").isNull()) | (F.col("TOT_DRUG_ADM_FEE_AMT") == 0),
        F.lit(0)
    ).otherwise(F.col("TOT_DRUG_ADM_FEE_AMT")).alias("RCVD_TOT_DRUG_ADM_FEE_AMT"),
    F.when(
        (F.col("TOT_COPAY_AMT").isNull()) | (F.col("TOT_COPAY_AMT") == 0),
        F.lit(0)
    ).otherwise(F.col("TOT_COPAY_AMT")).alias("RCVD_TOT_COPAY_AMT"),
    F.when(
        (F.col("TOT_PD_AMT").isNull()) | (F.col("TOT_PD_AMT") == 0),
        F.lit(0)
    ).otherwise(F.col("TOT_PD_AMT")).alias("RCVD_TOT_PD_AMT"),
    F.when(
        (F.col("TOT_QTY_CT").isNull()) | (F.col("TOT_QTY_CT") == 0),
        F.lit(0)
    ).otherwise(F.col("TOT_QTY_CT")).alias("RCVD_TOT_QTY_CT"),
    F.lit(None).alias("CALC_TOT_RCRD_CT"),
    F.lit(None).alias("CALC_TOT_INGR_CST_AMT"),
    F.lit(None).alias("CALC_TOT_DRUG_ADM_FEE_AMT"),
    F.lit(None).alias("CALC_TOT_COPAY_AMT"),
    F.lit(None).alias("CALC_TOT_PD_AMT"),
    F.lit(None).alias("CALC_TOT_QTY_CT"),
)

# 26) GRP_STOP_LOSS_PDX_CLM_RCVD_MNTHLY_SUM (PxSequentialFile) - writing
final_cols = [
    "FILE_DT_SK",
    "SRC_SYS_GRP_ID",
    "RCRD_TYP_NM",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "GRP_ID",
    "SRC_SYS_COV_NO",
    "RCVD_TOT_RCRD_CT",
    "RCVD_TOT_INGR_CST_AMT",
    "RCVD_TOT_DRUG_ADM_FEE_AMT",
    "RCVD_TOT_COPAY_AMT",
    "RCVD_TOT_PD_AMT",
    "RCVD_TOT_QTY_CT",
    "CALC_TOT_RCRD_CT",
    "CALC_TOT_INGR_CST_AMT",
    "CALC_TOT_DRUG_ADM_FEE_AMT",
    "CALC_TOT_COPAY_AMT",
    "CALC_TOT_PD_AMT",
    "CALC_TOT_QTY_CT"
]

df_final = df_Xfm_BusinessLogic.select(final_cols)

# For each char or varchar column in the final schema, apply rpad if needed.
# The metadata says: CRT_RUN_CYC_EXCTN_DT_SK, LAST_UPDT_RUN_CYC_EXCTN_DT_SK are char(10).
df_final = df_final.withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK",
    rpad("CRT_RUN_CYC_EXCTN_DT_SK", 10, " ")
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    rpad("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", 10, " ")
)

# Write the final file
write_files(
    df_final,
    f"{adls_path}/load/MEDTRAK_RCVDAMT_GRP_STOP_LOSS_PDX_CLM_RCVD_MNTHLY_SUM.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)