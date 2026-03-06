# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_3 02/20/09 11:05:46 Batch  15027_39950 PROMOTE bckcetl ids20 dsadm bls for sa
# MAGIC ^1_3 02/20/09 10:48:45 Batch  15027_38927 INIT bckcett testIDS dsadm bls for sa
# MAGIC ^1_2 02/19/09 15:48:36 Batch  15026_56922 PROMOTE bckcett testIDS u03651 steph for Sharon
# MAGIC ^1_2 02/19/09 15:43:11 Batch  15026_56617 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_1 01/13/09 08:41:23 Batch  14989_31287 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_1 01/30/08 05:00:10 Batch  14640_18019 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 10/31/07 14:11:20 Batch  14549_51083 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_2 10/31/07 12:55:15 Batch  14549_46521 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_2 10/31/07 12:32:28 Batch  14549_45152 INIT bckcett testIDS30 dsadm bls for on
# MAGIC ^1_1 10/26/07 08:07:18 Batch  14544_29241 PROMOTE bckcett testIDS30 u03651 steffy
# MAGIC ^1_1 10/26/07 07:46:17 Batch  14544_27982 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_6 05/12/06 15:03:19 Batch  14012_54207 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_5 05/11/06 16:20:33 Batch  14011_58841 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_4 05/11/06 12:36:55 Batch  14011_45419 PROMOTE bckcett devlIDS30 u10157 sa
# MAGIC ^1_4 05/11/06 12:31:29 Batch  14011_45092 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_1 05/02/06 13:04:24 Batch  14002_47066 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_2 01/26/06 07:54:36 Batch  13906_28483 INIT bckcetl ids20 dsadm Gina
# MAGIC ^1_1 01/24/06 14:49:28 Batch  13904_53375 PROMOTE bckcetl ids20 dsadm Gina Parr
# MAGIC ^1_1 01/24/06 14:47:17 Batch  13904_53242 INIT bckcett testIDS30 dsadm Gina Parr
# MAGIC ^1_2 01/24/06 14:36:07 Batch  13904_52574 INIT bckcett testIDS30 dsadm Gina Parr
# MAGIC ^1_1 01/24/06 14:21:30 Batch  13904_51695 INIT bckcett testIDS30 dsadm Gina Parr
# MAGIC ^1_2 12/23/05 11:39:15 Batch  13872_41979 PROMOTE bckcett testIDS30 u10913 Move Income Commission to test
# MAGIC ^1_2 12/23/05 11:12:01 Batch  13872_40337 INIT bckcett devlIDS30 u10913 Ollie Move Income/Commisions to test
# MAGIC ^1_1 12/23/05 10:39:46 Batch  13872_38400 INIT bckcett devlIDS30 u10913 Ollie Moving Income / Commission to Test
# MAGIC 
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     FctsComsnSchdExtr
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:    Pulls data from CMC_COSC_COMM_SCHD to a landing file for the IDS
# MAGIC       
# MAGIC 
# MAGIC INPUTS:	CMC_COSC_COMM_SCHD
# MAGIC   
# MAGIC 
# MAGIC HASH FILES:  hf_comsn_schd
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  STRIP.FIELD
# MAGIC                             FORMAT.DATE
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:  Output file is created with a temp. name.  File renamed in job control
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS:    Sequential file 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Date                 Developer                Change Description
# MAGIC ------------------      ----------------------------     --------------------------------------------------------------------------------------------------------
# MAGIC 2005-10-24      Suzanne Saylor         Original Programming.
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                       Change Description                                               Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -------------------------------------------------- ----------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              5/31/2007          3264                                      Added Balancing process to the overall                 devlIDS30              Steph Goddard             09/18/2007
# MAGIC                                                                                                                   job that takes a snapshot of the source data                        
# MAGIC 
# MAGIC 
# MAGIC Bhoomi Dasari                 09/12/2008       3567                                        Added new primay key contianer and SrcSysCdsk  devlIDS                    Steph Goddard            09/22/2008       
# MAGIC                                                                                                                  and SrcSysCd  
# MAGIC Ralph Tucker                  2012-08-15      4873 - Commissions Reporting  Added new field; updated to latest standards          IntegrateNewDevl          Kalyan Neelam           2012-11-06
# MAGIC Prabhu ES                       2022-03-01      S2S Remediation                      MSSQL connection parameters added                    IntegrateDev5

# MAGIC Hash file hf_comsn_schd_allcol cleared
# MAGIC Strip Carriage Return, Line Feed, and  Tab
# MAGIC 
# MAGIC Use STRIP.FIELD function on each character column coming in
# MAGIC Extract Facets Member Eligibility Data
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
from pyspark.sql.functions import col, lit, trim, length, substring, when, regexp_replace, rpad, concat
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------

facets_secret_name = get_widget_value('facets_secret_name','')
ids_secret_name = get_widget_value('ids_secret_name','')
FacetsOwner = get_widget_value('FacetsOwner','')
IDSOwner = get_widget_value('IDSOwner','')
TmpOutFile = get_widget_value('TmpOutFile','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')

# MAGIC %run ../../../../shared_containers/PrimaryKey/ComsnSchdPK
# COMMAND ----------

jdbc_url, jdbc_props = get_db_config(facets_secret_name)
df_CMC_COSC_COMM_SCHD = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT COSC_ID, COSC_CALC_METH, COSC_DESC FROM {FacetsOwner}.CMC_COSC_COMM_SCHD")
    .load()
)

df_StripField = df_CMC_COSC_COMM_SCHD.select(
    trim(col("COSC_ID")).alias("COSC_ID"),
    trim(col("COSC_CALC_METH")).alias("COSC_CALC_METH"),
    trim(col("COSC_DESC")).alias("COSC_DESC")
)

df_BusinessRules = df_StripField.withColumn("RowPassThru", lit("Y")) \
    .withColumn("svLenCoscId", length(col("COSC_ID"))) \
    .withColumn("svLstCharCoscId", substring(col("COSC_ID"), col("svLenCoscId"), 1)) \
    .withColumn("svLst2CharsCoscId", substring(col("COSC_ID"), col("svLenCoscId") - lit(1), 2)) \
    .withColumn(
        "COMSN_MTHDLGY_TYP_CD",
        when(col("COSC_ID") == lit("NET"), lit("NET"))
        .when(
            (col("svLstCharCoscId").isin("S", "D")) | (col("svLst2CharsCoscId") == lit("SL")),
            lit("SLIDESCALE")
        )
        .when(col("svLstCharCoscId") == lit("%"), lit("PCT"))
        .when(col("svLstCharCoscId") == lit("$"), lit("FLATDLR"))
        .when(col("svLstCharCoscId") == lit("P"), lit("PCPM"))
        .otherwise(lit("SCHD"))
    )

df_BusinessRulesAllCol = df_BusinessRules.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("COSC_ID").alias("COMSN_SCHD_ID"),
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit("I").alias("INSRT_UPDT_CD"),
    lit("N").alias("DISCARD_IN"),
    col("RowPassThru").alias("PASS_THRU_IN"),
    lit(CurrDate).alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    lit("FACETS").alias("SRC_SYS_CD"),
    concat(lit("FACETS"), lit(";"), col("COSC_ID")).alias("PRI_KEY_STRING"),
    col("COSC_CALC_METH").alias("COMSN_SCHD_CALC_METH_CD_SK"),
    col("COSC_DESC").alias("SCHD_DESC"),
    col("COMSN_MTHDLGY_TYP_CD").alias("COMSN_MTHDLGY_TYP_CD")
)

df_BusinessRulesTransform = df_BusinessRules.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("COSC_ID").alias("COMSN_SCHD_ID")
)

params = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "RunID": RunID,
    "CurrentDate": CurrDate,
    "IDSOwner": IDSOwner
}

df_ComsnSchdPK = ComsnSchdPK(df_BusinessRulesAllCol, df_BusinessRulesTransform, params)

df_IdsComsnSchdExtr = df_ComsnSchdPK.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "COMSN_SCHD_SK",
    "COMSN_SCHD_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "COMSN_SCHD_CALC_METH_CD_SK",
    "SCHD_DESC",
    "COMSN_MTHDLGY_TYP_CD"
).withColumn("INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " ")) \
 .withColumn("DISCARD_IN", rpad(col("DISCARD_IN"), 1, " ")) \
 .withColumn("PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " ")) \
 .withColumn("COMSN_SCHD_ID", rpad(col("COMSN_SCHD_ID"), 4, " ")) \
 .withColumn("COMSN_SCHD_CALC_METH_CD_SK", rpad(col("COMSN_SCHD_CALC_METH_CD_SK"), 1, " ")) \
 .withColumn("SCHD_DESC", rpad(col("SCHD_DESC"), 70, " "))

write_files(
    df_IdsComsnSchdExtr,
    f"{adls_path}/key/{TmpOutFile}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

df_Facets_Source = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT * FROM {FacetsOwner}.CMC_COSC_COMM_SCHD")
    .load()
)

df_Transform = df_Facets_Source.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    trim(regexp_replace(col("COSC_ID"), "[\\n\\r\\t]", "")).alias("COMSN_SCHD_ID")
)

df_Snapshot_File = df_Transform.select("SRC_SYS_CD_SK", "COMSN_SCHD_ID")

write_files(
    df_Snapshot_File,
    f"{adls_path}/load/B_COMSN_SCHD.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)