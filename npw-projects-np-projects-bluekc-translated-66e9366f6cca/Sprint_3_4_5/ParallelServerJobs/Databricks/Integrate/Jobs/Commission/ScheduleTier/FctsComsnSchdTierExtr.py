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
# MAGIC JOB NAME:     FctsComsnSchdTierExtr
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:    Pulls data from CMC_COSD_COSC_DUR and CMC_COST_COSC_TIER to a landing file for the IDS
# MAGIC       
# MAGIC 
# MAGIC INPUTS:	CMC_COSD_COSC_DUR and CMC_COST_COSC_TIER
# MAGIC   
# MAGIC 
# MAGIC HASH FILES:  hf_comsn_schd_tier
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
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              5/31/2007          3264                              Added Balancing process to the overall                 devlIDS30               Steph Goddard             09/18/2007
# MAGIC                                                                                                       job that takes a snapshot of the source data                        
# MAGIC 
# MAGIC 
# MAGIC Bhoomi Dasari                 09/12/2008       3567                             Added new primay key contianer and SrcSysCdsk  devlIDS                    Steph Goddard              09/22/2008   
# MAGIC                                                                                                       and SrcSysCd  
# MAGIC 
# MAGIC Prabhu ES                      2022-03-01         S2S Remediation          MSSQL connection parameters added                      IntegrateDev5

# MAGIC Hash file hf_comsn_schd_tier_allcol cleared
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
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/ComsnSchdTierPK
# COMMAND ----------

FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
TmpOutFile = get_widget_value('TmpOutFile','')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','')
SrcSyscdSk = get_widget_value('SrcSyscdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')

jdbc_url, jdbc_props = get_db_config(facets_secret_name)

df_CMC_COST_COSC_TIER = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT COSC_ID,COSD_EFF_DT,COSD_DUR_START,COST_FROM_AMT,COSC_CALC_METH,COST_THRU_AMT,COST_AMT,COST_PCT,COST_CK FROM {FacetsOwner}.CMC_COST_COSC_TIER TIER")
    .load()
)

df_Strip = df_CMC_COST_COSC_TIER.select(
    F.expr("trim(COSC_ID)").alias("COSC_ID"),
    col("COSD_EFF_DT").alias("COSD_EFF_DT"),
    col("COSD_DUR_START").alias("COSD_DUR_START"),
    F.expr("Oconv(COST_FROM_AMT,'MD2')").alias("COST_FROM_AMT"),
    F.expr("trim(COSC_CALC_METH)").alias("COSC_CALC_METH"),
    col("COST_THRU_AMT").alias("COST_THRU_AMT"),
    col("COST_AMT").alias("COST_AMT"),
    col("COST_PCT").alias("COST_PCT"),
    col("COST_CK").alias("COST_CK")
)

df_AllCol = df_Strip.select(
    lit(SrcSyscdSk).alias("SRC_SYS_CD_SK"),
    col("COSC_ID").alias("COMSN_SCHD_ID"),
    F.expr("FORMAT.DATE(COSD_EFF_DT,'SYBASE','TIMESTAMP','CCYY-MM-DD')").alias("DURATN_EFF_DT_SK"),
    col("COSD_DUR_START").alias("DURATN_STRT_PERD_NO"),
    col("COST_FROM_AMT").alias("PRM_FROM_THRS_HLD_AMT"),
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(lit("N"), 1, " ").alias("DISCARD_IN"),
    rpad(lit("Y"), 1, " ").alias("PASS_THRU_IN"),
    lit(CurrDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.expr("'FACETS' : ';' : COSC_ID : ';' : FORMAT.DATE(COSD_EFF_DT,'SYBASE','TIMESTAMP','CCYY-MM-DD') : ';' : COSD_DUR_START : ';' : COST_FROM_AMT").alias("PRI_KEY_STRING"),
    col("COSC_CALC_METH").alias("COMSN_SCHD_TIER_CALC_METH_CD_SK"),
    col("COST_THRU_AMT").alias("PRM_THRU_THRESOLD_AMT"),
    col("COST_AMT").alias("TIER_AMT"),
    col("COST_PCT").alias("TIER_PCT"),
    col("COST_CK").alias("TIER_UNIQ_KEY")
)

df_TransformBus = df_Strip.select(
    lit(SrcSyscdSk).alias("SRC_SYS_CD_SK"),
    col("COSC_ID").alias("COMSN_SCHD_ID"),
    F.expr("FORMAT.DATE(COSD_EFF_DT,'SYBASE','TIMESTAMP','CCYY-MM-DD')").alias("DURATN_EFF_DT_SK"),
    col("COSD_DUR_START").alias("DURATN_STRT_PERD_NO"),
    col("COST_FROM_AMT").alias("PRM_FROM_THRSHLD_AMT")
)

params = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "RunID": RunID,
    "CurrentDate": CurrDate,
    "IDSOwner": IDSOwner
}
df_Key = ComsnSchdTierPK(df_AllCol, df_TransformBus, params)

df_Key_final = (
    df_Key
    .withColumn("INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", rpad(col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " "))
    .withColumn("COMSN_SCHD_ID", rpad(col("COMSN_SCHD_ID"), 4, " "))
    .withColumn("DURATN_EFF_DT_SK", rpad(col("DURATN_EFF_DT_SK"), 10, " "))
    .withColumn("COMSN_SCHD_TIER_CALC_METH_CD_SK", rpad(col("COMSN_SCHD_TIER_CALC_METH_CD_SK"), 1, " "))
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
        "COMSN_SCHD_TIER_SK",
        "COMSN_SCHD_ID",
        "DURATN_EFF_DT_SK",
        "DURATN_STRT_PERD_NO",
        "PRM_FROM_THRS_HLD_AMT",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "COMSN_SCHD_SK",
        "COMSN_SCHD_TIER_CALC_METH_CD_SK",
        "PRM_THRU_THRESOLD_AMT",
        "TIER_AMT",
        "TIER_PCT",
        "TIER_UNIQ_KEY"
    )
)

write_files(
    df_Key_final,
    f"{adls_path}/key/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_Facets_Source = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT COSC_ID,COSD_EFF_DT,COSD_DUR_START,COST_FROM_AMT FROM {FacetsOwner}.CMC_COST_COSC_TIER")
    .load()
)

df_TransformSnap = df_Facets_Source.withColumn(
    "svDuratnEffDtSk",
    F.expr("FORMAT.DATE(COSD_EFF_DT,'SYBASE','TIMESTAMP','CCYY-MM-DD')")
).select(
    lit(SrcSyscdSk).alias("SRC_SYS_CD_SK"),
    F.expr("trim(Convert(CHAR(10) : CHAR(13) : CHAR(9), '', COSC_ID))").alias("COMSN_SCHD_ID"),
    col("svDuratnEffDtSk").alias("DURATN_EFF_DT_SK"),
    col("COSD_DUR_START").alias("DURATN_STRT_PERD_NO"),
    col("COST_FROM_AMT").alias("PRM_FROM_THRSHLD_AMT")
)

df_TransformSnap_final = (
    df_TransformSnap
    .withColumn("COMSN_SCHD_ID", rpad(col("COMSN_SCHD_ID"), 4, " "))
    .withColumn("DURATN_EFF_DT_SK", rpad(col("DURATN_EFF_DT_SK"), 10, " "))
    .select(
        "SRC_SYS_CD_SK",
        "COMSN_SCHD_ID",
        "DURATN_EFF_DT_SK",
        "DURATN_STRT_PERD_NO",
        "PRM_FROM_THRSHLD_AMT"
    )
)

write_files(
    df_TransformSnap_final,
    f"{adls_path}/load/B_COMSN_SCHD_TIER.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)