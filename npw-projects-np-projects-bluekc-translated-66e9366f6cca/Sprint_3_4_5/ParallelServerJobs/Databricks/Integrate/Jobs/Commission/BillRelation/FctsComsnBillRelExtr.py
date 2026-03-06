# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_4 02/20/09 11:05:46 Batch  15027_39950 PROMOTE bckcetl ids20 dsadm bls for sa
# MAGIC ^1_4 02/20/09 10:48:45 Batch  15027_38927 INIT bckcett testIDS dsadm bls for sa
# MAGIC ^1_2 02/19/09 15:48:36 Batch  15026_56922 PROMOTE bckcett testIDS u03651 steph for Sharon
# MAGIC ^1_2 02/19/09 15:43:11 Batch  15026_56617 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_1 01/13/09 08:41:23 Batch  14989_31287 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_1 01/30/08 05:00:10 Batch  14640_18019 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_3 11/09/07 15:23:10 Batch  14558_55394 INIT bckcetl ids20 dsadm dsadm
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
# MAGIC JOB NAME:     FctsComsnBillRelExtr
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:    Pulls data from FACETS CMC_COBL_RELATION to a landing file for the IDS
# MAGIC       
# MAGIC 
# MAGIC INPUTS:	CMC_COBL_RELATION
# MAGIC   
# MAGIC 
# MAGIC HASH FILES:  hf_comsn_bill_rel
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  STRIP.FIELD
# MAGIC                             FORMAT.DATE
# MAGIC                             Nulls and blanks to values
# MAGIC 
# MAGIC PROCESSING:  Output file is created with a temp. name.  File renamed in job control
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS:    Sequential file 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC   Tao Luo - 10/20/05 - Orginal Development
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                                                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                                                                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              5/30/2007          3264                              Added Balancing process to the overall                                                                 devlIDS30              Steph Goddard             09/17/2007
# MAGIC                                                                                                       job that takes a snapshot of the source data                        
# MAGIC 
# MAGIC 
# MAGIC Bhoomi Dasari                 09/18/2008       3567                             Added new primay key contianer and SrcSysCdsk  devlIDS                                                                   Steph Goddard             09/22/2008     
# MAGIC                                                                                                      and SrcSysCd  
# MAGIC 
# MAGIC Bhoomi Dasari                 01/09/2009       3567                             Added hash file after facets extract due to load failure to K _tmp table
# MAGIC Prabhu ES                       2022-03-01        S2S Remediation         MSSQL connection parameters added                                                                      IntegrateDev5
# MAGIC 
# MAGIC Goutham Kalidindi           2024-08-31        US-625182                  Part of facets release updated COBL_SOURCE_CK for from int to bigint.                  IntegrateDev3          Reddy Sanam           09/04/2024
# MAGIC                                                                                                       (Table: CMC_COBL_RELATION)

# MAGIC Strip Carriage Return, Line Feed, and  Tab
# MAGIC 
# MAGIC Use STRIP.FIELD function on each character column coming in
# MAGIC Extract Facets Member Eligibility Data
# MAGIC Hash file hf_comsn_bill_rel_allcol cleared
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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


TmpOutFile = get_widget_value("TmpOutFile","IdsComsnBillRelExtr.ComsnBillRel.dat")
CurrRunCycle = get_widget_value("CurrRunCycle","145")
RunID = get_widget_value("RunID","20080908")
CurrDate = get_widget_value("CurrDate","2008-09-08")
SrcSysCdSk = get_widget_value("SrcSysCdSk","105838")
SrcSysCd = get_widget_value("SrcSysCd","FACETS")
IDSOwner = get_widget_value("$IDSOwner","$PROJDEF")
ids_secret_name = get_widget_value("ids_secret_name","")
FacetsOwner = get_widget_value("$FacetsOwner","$PROJDEF")
facets_secret_name = get_widget_value("facets_secret_name","")

jdbc_url_CMC_COBL_RELATION, jdbc_props_CMC_COBL_RELATION = get_db_config(facets_secret_name)
sql_CMC_COBL_RELATION = """SELECT 
COBL.COBL_CK,
COBL.COCE_ID,
COBL.COAR_ID,
COBL.COAG_EFF_DT,
COBL.COSC_CALC_METH,
COBL.COBL_SOURCE_CD,
COBL.COBL_STS,
COBL.COBL_BASIS,
COBL.COBL_SOURCE_AMT,
COBL.COBL_PER_BILL_CTR,
COBL.CORQ_CK,
COBL.COBL_SOURCE_CK,
COBL.COCP_COMM_PER 

FROM #$FacetsOwner#.CMC_COBL_RELATION COBL,
     #$FacetsOwner#.CMC_COCC_CALC_COMM COCC

WHERE COCC.COCC_CREATE_DTM  >=  dateadd(dd,-7,GetDate())
  AND COBL.COBL_CK = COCC.COBL_CK
"""
df_CMC_COBL_RELATION = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_CMC_COBL_RELATION)
    .options(**jdbc_props_CMC_COBL_RELATION)
    .option("query", sql_CMC_COBL_RELATION)
    .load()
)
df_hf_comsn_bill_rel_drop_dups = df_CMC_COBL_RELATION.dropDuplicates(["COBL_CK"])

df_StripField = df_hf_comsn_bill_rel_drop_dups.select(
    col("COBL_CK"),
    trim(strip_field(col("COCE_ID"))).alias("COCE_ID"),
    trim(strip_field(col("COAR_ID"))).alias("COAR_ID"),
    col("COAG_EFF_DT"),
    trim(strip_field(col("COSC_CALC_METH"))).alias("COSC_CALC_METH"),
    trim(strip_field(col("COBL_SOURCE_CD"))).alias("COBL_SOURCE_CD"),
    col("COBL_STS"),
    col("COBL_BASIS"),
    col("COBL_SOURCE_AMT"),
    col("COBL_PER_BILL_CTR"),
    col("CORQ_CK"),
    col("COBL_SOURCE_CK"),
    col("COCP_COMM_PER")
)

RowPassThru = lit("Y")
df_BusinessRules_AllCol = df_StripField.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("COBL_CK").alias("COBL_CK"),
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit("I").alias("INSRT_UPDT_CD"),
    lit("N").alias("DISCARD_IN"),
    RowPassThru.alias("PASS_THRU_IN"),
    lit(CurrDate).alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    lit("FACETS").alias("SRC_SYS_CD"),
    (lit("FACETS;") + col("COBL_CK").cast("string")).alias("PRI_KEY_STRING"),
    col("COCE_ID").alias("COCE_ID"),
    col("COAR_ID").alias("COAR_ID"),
    col("COAG_EFF_DT").cast("string").substr(1,10).alias("COAG_EFF_DT"),
    col("COSC_CALC_METH").alias("COSC_CALC_METH"),
    col("COBL_SOURCE_CD").alias("COBL_SOURCE_CD"),
    col("COBL_STS").alias("COBL_STS"),
    col("COBL_BASIS").alias("COBL_BASIS"),
    col("COBL_SOURCE_AMT").alias("COBL_SOURCE_AMT"),
    col("COBL_PER_BILL_CTR").alias("COBL_PER_BILL_CTR"),
    col("CORQ_CK").alias("CORQ_CK"),
    col("COBL_SOURCE_CK").alias("COBL_SOURCE_CK"),
    col("COCP_COMM_PER").alias("COCP_COMM_PER")
)

df_BusinessRules_Transform = df_StripField.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("COBL_CK").alias("COMSN_BILL_REL_UNIQ_KEY")
)

# MAGIC %run ../../../../shared_containers/PrimaryKey/ComsnBillRelPK
# COMMAND ----------
params_ComsnBillRelPK = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "RunID": RunID,
    "CurrentDate": CurrDate,
    "$IDSOwner": IDSOwner
}
df_ComsnBillRelPK = ComsnBillRelPK(df_BusinessRules_AllCol, df_BusinessRules_Transform, params_ComsnBillRelPK)

df_IdsComsnBillRelExtr_out = df_ComsnBillRelPK.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"),10," ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"),1," ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"),1," ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("COMSN_BILL_REL_SK"),
    col("COMSN_BILL_REL_UNIQ_KEY"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("COCE_ID"),12," ").alias("COCE_ID"),
    rpad(col("COAR_ID"),12," ").alias("COAR_ID"),
    rpad(col("COAG_EFF_DT"),10," ").alias("COAG_EFF_DT"),
    rpad(col("COSC_CALC_METH"),1," ").alias("COSC_CALC_METH"),
    rpad(col("COBL_SOURCE_CD"),1," ").alias("COBL_SOURCE_CD"),
    col("COBL_STS"),
    col("COMSN_BSS_AMT"),
    col("INCM_AMT"),
    col("BILL_CT"),
    col("COMSN_RELCALC_RQST_UNIQ_KEY"),
    col("INCM_UNIQ_KEY"),
    col("COMSN_PERD_NO")
)
write_files(
    df_IdsComsnBillRelExtr_out,
    f"{adls_path}/key/{TmpOutFile}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

jdbc_url_Facets_Source, jdbc_props_Facets_Source = get_db_config(facets_secret_name)
sql_Facets_Source = """SELECT COBL.COBL_CK

FROM #$FacetsOwner#.CMC_COBL_RELATION COBL,
     #$FacetsOwner#.CMC_COCC_CALC_COMM COCC

WHERE  COCC.COCC_CREATE_DTM  >= dateadd(dd,-7,GetDate())
  AND COBL.COBL_CK = COCC.COBL_CK
"""
df_Facets_Source = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_Facets_Source)
    .options(**jdbc_props_Facets_Source)
    .option("query", sql_Facets_Source)
    .load()
)
df_hf_comsn_bill_rel_drop_dups_snap = df_Facets_Source.dropDuplicates(["COBL_CK"])

df_Transform = df_hf_comsn_bill_rel_drop_dups_snap.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("COBL_CK").alias("COMSN_BILL_REL_UNIQ_KEY")
)
df_Snapshot_File_out = df_Transform.select(
    col("SRC_SYS_CD_SK"),
    col("COMSN_BILL_REL_UNIQ_KEY")
)
write_files(
    df_Snapshot_File_out,
    f"{adls_path}/load/B_COMSN_BILL_REL.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)