# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_4 02/20/09 11:05:46 Batch  15027_39950 PROMOTE bckcetl ids20 dsadm bls for sa
# MAGIC ^1_4 02/20/09 10:48:45 Batch  15027_38927 INIT bckcett testIDS dsadm bls for sa
# MAGIC ^1_3 02/19/09 15:48:36 Batch  15026_56922 PROMOTE bckcett testIDS u03651 steph for Sharon
# MAGIC ^1_3 02/19/09 15:43:11 Batch  15026_56617 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_2 01/13/09 08:41:23 Batch  14989_31287 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_3 09/01/08 11:32:17 Batch  14855_41541 PROMOTE bckcetl ids20 dsadm bls for on'
# MAGIC ^1_3 09/01/08 10:35:59 Batch  14855_38164 INIT bckcett devlIDSnew dsadm bls for on
# MAGIC ^1_1 08/26/08 10:50:12 Batch  14849_39015 INIT bckcett devlIDSnew u03651 steffy
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
# MAGIC ^1_3 12/27/05 15:23:50 Batch  13876_55437 PROMOTE bckcett testIDS30 u10913 Ollie Move to test
# MAGIC ^1_3 12/27/05 15:20:19 Batch  13876_55225 INIT bckcett devlIDS30 u10913 Ollie Move To Test
# MAGIC ^1_2 12/23/05 11:12:01 Batch  13872_40337 INIT bckcett devlIDS30 u10913 Ollie Move Income/Commisions to test
# MAGIC ^1_1 12/23/05 10:39:46 Batch  13872_38400 INIT bckcett devlIDS30 u10913 Ollie Moving Income / Commission to Test
# MAGIC 
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     FctsComsnMnlAdjExtr
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:    Pulls data from FACETS CMC_COCA_COMM_ADJ to a landing file for the IDS
# MAGIC       
# MAGIC 
# MAGIC INPUTS:	CMC_COCA_COMM_ADJ
# MAGIC                 
# MAGIC HASH FILES:  hf_comsn_mnl_adj
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
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              5/31/2007          3264                              Added Balancing process to the overall                 devlIDS30              Steph Goddard              09/18/2007
# MAGIC                                                                                                       job that takes a snapshot of the source data         
# MAGIC Oliver Nielsen                   7/25/2008         Facets 4.5.1               Changed COCA_SEQ_NO from smallInt to INT         devlIDSnew             Steph Goddard              08/25/2008
# MAGIC 
# MAGIC 
# MAGIC Bhoomi Dasari                 09/12/2008       3567                             Added new primay key contianer and SrcSysCdsk  devlIDS                    Steph Goddard              09/22/2008     
# MAGIC                                                                                                       and SrcSysCd  
# MAGIC Prabhu ES                      2022-03-01          S2S Remediation         MSSQL connection parameters added                    IntegrateDev5

# MAGIC Strip Carriage Return, Line Feed, and  Tab
# MAGIC 
# MAGIC Use STRIP.FIELD function on each character column coming in
# MAGIC Extract Facets Member Eligibility Data
# MAGIC Hash file hf_comsn_mnl_adj_allcol cleared
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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
from pyspark.sql.functions import col, when, lit, rpad, concat
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
TmpOutFile = get_widget_value('TmpOutFile','IdsComsnMnlAdjExtr.dat.pkey')
CurrRunCycle = get_widget_value('CurrRunCycle','150')
RunID = get_widget_value('RunID','20080915')
CurrDate = get_widget_value('CurrDate','2008-09-15')
SrcSysCdSk = get_widget_value('SrcSysCdSk','105838')
SrcSysCd = get_widget_value('SrcSysCd','FACETS')

# MAGIC %run ../../../../shared_containers/PrimaryKey/ComsnMnlAdjPK
# COMMAND ----------

# -----------------------------------------------------------------------------
# CMC_COCA_COMM_ADJ (ODBCConnector) => StripField
# -----------------------------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(facets_secret_name)
extract_query = f"SELECT * FROM {FacetsOwner}.CMC_COCA_COMM_ADJ"
df_CMC_COCA_COMM_ADJ = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_StripField = df_CMC_COCA_COMM_ADJ.select(
    trim(strip_field(col("COCE_ID"))).alias("COCE_ID"),
    col("COCA_SEQ_NO").alias("COCA_SEQ_NO"),
    trim(strip_field(col("COAR_ID"))).alias("COAR_ID"),
    trim(strip_field(col("COCE_ID_PAYEE"))).alias("COCE_ID_PAYEE"),
    trim(strip_field(col("LOBD_ID"))).alias("LOBD_ID"),
    trim(strip_field(col("COCA_PYMT_TYPE"))).alias("COCA_PYMT_TYPE"),
    trim(strip_field(col("COCA_MCTR_RSN"))).alias("COCA_MCTR_RSN"),
    trim(strip_field(col("COCA_STS"))).alias("COCA_STS"),
    trim(strip_field(col("COAG_EFF_DT"))).alias("COAG_EFF_DT"),
    trim(strip_field(col("COCA_PAID_DT"))).alias("COCA_PAID_DT"),
    col("COCA_PYMT_AMT").alias("COCA_PYMT_AMT"),
    trim(strip_field(col("COCA_PYMT_DESC"))).alias("COCA_PYMT_DESC")
)

# -----------------------------------------------------------------------------
# BusinessRules (CTransformerStage) => Two output links: AllCol and Transform
# -----------------------------------------------------------------------------
# AllCol
df_BusinessRules_AllCol = df_StripField.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("COCA_SEQ_NO").alias("COCA_SEQ_NO"),
    col("COCE_ID").alias("COCE_ID"),
    when(col("COAR_ID").isNull() | (col("COAR_ID") == ''), lit('NA')).otherwise(col("COAR_ID")).alias("COAR_ID"),
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit('I').alias("INSRT_UPDT_CD"),
    lit('N').alias("DISCARD_IN"),
    lit('Y').alias("PASS_THRU_IN"),
    lit(CurrDate).alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    lit(SrcSysCd).alias("SRC_SYS_CD"),
    concat(lit(SrcSysCd), lit(';'), col("COCE_ID"), lit(';'), col("COCA_SEQ_NO")).alias("PRI_KEY_STRING"),
    col("COCE_ID_PAYEE").alias("COCE_ID_PAYEE"),
    col("LOBD_ID").alias("LOBD_ID"),
    col("COCA_PYMT_TYPE").alias("COCA_PYMT_TYPE"),
    when(col("COCA_MCTR_RSN").isNull() | (col("COCA_MCTR_RSN") == ''), lit('NA')).otherwise(col("COCA_MCTR_RSN")).alias("COCA_MCTR_RSN"),
    col("COCA_STS").alias("COCA_STS"),
    when(col("COAG_EFF_DT").isNull() | (col("COAG_EFF_DT") == ''), lit('NA')).otherwise(col("COAG_EFF_DT")).alias("COAG_EFF_DT"),
    when(col("COCA_PAID_DT").isNull() | (col("COCA_PAID_DT") == ''), lit('NA')).otherwise(col("COCA_PAID_DT")).alias("COCA_PAID_DT"),
    col("COCA_PYMT_AMT").alias("COCA_PYMT_AMT"),
    col("COCA_PYMT_DESC").alias("COCA_PYMT_DESC")
)

# Transform
df_BusinessRules_Transform = df_StripField.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("COCE_ID").alias("AGNT_ID"),
    col("COCA_SEQ_NO").alias("SEQ_NO")
)

# -----------------------------------------------------------------------------
# ComsnMnlAdjPK (Shared Container)
# -----------------------------------------------------------------------------
params_container = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "RunID": RunID,
    "CurrentDate": CurrDate,
    "IDSOwner": IDSOwner
}
df_ComsnMnlAdjPK = ComsnMnlAdjPK(df_BusinessRules_AllCol, df_BusinessRules_Transform, params_container)

# -----------------------------------------------------------------------------
# IdsComsnMnlAdjExtr (CSeqFileStage)
# -----------------------------------------------------------------------------
df_ComsnMnlAdjExtr_final = df_ComsnMnlAdjPK.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("COMSN_MNL_ADJ_SK"),
    col("AGNT_ID"),
    col("SEQ_NO"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("COAR_ID"), 12, " ").alias("COAR_ID"),
    rpad(col("COCE_ID_PAYEE"), 12, " ").alias("COCE_ID_PAYEE"),
    rpad(col("LOBD_ID"), 4, " ").alias("LOBD_ID"),
    rpad(col("COCA_PYMT_TYPE"), 1, " ").alias("COCA_PYMT_TYPE"),
    rpad(col("COCA_MCTR_RSN"), 4, " ").alias("COCA_MCTR_RSN"),
    rpad(col("COCA_STS"), 1, " ").alias("COCA_STS"),
    rpad(col("COAG_EFF_DT"), 10, " ").alias("COAG_EFF_DT"),
    rpad(col("COCA_PAID_DT"), 10, " ").alias("COCA_PAID_DT"),
    col("ADJ_AMT"),
    col("ADJ_DESC")
)

write_files(
    df_ComsnMnlAdjExtr_final,
    f"{adls_path}/key/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# -----------------------------------------------------------------------------
# Facets_Source (ODBCConnector) => Transform => Snapshot_File
# -----------------------------------------------------------------------------
jdbc_url_2, jdbc_props_2 = get_db_config(facets_secret_name)
facets_query = f"SELECT * FROM {FacetsOwner}.CMC_COCA_COMM_ADJ"
df_Facets_Source = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_2)
    .options(**jdbc_props_2)
    .option("query", facets_query)
    .load()
)

df_Transform2 = df_Facets_Source.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    trim(strip_field(col("COCE_ID"))).alias("AGNT_ID"),
    col("COCA_SEQ_NO").alias("SEQ_NO")
)

df_Snapshot_File_final = df_Transform2.select(
    col("SRC_SYS_CD_SK"),
    rpad(col("AGNT_ID"), 12, " ").alias("AGNT_ID"),
    col("SEQ_NO")
)

write_files(
    df_Snapshot_File_final,
    f"{adls_path}/load/B_COMSN_MNL_ADJ.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)