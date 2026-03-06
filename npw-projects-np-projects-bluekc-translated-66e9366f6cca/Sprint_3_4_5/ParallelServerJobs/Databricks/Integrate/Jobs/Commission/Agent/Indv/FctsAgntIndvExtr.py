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
# MAGIC ^1_3 12/23/05 11:39:15 Batch  13872_41979 PROMOTE bckcett testIDS30 u10913 Move Income Commission to test
# MAGIC ^1_3 12/23/05 11:12:01 Batch  13872_40337 INIT bckcett devlIDS30 u10913 Ollie Move Income/Commisions to test
# MAGIC ^1_2 12/23/05 10:39:46 Batch  13872_38400 INIT bckcett devlIDS30 u10913 Ollie Moving Income / Commission to Test
# MAGIC ^1_1 12/14/05 12:18:44 Batch  13863_44334 INIT bckcett devlIDS30 u11141 Hugh Sisson
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     FctsAgntIndvExtr
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Pulls data from CMC_COCI_COMN_ENTY for loading into IDS.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:     Facets -  CMC_COCI_COMN_ENTY 
# MAGIC 
# MAGIC 
# MAGIC HASH FILES: hf_agnt_indv
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:   STRIP.FIELD
# MAGIC                              FORMAT.DATE
# MAGIC                              FmtString
# MAGIC                              NullCode
# MAGIC                            
# MAGIC PROCESSING:  Extract, transform, and primary keying for Agent subject area.
# MAGIC   
# MAGIC OUTPUTS:   Sequential file
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               Hugh Sisson    10/11/2005  -  Originally Programmed
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              6/7/2007          3264                              Added Balancing process to the overall                 devlIDS30                 Steph Goddard             09/17/2007
# MAGIC                                                                                                       job that takes a snapshot of the source data                        
# MAGIC 
# MAGIC Bhoomi Dasari                 09/05/2008       3567                             Added new primay key contianer and SrcSysCdsk  devlIDS                    Steph Goddard              09/22/2008
# MAGIC                                                                                                       and SrcSysCd  
# MAGIC Prabhu ES                       2022-03-01         S2S Remediation         MSSQL connection parameters added                    IntegrateDev5

# MAGIC Apply business logic.
# MAGIC Extract Facets Agent Data
# MAGIC Hash file hf_agnt_indv_allcol cleared
# MAGIC Strip Carriage Return, Line Feed, and Tab.
# MAGIC FctsAgntIndvExtr
# MAGIC 
# MAGIC Extract, transform, and assign primary key.
# MAGIC Balancing snapshot of source table
# MAGIC Writing Sequential File to ../key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, length, upper, when, concat, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/AgntIndvPK
# COMMAND ----------

FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
TmpOutFile = get_widget_value('TmpOutFile','FctsAgntIndvExtr.tmp')
CurrRunCycle = get_widget_value('CurrRunCycle','150')
RunID = get_widget_value('RunID','20080905')
CurrDate = get_widget_value('CurrDate','2008-09-05')
SrcSysCdSk = get_widget_value('SrcSysCdSk','105838')
SrcSysCd = get_widget_value('SrcSysCd','FACETS')

jdbc_url, jdbc_props = get_db_config(facets_secret_name)
df_CMC_COCI_COMN_INFO = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT COCI_ID, COCI_TYPE, COCI_SSN, COCI_LST_NAME, COCI_FIRST_NAME, COCI_MID_INIT, COCI_TITLE FROM {FacetsOwner}.CMC_COCI_COMN_INFO WHERE COCI_TYPE = 'P'"
    )
    .load()
)

df_StripField = df_CMC_COCI_COMN_INFO.select(
    strip_field(col("COCI_ID")).alias("COCI_ID"),
    strip_field(col("COCI_TYPE")).alias("COCI_TYPE"),
    strip_field(col("COCI_SSN")).alias("COCI_SSN"),
    strip_field(col("COCI_LST_NAME")).alias("COCI_LST_NAME"),
    strip_field(col("COCI_FIRST_NAME")).alias("COCI_FIRST_NAME"),
    strip_field(col("COCI_MID_INIT")).alias("COCI_MID_INIT"),
    strip_field(col("COCI_TITLE")).alias("COCI_TITLE")
)

df_BusinessRules_AllCol = df_StripField.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    trim(col("COCI_ID")).alias("AGNT_INDV_ID"),
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit("I").alias("INSRT_UPDT_CD"),
    lit("N").alias("DISCARD_IN"),
    lit("Y").alias("PASS_THRU_IN"),
    lit(CurrDate).alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    lit(SrcSysCd).alias("SRC_SYS_CD"),
    concat(lit(SrcSysCd), lit(";"), trim(col("COCI_ID"))).alias("PRI_KEY_STRING"),
    lit(0).alias("AGNT_INDV_SK"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    when(col("COCI_TYPE").isNull() | (length(trim(col("COCI_TYPE"))) == 0), "").otherwise(upper(trim(col("COCI_TYPE")))).alias("AGNT_INDV_ENTY_TYP_CD"),
    when(col("COCI_SSN").isNull() | (length(trim(col("COCI_SSN"))) == 0), "UNK").otherwise(upper(trim(col("COCI_SSN")))).alias("SSN"),
    when(col("COCI_FIRST_NAME").isNull() | (length(trim(col("COCI_FIRST_NAME"))) == 0), "").otherwise(upper(trim(col("COCI_FIRST_NAME")))).alias("FIRST_NM"),
    when(col("COCI_MID_INIT").isNull() | (length(trim(col("COCI_MID_INIT"))) == 0), "").otherwise(upper(trim(col("COCI_MID_INIT")))).alias("MIDINIT"),
    when(col("COCI_LST_NAME").isNull() | (length(trim(col("COCI_LST_NAME"))) == 0), "").otherwise(upper(trim(col("COCI_LST_NAME")))).alias("LAST_NM"),
    when(col("COCI_TITLE").isNull() | (length(trim(col("COCI_TITLE"))) == 0), "").otherwise(upper(trim(col("COCI_TITLE")))).alias("INDV_TTL")
)

df_BusinessRules_Transform = df_StripField.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    trim(col("COCI_ID")).alias("AGNT_INDV_ID")
)

params_AgntIndvPK = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "RunID": RunID,
    "CurrentDate": CurrDate,
    "IDSOwner": IDSOwner
}
df_AgntIndvPK = AgntIndvPK(df_BusinessRules_AllCol, df_BusinessRules_Transform, params_AgntIndvPK)

df_IdsAgntIndvExtr = df_AgntIndvPK.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("AGNT_INDV_SK"),
    col("AGNT_INDV_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("AGNT_INDV_ENTY_TYP_CD"),
    rpad(col("SSN"), 20, " ").alias("SSN"),
    col("FIRST_NM"),
    rpad(col("MIDINIT"), 1, " ").alias("MIDINIT"),
    col("LAST_NM"),
    col("INDV_TTL")
)

write_files(
    df_IdsAgntIndvExtr,
    f"{adls_path}/key/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

jdbc_url_2, jdbc_props_2 = get_db_config(facets_secret_name)
df_Facets_Source = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_2)
    .options(**jdbc_props_2)
    .option(
        "query",
        f"SELECT COCI_ID FROM {FacetsOwner}.CMC_COCI_COMN_INFO WHERE COCI_TYPE = 'P'"
    )
    .load()
)

df_Transform = df_Facets_Source.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    trim(strip_field(col("COCI_ID"))).alias("AGNT_INDV_ID")
)

df_Snapshot_File = df_Transform.select(
    col("SRC_SYS_CD_SK"),
    col("AGNT_INDV_ID")
)

write_files(
    df_Snapshot_File,
    f"{adls_path}/load/B_AGNT_INDV.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)