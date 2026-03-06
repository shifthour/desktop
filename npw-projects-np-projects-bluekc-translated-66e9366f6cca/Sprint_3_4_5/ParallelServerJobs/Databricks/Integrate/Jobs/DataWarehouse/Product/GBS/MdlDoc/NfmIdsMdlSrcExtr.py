# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2009, 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY: GbsIdsProdMdlCntl
# MAGIC 
# MAGIC                            
# MAGIC PROCESSING:   Pulls the NFM Policy  data from NFM table to IDS MDL_DOC table
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC DinakarS                         2018-05-23             5205                           Originally Programmed                                      devlIDSnew                    Kalyan Neelam             2018-07-03

# MAGIC Job name: GrpMdlExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F, types as T
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# Retrieve job parameters
SrcSysCd = get_widget_value('SrcSysCd','FILEBOUND')
NFMOwner = get_widget_value('NFMOwner','')
nfm_secret_name = get_widget_value('nfm_secret_name','')
RunID = get_widget_value('RunID','')

# Set up JDBC connection
jdbc_url, jdbc_props = get_db_config(nfm_secret_name)

# Define the extract query (replacing #$NFMOwner# with the NFMOwner parameter)
extract_query = f"""select MDL_DOC.MODL_DOC_ID, MDL_DOC.MODL_DOC_NM, MDL_DOC.MDL_DOC_EFF_DT, MDL_DOC.POLICY_NO, MDL_DOC.POL_FORM_STTUS_ID, MDL_DOC.POLICY_FORM_TYP, MDL_DOC.POLICY_FORM_DT,MDL_DOC.POLICY_FORM_ID,MDL_DOC.OFF_RENEW_ID,MDL_DOC.GRP_ID,MDL_DOC.GRP_NM ,MDL_DOC.GRP_POL_FORM_EFF_DT,PROD_ID
FROM 
(
 SELECT DISTINCT DRVR.MODL_DOC_ID, DRVR.MODL_DOC_NM, DRVR.MDL_DOC_EFF_DT, B.POLICY_NO, B.POL_FORM_STTUS_ID, UPPER(B.POLICY_FORM_TYP) POLICY_FORM_TYP,
        CAST(B.POLICY_FORM_DT AS DATE) POLICY_FORM_DT, B.POLICY_FORM_ID, B.OFF_RENEW_ID, A.GRP_ID, A.GRP_NM,A.PROD_ID, CAST(A.GRP_POL_FORM_EFF_DT AS DATE) GRP_POL_FORM_EFF_DT
 FROM
 (
   SELECT DISTINCT A.FileID AS MODL_DOC_ID, A.Field1 AS MODL_DOC_NM, CAST(A.DateStarted AS DATE) AS MDL_DOC_EFF_DT 
   FROM {NFMOwner}.[Files] A
   INNER JOIN
   (
     Select Field1, Max(FileID) FileID 
     from {NFMOwner}.[Files] 
     WHERE ProjectID = 1 AND Field1 IS NOT NULL AND RTRIM(LTRIM(Field1)) <> '' 
     GROUP BY Field1
   ) MAX_ROW
   ON A.Field1 = MAX_ROW.Field1 AND A.FileID = MAX_ROW.FileID 
   WHERE A.ProjectID = 1 AND A.Status=1
 ) DRVR
 INNER JOIN
 (
   SELECT DISTINCT Field1 AS GRP_ID, Field2 AS GRP_NM, Field6 AS PROD_ID, Field9 AS GRP_POL_FORM_EFF_DT, Field8 AS MODL_DOC_NM 
   FROM {NFMOwner}.[Files]
   WHERE ProjectID = 2 AND Status = 1
 ) A 
 ON DRVR.MODL_DOC_NM = A.MODL_DOC_NM
 INNER JOIN
 (
   SELECT DISTINCT RemoteID, Field3 AS POLICY_NO, Field4 AS POLICY_FORM_TYP, Field5 AS POLICY_FORM_DT, Field1 AS POLICY_FORM_ID, Field6 AS OFF_RENEW_ID, Status AS POL_FORM_STTUS_ID 
   FROM {NFMOwner}.[Files] 
   WHERE ProjectID = 11 AND Status = 1 AND RTRIM(LTRIM(Field3)) <> ''
 ) B
 ON DRVR.MODL_DOC_ID = B.RemoteID 
 WHERE CAST(A.GRP_POL_FORM_EFF_DT AS DATE) >= CAST(B.POLICY_FORM_DT AS DATE) 
   AND CAST(B.POLICY_FORM_DT AS DATE) >= CAST(DRVR.MDL_DOC_EFF_DT AS DATE)
) MDL_DOC 
ORDER BY MDL_DOC.GRP_ID, MDL_DOC.POLICY_NO
"""

# Read from the database
df_Nfm_MDL_POL_PROD_Extr_1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# -----------------------------------------------------------------------------
# Transformer Stage: TXN_GBS_MDL
# Primary Input: df_Nfm_MDL_POL_PROD_Extr_1
# Outputs:

# 1) Ink_GrpMdlPolProdExtr_Out -> ds_GRP_MDL_POL_PROD
df_GrpMdlPolProdExtr_Out = df_Nfm_MDL_POL_PROD_Extr_1.select(
    trim(F.coalesce(F.col('GRP_ID'), F.lit(''))).alias('GRP_ID'),
    F.col('MODL_DOC_ID').alias('MDL_DOC_ID'),
    trim(F.coalesce(F.col('POLICY_NO'), F.lit(''))).alias('POL_NO'),
    F.when(
        Upcase(trim(F.coalesce(F.col('POLICY_FORM_TYP'), F.lit('')))) == 'CONTRACT',
        Upcase(trim(F.coalesce(F.col('POLICY_FORM_ID'), F.lit(''))))
    ).otherwise('NA').alias('CNTR_ID'),
    F.when(
        Upcase(trim(F.coalesce(F.col('POLICY_FORM_TYP'), F.lit('')))) == 'CERTIFICATE',
        Upcase(trim(F.coalesce(F.col('POLICY_FORM_ID'), F.lit(''))))
    ).otherwise('NA').alias('CERT_ID'),
    F.when(
        Upcase(trim(F.coalesce(F.col('POLICY_FORM_TYP'), F.lit('')))) == 'AMENDMENT',
        Upcase(trim(F.coalesce(F.col('POLICY_FORM_ID'), F.lit(''))))
    ).otherwise('NA').alias('AMNDMNT_ID'),
    StringToDate(F.col('GRP_POL_FORM_EFF_DT'), "%yyyy-%mm-%dd").alias('GRP_MDL_POL_PROD_EFF_DT'),
    F.lit(SrcSysCd).alias('SRC_SYS_CD'),
    Upcase(trim(F.coalesce(F.col('POLICY_FORM_ID'), F.lit('')))).alias('POLICY_FORM_ID'),
    StringToDate(F.col('POLICY_FORM_DT'), "%yyyy-%mm-%dd").alias('POLICY_FORM_DT'),
    StringToDate(F.lit("2199-12-31"), "%yyyy-%mm-%dd").alias('GRP_MDL_POL_PROD_TERM_DT'),
    trim(F.coalesce(F.col('PROD_ID'), F.lit(''))).alias('PROD_ID')
)

df_GrpMdlPolProdExtr_Out = df_GrpMdlPolProdExtr_Out.select(
    F.rpad(F.col('GRP_ID'), 50, ' ').alias('GRP_ID'),
    F.col('MDL_DOC_ID'),
    F.rpad(F.col('POL_NO'), 50, ' ').alias('POL_NO'),
    F.rpad(F.col('CNTR_ID'), 50, ' ').alias('CNTR_ID'),
    F.rpad(F.col('CERT_ID'), 50, ' ').alias('CERT_ID'),
    F.rpad(F.col('AMNDMNT_ID'), 50, ' ').alias('AMNDMNT_ID'),
    F.col('GRP_MDL_POL_PROD_EFF_DT'),
    F.rpad(F.col('SRC_SYS_CD'), 50, ' ').alias('SRC_SYS_CD'),
    F.rpad(F.col('POLICY_FORM_ID'), 50, ' ').alias('POLICY_FORM_ID'),
    F.col('POLICY_FORM_DT'),
    F.col('GRP_MDL_POL_PROD_TERM_DT'),
    F.rpad(F.col('PROD_ID'), 50, ' ').alias('PROD_ID')
)

# 2) Ink_MdlPolCntrExtr_Out -> ds_MDL_CNTR_DS
df_MdlPolCntrExtr_Out = df_Nfm_MDL_POL_PROD_Extr_1.filter(
    Upcase(trim(F.coalesce(F.col('POLICY_FORM_TYP'), F.lit('')))) == 'CONTRACT'
).select(
    F.col('MODL_DOC_ID').alias('MDL_DOC_ID'),
    trim(F.coalesce(F.col('POLICY_NO'), F.lit(''))).alias('POL_NO'),
    trim(F.coalesce(F.col('POLICY_FORM_ID'), F.lit(''))).alias('POLICY_FORM_ID'),
    StringToDate(F.col('POLICY_FORM_DT'), "%yyyy-%mm-%dd").alias('POLICY_FORM_DT'),
    Upcase(trim(F.coalesce(F.col('POLICY_FORM_ID'), F.lit('')))).alias('CNTR_ID'),
    StringToDate(F.col('MDL_DOC_EFF_DT'), "%yyyy-%mm-%dd").alias('MDL_DOC_EFF_DT'),
    StringToDate(F.col('POLICY_FORM_DT'), "%yyyy-%mm-%dd").alias('CNTR_EFF_DT'),
    StringToDate(F.lit("2199-12-31"), "%yyyy-%mm-%dd").alias('MDL_DOC_TERM_DT'),
    F.lit(SrcSysCd).alias('SRC_SYS_CD'),
    F.when(
        Upcase(trim(F.coalesce(F.col('OFF_RENEW_ID'), F.lit('')))) == 'ON RENEWAL',
        'Y'
    ).otherwise('N').alias('CNTR_EFF_ON_GRP_RNWL_IN'),
    trim(F.coalesce(F.col('POL_FORM_STTUS_ID'), F.lit(''))).alias('CNTR_STATUS_ID'),
    StringToDate(F.lit("2199-12-31"), "%yyyy-%mm-%dd").alias('CNTR_TERM_DT')
)

df_MdlPolCntrExtr_Out = df_MdlPolCntrExtr_Out.select(
    F.col('MDL_DOC_ID'),
    F.rpad(F.col('POL_NO'), 50, ' ').alias('POL_NO'),
    F.rpad(F.col('POLICY_FORM_ID'), 50, ' ').alias('POLICY_FORM_ID'),
    F.col('POLICY_FORM_DT'),
    F.rpad(F.col('CNTR_ID'), 50, ' ').alias('CNTR_ID'),
    F.col('MDL_DOC_EFF_DT'),
    F.col('CNTR_EFF_DT'),
    F.col('MDL_DOC_TERM_DT'),
    F.rpad(F.col('SRC_SYS_CD'), 50, ' ').alias('SRC_SYS_CD'),
    F.rpad(F.col('CNTR_EFF_ON_GRP_RNWL_IN'), 50, ' ').alias('CNTR_EFF_ON_GRP_RNWL_IN'),
    F.rpad(F.col('CNTR_STATUS_ID'), 50, ' ').alias('CNTR_STATUS_ID'),
    F.col('CNTR_TERM_DT')
)

# 3) Ink_MdlPolACertExtr_Out -> ds_MDL_CERT_DS
df_MdlPolACertExtr_Out = df_Nfm_MDL_POL_PROD_Extr_1.filter(
    Upcase(trim(F.coalesce(F.col('POLICY_FORM_TYP'), F.lit('')))) == 'CERTIFICATE'
).select(
    F.col('MODL_DOC_ID').alias('MDL_DOC_ID'),
    trim(F.coalesce(F.col('POLICY_NO'), F.lit(''))).alias('POL_NO'),
    Upcase(trim(F.coalesce(F.col('POLICY_FORM_ID'), F.lit('')))).alias('POLICY_FORM_ID'),
    StringToDate(F.col('POLICY_FORM_DT'), "%yyyy-%mm-%dd").alias('POLICY_FORM_DT'),
    Upcase(trim(F.coalesce(F.col('POLICY_FORM_ID'), F.lit('')))).alias('CERT_ID'),
    StringToDate(F.col('MDL_DOC_EFF_DT'), "%yyyy-%mm-%dd").alias('MDL_DOC_EFF_DT'),
    StringToDate(F.col('POLICY_FORM_DT'), "%yyyy-%mm-%dd").alias('CERT_EFF_DT'),
    StringToDate(F.lit("2199-12-31"), "%yyyy-%mm-%dd").alias('MDL_DOC_TERM_DT'),
    F.lit(SrcSysCd).alias('SRC_SYS_CD'),
    F.when(
        Upcase(trim(F.coalesce(F.col('OFF_RENEW_ID'), F.lit('')))) == 'ON RENEWAL',
        'Y'
    ).otherwise('N').alias('CERT_EFF_ON_GRP_RNWL_IN'),
    trim(F.coalesce(F.col('POL_FORM_STTUS_ID'), F.lit(''))).alias('CERT_STATUS_ID'),
    StringToDate(F.lit("2199-12-31"), "%yyyy-%mm-%dd").alias('CERT_TERM_DT')
)

df_MdlPolACertExtr_Out = df_MdlPolACertExtr_Out.select(
    F.col('MDL_DOC_ID'),
    F.rpad(F.col('POL_NO'), 50, ' ').alias('POL_NO'),
    F.rpad(F.col('POLICY_FORM_ID'), 50, ' ').alias('POLICY_FORM_ID'),
    F.col('POLICY_FORM_DT'),
    F.rpad(F.col('CERT_ID'), 50, ' ').alias('CERT_ID'),
    F.col('MDL_DOC_EFF_DT'),
    F.col('CERT_EFF_DT'),
    F.col('MDL_DOC_TERM_DT'),
    F.rpad(F.col('SRC_SYS_CD'), 50, ' ').alias('SRC_SYS_CD'),
    F.rpad(F.col('CERT_EFF_ON_GRP_RNWL_IN'), 50, ' ').alias('CERT_EFF_ON_GRP_RNWL_IN'),
    F.rpad(F.col('CERT_STATUS_ID'), 50, ' ').alias('CERT_STATUS_ID'),
    F.col('CERT_TERM_DT')
)

# 4) Ink_MdlPolAmndmntExtr_Out -> ds_MDL_AMNDMNT_DS
df_MdlPolAmndmntExtr_Out = df_Nfm_MDL_POL_PROD_Extr_1.filter(
    Upcase(trim(F.coalesce(F.col('POLICY_FORM_TYP'), F.lit('')))) == 'AMENDMENT'
).select(
    F.col('MODL_DOC_ID').alias('MDL_DOC_ID'),
    trim(F.coalesce(F.col('POLICY_NO'), F.lit(''))).alias('POL_NO'),
    Upcase(trim(F.coalesce(F.col('POLICY_FORM_ID'), F.lit('')))).alias('POLICY_FORM_ID'),
    StringToDate(F.col('POLICY_FORM_DT'), "%yyyy-%mm-%dd").alias('POLICY_FORM_DT'),
    Upcase(trim(F.coalesce(F.col('POLICY_FORM_ID'), F.lit('')))).alias('AMNDMNT_ID'),
    StringToDate(F.col('MDL_DOC_EFF_DT'), "%yyyy-%mm-%dd").alias('MDL_DOC_EFF_DT'),
    StringToDate(F.col('POLICY_FORM_DT'), "%yyyy-%mm-%dd").alias('AMNDMNT_EFF_DT'),
    StringToDate(F.lit("2199-12-31"), "%yyyy-%mm-%dd").alias('MDL_DOC_TERM_DT'),
    F.lit(SrcSysCd).alias('SRC_SYS_CD'),
    F.when(
        Upcase(trim(F.coalesce(F.col('OFF_RENEW_ID'), F.lit('')))) == 'ON RENEWAL',
        'Y'
    ).otherwise('N').alias('AMNDMNT_EFF_ON_GRP_RNWL_IN'),
    trim(F.coalesce(F.col('POL_FORM_STTUS_ID'), F.lit(''))).alias('AMNDMNT_STATUS_ID'),
    StringToDate(F.lit("2199-12-31"), "%yyyy-%mm-%dd").alias('AMNDMNT_TERM_DT')
)

df_MdlPolAmndmntExtr_Out = df_MdlPolAmndmntExtr_Out.select(
    F.col('MDL_DOC_ID'),
    F.rpad(F.col('POL_NO'), 50, ' ').alias('POL_NO'),
    F.rpad(F.col('POLICY_FORM_ID'), 50, ' ').alias('POLICY_FORM_ID'),
    F.col('POLICY_FORM_DT'),
    F.rpad(F.col('AMNDMNT_ID'), 50, ' ').alias('AMNDMNT_ID'),
    F.col('MDL_DOC_EFF_DT'),
    F.col('AMNDMNT_EFF_DT'),
    F.col('MDL_DOC_TERM_DT'),
    F.rpad(F.col('SRC_SYS_CD'), 50, ' ').alias('SRC_SYS_CD'),
    F.rpad(F.col('AMNDMNT_EFF_ON_GRP_RNWL_IN'), 50, ' ').alias('AMNDMNT_EFF_ON_GRP_RNWL_IN'),
    F.rpad(F.col('AMNDMNT_STATUS_ID'), 50, ' ').alias('AMNDMNT_STATUS_ID'),
    F.col('AMNDMNT_TERM_DT')
)

# 5) Ink_MdlDocExtr_Out -> ds_MDL_DOC_DS
df_MdlDocExtr_Out = df_Nfm_MDL_POL_PROD_Extr_1.select(
    trim(F.coalesce(F.col('MODL_DOC_ID'), F.lit(''))).alias('MDL_DOC_ID'),
    StringToDate(F.col('MDL_DOC_EFF_DT'), "%yyyy-%mm-%dd").alias('MDL_DOC_EFF_DT'),
    F.lit(SrcSysCd).alias('SRC_SYS_CD'),
    StringToDate(F.lit("2199-12-31"), "%yyyy-%mm-%dd").alias('MDL_DOC_TERM_DT'),
    trim(F.coalesce(F.col('MODL_DOC_NM'), F.lit(''))).alias('MDL_DOC_NM')
)

df_MdlDocExtr_Out = df_MdlDocExtr_Out.select(
    F.rpad(F.col('MDL_DOC_ID'), 50, ' ').alias('MDL_DOC_ID'),
    F.col('MDL_DOC_EFF_DT'),
    F.rpad(F.col('SRC_SYS_CD'), 50, ' ').alias('SRC_SYS_CD'),
    F.col('MDL_DOC_TERM_DT'),
    F.rpad(F.col('MDL_DOC_NM'), 50, ' ').alias('MDL_DOC_NM')
)

# -----------------------------------------------------------------------------
# Write Outputs (Dataset -> parquet)

write_files(
    df_GrpMdlPolProdExtr_Out,
    f"{adls_path}/ds/GRPMDLPOLPROD.{SrcSysCd}.xfrm.{RunID}.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

write_files(
    df_MdlPolCntrExtr_Out,
    f"{adls_path}/ds/MDLPOLCNTR.{SrcSysCd}.xfrm.{RunID}.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

write_files(
    df_MdlPolACertExtr_Out,
    f"{adls_path}/ds/MDLPOLCERT.{SrcSysCd}.xfrm.{RunID}.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

write_files(
    df_MdlPolAmndmntExtr_Out,
    f"{adls_path}/ds/MDLPOLAMNDMNT.{SrcSysCd}.xfrm.{RunID}.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

write_files(
    df_MdlDocExtr_Out,
    f"{adls_path}/ds/MDLDOC.{SrcSysCd}.xfrm.{RunID}.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)