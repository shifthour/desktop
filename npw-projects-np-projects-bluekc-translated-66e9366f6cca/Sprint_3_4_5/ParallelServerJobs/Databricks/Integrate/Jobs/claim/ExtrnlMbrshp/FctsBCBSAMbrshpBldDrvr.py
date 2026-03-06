# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:     FctsClmExtrnlMbrshExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from CMC_CLMI_MISC to a landing file for the IDS to build external member rows in FctsClmExtrnlMbrTrns
# MAGIC       
# MAGIC 
# MAGIC INPUTS:   FACETS:
# MAGIC CMC_CLCL_CLAIM
# MAGIC CMC_CLMI_MISC
# MAGIC CMC_CLIS_ITS_SUBSC
# MAGIC CMC_CLIP_ITS_PATNT
# MAGIC 
# MAGIC                IDS:
# MAGIC P_MBR_BCBSA_SUPLMT
# MAGIC                
# MAGIC   
# MAGIC PROCESSING:
# MAGIC                   Datatype changes to conform data to the driver table metadata
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Sequential file name is created with the name of the driver tables.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #                                                                      Change Description                                Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------                                                                       -------------------------------------------------              --------------------------------       -------------------------------   ----------------------------       
# MAGIC Nikhil Sinha             03/03/2017 -     Data Catalyst - 30001- Add Mbr_SK                                Originally Programmed                                   IntegrateDev2                Kalyan Neelam           2017-03-07
# MAGIC                                                           to CLM_EXTRNL_MBRSH
# MAGIC Prabhu ES               2022-02-28       S2S Remediation                                                             MSSQL connection parameters added          IntegrateDev5               Manasa Andru              2022-06-08

# MAGIC Extract P_MBR_BCBSA_SUPLMT data from IDS into DRIVER table. 
# MAGIC 
# MAGIC Transform datatypes for comparision
# MAGIC Extract CLM_EXTRNL_MBR data from Facets in DRIVER table. 
# MAGIC 
# MAGIC Flatten Subscriber/Member and SIB ID's into 1 set of ID and Member Name
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, substring, rpad, date_format
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Parameter retrieval
RunID = get_widget_value('RunID','')
DriverTable = get_widget_value('DriverTable','')
CurrRunCycle = get_widget_value('CurrRunCycle','100')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

# FACETS Stage (ODBCConnector)
jdbc_url, jdbc_props = get_db_config(facets_secret_name)
facets_sql = f"""
SELECT DISTINCT  A.CLCL_ID, A.SUB_ID, B.LAST_NAME, B.FIRST_NAME, B.MID_INIT, B.BIRTH_DT as CLIS_BIRTH_DT
FROM (
    SELECT M.CLCL_ID,
           CLMI_ITS_SB_ID_ACT AS SUB_ID
    FROM {FacetsOwner}.CMC_CLMI_MISC M,
         {FacetsOwner}.CMC_CLCL_CLAIM C,
         tempdb..{DriverTable} TMP
    WHERE TMP.CLM_ID = M.CLCL_ID
      AND TMP.CLM_ID = C.CLCL_ID
      AND (CLMI_ITS_SUB_TYPE = 'S' OR CLMI_ITS_SUB_TYPE = 'E')
    GROUP BY M.CLCL_ID, CLMI_ITS_SB_ID_ACT

    UNION ALL

    SELECT M.CLCL_ID,
           CLMI_ITS_SBSB_ID AS SUB_ID
    FROM {FacetsOwner}.CMC_CLMI_MISC M,
         {FacetsOwner}.CMC_CLCL_CLAIM C,
         tempdb..{DriverTable} TMP
    WHERE TMP.CLM_ID = M.CLCL_ID
      AND TMP.CLM_ID = C.CLCL_ID
      AND (CLMI_ITS_SUB_TYPE = 'S' OR CLMI_ITS_SUB_TYPE = 'E')
    GROUP BY M.CLCL_ID, CLMI_ITS_SBSB_ID
) A
INNER JOIN
(
    SELECT S.CLCL_ID,
           CLIS_LAST_NAME AS LAST_NAME,
           CLIS_FIRST_NAME AS FIRST_NAME,
           CLIS_MID_INIT AS MID_INIT,
           CLIS_BIRTH_DT AS BIRTH_DT
    FROM {FacetsOwner}.CMC_CLIS_ITS_SUBSC S,
         {FacetsOwner}.CMC_CLCL_CLAIM C,
         tempdb..{DriverTable} TMP
    WHERE TMP.CLM_ID = S.CLCL_ID
      AND TMP.CLM_ID = C.CLCL_ID

    UNION ALL

    SELECT P.CLCL_ID,
           CLIP_LAST_NAME AS LAST_NAME,
           CLIP_FIRST_NAME AS FIRST_NAME,
           CLIP_MID_INIT AS MID_INIT,
           CLIP_BIRTH_DT  AS BIRTH_DT
    FROM {FacetsOwner}.CMC_CLIP_ITS_PATNT P,
         {FacetsOwner}.CMC_CLCL_CLAIM C,
         tempdb..{DriverTable} TMP
    WHERE TMP.CLM_ID = P.CLCL_ID
      AND TMP.CLM_ID = C.CLCL_ID
) B
ON A.CLCL_ID = B.CLCL_ID
"""
df_FACETS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", facets_sql)
    .load()
)

# Tfm_ITS_Mbr (CTransformerStage)
df_Tfm_ITS_Mbr = (
    df_FACETS
    .select(
        trim(col("CLCL_ID")).alias("CLM_ID"),
        lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        trim(col("SUB_ID")).alias("SUB_ID"),
        trim(col("FIRST_NAME")).alias("FIRST_NM"),
        col("MID_INIT").alias("MIDINIT"),
        trim(col("LAST_NAME")).alias("LAST_NM"),
        date_format(col("CLIS_BIRTH_DT"), "yyyy-MM-dd").alias("MBR_BRTH_DT_SK")
    )
)

# Write_Extrnl_Mbr (CSeqFileStage)
df_Write_Extrnl_Mbr = df_Tfm_ITS_Mbr.select(
    col("CLM_ID"),
    col("SRC_SYS_CD_SK"),
    rpad(col("SUB_ID"), 20, " "),
    col("FIRST_NM"),
    rpad(col("MIDINIT"), 1, " "),
    col("LAST_NM"),
    rpad(col("MBR_BRTH_DT_SK"), 10, " ")
)
write_files(
    df_Write_Extrnl_Mbr,
    f"{adls_path}/load/W_CLM_EXTRNL_MBR_DRVR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# MBR_BCBSA_SUPLMT (DB2Connector)
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
mbr_sql = f"""
SELECT DISTINCT BCBSA_ITS_SUB_ID,
       MBR_UNIQ_KEY,
       MBR_SK,
       BCBSA_MBR_FIRST_NM,
       BCBSA_MBR_MIDINIT,
       BCBSA_MBR_LAST_NM,
       BCBSA_MBR_GNDR_CD,
       CAST(BCBSA_MBR_BRTH_DT AS CHAR(10)) as BCBSA_MBR_BRTH_DT,
       BCBSA_MBR_RELSHP_CD
FROM {IDSOwner}.P_MBR_BCBSA_SUPLMT
"""
df_MBR_BCBSA_SUPLMT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", mbr_sql)
    .load()
)

# Tfm_bcbsa_mbr (CTransformerStage)
df_Tfm_bcbsa_mbr = (
    df_MBR_BCBSA_SUPLMT
    .select(
        trim(col("BCBSA_ITS_SUB_ID")).alias("BCBSA_ITS_SUB_ID"),
        col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        trim(col("BCBSA_MBR_FIRST_NM")).alias("BCBSA_MBR_FIRST_NM"),
        col("BCBSA_MBR_BRTH_DT").alias("BCBSA_MBR_BRTH_DT"),
        substring(trim(col("BCBSA_MBR_MIDINIT")), 1, 1).alias("BCBSA_MBR_MIDINIT"),
        trim(col("BCBSA_MBR_LAST_NM")).alias("BCBSA_MBR_LAST_NM"),
        trim(col("BCBSA_MBR_GNDR_CD")).alias("BCBSA_MBR_GNDR_CD"),
        trim(col("BCBSA_MBR_RELSHP_CD")).alias("BCBSA_MBR_RELSHP_CD"),
        col("MBR_SK").alias("MBR_SK")
    )
)

# Write_BCBSA_Mbr (CSeqFileStage)
df_Write_BCBSA_Mbr = df_Tfm_bcbsa_mbr.select(
    col("BCBSA_ITS_SUB_ID"),
    col("MBR_UNIQ_KEY"),
    col("BCBSA_MBR_FIRST_NM"),
    rpad(col("BCBSA_MBR_BRTH_DT"), 10, " "),
    rpad(col("BCBSA_MBR_MIDINIT"), 1, " "),
    col("BCBSA_MBR_LAST_NM"),
    col("BCBSA_MBR_GNDR_CD"),
    col("BCBSA_MBR_RELSHP_CD"),
    col("MBR_SK")
)
write_files(
    df_Write_BCBSA_Mbr,
    f"{adls_path}/load/W_CLM_EXTRNL_BCBSA_MBR_DRVR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)