# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2020 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY :  GxOptumAccumReconSeq
# MAGIC 
# MAGIC  
# MAGIC DESCRIPTION:      Facets Accum Data processing for Staging Load                               
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                         Date                 Project/Altiris #                           Change Description                                                                                Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------                      --------------------     ------------------------                            -----------------------------------------------------------------------                                    --------------------------------       -------------------------------   ----------------------------      
# MAGIC Rekha Radhakrishna      10/01/2020    OPTUMRX Accum Exchange            Initial Programming                                                                                 IntegrateDev2                 Jaideep Mankala        11/19/2020
# MAGIC Rekha Radhakrishna      10/01/2020    OPTUMRX Accum Exchange           Changed FACETS queries                                                                      IntegrateDev2                 Manasa Andru               2020-12-28
# MAGIC Rekha Radhakrishna      10/01/2020    OPTUMRX Accum Exchange          Changed BENEFIT BEGIN DATE logic  in all 4 Facets Queries               IntegrateDev2           Jaideep Mankala       01/12/2021  
# MAGIC Sudeep Reddy                26/04/2022     US489715                                       Added PDBC_PFX value ('0013','0014','0015')  for                                   IntegrateDev2          Reddy Sanam            05/09/2022
# MAGIC                                                                                                                  ACAC_ACC_NO = 5 and removed PDBC_PFX value ('0013','0014','0015') 
# MAGIC                                                                                                                   for  ACAC_ACC_NO = 1
# MAGIC Rekha R                         2022-03-25         S2S Remediation                       Changed Sybase SQL syntaxes to SQL Server                                           IntegrateDev5\(9)\(9)Harsha Ravuri\(9)05-28-2022\(9) 
# MAGIC                                                                                                                      syntaxes for concatenation etc                          
# MAGIC  
# MAGIC Ashok kumar Baskaran      2024-07-22    624048-Fix OOP indv Total amt     Changed facts SQL to derive oop indv total amount                                   IntegrateDev2\(9)Jeyaprasanna         2024-07-22\(9)\(9)

# MAGIC Facets (MATX and FATX Accum Data Staging for PBM)
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import types as T
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# ----------------------------------------------------------------
# Define all required parameters and their corresponding secret_name if Owner is found
# ----------------------------------------------------------------
GxCAccumReconOwner = get_widget_value("GxCAccumReconOwner","")
gxcaccumrecon_secret_name = get_widget_value("gxcaccumrecon_secret_name","")
GxCProductOwner = get_widget_value("GxCProductOwner","")
gxcproduct_secret_name = get_widget_value("gxcproduct_secret_name","")
FacetsDB = get_widget_value("FacetsDB","")
FacetsOwner = get_widget_value("FacetsOwner","")
facets_secret_name = get_widget_value("facets_secret_name","")
FacetsCutOffDays = get_widget_value("FacetsCutOffDays","")

# ----------------------------------------------------------------
# Stage: Gx_Product_PlanLimit (ODBCConnectorPX)
# ----------------------------------------------------------------
jdbc_url_GxCProduct, jdbc_props_GxCProduct = get_db_config(gxcproduct_secret_name)

extract_query_Gx_Product_PlanLimit = f"""
SELECT [LOB],
       [PLANID],
       [CLIENT],
       [PLANNAME],
       EFF_STR_DT,
       EFF_END_DT,
       ACCUM_TYPE,
       PLANVALUE
FROM
(
    SELECT [LOB],
           [PLANID],
           [CLIENT],
           [PLANNAME],
           EFF_STR_DT,
           EFF_END_DT,
           ACCUM_TYPE,
           PLANVALUE,
           RANK() OVER ( PARTITION BY [PLANNAME], EFF_STR_DT, EFF_END_DT, ACCUM_TYPE ORDER BY PLANID ) RNK
    FROM 
    (
      SELECT [LOB],
             [PLANID],
             [CLIENT],
             [PLANNAME],
             CONVERT(DATETIME, [EFFECTIVESTARTDATE], 102) AS EFF_STR_DT,
             CONVERT(DATETIME, ISNULL(EFFECTIVEENDDATE ,'12/31/2199'), 102) AS EFF_END_DT,
             CASE 
               WHEN ATTRIBUTE = 'Individual Out of Pocket Maximum' THEN 'MOOP'
               WHEN ATTRIBUTE = 'Family Out of Pocket Maximum' THEN 'FOOP'
               WHEN ATTRIBUTE = 'Individual Deductible Amount' THEN 'MDED'
               WHEN ATTRIBUTE = 'Family Deductible Amount' THEN 'FDED'
               ELSE 'NA'
             END AS ACCUM_TYPE,
             CASE 
               WHEN TRIM(PLANVALUE) = '' THEN 0
               ELSE ISNULL(TRIM(PLANVALUE), 0)
             END PLANVALUE
      FROM {GxCProductOwner}.[VW_PLANLIMITS]
      WHERE ([LOB] = 'HIX' AND [CLIENT] LIKE '%Standard%')
            OR ([LOB] NOT LIKE 'HIX')
    ) SRC1
    WHERE SRC1.ACCUM_TYPE <> 'NA'
) SRC
WHERE RNK = 1
"""

df_Gx_Product_PlanLimit = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_GxCProduct)
    .options(**jdbc_props_GxCProduct)
    .option("query", extract_query_Gx_Product_PlanLimit)
    .load()
)

# ----------------------------------------------------------------
# Stage: SS_FATX_DED (ODBCConnectorPX)
# ----------------------------------------------------------------
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)

extract_query_SS_FATX_DED = f"""
set forceplan on
SELECT
T.MEME_CK, 
CONCAT(S.SBSB_ID, RIGHT(CONCAT('00',CAST(M.MEME_SFX AS VARCHAR)),2)) AS MEME_ID, 
S.SBSB_CK, 
S.SBSB_ID, 
M.MEME_SFX, 
M.MEME_LAST_NAME, 
M.MEME_FIRST_NAME, 
CURR_PLN.PDPD_ID, 
ISNULL(TRIM(CURR_CMP.PDBC_PFX), 'NA') AS CMPT_ID,
BPL.PDBC_PFX AS BPL_NBR, 
G.GRGR_ID,
G.GRGR_NAME,
G.GRGR_MCTR_TYPE,
'FDED' AS ACCUM_TYPE, 
1 AS ACAC_ACC_NO, 
CAST(T.FATX_BEN_BEG_DT AS datetime) AS BENEFIT_BEG_DT,
MAX(T.FATX_SEQ_NO) AS SEQ_NO, 
MAX(T.FATX_CREATE_DTM) AS LAST_UPDATE_DTM,
CAST(SUM(T.FATX_AMT1) AS DECIMAL(13,2)) AS ACCUM_TTL,
SUM(L.PBM_CT) AS PBM_CT,

CASE WHEN G.CICI_ID = 'MA' THEN 'BKCMEDD'
     WHEN G.GRGR_ID = '10001000' AND SUBSTRING(CURR_PLN.PDPD_ID,3,1) = 'M' AND SUBSTRING(BPL.PDBC_PFX,1,1) in ('8','7') THEN 'BKCM34762'
     WHEN G.GRGR_ID = '10001000' AND SUBSTRING(CURR_PLN.PDPD_ID,3,1) = 'K' AND SUBSTRING(BPL.PDBC_PFX,1,1) in ('8','7') THEN 'BKCK94248'
     ELSE 'BKC1' 
END AS CAR_ID,

CURR_PLN.MEPE_ELIG_IND,
CURR_PLN.MEPE_EFF_DT,
CURR_PLN.MEPE_TERM_DT,
COUNT(CMP.PDBC_PFX) AS PROD_CT,
CONCAT('BKC',BPL.PDBC_PFX) AS PLANNAME

FROM {FacetsOwner}.CMC_FATX_ACCUM_TXN T

INNER JOIN {FacetsOwner}.CMC_MEME_MEMBER M 
ON M.MEME_CK = T.MEME_CK

INNER JOIN {FacetsOwner}.CMC_MEPE_PRCS_ELIG E 
ON M.MEME_CK = E.MEME_CK
AND CAST(T.FATX_CREATE_DTM AS DATE) BETWEEN E.MEPE_EFF_DT AND E.MEPE_TERM_DT
AND E.CSPD_CAT = 'M'

INNER JOIN {FacetsOwner}.CMC_SBSB_SUBSC S 
ON M.SBSB_CK = S.SBSB_CK

INNER JOIN {FacetsOwner}.CMC_GRGR_GROUP G 
ON M.GRGR_CK = G.GRGR_CK

INNER JOIN {FacetsOwner}.CMC_PDBC_PROD_COMP CMP 
ON E.PDPD_ID = CMP.PDPD_ID
AND CAST(T.FATX_CREATE_DTM AS DATE) BETWEEN CMP.PDBC_EFF_DT AND CMP.PDBC_TERM_DT
AND CMP.PDBC_TYPE = 'RX'

INNER JOIN {FacetsOwner}.CMC_MEPE_PRCS_ELIG CURR_PLN 
ON M.MEME_CK = CURR_PLN.MEME_CK
AND CAST(GETDATE() AS DATE) BETWEEN CURR_PLN.MEPE_EFF_DT AND CURR_PLN.MEPE_TERM_DT
AND CURR_PLN.CSPD_CAT = 'M'

INNER JOIN {FacetsOwner}.CMC_PDBC_PROD_COMP CURR_CMP 
ON CURR_PLN.PDPD_ID = CURR_CMP.PDPD_ID
AND CAST(GETDATE() AS DATE) BETWEEN CURR_CMP.PDBC_EFF_DT AND CURR_CMP.PDBC_TERM_DT
AND CURR_CMP.PDBC_TYPE = 'RX'

INNER JOIN 
(
    SELECT * 
    FROM {FacetsOwner}.CMC_PDBC_PROD_COMP
    WHERE PDBC_TYPE = 'BPL'
      AND GETDATE() BETWEEN PDBC_EFF_DT AND PDBC_TERM_DT
) BPL 
ON CURR_PLN.PDPD_ID = BPL.PDPD_ID

INNER JOIN 
(
    SELECT M.SBSB_CK, M.ACAC_ACC_NO, M.FATX_BEN_BEG_DT, MAX(M.FATX_SEQ_NO) AS FATX_SEQ_NO, 0 AS PBM_CT
    FROM {FacetsOwner}.CMC_FATX_ACCUM_TXN M
    INNER JOIN {FacetsOwner}.CMC_GRGR_GROUP G ON M.GRGR_CK = G.GRGR_CK
    WHERE M.FATX_ACC_TYPE = 'D'
      AND M.PDPD_ACC_SFX = 'MED'
      AND M.ACAC_ACC_NO IN (1,53)
      AND M.FATX_CREATE_DTM < CONCAT(CONVERT(CHAR(10),DATEADD(DAY,-{FacetsCutOffDays}, GETDATE() ), 23 ),' ','23:59:59.000')
      AND M.FATX_BEN_BEG_DT >=  convert(char(7),convert(char(10),dateadd(month, 1, dateadd(year,-1, dateadd(day, -1, getdate()))), 23))+ '-01'
    GROUP BY M.SBSB_CK, M.ACAC_ACC_NO, M.FATX_BEN_BEG_DT
) L
ON T.SBSB_CK = L.SBSB_CK
AND T.ACAC_ACC_NO = L.ACAC_ACC_NO
AND T.FATX_SEQ_NO = L.FATX_SEQ_NO

WHERE T.FATX_ACC_TYPE = 'D'
AND PDPD_ACC_SFX = 'MED'
AND T.FATX_BEN_BEG_DT >=  convert(char(7),convert(char(10),dateadd(month, 1, dateadd(year,-1, dateadd(day, -1, getdate()))), 23))+ '-01'
AND 
(
   (T.ACAC_ACC_NO = 53 AND CMP.PDBC_PFX IN ('0005','0008','0009','0010','0015'))
   OR
   (T.ACAC_ACC_NO = 1 AND CMP.PDBC_PFX IN ('0001','0002','0003','0004','0013','0007','0017'))
)
GROUP BY 
T.MEME_CK, 
CONCAT(S.SBSB_ID, RIGHT(CONCAT('00',CAST(M.MEME_SFX AS VARCHAR)),2)),
S.SBSB_CK, 
S.SBSB_ID, 
M.MEME_SFX, 
M.MEME_LAST_NAME, 
M.MEME_FIRST_NAME, 
CURR_PLN.PDPD_ID, 
CURR_CMP.PDBC_PFX,
BPL.PDBC_PFX, 
G.GRGR_ID,
G.GRGR_NAME,
G.GRGR_MCTR_TYPE, 
G.CICI_ID, 
T.FATX_BEN_BEG_DT,
CURR_PLN.MEPE_ELIG_IND,
CURR_PLN.MEPE_EFF_DT,
CURR_PLN.MEPE_TERM_DT
"""

df_SS_FATX_DED = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_SS_FATX_DED)
    .load()
)

# ----------------------------------------------------------------
# Stage: SS_FATX_OOP (ODBCConnectorPX)
# ----------------------------------------------------------------
extract_query_SS_FATX_OOP = f"""
set forceplan on
SELECT
T.MEME_CK, 
CONCAT(S.SBSB_ID, RIGHT(CONCAT('00',CAST(M.MEME_SFX AS VARCHAR)),2)) AS MEME_ID, 
S.SBSB_CK, 
S.SBSB_ID, 
M.MEME_SFX, 
M.MEME_LAST_NAME, 
M.MEME_FIRST_NAME, 
CURR_PLN.PDPD_ID, 
ISNULL(TRIM(CURR_CMP.PDBC_PFX), 'NA') AS CMPT_ID,
BPL.PDBC_PFX AS BPL_NBR, 
G.GRGR_ID,
G.GRGR_NAME,
G.GRGR_MCTR_TYPE,
'FOOP' AS ACCUM_TYPE, 
1 AS ACAC_ACC_NO, 
T.FATX_BEN_BEG_DT AS BENEFIT_BEG_DT,
MAX(T.FATX_SEQ_NO) AS SEQ_NO, 
MAX(T.FATX_CREATE_DTM) AS LAST_UPDATE_DTM,
CAST(SUM(T.FATX_AMT1) AS DECIMAL(13,2)) AS ACCUM_TTL,
SUM(L.PBM_CT) AS PBM_CT,

CASE WHEN G.CICI_ID = 'MA' THEN 'BKCMEDD'
     WHEN G.GRGR_ID = '10001000' AND SUBSTRING(CURR_PLN.PDPD_ID,3,1) = 'M' AND SUBSTRING(BPL.PDBC_PFX,1,1) in ('8','7') THEN 'BKCM34762'
     WHEN G.GRGR_ID = '10001000' AND SUBSTRING(CURR_PLN.PDPD_ID,3,1) = 'K' AND SUBSTRING(BPL.PDBC_PFX,1,1) in ('8','7') THEN 'BKCK94248'
     ELSE 'BKC1'
END AS CAR_ID,

CURR_PLN.MEPE_ELIG_IND,
CURR_PLN.MEPE_EFF_DT,
CURR_PLN.MEPE_TERM_DT,
COUNT(CMP.PDBC_PFX) AS PROD_CT,
CONCAT('BKC',BPL.PDBC_PFX) AS PLANNAME

FROM {FacetsOwner}.CMC_FATX_ACCUM_TXN T

INNER JOIN {FacetsOwner}.CMC_MEME_MEMBER M 
ON M.MEME_CK = T.MEME_CK

INNER JOIN {FacetsOwner}.CMC_MEPE_PRCS_ELIG E 
ON M.MEME_CK = E.MEME_CK
AND CAST(T.FATX_CREATE_DTM AS DATE) BETWEEN E.MEPE_EFF_DT AND E.MEPE_TERM_DT
AND E.CSPD_CAT = 'M'

INNER JOIN {FacetsOwner}.CMC_SBSB_SUBSC S 
ON M.SBSB_CK = S.SBSB_CK

INNER JOIN {FacetsOwner}.CMC_GRGR_GROUP G 
ON M.GRGR_CK = G.GRGR_CK

INNER JOIN {FacetsOwner}.CMC_PDBC_PROD_COMP CMP 
ON E.PDPD_ID = CMP.PDPD_ID
AND CAST(T.FATX_CREATE_DTM AS DATE) BETWEEN CMP.PDBC_EFF_DT AND CMP.PDBC_TERM_DT
AND CMP.PDBC_TYPE = 'RX'

INNER JOIN {FacetsOwner}.CMC_MEPE_PRCS_ELIG CURR_PLN 
ON M.MEME_CK = CURR_PLN.MEME_CK
AND CAST(GETDATE() AS DATE) BETWEEN CURR_PLN.MEPE_EFF_DT AND CURR_PLN.MEPE_TERM_DT
AND CURR_PLN.CSPD_CAT = 'M'

INNER JOIN {FacetsOwner}.CMC_PDBC_PROD_COMP CURR_CMP 
ON CURR_PLN.PDPD_ID = CURR_CMP.PDPD_ID
AND CAST(GETDATE() AS DATE) BETWEEN CURR_CMP.PDBC_EFF_DT AND CURR_CMP.PDBC_TERM_DT
AND CURR_CMP.PDBC_TYPE = 'RX'

INNER JOIN
(
   SELECT * 
   FROM {FacetsOwner}.CMC_PDBC_PROD_COMP
   WHERE PDBC_TYPE = 'BPL'
     AND GETDATE() BETWEEN PDBC_EFF_DT AND PDBC_TERM_DT
) BPL
ON CURR_PLN.PDPD_ID = BPL.PDPD_ID

INNER JOIN
(
    SELECT M.SBSB_CK, M.ACAC_ACC_NO, M.FATX_BEN_BEG_DT, MAX(M.FATX_SEQ_NO) AS FATX_SEQ_NO, 0 AS PBM_CT
    FROM {FacetsOwner}.CMC_FATX_ACCUM_TXN M
    INNER JOIN {FacetsOwner}.CMC_GRGR_GROUP G ON M.GRGR_CK = G.GRGR_CK
    WHERE M.FATX_ACC_TYPE = 'L'
      AND M.PDPD_ACC_SFX = 'MED'
      AND M.ACAC_ACC_NO IN (2,9,8)
      AND M.FATX_CREATE_DTM < CONCAT(CONVERT(CHAR(10),DATEADD(DAY,-{FacetsCutOffDays}, GETDATE() ), 23 ),' ','23:59:59.000')
      AND M.FATX_BEN_BEG_DT >=  convert(char(7),convert(char(10),dateadd(month, 1, dateadd(year,-1, dateadd(day, -1, getdate()))), 23))+ '-01'
    GROUP BY M.SBSB_CK, M.ACAC_ACC_NO, M.FATX_BEN_BEG_DT
) L
ON T.SBSB_CK = L.SBSB_CK
AND T.ACAC_ACC_NO = L.ACAC_ACC_NO
AND T.FATX_SEQ_NO = L.FATX_SEQ_NO

WHERE T.FATX_ACC_TYPE = 'L'
AND PDPD_ACC_SFX = 'MED'
AND T.FATX_BEN_BEG_DT >= convert(char(7),convert(char(10),dateadd(month, 1, dateadd(year,-1, dateadd(day, -1, getdate()))), 23))+ '-01'
AND
(
  (T.ACAC_ACC_NO = 2 AND CMP.PDBC_PFX IN ('0001','0002','0003','0004','0009','0011','0013','0014'))
  OR
  (T.ACAC_ACC_NO = 9 AND CMP.PDBC_PFX IN ('0007','0008','0012','0015'))
  OR
  (T.ACAC_ACC_NO = 8 AND CMP.PDBC_PFX = '0010')
)
GROUP BY
T.MEME_CK, 
CONCAT(S.SBSB_ID, RIGHT(CONCAT('00',CAST(M.MEME_SFX AS VARCHAR)),2)), 
S.SBSB_CK, 
S.SBSB_ID, 
M.MEME_SFX, 
M.MEME_LAST_NAME, 
M.MEME_FIRST_NAME, 
CURR_PLN.PDPD_ID, 
CURR_CMP.PDBC_PFX,
BPL.PDBC_PFX, 
G.GRGR_ID,
G.GRGR_NAME,
G.GRGR_MCTR_TYPE, 
G.CICI_ID, 
T.FATX_BEN_BEG_DT,
CURR_PLN.MEPE_ELIG_IND,
CURR_PLN.MEPE_EFF_DT,
CURR_PLN.MEPE_TERM_DT
"""

df_SS_FATX_OOP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_SS_FATX_OOP)
    .load()
)

# ----------------------------------------------------------------
# Stage: SS_MATX_DED (ODBCConnectorPX)
# ----------------------------------------------------------------
extract_query_SS_MATX_DED = f"""
set forceplan on
SELECT
T.MEME_CK, 
CONCAT(S.SBSB_ID, RIGHT(CONCAT('00',CAST(M.MEME_SFX AS VARCHAR)),2)) AS MEME_ID, 
S.SBSB_CK, 
S.SBSB_ID, 
M.MEME_SFX, 
M.MEME_LAST_NAME, 
M.MEME_FIRST_NAME, 
CURR_PLN.PDPD_ID, 
ISNULL(TRIM(CURR_CMP.PDBC_PFX), 'NA') AS CMPT_ID,
BPL.PDBC_PFX AS BPL_NBR, 
G.GRGR_ID,
G.GRGR_NAME,
G.GRGR_MCTR_TYPE,
'MDED' AS ACCUM_TYPE, 
1 AS ACAC_ACC_NO, 
CAST(T.MATX_BEN_BEG_DT AS datetime) AS BENEFIT_BEG_DT,
MAX(T.MATX_SEQ_NO) AS SEQ_NO, 
MAX(T.MATX_CREATE_DTM) AS LAST_UPDATE_DTM,
CAST(SUM(T.MATX_AMT1) AS DECIMAL(13,2)) AS ACCUM_TTL,
SUM(L.PBM_CT) AS PBM_CT,

CASE WHEN G.CICI_ID = 'MA' THEN 'BKCMEDD'
     WHEN G.GRGR_ID = '10001000' AND SUBSTRING(CURR_PLN.PDPD_ID,3,1) = 'M' AND SUBSTRING(BPL.PDBC_PFX,1,1) in ('8','7') THEN 'BKCM34762'
     WHEN G.GRGR_ID = '10001000' AND SUBSTRING(CURR_PLN.PDPD_ID,3,1) = 'K' AND SUBSTRING(BPL.PDBC_PFX,1,1) in ('8','7') THEN 'BKCK94248'
     ELSE 'BKC1' 
END AS CAR_ID,

CURR_PLN.MEPE_ELIG_IND,
CURR_PLN.MEPE_EFF_DT,
CURR_PLN.MEPE_TERM_DT,
COUNT(CMP.PDBC_PFX) AS PROD_CT,
CONCAT('BKC',BPL.PDBC_PFX) AS PLANNAME

FROM {FacetsOwner}.CMC_MATX_ACCUM_TXN T

INNER JOIN {FacetsOwner}.CMC_MEME_MEMBER M 
ON M.MEME_CK = T.MEME_CK

INNER JOIN {FacetsOwner}.CMC_MEPE_PRCS_ELIG E 
ON M.MEME_CK = E.MEME_CK
AND CAST(T.MATX_CREATE_DTM AS DATE) BETWEEN E.MEPE_EFF_DT AND E.MEPE_TERM_DT
AND E.CSPD_CAT = 'M'

INNER JOIN {FacetsOwner}.CMC_SBSB_SUBSC S 
ON M.SBSB_CK = S.SBSB_CK

INNER JOIN {FacetsOwner}.CMC_GRGR_GROUP G 
ON M.GRGR_CK = G.GRGR_CK

INNER JOIN {FacetsOwner}.CMC_PDBC_PROD_COMP CMP 
ON E.PDPD_ID = CMP.PDPD_ID
AND CAST(T.MATX_CREATE_DTM AS DATE) BETWEEN CMP.PDBC_EFF_DT AND CMP.PDBC_TERM_DT
AND CMP.PDBC_TYPE = 'RX'

INNER JOIN {FacetsOwner}.CMC_MEPE_PRCS_ELIG CURR_PLN 
ON M.MEME_CK = CURR_PLN.MEME_CK
AND CAST(GETDATE() AS DATE) BETWEEN CURR_PLN.MEPE_EFF_DT AND CURR_PLN.MEPE_TERM_DT
AND CURR_PLN.CSPD_CAT = 'M'

INNER JOIN {FacetsOwner}.CMC_PDBC_PROD_COMP CURR_CMP 
ON CURR_PLN.PDPD_ID = CURR_CMP.PDPD_ID
AND CAST(GETDATE() AS DATE) BETWEEN CURR_CMP.PDBC_EFF_DT AND CURR_CMP.PDBC_TERM_DT
AND CURR_CMP.PDBC_TYPE = 'RX'

INNER JOIN 
(
    SELECT * 
    FROM {FacetsOwner}.CMC_PDBC_PROD_COMP
    WHERE PDBC_TYPE = 'BPL'
      AND GETDATE() BETWEEN PDBC_EFF_DT AND PDBC_TERM_DT
) BPL 
ON CURR_PLN.PDPD_ID = BPL.PDPD_ID

INNER JOIN
(
    SELECT M.MEME_CK, M.ACAC_ACC_NO, M.MATX_BEN_BEG_DT, MAX(M.MATX_SEQ_NO) AS MATX_SEQ_NO, 
           SUM(CASE WHEN M.MATX_MCTR_RSN = 'PBM1' THEN 1 ELSE 0 END) AS PBM_CT
    FROM {FacetsOwner}.CMC_MATX_ACCUM_TXN M
    INNER JOIN {FacetsOwner}.CMC_GRGR_GROUP G ON M.GRGR_CK = G.GRGR_CK
    WHERE M.MATX_ACC_TYPE = 'D' 
      AND M.ACAC_ACC_NO IN (1,53)
      AND M.PDPD_ACC_SFX = 'MED'
      AND M.MATX_CREATE_DTM < CONCAT(CONVERT(CHAR(10),DATEADD(DAY,-{FacetsCutOffDays}, GETDATE() ), 23 ),' ','23:59:59.000')
      AND M.MATX_BEN_BEG_DT >= convert(char(7),convert(char(10),dateadd(month, 1, dateadd(year,-1, dateadd(day, -1, getdate()))), 23))+ '-01'
    GROUP BY M.MEME_CK, M.ACAC_ACC_NO, M.MATX_BEN_BEG_DT
) L 
ON T.MEME_CK = L.MEME_CK 
AND T.ACAC_ACC_NO = L.ACAC_ACC_NO 
AND T.MATX_SEQ_NO = L.MATX_SEQ_NO

WHERE T.MATX_ACC_TYPE = 'D'
AND PDPD_ACC_SFX = 'MED'
AND T.MATX_BEN_BEG_DT >= convert(char(7),convert(char(10),dateadd(month, 1, dateadd(year,-1, dateadd(day, -1, getdate()))), 23))+ '-01'
AND
(
  (T.ACAC_ACC_NO = 53 AND CMP.PDBC_PFX IN ('0005','0008','0009','0010','0015'))
  OR
  (T.ACAC_ACC_NO = 1 AND CMP.PDBC_PFX IN ('0001','0002','0003','0004','0013','0007','0017'))
)
GROUP BY 
T.MEME_CK, 
CONCAT(S.SBSB_ID, RIGHT(CONCAT('00',CAST(M.MEME_SFX AS VARCHAR)),2)) ,
S.SBSB_CK, 
S.SBSB_ID, 
M.MEME_SFX, 
M.MEME_LAST_NAME, 
M.MEME_FIRST_NAME, 
CURR_PLN.PDPD_ID, 
CURR_CMP.PDBC_PFX,
BPL.PDBC_PFX, 
G.GRGR_ID,
G.GRGR_NAME,
G.GRGR_MCTR_TYPE, 
G.CICI_ID,
T.MATX_BEN_BEG_DT,
CURR_PLN.MEPE_ELIG_IND,
CURR_PLN.MEPE_EFF_DT,
CURR_PLN.MEPE_TERM_DT
"""

df_SS_MATX_DED = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_SS_MATX_DED)
    .load()
)

# ----------------------------------------------------------------
# Stage: SS_MATX_OOP (ODBCConnectorPX)
# ----------------------------------------------------------------
extract_query_SS_MATX_OOP = f"""
set forceplan on
select MEME_CK,MEME_ID,SBSB_CK,SBSB_ID,MEME_SFX,MEME_LAST_NAME,MEME_FIRST_NAME,
PDPD_ID,CMPT_ID,BPL_NBR,GRGR_ID,GRGR_NAME,GRGR_MCTR_TYPE,ACCUM_TYPE,1 AS ACAC_ACC_NO,
BENEFIT_BEG_DT,SEQ_NO,LAST_UPDATE_DTM,ACCUM_TTL,PBM_CT,CAR_ID,MEPE_ELIG_IND,MEPE_EFF_DT,
MEPE_TERM_DT,PROD_CT,PLANNAME
from
(
  select MEME_CK,MEME_ID,SBSB_CK,SBSB_ID,MEME_SFX,MEME_LAST_NAME,MEME_FIRST_NAME,
         PDPD_ID,CMPT_ID,BPL_NBR,GRGR_ID,GRGR_NAME,GRGR_MCTR_TYPE,ACCUM_TYPE,
         1 AS ACAC_ACC_NO,BENEFIT_BEG_DT,MAX(SEQ_NO) as SEQ_NO,LAST_UPDATE_DTM,
         ACCUM_TTL,PBM_CT,CAR_ID,MEPE_ELIG_IND,MEPE_EFF_DT,MEPE_TERM_DT,
         PROD_CT,PLANNAME,
         ROW_NUMBER() OVER(
            PARTITION BY MEME_CK,MEME_ID,SBSB_CK,SBSB_ID,MEME_SFX,MEME_LAST_NAME,
                         MEME_FIRST_NAME,PDPD_ID,CMPT_ID,PLANNAME
            ORDER BY LAST_UPDATE_DTM DESC
         ) AS RNK
  from
  (
    SELECT
    T.MEME_CK, 
    CONCAT(S.SBSB_ID, RIGHT(CONCAT('00',CAST(M.MEME_SFX AS VARCHAR)),2)) AS MEME_ID, 
    S.SBSB_CK, 
    S.SBSB_ID, 
    M.MEME_SFX, 
    M.MEME_LAST_NAME, 
    M.MEME_FIRST_NAME, 
    CURR_PLN.PDPD_ID, 
    ISNULL(TRIM(CURR_CMP.PDBC_PFX), 'NA') AS CMPT_ID,
    BPL.PDBC_PFX AS BPL_NBR, 
    G.GRGR_ID,
    G.GRGR_NAME,
    G.GRGR_MCTR_TYPE,
    'MOOP' AS ACCUM_TYPE,
    T.ACAC_ACC_NO AS ACAC_ACC_NO,
    CAST(T.MATX_BEN_BEG_DT AS datetime) AS BENEFIT_BEG_DT,
    MAX(T.MATX_SEQ_NO) AS SEQ_NO, 
    MAX(T.MATX_CREATE_DTM) AS LAST_UPDATE_DTM,
    CAST(SUM(T.MATX_AMT1) AS DECIMAL(13,2)) AS ACCUM_TTL,
    SUM(L.PBM_CT) AS PBM_CT,
    
    CASE WHEN G.CICI_ID = 'MA' THEN 'BKCMEDD'
         WHEN G.GRGR_ID = '10001000' AND SUBSTRING(CURR_PLN.PDPD_ID,3,1) = 'M' AND SUBSTRING(BPL.PDBC_PFX,1,1) in ('8','7') THEN 'BKCM34762'
         WHEN G.GRGR_ID = '10001000' AND SUBSTRING(CURR_PLN.PDPD_ID,3,1) = 'K' AND SUBSTRING(BPL.PDBC_PFX,1,1) in ('8','7') THEN 'BKCK94248'
         ELSE 'BKC1' 
    END AS CAR_ID,
    
    CURR_PLN.MEPE_ELIG_IND,
    CURR_PLN.MEPE_EFF_DT,
    CURR_PLN.MEPE_TERM_DT,
    COUNT(CMP.PDBC_PFX) AS PROD_CT,
    CONCAT('BKC',BPL.PDBC_PFX) AS PLANNAME
    
    FROM {FacetsOwner}.CMC_MATX_ACCUM_TXN T
    
    INNER JOIN {FacetsOwner}.CMC_MEME_MEMBER M 
    ON M.MEME_CK = T.MEME_CK
    
    INNER JOIN 
    (
      SELECT MEME_CK, MEPE_EFF_DT, MEPE_TERM_DT , PDPD_ID 
      FROM {FacetsOwner}.CMC_MEPE_PRCS_ELIG 
      WHERE CSPD_CAT = 'M'
    ) E
    ON M.MEME_CK = E.MEME_CK
    AND CAST(T.MATX_CREATE_DTM AS DATE) BETWEEN E.MEPE_EFF_DT AND E.MEPE_TERM_DT
    
    INNER JOIN {FacetsOwner}.CMC_SBSB_SUBSC S 
    ON M.SBSB_CK = S.SBSB_CK
    
    INNER JOIN {FacetsOwner}.CMC_GRGR_GROUP G 
    ON M.GRGR_CK = G.GRGR_CK
    
    INNER JOIN {FacetsOwner}.CMC_PDBC_PROD_COMP CMP 
    ON E.PDPD_ID = CMP.PDPD_ID
    AND CAST(T.MATX_CREATE_DTM AS DATE) BETWEEN CMP.PDBC_EFF_DT AND CMP.PDBC_TERM_DT
    AND CMP.PDBC_TYPE = 'RX'
    
    INNER JOIN {FacetsOwner}.CMC_MEPE_PRCS_ELIG CURR_PLN 
    ON M.MEME_CK = CURR_PLN.MEME_CK
    AND CAST(GETDATE() AS DATE) BETWEEN CURR_PLN.MEPE_EFF_DT AND CURR_PLN.MEPE_TERM_DT
    AND CURR_PLN.CSPD_CAT = 'M'
    
    INNER JOIN {FacetsOwner}.CMC_PDBC_PROD_COMP CURR_CMP 
    ON CURR_PLN.PDPD_ID = CURR_CMP.PDPD_ID
    AND CAST(GETDATE() AS DATE) BETWEEN CURR_CMP.PDBC_EFF_DT AND CURR_CMP.PDBC_TERM_DT
    AND CURR_CMP.PDBC_TYPE = 'RX'
    
    INNER JOIN 
    (
      SELECT * 
      FROM {FacetsOwner}.CMC_PDBC_PROD_COMP
      WHERE PDBC_TYPE = 'BPL'
        AND GETDATE() BETWEEN PDBC_EFF_DT AND PDBC_TERM_DT
    ) BPL 
    ON CURR_PLN.PDPD_ID = BPL.PDPD_ID
    
    INNER JOIN
    (
      SELECT M.MEME_CK, M.ACAC_ACC_NO, M.MATX_BEN_BEG_DT, MAX(M.MATX_SEQ_NO) AS MATX_SEQ_NO, 
             SUM(CASE WHEN M.MATX_MCTR_RSN = 'PBM1' THEN 1 ELSE 0 END) AS PBM_CT
      FROM {FacetsOwner}.CMC_MATX_ACCUM_TXN M
      INNER JOIN {FacetsOwner}.CMC_GRGR_GROUP G ON M.GRGR_CK = G.GRGR_CK
      WHERE M.MATX_ACC_TYPE = 'L'
        AND M.PDPD_ACC_SFX = 'MED'
        AND M.ACAC_ACC_NO IN (1,5,7)
        AND M.MATX_CREATE_DTM < CONCAT(CONVERT(CHAR(10),DATEADD(DAY,-{FacetsCutOffDays}, GETDATE() ), 23 ),' ','23:59:59.000')
        AND M.MATX_BEN_BEG_DT >= convert(char(7),convert(char(10),dateadd(month, 1, dateadd(year,-1, dateadd(day, -1, getdate()))), 23))+ '-01'
      GROUP BY M.MEME_CK, M.ACAC_ACC_NO, M.MATX_BEN_BEG_DT
    ) L
    ON T.MEME_CK = L.MEME_CK 
    AND T.ACAC_ACC_NO = L.ACAC_ACC_NO 
    AND T.MATX_SEQ_NO = L.MATX_SEQ_NO
    
    WHERE T.MATX_ACC_TYPE = 'L'
    AND PDPD_ACC_SFX = 'MED'
    AND T.MATX_BEN_BEG_DT >= convert(char(7),convert(char(10),dateadd(month, 1, dateadd(year,-1, dateadd(day, -1, getdate()))), 23))+ '-01'
    AND
    (
      (T.ACAC_ACC_NO = 5 AND CMP.PDBC_PFX IN ('0005','0007','0008','0012','0013','0014','0015','0019'))
      OR
      (T.ACAC_ACC_NO = 1 AND CMP.PDBC_PFX IN ('0001','0002','0003','0004','0009','0011','0016','0017','0018'))
      OR
      (T.ACAC_ACC_NO = 7 AND CMP.PDBC_PFX = '0010')
    )
    GROUP BY 
    T.MEME_CK, 
    CONCAT(S.SBSB_ID, RIGHT(CONCAT('00',CAST(M.MEME_SFX AS VARCHAR)),2)),
    T.ACAC_ACC_NO,
    S.SBSB_CK, 
    S.SBSB_ID, 
    M.MEME_SFX, 
    M.MEME_LAST_NAME, 
    M.MEME_FIRST_NAME, 
    CURR_PLN.PDPD_ID, 
    CURR_CMP.PDBC_PFX,
    BPL.PDBC_PFX, 
    G.GRGR_ID,
    G.GRGR_NAME,
    G.GRGR_MCTR_TYPE, 
    G.CICI_ID,
    T.MATX_BEN_BEG_DT,
    CURR_PLN.MEPE_ELIG_IND,
    CURR_PLN.MEPE_EFF_DT,
    CURR_PLN.MEPE_TERM_DT
  )a
  group by 
  MEME_CK,MEME_ID,SBSB_CK,SBSB_ID,MEME_SFX,MEME_LAST_NAME,MEME_FIRST_NAME,
  PDPD_ID,CMPT_ID,BPL_NBR,GRGR_ID,GRGR_NAME,GRGR_MCTR_TYPE,ACCUM_TYPE,ACAC_ACC_NO,
  BENEFIT_BEG_DT,SEQ_NO,ACCUM_TTL,PBM_CT,CAR_ID,MEPE_ELIG_IND,MEPE_EFF_DT,MEPE_TERM_DT,
  PROD_CT,PLANNAME,LAST_UPDATE_DTM
)DeDup
where RNK =1
"""

df_SS_MATX_OOP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_SS_MATX_OOP)
    .load()
)

# ----------------------------------------------------------------
# Funnel Stage (fnl): Combine (union) the 4 dataframes
# ----------------------------------------------------------------
funnel_cols = [
    "MEME_CK",
    "MEME_ID",
    "SBSB_CK",
    "SBSB_ID",
    "MEME_SFX",
    "MEME_LAST_NAME",
    "MEME_FIRST_NAME",
    "PDPD_ID",
    "CMPT_ID",
    "BPL_NBR",
    "GRGR_ID",
    "GRGR_NAME",
    "GRGR_MCTR_TYPE",
    "ACCUM_TYPE",
    "ACAC_ACC_NO",
    "BENEFIT_BEG_DT",
    "SEQ_NO",
    "LAST_UPDATE_DTM",
    "ACCUM_TTL",
    "PBM_CT",
    "CAR_ID",
    "MEPE_ELIG_IND",
    "MEPE_EFF_DT",
    "MEPE_TERM_DT",
    "PROD_CT",
    "PLANNAME",
]

df_SS_FATX_DED_sel = df_SS_FATX_DED.select(funnel_cols)
df_SS_FATX_OOP_sel = df_SS_FATX_OOP.select(funnel_cols)
df_SS_MATX_DED_sel = df_SS_MATX_DED.select(funnel_cols)
df_SS_MATX_OOP_sel = df_SS_MATX_OOP.select(funnel_cols)

df_fnl = (
    df_SS_MATX_OOP_sel.unionByName(df_SS_MATX_DED_sel)
    .unionByName(df_SS_FATX_OOP_sel)
    .unionByName(df_SS_FATX_DED_sel)
)

# ----------------------------------------------------------------
# Remove_Duplicates_167 (PxRemDup)
#   KeysThatDefineDuplicates: MEME_ID, ACCUM_TYPE
#   Sort: MEME_ID asc, ACCUM_TYPE asc, BENEFIT_BEG_DT desc
#   RetainRecord: first
# ----------------------------------------------------------------
df_in_xfm = dedup_sort(
    df_fnl,
    partition_cols=["MEME_ID", "ACCUM_TYPE"],
    sort_cols=[("MEME_ID", "A"), ("ACCUM_TYPE", "A"), ("BENEFIT_BEG_DT", "D")]
)

# ----------------------------------------------------------------
# Lookup_150 (PxLookup) - Left Join with df_Gx_Product_PlanLimit
#   Using a guessed join on (PLANNAME, ACCUM_TYPE)
# ----------------------------------------------------------------
df_lookup_150 = df_in_xfm.alias("in_xfm").join(
    df_Gx_Product_PlanLimit.alias("in_lkup_PlanLimit"),
    (
        (F.col("in_xfm.PLANNAME") == F.col("in_lkup_PlanLimit.PLANNAME")) &
        (F.col("in_xfm.ACCUM_TYPE") == F.col("in_lkup_PlanLimit.ACCUM_TYPE"))
    ),
    "left"
)

df_Lookup_150_sel = df_lookup_150.select(
    F.col("in_xfm.MEME_CK").alias("MBR_CK"),
    F.col("in_xfm.MEME_ID").alias("MBR_ID"),
    F.col("in_xfm.SBSB_CK").alias("SUB_CK"),
    F.col("in_xfm.SBSB_ID").alias("SUB_ID"),
    F.col("in_xfm.MEME_SFX").alias("MBR_SFX"),
    F.col("in_xfm.MEME_LAST_NAME").alias("MBR_LAST_NM"),
    F.col("in_xfm.MEME_FIRST_NAME").alias("MBR_FIRST_NM"),
    F.col("in_xfm.PDPD_ID").alias("PROD_ID"),
    F.col("in_xfm.CMPT_ID").alias("PROD_BILL_CMPNT_PFX"),
    F.col("in_xfm.GRGR_ID").alias("GRP_ID"),
    F.col("in_xfm.GRGR_NAME").alias("GRP_NM"),
    F.col("in_xfm.GRGR_MCTR_TYPE").alias("GRP_TYP_CD"),
    F.col("in_xfm.ACCUM_TYPE").alias("ACCUM_TYP_CD"),
    F.col("in_xfm.ACAC_ACC_NO").alias("ACCUM_NO"),
    F.col("in_xfm.BENEFIT_BEG_DT").alias("BNF_BEG_DT"),
    F.col("in_xfm.SEQ_NO").alias("SEQ_NO"),
    F.col("in_xfm.LAST_UPDATE_DTM").alias("LAST_UPDT_DTM"),
    F.col("in_xfm.ACCUM_TTL").alias("ACCUM_TOT_AMT"),
    F.col("in_xfm.BPL_NBR").alias("BNF_PLN_LMT_NO"),
    F.col("in_xfm.PBM_CT").alias("PBM_CT"),
    F.col("in_xfm.CAR_ID").alias("CAR_ID"),
    F.col("in_xfm.MEPE_ELIG_IND").alias("MBR_ELIG_IN"),
    F.col("in_xfm.MEPE_EFF_DT").alias("MBR_EFF_DT"),
    F.col("in_xfm.MEPE_TERM_DT").alias("MBR_TERM_DT"),
    F.col("in_xfm.PROD_CT").alias("PROD_CT"),
    F.col("in_lkup_PlanLimit.PLANVALUE").alias("PLANVALUE")
)

# ----------------------------------------------------------------
# Transformer_156 => two output links
#   1) in_Trg (no PLANVALUE)
#   2) inds_PlanLmits (with PLANVALUE)
#   MBR_ELIG_IN is char(1) => apply rpad
# ----------------------------------------------------------------
cols_in_Trg = [
    "MBR_CK","MBR_ID","SUB_CK","SUB_ID","MBR_SFX","MBR_LAST_NM","MBR_FIRST_NM",
    "PROD_ID","PROD_BILL_CMPNT_PFX","GRP_ID","GRP_NM","GRP_TYP_CD","ACCUM_TYP_CD",
    "ACCUM_NO","BNF_BEG_DT","SEQ_NO","LAST_UPDT_DTM","ACCUM_TOT_AMT","BNF_PLN_LMT_NO",
    "PBM_CT","CAR_ID","MBR_ELIG_IN","MBR_EFF_DT","MBR_TERM_DT","PROD_CT"
]

cols_inds_PlanLmits = [
    "MBR_CK","MBR_ID","SUB_CK","SUB_ID","MBR_SFX","MBR_LAST_NM","MBR_FIRST_NM",
    "PROD_ID","PROD_BILL_CMPNT_PFX","GRP_ID","GRP_NM","GRP_TYP_CD","ACCUM_TYP_CD",
    "ACCUM_NO","BNF_BEG_DT","SEQ_NO","LAST_UPDT_DTM","ACCUM_TOT_AMT","BNF_PLN_LMT_NO",
    "PBM_CT","CAR_ID","MBR_ELIG_IN","MBR_EFF_DT","MBR_TERM_DT","PROD_CT","PLANVALUE"
]

df_Transformer_156_in_Trg = (
    df_Lookup_150_sel
    .withColumn("MBR_ELIG_IN", F.rpad("MBR_ELIG_IN", 1, " "))
    .select(cols_in_Trg)
)

df_Transformer_156_inds_PlanLmits = (
    df_Lookup_150_sel
    .withColumn("MBR_ELIG_IN", F.rpad("MBR_ELIG_IN", 1, " "))
    .select(cols_inds_PlanLmits)
)

# ----------------------------------------------------------------
# Stage: FCTS_ACCUM_RECON_STG (ODBCConnectorPX) -> table append
#   Before_SQL: DELETE FROM {GxCAccumReconOwner}.FCTS_ACCUM_RECON_STG
# ----------------------------------------------------------------
jdbc_url_gxcaccumrecon, jdbc_props_gxcaccumrecon = get_db_config(gxcaccumrecon_secret_name)

delete_sql = f"DELETE FROM {GxCAccumReconOwner}.FCTS_ACCUM_RECON_STG"
execute_dml(delete_sql, jdbc_url_gxcaccumrecon, jdbc_props_gxcaccumrecon)

(
    df_Transformer_156_in_Trg
    .write
    .format("jdbc")
    .option("url", jdbc_url_gxcaccumrecon)
    .option("dbtable", f"{GxCAccumReconOwner}.FCTS_ACCUM_RECON_STG")
    .options(**jdbc_props_gxcaccumrecon)
    .mode("append")
    .save()
)

# ----------------------------------------------------------------
# Stage: ds_FctsStgPlanlimits (PxDataSet) -> translate to Parquet
#   File: datasets/FctsAccumReconStgPlanlimits.ds => .parquet
# ----------------------------------------------------------------
df_ds_FctsStgPlanlimits = df_Transformer_156_inds_PlanLmits

write_files(
    df_ds_FctsStgPlanlimits,
    f"{adls_path}/datasets/FctsAccumReconStgPlanlimits.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)