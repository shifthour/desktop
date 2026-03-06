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
# MAGIC DESCRIPTION:      Performs Preprocessing for reconcilation of Total amounts between Facets and OptumRXAccum  
# MAGIC                                
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                         Date                 Project/Altiris #                           Change Description                                                                                          Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------                      --------------------     ------------------------                            -----------------------------------------------------------------------                                                   --------------------------------       -------------------------------   ----------------------------      
# MAGIC Rekha Radhakrishna      2020/10/11     OPTUMRX Accum Exchange      Initial Programming                                                                                              IntegrateDev2       Jaideep Mankala        11/19/2020
# MAGIC Rekha Radhakrishna      2020/10/11     OPTUMRX Accum Exchange      Changed join to OPTUMRX to include BNF_BEG_DT                                       IntegrateDev2        Manasa Andru       2020-12-28
# MAGIC Rekha Radhakrishna      2020/01/06     OPTUMRX Accum Exchange      Changed OPTUMRX Staging Lookup query  for MEDD                                     IntegrateDev2      Jaideep Mankala  01/12/2021
# MAGIC Ashok kumar B                  2023/10/09     US583346                                    Commented out condition in SQL comparing ACCUM_CD to ACCUM_LIST    IntegrateDev2              Jeyaprasanna           2023-10-11
# MAGIC Ashok kumar B                  2023/12/04     US583346                            Added the accum_cd and accum_list logic in OPTUMRX_ACCUM_RECON_STG   IntegrateDev2            Jeyaprasanna           2023-11-05     
# MAGIC Ashok kumar B                  2024/08/14     US624048                            Modified the logic to calculate ind optum amount in optum extract                             IntegrateDev2             Jeyaprasanna          2024-08-14
# MAGIC Ashok kumar B                  2024/09/03     US628482                           Modified and derived  the logic to exlude group 20624000 and 34189000                                     IntegrateDev2              Jeyaprasanna          2024-09-05

# MAGIC Drop records that have Facets  PBM_CT = 0 And Optum Member Total = 0 , Load all other scenarios including IF PBM_CT = 0 And OPTUM Member Tot is > 0
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, DecimalType, TimestampType, StructType, StructField
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


GxCAccumReconOwner = get_widget_value('GxCAccumReconOwner','')
gxcaccumrecon_secret_name = get_widget_value('gxcaccumrecon_secret_name','')
optumrx_accum_recon_trg_lookup_secret_name = get_widget_value('optumrx_accum_recon_trg_lookup_secret_name','')

# --------------------------------------------------------------------------------
# Stage: OPTUMRX_ACCUM_RECON_STG (ODBCConnectorPX)
# --------------------------------------------------------------------------------
jdbc_url_OPTUMRX_ACCUM_RECON_STG, jdbc_props_OPTUMRX_ACCUM_RECON_STG = get_db_config(gxcaccumrecon_secret_name)
extract_query_OPTUMRX_ACCUM_RECON_STG = f"""
SELECT PERD_FROM_DT, INDV_ACCUM_BAL_AMT, FMLY_ACCUM_BAL_AMT, MBR_ID,
       ACCUM_TYP, ISNULL(MBR_FIRST_NM,' ') MBR_FIRST_NM, ISNULL(MBR_LAST_NM,' ') MBR_LAST_NM
FROM (
  SELECT CAR_ID, PERD_FROM_DT, MBR_ID, ACCUM_TYP,
         MAX(MBR_FIRST_NM) as MBR_FIRST_NM,
         MAX(MBR_LAST_NM) as MBR_LAST_NM,
         RANK() OVER (PARTITION BY MBR_ID, ACCUM_TYP ORDER BY PERD_FROM_DT ) RNK,
         SUM(INDV_ACCUM_BAL_AMT) AS INDV_ACCUM_BAL_AMT,
         SUM(FMLY_ACCUM_BAL_AMT) AS FMLY_ACCUM_BAL_AMT
  FROM {GxCAccumReconOwner}.OPTUMRX_ACCUM_RECON_STG
  WHERE INDV_ACCUM_BAL_AMT != 0
    AND (
         (CAR_ID = 'BKC1'
          AND ACCUM_LVL = 'A'
          AND CONCAT(ACCUM_CD,LEFT(ACCUM_LIST,6)) IN ('BKC1OOPBKCOOP','BKC1DEDBKCDED')
          AND YEAR(PERD_FROM_DT) != 2001)
         OR (CAR_ID != 'BKC1')
        )
  GROUP BY CAR_ID, PERD_FROM_DT, MBR_ID, ACCUM_TYP
) SRC
WHERE RNK = 1
"""
df_OPTUMRX_ACCUM_RECON_STG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_OPTUMRX_ACCUM_RECON_STG)
    .options(**jdbc_props_OPTUMRX_ACCUM_RECON_STG)
    .option("query", extract_query_OPTUMRX_ACCUM_RECON_STG)
    .load()
)

df_DSLink136_from_OPTUMRX_ACCUM_RECON_STG = df_OPTUMRX_ACCUM_RECON_STG

# --------------------------------------------------------------------------------
# Stage: xfm_AccumAmt (CTransformerStage)
# --------------------------------------------------------------------------------
df_xfm_AccumAmt_stagevars = df_DSLink136_from_OPTUMRX_ACCUM_RECON_STG.withColumn(
    "IndAccumBal", F.col("INDV_ACCUM_BAL_AMT") / 100
).withColumn(
    "FamAccumBal", F.col("FMLY_ACCUM_BAL_AMT")
)

df_injoin_amt = df_xfm_AccumAmt_stagevars.select(
    trim(F.col("MBR_ID")).alias("MBR_ID"),
    F.col("ACCUM_TYP").alias("ACCUM_TYP_CD"),
    (
        DecimalToString(Field(F.col("IndAccumBal"), '.', 1), "suppress_zero")
        + F.lit(".")
        + Field(F.col("IndAccumBal"), '.', 2)
    ).alias("INDV_ACCUM_BAL_AMT"),
    (
        DecimalToString(Field(F.col("FamAccumBal"), '.', 1), "suppress_zero")
        + F.lit(".")
        + Field(F.col("FamAccumBal"), '.', 2)
    ).alias("FMLY_ACCUM_BAL_AMT"),
    trim(F.col("MBR_FIRST_NM")).alias("MBR_FIRST_NM"),
    trim(F.col("MBR_LAST_NM")).alias("MBR_LAST_NM"),
    TimestampToDate(F.col("PERD_FROM_DT")).alias("BNF_BEG_DT")
)

# --------------------------------------------------------------------------------
# Stage: ds_FctsStgPlanlimits (PxDataSet) - reading from .ds => translate to .parquet
# --------------------------------------------------------------------------------
df_ds_FctsStgPlanlimits = spark.read.parquet(f"{adls_path}/datasets/FctsAccumReconStgPlanlimits.parquet")

# --------------------------------------------------------------------------------
# Stage: xfm_AccumTyp (CTransformerStage)
# --------------------------------------------------------------------------------
df_AccumTyp_OOP = df_ds_FctsStgPlanlimits.filter(
    (F.col("ACCUM_TYP_CD").substr(2, 1) == 'O')
    & (F.col("GRP_ID") != '44284000')
    & (F.col("GRP_ID") != '37939000')
    & (F.col("GRP_ID") != '43088000')
    & (F.col("GRP_ID") != '45143000')
).select(
    F.col("MBR_CK").alias("MBR_CK"),
    F.col("MBR_ID").alias("MBR_ID"),
    F.col("SUB_CK").alias("SUB_CK"),
    F.col("SUB_ID").alias("SUB_ID"),
    F.col("MBR_SFX").alias("MBR_SFX"),
    F.col("MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("PROD_BILL_CMPNT_PFX").alias("PROD_BILL_CMPNT_PFX"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("GRP_NM").alias("GRP_NM"),
    F.col("GRP_TYP_CD").alias("GRP_TYP_CD"),
    F.col("ACCUM_TYP_CD").alias("ACCUM_TYP_CD"),
    F.col("ACCUM_NO").alias("ACCUM_NO"),
    TimestampToDate(F.col("BNF_BEG_DT")).alias("BNF_BEG_DT"),
    F.col("SEQ_NO").alias("SEQ_NO"),
    F.col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    F.col("ACCUM_TOT_AMT").alias("ACCUM_TOT_AMT"),
    F.col("BNF_PLN_LMT_NO").alias("BNF_PLN_LMT_NO"),
    F.col("PBM_CT").alias("PBM_CT"),
    F.col("CAR_ID").alias("CAR_ID"),
    F.col("MBR_ELIG_IN").alias("MBR_ELIG_IN"),
    F.col("MBR_EFF_DT").alias("MBR_EFF_DT"),
    F.col("MBR_TERM_DT").alias("MBR_TERM_DT"),
    F.col("PROD_CT").alias("PROD_CT"),
    F.col("PLANVALUE").alias("PLANVALUE")
)

df_AccumTyp_DED = df_ds_FctsStgPlanlimits.filter(
    (F.col("ACCUM_TYP_CD").substr(2, 1) == 'D')
    & (F.col("GRP_ID") != '44284000')
    & (F.col("GRP_ID") != '37939000')
    & (F.col("GRP_ID") != '43088000')
    & (F.col("GRP_ID") != '45143000')
).select(
    F.col("MBR_CK").alias("MBR_CK"),
    F.col("MBR_ID").alias("MBR_ID"),
    F.col("SUB_CK").alias("SUB_CK"),
    F.col("SUB_ID").alias("SUB_ID"),
    F.col("MBR_SFX").alias("MBR_SFX"),
    F.col("MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("PROD_BILL_CMPNT_PFX").alias("PROD_BILL_CMPNT_PFX"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("GRP_NM").alias("GRP_NM"),
    F.col("GRP_TYP_CD").alias("GRP_TYP_CD"),
    F.col("ACCUM_TYP_CD").alias("ACCUM_TYP_CD"),
    F.col("ACCUM_NO").alias("ACCUM_NO"),
    TimestampToDate(F.col("BNF_BEG_DT")).alias("BNF_BEG_DT"),
    F.col("SEQ_NO").alias("SEQ_NO"),
    F.col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    F.col("ACCUM_TOT_AMT").alias("ACCUM_TOT_AMT"),
    F.col("BNF_PLN_LMT_NO").alias("BNF_PLN_LMT_NO"),
    F.col("PBM_CT").alias("PBM_CT"),
    F.col("CAR_ID").alias("CAR_ID"),
    F.col("MBR_ELIG_IN").alias("MBR_ELIG_IN"),
    F.col("MBR_EFF_DT").alias("MBR_EFF_DT"),
    F.col("MBR_TERM_DT").alias("MBR_TERM_DT"),
    F.col("PROD_CT").alias("PROD_CT"),
    F.col("PLANVALUE").alias("PLANVALUE")
)

# --------------------------------------------------------------------------------
# Stage: xfm_Member_Family_DED (CTransformerStage)
# --------------------------------------------------------------------------------
df_DED_Member = df_AccumTyp_DED.filter(F.col("ACCUM_TYP_CD").substr(1, 1) == 'M').select(
    F.col("MBR_CK").alias("MBR_CK"),
    F.col("MBR_ID").alias("MBR_ID"),
    F.col("SUB_CK").alias("SUB_CK"),
    F.col("SUB_ID").alias("SUB_ID"),
    F.col("MBR_SFX").alias("MBR_SFX"),
    F.col("MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("PROD_BILL_CMPNT_PFX").alias("PROD_BILL_CMPNT_PFX"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("GRP_NM").alias("GRP_NM"),
    F.col("GRP_TYP_CD").alias("GRP_TYP_CD"),
    F.col("ACCUM_TYP_CD").alias("ACCUM_TYP_CD"),
    F.col("ACCUM_NO").alias("ACCUM_NO"),
    F.col("BNF_BEG_DT").alias("BNF_BEG_DT"),
    F.col("SEQ_NO").alias("SEQ_NO"),
    F.col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    F.col("ACCUM_TOT_AMT").alias("ACCUM_TOT_AMT"),
    F.col("BNF_PLN_LMT_NO").alias("BNF_PLN_LMT_NO"),
    F.col("PBM_CT").alias("PBM_CT"),
    F.col("CAR_ID").alias("CAR_ID"),
    F.col("MBR_ELIG_IN").alias("MBR_ELIG_IN"),
    F.col("MBR_EFF_DT").alias("MBR_EFF_DT"),
    F.col("MBR_TERM_DT").alias("MBR_TERM_DT"),
    F.col("PROD_CT").alias("PROD_CT"),
    F.col("PLANVALUE").alias("PLANVALUE")
)

df_DED_Family = df_AccumTyp_DED.filter(F.col("ACCUM_TYP_CD").substr(1, 1) == 'F').select(
    F.col("MBR_CK").alias("MBR_CK"),
    F.col("MBR_ID").alias("MBR_ID"),
    F.col("SUB_CK").alias("SUB_CK"),
    F.col("SUB_ID").alias("SUB_ID"),
    F.col("MBR_SFX").alias("MBR_SFX"),
    F.col("MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("PROD_BILL_CMPNT_PFX").alias("PROD_BILL_CMPNT_PFX"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("GRP_NM").alias("GRP_NM"),
    F.col("GRP_TYP_CD").alias("GRP_TYP_CD"),
    F.col("ACCUM_TYP_CD").alias("ACCUM_TYP_CD"),
    F.col("ACCUM_NO").alias("ACCUM_NO"),
    F.col("BNF_BEG_DT").alias("BNF_BEG_DT"),
    F.col("SEQ_NO").alias("SEQ_NO"),
    F.col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    F.col("ACCUM_TOT_AMT").alias("ACCUM_TOT_AMT"),
    F.col("BNF_PLN_LMT_NO").alias("BNF_PLN_LMT_NO"),
    F.col("PBM_CT").alias("PBM_CT"),
    F.col("CAR_ID").alias("CAR_ID"),
    F.col("MBR_ELIG_IN").alias("MBR_ELIG_IN"),
    F.col("MBR_EFF_DT").alias("MBR_EFF_DT"),
    F.col("MBR_TERM_DT").alias("MBR_TERM_DT"),
    F.col("PROD_CT").alias("PROD_CT"),
    F.col("PLANVALUE").alias("PLANVALUE")
)

# --------------------------------------------------------------------------------
# Stage: Lkup_DED_MEM_SUB (PxLookup, left join)
# --------------------------------------------------------------------------------
df_Member_D = df_DED_Member.alias("Member_D")
df_Family_D = df_DED_Family.alias("Family_D")
df_Onerow_DED_Member_and_FamilyAmt = df_Member_D.join(
    df_Family_D,
    (F.col("Member_D.MBR_ID") == F.col("Family_D.MBR_ID")) & (F.col("Member_D.SUB_ID") == F.col("Family_D.SUB_ID")),
    "left"
).select(
    F.col("Member_D.MBR_CK").alias("MBR_CK"),
    F.col("Member_D.MBR_ID").alias("MBR_ID"),
    F.col("Member_D.SUB_CK").alias("SUB_CK"),
    F.col("Member_D.SUB_ID").alias("SUB_ID"),
    F.col("Member_D.MBR_SFX").alias("MBR_SFX"),
    F.col("Member_D.MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("Member_D.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("Member_D.PROD_ID").alias("PROD_ID"),
    F.col("Member_D.PROD_BILL_CMPNT_PFX").alias("PROD_BILL_CMPNT_PFX"),
    F.col("Member_D.GRP_ID").alias("GRP_ID"),
    F.col("Member_D.GRP_NM").alias("GRP_NM"),
    F.col("Member_D.GRP_TYP_CD").alias("GRP_TYP_CD"),
    F.col("Member_D.ACCUM_TYP_CD").alias("ACCUM_TYP_CD"),
    F.col("Member_D.ACCUM_NO").alias("ACCUM_NO"),
    F.col("Member_D.BNF_BEG_DT").alias("BNF_BEG_DT"),
    F.col("Member_D.SEQ_NO").alias("SEQ_NO"),
    F.col("Member_D.LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    F.col("Member_D.ACCUM_TOT_AMT").alias("ACCUM_TOT_AMT"),
    F.col("Member_D.BNF_PLN_LMT_NO").alias("BNF_PLN_LMT_NO"),
    F.col("Member_D.PBM_CT").alias("PBM_CT"),
    F.col("Family_D.ACCUM_TOT_AMT").alias("FAMILY_ACCUM_TTL"),
    F.col("Member_D.CAR_ID").alias("CAR_ID"),
    F.col("Member_D.MBR_ELIG_IN").alias("MBR_ELIG_IN"),
    F.col("Member_D.MBR_EFF_DT").alias("MBR_EFF_DT"),
    F.col("Member_D.MBR_TERM_DT").alias("MBR_TERM_DT"),
    F.col("Member_D.PROD_CT").alias("PROD_CT"),
    F.col("Member_D.PLANVALUE").alias("MBR_PLN_LMT_NO"),
    F.col("Family_D.PLANVALUE").alias("FMLY_PLN_LMT_NO")
)

# --------------------------------------------------------------------------------
# Stage: xfm_Member_family_OOP (CTransformerStage)
# --------------------------------------------------------------------------------
df_OOP_Member = df_AccumTyp_OOP.filter(F.col("ACCUM_TYP_CD").substr(1, 1) == 'M').select(
    F.col("MBR_CK").alias("MBR_CK"),
    F.col("MBR_ID").alias("MBR_ID"),
    F.col("SUB_CK").alias("SUB_CK"),
    F.col("SUB_ID").alias("SUB_ID"),
    F.col("MBR_SFX").alias("MBR_SFX"),
    F.col("MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("PROD_BILL_CMPNT_PFX").alias("PROD_BILL_CMPNT_PFX"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("GRP_NM").alias("GRP_NM"),
    F.col("GRP_TYP_CD").alias("GRP_TYP_CD"),
    F.col("ACCUM_TYP_CD").alias("ACCUM_TYP_CD"),
    F.col("ACCUM_NO").alias("ACCUM_NO"),
    F.col("BNF_BEG_DT").alias("BNF_BEG_DT"),
    F.col("SEQ_NO").alias("SEQ_NO"),
    F.col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    F.col("ACCUM_TOT_AMT").alias("ACCUM_TOT_AMT"),
    F.col("BNF_PLN_LMT_NO").alias("BNF_PLN_LMT_NO"),
    F.col("PBM_CT").alias("PBM_CT"),
    F.col("CAR_ID").alias("CAR_ID"),
    F.col("MBR_ELIG_IN").alias("MBR_ELIG_IN"),
    F.col("MBR_EFF_DT").alias("MBR_EFF_DT"),
    F.col("MBR_TERM_DT").alias("MBR_TERM_DT"),
    F.col("PROD_CT").alias("PROD_CT"),
    F.col("PLANVALUE").alias("PLANVALUE")
)

df_OOP_Family = df_AccumTyp_OOP.filter(F.col("ACCUM_TYP_CD").substr(1, 1) == 'F').select(
    F.col("MBR_CK").alias("MBR_CK"),
    F.col("MBR_ID").alias("MBR_ID"),
    F.col("SUB_CK").alias("SUB_CK"),
    F.col("SUB_ID").alias("SUB_ID"),
    F.col("MBR_SFX").alias("MBR_SFX"),
    F.col("MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("PROD_BILL_CMPNT_PFX").alias("PROD_BILL_CMPNT_PFX"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("GRP_NM").alias("GRP_NM"),
    F.col("GRP_TYP_CD").alias("GRP_TYP_CD"),
    F.col("ACCUM_TYP_CD").alias("ACCUM_TYP_CD"),
    F.col("ACCUM_NO").alias("ACCUM_NO"),
    F.col("BNF_BEG_DT").alias("BNF_BEG_DT"),
    F.col("SEQ_NO").alias("SEQ_NO"),
    F.col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    F.col("ACCUM_TOT_AMT").alias("ACCUM_TOT_AMT"),
    F.col("BNF_PLN_LMT_NO").alias("BNF_PLN_LMT_NO"),
    F.col("PBM_CT").alias("PBM_CT"),
    F.col("CAR_ID").alias("CAR_ID"),
    F.col("MBR_ELIG_IN").alias("MBR_ELIG_IN"),
    F.col("MBR_EFF_DT").alias("MBR_EFF_DT"),
    F.col("MBR_TERM_DT").alias("MBR_TERM_DT"),
    F.col("PROD_CT").alias("PROD_CT"),
    F.col("PLANVALUE").alias("PLANVALUE")
)

# --------------------------------------------------------------------------------
# Stage: Lkup_OOP_MEM_SUB (PxLookup, left join)
# --------------------------------------------------------------------------------
df_OOP_Member_alias = df_OOP_Member.alias("Member_L")
df_OOP_Family_alias = df_OOP_Family.alias("Family_L")

df_Onerow_OOP_Member_and_FamilyAmt = df_OOP_Member_alias.join(
    df_OOP_Family_alias,
    (F.col("Member_L.MBR_ID") == F.col("Family_L.MBR_ID")) & (F.col("Member_L.SUB_ID") == F.col("Family_L.SUB_ID")),
    "left"
).select(
    F.col("Member_L.MBR_CK").alias("MBR_CK"),
    F.col("Member_L.MBR_ID").alias("MBR_ID"),
    F.col("Member_L.SUB_CK").alias("SUB_CK"),
    F.col("Member_L.SUB_ID").alias("SUB_ID"),
    F.col("Member_L.MBR_SFX").alias("MBR_SFX"),
    F.col("Member_L.MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("Member_L.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("Member_L.PROD_ID").alias("PROD_ID"),
    F.col("Member_L.PROD_BILL_CMPNT_PFX").alias("PROD_BILL_CMPNT_PFX"),
    F.col("Member_L.GRP_ID").alias("GRP_ID"),
    F.col("Member_L.GRP_NM").alias("GRP_NM"),
    F.col("Member_L.GRP_TYP_CD").alias("GRP_TYP_CD"),
    F.col("Member_L.ACCUM_TYP_CD").alias("ACCUM_TYP_CD"),
    F.col("Member_L.ACCUM_NO").alias("ACCUM_NO"),
    F.col("Member_L.BNF_BEG_DT").alias("BNF_BEG_DT"),
    F.col("Member_L.SEQ_NO").alias("SEQ_NO"),
    F.col("Member_L.LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    F.col("Member_L.ACCUM_TOT_AMT").alias("ACCUM_TOT_AMT"),
    F.col("Member_L.BNF_PLN_LMT_NO").alias("BNF_PLN_LMT_NO"),
    F.col("Member_L.PBM_CT").alias("PBM_CT"),
    F.col("Family_L.ACCUM_TOT_AMT").alias("FAMILY_ACCUM_TTL"),
    F.col("Member_L.CAR_ID").alias("CAR_ID"),
    F.col("Member_L.MBR_ELIG_IN").alias("MBR_ELIG_IN"),
    F.col("Member_L.MBR_EFF_DT").alias("MBR_EFF_DT"),
    F.col("Member_L.MBR_TERM_DT").alias("MBR_TERM_DT"),
    F.col("Member_L.PROD_CT").alias("PROD_CT"),
    F.col("Member_L.PLANVALUE").alias("MBR_PLN_LMT_NO"),
    F.col("Family_L.PLANVALUE").alias("FMLY_PLN_LMT_NO")
)

# --------------------------------------------------------------------------------
# Stage: fnl_OOP_DED (PxFunnel)
# --------------------------------------------------------------------------------
# Funnel is effectively a union (not distinct) of Onerow_OOP_Member_and_FamilyAmt and Onerow_DED_Member_and_FamilyAmt
df_fnl_OOP_DED_1 = df_Onerow_OOP_Member_and_FamilyAmt.select(
    F.col("MBR_CK"),
    F.col("MBR_ID"),
    F.col("SUB_CK"),
    F.col("SUB_ID"),
    F.col("MBR_SFX"),
    F.col("MBR_LAST_NM"),
    F.col("MBR_FIRST_NM"),
    F.col("PROD_ID"),
    F.col("PROD_BILL_CMPNT_PFX"),
    F.col("GRP_ID"),
    F.col("GRP_NM"),
    F.col("GRP_TYP_CD"),
    F.col("ACCUM_TYP_CD"),
    F.col("ACCUM_NO"),
    F.col("BNF_BEG_DT"),
    F.col("SEQ_NO"),
    F.col("LAST_UPDT_DTM"),
    F.col("ACCUM_TOT_AMT"),
    F.col("BNF_PLN_LMT_NO"),
    F.col("PBM_CT"),
    F.col("FAMILY_ACCUM_TTL"),
    F.col("CAR_ID"),
    F.col("MBR_ELIG_IN"),
    F.col("MBR_EFF_DT"),
    F.col("MBR_TERM_DT"),
    F.col("PROD_CT"),
    F.col("MBR_PLN_LMT_NO"),
    F.col("FMLY_PLN_LMT_NO")
)
df_fnl_OOP_DED_2 = df_Onerow_DED_Member_and_FamilyAmt.select(
    F.col("MBR_CK"),
    F.col("MBR_ID"),
    F.col("SUB_CK"),
    F.col("SUB_ID"),
    F.col("MBR_SFX"),
    F.col("MBR_LAST_NM"),
    F.col("MBR_FIRST_NM"),
    F.col("PROD_ID"),
    F.col("PROD_BILL_CMPNT_PFX"),
    F.col("GRP_ID"),
    F.col("GRP_NM"),
    F.col("GRP_TYP_CD"),
    F.col("ACCUM_TYP_CD"),
    F.col("ACCUM_NO"),
    F.col("BNF_BEG_DT"),
    F.col("SEQ_NO"),
    F.col("LAST_UPDT_DTM"),
    F.col("ACCUM_TOT_AMT"),
    F.col("BNF_PLN_LMT_NO"),
    F.col("PBM_CT"),
    F.col("FAMILY_ACCUM_TTL"),
    F.col("CAR_ID"),
    F.col("MBR_ELIG_IN"),
    F.col("MBR_EFF_DT"),
    F.col("MBR_TERM_DT"),
    F.col("PROD_CT"),
    F.col("MBR_PLN_LMT_NO"),
    F.col("FMLY_PLN_LMT_NO")
)
df_DSLink162_fnl_OOP_DED = df_fnl_OOP_DED_1.union(df_fnl_OOP_DED_2)

# --------------------------------------------------------------------------------
# Stage: xfm_business_rules (CTransformerStage)
# --------------------------------------------------------------------------------
df_business_rules = df_DSLink162_fnl_OOP_DED.filter(
    (trim(F.col("GRP_ID")) != '20624000') | (trim(F.col("GRP_ID")) != '34189000')
).select(
    F.col("MBR_CK").alias("MBR_CK"),
    trim(F.col("MBR_ID")).alias("MBR_ID"),
    F.col("SUB_CK").alias("SUB_CK"),
    trim(F.col("SUB_ID")).alias("SUB_ID"),
    trim(F.col("MBR_SFX")).alias("MBR_SFX"),
    trim(F.col("MBR_LAST_NM")).alias("MBR_LAST_NAME"),
    trim(F.col("MBR_FIRST_NM")).alias("MBR_FIRST_NAME"),
    trim(F.col("PROD_ID")).alias("PROD_ID"),
    trim(F.col("PROD_BILL_CMPNT_PFX")).alias("CMPT_ID"),
    trim(F.col("GRP_ID")).alias("GRP_ID"),
    trim(F.col("GRP_NM")).alias("GRP_NAME"),
    trim(F.col("GRP_TYP_CD")).alias("GRP_TYP_CD"),
    trim(F.col("ACCUM_TYP_CD").substr(2,1)).alias("ACCUM_TYP_CD"),
    F.col("ACCUM_NO").alias("ACCUM_NO"),
    F.col("BNF_BEG_DT").alias("BNF_BEG_DT"),
    F.col("SEQ_NO").alias("SEQ_NO"),
    F.col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    F.col("ACCUM_TOT_AMT").alias("INDIVIDUAL_ACCUM_TTL"),
    trim(F.col("BNF_PLN_LMT_NO")).alias("BPL_NBR"),
    F.col("PBM_CT").alias("PBM_CT"),
    F.col("FAMILY_ACCUM_TTL").alias("FAMILY_ACCUM_TTL"),
    F.col("CAR_ID").alias("CAR_ID"),
    F.col("MBR_ELIG_IN").alias("MBR_ELIG_IN"),
    F.col("MBR_EFF_DT").alias("MBR_EFF_DT"),
    F.col("MBR_TERM_DT").alias("MBR_TERM_DT"),
    F.col("PROD_CT").alias("PROD_CT"),
    F.col("MBR_PLN_LMT_NO").alias("MBR_PLN_LMT_NO"),
    F.col("FMLY_PLN_LMT_NO").alias("FMLY_PLN_LMT_NO")
)

# --------------------------------------------------------------------------------
# Stage: Join_Facets_OptumRX (PxJoin, fullouterjoin)
# --------------------------------------------------------------------------------
# Left => DSLink137 (df_business_rules)
# Right => injoin_amt (df_injoin_amt)
df_left_alias = df_business_rules.alias("DSLink137")
df_right_alias = df_injoin_amt.alias("injoin_amt")
df_Join_Facets_OptumRX = df_left_alias.join(
    df_right_alias,
    (
        (F.col("DSLink137.MBR_ID") == F.col("injoin_amt.MBR_ID"))
        & (F.col("DSLink137.ACCUM_TYP_CD") == F.col("injoin_amt.ACCUM_TYP_CD"))
        & (F.col("DSLink137.BNF_BEG_DT") == F.col("injoin_amt.BNF_BEG_DT"))
    ),
    "fullouter"
).select(
    F.col("DSLink137.GRP_TYP_CD").alias("GRP_TYP_CD"),
    F.col("DSLink137.GRP_ID").alias("GRP_ID"),
    F.col("DSLink137.GRP_NAME").alias("GRP_NM"),
    F.col("DSLink137.PROD_ID").alias("PROD_ID"),
    F.col("DSLink137.CMPT_ID").alias("PROD_BILL_CMPNT_PFX"),
    F.col("DSLink137.SUB_ID").alias("SUB_ID"),
    F.col("DSLink137.MBR_ID").alias("MBR_ID"),
    F.col("DSLink137.MBR_CK").alias("MBR_CK"),
    F.col("DSLink137.MBR_FIRST_NAME").alias("FCTS_MBR_FIRST_NM"),
    F.col("DSLink137.MBR_LAST_NAME").alias("FCTS_MBR_LAST_NM"),
    F.col("DSLink137.ACCUM_TYP_CD").alias("FCTS_ACCUM_TYP_CD"),
    F.col("DSLink137.LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    F.col("DSLink137.BNF_BEG_DT").alias("leftRec_BNF_BEG_DT"),
    F.col("injoin_amt.BNF_BEG_DT").alias("rightRec_BNF_BEG_DT"),
    F.col("DSLink137.INDIVIDUAL_ACCUM_TTL").alias("FCTS_MBR_FCTS_TOT_AMT"),
    F.col("DSLink137.PBM_CT").alias("PBM_CT"),
    F.col("DSLink137.FAMILY_ACCUM_TTL").alias("FCTS_FMLY_FCTS_TOT_AMT"),
    F.col("DSLink137.CAR_ID").alias("CAR_ID"),
    F.col("DSLink137.MBR_ELIG_IN").alias("MBR_ELIG_IN"),
    F.col("DSLink137.MBR_EFF_DT").alias("MBR_EFF_DT"),
    F.col("DSLink137.MBR_TERM_DT").alias("MBR_TERM_DT"),
    F.col("DSLink137.PROD_CT").alias("PROD_CT"),
    F.col("DSLink137.MBR_PLN_LMT_NO").alias("MBR_PLN_LMT_NO"),
    F.col("DSLink137.FMLY_PLN_LMT_NO").alias("FMLY_PLN_LMT_NO"),
    F.col("injoin_amt.INDV_ACCUM_BAL_AMT").alias("OPTUMRX_MBR_OPTUMRX_TOT_AMT"),
    F.col("injoin_amt.FMLY_ACCUM_BAL_AMT").alias("OPTUMRX_FMLY_OPTUMRX_TOT_AMT"),
    F.col("injoin_amt.MBR_ID").alias("OPTUMRX_MBRID"),
    F.col("injoin_amt.ACCUM_TYP_CD").alias("OPTUMRX_ACCUM_TYP"),
    F.col("injoin_amt.MBR_FIRST_NM").alias("OPTUMRX_MBR_FIRST_NM"),
    F.col("injoin_amt.MBR_LAST_NM").alias("OPTUMRX_MBR_LAST_NM"),
    F.col("DSLink137.BPL_NBR").alias("BPL_NBR")
)

# --------------------------------------------------------------------------------
# Stage: xfm_pbmct (CTransformerStage)
# --------------------------------------------------------------------------------
df_xfm_pbmct_stagevars = (
    df_Join_Facets_OptumRX
    .withColumn(
        "MbrFacetsTotAmt",
        F.when(
            F.isnull(F.col("FCTS_MBR_FCTS_TOT_AMT")) | (F.length(F.col("FCTS_MBR_FCTS_TOT_AMT")) == 0),
            F.lit(0.00)
        ).otherwise(F.col("FCTS_MBR_FCTS_TOT_AMT"))
    )
    .withColumn(
        "FmlyFacetsTotAmt",
        F.when(
            F.isnull(F.col("FCTS_FMLY_FCTS_TOT_AMT")) | (F.length(F.col("FCTS_FMLY_FCTS_TOT_AMT")) == 0),
            F.lit(0.00)
        ).otherwise(F.col("FCTS_FMLY_FCTS_TOT_AMT"))
    )
    .withColumn(
        "MbrOptumRxTotAmt",
        F.when(
            F.isnull(F.col("OPTUMRX_MBR_OPTUMRX_TOT_AMT")) | (F.length(F.col("OPTUMRX_MBR_OPTUMRX_TOT_AMT")) == 0),
            F.lit(0.00)
        ).otherwise(F.col("OPTUMRX_MBR_OPTUMRX_TOT_AMT"))
    )
    .withColumn(
        "FmlyOptumRxTotAmt",
        F.when(
            F.isnull(F.col("OPTUMRX_FMLY_OPTUMRX_TOT_AMT")) | (F.length(F.col("OPTUMRX_FMLY_OPTUMRX_TOT_AMT")) == 0),
            F.lit(0.00)
        ).otherwise(F.col("OPTUMRX_FMLY_OPTUMRX_TOT_AMT"))
    )
    .withColumn(
        "PBMCt",
        F.when(
            F.isnull(F.col("GRP_ID")) | (F.trim(F.col("GRP_ID")) == ''),
            F.lit(9999)
        ).otherwise(F.col("PBM_CT"))
    )
    .withColumn("DefaultDt", F.lit("19000101"))
)

df_in_dsAccumReconPreProcExtr = df_xfm_pbmct_stagevars.filter(
    ~((F.col("PBMCt") == 0) & (F.col("MbrOptumRxTotAmt") == 0))
).select(
    F.when(
        F.isnull(F.col("GRP_TYP_CD")) | (F.trim(F.col("GRP_TYP_CD")) == ''),
        F.lit("NA")
    ).otherwise(F.col("GRP_TYP_CD")).alias("GRP_TYP_CD"),

    F.when(
        F.isnull(F.col("GRP_ID")) | (F.trim(F.col("GRP_ID")) == ''),
        F.lit("NA")
    ).otherwise(F.col("GRP_ID")).alias("GRP_ID"),

    F.when(
        F.isnull(F.col("GRP_NM")) | (F.trim(F.col("GRP_NM")) == ''),
        F.lit("NA")
    ).otherwise(F.col("GRP_NM")).alias("GRP_NM"),

    F.when(
        F.isnull(F.col("PROD_ID")) | (F.trim(F.col("PROD_ID")) == ''),
        F.lit("NA")
    ).otherwise(F.col("PROD_ID")).alias("PROD_ID"),

    F.when(
        (F.isnull(F.col("PROD_BILL_CMPNT_PFX"))) | (F.trim(F.col("PROD_ID")) == ''),
        F.lit("NA")
    ).otherwise(F.col("PROD_BILL_CMPNT_PFX")).alias("PROD_BILL_CMPNT_PFX"),

    F.when(
        (F.isnull(F.col("SUB_ID"))) | (F.trim(F.col("PROD_ID")) == ''),
        F.lit(0)
    ).otherwise(F.col("SUB_ID")).alias("SUB_ID"),

    F.when(
        F.isnull(F.col("MBR_ID")) | (F.trim(F.col("MBR_ID")) == ''),
        F.col("OPTUMRX_MBRID")
    ).otherwise(F.col("MBR_ID")).alias("MBR_ID"),

    F.when(
        F.isnull(F.col("MBR_CK")) | (F.length(F.col("MBR_CK")) == 0),
        F.lit(0)
    ).otherwise(F.col("MBR_CK")).alias("MBR_CK"),

    F.when(
        F.isnull(F.col("FCTS_MBR_FIRST_NM")) | (F.trim(F.col("FCTS_MBR_FIRST_NM")) == ''),
        F.col("OPTUMRX_MBR_FIRST_NM")
    ).otherwise(F.col("FCTS_MBR_FIRST_NM")).alias("MBR_FIRST_NM"),

    F.when(
        F.isnull(F.col("FCTS_MBR_LAST_NM")) | (F.trim(F.col("FCTS_MBR_LAST_NM")) == ''),
        F.col("OPTUMRX_MBR_LAST_NM")
    ).otherwise(F.col("FCTS_MBR_LAST_NM")).alias("MBR_LAST_NM"),

    F.when(
        F.isnull(F.col("FCTS_ACCUM_TYP_CD")) | (F.trim(F.col("FCTS_ACCUM_TYP_CD")) == ''),
        F.col("OPTUMRX_ACCUM_TYP")
    ).otherwise(F.col("FCTS_ACCUM_TYP_CD")).alias("ACCUM_TYP_CD"),

    F.when(
        F.isnull(F.col("MBR_ID")) | (F.trim(F.col("MBR_ID")) == ''),
        F.lit(0.00)
    ).otherwise(F.col("MBR_PLN_LMT_NO")).alias("MBR_PLN_LMT_NO"),

    F.lit(0.00).alias("MBR_PLN_TOT_PCT"),

    F.col("MbrOptumRxTotAmt").alias("MBR_OPTUMRX_TOT_AMT"),

    F.col("MbrFacetsTotAmt").alias("MBR_FCTS_TOT_AMT"),

    (F.col("MbrFacetsTotAmt") - F.col("MbrOptumRxTotAmt")).alias("MBR_DIFF_AMT"),

    F.when(
        F.isnull(F.col("MBR_ID")) | (F.trim(F.col("MBR_ID")) == ''),
        F.lit(0.00)
    ).otherwise(F.col("FMLY_PLN_LMT_NO")).alias("FMLY_PLN_LMT_NO"),

    F.lit(0.00).alias("FMLY_PLN_TOT_PCT"),

    F.col("FmlyOptumRxTotAmt").alias("FMLY_OPTUMRX_TOT_AMT"),

    F.col("FmlyFacetsTotAmt").alias("FMLY_FCTS_TOT_AMT"),

    (F.col("FmlyFacetsTotAmt") - F.col("FmlyOptumRxTotAmt")).alias("FMLY_DIFF_AMT"),

    F.when(
        F.isnull(F.col("MBR_ID")) | (F.trim(F.col("MBR_ID")) == ''),
        F.concat(F.col("rightRec_BNF_BEG_DT"), F.lit(" 00:00:000"))
    ).otherwise(
        F.concat(F.col("leftRec_BNF_BEG_DT"), F.lit(" 00:00:000"))
    ).alias("FCTS_LAST_UPDT_DTM"),

    F.lit("DSJobStartTimestamp").alias("LAST_UPDT_DTM"),

    F.when(
        (F.col("MbrFacetsTotAmt") - F.col("MbrOptumRxTotAmt")) != 0.00,
        F.lit("N")
    ).otherwise(F.lit("Y")).alias("RECON_IN"),

    F.when(
        F.isnull(F.col("CAR_ID")) | (F.trim(F.col("CAR_ID")) == ''),
        F.lit("NA")
    ).otherwise(F.col("CAR_ID")).alias("CAR_ID"),

    F.when(
        F.isnull(F.col("MBR_ELIG_IN")) | (F.trim(F.col("MBR_ELIG_IN")) == ''),
        F.lit("NA")
    ).otherwise(F.col("MBR_ELIG_IN")).alias("MBR_ELIG_IN"),

    F.when(
        F.isnull(F.col("MBR_ID")) | (F.trim(F.col("MBR_ID")) == ''),
        F.concat(
            F.col("DefaultDt").substr(F.lit(1), F.lit(4)), F.lit("-"),
            F.col("DefaultDt").substr(F.lit(5), F.lit(2)), F.lit("-"),
            F.col("DefaultDt").substr(F.lit(7), F.lit(2)), F.lit(" 00:00:000")
        )
    ).otherwise(F.col("MBR_EFF_DT")).alias("MBR_EFF_DT"),

    F.when(
        F.isnull(F.col("MBR_ID")) | (F.trim(F.col("MBR_ID")) == ''),
        F.concat(
            F.col("DefaultDt").substr(F.lit(1), F.lit(4)), F.lit("-"),
            F.col("DefaultDt").substr(F.lit(5), F.lit(2)), F.lit("-"),
            F.col("DefaultDt").substr(F.lit(7), F.lit(2)), F.lit(" 00:00:000")
        )
    ).otherwise(F.col("MBR_TERM_DT")).alias("MBR_TERM_DT"),

    F.when(
        F.isnull(F.col("PROD_CT")) | (F.trim(F.col("PROD_CT")) == ''),
        F.lit("NA")
    ).otherwise(F.col("PROD_CT")).alias("PROD_CT"),

    F.when(
        F.isnull(F.col("BPL_NBR")) | (F.trim(F.col("BPL_NBR")) == ''),
        F.lit("NA")
    ).otherwise(F.col("BPL_NBR")).alias("BPL_NBR")
)

# --------------------------------------------------------------------------------
# Stage: ds_AccumReconPreProcExtr (PxDataSet) => write to parquet
# --------------------------------------------------------------------------------
df_ds_AccumReconPreProcExtr_final = df_in_dsAccumReconPreProcExtr.select(
    "GRP_TYP_CD",
    "GRP_ID",
    "GRP_NM",
    "PROD_ID",
    "PROD_BILL_CMPNT_PFX",
    "SUB_ID",
    "MBR_ID",
    "MBR_CK",
    "MBR_FIRST_NM",
    "MBR_LAST_NM",
    "ACCUM_TYP_CD",
    "MBR_PLN_LMT_NO",
    "MBR_PLN_TOT_PCT",
    "MBR_OPTUMRX_TOT_AMT",
    "MBR_FCTS_TOT_AMT",
    "MBR_DIFF_AMT",
    "FMLY_PLN_LMT_NO",
    "FMLY_PLN_TOT_PCT",
    "FMLY_OPTUMRX_TOT_AMT",
    "FMLY_FCTS_TOT_AMT",
    "FMLY_DIFF_AMT",
    "FCTS_LAST_UPDT_DTM",
    "LAST_UPDT_DTM",
    "RECON_IN",
    "CAR_ID",
    "MBR_ELIG_IN",
    "MBR_EFF_DT",
    "MBR_TERM_DT",
    "PROD_CT",
    "BPL_NBR"
)

write_files(
    df_ds_AccumReconPreProcExtr_final,
    f"{adls_path}/datasets/AccumReconPreProcExtr.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# --------------------------------------------------------------------------------
# Stage: OPTUMRX_ACCUM_RECON_Trg_lookup (ODBCConnectorPX)
# --------------------------------------------------------------------------------
jdbc_url_OPTUMRX_ACCUM_RECON_Trg_lookup, jdbc_props_OPTUMRX_ACCUM_RECON_Trg_lookup = get_db_config(optumrx_accum_recon_trg_lookup_secret_name)
extract_query_OPTUMRX_ACCUM_RECON_Trg_lookup = """
SELECT MBR_ID, ACCUM_TYP_CD, FIRST_DIFF_DT, FIRST_DIFF_AMT, RECON_IN
FROM dbo.OPTUMRX_ACCUM_RECON
"""
df_OPTUMRX_ACCUM_RECON_Trg_lookup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_OPTUMRX_ACCUM_RECON_Trg_lookup)
    .options(**jdbc_props_OPTUMRX_ACCUM_RECON_Trg_lookup)
    .option("query", extract_query_OPTUMRX_ACCUM_RECON_Trg_lookup)
    .load()
)

df_DSLink136_Trg_lookup = df_OPTUMRX_ACCUM_RECON_Trg_lookup

# --------------------------------------------------------------------------------
# Stage: Copy_of_Trans2 (CTransformerStage)
# --------------------------------------------------------------------------------
df_Copy_of_Trans2 = df_DSLink136_Trg_lookup.select(
    F.col("MBR_ID").alias("MBR_ID"),
    F.col("ACCUM_TYP_CD").alias("ACCUM_TYP_CD"),
    TimestampToDate(F.col("FIRST_DIFF_DT")).alias("FIRST_DIFF_DT"),
    F.col("FIRST_DIFF_AMT").alias("FIRST_DIFF_AMT"),
    F.col("RECON_IN").alias("RECON_IN")
)

# --------------------------------------------------------------------------------
# Stage: AccumReconExtrTrgLkup_ds (PxDataSet) => write to parquet
# --------------------------------------------------------------------------------
df_AccumReconExtrTrgLkup_ds_final = df_Copy_of_Trans2.select(
    "MBR_ID",
    "ACCUM_TYP_CD",
    "FIRST_DIFF_DT",
    "FIRST_DIFF_AMT",
    "RECON_IN"
)

write_files(
    df_AccumReconExtrTrgLkup_ds_final,
    f"{adls_path}/datasets/AccumReconExtrTrgLkup.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)