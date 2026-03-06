# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2005, 2009 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     EdwProductExtrSeq
# MAGIC 
# MAGIC Processing:
# MAGIC                     *
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:    Restart, no other steps necessary
# MAGIC                     Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                  Project/                                                                                                                                                                                                            Code                   Date
# MAGIC Developer           Date              Altiris #     Change Description                                                                                                                                                                         Reviewer            Reviewed
# MAGIC -------------------------  -------------------   -------------   ----------------------------------------------------------------------------------------------------------------------------                                                                             -------------------------  -------------------
# MAGIC Hugh Sisson       11-14-2005                    Original Program
# MAGIC SAndrew            12-14-2005                     Changed #$IDSInstance# to #$IDSOwner# and removed unused parameters 
# MAGIC                                                                     for when designing sequencer job.
# MAGIC Steph Goddard   01-19-2006                     Added columns to prod_bill_cmpnt_d for exprnc_cat_cd and fncl_lob_cd
# MAGIC SAndrew            11-14-2006                     Prodject 1756 - standardized Run Cycle logix.    Removed inner join logix on  
# MAGIC                                                                    main extraction to ensure all prod bill cmpnts are extracted.   
# MAGIC                                                                    Added hash files 2;hf_prod_bill_cmpnt_exp_cat;hf_prod_bill_cmpnt_fncl_lob
# MAGIC Vish Yerabati                  09-19-2008     3Q2008                        Added new columns                                                                                                                                      Steph Goddard   05/04/2009        
# MAGIC Pete Cundy                    09/16/2009    3556                            Modified Where Clause                         devlEDWnew                                                                                   Steph Goddard   10/07/2009
# MAGIC                                                                                                      
# MAGIC Kimberly Doty                          04-30-2010                 4044 Blue Renew v2     Added 26 new fields/columns after PROD_RATE_TYP_CD_SK    EnterpriseCurDevl           Steph Goddard   05/19/2010
# MAGIC 
# MAGIC Rajasekhar Mangalampally     06/05/2013            5114        Original Programming                                                                                                 EnterpriseWrhsDevl          Peter Marshall        8/8/2013
# MAGIC                                                                                               (Server to Parallel Conversion)

# MAGIC 1) FNCL_LOB_SK
# MAGIC 2) EXPRNC_CAT_SK
# MAGIC Read from source table PROD_BILL_CMPNT from IDS;
# MAGIC Pull records based on LAST_UPDT_RUN_CYC_EXCTN_SK
# MAGIC Add Defaults and Null Handling
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC This is Source extract data from an IDS table and apply Code denormalization, create a load ready file.
# MAGIC 
# MAGIC Job Name:
# MAGIC IdsEdwProdBillCmpntDExtr
# MAGIC 
# MAGIC Table:
# MAGIC PROD_BILL_CMPNT_D
# MAGIC This is a load ready file conforms to the target table structure
# MAGIC 
# MAGIC Write PROD_BILL_CMPNT_D Data into a Sequential file for Load Job.
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC 1) PROD_BILL_CMPNT_CONV_CAT_CD_SK
# MAGIC 2) PROD_BILL_CMPNT_CONV_TYP_CD_SK
# MAGIC 3) PROD_RATE_TYP_CD_SK
# MAGIC 4) CONV_RATE_PFX_MOD_CD_SK
# MAGIC 5) PRM_FCTR_AREA_PFX_MOD_CD_SK
# MAGIC 6) PRM_FCTR_AREA_TYP_CD_SK
# MAGIC 7) PRM_FCTR_SIC_MOD_CD_SK
# MAGIC 8) PROD-BILL_CMPNT_TYP_CD_SK
# MAGIC 9) SPLT_BILL_CD_SK
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType, DateType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
IDSRunCycle = get_widget_value('IDSRunCycle','')

# --------------------------------------------------------------------------------
# Stage: db2_PROD_BILL_CMPNT_D_in (DB2ConnectorPX)
# --------------------------------------------------------------------------------
jdbc_url_db2_PROD_BILL_CMPNT_D_in, jdbc_props_db2_PROD_BILL_CMPNT_D_in = get_db_config(ids_secret_name)
extract_query_db2_PROD_BILL_CMPNT_D_in = (
    f"SELECT "
    f"  P.PROD_BILL_CMPNT_SK, "
    f"  COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD, "
    f"  P.PROD_CMPNT_PFX_ID, "
    f"  P.PROD_BILL_CMPNT_ID, "
    f"  P.PROD_BILL_CMPNT_EFF_DT_SK, "
    f"  P.EXPRNC_CAT_SK, "
    f"  P.FNCL_LOB_SK, "
    f"  P.PROD_BILL_CMPNT_COV_CAT_CD_SK, "
    f"  P.PROD_BILL_CMPNT_COV_TYP_CD_SK, "
    f"  P.PROD_BILL_CMPNT_DESC, "
    f"  P.PROD_BILL_CMPNT_TERM_DT_SK, "
    f"  P.CONV_RATE_PFX, "
    f"  P.PROD_RATE_TYP_CD_SK, "
    f"  P.CONV_RATE_PFX_MOD_CD_SK, "
    f"  P.PRM_FCTR_AREA_PFX_ID, "
    f"  P.PRM_FCTR_AREA_PFX_MOD_CD_SK, "
    f"  P.PRM_FCTR_AREA_TYP_CD_SK, "
    f"  P.PRM_FCTR_SIC_PFX_ID, "
    f"  P.PRM_FCTR_SIC_MOD_CD_SK, "
    f"  P.CAP_PRM_PCT, "
    f"  P.COMSN_PRCS_IN, "
    f"  P.LOB_PCT, "
    f"  P.SPLT_BILL_CD_SK, "
    f"  P.SPLT_BILL_PCT, "
    f"  P.PROD_BILL_CMPNT_TYP_CD_SK, "
    f"  P.PROD_VOL_PFX_ID, "
    f"  P.PROD_VOL_RDUCTN_PFX_ID "
    f"FROM {IDSOwner}.PROD_BILL_CMPNT P "
    f"LEFT OUTER JOIN {IDSOwner}.CD_MPPNG CD ON P.SRC_SYS_CD_SK = CD.CD_MPPNG_SK "
    f"WHERE P.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSRunCycle}"
)

df_db2_PROD_BILL_CMPNT_D_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_PROD_BILL_CMPNT_D_in)
    .options(**jdbc_props_db2_PROD_BILL_CMPNT_D_in)
    .option("query", extract_query_db2_PROD_BILL_CMPNT_D_in)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: db2_CD_Mppng_extr (DB2ConnectorPX)
# --------------------------------------------------------------------------------
jdbc_url_db2_CD_Mppng_extr, jdbc_props_db2_CD_Mppng_extr = get_db_config(ids_secret_name)
extract_query_db2_CD_Mppng_extr = (
    f"SELECT CD_MPPNG_SK, COALESCE(TRGT_CD,'UNK') TRGT_CD, COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM "
    f"FROM {IDSOwner}.CD_MPPNG"
)

df_db2_CD_Mppng_extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_CD_Mppng_extr)
    .options(**jdbc_props_db2_CD_Mppng_extr)
    .option("query", extract_query_db2_CD_Mppng_extr)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: cpy_cd_mppng (PxCopy) -- producing multiple outputs
# --------------------------------------------------------------------------------
df_cpy_cd_mppng_refProdBillCmpntCovTyp = df_db2_CD_Mppng_extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_cpy_cd_mppng_refProdBillCmpntCovCatTyp = df_db2_CD_Mppng_extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_cpy_cd_mppng_refProdRateTyp = df_db2_CD_Mppng_extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_cpy_cd_mppng_ConvRatePfxModcdLkup = df_db2_CD_Mppng_extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_cpy_cd_mppng_PrmFctAreaPfxModCdLkp = df_db2_CD_Mppng_extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_cpy_cd_mppng_PrmFctrAreaTypCdLkup = df_db2_CD_Mppng_extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_cpy_cd_mppng_PrmFctrSicModCdLkup = df_db2_CD_Mppng_extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_cpy_cd_mppng_SpltBillCdLkup = df_db2_CD_Mppng_extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_cpy_cd_mppng_ProdBillCmpntTypeCdLkup = df_db2_CD_Mppng_extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

# --------------------------------------------------------------------------------
# Stage: db2_Fncl_Lob_Extr (DB2ConnectorPX)
# --------------------------------------------------------------------------------
jdbc_url_db2_Fncl_Lob_Extr, jdbc_props_db2_Fncl_Lob_Extr = get_db_config(ids_secret_name)
extract_query_db2_Fncl_Lob_Extr = (
    f"SELECT FNCL_LOB_CD, FNCL_LOB_SK "
    f"FROM {IDSOwner}.FNCL_LOB"
)

df_db2_Fncl_Lob_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_Fncl_Lob_Extr)
    .options(**jdbc_props_db2_Fncl_Lob_Extr)
    .option("query", extract_query_db2_Fncl_Lob_Extr)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: db2_Expr_Cat_Extr (DB2ConnectorPX)
# --------------------------------------------------------------------------------
jdbc_url_db2_Expr_Cat_Extr, jdbc_props_db2_Expr_Cat_Extr = get_db_config(ids_secret_name)
extract_query_db2_Expr_Cat_Extr = (
    f"SELECT EXPRNC_CAT_SK, EXPRNC_CAT_CD "
    f"FROM {IDSOwner}.EXPRNC_CAT"
)

df_db2_Expr_Cat_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_Expr_Cat_Extr)
    .options(**jdbc_props_db2_Expr_Cat_Extr)
    .option("query", extract_query_db2_Expr_Cat_Extr)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: lkp_ProdBillCmpnt (PxLookup)
# (Primary link: df_db2_PROD_BILL_CMPNT_D_in) - left join with df_db2_Fncl_Lob_Extr, df_db2_Expr_Cat_Extr
# --------------------------------------------------------------------------------
df_lkp_ProdBillCmpnt_temp = df_db2_PROD_BILL_CMPNT_D_in.alias("lnk_IdsEdwProdBillCmpntDExtr_InABC") \
    .join(
        df_db2_Fncl_Lob_Extr.alias("lnk_FnclLob"),
        F.col("lnk_IdsEdwProdBillCmpntDExtr_InABC.FNCL_LOB_SK") == F.col("lnk_FnclLob.FNCL_LOB_SK"),
        how="left"
    ) \
    .join(
        df_db2_Expr_Cat_Extr.alias("lnk_ExprCat"),
        F.col("lnk_IdsEdwProdBillCmpntDExtr_InABC.EXPRNC_CAT_SK") == F.col("lnk_ExprCat.EXPRNC_CAT_SK"),
        how="left"
    )

df_lkp_ProdBillCmpnt = df_lkp_ProdBillCmpnt_temp.select(
    F.col("lnk_IdsEdwProdBillCmpntDExtr_InABC.PROD_BILL_CMPNT_SK").alias("PROD_BILL_CMPNT_SK"),
    F.col("lnk_IdsEdwProdBillCmpntDExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_IdsEdwProdBillCmpntDExtr_InABC.PROD_CMPNT_PFX_ID").alias("PROD_CMPNT_PFX_ID"),
    F.col("lnk_IdsEdwProdBillCmpntDExtr_InABC.PROD_BILL_CMPNT_ID").alias("PROD_BILL_CMPNT_ID"),
    F.col("lnk_IdsEdwProdBillCmpntDExtr_InABC.PROD_BILL_CMPNT_EFF_DT_SK").alias("PROD_BILL_CMPNT_EFF_DT_SK"),
    F.col("lnk_IdsEdwProdBillCmpntDExtr_InABC.PROD_BILL_CMPNT_COV_CAT_CD_SK").alias("PROD_BILL_CMPNT_COV_CAT_CD_SK"),
    F.col("lnk_IdsEdwProdBillCmpntDExtr_InABC.PROD_BILL_CMPNT_COV_TYP_CD_SK").alias("PROD_BILL_CMPNT_COV_TYP_CD_SK"),
    F.col("lnk_IdsEdwProdBillCmpntDExtr_InABC.PROD_RATE_TYP_CD_SK").alias("PROD_RATE_TYP_CD_SK"),
    F.col("lnk_IdsEdwProdBillCmpntDExtr_InABC.PROD_BILL_CMPNT_TERM_DT_SK").alias("PROD_BILL_CMPNT_TERM_DT_SK"),
    F.col("lnk_IdsEdwProdBillCmpntDExtr_InABC.CONV_RATE_PFX").alias("CONV_RATE_PFX"),
    F.col("lnk_IdsEdwProdBillCmpntDExtr_InABC.PROD_BILL_CMPNT_DESC").alias("PROD_BILL_CMPNT_DESC"),
    F.col("lnk_IdsEdwProdBillCmpntDExtr_InABC.CAP_PRM_PCT").alias("CAP_PRM_PCT"),
    F.col("lnk_IdsEdwProdBillCmpntDExtr_InABC.COMSN_PRCS_IN").alias("COMSN_PRCS_IN"),
    F.col("lnk_IdsEdwProdBillCmpntDExtr_InABC.CONV_RATE_PFX_MOD_CD_SK").alias("CONV_RATE_PFX_MOD_CD_SK"),
    F.col("lnk_IdsEdwProdBillCmpntDExtr_InABC.LOB_PCT").alias("LOB_PCT"),
    F.col("lnk_IdsEdwProdBillCmpntDExtr_InABC.PRM_FCTR_AREA_PFX_ID").alias("PRM_FCTR_AREA_PFX_ID"),
    F.col("lnk_IdsEdwProdBillCmpntDExtr_InABC.PRM_FCTR_AREA_PFX_MOD_CD_SK").alias("PRM_FCTR_AREA_PFX_MOD_CD_SK"),
    F.col("lnk_IdsEdwProdBillCmpntDExtr_InABC.PRM_FCTR_AREA_TYP_CD_SK").alias("PRM_FCTR_AREA_TYP_CD_SK"),
    F.col("lnk_IdsEdwProdBillCmpntDExtr_InABC.PRM_FCTR_SIC_PFX_ID").alias("PRM_FCTR_SIC_PFX_ID"),
    F.col("lnk_IdsEdwProdBillCmpntDExtr_InABC.PRM_FCTR_SIC_MOD_CD_SK").alias("PRM_FCTR_SIC_MOD_CD_SK"),
    F.col("lnk_IdsEdwProdBillCmpntDExtr_InABC.PROD_BILL_CMPNT_TYP_CD_SK").alias("PROD_BILL_CMPNT_TYP_CD_SK"),
    F.col("lnk_IdsEdwProdBillCmpntDExtr_InABC.PROD_VOL_PFX_ID").alias("PROD_VOL_PFX_ID"),
    F.col("lnk_IdsEdwProdBillCmpntDExtr_InABC.PROD_VOL_RDUCTN_PFX_ID").alias("PROD_VOL_RDUCTN_PFX_ID"),
    F.col("lnk_IdsEdwProdBillCmpntDExtr_InABC.SPLT_BILL_CD_SK").alias("SPLT_BILL_CD_SK"),
    F.col("lnk_IdsEdwProdBillCmpntDExtr_InABC.SPLT_BILL_PCT").alias("SPLT_BILL_PCT"),
    F.col("lnk_IdsEdwProdBillCmpntDExtr_InABC.EXPRNC_CAT_SK").alias("EXPRNC_CAT_SK"),
    F.col("lnk_IdsEdwProdBillCmpntDExtr_InABC.FNCL_LOB_SK").alias("FNCL_LOB_SK"),
    F.col("lnk_FnclLob.FNCL_LOB_CD").alias("FNCL_LOB_CD"),
    F.col("lnk_ExprCat.EXPRNC_CAT_CD").alias("EXPRNC_CAT_CD")
)

# --------------------------------------------------------------------------------
# Stage: lkp_codes (PxLookup)
# --------------------------------------------------------------------------------
df_lkp_codes_temp = df_lkp_ProdBillCmpnt.alias("lnk_Prod") \
    .join(
        df_cpy_cd_mppng_refProdBillCmpntCovTyp.alias("refProdBillCmpntCovTyp"),
        F.col("lnk_Prod.PROD_BILL_CMPNT_COV_TYP_CD_SK") == F.col("refProdBillCmpntCovTyp.CD_MPPNG_SK"),
        how="left"
    ) \
    .join(
        df_cpy_cd_mppng_refProdBillCmpntCovCatTyp.alias("refProdBillCmpntCovCatTyp"),
        F.col("lnk_Prod.PROD_BILL_CMPNT_COV_CAT_CD_SK") == F.col("refProdBillCmpntCovCatTyp.CD_MPPNG_SK"),
        how="left"
    ) \
    .join(
        df_cpy_cd_mppng_refProdRateTyp.alias("refProdRateTyp"),
        F.col("lnk_Prod.PROD_RATE_TYP_CD_SK") == F.col("refProdRateTyp.CD_MPPNG_SK"),
        how="left"
    ) \
    .join(
        df_cpy_cd_mppng_ConvRatePfxModcdLkup.alias("ConvRatePfxModcdLkup"),
        F.col("lnk_Prod.CONV_RATE_PFX_MOD_CD_SK") == F.col("ConvRatePfxModcdLkup.CD_MPPNG_SK"),
        how="left"
    ) \
    .join(
        df_cpy_cd_mppng_PrmFctAreaPfxModCdLkp.alias("PrmFctAreaPfxModCdLkp"),
        F.col("lnk_Prod.PRM_FCTR_AREA_PFX_MOD_CD_SK") == F.col("PrmFctAreaPfxModCdLkp.CD_MPPNG_SK"),
        how="left"
    ) \
    .join(
        df_cpy_cd_mppng_PrmFctrAreaTypCdLkup.alias("PrmFctrAreaTypCdLkup"),
        F.col("lnk_Prod.PRM_FCTR_AREA_TYP_CD_SK") == F.col("PrmFctrAreaTypCdLkup.CD_MPPNG_SK"),
        how="left"
    ) \
    .join(
        df_cpy_cd_mppng_PrmFctrSicModCdLkup.alias("PrmFctrSicModCdLkup"),
        F.col("lnk_Prod.PRM_FCTR_SIC_MOD_CD_SK") == F.col("PrmFctrSicModCdLkup.CD_MPPNG_SK"),
        how="left"
    ) \
    .join(
        df_cpy_cd_mppng_SpltBillCdLkup.alias("SpltBillCdLkup"),
        F.col("lnk_Prod.SPLT_BILL_CD_SK") == F.col("SpltBillCdLkup.CD_MPPNG_SK"),
        how="left"
    ) \
    .join(
        df_cpy_cd_mppng_ProdBillCmpntTypeCdLkup.alias("ProdBillCmpntTypeCdLkup"),
        F.col("lnk_Prod.PROD_BILL_CMPNT_TYP_CD_SK") == F.col("ProdBillCmpntTypeCdLkup.CD_MPPNG_SK"),
        how="left"
    )

df_lkp_codes = df_lkp_codes_temp.select(
    F.col("lnk_Prod.PROD_BILL_CMPNT_SK").alias("PROD_BILL_CMPNT_SK"),
    F.col("lnk_Prod.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_Prod.PROD_CMPNT_PFX_ID").alias("PROD_CMPNT_PFX_ID"),
    F.col("lnk_Prod.PROD_BILL_CMPNT_ID").alias("PROD_BILL_CMPNT_ID"),
    F.col("lnk_Prod.PROD_BILL_CMPNT_EFF_DT_SK").alias("PROD_BILL_CMPNT_EFF_DT_SK"),
    F.col("lnk_Prod.EXPRNC_CAT_CD").alias("EXPRNC_CAT_CD"),
    F.col("lnk_Prod.EXPRNC_CAT_SK").alias("EXPRNC_CAT_SK"),
    F.col("lnk_Prod.FNCL_LOB_CD").alias("FNCL_LOB_CD"),
    F.col("lnk_Prod.FNCL_LOB_SK").alias("FNCL_LOB_SK"),
    F.col("refProdBillCmpntCovCatTyp.TRGT_CD").alias("PROD_BILL_CMPNT_COV_CAT_CD"),
    F.col("refProdBillCmpntCovCatTyp.TRGT_CD_NM").alias("PROD_BILL_CMPNT_COV_CAT_NM"),
    F.col("refProdBillCmpntCovTyp.TRGT_CD").alias("PROD_BILL_CMPNT_COV_TYP_CD"),
    F.col("refProdBillCmpntCovTyp.TRGT_CD_NM").alias("PROD_BILL_CMPNT_COV_TYP_NM"),
    F.col("lnk_Prod.PROD_BILL_CMPNT_DESC").alias("PROD_BILL_CMPNT_DESC"),
    F.col("lnk_Prod.PROD_BILL_CMPNT_TERM_DT_SK").alias("PROD_BILL_CMPNT_TERM_DT_SK"),
    F.col("lnk_Prod.CONV_RATE_PFX").alias("PROD_CONV_RATE_PFX"),
    F.col("refProdRateTyp.TRGT_CD").alias("PROD_RATE_TYP_CD"),
    F.col("refProdRateTyp.TRGT_CD_NM").alias("PROD_RATE_TYP_NM"),
    F.col("lnk_Prod.PROD_BILL_CMPNT_COV_CAT_CD_SK").alias("PROD_BILL_CMPNT_COV_CAT_CD_SK"),
    F.col("lnk_Prod.PROD_BILL_CMPNT_COV_TYP_CD_SK").alias("PROD_BILL_CMPNT_COV_TYP_CD_SK"),
    F.col("lnk_Prod.PROD_RATE_TYP_CD_SK").alias("PROD_RATE_TYP_CD_SK"),
    F.col("ConvRatePfxModcdLkup.TRGT_CD").alias("CONV_RATE_PFX_MOD_CD"),
    F.col("ConvRatePfxModcdLkup.TRGT_CD_NM").alias("CONV_RATE_PFX_MOD_NM"),
    F.col("lnk_Prod.PRM_FCTR_AREA_PFX_ID").alias("PRM_FCTR_AREA_PFX_ID"),
    F.col("PrmFctAreaPfxModCdLkp.TRGT_CD").alias("PRM_FCTR_AREA_PFX_MOD_CD"),
    F.col("PrmFctAreaPfxModCdLkp.TRGT_CD_NM").alias("PRM_FCTR_AREA_PFX_MOD_NM"),
    F.col("PrmFctrAreaTypCdLkup.TRGT_CD").alias("PRM_FCTR_AREA_TYP_CD"),
    F.col("PrmFctrAreaTypCdLkup.TRGT_CD_NM").alias("PRM_FCTR_AREA_TYP_NM"),
    F.col("lnk_Prod.PRM_FCTR_SIC_PFX_ID").alias("PRM_FCTR_SIC_PFX_ID"),
    F.col("PrmFctrSicModCdLkup.TRGT_CD").alias("PRM_FCTR_SIC_PFX_MOD_CD"),
    F.col("PrmFctrSicModCdLkup.TRGT_CD_NM").alias("PRM_FCTR_SIC_PFX_MOD_NM"),
    F.col("lnk_Prod.CAP_PRM_PCT").alias("PROD_BILL_CMPNT_CAP_PRM_PCT"),
    F.col("lnk_Prod.COMSN_PRCS_IN").alias("PROD_BILL_CMPNT_COMSN_PRCS_IN"),
    F.col("lnk_Prod.LOB_PCT").alias("PROD_BILL_CMPNT_LOB_PCT"),
    F.col("SpltBillCdLkup.TRGT_CD").alias("PROD_BILL_CMPNT_SPLT_BILL_CD"),
    F.col("SpltBillCdLkup.TRGT_CD_NM").alias("PROD_BILL_CMPNT_SPLT_BILL_NM"),
    F.col("lnk_Prod.SPLT_BILL_PCT").alias("PROD_BILL_CMPNT_SPLT_BILL_PCT"),
    F.col("ProdBillCmpntTypeCdLkup.TRGT_CD").alias("PROD_BILL_CMPNT_TYP_CD"),
    F.col("ProdBillCmpntTypeCdLkup.TRGT_CD_NM").alias("PROD_BILL_CMPNT_TYP_NM"),
    F.col("lnk_Prod.PROD_VOL_PFX_ID").alias("PROD_VOL_PFX_ID"),
    F.col("lnk_Prod.PROD_VOL_RDUCTN_PFX_ID").alias("PROD_VOL_RDUCTN_PFX_ID"),
    F.col("lnk_Prod.CONV_RATE_PFX_MOD_CD_SK").alias("CONV_RATE_PFX_MOD_CD_SK"),
    F.col("lnk_Prod.PRM_FCTR_AREA_PFX_MOD_CD_SK").alias("PRM_FCTR_AREA_PFX_MOD_CD_SK"),
    F.col("lnk_Prod.PRM_FCTR_AREA_TYP_CD_SK").alias("PRM_FCTR_AREA_TYP_CD_SK"),
    F.col("lnk_Prod.PRM_FCTR_SIC_MOD_CD_SK").alias("PRM_FCTR_SIC_PFX_MOD_CD_SK"),
    F.col("lnk_Prod.SPLT_BILL_CD_SK").alias("PROD_BILL_CMPNT_SPLT_BIL_CD_SK"),
    F.col("lnk_Prod.PROD_BILL_CMPNT_TYP_CD_SK").alias("PROD_BILL_CMPNT_TYP_CD_SK")
)

# --------------------------------------------------------------------------------
# Stage: xfm_BusinessRules (CTransformerStage)
# --------------------------------------------------------------------------------
df_xfm_BusinessRules = df_lkp_codes.select(
    F.col("PROD_BILL_CMPNT_SK").alias("PROD_BILL_CMPNT_SK"),
    F.when(trim(F.col("SRC_SYS_CD")) == "", F.lit("UNK")).otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    F.col("PROD_CMPNT_PFX_ID").alias("PROD_CMPNT_PFX_ID"),
    F.col("PROD_BILL_CMPNT_ID").alias("PROD_BILL_CMPNT_ID"),
    F.col("PROD_BILL_CMPNT_EFF_DT_SK").alias("PROD_BILL_CMPNT_EFF_DT_SK"),
    F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("EXPRNC_CAT_CD").alias("EXPRNC_CAT_CD"),
    F.col("EXPRNC_CAT_SK").alias("EXPRNC_CAT_SK"),
    F.col("FNCL_LOB_CD").alias("FNCL_LOB_CD"),
    F.col("FNCL_LOB_SK").alias("FNCL_LOB_SK"),
    F.when(trim(F.col("PROD_BILL_CMPNT_COV_CAT_CD")) == "", F.lit("UNK")).otherwise(F.col("PROD_BILL_CMPNT_COV_CAT_CD")).alias("PROD_BILL_CMPNT_COV_CAT_CD"),
    F.when(trim(F.col("PROD_BILL_CMPNT_COV_CAT_NM")) == "", F.lit("UNK")).otherwise(F.col("PROD_BILL_CMPNT_COV_CAT_NM")).alias("PROD_BILL_CMPNT_COV_CAT_NM"),
    F.when(trim(F.col("PROD_BILL_CMPNT_COV_TYP_CD")) == "", F.lit("UNK")).otherwise(F.col("PROD_BILL_CMPNT_COV_TYP_CD")).alias("PROD_BILL_CMPNT_COV_TYP_CD"),
    F.when(trim(F.col("PROD_BILL_CMPNT_COV_TYP_NM")) == "", F.lit("UNK")).otherwise(F.col("PROD_BILL_CMPNT_COV_TYP_NM")).alias("PROD_BILL_CMPNT_COV_TYP_NM"),
    F.col("PROD_BILL_CMPNT_DESC").alias("PROD_BILL_CMPNT_DESC"),
    F.col("PROD_BILL_CMPNT_TERM_DT_SK").alias("PROD_BILL_CMPNT_TERM_DT_SK"),
    F.col("PROD_CONV_RATE_PFX").alias("PROD_CONV_RATE_PFX"),
    F.when(trim(F.col("PROD_RATE_TYP_CD")) == "", F.lit("UNK")).otherwise(F.col("PROD_RATE_TYP_CD")).alias("PROD_RATE_TYP_CD"),
    F.when(trim(F.col("PROD_RATE_TYP_NM")) == "", F.lit("UNK")).otherwise(F.col("PROD_RATE_TYP_NM")).alias("PROD_RATE_TYP_NM"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PROD_BILL_CMPNT_COV_CAT_CD_SK").alias("PROD_BILL_CMPNT_COV_CAT_CD_SK"),
    F.col("PROD_BILL_CMPNT_COV_TYP_CD_SK").alias("PROD_BILL_CMPNT_COV_TYP_CD_SK"),
    F.col("PROD_RATE_TYP_CD_SK").alias("PROD_RATE_TYP_CD_SK"),
    F.when(trim(F.col("CONV_RATE_PFX_MOD_CD")) == "", F.lit("UNK")).otherwise(F.col("CONV_RATE_PFX_MOD_CD")).alias("CONV_RATE_PFX_MOD_CD"),
    F.when(trim(F.col("CONV_RATE_PFX_MOD_NM")) == "", F.lit("UNK")).otherwise(F.col("CONV_RATE_PFX_MOD_NM")).alias("CONV_RATE_PFX_MOD_NM"),
    F.col("PRM_FCTR_AREA_PFX_ID").alias("PRM_FCTR_AREA_PFX_ID"),
    F.when(trim(F.col("PRM_FCTR_AREA_PFX_MOD_CD")) == "", F.lit("UNK")).otherwise(F.col("PRM_FCTR_AREA_PFX_MOD_CD")).alias("PRM_FCTR_AREA_PFX_MOD_CD"),
    F.when(trim(F.col("PRM_FCTR_AREA_PFX_MOD_NM")) == "", F.lit("UNK")).otherwise(F.col("PRM_FCTR_AREA_PFX_MOD_NM")).alias("PRM_FCTR_AREA_PFX_MOD_NM"),
    F.when(trim(F.col("PRM_FCTR_AREA_TYP_CD")) == "", F.lit("UNK")).otherwise(F.col("PRM_FCTR_AREA_TYP_CD")).alias("PRM_FCTR_AREA_TYP_CD"),
    F.when(trim(F.col("PRM_FCTR_AREA_TYP_NM")) == "", F.lit("UNK")).otherwise(F.col("PRM_FCTR_AREA_TYP_NM")).alias("PRM_FCTR_AREA_TYP_NM"),
    F.col("PRM_FCTR_SIC_PFX_ID").alias("PRM_FCTR_SIC_PFX_ID"),
    F.when(trim(F.col("PRM_FCTR_SIC_PFX_MOD_CD")) == "", F.lit("UNK")).otherwise(F.col("PRM_FCTR_SIC_PFX_MOD_CD")).alias("PRM_FCTR_SIC_PFX_MOD_CD"),
    F.when(trim(F.col("PRM_FCTR_SIC_PFX_MOD_NM")) == "", F.lit("UNK")).otherwise(F.col("PRM_FCTR_SIC_PFX_MOD_NM")).alias("PRM_FCTR_SIC_PFX_MOD_NM"),
    F.col("PROD_BILL_CMPNT_CAP_PRM_PCT").alias("PROD_BILL_CMPNT_CAP_PRM_PCT"),
    F.col("PROD_BILL_CMPNT_COMSN_PRCS_IN").alias("PROD_BILL_CMPNT_COMSN_PRCS_IN"),
    F.col("PROD_BILL_CMPNT_LOB_PCT").alias("PROD_BILL_CMPNT_LOB_PCT"),
    F.when(trim(F.col("PROD_BILL_CMPNT_SPLT_BILL_CD")) == "", F.lit("UNK")).otherwise(F.col("PROD_BILL_CMPNT_SPLT_BILL_CD")).alias("PROD_BILL_CMPNT_SPLT_BILL_CD"),
    F.when(trim(F.col("PROD_BILL_CMPNT_SPLT_BILL_NM")) == "", F.lit("UNK")).otherwise(F.col("PROD_BILL_CMPNT_SPLT_BILL_NM")).alias("PROD_BILL_CMPNT_SPLT_BILL_NM"),
    F.col("PROD_BILL_CMPNT_SPLT_BILL_PCT").alias("PROD_BILL_CMPNT_SPLT_BILL_PCT"),
    F.when(trim(F.col("PROD_BILL_CMPNT_TYP_CD")) == "", F.lit("UNK")).otherwise(F.col("PROD_BILL_CMPNT_TYP_CD")).alias("PROD_BILL_CMPNT_TYP_CD"),
    F.when(trim(F.col("PROD_BILL_CMPNT_TYP_NM")) == "", F.lit("UNK")).otherwise(F.col("PROD_BILL_CMPNT_TYP_NM")).alias("PROD_BILL_CMPNT_TYP_NM"),
    F.col("PROD_VOL_PFX_ID").alias("PROD_VOL_PFX_ID"),
    F.col("PROD_VOL_RDUCTN_PFX_ID").alias("PROD_VOL_RDUCTN_PFX_ID"),
    F.col("CONV_RATE_PFX_MOD_CD_SK").alias("CONV_RATE_PFX_MOD_CD_SK"),
    F.col("PRM_FCTR_AREA_PFX_MOD_CD_SK").alias("PRM_FCTR_AREA_PFX_MOD_CD_SK"),
    F.col("PRM_FCTR_AREA_TYP_CD_SK").alias("PRM_FCTR_AREA_TYP_CD_SK"),
    F.col("PRM_FCTR_SIC_PFX_MOD_CD_SK").alias("PRM_FCTR_SIC_PFX_MOD_CD_SK"),
    F.col("PROD_BILL_CMPNT_SPLT_BIL_CD_SK").alias("PROD_BILL_CMPNT_SPLT_BIL_CD_SK"),
    F.col("PROD_BILL_CMPNT_TYP_CD_SK").alias("PROD_BILL_CMPNT_TYP_CD_SK")
)

# --------------------------------------------------------------------------------
# Stage: seq_PROD_BILL_CMPNT_D_csv_load (PxSequentialFile - writing)
# Must rpad columns that have SqlType char
# --------------------------------------------------------------------------------
df_final = df_xfm_BusinessRules.select(
    F.col("PROD_BILL_CMPNT_SK"),
    F.col("SRC_SYS_CD"),
    F.col("PROD_CMPNT_PFX_ID"),
    F.col("PROD_BILL_CMPNT_ID"),
    F.rpad(F.col("PROD_BILL_CMPNT_EFF_DT_SK"), 10, " ").alias("PROD_BILL_CMPNT_EFF_DT_SK"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("EXPRNC_CAT_CD"),
    F.col("EXPRNC_CAT_SK"),
    F.col("FNCL_LOB_CD"),
    F.col("FNCL_LOB_SK"),
    F.when(trim(F.col("PROD_BILL_CMPNT_COV_CAT_CD")) == "", F.lit("UNK"))
     .otherwise(F.col("PROD_BILL_CMPNT_COV_CAT_CD")).alias("PROD_BILL_CMPNT_COV_CAT_CD"),
    F.when(trim(F.col("PROD_BILL_CMPNT_COV_CAT_NM")) == "", F.lit("UNK"))
     .otherwise(F.col("PROD_BILL_CMPNT_COV_CAT_NM")).alias("PROD_BILL_CMPNT_COV_CAT_NM"),
    F.when(trim(F.col("PROD_BILL_CMPNT_COV_TYP_CD")) == "", F.lit("UNK"))
     .otherwise(F.col("PROD_BILL_CMPNT_COV_TYP_CD")).alias("PROD_BILL_CMPNT_COV_TYP_CD"),
    F.when(trim(F.col("PROD_BILL_CMPNT_COV_TYP_NM")) == "", F.lit("UNK"))
     .otherwise(F.col("PROD_BILL_CMPNT_COV_TYP_NM")).alias("PROD_BILL_CMPNT_COV_TYP_NM"),
    F.col("PROD_BILL_CMPNT_DESC"),
    F.rpad(F.col("PROD_BILL_CMPNT_TERM_DT_SK"), 10, " ").alias("PROD_BILL_CMPNT_TERM_DT_SK"),
    F.col("PROD_CONV_RATE_PFX"),
    F.when(trim(F.col("PROD_RATE_TYP_CD")) == "", F.lit("UNK"))
     .otherwise(F.col("PROD_RATE_TYP_CD")).alias("PROD_RATE_TYP_CD"),
    F.when(trim(F.col("PROD_RATE_TYP_NM")) == "", F.lit("UNK"))
     .otherwise(F.col("PROD_RATE_TYP_NM")).alias("PROD_RATE_TYP_NM"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PROD_BILL_CMPNT_COV_CAT_CD_SK"),
    F.col("PROD_BILL_CMPNT_COV_TYP_CD_SK"),
    F.col("PROD_RATE_TYP_CD_SK"),
    F.when(trim(F.col("CONV_RATE_PFX_MOD_CD")) == "", F.lit("UNK"))
     .otherwise(F.col("CONV_RATE_PFX_MOD_CD")).alias("CONV_RATE_PFX_MOD_CD"),
    F.when(trim(F.col("CONV_RATE_PFX_MOD_NM")) == "", F.lit("UNK"))
     .otherwise(F.col("CONV_RATE_PFX_MOD_NM")).alias("CONV_RATE_PFX_MOD_NM"),
    F.col("PRM_FCTR_AREA_PFX_ID"),
    F.when(trim(F.col("PRM_FCTR_AREA_PFX_MOD_CD")) == "", F.lit("UNK"))
     .otherwise(F.col("PRM_FCTR_AREA_PFX_MOD_CD")).alias("PRM_FCTR_AREA_PFX_MOD_CD"),
    F.when(trim(F.col("PRM_FCTR_AREA_PFX_MOD_NM")) == "", F.lit("UNK"))
     .otherwise(F.col("PRM_FCTR_AREA_PFX_MOD_NM")).alias("PRM_FCTR_AREA_PFX_MOD_NM"),
    F.when(trim(F.col("PRM_FCTR_AREA_TYP_CD")) == "", F.lit("UNK"))
     .otherwise(F.col("PRM_FCTR_AREA_TYP_CD")).alias("PRM_FCTR_AREA_TYP_CD"),
    F.when(trim(F.col("PRM_FCTR_AREA_TYP_NM")) == "", F.lit("UNK"))
     .otherwise(F.col("PRM_FCTR_AREA_TYP_NM")).alias("PRM_FCTR_AREA_TYP_NM"),
    F.col("PRM_FCTR_SIC_PFX_ID"),
    F.when(trim(F.col("PRM_FCTR_SIC_PFX_MOD_CD")) == "", F.lit("UNK"))
     .otherwise(F.col("PRM_FCTR_SIC_PFX_MOD_CD")).alias("PRM_FCTR_SIC_PFX_MOD_CD"),
    F.when(trim(F.col("PRM_FCTR_SIC_PFX_MOD_NM")) == "", F.lit("UNK"))
     .otherwise(F.col("PRM_FCTR_SIC_PFX_MOD_NM")).alias("PRM_FCTR_SIC_PFX_MOD_NM"),
    F.col("PROD_BILL_CMPNT_CAP_PRM_PCT"),
    F.rpad(F.col("PROD_BILL_CMPNT_COMSN_PRCS_IN"), 1, " ").alias("PROD_BILL_CMPNT_COMSN_PRCS_IN"),
    F.col("PROD_BILL_CMPNT_LOB_PCT"),
    F.when(trim(F.col("PROD_BILL_CMPNT_SPLT_BILL_CD")) == "", F.lit("UNK"))
     .otherwise(F.col("PROD_BILL_CMPNT_SPLT_BILL_CD")).alias("PROD_BILL_CMPNT_SPLT_BILL_CD"),
    F.when(trim(F.col("PROD_BILL_CMPNT_SPLT_BILL_NM")) == "", F.lit("UNK"))
     .otherwise(F.col("PROD_BILL_CMPNT_SPLT_BILL_NM")).alias("PROD_BILL_CMPNT_SPLT_BILL_NM"),
    F.col("PROD_BILL_CMPNT_SPLT_BILL_PCT"),
    F.when(trim(F.col("PROD_BILL_CMPNT_TYP_CD")) == "", F.lit("UNK"))
     .otherwise(F.col("PROD_BILL_CMPNT_TYP_CD")).alias("PROD_BILL_CMPNT_TYP_CD"),
    F.when(trim(F.col("PROD_BILL_CMPNT_TYP_NM")) == "", F.lit("UNK"))
     .otherwise(F.col("PROD_BILL_CMPNT_TYP_NM")).alias("PROD_BILL_CMPNT_TYP_NM"),
    F.col("PROD_VOL_PFX_ID"),
    F.col("PROD_VOL_RDUCTN_PFX_ID"),
    F.col("CONV_RATE_PFX_MOD_CD_SK"),
    F.col("PRM_FCTR_AREA_PFX_MOD_CD_SK"),
    F.col("PRM_FCTR_AREA_TYP_CD_SK"),
    F.col("PRM_FCTR_SIC_PFX_MOD_CD_SK"),
    F.col("PROD_BILL_CMPNT_SPLT_BIL_CD_SK"),
    F.col("PROD_BILL_CMPNT_TYP_CD_SK")
)

write_files(
    df_final,
    f"{adls_path}/load/PROD_BILL_CMPNT_D.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)