# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2010 BLUE CROSS/BLUE SHIELD OF KANSAS CITY        
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                     Extracts from IDS CLS_PLN_DTL and loads the DataMart table MBRSH_DM_CLS_PLN_DTL
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #       Change Description                                                                        Development Project         Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                                 --------------------------------       -------------------------------   ----------------------------       
# MAGIC Kalyan Neelam        2010-10-28         3346                      Original Programming                                                                     IntegrateNewDevl          Steph Goddard          11/04/2010
# MAGIC 
# MAGIC Raja Gummadi         2013-07-12         5077                      Added 2 new columns at the end                                                   IntegrateNewDevl         Bhoomi Dasari            7/15/2013    
# MAGIC                                
# MAGIC Shiva Devagiri           07/17/2013     5114        Create Load File for  Web Access DM Table MBRSH_DM_MBR_COB      IntegrateWrhsDevl        Peter Marshall             10/21/2013         
# MAGIC 
# MAGIC Jag Yelavarthi           2013-12-18      5114                     Modified code to fix a coding error. PROD_ID column is                 IntegrateWrhsDevl           Bhoomi Dasari           2013-12-18
# MAGIC                                                                                         incorrectly  mapped from SRC_SYC_CD column 
# MAGIC Kalyan Neelam          2015-11-04         5212                    Added Hosted_Grp_Lookup to drop Host group records                IntegrateDev1                Bhoomi Dasari            11/5/2015    
# MAGIC        
# MAGIC Karthik Chintalapani        2016-11-11               5634      Added logic for new column PLN_YR_BEG_DT_MO_DAY   IntegrateDev2                         Kalyan Neelam            2016-11-28

# MAGIC Drop Hosted Groups
# MAGIC Read from source table CLS_PLN_DTL from IDS.
# MAGIC Job Name: IdsDmMbrshDmClsPlnDtlExtr
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC CLS_PLN_DTL_PROD_CAT_CD_SK,
# MAGIC CLS_SK,
# MAGIC CLS_PLN_DTL_NTWK_SET_PFX_CD_,
# MAGIC ALPHA_PFX_SK AND
# MAGIC PROD_SK
# MAGIC Add Defaults and Null Handling.
# MAGIC Write MBRSHP_DM_CLS_PLN_DTL Data into a Sequential file for Load Job IdsDmMbrshDmClsPlnDtlLoad.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# db2_GRP_In
query_db2_GRP_In = f"""
SELECT distinct
G.GRP_ID
FROM {IDSOwner}.GRP G,
     {IDSOwner}.PRNT_GRP PG
WHERE G.CLNT_SK = PG.CLNT_SK
  AND PG.CLNT_ID <> 'HS'
  AND G.GRP_ID NOT LIKE '90%'
"""
df_db2_GRP_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_db2_GRP_In)
    .load()
)

# db2_CLS_PLN_DTL_in
query_db2_CLS_PLN_DTL_in = f"""
SELECT
CLS_PLN_DTL.GRP_ID,
CLS_PLN_DTL.CLS_ID,
CLS_PLN_DTL.CLS_PLN_DTL_PROD_CAT_CD_SK,
CLS_PLN_DTL.CLS_PLN_ID,
CLS_PLN_DTL.EFF_DT_SK,
CLS_PLN_DTL.LAST_UPDT_RUN_CYC_EXCTN_SK,
CLS_PLN_DTL.CLS_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
CLS_PLN_DTL.CLS_PLN_DTL_NTWK_SET_PFX_CD_SK,
CLS_PLN_DTL.TERM_DT_SK,
CLS_PLN_DTL.ALPHA_PFX_SK,
CLS_PLN_DTL.PROD_SK,
CLS_PLN_DTL.PLN_BEG_DT_MO_DAY
FROM {IDSOwner}.CLS_PLN_DTL CLS_PLN_DTL
LEFT JOIN {IDSOwner}.CD_MPPNG CD
ON CLS_PLN_DTL.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
WHERE CLS_PLN_DTL.CLS_PLN_DTL_SK NOT IN (0,1)
ORDER BY
CLS_PLN_DTL.SRC_SYS_CD_SK,
CLS_PLN_DTL.GRP_ID,
CLS_PLN_DTL.CLS_ID,
CLS_PLN_DTL.CLS_PLN_DTL_PROD_CAT_CD_SK,
CLS_PLN_DTL.CLS_PLN_ID,
CLS_PLN_DTL.EFF_DT_SK
"""
df_db2_CLS_PLN_DTL_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_db2_CLS_PLN_DTL_in)
    .load()
)

# db2_PROD_In
query_db2_PROD_In = f"""
SELECT
PROD.PROD_SK,
PROD.PROD_ID
FROM {IDSOwner}.PROD PROD
"""
df_db2_PROD_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_db2_PROD_In)
    .load()
)

# db2_CLS_In
query_db2_CLS_In = f"""
SELECT
CLS.CLS_SK,
CLS.CLS_DESC
FROM {IDSOwner}.CLS CLS
"""
df_db2_CLS_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_db2_CLS_In)
    .load()
)

# db2_CD_MPPNG_In
query_db2_CD_MPPNG_In = f"""
SELECT
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_db2_CD_MPPNG_In)
    .load()
)

# Cpy_Cdes (PxCopy) - three outputs from the same input
df_Cpy_Cdes_ClsPlnDtl_ProdCatCd_lkup = df_db2_CD_MPPNG_In.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_Cpy_Cdes_AlphaPfxlkup = df_db2_CD_MPPNG_In.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_Cpy_Cdes_ClsPlnDtl_NtwkPfxCd_lkup = df_db2_CD_MPPNG_In.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

# lkp_Codes (PxLookup)
df_lkp_Codes = (
    df_db2_CLS_PLN_DTL_in.alias("lnk_IdsDmMbrshpDmClsPlnDtlExtr_InAbc")
    .join(
        df_Cpy_Cdes_ClsPlnDtl_NtwkPfxCd_lkup.alias("ClsPlnDtl_NtwkPfxCd_lkup"),
        F.col("lnk_IdsDmMbrshpDmClsPlnDtlExtr_InAbc.CLS_PLN_DTL_NTWK_SET_PFX_CD_SK")
        == F.col("ClsPlnDtl_NtwkPfxCd_lkup.CD_MPPNG_SK"),
        how="left"
    )
    .join(
        df_Cpy_Cdes_AlphaPfxlkup.alias("AlphaPfxlkup"),
        F.col("lnk_IdsDmMbrshpDmClsPlnDtlExtr_InAbc.ALPHA_PFX_SK")
        == F.col("AlphaPfxlkup.CD_MPPNG_SK"),
        how="left"
    )
    .join(
        df_Cpy_Cdes_ClsPlnDtl_ProdCatCd_lkup.alias("ClsPlnDtl_ProdCatCd_lkup"),
        F.col("lnk_IdsDmMbrshpDmClsPlnDtlExtr_InAbc.CLS_PLN_DTL_PROD_CAT_CD_SK")
        == F.col("ClsPlnDtl_ProdCatCd_lkup.CD_MPPNG_SK"),
        how="left"
    )
    .join(
        df_db2_PROD_In.alias("Prod_Id_lookup"),
        F.col("lnk_IdsDmMbrshpDmClsPlnDtlExtr_InAbc.PROD_SK")
        == F.col("Prod_Id_lookup.PROD_SK"),
        how="left"
    )
    .join(
        df_db2_CLS_In.alias("ClsDescLkup"),
        F.col("lnk_IdsDmMbrshpDmClsPlnDtlExtr_InAbc.CLS_SK")
        == F.col("ClsDescLkup.CLS_SK"),
        how="left"
    )
    .select(
        F.col("lnk_IdsDmMbrshpDmClsPlnDtlExtr_InAbc.GRP_ID").alias("GRP_ID"),
        F.col("lnk_IdsDmMbrshpDmClsPlnDtlExtr_InAbc.CLS_ID").alias("CLS_ID"),
        F.col("lnk_IdsDmMbrshpDmClsPlnDtlExtr_InAbc.CLS_PLN_ID").alias("CLS_PLN_ID"),
        F.col("lnk_IdsDmMbrshpDmClsPlnDtlExtr_InAbc.EFF_DT_SK").alias("EFF_DT_SK"),
        F.col("lnk_IdsDmMbrshpDmClsPlnDtlExtr_InAbc.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_IdsDmMbrshpDmClsPlnDtlExtr_InAbc.TERM_DT_SK").alias("TERM_DT_SK"),
        F.col("lnk_IdsDmMbrshpDmClsPlnDtlExtr_InAbc.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Prod_Id_lookup.PROD_ID").alias("PROD_ID"),
        F.col("ClsPlnDtl_ProdCatCd_lkup.TRGT_CD").alias("CLS_PLN_PROD_CAT_CD"),
        F.col("ClsPlnDtl_ProdCatCd_lkup.TRGT_CD_NM").alias("CLS_PLN_PROD_CAT_NM"),
        F.col("ClsDescLkup.CLS_SK").alias("CLSDES_CLS_SK"),
        F.col("ClsDescLkup.CLS_DESC").alias("CLSDES_CLS_DESC"),
        F.col("AlphaPfxlkup.CD_MPPNG_SK").alias("ALPHACD_CD_MPPNG_SK"),
        F.col("AlphaPfxlkup.TRGT_CD").alias("ALPHACD_TRGT_CD"),
        F.col("ClsPlnDtl_NtwkPfxCd_lkup.TRGT_CD").alias("CLS_PLN_DTL_NTWK_SET_PFX_CD"),
        F.col("ClsPlnDtl_NtwkPfxCd_lkup.TRGT_CD_NM").alias("CLS_PLN_DTL_NTWK_SET_PFX_NM"),
        F.col("lnk_IdsDmMbrshpDmClsPlnDtlExtr_InAbc.PLN_BEG_DT_MO_DAY").alias("PLN_BEG_DT_MO_DAY")
    )
)

# Hosted_Grp_Lookup (PxLookup)
df_Hosted_Grp_Lookup = (
    df_lkp_Codes.alias("lnkCodesLkpDataOut1")
    .join(
        df_db2_GRP_In.alias("Host_Grp_Id_lookup"),
        F.col("lnkCodesLkpDataOut1.GRP_ID") == F.col("Host_Grp_Id_lookup.GRP_ID"),
        how="inner"
    )
    .select(
        F.col("lnkCodesLkpDataOut1.GRP_ID").alias("GRP_ID"),
        F.col("lnkCodesLkpDataOut1.CLS_ID").alias("CLS_ID"),
        F.col("lnkCodesLkpDataOut1.CLS_PLN_ID").alias("CLS_PLN_ID"),
        F.col("lnkCodesLkpDataOut1.EFF_DT_SK").alias("EFF_DT_SK"),
        F.col("lnkCodesLkpDataOut1.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("lnkCodesLkpDataOut1.TERM_DT_SK").alias("TERM_DT_SK"),
        F.col("lnkCodesLkpDataOut1.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnkCodesLkpDataOut1.PROD_ID").alias("PROD_ID"),
        F.col("lnkCodesLkpDataOut1.CLS_PLN_PROD_CAT_CD").alias("CLS_PLN_PROD_CAT_CD"),
        F.col("lnkCodesLkpDataOut1.CLS_PLN_PROD_CAT_NM").alias("CLS_PLN_PROD_CAT_NM"),
        F.col("lnkCodesLkpDataOut1.CLSDES_CLS_SK").alias("CLSDES_CLS_SK"),
        F.col("lnkCodesLkpDataOut1.CLSDES_CLS_DESC").alias("CLSDES_CLS_DESC"),
        F.col("lnkCodesLkpDataOut1.ALPHACD_CD_MPPNG_SK").alias("ALPHACD_CD_MPPNG_SK"),
        F.col("lnkCodesLkpDataOut1.ALPHACD_TRGT_CD").alias("ALPHACD_TRGT_CD"),
        F.col("lnkCodesLkpDataOut1.CLS_PLN_DTL_NTWK_SET_PFX_CD").alias("CLS_PLN_DTL_NTWK_SET_PFX_CD"),
        F.col("lnkCodesLkpDataOut1.CLS_PLN_DTL_NTWK_SET_PFX_NM").alias("CLS_PLN_DTL_NTWK_SET_PFX_NM"),
        F.col("lnkCodesLkpDataOut1.PLN_BEG_DT_MO_DAY").alias("PLN_BEG_DT_MO_DAY")
    )
)

# xfm_BusinessLogic (CTransformerStage)
df_xfm_BusinessLogic = (
    df_Hosted_Grp_Lookup.alias("lnkCodesLkpDataOut")
    .withColumn(
        "CLS_PLN_PROD_CAT_CD",
        F.when(F.col("CLS_PLN_PROD_CAT_CD").isNull(), "").otherwise(F.col("CLS_PLN_PROD_CAT_CD"))
    )
    .withColumn(
        "CLS_PLN_EFF_DT",
        F.to_timestamp(F.col("EFF_DT_SK"), "yyyy-MM-dd")
    )
    .withColumn(
        "SRC_SYS_CD",
        F.when(F.trim(F.col("SRC_SYS_CD")) == "", "UNK ").otherwise(F.col("SRC_SYS_CD"))
    )
    .withColumn(
        "PROD_ID",
        F.when(F.trim(F.col("PROD_ID")) == "", "UNK ").otherwise(F.col("PROD_ID"))
    )
    .withColumn(
        "CLS_PLN_DTL_NTWK_SET_PFX_CD",
        F.when(F.col("CLS_PLN_DTL_NTWK_SET_PFX_CD").isNull(), "").otherwise(F.col("CLS_PLN_DTL_NTWK_SET_PFX_CD"))
    )
    .withColumn(
        "CLS_PLN_DTL_NTWK_SET_PFX_NM",
        F.when(F.col("CLS_PLN_DTL_NTWK_SET_PFX_NM").isNull(), "").otherwise(F.col("CLS_PLN_DTL_NTWK_SET_PFX_NM"))
    )
    .withColumn(
        "CLS_PLN_TERM_DT",
        F.to_timestamp(F.col("TERM_DT_SK"), "yyyy-MM-dd")
    )
    .withColumn(
        "CLS_PLN_PROD_CAT_NM",
        F.when(F.col("CLS_PLN_PROD_CAT_NM").isNull(), "").otherwise(F.col("CLS_PLN_PROD_CAT_NM"))
    )
    .withColumn(
        "LAST_UPDT_RUN_CYC_NO",
        F.lit(CurrRunCycle)
    )
    .withColumn(
        "IDS_LAST_UPDT_RUN_CYC_EXCTN_SK",
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
    .withColumn(
        "ALPHA_PFX_CD",
        F.when(
            F.col("ALPHACD_CD_MPPNG_SK").isNull() | (F.length(F.col("ALPHACD_CD_MPPNG_SK")) == 0),
            "UNK"
        ).otherwise(F.col("ALPHACD_TRGT_CD"))
    )
    .withColumn(
        "CLS_DESC",
        F.when(
            F.col("CLSDES_CLS_SK").isNull() | (F.length(F.col("CLSDES_CLS_SK")) == 0),
            "UNK"
        ).otherwise(F.col("CLSDES_CLS_DESC"))
    )
    .withColumn(
        "PLN_YR_BEG_DT_MO_DAY",
        F.col("PLN_BEG_DT_MO_DAY")
    )
)

df_final = df_xfm_BusinessLogic.select(
    F.col("GRP_ID"),
    F.col("CLS_ID"),
    F.col("CLS_PLN_ID"),
    F.col("CLS_PLN_PROD_CAT_CD"),
    F.col("CLS_PLN_EFF_DT"),
    F.col("SRC_SYS_CD"),
    F.rpad(F.col("PROD_ID"), 8, " ").alias("PROD_ID"),
    F.col("CLS_PLN_DTL_NTWK_SET_PFX_CD"),
    F.col("CLS_PLN_DTL_NTWK_SET_PFX_NM"),
    F.col("CLS_PLN_TERM_DT"),
    F.col("CLS_PLN_PROD_CAT_NM"),
    F.col("LAST_UPDT_RUN_CYC_NO"),
    F.col("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ALPHA_PFX_CD"),
    F.col("CLS_DESC"),
    F.rpad(F.col("PLN_YR_BEG_DT_MO_DAY"), 4, " ").alias("PLN_YR_BEG_DT_MO_DAY")
)

# seq_MBRSHP_DM_CLS_PLN_DTL_load (PxSequentialFile)
write_files(
    df_final,
    f"{adls_path}/load/MBRSHP_DM_CLS_PLN_DTL.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)