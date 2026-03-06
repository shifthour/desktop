# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2005, 2006, 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Job Name :  BcaFepIdsProvXfrm
# MAGIC 
# MAGIC                            
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC 
# MAGIC Sudhir Bomshetty            2017-08-16        5781                            Initial Programming                                                IntegrateDev2               Kalyan Neelam             2017-09-25
# MAGIC 
# MAGIC Sudhir Bomshetty            2017-08-16        5781                         Updated logic for PROV_FCLTY_TYP_CD_SK,     IntegrateDev2               Kalyan Neelam             2017-12-26
# MAGIC                                                                                                    PROV_SPEC_CD_SK,  PROV_TYP_CD_SK                                                
# MAGIC 
# MAGIC Sudhir Bomshetty            2018-03-26        5781                     Updated logic for PROV_ID, PROV_ENTY_CD_SK     IntegrateDev2            Jaideep Mankala          04/02/2018
# MAGIC 
# MAGIC Saikiran Subbagari        2019-01-15        5887                            Added the TXNMY_CD column                                      IntegrateDev1     Kalyan Neelam              2019-02-13

# MAGIC Transformed Data will land into a Dataset for Primary Keying job.
# MAGIC 
# MAGIC Data is partitioned on Natural Key Columns.
# MAGIC JobName: BcaFepIdsProvXfrm
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType
)
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
UWSOwner = get_widget_value('UWSOwner','')
uws_secret_name = get_widget_value('uws_secret_name','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
RunID = get_widget_value('RunID','')
RunIDTimeStamp = get_widget_value('RunIDTimeStamp','')

# db2_K_CMN_PRCT_Lkp
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = (
    "SELECT \nCMN_PRCT_SK ,\nSRC_SYS_CD,\nCMN_PRCT_ID\n\nFROM \n"
    + IDSOwner
    + ".K_CMN_PRCT"
)
df_db2_K_CMN_PRCT_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# db2_CMN_PRCT
extract_query = (
    "SELECT\n       PRCT.NTNL_PROV_ID,\n       MIN(PRCT.CMN_PRCT_SK) as CMN_PRCT_SK\n"
    "FROM "
    + IDSOwner
    + ".CMN_PRCT PRCT\n"
    "GROUP BY\nPRCT.NTNL_PROV_ID"
)
df_db2_CMN_PRCT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# ds_PROV_FEP_CLM (read from a .ds => translate to .parquet)
schema_ds_PROV_FEP_CLM = StructType([
    StructField("PROV_ID", StringType(), True),
    StructField("OFC_CD", StringType(), True),
    StructField("LOC", StringType(), True),
    StructField("PROV_LAST_NM", StringType(), True),
    StructField("PROV_FIRST_NM", StringType(), True),
    StructField("PROV_MIDINIT", StringType(), True),
    StructField("PROV_NM_SFX", StringType(), True),
    StructField("PROV_ADDR_LN_1", StringType(), True),
    StructField("PROV_ADDR_LN_2", StringType(), True),
    StructField("PROV_CITY_NM", StringType(), True),
    StructField("PROV_CNTY_CD", StringType(), True),
    StructField("PROV_ST_CD", StringType(), True),
    StructField("PROV_ZIP", StringType(), True),
    StructField("PROV_TEL_NO", StringType(), True),
    StructField("PROV_EMAIL_ADDR", StringType(), True),
    StructField("PROV_FAX_NO", StringType(), True),
    StructField("PROV_NTNL_PROV_ID", StringType(), True),
    StructField("CSTM_PROV_TYP", StringType(), True),
    StructField("PCP_FLAG", StringType(), True),
    StructField("PRSCRB_PROV_FLAG", StringType(), True),
    StructField("PROV_ORIG_PLN_CD", StringType(), True),
    StructField("PROV_TAX_ID", StringType(), True),
    StructField("ALT_FLD_1", StringType(), True),
    StructField("ALT_FLD_2", StringType(), True),
    StructField("ALT_FLD_3", StringType(), True),
    StructField("CSTM_FLD_1", StringType(), True),
    StructField("CSTM_FLD_2", StringType(), True),
    StructField("OFC_MGR_LAST_NM", StringType(), True),
    StructField("OFC_MGR_FIRST_NM", StringType(), True),
    StructField("OFC_MGR_TTL", StringType(), True),
    StructField("OFC_MGR_EMAIL", StringType(), True),
    StructField("RUN_DT", StringType(), True),
    StructField("SRC_SYS", StringType(), True)
])
df_ds_PROV_FEP_CLM = spark.read.schema(schema_ds_PROV_FEP_CLM).parquet(
    f"{adls_path}/ds/FEP_PROV.{SrcSysCd}.extr.{RunID}.parquet"
)

# Xfrm_BusinessLogic (Transformer)
df_Xfrm_BusinessLogic_stagevars = df_ds_PROV_FEP_CLM.withColumn(
    "svProvId",
    F.when(
        ~F.col("PROV_ID").substr(F.lit(1), F.lit(1)).rlike("^[0-9]$"),
        trim(F.col("PROV_ID").substr(F.lit(5), (F.length("PROV_ID") - F.lit(4))))
    ).otherwise(F.col("PROV_ID"))
)

df_Xfrm_BusinessLogic_out = (
    df_Xfrm_BusinessLogic_stagevars
    .withColumn("PROV_ID", F.col("svProvId"))
    .withColumn("OFC_CD", F.col("OFC_CD"))
    .withColumn("LOC", F.col("LOC"))
    .withColumn("PROV_LAST_NM", trim(F.col("PROV_LAST_NM")))
    .withColumn("PROV_FIRST_NM", trim(F.col("PROV_FIRST_NM")))
    .withColumn("PROV_MIDINIT", F.col("PROV_MIDINIT"))
    .withColumn("PROV_NM_SFX", F.col("PROV_NM_SFX"))
    .withColumn("PROV_ADDR_LN_1", F.col("PROV_ADDR_LN_1"))
    .withColumn("PROV_ADDR_LN_2", F.col("PROV_ADDR_LN_2"))
    .withColumn("PROV_CITY_NM", F.col("PROV_CITY_NM"))
    .withColumn("PROV_CNTY_CD", F.col("PROV_CNTY_CD"))
    .withColumn("PROV_ST_CD", F.col("PROV_ST_CD"))
    .withColumn("PROV_ZIP", F.col("PROV_ZIP"))
    .withColumn("PROV_EMAIL_ADDR", F.col("PROV_EMAIL_ADDR"))
    .withColumn("PROV_NTNL_PROV_ID", F.col("PROV_NTNL_PROV_ID"))
    .withColumn(
        "CSTM_PROV_TYP_ENTY",
        F.when(
            (
                (
                    (F.length(trim(F.col("CSTM_PROV_TYP"))) == 0)
                    | (F.col("CSTM_PROV_TYP").isNull())
                )
                & (
                    (
                        F.col("PROV_ID").substr(F.lit(1), F.lit(4)) == F.lit("PHY_")
                    )
                    | (F.length(F.col("PROV_ID")) == 10)
                )
            ),
            F.lit("RX")
        )
        .when(
            (
                (
                    (F.length(trim(F.col("CSTM_PROV_TYP"))) == 0)
                    | (F.col("CSTM_PROV_TYP").isNull())
                )
                & (
                    ~(
                        (
                            F.col("PROV_ID").substr(F.lit(1), F.lit(4)) == F.lit("PHY_")
                        )
                        | (F.length(F.col("PROV_ID")) == 10)
                    )
                )
            ),
            F.lit("")
        )
        .otherwise(F.col("CSTM_PROV_TYP"))
    )
    .withColumn(
        "CSTM_PROV_TYP",
        F.when(
            ( (F.length(trim(F.col("CSTM_PROV_TYP"))) == 0) | (F.col("CSTM_PROV_TYP").isNull()) ),
            F.lit("0")
        )
        .otherwise(F.col("CSTM_PROV_TYP"))
    )
    .withColumn("PCP_FLAG", F.col("PCP_FLAG"))
    .withColumn("PRSCRB_PROV_FLAG", F.col("PRSCRB_PROV_FLAG"))
    .withColumn("PROV_ORIG_PLN_CD", F.col("PROV_ORIG_PLN_CD"))
    .withColumn("PROV_TAX_ID", F.col("PROV_TAX_ID"))
    .withColumn("ALT_FLD_1", F.col("ALT_FLD_1"))
    .withColumn("ALT_FLD_2", F.col("ALT_FLD_2"))
    .withColumn("ALT_FLD_3", F.col("ALT_FLD_3"))
    .withColumn("CSTM_FLD_1", F.col("CSTM_FLD_1"))
    .withColumn("CSTM_FLD_2", F.col("CSTM_FLD_2"))
    .withColumn("OFC_MGR_LAST_NM", F.col("OFC_MGR_LAST_NM"))
    .withColumn("OFC_MGR_FIRST_NM", F.col("OFC_MGR_FIRST_NM"))
    .withColumn("OFC_MGR_TTL", F.col("OFC_MGR_TTL"))
    .withColumn("OFC_MGR_EMAIL", F.col("OFC_MGR_EMAIL"))
    .withColumn("RUN_DT", F.col("RUN_DT"))
    .withColumn("SRC_SYS", F.col("SRC_SYS"))
    .withColumn("SRC_SYS_CD", F.lit("FACETS"))
)

# db2_P_NTNL_PROV_TXNMY_lkp
jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)
extract_query = (
    "SELECT DISTINCT\nNTNL_PROV_ID,\nPROV_ORG_NM,\nPROV_LAST_NM,\nPROV_FIRST_NM,\nPROV_MID_NM\n"
    "FROM "
    + EDWOwner
    + ".P_NTNL_PROV_TXNMY"
)
df_db2_P_NTNL_PROV_TXNMY_lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", extract_query)
    .load()
)

# Cpy_TXNMY
df_Cpy_TXNMY = df_db2_P_NTNL_PROV_TXNMY_lkp.select(
    F.col("NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    F.col("PROV_ORG_NM").alias("PROV_ORG_NM_lkp"),
    F.col("PROV_LAST_NM").alias("PROV_LAST_NM_lkp"),
    F.col("PROV_FIRST_NM").alias("PROV_FIRST_NM_lkp"),
    F.col("PROV_MID_NM").alias("PROV_MID_NM_lkp")
)

# UwsSelPricCritr
jdbc_url_uws, jdbc_props_uws = get_db_config(uws_secret_name)
extract_query = (
    "SELECT CRITR_VAL_FROM_TX, CRITR_VAL_THRU_TX , SEL_PRCS_ITEM_ID FROM "
    + UWSOwner
    + ".SEL_PRCS_CRITR\nWHERE\nSEL_PRCS_ID = 'FEP BCBSA'"
)
df_UwsSelPricCritr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_uws)
    .options(**jdbc_props_uws)
    .option("query", extract_query)
    .load()
)

# fltr_SelProcCritr
df_fltr_SelProcCritr_Ref_ProvTyp = df_UwsSelPricCritr.filter("SEL_PRCS_ITEM_ID= 'PROVTYP'")
df_fltr_SelProcCritr_Ref_ProvSpec = df_UwsSelPricCritr.filter("SEL_PRCS_ITEM_ID= 'PROVSPEC'")
df_fltr_SelProcCritr_Ref_FcltyTyp = df_UwsSelPricCritr.filter("SEL_PRCS_ITEM_ID= 'FCLTYTYP'")

# ds_CD_MPPNG_Data
schema_ds_CD_MPPNG_Data = StructType([
    StructField("CD_MPPNG_SK", StringType(), True),
    StructField("SRC_CD", StringType(), True),
    StructField("SRC_CD_NM", StringType(), True),
    StructField("SRC_CLCTN_CD", StringType(), True),
    StructField("SRC_DRVD_LKUP_VAL", StringType(), True),
    StructField("SRC_DOMAIN_NM", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("TRGT_CD", StringType(), True),
    StructField("TRGT_CD_NM", StringType(), True),
    StructField("TRGT_CLCTN_CD", StringType(), True),
    StructField("TRGT_DOMAIN_NM", StringType(), True)
])
df_ds_CD_MPPNG_Data = spark.read.schema(schema_ds_CD_MPPNG_Data).parquet(
    f"{adls_path}/ds/CD_MPPNG.parquet"
)

# fltr_Cd_MppngData
df_fltr_Cd_MppngData = df_ds_CD_MPPNG_Data.filter(
    "SRC_SYS_CD='BCA' AND SRC_CLCTN_CD='BCA' AND TRGT_CLCTN_CD='IDS' "
    "AND SRC_DOMAIN_NM='PROVIDER ENTITY' AND TRGT_DOMAIN_NM='PROVIDER ENTITY'"
)

# Lkup_Codes (joins)
df_Lkup_Codes_joined = (
    df_Xfrm_BusinessLogic_out.alias("lnk_FepIdsProvXfrmLkup_in")
    .join(
        df_Cpy_TXNMY.alias("Lnk_Txnmy"),
        on=(
            F.col("lnk_FepIdsProvXfrmLkup_in.PROV_NTNL_PROV_ID")
            == F.col("Lnk_Txnmy.NTNL_PROV_ID")
        ),
        how="left"
    )
    .join(
        df_fltr_SelProcCritr_Ref_ProvTyp.alias("Ref_ProvTyp"),
        on=(
            F.col("lnk_FepIdsProvXfrmLkup_in.CSTM_PROV_TYP")
            == F.col("Ref_ProvTyp.CRITR_VAL_FROM_TX")
        ),
        how="left"
    )
    .join(
        df_fltr_SelProcCritr_Ref_ProvSpec.alias("Ref_ProvSpec"),
        on=(
            F.col("lnk_FepIdsProvXfrmLkup_in.CSTM_PROV_TYP")
            == F.col("Ref_ProvSpec.CRITR_VAL_FROM_TX")
        ),
        how="left"
    )
    .join(
        df_fltr_SelProcCritr_Ref_FcltyTyp.alias("Ref_FcltyTyp"),
        on=(
            F.col("lnk_FepIdsProvXfrmLkup_in.CSTM_PROV_TYP")
            == F.col("Ref_FcltyTyp.CRITR_VAL_FROM_TX")
        ),
        how="left"
    )
    .join(
        df_db2_CMN_PRCT.alias("Ref_CmnPrctSk"),
        on=[],
        how="left"
    )
    .join(
        df_fltr_Cd_MppngData.alias("ref_ProvEntyCdSK"),
        on=[],
        how="left"
    )
)

df_Lkup_Codes = df_Lkup_Codes_joined.select(
    F.col("lnk_FepIdsProvXfrmLkup_in.PROV_ID").alias("PROV_ID"),
    F.col("lnk_FepIdsProvXfrmLkup_in.OFC_CD").alias("OFC_CD"),
    F.col("lnk_FepIdsProvXfrmLkup_in.LOC").alias("LOC"),
    F.col("lnk_FepIdsProvXfrmLkup_in.PROV_LAST_NM").alias("PROV_LAST_NM"),
    F.col("lnk_FepIdsProvXfrmLkup_in.PROV_FIRST_NM").alias("PROV_FIRST_NM"),
    F.col("lnk_FepIdsProvXfrmLkup_in.PROV_MIDINIT").alias("PROV_MIDINIT"),
    F.col("lnk_FepIdsProvXfrmLkup_in.PROV_NM_SFX").alias("PROV_NM_SFX"),
    F.col("lnk_FepIdsProvXfrmLkup_in.PROV_ADDR_LN_1").alias("PROV_ADDR_LN_1"),
    F.col("lnk_FepIdsProvXfrmLkup_in.PROV_ADDR_LN_2").alias("PROV_ADDR_LN_2"),
    F.col("lnk_FepIdsProvXfrmLkup_in.PROV_CITY_NM").alias("PROV_CITY_NM"),
    F.col("lnk_FepIdsProvXfrmLkup_in.PROV_CNTY_CD").alias("PROV_CNTY_CD"),
    F.col("lnk_FepIdsProvXfrmLkup_in.PROV_ST_CD").alias("PROV_ST_CD"),
    F.col("lnk_FepIdsProvXfrmLkup_in.PROV_ZIP").alias("PROV_ZIP"),
    F.col("lnk_FepIdsProvXfrmLkup_in.PROV_EMAIL_ADDR").alias("PROV_EMAIL_ADDR"),
    F.col("lnk_FepIdsProvXfrmLkup_in.PROV_NTNL_PROV_ID").alias("PROV_NTNL_PROV_ID"),
    F.col("lnk_FepIdsProvXfrmLkup_in.CSTM_PROV_TYP").alias("CSTM_PROV_TYP"),
    F.col("lnk_FepIdsProvXfrmLkup_in.PCP_FLAG").alias("PCP_FLAG"),
    F.col("lnk_FepIdsProvXfrmLkup_in.PRSCRB_PROV_FLAG").alias("PRSCRB_PROV_FLAG"),
    F.col("lnk_FepIdsProvXfrmLkup_in.PROV_ORIG_PLN_CD").alias("PROV_ORIG_PLN_CD"),
    F.col("lnk_FepIdsProvXfrmLkup_in.PROV_TAX_ID").alias("PROV_TAX_ID"),
    F.col("lnk_FepIdsProvXfrmLkup_in.ALT_FLD_1").alias("ALT_FLD_1"),
    F.col("lnk_FepIdsProvXfrmLkup_in.ALT_FLD_2").alias("ALT_FLD_2"),
    F.col("lnk_FepIdsProvXfrmLkup_in.ALT_FLD_3").alias("ALT_FLD_3"),
    F.col("lnk_FepIdsProvXfrmLkup_in.CSTM_FLD_1").alias("CSTM_FLD_1"),
    F.col("lnk_FepIdsProvXfrmLkup_in.CSTM_FLD_2").alias("CSTM_FLD_2"),
    F.col("lnk_FepIdsProvXfrmLkup_in.OFC_MGR_LAST_NM").alias("OFC_MGR_LAST_NM"),
    F.col("lnk_FepIdsProvXfrmLkup_in.OFC_MGR_FIRST_NM").alias("OFC_MGR_FIRST_NM"),
    F.col("lnk_FepIdsProvXfrmLkup_in.OFC_MGR_TTL").alias("OFC_MGR_TTL"),
    F.col("lnk_FepIdsProvXfrmLkup_in.OFC_MGR_EMAIL").alias("OFC_MGR_EMAIL"),
    F.col("lnk_FepIdsProvXfrmLkup_in.RUN_DT").alias("RUN_DT"),
    F.col("lnk_FepIdsProvXfrmLkup_in.SRC_SYS").alias("SRC_SYS"),
    F.col("Lnk_Txnmy.PROV_ORG_NM_lkp").alias("PROV_ORG_NM_lkp"),
    F.col("Lnk_Txnmy.PROV_LAST_NM_lkp").alias("PROV_LAST_NM_lkp"),
    F.col("Lnk_Txnmy.PROV_FIRST_NM_lkp").alias("PROV_FIRST_NM_lkp"),
    F.col("Lnk_Txnmy.PROV_MID_NM_lkp").alias("PROV_MID_NM_lkp"),
    F.col("Lnk_Txnmy.NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    F.col("Ref_FcltyTyp.CRITR_VAL_THRU_TX").alias("PROV_FCLTY_TYP_CD"),
    F.col("Ref_ProvSpec.CRITR_VAL_THRU_TX").alias("PROV_SPEC_CD"),
    F.col("Ref_ProvTyp.CRITR_VAL_THRU_TX").alias("PROV_TYP_CD"),
    F.col("Ref_CmnPrctSk.CMN_PRCT_SK").alias("CMN_PRCT_SK"),
    F.col("ref_ProvEntyCdSK.TRGT_CD_NM").alias("TRGT_CD_NM"),
    F.col("ref_ProvEntyCdSK.CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("lnk_FepIdsProvXfrmLkup_in.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_FepIdsProvXfrmLkup_in.CSTM_PROV_TYP_ENTY").alias("CSTM_PROV_TYP_ENTY")
)

# Lkup_Fkey
df_Lkup_Fkey_joined = (
    df_Lkup_Codes.alias("lnk_FepIdsProv_Lkup_In")
    .join(
        df_db2_K_CMN_PRCT_Lkp.alias("Ref_CmnPrctSk"),
        on=[],
        how="left"
    )
)

df_Lkup_Fkey = df_Lkup_Fkey_joined.select(
    F.col("lnk_FepIdsProv_Lkup_In.PROV_ID").alias("PROV_ID"),
    F.col("lnk_FepIdsProv_Lkup_In.OFC_CD").alias("OFC_CD"),
    F.col("lnk_FepIdsProv_Lkup_In.LOC").alias("LOC"),
    F.col("lnk_FepIdsProv_Lkup_In.PROV_LAST_NM").alias("PROV_LAST_NM"),
    F.col("lnk_FepIdsProv_Lkup_In.PROV_FIRST_NM").alias("PROV_FIRST_NM"),
    F.col("lnk_FepIdsProv_Lkup_In.PROV_MIDINIT").alias("PROV_MIDINIT"),
    F.col("lnk_FepIdsProv_Lkup_In.PROV_NM_SFX").alias("PROV_NM_SFX"),
    F.col("lnk_FepIdsProv_Lkup_In.PROV_ADDR_LN_1").alias("PROV_ADDR_LN_1"),
    F.col("lnk_FepIdsProv_Lkup_In.PROV_ADDR_LN_2").alias("PROV_ADDR_LN_2"),
    F.col("lnk_FepIdsProv_Lkup_In.PROV_CITY_NM").alias("PROV_CITY_NM"),
    F.col("lnk_FepIdsProv_Lkup_In.PROV_CNTY_CD").alias("PROV_CNTY_CD"),
    F.col("lnk_FepIdsProv_Lkup_In.PROV_ST_CD").alias("PROV_ST_CD"),
    F.col("lnk_FepIdsProv_Lkup_In.PROV_ZIP").alias("PROV_ZIP"),
    F.col("lnk_FepIdsProv_Lkup_In.PROV_EMAIL_ADDR").alias("PROV_EMAIL_ADDR"),
    F.col("lnk_FepIdsProv_Lkup_In.PROV_NTNL_PROV_ID").alias("PROV_NTNL_PROV_ID"),
    F.col("lnk_FepIdsProv_Lkup_In.CSTM_PROV_TYP").alias("CSTM_PROV_TYP"),
    F.col("lnk_FepIdsProv_Lkup_In.PCP_FLAG").alias("PCP_FLAG"),
    F.col("lnk_FepIdsProv_Lkup_In.PRSCRB_PROV_FLAG").alias("PRSCRB_PROV_FLAG"),
    F.col("lnk_FepIdsProv_Lkup_In.PROV_ORIG_PLN_CD").alias("PROV_ORIG_PLN_CD"),
    F.col("lnk_FepIdsProv_Lkup_In.PROV_TAX_ID").alias("PROV_TAX_ID"),
    F.col("lnk_FepIdsProv_Lkup_In.ALT_FLD_1").alias("ALT_FLD_1"),
    F.col("lnk_FepIdsProv_Lkup_In.ALT_FLD_2").alias("ALT_FLD_2"),
    F.col("lnk_FepIdsProv_Lkup_In.ALT_FLD_3").alias("ALT_FLD_3"),
    F.col("lnk_FepIdsProv_Lkup_In.CSTM_FLD_1").alias("CSTM_FLD_1"),
    F.col("lnk_FepIdsProv_Lkup_In.CSTM_FLD_2").alias("CSTM_FLD_2"),
    F.col("lnk_FepIdsProv_Lkup_In.OFC_MGR_LAST_NM").alias("OFC_MGR_LAST_NM"),
    F.col("lnk_FepIdsProv_Lkup_In.OFC_MGR_FIRST_NM").alias("OFC_MGR_FIRST_NM"),
    F.col("lnk_FepIdsProv_Lkup_In.OFC_MGR_TTL").alias("OFC_MGR_TTL"),
    F.col("lnk_FepIdsProv_Lkup_In.OFC_MGR_EMAIL").alias("OFC_MGR_EMAIL"),
    F.col("lnk_FepIdsProv_Lkup_In.RUN_DT").alias("RUN_DT"),
    F.col("lnk_FepIdsProv_Lkup_In.SRC_SYS").alias("SRC_SYS"),
    F.col("lnk_FepIdsProv_Lkup_In.PROV_ORG_NM_lkp").alias("PROV_ORG_NM_lkp"),
    F.col("lnk_FepIdsProv_Lkup_In.PROV_LAST_NM_lkp").alias("PROV_LAST_NM_lkp"),
    F.col("lnk_FepIdsProv_Lkup_In.PROV_FIRST_NM_lkp").alias("PROV_FIRST_NM_lkp"),
    F.col("lnk_FepIdsProv_Lkup_In.PROV_MID_NM_lkp").alias("PROV_MID_NM_lkp"),
    F.col("lnk_FepIdsProv_Lkup_In.NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    F.col("lnk_FepIdsProv_Lkup_In.PROV_FCLTY_TYP_CD").alias("PROV_FCLTY_TYP_CD"),
    F.col("lnk_FepIdsProv_Lkup_In.PROV_SPEC_CD").alias("PROV_SPEC_CD"),
    F.col("lnk_FepIdsProv_Lkup_In.PROV_TYP_CD").alias("PROV_TYP_CD"),
    F.col("Ref_CmnPrctSk.CMN_PRCT_ID").alias("CMN_PRCT_ID"),
    F.col("lnk_FepIdsProv_Lkup_In.CMN_PRCT_SK").alias("CMN_PRCT_SK"),
    F.col("lnk_FepIdsProv_Lkup_In.TRGT_CD_NM").alias("TRGT_CD_NM"),
    F.col("lnk_FepIdsProv_Lkup_In.CD_MPPNG_SK").alias("PROV_ENTY_CD_SK"),
    F.col("lnk_FepIdsProv_Lkup_In.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_FepIdsProv_Lkup_In.CSTM_PROV_TYP_ENTY").alias("CSTM_PROV_TYP_ENTY")
)

# Xfrm_BusinessRules
df_Xfrm_BusinessRules_stagevars = (
    df_Lkup_Fkey
    .withColumn(
        "svNtnlProvId",
        F.when(
            (F.col("PROV_NTNL_PROV_ID") == F.col("NTNL_PROV_ID")),
            F.lit(1)
        ).otherwise(F.lit(0))
    )
    .withColumn(
        "svPrctrNm",
        F.when(
            (F.col("svNtnlProvId") == 0),
            F.when(
                F.col("PROV_FIRST_NM").isNull(),
                F.lit(" ") + F.lit(",") + F.lit(" ") + F.upper(trim(F.col("PROV_LAST_NM")))
            ).otherwise(
                F.when(
                    F.col("PROV_LAST_NM").isNull(),
                    F.upper(trim(F.col("PROV_FIRST_NM"))) + F.lit(",") + F.lit(" ") + F.lit(" ")
                ).otherwise(
                    F.upper(trim(F.col("PROV_FIRST_NM"))) + F.lit(",") + F.lit(" ") + F.upper(trim(F.col("PROV_LAST_NM")))
                )
            )
        ).otherwise(
            F.when(
                (
                    (
                        F.col("PROV_LAST_NM_lkp").isNull()
                        | (trim(F.col("PROV_LAST_NM_lkp")) == "")
                    )
                    & (
                        F.col("PROV_FIRST_NM_lkp").isNull()
                        | (trim(F.col("PROV_FIRST_NM_lkp")) == "")
                    )
                    & (
                        F.col("PROV_MID_NM_lkp").isNull()
                        | (trim(F.col("PROV_MID_NM_lkp")) == "")
                    )
                ),
                F.upper(trim(F.col("PROV_ORG_NM_lkp")))
            ).otherwise(
                F.when(
                    (F.col("PROV_MID_NM_lkp").isNotNull() & F.col("PROV_NM_SFX").isNotNull()),
                    F.upper(trim(F.col("PROV_LAST_NM_lkp"))) + F.lit(",") + F.lit(" ")
                    + F.upper(trim(F.col("PROV_FIRST_NM_lkp"))) + F.lit(" ")
                    + F.upper(trim(F.col("PROV_MID_NM_lkp"))) + F.lit(" ")
                    + F.upper(trim(F.col("PROV_NM_SFX")))
                ).otherwise(
                    F.when(
                        (F.col("PROV_MID_NM_lkp").isNull() & F.col("PROV_NM_SFX").isNotNull()),
                        F.upper(trim(F.col("PROV_LAST_NM_lkp"))) + F.lit(",") + F.lit(" ")
                        + F.upper(trim(F.col("PROV_FIRST_NM_lkp"))) + F.lit(" ")
                        + F.lit(" ") + F.lit(" ") + F.upper(trim(F.col("PROV_NM_SFX")))
                    ).otherwise(
                        F.when(
                            (F.col("PROV_MID_NM_lkp").isNotNull() & F.col("PROV_NM_SFX").isNull()),
                            F.upper(trim(F.col("PROV_LAST_NM_lkp"))) + F.lit(",") + F.lit(" ")
                            + F.upper(trim(F.col("PROV_FIRST_NM_lkp"))) + F.lit(" ")
                            + F.upper(trim(F.col("PROV_MID_NM_lkp"))) + F.lit(" ") + F.lit(" ")
                        ).otherwise(F.lit(None))
                    )
                )
            )
        )
    )
    .withColumn(
        "svNonPrctrNm",
        F.when(
            (F.col("svNtnlProvId") == 0),
            F.when(
                (F.col("PROV_FIRST_NM").isNull() & F.col("PROV_LAST_NM").isNull()),
                F.lit(None)
            ).otherwise(
                F.when(
                    F.col("PROV_FIRST_NM").isNull(),
                    F.upper(trim(F.col("PROV_LAST_NM"))) + F.lit(" ") + F.lit(" ")
                ).otherwise(
                    F.when(
                        F.col("PROV_LAST_NM").isNull(),
                        F.lit(" ") + F.lit(" ") + F.upper(trim(F.col("PROV_FIRST_NM")))
                    ).otherwise(
                        F.upper(trim(F.col("PROV_LAST_NM"))) + F.lit(" ") + F.lit(" ")
                        + F.upper(trim(F.col("PROV_FIRST_NM")))
                    )
                )
            )
        ).otherwise(
            F.when(
                (
                    (
                        F.col("PROV_LAST_NM_lkp").isNull()
                        | (trim(F.col("PROV_LAST_NM_lkp")) == "")
                    )
                    & (
                        F.col("PROV_FIRST_NM_lkp").isNull()
                        | (trim(F.col("PROV_FIRST_NM_lkp")) == "")
                    )
                    & (
                        F.col("PROV_MID_NM_lkp").isNull()
                        | (trim(F.col("PROV_MID_NM_lkp")) == "")
                    )
                ),
                F.upper(trim(F.col("PROV_ORG_NM_lkp")))
            ).otherwise(
                F.when(
                    (
                        F.col("PROV_FIRST_NM_lkp").isNotNull()
                        & F.col("PROV_MID_NM_lkp").isNotNull()
                        & F.col("PROV_LAST_NM_lkp").isNotNull()
                    ),
                    F.upper(trim(F.col("PROV_FIRST_NM_lkp"))) + F.lit(" ")
                    + F.upper(trim(F.col("PROV_MID_NM_lkp"))) + F.lit(" ")
                    + F.upper(trim(F.col("PROV_LAST_NM_lkp")))
                ).otherwise(
                    F.when(
                        (
                            F.col("PROV_FIRST_NM_lkp").isNull()
                            & F.col("PROV_MID_NM_lkp").isNotNull()
                            & F.col("PROV_LAST_NM_lkp").isNotNull()
                        ),
                        F.lit(" ") + F.lit(" ") + F.upper(trim(F.col("PROV_MID_NM_lkp")))
                        + F.lit(" ") + F.upper(trim(F.col("PROV_LAST_NM_lkp")))
                    ).otherwise(
                        F.when(
                            (
                                F.col("PROV_FIRST_NM_lkp").isNotNull()
                                & F.col("PROV_MID_NM_lkp").isNull()
                                & F.col("PROV_LAST_NM_lkp").isNotNull()
                            ),
                            F.upper(trim(F.col("PROV_FIRST_NM_lkp"))) + F.lit(" ")
                            + F.lit(" ") + F.lit(" ")
                            + F.upper(trim(F.col("PROV_LAST_NM_lkp")))
                        ).otherwise(
                            F.when(
                                (
                                    F.col("PROV_FIRST_NM_lkp").isNotNull()
                                    & F.col("PROV_MID_NM_lkp").isNotNull()
                                    & F.col("PROV_LAST_NM_lkp").isNull()
                                ),
                                F.upper(trim(F.col("PROV_FIRST_NM_lkp"))) + F.lit(" ")
                                + F.upper(trim(F.col("PROV_MID_NM_lkp"))) + F.lit(" ")
                                + F.lit(" ")
                            ).otherwise(F.lit(None))
                        )
                    )
                )
            )
        )
    )
    .withColumn("svPrctProvNm", trim(F.col("svPrctrNm")))
    .withColumn("svNonPrctProvNm", trim(F.col("svNonPrctrNm")))
)

df_Xfrm_BusinessRules_out = df_Xfrm_BusinessRules_stagevars.select(
    F.expr("PROV_ID").alias("PROV_ID"),
    F.expr("OFC_CD").alias("OFC_CD"),
    F.expr("LOC").alias("LOC"),
    F.expr("PROV_LAST_NM").alias("PROV_LAST_NM"),
    F.expr("PROV_FIRST_NM").alias("PROV_FIRST_NM"),
    F.expr("PROV_MIDINIT").alias("PROV_MIDINIT"),
    F.expr("PROV_NM_SFX").alias("PROV_NM_SFX"),
    F.expr("PROV_ADDR_LN_1").alias("PROV_ADDR_LN_1"),
    F.expr("PROV_ADDR_LN_2").alias("PROV_ADDR_LN_2"),
    F.expr("PROV_CITY_NM").alias("PROV_CITY_NM"),
    F.expr("PROV_CNTY_CD").alias("PROV_CNTY_CD"),
    F.expr("PROV_ST_CD").alias("PROV_ST_CD"),
    F.expr("PROV_ZIP").alias("PROV_ZIP"),
    F.expr("PROV_EMAIL_ADDR").alias("PROV_EMAIL_ADDR"),
    F.expr("PROV_NTNL_PROV_ID").alias("PROV_NTNL_PROV_ID"),
    F.expr("CSTM_PROV_TYP").alias("CSTM_PROV_TYP"),
    F.expr("PCP_FLAG").alias("PCP_FLAG"),
    F.expr("PRSCRB_PROV_FLAG").alias("PRSCRB_PROV_FLAG"),
    F.expr("PROV_ORIG_PLN_CD").alias("PROV_ORIG_PLN_CD"),
    F.expr("PROV_TAX_ID").alias("PROV_TAX_ID"),
    F.expr("ALT_FLD_1").alias("ALT_FLD_1"),
    F.expr("ALT_FLD_2").alias("ALT_FLD_2"),
    F.expr("ALT_FLD_3").alias("ALT_FLD_3"),
    F.expr("CSTM_FLD_1").alias("CSTM_FLD_1"),
    F.expr("CSTM_FLD_2").alias("CSTM_FLD_2"),
    F.expr("OFC_MGR_LAST_NM").alias("OFC_MGR_LAST_NM"),
    F.expr("OFC_MGR_FIRST_NM").alias("OFC_MGR_FIRST_NM"),
    F.expr("OFC_MGR_TTL").alias("OFC_MGR_TTL"),
    F.expr("OFC_MGR_EMAIL").alias("OFC_MGR_EMAIL"),
    F.expr("RUN_DT").alias("RUN_DT"),
    F.expr("SRC_SYS").alias("SRC_SYS"),
    F.expr("svPrctProvNm").alias("svPrctProvNm"),
    F.expr("svNonPrctrNm").alias("svNonPrctrNm"),
    F.expr("svNtnlProvId").alias("svNtnlProvId"),
    F.expr("CMN_PRCT_ID").alias("CMN_PRCT_ID"),
    F.expr("CMN_PRCT_SK").alias("CMN_PRCT_SK"),
    F.expr("PROV_FCLTY_TYP_CD").alias("PROV_FCLTY_TYP_CD"),
    F.expr("PROV_SPEC_CD").alias("PROV_SPEC_CD"),
    F.expr("PROV_TYP_CD").alias("PROV_TYP_CD"),
    F.expr("PROV_ENTY_CD_SK").alias("PROV_ENTY_CD_SK"),
    F.expr("TRGT_CD_NM").alias("TRGT_CD_NM"),
    F.expr("SRC_SYS_CD").alias("SrcSysCd"),
    F.expr("CSTM_PROV_TYP_ENTY").alias("CSTM_PROV_TYP_ENTY")
)

df_lnk_FepIdsProvXfrm_Out = df_Xfrm_BusinessRules_out.select(
    F.concat(F.col("PROV_ID"), F.lit(";"), F.col("SrcSysCd")).alias("PRI_NAT_KEY_STRING"),
    F.lit(RunIDTimeStamp).alias("FIRST_RECYC_TS"),
    F.lit(0).alias("PROV_SK"),
    F.col("PROV_ID").alias("PROV_ID"),
    F.col("SrcSysCd").alias("SRC_SYS_CD"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.col("CMN_PRCT_ID").alias("CMN_PRCT"),
    F.lit("NA").alias("REL_GRP_PROV"),
    F.lit("NA").alias("REL_IPA_PROV"),
    F.lit("NA").alias("PROV_CAP_PAYMT_EFT_METH_CD"),
    F.lit("NA").alias("PROV_CLM_PAYMT_EFT_METH_CD"),
    F.lit("NA").alias("PROV_CLM_PAYMT_METH_CD"),
    F.col("CSTM_PROV_TYP_ENTY").alias("PROV_ENTY_CD"),
    F.when(
        F.col("TRGT_CD_NM") == F.lit("FACILITY"),
        F.col("PROV_FCLTY_TYP_CD")
    ).otherwise(F.lit("UNK")).alias("PROV_FCLTY_TYP_CD"),
    F.lit("NA").alias("PROV_PRCTC_TYP_CD"),
    F.col("CSTM_PROV_TYP").alias("PROV_SVC_CAT_CD"),
    F.when(
        F.col("TRGT_CD_NM") == F.lit("PRACTITIONER"),
        F.col("PROV_SPEC_CD")
    ).otherwise(F.lit("UNK")).alias("PROV_SPEC_CD"),
    F.lit("NA").alias("PROV_STTUS_CD"),
    F.lit("NA").alias("PROV_TERM_RSN_CD"),
    F.when(
        F.col("TRGT_CD_NM") == F.lit("PRACTITIONER"),
        F.col("PROV_TYP_CD")
    ).otherwise(F.lit("UNK")).alias("PROV_TYP_CD"),
    F.rpad(F.lit("2199-12-31"), 10, " ").alias("TERM_DT"),
    F.rpad(F.lit("2199-12-31"), 10, " ").alias("PAYMT_HOLD_DT"),
    F.lit("NA").alias("CLRNGHOUSE_ID"),
    F.lit("NA").alias("EDI_DEST_ID"),
    F.lit("").alias("EDI_DEST_QUAL"),
    F.when(
        (F.col("PROV_NTNL_PROV_ID").isNull() | (trim(F.col("PROV_NTNL_PROV_ID")) == "")),
        F.lit("NA")
    ).otherwise(F.col("PROV_NTNL_PROV_ID")).alias("NTNL_PROV_ID"),
    F.col("PROV_ID").alias("PROV_ADDR_ID"),
    F.when(
        F.col("TRGT_CD_NM") == F.lit("PRACTITIONER"),
        F.when(
            F.col("svPrctProvNm").isNull(),
            F.lit("")
        ).otherwise(F.col("svPrctProvNm"))
    ).otherwise(
        F.when(
            F.col("svNonPrctrNm").isNull(),
            F.lit("")
        ).otherwise(F.col("svNonPrctrNm"))
    ).alias("PROV_NM"),
    F.lit("NA").alias("TAX_ID"),
    F.lit("UNK").alias("TXNMY_CD")
)

df_lnk_FepIdsProvXfrmBProd_Out = df_Xfrm_BusinessRules_out.select(
    F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.col("PROV_ID").alias("PROV_ID"),
    F.when(
        F.col("PROV_NTNL_PROV_ID") == F.lit("1"),
        F.lit("1")
    ).otherwise(
        F.when(
            F.col("CMN_PRCT_SK").isNull(),
            F.lit("0")
        ).otherwise(F.col("CMN_PRCT_SK"))
    ).alias("CMN_PRCT_SK"),
    F.lit("1").alias("REL_GRP_PROV"),
    F.lit("1").alias("REL_IPA_PROV"),
    F.when(
        F.col("PROV_ENTY_CD_SK").isNull(),
        F.lit("0")
    ).otherwise(F.col("PROV_ENTY_CD_SK")).alias("PROV_ENTY_CD_SK")
)

# ds_PROV_Xfrm (write to parquet, removing .ds)
df_final_ds_PROV_Xfrm = df_lnk_FepIdsProvXfrm_Out.select(
    rpad(F.col("PRI_NAT_KEY_STRING"), F.length("PRI_NAT_KEY_STRING"), " ").alias("PRI_NAT_KEY_STRING"),
    rpad(F.col("FIRST_RECYC_TS"), F.length("FIRST_RECYC_TS"), " ").alias("FIRST_RECYC_TS"),
    rpad(F.col("PROV_SK"), F.length("PROV_SK"), " ").alias("PROV_SK"),
    rpad(F.col("PROV_ID"), F.length("PROV_ID"), " ").alias("PROV_ID"),
    rpad(F.col("SRC_SYS_CD"), F.length("SRC_SYS_CD"), " ").alias("SRC_SYS_CD"),
    rpad(F.col("CRT_RUN_CYC_EXCTN_SK"), F.length("CRT_RUN_CYC_EXCTN_SK"), " ").alias("CRT_RUN_CYC_EXCTN_SK"),
    rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"), F.length("LAST_UPDT_RUN_CYC_EXCTN_SK"), " ").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(F.col("SRC_SYS_CD_SK"), F.length("SRC_SYS_CD_SK"), " ").alias("SRC_SYS_CD_SK"),
    rpad(F.col("CMN_PRCT"), F.length("CMN_PRCT"), " ").alias("CMN_PRCT"),
    rpad(F.col("REL_GRP_PROV"), F.length("REL_GRP_PROV"), " ").alias("REL_GRP_PROV"),
    rpad(F.col("REL_IPA_PROV"), F.length("REL_IPA_PROV"), " ").alias("REL_IPA_PROV"),
    rpad(F.col("PROV_CAP_PAYMT_EFT_METH_CD"), F.length("PROV_CAP_PAYMT_EFT_METH_CD"), " ").alias("PROV_CAP_PAYMT_EFT_METH_CD"),
    rpad(F.col("PROV_CLM_PAYMT_EFT_METH_CD"), F.length("PROV_CLM_PAYMT_EFT_METH_CD"), " ").alias("PROV_CLM_PAYMT_EFT_METH_CD"),
    rpad(F.col("PROV_CLM_PAYMT_METH_CD"), F.length("PROV_CLM_PAYMT_METH_CD"), " ").alias("PROV_CLM_PAYMT_METH_CD"),
    rpad(F.col("PROV_ENTY_CD"), F.length("PROV_ENTY_CD"), " ").alias("PROV_ENTY_CD"),
    rpad(F.col("PROV_FCLTY_TYP_CD"), 3, " ").alias("PROV_FCLTY_TYP_CD"),
    rpad(F.col("PROV_PRCTC_TYP_CD"), F.length("PROV_PRCTC_TYP_CD"), " ").alias("PROV_PRCTC_TYP_CD"),
    rpad(F.col("PROV_SVC_CAT_CD"), F.length("PROV_SVC_CAT_CD"), " ").alias("PROV_SVC_CAT_CD"),
    rpad(F.col("PROV_SPEC_CD"), 3, " ").alias("PROV_SPEC_CD"),
    rpad(F.col("PROV_STTUS_CD"), F.length("PROV_STTUS_CD"), " ").alias("PROV_STTUS_CD"),
    rpad(F.col("PROV_TERM_RSN_CD"), F.length("PROV_TERM_RSN_CD"), " ").alias("PROV_TERM_RSN_CD"),
    rpad(F.col("PROV_TYP_CD"), 3, " ").alias("PROV_TYP_CD"),
    F.col("TERM_DT").alias("TERM_DT"),
    F.col("PAYMT_HOLD_DT").alias("PAYMT_HOLD_DT"),
    rpad(F.col("CLRNGHOUSE_ID"), F.length("CLRNGHOUSE_ID"), " ").alias("CLRNGHOUSE_ID"),
    rpad(F.col("EDI_DEST_ID"), F.length("EDI_DEST_ID"), " ").alias("EDI_DEST_ID"),
    rpad(F.col("EDI_DEST_QUAL"), F.length("EDI_DEST_QUAL"), " ").alias("EDI_DEST_QUAL"),
    rpad(F.col("NTNL_PROV_ID"), F.length("NTNL_PROV_ID"), " ").alias("NTNL_PROV_ID"),
    rpad(F.col("PROV_ADDR_ID"), F.length("PROV_ADDR_ID"), " ").alias("PROV_ADDR_ID"),
    rpad(F.col("PROV_NM"), F.length("PROV_NM"), " ").alias("PROV_NM"),
    rpad(F.col("TAX_ID"), F.length("TAX_ID"), " ").alias("TAX_ID"),
    rpad(F.col("TXNMY_CD"), F.length("TXNMY_CD"), " ").alias("TXNMY_CD")
)

write_files(
    df_final_ds_PROV_Xfrm,
    f"PROV.{SrcSysCd}.xfrm.{RunID}.parquet",
    delimiter=",",
    mode="overwrite",
    is_parquet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# seq_B_PROV_csv
df_seq_B_PROV_csv = df_lnk_FepIdsProvXfrmBProd_Out.select(
    F.col("SRC_SYS_CD_SK"),
    F.col("PROV_ID"),
    F.col("CMN_PRCT_SK"),
    F.col("REL_GRP_PROV"),
    F.col("REL_IPA_PROV"),
    F.col("PROV_ENTY_CD_SK")
)

write_files(
    df_seq_B_PROV_csv,
    f"{adls_path}/load/B_PROV.{SrcSysCd}.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)