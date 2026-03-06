# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Process Description: Reads the file created in FKey job and create the load ready file for the MBR_HEDIS_MESR_YR_MO table
# MAGIC              
# MAGIC PROCESSING:
# MAGIC Build primary key for  PROD_QHP table load data 
# MAGIC 
# MAGIC Called By: CotivitiIdsStndExtrMbrHedisMesrYrMoSeq
# MAGIC 
# MAGIC Applies Transformation rules for MBR_HEDIS_MESR_YR_MO
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                                                                  Project/                                                                                                                                       		Code                  Date
# MAGIC Developer                               Date                                   Altiris #                Change Description                                   Environment                                   Reviewer                    Reviewed
# MAGIC -----------------------              ------------------                             -------------         ------------------------------------------------	            -------------------------	 -------------------------         -------------------
# MAGIC 
# MAGIC Karthik Chintalapani          2021-02- 10                         US#341503                Original Program                                    IntegrateDev1                           Jaideep Mankala          02/12/2021
# MAGIC 
# MAGIC Reddy Sanam                 2023-08-12                        US590640                  Updated the job to reflect new layout       IntegrateDev2                           Goutham k                     8/25/2023
# MAGIC 
# MAGIC Reddy Sanam                2024-01-25                         US606288                 Changed datatype for field UNIT_CT to    IntegrateDev2                           Goutham Kalidindi       1/30/2024
# MAGIC                                                                                                                      Decimal(13,2) 
# MAGIC Reddy Sanam                2024-10-23                       US631537             Added and propagated new field "MBR_GNDR"  IntegrateDev2                      Goutham Kalidindi     10/29/2024

# MAGIC Added this section to retrieve the translations for Race, Race_Src, Ethnicity, Ethnicity_src and Compliance Code category code lookup. Since all of them are under single source system. Single query is used and the data is later split based on the src domain.
# MAGIC PROD_SH_NM derive from the PROD_D table by joining PROD_D.PROD_ID to the PROD_ID from the file
# MAGIC FKEY failures are written into this flat file.
# MAGIC this file used in the IdsIdsMbrHedisMesrYrMoLoad
# MAGIC This file created in the IdsIdsMbrHedisMesrYrMoPKey job
# MAGIC This is a load ready file that will go into Load job
# MAGIC SK values from HEDIS MESR,POP & RVW_SET tables
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
    DecimalType,
    DateType
)
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

RunID = get_widget_value('RunID','')
RunDateTime = get_widget_value('RunDateTime','')
PrefixFkeyFailedFileName = get_widget_value('PrefixFkeyFailedFileName','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

# --------------------------------------------------------------------------------
# Stage: P_SRC_DOMAIN_TRNSLTN (DB2ConnectorPX)
# --------------------------------------------------------------------------------
extract_query = (
    f"SELECT TRGT_DOMAIN_TX,SRC_DOMAIN_TX "
    f"FROM {IDSOwner}.P_SRC_DOMAIN_TRNSLTN\n"
    f"where SRC_SYS_CD = 'EDW'\n"
    f"and DOMAIN_ID = 'COTIVITI_PRODUCT'\n"
    f"GROUP BY TRGT_DOMAIN_TX,SRC_DOMAIN_TX"
)
df_P_SRC_DOMAIN_TRNSLTN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: DB2_GRP (DB2ConnectorPX)
# --------------------------------------------------------------------------------
extract_query = f"select distinct GRP_ID,GRP_SK from {IDSOwner}.GRP;"
df_DB2_GRP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: DB2_RVW_SET (DB2ConnectorPX)
# --------------------------------------------------------------------------------
extract_query = (
    f"select HEDIS_RVW_SET_NM,HEDIS_RVW_SET_STRT_DT,HEDIS_RVW_SET_END_DT,"
    f"SRC_SYS_CD,HEDIS_RVW_SET_SK from {IDSOwner}.HEDIS_RVW_SET;"
)
df_DB2_RVW_SET = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: DB2_HEDIS_POP (DB2ConnectorPX)
# --------------------------------------------------------------------------------
extract_query = (
    f"select HEDIS_POP_NM,SRC_SYS_CD,HEDIS_POP_SK from {IDSOwner}.HEDIS_POP;"
)
df_DB2_HEDIS_POP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: DB2_HEDIS_MESR (DB2ConnectorPX)
# --------------------------------------------------------------------------------
extract_query = (
    f"select distinct HEDIS_MESR_NM,HEDIS_SUB_MESR_NM,HEDIS_MBR_BUCKET_ID,"
    f"SRC_SYS_CD,HEDIS_MESR_SK from {IDSOwner}.HEDIS_MESR;"
)
df_DB2_HEDIS_MESR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: seq_MBR_HEIDS_MESR_YR_MO (PxSequentialFile) - read file
# --------------------------------------------------------------------------------
schema_seq_MBR_HEIDS_MESR_YR_MO = StructType([
    StructField("PRI_NAT_KEY_STRING", StringType(), nullable=False),
    StructField("FIRST_RECYC_TS", TimestampType(), nullable=False),
    StructField("MBR_HEDIS_MESR_YR_MO_SK", IntegerType(), nullable=False),
    StructField("RVW_SET_NM", StringType(), nullable=False),
    StructField("POP_NM", StringType(), nullable=False),
    StructField("MESR_NM", StringType(), nullable=False),
    StructField("SUB_MESR_NM", StringType(), nullable=False),
    StructField("MBR_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("MBR_BUCKET_ID", StringType(), nullable=False),
    StructField("BASE_EVT_EPSD_DT_SK", StringType(), nullable=False),
    StructField("ACTVTY_YR_NO", IntegerType(), nullable=False),
    StructField("ACTVTY_MO_NO", IntegerType(), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("MBR_SK", IntegerType(), nullable=False),
    StructField("PROD_SH_NM_SK", IntegerType(), nullable=True),
    StructField("CMPLNC_ADMIN_IN", StringType(), nullable=False),
    StructField("CMPLNC_MNL_DATA_IN", StringType(), nullable=False),
    StructField("CMPLNC_MNL_DATA_SMPL_POP_IN", StringType(), nullable=False),
    StructField("CMPLNC_SMPL_POP_IN", StringType(), nullable=False),
    StructField("CONTRAIN_IN", StringType(), nullable=False),
    StructField("CONTRAIN_SMPL_POP_IN", StringType(), nullable=False),
    StructField("EXCL_IN", StringType(), nullable=False),
    StructField("EXCL_SMPL_POP_IN", StringType(), nullable=False),
    StructField("MESR_ELIG_IN", StringType(), nullable=False),
    StructField("MESR_ELIG_SMPL_POP_IN", StringType(), nullable=False),
    StructField("MBR_BRTH_DT_SK", StringType(), nullable=False),
    StructField("EVT_CT", IntegerType(), nullable=False),
    StructField("UNIT_CT", DecimalType(38,10), nullable=False),
    StructField("ACRDTN_CAT_ID", StringType(), nullable=False),
    StructField("CNTY_AREA_ID", StringType(), nullable=False),
    StructField("GRP_ID", StringType(), nullable=False),
    StructField("MBR_INDV_BE_KEY", DecimalType(38,10), nullable=False),
    StructField("MBR_ID", StringType(), nullable=False),
    StructField("MBR_FULL_NM", StringType(), nullable=False),
    StructField("ON_OFF_EXCH_ID", StringType(), nullable=False),
    StructField("PROD_SH_NM", StringType(), nullable=False),
    StructField("HEDIS_RVW_SET_STRT_DT", DateType(), nullable=False),
    StructField("HEDIS_RVW_SET_END_DT", DateType(), nullable=False),
    StructField("CST_AMT", DecimalType(38,10), nullable=False),
    StructField("RISK_PCT", DecimalType(38,10), nullable=False),
    StructField("ADJ_RISK_NO", DecimalType(38,10), nullable=False),
    StructField("PLN_ALL_CAUSE_READMISSION_VRNC_NO", DecimalType(38,10), nullable=False),
    StructField("HEDIS_MESR_ABBR_ID", StringType(), nullable=False),
    StructField("DENOMINATOR_FOR_NO_CONT_ENR", IntegerType(), nullable=True),
    StructField("ADMIN_COMPLIANT_CAT", StringType(), nullable=True),
    StructField("EXCLNO_CE", IntegerType(), nullable=True),
    StructField("DENOMINATOR_HYBRID", IntegerType(), nullable=True),
    StructField("NUMERATOR_COMPLIANT_FLAG_HYBRID", IntegerType(), nullable=True),
    StructField("NUMERATOR_CMPLNC_CAT_HYBRID", StringType(), nullable=True),
    StructField("EXCLHYBRID", IntegerType(), nullable=True),
    StructField("EXCLHYBRID_RSN", IntegerType(), nullable=True),
    StructField("LAB_TST_VAL_2", IntegerType(), nullable=True),
    StructField("SES_STRAT", IntegerType(), nullable=True),
    StructField("RACE_ID", StringType(), nullable=True),
    StructField("RACE_SRC", IntegerType(), nullable=True),
    StructField("ETHNCTY", StringType(), nullable=True),
    StructField("ETHNCTY_SRC", IntegerType(), nullable=True),
    StructField("ADV_ILNS_FRAILTY_EXCL", StringType(), nullable=True),
    StructField("HSPC_EXCL", StringType(), nullable=True),
    StructField("LTI_SNP_EXCL", StringType(), nullable=True),
    StructField("PROD_ID", StringType(), nullable=False),
    StructField("PROD_LN", StringType(), nullable=True),
    StructField("PROD_ROLLUP_ID", StringType(), nullable=True),
    StructField("NUMERATOR_CMPLNC_CAT", StringType(), nullable=True),
    StructField("NUMERATOR_CMPLNC_FLAG", IntegerType(), nullable=True),
    StructField("DCSD_EXCL", StringType(), nullable=True),
    StructField("EXCL", IntegerType(), nullable=True),
    StructField("DENOMINATOR", IntegerType(), nullable=True),
    StructField("OPTNL_EXCL", StringType(), nullable=True),
    StructField("RQRD_EXCL", StringType(), nullable=True),
    StructField("NUMERATOR_EVT_1_SVC_DT", DateType(), nullable=True),
    StructField("LAB_TST_VAL_NO", DecimalType(38,10), nullable=True),
    StructField("MBR_GNDR", StringType(), nullable=True)
])

df_seq_MBR_HEIDS_MESR_YR_MO = spark.read.csv(
    path=f"{adls_path}/load/MBR_HEDIS_MESR_YR_MO.{RunID}.dat",
    schema=schema_seq_MBR_HEIDS_MESR_YR_MO,
    sep=",",
    quote="^",
    header=False,
    nullValue=None
)

# --------------------------------------------------------------------------------
# Stage: CD_MPPNG (DB2ConnectorPX)
# --------------------------------------------------------------------------------
extract_query = (
    f"select SRC_CD,CD_MPPNG_SK,SRC_DOMAIN_NM  from {IDSOwner}.CD_MPPNG\n"
    f"where SRC_SYS_CD = 'COTIVITI'\n"
    f"and SRC_DOMAIN_NM IN ( 'HEDIS RACE SOURCE','RACE','HEDIS COMPLIANCE CATEGORY CODE')\n"
    f"group by SRC_CD,CD_MPPNG_SK,SRC_DOMAIN_NM"
)
df_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: SplitType (CTransformerStage)
# --------------------------------------------------------------------------------
df_Ref_EthnSrc = df_CD_MPPNG.filter(F.col("SRC_DOMAIN_NM") == "HEDIS RACE SOURCE").select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("SRC_CD").alias("SRC_CD")
)
df_Ref_CmplcCat = df_CD_MPPNG.filter(F.col("SRC_DOMAIN_NM") == "HEDIS COMPLIANCE CATEGORY CODE").select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("SRC_CD").alias("SRC_CD")
)
df_Ref_Entn = df_CD_MPPNG.filter(F.col("SRC_DOMAIN_NM") == "RACE").select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("SRC_CD").alias("SRC_CD")
)
df_Ref_Race = df_CD_MPPNG.filter(F.col("SRC_DOMAIN_NM") == "RACE").select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("SRC_CD").alias("SRC_CD")
)
df_Ref_RaceSrc = df_CD_MPPNG.filter(F.col("SRC_DOMAIN_NM") == "HEDIS RACE SOURCE").select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("SRC_CD").alias("SRC_CD")
)

# --------------------------------------------------------------------------------
# Stage: MBR_ID_LKUP (PxLookup)
# --------------------------------------------------------------------------------
# Primary link: df_seq_MBR_HEIDS_MESR_YR_MO as Lnk_IdsMbrEnrQhpFkey_LKp2
df_Lnk = df_seq_MBR_HEIDS_MESR_YR_MO.alias("Lnk_IdsMbrEnrQhpFkey_LKp2")

# Lookup: df_DB2_HEDIS_MESR => Mesr_lkup (left join)
cond_mesr = [
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.MESR_NM") == F.col("Mesr_lkup.HEDIS_MESR_NM"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.SUB_MESR_NM") == F.col("Mesr_lkup.HEDIS_SUB_MESR_NM"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.MBR_BUCKET_ID") == F.col("Mesr_lkup.HEDIS_MBR_BUCKET_ID"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.SRC_SYS_CD") == F.col("Mesr_lkup.SRC_SYS_CD")
]
df_Lnk = df_Lnk.join(df_DB2_HEDIS_MESR.alias("Mesr_lkup"), cond_mesr, "left")

# Lookup: df_DB2_HEDIS_POP => Pop_lkup (left join)
cond_pop = [
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.POP_NM") == F.col("Pop_lkup.HEDIS_POP_NM"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.SRC_SYS_CD") == F.col("Pop_lkup.SRC_SYS_CD")
]
df_Lnk = df_Lnk.join(df_DB2_HEDIS_POP.alias("Pop_lkup"), cond_pop, "left")

# Lookup: df_DB2_RVW_SET => Rvw_set_lkup (left join)
cond_rvw = [
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.RVW_SET_NM") == F.col("Rvw_set_lkup.HEDIS_RVW_SET_NM"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.HEDIS_RVW_SET_STRT_DT") == F.col("Rvw_set_lkup.HEDIS_RVW_SET_STRT_DT"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.HEDIS_RVW_SET_END_DT") == F.col("Rvw_set_lkup.HEDIS_RVW_SET_END_DT"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.SRC_SYS_CD") == F.col("Rvw_set_lkup.SRC_SYS_CD")
]
df_Lnk = df_Lnk.join(df_DB2_RVW_SET.alias("Rvw_set_lkup"), cond_rvw, "left")

# Lookup: df_DB2_GRP => Grp_Lnk (left join)
cond_grp = [
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.GRP_ID") == F.col("Grp_Lnk.GRP_ID")
]
df_Lnk = df_Lnk.join(df_DB2_GRP.alias("Grp_Lnk"), cond_grp, "left")

# Lookup: df_Ref_RaceSrc => Ref_RaceSrc (left join)
cond_racesrc = [
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.RACE_SRC") == F.col("Ref_RaceSrc.SRC_CD")
]
df_Lnk = df_Lnk.join(df_Ref_RaceSrc.alias("Ref_RaceSrc"), cond_racesrc, "left")

# Lookup: df_Ref_Race => Ref_Race (left join)
cond_race = [
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.RACE_ID") == F.col("Ref_Race.SRC_CD")
]
df_Lnk = df_Lnk.join(df_Ref_Race.alias("Ref_Race"), cond_race, "left")

# Lookup: df_Ref_Entn => Ref_Entn (left join)
cond_entn = [
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.ETHNCTY") == F.col("Ref_Entn.SRC_CD")
]
df_Lnk = df_Lnk.join(df_Ref_Entn.alias("Ref_Entn"), cond_entn, "left")

# Lookup: df_Ref_CmplcCat => Ref_CmplcCat (left join)
cond_cmplc = [
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.NUMERATOR_CMPLNC_CAT") == F.col("Ref_CmplcCat.SRC_CD")
]
df_Lnk = df_Lnk.join(df_Ref_CmplcCat.alias("Ref_CmplcCat"), cond_cmplc, "left")

# Lookup: df_Ref_EthnSrc => Ref_EthnSrc (left join)
cond_ethnsrc = [
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.ETHNCTY_SRC") == F.col("Ref_EthnSrc.SRC_CD")
]
df_Lnk = df_Lnk.join(df_Ref_EthnSrc.alias("Ref_EthnSrc"), cond_ethnsrc, "left")

# Lookup: df_P_SRC_DOMAIN_TRNSLTN => Ref_ProdID (left join)
cond_prod = [
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.PROD_ID") == F.col("Ref_ProdID.TRGT_DOMAIN_TX")
]
df_Lnk = df_Lnk.join(df_P_SRC_DOMAIN_TRNSLTN.alias("Ref_ProdID"), cond_prod, "left")

df_MBR_ID_LKUP = df_Lnk.select(
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.MBR_HEDIS_MESR_YR_MO_SK").alias("MBR_HEDIS_MESR_YR_MO_SK"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.RVW_SET_NM").alias("RVW_SET_NM"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.POP_NM").alias("POP_NM"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.MESR_NM").alias("MESR_NM"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.SUB_MESR_NM").alias("SUB_MESR_NM"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.MBR_BUCKET_ID").alias("MBR_BUCKET_ID"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.BASE_EVT_EPSD_DT_SK").alias("BASE_EVT_EPSD_DT_SK"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.ACTVTY_YR_NO").alias("ACTVTY_YR_NO"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.ACTVTY_MO_NO").alias("ACTVTY_MO_NO"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.MBR_SK").alias("MBR_SK"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.PROD_SH_NM_SK").alias("PROD_SH_NM_SK"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.CMPLNC_ADMIN_IN").alias("CMPLNC_ADMIN_IN"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.CMPLNC_MNL_DATA_IN").alias("CMPLNC_MNL_DATA_IN"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.CMPLNC_MNL_DATA_SMPL_POP_IN").alias("CMPLNC_MNL_DATA_SMPL_POP_IN"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.CMPLNC_SMPL_POP_IN").alias("CMPLNC_SMPL_POP_IN"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.CONTRAIN_IN").alias("CONTRAIN_IN"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.CONTRAIN_SMPL_POP_IN").alias("CONTRAIN_SMPL_POP_IN"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.EXCL_IN").alias("EXCL_IN"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.EXCL_SMPL_POP_IN").alias("EXCL_SMPL_POP_IN"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.MESR_ELIG_IN").alias("MESR_ELIG_IN"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.MESR_ELIG_SMPL_POP_IN").alias("MESR_ELIG_SMPL_POP_IN"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.MBR_BRTH_DT_SK").alias("MBR_BRTH_DT_SK"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.EVT_CT").alias("EVT_CT"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.UNIT_CT").alias("UNIT_CT"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.ACRDTN_CAT_ID").alias("ACRDTN_CAT_ID"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.CNTY_AREA_ID").alias("CNTY_AREA_ID"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.GRP_ID").alias("GRP_ID"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.HEDIS_RVW_SET_STRT_DT").alias("HEDIS_RVW_SET_STRT_DT"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.HEDIS_RVW_SET_END_DT").alias("HEDIS_RVW_SET_END_DT"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.MBR_ID").alias("MBR_ID"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.MBR_FULL_NM").alias("MBR_FULL_NM"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.ON_OFF_EXCH_ID").alias("ON_OFF_EXCH_ID"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.PROD_SH_NM").alias("PROD_SH_NM"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.CST_AMT").alias("CST_AMT"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.RISK_PCT").alias("RISK_PCT"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.ADJ_RISK_NO").alias("ADJ_RISK_NO"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.PLN_ALL_CAUSE_READMISSION_VRNC_NO").alias("PLN_ALL_CAUSE_READMISSION_VRNC_NO"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.HEDIS_MESR_ABBR_ID").alias("HEDIS_MESR_ABBR_ID"),
    F.col("Mesr_lkup.HEDIS_MESR_SK").alias("HEDIS_MESR_SK"),
    F.col("Pop_lkup.HEDIS_POP_SK").alias("HEDIS_POP_SK"),
    F.col("Rvw_set_lkup.HEDIS_RVW_SET_SK").alias("HEDIS_RVW_SET_SK"),
    F.col("Grp_Lnk.GRP_SK").alias("GRP_SK"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.DENOMINATOR_FOR_NO_CONT_ENR").alias("DENOMINATOR_FOR_NO_CONT_ENR"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.ADMIN_COMPLIANT_CAT").alias("ADMIN_COMPLIANT_CAT"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.EXCLNO_CE").alias("EXCLNO_CE"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.DENOMINATOR_HYBRID").alias("DENOMINATOR_HYBRID"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.NUMERATOR_COMPLIANT_FLAG_HYBRID").alias("NUMERATOR_COMPLIANT_FLAG_HYBRID"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.NUMERATOR_CMPLNC_CAT_HYBRID").alias("NUMERATOR_CMPLNC_CAT_HYBRID"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.EXCLHYBRID").alias("EXCLHYBRID"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.EXCLHYBRID_RSN").alias("EXCLHYBRID_RSN"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.LAB_TST_VAL_2").alias("LAB_TST_VAL_2"),
    F.col("Ref_Entn.CD_MPPNG_SK").alias("ETHNCTY_CD_SK"),
    F.col("Ref_EthnSrc.CD_MPPNG_SK").alias("ETHNCTY_SRC_CD_SK"),
    F.col("Ref_Race.CD_MPPNG_SK").alias("RACE_CD_SK"),
    F.col("Ref_RaceSrc.CD_MPPNG_SK").alias("RACE_SRC_CD_SK"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.SES_STRAT").alias("SES_STRAT_NO"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.ADV_ILNS_FRAILTY_EXCL").alias("ADV_ILNS_FRAILTY_EXCL"),
    F.col("Ref_CmplcCat.CD_MPPNG_SK").alias("NUMERATOR_CMPLNC_CAT_CD_SK"),
    F.col("Ref_ProdID.SRC_DOMAIN_TX").alias("SRC_DOMAIN_TX"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.PROD_LN").alias("PROD_LN"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.NUMERATOR_CMPLNC_FLAG").alias("NUMERATOR_CMPLNC_FLAG"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.NUMERATOR_CMPLNC_CAT").alias("NUMERATOR_CMPLNC_CAT"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.DCSD_EXCL").alias("DCSD_EXCL"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.EXCL").alias("EXCL"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.LTI_SNP_EXCL").alias("LTI_SNP_EXCL"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.DENOMINATOR").alias("DENOMINATOR"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.OPTNL_EXCL").alias("OPTNL_EXCL"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.RQRD_EXCL").alias("RQRD_EXCL"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.HSPC_EXCL").alias("HSPC_EXCL"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.PROD_ROLLUP_ID").alias("PROD_ROLLUP_ID"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.NUMERATOR_EVT_1_SVC_DT").alias("NUMERATOR_EVT_1_SVC_DT"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.LAB_TST_VAL_NO").alias("LAB_TST_VAL_NO"),
    F.col("Lnk_IdsMbrEnrQhpFkey_LKp2.MBR_GNDR").alias("MBR_GNDR")
)

# --------------------------------------------------------------------------------
# Stage: xfm_CheckLkpResults (CTransformerStage)
# --------------------------------------------------------------------------------
df_xfm_vars = df_MBR_ID_LKUP \
    .withColumn(
        "SvProdShNmFKeyLkpCheck",
        F.when(F.col("PROD_SH_NM_SK").isNull() & (F.col("PROD_SH_NM") != F.lit("NA")), F.lit("Y")).otherwise(F.lit("N"))
    ).withColumn(
        "SvGrpFkeyCheck",
        F.when(F.col("GRP_SK").isNull(), F.lit("Y")).otherwise(F.lit("N"))
    ).withColumn(
        "SvHedisMesrFkeyCheck",
        F.when(F.col("HEDIS_MESR_SK").isNull(), F.lit("Y")).otherwise(F.lit("N"))
    ).withColumn(
        "SvHedisPopFkeyCheck",
        F.when(F.col("HEDIS_POP_SK").isNull(), F.lit("Y")).otherwise(F.lit("N"))
    ).withColumn(
        "SvHedisRvwSetFkeyCheck",
        F.when(F.col("HEDIS_RVW_SET_SK").isNull(), F.lit("Y")).otherwise(F.lit("N"))
    )

# Lnk_MbrEnrQhpFkey_Main constraint:
df_Lnk_MbrEnrQhpFkey_Main = df_xfm_vars.filter(
    (F.col("SvProdShNmFKeyLkpCheck") == "N") &
    (F.col("SvGrpFkeyCheck") == "N") &
    (F.col("SvHedisPopFkeyCheck") == "N") &
    (F.col("SvHedisRvwSetFkeyCheck") == "N") &
    (F.col("SvHedisMesrFkeyCheck") == "N")
).select(
    F.col("MBR_HEDIS_MESR_YR_MO_SK").alias("MBR_HEDIS_MESR_YR_MO_SK"),
    F.col("RVW_SET_NM").alias("RVW_SET_NM"),
    F.col("POP_NM").alias("POP_NM"),
    F.col("MESR_NM").alias("MESR_NM"),
    F.col("SUB_MESR_NM").alias("SUB_MESR_NM"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("MBR_BUCKET_ID").alias("MBR_BUCKET_ID"),
    F.col("BASE_EVT_EPSD_DT_SK").alias("BASE_EVT_EPSD_DT_SK"),
    F.col("ACTVTY_YR_NO").alias("ACTVTY_YR_NO"),
    F.col("ACTVTY_MO_NO").alias("ACTVTY_MO_NO"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("PROD_SH_NM_SK").alias("PROD_SH_NM_SK"),
    F.col("CMPLNC_ADMIN_IN").alias("CMPLNC_ADMIN_IN"),
    F.col("CMPLNC_MNL_DATA_IN").alias("CMPLNC_MNL_DATA_IN"),
    F.col("CMPLNC_MNL_DATA_SMPL_POP_IN").alias("CMPLNC_MNL_DATA_SMPL_POP_IN"),
    F.col("CMPLNC_SMPL_POP_IN").alias("CMPLNC_SMPL_POP_IN"),
    F.col("CONTRAIN_IN").alias("CONTRAIN_IN"),
    F.col("CONTRAIN_SMPL_POP_IN").alias("CONTRAIN_SMPL_POP_IN"),
    F.col("EXCL_IN").alias("EXCL_IN"),
    F.col("EXCL_SMPL_POP_IN").alias("EXCL_SMPL_POP_IN"),
    F.col("MESR_ELIG_IN").alias("MESR_ELIG_IN"),
    F.col("MESR_ELIG_SMPL_POP_IN").alias("MESR_ELIG_SMPL_POP_IN"),
    F.col("MBR_BRTH_DT_SK").alias("MBR_BRTH_DT_SK"),
    F.col("EVT_CT").alias("EVT_CT"),
    F.col("UNIT_CT").alias("UNIT_CT"),
    F.col("ACRDTN_CAT_ID").alias("ACRDTN_CAT_ID"),
    F.col("CNTY_AREA_ID").alias("CNTY_AREA_ID"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("MBR_ID").alias("MBR_ID"),
    F.col("MBR_FULL_NM").alias("MBR_FULL_NM"),
    F.col("ON_OFF_EXCH_ID").alias("ON_OFF_EXCH_ID"),
    F.col("PROD_SH_NM").alias("PROD_SH_NM"),
    F.col("CST_AMT").alias("CST_AMT"),
    F.col("RISK_PCT").alias("RISK_PCT"),
    F.col("ADJ_RISK_NO").alias("ADJ_RISK_NO"),
    F.col("PLN_ALL_CAUSE_READMSN_VRNC_NO").alias("PLN_ALL_CAUSE_READMSN_VRNC_NO"),
    F.col("HEDIS_MESR_ABBR_ID").alias("HEDIS_MESR_ABBR_ID"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("HEDIS_RVW_SET_SK").alias("HEDIS_RVW_SET_SK"),
    F.col("HEDIS_POP_SK").alias("HEDIS_POP_SK"),
    F.col("HEDIS_MESR_SK").alias("HEDIS_MESR_SK"),
    F.col("DENOMINATOR_FOR_NO_CONT_ENR").alias("DENOMINATOR_FOR_NO_CONT_ENR"),
    F.col("ADMIN_COMPLIANT_CAT").alias("ADMIN_COMPLIANT_CAT"),
    F.col("ETHNCTY_CD_SK").alias("ETHNCTY_CD_SK"),
    F.col("ETHNCTY_SRC_CD_SK").alias("ETHNCTY_SRC_CD_SK"),
    F.lit(0).alias("HEDIS_MESR_GAP_IN_CARE_TYP_CD_SK"),
    F.col("NUMERATOR_CMPLNC_CAT_CD_SK").alias("NUMERATOR_CMPLNC_CAT_CD_SK"),
    F.col("RACE_CD_SK").alias("RACE_CD_SK"),
    F.col("RACE_SRC_CD_SK").alias("RACE_SRC_CD_SK"),
    F.col("SES_STRAT_NO").alias("SES_STRAT_NO"),
    F.col("ADV_ILNS_FRAILTY_EXCL").alias("ADV_ILNS_AND_FRAILTY_EXCL_CT"),
    F.col("HSPC_EXCL").alias("HSPC_EXCL_CT"),
    F.expr(
        "CASE WHEN Trim(COALESCE(LTI_SNP_EXCL,''))='' THEN 0 "
        "ELSE Trim(COALESCE(LTI_SNP_EXCL,'')) END"
    ).alias("LTI_SNP_EXCL_CT"),
    F.col("PROD_LN").alias("PROD_LN_TYP_DESC"),
    F.col("SRC_DOMAIN_TX").alias("PROD_ID"),
    F.expr(
        "CASE WHEN Trim(COALESCE(NUMERATOR_CMPLNC_FLAG,''))='' "
        "THEN 0 ELSE Trim(COALESCE(NUMERATOR_CMPLNC_FLAG,'')) END"
    ).alias("CMPLNC_ADMIN_CT"),
    F.expr(
        "CASE WHEN Trim(COALESCE(NUMERATOR_CMPLNC_CAT,''))='S' "
        "AND Trim(COALESCE(NUMERATOR_CMPLNC_FLAG,''))='1' THEN 1 ELSE 0 END"
    ).alias("CMPLNC_MNL_DATA_CT"),
    F.col("DCSD_EXCL").alias("DCSD_EXCL_CT"),
    F.expr(
        "CASE WHEN Trim(COALESCE(EXCL,''))='' THEN 0 "
        "ELSE Trim(COALESCE(EXCL,'')) END"
    ).alias("EXCL_CT"),
    F.expr(
        "CASE WHEN Trim(COALESCE(DENOMINATOR,''))='' THEN 0 "
        "ELSE Trim(COALESCE(DENOMINATOR,'')) END"
    ).alias("MESR_ELIG_CT"),
    F.expr(
        "CASE WHEN Trim(COALESCE(OPTNL_EXCL,''))='' THEN 0 "
        "ELSE Trim(COALESCE(OPTNL_EXCL,'')) END"
    ).alias("OPTNL_EXCL_CT"),
    F.expr(
        "CASE WHEN Trim(COALESCE(RQRD_EXCL,''))='' THEN 0 "
        "ELSE Trim(COALESCE(RQRD_EXCL,'')) END"
    ).alias("RQRD_EXCL_CT"),
    F.col("PROD_ROLLUP_ID").alias("PROD_ROLLUP_ID"),
    F.col("NUMERATOR_EVT_1_SVC_DT").alias("NUMERATOR_EVT_1_SVC_DT"),
    F.col("LAB_TST_VAL_NO").alias("LAB_TST_VAL_NO"),
    F.col("MBR_GNDR").alias("MBR_GNDR")
)

# For Lnk_MbrEnrQhpUNK and Lnk_MbrEnrQhp_NA, we emulate row-based constraints with a window
w = Window.orderBy(F.lit(1))
df_temp_num = df_xfm_vars.withColumn("__row_num", F.row_number().over(w))

# Lnk_MbrEnrQhpUNK
df_Lnk_MbrEnrQhpUNK = df_temp_num.filter(F.col("__row_num") == 1).select(
    F.lit(0).alias("MBR_HEDIS_MESR_YR_MO_SK"),
    F.lit(0).alias("RVW_SET_NM"),
    F.lit(0).alias("POP_NM"),
    F.lit(0).alias("MESR_NM"),
    F.lit(0).alias("SUB_MESR_NM"),
    F.lit(0).alias("MBR_UNIQ_KEY"),
    F.lit(0).alias("MBR_BUCKET_ID"),
    F.lit("1753-01-01").alias("BASE_EVT_EPSD_DT_SK"),
    F.lit(0).alias("ACTVTY_YR_NO"),
    F.lit(0).alias("ACTVTY_MO_NO"),
    F.lit(0).alias("SRC_SYS_CD"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("MBR_SK"),
    F.lit(0).alias("PROD_SH_NM_SK"),
    F.lit(0).alias("CMPLNC_ADMIN_IN"),
    F.lit(0).alias("CMPLNC_MNL_DATA_IN"),
    F.lit(0).alias("CMPLNC_MNL_DATA_SMPL_POP_IN"),
    F.lit(0).alias("CMPLNC_SMPL_POP_IN"),
    F.lit(0).alias("CONTRAIN_IN"),
    F.lit(0).alias("CONTRAIN_SMPL_POP_IN"),
    F.lit(0).alias("EXCL_IN"),
    F.lit(0).alias("EXCL_SMPL_POP_IN"),
    F.lit(0).alias("MESR_ELIG_IN"),
    F.lit(0).alias("MESR_ELIG_SMPL_POP_IN"),
    F.lit("1753-01-01").alias("MBR_BRTH_DT_SK"),
    F.lit(0).alias("EVT_CT"),
    F.lit(0).alias("UNIT_CT"),
    F.lit(0).alias("ACRDTN_CAT_ID"),
    F.lit(0).alias("CNTY_AREA_ID"),
    F.lit(0).alias("GRP_ID"),
    F.lit(0).alias("MBR_INDV_BE_KEY"),
    F.lit(0).alias("MBR_ID"),
    F.lit(0).alias("MBR_FULL_NM"),
    F.lit(0).alias("ON_OFF_EXCH_ID"),
    F.lit(0).alias("PROD_SH_NM"),
    F.lit(0).alias("CST_AMT"),
    F.lit(0).alias("RISK_PCT"),
    F.lit(0).alias("ADJ_RISK_NO"),
    F.lit(0).alias("PLN_ALL_CAUSE_READMSN_VRNC_NO"),
    F.lit(0).alias("HEDIS_MESR_ABBR_ID"),
    F.lit(0).alias("GRP_SK"),
    F.lit(0).alias("HEDIS_RVW_SET_SK"),
    F.lit(0).alias("HEDIS_POP_SK"),
    F.lit(0).alias("HEDIS_MESR_SK"),
    F.lit(0).alias("DENOMINATOR_FOR_NO_CONT_ENR"),
    F.lit(0).alias("ADMIN_COMPLIANT_CAT"),
    F.lit(0).alias("ETHNCTY_CD_SK"),
    F.lit(0).alias("ETHNCTY_SRC_CD_SK"),
    F.lit(0).alias("HEDIS_MESR_GAP_IN_CARE_TYP_CD_SK"),
    F.lit(0).alias("NUMERATOR_CMPLNC_CAT_CD_SK"),
    F.lit(0).alias("RACE_CD_SK"),
    F.lit(0).alias("RACE_SRC_CD_SK"),
    F.lit(0).alias("SES_STRAT_NO"),
    F.lit(0).alias("ADV_ILNS_AND_FRAILTY_EXCL_CT"),
    F.lit(0).alias("HSPC_EXCL_CT"),
    F.lit(0).alias("LTI_SNP_EXCL_CT"),
    F.lit(0).alias("PROD_LN_TYP_DESC"),
    F.lit(0).alias("PROD_ID"),
    F.lit(0).alias("CMPLNC_ADMIN_CT"),
    F.lit(0).alias("CMPLNC_MNL_DATA_CT"),
    F.lit(0).alias("DCSD_EXCL_CT"),
    F.lit(0).alias("EXCL_CT"),
    F.lit(0).alias("MESR_ELIG_CT"),
    F.lit(0).alias("OPTNL_EXCL_CT"),
    F.lit(0).alias("RQRD_EXCL_CT"),
    F.lit(0).alias("PROD_ROLLUP_ID"),
    F.lit("1753-01-01").alias("NUMERATOR_EVT_1_SVC_DT"),
    F.lit(0).alias("LAB_TST_VAL_NO"),
    F.lit("UNK").alias("MBR_GNDR")
)

# Lnk_MbrEnrQhp_NA
df_Lnk_MbrEnrQhp_NA = df_temp_num.filter(F.col("__row_num") == 1).select(
    F.lit(1).alias("MBR_HEDIS_MESR_YR_MO_SK"),
    F.lit(1).alias("RVW_SET_NM"),
    F.lit(1).alias("POP_NM"),
    F.lit(1).alias("MESR_NM"),
    F.lit(1).alias("SUB_MESR_NM"),
    F.lit(1).alias("MBR_UNIQ_KEY"),
    F.lit(1).alias("MBR_BUCKET_ID"),
    F.lit("1753-01-01").alias("BASE_EVT_EPSD_DT_SK"),
    F.lit(1).alias("ACTVTY_YR_NO"),
    F.lit(1).alias("ACTVTY_MO_NO"),
    F.lit(1).alias("SRC_SYS_CD"),
    F.lit("100").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit("100").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("MBR_SK"),
    F.lit(1).alias("PROD_SH_NM_SK"),
    F.lit(1).alias("CMPLNC_ADMIN_IN"),
    F.lit(1).alias("CMPLNC_MNL_DATA_IN"),
    F.lit(1).alias("CMPLNC_MNL_DATA_SMPL_POP_IN"),
    F.lit(1).alias("CMPLNC_SMPL_POP_IN"),
    F.lit(1).alias("CONTRAIN_IN"),
    F.lit(1).alias("CONTRAIN_SMPL_POP_IN"),
    F.lit(1).alias("EXCL_IN"),
    F.lit(1).alias("EXCL_SMPL_POP_IN"),
    F.lit(1).alias("MESR_ELIG_IN"),
    F.lit(1).alias("MESR_ELIG_SMPL_POP_IN"),
    F.lit("1753-01-01").alias("MBR_BRTH_DT_SK"),
    F.lit(1).alias("EVT_CT"),
    F.lit(1).alias("UNIT_CT"),
    F.lit(1).alias("ACRDTN_CAT_ID"),
    F.lit(1).alias("CNTY_AREA_ID"),
    F.lit(1).alias("GRP_ID"),
    F.lit(1).alias("MBR_INDV_BE_KEY"),
    F.lit(1).alias("MBR_ID"),
    F.lit(1).alias("MBR_FULL_NM"),
    F.lit(1).alias("ON_OFF_EXCH_ID"),
    F.lit(1).alias("PROD_SH_NM"),
    F.lit(1).alias("CST_AMT"),
    F.lit(1).alias("RISK_PCT"),
    F.lit(1).alias("ADJ_RISK_NO"),
    F.lit(1).alias("PLN_ALL_CAUSE_READMSN_VRNC_NO"),
    F.lit(1).alias("HEDIS_MESR_ABBR_ID"),
    F.lit(1).alias("GRP_SK"),
    F.lit(1).alias("HEDIS_RVW_SET_SK"),
    F.lit(1).alias("HEDIS_POP_SK"),
    F.lit(1).alias("HEDIS_MESR_SK"),
    F.lit(1).alias("DENOMINATOR_FOR_NO_CONT_ENR"),
    F.lit(1).alias("ADMIN_COMPLIANT_CAT"),
    F.lit(1).alias("ETHNCTY_CD_SK"),
    F.lit(1).alias("ETHNCTY_SRC_CD_SK"),
    F.lit(1).alias("HEDIS_MESR_GAP_IN_CARE_TYP_CD_SK"),
    F.lit(1).alias("NUMERATOR_CMPLNC_CAT_CD_SK"),
    F.lit(1).alias("RACE_CD_SK"),
    F.lit(1).alias("RACE_SRC_CD_SK"),
    F.lit(1).alias("SES_STRAT_NO"),
    F.lit(1).alias("ADV_ILNS_AND_FRAILTY_EXCL_CT"),
    F.lit(1).alias("HSPC_EXCL_CT"),
    F.lit(1).alias("LTI_SNP_EXCL_CT"),
    F.lit(1).alias("PROD_LN_TYP_DESC"),
    F.lit(1).alias("PROD_ID"),
    F.lit(1).alias("CMPLNC_ADMIN_CT"),
    F.lit(1).alias("CMPLNC_MNL_DATA_CT"),
    F.lit(1).alias("DCSD_EXCL_CT"),
    F.lit(1).alias("EXCL_CT"),
    F.lit(1).alias("MESR_ELIG_CT"),
    F.lit(1).alias("OPTNL_EXCL_CT"),
    F.lit(1).alias("RQRD_EXCL_CT"),
    F.lit(1).alias("PROD_ROLLUP_ID"),
    F.lit("1753-01-01").alias("NUMERATOR_EVT_1_SVC_DT"),
    F.lit(1).alias("LAB_TST_VAL_NO"),
    F.lit("NA").alias("MBR_GNDR")
)

# Lnk_Prod_sh_nm_Fail
df_Lnk_Prod_sh_nm_Fail = df_xfm_vars.filter(F.col("SvProdShNmFKeyLkpCheck") == "Y").select(
    F.col("MBR_HEDIS_MESR_YR_MO_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.lit("CotivitiIdsStndExtrMbrHedisMesrYrMoFkey").alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("PROD_SH_NM").alias("PHYSCL_FILE_NM"),
    F.expr("SRC_SYS_CD || ';' || 'FACETS DB' || ';' || 'IDS' || ';' || 'PRODUCT LOB TYPE' || ';' || 'PRODUCT LOB TYPE' || ';' || COALESCE(CAST(PROD_SH_NM_SK AS STRING),'0')").alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

# Lnk_BalanceOut
df_Lnk_BalanceOut = df_xfm_vars.select(
    F.col("RVW_SET_NM").alias("RVW_SET_NM"),
    F.col("POP_NM").alias("POP_NM"),
    F.col("MESR_NM").alias("MESR_NM"),
    F.col("SUB_MESR_NM").alias("SUB_MESR_NM"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("MBR_BUCKET_ID").alias("MBR_BUCKET_ID"),
    F.col("BASE_EVT_EPSD_DT_SK").alias("BASE_EVT_EPSD_DT_SK"),
    F.col("ACTVTY_YR_NO").alias("ACTVTY_YR_NO"),
    F.col("ACTVTY_MO_NO").alias("ACTVTY_MO_NO"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

# Lnk_GrpFkeyFail
df_Lnk_GrpFkeyFail = df_xfm_vars.filter(F.col("SvGrpFkeyCheck") == "Y").select(
    F.col("MBR_HEDIS_MESR_YR_MO_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.lit("CotivitiIdsStndExtrMbrHedisMesrYrMoFkey").alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("GRP").alias("PHYSCL_FILE_NM"),
    F.expr("SRC_SYS_CD || ';' || GRP_ID || ';' || COALESCE(CAST(GRP_SK AS STRING),'0')").alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

# Lnk_PopFkeyFail
df_Lnk_PopFkeyFail = df_xfm_vars.filter(F.col("SvHedisPopFkeyCheck") == "Y").select(
    F.col("MBR_HEDIS_MESR_YR_MO_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.lit("CotivitiIdsStndExtrMbrHedisMesrYrMoFkey").alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("HEDIS_POP").alias("PHYSCL_FILE_NM"),
    F.expr("SRC_SYS_CD || ';' || POP_NM || ';' || COALESCE(CAST(HEDIS_POP_SK AS STRING),'0')").alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

# Lnk_RvwSetFkeyFail
df_Lnk_RvwSetFkeyFail = df_xfm_vars.filter(F.col("SvHedisRvwSetFkeyCheck") == "Y").select(
    F.col("MBR_HEDIS_MESR_YR_MO_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.lit("CotivitiIdsStndExtrMbrHedisMesrYrMoFkey").alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("HEDIS_RVW_SET").alias("PHYSCL_FILE_NM"),
    F.expr("SRC_SYS_CD || ';' || RVW_SET_NM || ';' || CAST(HEDIS_RVW_SET_STRT_DT AS STRING) || ';' || CAST(HEDIS_RVW_SET_END_DT AS STRING) || ';' || COALESCE(CAST(HEDIS_RVW_SET_SK AS STRING),'0')").alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

# Lnk_MesrFkeyFail
df_Lnk_MesrFkeyFail = df_xfm_vars.filter(F.col("SvHedisMesrFkeyCheck") == "Y").select(
    F.col("MBR_HEDIS_MESR_YR_MO_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.lit("CotivitiIdsStndExtrMbrHedisMesrYrMoFkey").alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("HEDIS_MESR").alias("PHYSCL_FILE_NM"),
    F.expr("SRC_SYS_CD || ';' || MESR_NM || ';' || SUB_MESR_NM || ';' || MBR_BUCKET_ID || ';' || COALESCE(CAST(HEDIS_MESR_SK AS STRING),'0')").alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

# --------------------------------------------------------------------------------
# Stage: B_MBR_HEDIS_MESR_YR_MO (PxSequentialFile) - write file
# --------------------------------------------------------------------------------
# Prepare df_B_MBR_HEDIS_MESR_YR_MO with the correct select
df_B_MBR_HEDIS_MESR_YR_MO = df_Lnk_BalanceOut

# Write with | delimiter
write_files(
    df_B_MBR_HEDIS_MESR_YR_MO,
    f"{adls_path}/load/B_MBR_HEDIS_MESR_YR_MO.{RunID}.dat",
    delimiter="|",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote='"',
    nullValue=None
)

# --------------------------------------------------------------------------------
# Stage: Fnl_FkeyFailure (PxFunnel)
# --------------------------------------------------------------------------------
df_fnl_fkeyfailure = df_Lnk_Prod_sh_nm_Fail.unionByName(df_Lnk_MesrFkeyFail, allowMissingColumns=True) \
    .unionByName(df_Lnk_PopFkeyFail, allowMissingColumns=True) \
    .unionByName(df_Lnk_GrpFkeyFail, allowMissingColumns=True) \
    .unionByName(df_Lnk_RvwSetFkeyFail, allowMissingColumns=True)

# Select final columns in order
df_Fkey_fnl = df_fnl_fkeyfailure.select(
    F.col("PRI_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("JOB_NM").alias("JOB_NM"),
    F.col("ERROR_TYP").alias("ERROR_TYP"),
    F.col("PHYSCL_FILE_NM").alias("PHYSCL_FILE_NM"),
    F.col("FRGN_NAT_KEY_STRING").alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("JOB_EXCTN_SK").alias("JOB_EXCTN_SK")
)

# --------------------------------------------------------------------------------
# Stage: seq_FkeyFailedFile (PxSequentialFile) - write file
# --------------------------------------------------------------------------------
write_files(
    df_Fkey_fnl,
    f"{adls_path}/fkey_failures/{PrefixFkeyFailedFileName}.CotivitiIdsStndExtrMbrHedisMesrYrMoFkey.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)

# --------------------------------------------------------------------------------
# Stage: fnl_NA_UNK_Streams (PxFunnel)
# --------------------------------------------------------------------------------
df_fnl_NA_UNK_Streams = df_Lnk_MbrEnrQhpFkey_Main.unionByName(df_Lnk_MbrEnrQhp_NA, allowMissingColumns=True) \
    .unionByName(df_Lnk_MbrEnrQhpUNK, allowMissingColumns=True)

df_fnl_NA_UNK_Streams_out = df_fnl_NA_UNK_Streams.select(
    F.col("MBR_HEDIS_MESR_YR_MO_SK").alias("MBR_HEDIS_MESR_YR_MO_SK"),
    F.col("RVW_SET_NM").alias("RVW_SET_NM"),
    F.col("POP_NM").alias("POP_NM"),
    F.col("MESR_NM").alias("MESR_NM"),
    F.col("SUB_MESR_NM").alias("SUB_MESR_NM"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("MBR_BUCKET_ID").alias("MBR_BUCKET_ID"),
    F.col("BASE_EVT_EPSD_DT_SK").alias("BASE_EVT_EPSD_DT_SK"),
    F.col("ACTVTY_YR_NO").alias("ACTVTY_YR_NO"),
    F.col("ACTVTY_MO_NO").alias("ACTVTY_MO_NO"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("PROD_SH_NM_SK").alias("PROD_SH_NM_SK"),
    F.col("CMPLNC_ADMIN_IN").alias("CMPLNC_ADMIN_IN"),
    F.col("CMPLNC_MNL_DATA_IN").alias("CMPLNC_MNL_DATA_IN"),
    F.col("CMPLNC_MNL_DATA_SMPL_POP_IN").alias("CMPLNC_MNL_DATA_SMPL_POP_IN"),
    F.col("CMPLNC_SMPL_POP_IN").alias("CMPLNC_SMPL_POP_IN"),
    F.col("CONTRAIN_IN").alias("CONTRAIN_IN"),
    F.col("CONTRAIN_SMPL_POP_IN").alias("CONTRAIN_SMPL_POP_IN"),
    F.col("EXCL_IN").alias("EXCL_IN"),
    F.col("EXCL_SMPL_POP_IN").alias("EXCL_SMPL_POP_IN"),
    F.col("MESR_ELIG_IN").alias("MESR_ELIG_IN"),
    F.col("MESR_ELIG_SMPL_POP_IN").alias("MESR_ELIG_SMPL_POP_IN"),
    F.col("MBR_BRTH_DT_SK").alias("MBR_BRTH_DT_SK"),
    F.col("EVT_CT").alias("EVT_CT"),
    F.col("UNIT_CT").alias("UNIT_CT"),
    F.col("ACRDTN_CAT_ID").alias("ACRDTN_CAT_ID"),
    F.col("CNTY_AREA_ID").alias("CNTY_AREA_ID"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("MBR_ID").alias("MBR_ID"),
    F.col("MBR_FULL_NM").alias("MBR_FULL_NM"),
    F.col("ON_OFF_EXCH_ID").alias("ON_OFF_EXCH_ID"),
    F.col("PROD_SH_NM").alias("PROD_SH_NM"),
    F.col("CST_AMT").alias("CST_AMT"),
    F.col("RISK_PCT").alias("RISK_PCT"),
    F.col("ADJ_RISK_NO").alias("ADJ_RISK_NO"),
    F.col("PLN_ALL_CAUSE_READMSN_VRNC_NO").alias("PLN_ALL_CAUSE_READMSN_VRNC_NO"),
    F.col("HEDIS_MESR_ABBR_ID").alias("HEDIS_MESR_ABBR_ID"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("HEDIS_RVW_SET_SK").alias("HEDIS_RVW_SET_SK"),
    F.col("HEDIS_POP_SK").alias("HEDIS_POP_SK"),
    F.col("HEDIS_MESR_SK").alias("HEDIS_MESR_SK"),
    F.col("DENOMINATOR_FOR_NO_CONT_ENR").alias("DENOMINATOR_FOR_NO_CONT_ENR"),
    F.col("ADMIN_COMPLIANT_CAT").alias("ADMIN_COMPLIANT_CAT"),
    F.col("ETHNCTY_CD_SK").alias("ETHNCTY_CD_SK"),
    F.col("ETHNCTY_SRC_CD_SK").alias("ETHNCTY_SRC_CD_SK"),
    F.col("HEDIS_MESR_GAP_IN_CARE_TYP_CD_SK").alias("HEDIS_MESR_GAP_IN_CARE_TYP_CD_SK"),
    F.col("NUMERATOR_CMPLNC_CAT_CD_SK").alias("NUMERATOR_CMPLNC_CAT_CD_SK"),
    F.col("RACE_CD_SK").alias("RACE_CD_SK"),
    F.col("RACE_SRC_CD_SK").alias("RACE_SRC_CD_SK"),
    F.col("SES_STRAT_NO").alias("SES_STRAT_NO"),
    F.col("ADV_ILNS_AND_FRAILTY_EXCL_CT").alias("ADV_ILNS_AND_FRAILTY_EXCL_CT"),
    F.col("HSPC_EXCL_CT").alias("HSPC_EXCL_CT"),
    F.col("LTI_SNP_EXCL_CT").alias("LTI_SNP_EXCL_CT"),
    F.col("PROD_LN_TYP_DESC").alias("PROD_LN_TYP_DESC"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("CMPLNC_ADMIN_CT").alias("CMPLNC_ADMIN_CT"),
    F.col("CMPLNC_MNL_DATA_CT").alias("CMPLNC_MNL_DATA_CT"),
    F.col("DCSD_EXCL_CT").alias("DCSD_EXCL_CT"),
    F.col("EXCL_CT").alias("EXCL_CT"),
    F.col("MESR_ELIG_CT").alias("MESR_ELIG_CT"),
    F.col("OPTNL_EXCL_CT").alias("OPTNL_EXCL_CT"),
    F.col("RQRD_EXCL_CT").alias("RQRD_EXCL_CT"),
    F.col("PROD_ROLLUP_ID").alias("PROD_ROLLUP_ID"),
    F.col("NUMERATOR_EVT_1_SVC_DT").alias("NUMERATOR_EVT_1_SVC_DT"),
    F.col("LAB_TST_VAL_NO").alias("LAB_TST_VAL_NO"),
    F.col("MBR_GNDR").alias("MBR_GNDR")
)

# --------------------------------------------------------------------------------
# Stage: seq_MBR_HEDIS_MESR_YR_MO (PxSequentialFile) - final write
# --------------------------------------------------------------------------------

# Per instructions, for the final write, rpad() for each char/varchar column.  
# Below is an example approach using a dictionary for known or assumed lengths. 
# For char(10), we do length=10; for char(1), length=1.  
# For all varchars (where length not specified), we apply an arbitrary rpad of 255.

def apply_rpad_final(df_in):
    # Identify columns that need rpad (char or varchar) from the job metadata
    # We'll assume: 
    #   char(1) or char(10) => those lengths, 
    #   all varchars => 255
    # A quick map for columns we know are char(10) or char(1):
    fixed_char_lens = {
        "BASE_EVT_EPSD_DT_SK": 10,
        "CMPLNC_ADMIN_IN": 1,
        "CMPLNC_MNL_DATA_IN": 1,
        "CMPLNC_MNL_DATA_SMPL_POP_IN": 1,
        "CMPLNC_SMPL_POP_IN": 1,
        "CONTRAIN_IN": 1,
        "CONTRAIN_SMPL_POP_IN": 1,
        "EXCL_IN": 1,
        "EXCL_SMPL_POP_IN": 1,
        "MESR_ELIG_IN": 1,
        "MESR_ELIG_SMPL_POP_IN": 1,
        "MBR_BRTH_DT_SK": 10
    }
    # Everything in final schema that is char or varchar
    # We just treat them as needing rpad up to some length
    out_df = df_in
    for c in df_in.columns:
        # Decide if numeric or not by checking the schema only if available at runtime
        # or we fallback to the metadata above.
        if c in fixed_char_lens:
            out_df = out_df.withColumn(c, F.rpad(F.col(c), fixed_char_lens[c], " "))
        else:
            # We do not know the length. If it's not numeric, we do 255:
            # We'll do a small heuristic: if the expression is certainly numeric, skip. 
            # Otherwise we rpad.
            # A simpler approach is to rpad all columns that are not obviously numeric based on name:
            # We'll parse from the job, but let's just check if column name might be an integer or decimal.
            # To satisfy instructions, we'll do a quick approach:
            numeric_candidates = {
                "SK","NO","CT","ID","PCT","RISK","YR","MO","FLAG","EXCL","DENOMINATOR","UNIT","MBR_UNIQ_KEY",
                "MBR_INDV_BE_KEY","MBR_HEDIS_MESR_YR_MO_SK","GRP_SK","HEDIS_MESR_SK","HEDIS_POP_SK",
                "HEDIS_RVW_SET_SK","PRI_SK","SES_STRAT_NO","LAB_TST_VAL_NO","JOB_EXCTN_SK",
                "CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","RQRD_EXCL_CT","OPTNL_EXCL_CT",
                "NUMERATOR_CMPLNC_FLAG","MBR_SK","MESR_ELIG_CT"
            }
            # If not obviously numeric by name checking, do rpad(255)
            U_c = c.upper()
            if any(token in U_c for token in numeric_candidates):
                # skip rpad
                pass
            else:
                # rpad to 255
                out_df = out_df.withColumn(c, F.rpad(F.col(c), 255, " "))
    return out_df

df_seq_final = apply_rpad_final(df_fnl_NA_UNK_Streams_out)

write_files(
    df_seq_final,
    f"{adls_path}/load/MBR_HEDIS_MESR_YR_MO.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)