# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2005, 2006, 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Job Name :  BcaFepIdsProvAddrXfrm
# MAGIC 
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC 
# MAGIC     Transformation rules applied on Extracted data information that is used to populate the IDS ProvAddr table 
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Sudhir Bomshetty            2017-08-21         #5781                           Initial Programming                                             IntegrateDev2                Kalyan Neelam             2017-10-05
# MAGIC 
# MAGIC Sudhir Bomshetty            2018-03-26         #5781                    Changed logic for PROV_ID, PROV_ADDR_ID       IntegrateDev2                Jaideep Mankala         04/02/2018
# MAGIC 
# MAGIC Deepika C                      2021-11-25         US 468740              Added new field ATND_TEXT as Null in                  IntegrateSITF                Jeyaprasanna              2021-11-28
# MAGIC                                                                                                   Xfrm_BusinessRules Transformer Stage

# MAGIC JobName: BcbsaFepIdsProvAddrXfrm
# MAGIC JobName: FepIdsProvGrpRelshpXfrm
# MAGIC 
# MAGIC Transformation rules are applied on the data Extracted
# MAGIC Transformed Data will land into a Dataset for Primary Keying job.
# MAGIC 
# MAGIC Data is partitioned on Natural Key Columns.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


CurrDate = get_widget_value('CurrDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
IDSOwner = get_widget_value('IDSOwner','')
RunID = get_widget_value('RunID','')
RunIDTimeStamp = get_widget_value('RunIDTimeStamp','')
SrcSysCdSK = get_widget_value('SrcSysCdSK','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_db2_Prov_Addr_Lkp = """SELECT  PROV.NTNL_PROV_ID,PROV_ADDR.ADDR_LN_1, PROV_ADDR.ADDR_LN_2, PROV_ADDR.ADDR_LN_3, PROV_ADDR.CITY_NM, MAX(PROV_ADDR.PROV_ADDR_SK) AS PROV_ADDR_SK, PROV_ADDR.POSTAL_CD, CD_MPPNG.SRC_CD
FROM #$IDSOwner#.PROV_ADDR PROV_ADDR, #$IDSOwner#.PROV PROV, #$IDSOwner#.CD_MPPNG CD_MPPNG
where PROV.PROV_ADDR_ID = PROV_ADDR.PROV_ADDR_ID
AND  PROV_ADDR.PROV_ADDR_ST_CD_SK=CD_MPPNG.CD_MPPNG_SK
GROUP BY PROV.NTNL_PROV_ID,PROV_ADDR.ADDR_LN_1, PROV_ADDR.ADDR_LN_2, PROV_ADDR.ADDR_LN_3, PROV_ADDR.CITY_NM,  PROV_ADDR.POSTAL_CD, CD_MPPNG.SRC_CD"""
df_db2_Prov_Addr_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_Prov_Addr_Lkp)
    .load()
)
df_db2_Prov_Addr_Lkp = df_db2_Prov_Addr_Lkp.select(
    "NTNL_PROV_ID", "ADDR_LN_1", "ADDR_LN_2", "ADDR_LN_3", "CITY_NM", "PROV_ADDR_SK", "POSTAL_CD", "SRC_CD"
)

df_ds_PROV_ADDR_Extr = spark.read.parquet(f"{adls_path}/ds/FEP_PROV.{SrcSysCd}.extr.{RunID}.parquet")
df_ds_PROV_ADDR_Extr = df_ds_PROV_ADDR_Extr.select(
    "PROV_ID",
    "OFC_CD",
    "LOC",
    "PROV_LAST_NM",
    "PROV_FIRST_NM",
    "PROV_MIDINIT",
    "PROV_NM_SFX",
    "PROV_ADDR_LN_1",
    "PROV_ADDR_LN_2",
    "PROV_CITY_NM",
    "PROV_CNTY_CD",
    "PROV_ST_CD",
    "PROV_ZIP",
    "PROV_TEL_NO",
    "PROV_EMAIL_ADDR",
    "PROV_FAX_NO",
    "PROV_NTNL_PROV_ID",
    "CSTM_PROV_TYP",
    "PCP_FLAG",
    "PRSCRB_PROV_FLAG",
    "PROV_ORIG_PLN_CD",
    "PROV_TAX_ID",
    "ALT_FLD_1",
    "ALT_FLD_2",
    "ALT_FLD_3",
    "CSTM_FLD_1",
    "CSTM_FLD_2",
    "OFC_MGR_LAST_NM",
    "OFC_MGR_FIRST_NM",
    "OFC_MGR_TTL",
    "OFC_MGR_EMAIL",
    "RUN_DT",
    "SRC_SYS"
)

isFirstCharNumeric = F.col("PROV_ID").substr(F.lit(1), F.lit(1)).rlike("^[0-9]$")
df_Xfrm_BusinessLogic_vars = (
    df_ds_PROV_ADDR_Extr
    .withColumn(
        "svProvId",
        F.when(
            isFirstCharNumeric,
            F.col("PROV_ID")
        ).otherwise(
            trim(F.col("PROV_ID").substr(F.lit(5), (F.length("PROV_ID") - 4)))
        )
    )
    .withColumn(
        "svProvZip",
        F.substring(trim(F.col("PROV_ZIP")), 1, 5)
    )
    .withColumn(
        "svProvAddrLn1",
        F.when(
            trim(F.col("PROV_ADDR_LN_1")) == "?",
            F.lit(None)
        ).otherwise(
            F.substring(F.upper(trim(F.col("PROV_ADDR_LN_1"))), 1, 40)
        )
    )
    .withColumn(
        "svProvAddrLn2",
        F.when(
            trim(F.col("PROV_ADDR_LN_2")) == "?",
            F.lit(None)
        ).otherwise(
            F.substring(F.upper(trim(F.col("PROV_ADDR_LN_2"))), 1, 40)
        )
    )
    .withColumn(
        "svProvCityNm",
        F.when(
            trim(F.col("PROV_CITY_NM")) == "?",
            F.lit(None)
        ).otherwise(
            F.upper(trim(F.col("PROV_CITY_NM")))
        )
    )
)

df_lnk_Xfrm_out = df_Xfrm_BusinessLogic_vars.select(
    F.col("svProvId").alias("PROV_ID"),
    F.col("PROV_ID").alias("PROV_ADDR_ID"),
    F.col("LOC").alias("LOC"),
    F.col("PROV_LAST_NM").alias("PROV_LAST_NM"),
    F.col("PROV_FIRST_NM").alias("PROV_FIRST_NM"),
    F.col("PROV_MIDINIT").alias("PROV_MIDINIT"),
    F.col("PROV_NM_SFX").alias("PROV_NM_SFX"),
    F.col("svProvAddrLn1").alias("PROV_ADDR_LN_1"),
    F.col("svProvAddrLn2").alias("PROV_ADDR_LN_2"),
    F.col("svProvCityNm").alias("PROV_CITY_NM"),
    F.col("PROV_CNTY_CD").alias("PROV_CNTY_CD"),
    trim(F.upper(F.col("PROV_ST_CD"))).alias("PROV_ST_CD"),
    F.col("svProvZip").alias("PROV_ZIP"),
    F.col("PROV_TEL_NO").alias("PROV_TEL_NO"),
    F.col("PROV_EMAIL_ADDR").alias("PROV_EMAIL_ADDR"),
    F.col("PROV_FAX_NO").alias("PROV_FAX_NO"),
    F.col("PROV_NTNL_PROV_ID").alias("PROV_NTNL_PROV_ID"),
    F.col("CSTM_PROV_TYP").alias("CSTM_PROV_TYP"),
    F.col("PCP_FLAG").alias("PCP_FLAG"),
    F.col("PRSCRB_PROV_FLAG").alias("PRSCRB_PROV_FLAG"),
    F.col("PROV_ORIG_PLN_CD").alias("PROV_ORIG_PLN_CD"),
    F.col("PROV_TAX_ID").alias("PROV_TAX_ID"),
    F.col("ALT_FLD_1").alias("ALT_FLD_1"),
    F.col("ALT_FLD_2").alias("ALT_FLD_2"),
    F.col("ALT_FLD_3").alias("ALT_FLD_3"),
    F.col("CSTM_FLD_1").alias("CSTM_FLD_1"),
    F.col("CSTM_FLD_2").alias("CSTM_FLD_2"),
    F.col("OFC_MGR_LAST_NM").alias("OFC_MGR_LAST_NM"),
    F.col("OFC_MGR_FIRST_NM").alias("OFC_MGR_FIRST_NM"),
    F.col("OFC_MGR_TTL").alias("OFC_MGR_TTL"),
    F.col("OFC_MGR_EMAIL").alias("OFC_MGR_EMAIL"),
    F.col("RUN_DT").alias("RUN_DT"),
    F.col("SRC_SYS").alias("SRC_SYS")
)

df_lnk_Xfrm_Addr_lkp_in = df_Xfrm_BusinessLogic_vars.select(
    F.col("svProvId").alias("PROV_ID"),
    F.col("svProvAddrLn1").alias("PROV_ADDR_LN_1"),
    F.col("svProvAddrLn2").alias("PROV_ADDR_LN_2"),
    F.col("svProvCityNm").alias("PROV_CITY_NM"),
    F.col("PROV_ST_CD").alias("PROV_ST_CD"),
    F.col("svProvZip").alias("PROV_ZIP")
)

df_ds_CD_MPPNG_Lkp_Data = spark.read.parquet(f"{adls_path}/ds/CD_MPPNG.parquet")
df_ds_CD_MPPNG_Lkp_Data = df_ds_CD_MPPNG_Lkp_Data.select(
    "CD_MPPNG_SK",
    "SRC_CD",
    "SRC_CD_NM",
    "SRC_CLCTN_CD",
    "SRC_DRVD_LKUP_VAL",
    "SRC_DOMAIN_NM",
    "SRC_SYS_CD",
    "TRGT_CD",
    "TRGT_CD_NM",
    "TRGT_CLCTN_CD",
    "TRGT_DOMAIN_NM"
)

df_fltr_FilterData = df_ds_CD_MPPNG_Lkp_Data.filter(
    (F.col("SRC_SYS_CD") == "FACETS") &
    (F.col("SRC_CLCTN_CD") == "FACETS DBO") &
    (F.col("TRGT_CLCTN_CD") == "IDS") &
    (F.col("SRC_DOMAIN_NM") == "PROVIDER ADDRESS TYPE") &
    (F.col("TRGT_DOMAIN_NM") == "PROVIDER ADDRESS TYPE")
)

df_lnkProvAddrTyp = df_fltr_FilterData.select(
    F.col("SRC_CD").alias("SRC_CD"),
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

df_ds_CD_MPPNG_LkpData = spark.read.parquet(f"{adls_path}/ds/CD_MPPNG.parquet")
df_ds_CD_MPPNG_LkpData = df_ds_CD_MPPNG_LkpData.select(
    "CD_MPPNG_SK",
    "SRC_CD",
    "SRC_CD_NM",
    "SRC_CLCTN_CD",
    "SRC_DRVD_LKUP_VAL",
    "SRC_DOMAIN_NM",
    "SRC_SYS_CD",
    "TRGT_CD",
    "TRGT_CD_NM",
    "TRGT_CLCTN_CD",
    "TRGT_DOMAIN_NM"
)

df_fltr_Data = df_ds_CD_MPPNG_LkpData.filter(
    (F.col("SRC_SYS_CD") == "FACETS") &
    (F.col("SRC_CLCTN_CD") == "FACETS DBO") &
    (F.col("TRGT_CLCTN_CD") == "IDS") &
    (F.col("SRC_DOMAIN_NM") == "PROVIDER ADDRESS TYPE") &
    (F.col("TRGT_DOMAIN_NM") == "PROVIDER ADDRESS TYPE")
)

df_CdMppngExtr = df_fltr_Data.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("SRC_CD").alias("SRC_CD")
)

extract_query_db2_Prov_AddrId_Lkp = """SELECT  DISTINCT PROV_ADDR.PROV_ADDR_ID,PROV_ADDR.ADDR_LN_1, PROV_ADDR.ADDR_LN_2, PROV_ADDR.CITY_NM, PROV_ADDR.POSTAL_CD, CD_MPPNG.SRC_CD, PROV_ADDR.PROV_ADDR_TYP_CD_SK, PROV_ADDR.PROV_ADDR_ST_CD_SK
FROM #$IDSOwner#.PROV_ADDR PROV_ADDR,  #$IDSOwner#.CD_MPPNG CD_MPPNG
WHERE  PROV_ADDR.PROV_ADDR_ST_CD_SK=CD_MPPNG.CD_MPPNG_SK"""
df_db2_Prov_AddrId_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_Prov_AddrId_Lkp)
    .load()
)
df_db2_Prov_AddrId_Lkp = df_db2_Prov_AddrId_Lkp.select(
    "PROV_ADDR_ID", "ADDR_LN_1", "ADDR_LN_2", "CITY_NM", "POSTAL_CD", "SRC_CD", "PROV_ADDR_TYP_CD_SK", "PROV_ADDR_ST_CD_SK"
)

df_ds_CD_MPPNG_ST_CDData = spark.read.parquet(f"{adls_path}/ds/CD_MPPNG.parquet")
df_ds_CD_MPPNG_ST_CDData = df_ds_CD_MPPNG_ST_CDData.select(
    "CD_MPPNG_SK",
    "SRC_CD",
    "SRC_CD_NM",
    "SRC_CLCTN_CD",
    "SRC_DRVD_LKUP_VAL",
    "SRC_DOMAIN_NM",
    "SRC_SYS_CD",
    "TRGT_CD",
    "TRGT_CD_NM",
    "TRGT_CLCTN_CD",
    "TRGT_DOMAIN_NM"
)

df_fltr_FilterStCdData = df_ds_CD_MPPNG_ST_CDData.filter(
    (F.col("SRC_SYS_CD") == "IDS") &
    (F.col("SRC_CLCTN_CD") == "IDS") &
    (F.col("TRGT_CLCTN_CD") == "IDS") &
    (F.col("SRC_DOMAIN_NM") == "STATE") &
    (F.col("TRGT_DOMAIN_NM") == "STATE")
)

df_lnkProvAddrSt = df_fltr_FilterStCdData.select(
    F.col("SRC_CD").alias("SRC_CD"),
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

df_lkp_StCd_join = df_lnk_Xfrm_Addr_lkp_in.alias("Lnk_Xfrm_Addr_lkp_in").join(
    df_lnkProvAddrSt.alias("lnkProvAddrSt"),
    (F.col("Lnk_Xfrm_Addr_lkp_in.PROV_ST_CD") == F.col("lnkProvAddrSt.SRC_CD")),
    "left"
)
df_lkp_StCd = df_lkp_StCd_join.select(
    F.col("Lnk_Xfrm_Addr_lkp_in.PROV_ID").alias("PROV_ID"),
    F.col("Lnk_Xfrm_Addr_lkp_in.PROV_ADDR_LN_1").alias("PROV_ADDR_LN_1"),
    F.col("Lnk_Xfrm_Addr_lkp_in.PROV_ADDR_LN_2").alias("PROV_ADDR_LN_2"),
    F.col("Lnk_Xfrm_Addr_lkp_in.PROV_CITY_NM").alias("PROV_CITY_NM"),
    F.col("Lnk_Xfrm_Addr_lkp_in.PROV_ST_CD").alias("PROV_ST_CD"),
    F.col("Lnk_Xfrm_Addr_lkp_in.PROV_ZIP").alias("PROV_ZIP"),
    F.col("lnkProvAddrSt.CD_MPPNG_SK").alias("ST_CD_SK")
)

df_lkp_AddrId_join = df_lkp_StCd.alias("Lnk_lkp_in").join(
    df_db2_Prov_AddrId_Lkp.alias("lnk_lkp_Prov_AddrId"),
    [
        F.col("Lnk_lkp_in.PROV_ID") == F.col("lnk_lkp_Prov_AddrId.PROV_ADDR_ID")
    ],
    "left"
)
df_lkp_AddrId = df_lkp_AddrId_join.select(
    F.col("Lnk_lkp_in.PROV_ID").alias("PROV_ID"),
    F.col("Lnk_lkp_in.PROV_ADDR_LN_1").alias("PROV_ADDR_LN_1"),
    F.col("Lnk_lkp_in.PROV_ADDR_LN_2").alias("PROV_ADDR_LN_2"),
    F.col("Lnk_lkp_in.PROV_CITY_NM").alias("PROV_CITY_NM"),
    F.col("Lnk_lkp_in.PROV_ST_CD").alias("PROV_ST_CD"),
    F.col("Lnk_lkp_in.PROV_ZIP").alias("PROV_ZIP"),
    F.col("Lnk_lkp_in.ST_CD_SK").alias("ST_CD_SK"),
    F.col("lnk_lkp_Prov_AddrId.ADDR_LN_1").alias("ADDR_LN_1"),
    F.col("lnk_lkp_Prov_AddrId.ADDR_LN_2").alias("ADDR_LN_2"),
    F.col("lnk_lkp_Prov_AddrId.CITY_NM").alias("CITY_NM"),
    F.col("lnk_lkp_Prov_AddrId.POSTAL_CD").alias("POSTAL_CD"),
    F.col("lnk_lkp_Prov_AddrId.SRC_CD").alias("SRC_CD_ADDR"),
    F.col("lnk_lkp_Prov_AddrId.PROV_ADDR_TYP_CD_SK").alias("PROV_ADDR_TYP_CD_SK"),
    F.col("lnk_lkp_Prov_AddrId.PROV_ADDR_ST_CD_SK").alias("PROV_ADDR_ST_CD_SK")
)

df_Xfrm_BusinessLogic_1_vars = df_lkp_AddrId.withColumn(
    "svCmpAddr",
    F.when(
       (F.col("ADDR_LN_1").isNull() | (trim(F.col("ADDR_LN_1")) == "")) &
       (F.col("ADDR_LN_2").isNull() | (trim(F.col("ADDR_LN_2")) == "")) &
       (F.col("CITY_NM").isNull() | (trim(F.col("CITY_NM")) == "")) &
       (F.col("POSTAL_CD").isNull() | (trim(F.col("POSTAL_CD")) == "")) &
       (F.col("SRC_CD_ADDR").isNull() | (trim(F.col("SRC_CD_ADDR")) == "")),
       F.lit("01")
    ).otherwise(
       F.when(
         trim(F.col("PROV_ADDR_LN_1")) != trim(F.col("ADDR_LN_1")),
         F.lit("02")
       ).when(
         trim(F.col("PROV_ADDR_LN_2")) != trim(F.col("ADDR_LN_2")),
         F.lit("02")
       ).when(
         trim(F.col("PROV_CITY_NM")) != trim(F.col("CITY_NM")),
         F.lit("02")
       ).when(
         trim(F.col("PROV_ZIP")) != trim(F.col("POSTAL_CD")),
         F.lit("02")
       ).when(
         trim(F.col("ST_CD_SK")) != trim(F.col("PROV_ADDR_ST_CD_SK")),
         F.lit("02")
       ).otherwise(F.col("PROV_ADDR_TYP_CD_SK"))
    )
).withColumn(
    "svSrcCd",
    F.when(
        F.col("svCmpAddr") == "01",
        F.lit("1")
    ).when(
        F.col("svCmpAddr") == "02",
        F.lit("ADD")
    ).otherwise(F.col("PROV_ADDR_TYP_CD_SK"))
)

df_lnk_Xfrm_CdMppnglkp_in = df_Xfrm_BusinessLogic_1_vars.select(
    F.col("PROV_ID").alias("PROV_ID"),
    F.col("PROV_ADDR_LN_1").alias("PROV_ADDR_LN_1"),
    F.col("PROV_ADDR_LN_2").alias("PROV_ADDR_LN_2"),
    F.col("PROV_CITY_NM").alias("PROV_CITY_NM"),
    F.col("PROV_ST_CD").alias("PROV_ST_CD"),
    F.col("PROV_ZIP").alias("PROV_ZIP"),
    F.col("ADDR_LN_1").alias("ADDR_LN_1"),
    F.col("ADDR_LN_2").alias("ADDR_LN_2"),
    F.col("CITY_NM").alias("CITY_NM"),
    F.col("POSTAL_CD").alias("POSTAL_CD"),
    F.col("SRC_CD_ADDR").alias("SRC_CD_ADDR"),
    F.col("PROV_ADDR_TYP_CD_SK").alias("PROV_ADDR_TYP_CD_SK"),
    F.col("svSrcCd").alias("SRC_CD")
)

df_lkp_CdMppng_join = df_lnk_Xfrm_CdMppnglkp_in.alias("lnk_Xfrm_CdMppnglkp_in").join(
    df_CdMppngExtr.alias("CdMppngExtr"),
    (F.col("lnk_Xfrm_CdMppnglkp_in.PROV_ADDR_TYP_CD_SK") == F.col("CdMppngExtr.CD_MPPNG_SK")),
    "left"
)
df_lkp_CdMppng = df_lkp_CdMppng_join.select(
    F.col("lnk_Xfrm_CdMppnglkp_in.PROV_ID").alias("PROV_ID"),
    F.col("lnk_Xfrm_CdMppnglkp_in.PROV_ADDR_LN_1").alias("PROV_ADDR_LN_1"),
    F.col("lnk_Xfrm_CdMppnglkp_in.PROV_ADDR_LN_2").alias("PROV_ADDR_LN_2"),
    F.col("lnk_Xfrm_CdMppnglkp_in.PROV_CITY_NM").alias("PROV_CITY_NM"),
    F.col("lnk_Xfrm_CdMppnglkp_in.PROV_ST_CD").alias("PROV_ST_CD"),
    F.col("lnk_Xfrm_CdMppnglkp_in.PROV_ZIP").alias("PROV_ZIP"),
    F.col("lnk_Xfrm_CdMppnglkp_in.ADDR_LN_1").alias("ADDR_LN_1"),
    F.col("lnk_Xfrm_CdMppnglkp_in.ADDR_LN_2").alias("ADDR_LN_2"),
    F.col("lnk_Xfrm_CdMppnglkp_in.CITY_NM").alias("CITY_NM"),
    F.col("lnk_Xfrm_CdMppnglkp_in.POSTAL_CD").alias("POSTAL_CD"),
    F.col("lnk_Xfrm_CdMppnglkp_in.SRC_CD_ADDR").alias("SRC_CD_ADDR"),
    F.col("lnk_Xfrm_CdMppnglkp_in.PROV_ADDR_TYP_CD_SK").alias("PROV_ADDR_TYP_CD_SK"),
    F.col("lnk_Xfrm_CdMppnglkp_in.SRC_CD").alias("SRC_CD"),
    F.col("CdMppngExtr.SRC_CD").alias("PROV_ADDR_TYP_CD")
)

df_Xfrm_ProvAdrTyp_vars = df_lkp_CdMppng.withColumn(
    "svSrcCdMppng",
    F.when(F.col("PROV_ADDR_TYP_CD").isNull(), F.lit("UNK")).otherwise(F.col("PROV_ADDR_TYP_CD"))
).withColumn(
    "svSrcCd",
    F.when(
        F.col("SRC_CD") == "ADD",
        F.concat(F.col("svSrcCdMppng"), F.lit("1"))
    ).when(
        F.col("SRC_CD") == "1",
        F.lit("1")
    ).otherwise(F.col("svSrcCdMppng"))
)

df_lnk_Xfrm_lkp_in = df_Xfrm_ProvAdrTyp_vars.select(
    F.col("PROV_ID").alias("PROV_ID"),
    F.col("svSrcCd").alias("PROV_ADDR_TYP_CD")
)

df_lkp_Codes2_join_1 = df_lnk_Xfrm_out.alias("lnk_Xfrm_out")
df_lkp_Codes2_join_2 = df_db2_Prov_Addr_Lkp.alias("lnk_lkp_Prov_Addr")
df_lkp_Codes2_join_3 = df_lnk_Xfrm_lkp_in.alias("lnk_Xfrm_lkp_in")

df_lkp_Codes2_step = df_lkp_Codes2_join_1.join(
    df_lkp_Codes2_join_2,
    [
        F.col("lnk_Xfrm_out.PROV_NTNL_PROV_ID") == F.col("lnk_lkp_Prov_Addr.NTNL_PROV_ID")
    ],
    "left"
).join(
    df_lkp_Codes2_join_3,
    [
        F.col("lnk_Xfrm_out.PROV_ID") == F.col("lnk_Xfrm_lkp_in.PROV_ID")
    ],
    "left"
)

df_lkp_Codes2 = df_lkp_Codes2_step.select(
    F.col("lnk_Xfrm_out.PROV_ID").alias("PROV_ID"),
    F.col("lnk_Xfrm_out.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.col("lnk_Xfrm_out.LOC").alias("LOC"),
    F.col("lnk_Xfrm_out.PROV_LAST_NM").alias("PROV_LAST_NM"),
    F.col("lnk_Xfrm_out.PROV_FIRST_NM").alias("PROV_FIRST_NM"),
    F.col("lnk_Xfrm_out.PROV_MIDINIT").alias("PROV_MIDINIT"),
    F.col("lnk_Xfrm_out.PROV_NM_SFX").alias("PROV_NM_SFX"),
    F.col("lnk_Xfrm_out.PROV_ADDR_LN_1").alias("PROV_ADDR_LN_1"),
    F.col("lnk_Xfrm_out.PROV_ADDR_LN_2").alias("PROV_ADDR_LN_2"),
    F.col("lnk_Xfrm_out.PROV_CITY_NM").alias("PROV_CITY_NM"),
    F.col("lnk_Xfrm_out.PROV_CNTY_CD").alias("PROV_CNTY_CD"),
    F.col("lnk_Xfrm_out.PROV_ST_CD").alias("PROV_ST_CD"),
    F.col("lnk_Xfrm_out.PROV_ZIP").alias("PROV_ZIP"),
    F.col("lnk_Xfrm_out.PROV_TEL_NO").alias("PROV_TEL_NO"),
    F.col("lnk_Xfrm_out.PROV_EMAIL_ADDR").alias("PROV_EMAIL_ADDR"),
    F.col("lnk_Xfrm_out.PROV_FAX_NO").alias("PROV_FAX_NO"),
    F.col("lnk_Xfrm_out.PROV_NTNL_PROV_ID").alias("PROV_NTNL_PROV_ID"),
    F.col("lnk_Xfrm_out.CSTM_PROV_TYP").alias("CSTM_PROV_TYP"),
    F.col("lnk_Xfrm_out.PCP_FLAG").alias("PCP_FLAG"),
    F.col("lnk_Xfrm_out.PRSCRB_PROV_FLAG").alias("PRSCRB_PROV_FLAG"),
    F.col("lnk_Xfrm_out.PROV_ORIG_PLN_CD").alias("PROV_ORIG_PLN_CD"),
    F.col("lnk_Xfrm_out.PROV_TAX_ID").alias("PROV_TAX_ID"),
    F.col("lnk_Xfrm_out.ALT_FLD_1").alias("ALT_FLD_1"),
    F.col("lnk_Xfrm_out.ALT_FLD_2").alias("ALT_FLD_2"),
    F.col("lnk_Xfrm_out.ALT_FLD_3").alias("ALT_FLD_3"),
    F.col("lnk_Xfrm_out.CSTM_FLD_1").alias("CSTM_FLD_1"),
    F.col("lnk_Xfrm_out.CSTM_FLD_2").alias("CSTM_FLD_2"),
    F.col("lnk_Xfrm_out.OFC_MGR_LAST_NM").alias("OFC_MGR_LAST_NM"),
    F.col("lnk_Xfrm_out.OFC_MGR_FIRST_NM").alias("OFC_MGR_FIRST_NM"),
    F.col("lnk_Xfrm_out.OFC_MGR_TTL").alias("OFC_MGR_TTL"),
    F.col("lnk_Xfrm_out.OFC_MGR_EMAIL").alias("OFC_MGR_EMAIL"),
    F.col("lnk_Xfrm_out.RUN_DT").alias("RUN_DT"),
    F.col("lnk_Xfrm_out.SRC_SYS").alias("SRC_SYS"),
    F.col("lnk_lkp_Prov_Addr.ADDR_LN_1").alias("ADDR_LN_1"),
    F.col("lnk_lkp_Prov_Addr.ADDR_LN_2").alias("ADDR_LN_2"),
    F.col("lnk_lkp_Prov_Addr.ADDR_LN_3").alias("ADDR_LN_3"),
    F.col("lnk_lkp_Prov_Addr.CITY_NM").alias("CITY_NM"),
    F.col("lnk_lkp_Prov_Addr.PROV_ADDR_SK").alias("PROV_ADDR_SK"),
    F.col("lnk_lkp_Prov_Addr.POSTAL_CD").alias("POSTAL_CD"),
    F.col("lnk_lkp_Prov_Addr.SRC_CD").alias("SRC_CD"),
    F.col("lnk_Xfrm_lkp_in.PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD")
)

df_Xfrm_BusinessRules_vars = (
    df_lkp_Codes2
    .withColumn("svProvEffDt", F.lit("1753-01-01"))
    .withColumn(
        "svProvId",
        F.when(
            F.substring(F.col("PROV_ADDR_ID"), 1, 4) == F.lit("PHY_"),
            F.lit(1)
        ).otherwise(F.lit(2))
    )
    .withColumn(
        "svProvAddrId",
        F.when(
            ~isFirstCharNumeric,
            trim(F.col("PROV_ADDR_ID").substr(F.lit(5), (F.length("PROV_ADDR_ID") - 4)))
        ).otherwise(F.col("PROV_ADDR_ID"))
    )
)

df_lnk_FctsIdsProvAddrXfrm_Out = df_Xfrm_BusinessRules_vars.select(
    (
        F.lit("BCA") + F.lit(";") +
        trim(F.col("svProvAddrId")) + F.lit(";") +
        F.col("PROV_ADDR_TYP_CD") + F.lit(";") +
        F.col("svProvEffDt")
    ).alias("PRI_NAT_KEY_STRING"),
    F.col("RunIDTimeStamp").alias("FIRST_RECYC_TS"),
    F.lit(0).alias("PROV_ADDR_SK"),
    F.col("svProvAddrId").alias("PROV_ADDR_ID"),
    F.col("PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD"),
    F.col("svProvEffDt").alias("PROV_ADDR_EFF_DT"),
    F.col("SrcSysCd").alias("SRC_SYS_CD"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("SrcSysCdSK").alias("SRC_SYS_CD_SK"),
    F.lit("NA").alias("PROV_ADDR_CNTY_CLS_CD"),
    F.lit("NA").alias("PROV_ADDR_GEO_ACES_RTRN_CD"),
    F.lit("NA").alias("PROV_ADDR_METRORURAL_COV_CD"),
    F.lit("NA").alias("PROV_ADDR_TERM_RSN_CD"),
    F.lit("N").alias("HCAP_IN"),
    F.lit("N").alias("PDX_24_HR_IN"),
    F.lit("N").alias("PRCTC_LOC_IN"),
    F.lit("N").alias("PROV_ADDR_DIR_IN"),
    F.lit("2199-12-31").alias("TERM_DT"),
    F.when(
        F.col("svProvId") == 2,
        F.when(
            (F.col("PROV_ADDR_LN_1").isNull()) | (trim(F.col("PROV_ADDR_LN_1")) == ""),
            F.lit(None)
        ).otherwise(F.upper(trim(F.col("PROV_ADDR_LN_1"))))
    ).otherwise(
        F.when(
            (F.col("ADDR_LN_1").isNull()) | (trim(F.col("ADDR_LN_1")) == ""),
            F.lit(None)
        ).otherwise(F.upper(trim(F.col("ADDR_LN_1"))))
    ).alias("ADDR_LN_1"),
    F.when(
        F.col("svProvId") == 2,
        F.when(
            (F.col("PROV_ADDR_LN_2").isNull()) | (trim(F.col("PROV_ADDR_LN_2")) == ""),
            F.lit(None)
        ).otherwise(F.upper(trim(F.col("PROV_ADDR_LN_2"))))
    ).otherwise(
        F.when(
            (F.col("ADDR_LN_2").isNull()) | (trim(F.col("ADDR_LN_2")) == ""),
            F.lit(None)
        ).otherwise(F.upper(trim(F.col("ADDR_LN_2"))))
    ).alias("ADDR_LN_2"),
    F.lit(None).alias("ADDR_LN_3"),
    F.when(
        F.col("svProvId") == 2,
        F.when(
            (F.col("PROV_CITY_NM").isNull()) | (trim(F.col("PROV_CITY_NM")) == ""),
            F.lit(None)
        ).otherwise(F.upper(trim(F.col("PROV_CITY_NM"))))
    ).otherwise(
        F.when(
            (F.col("CITY_NM").isNull()) | (trim(F.col("CITY_NM")) == ""),
            F.lit(None)
        ).otherwise(F.upper(trim(F.col("CITY_NM"))))
    ).alias("CITY_NM"),
    F.when(
        F.col("PROV_ST_CD").isNull(),
        F.lit("1")
    ).otherwise(F.col("PROV_ST_CD")).alias("PROV_ADDR_ST_CD"),
    F.substring(
        F.when(
            F.col("svProvId") == 2,
            F.col("PROV_ZIP")
        ).otherwise(
            F.when(
                (F.col("POSTAL_CD").isNull()) | (trim(F.col("POSTAL_CD")) == ""),
                F.lit(None)
            ).otherwise(trim(F.col("POSTAL_CD")))
        ),
        1,
        11
    ).alias("POSTAL_CD"),
    F.lit("UNK").alias("CNTY_NM"),
    F.when(
        (F.col("PROV_TEL_NO").isNull()) | (trim(F.col("PROV_TEL_NO")) == ""),
        F.lit(None)
    ).otherwise(trim(F.col("PROV_TEL_NO"))).alias("PHN_NO"),
    F.lit(None).alias("PHN_NO_EXT"),
    F.when(
        (F.col("PROV_FAX_NO").isNull()) | (trim(F.col("PROV_FAX_NO")) == ""),
        F.lit(None)
    ).otherwise(trim(F.col("PROV_FAX_NO"))).alias("FAX_NO"),
    F.lit(None).alias("FAX_NO_EXT"),
    F.when(
        (F.col("PROV_EMAIL_ADDR").isNull()) | (trim(F.col("PROV_EMAIL_ADDR")) == ""),
        F.lit(None)
    ).otherwise(trim(F.col("PROV_EMAIL_ADDR"))).alias("EMAIL_ADDR_TX"),
    F.lit(0).alias("LAT_TX"),
    F.lit(0).alias("LONG_TX"),
    F.when(
        F.col("PROV_ADDR_TYP_CD").isNull() | (trim(F.col("PROV_ADDR_TYP_CD")) == ""),
        F.lit(1)
    ).otherwise(F.col("PROV_ADDR_TYP_CD")).alias("PRAD_TYPE_MAIL"),
    F.col("svProvEffDt").alias("PROV2_PRAD_EFF_DT"),
    trim(F.col("PROV_ADDR_TYP_CD")).alias("PROV_ADDR_TYP_CD_ORIG"),
    F.lit(None).alias("ATND_TEXT")
)

df_lnk_ToBalLkup = df_Xfrm_BusinessRules_vars.select(
    F.col("SrcSysCdSK").alias("SRC_SYS_CD_SK"),
    F.col("svProvAddrId").alias("PROV_ADDR_ID"),
    F.when(
        F.col("PROV_ADDR_TYP_CD").isNull() | (trim(F.col("PROV_ADDR_TYP_CD")) == ""),
        F.lit(1)
    ).otherwise(F.col("PROV_ADDR_TYP_CD")).alias("PROV_ADDR_TYP_CD"),
    F.col("svProvEffDt").alias("EFF_DT_SK")
)

write_files(
    df_lnk_FctsIdsProvAddrXfrm_Out,
    f"{adls_path}/ds/PROV_ADDR.{SrcSysCd}.xfrm.{RunID}.parquet",
    delimiter=",",
    mode="overwrite",
    is_parqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_lkp_Fkey_Bal_join = df_lnk_ToBalLkup.alias("lnk_ToBalLkup").join(
    df_lnkProvAddrTyp.alias("lnkProvAddrTyp"),
    (F.col("lnk_ToBalLkup.PROV_ADDR_TYP_CD") == F.col("lnkProvAddrTyp.SRC_CD")),
    "left"
)
df_lkp_Fkey_Bal = df_lkp_Fkey_Bal_join.select(
    F.col("lnk_ToBalLkup.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("lnk_ToBalLkup.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.col("lnkProvAddrTyp.CD_MPPNG_SK").alias("PROV_ADDR_TYP_CD_SK"),
    F.col("lnk_ToBalLkup.EFF_DT_SK").alias("EFF_DT_SK")
)

df_Xfrm_B_PROV_ADDR = df_lkp_Fkey_Bal.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.col("PROV_ADDR_TYP_CD_SK").alias("PROV_ADDR_TYP_CD_SK"),
    F.col("EFF_DT_SK").alias("EFF_DT_SK")
)

df_seq_B_PROV_ADDR = df_Xfrm_B_PROV_ADDR.select(
    F.rpad(F.col("SRC_SYS_CD_SK").cast(StringType()), 50, " ").alias("SRC_SYS_CD_SK"),
    F.rpad(F.col("PROV_ADDR_ID").cast(StringType()), 50, " ").alias("PROV_ADDR_ID"),
    F.rpad(F.col("PROV_ADDR_TYP_CD_SK").cast(StringType()), 50, " ").alias("PROV_ADDR_TYP_CD_SK"),
    F.rpad(F.col("EFF_DT_SK").cast(StringType()), 10, " ").alias("EFF_DT_SK")
)

write_files(
    df_seq_B_PROV_ADDR,
    f"{adls_path}/load/B_PROV_ADDR.{SrcSysCd}.dat",
    delimiter=",",
    mode="overwrite",
    is_parqruet=False,
    header=False,
    quote="^",
    nullValue=None
)