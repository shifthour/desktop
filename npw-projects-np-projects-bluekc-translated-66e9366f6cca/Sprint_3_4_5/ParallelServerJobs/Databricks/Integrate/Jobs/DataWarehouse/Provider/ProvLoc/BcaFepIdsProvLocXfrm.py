# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2005, 2006, 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Job Name :  BcaFepIdsProvLocXfrm
# MAGIC 
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC 
# MAGIC     Transformation rules applied on Extracted data information that is used to populate the IDS ProvAddr table 
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Sudhir Bomshetty            2017-08-21         #5781                           Initial Programming                                             IntegrateDev2                 Kalyan Neelam             2017-10-05
# MAGIC 
# MAGIC Sudhir Bomshetty            2018-03-26         #5781                        Updated logic for PROV_ID                                  IntegrateDev2                Jaideep Mankala          04/02/2018

# MAGIC JobName: BcaFepIdsProvLocXfrm
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
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, lit, when, substring, length, rlike, upper, concat
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Retrieve job parameters
SrcSysCd = get_widget_value("SrcSysCd", "")
IDSOwner = get_widget_value("IDSOwner", "")
ids_secret_name = get_widget_value("ids_secret_name", "")
RunID = get_widget_value("RunID", "")
RunIDTimeStamp = get_widget_value("RunIDTimeStamp", "")
SrcSysCdSK = get_widget_value("SrcSysCdSK", "")

# --------------------------------------------------------------------------------
# ds_FEP_PROV_Extr (PxDataSet) READ
# --------------------------------------------------------------------------------
df_ds_FEP_PROV_Extr = spark.read.parquet(f"{adls_path}/ds/FEP_PROV.{SrcSysCd}.extr.{RunID}.parquet")
df_ds_FEP_PROV_Extr = df_ds_FEP_PROV_Extr.select(
    col("PROV_ID"),
    col("OFC_CD"),
    col("LOC"),
    col("PROV_LAST_NM"),
    col("PROV_FIRST_NM"),
    col("PROV_MIDINIT"),
    col("PROV_NM_SFX"),
    col("PROV_ADDR_LN_1"),
    col("PROV_ADDR_LN_2"),
    col("PROV_CITY_NM"),
    col("PROV_CNTY_CD"),
    col("PROV_ST_CD"),
    col("PROV_ZIP"),
    col("PROV_TEL_NO"),
    col("PROV_EMAIL_ADDR"),
    col("PROV_FAX_NO"),
    col("PROV_NTNL_PROV_ID"),
    col("CSTM_PROV_TYP"),
    col("PCP_FLAG"),
    col("PRSCRB_PROV_FLAG"),
    col("PROV_ORIG_PLN_CD"),
    col("PROV_TAX_ID"),
    col("ALT_FLD_1"),
    col("ALT_FLD_2"),
    col("ALT_FLD_3"),
    col("CSTM_FLD_1"),
    col("CSTM_FLD_2"),
    col("OFC_MGR_LAST_NM"),
    col("OFC_MGR_FIRST_NM"),
    col("OFC_MGR_TTL"),
    col("OFC_MGR_EMAIL"),
    col("RUN_DT"),
    col("SRC_SYS")
)

# --------------------------------------------------------------------------------
# Xfrm_BusinessLogic (CTransformerStage)
# --------------------------------------------------------------------------------
df_xfrm_businesslogic_stagevars = (
    df_ds_FEP_PROV_Extr
    .withColumn(
        "svProvId",
        when(
            ~substring(col("PROV_ID"), 1, 1).rlike("^[0-9]$"),
            trim(substring(col("PROV_ID"), 5, length(col("PROV_ID")) - 4))
        ).otherwise(col("PROV_ID"))
    )
    .withColumn(
        "svProvAddrLn1",
        when(
            trim(col("PROV_ADDR_LN_1")) == "?",
            lit(None)
        ).otherwise(
            substring(upper(trim(col("PROV_ADDR_LN_1"))), 1, 40)
        )
    )
    .withColumn(
        "svProvAddrLn2",
        when(
            trim(col("PROV_ADDR_LN_2")) == "?",
            lit(None)
        ).otherwise(
            substring(upper(trim(col("PROV_ADDR_LN_2"))), 1, 40)
        )
    )
    .withColumn(
        "svProvCityNm",
        when(
            trim(col("PROV_CITY_NM")) == "?",
            lit(None)
        ).otherwise(
            substring(upper(trim(col("PROV_CITY_NM"))), 1, 40)
        )
    )
)

df_xfrm_businesslogic_out = df_xfrm_businesslogic_stagevars.select(
    col("svProvId").alias("PROV_ID"),
    col("svProvId").alias("PROV_ADDR_ID"),
    col("LOC"),
    col("PROV_LAST_NM"),
    col("PROV_FIRST_NM"),
    col("PROV_MIDINIT"),
    col("PROV_NM_SFX"),
    col("svProvAddrLn1").alias("PROV_ADDR_LN_1"),
    col("svProvAddrLn2").alias("PROV_ADDR_LN_2"),
    col("svProvCityNm").alias("PROV_CITY_NM"),
    col("PROV_CNTY_CD"),
    trim(upper(col("PROV_ST_CD"))).alias("PROV_ST_CD"),
    col("PROV_ZIP"),
    col("PROV_EMAIL_ADDR"),
    col("PROV_NTNL_PROV_ID"),
    col("CSTM_PROV_TYP"),
    col("PCP_FLAG"),
    col("PRSCRB_PROV_FLAG"),
    col("PROV_ORIG_PLN_CD"),
    col("PROV_TAX_ID"),
    col("ALT_FLD_1"),
    col("ALT_FLD_2"),
    col("ALT_FLD_3"),
    col("CSTM_FLD_1"),
    col("CSTM_FLD_2"),
    col("OFC_MGR_LAST_NM"),
    col("OFC_MGR_FIRST_NM"),
    col("OFC_MGR_TTL"),
    col("OFC_MGR_EMAIL"),
    col("RUN_DT"),
    col("SRC_SYS")
)

df_xfrm_businesslogic_addr = df_xfrm_businesslogic_stagevars.select(
    col("svProvId").alias("PROV_ID"),
    col("svProvAddrLn1").alias("PROV_ADDR_LN_1"),
    col("svProvAddrLn2").alias("PROV_ADDR_LN_2"),
    col("svProvCityNm").alias("PROV_CITY_NM"),
    col("PROV_ST_CD"),
    substring(trim(col("PROV_ZIP")), 1, 5).alias("PROV_ZIP")
)

# --------------------------------------------------------------------------------
# ds_CD_MPPNG_Lkp_Data (PxDataSet) -> fltr_FilterData (PxFilter)
# --------------------------------------------------------------------------------
df_ds_CD_MPPNG_Lkp_Data = spark.read.parquet(f"{adls_path}/ds/CD_MPPNG.parquet")
df_ds_CD_MPPNG_Lkp_Data = df_ds_CD_MPPNG_Lkp_Data.select(
    col("CD_MPPNG_SK"),
    col("SRC_CD"),
    col("SRC_CD_NM"),
    col("SRC_CLCTN_CD"),
    col("SRC_DRVD_LKUP_VAL"),
    col("SRC_DOMAIN_NM"),
    col("SRC_SYS_CD"),
    col("TRGT_CD"),
    col("TRGT_CD_NM"),
    col("TRGT_CLCTN_CD"),
    col("TRGT_DOMAIN_NM")
)

df_fltr_FilterData = df_ds_CD_MPPNG_Lkp_Data.filter(
    (col("SRC_SYS_CD") == "FACETS") &
    (col("SRC_CLCTN_CD") == "FACETS DBO") &
    (col("TRGT_CLCTN_CD") == "IDS") &
    (col("SRC_DOMAIN_NM") == "PROVIDER ADDRESS TYPE") &
    (col("TRGT_DOMAIN_NM") == "PROVIDER ADDRESS TYPE")
)

df_fltr_FilterData_out = df_fltr_FilterData.select(
    col("SRC_CD").alias("SRC_CD"),
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

# --------------------------------------------------------------------------------
# ds_CD_MPPNG_LkpData (PxDataSet) -> fltr_Data (PxFilter)
# --------------------------------------------------------------------------------
df_ds_CD_MPPNG_LkpData = spark.read.parquet(f"{adls_path}/ds/CD_MPPNG.parquet")
df_ds_CD_MPPNG_LkpData = df_ds_CD_MPPNG_LkpData.select(
    col("CD_MPPNG_SK"),
    col("SRC_CD"),
    col("SRC_CD_NM"),
    col("SRC_CLCTN_CD"),
    col("SRC_DRVD_LKUP_VAL"),
    col("SRC_DOMAIN_NM"),
    col("SRC_SYS_CD"),
    col("TRGT_CD"),
    col("TRGT_CD_NM"),
    col("TRGT_CLCTN_CD"),
    col("TRGT_DOMAIN_NM")
)

df_fltr_Data = df_ds_CD_MPPNG_LkpData.filter(
    (col("SRC_SYS_CD") == "FACETS") &
    (col("SRC_CLCTN_CD") == "FACETS DBO") &
    (col("TRGT_CLCTN_CD") == "IDS") &
    (col("SRC_DOMAIN_NM") == "PROVIDER ADDRESS TYPE") &
    (col("TRGT_DOMAIN_NM") == "PROVIDER ADDRESS TYPE")
)

df_CdMppngExtr = df_fltr_Data.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("SRC_CD").alias("SRC_CD")
)

# --------------------------------------------------------------------------------
# db2_Prov_AddrId_Lkp (DB2ConnectorPX) READ with JDBC
# --------------------------------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_Prov_AddrId = (
    f"SELECT DISTINCT PROV_ADDR.PROV_ADDR_ID, PROV_ADDR.ADDR_LN_1, PROV_ADDR.ADDR_LN_2, "
    f"PROV_ADDR.CITY_NM, PROV_ADDR.POSTAL_CD, CD_MPPNG.SRC_CD, PROV_ADDR.PROV_ADDR_TYP_CD_SK, "
    f"PROV_ADDR.PROV_ADDR_ST_CD_SK "
    f"FROM {IDSOwner}.PROV_ADDR PROV_ADDR, {IDSOwner}.CD_MPPNG CD_MPPNG "
    f"WHERE PROV_ADDR.PROV_ADDR_ST_CD_SK=CD_MPPNG.CD_MPPNG_SK"
)

df_db2_Prov_AddrId_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_Prov_AddrId)
    .load()
)

# --------------------------------------------------------------------------------
# ds_CD_MPPNG_ST_CD (PxDataSet) -> fltr_FilterStCdData (PxFilter)
# --------------------------------------------------------------------------------
df_ds_CD_MPPNG_ST_CD = spark.read.parquet(f"{adls_path}/ds/CD_MPPNG.parquet")
df_ds_CD_MPPNG_ST_CD = df_ds_CD_MPPNG_ST_CD.select(
    col("CD_MPPNG_SK"),
    col("SRC_CD"),
    col("SRC_CD_NM"),
    col("SRC_CLCTN_CD"),
    col("SRC_DRVD_LKUP_VAL"),
    col("SRC_DOMAIN_NM"),
    col("SRC_SYS_CD"),
    col("TRGT_CD"),
    col("TRGT_CD_NM"),
    col("TRGT_CLCTN_CD"),
    col("TRGT_DOMAIN_NM")
)

df_fltr_FilterStCdData = df_ds_CD_MPPNG_ST_CD.filter(
    (col("SRC_SYS_CD") == "IDS") &
    (col("SRC_CLCTN_CD") == "IDS") &
    (col("TRGT_CLCTN_CD") == "IDS") &
    (col("SRC_DOMAIN_NM") == "STATE") &
    (col("TRGT_DOMAIN_NM") == "STATE")
)

df_lnkProvAddrSt = df_fltr_FilterStCdData.select(
    col("SRC_CD").alias("SRC_CD"),
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

# --------------------------------------------------------------------------------
# lkp_StCd (PxLookup) -> merges df_xfrm_businesslogic_addr (PrimaryLink) with df_lnkProvAddrSt (LookupLink, left join)
# --------------------------------------------------------------------------------
df_lkp_StCd = (
    df_xfrm_businesslogic_addr.alias("Lnk_Xfrm_Addr_lkp_in")
    .join(
        df_lnkProvAddrSt.alias("lnkProvAddrSt"),
        on=[
            col("Lnk_Xfrm_Addr_lkp_in.PROV_ST_CD") == col("lnkProvAddrSt.SRC_CD")
        ],
        how="left"
    )
)

df_lkp_StCd_out = df_lkp_StCd.select(
    col("Lnk_Xfrm_Addr_lkp_in.PROV_ID").alias("PROV_ID"),
    col("Lnk_Xfrm_Addr_lkp_in.PROV_ADDR_LN_1").alias("PROV_ADDR_LN_1"),
    col("Lnk_Xfrm_Addr_lkp_in.PROV_ADDR_LN_2").alias("PROV_ADDR_LN_2"),
    col("Lnk_Xfrm_Addr_lkp_in.PROV_CITY_NM").alias("PROV_CITY_NM"),
    col("Lnk_Xfrm_Addr_lkp_in.PROV_ST_CD").alias("PROV_ST_CD"),
    col("Lnk_Xfrm_Addr_lkp_in.PROV_ZIP").alias("PROV_ZIP"),
    col("lnkProvAddrSt.CD_MPPNG_SK").alias("ST_CD_SK")
)

# --------------------------------------------------------------------------------
# lkp_AddrId (PxLookup) -> merges df_lkp_StCd_out (PrimaryLink) with df_db2_Prov_AddrId_Lkp (LookupLink, left join)
#    JoinConditions mention an unused "lnk_FctsIdsProvGrpRelshpXfrm_Strip_out.PRAD_EFF_DT" => no actual column exists, 
#    but we replicate the second condition with a dummy col to avoid skipping logic.
# --------------------------------------------------------------------------------
df_lkp_StCd_out_temp = df_lkp_StCd_out.withColumn("PRAD_EFF_DT", lit(None))  # placeholder for the missing column
df_lkp_AddrId = (
    df_lkp_StCd_out_temp.alias("Lnk_lkp_in")
    .join(
        df_db2_Prov_AddrId_Lkp.alias("lnk_lkp_Prov_AddrId"),
        on=[
            col("Lnk_lkp_in.PROV_ID") == col("lnk_lkp_Prov_AddrId.PROV_ADDR_ID"),
            col("Lnk_lkp_in.PRAD_EFF_DT") == col("lnk_lkp_Prov_AddrId.ADDR_LN_1")
        ],
        how="left"
    )
)

df_lkp_AddrId_out = df_lkp_AddrId.select(
    col("Lnk_lkp_in.PROV_ID").alias("PROV_ID"),
    col("Lnk_lkp_in.PROV_ADDR_LN_1").alias("PROV_ADDR_LN_1"),
    col("Lnk_lkp_in.PROV_ADDR_LN_2").alias("PROV_ADDR_LN_2"),
    col("Lnk_lkp_in.PROV_CITY_NM").alias("PROV_CITY_NM"),
    col("Lnk_lkp_in.PROV_ST_CD").alias("PROV_ST_CD"),
    col("Lnk_lkp_in.PROV_ZIP").alias("PROV_ZIP"),
    col("Lnk_lkp_in.ST_CD_SK").alias("ST_CD_SK"),
    col("lnk_lkp_Prov_AddrId.ADDR_LN_1").alias("ADDR_LN_1"),
    col("lnk_lkp_Prov_AddrId.ADDR_LN_2").alias("ADDR_LN_2"),
    col("lnk_lkp_Prov_AddrId.CITY_NM").alias("CITY_NM"),
    col("lnk_lkp_Prov_AddrId.POSTAL_CD").alias("POSTAL_CD"),
    col("lnk_lkp_Prov_AddrId.SRC_CD").alias("SRC_CD_ADDR"),
    col("lnk_lkp_Prov_AddrId.PROV_ADDR_TYP_CD_SK").alias("PROV_ADDR_TYP_CD_SK"),
    col("lnk_lkp_Prov_AddrId.PROV_ADDR_ST_CD_SK").alias("PROV_ADDR_ST_CD_SK")
)

# --------------------------------------------------------------------------------
# Xfrm_BusLogic (CTransformerStage)
# --------------------------------------------------------------------------------
df_xfrm_buslogic_stagevars = (
    df_lkp_AddrId_out
    .withColumn(
        "svCmpAddr",
        when(
            (
                (col("ADDR_LN_1").isNull()) | (trim(col("ADDR_LN_1")) == "")
            ) & (
                (col("ADDR_LN_2").isNull()) | (trim(col("ADDR_LN_2")) == "")
            ) & (
                (col("CITY_NM").isNull()) | (trim(col("CITY_NM")) == "")
            ) & (
                (col("POSTAL_CD").isNull()) | (trim(col("POSTAL_CD")) == "")
            ) & (
                (col("SRC_CD_ADDR").isNull()) | (trim(col("SRC_CD_ADDR")) == "")
            ),
            lit("01")
        )
        .otherwise(
            when(
                trim(col("PROV_ADDR_LN_1")) != trim(col("ADDR_LN_1")),
                lit("02")
            ).when(
                trim(col("PROV_ADDR_LN_2")) != trim(col("ADDR_LN_2")),
                lit("02")
            ).when(
                trim(col("PROV_CITY_NM")) != trim(col("CITY_NM")),
                lit("02")
            ).when(
                trim(col("PROV_ZIP")) != trim(col("POSTAL_CD")),
                lit("02")
            ).when(
                trim(col("ST_CD_SK")) != trim(col("PROV_ADDR_ST_CD_SK")),
                lit("02")
            ).otherwise(col("PROV_ADDR_TYP_CD_SK"))
        )
    )
    .withColumn(
        "svSrcCd",
        when(
            col("svCmpAddr") == lit("01"),
            lit("1")
        ).when(
            col("svCmpAddr") == lit("02"),
            lit("ADD")
        ).otherwise(col("PROV_ADDR_TYP_CD_SK"))
    )
)

df_xfrm_buslogic_out = df_xfrm_buslogic_stagevars.select(
    col("PROV_ID"),
    col("PROV_ADDR_LN_1"),
    col("PROV_ADDR_LN_2"),
    col("PROV_CITY_NM"),
    col("PROV_ST_CD"),
    col("PROV_ZIP"),
    col("ADDR_LN_1"),
    col("ADDR_LN_2"),
    col("CITY_NM"),
    col("POSTAL_CD"),
    col("SRC_CD_ADDR"),
    col("svSrcCd").alias("SRC_CD"),
    col("PROV_ADDR_TYP_CD_SK")
)

# --------------------------------------------------------------------------------
# lkp_CdMppng (PxLookup) -> merges df_xfrm_buslogic_out (PrimaryLink) with df_CdMppngExtr (LookupLink, left join)
# --------------------------------------------------------------------------------
df_lkp_CdMppng = (
    df_xfrm_buslogic_out.alias("lnk_Xfrm_CdMppnglkp_in")
    .join(
        df_CdMppngExtr.alias("CdMppngExtr"),
        on=[
            col("lnk_Xfrm_CdMppnglkp_in.PROV_ADDR_TYP_CD_SK") == col("CdMppngExtr.CD_MPPNG_SK")
        ],
        how="left"
    )
)

df_lkp_CdMppng_out = df_lkp_CdMppng.select(
    col("lnk_Xfrm_CdMppnglkp_in.PROV_ID").alias("PROV_ID"),
    col("lnk_Xfrm_CdMppnglkp_in.PROV_ADDR_LN_1").alias("PROV_ADDR_LN_1"),
    col("lnk_Xfrm_CdMppnglkp_in.PROV_ADDR_LN_2").alias("PROV_ADDR_LN_2"),
    col("lnk_Xfrm_CdMppnglkp_in.PROV_CITY_NM").alias("PROV_CITY_NM"),
    col("lnk_Xfrm_CdMppnglkp_in.PROV_ST_CD").alias("PROV_ST_CD"),
    col("lnk_Xfrm_CdMppnglkp_in.PROV_ZIP").alias("PROV_ZIP"),
    col("lnk_Xfrm_CdMppnglkp_in.ADDR_LN_1").alias("ADDR_LN_1"),
    col("lnk_Xfrm_CdMppnglkp_in.ADDR_LN_2").alias("ADDR_LN_2"),
    col("lnk_Xfrm_CdMppnglkp_in.CITY_NM").alias("CITY_NM"),
    col("lnk_Xfrm_CdMppnglkp_in.POSTAL_CD").alias("POSTAL_CD"),
    col("lnk_Xfrm_CdMppnglkp_in.SRC_CD_ADDR").alias("SRC_CD_ADDR"),
    col("lnk_Xfrm_CdMppnglkp_in.SRC_CD").alias("SRC_CD"),
    col("CdMppngExtr.SRC_CD").alias("PROV_ADDR_TYP_CD")
)

# --------------------------------------------------------------------------------
# Xfrm_ProvAdrTyp (CTransformerStage)
# --------------------------------------------------------------------------------
df_xfrm_ProvAdrTyp_stagevars = (
    df_lkp_CdMppng_out
    .withColumn(
        "svSrcCdMppng",
        when(
            col("PROV_ADDR_TYP_CD").isNull(),
            lit("UNK")
        ).otherwise(col("PROV_ADDR_TYP_CD"))
    )
    .withColumn(
        "svSrcCd",
        when(
            col("SRC_CD") == lit("ADD"),
            concat(col("svSrcCdMppng"), lit("1"))
        ).when(
            col("SRC_CD") == lit("1"),
            lit("1")
        ).otherwise(col("svSrcCdMppng"))
    )
)

df_xfrm_ProvAdrTyp_out = df_xfrm_ProvAdrTyp_stagevars.select(
    col("PROV_ID"),
    col("svSrcCd").alias("PROV_ADDR_TYP_CD")
)

# --------------------------------------------------------------------------------
# lkp_Codes2 (PxLookup) -> merges df_xfrm_businesslogic_out (PrimaryLink) with df_xfrm_ProvAdrTyp_out (LookupLink, left join)
#     join on PROV_ID == PROV_ID
# --------------------------------------------------------------------------------
df_lkp_Codes2 = (
    df_xfrm_businesslogic_out.alias("lnk_Xfrm_out")
    .join(
        df_xfrm_ProvAdrTyp_out.alias("lnk_Xfrm_lkp_in"),
        on=[
            col("lnk_Xfrm_out.PROV_ID") == col("lnk_Xfrm_lkp_in.PROV_ID")
        ],
        how="left"
    )
)

df_lkp_Codes2_out = df_lkp_Codes2.select(
    col("lnk_Xfrm_out.PROV_ID").alias("PROV_ID"),
    col("lnk_Xfrm_out.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    col("lnk_Xfrm_out.LOC").alias("LOC"),
    col("lnk_Xfrm_out.PROV_LAST_NM").alias("PROV_LAST_NM"),
    col("lnk_Xfrm_out.PROV_FIRST_NM").alias("PROV_FIRST_NM"),
    col("lnk_Xfrm_out.PROV_MIDINIT").alias("PROV_MIDINIT"),
    col("lnk_Xfrm_out.PROV_NM_SFX").alias("PROV_NM_SFX"),
    col("lnk_Xfrm_out.PROV_ADDR_LN_1").alias("PROV_ADDR_LN_1"),
    col("lnk_Xfrm_out.PROV_ADDR_LN_2").alias("PROV_ADDR_LN_2"),
    col("lnk_Xfrm_out.PROV_CITY_NM").alias("PROV_CITY_NM"),
    col("lnk_Xfrm_out.PROV_CNTY_CD").alias("PROV_CNTY_CD"),
    col("lnk_Xfrm_out.PROV_ST_CD").alias("PROV_ST_CD"),
    col("lnk_Xfrm_out.PROV_ZIP").alias("PROV_ZIP"),
    col("lnk_Xfrm_out.PROV_EMAIL_ADDR").alias("PROV_EMAIL_ADDR"),
    col("lnk_Xfrm_out.PROV_NTNL_PROV_ID").alias("PROV_NTNL_PROV_ID"),
    col("lnk_Xfrm_out.CSTM_PROV_TYP").alias("CSTM_PROV_TYP"),
    col("lnk_Xfrm_out.PCP_FLAG").alias("PCP_FLAG"),
    col("lnk_Xfrm_out.PRSCRB_PROV_FLAG").alias("PRSCRB_PROV_FLAG"),
    col("lnk_Xfrm_out.PROV_ORIG_PLN_CD").alias("PROV_ORIG_PLN_CD"),
    col("lnk_Xfrm_out.PROV_TAX_ID").alias("PROV_TAX_ID"),
    col("lnk_Xfrm_out.ALT_FLD_1").alias("ALT_FLD_1"),
    col("lnk_Xfrm_out.ALT_FLD_2").alias("ALT_FLD_2"),
    col("lnk_Xfrm_out.ALT_FLD_3").alias("ALT_FLD_3"),
    col("lnk_Xfrm_out.CSTM_FLD_1").alias("CSTM_FLD_1"),
    col("lnk_Xfrm_out.CSTM_FLD_2").alias("CSTM_FLD_2"),
    col("lnk_Xfrm_out.OFC_MGR_LAST_NM").alias("OFC_MGR_LAST_NM"),
    col("lnk_Xfrm_out.OFC_MGR_FIRST_NM").alias("OFC_MGR_FIRST_NM"),
    col("lnk_Xfrm_out.OFC_MGR_TTL").alias("OFC_MGR_TTL"),
    col("lnk_Xfrm_out.OFC_MGR_EMAIL").alias("OFC_MGR_EMAIL"),
    col("lnk_Xfrm_out.RUN_DT").alias("RUN_DT"),
    col("lnk_Xfrm_out.SRC_SYS").alias("SRC_SYS"),
    col("lnk_Xfrm_lkp_in.PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD")
)

# --------------------------------------------------------------------------------
# Xfrm_BusinessRules (CTransformerStage)
# --------------------------------------------------------------------------------
df_xfrm_brules_stagevars = df_lkp_Codes2_out.withColumn("svProvLocEffDt", lit("1753-01-01"))

df_xfrm_brules_out_1 = df_xfrm_brules_stagevars.select(
    concat(
        trim(col("PROV_ID")), lit(";"),
        trim(col("PROV_ADDR_TYP_CD")), lit(";"),
        lit("1753-01-01"), lit(";"),
        col("SrcSysCd")  # This references a parameter in DS expression. We'll replicate it by lit(SrcSysCd).
    ).alias("PRI_NAT_KEY_STRING"),
    lit("RunIDTimeStamp").alias("FIRST_RECYC_TS"),  # DS expression was just "RunIDTimeStamp"
    lit("0").alias("PROV_LOC_SK"),
    col("PROV_ID").alias("PROV_ID"),
    col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    col("PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD"),
    lit("1753-01-01").alias("PROV_ADDR_EFF_DT"),
    lit("SrcSysCd").alias("SRC_SYS_CD"),  # DS expression references parameter
    lit("0").alias("CRT_RUN_CYC_EXCTN_SK"),
    lit("0").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit("SrcSysCdSK").alias("SRC_SYS_CD_SK"),  # DS expression references parameter
    lit("1").alias("PROV_ADDR_SK"),
    lit("1").alias("PROV_SK"),
    lit("N").alias("PRI_ADDR_IN"),
    lit("N").alias("REMIT_ADDR_IN")
)

# The second output link from Xfrm_BusinessRules
df_xfrm_brules_out_2 = df_xfrm_brules_stagevars.select(
    lit("SrcSysCdSK").alias("SRC_SYS_CD_SK"),
    col("PROV_ID").alias("PROV_ID"),
    col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    when(
        (col("PROV_ADDR_TYP_CD").isNull()) | (trim(col("PROV_ADDR_TYP_CD")) == ""),
        lit("1")
    ).otherwise(col("PROV_ADDR_TYP_CD")).alias("PROV_ADDR_TYP_CD"),
    lit("1753-01-01").alias("EFF_DT_SK")
)

# --------------------------------------------------------------------------------
# ds_PROV_LOC_Xfm (PxDataSet) WRITE
# --------------------------------------------------------------------------------
# We must write df_xfrm_brules_out_1 in the same column order, applying rpad for char columns
# According to the final schema, columns are:
#  1 PRI_NAT_KEY_STRING (no char length)
#  2 FIRST_RECYC_TS (no char length)
#  3 PROV_LOC_SK (no char length)
#  4 PROV_ID (PK)
#  5 PROV_ADDR_ID (PK)
#  6 PROV_ADDR_TYP_CD (PK)
#  7 PROV_ADDR_EFF_DT (char, length=10)
#  8 SRC_SYS_CD (PK)
#  9 CRT_RUN_CYC_EXCTN_SK
# 10 LAST_UPDT_RUN_CYC_EXCTN_SK
# 11 SRC_SYS_CD_SK
# 12 PROV_ADDR_SK
# 13 PROV_SK
# 14 PRI_ADDR_IN (char, length=1)
# 15 REMIT_ADDR_IN (char, length=1)
df_PROV_LOC_Xfm_for_write = df_xfrm_brules_out_1.select(
    col("PRI_NAT_KEY_STRING"),
    col("FIRST_RECYC_TS"),
    col("PROV_LOC_SK"),
    col("PROV_ID"),
    col("PROV_ADDR_ID"),
    col("PROV_ADDR_TYP_CD"),
    rpad(col("PROV_ADDR_EFF_DT"), 10, " ").alias("PROV_ADDR_EFF_DT"),
    col("SRC_SYS_CD"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("SRC_SYS_CD_SK"),
    col("PROV_ADDR_SK"),
    col("PROV_SK"),
    rpad(col("PRI_ADDR_IN"), 1, " ").alias("PRI_ADDR_IN"),
    rpad(col("REMIT_ADDR_IN"), 1, " ").alias("REMIT_ADDR_IN")
)

write_files(
    df_PROV_LOC_Xfm_for_write,
    f"{adls_path}/ds/PROV_LOC.{SrcSysCd}.xfrm.{RunID}.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# --------------------------------------------------------------------------------
# Lookup_Fkey_Bal (PxLookup) -> merges df_xfrm_brules_out_2 (PrimaryLink) with df_fltr_FilterData_out (LookupLink, left join)
# --------------------------------------------------------------------------------
df_Lookup_Fkey_Bal = (
    df_xfrm_brules_out_2.alias("lnk_ToBalLkup")
    .join(
        df_fltr_FilterData_out.alias("lnkProvAddrTyp"),
        on=[
            col("lnk_ToBalLkup.PROV_ADDR_TYP_CD") == col("lnkProvAddrTyp.SRC_CD")
        ],
        how="left"
    )
)

df_Lookup_Fkey_Bal_out = df_Lookup_Fkey_Bal.select(
    col("lnk_ToBalLkup.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("lnk_ToBalLkup.PROV_ID").alias("PROV_ID"),
    col("lnk_ToBalLkup.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    col("lnkProvAddrTyp.CD_MPPNG_SK").alias("PROV_ADDR_TYP_CD_SK"),
    rpad(col("lnk_ToBalLkup.EFF_DT_SK"), 10, " ").alias("EFF_DT_SK")
)

# --------------------------------------------------------------------------------
# Xfrm_B_PROV_LOC (CTransformerStage)
# --------------------------------------------------------------------------------
# Direct pass-through with one small transformation on PROV_ADDR_TYP_CD_SK: if isnull then 0 else value
df_Xfrm_B_PROV_LOC_out = df_Lookup_Fkey_Bal_out.select(
    col("SRC_SYS_CD_SK"),
    col("PROV_ID"),
    col("PROV_ADDR_ID"),
    when(col("PROV_ADDR_TYP_CD_SK").isNull(), lit("0")).otherwise(col("PROV_ADDR_TYP_CD_SK")).alias("PROV_ADDR_TYP_CD_SK"),
    col("EFF_DT_SK")
)

# --------------------------------------------------------------------------------
# seq_B_PROV_LOC (PxSequentialFile) WRITE
# --------------------------------------------------------------------------------
df_seq_B_PROV_LOC_for_write = df_Xfrm_B_PROV_LOC_out.select(
    col("SRC_SYS_CD_SK"),
    col("PROV_ID"),
    col("PROV_ADDR_ID"),
    col("PROV_ADDR_TYP_CD_SK"),
    rpad(col("EFF_DT_SK"), 10, " ").alias("EFF_DT_SK")
)

write_files(
    df_seq_B_PROV_LOC_for_write,
    f"{adls_path}/load/B_PROV_LOC.{SrcSysCd}.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)