# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2005, 2006, 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Job Name :  EyeMedProvAddrXfrm
# MAGIC 
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC 
# MAGIC     Transformation rules applied on Extracted data information that is used to populate the IDS ProvAddr table 
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Goutham K              2020-09-23       US-261580              Original Programming.                                                        IntegrateDev2                 Kalyan Neelam            2020-10-26      
# MAGIC Goutham K              2020--02-10     US-345075              TRGT_DOMAIN_NM = 'PROVIDER ADDRESS TYPE'   IntegrateDev2                  Reddy Sanam              2021-02-10
# MAGIC                                                                                          added above statement in lookup SQL's       
# MAGIC Goutham K            2021-05-15         US-366403            New Provider file Change to include Loc and Svc loc id     IntegrateDev1                Jeyaprasanna              2021-05-24
# MAGIC 
# MAGIC Deepika C             2021-11-25         US 468740              Added new field ATND_TEXT as Null in                           IntegrateSITF                 Jeyaprasanna              2021-11-28
# MAGIC                                                                                          Xfrm_BusinessRules Transformer Stage

# MAGIC JobName: EyeMedProvAddrXfrm
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
from pyspark.sql.functions import when, col, lit, concat, substring, concat_ws, rpad
from pyspark.sql.functions import monotonically_increasing_id  # Needed for dedup_sort internal mechanics
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
RunIDTimeStamp = get_widget_value('RunIDTimeStamp','')
SrcSysCdSK = get_widget_value('SrcSysCdSK','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# db2_Prov_Addr_MaxId_Lkp (DB2ConnectorPX, input)
extract_query_db2_Prov_Addr_MaxId_Lkp = """SELECT
PA.PROV_ADDR_ID,
CAST(MAX(CAST(TRIM(COALESCE(AT.SRC_CD,'')) AS INTEGER)) + 1 AS VARCHAR(20)) AS PROV_ADDR_TYP_CD
FROM """ + f"{IDSOwner}" + """.PROV_ADDR AS PA
INNER JOIN """ + f"{IDSOwner}" + """.CD_MPPNG AS AT
 ON PA.PROV_ADDR_TYP_CD_SK = AT.CD_MPPNG_SK
INNER JOIN """ + f"{IDSOwner}" + """.CD_MPPNG AS SC
 ON PA.SRC_SYS_CD_SK = SC.CD_MPPNG_SK
 AND SC.SRC_CD = 'EYEMED'
and AT.TRGT_DOMAIN_NM = 'PROVIDER ADDRESS TYPE'
GROUP BY PA.PROV_ADDR_ID
"""
df_db2_Prov_Addr_MaxId_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_Prov_Addr_MaxId_Lkp)
    .load()
)

# ds_CD_MPPNG_Lkp_Data (PxDataSet, input -> converted to parquet)
df_ds_CD_MPPNG_Lkp_Data = spark.read.parquet(f"{adls_path}/ds/CD_MPPNG.parquet")

# fltr_FilterData (PxFilter)
df_fltr_temp = df_ds_CD_MPPNG_Lkp_Data.filter(
    (col("SRC_SYS_CD") == "FACETS")
    & (col("SRC_CLCTN_CD") == "FACETS DBO")
    & (col("TRGT_CLCTN_CD") == "IDS")
    & (col("SRC_DOMAIN_NM") == "PROVIDER ADDRESS TYPE")
    & (col("TRGT_DOMAIN_NM") == "PROVIDER ADDRESS TYPE")
)
df_fltr_FilterData = df_fltr_temp.select(
    col("SRC_CD").alias("SRC_CD"),
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

# ds_PROV_ADDR_Extr (PxDataSet, input -> converted to parquet)
df_ds_PROV_ADDR_Extr = spark.read.parquet(
    f"{adls_path}/ds/PROV_ADDR.{SrcSysCd}.extr.{RunID}.parquet"
)

# Xfrm_BusinessLogic (CTransformerStage)
df_Xfrm_BusinessLogic = (
    df_ds_PROV_ADDR_Extr
    .withColumn(
        "PROV_ID",
        trim(when(col("PROV_ID").isNotNull(), col("PROV_ID")).otherwise(""))  # type: ignore
    )
    .withColumn(
        "PROV_ADDR_ID",
        concat(
            trim(when(col("LOC_ID").isNotNull(), col("LOC_ID")).otherwise("")),  # type: ignore
            trim(when(col("LOC_SVC_ID").isNotNull(), col("LOC_SVC_ID")).otherwise(""))  # type: ignore
        )
    )
    .withColumn(
        "ADDR_LN_1",
        trim(when(col("PROV_ADDR").isNotNull(), col("PROV_ADDR")).otherwise(""))  # type: ignore
    )
    .withColumn(
        "ADDR_LN_2",
        trim(when(col("PROV_ADDR_2").isNotNull(), col("PROV_ADDR_2")).otherwise(""))  # type: ignore
    )
    .withColumn(
        "CITY_NM",
        trim(when(col("PROV_CITY").isNotNull(), col("PROV_CITY")).otherwise(""))  # type: ignore
    )
    .withColumn(
        "ST_CD",
        trim(when(col("PROV_ST").isNotNull(), col("PROV_ST")).otherwise(""))  # type: ignore
    )
    .withColumn(
        "POSTAL_CD",
        concat(
            trim(when(col("PROV_ZIP").isNotNull(), col("PROV_ZIP")).otherwise("")),  # type: ignore
            trim(when(col("PROV_ZIP_PLUS_4").isNotNull(), col("PROV_ZIP_PLUS_4")).otherwise(""))  # type: ignore
        )
    )
    .withColumn(
        "LOC_HCAP_ACES_IN",
        trim(when(col("LOC_HCAP_ACES_IN").isNotNull(), col("LOC_HCAP_ACES_IN")).otherwise(""))  # type: ignore
    )
    .withColumn(
        "LOC_PHN_NO",
        trim(when(col("LOC_PHN_NO").isNotNull(), col("LOC_PHN_NO")).otherwise(""))  # type: ignore
    )
    .withColumn(
        "LOC_FAX_NO",
        trim(when(col("LOC_FAX_NO").isNotNull(), col("LOC_FAX_NO")).otherwise(""))  # type: ignore
    )
    .withColumn(
        "LOC_EMAIL",
        trim(when(col("LOC_EMAIL").isNotNull(), col("LOC_EMAIL")).otherwise(""))  # type: ignore
    )
    .withColumn(
        "LOC_SVC_ID",
        when(
            substring(
                trim(when(col("LOC_SVC_ID").isNotNull(), col("LOC_SVC_ID")).otherwise("")),  # type: ignore
                1,
                1
            ) == "0",
            substring(
                trim(when(col("LOC_SVC_ID").isNotNull(), col("LOC_SVC_ID")).otherwise("")),  # type: ignore
                2,
                1
            )
        ).otherwise(
            trim(when(col("LOC_SVC_ID").isNotNull(), col("LOC_SVC_ID")).otherwise(""))  # type: ignore
        )
    )
    .withColumn("CNTY", col("CNTY"))
)

# db2_Prov_AddrId_Lkp (DB2ConnectorPX, input)
extract_query_db2_Prov_AddrId_Lkp = """SELECT
PA.PROV_ADDR_ID,
TRIM(COALESCE(PA.ADDR_LN_1,'')) AS ADDR_LN_1,
TRIM(COALESCE(PA.ADDR_LN_2,'')) AS ADDR_LN_2,
TRIM(COALESCE(PA.CITY_NM,'')) AS CITY_NM,
TRIM(COALESCE(ST.SRC_CD,'')) AS ST_CD,
TRIM(COALESCE(PA.POSTAL_CD,'')) AS POSTAL_CD,
TRIM(COALESCE(AT.SRC_CD,'')) AS PROV_ADDR_TYP_CD,
TRIM(COALESCE(AT.TRGT_CD,'')) AS TRGT_CD
FROM """ + f"{IDSOwner}" + """.PROV_ADDR AS PA
INNER JOIN """ + f"{IDSOwner}" + """.CD_MPPNG AS ST
 ON PA.PROV_ADDR_ST_CD_SK = ST.CD_MPPNG_SK
INNER JOIN """ + f"{IDSOwner}" + """.CD_MPPNG AS AT
 ON PA.PROV_ADDR_TYP_CD_SK = AT.CD_MPPNG_SK
AND AT.TRGT_DOMAIN_NM = 'PROVIDER ADDRESS TYPE'
INNER JOIN """ + f"{IDSOwner}" + """.CD_MPPNG AS SC
 ON PA.SRC_SYS_CD_SK = SC.CD_MPPNG_SK
 AND SC.SRC_CD = 'EYEMED'
"""
df_db2_Prov_AddrId_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_Prov_AddrId_Lkp)
    .load()
)

# Cp_Prov_Addr (PxCopy)
df_Cp_Prov_Addr_lnk_Addr_trgt = df_db2_Prov_AddrId_Lkp.select(
    col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    col("TRGT_CD").alias("TRGT_CD")
)
df_Cp_Prov_Addr_lnk_lkp_Prov_AddrId = df_db2_Prov_AddrId_Lkp.select(
    col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    col("ADDR_LN_1").alias("ADDR_LN_1"),
    col("ADDR_LN_2").alias("ADDR_LN_2"),
    col("CITY_NM").alias("CITY_NM"),
    col("ST_CD").alias("ST_CD"),
    col("POSTAL_CD").alias("POSTAL_CD"),
    col("PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD")
)

# RmDup_GetMinTRGT_CD (PxRemDup)
df_RmDup_GetMinTRGT_CD = dedup_sort(
    df_Cp_Prov_Addr_lnk_Addr_trgt,
    partition_cols=["PROV_ADDR_ID"],
    sort_cols=[("TRGT_CD", "A")]
)

# Lkp_Prov_Addr (PxLookup)
df_Lkp_Prov_Addr = (
    df_Xfrm_BusinessLogic.alias("lnk_Xfrm_out")
    .join(
        df_Cp_Prov_Addr_lnk_lkp_Prov_AddrId.alias("lnk_lkp_Prov_AddrId"),
        [
            col("lnk_Xfrm_out.PROV_ADDR_ID") == col("lnk_lkp_Prov_AddrId.PROV_ADDR_ID"),
            col("lnk_Xfrm_out.ADDR_LN_1") == col("lnk_lkp_Prov_AddrId.ADDR_LN_1"),
            col("lnk_Xfrm_out.ADDR_LN_2") == col("lnk_lkp_Prov_AddrId.ADDR_LN_2"),
            col("lnk_Xfrm_out.CITY_NM") == col("lnk_lkp_Prov_AddrId.CITY_NM"),
            col("lnk_Xfrm_out.ST_CD") == col("lnk_lkp_Prov_AddrId.ST_CD"),
            col("lnk_Xfrm_out.POSTAL_CD") == col("lnk_lkp_Prov_AddrId.POSTAL_CD"),
        ],
        how="left"
    )
    .join(
        df_RmDup_GetMinTRGT_CD.alias("Lnk_MINtrgt_cd"),
        [
            col("lnk_Xfrm_out.PROV_ADDR_ID") == col("Lnk_MINtrgt_cd.PROV_ADDR_ID")
        ],
        how="left"
    )
    .select(
        col("lnk_Xfrm_out.PROV_ID").alias("PROV_ID"),
        col("lnk_Xfrm_out.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
        col("lnk_Xfrm_out.ADDR_LN_1").alias("ADDR_LN_1"),
        col("lnk_Xfrm_out.ADDR_LN_2").alias("ADDR_LN_2"),
        col("lnk_Xfrm_out.CITY_NM").alias("CITY_NM"),
        col("lnk_Xfrm_out.ST_CD").alias("ST_CD"),
        col("lnk_Xfrm_out.POSTAL_CD").alias("POSTAL_CD"),
        col("lnk_Xfrm_out.LOC_HCAP_ACES_IN").alias("LOC_HCAP_ACES_IN"),
        col("lnk_Xfrm_out.LOC_PHN_NO").alias("LOC_PHN_NO"),
        col("lnk_Xfrm_out.LOC_FAX_NO").alias("LOC_FAX_NO"),
        col("lnk_Xfrm_out.LOC_EMAIL").alias("LOC_EMAIL"),
        col("lnk_Xfrm_out.LOC_SVC_ID").alias("LOC_SVC_ID"),
        col("lnk_lkp_Prov_AddrId.PROV_ADDR_ID").alias("PROV_ADDR_ID_1"),
        col("lnk_lkp_Prov_AddrId.PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD"),
        col("Lnk_MINtrgt_cd.TRGT_CD").alias("TRGT_CD"),
        col("lnk_Xfrm_out.CNTY").alias("CNTY")
    )
)

# Lkp_Max_AddrId (PxLookup)
df_Lkp_Max_AddrId = (
    df_Lkp_Prov_Addr.alias("DSLink185")
    .join(
        df_db2_Prov_Addr_MaxId_Lkp.alias("lnk_lkp_Prov_Addr_MaxId"),
        [
            col("DSLink185.PROV_ADDR_ID") == col("lnk_lkp_Prov_Addr_MaxId.PROV_ADDR_ID")
        ],
        how="left"
    )
    .select(
        col("DSLink185.PROV_ID").alias("PROV_ID"),
        col("DSLink185.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
        col("DSLink185.ADDR_LN_1").alias("ADDR_LN_1"),
        col("DSLink185.ADDR_LN_2").alias("ADDR_LN_2"),
        col("DSLink185.CITY_NM").alias("CITY_NM"),
        col("DSLink185.ST_CD").alias("ST_CD"),
        col("DSLink185.POSTAL_CD").alias("POSTAL_CD"),
        col("DSLink185.LOC_HCAP_ACES_IN").alias("LOC_HCAP_ACES_IN"),
        col("DSLink185.LOC_PHN_NO").alias("LOC_PHN_NO"),
        col("DSLink185.LOC_FAX_NO").alias("LOC_FAX_NO"),
        col("DSLink185.LOC_EMAIL").alias("LOC_EMAIL"),
        col("DSLink185.LOC_SVC_ID").alias("LOC_SVC_ID"),
        col("DSLink185.PROV_ADDR_ID_1").alias("PROV_ADDR_ID_1"),
        col("DSLink185.PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD"),
        col("DSLink185.TRGT_CD").alias("TRGT_CD"),
        col("DSLink185.CNTY").alias("CNTY"),
        col("lnk_lkp_Prov_Addr_MaxId.PROV_ADDR_ID").alias("PROV_ADDR_ID_2"),
        col("lnk_lkp_Prov_Addr_MaxId.PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD_2")
    )
)

# Xfrm_BusinessRules (CTransformerStage)
df_Xfrm_BusinessRules_base = (
    df_Lkp_Max_AddrId
    .withColumn("svProvEffDt", lit("1753-01-01"))
    .withColumn("svProvAddrTypCd", col("LOC_SVC_ID"))
)

df_Xfrm_BusinessRules_lnk_ProvAddrXfrm_Out = (
    df_Xfrm_BusinessRules_base
    .withColumn(
        "PRI_NAT_KEY_STRING",
        concat_ws(";", lit(SrcSysCd), col("PROV_ADDR_ID"), col("LOC_SVC_ID"), col("svProvEffDt"))
    )
    .withColumn("FIRST_RECYC_TS", lit(RunIDTimeStamp))
    .withColumn("PROV_ADDR_SK", lit(0))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(0))
    .withColumn(
        "SRC_SYS_CD",
        lit(SrcSysCd)
    )
    .withColumn(
        "SRC_SYS_CD_SK",
        lit(SrcSysCdSK)
    )
    .withColumn(
        "PROV_ADDR_CNTY_CLS_CD",
        lit("NA")
    )
    .withColumn(
        "PROV_ADDR_GEO_ACES_RTRN_CD",
        lit("NA")
    )
    .withColumn(
        "PROV_ADDR_METRORURAL_COV_CD",
        lit("NA")
    )
    .withColumn(
        "PROV_ADDR_TERM_RSN_CD",
        lit("NA")
    )
    .withColumn(
        "HCAP_IN",
        when(col("LOC_HCAP_ACES_IN") == "", lit("N")).otherwise(col("LOC_HCAP_ACES_IN"))
    )
    .withColumn("PDX_24_HR_IN", lit("N"))
    .withColumn("PRCTC_LOC_IN", lit("Y"))
    .withColumn("PROV_ADDR_DIR_IN", lit("Y"))
    .withColumn("TERM_DT", lit("2199-12-31"))
    .withColumn("ADDR_LN_3", lit(None))
    .withColumn("PHN_NO", when(col("LOC_PHN_NO") == "", lit(None)).otherwise(col("LOC_PHN_NO")))
    .withColumn("PHN_NO_EXT", lit(None))
    .withColumn("FAX_NO", when(col("LOC_FAX_NO") == "", lit(None)).otherwise(col("LOC_FAX_NO")))
    .withColumn("FAX_NO_EXT", lit(None))
    .withColumn("EMAIL_ADDR_TX", when(col("LOC_EMAIL") == "", lit(None)).otherwise(col("LOC_EMAIL")))
    .withColumn("LAT_TX", lit(0))
    .withColumn("LONG_TX", lit(0))
    .withColumn("PRAD_TYPE_MAIL", col("svProvAddrTypCd"))
    .withColumn("PROV2_PRAD_EFF_DT", col("svProvEffDt"))
    .withColumn("PROV_ADDR_TYP_CD_ORIG", col("svProvAddrTypCd"))
    .select(
        "PRI_NAT_KEY_STRING",
        "FIRST_RECYC_TS",
        "PROV_ADDR_SK",
        "PROV_ADDR_ID",
        "LOC_SVC_ID",
        "svProvEffDt",
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "SRC_SYS_CD_SK",
        "PROV_ADDR_CNTY_CLS_CD",
        "PROV_ADDR_GEO_ACES_RTRN_CD",
        "PROV_ADDR_METRORURAL_COV_CD",
        "PROV_ADDR_TERM_RSN_CD",
        "HCAP_IN",
        "PDX_24_HR_IN",
        "PRCTC_LOC_IN",
        "PROV_ADDR_DIR_IN",
        "TERM_DT",
        "ADDR_LN_1",
        "ADDR_LN_2",
        "ADDR_LN_3",
        "CITY_NM",
        "ST_CD",
        "POSTAL_CD",
        "CNTY",
        "PHN_NO",
        "PHN_NO_EXT",
        "FAX_NO",
        "FAX_NO_EXT",
        "EMAIL_ADDR_TX",
        "LAT_TX",
        "LONG_TX",
        "PRAD_TYPE_MAIL",
        "PROV2_PRAD_EFF_DT",
        "PROV_ADDR_TYP_CD_ORIG"
    )
    .withColumnRenamed("LOC_SVC_ID", "PROV_ADDR_TYP_CD")
    .withColumnRenamed("svProvEffDt", "PROV_ADDR_EFF_DT")
)

df_Xfrm_BusinessRules_lnk_ToBalLkup = (
    df_Xfrm_BusinessRules_base
    .select(
        col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
        col("svProvAddrTypCd").alias("PROV_ADDR_TYP_CD"),
        col("svProvEffDt").alias("EFF_DT_SK")
    )
)

df_Xfrm_BusinessRules_lnk_ProvLocXfrm_Out = (
    df_Xfrm_BusinessRules_base
    .withColumn(
        "PRI_NAT_KEY_STRING",
        concat_ws(";", lit(SrcSysCd), col("PROV_ADDR_ID"), col("LOC_SVC_ID"), col("svProvEffDt"))
    )
    .withColumn("FIRST_RECYC_TS", lit(RunIDTimeStamp))
    .withColumn("PROV_ADDR_SK", lit(0))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(0))
    .withColumn(
        "SRC_SYS_CD",
        lit(SrcSysCd)
    )
    .withColumn(
        "SRC_SYS_CD_SK",
        lit(SrcSysCdSK)
    )
    .withColumn(
        "PROV_ADDR_CNTY_CLS_CD",
        lit("NA")
    )
    .withColumn(
        "PROV_ADDR_GEO_ACES_RTRN_CD",
        lit("NA")
    )
    .withColumn(
        "PROV_ADDR_METRORURAL_COV_CD",
        lit("NA")
    )
    .withColumn(
        "PROV_ADDR_TERM_RSN_CD",
        lit("NA")
    )
    .withColumn(
        "HCAP_IN",
        when(col("LOC_HCAP_ACES_IN") == "", lit("N")).otherwise(col("LOC_HCAP_ACES_IN"))
    )
    .withColumn("PDX_24_HR_IN", lit("N"))
    .withColumn("PRCTC_LOC_IN", lit("N"))
    .withColumn("PROV_ADDR_DIR_IN", lit("N"))
    .withColumn("TERM_DT", lit("2199-12-31"))
    .withColumn("ADDR_LN_3", lit(None))
    .withColumn("PHN_NO", when(col("LOC_PHN_NO") == "", lit(None)).otherwise(col("LOC_PHN_NO")))
    .withColumn("PHN_NO_EXT", lit(None))
    .withColumn("FAX_NO", when(col("LOC_FAX_NO") == "", lit(None)).otherwise(col("LOC_FAX_NO")))
    .withColumn("FAX_NO_EXT", lit(None))
    .withColumn("EMAIL_ADDR_TX", when(col("LOC_EMAIL") == "", lit(None)).otherwise(col("LOC_EMAIL")))
    .withColumn("LAT_TX", lit(0))
    .withColumn("LONG_TX", lit(0))
    .withColumn("PRAD_TYPE_MAIL", col("svProvAddrTypCd"))
    .withColumn("PROV2_PRAD_EFF_DT", col("svProvEffDt"))
    .withColumn("PROV_ADDR_TYP_CD_ORIG", col("svProvAddrTypCd"))
    .select(
        "PRI_NAT_KEY_STRING",
        "FIRST_RECYC_TS",
        "PROV_ADDR_SK",
        "PROV_ID",
        "PROV_ADDR_ID",
        "LOC_SVC_ID",
        "svProvEffDt",
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "SRC_SYS_CD_SK",
        "PROV_ADDR_CNTY_CLS_CD",
        "PROV_ADDR_GEO_ACES_RTRN_CD",
        "PROV_ADDR_METRORURAL_COV_CD",
        "PROV_ADDR_TERM_RSN_CD",
        "HCAP_IN",
        "PDX_24_HR_IN",
        "PRCTC_LOC_IN",
        "PROV_ADDR_DIR_IN",
        "TERM_DT",
        "ADDR_LN_1",
        "ADDR_LN_2",
        "ADDR_LN_3",
        "CITY_NM",
        "ST_CD",
        "POSTAL_CD",
        "PHN_NO",
        "PHN_NO_EXT",
        "FAX_NO",
        "FAX_NO_EXT",
        "EMAIL_ADDR_TX",
        "LAT_TX",
        "LONG_TX",
        "PRAD_TYPE_MAIL",
        "PROV2_PRAD_EFF_DT",
        "PROV_ADDR_TYP_CD_ORIG"
    )
    .withColumnRenamed("LOC_SVC_ID", "PROV_ADDR_TYP_CD")
    .withColumnRenamed("svProvEffDt", "PROV_ADDR_EFF_DT")
)

# ds_PROV_ADDR_Xfm (PxDataSet, output -> parquet)
# Apply rpad for char/varchar columns, preserving final column order
df_PROV_ADDR_Xfm = (
    df_Xfrm_BusinessRules_lnk_ProvAddrXfrm_Out
    .withColumn("PROV_ADDR_EFF_DT", rpad(col("PROV_ADDR_EFF_DT"), 10, " "))
    .withColumn("HCAP_IN", rpad(col("HCAP_IN"), 1, " "))
    .withColumn("PDX_24_HR_IN", rpad(col("PDX_24_HR_IN"), 1, " "))
    .withColumn("PRCTC_LOC_IN", rpad(col("PRCTC_LOC_IN"), 1, " "))
    .withColumn("PROV_ADDR_DIR_IN", rpad(col("PROV_ADDR_DIR_IN"), 1, " "))
    .withColumn("TERM_DT", rpad(col("TERM_DT"), 10, " "))
    .withColumn("POSTAL_CD", rpad(col("POSTAL_CD"), 11, " "))
    .select(
        "PRI_NAT_KEY_STRING",
        "FIRST_RECYC_TS",
        "PROV_ADDR_SK",
        "PROV_ADDR_ID",
        "PROV_ADDR_TYP_CD",
        "PROV_ADDR_EFF_DT",
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "SRC_SYS_CD_SK",
        "PROV_ADDR_CNTY_CLS_CD",
        "PROV_ADDR_GEO_ACES_RTRN_CD",
        "PROV_ADDR_METRORURAL_COV_CD",
        "PROV_ADDR_TERM_RSN_CD",
        "HCAP_IN",
        "PDX_24_HR_IN",
        "PRCTC_LOC_IN",
        "PROV_ADDR_DIR_IN",
        "TERM_DT",
        "ADDR_LN_1",
        "ADDR_LN_2",
        "ADDR_LN_3",
        "CITY_NM",
        "ST_CD",
        "POSTAL_CD",
        "CNTY",
        "PHN_NO",
        "PHN_NO_EXT",
        "FAX_NO",
        "FAX_NO_EXT",
        "EMAIL_ADDR_TX",
        "LAT_TX",
        "LONG_TX",
        "PRAD_TYPE_MAIL",
        "PROV2_PRAD_EFF_DT",
        "PROV_ADDR_TYP_CD_ORIG"
    )
)
write_files(
    df_PROV_ADDR_Xfm,
    f"{adls_path}/ds/PROV_ADDR.{SrcSysCd}.xfrm.{RunID}.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

# ds_PROV_LOC_xfrm (PxDataSet, output -> parquet)
df_PROV_LOC_xfrm = (
    df_Xfrm_BusinessRules_lnk_ProvLocXfrm_Out
    .withColumn("PROV_ADDR_EFF_DT", rpad(col("PROV_ADDR_EFF_DT"), 10, " "))
    .withColumn("HCAP_IN", rpad(col("HCAP_IN"), 1, " "))
    .withColumn("PDX_24_HR_IN", rpad(col("PDX_24_HR_IN"), 1, " "))
    .withColumn("PRCTC_LOC_IN", rpad(col("PRCTC_LOC_IN"), 1, " "))
    .withColumn("PROV_ADDR_DIR_IN", rpad(col("PROV_ADDR_DIR_IN"), 1, " "))
    .withColumn("TERM_DT", rpad(col("TERM_DT"), 10, " "))
    .withColumn("POSTAL_CD", rpad(col("POSTAL_CD"), 11, " "))
    .select(
        "PRI_NAT_KEY_STRING",
        "FIRST_RECYC_TS",
        "PROV_ADDR_SK",
        "PROV_ID",
        "PROV_ADDR_ID",
        "PROV_ADDR_TYP_CD",
        "PROV_ADDR_EFF_DT",
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "SRC_SYS_CD_SK",
        "PROV_ADDR_CNTY_CLS_CD",
        "PROV_ADDR_GEO_ACES_RTRN_CD",
        "PROV_ADDR_METRORURAL_COV_CD",
        "PROV_ADDR_TERM_RSN_CD",
        "HCAP_IN",
        "PDX_24_HR_IN",
        "PRCTC_LOC_IN",
        "PROV_ADDR_DIR_IN",
        "TERM_DT",
        "ADDR_LN_1",
        "ADDR_LN_2",
        "ADDR_LN_3",
        "CITY_NM",
        "ST_CD",
        "POSTAL_CD",
        "PHN_NO",
        "PHN_NO_EXT",
        "FAX_NO",
        "FAX_NO_EXT",
        "EMAIL_ADDR_TX",
        "LAT_TX",
        "LONG_TX",
        "PRAD_TYPE_MAIL",
        "PROV2_PRAD_EFF_DT",
        "PROV_ADDR_TYP_CD_ORIG"
    )
)
write_files(
    df_PROV_LOC_xfrm,
    f"{adls_path}/ds/PROV_ADDRLOC.{SrcSysCd}.xfrm.{RunID}.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

# Lookup_Fkey_Bal (PxLookup)
df_Lookup_Fkey_Bal = (
    df_Xfrm_BusinessRules_lnk_ToBalLkup.alias("lnk_ToBalLkup")
    .join(
        df_fltr_FilterData.alias("lnkProvAddrTyp"),
        [
            col("lnk_ToBalLkup.PROV_ADDR_TYP_CD") == col("lnkProvAddrTyp.SRC_CD")
        ],
        how="left"
    )
    .select(
        col("lnk_ToBalLkup.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        col("lnk_ToBalLkup.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
        col("lnkProvAddrTyp.CD_MPPNG_SK").alias("PROV_ADDR_TYP_CD_SK"),
        col("lnk_ToBalLkup.EFF_DT_SK").alias("EFF_DT_SK")
    )
)

# Xfrm_B_PROV_ADDR (CTransformerStage)
df_Xfrm_B_PROV_ADDR = df_Lookup_Fkey_Bal.select(
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    col("PROV_ADDR_TYP_CD_SK").alias("PROV_ADDR_TYP_CD_SK"),
    col("EFF_DT_SK").alias("PROV_ADDR_EFF_DT_SK")
)

# seq_B_PROV_ADDR (PxSequentialFile, output -> .dat)
# Apply rpad if SqlType=char
df_seq_B_PROV_ADDR = (
    df_Xfrm_B_PROV_ADDR
    .withColumn("PROV_ADDR_EFF_DT_SK", rpad(col("PROV_ADDR_EFF_DT_SK"), 10, " "))
    .select(
        "SRC_SYS_CD_SK",
        "PROV_ADDR_ID",
        "PROV_ADDR_TYP_CD_SK",
        "PROV_ADDR_EFF_DT_SK"
    )
)
write_files(
    df_seq_B_PROV_ADDR,
    f"{adls_path}/load/B_PROV_ADDR.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)