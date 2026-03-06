# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2021 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Job Name :  MAInboundIdsProvAddrXfrm
# MAGIC Calling Job:  MAInboundProvExtrSeq
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION: Control job to process MA Inbound Encounter Claims into IDS
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer           	 Date                	User Story #        	Change Description                                               Project                    		Reviewed By                           Reviewed Date
# MAGIC -------------------------      ---------------------   	----------------   	--------------------------------------------------------             -----------------------------------------     	-------------------------  		-------------------
# MAGIC Lokesh K                 2021-11-29               US 404552                Initial programming                                     IntegrateDev2                                     Jeyaprasanna                          2022-02-05
# MAGIC 
# MAGIC Arpitha                    2024-02-02                US 599810               Update PROV_ADDR_DIR_IN to 'N'        IntegrateDev1                                      Jeyaprasanna                         2024-02-02
# MAGIC                                                                                                  in Xfrm_BusinessRules
# MAGIC                                                                                                   Updated logic to not create duplicate 
# MAGIC                                                                                                    ADDR_LN as part of Dominion Implementation

# MAGIC Data is sorted so that multiple entries for the for the same ADDR_ID will be in the same partition.
# MAGIC If the incoming address is a new address a sequnetial number is assigned in this transformer.
# MAGIC Assigning Mailing address as the value corresponding to Address Type Code = 1.  If the address assigned a value of type code -1 does not come in the file take from table. If the address coming in is a new address then take it from the file which has a ADR TYPE CODE assigned value of 1
# MAGIC JobName: MA InboundIdsProvAddrXfrm
# MAGIC Transformed Data will land into a Dataset for Primary Keying job.
# MAGIC 
# MAGIC Data is partitioned on Natural Key Columns. Additional attribute Primary_address_indicator is for location only. Since the Prov Addr Pkey is a common job, when reading the dataset, the Primary address indiactor will not be read. This will avoid creating an additional dataset just for location only.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Parameter retrieval
SrcSysCd = get_widget_value('SrcSysCd','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunID = get_widget_value('RunID','')
RunIDTimeStamp = get_widget_value('RunIDTimeStamp','')
SrcSysCdSK = get_widget_value('SrcSysCdSK','')

# --------------------------------------------------------------------------------
# db2_Prov_AddrId_Lkp (DB2ConnectorPX) - Read from IDS database
# --------------------------------------------------------------------------------
jdbc_url_db2_Prov_AddrId_Lkp, jdbc_props_db2_Prov_AddrId_Lkp = get_db_config(ids_secret_name)
extract_query_db2_Prov_AddrId_Lkp = f"""
SELECT
  PA.PROV_ADDR_ID,
  TRIM(COALESCE(AT.SRC_CD,'')) AS PROV_ADDR_TYP_CD,
  TRIM(COALESCE(PROV_ADDR_EFF_DT_SK,'')) AS PROV_ADDR_EFF_DT_SK,
  SC.SRC_CD
FROM {IDSOwner}.PROV_ADDR AS PA
INNER JOIN {IDSOwner}.CD_MPPNG AS ST
    ON PA.PROV_ADDR_ST_CD_SK = ST.CD_MPPNG_SK
INNER JOIN {IDSOwner}.CD_MPPNG AS AT
    ON PA.PROV_ADDR_TYP_CD_SK = AT.CD_MPPNG_SK
INNER JOIN {IDSOwner}.CD_MPPNG AS SC
    ON PA.SRC_SYS_CD_SK = SC.CD_MPPNG_SK
    AND SC.SRC_CD = '{SrcSysCd}'
"""
df_db2_Prov_AddrId_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_Prov_AddrId_Lkp)
    .options(**jdbc_props_db2_Prov_AddrId_Lkp)
    .option("query", extract_query_db2_Prov_AddrId_Lkp)
    .load()
)

# --------------------------------------------------------------------------------
# FilterData (PxFilter) - Splits into two outputs: Mail_AddrTyp and Ref_lkp_Prov_AddrId
# --------------------------------------------------------------------------------
df_Mail_AddrTyp_pre = df_db2_Prov_AddrId_Lkp.filter(F.col("SRC_CD") == SrcSysCd)
df_Ref_lkp_Prov_AddrId_pre = df_db2_Prov_AddrId_Lkp.filter(F.col("PROV_ADDR_TYP_CD") == "1")

# Produce the exact columns for Mail_AddrTyp
df_Mail_AddrTyp = df_Mail_AddrTyp_pre.select(
    F.col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.col("PROV_ADDR_EFF_DT_SK").alias("PROV_ADDR_EFF_DT_SK_MAIL")
)

# Produce the exact columns for Ref_lkp_Prov_AddrId
df_Ref_lkp_Prov_AddrId = df_Ref_lkp_Prov_AddrId_pre.select(
    F.col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.col("PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD"),
    F.col("PROV_ADDR_EFF_DT_SK").alias("PROV_ADDR_EFF_DT_SK")
)

# --------------------------------------------------------------------------------
# ds_CD_MPPNG_Lkp_Data (PxDataSet) - Read from parquet (CD_MPPNG.ds => CD_MPPNG.parquet)
# --------------------------------------------------------------------------------
df_ds_CD_MPPNG_Lkp_Data = spark.read.parquet(f"{adls_path}/ds/CD_MPPNG.parquet")

# --------------------------------------------------------------------------------
# fltr_FilterData (PxFilter) - Filter for:
# SRC_SYS_CD='FACETS' AND SRC_CLCTN_CD='FACETS DBO' AND TRGT_CLCTN_CD='IDS'
# AND SRC_DOMAIN_NM='PROVIDER ADDRESS TYPE' AND TRGT_DOMAIN_NM='PROVIDER ADDRESS TYPE'
# --------------------------------------------------------------------------------
df_lnkProvAddrTyp_pre = df_ds_CD_MPPNG_Lkp_Data.filter(
    (F.col("SRC_SYS_CD") == "FACETS") &
    (F.col("SRC_CLCTN_CD") == "FACETS DBO") &
    (F.col("TRGT_CLCTN_CD") == "IDS") &
    (F.col("SRC_DOMAIN_NM") == "PROVIDER ADDRESS TYPE") &
    (F.col("TRGT_DOMAIN_NM") == "PROVIDER ADDRESS TYPE")
)
df_lnkProvAddrTyp = df_lnkProvAddrTyp_pre.select(
    F.col("SRC_CD").alias("SRC_CD"),
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

# --------------------------------------------------------------------------------
# db2_Prov_Addr_MaxId_Lkp (DB2ConnectorPX) - Read from IDS database
# --------------------------------------------------------------------------------
jdbc_url_db2_Prov_Addr_MaxId_Lkp, jdbc_props_db2_Prov_Addr_MaxId_Lkp = get_db_config(ids_secret_name)
extract_query_db2_Prov_Addr_MaxId_Lkp = f"""
SELECT
  PA.PROV_ADDR_ID,
  CAST(MAX(CAST(TRIM(COALESCE(AT.SRC_CD,'')) AS INTEGER)) + 1 AS VARCHAR(20)) AS PROV_ADDR_TYP_CD
FROM {IDSOwner}.PROV_ADDR AS PA
INNER JOIN {IDSOwner}.CD_MPPNG AS AT
  ON PA.PROV_ADDR_TYP_CD_SK = AT.CD_MPPNG_SK
INNER JOIN {IDSOwner}.CD_MPPNG AS SC
  ON PA.SRC_SYS_CD_SK = SC.CD_MPPNG_SK
  AND SC.SRC_CD = '{SrcSysCd}'
GROUP BY PA.PROV_ADDR_ID
"""
df_db2_Prov_Addr_MaxId_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_Prov_Addr_MaxId_Lkp)
    .options(**jdbc_props_db2_Prov_Addr_MaxId_Lkp)
    .option("query", extract_query_db2_Prov_Addr_MaxId_Lkp)
    .load()
)

# --------------------------------------------------------------------------------
# ds_PROV_ADDR_DentaQuest_MA (PxDataSet) - Read from parquet
# PROV_ADDR.#SrcSysCd#.extr.#RunID#.ds => PROV_ADDR.{SrcSysCd}.extr.{RunID}.parquet
# --------------------------------------------------------------------------------
df_ds_PROV_ADDR_DentaQuest_MA = spark.read.parquet(
    f"{adls_path}/ds/PROV_ADDR.{SrcSysCd}.extr.{RunID}.parquet"
)

# --------------------------------------------------------------------------------
# Xfrm_BusinessLogic (CTransformerStage)
# --------------------------------------------------------------------------------
df_Xfrm_BusinessLogic_cols = (
    df_ds_PROV_ADDR_DentaQuest_MA
    .withColumn("PROV_ADDR_ID", F.col("PROVIDER_ID"))
    .withColumn(
        "ADDR_LN_1",
        F.when(F.col("PROVIDER_ADDR_1").isNotNull(), F.upper(F.col("PROVIDER_ADDR_1"))).otherwise(F.lit("")) 
    )
    .withColumn(
        "ADDR_LN_2",
        F.when(F.col("PROVIDER_ADDR_2").isNotNull(), F.upper(F.col("PROVIDER_ADDR_2"))).otherwise(F.lit(""))
    )
    .withColumn(
        "CITY_NM",
        F.when(F.col("PROVIDER_CITY").isNotNull(), F.upper(F.col("PROVIDER_CITY"))).otherwise(F.lit(""))
    )
    .withColumn(
        "ST_CD",
        F.upper(F.col("PROVIDER_STATE_CD"))
    )
    .withColumn(
        "POSTAL_CD",
        trim(
            F.when(F.col("PROVIDER_ZIP_CD_5").isNotNull(), F.col("PROVIDER_ZIP_CD_5")).otherwise(F.lit(""))
        ).substr(F.lit(1), F.lit(5))
    )
    .withColumn("County", F.lit(""))
    .withColumn("Phone_Number", F.lit(""))
    .withColumn("Fax_Number", F.lit(""))
    .withColumn("Effective_Date", F.lit("1753-01-01"))
    .withColumn("Term_Date", F.lit("2199-12-31"))
    .withColumn("Primary_Location_Indicator", F.lit("NA"))
)

df_Xfrm_BusinessLogic = df_Xfrm_BusinessLogic_cols.select(
    "PROV_ADDR_ID",
    "ADDR_LN_1",
    "ADDR_LN_2",
    "CITY_NM",
    "ST_CD",
    "POSTAL_CD",
    "County",
    "Phone_Number",
    "Fax_Number",
    "Effective_Date",
    "Term_Date",
    "Primary_Location_Indicator"
)

# --------------------------------------------------------------------------------
# Lkp_ADRID (PxLookup): left join with df_Ref_lkp_Prov_AddrId
# Conditions: (l.PROV_ADDR_ID == r.PROV_ADDR_ID) & (l.Effective_Date == r.PROV_ADDR_EFF_DT_SK)
# --------------------------------------------------------------------------------
df_Lkp_ADRID_join = (
    df_Xfrm_BusinessLogic.alias("l")
    .join(
        df_Ref_lkp_Prov_AddrId.alias("r"),
        (F.col("l.PROV_ADDR_ID") == F.col("r.PROV_ADDR_ID")) & 
        (F.col("l.Effective_Date") == F.col("r.PROV_ADDR_EFF_DT_SK")),
        "left"
    )
)
df_Lkp_ADRID = df_Lkp_ADRID_join.select(
    F.col("l.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.col("l.ADDR_LN_1").alias("ADDR_LN_1"),
    F.col("l.ADDR_LN_2").alias("ADDR_LN_2"),
    F.col("l.CITY_NM").alias("CITY_NM"),
    F.col("l.ST_CD").alias("ST_CD"),
    F.col("l.POSTAL_CD").alias("POSTAL_CD"),
    F.col("l.County").alias("County"),
    F.col("l.Phone_Number").alias("Phone_Number"),
    F.col("l.Fax_Number").alias("Fax_Number"),
    F.col("l.Effective_Date").alias("Effective_Date"),
    F.col("l.Term_Date").alias("Term_Date"),
    F.col("l.Primary_Location_Indicator").alias("Primary_Location_Indicator"),
    F.col("r.PROV_ADDR_ID").alias("PROV_ADDR_ID_1"),
    F.col("r.PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD")
)

# --------------------------------------------------------------------------------
# Att_Addr_Typ_Cd (PxLookup): left join with df_db2_Prov_Addr_MaxId_Lkp
# Condition: DSLink185.PROV_ADDR_ID == lnk_lkp_Prov_Addr_MaxId.PROV_ADDR_ID
# --------------------------------------------------------------------------------
df_Att_Addr_Typ_Cd_join = (
    df_Lkp_ADRID.alias("DSLink185")
    .join(
        df_db2_Prov_Addr_MaxId_Lkp.alias("lnk_lkp_Prov_Addr_MaxId"),
        F.col("DSLink185.PROV_ADDR_ID") == F.col("lnk_lkp_Prov_Addr_MaxId.PROV_ADDR_ID"),
        "left"
    )
)
df_Att_Addr_Typ_Cd = df_Att_Addr_Typ_Cd_join.select(
    F.col("DSLink185.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.col("DSLink185.ADDR_LN_1").alias("ADDR_LN_1"),
    F.col("DSLink185.ADDR_LN_2").alias("ADDR_LN_2"),
    F.col("DSLink185.CITY_NM").alias("CITY_NM"),
    F.col("DSLink185.ST_CD").alias("ST_CD"),
    F.col("DSLink185.POSTAL_CD").alias("POSTAL_CD"),
    F.col("DSLink185.County").alias("County"),
    F.col("DSLink185.Phone_Number").alias("Phone_Number"),
    F.col("DSLink185.Fax_Number").alias("Fax_Number"),
    F.col("DSLink185.Effective_Date").alias("Effective_Date"),
    F.col("DSLink185.Term_Date").alias("Term_Date"),
    F.col("DSLink185.Primary_Location_Indicator").alias("Primary_Location_Indicator"),
    F.col("DSLink185.PROV_ADDR_ID_1").alias("PROV_ADDR_ID_1"),
    F.col("DSLink185.PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD_1"),
    F.col("lnk_lkp_Prov_Addr_MaxId.PROV_ADDR_ID").alias("PROV_ADDR_ID_2"),
    F.col("lnk_lkp_Prov_Addr_MaxId.PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD_2")
)

# --------------------------------------------------------------------------------
# Srt_AddrId (PxSort) - sort by PROV_ADDR_ID (stable)
# Output includes "keyChange" => we must detect changes in PROV_ADDR_ID from previous row
# --------------------------------------------------------------------------------
window_srt_addrid = Window.orderBy("PROV_ADDR_ID")
df_Srt_AddrId_withLag = (
    df_Att_Addr_Typ_Cd
    .withColumn("prev_PROV_ADDR_ID", F.lag("PROV_ADDR_ID", 1).over(window_srt_addrid))
    .withColumn(
        "keyChange",
        F.when(F.col("PROV_ADDR_ID") != F.col("prev_PROV_ADDR_ID"), F.lit(1)).otherwise(F.lit(0))
    )
)
df_Srt_AddrId = df_Srt_AddrId_withLag.select(
    "PROV_ADDR_ID",
    "ADDR_LN_1",
    "ADDR_LN_2",
    "CITY_NM",
    "ST_CD",
    "POSTAL_CD",
    "County",
    "Phone_Number",
    "Fax_Number",
    "Effective_Date",
    "Term_Date",
    "Primary_Location_Indicator",
    "PROV_ADDR_ID_1",
    "PROV_ADDR_TYP_CD_1",
    "PROV_ADDR_ID_2",
    "PROV_ADDR_TYP_CD_2",
    "keyChange"
).orderBy("PROV_ADDR_ID")

# --------------------------------------------------------------------------------
# Xfrm_BusinessRules (CTransformerStage) with complex stage variables:
# svProvEffDt, svCurrCounter, svPrevCounter.
# This requires iterative row-by-row logic. We replicate via mapPartitions.
# --------------------------------------------------------------------------------
def _map_business_rules(partition):
    # We track svPrevCounter across rows.  We apply exactly the if/else logic from the stage.
    # Also track the needed columns in each row’s output.
    svPrevCounter = 0
    for row in partition:
        PROV_ADDR_ID = row["PROV_ADDR_ID"]
        ADDR_LN_1 = row["ADDR_LN_1"]
        ADDR_LN_2 = row["ADDR_LN_2"]
        CITY_NM = row["CITY_NM"]
        ST_CD = row["ST_CD"]
        POSTAL_CD = row["POSTAL_CD"]
        County = row["County"]
        Phone_Number = row["Phone_Number"]
        Fax_Number = row["Fax_Number"]
        Effective_Date = row["Effective_Date"]
        Term_Date = row["Term_Date"]
        Primary_Location_Indicator = row["Primary_Location_Indicator"]
        PROV_ADDR_ID_1 = row["PROV_ADDR_ID_1"]
        PROV_ADDR_TYP_CD_1 = row["PROV_ADDR_TYP_CD_1"]
        PROV_ADDR_ID_2 = row["PROV_ADDR_ID_2"]
        PROV_ADDR_TYP_CD_2 = row["PROV_ADDR_TYP_CD_2"]
        keyChange = row["keyChange"]
        svProvEffDt = Effective_Date  # direct from the logic

        # Convert nulls to something for numeric compare
        def toIntOr0(val):
            return int(val) if val and val.strip() != "" else 0

        x1 = toIntOr0(PROV_ADDR_TYP_CD_1)
        x2 = toIntOr0(PROV_ADDR_TYP_CD_2)
        kc = keyChange

        # svCurrCounter logic:
        # If keyChange=1 and x1=0 and x2=0 => 1
        # else if kc=1 and x1<>0 and x2<>0 => x1
        # else if kc=1 and x1=0 and x2<>0 => x2
        # else if kc=0 and x1=0 and x2<>0 and svPrevCounter=0 => x2
        # else if kc=0 and x1<>0 and x2<>0 => x1
        # else if kc=0 and x1=0 and x2=0 => svPrevCounter + 1
        # else if kc=0 and x1=0 and svPrevCounter>= x2 => svPrevCounter + 1
        # else => x1
        if kc == 1 and x1 == 0 and x2 == 0:
            svCurrCounter = 1
        elif kc == 1 and x1 != 0 and x2 != 0:
            svCurrCounter = x1
        elif kc == 1 and x1 == 0 and x2 != 0:
            svCurrCounter = x2
        elif kc == 0 and x1 == 0 and x2 != 0 and svPrevCounter == 0:
            svCurrCounter = x2
        elif kc == 0 and x1 != 0 and x2 != 0:
            svCurrCounter = x1
        elif kc == 0 and x1 == 0 and x2 == 0:
            svCurrCounter = svPrevCounter + 1
        elif kc == 0 and x1 == 0 and svPrevCounter >= x2:
            svCurrCounter = svPrevCounter + 1
        else:
            svCurrCounter = x1

        # Now update svPrevCounter
        if (
            (kc == 1 and x1 == 0 and x2 == 0) or
            (kc == 1 and x1 == 0 and x2 != 0) or
            (kc == 0 and x1 == 0 and x2 != 0) or
            (kc == 0 and x1 == 0 and x2 == 0) or
            (kc == 0 and x1 == 0 and svPrevCounter >= x2)
        ):
            svPrevCounter_out = svCurrCounter
        else:
            svPrevCounter_out = 0

        newRow = {
            "PROV_ADDR_ID": PROV_ADDR_ID,
            "ADDR_LN_1": ADDR_LN_1,
            "ADDR_LN_2": ADDR_LN_2,
            "CITY_NM": CITY_NM,
            "ST_CD": ST_CD,
            "POSTAL_CD": POSTAL_CD,
            "County": County,
            "Phone_Number": Phone_Number,
            "Fax_Number": Fax_Number,
            "Effective_Date": Effective_Date,
            "Term_Date": Term_Date,
            "Primary_Location_Indicator": Primary_Location_Indicator,
            "PrevCounter": svPrevCounter,
            "CurrCounter": svCurrCounter,
            "svProvEffDt": svProvEffDt
        }
        svPrevCounter = svPrevCounter_out
        yield newRow

rdd_Xfrm_BusinessRules = df_Srt_AddrId.rdd.mapPartitions(_map_business_rules)
df_Xfrm_BusinessRules_temp = spark.createDataFrame(rdd_Xfrm_BusinessRules)

# Three output links from Xfrm_BusinessRules:
# 1) lnk_Att_Mail_Eff_Dt
df_lnk_Att_Mail_Eff_Dt = (
    df_Xfrm_BusinessRules_temp
    .select(
        F.concat(
            F.lit(SrcSysCd), F.lit(";"),
            F.col("PROV_ADDR_ID"), F.lit(";"),
            F.col("CurrCounter"), F.lit(";"),
            F.col("svProvEffDt")
        ).alias("PRI_NAT_KEY_STRING"),
        F.lit(RunIDTimeStamp).alias("FIRST_RECYC_TS"),
        F.lit(0).alias("PROV_ADDR_SK"),
        F.col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
        F.col("CurrCounter").alias("PROV_ADDR_TYP_CD"),
        F.col("svProvEffDt").alias("PROV_ADDR_EFF_DT"),
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(SrcSysCdSK).alias("SRC_SYS_CD_SK"),
        F.when(
            (trim(F.when(F.col("County").isNotNull(), F.col("County")).otherwise(F.lit(""))) != "") &
            (trim(F.when(F.col("ST_CD").isNotNull(), F.col("ST_CD")).otherwise(F.lit(""))) != ""),
            F.concat(
                trim(F.when(F.col("County").isNotNull(), F.col("County")).otherwise(F.lit(""))),
                trim(F.when(F.col("ST_CD").isNotNull(), F.col("ST_CD")).otherwise(F.lit("")))
            )
        ).otherwise(F.lit("OOA")).alias("PROV_ADDR_CNTY_CLS_CD"),
        F.lit("NA").alias("PROV_ADDR_GEO_ACES_RTRN_CD"),
        F.when(
            (trim(F.when(F.col("County").isNotNull(), F.col("County")).otherwise(F.lit(""))) != "") &
            (trim(F.when(F.col("ST_CD").isNotNull(), F.col("ST_CD")).otherwise(F.lit(""))) != ""),
            F.concat(
                trim(F.when(F.col("County").isNotNull(), F.col("County")).otherwise(F.lit(""))),
                trim(F.when(F.col("ST_CD").isNotNull(), F.col("ST_CD")).otherwise(F.lit("")))
            )
        ).otherwise(F.lit("OOA")).alias("PROV_ADDR_METRORURAL_COV_CD"),
        F.lit("NA").alias("PROV_ADDR_TERM_RSN_CD"),
        F.lit("N").alias("HCAP_IN"),
        F.lit("N").alias("PDX_24_HR_IN"),
        F.lit("Y").alias("PRCTC_LOC_IN"),
        F.lit("N").alias("PROV_ADDR_DIR_IN"),
        F.when(
            trim(F.when(F.col("Term_Date").isNotNull(), F.col("Term_Date")).otherwise(F.lit(""))) == "9999-12-31",
            F.lit("2199-12-31")
        ).otherwise(F.col("Term_Date")).alias("TERM_DT"),
        F.col("ADDR_LN_1").alias("ADDR_LN_1"),
        F.col("ADDR_LN_2").alias("ADDR_LN_2"),
        F.lit(None).alias("ADDR_LN_3"),
        F.col("CITY_NM").alias("CITY_NM"),
        F.when(F.col("ST_CD") == "", F.lit("1")).otherwise(F.col("ST_CD")).alias("PROV_ADDR_ST_CD"),
        F.col("POSTAL_CD").alias("POSTAL_CD"),  # char(11)
        F.when(trim(F.when(F.col("County").isNotNull(), F.col("County")).otherwise(F.lit(""))) == "", F.lit("NA")).otherwise(F.col("County")).alias("CNTY_NM"),
        F.col("Phone_Number").alias("PHN_NO"),
        F.lit(None).alias("PHN_NO_EXT"),
        F.col("Fax_Number").alias("FAX_NO"),
        F.lit(None).alias("FAX_NO_EXT"),
        F.lit(None).alias("EMAIL_ADDR_TX"),
        F.lit(0).alias("LAT_TX"),
        F.lit(0).alias("LONG_TX"),
        F.lit("1").alias("PRAD_TYPE_MAIL"),
        F.col("CurrCounter").alias("PROV_ADDR_TYP_CD_ORIG"),
        F.col("Primary_Location_Indicator").alias("Primary_Location_Indicator")
    )
)

# 2) lnk_ToBalLkup
df_lnk_ToBalLkup = (
    df_Xfrm_BusinessRules_temp
    .select(
        F.lit(SrcSysCdSK).alias("SRC_SYS_CD_SK"),
        F.col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
        F.col("CurrCounter").alias("PROV_ADDR_TYP_CD"),
        F.col("svProvEffDt").alias("EFF_DT_SK")  # char(10)
    )
)

# 3) Ref_Mail_Addr_Eff_Dt (constraint svCurrCounter = '1')
df_Ref_Mail_Addr_Eff_Dt_pre = df_Xfrm_BusinessRules_temp.filter(F.col("CurrCounter") == 1)
df_Ref_Mail_Addr_Eff_Dt = df_Ref_Mail_Addr_Eff_Dt_pre.select(
    F.col("PROV_ADDR_ID").alias("MAIL_PROV_ADDR_ID"),
    F.col("svProvEffDt").alias("MAIL_EFF_DT_SK")
)

# --------------------------------------------------------------------------------
# Lookup_Fkey_Bal (PxLookup): primary link = lnk_ToBalLkup, lookup link = lnkProvAddrTyp
# Condition: lnk_ToBalLkup.PROV_ADDR_TYP_CD == lnkProvAddrTyp.SRC_CD
# --------------------------------------------------------------------------------
df_Lookup_Fkey_Bal_join = (
    df_lnk_ToBalLkup.alias("lnk_ToBalLkup")
    .join(
        df_lnkProvAddrTyp.alias("lnkProvAddrTyp"),
        F.col("lnk_ToBalLkup.PROV_ADDR_TYP_CD") == F.col("lnkProvAddrTyp.SRC_CD"),
        "left"
    )
)
df_Lookup_Fkey_Bal = df_Lookup_Fkey_Bal_join.select(
    F.col("lnk_ToBalLkup.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("lnk_ToBalLkup.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.col("lnkProvAddrTyp.CD_MPPNG_SK").alias("PROV_ADDR_TYP_CD_SK"),
    F.col("lnk_ToBalLkup.EFF_DT_SK").alias("EFF_DT_SK")
)

# --------------------------------------------------------------------------------
# Xfrm_B_PROV_ADDR (CTransformerStage)
# --------------------------------------------------------------------------------
df_Xfrm_B_PROV_ADDR = df_Lookup_Fkey_Bal.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.col("PROV_ADDR_TYP_CD_SK").alias("PROV_ADDR_TYP_CD_SK"),
    F.col("EFF_DT_SK").alias("PROV_ADDR_EFF_DT_SK")
)

# --------------------------------------------------------------------------------
# seq_B_PROV_ADDR (PxSequentialFile) -> write a .dat file with provided schema
# Name: B_PROV_ADDR.#SrcSysCd#.dat.#RunID#
# --------------------------------------------------------------------------------
df_seq_B_PROV_ADDR = df_Xfrm_B_PROV_ADDR.select(
    "SRC_SYS_CD_SK",
    "PROV_ADDR_ID",
    "PROV_ADDR_TYP_CD_SK",
    "PROV_ADDR_EFF_DT_SK"
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

# --------------------------------------------------------------------------------
# Att_Mail_Eff_Dt (PxLookup): primary link = lnk_Att_Mail_Eff_Dt,
#   lookup link1 = Ref_Mail_Addr_Eff_Dt, condition on PROV_ADDR_ID
#   lookup link2 = Mail_AddrTyp, condition on PROV_ADDR_ID
# --------------------------------------------------------------------------------
df_Att_Mail_Eff_Dt_join_1 = (
    df_lnk_Att_Mail_Eff_Dt.alias("lnk_Att_Mail_Eff_Dt")
    .join(
        df_Ref_Mail_Addr_Eff_Dt.alias("Ref_Mail_Addr_Eff_Dt"),
        F.col("lnk_Att_Mail_Eff_Dt.PROV_ADDR_ID") == F.col("Ref_Mail_Addr_Eff_Dt.MAIL_PROV_ADDR_ID"),
        "left"
    )
)
df_Att_Mail_Eff_Dt_join_2 = (
    df_Att_Mail_Eff_Dt_join_1.alias("tmp")
    .join(
        df_Mail_AddrTyp.alias("Mail_AddrTyp"),
        F.col("tmp.PROV_ADDR_ID") == F.col("Mail_AddrTyp.PROV_ADDR_ID"),
        "left"
    )
)
df_Att_Mail_Eff_Dt = df_Att_Mail_Eff_Dt_join_2.select(
    F.col("tmp.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("tmp.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("tmp.PROV_ADDR_SK").alias("PROV_ADDR_SK"),
    F.col("tmp.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.col("tmp.PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD"),
    F.col("tmp.PROV_ADDR_EFF_DT").alias("PROV_ADDR_EFF_DT"),
    F.col("tmp.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("tmp.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("tmp.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("tmp.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("tmp.PROV_ADDR_CNTY_CLS_CD").alias("PROV_ADDR_CNTY_CLS_CD"),
    F.col("tmp.PROV_ADDR_GEO_ACES_RTRN_CD").alias("PROV_ADDR_GEO_ACES_RTRN_CD"),
    F.col("tmp.PROV_ADDR_METRORURAL_COV_CD").alias("PROV_ADDR_METRORURAL_COV_CD"),
    F.col("tmp.PROV_ADDR_TERM_RSN_CD").alias("PROV_ADDR_TERM_RSN_CD"),
    F.col("tmp.HCAP_IN").alias("HCAP_IN"),
    F.col("tmp.PDX_24_HR_IN").alias("PDX_24_HR_IN"),
    F.col("tmp.PRCTC_LOC_IN").alias("PRCTC_LOC_IN"),
    F.col("tmp.PROV_ADDR_DIR_IN").alias("PROV_ADDR_DIR_IN"),
    F.col("tmp.TERM_DT").alias("TERM_DT"),
    F.col("tmp.ADDR_LN_1").alias("ADDR_LN_1"),
    F.col("tmp.ADDR_LN_2").alias("ADDR_LN_2"),
    F.col("tmp.ADDR_LN_3").alias("ADDR_LN_3"),
    F.col("tmp.CITY_NM").alias("CITY_NM"),
    F.col("tmp.PROV_ADDR_ST_CD").alias("PROV_ADDR_ST_CD"),
    F.col("tmp.POSTAL_CD").alias("POSTAL_CD"),
    F.col("tmp.CNTY_NM").alias("CNTY_NM"),
    F.col("tmp.PHN_NO").alias("PHN_NO"),
    F.col("tmp.PHN_NO_EXT").alias("PHN_NO_EXT"),
    F.col("tmp.FAX_NO").alias("FAX_NO"),
    F.col("tmp.FAX_NO_EXT").alias("FAX_NO_EXT"),
    F.col("tmp.EMAIL_ADDR_TX").alias("EMAIL_ADDR_TX"),
    F.col("tmp.LAT_TX").alias("LAT_TX"),
    F.col("tmp.LONG_TX").alias("LONG_TX"),
    F.col("tmp.PRAD_TYPE_MAIL").alias("PRAD_TYPE_MAIL"),
    F.col("Ref_Mail_Addr_Eff_Dt.MAIL_EFF_DT_SK").alias("PROV2_PRAD_EFF_DT"),
    F.col("Mail_AddrTyp.PROV_ADDR_EFF_DT_SK_MAIL").alias("PROV_ADDR_EFF_DT_SK_MAIL"),
    F.col("tmp.PROV_ADDR_TYP_CD_ORIG").alias("PROV_ADDR_TYP_CD_ORIG"),
    F.col("tmp.Primary_Location_Indicator").alias("Primary_Location_Indicator")
)

# --------------------------------------------------------------------------------
# Map_Eff_Dt_Mail (CTransformerStage)
# --------------------------------------------------------------------------------
df_Map_Eff_Dt_Mail = (
    df_Att_Mail_Eff_Dt
    .withColumn(
        "PROV2_PRAD_EFF_DT",
        F.when(
            F.col("PROV_ADDR_EFF_DT_SK_MAIL") == "",
            F.col("PROV2_PRAD_EFF_DT")
        ).otherwise(F.col("PROV_ADDR_EFF_DT_SK_MAIL"))
    )
    .select(
        F.col("PRI_NAT_KEY_STRING"),
        F.col("FIRST_RECYC_TS"),
        F.col("PROV_ADDR_SK"),
        F.col("PROV_ADDR_ID"),
        F.col("PROV_ADDR_TYP_CD"),
        F.col("PROV_ADDR_EFF_DT"),
        F.col("SRC_SYS_CD"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("SRC_SYS_CD_SK"),
        F.col("PROV_ADDR_CNTY_CLS_CD"),
        F.col("PROV_ADDR_GEO_ACES_RTRN_CD"),
        F.col("PROV_ADDR_METRORURAL_COV_CD"),
        F.col("PROV_ADDR_TERM_RSN_CD"),
        F.col("HCAP_IN"),
        F.col("PDX_24_HR_IN"),
        F.col("PRCTC_LOC_IN"),
        F.col("PROV_ADDR_DIR_IN"),
        F.col("TERM_DT"),
        F.col("ADDR_LN_1"),
        F.col("ADDR_LN_2"),
        F.col("ADDR_LN_3"),
        F.col("CITY_NM"),
        F.col("PROV_ADDR_ST_CD"),
        F.col("POSTAL_CD"),
        F.col("CNTY_NM"),
        F.col("PHN_NO"),
        F.col("PHN_NO_EXT"),
        F.col("FAX_NO"),
        F.col("FAX_NO_EXT"),
        F.col("EMAIL_ADDR_TX"),
        F.col("LAT_TX"),
        F.col("LONG_TX"),
        F.col("PRAD_TYPE_MAIL"),
        F.col("PROV2_PRAD_EFF_DT"),
        F.col("PROV_ADDR_TYP_CD_ORIG"),
        F.col("Primary_Location_Indicator"),
        F.lit(None).alias("ATND_TEXT")
    )
)

# --------------------------------------------------------------------------------
# ds_PROV_ADDR_Xfm (PxDataSet) -> write to Parquet
# PROV_ADDR.#SrcSysCd#.xfrm.#RunID#.ds => PROV_ADDR.{SrcSysCd}.xfrm.{RunID}.parquet
# --------------------------------------------------------------------------------
df_ds_PROV_ADDR_Xfm = df_Map_Eff_Dt_Mail.select(
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
    "PROV_ADDR_ST_CD",
    "POSTAL_CD",
    "CNTY_NM",
    "PHN_NO",
    "PHN_NO_EXT",
    "FAX_NO",
    "FAX_NO_EXT",
    "EMAIL_ADDR_TX",
    "LAT_TX",
    "LONG_TX",
    "PRAD_TYPE_MAIL",
    "PROV2_PRAD_EFF_DT",
    "PROV_ADDR_TYP_CD_ORIG",
    "Primary_Location_Indicator",
    "ATND_TEXT"
)
write_files(
    df_ds_PROV_ADDR_Xfm,
    f"{adls_path}/ds/PROV_ADDR.{SrcSysCd}.xfrm.{RunID}.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)