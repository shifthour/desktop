# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2020, 2022 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Job Name :  DominionIdsProvAddrXfrm
# MAGIC Calling Job:  DominionProvExtrSeq
# MAGIC 
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC 
# MAGIC     Transformation rules applied on Extracted data information that is used to populate the IDS ProvAddr table 
# MAGIC 
# MAGIC Developer		Date		Project/Altiris #	Change Description					Development Project		Code Reviewer		Date Reviewed
# MAGIC ================================================================================================================================================================================================
# MAGIC Deepika C		2023-11-24	US 600544             Initial Programming                                                                    IntegrateSITF                                            Jeyaprasanna                         2024-01-18

# MAGIC Data is sorted so that multiple entries for the for the same ADDR_ID will be in the same partition.
# MAGIC If the incoming address is a new address a sequnetial number is assigned in this transformer.
# MAGIC Assigning Mailing address as the value corresponding to Address Type Code = 1.  If the address assigned a value of type code -1 does not come in the file take from table. If the address coming in is a new address then take it from the file which has a ADR TYPE CODE assigned value of 1
# MAGIC JobName: DominionIdsProvAddrXfrm
# MAGIC ConvertAdrType to Number so it will help with the data sorting
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


# Parameters
SrcSysCd = get_widget_value('SrcSysCd','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunID = get_widget_value('RunID','')
RunIDTimeStamp = get_widget_value('RunIDTimeStamp','')
SrcSysCdSK = get_widget_value('SrcSysCdSK','')

# DB2ConnectorPX: db2_Prov_AddrId_Lkp
jdbc_url_db2_Prov_AddrId_Lkp, jdbc_props_db2_Prov_AddrId_Lkp = get_db_config(ids_secret_name)
query_db2_Prov_AddrId_Lkp = f"""
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
    .option("query", query_db2_Prov_AddrId_Lkp)
    .load()
)

# PxFilter: FilterData
# Two output links:
#  1) Condition (OutputLink=1): SRC_CD = '#SrcSysCd#'
df_FilterData_1 = df_db2_Prov_AddrId_Lkp.filter(F.col("SRC_CD") == SrcSysCd).select(
    F.col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.col("PROV_ADDR_EFF_DT_SK").alias("PROV_ADDR_EFF_DT_SK_MAIL")
)
#  0) Condition (OutputLink=0): PROV_ADDR_TYP_CD = 1
df_FilterData_0 = df_db2_Prov_AddrId_Lkp.filter(F.col("PROV_ADDR_TYP_CD") == "1").select(
    F.col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.col("PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD"),
    F.col("PROV_ADDR_EFF_DT_SK").alias("PROV_ADDR_EFF_DT_SK")
)

# PxDataSet: ds_CD_MPPNG_Lkp_Data (read as Parquet instead of .ds)
df_ds_CD_MPPNG_Lkp_Data_src = spark.read.parquet(f"{adls_path}/ds/CD_MPPNG.parquet")
df_ds_CD_MPPNG_Lkp_Data = df_ds_CD_MPPNG_Lkp_Data_src.select(
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

# PxFilter: fltr_FilterData
# Condition: SRC_SYS_CD='FACETS' AND SRC_CLCTN_CD='FACETS DBO' AND TRGT_CLCTN_CD='IDS'
#            AND SRC_DOMAIN_NM='PROVIDER ADDRESS TYPE' AND TRGT_DOMAIN_NM='PROVIDER ADDRESS TYPE'
df_fltr_FilterData = df_ds_CD_MPPNG_Lkp_Data.filter(
    (F.col("SRC_SYS_CD") == "FACETS")
    & (F.col("SRC_CLCTN_CD") == "FACETS DBO")
    & (F.col("TRGT_CLCTN_CD") == "IDS")
    & (F.col("SRC_DOMAIN_NM") == "PROVIDER ADDRESS TYPE")
    & (F.col("TRGT_DOMAIN_NM") == "PROVIDER ADDRESS TYPE")
).select(
    F.col("SRC_CD").alias("SRC_CD"),
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

# DB2ConnectorPX: db2_Prov_Addr_MaxId_Lkp
jdbc_url_db2_Prov_Addr_MaxId_Lkp, jdbc_props_db2_Prov_Addr_MaxId_Lkp = get_db_config(ids_secret_name)
query_db2_Prov_Addr_MaxId_Lkp = f"""
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
    .option("query", query_db2_Prov_Addr_MaxId_Lkp)
    .load()
)

# PxDataSet: ds_PROV_ADDR_Extr (read as Parquet instead of .ds)
# File: PROV_ADDR.#SrcSysCd#.extr.#RunID#.ds => becomes PROV_ADDR.{SrcSysCd}.extr.{RunID}.parquet
df_ds_PROV_ADDR_Extr_src = spark.read.parquet(f"{adls_path}/ds/PROV_ADDR.{SrcSysCd}.extr.{RunID}.parquet")
df_ds_PROV_ADDR_Extr = df_ds_PROV_ADDR_Extr_src.select(
    "Provider_Identifier",
    "Address",
    "City",
    "State",
    "Zip_Code",
    "County",
    "Phone_Number",
    "Fax_Number",
    "Effective_Date",
    "Term_Date",
    "Primary_Location_Indicator",
    "Handicap_Access_Indicator"
)

# CTransformerStage: Xfrm_BusinessLogic
df_Xfrm_BusinessLogic = df_ds_PROV_ADDR_Extr.select(
    F.col("Provider_Identifier").alias("PROV_ADDR_ID"),
    F.when(F.col("Address").isNotNull(), F.col("Address")).otherwise(F.lit("")).alias("ADDR_LN_1"),
    F.lit("").alias("ADDR_LN_2"),
    F.when(F.col("City").isNotNull(), F.col("City")).otherwise(F.lit("")).alias("CITY_NM"),
    F.col("State").alias("ST_CD"),
    F.concat(
        F.trim(F.substring(F.when(F.col("Zip_Code").isNotNull(), F.col("Zip_Code")).otherwise(F.lit("")), 1, 5)),
        F.lit("-"),
        F.trim(F.substring(F.when(F.col("Zip_Code").isNotNull(), F.col("Zip_Code")).otherwise(F.lit("")), 7, 4))
    ).alias("POSTAL_CD"),
    F.col("County").alias("County"),
    F.when(F.trim(F.col("Phone_Number")) != "", F.trim(F.col("Phone_Number"))).otherwise(F.lit("")).alias("Phone_Number"),
    F.when(F.trim(F.col("Fax_Number")) != "", F.trim(F.col("Fax_Number"))).otherwise(F.lit("")).alias("Fax_Number"),
    F.col("Effective_Date").alias("Effective_Date"),
    F.col("Term_Date").alias("Term_Date"),
    F.col("Primary_Location_Indicator").alias("Primary_Location_Indicator"),
    F.col("Handicap_Access_Indicator").alias("Handicap_Access_Indicator")
)

# PxLookup: Lkp_ADRID
# Primary link: df_Xfrm_BusinessLogic (alias Xfrm)
# Lookup link: df_FilterData_0 (alias Ref_lkp_Prov_AddrId), join on PROV_ADDR_ID and Effective_Date => PROV_ADDR_EFF_DT_SK
df_lkp_adrid = (
    df_Xfrm_BusinessLogic.alias("Xfrm")
    .join(
        df_FilterData_0.alias("Ref_lkp_Prov_AddrId"),
        (
            (F.col("Xfrm.PROV_ADDR_ID") == F.col("Ref_lkp_Prov_AddrId.PROV_ADDR_ID"))
            & (F.col("Xfrm.Effective_Date") == F.col("Ref_lkp_Prov_AddrId.PROV_ADDR_EFF_DT_SK"))
        ),
        "left"
    )
    .select(
        F.col("Xfrm.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
        F.col("Xfrm.ADDR_LN_1").alias("ADDR_LN_1"),
        F.col("Xfrm.ADDR_LN_2").alias("ADDR_LN_2"),
        F.col("Xfrm.CITY_NM").alias("CITY_NM"),
        F.col("Xfrm.ST_CD").alias("ST_CD"),
        F.col("Xfrm.POSTAL_CD").alias("POSTAL_CD"),
        F.col("Xfrm.County").alias("County"),
        F.col("Xfrm.Phone_Number").alias("Phone_Number"),
        F.col("Xfrm.Fax_Number").alias("Fax_Number"),
        F.col("Xfrm.Effective_Date").alias("Effective_Date"),
        F.col("Xfrm.Term_Date").alias("Term_Date"),
        F.col("Xfrm.Primary_Location_Indicator").alias("Primary_Location_Indicator"),
        F.col("Xfrm.Handicap_Access_Indicator").alias("Handicap_Access_Indicator"),
        F.col("Ref_lkp_Prov_AddrId.PROV_ADDR_ID").alias("PROV_ADDR_ID_1"),
        F.col("Ref_lkp_Prov_AddrId.PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD")
    )
)

# PxLookup: Att_Addr_Typ_Cd
# Primary link: df_lkp_adrid (alias lkp_out)
# Lookup link: df_db2_Prov_Addr_MaxId_Lkp (alias lkp_maxid), join on PROV_ADDR_ID
df_att_addr_typ_cd = (
    df_lkp_adrid.alias("lkp_out")
    .join(
        df_db2_Prov_Addr_MaxId_Lkp.alias("lkp_maxid"),
        F.col("lkp_out.PROV_ADDR_ID") == F.col("lkp_maxid.PROV_ADDR_ID"),
        "left"
    )
    .select(
        F.col("lkp_out.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
        F.col("lkp_out.ADDR_LN_1").alias("ADDR_LN_1"),
        F.col("lkp_out.ADDR_LN_2").alias("ADDR_LN_2"),
        F.col("lkp_out.CITY_NM").alias("CITY_NM"),
        F.col("lkp_out.ST_CD").alias("ST_CD"),
        F.col("lkp_out.POSTAL_CD").alias("POSTAL_CD"),
        F.col("lkp_out.County").alias("County"),
        F.col("lkp_out.Phone_Number").alias("Phone_Number"),
        F.col("lkp_out.Fax_Number").alias("Fax_Number"),
        F.col("lkp_out.Effective_Date").alias("Effective_Date"),
        F.col("lkp_out.Term_Date").alias("Term_Date"),
        F.col("lkp_out.Primary_Location_Indicator").alias("Primary_Location_Indicator"),
        F.col("lkp_out.Handicap_Access_Indicator").alias("Handicap_Access_Indicator"),
        F.col("lkp_out.PROV_ADDR_ID_1").alias("PROV_ADDR_ID_1"),
        F.col("lkp_out.PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD_1"),
        F.col("lkp_maxid.PROV_ADDR_ID").alias("PROV_ADDR_ID_2"),
        F.col("lkp_maxid.PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD_2")
    )
)

# CTransformerStage: Xmr_AdrTye (simply pass columns along)
df_Xmr_AdrTye = df_att_addr_typ_cd.select(
    F.col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.col("ADDR_LN_1").alias("ADDR_LN_1"),
    F.col("ADDR_LN_2").alias("ADDR_LN_2"),
    F.col("CITY_NM").alias("CITY_NM"),
    F.col("ST_CD").alias("ST_CD"),
    F.col("POSTAL_CD").alias("POSTAL_CD"),
    F.col("County").alias("County"),
    F.col("Phone_Number").alias("Phone_Number"),
    F.col("Fax_Number").alias("Fax_Number"),
    F.col("Effective_Date").alias("Effective_Date"),
    F.col("Term_Date").alias("Term_Date"),
    F.col("Primary_Location_Indicator").alias("Primary_Location_Indicator"),
    F.col("Handicap_Access_Indicator").alias("Handicap_Access_Indicator"),
    F.col("PROV_ADDR_ID_1").alias("PROV_ADDR_ID_1"),
    F.col("PROV_ADDR_TYP_CD_1").alias("PROV_ADDR_TYP_CD_1"),
    F.col("PROV_ADDR_ID_2").alias("PROV_ADDR_ID_2"),
    F.col("PROV_ADDR_TYP_CD_2").alias("PROV_ADDR_TYP_CD_2")
)

# PxSort: Srt_AddrId
# Sort by PROV_ADDR_ID asc, then PROV_ADDR_TYP_CD_1 desc
df_Srt_AddrId_pre = df_Xmr_AdrTye.orderBy(
    F.col("PROV_ADDR_ID").asc(),
    F.col("PROV_ADDR_TYP_CD_1").desc()
)

# Implement KeyChange() by comparing to previous row in that sorted order
windowSpecKeyChange = Window.orderBy("PROV_ADDR_ID","PROV_ADDR_TYP_CD_1")
df_Srt_AddrId_withLag = (
    df_Srt_AddrId_pre
    .withColumn("_lag_PROV_ADDR_ID", F.lag("PROV_ADDR_ID").over(windowSpecKeyChange))
    .withColumn("_lag_PROV_ADDR_TYP_CD_1", F.lag("PROV_ADDR_TYP_CD_1").over(windowSpecKeyChange))
    .withColumn(
        "keyChange",
        F.when(
            F.col("_lag_PROV_ADDR_ID").isNull()
            | (F.col("PROV_ADDR_ID") != F.col("_lag_PROV_ADDR_ID"))
            | (F.col("PROV_ADDR_TYP_CD_1") != F.col("_lag_PROV_ADDR_TYP_CD_1")),
            F.lit(1)
        ).otherwise(F.lit(0))
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
    "Handicap_Access_Indicator",
    "PROV_ADDR_ID_1",
    "PROV_ADDR_TYP_CD_1",
    "PROV_ADDR_ID_2",
    "PROV_ADDR_TYP_CD_2",
    "keyChange"
)

# CTransformerStage: Xfrm_BusinessRules
# This stage has stateful logic with stage variables. We will apply a row-by-row pass to replicate.
df_Srt_AddrId_collect = df_Srt_AddrId.collect()

transformed_rows = []
prevCounter = 0
for row in df_Srt_AddrId_collect:
    # Extract needed fields
    keyChange = row["keyChange"]
    padtyp1 = row["PROV_ADDR_TYP_CD_1"] if row["PROV_ADDR_TYP_CD_1"] else "0"
    padtyp2 = row["PROV_ADDR_TYP_CD_2"] if row["PROV_ADDR_TYP_CD_2"] else "0"
    effDt = row["Effective_Date"] if row["Effective_Date"] else ""
    
    # Evaluate svCurrCounter logic exactly as in the DataStage expression
    # (matching the chain of IFs from the job)
    if keyChange == 1 and padtyp1 == "0" and padtyp2 == "0":
        svCurrCounter = 1
    elif keyChange == 1 and padtyp1 != "0" and padtyp2 != "0":
        svCurrCounter = int(padtyp1)
    elif keyChange == 1 and padtyp1 == "0" and padtyp2 != "0":
        svCurrCounter = int(padtyp2)
    elif keyChange == 0 and padtyp1 == "0" and padtyp2 != "0" and prevCounter == 0:
        svCurrCounter = int(padtyp2)
    elif keyChange == 0 and padtyp1 != "0" and padtyp2 != "0":
        svCurrCounter = int(padtyp1)
    elif keyChange == 0 and padtyp1 == "0" and padtyp2 == "0":
        svCurrCounter = prevCounter + 1
    elif keyChange == 0 and padtyp1 == "0" and prevCounter >= int(padtyp2):
        svCurrCounter = prevCounter + 1
    else:
        # else map to padtyp1
        try:
            svCurrCounter = int(padtyp1)
        except:
            svCurrCounter = 0

    # Evaluate svPrevCounter logic
    # If <the big OR condition> then svPrevCounter = svCurrCounter else 0
    if (
        (keyChange == 1 and padtyp1 == "0" and padtyp2 == "0")
        or (keyChange == 1 and padtyp1 == "0" and padtyp2 != "0")
        or (keyChange == 0 and padtyp1 == "0" and padtyp2 != "0")
        or (keyChange == 0 and padtyp1 == "0" and padtyp2 == "0")
        or (keyChange == 0 and padtyp1 == "0" and prevCounter >= int(padtyp2))
    ):
        newPrevCounter = svCurrCounter
    else:
        newPrevCounter = 0

    # Build the row output for each link from this transformer
    # lnk_Att_Mail_Eff_Dt columns
    PRI_NAT_KEY_STRING = f"{SrcSysCd};{row['PROV_ADDR_ID']};{svCurrCounter};{effDt}"
    FIRST_RECYC_TS = RunIDTimeStamp
    PROV_ADDR_SK = 0
    PROV_ADDR_ID_Val = row["PROV_ADDR_ID"]
    PROV_ADDR_TYP_CD_Val = svCurrCounter
    PROV_ADDR_EFF_DT_Val = effDt
    SRC_SYS_CD_Val = SrcSysCd
    CRT_RUN_CYC_EXCTN_SK = 0
    LAST_UPDT_RUN_CYC_EXCTN_SK = 0
    SRC_SYS_CD_SK_Val = SrcSysCdSK
    County_Val = row["County"] if row["County"] else ""
    StCdVal = row["ST_CD"] if row["ST_CD"] else ""
    PROV_ADDR_CNTY_CLS_CD = F.lit("OOA").collect()[0]  # Placeholder, we will fix after row logic
    if County_Val.strip() != "" and StCdVal.strip() != "":
        PROV_ADDR_CNTY_CLS_CD = County_Val.strip() + StCdVal.strip()
    PROV_ADDR_GEO_ACES_RTRN_CD = "UNK"
    PROV_ADDR_METRORURAL_COV_CD = "OOA"
    if County_Val.strip() != "" and StCdVal.strip() != "":
        PROV_ADDR_METRORURAL_COV_CD = County_Val.strip() + StCdVal.strip()
    PROV_ADDR_TERM_RSN_CD = "NA"
    HCAP_IN_Val = "N"
    if row["Handicap_Access_Indicator"] and row["Handicap_Access_Indicator"].strip() != "":
        HCAP_IN_Val = row["Handicap_Access_Indicator"]
    PDX_24_HR_IN = "N"
    PRCTC_LOC_IN = "Y"
    PROV_ADDR_DIR_IN = "Y"
    termDtVal = row["Term_Date"] if row["Term_Date"] else ""
    if termDtVal.strip() == "9999-12-31":
        termDtVal = "2199-12-31"
    ADDR_LN_1_Val = row["ADDR_LN_1"]
    ADDR_LN_2_Val = row["ADDR_LN_2"]
    ADDR_LN_3_Val = None
    CITY_NM_Val = row["CITY_NM"]
    PROV_ADDR_ST_CD_Val = row["ST_CD"] if row["ST_CD"] else ""
    if PROV_ADDR_ST_CD_Val == "":
        PROV_ADDR_ST_CD_Val = "1"
    POSTAL_CD_Val = row["POSTAL_CD"]
    CNTY_NM_Val = (row["County"] if row["County"] else "").strip()
    PHN_NO_Val = row["Phone_Number"] if row["Phone_Number"] else ""
    PHN_NO_EXT_Val = None
    FAX_NO_Val = row["Fax_Number"] if row["Fax_Number"] else ""
    FAX_NO_EXT_Val = None
    EMAIL_ADDR_TX_Val = None
    LAT_TX_Val = 0
    LONG_TX_Val = 0
    PRAD_TYPE_MAIL_Val = "1"
    PROV_ADDR_TYP_CD_ORIG_Val = svCurrCounter
    primLocIndVal = row["Primary_Location_Indicator"] if row["Primary_Location_Indicator"] else ""

    # Build row for link "lnk_Att_Mail_Eff_Dt"
    row_lnk_Att_Mail_Eff_Dt = {
        "PRI_NAT_KEY_STRING": PRI_NAT_KEY_STRING,
        "FIRST_RECYC_TS": FIRST_RECYC_TS,
        "PROV_ADDR_SK": PROV_ADDR_SK,
        "PROV_ADDR_ID": PROV_ADDR_ID_Val,
        "PROV_ADDR_TYP_CD": PROV_ADDR_TYP_CD_Val,
        "PROV_ADDR_EFF_DT": PROV_ADDR_EFF_DT_Val,
        "SRC_SYS_CD": SRC_SYS_CD_Val,
        "CRT_RUN_CYC_EXCTN_SK": CRT_RUN_CYC_EXCTN_SK,
        "LAST_UPDT_RUN_CYC_EXCTN_SK": LAST_UPDT_RUN_CYC_EXCTN_SK,
        "SRC_SYS_CD_SK": SRC_SYS_CD_SK_Val,
        "PROV_ADDR_CNTY_CLS_CD": PROV_ADDR_CNTY_CLS_CD,
        "PROV_ADDR_GEO_ACES_RTRN_CD": PROV_ADDR_GEO_ACES_RTRN_CD,
        "PROV_ADDR_METRORURAL_COV_CD": PROV_ADDR_METRORURAL_COV_CD,
        "PROV_ADDR_TERM_RSN_CD": PROV_ADDR_TERM_RSN_CD,
        "HCAP_IN": HCAP_IN_Val,
        "PDX_24_HR_IN": PDX_24_HR_IN,
        "PRCTC_LOC_IN": PRCTC_LOC_IN,
        "PROV_ADDR_DIR_IN": PROV_ADDR_DIR_IN,
        "TERM_DT": termDtVal,
        "ADDR_LN_1": ADDR_LN_1_Val,
        "ADDR_LN_2": ADDR_LN_2_Val,
        "ADDR_LN_3": ADDR_LN_3_Val,
        "CITY_NM": CITY_NM_Val,
        "PROV_ADDR_ST_CD": PROV_ADDR_ST_CD_Val,
        "POSTAL_CD": POSTAL_CD_Val,
        "CNTY_NM": CNTY_NM_Val,
        "PHN_NO": PHN_NO_Val,
        "PHN_NO_EXT": PHN_NO_EXT_Val,
        "FAX_NO": FAX_NO_Val,
        "FAX_NO_EXT": FAX_NO_EXT_Val,
        "EMAIL_ADDR_TX": EMAIL_ADDR_TX_Val,
        "LAT_TX": LAT_TX_Val,
        "LONG_TX": LONG_TX_Val,
        "PRAD_TYPE_MAIL": PRAD_TYPE_MAIL_Val,
        "PROV_ADDR_TYP_CD_ORIG": PROV_ADDR_TYP_CD_ORIG_Val,
        "Primary_Location_Indicator": primLocIndVal
    }

    # Build row for link "lnk_ToBalLkup"
    row_lnk_ToBalLkup = {
        "SRC_SYS_CD_SK": SRC_SYS_CD_SK_Val,
        "PROV_ADDR_ID": PROV_ADDR_ID_Val,
        "PROV_ADDR_TYP_CD": str(svCurrCounter),
        "EFF_DT_SK": effDt
    }

    # Build row for link "Ref_Mail_Addr_Eff_Dt" (constraint: svCurrCounter = '1')
    row_Ref_Mail_Addr_Eff_Dt = None
    if str(svCurrCounter) == "1":
        row_Ref_Mail_Addr_Eff_Dt = {
            "MAIL_PROV_ADDR_ID": PROV_ADDR_ID_Val,
            "MAIL_EFF_DT_SK": effDt
        }

    # Keep them all in a single structure to re-create after the for-loop
    transformed_rows.append({
        "lnk_Att_Mail_Eff_Dt": row_lnk_Att_Mail_Eff_Dt,
        "lnk_ToBalLkup": row_lnk_ToBalLkup,
        "Ref_Mail_Addr_Eff_Dt": row_Ref_Mail_Addr_Eff_Dt
    })

    # Update prevCounter
    prevCounter = newPrevCounter

# Now create DataFrames for each output link of Xfrm_BusinessRules
rows_for_Att_Mail_Eff_Dt = []
rows_for_ToBalLkup = []
rows_for_Ref_Mail_Addr_Eff_Dt = []
for item in transformed_rows:
    rows_for_Att_Mail_Eff_Dt.append(item["lnk_Att_Mail_Eff_Dt"])
    rows_for_ToBalLkup.append(item["lnk_ToBalLkup"])
    if item["Ref_Mail_Addr_Eff_Dt"] is not None:
        rows_for_Ref_Mail_Addr_Eff_Dt.append(item["Ref_Mail_Addr_Eff_Dt"])

df_lnk_Att_Mail_Eff_Dt = spark.createDataFrame(rows_for_Att_Mail_Eff_Dt)
df_lnk_ToBalLkup = spark.createDataFrame(rows_for_ToBalLkup)
df_Ref_Mail_Addr_Eff_Dt = spark.createDataFrame(rows_for_Ref_Mail_Addr_Eff_Dt)

# PxLookup: Lookup_Fkey_Bal
# Primary link: df_lnk_ToBalLkup (alias lnk_ToBalLkup)
# Lookup link: df_fltr_FilterData (alias lnkProvAddrTyp), join on PROV_ADDR_TYP_CD -> SRC_CD
df_lookup_fkey_bal = (
    df_lnk_ToBalLkup.alias("lnk_ToBalLkup")
    .join(
        df_fltr_FilterData.alias("lnkProvAddrTyp"),
        F.col("lnk_ToBalLkup.PROV_ADDR_TYP_CD") == F.col("lnkProvAddrTyp.SRC_CD"),
        "left"
    )
    .select(
        F.col("lnk_ToBalLkup.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("lnk_ToBalLkup.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
        F.col("lnkProvAddrTyp.CD_MPPNG_SK").alias("PROV_ADDR_TYP_CD_SK"),
        F.col("lnk_ToBalLkup.EFF_DT_SK").alias("EFF_DT_SK")
    )
)

# CTransformerStage: Xfrm_B_PROV_ADDR (simply copy columns to the output)
df_Xfrm_B_PROV_ADDR = df_lookup_fkey_bal.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.col("PROV_ADDR_TYP_CD_SK").alias("PROV_ADDR_TYP_CD_SK"),
    F.col("EFF_DT_SK").alias("PROV_ADDR_EFF_DT_SK")
)

# PxSequentialFile: seq_B_PROV_ADDR => write .dat file
# B_PROV_ADDR.#SrcSysCd#.dat.#RunID#
# ContainsHeader = false, delimiter=",", quote="^", nullValue=None
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
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)

# PxLookup: Att_Mail_Eff_Dt
# Primary link: df_lnk_Att_Mail_Eff_Dt (alias lnk_Att_Mail_Eff_Dt)
# Lookup link #1: df_Ref_Mail_Addr_Eff_Dt (alias Ref_Mail_Addr_Eff_Dt), join on PROV_ADDR_ID -> MAIL_PROV_ADDR_ID
# Lookup link #2: df_FilterData_1 (alias Mail_AddrTyp), join on PROV_ADDR_ID -> PROV_ADDR_ID
df_att_mail_eff_dt_join = (
    df_lnk_Att_Mail_Eff_Dt.alias("lnk_Att_Mail_Eff_Dt")
    .join(
        df_Ref_Mail_Addr_Eff_Dt.alias("Ref_Mail_Addr_Eff_Dt"),
        F.col("lnk_Att_Mail_Eff_Dt.PROV_ADDR_ID") == F.col("Ref_Mail_Addr_Eff_Dt.MAIL_PROV_ADDR_ID"),
        "left"
    )
    .join(
        df_FilterData_1.alias("Mail_AddrTyp"),
        F.col("lnk_Att_Mail_Eff_Dt.PROV_ADDR_ID") == F.col("Mail_AddrTyp.PROV_ADDR_ID"),
        "left"
    )
    .select(
        F.col("lnk_Att_Mail_Eff_Dt.*"),
        F.col("Ref_Mail_Addr_Eff_Dt.MAIL_EFF_DT_SK").alias("PROV2_PRAD_EFF_DT"),
        F.col("Mail_AddrTyp.PROV_ADDR_EFF_DT_SK_MAIL").alias("PROV_ADDR_EFF_DT_SK_MAIL")
    )
)

# CTransformerStage: Map_Eff_Dt_Mail
df_map_eff_dt_mail = df_att_mail_eff_dt_join.select(
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("PROV_ADDR_SK").alias("PROV_ADDR_SK"),
    F.col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.col("PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD"),
    F.col("PROV_ADDR_EFF_DT").alias("PROV_ADDR_EFF_DT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("PROV_ADDR_CNTY_CLS_CD").alias("PROV_ADDR_CNTY_CLS_CD"),
    F.col("PROV_ADDR_GEO_ACES_RTRN_CD").alias("PROV_ADDR_GEO_ACES_RTRN_CD"),
    F.col("PROV_ADDR_METRORURAL_COV_CD").alias("PROV_ADDR_METRORURAL_COV_CD"),
    F.col("PROV_ADDR_TERM_RSN_CD").alias("PROV_ADDR_TERM_RSN_CD"),
    F.col("HCAP_IN").alias("HCAP_IN"),
    F.col("PDX_24_HR_IN").alias("PDX_24_HR_IN"),
    F.col("PRCTC_LOC_IN").alias("PRCTC_LOC_IN"),
    F.col("PROV_ADDR_DIR_IN").alias("PROV_ADDR_DIR_IN"),
    F.col("TERM_DT").alias("TERM_DT"),
    F.col("ADDR_LN_1").alias("ADDR_LN_1"),
    F.col("ADDR_LN_2").alias("ADDR_LN_2"),
    F.col("ADDR_LN_3").alias("ADDR_LN_3"),
    F.col("CITY_NM").alias("CITY_NM"),
    F.col("PROV_ADDR_ST_CD").alias("PROV_ADDR_ST_CD"),
    F.col("POSTAL_CD").alias("POSTAL_CD"),
    F.col("CNTY_NM").alias("CNTY_NM"),
    F.col("PHN_NO").alias("PHN_NO"),
    F.col("PHN_NO_EXT").alias("PHN_NO_EXT"),
    F.col("FAX_NO").alias("FAX_NO"),
    F.col("FAX_NO_EXT").alias("FAX_NO_EXT"),
    F.col("EMAIL_ADDR_TX").alias("EMAIL_ADDR_TX"),
    F.col("LAT_TX").alias("LAT_TX"),
    F.col("LONG_TX").alias("LONG_TX"),
    F.col("PRAD_TYPE_MAIL").alias("PRAD_TYPE_MAIL"),
    F.when(
        F.col("PROV_ADDR_EFF_DT_SK_MAIL") == "",
        F.col("PROV2_PRAD_EFF_DT")
    ).otherwise(F.col("PROV_ADDR_EFF_DT_SK_MAIL")).alias("PROV2_PRAD_EFF_DT"),
    F.col("PROV_ADDR_TYP_CD_ORIG").alias("PROV_ADDR_TYP_CD_ORIG"),
    F.col("Primary_Location_Indicator").alias("Primary_Location_Indicator"),
    F.lit(None).alias("ATND_TEXT")
)

# Final link out: lnk_ProvAddrXfrm_Out
df_ProvAddrXfrm_Out = df_map_eff_dt_mail.select(
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

# PxDataSet: ds_PROV_ADDR_Xfm => write as Parquet (instead of .ds)
# File: PROV_ADDR.#SrcSysCd#.xfrm.#RunID#.ds => becomes PROV_ADDR.{SrcSysCd}.xfrm.{RunID}.parquet
# For the final DataFrame, apply rpad for columns that are char/varchar with specified length in the job:
final_cols = []
for c in df_ProvAddrXfrm_Out.schema.fields:
    name = c.name
    t = c.dataType
    if name in ["PROV_ADDR_EFF_DT","TERM_DT","PROV2_PRAD_EFF_DT"] and isinstance(t, StringType):
        # length=10
        final_cols.append(F.rpad(F.col(name), 10, " ").alias(name))
    elif name in ["HCAP_IN","PDX_24_HR_IN","PRCTC_LOC_IN","PROV_ADDR_DIR_IN","Primary_Location_Indicator"] and isinstance(t, StringType):
        # length=1
        final_cols.append(F.rpad(F.col(name), 1, " ").alias(name))
    elif name == "POSTAL_CD" and isinstance(t, StringType):
        # length=11
        final_cols.append(F.rpad(F.col(name), 11, " ").alias(name))
    else:
        final_cols.append(F.col(name).alias(name))

df_ProvAddrXfrm_Out_rpad = df_ProvAddrXfrm_Out.select(*final_cols)

write_files(
    df_ProvAddrXfrm_Out_rpad,
    f"{adls_path}/ds/PROV_ADDR.{SrcSysCd}.xfrm.{RunID}.parquet",
    delimiter=",",
    mode="overwrite",
    is_parqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)