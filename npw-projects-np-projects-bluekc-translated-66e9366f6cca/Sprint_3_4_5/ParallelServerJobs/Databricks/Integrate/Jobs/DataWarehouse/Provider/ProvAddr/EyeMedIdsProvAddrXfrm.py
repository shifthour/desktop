# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2005, 2006, 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Job Name :  EyeMedIdsProvAddrXfrm
# MAGIC 
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC 
# MAGIC     Transformation rules applied on Extracted data information that is used to populate the IDS ProvAddr table 
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Madhavan B                   2018-03-20         5744                                Initial Programming                                             IntegrateDev2              Kalyan Neelam             2018-04-05
# MAGIC 
# MAGIC Goutham Kalidindi            2021-04-15       US-370552                      Updated SQL in Prov_Addr_MaxId_Lkp            IntegrateDev2               Jeyaprasanna               2020-04-21
# MAGIC                                                                                                           to fix PROD Issue 
# MAGIC 
# MAGIC Goutham K                     2021-05-15         US-366403            New Provider file Change to include Loc and Svc loc id    IntegrateDev1        Jeyaprasanna               2020-05-24
# MAGIC 
# MAGIC Arpitha V                        2021-11-25         US- 468740            Added new field ATND_TEXT as Null in                        IntegrateSITF             Jeyaprasanna              2021-11-28
# MAGIC                                                                                                 Xfrm_BusinessRules Transformer Stage

# MAGIC JobName: EyeMedIdsProvAddrXfrm
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
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import when, lit, substring, length, concat, col, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Parameters
SrcSysCd = get_widget_value("SrcSysCd","")
IDSOwner = get_widget_value("IDSOwner","")
RunID = get_widget_value("RunID","")
RunIDTimeStamp = get_widget_value("RunIDTimeStamp","")
SrcSysCdSK = get_widget_value("SrcSysCdSK","")
ids_secret_name = get_widget_value("ids_secret_name","")

# db2_Prov_Addr_MaxId_Lkp
jdbc_url_db2_Prov_Addr_MaxId_Lkp, jdbc_props_db2_Prov_Addr_MaxId_Lkp = get_db_config(ids_secret_name)
extract_query_db2_Prov_Addr_MaxId_Lkp = (
    f"SELECT\n"
    f"PA.PROV_ADDR_ID,\n"
    f"CAST(MAX(CAST(TRIM(COALESCE(AT.SRC_CD,'')) AS INTEGER)) + 1 AS VARCHAR(20)) AS PROV_ADDR_TYP_CD\n\n"
    f"FROM {IDSOwner}.PROV_ADDR AS PA\n\n"
    f"INNER JOIN {IDSOwner}.CD_MPPNG AS AT\n"
    f" ON PA.PROV_ADDR_TYP_CD_SK = AT.CD_MPPNG_SK\n\n"
    f"INNER JOIN {IDSOwner}.CD_MPPNG AS SC\n"
    f" ON PA.SRC_SYS_CD_SK = SC.CD_MPPNG_SK\n"
    f" AND SC.SRC_CD = 'EYEMED'\n"
    f"and AT.TRGT_DOMAIN_NM = 'PROVIDER ADDRESS TYPE'\n\n"
    f"GROUP BY PA.PROV_ADDR_ID"
)
df_db2_Prov_Addr_MaxId_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_Prov_Addr_MaxId_Lkp)
    .options(**jdbc_props_db2_Prov_Addr_MaxId_Lkp)
    .option("query", extract_query_db2_Prov_Addr_MaxId_Lkp)
    .load()
)

# db2_Prov_AddrId_Lkp
jdbc_url_db2_Prov_AddrId_Lkp, jdbc_props_db2_Prov_AddrId_Lkp = get_db_config(ids_secret_name)
extract_query_db2_Prov_AddrId_Lkp = (
    f"SELECT\n"
    f"PA.PROV_ADDR_ID,\n"
    f"TRIM(COALESCE(PA.ADDR_LN_1,'')) AS ADDR_LN_1,\n"
    f"TRIM(COALESCE(PA.ADDR_LN_2,'')) AS ADDR_LN_2,\n"
    f"TRIM(COALESCE(PA.CITY_NM,'')) AS CITY_NM,\n"
    f"TRIM(COALESCE(ST.SRC_CD,'')) AS ST_CD,\n"
    f"TRIM(COALESCE(PA.POSTAL_CD,'')) AS POSTAL_CD,\n"
    f"TRIM(COALESCE(AT.SRC_CD,'')) AS PROV_ADDR_TYP_CD\n\n"
    f"FROM {IDSOwner}.PROV_ADDR AS PA\n\n"
    f"INNER JOIN {IDSOwner}.CD_MPPNG AS ST\n"
    f" ON PA.PROV_ADDR_ST_CD_SK = ST.CD_MPPNG_SK\n\n"
    f"INNER JOIN {IDSOwner}.CD_MPPNG AS AT\n"
    f" ON PA.PROV_ADDR_TYP_CD_SK = AT.CD_MPPNG_SK\n\n"
    f"INNER JOIN {IDSOwner}.CD_MPPNG AS SC\n"
    f" ON PA.SRC_SYS_CD_SK = SC.CD_MPPNG_SK\n"
    f" AND SC.SRC_CD = 'EYEMED'"
)
df_db2_Prov_AddrId_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_Prov_AddrId_Lkp)
    .options(**jdbc_props_db2_Prov_AddrId_Lkp)
    .option("query", extract_query_db2_Prov_AddrId_Lkp)
    .load()
)

# ds_CD_MPPNG_Lkp_Data
df_ds_CD_MPPNG_Lkp_Data = spark.read.parquet(f"{adls_path}/ds/CD_MPPNG.parquet").select(
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

# fltr_FilterData
df_fltr_FilterData = df_ds_CD_MPPNG_Lkp_Data.filter(
    "SRC_SYS_CD='FACETS' AND SRC_CLCTN_CD='FACETS DBO' AND TRGT_CLCTN_CD='IDS' AND SRC_DOMAIN_NM='PROVIDER ADDRESS TYPE' AND TRGT_DOMAIN_NM='PROVIDER ADDRESS TYPE'"
).select(
    F.col("SRC_CD"),
    F.col("CD_MPPNG_SK")
)

# ds_PROV_ADDR_Extr
df_ds_PROV_ADDR_Extr = spark.read.parquet(
    f"{adls_path}/ds/PROV.{SrcSysCd}.extr.{RunID}.parquet"
).select(
    "PROV_ID",
    "PROV_NPI",
    "TAX_ENTY_NPI",
    "PROV_FIRST_NM",
    "PROV_LAST_NM",
    "BUS_NM",
    "PROV_ADDR",
    "PROV_ADDR_2",
    "PROV_CITY",
    "PROV_ST",
    "PROV_ZIP",
    "PROV_ZIP_PLUS_4",
    "PROF_DSGTN",
    "TAX_ENTY_ID",
    "TXNMY_CD",
    "PROV_ADDR_ID"
)

# Xfrm_BusinessLogic
df_Xfrm_BusinessLogic = df_ds_PROV_ADDR_Extr.select(
    F.col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    trim(when(col("PROV_ADDR").isNotNull(), col("PROV_ADDR")).otherwise("")).alias("ADDR_LN_1"),
    trim(when(col("PROV_ADDR_2").isNotNull(), col("PROV_ADDR_2")).otherwise("")).alias("ADDR_LN_2"),
    trim(when(col("PROV_CITY").isNotNull(), col("PROV_CITY")).otherwise("")).alias("CITY_NM"),
    trim(when(col("PROV_ST").isNotNull(), col("PROV_ST")).otherwise("")).alias("ST_CD"),
    concat(
        trim(when(col("PROV_ZIP").isNotNull(), col("PROV_ZIP")).otherwise("")),
        trim(when(col("PROV_ZIP_PLUS_4").isNotNull(), col("PROV_ZIP_PLUS_4")).otherwise(""))
    ).alias("POSTAL_CD"),
    F.col("PROV_ID").alias("PROV_ID")
)

# Lookup_183 (left join with df_db2_Prov_AddrId_Lkp)
df_Lookup_183 = (
    df_Xfrm_BusinessLogic.alias("lnk_Xfrm_out")
    .join(
        df_db2_Prov_AddrId_Lkp.alias("lnk_lkp_Prov_AddrId"),
        (
            (col("lnk_Xfrm_out.PROV_ADDR_ID") == col("lnk_lkp_Prov_AddrId.PROV_ADDR_ID")) &
            (col("lnk_Xfrm_out.ADDR_LN_1") == col("lnk_lkp_Prov_AddrId.ADDR_LN_1")) &
            (col("lnk_Xfrm_out.ADDR_LN_2") == col("lnk_lkp_Prov_AddrId.ADDR_LN_2")) &
            (col("lnk_Xfrm_out.CITY_NM") == col("lnk_lkp_Prov_AddrId.CITY_NM")) &
            (col("lnk_Xfrm_out.ST_CD") == col("lnk_lkp_Prov_AddrId.ST_CD")) &
            (col("lnk_Xfrm_out.POSTAL_CD") == col("lnk_lkp_Prov_AddrId.POSTAL_CD"))
        ),
        "left"
    )
    .select(
        col("lnk_Xfrm_out.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
        col("lnk_Xfrm_out.ADDR_LN_1").alias("ADDR_LN_1"),
        col("lnk_Xfrm_out.ADDR_LN_2").alias("ADDR_LN_2"),
        col("lnk_Xfrm_out.CITY_NM").alias("CITY_NM"),
        col("lnk_Xfrm_out.ST_CD").alias("ST_CD"),
        col("lnk_Xfrm_out.POSTAL_CD").alias("POSTAL_CD"),
        col("lnk_Xfrm_out.PROV_ID").alias("PROV_ID"),
        col("lnk_lkp_Prov_AddrId.PROV_ADDR_ID").alias("PROV_ADDR_ID_1"),
        col("lnk_lkp_Prov_AddrId.PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD")
    )
)

# Lookup_186 (left join with df_db2_Prov_Addr_MaxId_Lkp)
df_Lookup_186 = (
    df_Lookup_183.alias("DSLink185")
    .join(
        df_db2_Prov_Addr_MaxId_Lkp.alias("lnk_lkp_Prov_Addr_MaxId"),
        col("DSLink185.PROV_ADDR_ID") == col("lnk_lkp_Prov_Addr_MaxId.PROV_ADDR_ID"),
        "left"
    )
    .select(
        col("DSLink185.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
        col("DSLink185.ADDR_LN_1").alias("ADDR_LN_1"),
        col("DSLink185.ADDR_LN_2").alias("ADDR_LN_2"),
        col("DSLink185.CITY_NM").alias("CITY_NM"),
        col("DSLink185.ST_CD").alias("ST_CD"),
        col("DSLink185.POSTAL_CD").alias("POSTAL_CD"),
        col("DSLink185.PROV_ID").alias("PROV_ID"),
        col("DSLink185.PROV_ADDR_ID_1").alias("PROV_ADDR_ID_1"),
        col("DSLink185.PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD_1"),
        col("lnk_lkp_Prov_Addr_MaxId.PROV_ADDR_ID").alias("PROV_ADDR_ID_2"),
        col("lnk_lkp_Prov_Addr_MaxId.PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD_2")
    )
)

# Xfrm_BusinessRules
df_Xfrm_BusinessRules_input = df_Lookup_186.alias("DSLink189").withColumn(
    "svProvEffDt", lit("1753-01-01")
).withColumn(
    "svTrimmedProvAddrId", trim(col("DSLink189.PROV_ADDR_ID"))
).withColumn(
    "svProvAddrTypCd",
    substring(col("svTrimmedProvAddrId"), length(col("svTrimmedProvAddrId")) - 1, 2)
)

# lnk_ProvAddrXfrm_Out
df_lnk_ProvAddrXfrm_Out = df_Xfrm_BusinessRules_input.select(
    concat(
        lit(SrcSysCd), lit(";"),
        col("DSLink189.PROV_ADDR_ID"), lit(";"),
        when(col("svProvAddrTypCd").substr(1,1)=="0", col("svProvAddrTypCd").substr(2,1))
        .otherwise(col("svProvAddrTypCd")),
        lit(";"),
        col("svProvEffDt")
    ).alias("PRI_NAT_KEY_STRING"),
    lit(RunIDTimeStamp).alias("FIRST_RECYC_TS"),
    lit(0).alias("PROV_ADDR_SK"),
    col("DSLink189.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    when(col("svProvAddrTypCd").substr(1,1)=="0", col("svProvAddrTypCd").substr(2,1)).otherwise(col("svProvAddrTypCd")).alias("PROV_ADDR_TYP_CD"),
    col("svProvEffDt").alias("PROV_ADDR_EFF_DT"),
    lit(SrcSysCd).alias("SRC_SYS_CD"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(SrcSysCdSK).alias("SRC_SYS_CD_SK"),
    lit("NA").alias("PROV_ADDR_CNTY_CLS_CD"),
    lit("NA").alias("PROV_ADDR_GEO_ACES_RTRN_CD"),
    lit("NA").alias("PROV_ADDR_METRORURAL_COV_CD"),
    lit("NA").alias("PROV_ADDR_TERM_RSN_CD"),
    lit("N").alias("HCAP_IN"),
    lit("N").alias("PDX_24_HR_IN"),
    lit("N").alias("PRCTC_LOC_IN"),
    lit("N").alias("PROV_ADDR_DIR_IN"),
    lit("2199-12-31").alias("TERM_DT"),
    col("DSLink189.ADDR_LN_1").alias("ADDR_LN_1"),
    col("DSLink189.ADDR_LN_2").alias("ADDR_LN_2"),
    lit(None).alias("ADDR_LN_3"),
    col("DSLink189.CITY_NM").alias("CITY_NM"),
    when(col("DSLink189.ST_CD") == "", lit("1")).otherwise(col("DSLink189.ST_CD")).alias("PROV_ADDR_ST_CD"),
    col("DSLink189.POSTAL_CD").alias("POSTAL_CD"),
    lit("UNK").alias("CNTY_NM"),
    lit(None).alias("PHN_NO"),
    lit(None).alias("PHN_NO_EXT"),
    lit(None).alias("FAX_NO"),
    lit(None).alias("FAX_NO_EXT"),
    lit(None).alias("EMAIL_ADDR_TX"),
    lit(0).alias("LAT_TX"),
    lit(0).alias("LONG_TX"),
    when(col("svProvAddrTypCd").substr(1,1)=="0", col("svProvAddrTypCd").substr(2,1)).otherwise(col("svProvAddrTypCd")).alias("PRAD_TYPE_MAIL"),
    col("svProvEffDt").alias("PROV2_PRAD_EFF_DT"),
    when(col("svProvAddrTypCd").substr(1,1)=="0", col("svProvAddrTypCd").substr(2,1)).otherwise(col("svProvAddrTypCd")).alias("PROV_ADDR_TYP_CD_ORIG"),
    lit(None).alias("ATND_TEXT")
)

# lnk_ToBalLkup
df_lnk_ToBalLkup = df_Xfrm_BusinessRules_input.select(
    lit(SrcSysCdSK).alias("SRC_SYS_CD_SK"),
    col("DSLink189.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    when(col("svProvAddrTypCd").substr(1,1)=="0", col("svProvAddrTypCd").substr(2,1)).otherwise(col("svProvAddrTypCd")).alias("PROV_ADDR_TYP_CD"),
    col("svProvEffDt").alias("EFF_DT_SK")
)

# lnk_ProvLocXfrm_Out
df_lnk_ProvLocXfrm_Out = df_Xfrm_BusinessRules_input.select(
    concat(
        lit(SrcSysCd), lit(";"),
        col("DSLink189.PROV_ADDR_ID"), lit(";"),
        col("svProvAddrTypCd"), lit(";"),
        col("svProvEffDt")
    ).alias("PRI_NAT_KEY_STRING"),
    lit(RunIDTimeStamp).alias("FIRST_RECYC_TS"),
    lit(0).alias("PROV_ADDR_SK"),
    col("DSLink189.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    col("svProvAddrTypCd").alias("PROV_ADDR_TYP_CD"),
    col("svProvEffDt").alias("PROV_ADDR_EFF_DT"),
    lit(SrcSysCd).alias("SRC_SYS_CD"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(SrcSysCdSK).alias("SRC_SYS_CD_SK"),
    lit("NA").alias("PROV_ADDR_CNTY_CLS_CD"),
    lit("NA").alias("PROV_ADDR_GEO_ACES_RTRN_CD"),
    lit("NA").alias("PROV_ADDR_METRORURAL_COV_CD"),
    lit("NA").alias("PROV_ADDR_TERM_RSN_CD"),
    lit("N").alias("HCAP_IN"),
    lit("N").alias("PDX_24_HR_IN"),
    lit("N").alias("PRCTC_LOC_IN"),
    lit("N").alias("PROV_ADDR_DIR_IN"),
    lit("2199-12-31").alias("TERM_DT"),
    col("DSLink189.ADDR_LN_1").alias("ADDR_LN_1"),
    col("DSLink189.ADDR_LN_2").alias("ADDR_LN_2"),
    lit(None).alias("ADDR_LN_3"),
    col("DSLink189.CITY_NM").alias("CITY_NM"),
    when(col("DSLink189.ST_CD") == "", lit("1")).otherwise(col("DSLink189.ST_CD")).alias("PROV_ADDR_ST_CD"),
    col("DSLink189.POSTAL_CD").alias("POSTAL_CD"),
    lit("UNK").alias("CNTY_NM"),
    lit(None).alias("PHN_NO"),
    lit(None).alias("PHN_NO_EXT"),
    lit(None).alias("FAX_NO"),
    lit(None).alias("FAX_NO_EXT"),
    lit(None).alias("EMAIL_ADDR_TX"),
    lit(0).alias("LAT_TX"),
    lit(0).alias("LONG_TX"),
    col("svProvAddrTypCd").alias("PRAD_TYPE_MAIL"),
    col("svProvEffDt").alias("PROV2_PRAD_EFF_DT"),
    col("svProvAddrTypCd").alias("PROV_ADDR_TYP_CD_ORIG"),
    col("DSLink189.PROV_ID").alias("PROV_ID")
)

# ds_PROV_ADDR_Xfm (write parquet)
# Apply rpad on char/varchar columns before writing
df_lnk_ProvAddrXfrm_Out_rpad = (
    df_lnk_ProvAddrXfrm_Out
    .withColumn("PROV_ADDR_EFF_DT", rpad(col("PROV_ADDR_EFF_DT"), 10, " "))
    .withColumn("HCAP_IN", rpad(col("HCAP_IN"), 1, " "))
    .withColumn("PDX_24_HR_IN", rpad(col("PDX_24_HR_IN"), 1, " "))
    .withColumn("PRCTC_LOC_IN", rpad(col("PRCTC_LOC_IN"), 1, " "))
    .withColumn("PROV_ADDR_DIR_IN", rpad(col("PROV_ADDR_DIR_IN"), 1, " "))
    .withColumn("TERM_DT", rpad(col("TERM_DT"), 10, " "))
    .withColumn("POSTAL_CD", rpad(col("POSTAL_CD"), 11, " "))
    .withColumn("PROV2_PRAD_EFF_DT", rpad(col("PROV2_PRAD_EFF_DT"), 10, " "))
)

write_files(
    df_lnk_ProvAddrXfrm_Out_rpad.select(
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
        "ATND_TEXT"
    ),
    f"{adls_path}/ds/PROV_ADDR.{SrcSysCd}.xfrm.{RunID}.parquet",
    ",",
    "overwrite",
    True,
    True,
    '"',
    None
)

# ds_PROV_LOC_Xfm (write parquet)
df_lnk_ProvLocXfrm_Out_rpad = (
    df_lnk_ProvLocXfrm_Out
    .withColumn("PROV_ADDR_EFF_DT", rpad(col("PROV_ADDR_EFF_DT"), 10, " "))
    .withColumn("HCAP_IN", rpad(col("HCAP_IN"), 1, " "))
    .withColumn("PDX_24_HR_IN", rpad(col("PDX_24_HR_IN"), 1, " "))
    .withColumn("PRCTC_LOC_IN", rpad(col("PRCTC_LOC_IN"), 1, " "))
    .withColumn("PROV_ADDR_DIR_IN", rpad(col("PROV_ADDR_DIR_IN"), 1, " "))
    .withColumn("TERM_DT", rpad(col("TERM_DT"), 10, " "))
    .withColumn("POSTAL_CD", rpad(col("POSTAL_CD"), 11, " "))
    .withColumn("PROV2_PRAD_EFF_DT", rpad(col("PROV2_PRAD_EFF_DT"), 10, " "))
)

write_files(
    df_lnk_ProvLocXfrm_Out_rpad.select(
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
        "PROV_ID"
    ),
    f"{adls_path}/ds/PROV_LOC_CLM.{SrcSysCd}.xfrm.{RunID}.parquet",
    ",",
    "overwrite",
    True,
    True,
    '"',
    None
)

# Lookup_Fkey_Bal
df_Lookup_Fkey_Bal = (
    df_lnk_ToBalLkup.alias("lnk_ToBalLkup")
    .join(
        df_fltr_FilterData.alias("lnkProvAddrTyp"),
        col("lnk_ToBalLkup.PROV_ADDR_TYP_CD") == col("lnkProvAddrTyp.SRC_CD"),
        "left"
    )
    .select(
        col("lnk_ToBalLkup.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        col("lnk_ToBalLkup.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
        col("lnkProvAddrTyp.CD_MPPNG_SK").alias("PROV_ADDR_TYP_CD_SK"),
        col("lnk_ToBalLkup.EFF_DT_SK").alias("EFF_DT_SK")
    )
)

# Xfrm_B_PROV_ADDR
df_Xfrm_B_PROV_ADDR = df_Lookup_Fkey_Bal.alias("Snapshot").select(
    col("Snapshot.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("Snapshot.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    col("Snapshot.PROV_ADDR_TYP_CD_SK").alias("PROV_ADDR_TYP_CD_SK"),
    col("Snapshot.EFF_DT_SK").alias("PROV_ADDR_EFF_DT_SK")
)

# seq_B_PROV_ADDR (PxSequentialFile write)
df_lnk_ProvXfrmBProvAddr_Out = df_Xfrm_B_PROV_ADDR.select(
    "SRC_SYS_CD_SK",
    "PROV_ADDR_ID",
    "PROV_ADDR_TYP_CD_SK",
    "PROV_ADDR_EFF_DT_SK"
)

write_files(
    df_lnk_ProvXfrmBProvAddr_Out,
    f"{adls_path}/load/B_PROV_ADDR.{SrcSysCd}.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "^",
    None
)