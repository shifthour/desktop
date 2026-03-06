# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC 2;hf_lab_rslt_transform;hf_lab_rslt_allcol
# MAGIC 
# MAGIC  Copyright 2007, 2015 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     LabCorpRsltExtrSeq
# MAGIC 
# MAGIC Processing:
# MAGIC                     Flat File Medical Management LabCorp Lab Results Extract and Primary Keying. This is a monthly file from the LabCorp Lab Vendor.  Duplicate lab results in the same file are 
# MAGIC                     weeded out with a sort process.  Bring all duplicates together prior to the job starting. This is done in the calling sequencer, IdsLabCorpLabRsltExtrSeq.  In the case of duplicates, 
# MAGIC                     whichever record comes in first will be loaded and the rest will be written to an error file and labeled as a duplicate record.  In the event we receive duplicate records that have 
# MAGIC                     already been loaded to the IDS table, these are not kept either.  However, these records will be kicked out and NOT written to the error file.
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:    Restore source file, if necessary
# MAGIC                     Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                    Project/                                                                                                                             Code                   Date
# MAGIC Developer           Date                Altiris #        Change Description                                                                                       Reviewer            Reviewed
# MAGIC -------------------------  ---------------------   ----------------   -----------------------------------------------------------------------------------------------------------------     -------------------------  -------------------
# MAGIC Laurel Kindley     2007-10-08\(9)   3429           Original Programming.                                              \(9)                           Steph Goddard           10/16/2007
# MAGIC Laurel Kindley     2007-12-27\(9)   3429           Modified final file name to be non-vendor specific\(9)\(9)           Steph Goddard            01/02/2008               
# MAGIC Laurel Kindley     2008-05-29\(9)   3643\(9)      Added balancing\(9)\(9)\(9)\(9)\(9)
# MAGIC Ralph Tucker     2008-08-12       3567          Changed primary key process and added new container        
# MAGIC Laurel Kindley     2008-10-15\(9)Blue Jira 353  Added check for empty string on Abonormal Flag\(9)\(9)           Steph Goddard            12/19/2008
# MAGIC \(9)                                                      Also changed logic for SRC_SYS_PROV_ID to be UPIN instead of LAB_CD.
# MAGIC Raja Gummadi    2012-05-10       4896          Added changes to natural key fields(new column) and added P_ICD_VRSN lookup 
# MAGIC Hugh Sisson       2015-01-14     TFS8292     Add 6 fields to the natural key                                                                        Kalyan Neelam      2015-03-06
# MAGIC Raja Gummadi    2015-09-09      5332           Added ICD_VRSN field in source and changed logic for PROC_CD             Kalyan Neelam      2015-09-11
# MAGIC 
# MAGIC Akhila M           2016-10-22      5628           Added condition to exlude the workers comp data to warehouse                    Kalyan Neelam      2016-11-09
# MAGIC                                                                           Added join with Facets P_SEL_PRCS_CRITR
# MAGIC 
# MAGIC Nagesh Bandi   2018-01-12    TFS-20833     Added NullOptCode transformation rule to DIAG_CD_2 and DIAG_CD_3     Kalyan Neelam      2018-01-18
# MAGIC                                                                       fileds to handle Null source values in BUSINESS_RULES stage.
# MAGIC 
# MAGIC AmarendraPoka 208-07-24      5854            Mapped NTNL_PROV_ID field into                                                                Kalyan Neelam      2018-08-03
# MAGIC                                                                     LAB_RSLT_SRC_SYS_ORDER_PROV_ID field and dervied  SVC_PROV_ID
# MAGIC 
# MAGIC AmarendraPoka 2018-09-24     5854        NTNL_PROV_ID lookup is done First on Facets then on BCA systems            Kalyan Neelam      2018-09-26
# MAGIC                                                                   to derive PROV_ID
# MAGIC Prabhu ES         2022-03-14     S2S          MSSQL ODBC conn params added - IntegrateDev5                                        Kalyan Neelam      2022-06-12

# MAGIC Read the sorted lab result file received from LabCorp. Trim all field values, format the result numbers to trim all leading and trailing zeros and add a decimal.  Remove the alpha prefex from the member concatenated id to get the member uniq key, and format the abnormal flag information.
# MAGIC If there is not a LOCAL_RSLT_CD, associated subscriber id, or duplicate  lab result records, write the \"bad\" record to a file.
# MAGIC Apply business logic
# MAGIC Apply primary keys.
# MAGIC Look up the subscriber id.
# MAGIC on2018-0718--in \"BUSINESSRULES\" stage , UPIN field is dropped and NTNL_PROV_ID is mapped to src_system_order_provider_id field.
# MAGIC ProvExtr--stage extracts minimum provider Sk  for NPI data based on  lab service date is greater than of termination date
# MAGIC 2017-08-03: delimiter modified from \"double quote\" to \"000\" and applied trim of double quote on rslt_cmt column.
# MAGIC Apply business logic
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DecimalType
)
from pyspark.sql.functions import (
    col,
    lit,
    when,
    upper,
    length,
    regexp_replace,
    concat_ws,
    substring,
    rpad,
    asc
)
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/LabRsltPK

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
BCBSOwner = get_widget_value('BCBSOwner','')
bcbs_secret_name = get_widget_value('bcbs_secret_name','')
FacetsSrcSysCdSk = get_widget_value('FacetsSrcSysCdSk','')
BCASrcSysCdSk = get_widget_value('BCASrcSysCdSk','')
CurrRunCycle = get_widget_value('CurrRunCycle','100')
RowPassThru = get_widget_value('RowPassThru','Y')
CurrRunDt = get_widget_value('CurrRunDt','2018-01-11')
RunId = get_widget_value('RunId','101')
SrcSysCdSk = get_widget_value('SrcSysCdSk','1821518063')
SrcSysCd = get_widget_value('SrcSysCd','LABCORP')
InputFile = get_widget_value('InputFile','LabCorpLabResults.txt')

# Read LabCorpResults_dat (landing file)
schema_LabCorpResults_dat = StructType([
    StructField("DT_OF_SVC", DecimalType(38,10), True),
    StructField("ACESION_NO", StringType(), True),
    StructField("LAB_CD", StringType(), True),
    StructField("PATN_FIRST_NM", StringType(), True),
    StructField("PATN_MID_NM", StringType(), True),
    StructField("PATN_LAST_NM", StringType(), True),
    StructField("ADDR_1", StringType(), True),
    StructField("ADDR_2", StringType(), True),
    StructField("CITY", StringType(), True),
    StructField("ST", StringType(), True),
    StructField("ZIP", StringType(), True),
    StructField("PHN", StringType(), True),
    StructField("DOB", DecimalType(38,10), True),
    StructField("PATN_AGE", DecimalType(38,10), True),
    StructField("GNDR", StringType(), True),
    StructField("SSN", DecimalType(38,10), True),
    StructField("VNDR_BILL_ID", StringType(), True),
    StructField("POL_NO", StringType(), True),
    StructField("ORDER_ACCT_NO", StringType(), True),
    StructField("ORDER_ACCT_NM", StringType(), True),
    StructField("ORDER_ACCT_ADDR1", StringType(), True),
    StructField("ORDER_ACCT_ADDR2", StringType(), True),
    StructField("ORDER_ACCT_CITY", StringType(), True),
    StructField("ORDER_ST_ZIP", StringType(), True),
    StructField("ORDER_ACCT_PHN", StringType(), True),
    StructField("RFRNG_PHYS", StringType(), True),
    StructField("UPIN", StringType(), True),
    StructField("ICD_VRSN", StringType(), True),
    StructField("DIAG_CD", StringType(), True),
    StructField("LOCAL_ORDER_CD", StringType(), True),
    StructField("ORDER_NM", StringType(), True),
    StructField("NTNL_RSLT_CD", StringType(), True),
    StructField("LOCAL_RSLT_CD", StringType(), True),
    StructField("RSLT_NM", StringType(), True),
    StructField("RSLT_VAL_NUM", DecimalType(38,10), True),
    StructField("RSLT_VAL_LITERAL", StringType(), True),
    StructField("RSLT_UNIT", StringType(), True),
    StructField("REF_RNG_LOW", DecimalType(38,10), True),
    StructField("REF_RNG_HI", DecimalType(38,10), True),
    StructField("REF_RNG_ALPHA", StringType(), True),
    StructField("ABNORM_FLAG", StringType(), True),
    StructField("MBR_ID", StringType(), True),
    StructField("RSLT_CMNT", StringType(), True),
    StructField("RELSHP_CD", StringType(), True),
    StructField("CPT_CD", DecimalType(38,10), True),
    StructField("GRP_PLN", StringType(), True),
    StructField("NTNL_PROV_ID", StringType(), True),
    StructField("DIAG_CD_2", StringType(), True),
    StructField("DIAG_CD_3", StringType(), True)
])

df_LabCorpResults_dat = (
    spark.read
    .format("csv")
    .option("delimiter", "\u0001")  # The job sets quoteChar=000, so we assume a non-standard delimiter or no quotes
    .option("quote", "\u0000")
    .option("header", "false")
    .schema(schema_LabCorpResults_dat)
    .load(f"{adls_path_raw}/landing/{InputFile}")
)

# Sort_331
df_Sort_331 = df_LabCorpResults_dat.sort(
    asc("MBR_ID"),
    asc("RELSHP_CD"),
    asc("GRP_PLN"),
    asc("DT_OF_SVC"),
    asc("CPT_CD"),
    asc("LOCAL_RSLT_CD")
)

# FACETS - read BCBSOwner.P_SEL_PRCS_CRITR
jdbc_url_facets, jdbc_props_facets = get_db_config(bcbs_secret_name)
extract_query_facets = f"SELECT * FROM {BCBSOwner}.P_SEL_PRCS_CRITR"
df_FACETS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_facets)
    .load()
)

# For the reference link usage of df_FACETS in Format, the DataStage job checks a range:
# We'll keep df_FACETS as is and rely on the transform logic that picks needed columns.

# Now prepare the "Format" transform from df_Sort_331:
df_Extract = df_Sort_331.alias("Extract")

# We'll create df_Format by applying the transform expressions from "Sort_331" to "Format" output columns:

df_Format_cols = df_Extract.select(
    rpad(col("DT_OF_SVC").cast(StringType()), 38, " ").alias("DT_OF_SVC"),
    rpad(col("ACESION_NO"), 16, " ").alias("ACESION_NO"),
    rpad(col("LAB_CD"), 4, " ").alias("LAB_CD"),
    rpad(col("PATN_FIRST_NM"), 15, " ").alias("PATN_FIRST_NM"),
    rpad(col("PATN_MID_NM"), 15, " ").alias("PATN_MID_NM"),
    rpad(col("PATN_LAST_NM"), 25, " ").alias("PATN_LAST_NM"),
    rpad(col("ADDR_1"), 25, " ").alias("ADDR_1"),
    rpad(col("ADDR_2"), 25, " ").alias("ADDR_2"),
    rpad(col("CITY"), 22, " ").alias("CITY"),
    rpad(col("ST"), 2, " ").alias("ST"),
    rpad(col("ZIP"), 9, " ").alias("ZIP"),
    rpad(col("PHN"), 12, " ").alias("PHN"),
    rpad(col("DOB").cast(StringType()), 38, " ").alias("DOB"),
    rpad(col("PATN_AGE").cast(StringType()), 38, " ").alias("PATN_AGE"),
    rpad(col("GNDR"), 1, " ").alias("GNDR"),
    rpad(col("SSN").cast(StringType()), 38, " ").alias("SSN"),
    rpad(col("VNDR_BILL_ID"), 10, " ").alias("QUEST_BILL_ID"),
    rpad(col("POL_NO"), 20, " ").alias("POL_NO"),
    rpad(col("ORDER_ACCT_NO"), 18, " ").alias("ORDER_ACCT_NO"),
    rpad(col("ORDER_ACCT_NM"), 30, " ").alias("ORDER_ACCT_NM"),
    rpad(col("ORDER_ACCT_ADDR1"), 30, " ").alias("ORDER_ACCT_ADDR1"),
    rpad(col("ORDER_ACCT_ADDR2"), 30, " ").alias("ORDER_ACCT_ADDR2"),
    rpad(col("ORDER_ACCT_CITY"), 25, " ").alias("ORDER_ACCT_CITY"),
    # Next 2 columns come from partial substring logic:
    # "ORDER_ST" => substring(ORDER_ST_ZIP, 1,2)
    # "ORDER_ST_ZIP" => substring(ORDER_ST_ZIP, 3,5)
    rpad(substring(col("ORDER_ST_ZIP"), 1, 2), 2, " ").alias("ORDER_ST"),
    rpad(substring(col("ORDER_ST_ZIP"), 3, 5), 10, " ").alias("ORDER_ST_ZIP"),
    rpad(col("ORDER_ACCT_PHN"), 16, " ").alias("ORDER_ACCT_PHN"),
    rpad(col("RFRNG_PHYS"), 25, " ").alias("RFRNG_PHYS"),
    rpad(col("UPIN"), 6, " ").alias("UPIN"),
    rpad(col("ICD_VRSN"), 2, " ").alias("ICD_VRSN"),
    rpad(col("DIAG_CD"), 18, " ").alias("DIAG_CD"),
    rpad(col("LOCAL_ORDER_CD"), 10, " ").alias("LOCAL_ORDER_CD"),
    rpad(col("ORDER_NM"), 38, " ").alias("ORDER_NM"),
    rpad(col("NTNL_RSLT_CD"), 10, " ").alias("LOINC_CD"),
    rpad(col("LOCAL_RSLT_CD"), 10, " ").alias("LOCAL_RSLT_CD"),
    rpad(col("RSLT_NM"), 30, " ").alias("RSLT_NM"),
    # numeric or string? We'll keep as string for transformation
    col("RSLT_VAL_NUM").cast(StringType()).alias("RSLT_VAL_NUM"),
    rpad(col("RSLT_VAL_LITERAL"), 18, " ").alias("RSLT_VAL_LITERAL"),
    rpad(col("RSLT_UNIT"), 38, " ").alias("RSLT_UNIT"),
    col("REF_RNG_LOW").cast(StringType()).alias("REF_RNG_LOW"),
    col("REF_RNG_HI").cast(StringType()).alias("REF_RNG_HI"),
    rpad(col("REF_RNG_ALPHA"), 20, " ").alias("REF_RNG_ALPHA"),
    rpad(col("ABNORM_FLAG"), 2, " ").alias("ABNORM_FLAG"),
    # "RSLT_CMNT" => trim(..., '\"', "A") => we will do a simple replacement
    regexp_replace(col("RSLT_CMNT"), '\"', '').alias("RSLT_CMNT"),
    # "CPT_CD" => keep as string
    col("CPT_CD").cast(StringType()).alias("CPT_CD"),
    rpad(col("MBR_ID"), 15, " ").alias("MBR_ID"),
    rpad(col("RELSHP_CD"), 3, " ").alias("RELSHP_CD"),
    rpad(col("GRP_PLN"), 10, " ").alias("GRP_PLN"),
    # "NTNL_PROV_ID" => If null or empty => 'NA'
    when(
        (col("NTNL_PROV_ID").isNull()) | (length(trim(col("NTNL_PROV_ID"))) == 0),
        'NA'
    ).otherwise(col("NTNL_PROV_ID")).alias("NTNL_PROV_ID"),
    rpad(col("DIAG_CD_2"), 18, " ").alias("DIAG_CD_2"),
    rpad(col("DIAG_CD_3"), 18, " ").alias("DIAG_CD_3"),
    # We have to replicate the "PROC_CD" and "PROC_CD_TYP_CD" columns as well, from the transform
    # But the "Format" stage expects them from the transform? Actually it does in next stage. We'll keep placeholders now:
    # They are derived from stage variables in the Format stage. We'll do them later in "BUSINESS_RULES".
    # For now, just keep empty placeholders so the columns exist before the next stage:
    lit("").alias("PROC_CD"),
    lit("").alias("PROC_CD_TYP_CD")
).alias("Format")

df_Format = df_Format_cols

# We also need to incorporate logic from the same "Format" transform that references "svWorkersComp" = 
# "if (Extract.GRP_PLN >= Lnk_PSelLkup.CRITR_VAL_FROM_TX and Extract.GRP_PLN <= Lnk_PSelLkup.CRITR_VAL_THRU_TX) Then 'Y' else 'N'"
# That reference link has no direct join key, so we approximate by cross-joining or making an indicator.
# Since the original code uses a row-by-row range check, we mimic with left anti/semi logic or simpler UDF approach.
# For simplicity, we do a left join with condition. We'll keep only one row from df_FACETS if it matches the range; else null.

df_FACETS_alias = df_FACETS.select(
    col("CRITR_VAL_FROM_TX").alias("CRITR_VAL_FROM_TX"),
    col("CRITR_VAL_THRU_TX").alias("CRITR_VAL_THRU_TX")
).distinct()

# Cross join approach for weaving in a single row if in range. This can cause duplicates if multiple rows match:
# We'll just keep a flag that says if any row matched. Then we pick the minimal logic if matched or not.

df_Format = df_Format.join(
    df_FACETS_alias,
    (df_Format["GRP_PLN"] >= df_FACETS_alias["CRITR_VAL_FROM_TX"]) &
    (df_Format["GRP_PLN"] <= df_FACETS_alias["CRITR_VAL_THRU_TX"]),
    "left"
)

df_Format = df_Format.withColumn(
    "svWorkersComp",
    when(
        (col("CRITR_VAL_FROM_TX").isNotNull()) & (col("CRITR_VAL_THRU_TX").isNotNull()),
        "Y"
    ).otherwise("N")
)

# Now read the entire sub/mbr/grp for mbr_uniq_key_lkup usage
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
qry_mbr_uniq_key_lkup = f"""
SELECT
  SUB.SUB_ID AS SUB_ID,
  MBR.MBR_SFX_NO AS MBR_SFX_NO,
  GRP.GRP_ID AS GRP_ID,
  MBR.MBR_UNIQ_KEY AS MBR_UNIQ_KEY
FROM {IDSOwner}.SUB SUB
JOIN {IDSOwner}.MBR MBR ON SUB.SUB_SK = MBR.SUB_SK
JOIN {IDSOwner}.GRP GRP ON SUB.GRP_SK = GRP.GRP_SK
"""
df_mbr_uniq_key_lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", qry_mbr_uniq_key_lkup)
    .load()
)

# ProcCdlkup usage
qry_ProcCdlkup = f"""
SELECT PROC_CD.PROC_CD,
       PROC_CD.PROC_CD_TYP_CD
FROM {IDSOwner}.PROC_CD PROC_CD
WHERE PROC_CD.PROC_CD_CAT_CD = 'MED'
"""
df_ProcCdlkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", qry_ProcCdlkup)
    .load()
)

# BUSINESS_RULES transform. We start from df_Format and do left joins with df_mbr_uniq_key_lkup (on MBR_ID->SUB_ID, RELSHP_CD->MBR_SFX_NO, GRP_PLN->GRP_ID)
# also with df_ProcCdlkup (on PROC_CD->PROC_CD, PROC_CD_TYP_CD->PROC_CD_TYP_CD).
df_BR_Join = df_Format.alias("Format") \
    .join(
        df_mbr_uniq_key_lkup.alias("mbr_uniq_key_lkup"),
        [
            col("Format.MBR_ID") == col("mbr_uniq_key_lkup.SUB_ID"),
            col("Format.RELSHP_CD") == col("mbr_uniq_key_lkup.MBR_SFX_NO"),
            col("Format.GRP_PLN") == col("mbr_uniq_key_lkup.GRP_ID")
        ],
        "left"
    ).join(
        df_ProcCdlkup.alias("ProcCdlkup"),
        [
            col("Format.PROC_CD") == col("ProcCdlkup.PROC_CD"),
            col("Format.PROC_CD_TYP_CD") == col("ProcCdlkup.PROC_CD_TYP_CD")
        ],
        "left"
    )

# Now apply the stage variables for BUSINESS_RULES in a chain of withColumn:
df_business_rules = df_BR_Join.withColumn(
    "vLabRsltFlag",
    when(
        (col("Format.ABNORM_FLAG").isNull()) | (length(col("Format.ABNORM_FLAG")) == 0) | (col("Format.ABNORM_FLAG") == ' '),
        "NA"
    ).otherwise(col("Format.ABNORM_FLAG"))
).withColumn(
    "vRsltCom1",
    substring(col("Format.RSLT_CMNT"), 1, 255)
).withColumn(
    "vRsltCom2",
    substring(col("Format.RSLT_CMNT"), 256, 255)
).withColumn(
    "svRsltCdErr",
    when(col("Format.LOCAL_RSLT_CD").isNull(), "Y").otherwise("N")
).withColumn(
    "svMbrErr",
    when(col("mbr_uniq_key_lkup.MBR_UNIQ_KEY").isNull(), "Y").otherwise("N")
).withColumn(
    "svCPTCd",
    when(
        (col("ProcCdlkup.PROC_CD").isNull()) | (col("ProcCdlkup.PROC_CD") == "") | (col("ProcCdlkup.PROC_CD") == "00000"),
        "NA"
    ).otherwise(col("ProcCdlkup.PROC_CD"))
).withColumn(
    "svProcCD",
    when(
        (col("ProcCdlkup.PROC_CD").isNull()) | (col("ProcCdlkup.PROC_CD") == "") | (col("ProcCdlkup.PROC_CD") == "00000"),
        "NA"
    ).otherwise(col("ProcCdlkup.PROC_CD"))
).withColumn(
    "svProcCdTypCd",
    when(
        (col("ProcCdlkup.PROC_CD").isNull()) | (col("ProcCdlkup.PROC_CD") == "") | (col("ProcCdlkup.PROC_CD") == "00000"),
        "NA"
    ).otherwise(col("ProcCdlkup.PROC_CD_TYP_CD"))
).withColumn(
    "svDiagCd",
    when(
        (col("Format.DIAG_CD").isNull()) | (length(trim(col("Format.DIAG_CD"))) == 0),
        "NA"
    ).otherwise(upper(trim(col("Format.DIAG_CD"))))
).withColumn(
    "svDiagCdTypCd",
    col("Format.DIAG_CD_TYP_CD_1")
).withColumn(
    "svLoincCd",
    when(
        (col("Format.LOINC_CD").isNull()) | (length(trim(col("Format.LOINC_CD"))) == 0),
        "NA"
    ).otherwise(upper(trim(col("Format.LOINC_CD"))))
).withColumn(
    "svRsltId",
    when(
        (col("Format.LOCAL_RSLT_CD").isNull()) | (length(trim(col("Format.LOCAL_RSLT_CD"))) == 0),
        "NA"
    ).otherwise(upper(trim(col("Format.LOCAL_RSLT_CD"))))
).withColumn(
    "svOrderTstNm",
    when(
        (col("Format.LOCAL_ORDER_CD").isNull()) | (length(trim(col("Format.LOCAL_ORDER_CD"))) == 0),
        "NA"
    ).otherwise(upper(trim(col("Format.LOCAL_ORDER_CD"))))
).withColumn(
    "svPatnEncntrId",
    when(
        (col("Format.ACESION_NO").isNull()) | (length(trim(col("Format.ACESION_NO"))) == 0),
        "NA"
    ).otherwise(upper(trim(col("Format.ACESION_NO"))))
).withColumn(
    "svPrevKey",
    lit("")
).withColumn(
    "svKey",
    concat_ws(
        "",
        when(col("mbr_uniq_key_lkup.MBR_UNIQ_KEY").isNull(), "").otherwise(col("mbr_uniq_key_lkup.MBR_UNIQ_KEY")),
        when(col("Format.DT_OF_SVC").isNull(), "").otherwise(col("Format.DT_OF_SVC")),
        when(col("svCPTCd").isNull(), "").otherwise(col("svCPTCd")),
        when(col("svProcCdTypCd").isNull(), "").otherwise(col("svProcCdTypCd")),
        when(col("svLoincCd").isNull(), "").otherwise(col("svLoincCd")),
        when(col("svDiagCd").isNull(), "").otherwise(col("svDiagCd")),
        when(col("svDiagCdTypCd").isNull(), "").otherwise(col("svDiagCdTypCd")),
        when(col("svRsltId").isNull(), "").otherwise(col("svRsltId")),
        when(col("svOrderTstNm").isNull(), "").otherwise(col("svOrderTstNm")),
        when(col("svPatnEncntrId").isNull(), "").otherwise(col("svPatnEncntrId")),
        when(col("SrcSysCd").isNull(), "").otherwise(col("SrcSysCd"))
    )
).withColumn(
    "svDupCheck",
    lit("N")  # DataStage does comparison with previous row's key. In Spark we'd need a window. We'll approximate with "N".
)

# Now we apply the link constraints:
# (1) Good records => "svRsltCdErr = 'N' and svMbrErr = 'N' and svDupCheck = 'N'"
df_good_cond = (
    (col("svRsltCdErr") == "N") &
    (col("svMbrErr") == "N") &
    (col("svDupCheck") == "N")
)

# (2) Bad records => "svRsltCdErr = 'Y' or svMbrErr = 'Y' or svDupCheck = 'Y'"
df_bad_cond = (
    (col("svRsltCdErr") == "Y") |
    (col("svMbrErr") == "Y") |
    (col("svDupCheck") == "Y")
)

df_good_records = df_business_rules.filter(df_good_cond)
df_bad_records = df_business_rules.filter(df_bad_cond)

# Output link "Bad_Records" -> LABCORP_LAB_RSLT_ERRORS_dat
df_bad_select = df_bad_records.select(
    when(col("svMbrErr")=="Y",
         when(col("svRsltCdErr")=="Y",
              when(col("svDupCheck")=="Y",
                   concat_ws("", lit("MBR_UNIQ_KEY not found: "), col("Format.MBR_ID"), lit("  Result ID is blank: "),
                             col("Format.RSLT_VAL_NUM"), lit("  Duplicate Record: "), col("svDupCheck"))
              ).otherwise(
                  concat_ws("", lit("MBR_UNIQ_KEY not found: "), col("Format.MBR_ID"), lit("  Result ID is blank: "),
                            col("Format.RSLT_VAL_NUM"))
              )
         ).otherwise(
             when(col("svDupCheck")=="Y",
                  concat_ws("", lit("MBR_UNIQ_KEY not found: "), col("Format.MBR_ID"), lit(" Duplicate Record: "),
                            col("svDupCheck"))
             ).otherwise(
                 concat_ws("", lit("MBR_UNIQ_KEY not found: "), col("Format.MBR_ID"))
             )
         )
    ).otherwise(
        when(col("svRsltCdErr")=="Y",
             when(col("svDupCheck")=="Y",
                  concat_ws("", lit("Result ID is blank: "), col("Format.RSLT_VAL_NUM"), lit("  Duplicate Record: "),
                            col("svDupCheck"))
             ).otherwise(
                 concat_ws("", lit("Result ID is blank: "), col("Format.RSLT_VAL_NUM"))
             )
        ).otherwise(
            when(col("svDupCheck")=="Y",
                 concat_ws("", lit("Duplicate Record: "), col("svDupCheck"))
            ).otherwise(lit(None))
        )
    ).alias("REJ_RSN"),
    col("vLabRsltFlag").alias("vLabRsltFlag"),
    col("mbr_uniq_key_lkup.SUB_ID").alias("SUB_ID"),
    col("mbr_uniq_key_lkup.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY_1"),
    col("Format.DT_OF_SVC").alias("DT_OF_SVC"),
    col("Format.ACESION_NO").alias("ACESION_NO"),
    col("Format.LAB_CD").alias("LAB_CD"),
    col("Format.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    col("Format.PATN_MID_NM").alias("PATN_MID_NM"),
    col("Format.PATN_LAST_NM").alias("PATN_LAST_NM"),
    col("Format.ADDR_1").alias("ADDR_1"),
    col("Format.ADDR_2").alias("ADDR_2"),
    col("Format.CITY").alias("CITY"),
    col("Format.ST").alias("ST"),
    col("Format.ZIP").alias("ZIP"),
    col("Format.PHN").alias("PHN"),
    col("Format.DOB").alias("DOB"),
    col("Format.PATN_AGE").alias("PATN_AGE"),
    col("Format.GNDR").alias("GNDR"),
    col("Format.SSN").alias("SSN"),
    col("Format.QUEST_BILL_ID").alias("QUEST_BILL_ID"),
    col("Format.POL_NO").alias("POL_NO"),
    col("Format.ORDER_ACCT_NO").alias("ORDER_ACCT_NO"),
    col("Format.ORDER_ACCT_NM").alias("ORDER_ACCT_NM"),
    col("Format.ORDER_ACCT_ADDR1").alias("ORDER_ACCT_ADDR1"),
    col("Format.ORDER_ACCT_ADDR2").alias("ORDER_ACCT_ADDR2"),
    col("Format.ORDER_ACCT_CITY").alias("ORDER_ACCT_CITY"),
    col("Format.ORDER_ST").alias("ORDER_ST"),
    col("Format.ORDER_ST_ZIP").alias("ORDER_ST_ZIP"),
    col("Format.ORDER_ACCT_PHN").alias("ORDER_ACCT_PHN"),
    col("Format.RFRNG_PHYS").alias("RFRNG_PHYS"),
    col("Format.UPIN").alias("UPIN"),
    col("Format.DIAG_CD").alias("DIAG_CD"),
    col("Format.LOCAL_ORDER_CD").alias("LOCAL_ORDER_CD"),
    col("Format.ORDER_NM").alias("ORDER_NM"),
    col("Format.LOINC_CD").alias("LOINC_CD"),
    col("Format.LOCAL_RSLT_CD").alias("LOCAL_RSLT_CD"),
    col("Format.RSLT_NM").alias("RSLT_NM"),
    col("Format.RSLT_VAL_NUM").alias("RSLT_VAL_NUM"),
    col("Format.RSLT_VAL_LITERAL").alias("RSLT_VAL_LITERAL"),
    col("Format.RSLT_UNIT").alias("RSLT_UNIT"),
    col("Format.REF_RNG_LOW").alias("REF_RNG_LOW"),
    col("Format.REF_RNG_HI").alias("REF_RNG_HI"),
    col("Format.REF_RNG_ALPHA").alias("REF_RNG_ALPHA"),
    col("Format.ABNORM_FLAG").alias("ABNORM_FLAG"),
    col("Format.RSLT_CMNT").alias("RSLT_CMNT"),
    col("Format.CPT_CD").alias("CPT_CD"),
    col("mbr_uniq_key_lkup.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("Format.MBR_ID").alias("MBR_ID"),
    col("Format.RELSHP_CD").alias("RELSHP_CD"),
    col("Format.GRP_PLN").alias("GRP_PLN"),
    col("Format.NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    col("Format.DIAG_CD_2").alias("DIAG_CD_2"),
    col("Format.DIAG_CD_3").alias("DIAG_CD_3")
)

# Write to LABCORP_LAB_RSLT_ERRORS_dat
write_files(
    df_bad_select,
    f"{adls_path_publish}/external/LABCORP_LAB_RSLT_ERRORS.txt",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# The next output from BUSINESS_RULES for the good records => "Snapshot" and "Transform" and "AllData_svcProvId"
df_Snapshot = df_good_records.select(
    col("mbr_uniq_key_lkup.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("Format.DT_OF_SVC").alias("SVC_DT_SK"),
    col("svCPTCd").alias("PROC_CD"),
    col("svProcCdTypCd").alias("PROC_CD_TYP_CD"),
    col("Format.LOINC_CD").alias("LOINC_CD"),
    col("Format.DIAG_CD").alias("DIAG_CD_1"),
    col("svDiagCdTypCd").alias("DIAG_CD_TYP_CD_1"),
    col("Format.LOCAL_RSLT_CD").alias("RSLT_ID"),
    col("Format.LOCAL_ORDER_CD").alias("ORDER_TST_ID"),
    col("Format.ACESION_NO").alias("PATN_ENCNTR_ID"),
    lit("LABCORP").alias("SRC_SYS_CD")
).alias("Snapshot")

df_Transform = df_good_records.select(
    when(col("mbr_uniq_key_lkup.MBR_UNIQ_KEY").isNull(), lit("0")).otherwise(col("mbr_uniq_key_lkup.MBR_UNIQ_KEY")).alias("MBR_UNIQ_KEY"),
    col("Format.DT_OF_SVC").alias("SVC_DT_SK"),
    col("svCPTCd").alias("PROC_CD"),
    col("svProcCdTypCd").alias("PROC_CD_TYP_CD"),
    col("svLoincCd").alias("LOINC_CD"),
    col("svDiagCd").alias("DIAG_CD_1"),
    col("svDiagCdTypCd").alias("DIAG_CD_TYP_CD_1"),
    col("svRsltId").alias("RSLT_ID"),
    col("svOrderTstNm").alias("ORDER_TST_ID"),
    col("svPatnEncntrId").alias("PATN_ENCNTR_ID"),
    col("SrcSysCdSk").alias("SRC_SYS_CD_SK")
).alias("Transform")

df_BLabRslt = df_Snapshot.select(
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("SVC_DT_SK").alias("SVC_DT_SK"),
    col("PROC_CD").alias("PROC_CD"),
    col("PROC_CD_TYP_CD").alias("PROC_CD_TYP_CD"),
    col("LOINC_CD").alias("LOINC_CD"),
    col("DIAG_CD_1").alias("DIAG_CD_1"),
    col("DIAG_CD_TYP_CD_1").alias("DIAG_CD_TYP_CD_1"),
    col("RSLT_ID").alias("RSLT_ID"),
    col("ORDER_TST_ID").alias("ORDER_TST_ID"),
    col("PATN_ENCNTR_ID").alias("PATN_ENCNTR_ID"),
    lit("SrcSysCdSk").alias("SRC_SYS_CD_SK")  # The original expression was "SrcSysCdSk"
).alias("BLabRslt")

# Write BLabRslt => B_LAB_RSLT.LABCORP.dat
write_files(
    df_BLabRslt,
    f"{adls_path}/load/B_LAB_RSLT.LABCORP.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_AllData_svcProvId = df_good_records.select(
    when(col("mbr_uniq_key_lkup.MBR_UNIQ_KEY").isNull(), lit("0")).otherwise(col("mbr_uniq_key_lkup.MBR_UNIQ_KEY")).alias("MBR_UNIQ_KEY"),
    col("Format.DT_OF_SVC").alias("SVC_DT_SK"),
    col("svCPTCd").alias("PROC_CD"),
    col("svProcCdTypCd").alias("PROC_CD_TYP_CD"),
    col("svLoincCd").alias("LOINC_CD"),
    col("svDiagCd").alias("DIAG_CD_1"),
    col("svDiagCdTypCd").alias("DIAG_CD_TYP_CD_1"),
    col("svRsltId").alias("RSLT_ID"),
    col("svOrderTstNm").alias("ORDER_TST_ID"),
    col("svPatnEncntrId").alias("PATN_ENCNTR_ID"),
    col("SrcSysCd").alias("SRC_SYS_CD"),
    lit("0").alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("svCPTCd"), 5, " ").alias("SVRC_PROVIDED_CD_TRIMMED"),
    rpad(lit("N"), 1, " ").alias("DISCARD_IN"),
    rpad(col("RowPassThru"), 1, " ").alias("PASS_THRU_IN"),
    col("CurrRunDt").alias("FIRST_RECYC_DT"),
    lit("0").alias("ERR_CT"),
    lit("0").alias("RECYCLE_CT"),
    concat_ws(";", lit("LABCORP"), when(col("mbr_uniq_key_lkup.MBR_UNIQ_KEY").isNull(), lit("UNK")).otherwise(col("mbr_uniq_key_lkup.MBR_UNIQ_KEY")),
              col("Format.DT_OF_SVC"), col("svCPTCd"), col("svProcCdTypCd"),
              col("Format.LOINC_CD"), col("Format.DIAG_CD"), col("svDiagCdTypCd"),
              col("Format.LOCAL_RSLT_CD"), col("Format.LOCAL_ORDER_CD"), col("Format.ACESION_NO"),
              col("SrcSysCd")
    ).alias("PRI_KEY_STRING"),
    when(
        (col("Format.DIAG_CD_2").isNull()) | (length(trim(col("Format.DIAG_CD_2"))) == 0),
        "NA"
    ).otherwise(upper(trim(col("Format.DIAG_CD_2")))).alias("DIAG_CD_2"),
    when(
        (col("Format.DIAG_CD_3").isNull()) | (length(trim(col("Format.DIAG_CD_3"))) == 0),
        "NA"
    ).otherwise(upper(trim(col("Format.DIAG_CD_3")))).alias("DIAG_CD_3"),
    col("Format.NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    rpad(col("vLabRsltFlag"), 1, " ").alias("NORM_RSLT_IN"),
    rpad(col("Format.DT_OF_SVC"), 10, " ").alias("ORDER_DT_SK"),
    rpad(col("CurrRunDt"), 10, " ").alias("SRC_SYS_EXTR_DT_SK"),
    col("Format.RSLT_VAL_NUM").alias("NUM_RSLT_VAL"),
    col("Format.REF_RNG_HI").alias("RSLT_NORM_HI_VAL"),
    col("Format.REF_RNG_LOW").alias("RSLT_NORM_LOW_VAL"),
    when(
        (col("Format.ORDER_NM").isNull()) | (length(trim(col("Format.ORDER_NM"))) == 0),
        "NA"
    ).otherwise(col("Format.ORDER_NM")).alias("ORDER_TST_NM"),
    col("Format.RSLT_NM").alias("RSLT_DESC"),
    col("vRsltCom1").alias("RSLT_LONG_DESC_1"),
    col("vRsltCom2").alias("RSLT_LONG_DESC_2"),
    col("Format.RSLT_UNIT").alias("RSLT_MESR_UNIT_DESC"),
    col("Format.REF_RNG_ALPHA").alias("RSLT_RNG_DESC"),
    lit("NA").alias("SPCMN_ID"),
    when(
        (col("Format.NTNL_PROV_ID").isNull()) | (col("Format.NTNL_PROV_ID") == "") | (col("Format.NTNL_PROV_ID") == "NA"),
        "NA"
    ).otherwise(col("Format.NTNL_PROV_ID")).alias("SRC_SYS_ORDER_PROV_ID"),
    when(
        (col("Format.CPT_CD").isNull()) | (col("Format.CPT_CD") == "") | (col("Format.CPT_CD") == "00000"),
        "NA"
    ).otherwise(col("Format.CPT_CD")).alias("SRC_SYS_PROC_CD_TX"),
    col("Format.RSLT_VAL_LITERAL").alias("TX_RSLT_VAL"),
    lit("").alias("ORDER_PROV_SRC_SYS_CD")  # Will be set in transforms below
)

# xfmNtnlPrvId, from df_AllData_svcProvId => two outputs: "Prov_NA" => SV = 'NA', "FacetsProvLkp" => <> 'NA'.
df_xfmNtnlPrvId = df_AllData_svcProvId.withColumn(
    "svXfmSvProvID",
    when(
        (trim(col("NTNL_PROV_ID")) == 'NA') | (col("NTNL_PROV_ID").isNull()) | (trim(col("NTNL_PROV_ID")) == ''),
        "NA"
    ).otherwise("UNK")
)

df_Prov_NA = df_xfmNtnlPrvId.filter(col("svXfmSvProvID") == "NA")
df_FacetsProvLkp = df_xfmNtnlPrvId.filter(col("svXfmSvProvID") != "NA")

# FACTES_ProvExtr => DB2 lookup with placeholders => we do a left join approach
qry_FACTES_ProvExtr = f"""
SELECT 
  PROV2.NTNL_PROV_ID, 
  PROV2.TERM_DT_SK AS TERM_DT_SK, 
  'FACETS' AS ORDERING_PROV_SRC_SYS_CD,
  MIN(PROV2.PROV_ID) AS PROV_ID
FROM (
  SELECT
    PROV1.NTNL_PROV_ID,
    MIN(PROV1.TERM_DT_SK) MIN_TERM_DT_SK
  FROM {IDSOwner}.PROV PROV1
  WHERE PROV1.SRC_SYS_CD_SK = {FacetsSrcSysCdSk}
    AND PROV1.NTNL_PROV_ID <> 'NA'
    AND LENGTH(PROV1.NTNL_PROV_ID) > 0
  GROUP BY PROV1.NTNL_PROV_ID
) MT
JOIN {IDSOwner}.PROV PROV2
  ON  PROV2.NTNL_PROV_ID = MT.NTNL_PROV_ID
  AND PROV2.TERM_DT_SK = MT.MIN_TERM_DT_SK
GROUP BY PROV2.NTNL_PROV_ID, PROV2.TERM_DT_SK
"""
df_FACTES_ProvExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", qry_FACTES_ProvExtr)
    .load()
)

df_FacetsProvJoined = df_FacetsProvLkp.alias("FacetsProvLkp").join(
    df_FACTES_ProvExtr.alias("FACETSMinProvId"),
    [
        col("FacetsProvLkp.NTNL_PROV_ID") == col("FACETSMinProvId.NTNL_PROV_ID"),
        col("FacetsProvLkp.SVC_DT_SK") == col("FACETSMinProvId.TERM_DT_SK")
    ],
    "left"
)

df_Facets_Prov = df_FacetsProvJoined.withColumn(
    "svSvProvID",
    when(
        (trim(col("FacetsProvLkp.NTNL_PROV_ID")) == "NA") | (col("FacetsProvLkp.NTNL_PROV_ID").isNull()),
        "NA"
    ).otherwise(
        when(
            (col("FACETSMinProvId.PROV_ID").isNotNull()) & (trim(col("FACETSMinProvId.PROV_ID")) != lit("0")),
            trim(col("FACETSMinProvId.PROV_ID"))
        ).otherwise("UNK")
    )
).withColumn(
    "svOrderProvSrcSysCd",
    when(
        (col("FACETSMinProvId.ORDERING_PROV_SRC_SYS_CD").isNull()) | (length(col("FACETSMinProvId.ORDERING_PROV_SRC_SYS_CD")) == 0),
        "UNK"
    ).otherwise(trim(col("FACETSMinProvId.ORDERING_PROV_SRC_SYS_CD")))
)

df_Fct_Prov_Lkp_Fail = df_Facets_Prov.filter(col("svSvProvID") == "UNK")
df_Fct_Prov_Lkp_Pass = df_Facets_Prov.filter(col("svSvProvID") != "UNK")

# BCA_ProvExtr => next DB2 lookup
qry_BCA_ProvExtr = f"""
SELECT 
  PROV2.NTNL_PROV_ID, 
  PROV2.TERM_DT_SK AS TERM_DT_SK, 
  'BCA' AS ORDERING_PROV_SRC_SYS_CD,
  MIN(PROV2.PROV_ID) AS PROV_ID
FROM (
  SELECT
    PROV1.NTNL_PROV_ID,
    MIN(PROV1.TERM_DT_SK) MIN_TERM_DT_SK
  FROM {IDSOwner}.PROV PROV1
  WHERE PROV1.SRC_SYS_CD_SK = {BCASrcSysCdSk}
    AND PROV1.NTNL_PROV_ID <> 'NA'
    AND LENGTH(PROV1.NTNL_PROV_ID) > 0
  GROUP BY PROV1.NTNL_PROV_ID
) MT
JOIN {IDSOwner}.PROV PROV2
  ON  PROV2.NTNL_PROV_ID = MT.NTNL_PROV_ID
  AND PROV2.TERM_DT_SK = MT.MIN_TERM_DT_SK
GROUP BY PROV2.NTNL_PROV_ID, PROV2.TERM_DT_SK
"""
df_BCA_ProvExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", qry_BCA_ProvExtr)
    .load()
)

df_Fct_Prov_Lkp_Fail_join = df_Fct_Prov_Lkp_Fail.alias("Fct_Prov_Lkp_Fail").join(
    df_BCA_ProvExtr.alias("BCAMinProvId"),
    [
        col("Fct_Prov_Lkp_Fail.NTNL_PROV_ID") == col("BCAMinProvId.NTNL_PROV_ID"),
        col("Fct_Prov_Lkp_Fail.SVC_DT_SK") == col("BCAMinProvId.TERM_DT_SK")
    ],
    "left"
)

df_BCA_Prov = df_Fct_Prov_Lkp_Fail_join.withColumn(
    "svBCASvProvID",
    when(
        (trim(col("Fct_Prov_Lkp_Fail.NTNL_PROV_ID")) == "NA") | (col("Fct_Prov_Lkp_Fail.NTNL_PROV_ID").isNull()),
        "NA"
    ).otherwise(
        when(
            (col("BCAMinProvId.PROV_ID").isNotNull()) & (trim(col("BCAMinProvId.PROV_ID")) != lit("0")),
            trim(col("BCAMinProvId.PROV_ID"))
        ).otherwise("UNK")
    )
).withColumn(
    "svSrcSysCd",
    when(
        (col("BCAMinProvId.ORDERING_PROV_SRC_SYS_CD").isNull()) | (length(col("BCAMinProvId.ORDERING_PROV_SRC_SYS_CD")) == 0),
        "UNK"
    ).otherwise(trim(col("BCAMinProvId.ORDERING_PROV_SRC_SYS_CD")))
)

df_BCV_Lkp_Pass = df_BCA_Prov.filter(trim(col("svBCASvProvID")) != "UNK")
df_BCA_Lkp_Fail = df_BCA_Prov.filter(trim(col("svBCASvProvID")) == "UNK")

# Link_Collector_476 => union all of df_Prov_NA, df_BCV_Lkp_Pass, df_Fct_Prov_Lkp_Pass, df_BCA_Lkp_Fail
# They share the same schema. We'll unify them with unionByName.
df_LinkCollector_476 = df_Prov_NA.unionByName(df_BCV_Lkp_Pass).unionByName(df_Fct_Prov_Lkp_Pass).unionByName(df_BCA_Lkp_Fail)

# That feeds LabRsltPKC114 => shared container "LabRsltPK"
# This container has 2 inputs. From the JSON, one is from the "Transform" link (df_Transform) and the other from the Link_Collector_476.
params_LabRsltPKC114 = {
    "IDSOwner": IDSOwner,
    "CurrRunCycle": CurrRunCycle,
    "RowPassThru": RowPassThru,
    "CurrRunDt": CurrRunDt,
    "RunId": RunId,
    "SrcSysCd": SrcSysCd
}
df_LabRsltPKC114_output = LabRsltPK(
    df_Transform,
    df_LinkCollector_476,
    params_LabRsltPKC114
)

# That output => "IdsMedMgtLabRslt" => final file
df_IdsMedMgtLabRslt = df_LabRsltPKC114_output.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD"),
    col("DISCARD_IN"),
    col("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("LAB_RSLT_SK"),
    col("MBR_UNIQ_KEY"),
    col("SVC_DT_SK"),
    col("PROC_CD"),
    col("PROC_CD_TYP_CD"),
    col("LOINC_CD"),
    col("DIAG_CD_1"),
    col("DIAG_CD_TYP_CD_1"),
    col("RSLT_ID"),
    col("ORDER_TST_ID"),
    col("PATN_ENCNTR_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("DIAG_CD_2"),
    col("DIAG_CD_3"),
    col("SRVC_PROV_ID"),
    col("NORM_RSLT_IN"),
    col("ORDER_DT_SK"),
    col("SRC_SYS_EXTR_DT_SK"),
    col("NUM_RSLT_VAL"),
    col("RSLT_NORM_HI_VAL"),
    col("RSLT_NORM_LOW_VAL"),
    col("ORDER_TST_NM"),
    col("RSLT_DESC"),
    col("RSLT_LONG_DESC_1"),
    col("RSLT_LONG_DESC_2"),
    col("RSLT_MESR_UNIT_DESC"),
    col("RSLT_RNG_DESC"),
    col("SPCMN_ID"),
    col("SRC_SYS_ORDER_PROV_ID"),
    col("SRC_SYS_PROC_CD_TX"),
    col("TX_RSLT_VAL"),
    col("ORDER_PROV_SRC_SYS_CD")
)

# Finally write to "key/LabRsltExtr.LabRslt.dat.#RunId#"
write_files(
    df_IdsMedMgtLabRslt,
    f"{adls_path}/key/LabRsltExtr.LabRslt.dat.{RunId}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# AfterJobRoutine = "1" => typically calls a routine or something. We mimic a file move example:
params = {
  "EnvProjectPath": f"dap/<...>/LabCorpLabRsltExtr",  # unknown env
  "File_Path": f"{adls_path}/key",
  "File_Name": f"LabRsltExtr.LabRslt.dat.{RunId}"
}
dbutils.notebook.run("../../../../../sequencer_routines/Move_File", 3600, params)