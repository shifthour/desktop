# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Job Name :  DominionIdsProvExtr
# MAGIC Calling Job: DominionProvExtrSeq
# MAGIC 
# MAGIC                            
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                        Date                 Project/Altiris #               Change Description                                                           Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------      --------------------------------       ---------------------------------------------------------                                ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Deepika C                       2023-11-24       US 600544                      Initial Programming                                                              IntegrateSITF              Jeyaprasanna               2024-01-18

# MAGIC Duplicates are removed on the provider identifier to pick the row corresponding to the latest Effective_Date
# MAGIC Dupilcates are removed to retain 1 entry for each address
# MAGIC Job Name: DominionIdsProvExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, when, substring, concat, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Parameters
SrcSysCd = get_widget_value("SrcSysCd","")
RunID = get_widget_value("RunID","")
DominionProvFile = get_widget_value("DominionProvFile","")

# Schema for DominionProvFile
DominionProvFile_schema = StructType([
    StructField("Last_Name", StringType(), True),
    StructField("First_Name", StringType(), True),
    StructField("Middle_Name", StringType(), True),
    StructField("Suffix", StringType(), True),
    StructField("Specialty", StringType(), True),
    StructField("Board_Certified_Indicator", StringType(), True),
    StructField("Address", StringType(), True),
    StructField("City", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Zip_Code", StringType(), True),
    StructField("County", StringType(), True),
    StructField("Phone_Number", StringType(), True),
    StructField("Fax_Number", StringType(), True),
    StructField("National_Provider_Identifer", StringType(), True),
    StructField("Prescriber_Flag", StringType(), True),
    StructField("Provider_DEA", StringType(), True),
    StructField("Provider_Identifier", StringType(), True),
    StructField("Network_Identifier", StringType(), True),
    StructField("Provider_Status", StringType(), True),
    StructField("Effective_Date", StringType(), True),
    StructField("Term_Date", StringType(), True),
    StructField("Primary_Location_Indicator", StringType(), True),
    StructField("Provider_Address_Identifier", StringType(), True),
    StructField("Provider_Tax_Identifier", StringType(), True),
    StructField("Provider_Taxonomy_Code", StringType(), True),
    StructField("Handicap_Access_Indicator", StringType(), True),
    StructField("Cultural_Capability_Name", StringType(), True),
    StructField("Language_Description", StringType(), True)
])

# Read DominionProvFile
df_DominionProvFile = (
    spark.read
    .schema(DominionProvFile_schema)
    .option("header", True)
    .option("sep", "|")
    .option("quote", None)
    .csv(f"{adls_path_raw}/landing/{DominionProvFile}")
)

# Apply Business Rules (Transformer Stage)
df_businessRules = (
    df_DominionProvFile
    .withColumn("svLastName", trim(col("Last_Name")))
    .withColumn("svFistName", trim(col("First_Name")))
    .withColumn("svMiddleName", trim(col("Middle_Name")))
    .withColumn("svSuffix", trim(col("Suffix")))
    .withColumn("SvSpecialty", trim(col("Specialty")))
    .withColumn("svBrdCertified", trim(col("Board_Certified_Indicator")))
    .withColumn("svAddress", UpCase(trim(substring(col("Address"), 1, 40))))
    .withColumn("svCity", UpCase(trim(substring(col("City"), 1, 35))))
    .withColumn("svState", UpCase(trim(col("State"))))
    .withColumn("svZipCode", UpCase(trim(col("Zip_Code"))))
    .withColumn("svCounty", UpCase(trim(col("County"))))
    .withColumn("svPhoneNumber", UpCase(trim(col("Phone_Number"))))
    .withColumn("svFaxNumber", UpCase(trim(col("Fax_Number"))))
    .withColumn("svNPI", trim(col("National_Provider_Identifer")))
    .withColumn("svPresFlag", trim(col("Prescriber_Flag")))
    .withColumn("svDEA", trim(col("Provider_DEA")))
    .withColumn("svProvIdentifier", trim(col("Provider_Identifier")))
    .withColumn("svNtwkIdentifier", trim(col("Network_Identifier")))
    .withColumn("svProvStatus", trim(col("Provider_Status")))
    .withColumn(
        "svEffDt",
        when(trim(col("Effective_Date")) == "", "")
        .otherwise(
            concat(
                substring(trim(col("Effective_Date")), 1, 4), lit("-"),
                substring(trim(col("Effective_Date")), 6, 2), lit("-"),
                substring(trim(col("Effective_Date")), 9, 2)
            )
        )
    )
    .withColumn(
        "svTermDt",
        when(trim(col("Term_Date")) == "", "")
        .otherwise(
            concat(
                substring(trim(col("Term_Date")), 1, 4), lit("-"),
                substring(trim(col("Term_Date")), 6, 2), lit("-"),
                substring(trim(col("Term_Date")), 9, 2)
            )
        )
    )
    .withColumn("svPrimaryLocIndicator", trim(col("Primary_Location_Indicator")))
    .withColumn("svProvAddrIdentifier", trim(col("Provider_Address_Identifier")))
    .withColumn("svProvTaxIdentifier", trim(col("Provider_Tax_Identifier")))
    .withColumn("svProvTaxCode", trim(col("Provider_Taxonomy_Code")))
    .withColumn("svHandicapAccIndicator", trim(col("Handicap_Access_Indicator")))
    .withColumn("svCultCapName", trim(col("Cultural_Capability_Name")))
    .withColumn("svLangDesc", trim(col("Language_Description")))
)

# PROV link
df_prov = df_businessRules.select(
    col("svLastName").alias("Last_Name"),
    col("svFistName").alias("First_Name"),
    col("svMiddleName").alias("Middle_Name"),
    col("svSuffix").alias("Suffix"),
    col("SvSpecialty").alias("Specialty"),
    col("svBrdCertified").alias("Board_Certified_Indicator"),
    col("svNPI").alias("National_Provider_Identifer"),
    col("svPresFlag").alias("Prescriber_Flag"),
    col("svDEA").alias("Provider_DEA"),
    (col("svProvIdentifier") + col("svProvAddrIdentifier")).alias("Provider_Identifier"),
    col("svNtwkIdentifier").alias("Network_Identifier"),
    col("svProvStatus").alias("Provider_Status"),
    col("svEffDt").alias("Effective_Date"),
    col("svTermDt").alias("Term_Date"),
    col("svState").alias("State"),
    (col("svProvIdentifier") + col("svProvAddrIdentifier")).alias("Provider_Address_Identifier"),
    col("svProvTaxIdentifier").alias("Provider_Tax_Identifier"),
    col("svProvTaxCode").alias("Provider_Taxonomy_Code")
)

# ADDR link
df_addr = df_businessRules.select(
    (col("svProvIdentifier") + col("svProvAddrIdentifier")).alias("Provider_Identifier"),
    col("svAddress").alias("Address"),
    col("svCity").alias("City"),
    col("svState").alias("State"),
    col("svZipCode").alias("Zip_Code"),
    col("svCounty").alias("County"),
    col("svPhoneNumber").alias("Phone_Number"),
    col("svFaxNumber").alias("Fax_Number"),
    col("svEffDt").alias("Effective_Date"),
    col("svTermDt").alias("Term_Date"),
    col("svPrimaryLocIndicator").alias("Primary_Location_Indicator"),
    col("svHandicapAccIndicator").alias("Handicap_Access_Indicator")
)

# Rmdup_prov
df_rmdup_prov = dedup_sort(
    df_prov,
    partition_cols=["Provider_Identifier"],
    sort_cols=[("Provider_Identifier", "A"), ("Effective_Date", "D")]
)

# ds_PROV_Dominion (write to parquet)
df_prov_final = df_rmdup_prov.select(
    rpad(col("Last_Name"), 255, " ").alias("Last_Name"),
    rpad(col("First_Name"), 255, " ").alias("First_Name"),
    rpad(col("Middle_Name"), 255, " ").alias("Middle_Name"),
    rpad(col("Suffix"), 255, " ").alias("Suffix"),
    rpad(col("Specialty"), 255, " ").alias("Specialty"),
    rpad(col("Board_Certified_Indicator"), 255, " ").alias("Board_Certified_Indicator"),
    rpad(col("National_Provider_Identifer"), 255, " ").alias("National_Provider_Identifer"),
    rpad(col("Prescriber_Flag"), 255, " ").alias("Prescriber_Flag"),
    rpad(col("Provider_DEA"), 255, " ").alias("Provider_DEA"),
    rpad(col("Provider_Identifier"), 255, " ").alias("Provider_Identifier"),
    rpad(col("Network_Identifier"), 255, " ").alias("Network_Identifier"),
    rpad(col("Provider_Status"), 255, " ").alias("Provider_Status"),
    rpad(col("Effective_Date"), 255, " ").alias("Effective_Date"),
    rpad(col("Term_Date"), 255, " ").alias("Term_Date"),
    rpad(col("State"), 255, " ").alias("State"),
    rpad(col("Provider_Address_Identifier"), 255, " ").alias("Provider_Address_Identifier"),
    rpad(col("Provider_Tax_Identifier"), 255, " ").alias("Provider_Tax_Identifier"),
    rpad(col("Provider_Taxonomy_Code"), 255, " ").alias("Provider_Taxonomy_Code")
)

write_files(
    df_prov_final,
    f"PROV.{SrcSysCd}.extr.{RunID}.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

# Rm_Dup_Addr
df_rmdup_addr = dedup_sort(
    df_addr,
    partition_cols=["Provider_Identifier", "Effective_Date", "Address", "City", "State", "Zip_Code"],
    sort_cols=[
        ("Provider_Identifier", "A"),
        ("Effective_Date", "A"),
        ("Address", "A"),
        ("City", "A"),
        ("State", "A"),
        ("Zip_Code", "A")
    ]
)

# ds_PROV_ADDR_Dominion (write to parquet)
df_addr_final = df_rmdup_addr.select(
    rpad(col("Provider_Identifier"), 255, " ").alias("Provider_Identifier"),
    rpad(col("Address"), 255, " ").alias("Address"),
    rpad(col("City"), 255, " ").alias("City"),
    rpad(col("State"), 255, " ").alias("State"),
    rpad(col("Zip_Code"), 255, " ").alias("Zip_Code"),
    rpad(col("County"), 255, " ").alias("County"),
    rpad(col("Phone_Number"), 255, " ").alias("Phone_Number"),
    rpad(col("Fax_Number"), 255, " ").alias("Fax_Number"),
    rpad(col("Effective_Date"), 255, " ").alias("Effective_Date"),
    rpad(col("Term_Date"), 255, " ").alias("Term_Date"),
    rpad(col("Primary_Location_Indicator"), 1, " ").alias("Primary_Location_Indicator"),
    rpad(col("Handicap_Access_Indicator"), 255, " ").alias("Handicap_Access_Indicator")
)

write_files(
    df_addr_final,
    f"PROV_ADDR.{SrcSysCd}.extr.{RunID}.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)