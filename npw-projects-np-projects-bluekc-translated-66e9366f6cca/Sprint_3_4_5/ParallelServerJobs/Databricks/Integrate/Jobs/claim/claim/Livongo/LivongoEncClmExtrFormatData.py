# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2007, 2020 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by: LivongoEncClmInitialExtrSeq
# MAGIC                    
# MAGIC 
# MAGIC Processing:Source Input file for Encounter Procedure  file are extracted and formatted
# MAGIC                     
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:  Restore the source file before re-running the job
# MAGIC                     Previous Run Aborted: Restart, no other steps necessary  
# MAGIC Modifications:                        
# MAGIC                                                    \(9)\(9)Project/                                                                                                                             \(9)\(9)Code                   \(9)\(9)Date
# MAGIC Developer           \(9)\(9)Date                \(9)Altiris #        \(9)Change Description                                                                   \(9)               Reviewer            \(9)\(9)Reviewed
# MAGIC -------------------------  \(9)\(9)---------------------   \(9)----------------   \(9)-----------------------------------------------------------------------------------------------------------------     \(9)-------------------------  \(9)\(9)-------------------
# MAGIC SravyaSree Yarlagadda    \(9)2020-12-09\(9)   311337      \(9) Original Programming.  \(9)\(9)                                                                 Jaideep Mankala                               03/18/2021
# MAGIC Naveen N                   \(9)2021-04-17\(9)   362808      \(9) Added Acknowledgement Notification.  \(9)\(9)                                                  Kalyan Neelam                       2021-04-22
# MAGIC Vikas A                                    2021-05-25                 RA                         Added Member Only Lookup                                                                                    Manasa Andru                                     2021-06-02        
# MAGIC Vikas Abbu                              2021-08-28                423921                   CLM ID concatenated with Livongo ID                          
# MAGIC                                                                                                                  and Servicedate  in Xfm_Formatfile                                   IntegrateDev2              Jaideep Mankala                                    09/01/2021     
# MAGIC Shreya M B                             2022-01-18           US-482859                  Claim Member Match to remove Prefix and only 
# MAGIC                                                                                                                 match with Sub ID                                                              IntegrateDevB/2          Jeyaprasanna                             2021-01-22        
# MAGIC HariKrishnaRao Yadav           2022-02-02           US 482209                  Added columns VENDOR_NAME, FILE_NM,                    IntegrateSITF               Jeyaprasanna                          2022-02-10
# MAGIC                                                                                                                 DTL_TYP_CD, DTL_SUBTYP_TX  in Error File 
# MAGIC                                                                                                                 and Added Success File

# MAGIC Livongo Encounter Claims Format Data.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Retrieve job parameters
InFile = get_widget_value("InFile", "")
InFile_F = get_widget_value("InFile_F", "")
IDSOwner = get_widget_value("IDSOwner", "")
ids_secret_name = get_widget_value("ids_secret_name", "")

# Configure DB connection for IDS
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

# Read from DB: MBR_SUB_Only
extract_query_mbr_sub_only = (
    f"SELECT \n"
    f"PFX.ALPHA_PFX_CD||SUB.SUB_ID AS MEMBER_ID,\n"
    f"SUB.SUB_ID AS SUB_ID\n"
    f"FROM \n{IDSOwner}.MBR MBR\n"
    f"INNER JOIN {IDSOwner}.SUB SUB\n"
    f"ON MBR.SUB_SK = SUB.SUB_SK\n"
    f"INNER JOIN {IDSOwner}.ALPHA_PFX PFX\n"
    f"ON SUB.ALPHA_PFX_SK= PFX.ALPHA_PFX_SK\n"
    f"INNER JOIN {IDSOwner}.GRP GRP \n"
    f"ON GRP.GRP_SK = SUB.GRP_SK\n"
    f"INNER JOIN {IDSOwner}.CLNT CLNT \n"
    f"ON CLNT.CLNT_SK = GRP.CLNT_SK\n"
    f"AND CLNT.CLNT_ID = 'MA'"
)
df_MBR_SUB_Only = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_mbr_sub_only)
    .load()
)

# Read from DB: MBR_MATCH
extract_query_mbr_match = (
    f"SELECT \n"
    f"PFX.ALPHA_PFX_CD||SUB.SUB_ID AS MEMBER_ID,\n"
    f"SUB.SUB_ID AS SUB_ID\n"
    f"FROM \n{IDSOwner}.MBR MBR\n"
    f"INNER JOIN {IDSOwner}.SUB SUB\n"
    f"ON MBR.SUB_SK = SUB.SUB_SK\n"
    f"INNER JOIN {IDSOwner}.ALPHA_PFX PFX\n"
    f"ON SUB.ALPHA_PFX_SK= PFX.ALPHA_PFX_SK\n"
    f"INNER JOIN {IDSOwner}.MBR_ENR ME\n"
    f"ON MBR.MBR_SK=ME.MBR_SK\n"
    f"AND ME.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = 1949\n"
    f"INNER JOIN {IDSOwner}.GRP GRP \n"
    f"ON GRP.GRP_SK = SUB.GRP_SK\n"
    f"INNER JOIN {IDSOwner}.CLNT CLNT \n"
    f"ON CLNT.CLNT_SK = GRP.CLNT_SK\n"
    f"AND CLNT.CLNT_ID = 'MA'\n"
    f"LEFT OUTER JOIN  {IDSOwner}.CLS_PLN CLSPLN\n"
    f"ON ME.CLS_PLN_SK= CLSPLN.CLS_PLN_SK\n"
    f"LEFT OUTER JOIN {IDSOwner}.PROD P\n"
    f"ON ME.PROD_SK=P.PROD_SK\n"
    f"LEFT OUTER JOIN {IDSOwner}.EXPRNC_CAT CAT\n"
    f"ON P.EXPRNC_CAT_SK=CAT.EXPRNC_CAT_SK\n"
    f"LEFT OUTER JOIN {IDSOwner}.FNCL_LOB LOB\n"
    f"ON P.FNCL_LOB_SK = LOB.FNCL_LOB_SK\n"
    f"WHERE ME.term_dt_sk >= ((SELECT CAST(dbo.GetDateCST() AS DATE) FROM sysibm.sysdummy1))"
)
df_MBR_MATCH = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_mbr_match)
    .load()
)

# Read Liv_Enc_Src_File
schema_Liv_Enc_Src_File = StructType([
    StructField("livongo_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("birth_date", StringType(), True),
    StructField("client_name", StringType(), True),
    StructField("member_id", StringType(), True),
    StructField("claim_code", StringType(), True),
    StructField("service_date", StringType(), True),
    StructField("quantity", StringType(), True)
])
df_Liv_Enc_Src_File = (
    spark.read.format("csv")
    .option("header", True)
    .option("sep", ",")
    .option("quote", "\"")
    .option("nullValue", None)
    .schema(schema_Liv_Enc_Src_File)
    .load(f"{adls_path_raw}/landing/{InFile}")
)

# xfrm_Format_File
df_xfrm_Format_File_filtered = df_Liv_Enc_Src_File.filter(
    F.substring(trim(F.col("claim_code")), 1, 1) == "A"
)
df_xfrm_Format_File = df_xfrm_Format_File_filtered.select(
    F.concat(
        trim(F.col("livongo_id")),
        F.substring(trim(F.col("service_date")), 9, 2),
        F.substring(trim(F.col("service_date")), 1, 2),
        F.substring(trim(F.col("service_date")), 4, 2)
    ).alias("livongo_id"),
    trim(F.col("first_name")).alias("first_name"),
    trim(F.col("last_name")).alias("last_name"),
    trim(F.col("birth_date")).alias("birth_date"),
    trim(F.col("client_name")).alias("client_name"),
    F.upper(trim(F.col("member_id"))).alias("member_id"),
    F.col("claim_code").alias("claim_code"),
    F.concat(
        F.substring(trim(F.col("service_date")), 7, 4),
        F.lit("-"),
        F.substring(trim(F.col("service_date")), 1, 2),
        F.lit("-"),
        F.substring(trim(F.col("service_date")), 4, 2)
    ).alias("service_date"),
    trim(F.col("quantity")).alias("quantity"),
    F.substring(trim(F.col("member_id")), 4, 2000).alias("SUB_ID")
)

# LKP_MbrMatch
df_LKP_MbrMatch = df_xfrm_Format_File.alias("Lnk_Lkp").join(
    df_MBR_MATCH.alias("Lnk_MbrLkp"),
    (
        (F.col("Lnk_Lkp.member_id") == F.col("Lnk_MbrLkp.MEMBER_ID")) &
        (F.col("Lnk_Lkp.SUB_ID") == F.col("Lnk_MbrLkp.SUB_ID"))
    ),
    "left"
).select(
    F.col("Lnk_Lkp.livongo_id").alias("livongo_id"),
    F.col("Lnk_Lkp.first_name").alias("first_name"),
    F.col("Lnk_Lkp.last_name").alias("last_name"),
    F.col("Lnk_Lkp.birth_date").alias("birth_date"),
    F.col("Lnk_Lkp.client_name").alias("client_name"),
    F.col("Lnk_Lkp.member_id").alias("member_id"),
    F.col("Lnk_Lkp.claim_code").alias("claim_code"),
    F.col("Lnk_Lkp.service_date").alias("service_date"),
    F.col("Lnk_Lkp.quantity").alias("quantity"),
    F.col("Lnk_MbrLkp.MEMBER_ID").alias("MEMBER_ID1"),
    F.col("Lnk_MbrLkp.SUB_ID").alias("SUB_ID")
)

# xfm_NullChck
df_xfm_NullChck_stagevar = df_LKP_MbrMatch.withColumn(
    "svNullChk",
    F.when(
        (F.col("MEMBER_ID1").isNull()) | (F.length(trim(F.col("MEMBER_ID1"))) == 0),
        F.lit("Member Match Not Found")
    ).otherwise(
        F.when(
            (F.col("service_date").isNull()) |
            (F.length(trim(F.col("service_date"))) == 0),
            F.lit("SERVICE_DATE Not Found")
        ).otherwise(F.lit("0"))
    )
)

df_xfm_NullChck_OK = df_xfm_NullChck_stagevar.filter(F.col("svNullChk") == "0").select(
    F.col("livongo_id").alias("livongo_id"),
    F.col("first_name").alias("first_name"),
    F.col("last_name").alias("last_name"),
    F.col("birth_date").alias("birth_date"),
    F.col("client_name").alias("client_name"),
    F.upper(F.col("member_id")).alias("member_id"),
    F.col("claim_code").alias("claim_code"),
    F.col("service_date").alias("service_date"),
    F.col("quantity").alias("quantity")
)

df_xfm_NullChck_NOK = df_xfm_NullChck_stagevar.filter(F.col("svNullChk") != "0").select(
    F.col("livongo_id").alias("CLM_ID"),
    F.col("svNullChk").alias("ERROR_REASON"),
    F.col("livongo_id").alias("livongo_id"),
    F.col("first_name").alias("first_name"),
    F.col("last_name").alias("last_name"),
    F.col("birth_date").alias("birth_date"),
    F.col("client_name").alias("client_name"),
    F.col("member_id").alias("member_id"),
    F.col("claim_code").alias("claim_code"),
    F.col("service_date").alias("service_date"),
    F.col("quantity").alias("quantity"),
    F.col("MEMBER_ID1").alias("MEMBER_ID1"),
    F.col("SUB_ID").alias("SUB_ID")
)

# LKP_Mbr_Only
df_LKP_Mbr_Only = df_xfm_NullChck_NOK.alias("Lnk_Mbr_NotFound").join(
    df_MBR_SUB_Only.alias("Lnk_MbrLkp"),
    (
        (F.col("Lnk_Mbr_NotFound.member_id") == F.col("Lnk_MbrLkp.MEMBER_ID")) &
        (F.col("Lnk_Mbr_NotFound.SUB_ID") == F.col("Lnk_MbrLkp.SUB_ID"))
    ),
    "left"
).select(
    F.col("Lnk_Mbr_NotFound.CLM_ID").alias("CLM_ID"),
    F.col("Lnk_Mbr_NotFound.ERROR_REASON").alias("ERROR_REASON"),
    F.col("Lnk_Mbr_NotFound.livongo_id").alias("livongo_id"),
    F.col("Lnk_Mbr_NotFound.first_name").alias("first_name"),
    F.col("Lnk_Mbr_NotFound.last_name").alias("last_name"),
    F.col("Lnk_Mbr_NotFound.birth_date").alias("birth_date"),
    F.col("Lnk_Mbr_NotFound.client_name").alias("client_name"),
    F.col("Lnk_Mbr_NotFound.member_id").alias("member_id"),
    F.col("Lnk_Mbr_NotFound.claim_code").alias("claim_code"),
    F.col("Lnk_Mbr_NotFound.service_date").alias("service_date"),
    F.col("Lnk_Mbr_NotFound.quantity").alias("quantity"),
    F.col("Lnk_MbrLkp.MEMBER_ID").alias("MEMBER_ID1"),
    F.col("Lnk_MbrLkp.SUB_ID").alias("SUB_ID")
)

# xfm_NullChck1
df_xfm_NullChck1_stagevar = df_LKP_Mbr_Only.withColumn(
    "svNullChk",
    F.when(
        (F.col("MEMBER_ID1").isNull()) | (F.length(trim(F.col("MEMBER_ID1"))) == 0),
        F.lit("Member Match Not Found")
    ).otherwise(
        F.when(
            trim(F.col("ERROR_REASON")) != F.lit("Member Match Not Found"),
            F.col("ERROR_REASON")
        ).otherwise(F.lit("0"))
    )
)

df_xfm_NullChck1_OK = df_xfm_NullChck1_stagevar.filter(F.col("svNullChk") == "0").select(
    F.col("livongo_id").alias("livongo_id"),
    F.col("first_name").alias("first_name"),
    F.col("last_name").alias("last_name"),
    F.col("birth_date").alias("birth_date"),
    F.col("client_name").alias("client_name"),
    F.upper(F.col("member_id")).alias("member_id"),
    F.col("claim_code").alias("claim_code"),
    F.col("service_date").alias("service_date"),
    F.col("quantity").alias("quantity")
)

df_xfm_NullChck1_ERR = df_xfm_NullChck1_stagevar.filter(F.col("svNullChk") != "0").select(
    F.lit("LIVONGO").alias("VENDOR_NAME"),
    F.col("InFile").alias("FILE_NM"),  # Using the parameter name as expression
    F.lit("CLAIM_ERROR").alias("DTL_TYP_CD"),
    F.col("livongo_id").alias("DTL_TYP_TX"),
    F.lit("1").alias("DTL_SUBTYP_TX"),
    F.col("svNullChk").alias("ERR_DESC")
).withColumn("FILE_NM", F.lit(InFile))

# Seq_Error_File write
df_error_file_to_write = df_xfm_NullChck1_ERR.select(
    F.rpad(F.col("VENDOR_NAME"), <...>, " ").alias("VENDOR_NAME"),
    F.rpad(F.col("FILE_NM"), <...>, " ").alias("FILE_NM"),
    F.rpad(F.col("DTL_TYP_CD"), <...>, " ").alias("DTL_TYP_CD"),
    F.rpad(F.col("DTL_TYP_TX"), <...>, " ").alias("DTL_TYP_TX"),
    F.rpad(F.col("DTL_SUBTYP_TX"), <...>, " ").alias("DTL_SUBTYP_TX"),
    F.rpad(F.col("ERR_DESC"), <...>, " ").alias("ERR_DESC")
)
write_files(
    df_error_file_to_write,
    f"{adls_path}/verified/ERROR_{InFile}",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote=None,
    nullValue=None
)

# Funnel (union) of df_xfm_NullChck_OK and df_xfm_NullChck1_OK
df_funnel = df_xfm_NullChck_OK.unionByName(df_xfm_NullChck1_OK)

# xfm_Success => no transformation for the first link
df_Lnk_All_Rec = df_funnel.select(
    F.col("livongo_id"),
    F.col("first_name"),
    F.col("last_name"),
    F.col("birth_date"),
    F.col("client_name"),
    F.col("member_id"),
    F.col("claim_code"),
    F.col("service_date"),
    F.col("quantity")
)

# xfm_Success => second link
df_Lnk_Success = df_funnel.select(
    F.lit("LIVONGO").alias("VENDOR_NAME"),
    F.col("livongo_id").alias("DTL_TYP_TX"),
    F.lit("1").alias("DTL_SUBTYP_TX"),
    F.lit(InFile).alias("FILE_NM")
)

# Write Format_File
df_format_file_to_write = df_Lnk_All_Rec.select(
    F.rpad(F.col("livongo_id"), <...>, " ").alias("livongo_id"),
    F.rpad(F.col("first_name"), <...>, " ").alias("first_name"),
    F.rpad(F.col("last_name"), <...>, " ").alias("last_name"),
    F.rpad(F.col("birth_date"), <...>, " ").alias("birth_date"),
    F.rpad(F.col("client_name"), <...>, " ").alias("client_name"),
    F.rpad(F.col("member_id"), <...>, " ").alias("member_id"),
    F.rpad(F.col("claim_code"), <...>, " ").alias("claim_code"),
    F.rpad(F.col("service_date"), <...>, " ").alias("service_date"),
    F.rpad(F.col("quantity"), <...>, " ").alias("quantity")
)
write_files(
    df_format_file_to_write,
    f"{adls_path}/verified/{InFile_F}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Write Seq_Success_File
df_success_file_to_write = df_Lnk_Success.select(
    F.rpad(F.col("VENDOR_NAME"), <...>, " ").alias("VENDOR_NAME"),
    F.rpad(F.col("DTL_TYP_TX"), <...>, " ").alias("DTL_TYP_TX"),
    F.rpad(F.col("DTL_SUBTYP_TX"), <...>, " ").alias("DTL_SUBTYP_TX"),
    F.rpad(F.col("FILE_NM"), <...>, " ").alias("FILE_NM")
)
write_files(
    df_success_file_to_write,
    f"{adls_path}/verified/SUCCESS_{InFile}",
    delimiter="|",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote=None,
    nullValue=None
)