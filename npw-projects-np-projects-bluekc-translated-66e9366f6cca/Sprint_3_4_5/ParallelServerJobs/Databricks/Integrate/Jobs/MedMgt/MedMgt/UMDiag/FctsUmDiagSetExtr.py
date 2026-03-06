# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     FctsUmDiagSetExtr
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:    Pulls data from CMC_UMDX_DIAG to a landing file for the IDS
# MAGIC       
# MAGIC 
# MAGIC INPUTS:	CMC_UMDX_DIAG
# MAGIC   
# MAGIC 
# MAGIC HASH FILES:  hf_um_diag_set
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  STRIP.FIELD
# MAGIC                             FORMAT.DATE
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:  Output file is created with a temp. name.  File renamed in job control
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS:    Sequential file 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Date                 Developer                Change Description
# MAGIC ------------------      ----------------------------     --------------------------------------------------------------------------------------------------------
# MAGIC 2006-02-16      Suzanne Saylor         Original Programming.
# MAGIC  
# MAGIC 
# MAGIC Developer                          Date                    Project/Altiris #                     Change Description                                                       Development Project         Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------      -----------------------------------           ---------------------------------------------------------                                 ----------------------------------        ------------------------------       -------------------------
# MAGIC    
# MAGIC Parik                               04/11/2007              3264                            Added Balancing process to the overall job that takes           devlIDS30                       Steph Goddard              9/14/07        
# MAGIC                                                                                                               a snapshot of the source data         
# MAGIC O, Nielsen                       07/29/2008            Facets 4.5.1                     changed IDCD_CD from char(6) to VarChar(10)                  devlIDSnew                    Steph Goddard               08/20/2008
# MAGIC 
# MAGIC Bhoomi Dasari                04/09/2009          3808                               Added SrcSysCdSk to balancing snapshot                              devlIDS                           Steph Goddard               04/10/2009
# MAGIC 
# MAGIC Rick Henry                     2012-05-10          4896                                Get Diag_Cd_Typ_Cd from Cd_Mppg                                     IntegrateNewDevl             Sharon Andrew           2012-05-20
# MAGIC 
# MAGIC Manasa Andru                2014-01-31          TFS - 3481                       Corrected the job for standards                                              IntegrateNewDevl          Kalyan Neelam                    2014-02-03
# MAGIC Prabhu ES                      2022-03-07          S2S Remediation             MSSQL ODBC conn params added                                       IntegrateDev5	Harsha Ravuri		06-14-2022

# MAGIC Strip Carriage Return, Line Feed, and  Tab
# MAGIC 
# MAGIC Use STRIP.FIELD function on each character column coming in
# MAGIC Assign primary surrogate key
# MAGIC Extract Facets Data
# MAGIC Apply business logic
# MAGIC Writing Sequential File to ../key
# MAGIC Balancing snapshot of source table
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
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ./Manual_Shared_Containers_Path/ (No shared containers referenced, so nothing to import here)

# Retrieve job parameters
FacetsOwner = get_widget_value('FacetsOwner','')
TmpOutFile = get_widget_value('TmpOutFile','IdsUmDiagSetExtr.dat.pkey')
CurrRunCycle = get_widget_value('CurrRunCycle','3')
DriverTable = get_widget_value('DriverTable','TMP_IDS_UM')
RunID = get_widget_value('RunID','20070420')
CurrDate = get_widget_value('CurrDate','20120425')
SrcSysCdSk = get_widget_value('SrcSysCdSk','1')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
facets_secret_name = get_widget_value('facets_secret_name','')

# 1) Replace hashed file "hf_lkup" with dummy table read (Scenario B)
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_hf_lkup = (
    "SELECT SRC_SYS_CD, UM_REF_ID, DIAG_SET_CRT_DTM, SEQ_NO, CRT_RUN_CYC_EXCTN_SK, UM_DIAG_SET_SK "
    "FROM dummy_hf_um_diag_set"
)
df_hf_lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_hf_lkup)
    .load()
)

# 2) Stage "CD_MPPG" - DB2Connector to IDS
extract_query_CD_MPPG = (
    f"SELECT SRC_CD, TRGT_CD "
    f"FROM {IDSOwner}.CD_MPPNG "
    f"WHERE SRC_DOMAIN_NM = 'DIAGNOSIS CODE TYPE' "
    f"AND TRGT_DOMAIN_NM = 'DIAGNOSIS CODE TYPE' "
    f"AND SRC_CLCTN_CD = 'FACETS DBO'"
)
df_CD_MPPG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_CD_MPPG)
    .load()
)

# 3) Intermediate hashed file "hf_umdiagset_cd_mppng_trgt_cd" (Scenario A => deduplicate)
#    Key column is SRC_CD
df_diag_cd_typ_cd = dedup_sort(df_CD_MPPG, ["SRC_CD"], [])

# 4) Stage "CMC_UMDX_DIAG" - ODBCConnector (Facets DB read)
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
extract_query_CMC_UMDX_DIAG = (
    f"SELECT DIAG.UMUM_REF_ID, DIAG.UMDX_REF_DTM, DIAG.UMDX_SEQ_NO, "
    f"DIAG.IDCD_ID, DIAG.UMDX_ONSET_DT, TYPE.UMUM_ICD_IND_PROC "
    f"FROM {FacetsOwner}.CMC_UMDX_DIAG AS DIAG, {FacetsOwner}.CMC_UMUM_UTIL_MGT TYPE, tempdb..{DriverTable} AS T "
    f"WHERE DIAG.UMUM_REF_ID = TYPE.UMUM_REF_ID "
    f"AND DIAG.UMUM_REF_ID = T.UM_REF_ID"
)
df_CMC_UMDX_DIAG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_CMC_UMDX_DIAG)
    .load()
)

# 5) Stage "StripField" - CTransformerStage
df_StripField = (
    df_CMC_UMDX_DIAG
    .withColumn(
        "UMUM_REF_ID",
        trim(
            F.regexp_replace(
                F.regexp_replace(
                    F.regexp_replace(F.col("UMUM_REF_ID"), "\r", ""),
                    "\n", ""
                ),
                "\t", ""
            )
        )
    )
    .withColumn("UMDX_REF_DTM", F.col("UMDX_REF_DTM"))
    .withColumn("UMDX_SEQ_NO", F.col("UMDX_SEQ_NO"))
    .withColumn(
        "IDCD_ID",
        trim(
            F.regexp_replace(
                F.regexp_replace(
                    F.regexp_replace(F.col("IDCD_ID"), "\r", ""),
                    "\n", ""
                ),
                "\t", ""
            )
        )
    )
    .withColumn(
        "UMDX_ONSET_DT",
        F.concat(
            F.substring(F.col("UMDX_ONSET_DT"), 1, 10),
            F.lit(" "),
            F.substring(F.col("UMDX_ONSET_DT"), 12, 2),
            F.lit(":"),
            F.substring(F.col("UMDX_ONSET_DT"), 15, 2),
            F.lit(":"),
            F.substring(F.col("UMDX_ONSET_DT"), 18, 2),
            F.lit("."),
            F.substring(F.col("UMDX_ONSET_DT"), 21, 6)
        )
    )
    .withColumn(
        "UMUM_ICD_IND_PROC",
        F.when(
            F.length(
                trim(
                    F.regexp_replace(
                        F.regexp_replace(
                            F.regexp_replace(F.col("UMUM_ICD_IND_PROC"), "\r", ""),
                            "\n", ""
                        ),
                        "\t", ""
                    )
                )
            ) == 0,
            F.lit("9")
        ).otherwise(
            trim(
                F.regexp_replace(
                    F.regexp_replace(
                        F.regexp_replace(F.col("UMUM_ICD_IND_PROC"), "\r", ""),
                        "\n", ""
                    ),
                    "\t", ""
                )
            )
        )
    )
)

# 6) Stage "BusinessRules" - CTransformerStage
#    Left join with df_diag_cd_typ_cd on Strip.UMUM_ICD_IND_PROC = diag_cd_typ_cd.SRC_CD
df_BusinessRules_pre = df_StripField.alias("Strip").join(
    df_diag_cd_typ_cd.alias("diag_cd_typ_cd"),
    F.col("Strip.UMUM_ICD_IND_PROC") == F.col("diag_cd_typ_cd.SRC_CD"),
    how="left"
)

df_BusinessRules = (
    df_BusinessRules_pre
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.lit("Y"))
    .withColumn("FIRST_RECYC_DT", F.lit(CurrDate))
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit("FACETS"))
    .withColumn(
        "PRI_KEY_STRING",
        F.concat(
            F.lit("FACETS;"),
            F.col("Strip.UMUM_REF_ID"),
            F.lit(";"),
            F.col("Strip.UMDX_REF_DTM"),
            F.lit(";"),
            F.col("Strip.UMDX_SEQ_NO")
        )
    )
    .withColumn("UM_REF_ID", F.col("Strip.UMUM_REF_ID"))
    .withColumn("DIAG_SET_CRT_DTM", F.col("Strip.UMDX_REF_DTM"))
    .withColumn("SEQ_NO", F.col("Strip.UMDX_SEQ_NO"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn(
        "DIAG_CD_SK",
        F.when(F.col("Strip.IDCD_ID") == "", F.lit("NA")).otherwise(F.col("Strip.IDCD_ID"))
    )
    .withColumn("UM_SK", F.col("Strip.UMUM_REF_ID"))
    .withColumn("ONSET_DT_SK", F.col("Strip.UMDX_ONSET_DT"))
    .withColumn(
        "DIAG_CD_TYP_CD",
        F.when(F.isnull(F.col("diag_cd_typ_cd.TRGT_CD")), F.lit("")).otherwise(F.col("diag_cd_typ_cd.TRGT_CD"))
    )
)

# 7) Stage "PrimaryKey" - CTransformerStage, left-join with df_hf_lkup
df_PrimaryKey_in = (
    df_BusinessRules.alias("Transform")
    .join(
        df_hf_lkup.alias("lkup"),
        (
            (F.col("Transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD")) &
            (F.col("Transform.UM_REF_ID") == F.col("lkup.UM_REF_ID")) &
            (F.col("Transform.DIAG_SET_CRT_DTM") == F.col("lkup.DIAG_SET_CRT_DTM")) &
            (F.col("Transform.SEQ_NO") == F.col("lkup.SEQ_NO"))
        ),
        how="left"
    )
)

df_PrimaryKey = (
    df_PrimaryKey_in
    .withColumn("lkup_UM_DIAG_SET_SK", F.col("lkup.UM_DIAG_SET_SK"))
    .withColumn("UM_DIAG_SET_SK", F.col("lkup.UM_DIAG_SET_SK"))
    .withColumn(
        "CRT_RUN_CYC_EXCTN_SK",
        F.when(F.isnull(F.col("lkup.UM_DIAG_SET_SK")), F.lit(CurrRunCycle)).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK"))
    )
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle))
)

# SurrogateKeyGen call for KeyMgtGetNextValueConcurrent(( "UM_DIAG_SET_SK" ))
df_enriched = SurrogateKeyGen(df_PrimaryKey, <DB sequence name>, 'UM_DIAG_SET_SK', <schema>, <secret_name>)

# Output link "Key" => "IdsUmDiagSetExtr" (select all columns in the correct order)
df_IdsUmDiagSetExtr = df_enriched.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "UM_DIAG_SET_SK",
    "UM_REF_ID",
    "DIAG_SET_CRT_DTM",
    "SEQ_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "DIAG_CD_SK",
    "UM_SK",
    "ONSET_DT_SK",
    "DIAG_CD_TYP_CD"
)

# Apply rpad for char columns on final output
df_IdsUmDiagSetExtr_padded = (
    df_IdsUmDiagSetExtr
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("UM_REF_ID", F.rpad(F.col("UM_REF_ID"), 9, " "))
    .withColumn("ONSET_DT_SK", F.rpad(F.col("ONSET_DT_SK"), 10, " "))
)

# Write out the file (CSeqFileStage => not parquet). The path has directory "key".
write_files(
    df_IdsUmDiagSetExtr_padded,
    f"{adls_path}/key/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Output link "updt" => "hf_write" (Scenario B) => we must insert new rows into dummy_hf_um_diag_set
# Filter rows where original lkup was null => meaning brand-new records
df_hf_write = df_enriched.filter(F.isnull(F.col("lkup_UM_DIAG_SET_SK"))).select(
    "SRC_SYS_CD",
    "UM_REF_ID",
    "DIAG_SET_CRT_DTM",
    "SEQ_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "UM_DIAG_SET_SK"
)

# Create staging table and then merge-insert
temp_table_name_hf_write = "STAGING.FctsUmDiagSetExtr_hf_write_temp"
merge_sql_hf_write = (
    "MERGE INTO dummy_hf_um_diag_set AS T "
    "USING STAGING.FctsUmDiagSetExtr_hf_write_temp AS S "
    "ON "
    "(T.SRC_SYS_CD = S.SRC_SYS_CD AND "
    " T.UM_REF_ID = S.UM_REF_ID AND "
    " T.DIAG_SET_CRT_DTM = S.DIAG_SET_CRT_DTM AND "
    " T.SEQ_NO = S.SEQ_NO) "
    "WHEN MATCHED THEN "
    "  UPDATE SET "
    "    T.CRT_RUN_CYC_EXCTN_SK = T.CRT_RUN_CYC_EXCTN_SK, "
    "    T.UM_DIAG_SET_SK = T.UM_DIAG_SET_SK "
    "WHEN NOT MATCHED THEN "
    "  INSERT (SRC_SYS_CD, UM_REF_ID, DIAG_SET_CRT_DTM, SEQ_NO, CRT_RUN_CYC_EXCTN_SK, UM_DIAG_SET_SK) "
    "  VALUES (S.SRC_SYS_CD, S.UM_REF_ID, S.DIAG_SET_CRT_DTM, S.SEQ_NO, S.CRT_RUN_CYC_EXCTN_SK, S.UM_DIAG_SET_SK);"
)

execute_dml(f"DROP TABLE IF EXISTS {temp_table_name_hf_write}", jdbc_url_ids, jdbc_props_ids)
df_hf_write.write.jdbc(
    url=jdbc_url_ids,
    table=temp_table_name_hf_write,
    mode="append",
    properties=jdbc_props_ids
)
execute_dml(merge_sql_hf_write, jdbc_url_ids, jdbc_props_ids)

# 8) Stage "Facets_Source" => ODBC => read
extract_query_Facets_Source = (
    f"SELECT DIAG.UMUM_REF_ID, DIAG.UMDX_REF_DTM, DIAG.UMDX_SEQ_NO "
    f"FROM {FacetsOwner}.CMC_UMDX_DIAG AS DIAG, tempdb..{DriverTable} AS T "
    f"WHERE DIAG.UMUM_REF_ID = T.UM_REF_ID"
)
df_Facets_Source = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_Facets_Source)
    .load()
)

# 9) Stage "Transform" => CTransformerStage
df_Transform = (
    df_Facets_Source
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))
    .withColumn(
        "UM_REF_ID",
        trim(
            F.regexp_replace(
                F.regexp_replace(
                    F.regexp_replace(F.col("UMUM_REF_ID"), "\r", ""),
                    "\n", ""
                ),
                "\t", ""
            )
        )
    )
    # "FORMAT.DATE(...)" with "SYBASE" => no direct rule, treat as pass-through or direct assignment
    .withColumn("DIAG_SET_CRT_DTM", F.col("UMDX_REF_DTM"))
    .withColumn("SEQ_NO", F.col("UMDX_SEQ_NO"))
)

# 10) Stage "Snapshot_File" => CSeqFileStage (write delimited)
df_Snapshot_File = df_Transform.select(
    "SRC_SYS_CD_SK",
    "UM_REF_ID",
    "DIAG_SET_CRT_DTM",
    "SEQ_NO"
)

# Apply rpad for UM_REF_ID if it is char(9)
df_Snapshot_File_padded = df_Snapshot_File.withColumn("UM_REF_ID", F.rpad(F.col("UM_REF_ID"), 9, " "))

write_files(
    df_Snapshot_File_padded,
    f"{adls_path}/load/B_UM_DIAG_SET.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)