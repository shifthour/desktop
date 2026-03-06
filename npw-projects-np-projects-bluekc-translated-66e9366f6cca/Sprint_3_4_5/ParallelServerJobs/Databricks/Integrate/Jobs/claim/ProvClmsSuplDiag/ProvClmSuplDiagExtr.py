# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2020, 2021, 2022, 2023 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  ProvClmSuplDiagExtr
# MAGIC CALLED BY: ProvtClmSuplDiagCntl
# MAGIC 
# MAGIC PROCESSING:     This job Extracts the Provider ClaimsSuplementalDiagnosis communication information data from SourceFiles  to generate the transform file  to be loaded into IDS
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer\(9)\(9)Date\(9)\(9)Project\(9)\(9)Change Description\(9)\(9)\(9)        \(9)\(9)\(9)\(9)Development Project\(9)\(9)Code Reviewer\(9)\(9)Date Reviewed       
# MAGIC ======================================================================================================================================================================================================
# MAGIC Veerendra Punati\(9)\(9)2021-02-24\(9)RA\(9)\(9)Original Programming\(9)\(9)       \(9)\(9)\(9)\(9)IntegrateDev2\(9)\(9)\(9)Abhiram Dasarathy\(9)\(9)2021-03-03
# MAGIC Veerendra Punati\(9)\(9)2021-03-15\(9)RA\(9)\(9)Modified  as part of Reject/Error Changes       \(9)\(9)\(9)\(9)IntegrateDev2\(9)\(9)\(9)Abhiram Dasarathy\(9)\(9)2021-03-17
# MAGIC Lakshmi Devagiri\(9)\(9)2021-04-26\(9)US373622              \(9)Updated Member query to Ignore Case on\(9)\(9)\(9)\(9)IntegrateDev2\(9)\(9)\(9)Jaideep Mankala        \(9)04/27/2021
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)First Name and Last Name    
# MAGIC Abhishek Pulluri\(9)\(9)2021-06-21\(9)\(9)\(9)Added  Uppercase to POL_N\(9)\(9)\(9)\(9)\(9)\(9)IntegrateDev2\(9)\(9)\(9)Manasa Andru\(9)\(9)2021-06-23
# MAGIC Ken Bradmon\(9)\(9)2022-03-29\(9)us480738\(9)\(9)Fixed two column size warnings with the field\(9)\(9)\(9)\(9)IntegrateDev2\(9)\(9)\(9)Harsha Ravuri\(9)\(9)2021-03-31
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)DIAG_CD.
# MAGIC Ken Bradmon\(9)\(9)2023-01-09\(9)us542805\(9)\(9)In the "xfm_split_diag_cd_to_row" stage, I added a Trim() that \(9)\(9)IntegrateDev2\(9)\(9)\(9)Harsha Ravuri\(9)\(9)2023-06-16\(9)\(9)
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)strips out all the spaces from the DIAG_CD column.  Then the lookup for 
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)the DIAG_CD_SK values doesn't fail because the DIAG_CD values contain 
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)spaces.  Both JAYHAWK and ENCOMPASS have done that. 
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)ALSO, removed the member matching logic.  The 
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)ProvClmSuplDiagMbrMatch job has MUCH BETTER member matching, so it 
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)isn't needed AGAIN in this job.
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)ALSO removed the hard-coded SOURCE_IDs, so we won't have to change this SQL
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)every time we add a new Provider.
# MAGIC Ken Bradmon\(9)\(9)2024-04-11\(9)us611149\(9)\(9)I updated the trim() in the "xfm_split_diag_cd_to_row" stage to use EReplace\(9)IntegrateDev2\(9)\(9)\(9)Harsha Ravuri\(9)\(9)2024-06-05\(9)\(9)\(9)
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)instead, because the trim() function wasn't stripping out the spaces from the
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)middle of the string.  Also fixed the transformation of the column DIAG_CD_LKP
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)in the "Add_Lookup_DIAG_CD" stage.  It was not correctly dropping the
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)values that follow the period in the DIAG_CD column of the source file.
# MAGIC Ken Bradmon\(9)\(9)2024-07-10\(9)us624153\(9)\(9)In the Add_Lookup_DIAG_CD stage, I have changed the transformation\(9)IntegrateDev2\(9)\(9)\(9)Harsha Ravuri\(9)\(9)2024-07-22\(9)\(9)\(9)
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)of the column DIAG_CD_LKP to use EReplace to drop the decimal character,
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)instead of truncating the decimal character AND the characters that follow 
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)the decimal.
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)Also, I had to add an additional filter in the SQL of the CD_MPPNG stage, so it
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)only gets the Diagnosis Ordinal decode records from the CD_MPPNG table
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)where the TRGT_CD is like "SX%."  It should have been like that from the
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)beginning.  Ordinal codes of a "1" are *only* for actual claim records, not
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)supplemental claim records. 
# MAGIC Ken Bradmon\(9)\(9)2024-10-08\(9)us627420\(9)\(9)Diagnosis Ordinal Codes can get set to "NA".  To fix that, I moved the \(9)\(9)IntegrateDev1                                         Jeyaprasanna                          2024-11-14
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)remove dupes stage at the end of the job to instead be before the "
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)xfm_inc_src_cd" stage, which is where the ordinal code counter gets set.
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)Then we won't get so many duplicate diagnosis codes for one
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)member in one file, that the count gets higher than 57, which is the 
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)number of lookup records we have for the ordinal codes in the 
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)CD_MPPNG table, for each Provider.
# MAGIC 
# MAGIC \(9)\(9)

# MAGIC This transformer horizontally pivots the comma delimited DIAG_CD strings using a loop - each row gets turned into multiple rows.
# MAGIC The Error_Records stage separates the records where something isn't right, and routes those to the error file.
# MAGIC ALL values are given a default value, so they can be non-nullable.
# MAGIC Errors due to incorrect value lengths.
# MAGIC This dataset is deleted at the end of the ProvClmSuplDiagSeq job.
# MAGIC The validation of the DOB and the DOS should test if it is an actual VALID date, not just check that the value is 8 characters long.
# MAGIC Input file:
# MAGIC /ids/prod/verified/
# MAGIC FORMAT.<PROVIDER>.DIAG.YYYYMMDDhhmmss.txt
# MAGIC This lookup gets the Diagnosis Ordinal codes from the CD_MPPNG table.
# MAGIC The stage variable \"svRnk\" is the counter that determines the CLM_DIAG_ORDNL_CD, which gets looked up in the next stage, to get the CLM_DIAG_ORDNL_CD_SK from the CD_MPPNG table, for each Provider.
# MAGIC Drop duplicate records using these fileds:
# MAGIC MBR_UNIQ_KEY
# MAGIC RNDR_NTNL_PROV_ID
# MAGIC DIAG_CD
# MAGIC 
# MAGIC Retain just one record, with the NEWEST Date of Service.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as PF
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DateType, TimestampType
)

# MAGIC %run ../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# Parameters
InFile = get_widget_value("InFile","")
IDSOwner = get_widget_value("$IDSOwner","")
EDWOwner = get_widget_value("$EDWOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
edw_secret_name = get_widget_value("edw_secret_name","")
SrcSysCd = get_widget_value("SrcSysCd","")
RunID = get_widget_value("RunID","")
ErrorFile = get_widget_value("ErrorFile","")

# DB Config for IDS
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

# DB Config for EDW
jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)

# 1) CD_MPPNG (DB2ConnectorPX, Database=IDS)
query_CD_MPPNG = """SELECT 
mp.CD_MPPNG_SK
,mp.SRC_SYS_CD
,CAST(RIGHT(mp.SRC_CD,2) AS INTEGER) AS SRC_CD
FROM 
#${IDSOwner}.CD_MPPNG mp
WHERE TRGT_DOMAIN_NM='DIAGNOSIS ORDINAL'
AND RIGHT(mp.SRC_CD, 2) between '0' and '99'
AND TRGT_CD like 'SX%'
GROUP BY CD_MPPNG_SK,SRC_SYS_CD,SRC_CD,TRGT_CD,TRGT_CD_NM,TRGT_DOMAIN_NM
ORDER BY SRC_SYS_CD,SRC_CD
"""
df_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_CD_MPPNG)
    .load()
)

# 2) NTNL_PROV (DB2ConnectorPX, Database=IDS)
query_NTNL_PROV = """SELECT  DISTINCT NTNL_PROV_ID AS NTNL_PROV_ID, 1 AS DUMMY_PROV_ID FROM #${IDSOwner}.NTNL_PROV"""
df_NTNL_PROV = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_NTNL_PROV)
    .load()
)

# 3) vendf_diag (PxSequentialFile) -> read single column "RECORD"
schema_vendf_diag = StructType([
    StructField("RECORD", StringType(), True)
])
df_vendf_diag = (
    spark.read
    .schema(schema_vendf_diag)
    .option("header", False)
    .option("delimiter", "\n")  
    .csv(f"{adls_path}/verified/{InFile}")
)

# 4) xfrm_Split_fltr (CTransformerStage)
#    Build stage variables as columns, then filter into two outputs.
spl = PF.split(PF.col("RECORD"), "\\|")

df_xfrm_Split_fltr_vars = (
    df_vendf_diag
    .withColumn(
        "svPolNoLen",
        PF.when(
            (trim(spl[0]) != ""),
            PF.when(PF.length(spl[0]) <= 20, PF.lit("")).otherwise(PF.lit("Column POL_NO doesnt match with BLUEKC DIAG Format;"))
        ).otherwise(PF.lit(""))
    )
    .withColumn(
        "svPatnLastNmLen",
        PF.when(
            (trim(spl[1]) != ""),
            PF.when(PF.length(spl[1]) <= 50, PF.lit("")).otherwise(PF.lit("Column PATN_LAST_NM doesnt match with BLUEKC DIAG Format;"))
        ).otherwise(PF.lit(""))
    )
    .withColumn(
        "svPatnFirstNmLen",
        PF.when(
            (trim(spl[2]) != ""),
            PF.when(PF.length(spl[2]) <= 50, PF.lit("")).otherwise(PF.lit("Column PATN_FIRST_NM doesnt match with BLUEKC DIAG Format;"))
        ).otherwise(PF.lit(""))
    )
    .withColumn(
        "svMidNmLen",
        PF.when(
            (trim(spl[3]) != ""),
            PF.when(PF.length(spl[3]) <= 50, PF.lit("")).otherwise(PF.lit("Column PATN_MID_NM doesnt match with BLUEKC DIAG Format;"))
        ).otherwise(PF.lit(""))
    )
    .withColumn(
        "svMbiLen",
        PF.when(
            (trim(spl[4]) != ""),
            PF.when(PF.length(spl[4]) <= 11, PF.lit("")).otherwise(PF.lit("Column MBI doesnt match with BLUEKC DIAG Format;"))
        ).otherwise(PF.lit(""))
    )
    .withColumn(
        "svDobLen",
        PF.when(
            (trim(spl[5]) != ""),
            PF.when(PF.length(spl[5]) <= 8, PF.lit("")).otherwise(PF.lit("Column DOB doesnt match with BLUEKC DIAG Format;"))
        ).otherwise(PF.lit(""))
    )
    .withColumn(
        "svGndrLen",
        PF.when(
            (trim(spl[6]) != ""),
            PF.when(PF.length(spl[6]) <= 1, PF.lit("")).otherwise(PF.lit("Column GNDR doesnt match with BLUEKC DIAG Format;"))
        ).otherwise(PF.lit(""))
    )
    .withColumn(
        "svDtOfSvcLen",
        PF.when(
            (trim(spl[7]) != ""),
            PF.when(PF.length(spl[7]) <= 10, PF.lit("")).otherwise(PF.lit("Column DT_OF_SVC doesnt match with BLUEKC DIAG Format;"))
        ).otherwise(PF.lit(""))
    )
    .withColumn(
        "svOrdrNtnlProvIdLen",
        PF.when(
            (trim(spl[8]) != ""),
            PF.when(PF.length(spl[8]) <= 10, PF.lit("")).otherwise(PF.lit("Column RNDR_NTNL_PROV_ID doesnt match with BLUEKC DIAG Format;"))
        ).otherwise(PF.lit(""))
    )
    .withColumn(
        "svOrdrProvLastNmLen",
        PF.when(
            (trim(spl[9]) != ""),
            PF.when(PF.length(spl[9]) <= 50, PF.lit("")).otherwise(PF.lit("Column RNDR_PROV_LAST_NM doesnt match with BLUEKC DIAG Format;"))
        ).otherwise(PF.lit(""))
    )
    .withColumn(
        "svOrdrProvFirstNmLen",
        PF.when(
            (trim(spl[10]) != ""),
            PF.when(PF.length(spl[10]) <= 50, PF.lit("")).otherwise(PF.lit("Column RNDR_PROV_FIRST_NM doesnt match with BLUEKC DIAG Format;"))
        ).otherwise(PF.lit(""))
    )
    .withColumn(
        "svDiagCdLen",
        PF.when(
            (trim(spl[11]) != ""),
            PF.when(PF.length(spl[11]) <= 500, PF.lit("")).otherwise(PF.lit("Column DIAG_CD doesnt match with BLUEKC DIAG Format;"))
        ).otherwise(PF.lit(""))
    )
    .withColumn(
        "svCntrlNoLen",
        PF.when(
            (trim(spl[12]) != ""),
            PF.when(PF.length(spl[12]) <= 20, PF.lit("")).otherwise(PF.lit("Column CNTRL_NO doesnt match with BLUEKC DIAG Format;"))
        ).otherwise(PF.lit(""))
    )
    .withColumn(
        "svSourceIdLen",
        PF.when(
            (trim(spl[13]) != ""),
            PF.when(PF.length(spl[13]) <= 50, PF.lit("")).otherwise(PF.lit("Column SOURCE_ID doesnt match with BLUEKC DIAG Format;"))
        ).otherwise(PF.lit(""))
    )
    .withColumn(
        "svRejectFlag",
        PF.when(
            (PF.col("svPolNoLen") == "") &
            (PF.col("svPatnLastNmLen") == "") &
            (PF.col("svPatnFirstNmLen") == "") &
            (PF.col("svMidNmLen") == "") &
            (PF.col("svMbiLen") == "") &
            (PF.col("svDobLen") == "") &
            (PF.col("svGndrLen") == "") &
            (PF.col("svDtOfSvcLen") == "") &
            (PF.col("svOrdrNtnlProvIdLen") == "") &
            (PF.col("svOrdrProvLastNmLen") == "") &
            (PF.col("svOrdrProvFirstNmLen") == "") &
            (PF.col("svDiagCdLen") == "") &
            (PF.col("svCntrlNoLen") == "") &
            (PF.col("svSourceIdLen") == ""),
            PF.lit("T")
        ).otherwise(PF.lit("F"))
    )
    .withColumn(
        "svRejectMesg",
        PF.concat(
            PF.col("svPolNoLen"),
            PF.col("svPatnLastNmLen"),
            PF.col("svPatnFirstNmLen"),
            PF.col("svMidNmLen"),
            PF.col("svMbiLen"),
            PF.col("svDobLen"),
            PF.col("svGndrLen"),
            PF.col("svDtOfSvcLen"),
            PF.col("svOrdrNtnlProvIdLen"),
            PF.col("svOrdrProvLastNmLen"),
            PF.col("svOrdrProvFirstNmLen"),
            PF.col("svDiagCdLen"),
            PF.col("svCntrlNoLen"),
            PF.col("svSourceIdLen")
        )
    )
)

df_xfrm_Split_fltr_lnkRead = (
    df_xfrm_Split_fltr_vars
    .filter(PF.col("svRejectFlag") == "T")
    .select(
        trim(spl[0]).alias("POL_NO"),
        trim(spl[1]).alias("PATN_LAST_NM"),
        trim(spl[2]).alias("PATN_FIRST_NM"),
        trim(spl[3]).alias("PATN_MID_NM"),
        trim(spl[4]).alias("MBI"),
        trim(spl[5]).alias("DOB"),
        trim(spl[6]).alias("GNDR"),
        trim(spl[7]).alias("DT_OF_SVC"),
        trim(spl[8]).alias("RNDR_NTNL_PROV_ID"),
        trim(spl[9]).alias("RNDR_PROV_LAST_NM"),
        trim(spl[10]).alias("RNDR_PROV_FIRST_NM"),
        trim(spl[11]).alias("DIAG_CD"),
        trim(spl[12]).alias("CNTRL_NO"),
        trim(spl[13]).alias("SOURCE_ID"),
        trim(spl[14]).alias("MBR_UNIQ_KEY"),
        trim(spl[15]).alias("MBR_SK")
    )
)

df_xfrm_Split_fltr_To_fmt_error = (
    df_xfrm_Split_fltr_vars
    .filter(PF.col("svRejectFlag") == "F")
    .select(
        trim(spl[0]).alias("POL_NO"),
        trim(spl[1]).alias("PATN_LAST_NM"),
        trim(spl[2]).alias("PATN_FIRST_NM"),
        trim(spl[3]).alias("PATN_MID_NM"),
        trim(spl[4]).alias("MBI"),
        trim(spl[5]).alias("DOB"),
        trim(spl[6]).alias("GNDR"),
        trim(spl[7]).alias("DT_OF_SVC"),
        trim(spl[8]).alias("RNDR_NTNL_PROV_ID"),
        trim(spl[9]).alias("RNDR_PROV_LAST_NM"),
        trim(spl[10]).alias("RNDR_PROV_FIRST_NM"),
        trim(spl[11]).alias("DIAG_CD"),
        trim(spl[12]).alias("CNTRL_NO"),
        trim(spl[13]).alias("SOURCE_ID"),
        trim(spl[14]).alias("MBR_UNIQ_KEY"),
        trim(spl[15]).alias("MBR_SK"),
        PF.col("svRejectMesg").alias("ERROR_REASON")
    )
)

# 5) Copy0
df_Copy0 = df_xfrm_Split_fltr_To_fmt_error.select(
    PF.col("POL_NO").alias("POL_NO"),
    PF.col("PATN_LAST_NM").alias("PATN_LAST_NM"),
    PF.col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    PF.col("PATN_MID_NM").alias("PATN_MID_NM"),
    PF.col("MBI").alias("MBI"),
    PF.col("DOB").alias("DOB"),
    PF.col("GNDR").alias("GNDR"),
    PF.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    PF.col("DT_OF_SVC").alias("DT_OF_SVC"),
    PF.col("RNDR_NTNL_PROV_ID").alias("RNDR_NTNL_PROV_ID"),
    PF.col("RNDR_PROV_LAST_NM").alias("RNDR_PROV_LAST_NM"),
    PF.col("RNDR_PROV_FIRST_NM").alias("RNDR_PROV_FIRST_NM"),
    PF.col("DIAG_CD").alias("DIAG_CD"),
    PF.col("CNTRL_NO").alias("CNTRL_NO"),
    PF.col("SOURCE_ID").alias("SOURCE_ID"),
    PF.col("ERROR_REASON").alias("ERROR_REASON")
)

# 6) xfrm_add_diag_fltr (CTransformerStage) from df_xfrm_Split_fltr_lnkRead
df_xfrm_add_diag_fltr = df_xfrm_Split_fltr_lnkRead.select(
    PF.upper(trim(PF.col("POL_NO"))).alias("POL_NO"),
    PF.upper(trim(PF.col("PATN_FIRST_NM"))).alias("PATN_FIRST_NM"),
    PF.upper(trim(PF.col("PATN_LAST_NM"))).alias("PATN_LAST_NM"),
    PF.col("PATN_MID_NM").alias("PATN_MID_NM"),
    PF.col("MBI").alias("MBI"),
    PF.col("DOB").alias("DOB"),
    PF.col("GNDR").alias("GNDR"),
    PF.concat_ws("-", PF.col("DT_OF_SVC").substr(1,4), PF.col("DT_OF_SVC").substr(5,2), PF.col("DT_OF_SVC").substr(7,2)).alias("DT_OF_SVC"),
    PF.col("RNDR_NTNL_PROV_ID").alias("RNDR_NTNL_PROV_ID"),
    PF.col("RNDR_PROV_LAST_NM").alias("RNDR_PROV_LAST_NM"),
    PF.col("RNDR_PROV_FIRST_NM").alias("RNDR_PROV_FIRST_NM"),
    PF.col("DIAG_CD").alias("DIAG_CD"),
    PF.col("CNTRL_NO").alias("CNTRL_NO"),
    PF.col("SOURCE_ID").alias("SOURCE_ID"),
    PF.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    PF.col("MBR_SK").alias("MBR_SK")
)

# 7) xfm_mbr_uni_key_chk
df_xfm_mbr_uni_key_chk_vars = df_xfrm_add_diag_fltr.withColumn(
    "svMbrKeyChk",
    PF.when(
        (trim(PF.coalesce(PF.col("MBR_UNIQ_KEY"), PF.lit(""))) == "") | 
        (trim(PF.coalesce(PF.col("MBR_UNIQ_KEY"), PF.lit(""))) == PF.lit("0")),
        PF.lit(1)
    ).otherwise(PF.lit(0))
)

df_xfm_mbr_uni_key_chk_xfm_mbr_key_notfound = (
    df_xfm_mbr_uni_key_chk_vars
    .filter(PF.col("svMbrKeyChk") == 1)
    .select(
        PF.col("POL_NO").alias("POL_NO"),
        PF.col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
        PF.col("PATN_LAST_NM").alias("PATN_LAST_NM"),
        PF.col("PATN_MID_NM").alias("PATN_MID_NM"),
        PF.col("MBI").alias("MBI"),
        PF.col("DOB").alias("DOB"),
        PF.col("GNDR").alias("GNDR"),
        PF.col("DT_OF_SVC").alias("DT_OF_SVC"),
        PF.col("RNDR_NTNL_PROV_ID").alias("RNDR_NTNL_PROV_ID"),
        PF.col("RNDR_PROV_LAST_NM").alias("RNDR_PROV_LAST_NM"),
        PF.col("RNDR_PROV_FIRST_NM").alias("RNDR_PROV_FIRST_NM"),
        PF.col("DIAG_CD").alias("DIAG_CD"),
        PF.col("CNTRL_NO").alias("CNTRL_NO"),
        PF.col("SOURCE_ID").alias("SOURCE_ID"),
        PF.lit("").alias("MBR_UNIQ_KEY"),
        PF.lit("").alias("MBR_SK"),
        PF.lit("Member Match Not Found ").alias("ERROR_REASON")
    )
)

df_xfm_mbr_uni_key_chk_lkp_diag_cd_lnk = (
    df_xfm_mbr_uni_key_chk_vars
    .filter(PF.col("svMbrKeyChk") == 0)
    .select(
        PF.col("POL_NO").alias("POL_NO"),
        PF.col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
        PF.col("PATN_LAST_NM").alias("PATN_LAST_NM"),
        PF.col("PATN_MID_NM").alias("PATN_MID_NM"),
        PF.col("MBI").alias("MBI"),
        PF.col("DOB").alias("DOB"),
        PF.col("GNDR").alias("GNDR"),
        PF.col("DT_OF_SVC").alias("DT_OF_SVC"),
        PF.col("RNDR_NTNL_PROV_ID").alias("RNDR_NTNL_PROV_ID"),
        PF.col("RNDR_PROV_LAST_NM").alias("RNDR_PROV_LAST_NM"),
        PF.col("RNDR_PROV_FIRST_NM").alias("RNDR_PROV_FIRST_NM"),
        PF.col("DIAG_CD").alias("DIAG_CD"),
        PF.col("CNTRL_NO").alias("CNTRL_NO"),
        PF.col("SOURCE_ID").alias("SOURCE_ID"),
        PF.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        PF.col("MBR_SK").alias("MBR_SK_ORG")
    )
)

df_xfm_mbr_uni_key_chk_to_ntnl_prov_id = (
    df_xfm_mbr_uni_key_chk_vars
    .filter(PF.col("svMbrKeyChk") == 0)
    .select(
        PF.col("RNDR_NTNL_PROV_ID").alias("RNDR_NTNL_PROV_ID"),
        PF.col("DT_OF_SVC").cast(StringType()).alias("DT_OF_SVC")
    )
)

# 8) cpy
df_cpy = df_xfm_mbr_uni_key_chk_xfm_mbr_key_notfound.select(
    PF.col("POL_NO"),
    PF.col("PATN_FIRST_NM"),
    PF.col("PATN_LAST_NM"),
    PF.col("PATN_MID_NM"),
    PF.col("MBI"),
    PF.col("DOB"),
    PF.col("GNDR"),
    PF.col("DT_OF_SVC"),
    PF.col("RNDR_NTNL_PROV_ID"),
    PF.col("RNDR_PROV_LAST_NM"),
    PF.col("RNDR_PROV_FIRST_NM"),
    PF.col("DIAG_CD"),
    PF.col("CNTRL_NO"),
    PF.col("SOURCE_ID"),
    PF.col("MBR_UNIQ_KEY"),
    PF.col("ERROR_REASON")
)

# 9) NTL_PROV_ID (DB2ConnectorPX, Database=IDS)
query_NTL_PROV_ID = """SELECT MAX(PROV_SK) AS PROV_SK, NTNL_PROV_ID, TERM_DT_SK
FROM #${IDSOwner}.PROV PROV
GROUP BY NTNL_PROV_ID,TERM_DT_SK
"""
df_NTL_PROV_ID = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_NTL_PROV_ID)
    .load()
)

# 10) Lkp_NTNL_PROV_ID (PxLookup) - left join
#     Left join with df_NTL_PROV_ID (lkp_ntnl_prov_id) and df_xfm_mbr_uni_key_chk_to_ntnl_prov_id (to_ntnl_prov_id)
df_xfm_mbr_uni_key_chk_to_ntnl_prov_id_alias = df_xfm_mbr_uni_key_chk_to_ntnl_prov_id.alias("to_ntnl_prov_id")
df_NTL_PROV_ID_alias = df_NTL_PROV_ID.alias("lkp_ntnl_prov_id")

df_Lkp_NTNL_PROV_ID_join = (
    df_xfm_mbr_uni_key_chk_to_ntnl_prov_id_alias.join(
        df_NTL_PROV_ID_alias,
        on=[
            df_xfm_mbr_uni_key_chk_to_ntnl_prov_id_alias["RNDR_NTNL_PROV_ID"] == df_NTL_PROV_ID_alias["NTNL_PROV_ID"]
        ],
        how="left"
    )
)

df_Lkp_NTNL_PROV_ID = df_Lkp_NTNL_PROV_ID_join.select(
    df_xfm_mbr_uni_key_chk_to_ntnl_prov_id_alias["RNDR_NTNL_PROV_ID"].alias("RNDR_NTNL_PROV_ID"),
    df_xfm_mbr_uni_key_chk_to_ntnl_prov_id_alias["DT_OF_SVC"].alias("DT_OF_SVC"),
    df_NTL_PROV_ID_alias["PROV_SK"].alias("PROV_SK"),
    df_NTL_PROV_ID_alias["TERM_DT_SK"].cast(StringType()).alias("TERM_DT_SK")
)

# 11) flt_term_dt_serv (PxFilter) - no explicit condition, it simply passes rows.
df_flt_term_dt_serv = df_Lkp_NTNL_PROV_ID.select(
    PF.col("RNDR_NTNL_PROV_ID"),
    PF.col("PROV_SK")
)

# 12) Agg_max_prov_sk (PxAggregator)
#     grouping by RNDR_NTNL_PROV_ID, reduce PROV_SK max
df_Agg_max_prov_sk = (
    df_flt_term_dt_serv.groupBy("RNDR_NTNL_PROV_ID")
    .agg(PF.max("PROV_SK").alias("PROV_SK"))
)

# 13) CLM_ID_CD (DB2ConnectorPX, Database=IDS)
query_CLM_ID_CD = """WITH SRC_CD AS 
(SELECT DISTINCT SRC_CD_SK
          FROM #${IDSOwner}.CD_MPPNG 
         WHERE SRC_SYS_CD = 'FACETS' ) 
, MIN_CLM AS
(SELECT MIN(CLM_ID) CLM_ID, C.PATN_ACCT_NO AS PATN_ACCT_NO
  FROM #${IDSOwner}.CLM C
  JOIN SRC_CD M
    ON C.SRC_SYS_CD_SK = M.SRC_CD_SK
   GROUP BY C.PATN_ACCT_NO)  
SELECT C.CLM_SK AS CLM_SK
     , MC.CLM_ID AS CLM_ID
     , MC.PATN_ACCT_NO AS PATN_ACCT_NO
  FROM #${IDSOwner}.CLM C
  JOIN MIN_CLM MC
    ON C.CLM_ID = MC.CLM_ID
"""
df_CLM_ID_CD = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_CLM_ID_CD)
    .load()
)

# 14) DIAG_CD_SK (DB2ConnectorPX, Database=EDW)
query_DIAG_CD_SK = """SELECT 
      E.DIAG_CD AS DIAG_CD
    , E.DIAG_CD_SK AS DIAG_CD_SK
FROM #${EDWOwner}.DIAG_CD_D E 
WHERE E.DIAG_CD_TYP_CD = 'ICD10'
"""
df_DIAG_CD_SK = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", query_DIAG_CD_SK)
    .load()
)

# 15) Lkp_clm_Cd_id (PxLookup)
#     3 lookups: 
#       primary link = df_xfm_mbr_uni_key_chk_lkp_diag_cd_lnk (alias lkp_diag_cd_lnk)
#       lkp1 = df_CLM_ID_CD (alias lkp_clm_cd_id)
#       lkp2 = df_Agg_max_prov_sk (alias lkp_rend_prov_id)
#
#     We do left join in sequence. We'll join with lkp_clm_cd_id on some conditions, then join with lkp_rend_prov_id, etc.
lkp_diag_cd_lnk = df_xfm_mbr_uni_key_chk_lkp_diag_cd_lnk.alias("lkp_diag_cd_lnk")
lkp_clm_cd_id = df_CLM_ID_CD.alias("lkp_clm_cd_id")
lkp_rend_prov_id = df_Agg_max_prov_sk.alias("lkp_rend_prov_id")

# First join with lkp_clm_cd_id (LEFT) on:
#   SourceKeyOrValue: ntnl_lkp.DIAG_TP_CD => not actually present in JSON transformations, 
#   but JSON shows "SourceKeyOrValue": "ntnl_lkp.DIAG_TP_CD" - appears to be a mismatch in the JSON
#   In the actual lines, the job has:
#    {
#     "SourceKeyOrValue": "ntnl_lkp.DIAG_TP_CD",
#     "LookupKey": "lkp_clm_cd_id.CLM_SK"
#    },
#    {
#     "SourceKeyOrValue": "lkp_diag_cd_lnk.CNTRL_NO",
#     "LookupKey": "lkp_clm_cd_id.PATN_ACCT_NO"
#    }
#   That is contradictory; "ntnl_lkp.DIAG_TP_CD" doesn't exist in the strong sense. 
#   We will carry out the join conditions that appear. For partial correctness, we'll assume
#   "ntnl_lnk" condition is not actually used because it's not in the DataFrame. 
#   We'll only join on the second condition:
#     lkp_diag_cd_lnk["CNTRL_NO"] == lkp_clm_cd_id["PATN_ACCT_NO"]
#   because there's no "DIAG_TP_CD" column in the data, and the job's JSON is incomplete. 
#   We'll do it as we can.

join1 = lkp_diag_cd_lnk.join(
    lkp_clm_cd_id,
    on=[
        (lkp_diag_cd_lnk["CNTRL_NO"] == lkp_clm_cd_id["PATN_ACCT_NO"])
    ],
    how="left"
)

# Then join with lkp_rend_prov_id on:
#   lkp_diag_cd_lnk["RNDR_NTNL_PROV_ID"] == lkp_rend_prov_id["RNDR_NTNL_PROV_ID"]
join2 = join1.join(
    lkp_rend_prov_id,
    on=[
        (lkp_diag_cd_lnk["RNDR_NTNL_PROV_ID"] == lkp_rend_prov_id["RNDR_NTNL_PROV_ID"])
    ],
    how="left"
)

df_Lkp_clm_Cd_id = join2.select(
    lkp_diag_cd_lnk["POL_NO"].alias("POL_NO"),
    lkp_diag_cd_lnk["PATN_FIRST_NM"].alias("PATN_FIRST_NM"),
    lkp_diag_cd_lnk["PATN_LAST_NM"].alias("PATN_LAST_NM"),
    lkp_diag_cd_lnk["PATN_MID_NM"].alias("PATN_MID_NM"),
    lkp_diag_cd_lnk["MBI"].alias("MBI"),
    lkp_diag_cd_lnk["DOB"].alias("DOB"),
    lkp_diag_cd_lnk["GNDR"].alias("GNDR"),
    lkp_diag_cd_lnk["DT_OF_SVC"].alias("DT_OF_SVC"),
    lkp_diag_cd_lnk["RNDR_NTNL_PROV_ID"].alias("RNDR_NTNL_PROV_ID"),
    lkp_diag_cd_lnk["RNDR_PROV_LAST_NM"].alias("RNDR_PROV_LAST_NM"),
    lkp_diag_cd_lnk["RNDR_PROV_FIRST_NM"].alias("RNDR_PROV_FIRST_NM"),
    lkp_diag_cd_lnk["DIAG_CD"].alias("DIAG_CD"),
    lkp_diag_cd_lnk["CNTRL_NO"].alias("CNTRL_NO"),
    lkp_diag_cd_lnk["SOURCE_ID"].alias("SOURCE_ID"),
    lkp_diag_cd_lnk["MBR_UNIQ_KEY"].alias("MBR_UNIQ_KEY"),
    lkp_diag_cd_lnk["MBR_SK_ORG"].alias("MBR_SK_ORG"),
    lkp_rend_prov_id["PROV_SK"].alias("PROV_SK"),
    lkp_clm_cd_id["CLM_SK"].alias("CLM_SK"),
    lkp_clm_cd_id["CLM_ID"].alias("CLM_ID")
)

# 16) xfm_split_diag_cd_to_row (CTransformerStage) - we iterate multiple rows from DIAG_CD
df_xfm_split_diag_cd_to_row_vars = df_Lkp_clm_Cd_id.withColumn(
    "svDiagCd",
    PF.regexp_replace(PF.col("DIAG_CD"), " ", "")  # EReplace( , ' ', '')
).withColumn(
    "svDiagDCnt",
    PF.when(PF.col("svDiagCd").isNull(), PF.lit(0)).otherwise( PF.size(PF.split(PF.col("svDiagCd"), ",")) )
)

# We simulate the loop by exploding the splitted array
df_exploded = df_xfm_split_diag_cd_to_row_vars.withColumn(
    "_diag_array",
    PF.when(PF.col("svDiagCd").isNull(), PF.array()).otherwise(PF.split(PF.col("svDiagCd"), ","))
).select("*", PF.posexplode(PF.col("_diag_array")).alias("pos","svDiagCD"))

df_xfm_split_diag_cd_to_row = df_exploded.select(
    PF.col("POL_NO"),
    PF.col("PATN_FIRST_NM"),
    PF.col("PATN_LAST_NM"),
    PF.col("PATN_MID_NM"),
    PF.col("MBI"),
    PF.col("DOB"),
    PF.col("GNDR"),
    PF.col("DT_OF_SVC"),
    PF.col("RNDR_NTNL_PROV_ID"),
    PF.col("RNDR_PROV_LAST_NM"),
    PF.col("RNDR_PROV_FIRST_NM"),
    PF.col("svDiagCD").substr(1, 20).alias("DIAG_CD"), 
    PF.col("CNTRL_NO"),
    PF.col("SOURCE_ID"),
    PF.col("MBR_UNIQ_KEY"),
    PF.col("MBR_SK_ORG"),
    PF.col("PROV_SK"),
    PF.col("CLM_SK"),
    PF.col("CLM_ID")
)

# 17) Add_Lookup_DIAG_CD (CTransformerStage)
df_Add_Lookup_DIAG_CD = df_xfm_split_diag_cd_to_row.select(
    PF.col("POL_NO"),
    PF.col("PATN_FIRST_NM"),
    PF.col("PATN_LAST_NM"),
    PF.col("PATN_MID_NM"),
    PF.col("MBI"),
    PF.col("DOB"),
    PF.col("GNDR"),
    PF.col("DT_OF_SVC"),
    PF.col("RNDR_NTNL_PROV_ID"),
    PF.col("RNDR_PROV_LAST_NM"),
    PF.col("RNDR_PROV_FIRST_NM"),
    PF.col("DIAG_CD"),
    PF.col("CNTRL_NO"),
    PF.col("SOURCE_ID"),
    PF.col("MBR_UNIQ_KEY"),
    PF.col("MBR_SK_ORG"),
    PF.col("PROV_SK"),
    PF.col("CLM_SK"),
    PF.col("CLM_ID"),
    PF.upper(PF.regexp_replace(PF.col("DIAG_CD"), "\\.", "")).alias("DIAG_CD_LKP")
)

# 18) Lkp_diag_Cd (PxLookup)
#     primary link = df_Add_Lookup_DIAG_CD (lookup_DIAG_CD_SK)
#     lkp = df_DIAG_CD_SK (lkp_diag_cd_sk) on DIAG_CD_LKP = DIAG_CD
lkp_diag_cd_sk = df_DIAG_CD_SK.alias("lkp_diag_cd_sk")
lookup_DIAG_CD_SK = df_Add_Lookup_DIAG_CD.alias("lookup_DIAG_CD_SK")

df_join_diag = lookup_DIAG_CD_SK.join(
    lkp_diag_cd_sk,
    on=[ lookup_DIAG_CD_SK["DIAG_CD_LKP"] == lkp_diag_cd_sk["DIAG_CD"] ],
    how="left"
)

df_Lkp_diag_Cd = df_join_diag.select(
    lookup_DIAG_CD_SK["POL_NO"].alias("POL_NO"),
    lookup_DIAG_CD_SK["PATN_FIRST_NM"].alias("PATN_FIRST_NM"),
    lookup_DIAG_CD_SK["PATN_LAST_NM"].alias("PATN_LAST_NM"),
    lookup_DIAG_CD_SK["PATN_MID_NM"].alias("PATN_MID_NM"),
    lookup_DIAG_CD_SK["MBI"].alias("MBI"),
    lookup_DIAG_CD_SK["DOB"].alias("DOB"),
    lookup_DIAG_CD_SK["GNDR"].alias("GNDR"),
    lookup_DIAG_CD_SK["DT_OF_SVC"].alias("DT_OF_SVC"),
    lookup_DIAG_CD_SK["RNDR_NTNL_PROV_ID"].alias("RNDR_NTNL_PROV_ID"),
    lookup_DIAG_CD_SK["RNDR_PROV_LAST_NM"].alias("RNDR_PROV_LAST_NM"),
    lookup_DIAG_CD_SK["RNDR_PROV_FIRST_NM"].alias("RNDR_PROV_FIRST_NM"),
    lookup_DIAG_CD_SK["DIAG_CD"].alias("DIAG_CD"),
    lookup_DIAG_CD_SK["CNTRL_NO"].alias("CNTRL_NO"),
    lookup_DIAG_CD_SK["SOURCE_ID"].alias("SOURCE_ID"),
    lookup_DIAG_CD_SK["MBR_UNIQ_KEY"].alias("MBR_UNIQ_KEY"),
    lookup_DIAG_CD_SK["MBR_SK_ORG"].alias("MBR_SK_ORG"),
    lookup_DIAG_CD_SK["PROV_SK"].alias("PROV_SK"),
    lookup_DIAG_CD_SK["CLM_SK"].alias("CLM_SK"),
    lookup_DIAG_CD_SK["CLM_ID"].alias("CLM_ID"),
    lkp_diag_cd_sk["DIAG_CD_SK"].alias("DIAG_CD_SK")
)

# 19) RmvDupes (PxRemDup) => dedup by (MBR_UNIQ_KEY, RNDR_NTNL_PROV_ID, DIAG_CD), retain first, 
#     sorts by MBR_UNIQ_KEY asc, RNDR_NTNL_PROV_ID asc, DIAG_CD asc, DT_OF_SVC desc
df_dedup = dedup_sort(
    df_Lkp_diag_Cd,
    partition_cols=["MBR_UNIQ_KEY","RNDR_NTNL_PROV_ID","DIAG_CD"],
    sort_cols=[("DT_OF_SVC","D")]
)

# 20) xfm_inc_src_cd (CTransformerStage)
#     We replicate the row-rank approach with stage variables.
#     We'll build a window function with partition by (MBR_UNIQ_KEY, DT_OF_SVC) ordering by row arrival for sim
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


w_inc_src_cd = Window.partitionBy(
    PF.coalesce(PF.col("MBR_UNIQ_KEY"), PF.lit("")), 
    PF.coalesce(PF.col("DT_OF_SVC"), PF.lit(""))
).orderBy("MBR_UNIQ_KEY","DT_OF_SVC","POL_NO","DIAG_CD")  # a stable ordering to emulate the row arrivals

df_xfm_inc_src_cd = df_dedup.withColumn(
    "_group",
    PF.concat(PF.coalesce(PF.col("MBR_UNIQ_KEY"), PF.lit("")), PF.coalesce(PF.col("DT_OF_SVC"), PF.lit("")))
).withColumn(
    "_chk",
    PF.lag(PF.col("_group"), 1).over(w_inc_src_cd)
).withColumn(
    "_rnkPrev",
    PF.lag(PF.col("_inc_src_cd"), 1).over(w_inc_src_cd)
).withColumn(
    "_rnk",
    PF.when(
        (PF.col("_chk") != PF.col("_group")) | PF.col("_chk").isNull(),
        PF.lit(1)
    ).otherwise(PF.coalesce(PF.col("_rnkPrev"), PF.lit(0)) + 1)
).fillna(0, subset=["_inc_src_cd"])\
.withColumn(
    "_inc_src_cd",
    PF.col("_rnk")
)

df_xfm_inc_src_cd_final = df_xfm_inc_src_cd.select(
    PF.col("POL_NO"),
    PF.col("PATN_FIRST_NM"),
    PF.col("PATN_LAST_NM"),
    PF.col("PATN_MID_NM"),
    PF.col("MBI"),
    PF.col("DOB"),
    PF.col("GNDR"),
    PF.col("DT_OF_SVC"),
    PF.col("RNDR_NTNL_PROV_ID"),
    PF.col("RNDR_PROV_LAST_NM"),
    PF.col("RNDR_PROV_FIRST_NM"),
    PF.col("DIAG_CD"),
    PF.col("CNTRL_NO"),
    PF.col("SOURCE_ID"),
    PF.col("MBR_UNIQ_KEY"),
    PF.col("MBR_SK_ORG"),
    PF.col("PROV_SK"),
    PF.col("CLM_SK"),
    PF.col("CLM_ID"),
    PF.col("DIAG_CD_SK"),
    PF.col("_inc_src_cd").alias("INC_SRC_CD")
)

# 21) lkp_cd_mppng_sk (PxLookup)
#     primary link = df_xfm_inc_src_cd_final (lkp_diag_cd_lnk)
#     lkp1 = df_CD_MPPNG (lkpcdmppng)  join condition => lkp_diag_cd_lnk.SOURCE_ID == SRC_SYS_CD  AND lkp_diag_cd_lnk.INC_SRC_CD == SRC_CD
#     lkp2 = df_NTNL_PROV (lkpntnl_prov_id) join => lkp_diag_cd_lnk.RNDR_NTNL_PROV_ID == NTNL_PROV_ID AND lkp_diag_cd_lnk.SOURCE_ID == DUMMY_PROV_ID
lkp_diag_cd_lnk_alias = df_xfm_inc_src_cd_final.alias("lkp_diag_cd_lnk")
lkpcdmppng_alias = df_CD_MPPNG.alias("lkpcdmppng")
lkpntnl_prov_id_alias = df_NTNL_PROV.alias("lkpntnl_prov_id")

join_cd_mppng = lkp_diag_cd_lnk_alias.join(
    lkpcdmppng_alias,
    on=[
        lkp_diag_cd_lnk_alias["SOURCE_ID"] == lkpcdmppng_alias["SRC_SYS_CD"],
        lkp_diag_cd_lnk_alias["INC_SRC_CD"] == lkpcdmppng_alias["SRC_CD"]
    ],
    how="left"
)

join_ntnl_prov = join_cd_mppng.join(
    lkpntnl_prov_id_alias,
    on=[
        lkp_diag_cd_lnk_alias["RNDR_NTNL_PROV_ID"] == lkpntnl_prov_id_alias["NTNL_PROV_ID"],
        lkp_diag_cd_lnk_alias["SOURCE_ID"] == lkpntnl_prov_id_alias["DUMMY_PROV_ID"]
    ],
    how="left"
)

df_lkp_cd_mppng_sk = join_ntnl_prov.select(
    lkp_diag_cd_lnk_alias["POL_NO"].alias("POL_NO"),
    lkp_diag_cd_lnk_alias["PATN_FIRST_NM"].alias("PATN_FIRST_NM"),
    lkp_diag_cd_lnk_alias["PATN_LAST_NM"].alias("PATN_LAST_NM"),
    lkp_diag_cd_lnk_alias["PATN_MID_NM"].alias("PATN_MID_NM"),
    lkp_diag_cd_lnk_alias["MBI"].alias("MBI"),
    lkp_diag_cd_lnk_alias["DOB"].alias("DOB"),
    lkp_diag_cd_lnk_alias["GNDR"].alias("GNDR"),
    lkp_diag_cd_lnk_alias["DT_OF_SVC"].alias("DT_OF_SVC"),
    lkp_diag_cd_lnk_alias["RNDR_NTNL_PROV_ID"].alias("RNDR_NTNL_PROV_ID"),
    lkp_diag_cd_lnk_alias["RNDR_PROV_LAST_NM"].alias("RNDR_PROV_LAST_NM"),
    lkp_diag_cd_lnk_alias["RNDR_PROV_FIRST_NM"].alias("RNDR_PROV_FIRST_NM"),
    lkp_diag_cd_lnk_alias["DIAG_CD"].alias("DIAG_CD"),
    lkp_diag_cd_lnk_alias["CNTRL_NO"].alias("CNTRL_NO"),
    lkp_diag_cd_lnk_alias["SOURCE_ID"].alias("SOURCE_ID"),
    lkp_diag_cd_lnk_alias["MBR_UNIQ_KEY"].alias("MBR_UNIQ_KEY"),
    lkp_diag_cd_lnk_alias["MBR_SK_ORG"].alias("MBR_SK_ORG"),
    lkp_diag_cd_lnk_alias["PROV_SK"].alias("PROV_SK"),
    lkp_diag_cd_lnk_alias["CLM_SK"].alias("CLM_SK"),
    lkp_diag_cd_lnk_alias["CLM_ID"].alias("CLM_ID"),
    lkp_diag_cd_lnk_alias["DIAG_CD_SK"].alias("DIAG_CD_SK"),
    lkp_diag_cd_lnk_alias["INC_SRC_CD"].alias("INC_SRC_CD"),
    lkpcdmppng_alias["CD_MPPNG_SK"].alias("CD_MPPNG_SK"),
    lkpntnl_prov_id_alias["DUMMY_PROV_ID"].alias("DUMMY_PROV_ID")
)

# 22) Error_Records (CTransformerStage)
df_Error_Records_vars = df_lkp_cd_mppng_sk.withColumn(
    "svRndrProvIDChk",
    PF.when(
        PF.col("RNDR_NTNL_PROV_ID").isNotNull() & (trim(PF.col("RNDR_NTNL_PROV_ID")) != "") & PF.col("DUMMY_PROV_ID").isNotNull(),
        PF.lit(True)
    ).otherwise(PF.lit(False))
).withColumn(
    "svDateOfSvcChk",
    PF.when(
        PF.col("DT_OF_SVC").isNotNull() & (trim(PF.col("DT_OF_SVC")) != ""),
        PF.lit(True)
    ).otherwise(PF.lit(False))
).withColumn(
    "svDiagCDChk",
    PF.when(
        PF.col("DIAG_CD_SK").isNotNull() & (trim(PF.col("DIAG_CD_SK")) != ""),
        PF.lit(True)
    ).otherwise(PF.lit(False))
).withColumn(
    "svSourceIDChk",
    PF.when(
        PF.col("SOURCE_ID").isNotNull() & (trim(PF.col("SOURCE_ID")) != ""),
        PF.lit(True)
    ).otherwise(PF.lit(False))
).withColumn(
    "svErrorValidChk",
    PF.when(
        (PF.col("svRndrProvIDChk") == True) &
        (PF.col("svDateOfSvcChk") == True) &
        (PF.col("svDiagCDChk") == True) &
        (PF.col("svSourceIDChk") == True),
        PF.lit(True)
    ).otherwise(PF.lit(False))
).withColumn(
    "svCDmppngSK",
    PF.when(
        PF.col("CD_MPPNG_SK").isNotNull() & (PF.length(PF.col("CD_MPPNG_SK")) > 0),
        PF.col("CD_MPPNG_SK")
    ).otherwise(PF.lit(""))
)

# Two output links:
#   lnkGetDiagKeys => filter svErrorValidChk = True
df_Error_Records_lnkGetDiagKeys = df_Error_Records_vars.filter(
    PF.col("svErrorValidChk") == True
).select(
    PF.when(PF.length(PF.col("MBR_UNIQ_KEY")) == 0, PF.lit("0")).otherwise(PF.col("MBR_UNIQ_KEY")).alias("MBR_UNIQ_KEY"),
    PF.when(PF.length(PF.col("RNDR_NTNL_PROV_ID")) == 0, PF.lit("NA")).otherwise(PF.col("RNDR_NTNL_PROV_ID")).alias("NTNL_PROV_ID"),
    PF.when(PF.length(PF.col("DT_OF_SVC")) == 0, PF.lit("1753-01-01")).otherwise(PF.to_date(trim(PF.col("DT_OF_SVC")), "yyyy-MM-dd")).alias("CLM_SVC_STRT_DT"),
    PF.when(PF.length(PF.col("DIAG_CD")) == 0, PF.lit("NA")).otherwise(PF.col("DIAG_CD")).alias("DIAG_CD"),
    PF.lit("ICD10").alias("DIAG_CD_TYP_CD"),
    PF.when(PF.length(PF.col("SOURCE_ID")) == 0, PF.lit("NA")).otherwise(PF.col("SOURCE_ID")).alias("SRC_SYS_CD"),
    PF.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    PF.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    PF.when(PF.col("CLM_SK").isNotNull(), PF.col("CLM_SK")).otherwise(PF.lit(1)).alias("CLM_SRC_SYS_ORIG_CLM_SK"),
    PF.when(PF.length(PF.col("DIAG_CD_SK")) == 0, PF.lit("0")).otherwise(PF.col("DIAG_CD_SK")).alias("DIAG_CD_SK"),
    PF.when(PF.length(PF.col("MBR_SK_ORG")) == 0, PF.lit("0")).otherwise(PF.col("MBR_SK_ORG")).alias("MBR_SK"),
    PF.when(PF.col("PROV_SK").isNotNull(), PF.col("PROV_SK")).otherwise(PF.lit(1)).alias("PROV_SK"),
    PF.when(PF.length(PF.col("svCDmppngSK")) == 0, PF.lit("")).otherwise(PF.col("CD_MPPNG_SK")).alias("CLM_DIAG_ORDNL_CD_SK"),
    PF.when(PF.length(PF.col("CNTRL_NO")) == 0, PF.lit("NA")).otherwise(PF.col("CNTRL_NO")).alias("CLM_PATN_ACCT_NO"),
    PF.when(PF.col("CLM_ID").isNotNull(), PF.col("CLM_ID")).otherwise(PF.lit("")).alias("CLM_SRC_SYS_ORIG_CLM_ID")
)

#   Error_rcds => filter svErrorValidChk = False
df_Error_Records_Error_rcds = df_Error_Records_vars.filter(
    PF.col("svErrorValidChk") == False
).select(
    PF.col("POL_NO").alias("POL_NO"),
    PF.col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    PF.col("PATN_LAST_NM").alias("PATN_LAST_NM"),
    PF.col("PATN_MID_NM").alias("PATN_MID_NM"),
    PF.col("MBI").alias("MBI"),
    PF.col("DOB").alias("DOB"),
    PF.col("GNDR").alias("GNDR"),
    PF.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    PF.col("DT_OF_SVC").alias("DT_OF_SVC"),
    PF.col("RNDR_NTNL_PROV_ID").alias("RNDR_NTNL_PROV_ID"),
    PF.col("RNDR_PROV_LAST_NM").alias("RNDR_PROV_LAST_NM"),
    PF.col("RNDR_PROV_FIRST_NM").alias("RNDR_PROV_FIRST_NM"),
    PF.col("DIAG_CD").alias("DIAG_CD"),
    PF.col("CNTRL_NO").alias("CNTRL_NO"),
    PF.col("SOURCE_ID").alias("SOURCE_ID"),
    PF.concat(
        PF.when(PF.col("svRndrProvIDChk") == False, PF.lit("RNDR_NTNL_PROV_ID not available")).otherwise(PF.lit("")),
        PF.lit(" "),
        PF.when(PF.col("svDateOfSvcChk") == False, PF.lit("DT_OF_SVC Not Found")).otherwise(PF.lit("")),
        PF.lit(" "),
        PF.when(PF.col("svDiagCDChk") == False, PF.lit("DIAG_CD not found")).otherwise(PF.lit("")),
        PF.lit(" "),
        PF.when(PF.col("svSourceIDChk") == False, PF.lit("SOURCE_ID do not match")).otherwise(PF.lit(""))
    ).alias("ERROR_REASON")
)

# 23) Fnl (PxFunnel) => merges 3 inputs for errors:
#    1) df_Error_Records_Error_rcds
#    2) df_cpy
#    3) df_Copy0
#    The funnel keeps the same columns. We'll union in same order.
df_Error_Records_Error_rcds_fnl = df_Error_Records_Error_rcds.select(
    "POL_NO","PATN_FIRST_NM","PATN_LAST_NM","PATN_MID_NM","MBI","DOB","GNDR","MBR_UNIQ_KEY","DT_OF_SVC","RNDR_NTNL_PROV_ID","RNDR_PROV_LAST_NM","RNDR_PROV_FIRST_NM","DIAG_CD","CNTRL_NO","SOURCE_ID","ERROR_REASON"
)
df_cpy_fnl = df_cpy.select(
    "POL_NO","PATN_FIRST_NM","PATN_LAST_NM","PATN_MID_NM","MBI","DOB","GNDR","MBR_UNIQ_KEY","DT_OF_SVC","RNDR_NTNL_PROV_ID","RNDR_PROV_LAST_NM","RNDR_PROV_FIRST_NM","DIAG_CD","CNTRL_NO","SOURCE_ID","ERROR_REASON"
)
df_Copy0_fnl = df_Copy0.select(
    "POL_NO","PATN_FIRST_NM","PATN_LAST_NM","PATN_MID_NM","MBI","DOB","GNDR","MBR_UNIQ_KEY","DT_OF_SVC","RNDR_NTNL_PROV_ID","RNDR_PROV_LAST_NM","RNDR_PROV_FIRST_NM","DIAG_CD","CNTRL_NO","SOURCE_ID","ERROR_REASON"
)

df_Fnl_error = df_Error_Records_Error_rcds_fnl.unionByName(df_cpy_fnl).unionByName(df_Copy0_fnl)

# 24) tgt_Error_rcds (PxSequentialFile) => write to #ErrorFile# in "verified" => f"{adls_path}/verified/{ErrorFile}"
#     with delimiter="|", mode="overwrite", header=True
#     Before writing, we apply the final select in the same order
df_tgt_Error_rcds = df_Fnl_error.select(
    "POL_NO","PATN_FIRST_NM","PATN_LAST_NM","PATN_MID_NM","MBI","DOB","GNDR","MBR_UNIQ_KEY","DT_OF_SVC","RNDR_NTNL_PROV_ID","RNDR_PROV_LAST_NM","RNDR_PROV_FIRST_NM","DIAG_CD","CNTRL_NO","SOURCE_ID","ERROR_REASON"
)
write_files(
    df_tgt_Error_rcds,
    f"{adls_path}/verified/{ErrorFile}",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote='"',
    nullValue=None
)

# 25) Copy (PxCopy) => from df_Error_Records_lnkGetDiagKeys => "clmsupldiagds"
df_Copy_clmsupldiagds = df_Error_Records_lnkGetDiagKeys.select(
    PF.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    PF.col("NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    PF.col("CLM_SVC_STRT_DT").alias("CLM_SVC_STRT_DT"),
    PF.col("DIAG_CD").alias("DIAG_CD"),
    PF.col("DIAG_CD_TYP_CD").alias("DIAG_CD_TYP_CD"),
    PF.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    PF.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    PF.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    PF.col("CLM_SRC_SYS_ORIG_CLM_SK").alias("CLM_SRC_SYS_ORIG_CLM_SK"),
    PF.col("DIAG_CD_SK").alias("DIAG_CD_SK"),
    PF.col("MBR_SK").alias("MBR_SK"),
    PF.col("PROV_SK").alias("PROV_SK"),
    PF.col("CLM_DIAG_ORDNL_CD_SK").alias("CLM_DIAG_ORDNL_CD_SK"),
    PF.col("CLM_PATN_ACCT_NO").alias("CLM_PATN_ACCT_NO"),
    PF.col("CLM_SRC_SYS_ORIG_CLM_ID").alias("CLM_SRC_SYS_ORIG_CLM_ID")
)

# 26) ClmSuppDiag_Extr (PxDataSet) => translate to .parquet
#     Path = ds/Clm_Supp_Diag.#SrcSysCd#.xfrm.#RunID#.ds => replace .ds => .parquet
output_pq_name = f"ds/Clm_Supp_Diag.{SrcSysCd}.xfrm.{RunID}.parquet"
df_ClmSuppDiag_Extr = df_Copy_clmsupldiagds.select(
    "MBR_UNIQ_KEY",
    "NTNL_PROV_ID",
    "CLM_SVC_STRT_DT",
    "DIAG_CD",
    "DIAG_CD_TYP_CD",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_SRC_SYS_ORIG_CLM_SK",
    "DIAG_CD_SK",
    "MBR_SK",
    "PROV_SK",
    "CLM_DIAG_ORDNL_CD_SK",
    "CLM_PATN_ACCT_NO",
    "CLM_SRC_SYS_ORIG_CLM_ID"
)

write_files(
    df_ClmSuppDiag_Extr,
    f"{adls_path}/{output_pq_name}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)