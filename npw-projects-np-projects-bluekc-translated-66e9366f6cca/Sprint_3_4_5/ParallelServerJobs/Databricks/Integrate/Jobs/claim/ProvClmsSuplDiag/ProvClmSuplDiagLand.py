# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2021, 2023 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  ProvClmSuplDiagLand
# MAGIC CALLED BY:  ProvClmSuplDiagSeq
# MAGIC PURPOSE:  Counts the columns of the Source File, and confirms the SOURCE ID is correct.
# MAGIC                    
# MAGIC 
# MAGIC Processing:Source Input file for Claims Supl Diag  file are extracted and formatted
# MAGIC                     
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:  Restore the source file before re-running the job
# MAGIC                     Previous Run Aborted: Restart, no other steps necessary  
# MAGIC Modifications:                        
# MAGIC Developer\(9)\(9)Date\(9)\(9)Project\(9)\(9)Change Description\(9)\(9)\(9)        \(9)\(9)\(9)Development Project\(9)\(9)Code Reviewer\(9)\(9)Date Reviewed       
# MAGIC ======================================================================================================================================================================================================
# MAGIC Veerendra Punati       \(9)2021-02-24\(9)              \(9)   \(9)Original Programming\(9)\(9)\(9) \(9)\(9)\(9)\(9)\(9)\(9)Abhiram Dasarathy\(9)\(9)2021-03-03
# MAGIC Veerendra Punati\(9)\(9)2021-03-15\(9)\(9)\(9)Modified  as part of Reject/Error Changes\(9)\(9)\(9)\(9)\(9)\(9)\(9)Abhiram Dasarathy\(9)\(9)2021-03-17
# MAGIC Lakshmi Devagiri      \(9)2021-04-26                   \(9)\(9)Updated Job to remove CR/LF characters at the last column         \(9)\(9)\(9)\(9)\(9)Jaideep Mankala \(9)\(9)04/27/2021
# MAGIC Ken Bradmon\(9)\(9)2023-04-28\(9)us542805\(9)\(9)Added MBR_UNIQ_KEY and MBR_SK to incoming and\(9)\(9)IntegrateDev1\(9)\(9)\(9)Harsha Ravuri\(9)\(9)06/14/2023
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)outgoing file.
# MAGIC Ken Bradmon\(9)\(9)2024-03-12\(9)us611149\(9)\(9)Added code to ignore the new MemberMatchLevel column in\(9)IntegrateDev2                                        Reddy Sanam                            04/11/2024
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)the input file.  We *WANT* that column in the input file, but it isn't
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)used anywhere in this job.  It is just used for troubleshooting, if there
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)is any question about member matching logic in the 
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)ProvClmSuplDiagMbrMatch job.

# MAGIC JOB NAME:  ProvClmSuplDiagLand
# MAGIC CALLED BY:  ProvClmSuplDiagSeq
# MAGIC PURPOSE:  Counts the columns of the Source File, and confirms the SOURCE ID is correct.
# MAGIC Get the SRC_SYS_CD from the file, for lookup in the CD_MPPNG table.
# MAGIC This counts the columns.  Wrong number of columns -- reject the whole file.
# MAGIC Split up the \"single column\" of the source file in to separate columns, after the columns have been counted.
# MAGIC Output file:
# MAGIC /ids/prod/verified/
# MAGIC FORMAT.<PROVIDER>.DIAG.YYYYMMDDhhmmss.txt
# MAGIC The source file is created by the ProvClmSuplDiagMbrMatch job:
# MAGIC /ids/prod/landing
# MAGIC <PROVIDER>.DIAG.CCYYMMDDhhmmss.TXT_MBRMATCHFINAL
# MAGIC 
# MAGIC The source file isn't REALLY just one column.  But read it that way initially, so the number of columns can be counted.
# MAGIC Output file:
# MAGIC /ids/prod/verified/
# MAGIC <PROVIDER>.DIAG_REJECT.YYYYMMDDhhmmss.TXT
# MAGIC In this case the rejects are the GOOD records.
# MAGIC Rejects
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, split, size, regexp_replace, trim, when, lit, concat_ws, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------

# MAGIC %run ../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

InFile = get_widget_value('InFile','')
InFile_F = get_widget_value('InFile_F','')
RejectFile = get_widget_value('RejectFile','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""
SELECT DISTINCT TRIM(SRC_SYS_CD) AS SRC_SYS_CD,
       1 AS FLG
FROM {IDSOwner}.CD_MPPNG
"""
df_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

schema_Diag_InputFile = StructType([
    StructField("RECORD", StringType(), True)
])
df_Diag_InputFile = (
    spark.read.format("csv")
    .option("header", True)
    .schema(schema_Diag_InputFile)
    .load(f"{adls_path_raw}/landing/{InFile}")
)

df_Xfm_Src_sys_Cd = df_Diag_InputFile.select(
    regexp_replace(regexp_replace(col("RECORD"), "\r", ""), "\n", "").alias("RECORD"),
    regexp_replace(
        regexp_replace(
            trim(split(col("RECORD"), "\\|").getItem(13)), "\r", ""
        ),
        "\n", ""
    ).alias("SOURCE_ID")
)

df_Lkp_src_sys_cd = (
    df_Xfm_Src_sys_Cd.alias("To_Lkp_Sys_cd")
    .join(
        df_CD_MPPNG.alias("lkpsrcsyscd"),
        col("To_Lkp_Sys_cd.SOURCE_ID") == col("lkpsrcsyscd.SRC_SYS_CD"),
        "left"
    )
    .select(
        col("To_Lkp_Sys_cd.RECORD").alias("RECORD"),
        col("lkpsrcsyscd.FLG").alias("FLG")
    )
)

df_Xfm_Delimter_Chk = df_Lkp_src_sys_cd.withColumn(
    "svDelimiter",
    size(split(regexp_replace(col("RECORD"), "\r", ""), "\\|"))
).withColumn(
    "svFLG",
    (trim(col("FLG")) != "") & col("FLG").isNotNull()
)

df_Rec_Out = (
    df_Xfm_Delimter_Chk
    .filter((col("svDelimiter") == 17) & (col("svFLG") == True))
    .select(
        regexp_replace(col("RECORD"), "\r", "").alias("RECORD"),
        lit("1").alias("CMN_LKP")
    )
)

df_Lnk_Reject = (
    df_Xfm_Delimter_Chk
    .filter((col("svDelimiter") != 17) | (col("svFLG") == False))
    .select(
        regexp_replace(col("RECORD"), "\r", "").alias("RECORD"),
        lit("1").alias("CMN_LKP"),
        col("FLG").alias("FLG"),
        col("svDelimiter").alias("DEL_FLG")
    )
)

df_Cp_2 = df_Rec_Out.select(
    col("RECORD"),
    col("CMN_LKP")
)

df_Cp1_to_RmDups1 = df_Lnk_Reject.select(
    col("CMN_LKP")
)

df_Out_Cp1 = df_Lnk_Reject.select(
    col("RECORD"),
    col("FLG"),
    col("DEL_FLG")
)

df_Xfm_1 = df_Out_Cp1.select(
    split(col("RECORD"), "\\|").getItem(0).alias("POL_NO"),
    split(col("RECORD"), "\\|").getItem(1).alias("PATN_LAST_NM"),
    split(col("RECORD"), "\\|").getItem(2).alias("PATN_FIRST_NM"),
    split(col("RECORD"), "\\|").getItem(3).alias("PATN_MID_NM"),
    split(col("RECORD"), "\\|").getItem(4).alias("MBI"),
    split(col("RECORD"), "\\|").getItem(5).alias("DOB"),
    split(col("RECORD"), "\\|").getItem(6).alias("GNDR"),
    split(col("RECORD"), "\\|").getItem(7).alias("DT_OF_SVC"),
    split(col("RECORD"), "\\|").getItem(8).alias("RNDR_NTNL_PROV_ID"),
    split(col("RECORD"), "\\|").getItem(9).alias("RNDR_PROV_LAST_NM"),
    split(col("RECORD"), "\\|").getItem(10).alias("RNDR_PROV_FIRST_NM"),
    split(col("RECORD"), "\\|").getItem(11).alias("DIAG_CD"),
    split(col("RECORD"), "\\|").getItem(12).alias("CNTRL_NO"),
    split(col("RECORD"), "\\|").getItem(13).alias("SOURCE_ID"),
    split(col("RECORD"), "\\|").getItem(14).alias("MBR_UNIQ_KEY"),
    split(col("RECORD"), "\\|").getItem(15).alias("MBR_SK"),
    concat_ws(
        " ",
        when(col("DEL_FLG") != lit(16), "Number of source file columns does not match file spec.").otherwise(""),
        when(col("FLG").isNull(), "SOURCE_ID does not match.").otherwise("")
    ).alias("REJECT_REASON")
)

df_Rm_Dups_1 = dedup_sort(df_Cp1_to_RmDups1, ["CMN_LKP"], [])

df_Lkp_1_join = (
    df_Cp_2.alias("to_Lkp1")
    .join(
        df_Rm_Dups_1.alias("out_RmDups1"),
        col("to_Lkp1.CMN_LKP") == col("out_RmDups1.CMN_LKP"),
        "left"
    )
)

df_to_xfm2 = df_Lkp_1_join.select(
    col("to_Lkp1.RECORD").alias("RECORD")
)

df_out_lkp = df_Lkp_1_join.select(
    col("to_Lkp1.RECORD").alias("RECORD"),
    col("to_Lkp1.CMN_LKP").alias("CMN_LKP")
)

df_Copy = df_out_lkp.select(
    col("RECORD")
)

df_ClmSuplDiag = df_Copy.select(
    rpad(col("RECORD"), <...>, " ").alias("RECORD")
)
write_files(
    df_ClmSuplDiag,
    f"{adls_path}/verified/{InFile_F}",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='',
    nullValue=None
)

df_Xfm_2 = df_to_xfm2.select(
    split(col("RECORD"), "\\|").getItem(0).alias("POL_NO"),
    split(col("RECORD"), "\\|").getItem(1).alias("PATN_LAST_NM"),
    split(col("RECORD"), "\\|").getItem(2).alias("PATN_FIRST_NM"),
    split(col("RECORD"), "\\|").getItem(3).alias("PATN_MID_NM"),
    split(col("RECORD"), "\\|").getItem(4).alias("MBI"),
    split(col("RECORD"), "\\|").getItem(5).alias("DOB"),
    split(col("RECORD"), "\\|").getItem(6).alias("GNDR"),
    split(col("RECORD"), "\\|").getItem(7).alias("DT_OF_SVC"),
    split(col("RECORD"), "\\|").getItem(8).alias("RNDR_NTNL_PROV_ID"),
    split(col("RECORD"), "\\|").getItem(9).alias("RNDR_PROV_LAST_NM"),
    split(col("RECORD"), "\\|").getItem(10).alias("RNDR_PROV_FIRST_NM"),
    split(col("RECORD"), "\\|").getItem(11).alias("DIAG_CD"),
    split(col("RECORD"), "\\|").getItem(12).alias("CNTRL_NO"),
    split(col("RECORD"), "\\|").getItem(13).alias("SOURCE_ID"),
    split(col("RECORD"), "\\|").getItem(14).alias("MBR_UNIQ_KEY"),
    split(col("RECORD"), "\\|").getItem(15).alias("MBR_SK"),
    lit(" ").alias("REJECT_REASON")
)

df_Fn_1 = df_Xfm_2.unionByName(df_Xfm_1)

df_Clm_Diag_RejectFile = df_Fn_1.select(
    rpad(col("POL_NO"), <...>, " ").alias("POL_NO"),
    rpad(col("PATN_LAST_NM"), <...>, " ").alias("PATN_LAST_NM"),
    rpad(col("PATN_FIRST_NM"), <...>, " ").alias("PATN_FIRST_NM"),
    rpad(col("PATN_MID_NM"), <...>, " ").alias("PATN_MID_NM"),
    rpad(col("MBI"), <...>, " ").alias("MBI"),
    rpad(col("DOB"), <...>, " ").alias("DOB"),
    rpad(col("GNDR"), <...>, " ").alias("GNDR"),
    rpad(col("DT_OF_SVC"), <...>, " ").alias("DT_OF_SVC"),
    rpad(col("RNDR_NTNL_PROV_ID"), <...>, " ").alias("RNDR_NTNL_PROV_ID"),
    rpad(col("RNDR_PROV_LAST_NM"), <...>, " ").alias("RNDR_PROV_LAST_NM"),
    rpad(col("RNDR_PROV_FIRST_NM"), <...>, " ").alias("RNDR_PROV_FIRST_NM"),
    rpad(col("DIAG_CD"), <...>, " ").alias("DIAG_CD"),
    rpad(col("CNTRL_NO"), <...>, " ").alias("CNTRL_NO"),
    rpad(col("SOURCE_ID"), <...>, " ").alias("SOURCE_ID"),
    rpad(col("MBR_UNIQ_KEY"), <...>, " ").alias("MBR_UNIQ_KEY"),
    rpad(col("REJECT_REASON"), <...>, " ").alias("REJECT_REASON")
)
write_files(
    df_Clm_Diag_RejectFile,
    f"{adls_path}/verified/{RejectFile}",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote='',
    nullValue=None
)