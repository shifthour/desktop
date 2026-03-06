# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2021 ,2022 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC IdsBCALivongoLabRsltExtrFormatData
# MAGIC Called by: IdsBCALivongoLabRsltSeq
# MAGIC                    
# MAGIC 
# MAGIC Processing: All Lab Pre processing Job. 
# MAGIC                     
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:  Restore the source file before re-running the job
# MAGIC                     Previous Run Aborted: Restart, no other steps necessary     
# MAGIC 
# MAGIC Modifications:                        
# MAGIC \(9)\(9)\(9)\(9)\(9)Project/                                                                                                                             \(9)\(9)\(9)\(9)Develop\(9)\(9)\(9)Code                   \(9)\(9)Date
# MAGIC Developer  \(9)\(9)Date  \(9)\(9)Altiris #\(9)\(9)Change Description\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)Environment\(9)\(9)Reviewer            \(9)\(9)Reviewed
# MAGIC =================================================================================================================================================================================================================
# MAGIC Reddy Sanam   \(9)\(9)2024-01-30\(9)US610374\(9)Original Programming.\(9)\(9)\(9)\(9)\(9)\(9)\(9)\(9)IntegrateDev2\(9)\(9)Goutham Kalidindi               2/26/2024

# MAGIC Rejects
# MAGIC ALL ERROR RECORDS
# MAGIC This part just counts the columns, and if there are not 26 columns, the whole file gets rejected.
# MAGIC These records all get rejected because the file does not have  26 columns.
# MAGIC This stage is where the values of columns are checked for not being populated.  These records get routed to the Error file.
# MAGIC This will check the source code to be LIVONGO. If not, will write the files to the error file
# MAGIC The records that go to the ERROR file are sent here because 
# MAGIC 1) the record contains a NULL for a not-nullable column OR
# MAGIC 2) Prov Look up failed
# MAGIC 3) Loinc lookup failed
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
InFile = get_widget_value('InFile','')
ErrorFile = get_widget_value('ErrorFile','')
RejectFile = get_widget_value('RejectFile','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# ----------------------------------------------------
# DB2ConnectorPX => LOINC_CD
df_LOINC_CD = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"select DISTINCT LOINC_CD\nFROM {IDSOwner}.LOINC_CD")
    .load()
)

# ----------------------------------------------------
# DB2ConnectorPX => PROC_CD
df_PROC_CD_raw = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"select proc_cd, proc_cd_typ_cd from {IDSOwner}.proc_cd\ngroup by proc_cd,proc_cd_typ_cd")
    .load()
)

# RmDup_Proc_CD => PxRemDup (RetainRecord=first, Keys=PROC_CD)
df_RmDup_Proc_CD_dedup = dedup_sort(df_PROC_CD_raw, ["PROC_CD"], [])
df_RmDup_Proc_CD = df_RmDup_Proc_CD_dedup.select("PROC_CD","PROC_CD_TYP_CD")

# ----------------------------------------------------
# Lab_InputFile => PxSequentialFile (Single column "RECORD")
schema_Lab_InputFile = StructType([
    StructField("RECORD", StringType(), True)
])
df_Lab_InputFile = (
    spark.read.format("csv")
    .option("header", True)
    .option("sep", "\u0003")
    .schema(schema_Lab_InputFile)
    .load(f"{adls_path_raw}/landing/{InFile}")
)

# ----------------------------------------------------
# Xfm_Delimter_Chk => CTransformerStage
df_XfmDc_temp = (
    df_Lab_InputFile
    .withColumn("temp_no30", F.regexp_replace(F.col("RECORD"), "\u001E", ""))
    .withColumn("svDelimiter", F.size(F.split(F.col("temp_no30"), "\|")))
)

df_Rec_Out = (
    df_XfmDc_temp
    .filter(F.col("svDelimiter") == 26)
    .selectExpr("regexp_replace(RECORD, '\r', '') as RECORD", "1 as CMN_LKP")
)

df_Lnk_Reject = (
    df_XfmDc_temp
    .filter(F.col("svDelimiter") != 26)
    .selectExpr("regexp_replace(RECORD, '\r', '') as RECORD", "1 as CMN_LKP")
)

# ----------------------------------------------------
# Cp_1 => PxCopy
df_Cp_1_to_RmDups1 = df_Lnk_Reject.select(F.col("CMN_LKP").alias("CMN_LKP"))
df_Cp_1_Out_Cp1 = df_Lnk_Reject.select(F.col("RECORD").alias("RECORD"))

# ----------------------------------------------------
# Xfm_1 => CTransformerStage
df_out_Xfm1 = (
    df_Cp_1_Out_Cp1.select(
        F.split(F.col("RECORD"), "\|").getItem(0).alias("FEP_MBR_ID"),
        F.split(F.col("RECORD"), "\|").getItem(1).alias("CNTR_ID"),
        F.split(F.col("RECORD"), "\|").getItem(2).alias("MBR_FIRST_NM"),
        F.split(F.col("RECORD"), "\|").getItem(3).alias("MBR_MID_NM"),
        F.split(F.col("RECORD"), "\|").getItem(4).alias("MBR_LAST_NM"),
        F.split(F.col("RECORD"), "\|").getItem(5).alias("MBR_DOB"),
        F.split(F.col("RECORD"), "\|").getItem(6).alias("RPTNG_PLN_CD"),
        F.split(F.col("RECORD"), "\|").getItem(7).alias("CLM_ID"),
        F.split(F.col("RECORD"), "\|").getItem(8).alias("CLM_LN_NO"),
        F.split(F.col("RECORD"), "\|").getItem(9).alias("ORDER_PROV_ID"),
        F.split(F.col("RECORD"), "\|").getItem(10).alias("ORDER_PROV_NPI"),
        F.split(F.col("RECORD"), "\|").getItem(11).alias("SVC_DT"),
        F.split(F.col("RECORD"), "\|").getItem(12).alias("PROC_CD"),
        F.split(F.col("RECORD"), "\|").getItem(13).alias("PROC_CD_TYP"),
        F.split(F.col("RECORD"), "\|").getItem(14).alias("TST_CD"),
        F.split(F.col("RECORD"), "\|").getItem(15).alias("TST_CD_TYP"),
        F.split(F.col("RECORD"), "\|").getItem(16).alias("LAB_VAL_NUM"),
        F.split(F.col("RECORD"), "\|").getItem(17).alias("MEASURING_UNIT"),
        F.split(F.col("RECORD"), "\|").getItem(18).alias("LAB_VAL_BINARY"),
        F.split(F.col("RECORD"), "\|").getItem(19).alias("STTUS"),
        F.split(F.col("RECORD"), "\|").getItem(20).alias("RSLT_TYP"),
        F.split(F.col("RECORD"), "\|").getItem(21).alias("RSLT_VAL_FLAG"),
        F.split(F.col("RECORD"), "\|").getItem(22).alias("TYP"),
        F.split(F.col("RECORD"), "\|").getItem(23).alias("DATA_TYP"),
        F.split(F.col("RECORD"), "\|").getItem(24).alias("SRC_TYP"),
        F.split(F.col("RECORD"), "\|").getItem(25).alias("SRC_SYS_NM"),
        F.lit("Record doesnt have 30 columns").alias("REJECT_REASON")
    )
)

# ----------------------------------------------------
# Cp_2 => PxCopy
df_Cp_2_to_Lkp1 = df_Rec_Out.select("RECORD","CMN_LKP")

# ----------------------------------------------------
# Rm_Dups_1 => PxRemDup (Keys=CMN_LKP)
df_Rm_Dups_1_dedup = dedup_sort(df_Cp_1_to_RmDups1, ["CMN_LKP"], [])
df_Rm_Dups_1 = df_Rm_Dups_1_dedup.select("CMN_LKP")

# ----------------------------------------------------
# Lkp_1 => PxLookup => left join primaryLink=Cp_2_to_Lkp1, lookupLink=Rm_Dups_1
df_Lkp_1_joined = (
    df_Cp_2_to_Lkp1.alias("to_Lkp1")
    .join(
        df_Rm_Dups_1.alias("out_RmDups1"),
        F.col("to_Lkp1.CMN_LKP") == F.col("out_RmDups1.CMN_LKP"),
        how="left"
    )
)

df_Lkp_1_to_Xfm2 = df_Lkp_1_joined.select(
    F.col("to_Lkp1.RECORD").alias("RECORD")
)

df_Lkp_1_Out_Lkp1 = df_Lkp_1_joined.select(
    F.col("to_Lkp1.RECORD").alias("RECORD"),
    F.col("to_Lkp1.CMN_LKP").alias("CMN_LKP")
)

# ----------------------------------------------------
# Xfm_2 => CTransformerStage
df_out_ColGen1 = (
    df_Lkp_1_to_Xfm2.select(
        F.split(F.col("RECORD"), "\|").getItem(0).alias("FEP_MBR_ID"),
        F.split(F.col("RECORD"), "\|").getItem(1).alias("CNTR_ID"),
        F.split(F.col("RECORD"), "\|").getItem(2).alias("MBR_FIRST_NM"),
        F.split(F.col("RECORD"), "\|").getItem(3).alias("MBR_MID_NM"),
        F.split(F.col("RECORD"), "\|").getItem(4).alias("MBR_LAST_NM"),
        F.split(F.col("RECORD"), "\|").getItem(5).alias("MBR_DOB"),
        F.split(F.col("RECORD"), "\|").getItem(6).alias("RPTNG_PLN_CD"),
        F.split(F.col("RECORD"), "\|").getItem(7).alias("CLM_ID"),
        F.split(F.col("RECORD"), "\|").getItem(8).alias("CLM_LN_NO"),
        F.split(F.col("RECORD"), "\|").getItem(9).alias("ORDER_PROV_ID"),
        F.split(F.col("RECORD"), "\|").getItem(10).alias("ORDER_PROV_NPI"),
        F.split(F.col("RECORD"), "\|").getItem(11).alias("SVC_DT"),
        F.split(F.col("RECORD"), "\|").getItem(12).alias("PROC_CD"),
        F.split(F.col("RECORD"), "\|").getItem(13).alias("PROC_CD_TYP"),
        F.split(F.col("RECORD"), "\|").getItem(14).alias("TST_CD"),
        F.split(F.col("RECORD"), "\|").getItem(15).alias("TST_CD_TYP"),
        F.split(F.col("RECORD"), "\|").getItem(16).alias("LAB_VAL_NUM"),
        F.split(F.col("RECORD"), "\|").getItem(17).alias("MEASURING_UNIT"),
        F.split(F.col("RECORD"), "\|").getItem(18).alias("LAB_VAL_BINARY"),
        F.split(F.col("RECORD"), "\|").getItem(19).alias("STTUS"),
        F.split(F.col("RECORD"), "\|").getItem(20).alias("RSLT_TYP"),
        F.split(F.col("RECORD"), "\|").getItem(21).alias("RSLT_VAL_FLAG"),
        F.split(F.col("RECORD"), "\|").getItem(22).alias("TYP"),
        F.split(F.col("RECORD"), "\|").getItem(23).alias("DATA_TYP"),
        F.split(F.col("RECORD"), "\|").getItem(24).alias("SRC_TYP"),
        F.split(F.col("RECORD"), "\|").getItem(25).alias("SRC_SYS_NM"),
        F.lit("").alias("REJECT_REASON")
    )
)

# ----------------------------------------------------
# Xfm_5 => CTransformerStage
# Input => df_Lkp_1_Out_Lkp1
# This transformation checks 2 conditions and sets a "reject flag" T or F
df_Xfm_5_temp = (
    df_Lkp_1_Out_Lkp1
    .withColumn("col12_trimmed", F.trim(F.split(F.col("RECORD"), "\|").getItem(11)))
    .withColumn(
        "svSVCDTChk",
        F.when(
            (F.col("col12_trimmed") == "") | ((F.col("col12_trimmed") != "") & (F.length("col12_trimmed") <= 8)),
            F.lit("Column SVC_DT  is either empty or Not Valid Format;")
        ).otherwise(F.lit(""))
    )
    .withColumn(
        "svLOINCChk",
        F.when(
            F.trim(F.split(F.col("RECORD"), "\|").getItem(14)) == "",
            F.lit("Column TST_CD  is empty;")
        ).otherwise(F.lit(""))
    )
)
df_Xfm_5_temp2 = df_Xfm_5_temp.withColumn(
    "svRejectFlg",
    F.when(
        (F.col("svSVCDTChk") == "") & (F.col("svLOINCChk") == ""),
        F.lit("T")
    ).otherwise(F.lit("F"))
).withColumn(
    "svRejectMesg",
    F.concat(F.col("svSVCDTChk"), F.col("svLOINCChk"))
)

df_Xfm_5_Columns_Ou = (
    df_Xfm_5_temp2
    .filter(F.col("svRejectFlg") == "T")
    .select(
        F.split(F.col("RECORD"), "\|").getItem(0).alias("FEP_MBR_ID"),
        F.split(F.col("RECORD"), "\|").getItem(1).alias("CNTR_ID"),
        F.split(F.col("RECORD"), "\|").getItem(2).alias("MBR_FIRST_NM"),
        F.split(F.col("RECORD"), "\|").getItem(3).alias("MBR_MID_NM"),
        F.split(F.col("RECORD"), "\|").getItem(4).alias("MBR_LAST_NM"),
        F.split(F.col("RECORD"), "\|").getItem(5).alias("MBR_DOB"),
        F.split(F.col("RECORD"), "\|").getItem(6).alias("RPTNG_PLN_CD"),
        F.split(F.col("RECORD"), "\|").getItem(7).alias("CLM_ID"),
        F.split(F.col("RECORD"), "\|").getItem(8).alias("CLM_LN_NO"),
        F.split(F.col("RECORD"), "\|").getItem(9).alias("ORDER_PROV_ID"),
        F.split(F.col("RECORD"), "\|").getItem(10).alias("ORDER_PROV_NPI"),
        F.split(F.col("RECORD"), "\|").getItem(11).alias("SVC_DT"),
        F.split(F.col("RECORD"), "\|").getItem(12).alias("PROC_CD"),
        F.split(F.col("RECORD"), "\|").getItem(13).alias("PROC_CD_TYP"),
        F.split(F.col("RECORD"), "\|").getItem(14).alias("TST_CD"),
        F.split(F.col("RECORD"), "\|").getItem(15).alias("TST_CD_TYP"),
        F.split(F.col("RECORD"), "\|").getItem(16).alias("LAB_VAL_NUM"),
        F.split(F.col("RECORD"), "\|").getItem(17).alias("MEASURING_UNIT"),
        F.split(F.col("RECORD"), "\|").getItem(18).alias("LAB_VAL_BINARY"),
        F.split(F.col("RECORD"), "\|").getItem(19).alias("STTUS"),
        F.split(F.col("RECORD"), "\|").getItem(20).alias("RSLT_TYP"),
        F.split(F.col("RECORD"), "\|").getItem(21).alias("RSLT_VAL_FLAG"),
        F.split(F.col("RECORD"), "\|").getItem(22).alias("TYP"),
        F.split(F.col("RECORD"), "\|").getItem(23).alias("DATA_TYP"),
        F.split(F.col("RECORD"), "\|").getItem(24).alias("SRC_TYP"),
        F.upper(F.split(F.col("RECORD"), "\|").getItem(25)).alias("SRC_SYS_NM"),
        F.lit("").alias("REJECT_REASON")
    )
)

df_Xfm_5_Out_Xfm5 = (
    df_Xfm_5_temp2
    .filter(F.col("svRejectFlg") == "F")
    .select(
        F.split(F.col("RECORD"), "\|").getItem(0).alias("FEP_MBR_ID"),
        F.split(F.col("RECORD"), "\|").getItem(1).alias("CNTR_ID"),
        F.split(F.col("RECORD"), "\|").getItem(2).alias("MBR_FIRST_NM"),
        F.split(F.col("RECORD"), "\|").getItem(3).alias("MBR_MID_NM"),
        F.split(F.col("RECORD"), "\|").getItem(4).alias("MBR_LAST_NM"),
        F.split(F.col("RECORD"), "\|").getItem(5).alias("MBR_DOB"),
        F.split(F.col("RECORD"), "\|").getItem(6).alias("RPTNG_PLN_CD"),
        F.split(F.col("RECORD"), "\|").getItem(7).alias("CLM_ID"),
        F.split(F.col("RECORD"), "\|").getItem(8).alias("CLM_LN_NO"),
        F.split(F.col("RECORD"), "\|").getItem(9).alias("ORDER_PROV_ID"),
        F.split(F.col("RECORD"), "\|").getItem(10).alias("ORDER_PROV_NPI"),
        F.split(F.col("RECORD"), "\|").getItem(11).alias("SVC_DT"),
        F.split(F.col("RECORD"), "\|").getItem(12).alias("PROC_CD"),
        F.split(F.col("RECORD"), "\|").getItem(13).alias("PROC_CD_TYP"),
        F.split(F.col("RECORD"), "\|").getItem(14).alias("TST_CD"),
        F.split(F.col("RECORD"), "\|").getItem(15).alias("TST_CD_TYP"),
        F.split(F.col("RECORD"), "\|").getItem(16).alias("LAB_VAL_NUM"),
        F.split(F.col("RECORD"), "\|").getItem(17).alias("MEASURING_UNIT"),
        F.split(F.col("RECORD"), "\|").getItem(18).alias("LAB_VAL_BINARY"),
        F.split(F.col("RECORD"), "\|").getItem(19).alias("STTUS"),
        F.split(F.col("RECORD"), "\|").getItem(20).alias("RSLT_TYP"),
        F.split(F.col("RECORD"), "\|").getItem(21).alias("RSLT_VAL_FLAG"),
        F.split(F.col("RECORD"), "\|").getItem(22).alias("TYP"),
        F.split(F.col("RECORD"), "\|").getItem(23).alias("DATA_TYP"),
        F.split(F.col("RECORD"), "\|").getItem(24).alias("SRC_TYP"),
        F.split(F.col("RECORD"), "\|").getItem(25).alias("SRC_SYS_NM"),
        F.trim(F.col("svRejectMesg"), ';', 'T').alias("ERROR_REASON")
    )
)

# ----------------------------------------------------
# IDS_NTNL_PROV => DB2ConnectorPX
df_IDS_NTNL_PROV = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"select NTNL_PROV_ID, PROV_SK\nFROM {IDSOwner}.PROV\ngroup by NTNL_PROV_ID, PROV_SK")
    .load()
)

# ----------------------------------------------------
# CD_MPPNG => DB2ConnectorPX
df_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"""SELECT  CASE WHEN TRIM(SRC_DRVD_LKUP_VAL) = 'BCA' THEN 'LIVONGO' ELSE TRIM(SRC_DRVD_LKUP_VAL) END AS SRC_DRVD_LKUP_VAL,
       1 AS FLG
FROM {IDSOwner}.CD_MPPNG
WHERE TRGT_DOMAIN_NM='SOURCE SYSTEM'
AND SRC_CLCTN_CD='IDS'
AND SRC_SYS_CD='IDS'
AND TRGT_DOMAIN_NM='SOURCE SYSTEM'
AND TRGT_CLCTN_CD='IDS'
AND SRC_CD = 'BCA'""")
    .load()
)

# ----------------------------------------------------
# Lkp_Source_Id => PxLookup => left join: primaryLink=df_Xfm_5_Columns_Ou, lookupLink=df_CD_MPPNG
df_Lkp_Source_Id_joined = (
    df_Xfm_5_Columns_Ou.alias("Columns_Ou")
    .join(
        df_CD_MPPNG.alias("Lkp_CD_MPPNG"),
        F.col("Columns_Ou.SRC_SYS_NM") == F.col("Lkp_CD_MPPNG.SRC_DRVD_LKUP_VAL"),
        how="left"
    )
)

df_Lkp_Source_Id_to_Xfm3 = df_Lkp_Source_Id_joined.select(
    F.col("Columns_Ou.FEP_MBR_ID").alias("FEP_MBR_ID"),
    F.col("Columns_Ou.CNTR_ID").alias("CNTR_ID"),
    F.col("Columns_Ou.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("Columns_Ou.MBR_MID_NM").alias("MBR_MID_NM"),
    F.col("Columns_Ou.MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("Columns_Ou.MBR_DOB").alias("MBR_DOB"),
    F.col("Columns_Ou.RPTNG_PLN_CD").alias("RPTNG_PLN_CD"),
    F.col("Columns_Ou.CLM_ID").alias("CLM_ID"),
    F.col("Columns_Ou.CLM_LN_NO").alias("CLM_LN_NO"),
    F.col("Columns_Ou.ORDER_PROV_ID").alias("ORDER_PROV_ID"),
    F.col("Columns_Ou.ORDER_PROV_NPI").alias("ORDER_PROV_NPI"),
    F.col("Columns_Ou.SVC_DT").alias("SVC_DT"),
    F.col("Columns_Ou.PROC_CD").alias("PROC_CD"),
    F.col("Columns_Ou.PROC_CD_TYP").alias("PROC_CD_TYP"),
    F.col("Columns_Ou.TST_CD").alias("TST_CD"),
    F.col("Columns_Ou.TST_CD_TYP").alias("TST_CD_TYP"),
    F.col("Columns_Ou.LAB_VAL_NUM").alias("LAB_VAL_NUM"),
    F.col("Columns_Ou.MEASURING_UNIT").alias("MEASURING_UNIT"),
    F.col("Columns_Ou.LAB_VAL_BINARY").alias("LAB_VAL_BINARY"),
    F.col("Columns_Ou.STTUS").alias("STTUS"),
    F.col("Columns_Ou.RSLT_TYP").alias("RSLT_TYP"),
    F.col("Columns_Ou.RSLT_VAL_FLAG").alias("RSLT_VAL_FLAG"),
    F.col("Columns_Ou.TYP").alias("TYP"),
    F.col("Columns_Ou.DATA_TYP").alias("DATA_TYP"),
    F.col("Columns_Ou.SRC_TYP").alias("SRC_TYP"),
    F.col("Columns_Ou.SRC_SYS_NM").alias("SRC_SYS_NM"),
    F.col("Lkp_CD_MPPNG.FLG").alias("FLG")
)

# ----------------------------------------------------
# Xfm_3 => CTransformerStage
df_Xfm_3_Reject = df_Lkp_Source_Id_to_Xfm3.filter(F.col("FLG") == 0)
df_Xfm_3_NoRejects = df_Lkp_Source_Id_to_Xfm3.filter(F.col("FLG") == 1)

df_Xfm_3_Reject_out = df_Xfm_3_Reject.select(
    "FEP_MBR_ID","CNTR_ID","MBR_FIRST_NM","MBR_MID_NM","MBR_LAST_NM","MBR_DOB","RPTNG_PLN_CD","CLM_ID","CLM_LN_NO",
    "ORDER_PROV_ID","ORDER_PROV_NPI","SVC_DT","PROC_CD","PROC_CD_TYP","TST_CD","TST_CD_TYP","LAB_VAL_NUM","MEASURING_UNIT",
    "LAB_VAL_BINARY","STTUS","RSLT_TYP","RSLT_VAL_FLAG","TYP","DATA_TYP","SRC_TYP","SRC_SYS_NM"
).withColumn("LKP", F.lit("1"))

df_Xfm_3_NoRejects_out = df_Xfm_3_NoRejects.select(
    "FEP_MBR_ID","CNTR_ID","MBR_FIRST_NM","MBR_MID_NM","MBR_LAST_NM","MBR_DOB","RPTNG_PLN_CD","CLM_ID","CLM_LN_NO",
    "ORDER_PROV_ID","ORDER_PROV_NPI","SVC_DT","PROC_CD","PROC_CD_TYP","TST_CD","TST_CD_TYP","LAB_VAL_NUM","MEASURING_UNIT",
    "LAB_VAL_BINARY","STTUS","RSLT_TYP","RSLT_VAL_FLAG","TYP","DATA_TYP","SRC_TYP","SRC_SYS_NM"
).withColumn("LKP", F.lit("1"))

# ----------------------------------------------------
# Rm_Dups3 => PxRemDup => input from Xfm_3_Reject => key = LKP
df_Rm_Dups3_dedup = dedup_sort(df_Xfm_3_Reject_out, ["LKP"], [])
df_Rm_Dups3 = df_Rm_Dups3_dedup.select("LKP")

# ----------------------------------------------------
# Lkp => PxLookup => primaryLink=df_Xfm_3_NoRejects_out, lookupLink=df_Rm_Dups3
df_Lkp_joined = (
    df_Xfm_3_NoRejects_out.alias("NoRejects")
    .join(
        df_Rm_Dups3.alias("to_Lkp"),
        F.col("NoRejects.LKP") == F.col("to_Lkp.LKP"),
        how="left"
    )
)

df_Lkp_to_Xfm4 = df_Lkp_joined.select(
    F.col("NoRejects.FEP_MBR_ID").alias("FEP_MBR_ID"),
    F.col("NoRejects.CNTR_ID").alias("CNTR_ID"),
    F.col("NoRejects.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("NoRejects.MBR_MID_NM").alias("MBR_MID_NM"),
    F.col("NoRejects.MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("NoRejects.MBR_DOB").alias("MBR_DOB"),
    F.col("NoRejects.RPTNG_PLN_CD").alias("RPTNG_PLN_CD"),
    F.col("NoRejects.CLM_ID").alias("CLM_ID"),
    F.col("NoRejects.CLM_LN_NO").alias("CLM_LN_NO"),
    F.col("NoRejects.ORDER_PROV_ID").alias("ORDER_PROV_ID"),
    F.col("NoRejects.ORDER_PROV_NPI").alias("ORDER_PROV_NPI"),
    F.col("NoRejects.SVC_DT").alias("SVC_DT"),
    F.col("NoRejects.PROC_CD").alias("PROC_CD"),
    F.col("NoRejects.PROC_CD_TYP").alias("PROC_CD_TYP"),
    F.col("NoRejects.TST_CD").alias("TST_CD"),
    F.col("NoRejects.TST_CD_TYP").alias("TST_CD_TYP"),
    F.col("NoRejects.LAB_VAL_NUM").alias("LAB_VAL_NUM"),
    F.col("NoRejects.MEASURING_UNIT").alias("MEASURING_UNIT"),
    F.col("NoRejects.LAB_VAL_BINARY").alias("LAB_VAL_BINARY"),
    F.col("NoRejects.STTUS").alias("STTUS"),
    F.col("NoRejects.RSLT_TYP").alias("RSLT_TYP"),
    F.col("NoRejects.RSLT_VAL_FLAG").alias("RSLT_VAL_FLAG"),
    F.col("NoRejects.TYP").alias("TYP"),
    F.col("NoRejects.DATA_TYP").alias("DATA_TYP"),
    F.col("NoRejects.SRC_TYP").alias("SRC_TYP"),
    F.col("NoRejects.SRC_SYS_NM").alias("SRC_SYS_NM"),
    F.col("NoRejects.LKP").alias("LKP")
)

df_Lkp_main = df_Lkp_joined.select(
    F.col("NoRejects.FEP_MBR_ID").alias("FEP_MBR_ID"),
    F.col("NoRejects.CNTR_ID").alias("CNTR_ID"),
    F.col("NoRejects.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("NoRejects.MBR_MID_NM").alias("MBR_MID_NM"),
    F.col("NoRejects.MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("NoRejects.MBR_DOB").alias("MBR_DOB"),
    F.col("NoRejects.RPTNG_PLN_CD").alias("RPTNG_PLN_CD"),
    F.col("NoRejects.CLM_ID").alias("CLM_ID"),
    F.col("NoRejects.CLM_LN_NO").alias("CLM_LN_NO"),
    F.col("NoRejects.ORDER_PROV_ID").alias("ORDER_PROV_ID"),
    F.col("NoRejects.ORDER_PROV_NPI").alias("ORDER_PROV_NPI"),
    F.col("NoRejects.SVC_DT").alias("SVC_DT"),
    F.col("NoRejects.PROC_CD").alias("PROC_CD"),
    F.col("NoRejects.PROC_CD_TYP").alias("PROC_CD_TYP"),
    F.col("NoRejects.TST_CD").alias("TST_CD"),
    F.col("NoRejects.TST_CD_TYP").alias("TST_CD_TYP"),
    F.col("NoRejects.LAB_VAL_NUM").alias("LAB_VAL_NUM"),
    F.col("NoRejects.MEASURING_UNIT").alias("MEASURING_UNIT"),
    F.col("NoRejects.LAB_VAL_BINARY").alias("LAB_VAL_BINARY"),
    F.col("NoRejects.STTUS").alias("STTUS"),
    F.col("NoRejects.RSLT_TYP").alias("RSLT_TYP"),
    F.col("NoRejects.RSLT_VAL_FLAG").alias("RSLT_VAL_FLAG"),
    F.col("NoRejects.TYP").alias("TYP"),
    F.col("NoRejects.DATA_TYP").alias("DATA_TYP"),
    F.col("NoRejects.SRC_TYP").alias("SRC_TYP"),
    F.col("NoRejects.SRC_SYS_NM").alias("SRC_SYS_NM"),
    F.lit(1).alias("LKP")
)

# ----------------------------------------------------
# Xfm_4 => CTransformerStage => input = df_Lkp_to_Xfm4
df_out_Xfm4 = (
    df_Lkp_to_Xfm4.select(
        F.col("FEP_MBR_ID"),
        F.col("CNTR_ID"),
        F.col("MBR_FIRST_NM"),
        F.col("MBR_MID_NM"),
        F.col("MBR_LAST_NM"),
        F.col("MBR_DOB"),
        F.col("RPTNG_PLN_CD"),
        F.col("CLM_ID"),
        F.col("CLM_LN_NO"),
        F.col("ORDER_PROV_ID"),
        F.col("ORDER_PROV_NPI"),
        F.col("SVC_DT"),
        F.col("PROC_CD"),
        F.col("PROC_CD_TYP"),
        F.col("TST_CD"),
        F.col("TST_CD_TYP"),
        F.col("LAB_VAL_NUM"),
        F.col("MEASURING_UNIT"),
        F.col("LAB_VAL_BINARY"),
        F.col("STTUS"),
        F.col("RSLT_TYP"),
        F.col("RSLT_VAL_FLAG"),
        F.col("TYP"),
        F.col("DATA_TYP"),
        F.col("SRC_TYP"),
        F.col("SRC_SYS_NM"),
        F.lit("SOURCE ID Does not match").alias("REJECT_REASON")
    )
)

# ----------------------------------------------------
# Fn_1 => PxFunnel with 3 inputs: df_out_ColGen1, df_out_Xfm1, df_out_Xfm4
df_Fn_1_all = df_out_ColGen1.select(
    "FEP_MBR_ID","CNTR_ID","MBR_FIRST_NM","MBR_MID_NM","MBR_LAST_NM","MBR_DOB","RPTNG_PLN_CD","CLM_ID","CLM_LN_NO",
    "ORDER_PROV_ID","ORDER_PROV_NPI","SVC_DT","PROC_CD","PROC_CD_TYP","TST_CD","TST_CD_TYP","LAB_VAL_NUM",
    "MEASURING_UNIT","LAB_VAL_BINARY","STTUS","RSLT_TYP","RSLT_VAL_FLAG","TYP","DATA_TYP","SRC_TYP","SRC_SYS_NM","REJECT_REASON"
).unionByName(
    df_out_Xfm1.select(
        "FEP_MBR_ID","CNTR_ID","MBR_FIRST_NM","MBR_MID_NM","MBR_LAST_NM","MBR_DOB","RPTNG_PLN_CD","CLM_ID","CLM_LN_NO",
        "ORDER_PROV_ID","ORDER_PROV_NPI","SVC_DT","PROC_CD","PROC_CD_TYP","TST_CD","TST_CD_TYP","LAB_VAL_NUM",
        "MEASURING_UNIT","LAB_VAL_BINARY","STTUS","RSLT_TYP","RSLT_VAL_FLAG","TYP","DATA_TYP","SRC_TYP","SRC_SYS_NM","REJECT_REASON"
    )
).unionByName(
    df_out_Xfm4.select(
        "FEP_MBR_ID","CNTR_ID","MBR_FIRST_NM","MBR_MID_NM","MBR_LAST_NM","MBR_DOB","RPTNG_PLN_CD","CLM_ID","CLM_LN_NO",
        "ORDER_PROV_ID","ORDER_PROV_NPI","SVC_DT","PROC_CD","PROC_CD_TYP","TST_CD","TST_CD_TYP","LAB_VAL_NUM",
        "MEASURING_UNIT","LAB_VAL_BINARY","STTUS","RSLT_TYP","RSLT_VAL_FLAG","TYP","DATA_TYP","SRC_TYP","SRC_SYS_NM","REJECT_REASON"
    )
)

# Lab_RejectFile => PxSequentialFile
# Write df_Fn_1_all
write_files(
    df_Fn_1_all.select(
        "FEP_MBR_ID","CNTR_ID","MBR_FIRST_NM","MBR_MID_NM","MBR_LAST_NM","MBR_DOB","RPTNG_PLN_CD","CLM_ID","CLM_LN_NO",
        "ORDER_PROV_ID","ORDER_PROV_NPI","SVC_DT","PROC_CD","PROC_CD_TYP","TST_CD","TST_CD_TYP","LAB_VAL_NUM",
        "MEASURING_UNIT","LAB_VAL_BINARY","STTUS","RSLT_TYP","RSLT_VAL_FLAG","TYP","DATA_TYP","SRC_TYP","SRC_SYS_NM","REJECT_REASON"
    ),
    f"{adls_path}/verified/{RejectFile}",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

# ----------------------------------------------------
# Xfm_Column_Chk => CTransformerStage => input = df_Lkp_main
df_Xfm_Column_Chk = df_Lkp_main.select(
    F.col("FEP_MBR_ID"),
    F.col("CNTR_ID"),
    F.col("MBR_FIRST_NM"),
    F.col("MBR_MID_NM"),
    F.col("MBR_LAST_NM"),
    F.col("MBR_DOB"),
    F.col("RPTNG_PLN_CD"),
    F.col("CLM_ID"),
    F.col("CLM_LN_NO"),
    F.col("ORDER_PROV_ID"),
    F.col("ORDER_PROV_NPI"),
    F.col("SVC_DT"),
    trim(F.col("PROC_CD")).alias("PROC_CD"),
    F.col("PROC_CD_TYP"),
    F.col("TST_CD"),
    F.col("TST_CD_TYP"),
    F.col("LAB_VAL_NUM"),
    F.col("MEASURING_UNIT"),
    F.col("LAB_VAL_BINARY"),
    F.col("STTUS"),
    F.col("RSLT_TYP"),
    F.col("RSLT_VAL_FLAG"),
    F.col("TYP"),
    F.col("DATA_TYP"),
    F.col("SRC_TYP"),
    F.col("SRC_SYS_NM")
)

# ----------------------------------------------------
# FEP_MBR_ENR => DB2ConnectorPX
df_FEP_MBR_ENR_raw = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"""select MBR.MBR_SK AS MBR_SK,
MBR.MBR_UNIQ_KEY AS MBR_UNIQ_KEY,
MBR.TERM_DT_SK AS TERM_DT_SK,
FEP_MBR_ENR.LGCY_FEP_MBR_ID AS LGCY_FEP_MBR_ID 
from {IDSOwner}.MBR MBR
INNER JOIN {IDSOwner}.FEP_MBR_ENR  FEP_MBR_ENR ON MBR.MBR_SK = FEP_MBR_ENR.MBR_SK
GROUP BY MBR.MBR_SK,MBR.MBR_UNIQ_KEY,MBR.TERM_DT_SK,FEP_MBR_ENR.LGCY_FEP_MBR_ID""")
    .load()
)

# RmDup_TermDate => PxRemDup => Keys = LGCY_FEP_MBR_ID with a sorting => we rely on dedup_sort
df_RmDup_TermDate_dedup = dedup_sort(df_FEP_MBR_ENR_raw, ["LGCY_FEP_MBR_ID"], [])
df_RmDup_TermDate = df_RmDup_TermDate_dedup.select(
    "MBR_SK","MBR_UNIQ_KEY","TERM_DT_SK","LGCY_FEP_MBR_ID"
)

# ----------------------------------------------------
# Lnk_MbrID => PxLookup => primaryLink=df_Xfm_Column_Chk, lookupLink=df_RmDup_TermDate => left join
df_Lnk_MbrID_joined = (
    df_Xfm_Column_Chk.alias("Lnk_Valid")
    .join(
        df_RmDup_TermDate.alias("LkpRef_MbrID"),
        F.col("Lnk_Valid.FEP_MBR_ID") == F.col("LkpRef_MbrID.LGCY_FEP_MBR_ID"),
        how="left"
    )
)

df_Lnk_MbrID_Out_XfmValid = df_Lnk_MbrID_joined.select(
    F.col("Lnk_Valid.FEP_MBR_ID").alias("FEP_MBR_ID"),
    F.col("Lnk_Valid.CNTR_ID").alias("CNTR_ID"),
    F.col("Lnk_Valid.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("Lnk_Valid.MBR_MID_NM").alias("MBR_MID_NM"),
    F.col("Lnk_Valid.MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("Lnk_Valid.MBR_DOB").alias("MBR_DOB"),
    F.col("Lnk_Valid.RPTNG_PLN_CD").alias("RPTNG_PLN_CD"),
    F.col("Lnk_Valid.CLM_ID").alias("CLM_ID"),
    F.col("Lnk_Valid.CLM_LN_NO").alias("CLM_LN_NO"),
    F.col("Lnk_Valid.ORDER_PROV_ID").alias("ORDER_PROV_ID"),
    F.col("Lnk_Valid.ORDER_PROV_NPI").alias("ORDER_PROV_NPI"),
    F.col("Lnk_Valid.SVC_DT").alias("SVC_DT"),
    F.col("Lnk_Valid.PROC_CD").alias("PROC_CD"),
    F.col("Lnk_Valid.PROC_CD_TYP").alias("PROC_CD_TYP"),
    F.col("Lnk_Valid.TST_CD").alias("TST_CD"),
    F.col("Lnk_Valid.TST_CD_TYP").alias("TST_CD_TYP"),
    F.col("Lnk_Valid.LAB_VAL_NUM").alias("LAB_VAL_NUM"),
    F.col("Lnk_Valid.MEASURING_UNIT").alias("MEASURING_UNIT"),
    F.col("Lnk_Valid.LAB_VAL_BINARY").alias("LAB_VAL_BINARY"),
    F.col("Lnk_Valid.STTUS").alias("STTUS"),
    F.col("Lnk_Valid.RSLT_TYP").alias("RSLT_TYP"),
    F.col("Lnk_Valid.RSLT_VAL_FLAG").alias("RSLT_VAL_FLAG"),
    F.col("Lnk_Valid.TYP").alias("TYP"),
    F.col("Lnk_Valid.DATA_TYP").alias("DATA_TYP"),
    F.col("Lnk_Valid.SRC_TYP").alias("SRC_TYP"),
    F.col("Lnk_Valid.SRC_SYS_NM").alias("SRC_SYS_NM"),
    F.col("LkpRef_MbrID.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("LkpRef_MbrID.MBR_SK").alias("MBR_SK")
)

df_Lnk_MbrID_Mbr_Rej = df_Lnk_MbrID_joined.select(
    F.col("Lnk_Valid.FEP_MBR_ID").alias("FEP_MBR_ID"),
    F.col("Lnk_Valid.CNTR_ID").alias("CNTR_ID"),
    F.col("Lnk_Valid.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("Lnk_Valid.MBR_MID_NM").alias("MBR_MID_NM"),
    F.col("Lnk_Valid.MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("Lnk_Valid.MBR_DOB").alias("MBR_DOB"),
    F.col("Lnk_Valid.RPTNG_PLN_CD").alias("RPTNG_PLN_CD"),
    F.col("Lnk_Valid.CLM_ID").alias("CLM_ID"),
    F.col("Lnk_Valid.CLM_LN_NO").alias("CLM_LN_NO"),
    F.col("Lnk_Valid.ORDER_PROV_ID").alias("ORDER_PROV_ID"),
    F.col("Lnk_Valid.ORDER_PROV_NPI").alias("ORDER_PROV_NPI"),
    F.col("Lnk_Valid.SVC_DT").alias("SVC_DT"),
    F.col("Lnk_Valid.PROC_CD").alias("PROC_CD"),
    F.col("Lnk_Valid.PROC_CD_TYP").alias("PROC_CD_TYP"),
    F.col("Lnk_Valid.TST_CD").alias("TST_CD"),
    F.col("Lnk_Valid.TST_CD_TYP").alias("TST_CD_TYP"),
    F.col("Lnk_Valid.LAB_VAL_NUM").alias("LAB_VAL_NUM"),
    F.col("Lnk_Valid.MEASURING_UNIT").alias("MEASURING_UNIT"),
    F.col("Lnk_Valid.LAB_VAL_BINARY").alias("LAB_VAL_BINARY"),
    F.col("Lnk_Valid.STTUS").alias("STTUS"),
    F.col("Lnk_Valid.RSLT_TYP").alias("RSLT_TYP"),
    F.col("Lnk_Valid.RSLT_VAL_FLAG").alias("RSLT_VAL_FLAG"),
    F.col("Lnk_Valid.TYP").alias("TYP"),
    F.col("Lnk_Valid.DATA_TYP").alias("DATA_TYP"),
    F.col("Lnk_Valid.SRC_TYP").alias("SRC_TYP"),
    F.col("Lnk_Valid.SRC_SYS_NM").alias("SRC_SYS_NM")
)

# ----------------------------------------------------
# Xmr_Mbr_Rej => CTransformerStage => input = df_Lnk_MbrID_Mbr_Rej
df_Xmr_Mbr_Rej = df_Lnk_MbrID_Mbr_Rej.select(
    "FEP_MBR_ID","CNTR_ID","MBR_FIRST_NM","MBR_MID_NM","MBR_LAST_NM","MBR_DOB","RPTNG_PLN_CD","CLM_ID","CLM_LN_NO",
    "ORDER_PROV_ID","ORDER_PROV_NPI","SVC_DT","PROC_CD","PROC_CD_TYP","TST_CD","TST_CD_TYP","LAB_VAL_NUM","MEASURING_UNIT",
    "LAB_VAL_BINARY","STTUS","RSLT_TYP","RSLT_VAL_FLAG","TYP","DATA_TYP","SRC_TYP","SRC_SYS_NM"
).withColumn("ERROR_REASON", F.lit("FEP_MBR_ID Not Found"))

# ----------------------------------------------------
# Rm_Dups_5 => PxRemDup => input = df_Lnk_MbrID_Out_XfmValid => big list of keys
df_Rm_Dups_5_dedup = dedup_sort(
    df_Lnk_MbrID_Out_XfmValid,
    [
        "CLM_ID","CLM_LN_NO","CNTR_ID","DATA_TYP","FEP_MBR_ID","LAB_VAL_BINARY","LAB_VAL_NUM",
        "MBR_DOB","MBR_FIRST_NM","MBR_LAST_NM","MBR_MID_NM","MBR_SK","MBR_UNIQ_KEY","MEASURING_UNIT",
        "ORDER_PROV_ID","ORDER_PROV_NPI","PROC_CD","PROC_CD_TYP","RPTNG_PLN_CD","RSLT_TYP","RSLT_VAL_FLAG",
        "SRC_SYS_NM","SRC_TYP","STTUS","SVC_DT","TST_CD","TST_CD_TYP","TYP"
    ],
    []
)
df_Rm_Dups_5 = df_Rm_Dups_5_dedup.select(
    "FEP_MBR_ID","CNTR_ID","MBR_FIRST_NM","MBR_MID_NM","MBR_LAST_NM","MBR_DOB","RPTNG_PLN_CD","CLM_ID","CLM_LN_NO",
    "ORDER_PROV_ID","ORDER_PROV_NPI","SVC_DT","PROC_CD","PROC_CD_TYP","TST_CD","TST_CD_TYP","LAB_VAL_NUM","MEASURING_UNIT",
    "LAB_VAL_BINARY","STTUS","RSLT_TYP","RSLT_VAL_FLAG","TYP","DATA_TYP","SRC_TYP","SRC_SYS_NM","MBR_UNIQ_KEY","MBR_SK"
)

# ----------------------------------------------------
# Lkp3 => PxLookup => left join => 2 links => Lnk_Valid (primary) and Lkp_PROV_ID (IDS_NTNL_PROV)
# but the JSON reads: "LinkName": "Lkp_PROV_ID", "JoinType": "left", join on ORDER_PROV_NPI=NTNL_PROV_ID
df_Lkp3_joined = (
    df_Rm_Dups_5.alias("Lnk_Valid")
    .join(
        df_IDS_NTNL_PROV.alias("Lkp_PROV_ID"),
        F.col("Lnk_Valid.ORDER_PROV_NPI") == F.col("Lkp_PROV_ID.NTNL_PROV_ID"),
        how="left"
    )
)

df_Lkp3_to_Lkp4 = df_Lkp3_joined.select(
    F.col("Lnk_Valid.FEP_MBR_ID").alias("FEP_MBR_ID"),
    F.col("Lnk_Valid.CNTR_ID").alias("CNTR_ID"),
    F.col("Lnk_Valid.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("Lnk_Valid.MBR_MID_NM").alias("MBR_MID_NM"),
    F.col("Lnk_Valid.MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("Lnk_Valid.MBR_DOB").alias("MBR_DOB"),
    F.col("Lnk_Valid.RPTNG_PLN_CD").alias("RPTNG_PLN_CD"),
    F.col("Lnk_Valid.CLM_ID").alias("CLM_ID"),
    F.col("Lnk_Valid.CLM_LN_NO").alias("CLM_LN_NO"),
    F.col("Lnk_Valid.ORDER_PROV_ID").alias("ORDER_PROV_ID"),
    F.col("Lnk_Valid.ORDER_PROV_NPI").alias("ORDER_PROV_NPI"),
    F.col("Lnk_Valid.SVC_DT").alias("SVC_DT"),
    F.col("Lnk_Valid.PROC_CD").alias("PROC_CD"),
    F.col("Lnk_Valid.PROC_CD_TYP").alias("PROC_CD_TYP"),
    F.col("Lnk_Valid.TST_CD").alias("TST_CD"),
    F.col("Lnk_Valid.TST_CD_TYP").alias("TST_CD_TYP"),
    F.col("Lnk_Valid.LAB_VAL_NUM").alias("LAB_VAL_NUM"),
    F.col("Lnk_Valid.MEASURING_UNIT").alias("MEASURING_UNIT"),
    F.col("Lnk_Valid.LAB_VAL_BINARY").alias("LAB_VAL_BINARY"),
    F.col("Lnk_Valid.STTUS").alias("STTUS"),
    F.col("Lnk_Valid.RSLT_TYP").alias("RSLT_TYP"),
    F.col("Lnk_Valid.RSLT_VAL_FLAG").alias("RSLT_VAL_FLAG"),
    F.col("Lnk_Valid.TYP").alias("TYP"),
    F.col("Lnk_Valid.DATA_TYP").alias("DATA_TYP"),
    F.col("Lnk_Valid.SRC_TYP").alias("SRC_TYP"),
    F.col("Lnk_Valid.SRC_SYS_NM").alias("SRC_SYS_NM"),
    F.col("Lnk_Valid.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("Lnk_Valid.MBR_SK").alias("MBR_SK"),
    F.col("Lkp_PROV_ID.PROV_SK").alias("PROV_SK")
)

df_Lkp3_Out_Lkp3 = df_Rm_Dups_5.select(
    F.col("FEP_MBR_ID"),
    F.col("CNTR_ID"),
    F.col("MBR_FIRST_NM"),
    F.col("MBR_MID_NM"),
    F.col("MBR_LAST_NM"),
    F.col("MBR_DOB"),
    F.col("RPTNG_PLN_CD"),
    F.col("CLM_ID"),
    F.col("CLM_LN_NO"),
    F.col("ORDER_PROV_ID"),
    F.col("ORDER_PROV_NPI"),
    F.col("SVC_DT"),
    F.col("PROC_CD"),
    F.col("PROC_CD_TYP"),
    F.col("TST_CD"),
    F.col("TST_CD_TYP"),
    F.col("LAB_VAL_NUM"),
    F.col("MEASURING_UNIT"),
    F.col("LAB_VAL_BINARY"),
    F.col("STTUS"),
    F.col("RSLT_TYP"),
    F.col("RSLT_VAL_FLAG"),
    F.col("TYP"),
    F.col("DATA_TYP"),
    F.col("SRC_TYP"),
    F.col("SRC_SYS_NM"),
    F.lit("1").alias("LKP"),
    F.col("MBR_UNIQ_KEY"),
    F.col("MBR_SK")
)

# ----------------------------------------------------
# Lkp_4 => PxLookup => primaryLink=df_Lkp3_to_Lkp4, lookupLink=df_LOINC_CD => left join TST_CD=LOINC_CD
df_Lkp_4_joined = (
    df_Lkp3_to_Lkp4.alias("to_Lkp4")
    .join(
        df_LOINC_CD.alias("Lkp_LOINC_CD"),
        F.col("to_Lkp4.TST_CD") == F.col("Lkp_LOINC_CD.LOINC_CD"),
        how="left"
    )
)

df_Lkp_4_LkpO_ProcCd = df_Lkp_4_joined.select(
    F.col("to_Lkp4.FEP_MBR_ID").alias("FEP_MBR_ID"),
    F.col("to_Lkp4.CNTR_ID").alias("CNTR_ID"),
    F.col("to_Lkp4.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("to_Lkp4.MBR_MID_NM").alias("MBR_MID_NM"),
    F.col("to_Lkp4.MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("to_Lkp4.MBR_DOB").alias("MBR_DOB"),
    F.col("to_Lkp4.RPTNG_PLN_CD").alias("RPTNG_PLN_CD"),
    F.col("to_Lkp4.CLM_ID").alias("CLM_ID"),
    F.col("to_Lkp4.CLM_LN_NO").alias("CLM_LN_NO"),
    F.col("to_Lkp4.ORDER_PROV_ID").alias("ORDER_PROV_ID"),
    F.col("to_Lkp4.ORDER_PROV_NPI").alias("ORDER_PROV_NPI"),
    F.col("to_Lkp4.SVC_DT").alias("SVC_DT"),
    F.col("to_Lkp4.PROC_CD").alias("PROC_CD"),
    F.col("to_Lkp4.PROC_CD_TYP").alias("PROC_CD_TYP"),
    F.col("to_Lkp4.TST_CD").alias("TST_CD"),
    F.col("to_Lkp4.TST_CD_TYP").alias("TST_CD_TYP"),
    F.col("to_Lkp4.LAB_VAL_NUM").alias("LAB_VAL_NUM"),
    F.col("to_Lkp4.MEASURING_UNIT").alias("MEASURING_UNIT"),
    F.col("to_Lkp4.LAB_VAL_BINARY").alias("LAB_VAL_BINARY"),
    F.col("to_Lkp4.STTUS").alias("STTUS"),
    F.col("to_Lkp4.RSLT_TYP").alias("RSLT_TYP"),
    F.col("to_Lkp4.RSLT_VAL_FLAG").alias("RSLT_VAL_FLAG"),
    F.col("to_Lkp4.TYP").alias("TYP"),
    F.col("to_Lkp4.DATA_TYP").alias("DATA_TYP"),
    F.col("to_Lkp4.SRC_TYP").alias("SRC_TYP"),
    F.col("to_Lkp4.SRC_SYS_NM").alias("SRC_SYS_NM"),
    F.col("to_Lkp4.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("to_Lkp4.MBR_SK").alias("MBR_SK"),
    F.col("to_Lkp4.PROV_SK").alias("PROV_SK")
)

df_Lkp_4_out_Lkp4 = df_Lkp3_to_Lkp4.select(
    F.col("FEP_MBR_ID"),
    F.col("CNTR_ID"),
    F.col("MBR_FIRST_NM"),
    F.col("MBR_MID_NM"),
    F.col("MBR_LAST_NM"),
    F.col("MBR_DOB"),
    F.col("RPTNG_PLN_CD"),
    F.col("CLM_ID"),
    F.col("CLM_LN_NO"),
    F.col("ORDER_PROV_ID"),
    F.col("ORDER_PROV_NPI"),
    F.col("SVC_DT"),
    F.col("PROC_CD"),
    F.col("PROC_CD_TYP"),
    F.col("TST_CD"),
    F.col("TST_CD_TYP"),
    F.col("LAB_VAL_NUM"),
    F.col("MEASURING_UNIT"),
    F.col("LAB_VAL_BINARY"),
    F.col("STTUS"),
    F.col("RSLT_TYP"),
    F.col("RSLT_VAL_FLAG"),
    F.col("TYP"),
    F.col("DATA_TYP"),
    F.col("SRC_TYP"),
    F.col("SRC_SYS_NM"),
    F.lit("1").alias("LKP"),
    F.col("MBR_UNIQ_KEY"),
    F.col("MBR_SK"),
    F.col("PROV_SK").alias("PROV_SK")
)

# ----------------------------------------------------
# Lkp_Proc_CD => PxLookup => primaryLink=df_Lkp_4_LkpO_ProcCd, lookupLink=df_RmDup_Proc_CD => left join on PROC_CD
df_Lkp_Proc_CD_joined = (
    df_Lkp_4_LkpO_ProcCd.alias("LkpO_ProcCd")
    .join(
        df_RmDup_Proc_CD.alias("Lkp_PROC_CD"),
        F.col("LkpO_ProcCd.PROC_CD") == F.col("Lkp_PROC_CD.PROC_CD"),
        how="left"
    )
)

df_Lkp_Proc_CD_main = df_Lkp_Proc_CD_joined.select(
    F.col("LkpO_ProcCd.FEP_MBR_ID").alias("FEP_MBR_ID"),
    F.col("LkpO_ProcCd.CNTR_ID").alias("CNTR_ID"),
    F.col("LkpO_ProcCd.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("LkpO_ProcCd.MBR_MID_NM").alias("MBR_MID_NM"),
    F.col("LkpO_ProcCd.MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("LkpO_ProcCd.MBR_DOB").alias("MBR_DOB"),
    F.col("LkpO_ProcCd.RPTNG_PLN_CD").alias("RPTNG_PLN_CD"),
    F.col("LkpO_ProcCd.CLM_ID").alias("CLM_ID"),
    F.col("LkpO_ProcCd.CLM_LN_NO").alias("CLM_LN_NO"),
    F.col("LkpO_ProcCd.ORDER_PROV_ID").alias("ORDER_PROV_ID"),
    F.col("LkpO_ProcCd.ORDER_PROV_NPI").alias("ORDER_PROV_NPI"),
    F.col("LkpO_ProcCd.SVC_DT").alias("SVC_DT"),
    F.col("LkpO_ProcCd.PROC_CD").alias("PROC_CD"),
    F.col("LkpO_ProcCd.PROC_CD_TYP").alias("PROC_CD_TYP"),
    F.col("LkpO_ProcCd.TST_CD").alias("TST_CD"),
    F.col("LkpO_ProcCd.TST_CD_TYP").alias("TST_CD_TYP"),
    F.col("LkpO_ProcCd.LAB_VAL_NUM").alias("LAB_VAL_NUM"),
    F.col("LkpO_ProcCd.MEASURING_UNIT").alias("MEASURING_UNIT"),
    F.col("LkpO_ProcCd.LAB_VAL_BINARY").alias("LAB_VAL_BINARY"),
    F.col("LkpO_ProcCd.STTUS").alias("STTUS"),
    F.col("LkpO_ProcCd.RSLT_TYP").alias("RSLT_TYP"),
    F.col("LkpO_ProcCd.RSLT_VAL_FLAG").alias("RSLT_VAL_FLAG"),
    F.col("LkpO_ProcCd.TYP").alias("TYP"),
    F.col("LkpO_ProcCd.DATA_TYP").alias("DATA_TYP"),
    F.col("LkpO_ProcCd.SRC_TYP").alias("SRC_TYP"),
    F.col("LkpO_ProcCd.SRC_SYS_NM").alias("SRC_SYS_NM"),
    F.col("LkpO_ProcCd.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("LkpO_ProcCd.MBR_SK").alias("MBR_SK"),
    F.col("LkpO_ProcCd.PROV_SK").alias("PROV_SK"),
    F.col("Lkp_PROC_CD.PROC_CD_TYP_CD").alias("PROC_CD_TYP_CD")
)

# ----------------------------------------------------
# Xfm8 => CTransformerStage => input = df_Lkp_Proc_CD_main
df_Xfm8 = df_Lkp_Proc_CD_main.select(
    "FEP_MBR_ID","CNTR_ID","MBR_FIRST_NM","MBR_MID_NM","MBR_LAST_NM","MBR_DOB","RPTNG_PLN_CD","CLM_ID","CLM_LN_NO",
    "ORDER_PROV_ID","ORDER_PROV_NPI","SVC_DT","PROC_CD",
    F.when(
        F.trim(F.coalesce(F.col("PROC_CD_TYP_CD"), F.lit(""))) == "",
        F.lit("NA")
    ).otherwise(F.col("PROC_CD_TYP_CD")).alias("PROC_CD_TYP"),
    "TST_CD","TST_CD_TYP","LAB_VAL_NUM","MEASURING_UNIT","LAB_VAL_BINARY","STTUS","RSLT_TYP","RSLT_VAL_FLAG",
    "TYP","DATA_TYP","SRC_TYP","SRC_SYS_NM","MBR_UNIQ_KEY","MBR_SK","PROV_SK"
)

# ----------------------------------------------------
# Seq_Valid_Recds => PxSequentialFile => input = df_Xfm8
df_Seq_Valid_Recds = df_Xfm8.select(
    "FEP_MBR_ID","CNTR_ID","MBR_FIRST_NM","MBR_MID_NM","MBR_LAST_NM","MBR_DOB","RPTNG_PLN_CD","CLM_ID","CLM_LN_NO",
    "ORDER_PROV_ID","ORDER_PROV_NPI","SVC_DT","PROC_CD","PROC_CD_TYP","TST_CD","TST_CD_TYP","LAB_VAL_NUM",
    "MEASURING_UNIT","LAB_VAL_BINARY","STTUS","RSLT_TYP","RSLT_VAL_FLAG","TYP","DATA_TYP","SRC_TYP","SRC_SYS_NM",
    "MBR_UNIQ_KEY","MBR_SK","PROV_SK"
)

write_files(
    df_Seq_Valid_Recds,
    f"{adls_path}/verified/BCA_LIVONGO_LAB_FORMATTED_LABFile.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

# ----------------------------------------------------
# Xfm6 => CTransformerStage => input = df_Lkp3_Out_Lkp3
df_Xfm6 = df_Lkp3_Out_Lkp3.select(
    F.col("FEP_MBR_ID"),
    F.col("CNTR_ID"),
    F.col("MBR_FIRST_NM"),
    F.col("MBR_MID_NM"),
    F.col("MBR_LAST_NM"),
    F.col("MBR_DOB"),
    F.col("RPTNG_PLN_CD"),
    F.col("CLM_ID"),
    F.col("CLM_LN_NO"),
    F.col("ORDER_PROV_ID"),
    F.col("ORDER_PROV_NPI"),
    F.col("SVC_DT"),
    F.col("PROC_CD"),
    F.col("PROC_CD_TYP"),
    F.col("TST_CD"),
    F.col("TST_CD_TYP"),
    F.col("LAB_VAL_NUM"),
    F.col("MEASURING_UNIT"),
    F.col("LAB_VAL_BINARY"),
    F.col("STTUS"),
    F.col("RSLT_TYP"),
    F.col("RSLT_VAL_FLAG"),
    F.col("TYP"),
    F.col("DATA_TYP"),
    F.col("SRC_TYP"),
    F.col("SRC_SYS_NM"),
    F.lit("ORDER_NTNL_PROV_ID Not Found").alias("ERROR_REASON")
)

# ----------------------------------------------------
# Xfm_9 => CTransformerStage => input = df_Lkp_4_out_Lkp4
df_Xfm_9 = df_Lkp_4_out_Lkp4.select(
    F.col("FEP_MBR_ID"),
    F.col("CNTR_ID"),
    F.col("MBR_FIRST_NM"),
    F.col("MBR_MID_NM"),
    F.col("MBR_LAST_NM"),
    F.col("MBR_DOB"),
    F.col("RPTNG_PLN_CD"),
    F.col("CLM_ID"),
    F.col("CLM_LN_NO"),
    F.col("ORDER_PROV_ID"),
    F.col("ORDER_PROV_NPI"),
    F.col("SVC_DT"),
    F.col("PROC_CD"),
    F.col("PROC_CD_TYP"),
    F.col("TST_CD"),
    F.col("TST_CD_TYP"),
    F.col("LAB_VAL_NUM"),
    F.col("MEASURING_UNIT"),
    F.col("LAB_VAL_BINARY"),
    F.col("STTUS"),
    F.col("RSLT_TYP"),
    F.col("RSLT_VAL_FLAG"),
    F.col("TYP"),
    F.col("DATA_TYP"),
    F.col("SRC_TYP"),
    F.col("SRC_SYS_NM"),
    F.lit("LOINC_CD_SK Not Found in BLUEKC").alias("ERROR_REASON")
)

# ----------------------------------------------------
# Fn1 => PxFunnel => input = [df_Xfm_9, df_Xfm6, df_Xmr_Mbr_Rej]
df_Fn1_funnel = (
    df_Xfm_9
    .select(
        "FEP_MBR_ID","CNTR_ID","MBR_FIRST_NM","MBR_MID_NM","MBR_LAST_NM","MBR_DOB","RPTNG_PLN_CD","CLM_ID","CLM_LN_NO",
        "ORDER_PROV_ID","ORDER_PROV_NPI","SVC_DT","PROC_CD","PROC_CD_TYP","TST_CD","TST_CD_TYP","LAB_VAL_NUM",
        "MEASURING_UNIT","LAB_VAL_BINARY","STTUS","RSLT_TYP","RSLT_VAL_FLAG","TYP","DATA_TYP","SRC_TYP","SRC_SYS_NM",
        "ERROR_REASON"
    )
    .unionByName(
        df_Xfm6.select(
            "FEP_MBR_ID","CNTR_ID","MBR_FIRST_NM","MBR_MID_NM","MBR_LAST_NM","MBR_DOB","RPTNG_PLN_CD","CLM_ID","CLM_LN_NO",
            "ORDER_PROV_ID","ORDER_PROV_NPI","SVC_DT","PROC_CD","PROC_CD_TYP","TST_CD","TST_CD_TYP","LAB_VAL_NUM",
            "MEASURING_UNIT","LAB_VAL_BINARY","STTUS","RSLT_TYP","RSLT_VAL_FLAG","TYP","DATA_TYP","SRC_TYP","SRC_SYS_NM",
            "ERROR_REASON"
        )
    )
    .unionByName(
        df_Xmr_Mbr_Rej.select(
            "FEP_MBR_ID","CNTR_ID","MBR_FIRST_NM","MBR_MID_NM","MBR_LAST_NM","MBR_DOB","RPTNG_PLN_CD","CLM_ID","CLM_LN_NO",
            "ORDER_PROV_ID","ORDER_PROV_NPI","SVC_DT","PROC_CD","PROC_CD_TYP","TST_CD","TST_CD_TYP","LAB_VAL_NUM",
            "MEASURING_UNIT","LAB_VAL_BINARY","STTUS","RSLT_TYP","RSLT_VAL_FLAG","TYP","DATA_TYP","SRC_TYP","SRC_SYS_NM",
            "ERROR_REASON"
        )
    )
)

# ----------------------------------------------------
# Rm_Dups_3 => PxRemDup => input = df_Fn1_funnel => big list of keys
df_Rm_Dups_3_dedup = dedup_sort(
    df_Fn1_funnel,
    [
        "CLM_ID","CLM_LN_NO","CNTR_ID","DATA_TYP","ERROR_REASON","FEP_MBR_ID","LAB_VAL_BINARY","LAB_VAL_NUM","MBR_DOB",
        "MBR_FIRST_NM","MBR_LAST_NM","MBR_MID_NM","MEASURING_UNIT","ORDER_PROV_ID","ORDER_PROV_NPI","PROC_CD",
        "PROC_CD_TYP","RPTNG_PLN_CD","RSLT_TYP","RSLT_VAL_FLAG","SRC_SYS_NM","SRC_TYP","STTUS","SVC_DT","TST_CD",
        "TST_CD_TYP","TYP"
    ],
    []
)
df_Rm_Dups_3_to_Fn = df_Rm_Dups_3_dedup.select(
    "FEP_MBR_ID","CNTR_ID","MBR_FIRST_NM","MBR_MID_NM","MBR_LAST_NM","MBR_DOB","RPTNG_PLN_CD","CLM_ID","CLM_LN_NO",
    "ORDER_PROV_ID","ORDER_PROV_NPI","SVC_DT","PROC_CD","PROC_CD_TYP","TST_CD","TST_CD_TYP","LAB_VAL_NUM",
    "MEASURING_UNIT","LAB_VAL_BINARY","STTUS","RSLT_TYP","RSLT_VAL_FLAG","TYP","DATA_TYP","SRC_TYP","SRC_SYS_NM",
    "ERROR_REASON"
)

# ----------------------------------------------------
# Fn => PxFunnel => input = [df_Xfm_5_Out_Xfm5, df_Rm_Dups_3_to_Fn]
df_Fn_all_error = (
    df_Xfm_5_Out_Xfm5.select(
        "FEP_MBR_ID","CNTR_ID","MBR_FIRST_NM","MBR_MID_NM","MBR_LAST_NM","MBR_DOB","RPTNG_PLN_CD","CLM_ID","CLM_LN_NO",
        "ORDER_PROV_ID","ORDER_PROV_NPI","SVC_DT","PROC_CD","PROC_CD_TYP","TST_CD","TST_CD_TYP","LAB_VAL_NUM",
        "MEASURING_UNIT","LAB_VAL_BINARY","STTUS","RSLT_TYP","RSLT_VAL_FLAG","TYP","DATA_TYP","SRC_TYP","SRC_SYS_NM",
        "ERROR_REASON"
    )
    .unionByName(
        df_Rm_Dups_3_to_Fn.select(
            "FEP_MBR_ID","CNTR_ID","MBR_FIRST_NM","MBR_MID_NM","MBR_LAST_NM","MBR_DOB","RPTNG_PLN_CD","CLM_ID","CLM_LN_NO",
            "ORDER_PROV_ID","ORDER_PROV_NPI","SVC_DT","PROC_CD","PROC_CD_TYP","TST_CD","TST_CD_TYP","LAB_VAL_NUM",
            "MEASURING_UNIT","LAB_VAL_BINARY","STTUS","RSLT_TYP","RSLT_VAL_FLAG","TYP","DATA_TYP","SRC_TYP","SRC_SYS_NM",
            "ERROR_REASON"
        )
    )
)

# ----------------------------------------------------
# Seq_Error_File => PxSequentialFile => input = df_Fn_all_error
df_Seq_Error_File = df_Fn_all_error.select(
    "FEP_MBR_ID","CNTR_ID","MBR_FIRST_NM","MBR_MID_NM","MBR_LAST_NM","MBR_DOB","RPTNG_PLN_CD","CLM_ID","CLM_LN_NO",
    "ORDER_PROV_ID","ORDER_PROV_NPI","SVC_DT","PROC_CD","PROC_CD_TYP","TST_CD","TST_CD_TYP","LAB_VAL_NUM",
    "MEASURING_UNIT","LAB_VAL_BINARY","STTUS","RSLT_TYP","RSLT_VAL_FLAG","TYP","DATA_TYP","SRC_TYP","SRC_SYS_NM",
    "ERROR_REASON"
)

write_files(
    df_Seq_Error_File,
    f"{adls_path}/verified/{ErrorFile}",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)