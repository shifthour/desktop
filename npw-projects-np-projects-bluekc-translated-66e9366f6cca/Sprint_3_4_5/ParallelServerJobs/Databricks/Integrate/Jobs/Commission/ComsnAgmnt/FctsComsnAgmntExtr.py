# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005, 2022 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     FctsComsnAgmntExtr
# MAGIC CALLED BY:   FctsCommissionDailyExtrSeq
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Pulls data from CMC_COAG_AGREEMENT for loading into IDS.
# MAGIC 
# MAGIC INPUTS:     Facets -  CMC_COAG_AGREEMENT 
# MAGIC 
# MAGIC HASH FILES: none
# MAGIC 
# MAGIC TRANSFORMS:   STRIP.FIELD
# MAGIC                              FORMAT.DATE
# MAGIC                              Trim
# MAGIC                              Left
# MAGIC                              NullOptCode
# MAGIC                              GetFkeyAgntIndv
# MAGIC                              GetFkeyTaxDmgrphc
# MAGIC                            
# MAGIC PROCESSING:  Extract, transform, and primary keying for Comission Agreement subject area.
# MAGIC 
# MAGIC OUTPUTS:   Sequential file
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               
# MAGIC 
# MAGIC Developer                  Date               Project/Altiris #         Change Description                                                                     Development Project    Code Reviewer            Date Reviewed
# MAGIC ------------------------------    -------------------    -----------------------------    ------------------------------------------------------------------------------------------------    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Hugh Sisson              11/2/2005                                       Original Program
# MAGIC Parikshith Chada       5/29/2007      3264                         Added Balancing process to the overall                                       devlIDS30                    Steph Goddard            09/17/2007
# MAGIC                                                                                           job that takes a snapshot of the source data                        
# MAGIC Bhoomi Dasari           09/09/2008    3567                         Added new primay key contianer and SrcSysCdsk                      devlIDS                        Steph Goddard            09/22/2008        
# MAGIC                                                                                           and SrcSysCd  
# MAGIC Manasa Andru           2015-03-05     TFS - 10619             Updated the business rule to drop the record for spaces/blank   IntegrateNewDevl        Kalyan Neelam            2015-03-11
# MAGIC                                                                                           for COAR_ID field in the BusinessRules transformer. 
# MAGIC Prabhu ES                 2022-03-01     S2S Remediation      MSSQL connection parameters added                                        IntegrateDev5	   Ken Bradmon	      2022-06-12
# MAGIC                                                                                           Added 2 Dedup stages to remove rows with 
# MAGIC                                                                                           duplicate COAG_ID value

# MAGIC Extract Facets Commission Agreement Data
# MAGIC Apply business logic
# MAGIC Strip Carriage Return, Line Feed, and Tab.
# MAGIC Trim all string variables except for COAR_ID.  (TrimB is used on COAR_ID as some values have a leading space character)
# MAGIC Hash file hf_comsn_agmnt_allcol cleared
# MAGIC Balancing snapshot of source table
# MAGIC Writing Sequential File to /pkey
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, length, upper, substring, when, concat, lit, regexp_replace, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------

# MAGIC %run ../../../../shared_containers/PrimaryKey/ComsnAgmntPK
# COMMAND ----------

FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
TmpOutFile = get_widget_value('TmpOutFile','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url_CMC_COAG_AGREEMENT, jdbc_props_CMC_COAG_AGREEMENT = get_db_config(facets_secret_name)
df_CMC_COAG_AGREEMENT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_CMC_COAG_AGREEMENT)
    .options(**jdbc_props_CMC_COAG_AGREEMENT)
    .option("query", f"SELECT * FROM {FacetsOwner}.CMC_COAG_AGREEMENT")
    .load()
)

df_Extract = df_CMC_COAG_AGREEMENT

df_Strip = (
    df_Extract
    .filter(length(trim(regexp_replace(col("COAR_ID"), "[\r\n\t]", ""))) > 0)
    .select(
        regexp_replace(col("COAR_ID"), "[\r\n\t]", "").alias("COAR_ID_TEMP"),
        col("COAG_EFF_DT").alias("COAG_EFF_DT_temp"),
        regexp_replace(col("COCE_ID"), "[\r\n\t]", "").alias("COCE_ID_TEMP"),
        regexp_replace(col("COSC_ID"), "[\r\n\t]", "").alias("COSC_ID_TEMP"),
        regexp_replace(col("COAG_PT_OF_SCL_IND"), "[\r\n\t]", "").alias("COAG_PT_OF_SCL_IND_temp"),
        regexp_replace(col("COAG_MCTR_TRSN"), "[\r\n\t]", "").alias("COAG_MCTR_TRSN_temp"),
        col("COAG_TERM_DT").alias("COAG_TERM_DT_temp"),
        col("COAG_SCHD_FCTR").alias("COAG_SCHD_FCTR_temp")
    )
    .select(
        trim(col("COAR_ID_TEMP")).alias("COAR_ID"),
        col("COAG_EFF_DT_temp").alias("COAG_EFF_DT"),
        trim(col("COCE_ID_TEMP")).alias("COCE_ID"),
        trim(col("COSC_ID_TEMP")).alias("COSC_ID"),
        trim(col("COAG_PT_OF_SCL_IND_temp")).alias("COAG_PT_OF_SCL_IND"),
        trim(col("COAG_MCTR_TRSN_temp")).alias("COAG_MCTR_TRSN"),
        col("COAG_TERM_DT_temp").alias("COAG_TERM_DT"),
        col("COAG_SCHD_FCTR_temp").alias("COAG_SCHD_FCTR")
    )
)

df_Dedup_main = df_Strip.dropDuplicates(["COAR_ID", "COAG_EFF_DT", "COCE_ID"]).select(
    "COAR_ID",
    "COAG_EFF_DT",
    "COCE_ID",
    "COSC_ID",
    "COAG_PT_OF_SCL_IND",
    "COAG_MCTR_TRSN",
    "COAG_TERM_DT",
    "COAG_SCHD_FCTR"
)

df_BusinessRules_base = (
    df_Dedup_main
    .withColumn("SRC_SYS_CD_SK", lit(SrcSysCdSk))
    .withColumn("svComsnArgmtId", col("COAR_ID"))
    .withColumn("svEffDt", substring(col("COAG_EFF_DT"), 1, 10))
    .withColumn("svAgntId", col("COCE_ID"))
    .withColumn("svSrcSysCd", lit("FACETS"))
    .withColumn("svSchdFctrDivider", lit(10000))
    .withColumn(
        "COMSN_AGMNT_TERM_RSN_CD_temp",
        when(
            col("COAG_MCTR_TRSN").isNull() | (length(trim(col("COAG_MCTR_TRSN"))) == 0),
            lit("NA")
        ).otherwise(upper(trim(col("COAG_MCTR_TRSN"))))
    )
    .withColumn("TERM_DT_temp", substring(col("COAG_TERM_DT"), 1, 10))
    .withColumn("SCHD_FCTR_temp", col("COAG_SCHD_FCTR") / col("svSchdFctrDivider"))
)

df_AllCol = df_BusinessRules_base.select(
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("svComsnArgmtId").alias("COMSN_ARGMT_ID"),
    col("svEffDt").alias("EFF_DT"),
    col("svAgntId").alias("AGNT_ID"),
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit("I").alias("INSRT_UPDT_CD"),
    lit("N").alias("DISCARD_IN"),
    lit("Y").alias("PASS_THRU_IN"),
    lit(CurrDate).alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    col("svSrcSysCd").alias("SRC_SYS_CD"),
    concat(
        col("svSrcSysCd"), lit(";"),
        col("svComsnArgmtId"), lit(";"),
        col("svEffDt"), lit(";"),
        col("svAgntId")
    ).alias("PRI_KEY_STRING"),
    lit(0).alias("COMSN_AGMNT"),
    lit(0).alias("CRT_RUN_CYC_EXTCN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
    col("COCE_ID").alias("AGNT"),
    col("COAR_ID").alias("COMSN_ARGMT"),
    col("COSC_ID").alias("COMSN_SCHD"),
    col("COAG_PT_OF_SCL_IND").alias("COMSN_AGMNT_PT_OF_SCHD_CD"),
    col("COMSN_AGMNT_TERM_RSN_CD_temp").alias("COMSN_AGMNT_TERM_RSN_CD"),
    col("TERM_DT_temp").alias("TERM_DT"),
    col("SCHD_FCTR_temp").alias("SCHD_FCTR")
)

df_Transform = df_BusinessRules_base.select(
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("svComsnArgmtId").alias("COMSN_ARGMT_ID"),
    col("svEffDt").alias("EFF_DT_SK"),
    col("svAgntId").alias("AGNT_ID")
)

params_ComsnAgmntPK = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "RunID": RunID,
    "CurrentDate": CurrDate,
    "IDSOwner": IDSOwner
}

df_Key = ComsnAgmntPK(df_AllCol, df_Transform, params_ComsnAgmntPK)

df_IdsComsnAgmntExtr = df_Key.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("COMSN_AGMNT_SK"),
    rpad(col("COMSN_ARGMT_ID"), 12, " ").alias("COMSN_ARGMT_ID"),
    rpad(col("EFF_DT"), 10, " ").alias("EFF_DT"),
    rpad(col("AGNT_ID"), 10, " ").alias("AGNT_ID"),
    col("CRT_RUN_CYC_EXTCN_SK"),
    col("LAST_UPDT_RUN_CYC_EXTCN_SK"),
    rpad(col("AGNT"), 12, " ").alias("AGNT"),
    rpad(col("COMSN_ARGMT"), 12, " ").alias("COMSN_ARGMT"),
    rpad(col("COMSN_SCHD"), 4, " ").alias("COMSN_SCHD"),
    rpad(col("COMSN_AGMNT_PT_OF_SCHD_CD"), 1, " ").alias("COMSN_AGMNT_PT_OF_SCHD_CD"),
    rpad(col("COMSN_AGMNT_TERM_RSN_CD"), 4, " ").alias("COMSN_AGMNT_TERM_RSN_CD"),
    rpad(col("TERM_DT"), 10, " ").alias("TERM_DT"),
    col("SCHD_FCTR")
)

write_files(
    df_IdsComsnAgmntExtr,
    f"{adls_path}/key/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

jdbc_url_Facets_Source, jdbc_props_Facets_Source = get_db_config(facets_secret_name)
df_Facets_Source = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_Facets_Source)
    .options(**jdbc_props_Facets_Source)
    .option("query", f"SELECT agmt.COAR_ID, agmt.COCE_ID, agmt.COAG_EFF_DT FROM {FacetsOwner}.CMC_COAG_AGREEMENT agmt")
    .load()
)

df_Snapshot = df_Facets_Source

df_T_transform = (
    df_Snapshot
    .withColumn("svEffDt", substring(col("COAG_EFF_DT"), 1, 10))
    .withColumn("svEffDtSk", GetFkeyDate("IDS", lit(110), col("svEffDt"), lit("X")))
)

df_dedup_temp = (
    df_T_transform
    .filter(length(trim(regexp_replace(col("COAR_ID"), "[\r\n\t]", ""))) > 0)
    .select(
        lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        trim(regexp_replace(col("COAR_ID"), "[\r\n\t]", "")).alias("COMSN_ARGMT_ID"),
        col("svEffDtSk").alias("EFF_DT_SK"),
        trim(regexp_replace(col("COCE_ID"), "[\r\n\t]", "")).alias("AGNT_ID")
    )
)

df_Dedup_balancing = df_dedup_temp.dropDuplicates(["SRC_SYS_CD_SK", "COMSN_ARGMT_ID", "EFF_DT_SK", "AGNT_ID"]).select(
    "SRC_SYS_CD_SK",
    "COMSN_ARGMT_ID",
    "EFF_DT_SK",
    "AGNT_ID"
)

df_Snapshot_File = df_Dedup_balancing.select(
    "SRC_SYS_CD_SK",
    "COMSN_ARGMT_ID",
    "EFF_DT_SK",
    "AGNT_ID"
)

df_Snapshot_File_out = df_Snapshot_File.select(
    col("SRC_SYS_CD_SK"),
    rpad(col("COMSN_ARGMT_ID"), <...>, " ").alias("COMSN_ARGMT_ID"),
    rpad(col("EFF_DT_SK"), 10, " ").alias("EFF_DT_SK"),
    rpad(col("AGNT_ID"), <...>, " ").alias("AGNT_ID")
)

write_files(
    df_Snapshot_File_out,
    f"{adls_path}/load/B_COMSN_AGMNT.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)