# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_3 02/20/09 11:05:46 Batch  15027_39950 PROMOTE bckcetl ids20 dsadm bls for sa
# MAGIC ^1_3 02/20/09 10:48:45 Batch  15027_38927 INIT bckcett testIDS dsadm bls for sa
# MAGIC ^1_2 02/19/09 15:48:36 Batch  15026_56922 PROMOTE bckcett testIDS u03651 steph for Sharon
# MAGIC ^1_2 02/19/09 15:43:11 Batch  15026_56617 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_1 01/13/09 08:41:23 Batch  14989_31287 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_1 01/30/08 05:00:10 Batch  14640_18019 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 10/31/07 14:11:20 Batch  14549_51083 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_2 10/31/07 12:55:15 Batch  14549_46521 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_2 10/31/07 12:32:28 Batch  14549_45152 INIT bckcett testIDS30 dsadm bls for on
# MAGIC ^1_1 10/26/07 08:07:18 Batch  14544_29241 PROMOTE bckcett testIDS30 u03651 steffy
# MAGIC ^1_1 10/26/07 07:46:17 Batch  14544_27982 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_7 05/12/06 15:03:19 Batch  14012_54207 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_6 05/11/06 16:20:33 Batch  14011_58841 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_5 05/11/06 12:36:55 Batch  14011_45419 PROMOTE bckcett devlIDS30 u10157 sa
# MAGIC ^1_5 05/11/06 12:31:29 Batch  14011_45092 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_2 05/02/06 13:04:24 Batch  14002_47066 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_2 01/26/06 07:54:36 Batch  13906_28483 INIT bckcetl ids20 dsadm Gina
# MAGIC ^1_1 01/24/06 14:49:28 Batch  13904_53375 PROMOTE bckcetl ids20 dsadm Gina Parr
# MAGIC ^1_1 01/24/06 14:47:17 Batch  13904_53242 INIT bckcett testIDS30 dsadm Gina Parr
# MAGIC ^1_2 01/24/06 14:36:07 Batch  13904_52574 INIT bckcett testIDS30 dsadm Gina Parr
# MAGIC ^1_1 01/24/06 14:21:30 Batch  13904_51695 INIT bckcett testIDS30 dsadm Gina Parr
# MAGIC ^1_1 01/23/06 12:01:30 Batch  13903_43296 PROMOTE bckcett testIDS30 u11141 Hugh Sisson
# MAGIC ^1_1 01/23/06 12:00:34 Batch  13903_43240 INIT bckcett devlIDS30 u11141 Hugh Sisson
# MAGIC ^1_3 12/23/05 11:12:01 Batch  13872_40337 INIT bckcett devlIDS30 u10913 Ollie Move Income/Commisions to test
# MAGIC ^1_2 12/23/05 10:39:46 Batch  13872_38400 INIT bckcett devlIDS30 u10913 Ollie Moving Income / Commission to Test
# MAGIC ^1_1 12/14/05 12:18:44 Batch  13863_44334 INIT bckcett devlIDS30 u11141 Hugh Sisson
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     FctsAgntRelshpExtr
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Pulls data from CMC_COME_COMM_ENTY and CMC_COER_RELATION for loading into IDS.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:     Facets -  CMC_COME_COMM_ENTY 
# MAGIC                    Facets -  CMC_COER_RELATION 
# MAGIC 
# MAGIC HASH FILES: hf_agnt_relshp
# MAGIC 
# MAGIC TRANSFORMS:   STRIP.FIELD
# MAGIC                              FORMAT.DATE
# MAGIC                              Trim
# MAGIC                              Left
# MAGIC                              Index
# MAGIC                            
# MAGIC PROCESSING:  Extract, transform, and primary keying for Agent subject area.
# MAGIC   
# MAGIC OUTPUTS:   Sequential file
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               Hugh Sisson    10/11/2005  -  Originally Programmed
# MAGIC               Hugh Sisson    01/23/2006  -  Adjusted AgntRelshpTermRsnCode formula
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              6/7/2007          3264                              Added Balancing process to the overall                 devlIDS30                 Steph Goddard            09/17/2007
# MAGIC                                                                                                       job that takes a snapshot of the source data     
# MAGIC                    
# MAGIC Bhoomi Dasari                 09/05/2008       3567                             Added new primay key contianer and SrcSysCdsk  devlIDS                    Steph Goddard            09/22/2008                
# MAGIC                                                                                                       and SrcSysCd             
# MAGIC Prabhu ES                      2022-03-01         S2S Remediation          MSSQL connection parameters added                   IntegrateDev5

# MAGIC Strip Carriage Return, Line Feed, and Tab.
# MAGIC Trim leading and trailing blanks
# MAGIC Extract Facets Agent Relationship data from the CMC_COCE_COMM_ENTY and CMC_COER_RELATION tables
# MAGIC Hash file hf_agnt_relshp_allcol cleared
# MAGIC Apply business logic
# MAGIC Eliminates rows that do not meet the filter and join criteria, and some rows where the COCE_ID contains a comma (,)
# MAGIC Balancing snapshot of source table
# MAGIC Writing Sequential File to /key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Parameter Initialization
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
TmpOutFile = get_widget_value('TmpOutFile','FctsAgntRelshpExtr.tmp')
CurrRunCycle = get_widget_value('CurrRunCycle','150')
CurrDate = get_widget_value('CurrDate','2008-09-05')
RunID = get_widget_value('RunID','20080905')
SrcSysCdSk = get_widget_value('SrcSysCdSk','105838')
SrcSysCd = get_widget_value('SrcSysCd','FACETS')

# MAGIC %run ../../../../../shared_containers/PrimaryKey/AgntRelshpPK
# COMMAND ----------

jdbc_url, jdbc_props = get_db_config(facets_secret_name)
extract_query_facets = """SELECT
 enty.COCE_ID C_COCE_ID,
 enty.COTD_ENTY_TYPE C_COTD_ENTY_TYPE,
 enty.COCE_TERM_DT C_COCE_TERM_DT,
 rel.COTD_ENTY_TYPE R_COTD_ENTY_TYPE,
 rel.COER_EFF_DT R_COER_EFF_DT,
 rel.COER_TERM_DT R_COER_TERM_DT,
 rel.COER_MCTR_TRSN R_COER_MCTR_TRSN,
 rel.COCE_ID_RELATION R_COCE_ID_RELATION
 FROM {0}.CMC_COCE_COMM_ENTY enty
 LEFT JOIN {0}.CMC_COER_RELATION rel
 ON enty.COCE_ID = rel.COCE_ID
""".format(FacetsOwner)

df_FACETS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_facets)
    .load()
)

df_StripField = (
    df_FACETS.filter(
        (F.instr(F.col("C_COCE_ID"), ",") == 0)
        & ((F.col("C_COTD_ENTY_TYPE") == "AGNT") | (F.col("C_COTD_ENTY_TYPE") == "SUBA"))
        & (
            ((F.col("C_COTD_ENTY_TYPE") == "SUBA") & (F.col("R_COTD_ENTY_TYPE") == "AGCY"))
            | (F.col("C_COTD_ENTY_TYPE") == "AGNT")
        )
    )
    .select(
        strip_field(trim(F.col("C_COCE_ID"))).alias("COCE_ID"),
        strip_field(trim(F.col("C_COTD_ENTY_TYPE"))).alias("COTD_ENTY_TYPE"),
        strip_field(trim(F.col("R_COCE_ID_RELATION"))).alias("COCE_ID_RELATION"),
        F.substring(strip_field(F.col("R_COER_EFF_DT")), 1, 10).alias("COER_EFF_DT"),
        strip_field(trim(F.col("R_COER_MCTR_TRSN"))).alias("COER_MCTR_TRSN"),
        F.substring(strip_field(F.col("C_COCE_TERM_DT")), 1, 10).alias("COCE_TERM_DT"),
        F.col("C_COCE_TERM_DT").alias("FULL_COCE_TERM_DT"),
        F.substring(strip_field(F.col("R_COER_TERM_DT")), 1, 10).alias("COER_TERM_DT")
    )
)

df_BusinessRules = (
    df_StripField
    .withColumn("RowPassThru", F.lit("Y"))
    .withColumn("svRunDate", F.to_timestamp(F.lit(CurrDate), "yyyy-MM-dd"))
    .withColumn(
        "svRelAgntId",
        F.when(F.col("COTD_ENTY_TYPE") == "AGNT", F.col("COCE_ID")).otherwise(F.col("COCE_ID_RELATION"))
    )
    .withColumn(
        "svEffDt",
        F.when(F.col("COTD_ENTY_TYPE") == "AGNT", F.lit("1753-01-01")).otherwise(F.col("COER_EFF_DT"))
    )
    .withColumn(
        "svTermRsnCd",
        F.when(
            F.col("COTD_ENTY_TYPE") == "AGNT",
            F.when(F.col("FULL_COCE_TERM_DT") > F.col("svRunDate"), F.lit("NA")).otherwise(F.lit("TERM"))
        ).otherwise(
            F.when(
                (F.col("COER_MCTR_TRSN").isNull())
                | (F.length(trim(F.col("COER_MCTR_TRSN"))) == 0),
                F.lit("NA")
            ).otherwise(F.upper(trim(F.col("COER_MCTR_TRSN"))))
        )
    )
    .withColumn(
        "svTermDt",
        F.when(F.col("COTD_ENTY_TYPE") == "AGNT", F.col("COCE_TERM_DT")).otherwise(F.col("COER_TERM_DT"))
    )
)

df_AllCol = df_BusinessRules.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("COCE_ID").alias("AGNT_ID"),
    F.col("svRelAgntId").alias("REL_AGNT_ID"),
    F.col("svEffDt").alias("EFF_DT"),
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.col("RowPassThru").alias("PASS_THRU_IN"),
    F.lit(CurrDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(
        F.lit(SrcSysCd), F.lit(";"),
        F.col("COCE_ID"), F.lit(";"),
        F.col("svRelAgntId"), F.lit(";"),
        F.col("svEffDt")
    ).alias("PRI_KEY_STRING"),
    F.lit(0).alias("AGNT_RELSHP_SK"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("COCE_ID").alias("AGNT"),
    F.col("svRelAgntId").alias("REL_AGNT"),
    F.col("svTermRsnCd").alias("AGNT_RELSHP_TERM_RSN_CD"),
    F.col("svTermDt").alias("TERM_DT")
)

df_Transform = df_BusinessRules.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("COCE_ID").alias("AGNT_ID"),
    F.col("svRelAgntId").alias("REL_AGNT_ID"),
    F.col("svEffDt").alias("EFF_DT_SK")
)

params_container = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "RunID": RunID,
    "CurrentDate": CurrDate,
    "IDSOwner": IDSOwner
}

df_Key = AgntRelshpPK(df_AllCol, df_Transform, params_container)

df_IdsAgntRelshpExtr = df_Key.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "AGNT_RELSHP_SK",
    "AGNT_ID",
    "REL_AGNT_ID",
    "EFF_DT",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "AGNT",
    "REL_AGNT",
    "AGNT_RELSHP_TERM_RSN_CD",
    "TERM_DT"
)

df_IdsAgntRelshpExtr = df_IdsAgntRelshpExtr.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("AGNT_RELSHP_SK"),
    F.rpad(F.col("AGNT_ID"), 12, " ").alias("AGNT_ID"),
    F.rpad(F.col("REL_AGNT_ID"), 12, " ").alias("REL_AGNT_ID"),
    F.rpad(F.col("EFF_DT"), 10, " ").alias("EFF_DT"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.col("AGNT"), 12, " ").alias("AGNT"),
    F.rpad(F.col("REL_AGNT"), 12, " ").alias("REL_AGNT"),
    F.rpad(F.col("AGNT_RELSHP_TERM_RSN_CD"), 4, " ").alias("AGNT_RELSHP_TERM_RSN_CD"),
    F.rpad(F.col("TERM_DT"), 10, " ").alias("TERM_DT")
)

write_files(
    df_IdsAgntRelshpExtr,
    f"{adls_path}/key/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

extract_query_facets_source = """SELECT
 enty.COCE_ID C_COCE_ID,
 enty.COTD_ENTY_TYPE C_COTD_ENTY_TYPE,
 rel.COTD_ENTY_TYPE R_COTD_ENTY_TYPE,
 rel.COER_EFF_DT R_COER_EFF_DT,
 rel.COCE_ID_RELATION R_COCE_ID_RELATION
 FROM {0}.CMC_COCE_COMM_ENTY enty
 LEFT JOIN {0}.CMC_COER_RELATION rel
 ON enty.COCE_ID = rel.COCE_ID
""".format(FacetsOwner)

df_Facets_Source = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_facets_source)
    .load()
)

df_Transform2 = (
    df_Facets_Source
    .withColumn(
        "svHasNoComma",
        F.when(F.instr(F.col("C_COCE_ID"), ",") == 0, True).otherwise(False)
    )
    .withColumn(
        "svPassedFilterCriteria",
        F.when(
            (trim(strip_field(F.col("C_COTD_ENTY_TYPE"))) == "AGNT")
            | (trim(strip_field(F.col("C_COTD_ENTY_TYPE"))) == "SUBA"), True
        ).otherwise(False)
    )
    .withColumn(
        "svPassedJoinCriteria",
        F.when(
            (
                (trim(strip_field(F.col("C_COTD_ENTY_TYPE"))) == "SUBA")
                & (trim(strip_field(F.col("R_COTD_ENTY_TYPE"))) == "AGCY")
            )
            | (trim(strip_field(F.col("C_COTD_ENTY_TYPE"))) == "AGNT"),
            True
        ).otherwise(False)
    )
    .withColumn(
        "svRelAgntId",
        F.when(
            trim(strip_field(F.col("C_COTD_ENTY_TYPE"))) == "AGNT",
            trim(strip_field(F.col("C_COCE_ID")))
        ).otherwise(trim(strip_field(F.col("R_COCE_ID_RELATION"))))
    )
    .withColumn(
        "svEffDt",
        F.when(
            trim(strip_field(F.col("C_COTD_ENTY_TYPE"))) == "AGNT",
            F.lit("1753-01-01")
        ).otherwise(F.date_format(F.col("R_COER_EFF_DT"), "yyyy-MM-dd"))
    )
    .withColumn(
        "svEffDtSk",
        GetFkeyDate("IDS", F.lit(100), F.col("svEffDt"), F.lit("X"))
    )
    .filter(
        (F.col("svHasNoComma") == True)
        & (F.col("svPassedFilterCriteria") == True)
        & (F.col("svPassedJoinCriteria") == True)
    )
    .select(
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        trim(strip_field(F.col("C_COCE_ID"))).alias("AGNT_ID"),
        F.col("svRelAgntId").alias("REL_AGNT_ID"),
        F.col("svEffDtSk").alias("EFF_DT_SK")
    )
)

df_Snapshot_File = df_Transform2.select(
    F.col("SRC_SYS_CD_SK"),
    F.rpad(F.col("AGNT_ID"), 12, " ").alias("AGNT_ID"),
    F.rpad(F.col("REL_AGNT_ID"), 12, " ").alias("REL_AGNT_ID"),
    F.rpad(F.col("EFF_DT_SK"), 10, " ").alias("EFF_DT_SK")
)

write_files(
    df_Snapshot_File,
    f"{adls_path}/load/B_AGNT_RELSHP.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)