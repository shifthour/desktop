# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY: IhmfConstituentVbbExtrSeq
# MAGIC  
# MAGIC PROCESSING:   Extracts data from Ihmf Constituent database and creates a load file for P_GRP_VBB_ELIG table and a landing file for GRP_VBB_PLN_ELIG with all the available Plans and Classes
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Kalyan Neelam        2013-05611     4963 VBB Phase III    Initial Programming                                                                       IntegrateNewDevl       Bhoomi Dasari          7/3/2013

# MAGIC IDS GRP_VBB_PLN_ELIG Extract
# MAGIC Processing table used in IdsGrpVbbPlnEligExtr to identigy the Classes and Plans for including or excluding
# MAGIC VBB_PLN primary key hash file
# MAGIC should NOT cleared.
# MAGIC Landing file used in IdsGrpVbbPlnEligExtr, contains the current processing records
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F_
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Parameters
VBBOwner = get_widget_value('VBBOwner','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
RunId = get_widget_value('RunId','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
LastRunDtm = get_widget_value('LastRunDtm','')
CurrDate = get_widget_value('CurrDate','')
Environment = get_widget_value('Environment','')
vbb_secret_name = get_widget_value('vbb_secret_name','')

# Get DB config (for the CODBCStage)
jdbc_url, jdbc_props = get_db_config(vbb_secret_name)

# Read from IHMFCONSTITUENT (CODBCStage)
extract_query = f"""
SELECT
TZIM_HIEL_ELIG_LIST.HIPL_ID,
TZIM_HIEL_ELIG_LIST.HIEL_ID_SEQ,
TZIM_HIEL_ELIG_LIST.HIEL_START_DT,
TZIM_HIEL_ELIG_LIST.HIEL_END_DT,
TZIM_HIEL_ELIG_LIST.HIEL_INC_EXC_IND,
TZIM_HIEL_ELIG_LIST.HIEL_SEQ_TYPE,
TZIM_HIEL_ELIG_LIST.HIEL_LOG_LEVEL1,
TZIM_HIEL_ELIG_LIST.HIEL_LOG_LEVEL2,
TZIM_HIEL_ELIG_LIST.HIEL_LOG_LEVEL3,
TZIM_HIEL_ELIG_LIST.HIEL_PHY_LEVEL1,
TZIM_HIEL_ELIG_LIST.HIEL_PHY_LEVEL2,
TZIM_HIEL_ELIG_LIST.HIEL_PHY_LEVEL3,
TZIM_HIEL_ELIG_LIST.HIEL_NAME_LEVEL1,
TZIM_HIEL_ELIG_LIST.HIEL_NAME_LEVEL2,
TZIM_HIEL_ELIG_LIST.HIEL_NAME_LEVEL3,
TZIM_HIEL_ELIG_LIST.PAYR_ID,
TZIM_HIEL_ELIG_LIST.HIEL_CREATE_DT,
TZIM_HIEL_ELIG_LIST.HIEL_CREATE_APP_USER,
TZIM_HIEL_ELIG_LIST.HIEL_UPDATE_DT,
TZIM_HIEL_ELIG_LIST.HIEL_UPDATE_DB_USER,
TZIM_HIEL_ELIG_LIST.HIEL_UPDATE_APP_USER,
TZIM_HIEL_ELIG_LIST.HIEL_UPDATE_SEQ,
TZIM_HIPL_HEALTH_PLAN.CDVL_VALUE_CATEGORY,
TZIM_HIPL_HEALTH_PLAN.HIPL_YEAR,
TZIM_HIPL_HEALTH_PLAN.CDVL_VALUE_COVERAGE,
TZIM_HIPL_HEALTH_PLAN.HIPL_START_DT
FROM {VBBOwner}.TZIM_HIEL_ELIG_LIST TZIM_HIEL_ELIG_LIST,
{VBBOwner}.TZIM_HIPL_HEALTH_PLAN TZIM_HIPL_HEALTH_PLAN
WHERE
TZIM_HIEL_ELIG_LIST.HIPL_ID = TZIM_HIPL_HEALTH_PLAN.HIPL_ID
AND
TZIM_HIEL_ELIG_LIST.HIEL_UPDATE_DT > '{LastRunDtm}'
"""

df_ihmfconstituent = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)
df_ihmfconstituent = df_ihmfconstituent.select(
    F_.col("HIPL_ID"),
    F_.col("HIEL_ID_SEQ"),
    F_.col("HIEL_START_DT"),
    F_.col("HIEL_END_DT"),
    F_.col("HIEL_INC_EXC_IND"),
    F_.col("HIEL_SEQ_TYPE"),
    F_.col("HIEL_LOG_LEVEL1"),
    F_.col("HIEL_LOG_LEVEL2"),
    F_.col("HIEL_LOG_LEVEL3"),
    F_.col("HIEL_PHY_LEVEL1"),
    F_.col("HIEL_PHY_LEVEL2"),
    F_.col("HIEL_PHY_LEVEL3"),
    F_.col("HIEL_NAME_LEVEL1"),
    F_.col("HIEL_NAME_LEVEL2"),
    F_.col("HIEL_NAME_LEVEL3"),
    F_.col("PAYR_ID"),
    F_.col("HIEL_CREATE_DT"),
    F_.col("HIEL_CREATE_APP_USER"),
    F_.col("HIEL_UPDATE_DT"),
    F_.col("HIEL_UPDATE_DB_USER"),
    F_.col("HIEL_UPDATE_APP_USER"),
    F_.col("HIEL_UPDATE_SEQ"),
    F_.col("CDVL_VALUE_CATEGORY"),
    F_.col("HIPL_YEAR"),
    F_.col("CDVL_VALUE_COVERAGE"),
    F_.col("HIPL_START_DT")
)

# Read from hf_vbb_pln (CHashedFileStage) - Scenario C => read parquet
df_hf_vbb_pln = spark.read.parquet(f"{adls_path}/hf_vbb_pln.parquet")
df_hf_vbb_pln = df_hf_vbb_pln.select(
    F_.col("VBB_PLN_UNIQ_KEY"),
    F_.col("SRC_SYS_CD"),
    F_.col("CRT_RUN_CYC_EXCTN_SK"),
    F_.col("VBB_PLN_SK")
)

# Transformer "Trim" with left lookup
df_trim_pre = df_ihmfconstituent.alias("Extract").join(
    df_hf_vbb_pln.alias("vbb_pln_lkup"),
    (
        trim(strip_field(F_.col("Extract.HIPL_ID"))) == F_.col("vbb_pln_lkup.VBB_PLN_UNIQ_KEY")
    ) &
    (
        F_.lit(SrcSysCd) == F_.col("vbb_pln_lkup.SRC_SYS_CD")
    ),
    how="left"
)

df_trim = (
    df_trim_pre
    .withColumn(
        "svHiplId",
        F_.when(
            F_.col("Extract.HIPL_ID").isNull() | (trim(strip_field(F_.col("Extract.HIPL_ID"))) == ""),
            F_.lit("N")
        ).otherwise(F_.lit("Y"))
    )
    .withColumn(
        "svHielIncExcInd",
        F_.when(
            F_.col("Extract.HIEL_INC_EXC_IND").isNull() | (trim(strip_field(F_.col("Extract.HIEL_INC_EXC_IND"))) == ""),
            F_.lit("N")
        ).otherwise(F_.lit("Y"))
    )
    .withColumn(
        "svGoodRecord",
        F_.when(
            (F_.col("svHiplId") == "Y") & (F_.col("svHielIncExcInd") == "Y"),
            F_.lit("Y")
        ).otherwise(F_.lit("N"))
    )
)

# Output link "PlanData" (Constraint: vbb_pln_lkup.VBB_PLN_UNIQ_KEY not null AND svGoodRecord='Y')
df_planData = df_trim.filter(
    (F_.col("vbb_pln_lkup.VBB_PLN_UNIQ_KEY").isNotNull()) &
    (F_.col("svGoodRecord") == "Y")
).select(
    trim(strip_field(F_.col("Extract.HIPL_ID"))).alias("HIPL_ID"),
    trim(strip_field(F_.col("Extract.HIEL_ID_SEQ"))).alias("HIEL_ID_SEQ"),
    trim(strip_field(F_.col("Extract.HIEL_START_DT"))).alias("HIEL_START_DT"),
    trim(strip_field(F_.col("Extract.HIEL_END_DT"))).alias("HIEL_END_DT"),
    trim(strip_field(F_.col("Extract.HIEL_INC_EXC_IND"))).alias("HIEL_INC_EXC_IND"),
    trim(strip_field(F_.col("Extract.HIEL_SEQ_TYPE"))).alias("HIEL_SEQ_TYPE"),
    trim(strip_field(F_.col("Extract.HIEL_LOG_LEVEL1"))).alias("HIEL_LOG_LEVEL1"),
    trim(strip_field(F_.col("Extract.HIEL_LOG_LEVEL2"))).alias("HIEL_LOG_LEVEL2"),
    trim(strip_field(F_.col("Extract.HIEL_LOG_LEVEL3"))).alias("HIEL_LOG_LEVEL3"),
    trim(strip_field(F_.col("Extract.HIEL_PHY_LEVEL1"))).alias("HIEL_PHY_LEVEL1"),
    trim(strip_field(F_.col("Extract.HIEL_PHY_LEVEL2"))).alias("HIEL_PHY_LEVEL2"),
    trim(strip_field(F_.col("Extract.HIEL_PHY_LEVEL3"))).alias("HIEL_PHY_LEVEL3"),
    trim(strip_field(F_.col("Extract.HIEL_NAME_LEVEL1"))).alias("HIEL_NAME_LEVEL1"),
    trim(strip_field(F_.col("Extract.HIEL_NAME_LEVEL2"))).alias("HIEL_NAME_LEVEL2"),
    trim(strip_field(F_.col("Extract.HIEL_NAME_LEVEL3"))).alias("HIEL_NAME_LEVEL3"),
    trim(strip_field(F_.col("Extract.PAYR_ID"))).alias("PAYR_ID"),
    trim(strip_field(F_.col("Extract.HIEL_CREATE_DT"))).alias("HIEL_CREATE_DT"),
    trim(strip_field(F_.col("Extract.HIEL_CREATE_APP_USER"))).alias("HIEL_CREATE_APP_USER"),
    trim(strip_field(F_.col("Extract.HIEL_UPDATE_DT"))).alias("HIEL_UPDATE_DT"),
    trim(strip_field(F_.col("Extract.HIEL_UPDATE_DB_USER"))).alias("HIEL_UPDATE_DB_USER"),
    trim(strip_field(F_.col("Extract.HIEL_UPDATE_APP_USER"))).alias("HIEL_UPDATE_APP_USER"),
    trim(strip_field(F_.col("Extract.HIEL_UPDATE_SEQ"))).alias("HIEL_UPDATE_SEQ"),
    trim(strip_field(F_.col("Extract.CDVL_VALUE_CATEGORY"))).alias("CDVL_VALUE_CATEGORY"),
    trim(strip_field(F_.col("Extract.HIPL_YEAR"))).alias("HIPL_YEAR"),
    trim(strip_field(F_.col("Extract.CDVL_VALUE_COVERAGE"))).alias("CDVL_VALUE_COVERAGE"),
    trim(strip_field(F_.col("Extract.HIPL_START_DT"))).alias("HIPL_START_DT")
)

# Output link "Error" (Constraint: svGoodRecord = 'N')
df_error = df_trim.filter(
    F_.col("svGoodRecord") == "N"
).select(
    F_.col("Extract.HIPL_ID").alias("HIPL_ID"),
    F_.col("Extract.HIEL_ID_SEQ").alias("HIEL_ID_SEQ"),
    F_.col("Extract.HIEL_START_DT").alias("HIEL_START_DT"),
    F_.col("Extract.HIEL_END_DT").alias("HIEL_END_DT"),
    F_.col("Extract.HIEL_INC_EXC_IND").alias("HIEL_INC_EXC_IND"),
    F_.col("Extract.HIEL_SEQ_TYPE").alias("HIEL_SEQ_TYPE"),
    F_.col("Extract.HIEL_LOG_LEVEL1").alias("HIEL_LOG_LEVEL1"),
    F_.col("Extract.HIEL_LOG_LEVEL2").alias("HIEL_LOG_LEVEL2"),
    F_.col("Extract.HIEL_LOG_LEVEL3").alias("HIEL_LOG_LEVEL3"),
    F_.col("Extract.HIEL_PHY_LEVEL1").alias("HIEL_PHY_LEVEL1"),
    F_.col("Extract.HIEL_PHY_LEVEL2").alias("HIEL_PHY_LEVEL2"),
    F_.col("Extract.HIEL_PHY_LEVEL3").alias("HIEL_PHY_LEVEL3"),
    F_.col("Extract.HIEL_NAME_LEVEL1").alias("HIEL_NAME_LEVEL1"),
    F_.col("Extract.HIEL_NAME_LEVEL2").alias("HIEL_NAME_LEVEL2"),
    F_.col("Extract.HIEL_NAME_LEVEL3").alias("HIEL_NAME_LEVEL3"),
    F_.col("Extract.PAYR_ID").alias("PAYR_ID"),
    F_.col("Extract.HIEL_CREATE_DT").alias("HIEL_CREATE_DT"),
    F_.col("Extract.HIEL_CREATE_APP_USER").alias("HIEL_CREATE_APP_USER"),
    F_.col("Extract.HIEL_UPDATE_DT").alias("HIEL_UPDATE_DT"),
    F_.col("Extract.HIEL_UPDATE_DB_USER").alias("HIEL_UPDATE_DB_USER"),
    F_.col("Extract.HIEL_UPDATE_APP_USER").alias("HIEL_UPDATE_APP_USER"),
    F_.col("Extract.HIEL_UPDATE_SEQ").alias("HIEL_UPDATE_SEQ"),
    F_.col("Extract.CDVL_VALUE_CATEGORY").alias("CDVL_VALUE_CATEGORY"),
    F_.col("Extract.HIPL_YEAR").alias("HIPL_YEAR"),
    F_.col("Extract.CDVL_VALUE_COVERAGE").alias("CDVL_VALUE_COVERAGE")
)

# Write "Error" to file
write_files(
    df_error,
    f"{adls_path_publish}/external/GRP_VBB_PLN_ELIG_ErrorsOut_{RunId}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Transformer_6 -> two outputs: "Load" and "WriteToFile"

# "Load" link => columns
df_load = df_planData.select(
    F_.col("HIPL_ID").alias("VBB_PLN_UNIQ_KEY"),
    F_.col("HIEL_ID_SEQ").alias("VBB_PLN_ELIG_SEQ_NO"),
    F_.col("CDVL_VALUE_COVERAGE").alias("VBB_PLN_CAT_CD"),
    F_.when(
        F_.col("HIEL_LOG_LEVEL1").isNull() | (trim(F_.col("HIEL_LOG_LEVEL1")) == ""),
        F_.lit(None)
    ).otherwise(F_.col("HIEL_LOG_LEVEL1")).alias("GRP_ID"),
    F_.when(
        F_.col("HIEL_LOG_LEVEL2").isNull() | (trim(F_.col("HIEL_LOG_LEVEL2")) == ""),
        F_.lit(None)
    ).otherwise(F_.col("HIEL_LOG_LEVEL2")).alias("CLS_ID"),
    F_.when(
        F_.col("HIEL_LOG_LEVEL3").isNull() | (trim(F_.col("HIEL_LOG_LEVEL3")) == ""),
        F_.lit(None)
    ).otherwise(F_.col("HIEL_LOG_LEVEL3")).alias("CLS_PLN_ID"),
    F_.rpad(
        F_.when(
            F_.col("HIEL_INC_EXC_IND").isNull() | (trim(F_.col("HIEL_INC_EXC_IND")) == ""),
            F_.lit(None)
        ).otherwise(F_.col("HIEL_INC_EXC_IND")),
        1,
        " "
    ).alias("GRP_VBB_ELIG_INCLD_IN"),
    F_.when(
        F_.col("HIPL_YEAR").isNull() | (trim(F_.col("HIPL_YEAR")) == ""),
        F_.lit(None)
    ).otherwise(F_.col("HIPL_YEAR")).alias("VBB_PLN_STRT_YR_NO"),
    F_.when(
        F_.col("HIEL_START_DT").isNull() | (trim(F_.col("HIEL_START_DT")) == ""),
        F_.lit(None)
    ).otherwise(F_.col("HIEL_START_DT").substr(1, 10)).alias("VBB_PLN_ELIG_STRT_DT_SK"),
    F_.when(
        F_.col("HIEL_END_DT").isNull() | (trim(F_.col("HIEL_END_DT")) == ""),
        F_.lit(None)
    ).otherwise(F_.col("HIEL_END_DT").substr(1, 10)).alias("VBB_PLN_ELIG_END_DT_SK"),
    F_.rpad(
        F_.when(
            F_.col("HIPL_START_DT").isNull() | (trim(F_.col("HIPL_START_DT")) == ""),
            F_.lit(None)
        ).otherwise(F_.col("HIPL_START_DT").substr(1, 10)),
        10,
        " "
    ).alias("VBB_PLN_STRT_DT_SK")
)

# "WriteToFile" link => columns
df_writeToFile = df_planData.select(
    F_.col("HIPL_ID"),
    F_.col("HIEL_ID_SEQ"),
    F_.col("HIEL_START_DT"),
    F_.col("HIEL_END_DT"),
    F_.col("HIEL_INC_EXC_IND"),
    F_.col("HIEL_SEQ_TYPE"),
    F_.col("HIEL_LOG_LEVEL1"),
    F_.col("HIEL_LOG_LEVEL2"),
    F_.col("HIEL_LOG_LEVEL3"),
    F_.col("HIEL_PHY_LEVEL1"),
    F_.col("HIEL_PHY_LEVEL2"),
    F_.col("HIEL_PHY_LEVEL3"),
    F_.col("HIEL_NAME_LEVEL1"),
    F_.col("HIEL_NAME_LEVEL2"),
    F_.col("HIEL_NAME_LEVEL3"),
    F_.col("PAYR_ID"),
    F_.col("HIEL_CREATE_DT"),
    F_.col("HIEL_CREATE_APP_USER"),
    F_.col("HIEL_UPDATE_DT"),
    F_.col("HIEL_UPDATE_DB_USER"),
    F_.col("HIEL_UPDATE_APP_USER"),
    F_.col("HIEL_UPDATE_SEQ"),
    F_.col("CDVL_VALUE_CATEGORY"),
    F_.col("HIPL_YEAR"),
    F_.col("HIPL_START_DT")
)

# Write IhmfConstituent_GrpVbbPlnEligLanding (CSeqFileStage)
write_files(
    df_writeToFile,
    f"{adls_path}/verified/IhmfConstituent_GrpVbbPlnEligLanding.dat.{RunId}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Write P_GRP_VBB_ELIG (CSeqFileStage)
write_files(
    df_load,
    f"{adls_path}/load/P_GRP_VBB_ELIG.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)