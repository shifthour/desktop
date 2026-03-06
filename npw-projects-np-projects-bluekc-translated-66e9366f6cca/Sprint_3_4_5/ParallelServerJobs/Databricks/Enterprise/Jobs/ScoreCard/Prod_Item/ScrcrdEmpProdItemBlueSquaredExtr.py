# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2018 - 2023 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  ScrcrdEmpProdItemBlueSquaredExtr
# MAGIC Called by:  ScorecardEmplProdItemDailyCntl
# MAGIC 
# MAGIC 
# MAGIC Modifications:
# MAGIC =====================================================================================================================================================================
# MAGIC Developer \(9)Date\(9)\(9)Project/Altiris #\(9)                    Change Description\(9)\(9)\(9)\(9)Development Project\(9)Code Reviewer\(9)             Date Reviewed
# MAGIC =====================================================================================================================================================================
# MAGIC Sruthi M    \(9)07/01/2018\(9)5236-Indigo Replacement\(9)    Original Development\(9)\(9)\(9)\(9)EnterpriseDev1\(9)\(9)Hugh Sisson                         2018-07-23
# MAGIC 
# MAGIC Sruthi M    \(9)11/14/2018\(9)5236-Indigo Replacement\(9)    Changed the size of \(9)\(9)\(9)\(9)EnterpriseDev2\(9)\(9)Abhiram Dasarathy                 2018-11-14
# MAGIC                                                                                                                     MSG_STTUS field in the source layout
# MAGIC Goutham Kalidindi     04/04/2019          85906                                       when msg dt is equal to field date SRC_SYS_CD is set         EnterpriseDev2                       Kalyan Neelam                      2019-04-08
# MAGIC                                                                                                                     msg_dt - 1 otherwise msg_dt
# MAGIC 
# MAGIC Karthik C                 06/14/2019             Bug-114191            \(9)\(9)Changed   ther logic to ITS Home-Host Indcator = 1                                EnterpriseDev1                      Kalyan Neelam                      2019-06-14\(9)
# MAGIC                                                                                               \(9)\(9)then "ITS_Host" else "ITS_Home"   in the 
# MAGIC                                                                                                \(9)\(9)Xfm_blue_squared_Info_Business_Logic 
# MAGIC                                                                                                \(9)\(9)stage  for  SCRCRD_ITEM_CAT_ID                           
# MAGIC 
# MAGIC Venkata yama      11/09/2023                592924- Npx report\(9)\(9)Chage filename and parameterised\(9)\(9)\(9)EnterpriseDev2\(9)\(9)Ken Bradmon\(9)\(9)2023-11-10

# MAGIC Blue Squared Extract
# MAGIC The sequential file is used by the ScrcrdEmplProdItemLoad  job to Load Table SCRCRD_EMPL_PROD_ITEM table in ScoreCard Database
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


ScoreCardOwner = get_widget_value('ScoreCardOwner','')
scorecard_secret_name = get_widget_value('scorecard_secret_name','')
CurrentRunDtm = get_widget_value('CurrentRunDtm','')
Source = get_widget_value('Source','BlueSquare')
FileDate = get_widget_value('FileDate','')
OutputFileNm = get_widget_value('OutputFileNm','')

jdbc_url, jdbc_props = get_db_config(scorecard_secret_name)

# ----------------------------------------------------------------------------
# Stage: ScrCrd_EMPL (ODBCConnectorPX)
# ----------------------------------------------------------------------------
df_ScrCrd_EMPL = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT UPPER(EMPL_BLUE_SQRD_ID) as EMPL_BLUE_SQRD_ID , EMPL_ID\nFROM \n{ScoreCardOwner}.EMPL"
    )
    .load()
)

# ----------------------------------------------------------------------------
# Stage: Blue_Squared (PxSequentialFile) - Read
# ----------------------------------------------------------------------------
schema_Blue_Squared = StructType([
    StructField("MSG_DT", StringType(), True),
    StructField("B2_USER_ID", StringType(), True),
    StructField("MSG_TYP", StringType(), True),
    StructField("PLN_CD", StringType(), True),
    StructField("ITS_HOME_HOST_IN", StringType(), True),
    StructField("SCCF_NO", StringType(), True),
    StructField("B2_MSG_ID", StringType(), True),
    StructField("MSG_CD", StringType(), True),
    StructField("MSG_STTUS", StringType(), True)
])

df_Blue_Squared = (
    spark.read.format("csv")
    .option("header", "false")
    .option("sep", ",")
    .option("quote", '"')
    .schema(schema_Blue_Squared)
    .load(f"{adls_path_raw}/landing/{OutputFileNm}")
)

# ----------------------------------------------------------------------------
# Stage: lnk_blue_squared (CTransformerStage)
# ----------------------------------------------------------------------------
df_lnk_blue_squared = (
    df_Blue_Squared
    .withColumn("MSG_DT", F.col("MSG_DT"))
    .withColumn("B2_USER_ID", trim(strip_field(F.col("B2_USER_ID"))))
    .withColumn("MSG_TYP", trim(strip_field(F.col("MSG_TYP"))))
    .withColumn("PLN_CD", trim(strip_field(F.col("PLN_CD"))))
    .withColumn("ITS_HOME_HOST_IN", trim(strip_field(F.col("ITS_HOME_HOST_IN"))))
    .withColumn("SCCF_NO", trim(strip_field(F.col("SCCF_NO"))))
    .withColumn("B2_MSG_ID", trim(strip_field(F.col("B2_MSG_ID"))))
    .withColumn("MSG_CD", trim(strip_field(F.col("MSG_CD"))))
    .withColumn("MSG_STTUS", trim(strip_field(F.col("MSG_STTUS"))))
)

# ----------------------------------------------------------------------------
# Stage: lkp_empl_bluesquared (PxLookup)
#    - Primary input: df_lnk_blue_squared
#    - Reference (left join): df_ScrCrd_EMPL
# ----------------------------------------------------------------------------
df_lkp_empl_bluesquared_joined = (
    df_lnk_blue_squared.alias("Lnk_Blue_Square")
    .join(
        df_ScrCrd_EMPL.alias("Lnk_ScrCrd_Empl"),
        F.upper(F.col("Lnk_Blue_Square.B2_USER_ID")) == F.col("Lnk_ScrCrd_Empl.EMPL_BLUE_SQRD_ID"),
        "left"
    )
)

df_Lnk_bluesquare_Info = df_lkp_empl_bluesquared_joined.select(
    F.col("Lnk_Blue_Square.MSG_DT").alias("MSG_DT"),
    F.col("Lnk_Blue_Square.B2_USER_ID").alias("B2_USER_ID"),
    F.col("Lnk_Blue_Square.MSG_TYP").alias("MSG_TYP"),
    F.col("Lnk_Blue_Square.PLN_CD").alias("PLN_CD"),
    F.col("Lnk_Blue_Square.ITS_HOME_HOST_IN").alias("ITS_HOME_HOST_IN"),
    F.col("Lnk_Blue_Square.SCCF_NO").alias("SCCF_NO"),
    F.col("Lnk_Blue_Square.B2_MSG_ID").alias("B2_MSG_ID"),
    F.col("Lnk_Blue_Square.MSG_CD").alias("MSG_CD"),
    F.col("Lnk_Blue_Square.MSG_STTUS").alias("MSG_STTUS"),
    F.col("Lnk_ScrCrd_Empl.EMPL_ID").alias("EMPL_ID")
)

df_lnk_empl_rej = df_lkp_empl_bluesquared_joined.filter(
    F.col("Lnk_ScrCrd_Empl.EMPL_ID").isNull()
).select(
    F.col("Lnk_Blue_Square.MSG_DT").alias("MSG_DT"),
    F.col("Lnk_Blue_Square.B2_USER_ID").alias("B2_USER_ID"),
    F.col("Lnk_Blue_Square.MSG_TYP").alias("MSG_TYP"),
    F.col("Lnk_Blue_Square.PLN_CD").alias("PLN_CD"),
    F.col("Lnk_Blue_Square.ITS_HOME_HOST_IN").alias("ITS_HOME_HOST_IN"),
    F.col("Lnk_Blue_Square.SCCF_NO").alias("SCCF_NO"),
    F.col("Lnk_Blue_Square.B2_MSG_ID").alias("B2_MSG_ID"),
    F.col("Lnk_Blue_Square.MSG_CD").alias("MSG_CD"),
    F.col("Lnk_Blue_Square.MSG_STTUS").alias("MSG_STTUS"),
    F.col("Lnk_ScrCrd_Empl.EMPL_ID").alias("EMPL_ID")
)

# ----------------------------------------------------------------------------
# Stage: Seq_Scrcrd_Empl_Id_Rej (PxSequentialFile) - Write
# ----------------------------------------------------------------------------
write_files(
    df_lnk_empl_rej,
    f"{adls_path_publish}/external/Scrcrd_Empl_{Source}_Empl_Id_Rej.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote=None,
    nullValue=None
)

# ----------------------------------------------------------------------------
# Stage: Xfm_blue_squared_Info_Business_Logic (CTransformerStage)
# ----------------------------------------------------------------------------
df_Xfm_blue_squared_Info_Business_Logic = (
    df_Lnk_bluesquare_Info
    .withColumn(
        "MsgDt",
        F.concat(
            F.expr("right(substring(MSG_DT,1,10),4)"),
            F.lit("-"),
            F.expr("right(concat('00', trim(strip_field(substring(MSG_DT,1,2)))), 2)"),
            F.lit("-"),
            F.expr("right(substring(MSG_DT,1,5),2)")
        )
    )
    .withColumn("EMPL_ID", F.when(F.col("EMPL_ID").isNull(), F.lit("NA")).otherwise(F.col("EMPL_ID")))
    .withColumn("SCRCRD_ITEM_ID", trim(strip_field(F.col("B2_MSG_ID"))))
    .withColumn("RCRD_EXTR_DTM", CurrentTimestampMS())
    .withColumn("SRC_SYS_CD", F.lit("B2T_OPI_ACTIVITY_FILE"))
    .withColumn(
        "SCRCRD_ITEM_CAT_ID",
        F.when(
            trim(strip_field(F.col("ITS_HOME_HOST_IN"))) == F.lit("1"),
            F.lit("ITS_HOST")
        ).otherwise(F.lit("ITS_HOME"))
    )
    .withColumn("SCRCRD_ITEM_TYP_ID", trim(strip_field(F.col("MSG_TYP"))))
    .withColumn(
        "SCRCRD_WORK_TYP_ID",
        F.when(
            (trim(F.col("MSG_CD")) == F.lit("O")) & (trim(F.col("MSG_TYP")) == F.lit("INFOMSG")), F.lit("B2INFO")
        ).when(
            trim(F.col("MSG_TYP")) == F.lit("CLMAPPL"), F.lit("B2CLMAPPL")
        ).when(
            trim(F.col("MSG_TYP")) == F.lit("MISROUTE"), F.lit("B2MISROUTE")
        ).when(
            trim(F.col("MSG_TYP")) == F.lit("MEDREC"), F.lit("B2")
        ).when(
            ((trim(F.col("MSG_CD")) == F.lit("Q")) | (trim(F.col("MSG_CD")) == F.lit("R")))
            & trim(F.col("MSG_TYP")).isin("GENINQ","GLOBAL","276","277","PQI","CSRN"),
            F.lit("B2")
        ).when(
            ((trim(F.col("MSG_CD")) == F.lit("Q")) | (trim(F.col("MSG_CD")) == F.lit("R")))
            & (trim(F.col("MSG_TYP")) == F.lit("ADJUST")),
            F.lit("B2ADJ")
        ).when(
            ((trim(F.col("MSG_CD")) == F.lit("Q")) | (trim(F.col("MSG_CD")) == F.lit("R")))
            & (trim(F.col("MSG_TYP")) == F.lit("CNCLADJ")),
            F.lit("B2CNCL2ADJ")
        ).otherwise(F.lit("B2ADJ"))
    )
    .withColumn("SRC_SYS_INPT_METH_CD", F.lit("NA"))
    .withColumn("SRC_SYS_STATUS_CD", trim(strip_field(F.col("MSG_STTUS"))))
    .withColumn("GRP_ID", F.lit("NA"))
    .withColumn("MBR_ID", F.lit("NA"))
    .withColumn("PROD_ABBR", F.lit("NA"))
    .withColumn("PROD_LOB_NO", F.lit("NA"))
    .withColumn("SCCF_NO", trim(strip_field(F.col("SCCF_NO"))))
    .withColumn("SRC_SYS_RCVD_DTM", F.concat(F.col("MsgDt"), F.lit(" 00:00:00")))
    .withColumn("SRC_SYS_CMPLD_DTM", F.concat(F.col("MsgDt"), F.lit(" 00:00:00")))
    .withColumn(
        "SRC_SYS_TRANS_DTM",
        F.when(
            F.col("MSG_DT") == FileDate,
            F.concat(DateOffsetByComponents(F.col("MsgDt"), F.lit(0), F.lit(0), F.lit(-1)), F.lit(" 00:00:00"))
        ).otherwise(F.concat(F.col("MsgDt"), F.lit(" 00:00:00")))
    )
    .withColumn("ACTVTY_CT", F.lit("1"))
    .withColumn("CRT_USER_ID", F.lit("BLUE SQUARED"))
    .withColumn("LAST_UPDT_DTM", CurrentRunDtm())
    .withColumn("LAST_UPDT_USER_ID", F.lit("BLUE SQUARED"))
    .withColumn("MSG_TYP", F.col("MSG_TYP"))
    .withColumn("MSG_CD", F.col("MSG_CD"))
)

# ----------------------------------------------------------------------------
# Stage: Xfm_Inquiry_Info (CTransformerStage)
# ----------------------------------------------------------------------------
df_Xfm_Inquiry_Info = df_Xfm_blue_squared_Info_Business_Logic.select(
    F.col("EMPL_ID").alias("EMPL_ID"),
    F.col("SCRCRD_ITEM_ID").alias("SCRCRD_ITEM_ID"),
    StringToTimestamp(F.col("RCRD_EXTR_DTM"), "%yyyy-%mm-%dd %hh:%nn:%ss.6").alias("RCRD_EXTR_DTM"),
    convert("char(10) : char(13)", "", F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    convert("char(10) : char(13)", "", F.col("SCRCRD_ITEM_CAT_ID")).alias("SCRCRD_ITEM_CAT_ID"),
    convert("char(10) : char(13)", "", F.col("SCRCRD_ITEM_TYP_ID")).alias("SCRCRD_ITEM_TYP_ID"),
    convert("char(10) : char(13)", "", F.col("SCRCRD_WORK_TYP_ID")).alias("SCRCRD_WORK_TYP_ID"),
    convert("char(10) : char(13)", "", F.col("SRC_SYS_INPT_METH_CD")).alias("SRC_SYS_INPT_METH_CD"),
    convert("char(10) : char(13)", "", F.col("SRC_SYS_STATUS_CD")).alias("SRC_SYS_STTUS_CD"),
    convert("char(10) : char(13)", "", F.col("GRP_ID")).alias("GRP_ID"),
    convert("char(10) : char(13)", "", F.col("MBR_ID")).alias("MBR_ID"),
    convert("char(10) : char(13)", "", F.col("PROD_ABBR")).alias("PROD_ABBR"),
    convert("char(10) : char(13)", "", F.col("PROD_LOB_NO")).alias("PROD_LOB_NO"),
    convert("char(10) : char(13)", "", F.col("SCCF_NO")).alias("SCCF_NO"),
    convert("char(10) : char(13)", "", F.col("SRC_SYS_RCVD_DTM")).alias("SRC_SYS_RCVD_DTM"),
    convert("char(10) : char(13)", "", F.col("SRC_SYS_CMPLD_DTM")).alias("SRC_SYS_CMPLD_DTM"),
    convert("char(10) : char(13)", "", F.col("SRC_SYS_TRANS_DTM")).alias("SRC_SYS_TRANS_DTM"),
    convert("char(10) : char(13)", "", F.col("ACTVTY_CT")).alias("ACTVTY_CT"),
    CurrentRunDtm().alias("CRT_DTM"),
    F.col("CRT_USER_ID").alias("CRT_USER_ID"),
    CurrentRunDtm().alias("LAST_UPDT_DTM"),
    F.col("LAST_UPDT_USER_ID").alias("LAST_UPDT_USER_ID")
)

# ----------------------------------------------------------------------------
# Stage: Scrcrd_Empl_Prod_Item_Load_Ready_File (PxSequentialFile) - Write
# ----------------------------------------------------------------------------
write_files(
    df_Xfm_Inquiry_Info,
    f"{adls_path}/load/Scrcrd_Prod_Item_{Source}.dat",
    delimiter="#$",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote='"',
    nullValue=None
)