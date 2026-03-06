# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC CALLED BY: 
# MAGIC 
# MAGIC PROCESSING:  Strip field function and Tranforamtion rules are applied on the data Extracted from inbound file
# MAGIC                                                                 
# MAGIC      
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                   DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                       DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Kalyan Neelam          2015-07-27           5460                              Initial Programming                                                                                IntegrateDev2          Bhoomi Dasari              8/30/2015
# MAGIC 
# MAGIC 
# MAGIC Nathan Reynolds      2016-09-06          13152                              Change Transforms, move null check Stage Variable area in                IntegrateDev1         Jag Yelavarthi              2016-09-16
# MAGIC                                                                                                               Business Rules transform to avoid warnings                               
# MAGIC 
# MAGIC Krishnakanth             2017-12-06          30001                             Parameterize the variable Vendor                                                          IntegrateDev2           Kalyan Neelam          2017-12-06
# MAGIC   Manivannan
# MAGIC 
# MAGIC Goutham Kalidindi          2022-03-29           US500022                     Add source system code filter in the stage                                               IntegrateDev2    Reddy Sanam            2022-04-13
# MAGIC                                                                                                    "db2_K_CCHG_SNGL_CAT_GRP"

# MAGIC CCHG_MULT_CAT_GRP Transform job
# MAGIC Strip Carriage Return, Line Feed, and Tab.
# MAGIC Transformed Data will land into a Dataset for Primary Keying job.
# MAGIC 
# MAGIC Data is partitioned on Natural Key Columns.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, StructType, StructField
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# Parameters
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
RunID = get_widget_value('RunID','')
RunIDTimeStamp = get_widget_value('RunIDTimeStamp','')
CurrDate = get_widget_value('CurrDate','')
Vendor = get_widget_value('Vendor','')

# Read from DB2ConnectorPX: db2_K_CCHG_SNGL_CAT_GRP
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = (
    f"SELECT CCHG_SNGL_CAT_GRP_CD, CCHG_SNGL_CAT_GRP_SK "
    f"FROM {IDSOwner}.K_CCHG_SNGL_CAT_GRP "
    f"WHERE SRC_SYS_CD = '{SrcSysCd}'"
)
df_db2_K_CCHG_SNGL_CAT_GRP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Copy Stage: produce 9 separate DataFrames for SnglCatGrp1..SnglCatGrp9
df_SnglCatGrp1 = df_db2_K_CCHG_SNGL_CAT_GRP.select(
    F.col("CCHG_SNGL_CAT_GRP_CD").alias("CCHG_SNGL_CAT_GRP_CD"),
    F.col("CCHG_SNGL_CAT_GRP_SK").alias("CCHG_SNGL_CAT_GRP_SK")
)
df_SnglCatGrp2 = df_db2_K_CCHG_SNGL_CAT_GRP.select(
    F.col("CCHG_SNGL_CAT_GRP_CD").alias("CCHG_SNGL_CAT_GRP_CD"),
    F.col("CCHG_SNGL_CAT_GRP_SK").alias("CCHG_SNGL_CAT_GRP_SK")
)
df_SnglCatGrp3 = df_db2_K_CCHG_SNGL_CAT_GRP.select(
    F.col("CCHG_SNGL_CAT_GRP_CD").alias("CCHG_SNGL_CAT_GRP_CD"),
    F.col("CCHG_SNGL_CAT_GRP_SK").alias("CCHG_SNGL_CAT_GRP_SK")
)
df_SnglCatGrp4 = df_db2_K_CCHG_SNGL_CAT_GRP.select(
    F.col("CCHG_SNGL_CAT_GRP_CD").alias("CCHG_SNGL_CAT_GRP_CD"),
    F.col("CCHG_SNGL_CAT_GRP_SK").alias("CCHG_SNGL_CAT_GRP_SK")
)
df_SnglCatGrp5 = df_db2_K_CCHG_SNGL_CAT_GRP.select(
    F.col("CCHG_SNGL_CAT_GRP_CD").alias("CCHG_SNGL_CAT_GRP_CD"),
    F.col("CCHG_SNGL_CAT_GRP_SK").alias("CCHG_SNGL_CAT_GRP_SK")
)
df_SnglCatGrp6 = df_db2_K_CCHG_SNGL_CAT_GRP.select(
    F.col("CCHG_SNGL_CAT_GRP_CD").alias("CCHG_SNGL_CAT_GRP_CD"),
    F.col("CCHG_SNGL_CAT_GRP_SK").alias("CCHG_SNGL_CAT_GRP_SK")
)
df_SnglCatGrp7 = df_db2_K_CCHG_SNGL_CAT_GRP.select(
    F.col("CCHG_SNGL_CAT_GRP_CD").alias("CCHG_SNGL_CAT_GRP_CD"),
    F.col("CCHG_SNGL_CAT_GRP_SK").alias("CCHG_SNGL_CAT_GRP_SK")
)
df_SnglCatGrp8 = df_db2_K_CCHG_SNGL_CAT_GRP.select(
    F.col("CCHG_SNGL_CAT_GRP_CD").alias("CCHG_SNGL_CAT_GRP_CD"),
    F.col("CCHG_SNGL_CAT_GRP_SK").alias("CCHG_SNGL_CAT_GRP_SK")
)
df_SnglCatGrp9 = df_db2_K_CCHG_SNGL_CAT_GRP.select(
    F.col("CCHG_SNGL_CAT_GRP_CD").alias("CCHG_SNGL_CAT_GRP_CD"),
    F.col("CCHG_SNGL_CAT_GRP_SK").alias("CCHG_SNGL_CAT_GRP_SK")
)

# Read PxDataSet: ds_CCHG_MULT_CAT_GRP_Extr
df_ds_CCHG_MULT_CAT_GRP_Extr = spark.read.parquet(
    f"{adls_path}/ds/CCHG_MULT_CAT_GRP.{SrcSysCd}.extr.{RunID}.parquet"
)

# Transformer: StripField
df_StripField = (
    df_ds_CCHG_MULT_CAT_GRP_Extr
    .withColumn("CCHG_ID", trim(F.col("CCHG_ID")))
    .withColumn("CCHG_MULT_CAT_GRP_DESC", trim(F.col("CCHG_MULT_CAT_GRP_DESC")))
    .withColumn("CCHG_SNGL_CAT_GRP_CD", trim(F.col("CCHG_SNGL_CAT_GRP_CD")))
    .withColumn("CCHG_SNGL_CAT_GRP_MPPNG_DESC", trim(F.col("CCHG_SNGL_CAT_GRP_MPPNG_DESC")))
)

# BASIC_Transformer
df_BASIC_Transformer = df_StripField.withColumn(
    "CCHG_SNGL_CAT_GRP_MPPNG_DESC",
    EReplace(F.col("CCHG_SNGL_CAT_GRP_MPPNG_DESC"), F.lit("."), F.lit(","))
)

# SnglCatGrps Transformer
df_SnglCatGrps = (
    df_BASIC_Transformer
    .withColumn(
        "ErrInd",
        F.when(
            F.col("CCHG_ID").isNull() | (F.length(trim(F.col("CCHG_ID"))) == 0),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn("CCHG_MULT_CAT_GRP_ID", F.col("CCHG_ID"))
    .withColumn(
        "CCHG_SNGL_CAT_GRP_1",
        trim(
            FIELD(
                FIELD(F.col("CCHG_SNGL_CAT_GRP_MPPNG_DESC"), F.lit(","), F.lit(1)),
                F.lit("("),
                F.lit("1")
            )
        )
    )
    .withColumn(
        "CCHG_SNGL_CAT_GRP_2",
        trim(
            FIELD(
                FIELD(F.col("CCHG_SNGL_CAT_GRP_MPPNG_DESC"), F.lit(","), F.lit(2)),
                F.lit("("),
                F.lit("1")
            )
        )
    )
    .withColumn(
        "CCHG_SNGL_CAT_GRP_3",
        trim(
            FIELD(
                FIELD(F.col("CCHG_SNGL_CAT_GRP_MPPNG_DESC"), F.lit(","), F.lit(3)),
                F.lit("("),
                F.lit("1")
            )
        )
    )
    .withColumn(
        "CCHG_SNGL_CAT_GRP_4",
        trim(
            FIELD(
                FIELD(F.col("CCHG_SNGL_CAT_GRP_MPPNG_DESC"), F.lit(","), F.lit(4)),
                F.lit("("),
                F.lit("1")
            )
        )
    )
    .withColumn(
        "CCHG_SNGL_CAT_GRP_5",
        trim(
            FIELD(
                FIELD(F.col("CCHG_SNGL_CAT_GRP_MPPNG_DESC"), F.lit(","), F.lit(5)),
                F.lit("("),
                F.lit("1")
            )
        )
    )
    .withColumn(
        "CCHG_SNGL_CAT_GRP_6",
        trim(
            FIELD(
                FIELD(F.col("CCHG_SNGL_CAT_GRP_MPPNG_DESC"), F.lit(","), F.lit(6)),
                F.lit("("),
                F.lit("1")
            )
        )
    )
    .withColumn(
        "CCHG_SNGL_CAT_GRP_7",
        trim(
            FIELD(
                FIELD(F.col("CCHG_SNGL_CAT_GRP_MPPNG_DESC"), F.lit(","), F.lit(7)),
                F.lit("("),
                F.lit("1")
            )
        )
    )
    .withColumn(
        "CCHG_SNGL_CAT_GRP_8",
        trim(
            FIELD(
                FIELD(F.col("CCHG_SNGL_CAT_GRP_MPPNG_DESC"), F.lit(","), F.lit(8)),
                F.lit("("),
                F.lit("1")
            )
        )
    )
    .withColumn(
        "CCHG_SNGL_CAT_GRP_9",
        trim(
            FIELD(
                FIELD(F.col("CCHG_SNGL_CAT_GRP_MPPNG_DESC"), F.lit(","), F.lit(9)),
                F.lit("("),
                F.lit("1")
            )
        )
    )
    .withColumn("CCHG_SNGL_CAT_GRP_CD", F.col("CCHG_SNGL_CAT_GRP_CD"))
    .withColumn("CCHG_MULT_CAT_GRP_DESC", F.col("CCHG_MULT_CAT_GRP_DESC"))
    .withColumn("CCHG_SNGL_CAT_GRP_MPPNG_TX", F.col("CCHG_SNGL_CAT_GRP_MPPNG_DESC"))
)

# Lookup_78: 9 left joins
df_Lookup_78_joined = (
    df_SnglCatGrps.alias("In")
    .join(df_SnglCatGrp1.alias("SnglCatGrp1"),
          F.col("In.CCHG_SNGL_CAT_GRP_1") == F.col("SnglCatGrp1.CCHG_SNGL_CAT_GRP_CD"), "left")
    .join(df_SnglCatGrp2.alias("SnglCatGrp2"),
          F.col("In.CCHG_SNGL_CAT_GRP_2") == F.col("SnglCatGrp2.CCHG_SNGL_CAT_GRP_CD"), "left")
    .join(df_SnglCatGrp3.alias("SnglCatGrp3"),
          F.col("In.CCHG_SNGL_CAT_GRP_3") == F.col("SnglCatGrp3.CCHG_SNGL_CAT_GRP_CD"), "left")
    .join(df_SnglCatGrp4.alias("SnglCatGrp4"),
          F.col("In.CCHG_SNGL_CAT_GRP_4") == F.col("SnglCatGrp4.CCHG_SNGL_CAT_GRP_CD"), "left")
    .join(df_SnglCatGrp5.alias("SnglCatGrp5"),
          F.col("In.CCHG_SNGL_CAT_GRP_5") == F.col("SnglCatGrp5.CCHG_SNGL_CAT_GRP_CD"), "left")
    .join(df_SnglCatGrp6.alias("SnglCatGrp6"),
          F.col("In.CCHG_SNGL_CAT_GRP_6") == F.col("SnglCatGrp6.CCHG_SNGL_CAT_GRP_CD"), "left")
    .join(df_SnglCatGrp7.alias("SnglCatGrp7"),
          F.col("In.CCHG_SNGL_CAT_GRP_7") == F.col("SnglCatGrp7.CCHG_SNGL_CAT_GRP_CD"), "left")
    .join(df_SnglCatGrp8.alias("SnglCatGrp8"),
          F.col("In.CCHG_SNGL_CAT_GRP_8") == F.col("SnglCatGrp8.CCHG_SNGL_CAT_GRP_CD"), "left")
    .join(df_SnglCatGrp9.alias("SnglCatGrp9"),
          F.col("In.CCHG_SNGL_CAT_GRP_9") == F.col("SnglCatGrp9.CCHG_SNGL_CAT_GRP_CD"), "left")
)

df_Lookup_78 = df_Lookup_78_joined.select(
    F.col("In.CCHG_MULT_CAT_GRP_ID").alias("CCHG_MULT_CAT_GRP_ID"),
    F.col("In.CCHG_SNGL_CAT_GRP_1").alias("CCHG_SNGL_CAT_GRP_1"),
    F.col("In.CCHG_SNGL_CAT_GRP_2").alias("CCHG_SNGL_CAT_GRP_2"),
    F.col("In.CCHG_SNGL_CAT_GRP_3").alias("CCHG_SNGL_CAT_GRP_3"),
    F.col("In.CCHG_SNGL_CAT_GRP_4").alias("CCHG_SNGL_CAT_GRP_4"),
    F.col("In.CCHG_SNGL_CAT_GRP_5").alias("CCHG_SNGL_CAT_GRP_5"),
    F.col("In.CCHG_SNGL_CAT_GRP_6").alias("CCHG_SNGL_CAT_GRP_6"),
    F.col("In.CCHG_SNGL_CAT_GRP_7").alias("CCHG_SNGL_CAT_GRP_7"),
    F.col("In.CCHG_SNGL_CAT_GRP_8").alias("CCHG_SNGL_CAT_GRP_8"),
    F.col("In.CCHG_SNGL_CAT_GRP_9").alias("CCHG_SNGL_CAT_GRP_9"),
    F.col("In.CCHG_MULT_CAT_GRP_DESC").alias("CCHG_MULT_CAT_GRP_DESC"),
    F.col("In.CCHG_SNGL_CAT_GRP_MPPNG_TX").alias("CCHG_SNGL_CAT_GRP_MPPNG_TX"),
    F.col("SnglCatGrp1.CCHG_SNGL_CAT_GRP_SK").alias("CCHG_SNGL_CAT_GRP_SK_1"),
    F.col("SnglCatGrp2.CCHG_SNGL_CAT_GRP_SK").alias("CCHG_SNGL_CAT_GRP_SK_2"),
    F.col("SnglCatGrp3.CCHG_SNGL_CAT_GRP_SK").alias("CCHG_SNGL_CAT_GRP_SK_3"),
    F.col("SnglCatGrp4.CCHG_SNGL_CAT_GRP_SK").alias("CCHG_SNGL_CAT_GRP_SK_4"),
    F.col("SnglCatGrp5.CCHG_SNGL_CAT_GRP_SK").alias("CCHG_SNGL_CAT_GRP_SK_5"),
    F.col("SnglCatGrp6.CCHG_SNGL_CAT_GRP_SK").alias("CCHG_SNGL_CAT_GRP_SK_6"),
    F.col("SnglCatGrp7.CCHG_SNGL_CAT_GRP_SK").alias("CCHG_SNGL_CAT_GRP_SK_7"),
    F.col("SnglCatGrp8.CCHG_SNGL_CAT_GRP_SK").alias("CCHG_SNGL_CAT_GRP_SK_8"),
    F.col("SnglCatGrp9.CCHG_SNGL_CAT_GRP_SK").alias("CCHG_SNGL_CAT_GRP_SK_9"),
    F.col("In.CCHG_SNGL_CAT_GRP_CD").alias("CCHG_SNGL_CAT_GRP_CD")
)

# BusinessRules Transformer
df_BusinessRules = (
    df_Lookup_78
    .withColumn(
        "ErrInd",
        F.when(
            F.col("CCHG_MULT_CAT_GRP_ID").isNull() | (F.length(trim(F.col("CCHG_MULT_CAT_GRP_ID"))) == 0),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "svCchgSnglCatGrp1",
        F.when(
            F.col("CCHG_SNGL_CAT_GRP_1").isNull()
            | (F.col("CCHG_SNGL_CAT_GRP_1") == "")
            | (F.col("CCHG_SNGL_CAT_GRP_1") == " "),
            F.lit("NA")
        ).otherwise(F.col("CCHG_SNGL_CAT_GRP_1"))
    )
    .withColumn(
        "svCchgSnglCatGrp2",
        F.when(
            F.col("CCHG_SNGL_CAT_GRP_2").isNull()
            | (F.col("CCHG_SNGL_CAT_GRP_2") == "")
            | (F.col("CCHG_SNGL_CAT_GRP_2") == " "),
            F.lit("NA")
        ).otherwise(F.col("CCHG_SNGL_CAT_GRP_2"))
    )
    .withColumn(
        "svCchgSnglCatGrp3",
        F.when(
            F.col("CCHG_SNGL_CAT_GRP_3").isNull()
            | (F.col("CCHG_SNGL_CAT_GRP_3") == "")
            | (F.col("CCHG_SNGL_CAT_GRP_3") == " "),
            F.lit("NA")
        ).otherwise(F.col("CCHG_SNGL_CAT_GRP_3"))
    )
    .withColumn(
        "svCchgSnglCatGrp4",
        F.when(
            F.col("CCHG_SNGL_CAT_GRP_4").isNull()
            | (F.col("CCHG_SNGL_CAT_GRP_4") == "")
            | (F.col("CCHG_SNGL_CAT_GRP_4") == " "),
            F.lit("NA")
        ).otherwise(F.col("CCHG_SNGL_CAT_GRP_4"))
    )
    .withColumn(
        "svCchgSnglCatGrp5",
        F.when(
            F.col("CCHG_SNGL_CAT_GRP_5").isNull()
            | (F.col("CCHG_SNGL_CAT_GRP_5") == "")
            | (F.col("CCHG_SNGL_CAT_GRP_5") == " "),
            F.lit("NA")
        ).otherwise(F.col("CCHG_SNGL_CAT_GRP_5"))
    )
    .withColumn(
        "svCchgSnglCatGrp6",
        F.when(
            F.col("CCHG_SNGL_CAT_GRP_6").isNull()
            | (F.col("CCHG_SNGL_CAT_GRP_6") == "")
            | (F.col("CCHG_SNGL_CAT_GRP_6") == " "),
            F.lit("NA")
        ).otherwise(F.col("CCHG_SNGL_CAT_GRP_6"))
    )
    .withColumn(
        "svCchgSnglCatGrp7",
        F.when(
            F.col("CCHG_SNGL_CAT_GRP_7").isNull()
            | (F.col("CCHG_SNGL_CAT_GRP_7") == "")
            | (F.col("CCHG_SNGL_CAT_GRP_7") == " "),
            F.lit("NA")
        ).otherwise(F.col("CCHG_SNGL_CAT_GRP_7"))
    )
    .withColumn(
        "svCchgSnglCatGrp8",
        F.when(
            F.col("CCHG_SNGL_CAT_GRP_8").isNull()
            | (F.col("CCHG_SNGL_CAT_GRP_8") == "")
            | (F.col("CCHG_SNGL_CAT_GRP_8") == " "),
            F.lit("NA")
        ).otherwise(F.col("CCHG_SNGL_CAT_GRP_8"))
    )
    .withColumn(
        "svCchgSnglCatGrp9",
        F.when(
            F.col("CCHG_SNGL_CAT_GRP_9").isNull()
            | (F.col("CCHG_SNGL_CAT_GRP_9") == "")
            | (F.col("CCHG_SNGL_CAT_GRP_9") == " "),
            F.lit("NA")
        ).otherwise(F.col("CCHG_SNGL_CAT_GRP_9"))
    )
)

# Partition into three outputs based on ErrInd
df_BusinessRulesPkeyOut = df_BusinessRules.filter(F.col("ErrInd") == "N").select(
    F.concat(F.col("CCHG_MULT_CAT_GRP_ID"), F.lit(";"), F.lit(SrcSysCd)).alias("PRI_NAT_KEY_STRING"),
    F.lit(RunIDTimeStamp).alias("FIRST_RECYC_TS"),
    F.lit(0).alias("CCHG_MULT_CAT_GRP_SK"),
    F.col("CCHG_MULT_CAT_GRP_ID").alias("CCHG_MULT_CAT_GRP_ID"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(Num(F.col("svCchgSnglCatGrp1")) == 0, F.lit("NA")).otherwise(F.col("svCchgSnglCatGrp1")).alias("CCHG_SNGL_CAT_GRP_1"),
    F.when(Num(F.col("svCchgSnglCatGrp2")) == 0, F.lit("NA")).otherwise(F.col("svCchgSnglCatGrp2")).alias("CCHG_SNGL_CAT_GRP_2"),
    F.when(Num(F.col("svCchgSnglCatGrp3")) == 0, F.lit("NA")).otherwise(F.col("svCchgSnglCatGrp3")).alias("CCHG_SNGL_CAT_GRP_3"),
    F.when(Num(F.col("svCchgSnglCatGrp4")) == 0, F.lit("NA")).otherwise(F.col("svCchgSnglCatGrp4")).alias("CCHG_SNGL_CAT_GRP_4"),
    F.when(Num(F.col("svCchgSnglCatGrp5")) == 0, F.lit("NA")).otherwise(F.col("svCchgSnglCatGrp5")).alias("CCHG_SNGL_CAT_GRP_5"),
    F.when(Num(F.col("svCchgSnglCatGrp6")) == 0, F.lit("NA")).otherwise(F.col("svCchgSnglCatGrp6")).alias("CCHG_SNGL_CAT_GRP_6"),
    F.when(Num(F.col("svCchgSnglCatGrp7")) == 0, F.lit("NA")).otherwise(F.col("svCchgSnglCatGrp7")).alias("CCHG_SNGL_CAT_GRP_7"),
    F.when(Num(F.col("svCchgSnglCatGrp8")) == 0, F.lit("NA")).otherwise(F.col("svCchgSnglCatGrp8")).alias("CCHG_SNGL_CAT_GRP_8"),
    F.when(Num(F.col("svCchgSnglCatGrp9")) == 0, F.lit("NA")).otherwise(F.col("svCchgSnglCatGrp9")).alias("CCHG_SNGL_CAT_GRP_9"),
    F.col("CCHG_SNGL_CAT_GRP_CD").alias("CCHG_SNGL_CAT_GRP_CD"),
    F.col("CCHG_MULT_CAT_GRP_DESC").alias("CCHG_MULT_CAT_GRP_DESC"),
    F.col("CCHG_SNGL_CAT_GRP_MPPNG_TX").alias("CCHG_SNGL_CAT_GRP_MPPNG_TX")
)

df_BusinessRulesSnapshot = df_BusinessRules.filter(F.col("ErrInd") == "N").select(
    F.col("CCHG_MULT_CAT_GRP_ID").alias("CCHG_MULT_CAT_GRP_ID"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK")
)

df_BusinessRulesError = df_BusinessRules.filter(F.col("ErrInd") == "Y").select(
    F.col("CCHG_MULT_CAT_GRP_ID").alias("CCHG_MULT_CAT_GRP_ID"),
    F.col("CCHG_MULT_CAT_GRP_DESC").alias("CCHG_MULT_CAT_GRP_DESC"),
    F.col("CCHG_SNGL_CAT_GRP_CD").alias("CCHG_SNGL_CAT_GRP_CD"),
    F.col("CCHG_SNGL_CAT_GRP_MPPNG_TX").alias("CCHG_SNGL_CAT_GRP_MPPNG_TX")
)

# B_CCHG_SNGL_CAT_GRP (PxSequentialFile)
write_files(
    df_BusinessRulesSnapshot.select("CCHG_MULT_CAT_GRP_ID","SRC_SYS_CD_SK"),
    f"{adls_path}/load/B_CCHG_MULT_CAT_GRP.{SrcSysCd}.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

# ds_CCHG_MULT_CAT_GRP_Xfrm (PxDataSet)
write_files(
    df_BusinessRulesPkeyOut.select(
        "PRI_NAT_KEY_STRING",
        "FIRST_RECYC_TS",
        "CCHG_MULT_CAT_GRP_SK",
        "CCHG_MULT_CAT_GRP_ID",
        "SRC_SYS_CD",
        "SRC_SYS_CD_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CCHG_SNGL_CAT_GRP_1",
        "CCHG_SNGL_CAT_GRP_2",
        "CCHG_SNGL_CAT_GRP_3",
        "CCHG_SNGL_CAT_GRP_4",
        "CCHG_SNGL_CAT_GRP_5",
        "CCHG_SNGL_CAT_GRP_6",
        "CCHG_SNGL_CAT_GRP_7",
        "CCHG_SNGL_CAT_GRP_8",
        "CCHG_SNGL_CAT_GRP_9",
        "CCHG_SNGL_CAT_GRP_CD",
        "CCHG_MULT_CAT_GRP_DESC",
        "CCHG_SNGL_CAT_GRP_MPPNG_TX"
    ),
    f"{adls_path}/ds/CCHG_MULT_CAT_GRP.{SrcSysCd}.xfrm.{RunID}.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# ErrorFile (PxSequentialFile)
write_files(
    df_BusinessRulesError.select(
        "CCHG_MULT_CAT_GRP_ID",
        "CCHG_MULT_CAT_GRP_DESC",
        "CCHG_SNGL_CAT_GRP_CD",
        "CCHG_SNGL_CAT_GRP_MPPNG_TX"
    ),
    f"{adls_path_publish}/external/{Vendor}_CCHG_MULT_CAT_GRP_ERRORS.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)