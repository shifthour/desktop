# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2005, 2006, 2007, 2009, 2010 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by: 
# MAGIC                     
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                      Project/                                                                                                                                                                     Code                   Date
# MAGIC Developer               Date              Altiris #                              Change Description                                              Environment                                       Reviewer            Reviewed
# MAGIC --------------------------     -------------------   -------------                            -----------------------------------------------------------------------------------------------------------------                       -------------------------  -------------------
# MAGIC Tejaswi Gogineni    2018-12-18    60037                              Initial Programming                                                IntegrateDev2\(9)\(9)       Kalyan Neelam   2019-01-02
# MAGIC                                                                                      Extracts data from the BCBSA_MBR_CRSWALK
# MAGIC                                                                                             table.
# MAGIC Razia F               2022-03-01    S2S Remediation     MSSQL connection parameters added                        IntegrateDev5                                    Manasa Andru      2022-04-28

# MAGIC Job name: FctsIdsPBcbsaMbrCrsWalkExtr
# MAGIC Extract Data from Factes and BCBS table BCBSA_MBR_CRSWALK
# MAGIC ,CMC_GRGR_GROUP
# MAGIC ,CMC_SBIH_ID_HIST
# MAGIC ,CMC_SBSB_SUBSC
# MAGIC ,CMC_MEME_MEMBER
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.functions import lit, when, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Parameters
RunID = get_widget_value('RunID','')
FacetsOwner = get_widget_value('$FacetsOwner','')
IDSOwner = get_widget_value('$IDSOwner','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
BCBSOwner = get_widget_value('$BCBSOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
ids_secret_name = get_widget_value('ids_secret_name','')
bcbs_secret_name = get_widget_value('bcbs_secret_name','')

# Stage: db2_MBR (DB2ConnectorPX)
db2_MBR_sql = f"""SELECT
  GRP.GRP_SK
, MBR.SUB_SK
, SUB.SUB_ID
, MBR.MBR_SK
, Cast(SUB.SUB_ID || MBR.MBR_SFX_NO as Varchar(20)) as MBR_ID
, MBR.FIRST_NM
, MBR.MIDINIT
, MBR.LAST_NM
, MBR.MBR_SFX_NO
, MBR.INDV_BE_KEY INDV_BE_KEY
, MBR.MBR_UNIQ_KEY
, MBR.BRTH_DT_SK

FROM {IDSOwner}.MBR MBR
INNER JOIN {IDSOwner}.SUB SUB ON MBR.SUB_SK = SUB.SUB_SK
INNER JOIN {IDSOwner}.GRP GRP ON SUB.GRP_SK = GRP.GRP_SK

--fetch first 10 rows only 

with ur
"""
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_db2_MBR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", db2_MBR_sql)
    .load()
)

# Stage: SEL_BCBSA_MBR_CRSWALK (ODBCConnectorPX)
sel_bcbsa_mbr_crswalk_sql = f"""SELECT  
CRSWALK.BCBSA_MBR_CRSWALK_ID
, CRSWALK.GRGR_ID
, CRSWALK.BCBSA_HOME_PLN_ID
, CRSWALK.BCBSA_HOME_PLN_CONSIS_MBR_ID
, CRSWALK.BCBSA_ITS_SUB_ID
, CRSWALK.PRIOR_HOME_PLN_CONSIS_MBR_ID
, MEME.MEME_CK
, MEME.MEME_SFX
, MEME.MEME_REL
, SBSB.SBSB_ID + '00' MBR_ID
, SBSB.SBSB_ID SUB_ID
, MEME.MEME_BIRTH_DT
FROM
{BCBSOwner}.BCBSA_MBR_CRSWALK CRSWALK
INNER JOIN {FacetsOwner}.CMC_GRGR_GROUP G
ON CRSWALK.GRGR_ID = G.GRGR_ID
INNER JOIN {FacetsOwner}.CMC_SBIH_ID_HIST HIST
ON CRSWALK.BCBSA_MBR_CRSWALK_ID = HIST.SBIH_SBSB_ID_ORIG
AND HIST.GRGR_CK = G.GRGR_CK
INNER JOIN {FacetsOwner}.CMC_SBSB_SUBSC SBSB
ON HIST.SBSB_CK = SBSB.SBSB_CK
INNER JOIN {FacetsOwner}.CMC_MEME_MEMBER MEME
ON SBSB.SBSB_CK = MEME.SBSB_CK AND
MEME.MEME_REL = 'M'
"""
jdbc_url_bcbs, jdbc_props_bcbs = get_db_config(bcbs_secret_name)
df_SEL_BCBSA_MBR_CRSWALK = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bcbs)
    .options(**jdbc_props_bcbs)
    .option("query", sel_bcbsa_mbr_crswalk_sql)
    .load()
)

# Stage: BCBSA_MBR_CRSWALK_xfm (CTransformerStage)
df_BCBSA_MBR_CRSWALK_xfm = df_SEL_BCBSA_MBR_CRSWALK.select(
    F.col("BCBSA_MBR_CRSWALK_ID").alias("BCBSA_MBR_CRSWALK_ID"),
    F.col("GRGR_ID").alias("GRGR_ID"),
    F.col("BCBSA_HOME_PLN_ID").alias("BCBSA_HOME_PLN_ID"),
    F.col("BCBSA_HOME_PLN_CONSIS_MBR_ID").alias("BCBSA_HOME_PLN_CONSIS_MBR_ID"),
    F.col("BCBSA_ITS_SUB_ID").alias("BCBSA_ITS_SUB_ID"),
    F.col("PRIOR_HOME_PLN_CONSIS_MBR_ID").alias("PRIOR_HOME_PLN_CONSIS_MBR_ID"),
    F.col("MEME_CK").alias("MBR_UNIQ_KEY"),
    F.col("MEME_SFX").alias("MEME_SFX"),
    F.col("MEME_REL").alias("MEME_REL"),
    F.col("MBR_ID").alias("MBR_ID_F"),
    F.col("SUB_ID").alias("SUB_ID_F"),
    F.col("MEME_BIRTH_DT").alias("MEME_BIRTH_DT")
)

# Stage: Jn_MBR_UNIQ_KEY (PxJoin) - innerjoin on MBR_UNIQ_KEY
df_join_Jn_MBR_UNIQ_KEY = df_BCBSA_MBR_CRSWALK_xfm.alias("To_MBR_UNIQ_KEY_Jn").join(
    df_db2_MBR.alias("From_db2_MBR"),
    F.col("To_MBR_UNIQ_KEY_Jn.MBR_UNIQ_KEY") == F.col("From_db2_MBR.MBR_UNIQ_KEY"),
    "inner"
)
df_Jn_MBR_UNIQ_KEY_out = df_join_Jn_MBR_UNIQ_KEY.select(
    F.col("From_db2_MBR.GRP_SK").alias("GRP_SK"),
    F.col("From_db2_MBR.SUB_SK").alias("SUB_SK"),
    F.col("From_db2_MBR.SUB_ID").alias("SUB_ID"),
    F.col("From_db2_MBR.MBR_SK").alias("MBR_SK"),
    F.col("From_db2_MBR.MBR_ID").alias("MBR_ID"),
    F.col("From_db2_MBR.FIRST_NM").alias("FIRST_NM"),
    F.col("From_db2_MBR.MIDINIT").alias("MIDINIT"),
    F.col("From_db2_MBR.LAST_NM").alias("LAST_NM"),
    F.col("From_db2_MBR.MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.col("From_db2_MBR.INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("To_MBR_UNIQ_KEY_Jn.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("To_MBR_UNIQ_KEY_Jn.BCBSA_MBR_CRSWALK_ID").alias("BCBSA_MBR_CRSWALK_ID"),
    F.col("To_MBR_UNIQ_KEY_Jn.GRGR_ID").alias("GRGR_ID"),
    F.col("To_MBR_UNIQ_KEY_Jn.BCBSA_HOME_PLN_ID").alias("BCBSA_HOME_PLN_ID"),
    F.col("To_MBR_UNIQ_KEY_Jn.BCBSA_HOME_PLN_CONSIS_MBR_ID").alias("BCBSA_HOME_PLN_CONSIS_MBR_ID"),
    F.col("To_MBR_UNIQ_KEY_Jn.BCBSA_ITS_SUB_ID").alias("BCBSA_ITS_SUB_ID"),
    F.col("To_MBR_UNIQ_KEY_Jn.PRIOR_HOME_PLN_CONSIS_MBR_ID").alias("PRIOR_HOME_PLN_CONSIS_MBR_ID"),
    F.col("To_MBR_UNIQ_KEY_Jn.MEME_SFX").alias("MEME_SFX"),
    F.col("To_MBR_UNIQ_KEY_Jn.MEME_REL").alias("MEME_REL"),
    F.col("To_MBR_UNIQ_KEY_Jn.MBR_ID_F").alias("MBR_ID_F"),
    F.col("To_MBR_UNIQ_KEY_Jn.SUB_ID_F").alias("SUB_ID_F"),
    F.col("From_db2_MBR.BRTH_DT_SK").alias("BRTH_DT_SK")
)

# Stage: BusinessRules (CTransformerStage)
df_BusinessRules = (
  df_Jn_MBR_UNIQ_KEY_out
  .withColumn("BCBSA_MBR_CRSWALK_ID", trim(F.col("BCBSA_MBR_CRSWALK_ID")))
  .withColumn("GRP_ID", trim(F.col("GRGR_ID")))
  .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(CurrRunCycle))
  .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(CurrRunCycle))
  .withColumn("BCBSA_HOME_PLN_ID", trim(F.col("BCBSA_HOME_PLN_ID")))
  .withColumn("BCBSA_HOME_PLN_CONSIS_MBR_ID", trim(F.col("BCBSA_HOME_PLN_CONSIS_MBR_ID")))
  .withColumn("BCBSA_ITS_SUB_ID", trim(F.col("BCBSA_ITS_SUB_ID")))
  .withColumn(
      "PRIOR_BCBSA_HOME_PLN_CONSIS_MBR_ID",
      F.when(
          trim(
              F.when(F.col("PRIOR_HOME_PLN_CONSIS_MBR_ID").isNotNull(), F.col("PRIOR_HOME_PLN_CONSIS_MBR_ID")).otherwise("")
          )
          == "",
          "NA"
      ).otherwise(
          trim(
              F.when(F.col("PRIOR_HOME_PLN_CONSIS_MBR_ID").isNotNull(), F.col("PRIOR_HOME_PLN_CONSIS_MBR_ID")).otherwise("")
          )
      )
  )
  .withColumn(
      "GRP_SK",
      F.when(
          F.when(F.col("GRP_SK").isNotNull(), F.col("GRP_SK")).otherwise(lit(0)).eqNullSafe(0),
          lit(0)
      ).otherwise(F.col("GRP_SK"))
  )
  .withColumn(
      "SUB_SK",
      F.when(
          F.when(F.col("SUB_SK").isNotNull(), F.col("SUB_SK")).otherwise(lit(0)).eqNullSafe(0),
          lit(0)
      ).otherwise(F.col("SUB_SK"))
  )
  .withColumn(
      "SUB_ID",
      F.when(
          trim(
              F.when(F.col("SUB_ID").isNotNull(), F.col("SUB_ID")).otherwise("")
          )
          == "",
          "UNK"
      ).otherwise(
          trim(F.col("SUB_ID"))
      )
  )
  .withColumn(
      "MBR_SK",
      F.when(
          F.when(F.col("MBR_SK").isNotNull(), F.col("MBR_SK")).otherwise(lit(0)).eqNullSafe(0),
          lit(0)
      ).otherwise(F.col("MBR_SK"))
  )
  .withColumn(
      "MBR_ID",
      F.when(
          trim(
              F.when(F.col("MBR_ID").isNotNull(), F.col("MBR_ID")).otherwise("")
          )
          == "",
          "UNK"
      ).otherwise(
          trim(F.col("MBR_ID"))
      )
  )
  .withColumn("MBR_BRTH_DT_SK", F.col("BRTH_DT_SK"))
  .withColumn(
      "MBR_FIRST_NM",
      F.when(
          trim(
              F.when(F.col("FIRST_NM").isNotNull(), F.col("FIRST_NM")).otherwise("")
          )
          == "",
          "UNK"
      ).otherwise(trim(F.col("FIRST_NM")))
  )
  .withColumn(
      "MBR_MIDINIT",
      F.when(
          trim(
              F.when(F.col("MIDINIT").isNotNull(), F.col("MIDINIT")).otherwise("")
          )
          == "",
          F.lit(None)
      ).otherwise(trim(F.col("MIDINIT")))
  )
  .withColumn(
      "MBR_LAST_NM",
      F.when(
          trim(
              F.when(F.col("LAST_NM").isNotNull(), F.col("LAST_NM")).otherwise("")
          )
          == "",
          "UNK"
      ).otherwise(trim(F.col("LAST_NM")))
  )
  .withColumn(
      "MBR_SFX_NO",
      F.when(
          trim(
              F.when(F.col("MBR_SFX_NO").isNotNull(), F.col("MBR_SFX_NO")).otherwise("")
          )
          == "",
          "00"
      ).otherwise(trim(F.col("MBR_SFX_NO")))
  )
  .withColumn("MBR_UNIQ_KEY", F.col("MBR_UNIQ_KEY"))
  .withColumn(
      "INDV_BE_KEY",
      F.when(
          F.when(F.col("INDV_BE_KEY").isNotNull(), F.col("INDV_BE_KEY")).otherwise(lit(0)).eqNullSafe(0),
          lit(0)
      ).otherwise(F.col("INDV_BE_KEY"))
  )
)

# Final select with rpad for char/varchar columns in the final output
df_final = df_BusinessRules.select(
    rpad(F.col("BCBSA_MBR_CRSWALK_ID"), 9, " ").alias("BCBSA_MBR_CRSWALK_ID"),
    rpad(F.col("GRP_ID"), 8, " ").alias("GRP_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(F.col("BCBSA_HOME_PLN_ID"), 3, " ").alias("BCBSA_HOME_PLN_ID"),
    rpad(F.col("BCBSA_HOME_PLN_CONSIS_MBR_ID"), 22, " ").alias("BCBSA_HOME_PLN_CONSIS_MBR_ID"),
    rpad(F.col("BCBSA_ITS_SUB_ID"), 17, " ").alias("BCBSA_ITS_SUB_ID"),
    rpad(F.col("PRIOR_BCBSA_HOME_PLN_CONSIS_MBR_ID"), 22, " ").alias("PRIOR_BCBSA_HOME_PLN_CONSIS_MBR_ID"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("SUB_SK").alias("SUB_SK"),
    F.col("SUB_ID").alias("SUB_ID"),
    F.col("MBR_SK").alias("MBR_SK"),
    rpad(F.col("MBR_ID"), 20, " ").alias("MBR_ID"),
    rpad(F.col("MBR_BRTH_DT_SK"), 10, " ").alias("MBR_BRTH_DT_SK"),
    F.col("MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    rpad(F.col("MBR_MIDINIT"), 1, " ").alias("MBR_MIDINIT"),
    F.col("MBR_LAST_NM").alias("MBR_LAST_NM"),
    rpad(F.col("MBR_SFX_NO"), 2, " ").alias("MBR_SFX_NO"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("INDV_BE_KEY").alias("INDV_BE_KEY")
)

# Stage: Seq_P_BCBSA_MBR_CRSWALK (PxSequentialFile) - write the final data
write_files(
    df_final,
    f"{adls_path}/load/P_BCBSA_MBR_CRSWALK.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)