# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 06/12/08 10:08:56 Batch  14774_36805 PROMOTE bckcetl ids20 dsadm bls for lk
# MAGIC ^1_1 06/12/08 09:53:05 Batch  14774_35590 INIT bckcett testIDS dsadm BLS FOR LK
# MAGIC ^1_2 06/02/08 20:20:40 Batch  14764_73245 PROMOTE bckcett testIDS u03651 steph for Laurel
# MAGIC ^1_2 06/02/08 20:14:00 Batch  14764_72849 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_1 05/25/08 15:25:52 Batch  14756_55555 INIT bckcett devlIDS u03651 steffy
# MAGIC 
# MAGIC Â© Copyright 2007 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  <Sequencer Name>
# MAGIC                
# MAGIC 
# MAGIC PROCESSING:   <Tell what is not obvious or may be difficult to understand>
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 \(9)Project/Altiris #\(9)Change Description\(9)\(9)\(9)\(9)Development Project\(9)Code Reviewer\(9)Date Reviewed       
# MAGIC ------------------              --------------------     \(9)------------------------\(9)-----------------------------------------------------------------------\(9)--------------------------------\(9)-------------------------------\(9)----------------------------       
# MAGIC Caveman\(9)\(9)2007-MM-DD\(9)\(9)\(9)Original Programming

# MAGIC Find all members who are associated via the sub_sk to fep members.
# MAGIC P_CAE_MBR_PRMCY into P_CAE_MBR_DRVR
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
CurrDate = get_widget_value("CurrDate","")

# --------------------------------------------------------------------------------
# Stage: IDS (DB2Connector) - Multiple output pins
# --------------------------------------------------------------------------------
jdbc_url_IDS, jdbc_props_IDS = get_db_config(ids_secret_name)

query_IDS_Extract = (
    "SELECT P_CAE_MBR_PRMCY.MBR_SK as MBR_SK,"
    "P_CAE_MBR_PRMCY.CAE_UNIQ_KEY as CAE_UNIQ_KEY,"
    "P_CAE_MBR_PRMCY.INDV_BE_KEY as INDV_BE_KEY,"
    "P_CAE_MBR_PRMCY.PRI_IN as PRI_IN,"
    "P_CAE_MBR_PRMCY.GRP_SK as GRP_SK,"
    "P_CAE_MBR_PRMCY.MBR_BRTH_DT as MBR_BRTH_DT,"
    "P_CAE_MBR_PRMCY.MBR_GNDR_CD as MBR_GNDR_CD,"
    "P_CAE_MBR_PRMCY.LAST_UPDT_DT as LAST_UPDT_DT,"
    "MBR.SUB_SK as SUB_SK,"
    "GRP.GRP_ID as GRP_ID "
    "FROM " + IDSOwner + ".p_cae_mbr_prmcy P_CAE_MBR_PRMCY, "
    + IDSOwner + ".mbr MBR, "
    + IDSOwner + ".grp GRP "
    "WHERE P_CAE_MBR_PRMCY.MBR_SK = MBR.MBR_SK AND "
    "P_CAE_MBR_PRMCY.GRP_SK = GRP.GRP_SK"
)
df_IDS_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", query_IDS_Extract)
    .load()
)

query_fep_lkup = (
    "SELECT MBR.SUB_SK as SUB_SK "
    "FROM " + IDSOwner + ".p_cae_mbr_prmcy P_CAE_MBR_PRMCY, "
    + IDSOwner + ".grp GRP, "
    + IDSOwner + ".mbr MBR "
    "WHERE P_CAE_MBR_PRMCY.GRP_SK = GRP.GRP_SK AND "
    "P_CAE_MBR_PRMCY.MBR_SK = MBR.MBR_SK AND "
    "GRP.GRP_ID = '10023000' AND "
    "MBR.CONF_COMM_IN = 'Y'"
)
df_fep_lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", query_fep_lkup)
    .load()
)

query_cci_lkup = (
    "SELECT MBR.INDV_BE_KEY as INDV_BE_KEY "
    "FROM " + IDSOwner + ".p_cae_mbr_prmcy P_CAE_MBR_PRMCY, "
    + IDSOwner + ".mbr MBR "
    "WHERE P_CAE_MBR_PRMCY.MBR_SK = MBR.MBR_SK AND "
    "MBR.CONF_COMM_IN = 'Y'"
)
df_cci_lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", query_cci_lkup)
    .load()
)

query_cob_out = (
    "SELECT "
    "P_CAE_MBR_PRMCY.INDV_BE_KEY, "
    "MBR.MBR_SK "
    "FROM "
    + IDSOwner + ".p_cae_mbr_prmcy P_CAE_MBR_PRMCY, "
    + IDSOwner + ".grp GRP, "
    + IDSOwner + ".mbr_cob MBR_COB, "
    + IDSOwner + ".cd_mppng CD_MPPNG, "
    + IDSOwner + ".cd_mppng CD_MPPNG1, "
    + IDSOwner + ".mbr MBR "
    "WHERE "
    "P_CAE_MBR_PRMCY.MBR_SK = MBR_COB.MBR_SK AND "
    "P_CAE_MBR_PRMCY.GRP_SK = GRP.GRP_SK AND "
    "MBR_COB.MBR_COB_PAYMT_PRTY_CD_SK = CD_MPPNG.CD_MPPNG_SK AND "
    "MBR_COB.MBR_COB_TYP_CD_SK = CD_MPPNG1.CD_MPPNG_SK AND "
    "GRP.GRP_ID = '10023000' AND "
    "CD_MPPNG.TRGT_CD = 'PRI' AND "
    "CD_MPPNG1.TRGT_CD IN ('MCARE','MCARED') AND "
    "MBR_COB.EFF_DT_SK <= '" + CurrDate + "' AND "
    "MBR_COB.TERM_DT_SK >= '" + CurrDate + "' AND "
    "P_CAE_MBR_PRMCY.INDV_BE_KEY = MBR.INDV_BE_KEY"
)
df_cob_out = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", query_cob_out)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: COB_Business_Rules (CTransformerStage)
#  Outputs: "bekey" => INDV_BE_KEY, "mbrsk" => MBR_SK
# --------------------------------------------------------------------------------
df_bekey = df_cob_out.select(F.col("INDV_BE_KEY").alias("INDV_BE_KEY"))
df_mbrsk = df_cob_out.select(F.col("MBR_SK").alias("MBR_SK"))

# --------------------------------------------------------------------------------
# Stage: hf_ids_cae_fep_cob_lkup (CHashedFileStage) - Scenario A
#  We deduplicate each input link on its key and produce separate outputs:
#   "cob_bekey" => key: INDV_BE_KEY
#   "cob_mbrsk" => key: MBR_SK
#   "cci_mbr"   => key: INDV_BE_KEY (input from df_cci_lkup)
# --------------------------------------------------------------------------------
df_cob_bekey = df_bekey.dropDuplicates(["INDV_BE_KEY"])
df_cob_mbrsk = df_mbrsk.dropDuplicates(["MBR_SK"])
df_cci_mbr = df_cci_lkup.dropDuplicates(["INDV_BE_KEY"])

# --------------------------------------------------------------------------------
# Stage: Copy_of_IDS (DB2Connector)
# --------------------------------------------------------------------------------
query_Copy_of_IDS = (
    "SELECT MBR.MBR_SK as MBR_SK, "
    "MBR.SUB_SK as SUB_SK, "
    "MBR.CONF_COMM_IN as CONF_COMM_IN "
    "FROM " + IDSOwner + ".p_cae_mbr_prmcy P_CAE_MBR_PRMCY, "
    + IDSOwner + ".mbr MBR "
    "WHERE P_CAE_MBR_PRMCY.MBR_SK = MBR.MBR_SK"
)
df_Copy_of_IDS_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", query_Copy_of_IDS)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: hf_ids_fep_sub_lkup (CHashedFileStage) - Scenario A
#  Input: df_fep_lkup (SUB_SK)
#  Output: fep_sub_lkup => deduplicate on SUB_SK
# --------------------------------------------------------------------------------
df_fep_sub_lkup = df_fep_lkup.dropDuplicates(["SUB_SK"])

# --------------------------------------------------------------------------------
# Stage: Transformer_351 (CTransformerStage)
#   Left join: df_Copy_of_IDS_Extract => df_fep_sub_lkup on SUB_SK
#   Constraint: IsNull(fep_sub_lkup.SUB_SK) = @FALSE
#   Output: fep_sub_cci_out => columns: MBR_SK
# --------------------------------------------------------------------------------
df_351_join = df_Copy_of_IDS_Extract.alias("IDS_Extract").join(
    df_fep_sub_lkup.alias("fep_sub_lkup"),
    F.col("IDS_Extract.SUB_SK") == F.col("fep_sub_lkup.SUB_SK"),
    "left"
)
df_fep_sub_cci_out = df_351_join.filter(
    F.col("fep_sub_lkup.SUB_SK").isNotNull()
).select(
    F.col("IDS_Extract.MBR_SK").alias("MBR_SK")
)

# --------------------------------------------------------------------------------
# Stage: hf_ids_cae_fep_mbr_lkup (CHashedFileStage) - Scenario A
#  Input: df_fep_sub_cci_out => deduplicate on MBR_SK
#  Output: fep_sub_cci => MBR_SK
# --------------------------------------------------------------------------------
df_fep_sub_cci = df_fep_sub_cci_out.dropDuplicates(["MBR_SK"])

# --------------------------------------------------------------------------------
# Stage: FilterFep (CTransformerStage)
#   Primary link: df_IDS_Extract
#   Lookups (all left):
#       cob_bekey => df_cob_bekey on (INDV_BE_KEY)
#       cob_mbrsk => df_cob_mbrsk on (MBR_SK)
#       cci_mbr   => df_cci_mbr on (INDV_BE_KEY)
#       fep_sub_cci => df_fep_sub_cci on (MBR_SK)
#   StageVar: svFEP = If GRP_ID <> '10023000' and fep_sub_cci.MBR_SK=null and cob_mbrsk.MBR_SK=null then 'N' else 'Y'
#   Two outputs: non_fep (svFEP='N'), fep_out (svFEP='Y')
# --------------------------------------------------------------------------------
df_filter_join_1 = df_IDS_Extract.alias("IDS_Extract").join(
    df_cob_bekey.alias("cob_bekey"),
    F.col("IDS_Extract.INDV_BE_KEY") == F.col("cob_bekey.INDV_BE_KEY"),
    "left"
)
df_filter_join_2 = df_filter_join_1.join(
    df_cob_mbrsk.alias("cob_mbrsk"),
    F.col("IDS_Extract.MBR_SK") == F.col("cob_mbrsk.MBR_SK"),
    "left"
)
df_filter_join_3 = df_filter_join_2.join(
    df_cci_mbr.alias("cci_mbr"),
    F.col("IDS_Extract.INDV_BE_KEY") == F.col("cci_mbr.INDV_BE_KEY"),
    "left"
)
df_filter_join_4 = df_filter_join_3.join(
    df_fep_sub_cci.alias("fep_sub_cci"),
    F.col("IDS_Extract.MBR_SK") == F.col("fep_sub_cci.MBR_SK"),
    "left"
)

df_filter_with_var = df_filter_join_4.withColumn(
    "svFEP",
    F.when(
        (F.col("IDS_Extract.GRP_ID") != "10023000")
        & F.col("fep_sub_cci.MBR_SK").isNull()
        & F.col("cob_mbrsk.MBR_SK").isNull(),
        F.lit("N")
    ).otherwise(F.lit("Y"))
)

# non_fep
df_non_fep_raw = df_filter_with_var.filter(F.col("svFEP") == "N").select(
    F.col("IDS_Extract.MBR_SK").alias("MBR_SK"),
    F.col("IDS_Extract.CAE_UNIQ_KEY").alias("CAE_UNIQ_KEY"),
    F.col("IDS_Extract.INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.when(F.col("cci_mbr.INDV_BE_KEY").isNull(), F.col("IDS_Extract.PRI_IN")).otherwise(F.lit("N")).alias("PRI_IN"),
    F.col("IDS_Extract.GRP_SK").alias("GRP_SK"),
    F.col("IDS_Extract.MBR_BRTH_DT").alias("MBR_BRTH_DT"),
    F.col("IDS_Extract.MBR_GNDR_CD").alias("MBR_GNDR_CD"),
    F.col("IDS_Extract.LAST_UPDT_DT").alias("LAST_UPDT_DT")
)

# fep_out - complicated PRI_IN expression
df_fep_out_raw = df_filter_with_var.filter(F.col("svFEP") == "Y").select(
    F.col("IDS_Extract.MBR_SK").alias("MBR_SK"),
    F.col("IDS_Extract.CAE_UNIQ_KEY").alias("CAE_UNIQ_KEY"),
    F.col("IDS_Extract.INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.when(
        F.col("fep_sub_cci.MBR_SK").isNull(),
        F.when(
            F.col("cci_mbr.INDV_BE_KEY").isNull(),
            F.when(
                F.col("cob_bekey.INDV_BE_KEY").isNull(),
                F.col("IDS_Extract.PRI_IN")
            ).otherwise(
                F.when(
                    F.col("cob_mbrsk.MBR_SK").isNull(),
                    F.lit("Y")
                ).otherwise(
                    F.when(
                        F.col("IDS_Extract.GRP_ID") == "10023000",
                        F.lit("Y")
                    ).otherwise(F.lit("N"))
                )
            )
        ).otherwise(F.lit("N"))
    ).otherwise(F.lit("N")).alias("PRI_IN"),
    F.col("IDS_Extract.GRP_SK").alias("GRP_SK"),
    F.col("IDS_Extract.MBR_BRTH_DT").alias("MBR_BRTH_DT"),
    F.col("IDS_Extract.MBR_GNDR_CD").alias("MBR_GNDR_CD"),
    F.col("IDS_Extract.LAST_UPDT_DT").alias("LAST_UPDT_DT")
)

df_non_fep = df_non_fep_raw.select(
    "MBR_SK",
    "CAE_UNIQ_KEY",
    "INDV_BE_KEY",
    "PRI_IN",
    "GRP_SK",
    "MBR_BRTH_DT",
    "MBR_GNDR_CD",
    "LAST_UPDT_DT"
)
df_fep_out = df_fep_out_raw.select(
    "MBR_SK",
    "CAE_UNIQ_KEY",
    "INDV_BE_KEY",
    "PRI_IN",
    "GRP_SK",
    "MBR_BRTH_DT",
    "MBR_GNDR_CD",
    "LAST_UPDT_DT"
)

df_link_collector = df_non_fep.unionByName(df_fep_out)

# --------------------------------------------------------------------------------
# Stage: Link_Collector_317 (CCollector)
#   Round-Robin => we simply union the inputs
#   Output: drvr_out
# --------------------------------------------------------------------------------
df_drvr_out = df_link_collector.select(
    "MBR_SK",
    "CAE_UNIQ_KEY",
    "INDV_BE_KEY",
    "PRI_IN",
    "GRP_SK",
    "MBR_BRTH_DT",
    "MBR_GNDR_CD",
    "LAST_UPDT_DT"
)

# --------------------------------------------------------------------------------
# Apply rpad for char columns (length specified)
# --------------------------------------------------------------------------------
df_final = (
    df_drvr_out
    .withColumn("PRI_IN", F.rpad(F.col("PRI_IN"), 1, " "))
    .withColumn("MBR_BRTH_DT", F.rpad(F.col("MBR_BRTH_DT"), 10, " "))
    .withColumn("LAST_UPDT_DT", F.rpad(F.col("LAST_UPDT_DT"), 10, " "))
)

# --------------------------------------------------------------------------------
# Stage: P_CAE_MBR_DRVR_dat (CSeqFileStage)
#   Write to .../load/P_CAE_MBR_DRVR.dat with delimiter ',' and quote '"'
# --------------------------------------------------------------------------------
write_files(
    df_final,
    f"{adls_path}/load/P_CAE_MBR_DRVR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)