# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2011 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY: IdsExtrnlUserExtrSeq
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #               Change Description                             Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                  02/09/2011      4574                                Originally Programmed                          IntegrateNewDevl           Steph Goddard           02/11/2011
# MAGIC 
# MAGIC Akhila Manickavelu          11/16/2016      5628 - Workers comp   Added condition to exlude the 
# MAGIC                                                                                                             workers comp data to warehouse               IntegrateDevl        Kalyan Neelam           2016-11-17      
# MAGIC                                                                                                   Added join with Facets P_SEL_PRCS_CRITR  
# MAGIC 
# MAGIC Anoop Nair                2022-03-08         S2S Remediation      Added FACETS DSN Connection parameters      IntegrateDev5

# MAGIC Strip Carriage Return, Line Feed, and Tab.
# MAGIC Extract Facets Extrnl Enty Data
# MAGIC Apply business logic
# MAGIC Assign primary surrogate key
# MAGIC Writing Sequential File to /pkey
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


facetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
bcbsOwner = get_widget_value('BCBSOwner','')
bcbs_secret_name = get_widget_value('bcbs_secret_name','')
ids_secret_name = get_widget_value('ids_secret_name','')

# 1) Read from dummy table replacing hashed file hf_extrnl_user (Scenario B)
jdbc_url_hf_extrnl_user, jdbc_props_hf_extrnl_user = get_db_config(ids_secret_name)
df_hf_extrnl_user = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_hf_extrnl_user)
    .options(**jdbc_props_hf_extrnl_user)
    .option(
        "query",
        "SELECT SRC_SYS_CD, EXTRNL_USER_CNSTTNT_TYP_CD, EXTRNL_USER_FCTS_ID, GRP_ID, MBR_SFX_NO, CRT_RUN_CYC_EXCTN_SK, EXTRNL_USER_SK "
        "FROM IDS.dummy_hf_extrnl_user"
    )
    .load()
)

# 2) Read from Facets (ODBC Connector)
jdbc_url_Facets, jdbc_props_Facets = get_db_config(facets_secret_name)
extract_query = (
    f"SELECT EXTU_CONSTITUENT,EXTE_ID,GRGR_ID,MEME_SFX,EXTU_ID,MEME_CK,EXTE_STS,EXTE_CREATE_DTM,EXTE_PROCESS_DTM "
    f"FROM {facetsOwner}.ER_TB_SYST_EXTE_EUSER ER_TB_SYST_EXTE_EUSER "
    f"WHERE NOT EXISTS ("
    f"SELECT DISTINCT CMC_GRGR.GRGR_CK "
    f"FROM {bcbsOwner}.P_SEL_PRCS_CRITR P_SEL_PRCS_CRITR,"
    f"{facetsOwner}.CMC_GRGR_GROUP CMC_GRGR "
    f"WHERE P_SEL_PRCS_CRITR.SEL_PRCS_ID = 'WRHSINBEXCL' "
    f"AND P_SEL_PRCS_CRITR.SEL_PRCS_ITEM_ID = 'WORKCOMP' "
    f"AND P_SEL_PRCS_CRITR.SEL_PRCS_CRITR_CD = 'MEDPROC' "
    f"AND CMC_GRGR.GRGR_ID >= P_SEL_PRCS_CRITR.CRITR_VAL_FROM_TX "
    f"AND CMC_GRGR.GRGR_ID <= P_SEL_PRCS_CRITR.CRITR_VAL_THRU_TX "
    f"AND CMC_GRGR.GRGR_ID=ER_TB_SYST_EXTE_EUSER.GRGR_ID)"
)
df_Facets = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_Facets)
    .options(**jdbc_props_Facets)
    .option("query", extract_query)
    .load()
)

# 3) StripFields Transformer
#    Stage variable: svExteId = Ereplace(trim(EXTE_ID), ' ', '$')
df_StripFields = (
    df_Facets
    .withColumn("svExteId", Ereplace(trim(F.col("EXTE_ID")), F.lit(" "), F.lit("$")))
    .select(
        F.col("EXTU_CONSTITUENT").alias("EXTU_CONSTITUENT_OUT")
            .alias("EXTU_CONSTITUENT"),
        F.col("svExteId").alias("EXTE_ID"),
        F.col("GRGR_ID").alias("GRGR_ID"),
        F.col("MEME_SFX").alias("MEME_SFX"),
        F.col("EXTU_ID").alias("EXTU_ID"),
        F.col("MEME_CK").alias("MEME_CK"),
        trim(F.col("EXTE_STS")).alias("EXTE_STS"),
        F.col("EXTE_CREATE_DTM").alias("EXTE_CREATE_DTM"),
        F.col("EXTE_PROCESS_DTM").alias("EXTE_PROCESS_DTM"),
    )
)

# 4) BusinessRules Transformer
#    Filter constraint: Index(EXTE_ID, '$', 1) <> 1
#    Output columns with expressions or constants
df_BusinessRules_pre = df_StripFields.filter(Index(F.col("EXTE_ID"), F.lit("$"), F.lit(1)) != 1)

df_BusinessRules = (
    df_BusinessRules_pre
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.lit("Y"))
    .withColumn("FIRST_RECYC_DT", F.lit(CurrentDate))
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
    .withColumn(
        "PRI_KEY_STRING",
        F.concat_ws(";", F.lit(SrcSysCd),
                    F.col("EXTU_CONSTITUENT"),
                    F.col("EXTE_ID"),
                    F.col("GRGR_ID"),
                    F.col("MEME_SFX"))
    )
    .withColumn("EXTRNL_USER_SK", F.lit(0))
    .withColumn("EXTRNL_USER_CNSTTNT_TYP_CD", F.col("EXTU_CONSTITUENT"))
    .withColumn(
        "EXTRNL_USER_FCTS_ID",
        F.when(F.isnull(F.col("EXTE_ID")) | (F.length(F.col("EXTE_ID")) == 0), F.lit("NA"))
         .otherwise(F.col("EXTE_ID"))
    )
    .withColumn(
        "GRP_ID",
        F.when(
            (F.col("EXTU_CONSTITUENT") == "MEMBER") | (F.col("EXTU_CONSTITUENT") == "GROUP"),
            F.col("GRGR_ID")
        ).otherwise(F.lit("NA"))
    )
    .withColumn("MBR_SFX_NO", F.rpad(F.right(F.concat(F.lit("0"), F.col("MEME_SFX")), 2), 2, " "))
    .withColumn("SRC_SYS_CD_SK", F.lit(0))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn(
        "AGNT_SK",
        F.when(F.col("EXTU_CONSTITUENT") == "BROKER", F.col("EXTE_ID")).otherwise(F.lit("NA"))
    )
    .withColumn(
        "GRP_SK",
        F.when(F.isnull(F.col("GRGR_ID")) | (F.length(F.col("GRGR_ID")) == 0), F.lit("NA"))
         .otherwise(F.col("GRGR_ID"))
    )
    .withColumn(
        "MBR_SK",
        F.when(F.col("EXTU_CONSTITUENT") == "MEMBER", F.col("MEME_CK")).otherwise(F.lit("NA"))
    )
    .withColumn(
        "PROV_SK",
        F.when(
            (F.col("EXTU_CONSTITUENT") == "PROVIDER")
            | (F.col("EXTU_CONSTITUENT") == "PROVGRP")
            | (F.col("EXTU_CONSTITUENT") == "PROVIPA"),
            F.col("EXTE_ID")
        ).otherwise(F.lit("NA"))
    )
    .withColumn("EXTRNL_USER_CNSTTNT_TYP_CD_SK", F.col("EXTU_CONSTITUENT"))
    .withColumn("STTUS_CD_SK", F.col("EXTE_STS"))
    .withColumn(
        "SRC_SYS_CRT_DTM",
        FORMAT.DATE(F.col("EXTE_CREATE_DTM"), F.lit("SYBASE"), F.lit("TIMESTAMP"), F.lit("DB2TIMESTAMP"))
    )
    .withColumn(
        "SRC_SYS_PRCS_DTM",
        FORMAT.DATE(F.col("EXTE_PROCESS_DTM"), F.lit("SYBASE"), F.lit("TIMESTAMP"), F.lit("DB2TIMESTAMP"))
    )
    .withColumn(
        "MBR_UNIQ_KEY",
        F.when(F.col("EXTU_CONSTITUENT") == "MEMBER", F.col("MEME_CK")).otherwise(F.lit("1"))
    )
    .withColumn(
        "EXTRNL_USER_ID",
        F.when(F.isnull(F.col("EXTU_ID")) | (F.length(F.col("EXTU_ID")) == 0), F.lit("NA"))
         .otherwise(F.col("EXTU_ID"))
    )
)

# 5) PrimaryKey Transformer: left join with df_hf_extrnl_user
df_PrimaryKey_pre = (
    df_BusinessRules.alias("Transform")
    .join(
        df_hf_extrnl_user.alias("lkup"),
        on=[
            F.col("Transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD"),
            F.col("Transform.EXTRNL_USER_CNSTTNT_TYP_CD") == F.col("lkup.EXTRNL_USER_CNSTTNT_TYP_CD"),
            F.col("Transform.EXTRNL_USER_FCTS_ID") == F.col("lkup.EXTRNL_USER_FCTS_ID"),
            F.col("Transform.GRP_ID") == F.col("lkup.GRP_ID"),
            F.col("Transform.MBR_SFX_NO") == F.col("lkup.MBR_SFX_NO"),
        ],
        how="left"
    )
)

df_PrimaryKey_vars = (
    df_PrimaryKey_pre
    .withColumn(
        "NewCurrRunCycExtcnSk",
        F.when(F.isnull(F.col("lkup.EXTRNL_USER_SK")), F.lit(CurrRunCycle))
         .otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK"))
    )
    .withColumn("EXTRNL_USER_SK_temp", F.col("lkup.EXTRNL_USER_SK"))
)

# 5a) We now map "Sk" via SurrogateKeyGen.  First create df_enriched with a placeholder "EXTRNL_USER_SK" = None if lkup is null else lkup
df_enriched = (
    df_PrimaryKey_vars
    .withColumn(
        "EXTRNL_USER_SK",
        F.when(F.isnull(F.col("EXTRNL_USER_SK_temp")), F.lit(None)).otherwise(F.col("EXTRNL_USER_SK_temp"))
    )
)

# 5b) SurrogateKeyGen to fill missing EXTRNL_USER_SK
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"EXTRNL_USER_SK",<schema>,<secret_name>)

# 6) Prepare final "Key" link DataFrame (all rows)
df_Key = (
    df_enriched
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.col("Transform.JOB_EXCTN_RCRD_ERR_SK"))
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("Transform.INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("Transform.DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("Transform.PASS_THRU_IN"), 1, " "))
    .withColumn("FIRST_RECYC_DT", F.col("Transform.FIRST_RECYC_DT"))
    .withColumn("ERR_CT", F.col("Transform.ERR_CT"))
    .withColumn("RECYCLE_CT", F.col("Transform.RECYCLE_CT"))
    .withColumn("SRC_SYS_CD", F.col("Transform.SRC_SYS_CD"))
    .withColumn("PRI_KEY_STRING", F.col("Transform.PRI_KEY_STRING"))
    .withColumn("EXTRNL_USER_SK", F.col("EXTRNL_USER_SK"))
    .withColumn("EXTRNL_USER_CNSTTNT_TYP_CD", F.col("Transform.EXTRNL_USER_CNSTTNT_TYP_CD"))
    .withColumn("EXTRNL_USER_FCTS_ID", Ereplace(F.col("Transform.EXTRNL_USER_FCTS_ID"), F.lit("$"), F.lit(" ")))
    .withColumn("GRP_ID", F.col("Transform.GRP_ID"))
    .withColumn("MBR_SFX_NO", F.rpad(F.col("Transform.MBR_SFX_NO"), 2, " "))
    .withColumn("SRC_SYS_CD_SK", F.col("Transform.SRC_SYS_CD_SK"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.col("NewCurrRunCycExtcnSk").cast(IntegerType()))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle).cast(IntegerType()))
    .withColumn("AGNT_SK", F.rpad(F.col("Transform.AGNT_SK"), 10, " "))
    .withColumn("GRP_SK", F.rpad(F.col("Transform.GRP_SK"), 10, " "))
    .withColumn("MBR_SK", F.rpad(F.col("Transform.MBR_SK"), 10, " "))
    .withColumn("PROV_SK", F.rpad(F.col("Transform.PROV_SK"), 10, " "))
    .withColumn("EXTRNL_USER_CNSTTNT_TYP_CD_SK", F.rpad(F.col("Transform.EXTRNL_USER_CNSTTNT_TYP_CD_SK"), 10, " "))
    .withColumn("STTUS_CD_SK", F.rpad(F.col("Transform.STTUS_CD_SK"), 10, " "))
    .withColumn("SRC_SYS_CRT_DTM", F.col("Transform.SRC_SYS_CRT_DTM"))
    .withColumn("SRC_SYS_PRCS_DTM", F.col("Transform.SRC_SYS_PRCS_DTM"))
    .withColumn("MBR_UNIQ_KEY", F.col("Transform.MBR_UNIQ_KEY"))
    .withColumn("EXTRNL_USER_ID", F.col("Transform.EXTRNL_USER_ID"))
    .select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "EXTRNL_USER_SK",
        "EXTRNL_USER_CNSTTNT_TYP_CD",
        "EXTRNL_USER_FCTS_ID",
        "GRP_ID",
        "MBR_SFX_NO",
        "SRC_SYS_CD_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "AGNT_SK",
        "GRP_SK",
        "MBR_SK",
        "PROV_SK",
        "EXTRNL_USER_CNSTTNT_TYP_CD_SK",
        "STTUS_CD_SK",
        "SRC_SYS_CRT_DTM",
        "SRC_SYS_PRCS_DTM",
        "MBR_UNIQ_KEY",
        "EXTRNL_USER_ID"
    )
)

# 7) Write to IdsExtrnlUserExtr (CSeqFileStage)
write_files(
    df_Key,
    f"{adls_path}/key/FctsExtrnlUser.ExtrnlUser.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# 8) hf_extrnl_user_updt link = rows where lkup.EXTRNL_USER_SK is null => prepare for upsert to dummy table
df_updt = df_enriched.filter(F.isnull(F.col("EXTRNL_USER_SK_temp"))).select(
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.EXTRNL_USER_CNSTTNT_TYP_CD").alias("EXTRNL_USER_CNSTTNT_TYP_CD"),
    F.col("Transform.EXTRNL_USER_FCTS_ID").alias("EXTRNL_USER_FCTS_ID"),
    F.col("Transform.GRP_ID").alias("GRP_ID"),
    F.rpad(F.col("Transform.MBR_SFX_NO"), 2, " ").alias("MBR_SFX_NO"),
    F.lit(CurrRunCycle).cast(IntegerType()).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("EXTRNL_USER_SK").alias("EXTRNL_USER_SK")
)

# 9) Merge df_updt into "IDS.dummy_hf_extrnl_user"
merge_temp_table = "STAGING.FctsExtrnlUserExtr_hf_extrnl_user_updt_temp"
drop_ddl = f"DROP TABLE IF EXISTS {merge_temp_table}"
execute_dml(drop_ddl, jdbc_url_hf_extrnl_user, jdbc_props_hf_extrnl_user)

df_updt.write \
    .format("jdbc") \
    .option("url", jdbc_url_hf_extrnl_user) \
    .options(**jdbc_props_hf_extrnl_user) \
    .option("dbtable", merge_temp_table) \
    .mode("overwrite") \
    .save()

merge_sql = (
    f"MERGE INTO IDS.dummy_hf_extrnl_user AS t "
    f"USING {merge_temp_table} AS s "
    f"ON (t.SRC_SYS_CD = s.SRC_SYS_CD "
    f"AND t.EXTRNL_USER_CNSTTNT_TYP_CD = s.EXTRNL_USER_CNSTTNT_TYP_CD "
    f"AND t.EXTRNL_USER_FCTS_ID = s.EXTRNL_USER_FCTS_ID "
    f"AND t.GRP_ID = s.GRP_ID "
    f"AND t.MBR_SFX_NO = s.MBR_SFX_NO) "
    f"WHEN MATCHED THEN UPDATE SET "
    f"  t.CRT_RUN_CYC_EXCTN_SK = s.CRT_RUN_CYC_EXCTN_SK, "
    f"  t.EXTRNL_USER_SK = s.EXTRNL_USER_SK "
    f"WHEN NOT MATCHED THEN INSERT ("
    f"  SRC_SYS_CD, EXTRNL_USER_CNSTTNT_TYP_CD, EXTRNL_USER_FCTS_ID, GRP_ID, MBR_SFX_NO, CRT_RUN_CYC_EXCTN_SK, EXTRNL_USER_SK"
    f") VALUES ("
    f"  s.SRC_SYS_CD, s.EXTRNL_USER_CNSTTNT_TYP_CD, s.EXTRNL_USER_FCTS_ID, s.GRP_ID, s.MBR_SFX_NO, s.CRT_RUN_CYC_EXCTN_SK, s.EXTRNL_USER_SK"
    f");"
)
execute_dml(merge_sql, jdbc_url_hf_extrnl_user, jdbc_props_hf_extrnl_user)