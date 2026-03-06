# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2020 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  ErwinCdmaXMLTrans
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  Read Erwin standard XML and create CDMA XML for metadata input.
# MAGIC 
# MAGIC 
# MAGIC CALLED BY: ErwinXMLTranformCntl
# MAGIC 
# MAGIC 
# MAGIC HASH FILES:  None
# MAGIC 
# MAGIC 
# MAGIC RERUN INFORMATION: 
# MAGIC 
# MAGIC              PREVIOUS RUN SUCCESSFUL:   Nothing to reset. 
# MAGIC              PREVIOUS RUN ABORTED:         Nothing to reset, just restart.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer             Date                Project/Ticket #\(9)      Change Description\(9)\(9)\(9)Development Project   Code Reviewer\(9)      Date Reviewed       
# MAGIC ------------------            -------------------    ------------------------\(9)      -----------------------------------------------------------------------\(9)--------------------------------    -------------------------------   ---------------------------- 
# MAGIC Reddy Sanam       2019-11-01                                       Initial creation                                                     OutboundDev3                    
# MAGIC Reddy Sanam       2020-11-17                                       Added parameter ($APT_LUTCREATE_FIXEDBLOCK
# MAGIC                                                                                       to handle large table column count.

# MAGIC When a Table/View has multiple instance groups, consider only instance with \"HOMESUBJECTAREA\"
# MAGIC Erwin 2021
# MAGIC This section Parses the Table/View Names section(Entity Section) from Input XML and Forms the Table name section for CDMA XML
# MAGIC This section Parses the Column Names section(Attributes Section) from Input XML, Rolls up the columns at the table Level and creates XMLTag
# MAGIC This section loops through all the tables and creates a XML string at the Model Level. In the input tab, the table/view name is added for sorting to produce the output XML in sorted order
# MAGIC This section Parses the Instace groups section for Tables/views (A table/View can have multiple instance. So this section has been separated out from Tabls/View stream)
# MAGIC This File is  a complete Data Model Export from Erwin. New File is created by removing all the line endings except the last one, so the file can be read as a single file and single column. DataStage does not support more than 256MB in the column size
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, lit, expr, when, rpad
# COMMAND ----------
# MAGIC %run ../../../Utility_Outbound
# COMMAND ----------


# Parameter retrieval
APT_LUTCREATE_FIXEDBLOCK = get_widget_value('$APT_LUTCREATE_FIXEDBLOCK','3')
APT_TSORT_STRESS_BLOCKSIZE = get_widget_value('$APT_TSORT_STRESS_BLOCKSIZE','268435456')
APT_LATENCY_COEFFICIENT = get_widget_value('$APT_LATENCY_COEFFICIENT','3')
APT_DISABLE_COMBINATION = get_widget_value('$APT_DISABLE_COMBINATION','True')
APT_DEFAULT_TRANSPORT_BLOCK_SIZE = get_widget_value('$APT_DEFAULT_TRANSPORT_BLOCK_SIZE','268435456')
APT_MAX_TRANSPORT_BLOCK_SIZE = get_widget_value('$APT_MAX_TRANSPORT_BLOCK_SIZE','268435456')
APT_MAX_DELIMITED_READ_SIZE = get_widget_value('$APT_MAX_DELIMITED_READ_SIZE','268435456')
InputFilePath = get_widget_value('InputFilePath','/datamart3/dev3/temp')
OutPutFilePath = get_widget_value('OutPutFilePath','/datamart3/dev3/temp')
InputFileName = get_widget_value('InputFileName','IDS_ERwin.xml')
OutFileName = get_widget_value('OutFileName','XX_cdma.xml')

# Erwin_Exp_Xml (PxSequentialFile) - single column XmlFile
schema_Erwin_Exp_Xml = StructType([
    StructField("XmlFile", StringType(), False)
])
df_Erwin_Exp_Xml = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", '"')
    .schema(schema_Erwin_Exp_Xml)
    .load(f"{adls_path}{InputFilePath}/le1.xml")
)

# Parse_Xml (XMLInputPX): producing multiple outputs via assumed user-defined XML parser expressions
df_Parse_Xml_Table_Id = df_Erwin_Exp_Xml.selectExpr(
    "FromXMLFile_CDMAColumns(XmlFile).Modelid as Modelid",
    "FromXMLFile_CDMAColumns(XmlFile).id as id",
    "FromXMLFile_CDMAColumns(XmlFile).Entityname as Entityname",
    "FromXMLFile_CDMAColumns(XmlFile).Definition as Definition",
    "FromXMLFile_CDMAColumns(XmlFile).EntityPropsName as EntityPropsName",
    "FromXMLFile_CDMAColumns(XmlFile).EntityPropsType as EntityPropsType",
    "FromXMLFile_CDMAColumns(XmlFile).Comment as Comment",
    "FromXMLFile_CDMAColumns(XmlFile).User_Formatted_Physical_Name as User_Formatted_Physical_Name"
)
df_Parse_Xml_Model_Props_Id = df_Erwin_Exp_Xml.selectExpr(
    "FromXMLFile_CDMAColumns(XmlFile).Modelid as Modelid",
    "FromXMLFile_CDMAColumns(XmlFile).name as name",
    "FromXMLFile_CDMAColumns(XmlFile).Owner_PathReadOnly as Owner_PathReadOnly",
    "FromXMLFile_CDMAColumns(XmlFile).ModelPropsType as ModelPropsType",
    "FromXMLFile_CDMAColumns(XmlFile).Target_ServerReadOnly as Target_ServerReadOnly",
    "FromXMLFile_CDMAColumns(XmlFile).Target_Server as Target_Server",
    "FromXMLFile_CDMAColumns(XmlFile).DBMS_Major_VersionReadOnly as DBMS_Major_VersionReadOnly",
    "FromXMLFile_CDMAColumns(XmlFile).DBMS_Major_Version as DBMS_Major_Version",
    "FromXMLFile_CDMAColumns(XmlFile).DBMS_Minor_VersionReadOnly as DBMS_Minor_VersionReadOnly",
    "FromXMLFile_CDMAColumns(XmlFile).DBMS_Minor_Version as DBMS_Minor_Version"
)
df_Parse_Xml_AttrProps = df_Erwin_Exp_Xml.selectExpr(
    "FromXMLFile_CDMAColumns(XmlFile).id as id",
    "FromXMLFile_CDMAColumns(XmlFile).Attributeid as Attributeid",
    "FromXMLFile_CDMAColumns(XmlFile).Attributename as Attributename",
    "FromXMLFile_CDMAColumns(XmlFile).AttributePropsDefinition as AttributePropsDefinition",
    "FromXMLFile_CDMAColumns(XmlFile).AttributePropsType as AttributePropsType",
    "FromXMLFile_CDMAColumns(XmlFile).Physical_Data_Type as Physical_Data_Type",
    "FromXMLFile_CDMAColumns(XmlFile).Null_Option_Type as Null_Option_Type",
    "FromXMLFile_CDMAColumns(XmlFile).AttributePropsComment as AttributePropsComment",
    "FromXMLFile_CDMAColumns(XmlFile).Logical_Data_Type as Logical_Data_Type",
    "FromXMLFile_CDMAColumns(XmlFile).Column_Order as Column_Order",
    "FromXMLFile_CDMAColumns(XmlFile).User_Formatted_Name as User_Formatted_Name",
    "FromXMLFile_CDMAColumns(XmlFile).AttributePropsUser_Formatted_Physical_Name as AttributePropsUser_Formatted_Physical_Name"
)
df_Parse_Xml_Views_Id = df_Erwin_Exp_Xml.selectExpr(
    "FromXMLFile_CDMAColumns(XmlFile).Modelid as Modelid",
    "FromXMLFile_CDMAColumns(XmlFile).id as id",
    "FromXMLFile_CDMAColumns(XmlFile).name as name",
    "FromXMLFile_CDMAColumns(XmlFile).Definition as Definition",
    "FromXMLFile_CDMAColumns(XmlFile).ViewPropsName as ViewPropsName",
    "FromXMLFile_CDMAColumns(XmlFile).Type as Type",
    "FromXMLFile_CDMAColumns(XmlFile).Comment as Comment",
    "FromXMLFile_CDMAColumns(XmlFile).User_Formatted_Physical_Name as User_Formatted_Physical_Name",
    "FromXMLFile_CDMAColumns(XmlFile).UDP_Instancename as UDP_Instancename",
    "FromXMLFile_CDMAColumns(XmlFile).UDP_Instance as UDP_Instance"
)
df_Parse_Xml_View_AttrProps = df_Erwin_Exp_Xml.selectExpr(
    "FromXMLFile_CDMAColumns(XmlFile).id as id",
    "FromXMLFile_CDMAColumns(XmlFile).Attributeid as Attributeid",
    "FromXMLFile_CDMAColumns(XmlFile).Attributename as Attributename",
    "FromXMLFile_CDMAColumns(XmlFile).AttributePropsDefinition as AttributePropsDefinition",
    "FromXMLFile_CDMAColumns(XmlFile).AttributePropsType as AttributePropsType",
    "FromXMLFile_CDMAColumns(XmlFile).Physical_Data_Type as Physical_Data_Type",
    "FromXMLFile_CDMAColumns(XmlFile).Null_Option_Type as Null_Option_Type",
    "FromXMLFile_CDMAColumns(XmlFile).Comment as Comment",
    "FromXMLFile_CDMAColumns(XmlFile).Logical_Data_Type as Logical_Data_Type",
    "FromXMLFile_CDMAColumns(XmlFile).Column_Order as Column_Order",
    "FromXMLFile_CDMAColumns(XmlFile).AttributePropsUser_Formatted_Name as AttributePropsUser_Formatted_Name",
    "FromXMLFile_CDMAColumns(XmlFile).AttributePropsUser_Formatted_Physical_Name as AttributePropsUser_Formatted_Physical_Name"
)
df_Parse_Xml_Views_Id_Instance = df_Erwin_Exp_Xml.selectExpr(
    "FromXMLFile_CDMAColumns(XmlFile).id as id",
    "FromXMLFile_CDMAColumns(XmlFile).UDP_Instancename as UDP_Instancename",
    "FromXMLFile_CDMAColumns(XmlFile).UDP_Instance as UDP_Instance"
)
df_Parse_Xml_Table_Id_Instance = df_Erwin_Exp_Xml.selectExpr(
    "FromXMLFile_CDMAColumns(XmlFile).id as id",
    "FromXMLFile_CDMAColumns(XmlFile).UDP_Instancename as UDP_Instancename",
    "FromXMLFile_CDMAColumns(XmlFile).UDP_Instance as UDP_Instance"
)

# Model_Props (CTransformerStage)
df_Model_Props = df_Parse_Xml_Model_Props_Id.select(
    col("Modelid").alias("Model_id"),
    col("Modelid").alias("Modelid"),
    col("ModelPropsType").alias("ModelType"),
    col("Target_Server").alias("TargetServer"),
    col("DBMS_Major_Version").alias("DBMSVersion"),
    col("DBMS_Minor_Version").alias("DBMSMinorVersion"),
    col("name").alias("name"),
    lit("Y").alias("Owner_PathReadOnly"),
    col("ModelPropsType").alias("ModelPropsType"),
    col("Target_ServerReadOnly").alias("Target_ServerReadOnly"),
    col("Target_Server").alias("Target_Server"),
    col("DBMS_Major_VersionReadOnly").alias("DBMS_Major_VersionReadOnly"),
    col("DBMS_Major_Version").alias("DBMS_Major_Version"),
    col("DBMS_Minor_VersionReadOnly").alias("DBMS_Minor_VersionReadOnly"),
    col("DBMS_Minor_Version").alias("DBMS_Minor_Version")
)

# Xml_ModelProps (XMLOutputPX)
df_Xml_ModelProps = df_Model_Props.select(
    col("Model_id"),
    col("Modelid"),
    col("ModelType"),
    col("TargetServer"),
    col("DBMSVersion"),
    col("DBMSMinorVersion"),
    expr("XMLOutputPX_ModelPropsTag(Modelid,ModelType,TargetServer,DBMSVersion,DBMSMinorVersion) as ModelPrpsTag")
)

# Cpy_Vw (PxCopy)
df_Cpy_Vw_vw_id = df_Parse_Xml_Views_Id.select(
    col("Modelid"),
    col("id"),
    col("name").alias("Entityname"),
    col("Definition"),
    col("ViewPropsName").alias("EntityPropsName"),
    col("Type").alias("EntityPropsType"),
    col("Comment"),
    col("User_Formatted_Physical_Name")
)

# Fnl_Tbl_Vw (PxFunnel)
df_Fnl_Tbl_Vw_Entity_Props = df_Cpy_Vw_vw_id.unionByName(df_Parse_Xml_Table_Id)

# Cpy_vw_Attr (PxCopy)
df_Cpy_vw_Attr_Vw_AttrProps = df_Parse_Xml_View_AttrProps.select(
    col("id"),
    col("Attributeid"),
    col("Attributename"),
    col("AttributePropsDefinition"),
    col("AttributePropsType"),
    col("Physical_Data_Type"),
    col("Null_Option_Type"),
    col("Comment").alias("AttributePropsComment"),
    col("Logical_Data_Type"),
    col("Column_Order"),
    col("AttributePropsUser_Formatted_Name").alias("User_Formatted_Name"),
    col("AttributePropsUser_Formatted_Physical_Name")
)

# Fnl_Attr (PxFunnel)
df_Fnl_Attr_AttributeProps = df_Cpy_vw_Attr_Vw_AttrProps.unionByName(df_Parse_Xml_AttrProps)

# Attribute_Map (CTransformerStage)
df_Attribute_Map_AttrInfo = df_Fnl_Attr_AttributeProps.select(
    col("id").alias("EntityID"),
    col("Column_Order").alias("SortORder"),
    col("Attributeid").alias("AttributeId"),
    col("Attributename").alias("AttributeName"),
    col("User_Formatted_Name").alias("AttributePropsname"),
    col("AttributePropsUser_Formatted_Physical_Name").alias("Physical_Name"),
    col("AttributePropsType"),
    col("Null_Option_Type"),
    col("AttributePropsDefinition"),
    col("AttributePropsComment"),
    col("Column_Order").alias("Attribute_Order"),
    col("Column_Order"),
    expr("IF(Physical_Data_Type = 'Datetime2','Datetime',Physical_Data_Type) as Physical_Data_Type"),
    col("Logical_Data_Type").alias("Logical_Data_Type"),
    lit("").alias("Parent_Domain_Ref")
)

# XML_Attribute (XMLOutputPX)
df_XML_Attribute_Attribute = df_Attribute_Map_AttrInfo.select(
    col("EntityID"),
    expr("XMLOutputPX_Attribute_Logic(EntityID,SortORder,AttributeId,AttributeName,AttributePropsname,Physical_Name,AttributePropsType,Null_Option_Type,AttributePropsDefinition,AttributePropsComment,Attribute_Order,Column_Order,Physical_Data_Type,Logical_Data_Type,Parent_Domain_Ref) as AttrGrpsXmlTag")
)

# Cpy (PxCopy)
df_Cpy_Attribute_Ref = df_XML_Attribute_Attribute.select(
    col("EntityID"),
    col("AttrGrpsXmlTag")
)

# Fnl_Instance (PxFunnel)
df_Fnl_Instance_Instance_Grp = df_Parse_Xml_Views_Id_Instance.unionByName(df_Parse_Xml_Table_Id_Instance)

# Xmr_Filter (CTransformerStage) - filtering rows
df_Xmr_Filter_Inst_Ref = df_Fnl_Instance_Instance_Grp.filter(
    "Index(UpCase(UDP_Instancename), 'HOMESUBJECTAREA', 1) > 0"
).select(
    col("id"),
    col("UDP_Instancename"),
    col("UDP_Instance")
)

# Att_Instance (PxLookup, left join)
df_Att_Instance_Entity_Props_Id = (
    df_Fnl_Tbl_Vw_Entity_Props.alias("Entity_Props")
    .join(
        df_Xmr_Filter_Inst_Ref.alias("Inst_Ref"),
        col("Entity_Props.id") == col("Inst_Ref.id"),
        "left"
    )
    .select(
        col("Entity_Props.Modelid").alias("Modelid"),
        col("Entity_Props.id").alias("id"),
        col("Entity_Props.Entityname").alias("Entityname"),
        col("Entity_Props.Definition").alias("Definition"),
        col("Entity_Props.EntityPropsName").alias("EntityPropsName"),
        col("Entity_Props.EntityPropsType").alias("EntityPropsType"),
        col("Entity_Props.Comment").alias("Comment"),
        col("Entity_Props.User_Formatted_Physical_Name").alias("User_Formatted_Physical_Name"),
        col("Inst_Ref.UDP_Instancename").alias("UDP_Instancename"),
        col("Inst_Ref.UDP_Instance").alias("UDP_Instance")
    )
)

# Table_Map (CTransformerStage)
df_Table_Map_EntityProps = df_Att_Instance_Entity_Props_Id.select(
    col("Modelid"),
    col("id").alias("EntityID"),
    col("User_Formatted_Physical_Name").alias("TableName_Sort"),
    expr("'<Entity id=\"' || id || '\" Name=\"' || Entityname || '\">'").alias("EntityIDTag"),
    col("EntityPropsName"),
    col("User_Formatted_Physical_Name"),
    col("EntityPropsType"),
    col("Definition"),
    col("Comment"),
    expr("IF(TrimLeadingTrailing(Field(UDP_Instancename, '.', 3)) = '', '', Field(UDP_Instancename, '.', 3))").alias("UDP_Instancename"),
    expr("IF(TrimLeadingTrailing(Field(UDP_Instancename, '.', 3)) = '', '', TrimLeadingTrailing((IF IsNotNull(UDP_Instance) THEN UDP_Instance ELSE '')))").alias("UDP_Instance"),
    lit("(5D8E203D-6)").alias("Entity_Name_Font"),
    lit("(5D8E203D-7)").alias("Entity_Name_Color"),
    lit("(5D8E203D-8)").alias("Entity_Name_Font_Fill_Color")
)

# XML_Entity (XMLOutputPX)
df_XML_Entity_Entity = df_Table_Map_EntityProps.select(
    col("Modelid"),
    col("EntityIDTag"),
    col("TableName_Sort"),
    col("EntityID"),
    expr("XMLOutputPX_EntityProps(Modelid,EntityIDTag,TableName_Sort,EntityID,EntityPropsName,User_Formatted_Physical_Name,EntityPropsType,Definition,Comment,UDP_Instancename,UDP_Instance,Entity_Name_Font,Entity_Name_Color,Entity_Name_Font_Fill_Color) as EntityProps")
)

# Att_AttrGrp (PxLookup, left join)
df_Att_AttrGrp_Loop_Attr = (
    df_XML_Entity_Entity.alias("Entity")
    .join(
        df_Cpy_Attribute_Ref.alias("Attribute_Ref"),
        col("Entity.EntityID") == col("Attribute_Ref.EntityID"),
        "left"
    )
    .select(
        col("Entity.Modelid").alias("Modelid"),
        col("Entity.EntityIDTag").alias("EntityIDTag"),
        col("Entity.TableName_Sort").alias("TableName_Sort"),
        col("Entity.EntityProps").alias("EntityProps"),
        col("Attribute_Ref.AttrGrpsXmlTag").alias("AttrGrpsXmlTag"),
        col("Entity.EntityID").alias("EntityID")
    )
)

# Build_EntityGrp (CTransformerStage) - complex looping replaced with a user-defined aggregator
df_Build_EntityGrp_Entity_Grp_Ref = (
    df_Att_AttrGrp_Loop_Attr.groupBy("Modelid")
    .agg(expr("BuildEntityGroupsString(Modelid,collect_list(struct(*))) as Col_Xml"))
    .select(
        col("Modelid").alias("Model_id"),
        col("Col_Xml")
    )
)

# Att_EntyGrp (PxJoin)
df_Att_EntyGrp_Final_Map = (
    df_Xml_ModelProps.alias("Model_Props")
    .join(
        df_Build_EntityGrp_Entity_Grp_Ref.alias("Entity_Grp_Ref"),
        col("Model_Props.Model_id") == col("Entity_Grp_Ref.Model_id"),
        "inner"
    )
    .select(
        col("Model_Props.Model_id").alias("Model_id"),
        col("Model_Props.Modelid").alias("Modelid"),
        col("Model_Props.ModelType").alias("ModelType"),
        col("Model_Props.TargetServer").alias("TargetServer"),
        col("Model_Props.DBMSVersion").alias("DBMSVersion"),
        col("Model_Props.DBMSMinorVersion").alias("DBMSMinorVersion"),
        col("Model_Props.ModelPrpsTag").alias("ModelPrpsTag"),
        col("Entity_Grp_Ref.Col_Xml").alias("Col_Xml")
    )
)

# FinalMap (CTransformerStage)
df_FinalMap_CDMA_xml = df_Att_EntyGrp_Final_Map.select(
    expr(" '<?xml version=\"1.0\" encoding=\"utf-8\"?><ERwin4 FileVersion=\"4002\"><Model id=\"' || Model_id || '\" ModelType=\"' || ModelType || '\" TargetServer=\"' || TargetServer || '\" DBMSVersion=\"' || DBMSVersion || '\" DBMSMinorVersion=\"' || DBMSMinorVersion || '\">' || ModelPrpsTag || Col_Xml || '</Model></ERwin4>' ").alias("CDMA_XML")
)

# CDMA_Xml (PxSequentialFile) - write to file
df_to_write = df_FinalMap_CDMA_xml.select(
    rpad(col("CDMA_XML"), 255, " ").alias("CDMA_XML")
)
write_files(
    df_to_write,
    f"{adls_path}{OutPutFilePath}/{OutFileName}",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='\"',
    nullValue=None
)