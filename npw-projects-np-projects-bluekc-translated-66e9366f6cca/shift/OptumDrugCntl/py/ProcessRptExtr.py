#!/usr/bin/python3

from npadf import *

def ProcessRptExtrActivities(ctx):
  def dfProcessRptExtr():
    def IDS():
       ds_ls_db = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.sqldb_linked_service)
       df_sources_inline = DataFlowSource(name='IDS',linked_service=ds_ls_db)
       return DataflowSegment([df_sources_inline])
    def Trans1():
       return DataflowSegment([
         Transformation(name='Trans1Derived'),
         Transformation(name='Trans1')])
    def processingrpt():
       ds_ls_blob = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.file_linked_service)
       df_sources_inline = DataFlowSource(name='processingrptSource', linked_service=ds_ls_blob)
       df_sinks_inline = DataFlowSink(name='processingrpt', linked_service=ds_ls_blob)
       return DataflowSegment([df_sources_inline,
         Transformation(name='processingrptUnion'),
         df_sinks_inline])
    dataflowName = "ProcessRptExtr"
    buffer = DataflowBuffer()
    buffer.append(IDS())
    buffer.append(Trans1())
    buffer.append(processingrpt())
    dataflow = MappingDataFlow(
      folder = DataFlowFolder(name="util/ProcessRpt"),
      sources = buffer.sources,
      sinks = buffer.sinks,
      transformations = buffer.transformations,
      script = f"""parameters{{
        IDSFilePath as string ,
        RunCycle as integer (toInteger("470")),
        Subject as string (toString("CLAIM")),
        SourceSys as string (toString("FACETS")),
        TargetSys as string (toString("IDS")),
        RowCount as integer (toInteger("10000")),
        IDSInstance as string ("{ctx.IDSInstance}"),
        IDSDB as string ("{ctx.IDSDB}"),
        IDSOwner as string ("{ctx.IDSOwner}"),
        IDSAcct as string (toString("u08717")),
        IDSPW as string (toString("L4;@0KVAN9;L4OFI4HJ41KH5B<N"))
      }}
    source(output(
        SUBJ_CD as string,
        SRC_SYS_CD as string,
        TRGT_SYS_CD as string,
        STRT_DTM as timestamp,
        END_DTM as timestamp,
        DAY_OF_WK_FULL_NM as string,
        DURATION as decimal(20, 6)),
      allowSchemaDrift: true,
      validateSchema: false,
      format: 'query',
      store: 'sqlserver',
      query: ("SELECT \\nr.SUBJ_CD,\\nr.SRC_SYS_CD,\\nr.TRGT_SYS_CD,\\nr.STRT_DTM,  \\nr.END_DTM,\\nc.DAY_OF_WK_FULL_NM, \\n(r.END_DTM - r.STRT_DTM) AS DURATION\\n\\nFROM " + toString($IDSOwner) + ".P_RUN_CYC r,\\n           " + toString($IDSOwner) + ".P_BTCH_CYC b,\\n           " + toString($IDSOwner) + ".CLNDR_DT c\\nWHERE r.SUBJ_CD = '" + toString($Subject) + "'\\n      AND r.TRGT_SYS_CD = '" + toString($TargetSys) + "'\\n      AND r.SRC_SYS_CD = '" + toString($SourceSys) + "'\\n      AND r.BTCH_CYC_SK = b.BTCH_CYC_SK\\n      AND b.BTCH_CYC_DESC = c.CLNDR_DT_SK\\n      AND r.RUN_CYC_NO = " + toString($RunCycle) + "\\n order by r.STRT_DTM\\n"),
      schemaName: (""" + ctx.getSchemaName("""$IDSOwner""") + f"""),
      tableName: (""" + ctx.getTableName("""$IDSOwner""") + f"""),
      isolationLevel: 'READ_UNCOMMITTED') ~> IDS
    IDS
        derive(
              SUBJ_CD = SUBJ_CD,
              SRC_SYS_CD = SRC_SYS_CD,
              TRGT_SYS_CD = TRGT_SYS_CD,
              STRT_DTM = STRT_DTM,
              END_DTM = END_DTM,
              DAY_OF_WK_FULL_NM = DAY_OF_WK_FULL_NM,
              TIME_DURATION = :Hrs + ":" + :Min,
              ROW_COUNT = $RowCount,
              Value = :Value,
              PerPos = :PerPos,
              Sec = :Sec,
              Min = :Min,
              Hrs = :Hrs,
              Value := {'"000000" + npw_DS_TRIM_1(DURATION)'.replace("IDS_Extract.","")},
              PerPos := {'instr(:Value,".")'.replace("IDS_Extract.","")},
              Sec := {'substring(:Value, :PerPos - 2, 2)'.replace("IDS_Extract.","")},
              Min := {'substring(:Value, :PerPos - 4, 2)'.replace("IDS_Extract.","")},
              Hrs := {'substring(:Value, :PerPos - 6, 2)'.replace("IDS_Extract.","")}) ~> Trans1Derived
        Trans1Derived
        select(mapColumn(
            SUBJ_CD,
            SRC_SYS_CD,
            TRGT_SYS_CD,
            STRT_DTM,
            END_DTM,
            DAY_OF_WK_FULL_NM,
            TIME_DURATION,
            ROW_COUNT
        ),
        skipDuplicateMapInputs: true,
        skipDuplicateMapOutputs: true) ~> Trans1
    source(allowSchemaDrift: true,
      validateSchema: false,
      ignoreNoFilesFound: true,
      format: 'delimited',
      fileSystem: '{ctx.file_container}',
      folderPath: ({ctx.getFolderPath("$IDSFilePath + '/scripts/log/processing_rpt.dat'")}),
      fileName: ({ctx.getFileName("$IDSFilePath + '/scripts/log/processing_rpt.dat'")}),
      columnDelimiter: ',',
      escapeChar: '\\\\',
      quoteChar: '\\"',
      columnNamesAsHeader:false) ~> processingrptSource
    Trans1, processingrptSource union(byName: false) ~> processingrptUnion
    processingrptUnion sink(
      allowSchemaDrift: true,
      validateSchema: false,
      partitionFileNames:[({ctx.getFileName("$IDSFilePath + '/scripts/log/processing_rpt.dat'")})],
      partitionBy('hash', 1),
      skipDuplicateMapInputs: true,
      skipDuplicateMapOutputs: true,
      format: 'delimited',
      fileSystem: '{ctx.file_container}',
      folderPath: ({ctx.getFolderPath("$IDSFilePath + '/scripts/log/processing_rpt.dat'")}) ,
      columnDelimiter: ',',
      escapeChar: '\\\\',
      quoteChar: '\\"',
      columnNamesAsHeader:false) ~> processingrpt""", type='MappingDataFlow')
    artifacts = buffer.artifacts + [(dataflowName, dataflow)]
    return artifacts, dataflowName
  artifacts, dataflowName = dfProcessRptExtr()
  activityName = "ProcessRptExtr"
  activityParameters = {"IDSFilePath":  {"value": "'@{pipeline().parameters.IDSFilePath}'","type": "Expression"},
  "RunCycle":  {"value": "'@{pipeline().parameters.RunCycle}'","type": "Expression"},
  "Subject":  {"value": "'@{pipeline().parameters.Subject}'","type": "Expression"},
  "SourceSys":  {"value": "'@{pipeline().parameters.SourceSys}'","type": "Expression"},
  "TargetSys":  {"value": "'@{pipeline().parameters.TargetSys}'","type": "Expression"},
  "RowCount":  {"value": "'@{pipeline().parameters.RowCount}'","type": "Expression"},
  "IDSInstance":  {"value": "'@{pipeline().parameters.IDSInstance}'","type": "Expression"},
  "IDSDB":  {"value": "'@{pipeline().parameters.IDSDB}'","type": "Expression"},
  "IDSOwner":  {"value": "'@{pipeline().parameters.IDSOwner}'","type": "Expression"},
  "IDSAcct":  {"value": "'@{pipeline().parameters.IDSAcct}'","type": "Expression"},
  "IDSPW":  {"value": "'@{pipeline().parameters.IDSPW}'","type": "Expression"}}
  activity = ExecuteDataFlowActivity(
    name = activityName,
    data_flow = DataFlowReference(reference_name=dataflowName,
      parameters=activityParameters, type='DataFlowReference'))
  return artifacts, [activity]

def ProcessRptExtr(ctx):
  name = "ProcessRptExtr"
  artifacts, activities = ProcessRptExtrActivities(ctx)
  pipeline = PipelineResource(
    folder = PipelineFolder(name="util/ProcessRpt"),
    activities = activities,
    description = """
      Extract <Subject> Information
      © Copyright 2008 Blue Cross/Blue Shield of Kansas City


      CALLED BY:  Various job controls
                     

      PROCESSING:   Select row from P_RUN_CYC based on input source, target, subject, and run cycle.  Caculate run time durartion and append information to file /ids/prod/landing/ids_processing.dat.


      MODIFICATIONS:
      Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
      ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
      Brent Leland            2008-02-14       IAD Prod. Supp     Original Programming.                                                                   devlIDScur                     Steph Goddard          02/22/2008

      Jaideep Mankala     2017-03-13           5321                 Updated Aliase for output column 'DURATION' in                        IntegrateDev2                Jag Yelavarthi             2017-03-13         
                                                                                              IDS stage to match with stage meta data.
                                                                                            Changed Timestamp precision from 3 to 6
      Parameters:
      -----------
      IDSFilePath:
        IDS File Path
      RunCycle:
        Run Cycle
      Subject:
        Subject
      SourceSys:
        Source System
      TargetSys:
        Target System
      RowCount:
        Row Count
      IDSInstance:
        IDS Instance
      IDSDB:
        IDS Database
      IDSOwner:
        IDS Table Owner
      IDSAcct:
        IDS Account
      IDSPW:
        IDS Password""",
    parameters = {
      "IDSFilePath": ParameterSpecification(type="String"),
      "RunCycle": ParameterSpecification(type="Int", default_value="470"),
      "Subject": ParameterSpecification(type="String", default_value="CLAIM"),
      "SourceSys": ParameterSpecification(type="String", default_value="FACETS"),
      "TargetSys": ParameterSpecification(type="String", default_value="IDS"),
      "RowCount": ParameterSpecification(type="Int", default_value="10000"),
      "IDSInstance": ParameterSpecification(type="String", default_value=ctx.IDSInstance),
      "IDSDB": ParameterSpecification(type="String", default_value=ctx.IDSDB),
      "IDSOwner": ParameterSpecification(type="String", default_value=ctx.IDSOwner),
      "IDSAcct": ParameterSpecification(type="String", default_value="u08717"),
      "IDSPW": ParameterSpecification(type="String", default_value="L4;@0KVAN9;L4OFI4HJ41KH5B<N")})
  return artifacts + [(name, pipeline)]

def main():
  ctx = TranslationContext()
  artifacts = (ProcessRptExtr(ctx))
  if ctx.validateArtifacts(artifacts):
    ctx.deployArtifacts(artifacts)

if __name__ == '__main__':
  main()
