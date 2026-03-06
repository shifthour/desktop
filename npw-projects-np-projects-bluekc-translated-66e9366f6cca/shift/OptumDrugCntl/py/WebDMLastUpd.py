#!/usr/bin/python3

from npadf import *

def WebDMLastUpdActivities(ctx):
  def dfWebDMLastUpd():
    dataflowName = "WebDMLastUpd"
    buffer = DataflowBuffer()
    dataflow = MappingDataFlow(
      folder = DataFlowFolder(name="WebDataMart/Claim/CycUpdt"),
      sources = buffer.sources,
      sinks = buffer.sinks,
      transformations = buffer.transformations,
      script = f"""parameters{{
        IDSInstance as string ("{ctx.IDSInstance}"),
        IDSDB as string ("{ctx.IDSDB}"),
        IDSAcct as string ,
        IDSPW as string ,
        ClmMartServer as string ("{ctx.ClmMartServer}"),
        ClmMartDB as string ("{ctx.ClmMartDB}"),
        ClmMartOwner as string ("{ctx.ClmMartOwner}"),
        ClmMartAcct as string ,
        ClmMartPW as string ,
        SubjectName as string ,
        UpdateDate as string 
      }}
  """, type='MappingDataFlow')
    artifacts = buffer.artifacts + [(dataflowName, dataflow)]
    return artifacts, dataflowName
  artifacts, dataflowName = dfWebDMLastUpd()
  activityName = "WebDMLastUpd"
  activityParameters = {"IDSInstance":  {"value": "'@{pipeline().parameters.IDSInstance}'","type": "Expression"},
  "IDSDB":  {"value": "'@{pipeline().parameters.IDSDB}'","type": "Expression"},
  "IDSAcct":  {"value": "'@{pipeline().parameters.IDSAcct}'","type": "Expression"},
  "IDSPW":  {"value": "'@{pipeline().parameters.IDSPW}'","type": "Expression"},
  "ClmMartServer":  {"value": "'@{pipeline().parameters.ClmMartServer}'","type": "Expression"},
  "ClmMartDB":  {"value": "'@{pipeline().parameters.ClmMartDB}'","type": "Expression"},
  "ClmMartOwner":  {"value": "'@{pipeline().parameters.ClmMartOwner}'","type": "Expression"},
  "ClmMartAcct":  {"value": "'@{pipeline().parameters.ClmMartAcct}'","type": "Expression"},
  "ClmMartPW":  {"value": "'@{pipeline().parameters.ClmMartPW}'","type": "Expression"},
  "SubjectName":  {"value": "'@{pipeline().parameters.SubjectName}'","type": "Expression"},
  "UpdateDate":  {"value": "'@{pipeline().parameters.UpdateDate}'","type": "Expression"}}
  activity = ExecuteDataFlowActivity(
    name = activityName,
    data_flow = DataFlowReference(reference_name=dataflowName,
      parameters=activityParameters, type='DataFlowReference'))
  return artifacts, [activity]

def WebDMLastUpd(ctx):
  name = "WebDMLastUpd"
  artifacts, activities = WebDMLastUpdActivities(ctx)
  pipeline = PipelineResource(
    folder = PipelineFolder(name="WebDataMart/Claim/CycUpdt"),
    activities = activities,
    description = """
      Update last update date time for Web DM subject
      * VC LOGS *
      ^1_1 01/29/08 14:29:13 Batch  14639_52158 INIT bckcetl ids20 dsadm dsadm
      ^1_1 01/09/08 13:32:13 Batch  14619_48738 INIT bckcetl ids20 dsadm dsadm
      ^1_1 11/24/07 09:35:50 Batch  14573_34558 INIT bckcetl ids20 dsadm dsadm
      ^1_3 03/28/07 06:21:15 Batch  14332_22879 INIT bckcetl ids20 dsadm dsadm
      ^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm
      ^1_1 05/19/06 09:43:19 Batch  14019_35008 INIT bckcett devlIDS30 u10913 Ollie move KCREE from devl to test

      COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 


      JOB NAME:     WebDMLastUpd

      DESCRIPTION:     Update reference table with subject area last update date and time
            

      INPUTS:          Subject name
                              Date time
      	
        
      HASH FILES: None


      TRANSFORMS:  None
                                 

      PROCESSING:  The sysdummy table is used to initiate the SQL server update.


      OUTPUTS:     direct update of REF_DM_LAST_UPDT


      MODIFICATIONS:
                 Brent Leland  05/17/2006  -   Originally Programmed
      Parameters:
      -----------
      IDSInstance:
        IDS Instance
      IDSDB:
        IDS Database
      IDSAcct:
        IDS Account
      IDSPW:
        IDS Password
      ClmMartServer:
        Claim Mart Server
      ClmMartDB:
        Claim Mart Database
      ClmMartOwner:
        Claim Mart Table Owner
      ClmMartAcct:
        Claim Mart Account
      ClmMartPW:
        Claim Mart Password
      SubjectName:
        Subject Name
      UpdateDate:
        Update Date""",
    parameters = {
      "IDSInstance": ParameterSpecification(type="String", default_value=ctx.IDSInstance),
      "IDSDB": ParameterSpecification(type="String", default_value=ctx.IDSDB),
      "IDSAcct": ParameterSpecification(type="String"),
      "IDSPW": ParameterSpecification(type="String"),
      "ClmMartServer": ParameterSpecification(type="String", default_value=ctx.ClmMartServer),
      "ClmMartDB": ParameterSpecification(type="String", default_value=ctx.ClmMartDB),
      "ClmMartOwner": ParameterSpecification(type="String", default_value=ctx.ClmMartOwner),
      "ClmMartAcct": ParameterSpecification(type="String"),
      "ClmMartPW": ParameterSpecification(type="String"),
      "SubjectName": ParameterSpecification(type="String"),
      "UpdateDate": ParameterSpecification(type="String")})
  return artifacts + [(name, pipeline)]

def main():
  ctx = TranslationContext()
  artifacts = (WebDMLastUpd(ctx))
  if ctx.validateArtifacts(artifacts):
    ctx.deployArtifacts(artifacts)

if __name__ == '__main__':
  main()
