#!/usr/bin/python3

from npadf import *

def WebDmLoadRunCycUpdActivities(ctx):
  def dfWebDmLoadRunCycUpd():
    dataflowName = "WebDmLoadRunCycUpd"
    buffer = DataflowBuffer()
    dataflow = MappingDataFlow(
      folder = DataFlowFolder(name="WebDataMart/Claim/CycUpdt"),
      sources = buffer.sources,
      sinks = buffer.sinks,
      transformations = buffer.transformations,
      script = f"""parameters{{
        FilePath as string ("{ctx.FilePath}"),
        IDSAcct as string ,
        IDSPW as string ,
        IDSInstance as string ("{ctx.IDSInstance}"),
        IDSDB as string ("{ctx.IDSDB}"),
        IDSDSN as string ("{ctx.IDSDSN}"),
        IDSOwner as string ("{ctx.IDSOwner}")
      }}
  """, type='MappingDataFlow')
    artifacts = buffer.artifacts + [(dataflowName, dataflow)]
    return artifacts, dataflowName
  artifacts, dataflowName = dfWebDmLoadRunCycUpd()
  activityName = "WebDmLoadRunCycUpd"
  activityParameters = {"FilePath":  {"value": "'@{pipeline().parameters.FilePath}'","type": "Expression"},
  "IDSAcct":  {"value": "'@{pipeline().parameters.IDSAcct}'","type": "Expression"},
  "IDSPW":  {"value": "'@{pipeline().parameters.IDSPW}'","type": "Expression"},
  "IDSInstance":  {"value": "'@{pipeline().parameters.IDSInstance}'","type": "Expression"},
  "IDSDB":  {"value": "'@{pipeline().parameters.IDSDB}'","type": "Expression"},
  "IDSDSN":  {"value": "'@{pipeline().parameters.IDSDSN}'","type": "Expression"},
  "IDSOwner":  {"value": "'@{pipeline().parameters.IDSOwner}'","type": "Expression"}}
  activity = ExecuteDataFlowActivity(
    name = activityName,
    data_flow = DataFlowReference(reference_name=dataflowName,
      parameters=activityParameters, type='DataFlowReference'))
  return artifacts, [activity]

def WebDmLoadRunCycUpd(ctx):
  name = "WebDmLoadRunCycUpd"
  artifacts, activities = WebDmLoadRunCycUpdActivities(ctx)
  pipeline = PipelineResource(
    folder = PipelineFolder(name="WebDataMart/Claim/CycUpdt"),
    activities = activities,
    description = """
      Web Data Mart Claim extract from IDS run cycle load indicator update
      COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

      DESCRIPTION:    Update the P_RUN_CYC table CDM load indicator fields to show those records have been copied to the Claim Data Mart.
            
      PROCESSING:  Each IDS Claim source has a seperate SQL to extract the maximum run cycle value.  These run cycle values are used to update the IDS P_RUN_CYCLE, load indicator field for the particular source and subject.  All run cycles less than the maximum where the indicator is "N" are updated to "Y".

      Called by :IdsWebDMDrugLoadSeq
        
      MODIFICATIONS:
      Date                 Developer                Change Description
      ------------------      ----------------------------     --------------------------------------------------------------------------------------------------------
      2006-04-07      Brent Leland            Original Programming.
      2006-05-08      Brent Leland            Added Argus SQL
      2006-07-03      Brent Leland            Changed SQL update statement.  Was incorrectly updating EDW_LOAD_IN.  Changed to update CDM_LOAD_IN.

                                                       Project/Altiris #                      Change Description                                                                    Development                 Code                          Date 
      Developer          Date                                                                                                                                                                Project                           Reviewer                   Reviewed       
      ------------------       ----------------       ------------------------                      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
      Sandrew           2008-10-03     3784(PBM)                            Added ESI as a Source to update CDM_LOAD_IN                      devlIDSnew                  Steph Goddard           10/28/2008

      Srinidhi              2019-10-11   6131- PBM Replacement       Added new data source for OPTUMRX                                        IntegrateDev1                Kalyan Neelam            2019-11-25
      Kambham

      Reddy sanam  2020-09-14   US281070                              Added new data source for Lumeris and Medimpact                      IntegrateDev2                Kalyan Neelam            2020-09-15

      Reddy sanam  2020-12-09   US329127                              Added new data source for EyeMed and DentaQuest                    IntegrateDev2                


      Goutham Kalidindi 2021-05-05   US-377804                       Added new data source for LIVONGO(Lvnghlth)                            IntegrateDev2                 Jeyaprasanna             2021-05-06
      Parameters:
      -----------
      FilePath:
        EDW File Path
      IDSAcct:
        IDS Account
      IDSPW:
        IDS Password
      IDSInstance:
        IDS Instance
      IDSDB:
        IDS Database
      IDSDSN:
        IDS ODBC DSN
      IDSOwner:
        IDS Table Owner""",
    parameters = {
      "FilePath": ParameterSpecification(type="String", default_value=ctx.FilePath),
      "IDSAcct": ParameterSpecification(type="String"),
      "IDSPW": ParameterSpecification(type="String"),
      "IDSInstance": ParameterSpecification(type="String", default_value=ctx.IDSInstance),
      "IDSDB": ParameterSpecification(type="String", default_value=ctx.IDSDB),
      "IDSDSN": ParameterSpecification(type="String", default_value=ctx.IDSDSN),
      "IDSOwner": ParameterSpecification(type="String", default_value=ctx.IDSOwner)})
  return artifacts + [(name, pipeline)]

def main():
  ctx = TranslationContext()
  artifacts = (WebDmLoadRunCycUpd(ctx))
  if ctx.validateArtifacts(artifacts):
    ctx.deployArtifacts(artifacts)

if __name__ == '__main__':
  main()
