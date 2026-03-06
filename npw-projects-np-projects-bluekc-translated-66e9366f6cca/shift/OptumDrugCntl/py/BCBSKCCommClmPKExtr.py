#!/usr/bin/python3

from npadf import *

def BCBSKCCommClmPKExtrActivities(ctx):
  def dfBCBSKCCommClmPKExtr():
    dataflowName = "BCBSKCCommClmPKExtr"
    buffer = DataflowBuffer()
    dataflow = MappingDataFlow(
      folder = DataFlowFolder(name="claim/claim/BCBSKCCommon"),
      sources = buffer.sources,
      sinks = buffer.sinks,
      transformations = buffer.transformations,
      script = f"""parameters{{
        FilePath as string ("{ctx.FilePath}"),
        SrcSysCd as string (toString("SAVRX")),
        SrcSysCdSK as integer ,
        CurrRunCycle as integer (toInteger("100")),
        RunID as string ,
        IDSDB as string ("{ctx.IDSDB}"),
        IDSOwner as string ("{ctx.IDSOwner}"),
        IDSAcct as string ,
        IDSPW as string 
      }}
  """, type='MappingDataFlow')
    artifacts = buffer.artifacts + [(dataflowName, dataflow)]
    return artifacts, dataflowName
  artifacts, dataflowName = dfBCBSKCCommClmPKExtr()
  activityName = "BCBSKCCommClmPKExtr"
  activityParameters = {"FilePath":  {"value": "'@{pipeline().parameters.FilePath}'","type": "Expression"},
  "SrcSysCd":  {"value": "'@{pipeline().parameters.SrcSysCd}'","type": "Expression"},
  "SrcSysCdSK":  {"value": "'@{pipeline().parameters.SrcSysCdSK}'","type": "Expression"},
  "CurrRunCycle":  {"value": "'@{pipeline().parameters.CurrRunCycle}'","type": "Expression"},
  "RunID":  {"value": "'@{pipeline().parameters.RunID}'","type": "Expression"},
  "IDSDB":  {"value": "'@{pipeline().parameters.IDSDB}'","type": "Expression"},
  "IDSOwner":  {"value": "'@{pipeline().parameters.IDSOwner}'","type": "Expression"},
  "IDSAcct":  {"value": "'@{pipeline().parameters.IDSAcct}'","type": "Expression"},
  "IDSPW":  {"value": "'@{pipeline().parameters.IDSPW}'","type": "Expression"}}
  activity = ExecuteDataFlowActivity(
    name = activityName,
    data_flow = DataFlowReference(reference_name=dataflowName,
      parameters=activityParameters, type='DataFlowReference'))
  return artifacts, [activity,
    CustomActivity(
      name = ctx.removeAlias("DSU.HASH.CLEAR\\2;hf_clm_pk_lkup;hf_BCBSKCClmPkExtr_Dedup"),
      command = ctx.getRoutineCommand(ctx.removeAlias("DSU.HASH.CLEAR\\2;hf_clm_pk_lkup;hf_BCBSKCClmPkExtr_Dedup"), isSubroutine=True),
      linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service),
      depends_on = [ActivityDependency( activity = activityName,
        dependency_conditions = [DependencyCondition.COMPLETED])]),
    FailActivity(
      name = "Fail",
      message = "Dataflow execution failed",
      error_code = "1",
      depends_on = [ActivityDependency(activity = activityName,
        dependency_conditions = [DependencyCondition.FAILED]),
        ActivityDependency(activity = ctx.removeAlias("DSU.HASH.CLEAR\\2;hf_clm_pk_lkup;hf_BCBSKCClmPkExtr_Dedup"),
        dependency_conditions = [DependencyCondition.COMPLETED])])]

def BCBSKCCommClmPKExtr(ctx):
  name = "BCBSKCCommClmPKExtr"
  artifacts, activities = BCBSKCCommClmPKExtrActivities(ctx)
  pipeline = PipelineResource(
    folder = PipelineFolder(name="claim/claim/BCBSKCCommon"),
    activities = activities,
    description = """
      BCBSKCCommon Claim SK Assignment
      © Copyright 2010 Blue Cross/Blue Shield of Kansas City


      CALLED BY:  
                     

      PROCESSING:   
               *  Assigns / creates claim primary key surrogate keys for all records processed in current run.  Adjusted to/from claims don't need to be in hash file. 
               *  The primary key hash file hf_clm is the output of this job and is used by the following tables for keying
                   CLM
                   DRUG_CLM
                   CLM_REMIT_HIST


      MODIFICATIONS:
      Developer                    Date                 	Project/Altiris #	Change Description				   Development Project	Code Reviewer	Date Reviewed       
      ------------------                  --------------------     	------------------------	-----------------------------------------------------------------------	    --------------------------------	-------------------------------	----------------------------       
      Kaushik Kapoor             2018-01-08              5828                 Original Programming                                                    IntegrateDev2                      Kalyan Neelam         2018-02-26 
      Rekha Radhakrishna    2020-08-13              6131                 Modified common file layout to include new 8 fields      IntegrateDev2                      Sravya Gorla             2020-09-12
                                                                                                     ( field 151 - 158)
      Parameters:
      -----------
      FilePath:
        IDS File Path
      SrcSysCd:
        Source System Code
      SrcSysCdSK:
        Source System SK
      CurrRunCycle:
        Current Run Cycle
      RunID:
        Run ID
      IDSDB:
        IDS Database
      IDSOwner:
        IDS Table Owner
      IDSAcct:
        IDS Account
      IDSPW:
        IDS Password""",
    parameters = {
      "FilePath": ParameterSpecification(type="String", default_value=ctx.FilePath),
      "SrcSysCd": ParameterSpecification(type="String", default_value="SAVRX"),
      "SrcSysCdSK": ParameterSpecification(type="Int"),
      "CurrRunCycle": ParameterSpecification(type="Int", default_value="100"),
      "RunID": ParameterSpecification(type="String"),
      "IDSDB": ParameterSpecification(type="String", default_value=ctx.IDSDB),
      "IDSOwner": ParameterSpecification(type="String", default_value=ctx.IDSOwner),
      "IDSAcct": ParameterSpecification(type="String"),
      "IDSPW": ParameterSpecification(type="String")})
  return artifacts + [(name, pipeline)]

def main():
  ctx = TranslationContext()
  artifacts = (BCBSKCCommClmPKExtr(ctx))
  if ctx.validateArtifacts(artifacts):
    ctx.deployArtifacts(artifacts)

if __name__ == '__main__':
  main()
