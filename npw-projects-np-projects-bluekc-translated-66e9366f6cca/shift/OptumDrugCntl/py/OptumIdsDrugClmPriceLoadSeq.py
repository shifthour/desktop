#!/usr/bin/python3

from npadf import *

def OptumIdsDrugClmPriceLoadSeqActivities(ctx):
  def If_1B():
    def OptumIdsMbrPdxDrugClmPriceFkey():
      parameters= {
        "FilePath": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.FilePath", ' ', 'S'), '\'', 'B')},
        "IDSDB": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSDB", ' ', 'S'), '\'', 'B')},
        "IDSOwner": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSOwner", ' ', 'S'), '\'', 'B')},
        "IDSPW": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSPW", ' ', 'S'), '\'', 'B')},
        "IDSAcct": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSAcct", ' ', 'S'), '\'', 'B')},
        "UWSOwner": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.UWSOwner", ' ', 'S'), '\'', 'B')},
        "UWSDB": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.UWSDB", ' ', 'S'), '\'', 'B')},
        "UWSAcct": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.UWSAcct", ' ', 'S'), '\'', 'B')},
        "UWSPW": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.UWSPW", ' ', 'S'), '\'', 'B')},
        "SrcSysCd": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.SrcSysCd", ' ', 'S'), '\'', 'B')},
        "RunID": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.RunID", ' ', 'S'), '\'', 'B')},
        "IDSRunCycle": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSRunCycle", ' ', 'S'), '\'', 'B')},
        "CurrentDate": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.Current_Dt", ' ', 'S'), '\'', 'B')}}
      parameters.update(ctx.parameter_override)
      activity = ExecutePipelineActivity(
        name = "OptumIdsMbrPdxDrugClmPriceFkey",
        wait_on_completion = True,
        pipeline = PipelineReference(type="PipelineReference",reference_name="OptumIdsDrugClmPriceFkey"),
        parameters = parameters)
      return [], [activity]
    artifacts = []
    activities = [IfConditionActivity(name = "If_1B",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('OptumIdsMbrPdxDrugClmPriceFkey', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        OptumIdsMbrPdxDrugClmPriceFkey()))]
    return artifacts, activities
  def If_1C():
    def OptumIdsMbrPdxDrugClmPriceLoad():
      parameters= {
        "FilePath": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.FilePath", ' ', 'S'), '\'', 'B')},
        "IDSInstance": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSInstance", ' ', 'S'), '\'', 'B')},
        "IDSAcct": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSAcct", ' ', 'S'), '\'', 'B')},
        "IDSPW": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSPW", ' ', 'S'), '\'', 'B')},
        "IDSDB": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSDB", ' ', 'S'), '\'', 'B')},
        "IDSDB2ArraySize": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSDB2ArraySize", ' ', 'S'), '\'', 'B')},
        "IDSDB2RecordCount": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSDB2RecordCount", ' ', 'S'), '\'', 'B')},
        "IDSOwner": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSOwner", ' ', 'S'), '\'', 'B')}}
      parameters.update(ctx.parameter_override)
      activity = ExecutePipelineActivity(
        name = "OptumIdsMbrPdxDrugClmPriceLoad",
        wait_on_completion = True,
        pipeline = PipelineReference(type="PipelineReference",reference_name="OptumIdsDrugClmPriceLoad"),
        parameters = parameters)
      return [], [activity]
    artifacts = []
    activities = [IfConditionActivity(name = "If_1C",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('OptumIdsMbrPdxDrugClmPriceLoad', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        OptumIdsMbrPdxDrugClmPriceLoad()),
      depends_on = [
        ActivityDependency(
          activity = "If_1B",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  return merge(
    If_1C(),
    If_1B())

def OptumIdsDrugClmPriceLoadSeq(ctx):
  name = "OptumIdsDrugClmPriceLoadSeq"
  artifacts, activities = OptumIdsDrugClmPriceLoadSeqActivities(ctx)
  pipeline = PipelineResource(
    folder = PipelineFolder(name="claim/SeqOptum"),
    activities = activities,
    description = """
      Extract Spread pricing data perform lookup in fkey job on Optum file and load IDS DRUG_CLM_PRICE tables.
      Parameters:
      -----------
      APT_CONFIG_FILE:
        Configuration file
        The Parallel job configuration file.
      IDSInstance:
        IDS Instance
      IDSDSN:
        IDS ODBC DSN
      IDSAcct:
        IDS Account
      IDSDB:
        IDS Database
      IDSPW:
        IDS Password
      IDSOwner:
        IDS Owner
      FilePath:
        File Path
      SrcSysCd:
        SrcSysCd
      SrcSysCdSK:
        SrcSysCdSK
      RunID:
        RunID
      IDSRunCycle:
        IDSRunCycle
      IDSDB2ArraySize:
        IDS DB2 Array Size for Load
      IDSDB2RecordCount:
        IDS DB2 Record Count for Load
      ExclusionList:
        ExclusionList
      UWSAcct:
        UWS Account
      UWSPW:
        UWS Password
      UWSOwner:
        UWS Table Owner
      UWSDB:
        UWS Database
      Current_Dt:
        Current_Dt""",
    parameters = {
      "APT_CONFIG_FILE": ParameterSpecification(type="String", default_value=ctx.APT_CONFIG_FILE),
      "IDSInstance": ParameterSpecification(type="String", default_value=ctx.IDSInstance),
      "IDSDSN": ParameterSpecification(type="String", default_value=ctx.IDSDSN),
      "IDSAcct": ParameterSpecification(type="String", default_value=ctx.IDSAcct),
      "IDSDB": ParameterSpecification(type="String", default_value=ctx.IDSDB),
      "IDSPW": ParameterSpecification(type="String", default_value="{iisenc}eHG47YUwCsilbsYLdMUwUw=="),
      "IDSOwner": ParameterSpecification(type="String", default_value=ctx.IDSOwner),
      "FilePath": ParameterSpecification(type="String", default_value=ctx.FilePath),
      "SrcSysCd": ParameterSpecification(type="String", default_value="OPTUMRX"),
      "SrcSysCdSK": ParameterSpecification(type="Int", default_value="-1951778164"),
      "RunID": ParameterSpecification(type="String", default_value="101"),
      "IDSRunCycle": ParameterSpecification(type="String", default_value="102"),
      "IDSDB2ArraySize": ParameterSpecification(type="String", default_value=ctx.IDSDB2ArraySize),
      "IDSDB2RecordCount": ParameterSpecification(type="String", default_value=ctx.IDSDB2RecordCount),
      "ExclusionList": ParameterSpecification(type="String"),
      "UWSAcct": ParameterSpecification(type="String", default_value=ctx.UWSAcct),
      "UWSPW": ParameterSpecification(type="String", default_value="{iisenc}eHG47YUwCsilbsYLdMUwUw=="),
      "UWSOwner": ParameterSpecification(type="String", default_value=ctx.UWSOwner),
      "UWSDB": ParameterSpecification(type="String", default_value=ctx.UWSDB),
      "Current_Dt": ParameterSpecification(type="String")})
  return artifacts + [(name, pipeline)]

def main():
  ctx = TranslationContext()
  artifacts = (OptumIdsDrugClmPriceLoadSeq(ctx))
  if ctx.validateArtifacts(artifacts):
    ctx.deployArtifacts(artifacts)

if __name__ == '__main__':
  main()
