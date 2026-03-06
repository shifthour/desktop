#!/usr/bin/python3

from npadf import *

def OptumIdsMbrPdxDeniedTransLoadSeqActivities(ctx):
  def If_1A():
    def OptumIdsMbrPdxDeniedClmXfm():
      parameters= {
        "IDSAcct": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSAcct", ' ', 'S'), '\'', 'B')},
        "IDSDB": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSDB", ' ', 'S'), '\'', 'B')},
        "IDSPW": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSPW", ' ', 'S'), '\'', 'B')},
        "APT_CONFIG_FILE": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.APT_CONFIG_FILE", ' ', 'S'), '\'', 'B')},
        "FilePath": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.FilePath", ' ', 'S'), '\'', 'B')},
        "IDSOwner": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSOwner", ' ', 'S'), '\'', 'B')},
        "IDSDB2ArraySize": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSDB2ArraySize", ' ', 'S'), '\'', 'B')},
        "IDSDB2RecordCount": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSDB2RecordCount", ' ', 'S'), '\'', 'B')},
        "IDSRunCycle": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSRunCycle", ' ', 'S'), '\'', 'B')},
        "SrcSysCd": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.SrcSysCd", ' ', 'S'), '\'', 'B')},
        "RunID": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.RunID", ' ', 'S'), '\'', 'B')},
        "CurrDate": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.CurrDate", ' ', 'S'), '\'', 'B')}}
      parameters.update(ctx.parameter_override)
      activity = ExecutePipelineActivity(
        name = "OptumIdsMbrPdxDeniedClmXfm",
        wait_on_completion = True,
        pipeline = PipelineReference(type="PipelineReference",reference_name="OptumIdsMbrPdxDeniedClmXfm"),
        parameters = parameters)
      return [], [activity]
    artifacts = []
    activities = [IfConditionActivity(name = "If_1A",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('OptumIdsMbrPdxDeniedClmXfm', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        OptumIdsMbrPdxDeniedClmXfm()))]
    return artifacts, activities
  def If_1B():
    def IdsMbrPdxDeniedClmPkey():
      parameters= {
        "FilePath": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.FilePath", ' ', 'S'), '\'', 'B')},
        "APT_CONFIG_FILE": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.APT_CONFIG_FILE", ' ', 'S'), '\'', 'B')},
        "IDSDB": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSDB", ' ', 'S'), '\'', 'B')},
        "IDSOwner": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSOwner", ' ', 'S'), '\'', 'B')},
        "IDSAcct": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSAcct", ' ', 'S'), '\'', 'B')},
        "IDSPW": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSPW", ' ', 'S'), '\'', 'B')},
        "IDSRunCycle": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSRunCycle", ' ', 'S'), '\'', 'B')},
        "SrcSysCd": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.SrcSysCd", ' ', 'S'), '\'', 'B')},
        "IDSDB2ArraySize": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSDB2ArraySize", ' ', 'S'), '\'', 'B')},
        "IDSDB2RecordCount": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSDB2RecordCount", ' ', 'S'), '\'', 'B')},
        "RunID": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.RunID", ' ', 'S'), '\'', 'B')}}
      parameters.update(ctx.parameter_override)
      activity = ExecutePipelineActivity(
        name = "IdsMbrPdxDeniedClmPkey",
        wait_on_completion = True,
        pipeline = PipelineReference(type="PipelineReference",reference_name="IdsMbrPdxDeniedClmPkey"),
        parameters = parameters)
      return [], [activity]
    artifacts = []
    activities = [IfConditionActivity(name = "If_1B",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('IdsMbrPdxDeniedTransPkey', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        IdsMbrPdxDeniedClmPkey()),
      depends_on = [
        ActivityDependency(
          activity = "If_1A",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def If_1C():
    def IdsMbrPdxDeniedClaimsFkey():
      parameters= {
        "FilePath": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.FilePath", ' ', 'S'), '\'', 'B')},
        "APT_CONFIG_FILE": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.APT_CONFIG_FILE", ' ', 'S'), '\'', 'B')},
        "SrcSysCd": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.SrcSysCd", ' ', 'S'), '\'', 'B')},
        "IDSRunCycle": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSRunCycle", ' ', 'S'), '\'', 'B')},
        "IDSDB": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSDB", ' ', 'S'), '\'', 'B')},
        "IDSAcct": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSAcct", ' ', 'S'), '\'', 'B')},
        "IDSOwner": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSOwner", ' ', 'S'), '\'', 'B')},
        "IDSPW": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSPW", ' ', 'S'), '\'', 'B')},
        "RunID": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.RunID", ' ', 'S'), '\'', 'B')},
        "PrefixFkeyFailedFileName": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.PrefixFkeyFailedFileName", ' ', 'S'), '\'', 'B')}}
      parameters.update(ctx.parameter_override)
      activity = ExecutePipelineActivity(
        name = "IdsMbrPdxDeniedClaimsFkey",
        wait_on_completion = True,
        pipeline = PipelineReference(type="PipelineReference",reference_name="IdsMbrPdxDeniedClmFkey"),
        parameters = parameters)
      return [], [activity]
    artifacts = []
    activities = [IfConditionActivity(name = "If_1C",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('IdsMbrPdxDeniedTransFkey', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        IdsMbrPdxDeniedClaimsFkey()),
      depends_on = [
        ActivityDependency(
          activity = "If_1B",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def If_1D():
    def IdsMbrPdxDeniedClmLoad():
      parameters= {
        "FilePath": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.FilePath", ' ', 'S'), '\'', 'B')},
        "IDSInstance": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSInstance", ' ', 'S'), '\'', 'B')},
        "IDSAcct": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSAcct", ' ', 'S'), '\'', 'B')},
        "IDSPW": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSPW", ' ', 'S'), '\'', 'B')},
        "IDSDB": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSDB", ' ', 'S'), '\'', 'B')},
        "IDSOwner": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSOwner", ' ', 'S'), '\'', 'B')},
        "IDSDB2ArraySize": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSDB2ArraySize", ' ', 'S'), '\'', 'B')},
        "IDSDB2RecordCount": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSDB2RecordCount", ' ', 'S'), '\'', 'B')},
        "SrcSysCd": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.SrcSysCd", ' ', 'S'), '\'', 'B')},
        "RunID": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.RunID", ' ', 'S'), '\'', 'B')}}
      parameters.update(ctx.parameter_override)
      activity = ExecutePipelineActivity(
        name = "IdsMbrPdxDeniedClmLoad",
        wait_on_completion = True,
        pipeline = PipelineReference(type="PipelineReference",reference_name="IdsMbrPdxDeniedClmLoad"),
        parameters = parameters)
      return [], [activity]
    artifacts = []
    activities = [IfConditionActivity(name = "If_1D",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('IdsMbrPdxDeniedTransLoad', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        IdsMbrPdxDeniedClmLoad()),
      depends_on = [
        ActivityDependency(
          activity = "If_1C",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def Sequencer_675():
    return [], [SetVariableActivity(
      name = "Sequencer_675",
      variable_name = "Sequencer_675_fired",
      value = "y",
      depends_on = [
        ActivityDependency(
          activity = "If_1D",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
  def DeniedClaimsLandingFile():
    activities = [CustomActivity(
      name = "DeniedClaimsLandingFile",
      command = ctx.formatShellCommand('cd' toString($FilePath) + '/verified; mv -f ' + toString($SrcSysCd) + '_DeniedClaims_Landing.dat.' + toString($RunID) + '   processed/;    compress -f   processed/' + toString($SrcSysCd) + '_DeniedClaims_Landing.dat.' + toString($RunID) + ' '),
      depends_on = [
        ActivityDependency(
          activity = "Sequencer_675",
          dependency_conditions = [DependencyCondition.COMPLETED])],
      linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
    return [], activities
  def DeniedPkeyFile():
    activities = [CustomActivity(
      name = "DeniedPkeyFile",
      command = ctx.formatShellCommand('cd' toString($FilePath) + '/key/; mv -f MBR_PDX_DENIED_TRANS.' + toString($SrcSysCd) + '.pkey.' + toString($RunID) + '.dat processed/; compress -f   processed/MBR_PDX_DENIED_TRANS.' + toString($SrcSysCd) + '.pkey.' + toString($RunID) + '.dat'),
      depends_on = [
        ActivityDependency(
          activity = "DeniedClaimsLandingFile",
          dependency_conditions = [DependencyCondition.COMPLETED])],
      linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
    return [], activities
  def DeniedLoadile():
    activities = [CustomActivity(
      name = "DeniedLoadile",
      command = ctx.formatShellCommand('cd' toString($FilePath) + '/load/; mv -f MBR_PDX_DENIED_TRANS.' + toString($SrcSysCd) + '.' + toString($RunID) + '.dat processed/; compress -f   processed/MBR_PDX_DENIED_TRANS.' + toString($SrcSysCd) + '.' + toString($RunID) + '.dat'),
      depends_on = [
        ActivityDependency(
          activity = "DeniedPkeyFile",
          dependency_conditions = [DependencyCondition.COMPLETED])],
      linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
    return [], activities
  return merge(
    DeniedLoadile(),
    DeniedPkeyFile(),
    DeniedClaimsLandingFile(),
    Sequencer_675(),
    If_1D(),
    If_1C(),
    If_1B(),
    If_1A())

def OptumIdsMbrPdxDeniedTransLoadSeq(ctx):
  name = "OptumIdsMbrPdxDeniedTransLoadSeq"
  artifacts, activities = OptumIdsMbrPdxDeniedTransLoadSeqActivities(ctx)
  pipeline = PipelineResource(
    folder = PipelineFolder(name="DataWarehouse/Claim/Seq"),
    activities = activities,
    description = """
      6131 - PBM REPLACEMENT: Extract OPTUMRX Denied data from FILE and load IDS tables
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
      PrefixFkeyFailedFileName:
        PrefixFkeyFailedFileName
      CurrDate:
        CurrDate""",
    parameters = {
      "APT_CONFIG_FILE": ParameterSpecification(type="String", default_value=ctx.APT_CONFIG_FILE),
      "IDSInstance": ParameterSpecification(type="String", default_value=ctx.IDSInstance),
      "IDSDSN": ParameterSpecification(type="String", default_value=ctx.IDSDSN),
      "IDSAcct": ParameterSpecification(type="String", default_value=ctx.IDSAcct),
      "IDSDB": ParameterSpecification(type="String", default_value=ctx.IDSDB),
      "IDSPW": ParameterSpecification(type="String"),
      "IDSOwner": ParameterSpecification(type="String", default_value=ctx.IDSOwner),
      "FilePath": ParameterSpecification(type="String", default_value=ctx.FilePath),
      "SrcSysCd": ParameterSpecification(type="String", default_value="OPTUMRX"),
      "RunID": ParameterSpecification(type="String"),
      "IDSRunCycle": ParameterSpecification(type="String"),
      "IDSDB2ArraySize": ParameterSpecification(type="String", default_value=ctx.IDSDB2ArraySize),
      "IDSDB2RecordCount": ParameterSpecification(type="String", default_value=ctx.IDSDB2RecordCount),
      "ExclusionList": ParameterSpecification(type="String"),
      "PrefixFkeyFailedFileName": ParameterSpecification(type="String", default_value="PrefixFkeyFailedFileName"),
      "CurrDate": ParameterSpecification(type="String")},
    variables = {
      "Sequencer_675_fired": VariableSpecification(type="String", default_value="n")})
  return artifacts + [(name, pipeline)]

def main():
  ctx = TranslationContext()
  artifacts = (OptumIdsMbrPdxDeniedTransLoadSeq(ctx))
  if ctx.validateArtifacts(artifacts):
    ctx.deployArtifacts(artifacts)

if __name__ == '__main__':
  main()
