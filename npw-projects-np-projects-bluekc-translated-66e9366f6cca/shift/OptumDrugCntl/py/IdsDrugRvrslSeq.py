#!/usr/bin/python3

from npadf import *

def IdsDrugRvrslSeqActivities(ctx):
  def If_1A():
    def IdsDrugRvrslBuild():
      parameters= {
        "FilePath": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.FilePath", ' ', 'S'), '\'', 'B')},
        "Source": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.Source", ' ', 'S'), '\'', 'B')},
        "IDSInstance": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSInstance", ' ', 'S'), '\'', 'B')},
        "IDSDB": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSDB", ' ', 'S'), '\'', 'B')},
        "IDSOwner": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSOwner", ' ', 'S'), '\'', 'B')},
        "IDSAcct": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSAcct", ' ', 'S'), '\'', 'B')},
        "IDSPW": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSPW", ' ', 'S'), '\'', 'B')}}
      parameters.update(ctx.parameter_override)
      activity = ExecutePipelineActivity(
        name = "IdsDrugRvrslBuild",
        wait_on_completion = True,
        pipeline = PipelineReference(type="PipelineReference",reference_name="IdsDrugRvrslBuild"),
        parameters = parameters)
      return [], [activity]
    artifacts = []
    activities = [IfConditionActivity(name = "If_1A",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('IdsDrugRvrslBuild', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        IdsDrugRvrslBuild()))]
    return artifacts, activities
  def If_1B():
    def LoadW_CLM_RVRSL():
      activities = [CustomActivity(
        name = "LoadW_CLM_RVRSL",
        command = ctx.getRoutineCommand("DB.LOAD"),
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "If_1B",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('LoadW_CLM_RVRSL', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        LoadW_CLM_RVRSL()),
      depends_on = [
        ActivityDependency(
          activity = "If_1A",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def If_1C():
    def IdsDrugRvrslExtr():
      parameters= {
        "FilePath": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.FilePath", ' ', 'S'), '\'', 'B')},
        "RunID": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.RunID", ' ', 'S'), '\'', 'B')},
        "Source": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.Source", ' ', 'S'), '\'', 'B')},
        "IDSInstance": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSInstance", ' ', 'S'), '\'', 'B')},
        "IDSDB": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSDB", ' ', 'S'), '\'', 'B')},
        "IDSOwner": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSOwner", ' ', 'S'), '\'', 'B')},
        "IDSAcct": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSAcct", ' ', 'S'), '\'', 'B')},
        "IDSPW": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSPW", ' ', 'S'), '\'', 'B')}}
      parameters.update(ctx.parameter_override)
      activity = ExecutePipelineActivity(
        name = "IdsDrugRvrslExtr",
        wait_on_completion = True,
        pipeline = PipelineReference(type="PipelineReference",reference_name="IdsDrugRvrslExtr"),
        parameters = parameters)
      return [], [activity]
    artifacts = []
    activities = [IfConditionActivity(name = "If_1C",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('IdsDrugRvrslExtr', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        IdsDrugRvrslExtr()),
      depends_on = [
        ActivityDependency(
          activity = "If_1B",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def If_6C():
    def LoadDrugClmPriceReversal():
      activities = [CustomActivity(
        name = "LoadDrugClmPriceReversal",
        command = ctx.getRoutineCommand("DB.LOAD"),
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "If_6C",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('LoadDrugClmPriceReversal', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        LoadDrugClmPriceReversal()),
      depends_on = [
        ActivityDependency(
          activity = "If_1C",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def If_2C():
    def LoadClmLnReversal():
      activities = [CustomActivity(
        name = "LoadClmLnReversal",
        command = ctx.getRoutineCommand("DB.LOAD"),
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "If_2C",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('LoadClmLnReversal', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        LoadClmLnReversal()),
      depends_on = [
        ActivityDependency(
          activity = "If_1C",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def If_3C():
    def LoadClmRemitHistReversal():
      activities = [CustomActivity(
        name = "LoadClmRemitHistReversal",
        command = ctx.getRoutineCommand("DB.LOAD"),
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "If_3C",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('LoadClmRemitHistReversal', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        LoadClmRemitHistReversal()),
      depends_on = [
        ActivityDependency(
          activity = "If_1C",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def If_4C():
    def LoadDrugClmReversal():
      activities = [CustomActivity(
        name = "LoadDrugClmReversal",
        command = ctx.getRoutineCommand("DB.LOAD"),
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "If_4C",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('LoadDrugClmReversal', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        LoadDrugClmReversal()),
      depends_on = [
        ActivityDependency(
          activity = "If_1C",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def If_5C():
    def LoadClmCobReversal():
      activities = [CustomActivity(
        name = "LoadClmCobReversal",
        command = ctx.getRoutineCommand("DB.LOAD"),
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "If_5C",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('LoadClmCobReversal', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        LoadClmCobReversal()),
      depends_on = [
        ActivityDependency(
          activity = "If_1C",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def If_1D():
    def ConcatClmReversal():
      activities = [CustomActivity(
        name = "ConcatClmReversal",
        command = ctx.formatShellCommand('cat' toString($FilePath) + '/load/CLM_REVERSAL_DRUG*  > ' + toString($FilePath) + '/load/CLM.' + toString($Source) + '.dat'),
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "If_1D",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('ConcatClmReversal', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        ConcatClmReversal()),
      depends_on = [
        ActivityDependency(
          activity = "If_1C",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def If_1E():
    def LoadClm():
      activities = [CustomActivity(
        name = "LoadClm",
        command = ctx.getRoutineCommand("DB.LOAD"),
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "If_1E",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('LoadClm', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        LoadClm()),
      depends_on = [
        ActivityDependency(
          activity = "If_1D",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def If_1F():
    def RmClmReversal():
      activities = [CustomActivity(
        name = "RmClmReversal",
        command = ctx.formatShellCommand('rm' ' -f ' + toString($FilePath) + '/load/CLM_REVERSAL_DRUG*'),
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "If_1F",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('RmClmReversal', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        RmClmReversal()),
      depends_on = [
        ActivityDependency(
          activity = "If_1E",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  return merge(
    If_1F(),
    If_1E(),
    If_1D(),
    If_5C(),
    If_4C(),
    If_3C(),
    If_2C(),
    If_6C(),
    If_1C(),
    If_1B(),
    If_1A())

def IdsDrugRvrslSeq(ctx):
  name = "IdsDrugRvrslSeq"
  artifacts, activities = IdsDrugRvrslSeqActivities(ctx)
  pipeline = PipelineResource(
    folder = PipelineFolder(name="claim/SeqDrug"),
    activities = activities,
    description = """
      IDS Drug Claim Reversal Process
      Parameters:
      -----------
      FilePath:
        File Path
      Logging:
        Error Log Indicator
      RunCycle:
        Run Cycle
      RunID:
        RunID
      ExclusionList:
        Exclusion List
      Source:
        Source System
      IDSInstance:
        IDS DB Instance
      IDSDB:
        IDS Database
      IDSDSN:
        IDS DSN
      IDSOwner:
        IDS DB Owner
      IDSAcct:
        IDS DB Account
      IDSPW:
        IDS DB Password""",
    parameters = {
      "FilePath": ParameterSpecification(type="String", default_value=ctx.FilePath),
      "Logging": ParameterSpecification(type="String", default_value="Y"),
      "RunCycle": ParameterSpecification(type="Int"),
      "RunID": ParameterSpecification(type="String"),
      "ExclusionList": ParameterSpecification(type="String", default_value="\"\""),
      "Source": ParameterSpecification(type="String"),
      "IDSInstance": ParameterSpecification(type="String", default_value=ctx.IDSInstance),
      "IDSDB": ParameterSpecification(type="String", default_value=ctx.IDSDB),
      "IDSDSN": ParameterSpecification(type="String", default_value=ctx.IDSDSN),
      "IDSOwner": ParameterSpecification(type="String", default_value=ctx.IDSOwner),
      "IDSAcct": ParameterSpecification(type="String"),
      "IDSPW": ParameterSpecification(type="String")},
    variables = {
      "If_1F_fired": VariableSpecification(type="String", default_value="n"),
      "If_2C_fired": VariableSpecification(type="String", default_value="n"),
      "If_3C_fired": VariableSpecification(type="String", default_value="n"),
      "If_4C_fired": VariableSpecification(type="String", default_value="n"),
      "If_5C_fired": VariableSpecification(type="String", default_value="n"),
      "If_6C_fired": VariableSpecification(type="String", default_value="n")})
  return artifacts + [(name, pipeline)]

def main():
  ctx = TranslationContext()
  artifacts = (IdsDrugRvrslSeq(ctx))
  if ctx.validateArtifacts(artifacts):
    ctx.deployArtifacts(artifacts)

if __name__ == '__main__':
  main()
