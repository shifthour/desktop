#!/usr/bin/python3

from npadf import *

def BCBSKCCommClmLoadSeqActivities(ctx):
  def If_3A():
    def ConcatClmProvDrug():
      activities = [CustomActivity(
        name = "ConcatClmProvDrug",
        command = ctx.formatShellCommand('cd' toString($FilePath) + '/scripts; prepare_files.ksh ' + toString($FilePath) + '/key  *.DrugClmProv*  IdsClmProvDrugExtr.DrugClmProv ''-k9,9 -k5,5r''  ''-k9,9'''),
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "If_3A",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('ConcatClmProvDrug', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        ConcatClmProvDrug()))]
    return artifacts, activities
  def If_3B():
    def IdsClmProvFkey():
      parameters= {
        "FilePath": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.FilePath", ' ', 'S'), '\'', 'B')},
        "Logging": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.Logging", ' ', 'S'), '\'', 'B')},
        "Source": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.SrcSysCd", ' ', 'S'), '\'', 'B')},
        "InFile": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("'IdsClmProvDrugExtr.DrugClmProv.uniq'", ' ', 'S'), '\'', 'B')}}
      parameters.update(ctx.parameter_override)
      activity = ExecutePipelineActivity(
        name = "IdsClmProvFkey",
        wait_on_completion = True,
        pipeline = PipelineReference(type="PipelineReference",reference_name="IdsClmProvFkey"),
        parameters = parameters)
      return [], [activity]
    artifacts = []
    activities = [IfConditionActivity(name = "If_3B",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('IdsClmProvFkey', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        IdsClmProvFkey()),
      depends_on = [
        ActivityDependency(
          activity = "If_3A",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def If_3C():
    def RmClmProv():
      activities = [CustomActivity(
        name = "RmClmProv",
        command = ctx.formatShellCommand('rm' ' -f "' + toString($FilePath) + '/key/IdsClmProvDrugExtr.DrugClmProv.uniq" '),
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "If_3C",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('RmClmProv', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        RmClmProv()),
      depends_on = [
        ActivityDependency(
          activity = "If_3B",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def If_3D():
    return [], [SetVariableActivity(
      name = "If_3D",
      variable_name = "If_3D_fired",
      value = "'y'",
      depends_on = [
        ActivityDependency(
          activity = "If_3C",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
  def SW2_If_3D():
    def K_CLM_PROV():
      activities = [CustomActivity(
        name = "K_CLM_PROV",
        command = ctx.getRoutineCommand("DB.LOAD"),
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "SW2_If_3D",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('K_CLM_PROV', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        K_CLM_PROV()),
      depends_on = [
        ActivityDependency(
          activity = "If_3D",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def SW1_If_3D():
    def LoadClmProv():
      activities = [CustomActivity(
        name = "LoadClmProv",
        command = ctx.getRoutineCommand("DB.LOAD"),
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "SW1_If_3D",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('LoadClmProv', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        LoadClmProv()),
      depends_on = [
        ActivityDependency(
          activity = "If_3D",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def If_2A():
    def ConcatClmLnDrug():
      activities = [CustomActivity(
        name = "ConcatClmLnDrug",
        command = ctx.formatShellCommand('cd' toString($FilePath) + '/scripts; prepare_files.ksh ' + toString($FilePath) + '/key *.DrugClmLn*  IdsClmLnDrugExtr.DrugClmLn ''-k9,9 -k5,5r''  ''-k9,9'''),
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "If_2A",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('ConcatClmLnDrug', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        ConcatClmLnDrug()))]
    return artifacts, activities
  def If_2B():
    def IdsClmLnFkey():
      parameters= {
        "FilePath": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.FilePath", ' ', 'S'), '\'', 'B')},
        "RunCycle": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.RunCycle", ' ', 'S'), '\'', 'B')},
        "Logging": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.Logging", ' ', 'S'), '\'', 'B')},
        "Source": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.SrcSysCd", ' ', 'S'), '\'', 'B')},
        "InFile": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("'IdsClmLnDrugExtr.DrugClmLn.uniq'", ' ', 'S'), '\'', 'B')}}
      parameters.update(ctx.parameter_override)
      activity = ExecutePipelineActivity(
        name = "IdsClmLnFkey",
        wait_on_completion = True,
        pipeline = PipelineReference(type="PipelineReference",reference_name="IdsClmLnFkey"),
        parameters = parameters)
      return [], [activity]
    artifacts = []
    activities = [IfConditionActivity(name = "If_2B",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('IdsClmLnFkey', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        IdsClmLnFkey()),
      depends_on = [
        ActivityDependency(
          activity = "If_2A",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def If_2C():
    def RmClmLn():
      activities = [CustomActivity(
        name = "RmClmLn",
        command = ctx.formatShellCommand('rm' ' -f "' + toString($FilePath) + '/key/IdsClmLnDrugExtr.DrugClmLn.uniq" '),
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "If_2C",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('RmClmLn', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        RmClmLn()),
      depends_on = [
        ActivityDependency(
          activity = "If_2B",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def If_2D():
    def LoadClmLn():
      activities = [CustomActivity(
        name = "LoadClmLn",
        command = ctx.getRoutineCommand("DB.LOAD"),
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "If_2D",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('LoadClmLn', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        LoadClmLn()),
      depends_on = [
        ActivityDependency(
          activity = "If_2C",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def If_1A():
    def ConcatClmDrug():
      activities = [CustomActivity(
        name = "ConcatClmDrug",
        command = ctx.formatShellCommand('cd' toString($FilePath) + '/scripts; prepare_files.ksh ' + toString($FilePath) + '/key *.DrugClm.* IdsClmDrugExtr.DrugClm ''-k9,9 -k5,5r''  ''-k9,9'''),
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "If_1A",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('ConcatClmDrug', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        ConcatClmDrug()))]
    return artifacts, activities
  def If_1B():
    def IdsClmFkey():
      parameters= {
        "FilePath": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.FilePath", ' ', 'S'), '\'', 'B')},
        "Source": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.SrcSysCd", ' ', 'S'), '\'', 'B')},
        "SrcSysCdSk": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.SrcSysCdSk", ' ', 'S'), '\'', 'B')},
        "InFile": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("'IdsClmDrugExtr.DrugClm.uniq'", ' ', 'S'), '\'', 'B')},
        "Logging": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.Logging", ' ', 'S'), '\'', 'B')},
        "RunCycle": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.RunCycle", ' ', 'S'), '\'', 'B')},
        "RunID": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.RunID", ' ', 'S'), '\'', 'B')},
        "IDSInstance": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSInstance", ' ', 'S'), '\'', 'B')},
        "IDSDB": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSDB", ' ', 'S'), '\'', 'B')},
        "IDSOwner": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSOwner", ' ', 'S'), '\'', 'B')},
        "IDSAcct": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSAcct", ' ', 'S'), '\'', 'B')},
        "IDSPW": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSPW", ' ', 'S'), '\'', 'B')}}
      parameters.update(ctx.parameter_override)
      activity = ExecutePipelineActivity(
        name = "IdsClmFkey",
        wait_on_completion = True,
        pipeline = PipelineReference(type="PipelineReference",reference_name="IdsClmFkey"),
        parameters = parameters)
      return [], [activity]
    artifacts = []
    activities = [IfConditionActivity(name = "If_1B",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('IdsClmFkey', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        IdsClmFkey()),
      depends_on = [
        ActivityDependency(
          activity = "If_1A",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def If_1C():
    def RmClm():
      activities = [CustomActivity(
        name = "RmClm",
        command = ctx.formatShellCommand('rm' ' -f "' + toString($FilePath) + '/key/IdsClmDrugExtr.DrugClm.uniq" '),
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "If_1C",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('RmClm', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        RmClm()),
      depends_on = [
        ActivityDependency(
          activity = "If_1B",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def If_1D():
    def LoadClm():
      activities = [CustomActivity(
        name = "LoadClm",
        command = ctx.getRoutineCommand("DB.LOAD"),
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "If_1D",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('LoadClm', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        LoadClm()),
      depends_on = [
        ActivityDependency(
          activity = "If_1C",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def If_4A():
    def ConcatClmRemitHistDrug():
      activities = [CustomActivity(
        name = "ConcatClmRemitHistDrug",
        command = ctx.formatShellCommand('cd' toString($FilePath) + '/scripts; prepare_files.ksh ' + toString($FilePath) + '/key *.DrugClmRemitHist  IdsClmRemitHistDrugExtr.DrugClmRemitHist ''-k9,9 -k5,5r''  ''-k9,9'''),
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "If_4A",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('ConcatClmRemitHistDrug', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        ConcatClmRemitHistDrug()))]
    return artifacts, activities
  def If_4B():
    def IdsClmRemitHistFkey():
      parameters= {
        "FilePath": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.FilePath", ' ', 'S'), '\'', 'B')},
        "Source": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.SrcSysCd", ' ', 'S'), '\'', 'B')},
        "SrcSysCdSk": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.SrcSysCdSk", ' ', 'S'), '\'', 'B')},
        "InFile": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("'IdsClmRemitHistDrugExtr.DrugClmRemitHist.uniq'", ' ', 'S'), '\'', 'B')},
        "Logging": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.Logging", ' ', 'S'), '\'', 'B')}}
      parameters.update(ctx.parameter_override)
      activity = ExecutePipelineActivity(
        name = "IdsClmRemitHistFkey",
        wait_on_completion = True,
        pipeline = PipelineReference(type="PipelineReference",reference_name="IdsClmRemitHistFkey"),
        parameters = parameters)
      return [], [activity]
    artifacts = []
    activities = [IfConditionActivity(name = "If_4B",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('IdsClmRemitHistFkey', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        IdsClmRemitHistFkey()),
      depends_on = [
        ActivityDependency(
          activity = "If_4A",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def If_4C():
    def RmClmRemitHist():
      activities = [CustomActivity(
        name = "RmClmRemitHist",
        command = ctx.formatShellCommand('rm' ' -f "' + toString($FilePath) + '/key/IdsClmRemitHistDrugExtr.DrugClmRemitHist.uniq" '),
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "If_4C",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('RmClmRemitHist', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        RmClmRemitHist()),
      depends_on = [
        ActivityDependency(
          activity = "If_4B",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def If_4D():
    def LoadClmRemitHist():
      activities = [CustomActivity(
        name = "LoadClmRemitHist",
        command = ctx.getRoutineCommand("DB.LOAD"),
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "If_4D",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('LoadClmRemitHist', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        LoadClmRemitHist()),
      depends_on = [
        ActivityDependency(
          activity = "If_4C",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  return merge(
    If_4D(),
    If_4C(),
    If_4B(),
    If_4A(),
    If_1D(),
    If_1C(),
    If_1B(),
    If_1A(),
    If_2D(),
    If_2C(),
    If_2B(),
    If_2A(),
    SW1_If_3D(),
    SW2_If_3D(),
    If_3D(),
    If_3C(),
    If_3B(),
    If_3A())

def BCBSKCCommClmLoadSeq(ctx):
  name = "BCBSKCCommClmLoadSeq"
  artifacts, activities = BCBSKCCommClmLoadSeqActivities(ctx)
  pipeline = PipelineResource(
    folder = PipelineFolder(name="claim/SeqBCBSKCCommon"),
    activities = activities,
    description = """
      Parameters:
      -----------
      FilePath:
        File Path
      IDSDSN:
        IDS ODBC DSN
      IDSInstance:
        IDS Instance
      IDSDB:
        IDS Database
      IDSOwner:
        IDS Table Owner
      IDSAcct:
        IDS Account
      IDSPW:
        IDS Password
      Logging:
        Logging
      RunCycle:
        RunCycle
      RunID:
        RunID
      ExclusionList:
        ExclusionList
      SrcSysCd:
        SourceSys
      SrcSysCdSk:
        SrcSysCdSk""",
    parameters = {
      "FilePath": ParameterSpecification(type="String", default_value=ctx.FilePath),
      "IDSDSN": ParameterSpecification(type="String", default_value=ctx.IDSDSN),
      "IDSInstance": ParameterSpecification(type="String", default_value=ctx.IDSInstance),
      "IDSDB": ParameterSpecification(type="String", default_value=ctx.IDSDB),
      "IDSOwner": ParameterSpecification(type="String", default_value=ctx.IDSOwner),
      "IDSAcct": ParameterSpecification(type="String"),
      "IDSPW": ParameterSpecification(type="String"),
      "Logging": ParameterSpecification(type="String", default_value="X"),
      "RunCycle": ParameterSpecification(type="String"),
      "RunID": ParameterSpecification(type="String"),
      "ExclusionList": ParameterSpecification(type="String", default_value="\"\""),
      "SrcSysCd": ParameterSpecification(type="String"),
      "SrcSysCdSk": ParameterSpecification(type="Int")},
    variables = {
      "If_1D_fired": VariableSpecification(type="String", default_value="n"),
      "If_2D_fired": VariableSpecification(type="String", default_value="n"),
      "If_3D_fired": VariableSpecification(type="String", default_value="n")})
  return artifacts + [(name, pipeline)]

def main():
  ctx = TranslationContext()
  artifacts = (BCBSKCCommClmLoadSeq(ctx))
  if ctx.validateArtifacts(artifacts):
    ctx.deployArtifacts(artifacts)

if __name__ == '__main__':
  main()
