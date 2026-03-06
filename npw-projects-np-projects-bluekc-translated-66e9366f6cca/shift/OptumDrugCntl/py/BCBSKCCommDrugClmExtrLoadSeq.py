#!/usr/bin/python3

from npadf import *

def BCBSKCCommDrugClmExtrLoadSeqActivities(ctx):
  def If_5A1():
    def BCBSKCCommDrugClmExtr():
      parameters= {
        "CurrentDate": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.CurrentDate", ' ', 'S'), '\'', 'B')},
        "SrcSysCd": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.SrcSysCd", ' ', 'S'), '\'', 'B')},
        "SrcSysCdSk": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.SrcSysCdSK", ' ', 'S'), '\'', 'B')},
        "FilePath": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.FilePath", ' ', 'S'), '\'', 'B')},
        "RunCycle": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.RunCycle", ' ', 'S'), '\'', 'B')},
        "ProvRunCycle": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.ProvRunCycle", ' ', 'S'), '\'', 'B')},
        "RunID": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.RunID", ' ', 'S'), '\'', 'B')},
        "IDSAcct": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSAcct", ' ', 'S'), '\'', 'B')},
        "IDSPW": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSPW", ' ', 'S'), '\'', 'B')},
        "IDSDSN": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSDSN", ' ', 'S'), '\'', 'B')},
        "IDSDB": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSDB", ' ', 'S'), '\'', 'B')},
        "IDSOwner": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSOwner", ' ', 'S'), '\'', 'B')},
        "CactusAcct": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.CactusAcct", ' ', 'S'), '\'', 'B')},
        "CactusPW": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.CactusPW", ' ', 'S'), '\'', 'B')},
        "CactusOwner": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.CactusOwner", ' ', 'S'), '\'', 'B')},
        "CactusServer": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.CactusServer", ' ', 'S'), '\'', 'B')},
        "CactusDB": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.CactusDB", ' ', 'S'), '\'', 'B')}}
      parameters.update(ctx.parameter_override)
      activity = ExecutePipelineActivity(
        name = "BCBSKCCommDrugClmExtr",
        wait_on_completion = True,
        pipeline = PipelineReference(type="PipelineReference",reference_name="BCBSKCCommDrugClmExtr"),
        parameters = parameters)
      return [], [activity]
    artifacts = []
    activities = [IfConditionActivity(name = "If_5A1",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('BCBSKCCommDrugClmExtr', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        BCBSKCCommDrugClmExtr()))]
    return artifacts, activities
  def Sequencer_209():
    return [], [SetVariableActivity(
      name = "Sequencer_209",
      variable_name = "Sequencer_209_fired",
      value = "y",
      depends_on = [
        ActivityDependency(
          activity = "If_5A1",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
  def If_5A():
    def ConcatDrugClmDrug():
      activities = [CustomActivity(
        name = "ConcatDrugClmDrug",
        command = ctx.formatShellCommand('cd' toString($FilePath) + '/scripts; prepare_files.ksh ' + toString($FilePath) + '/key *.DrugClmDrug*  IdsDrugClmDrugExtr.DrugClmDrug ''-k9,9 -k5,5r''  ''-k9,9'' '),
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "If_5A",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('ConcatDrugClmDrug', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        ConcatDrugClmDrug()),
      depends_on = [
        ActivityDependency(
          activity = "Sequencer_209",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def If_5B():
    def IdsDrugClmFkey():
      parameters= {
        "IDSDB": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSDB", ' ', 'S'), '\'', 'B')},
        "Source": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.SrcSysCd", ' ', 'S'), '\'', 'B')},
        "SrcSysCdSk": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.SrcSysCdSK", ' ', 'S'), '\'', 'B')},
        "RunCycle": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.RunCycle", ' ', 'S'), '\'', 'B')},
        "Logging": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.Logging", ' ', 'S'), '\'', 'B')},
        "RunID": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.RunID", ' ', 'S'), '\'', 'B')},
        "FilePath": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.FilePath", ' ', 'S'), '\'', 'B')}}
      parameters.update(ctx.parameter_override)
      activity = ExecutePipelineActivity(
        name = "IdsDrugClmFkey",
        wait_on_completion = True,
        pipeline = PipelineReference(type="PipelineReference",reference_name="IdsDrugClmFkey"),
        parameters = parameters)
      return [], [activity]
    artifacts = []
    activities = [IfConditionActivity(name = "If_5B",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('IdsDrugClmFkey', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        IdsDrugClmFkey()),
      depends_on = [
        ActivityDependency(
          activity = "If_5A",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def If_5C():
    def RmDrugClm():
      activities = [CustomActivity(
        name = "RmDrugClm",
        command = ctx.formatShellCommand('rm' ' -f "' + toString($FilePath) + '/key/IdsDrugClmDrugExtr.DrugClmDrug.uniq" '),
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "If_5C",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('RmDrugClm', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        RmDrugClm()),
      depends_on = [
        ActivityDependency(
          activity = "If_5B",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def If_5F():
    def LoadP_CLM_PDX():
      activities = [CustomActivity(
        name = "LoadP_CLM_PDX",
        command = ctx.getRoutineCommand("DB.LOAD"),
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "If_5F",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('LoadP_CLM_PDX', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        LoadP_CLM_PDX()),
      depends_on = [
        ActivityDependency(
          activity = "If_5C",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def If_5E():
    def LoadW_DRUG_CLM():
      activities = [CustomActivity(
        name = "LoadW_DRUG_CLM",
        command = ctx.getRoutineCommand("DB.LOAD"),
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "If_5E",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('LoadW_DRUG_CLM', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        LoadW_DRUG_CLM()),
      depends_on = [
        ActivityDependency(
          activity = "If_5C",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def If_5D():
    def LoadDrugClm():
      activities = [CustomActivity(
        name = "LoadDrugClm",
        command = ctx.getRoutineCommand("DB.LOAD"),
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "If_5D",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('LoadDrugClm', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        LoadDrugClm()),
      depends_on = [
        ActivityDependency(
          activity = "If_5C",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def If_6A():
    def ConcatNDCDrug():
      activities = [CustomActivity(
        name = "ConcatNDCDrug",
        command = ctx.formatShellCommand('cd' toString($FilePath) + '/scripts; prepare_files.ksh ' + toString($FilePath) + '/key *.DrugNDC* IdsNdcDrugExtr.DrugNDC ''-k9,9 -k5,5r''  ''-k9,9'''),
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "If_6A",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('ConcatNDCDrug', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        ConcatNDCDrug()),
      depends_on = [
        ActivityDependency(
          activity = "Sequencer_209",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def If_6B():
    def IdsNdcFkey():
      parameters= {
        "FilePath": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.FilePath", ' ', 'S'), '\'', 'B')},
        "InFile": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("'IdsNdcDrugExtr.DrugNDC.uniq'", ' ', 'S'), '\'', 'B')},
        "Logging": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("'X'", ' ', 'S'), '\'', 'B')},
        "Source": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.SrcSysCd", ' ', 'S'), '\'', 'B')}}
      parameters.update(ctx.parameter_override)
      activity = ExecutePipelineActivity(
        name = "IdsNdcFkey",
        wait_on_completion = True,
        pipeline = PipelineReference(type="PipelineReference",reference_name="IdsNdcFkey"),
        parameters = parameters)
      return [], [activity]
    artifacts = []
    activities = [IfConditionActivity(name = "If_6B",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('IdsNdcFkey', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        IdsNdcFkey()),
      depends_on = [
        ActivityDependency(
          activity = "If_6A",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def If_6C():
    def RmNDC():
      activities = [CustomActivity(
        name = "RmNDC",
        command = ctx.formatShellCommand('rm' ' -f "' + toString($FilePath) + '/key/IdsNdcDrugExtr.DrugNDC.uniq" '),
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "If_6C",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('RmNDC', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        RmNDC()),
      depends_on = [
        ActivityDependency(
          activity = "If_6B",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def If_6D():
    def LoadNDC():
      activities = [CustomActivity(
        name = "LoadNDC",
        command = ctx.getRoutineCommand("DB.LOAD"),
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "If_6D",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('LoadNDC', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        LoadNDC()),
      depends_on = [
        ActivityDependency(
          activity = "If_6C",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def If_7A():
    def ConcatProvLoc():
      activities = [CustomActivity(
        name = "ConcatProvLoc",
        command = ctx.formatShellCommand('cd' toString($FilePath) + '/scripts; prepare_files.ksh ' + toString($FilePath) + '/key *.ProvLoc*  IdsProvLocExtr.ProvLoc ''-k9,9 -k5,5r''  ''-k9,9'''),
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "If_7A",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('ConcatProvLoc', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        ConcatProvLoc()),
      depends_on = [
        ActivityDependency(
          activity = "Sequencer_209",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def If_7B():
    def IdsProvLocFkey():
      parameters= {
        "InFile": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("'IdsProvLocExtr.ProvLoc.uniq'", ' ', 'S'), '\'', 'B')},
        "Logging": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.Logging", ' ', 'S'), '\'', 'B')},
        "FilePath": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.FilePath", ' ', 'S'), '\'', 'B')}}
      parameters.update(ctx.parameter_override)
      activity = ExecutePipelineActivity(
        name = "IdsProvLocFkey",
        wait_on_completion = True,
        pipeline = PipelineReference(type="PipelineReference",reference_name="IdsProvLocFkey"),
        parameters = parameters)
      return [], [activity]
    artifacts = []
    activities = [IfConditionActivity(name = "If_7B",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('IdsProvLocFkey', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        IdsProvLocFkey()),
      depends_on = [
        ActivityDependency(
          activity = "If_7A",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def If_7C():
    def RmProvLoc():
      activities = [CustomActivity(
        name = "RmProvLoc",
        command = ctx.formatShellCommand('rm' ' -f "' + toString($FilePath) + '/key/IdsProvLocExtr.ProvLoc.uniq" '),
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "If_7C",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('RmProvLoc', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        RmProvLoc()),
      depends_on = [
        ActivityDependency(
          activity = "If_7B",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def If_7D():
    def LoadProvLoc():
      activities = [CustomActivity(
        name = "LoadProvLoc",
        command = ctx.getRoutineCommand("DB.LOAD"),
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "If_7D",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('LoadProvLoc', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        LoadProvLoc()),
      depends_on = [
        ActivityDependency(
          activity = "If_7C",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def If_2A():
    def ConcatDEADrug():
      activities = [CustomActivity(
        name = "ConcatDEADrug",
        command = ctx.formatShellCommand('cd' toString($FilePath) + '/scripts; prepare_files.ksh ' + toString($FilePath) + '/key *.DrugProvDea*  IdsProvDEAExtr.ProvDEA ''-k9,9 -k5,5r''  ''-k9,9'''),
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "If_2A",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('ConcatDEADrug', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        ConcatDEADrug()),
      depends_on = [
        ActivityDependency(
          activity = "Sequencer_209",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def If_2B():
    def IdsProvDEAFkey():
      parameters= {
        "FilePath": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.FilePath", ' ', 'S'), '\'', 'B')},
        "Logging": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("'X'", ' ', 'S'), '\'', 'B')}}
      parameters.update(ctx.parameter_override)
      activity = ExecutePipelineActivity(
        name = "IdsProvDEAFkey",
        wait_on_completion = True,
        pipeline = PipelineReference(type="PipelineReference",reference_name="IdsProvDEAFkey"),
        parameters = parameters)
      return [], [activity]
    artifacts = []
    activities = [IfConditionActivity(name = "If_2B",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('IdsProvDEAFkey', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        IdsProvDEAFkey()),
      depends_on = [
        ActivityDependency(
          activity = "If_2A",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def If_2C():
    def RmProvDea():
      activities = [CustomActivity(
        name = "RmProvDea",
        command = ctx.formatShellCommand('rm' ' -f "' + toString($FilePath) + '/key/IdsProvDEAExtr.ProvDEA.uniq" '),
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "If_2C",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('RmProvDea', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        RmProvDea()),
      depends_on = [
        ActivityDependency(
          activity = "If_2B",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def If_2D():
    def LoadDEA():
      activities = [CustomActivity(
        name = "LoadDEA",
        command = ctx.getRoutineCommand("DB.LOAD"),
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "If_2D",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('LoadDEA', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        LoadDEA()),
      depends_on = [
        ActivityDependency(
          activity = "If_2C",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def If_7E():
    def ConcatProvDrug():
      activities = [CustomActivity(
        name = "ConcatProvDrug",
        command = ctx.formatShellCommand('cd' toString($FilePath) + '/scripts; prepare_files.ksh ' + toString($FilePath) + '/key *.DrugProv*  IdsProvExtr.DrugProv ''-k9,9 -k5,5r''  ''-k9,9'''),
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "If_7E",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('ConcatProvDrug', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        ConcatProvDrug()),
      depends_on = [
        ActivityDependency(
          activity = "Sequencer_209",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def If_7F():
    def IdsProvFkey():
      parameters= {
        "FilePath": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.FilePath", ' ', 'S'), '\'', 'B')},
        "InFile": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("'IdsProvExtr.DrugProv.uniq'", ' ', 'S'), '\'', 'B')},
        "Logging": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("'X'", ' ', 'S'), '\'', 'B')}}
      parameters.update(ctx.parameter_override)
      activity = ExecutePipelineActivity(
        name = "IdsProvFkey",
        wait_on_completion = True,
        pipeline = PipelineReference(type="PipelineReference",reference_name="IdsProvFkey"),
        parameters = parameters)
      return [], [activity]
    artifacts = []
    activities = [IfConditionActivity(name = "If_7F",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('IdsProvFkey', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        IdsProvFkey()),
      depends_on = [
        ActivityDependency(
          activity = "If_7E",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def If_7G():
    def RmProv():
      activities = [CustomActivity(
        name = "RmProv",
        command = ctx.formatShellCommand('rm' ' -f "' + toString($FilePath) + '/key/IdsProvExtr.DrugProv.uniq" '),
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "If_7G",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('RmProv', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        RmProv()),
      depends_on = [
        ActivityDependency(
          activity = "If_7F",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def If_7H():
    def LoadProv():
      activities = [CustomActivity(
        name = "LoadProv",
        command = ctx.getRoutineCommand("DB.LOAD"),
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "If_7H",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('LoadProv', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        LoadProv()),
      depends_on = [
        ActivityDependency(
          activity = "If_7G",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  return merge(
    If_7H(),
    If_7G(),
    If_7F(),
    If_7E(),
    If_2D(),
    If_2C(),
    If_2B(),
    If_2A(),
    If_7D(),
    If_7C(),
    If_7B(),
    If_7A(),
    If_6D(),
    If_6C(),
    If_6B(),
    If_6A(),
    If_5D(),
    If_5E(),
    If_5F(),
    If_5C(),
    If_5B(),
    If_5A(),
    Sequencer_209(),
    If_5A1())

def BCBSKCCommDrugClmExtrLoadSeq(ctx):
  name = "BCBSKCCommDrugClmExtrLoadSeq"
  artifacts, activities = BCBSKCCommDrugClmExtrLoadSeqActivities(ctx)
  pipeline = PipelineResource(
    folder = PipelineFolder(name="claim/SeqBCBSKCCommon"),
    activities = activities,
    description = """
      Parameters:
      -----------
      CurrentDate:
        CurrentDate
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
      SrcSysCd:
        source system
      SrcSysCdSK:
        SrcSysCdSK
      IDSAcct:
        IDS DB Account
      IDSPW:
        IDS DB Password
      IDSInstance:
        IDS DB Instance
      IDSOwner:
        IDS DB Owner
      IDSDB:
        IDS Database
      IDSDSN:
        IDS DSN
      CactusAcct:
        Cactus Account
      CactusPW:
        Cactus Password
      CactusDB:
        Cactus Database
      CactusServer:
        Cactus Server
      CactusOwner:
        Cactus Table Owner
      ProvRunCycle:
        ProvRunCycle""",
    parameters = {
      "CurrentDate": ParameterSpecification(type="String", default_value="2008-10-01"),
      "FilePath": ParameterSpecification(type="String", default_value=ctx.FilePath),
      "Logging": ParameterSpecification(type="String", default_value="N"),
      "RunCycle": ParameterSpecification(type="Int"),
      "RunID": ParameterSpecification(type="String"),
      "ExclusionList": ParameterSpecification(type="String", default_value="\"\""),
      "SrcSysCd": ParameterSpecification(type="String"),
      "SrcSysCdSK": ParameterSpecification(type="Int"),
      "IDSAcct": ParameterSpecification(type="String"),
      "IDSPW": ParameterSpecification(type="String"),
      "IDSInstance": ParameterSpecification(type="String", default_value=ctx.IDSInstance),
      "IDSOwner": ParameterSpecification(type="String", default_value=ctx.IDSOwner),
      "IDSDB": ParameterSpecification(type="String", default_value=ctx.IDSDB),
      "IDSDSN": ParameterSpecification(type="String", default_value=ctx.IDSDSN),
      "CactusAcct": ParameterSpecification(type="String"),
      "CactusPW": ParameterSpecification(type="String"),
      "CactusDB": ParameterSpecification(type="String", default_value=ctx.CactusDB),
      "CactusServer": ParameterSpecification(type="String", default_value=ctx.CactusServer),
      "CactusOwner": ParameterSpecification(type="String", default_value=ctx.CactusOwner),
      "ProvRunCycle": ParameterSpecification(type="Int")},
    variables = {
      "If_2D_fired": VariableSpecification(type="String", default_value="n"),
      "If_5D_fired": VariableSpecification(type="String", default_value="n"),
      "If_5E_fired": VariableSpecification(type="String", default_value="n"),
      "If_5F_fired": VariableSpecification(type="String", default_value="n"),
      "If_6D_fired": VariableSpecification(type="String", default_value="n"),
      "If_7D_fired": VariableSpecification(type="String", default_value="n"),
      "If_7H_fired": VariableSpecification(type="String", default_value="n"),
      "Sequencer_209_fired": VariableSpecification(type="String", default_value="n")})
  return artifacts + [(name, pipeline)]

def main():
  ctx = TranslationContext()
  artifacts = (BCBSKCCommDrugClmExtrLoadSeq(ctx))
  if ctx.validateArtifacts(artifacts):
    ctx.deployArtifacts(artifacts)

if __name__ == '__main__':
  main()
