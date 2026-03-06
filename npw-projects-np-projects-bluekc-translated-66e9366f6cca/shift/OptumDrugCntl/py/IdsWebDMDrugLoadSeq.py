#!/usr/bin/python3

from npadf import *

def L1_IdsWebDMDrugLoadSeqActivities(ctx):
  def StartLoop():

    artifacts = []
    activities = [ForEachActivity(
      name = "StartLoop",
      is_sequential = True,
      items = Expression(type="Expression", value="@range(1, add(sub(variables('LinkCount'), 1), 1))"),
      activities = collect(artifacts,
    ))]
    return artifacts, activities
  return merge(
    StartLoop())

def L1_IdsWebDMDrugLoadSeq(ctx):
  name = "L1_IdsWebDMDrugLoadSeq"
  artifacts, activities = L1_IdsWebDMDrugLoadSeqActivities(ctx)
  pipeline = PipelineResource(
    folder = PipelineFolder(name="claim/SeqDrug"),
    activities = activities,
    description = """
      Job to run FOR EACH activity StartLoop outside  of IF activity
      Parameters:
      -----------
      FilePath:
        File Path
      ExclusionList:
        Exclusion List
      RunID:
        RunID
      RunCycle:
        Run Cycle
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
        IDS DB Password
      ClmMartServer:
        Claim Mart Server
      ClmMartDB:
        Claim Mart Database
      ClmMartOwner:
        Claim Mart Table Owner
      ClmMartAcct:
        Claim Mart Account
      ClmMartPW:
        Claim Mart Password""",
    parameters = {
      "FilePath": ParameterSpecification(type="String", default_value=ctx.FilePath),
      "ExclusionList": ParameterSpecification(type="String", default_value="\"\""),
      "RunID": ParameterSpecification(type="String"),
      "RunCycle": ParameterSpecification(type="String"),
      "IDSInstance": ParameterSpecification(type="String", default_value=ctx.IDSInstance),
      "IDSDB": ParameterSpecification(type="String", default_value=ctx.IDSDB),
      "IDSDSN": ParameterSpecification(type="String", default_value=ctx.IDSDSN),
      "IDSOwner": ParameterSpecification(type="String", default_value=ctx.IDSOwner),
      "IDSAcct": ParameterSpecification(type="String"),
      "IDSPW": ParameterSpecification(type="String"),
      "ClmMartServer": ParameterSpecification(type="String", default_value=ctx.ClmMartServer),
      "ClmMartDB": ParameterSpecification(type="String", default_value=ctx.ClmMartDB),
      "ClmMartOwner": ParameterSpecification(type="String", default_value=ctx.ClmMartOwner),
      "ClmMartAcct": ParameterSpecification(type="String"),
      "ClmMartPW": ParameterSpecification(type="String")},
    variables = {
      "S1_All_fired": VariableSpecification(type="String", default_value="n"),
      "S1_If_3B_fired": VariableSpecification(type="String", default_value="n"),
      "LinkCount": VariableSpecification(type="String")})
  return artifacts + [(name, pipeline)]

def IdsWebDMDrugLoadSeqActivities(ctx):
  def If_2A():
    def IdsClmMartClmLnExtr():
      parameters= {
        "FilePath": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.FilePath", ' ', 'S'), '\'', 'B')},
        "RunCycle": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.RunCycle", ' ', 'S'), '\'', 'B')},
        "IDSInstance": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSInstance", ' ', 'S'), '\'', 'B')},
        "IDSDB": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSDB", ' ', 'S'), '\'', 'B')},
        "IDSOwner": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSOwner", ' ', 'S'), '\'', 'B')},
        "IDSAcct": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSAcct", ' ', 'S'), '\'', 'B')},
        "IDSPW": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSPW", ' ', 'S'), '\'', 'B')},
        "ClmMartServer": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.ClmMartServer", ' ', 'S'), '\'', 'B')},
        "ClmMartDB": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.ClmMartDB", ' ', 'S'), '\'', 'B')},
        "ClmMartOwner": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.ClmMartOwner", ' ', 'S'), '\'', 'B')},
        "ClmMartAcct": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.ClmMartAcct", ' ', 'S'), '\'', 'B')},
        "ClmMartPW": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.ClmMartPW", ' ', 'S'), '\'', 'B')}}
      parameters.update(ctx.parameter_override)
      activity = ExecutePipelineActivity(
        name = "IdsClmMartClmLnExtr",
        wait_on_completion = True,
        pipeline = PipelineReference(type="PipelineReference",reference_name="IdsClmMartClmLnExtr"),
        parameters = parameters)
      return [], [activity]
    artifacts = []
    activities = [IfConditionActivity(name = "If_2A",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('IdsClmMartClmLnExtr', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        IdsClmMartClmLnExtr()))]
    return artifacts, activities
  def If_1A():
    def IdsClmMartClmExtr():
      parameters= {
        "FilePath": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.FilePath", ' ', 'S'), '\'', 'B')},
        "CommitPoint": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("5000", ' ', 'S'), '\'', 'B')},
        "RunCycle": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.RunCycle", ' ', 'S'), '\'', 'B')},
        "ClmMartServer": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.ClmMartServer", ' ', 'S'), '\'', 'B')},
        "ClmMartDB": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.ClmMartDB", ' ', 'S'), '\'', 'B')},
        "ClmMartOwner": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.ClmMartOwner", ' ', 'S'), '\'', 'B')},
        "ClmMartAcct": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.ClmMartAcct", ' ', 'S'), '\'', 'B')},
        "ClmMartPW": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.ClmMartPW", ' ', 'S'), '\'', 'B')},
        "IDSInstance": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSInstance", ' ', 'S'), '\'', 'B')},
        "IDSDB": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSDB", ' ', 'S'), '\'', 'B')},
        "IDSOwner": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSOwner", ' ', 'S'), '\'', 'B')},
        "IDSAcct": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSAcct", ' ', 'S'), '\'', 'B')},
        "IDSPW": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSPW", ' ', 'S'), '\'', 'B')}}
      parameters.update(ctx.parameter_override)
      activity = ExecutePipelineActivity(
        name = "IdsClmMartClmExtr",
        wait_on_completion = True,
        pipeline = PipelineReference(type="PipelineReference",reference_name="IdsClmMartClmExtr"),
        parameters = parameters)
      return [], [activity]
    artifacts = []
    activities = [IfConditionActivity(name = "If_1A",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('IdsClmMartClmExtr', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        IdsClmMartClmExtr()))]
    return artifacts, activities
  def LinkCount():
    return [], [SetVariableActivity(
      name = "LinkCount",
      variable_name = "LinkCount",
      value = "@JobLinkCount('IdsClmMartClmExtr', 'Trans2', 'Count')",
      depends_on = [
        ActivityDependency(
          activity = "If_1A",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
  def If_1B():
    def StartLoop():
      parameters= {
        "ClmMartAcct": {"type": "Expression", "value": "@variables('ClmMartAcct')"},
        "ClmMartDB": {"type": "Expression", "value": "@variables('ClmMartDB')"},
        "ClmMartOwner": {"type": "Expression", "value": "@variables('ClmMartOwner')"},
        "ClmMartPW": {"type": "Expression", "value": "@variables('ClmMartPW')"},
        "ClmMartServer": {"type": "Expression", "value": "@variables('ClmMartServer')"},
        "ExclusionList": {"type": "Expression", "value": "@variables('ExclusionList')"},
        "FilePath": {"type": "Expression", "value": "@variables('FilePath')"},
        "IDSAcct": {"type": "Expression", "value": "@variables('IDSAcct')"},
        "IDSDB": {"type": "Expression", "value": "@variables('IDSDB')"},
        "IDSDSN": {"type": "Expression", "value": "@variables('IDSDSN')"},
        "IDSInstance": {"type": "Expression", "value": "@variables('IDSInstance')"},
        "IDSOwner": {"type": "Expression", "value": "@variables('IDSOwner')"},
        "IDSPW": {"type": "Expression", "value": "@variables('IDSPW')"},
        "RunCycle": {"type": "Expression", "value": "@variables('RunCycle')"},
        "RunID": {"type": "Expression", "value": "@variables('RunID')"}}
      parameters.update(ctx.parameter_override)
      activity = ExecutePipelineActivity(
        name = "StartLoop",
        wait_on_completion = True,
        pipeline = PipelineReference(type="PipelineReference",reference_name="L1_IdsWebDMDrugLoadSeq"),
        parameters = parameters)
      return [], [activity]
    def IdsClmMartClmInitUpd():
      parameters= {
        "FilePath": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.FilePath", ' ', 'S'), '\'', 'B')},
        "CommitCnt": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@item()", ' ', 'S'), '\'', 'B')},
        "CommitCnt2": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@add(1, )", ' ', 'S'), '\'', 'B')},
        "CommitCnt3": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@add(2, )", ' ', 'S'), '\'', 'B')},
        "CommitCnt4": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@add(3, )", ' ', 'S'), '\'', 'B')},
        "ClmMartServer": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.ClmMartServer", ' ', 'S'), '\'', 'B')},
        "RunCycle": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.RunCycle", ' ', 'S'), '\'', 'B')},
        "ClmMartDB": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.ClmMartDB", ' ', 'S'), '\'', 'B')},
        "ClmMartOwner": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.ClmMartOwner", ' ', 'S'), '\'', 'B')},
        "ClmMartAcct": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.ClmMartAcct", ' ', 'S'), '\'', 'B')},
        "ClmMartPW": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.ClmMartPW", ' ', 'S'), '\'', 'B')},
        "IDSInstance": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSInstance", ' ', 'S'), '\'', 'B')},
        "IDSDB": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSDB", ' ', 'S'), '\'', 'B')},
        "IDSOwner": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSOwner", ' ', 'S'), '\'', 'B')},
        "IDSAcct": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSAcct", ' ', 'S'), '\'', 'B')},
        "IDSPW": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSPW", ' ', 'S'), '\'', 'B')}}
      parameters.update(ctx.parameter_override)
      activity = ExecutePipelineActivity(
        name = "IdsClmMartClmInitUpd",
        wait_on_completion = True,
        pipeline = PipelineReference(type="PipelineReference",reference_name="IdsClmMartClmInitUpd"),
        depends_on = [
          ActivityDependency(
            activity = "StartLoop",
            dependency_conditions = [DependencyCondition.COMPLETED])],
        parameters = parameters)
      return [], [activity]
    def EndLoop():
      # endLoop activity
      return [], []
    artifacts = []
    activities = [IfConditionActivity(name = "If_1B",
      expression = Expression(type="Expression", value="@greater(int(variables('LinkCount')),0)"),
      if_true_activities = collect(artifacts,
        StartLoop(),
        IdsClmMartClmInitUpd(),
        EndLoop()),
      depends_on = [
        ActivityDependency(
          activity = "LinkCount",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def S1_All():
    return [], [SetVariableActivity(
      name = "S1_All",
      variable_name = "S1_All_fired",
      value = "y",
      depends_on = [
        ActivityDependency(
          activity = "If_1B",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
  def S1_If_2B():
    def IdsDMDrugClmAccumImpctExtr():
      parameters= {
        "IDSAcct": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSAcct", ' ', 'S'), '\'', 'B')},
        "IDSDB": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSDB", ' ', 'S'), '\'', 'B')},
        "FilePath": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.FilePath", ' ', 'S'), '\'', 'B')},
        "IDSPW": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSPW", ' ', 'S'), '\'', 'B')},
        "IDSOwner": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSOwner", ' ', 'S'), '\'', 'B')},
        "RunCycle": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.RunCycle", ' ', 'S'), '\'', 'B')}}
      parameters.update(ctx.parameter_override)
      activity = ExecutePipelineActivity(
        name = "IdsDMDrugClmAccumImpctExtr",
        wait_on_completion = True,
        pipeline = PipelineReference(type="PipelineReference",reference_name="IdsDMDrugClmAccumImpctExtr"),
        parameters = parameters)
      return [], [activity]
    artifacts = []
    activities = [IfConditionActivity(name = "S1_If_2B",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('IdsDMDrugClmAccumImpctExtr', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        IdsDMDrugClmAccumImpctExtr()),
      depends_on = [
        ActivityDependency(
          activity = "S1_All",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def S1_If_2C():
    def IdsDMDrugClmAccumImpctLoad():
      parameters= {
        "FilePath": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.FilePath", ' ', 'S'), '\'', 'B')},
        "ClmMartServer": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.ClmMartServer", ' ', 'S'), '\'', 'B')},
        "ClmMartDB": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.ClmMartDB", ' ', 'S'), '\'', 'B')},
        "ClmMartOwner": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.ClmMartOwner", ' ', 'S'), '\'', 'B')},
        "ClmMartAcct": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.ClmMartAcct", ' ', 'S'), '\'', 'B')},
        "ClmMartPW": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.ClmMartPW", ' ', 'S'), '\'', 'B')},
        "ClmMartDSN": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.ClmMartServer", ' ', 'S'), '\'', 'B')},
        "ClmMartArraySize": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.ClmMartArraySize", ' ', 'S'), '\'', 'B')},
        "ClmMartRecordCount": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.ClmMartRecordCount", ' ', 'S'), '\'', 'B')}}
      parameters.update(ctx.parameter_override)
      activity = ExecutePipelineActivity(
        name = "IdsDMDrugClmAccumImpctLoad",
        wait_on_completion = True,
        pipeline = PipelineReference(type="PipelineReference",reference_name="IdsDMDrugClmAccumImpctLoad"),
        parameters = parameters)
      return [], [activity]
    artifacts = []
    activities = [IfConditionActivity(name = "S1_If_2C",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('IdsDMDrugClmAccumImpctLoad', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        IdsDMDrugClmAccumImpctLoad()),
      depends_on = [
        ActivityDependency(
          activity = "S1_If_2B",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def S1_If_3A():
    def WebDmLoadRunCycUpd():
      parameters= {
        "FilePath": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.FilePath", ' ', 'S'), '\'', 'B')},
        "IDSInstance": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSInstance", ' ', 'S'), '\'', 'B')},
        "IDSDB": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSDB", ' ', 'S'), '\'', 'B')},
        "IDSDSN": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSDSN", ' ', 'S'), '\'', 'B')},
        "IDSOwner": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSOwner", ' ', 'S'), '\'', 'B')},
        "IDSAcct": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSAcct", ' ', 'S'), '\'', 'B')},
        "IDSPW": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSPW", ' ', 'S'), '\'', 'B')}}
      parameters.update(ctx.parameter_override)
      activity = ExecutePipelineActivity(
        name = "WebDmLoadRunCycUpd",
        wait_on_completion = True,
        pipeline = PipelineReference(type="PipelineReference",reference_name="WebDmLoadRunCycUpd"),
        parameters = parameters)
      return [], [activity]
    artifacts = []
    activities = [IfConditionActivity(name = "S1_If_3A",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('WebDmLoadRunCycUpd', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        WebDmLoadRunCycUpd()),
      depends_on = [
        ActivityDependency(
          activity = "S1_If_2C",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def S1_If_3B():
    def S1_TruncateW_WEBDM_ETL_DRVR():
      activities = [CustomActivity(
        name = "S1_TruncateW_WEBDM_ETL_DRVR",
        command = ctx.getRoutineCommand("DB.TRUNCATE"),
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "S1_If_3B",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('TruncateW_WEBDM_ETL_DRVR', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        S1_TruncateW_WEBDM_ETL_DRVR()),
      depends_on = [
        ActivityDependency(
          activity = "S1_If_3A",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  return merge(
    S1_If_3B(),
    S1_If_3A(),
    S1_If_2C(),
    S1_If_2B(),
    S1_All(),
    If_1B(),
    LinkCount(),
    If_1A(),
    If_2A())

def IdsWebDMDrugLoadSeq(ctx):
  name = "IdsWebDMDrugLoadSeq"
  artifacts, activities = IdsWebDMDrugLoadSeqActivities(ctx)
  pipeline = PipelineResource(
    folder = PipelineFolder(name="claim/SeqDrug"),
    activities = activities,
    description = """
      Pull Argus data from IDS for Web Data Mart CLM, INIT_CLM and CLM_LN
      Parameters:
      -----------
      FilePath:
        File Path
      ExclusionList:
        Exclusion List
      RunID:
        RunID
      RunCycle:
        Run Cycle
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
        IDS DB Password
      ClmMartServer:
        Claim Mart Server
      ClmMartDB:
        Claim Mart Database
      ClmMartOwner:
        Claim Mart Table Owner
      ClmMartAcct:
        Claim Mart Account
      ClmMartPW:
        Claim Mart Password""",
    parameters = {
      "FilePath": ParameterSpecification(type="String", default_value=ctx.FilePath),
      "ExclusionList": ParameterSpecification(type="String", default_value="\"\""),
      "RunID": ParameterSpecification(type="String"),
      "RunCycle": ParameterSpecification(type="String"),
      "IDSInstance": ParameterSpecification(type="String", default_value=ctx.IDSInstance),
      "IDSDB": ParameterSpecification(type="String", default_value=ctx.IDSDB),
      "IDSDSN": ParameterSpecification(type="String", default_value=ctx.IDSDSN),
      "IDSOwner": ParameterSpecification(type="String", default_value=ctx.IDSOwner),
      "IDSAcct": ParameterSpecification(type="String"),
      "IDSPW": ParameterSpecification(type="String"),
      "ClmMartServer": ParameterSpecification(type="String", default_value=ctx.ClmMartServer),
      "ClmMartDB": ParameterSpecification(type="String", default_value=ctx.ClmMartDB),
      "ClmMartOwner": ParameterSpecification(type="String", default_value=ctx.ClmMartOwner),
      "ClmMartAcct": ParameterSpecification(type="String"),
      "ClmMartPW": ParameterSpecification(type="String")},
    variables = {
      "S1_All_fired": VariableSpecification(type="String", default_value="n"),
      "S1_If_3B_fired": VariableSpecification(type="String", default_value="n"),
      "LinkCount": VariableSpecification(type="String")})
  return artifacts + [(name, pipeline)]

def main():
  ctx = TranslationContext()
  artifacts = (L1_IdsWebDMDrugLoadSeq(ctx)
    + IdsWebDMDrugLoadSeq(ctx))
  if ctx.validateArtifacts(artifacts):
    ctx.deployArtifacts(artifacts)

if __name__ == '__main__':
  main()
