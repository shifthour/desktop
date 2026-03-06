#!/usr/bin/python3

from npadf import *

def BCBSKCCommDrugExtrSeqActivities(ctx):
  def If_2A():
    def BCBSKCCommClmExtr():
      parameters= {
        "CurrentDate": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.CurrentDate", ' ', 'S'), '\'', 'B')},
        "SrcSysCd": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.SrcSysCd", ' ', 'S'), '\'', 'B')},
        "SrcSysCdSk": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.SrcSysCdSk", ' ', 'S'), '\'', 'B')},
        "FilePath": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.FilePath", ' ', 'S'), '\'', 'B')},
        "RunCycle": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.RunCycle", ' ', 'S'), '\'', 'B')},
        "RunID": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.RunID", ' ', 'S'), '\'', 'B')},
        "IDSAcct": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSAcct", ' ', 'S'), '\'', 'B')},
        "IDSPW": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSPW", ' ', 'S'), '\'', 'B')},
        "IDSInstance": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSInstance", ' ', 'S'), '\'', 'B')},
        "IDSDB": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSDB", ' ', 'S'), '\'', 'B')},
        "IDSOwner": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSOwner", ' ', 'S'), '\'', 'B')}}
      parameters.update(ctx.parameter_override)
      activity = ExecutePipelineActivity(
        name = "BCBSKCCommClmExtr",
        wait_on_completion = True,
        pipeline = PipelineReference(type="PipelineReference",reference_name="BCBSKCCommClmExtr"),
        parameters = parameters)
      return [], [activity]
    artifacts = []
    activities = [IfConditionActivity(name = "If_2A",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('BCBSKCCommClmExtr', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        BCBSKCCommClmExtr()))]
    return artifacts, activities
  def If_5A():
    def BCBSKCCommDrugClmExtrLoadSeq():
      parameters= {
        "CurrentDate": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.CurrentDate", ' ', 'S'), '\'', 'B')},
        "FilePath": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.FilePath", ' ', 'S'), '\'', 'B')},
        "Logging": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.Logging", ' ', 'S'), '\'', 'B')},
        "RunCycle": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.RunCycle", ' ', 'S'), '\'', 'B')},
        "RunID": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.RunID", ' ', 'S'), '\'', 'B')},
        "ExclusionList": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.ExclusionList", ' ', 'S'), '\'', 'B')},
        "SrcSysCd": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.SrcSysCd", ' ', 'S'), '\'', 'B')},
        "SrcSysCdSK": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.SrcSysCdSk", ' ', 'S'), '\'', 'B')},
        "IDSAcct": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSAcct", ' ', 'S'), '\'', 'B')},
        "IDSPW": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSPW", ' ', 'S'), '\'', 'B')},
        "IDSInstance": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSInstance", ' ', 'S'), '\'', 'B')},
        "IDSOwner": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSOwner", ' ', 'S'), '\'', 'B')},
        "IDSDB": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSDB", ' ', 'S'), '\'', 'B')},
        "IDSDSN": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSDSN", ' ', 'S'), '\'', 'B')},
        "CactusAcct": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.CactusAcct", ' ', 'S'), '\'', 'B')},
        "CactusPW": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.CactusPW", ' ', 'S'), '\'', 'B')},
        "CactusDB": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.CactusDB", ' ', 'S'), '\'', 'B')},
        "CactusServer": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.CactusServer", ' ', 'S'), '\'', 'B')},
        "CactusOwner": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.CactusOwner", ' ', 'S'), '\'', 'B')},
        "ProvRunCycle": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.ProvRunCycle", ' ', 'S'), '\'', 'B')}}
      parameters.update(ctx.parameter_override)
      activity = ExecutePipelineActivity(
        name = "BCBSKCCommDrugClmExtrLoadSeq",
        wait_on_completion = True,
        pipeline = PipelineReference(type="PipelineReference",reference_name="BCBSKCCommDrugClmExtrLoadSeq"),
        parameters = parameters)
      return [], [activity]
    artifacts = []
    activities = [IfConditionActivity(name = "If_5A",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('BCBSKCCommDrugClmExtrLoadSeq', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        BCBSKCCommDrugClmExtrLoadSeq()),
      depends_on = [
        ActivityDependency(
          activity = "If_2A",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  return merge(
    If_5A(),
    If_2A())

def BCBSKCCommDrugExtrSeq(ctx):
  name = "BCBSKCCommDrugExtrSeq"
  artifacts, activities = BCBSKCCommDrugExtrSeqActivities(ctx)
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
      CactusServer:
        Cactus Server
      CactusDB:
        Cactus Database
      CactusOwner:
        Cactus Table Owner
      CactusAcct:
        Cactus Account
      CactusPW:
        Cactus Password
      CurrentDate:
        CurrentDate
      RunCycle:
        RunCycle
      RunID:
        RunID
      ExclusionList:
        ExclusionList
      SrcSysCd:
        SourceSys
      SrcSysCdSk:
        SrcSysCdSk
      Logging:
        Logging
      ProvRunCycle:
        ProvRunCycle""",
    parameters = {
      "FilePath": ParameterSpecification(type="String", default_value=ctx.FilePath),
      "IDSDSN": ParameterSpecification(type="String", default_value=ctx.IDSDSN),
      "IDSInstance": ParameterSpecification(type="String", default_value=ctx.IDSInstance),
      "IDSDB": ParameterSpecification(type="String", default_value=ctx.IDSDB),
      "IDSOwner": ParameterSpecification(type="String", default_value=ctx.IDSOwner),
      "IDSAcct": ParameterSpecification(type="String"),
      "IDSPW": ParameterSpecification(type="String"),
      "CactusServer": ParameterSpecification(type="String", default_value=ctx.CactusServer),
      "CactusDB": ParameterSpecification(type="String", default_value=ctx.CactusDB),
      "CactusOwner": ParameterSpecification(type="String", default_value=ctx.CactusOwner),
      "CactusAcct": ParameterSpecification(type="String"),
      "CactusPW": ParameterSpecification(type="String"),
      "CurrentDate": ParameterSpecification(type="String"),
      "RunCycle": ParameterSpecification(type="String"),
      "RunID": ParameterSpecification(type="String"),
      "ExclusionList": ParameterSpecification(type="String", default_value="\"\""),
      "SrcSysCd": ParameterSpecification(type="String", default_value="SAVRX"),
      "SrcSysCdSk": ParameterSpecification(type="String"),
      "Logging": ParameterSpecification(type="String"),
      "ProvRunCycle": ParameterSpecification(type="Int")})
  return artifacts + [(name, pipeline)]

def main():
  ctx = TranslationContext()
  artifacts = (BCBSKCCommDrugExtrSeq(ctx))
  if ctx.validateArtifacts(artifacts):
    ctx.deployArtifacts(artifacts)

if __name__ == '__main__':
  main()
