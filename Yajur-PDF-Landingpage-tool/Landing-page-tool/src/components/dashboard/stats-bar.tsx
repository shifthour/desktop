interface StatsBarProps {
  totalProjects: number;
  deployedCount: number;
}

export function StatsBar({ totalProjects, deployedCount }: StatsBarProps) {
  return (
    <div className="flex items-center gap-6">
      <div className="flex items-center gap-2">
        <div className="h-2 w-2 rounded-full gradient-brand" />
        <span className="text-sm text-gray-400">
          <span className="font-semibold text-white">{totalProjects}</span>{" "}
          {totalProjects === 1 ? "project" : "projects"}
        </span>
      </div>
      <div className="flex items-center gap-2">
        <div className="h-2 w-2 rounded-full bg-green-500" />
        <span className="text-sm text-gray-400">
          <span className="font-semibold text-green-400">{deployedCount}</span>{" "}
          deployed
        </span>
      </div>
    </div>
  );
}
