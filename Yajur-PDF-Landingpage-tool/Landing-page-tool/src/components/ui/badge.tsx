interface BadgeProps {
  status: "draft" | "generating" | "deploying" | "deployed" | "error";
}

const statusConfig = {
  draft: {
    label: "Draft",
    className: "bg-gray-500/10 text-gray-400 border-gray-500/20",
  },
  generating: {
    label: "Generating",
    className: "bg-yellow-500/10 text-yellow-400 border-yellow-500/20",
  },
  deploying: {
    label: "Deploying",
    className: "bg-blue-500/10 text-blue-400 border-blue-500/20",
  },
  deployed: {
    label: "Deployed",
    className: "bg-green-500/10 text-green-400 border-green-500/20",
  },
  error: {
    label: "Error",
    className: "bg-red-500/10 text-red-400 border-red-500/20",
  },
};

export function Badge({ status }: BadgeProps) {
  const config = statusConfig[status];

  return (
    <span
      className={`inline-flex items-center rounded-full border px-2.5 py-0.5 text-[10px] font-semibold uppercase tracking-wider ${config.className}`}
    >
      {config.label}
    </span>
  );
}
