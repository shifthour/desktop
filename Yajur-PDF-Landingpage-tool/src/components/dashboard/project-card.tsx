"use client";

import Link from "next/link";
import type { Project } from "@/lib/types";
import { Card } from "@/components/ui/card";

const statusConfig = {
  draft: { label: "Draft", color: "bg-gray-500/20 text-gray-400 border-gray-500/30" },
  generating: { label: "Generating", color: "bg-yellow-500/20 text-yellow-400 border-yellow-500/30" },
  deploying: { label: "Deploying", color: "bg-blue-500/20 text-blue-400 border-blue-500/30" },
  deployed: { label: "Live", color: "bg-green-500/20 text-green-400 border-green-500/30" },
  error: { label: "Error", color: "bg-red-500/20 text-red-400 border-red-500/30" },
};

export function ProjectCard({ project }: { project: Project }) {
  const status = statusConfig[project.status];
  const timeAgo = getTimeAgo(project.updatedAt);

  return (
    <Link href={`/project/${project.id}`}>
      <Card hover className="group cursor-pointer">
        <div className="flex items-start justify-between">
          <div className="flex-1 min-w-0">
            <h3 className="truncate text-lg font-semibold text-white group-hover:gradient-text transition-all">
              {project.name}
            </h3>
            <p className="mt-1 text-xs text-gray-500">
              {project.pdfOriginalName}
            </p>
          </div>
          <span
            className={`ml-3 shrink-0 rounded-full border px-2.5 py-0.5 text-[10px] font-semibold uppercase tracking-wider ${status.color}`}
          >
            {status.label}
          </span>
        </div>

        {project.vercelUrl && (
          <div className="mt-4 flex items-center gap-2 rounded-lg bg-dark-bg/50 px-3 py-2">
            <svg className="h-3.5 w-3.5 text-green-400 shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1" />
            </svg>
            <span className="truncate text-xs text-green-400 font-mono">
              {project.vercelUrl}
            </span>
          </div>
        )}

        {project.errorMessage && (
          <div className="mt-4 rounded-lg bg-red-500/5 border border-red-500/10 px-3 py-2">
            <p className="text-xs text-red-400 line-clamp-2">{project.errorMessage}</p>
          </div>
        )}

        <div className="mt-4 flex items-center justify-between text-[11px] text-gray-600">
          <span>Updated {timeAgo}</span>
          <span className="text-gray-500 group-hover:text-brand-orange transition-colors">
            View details →
          </span>
        </div>
      </Card>
    </Link>
  );
}

function getTimeAgo(dateStr: string): string {
  const now = new Date();
  const date = new Date(dateStr);
  const diffMs = now.getTime() - date.getTime();
  const diffMins = Math.floor(diffMs / 60000);
  const diffHrs = Math.floor(diffMins / 60);
  const diffDays = Math.floor(diffHrs / 24);

  if (diffMins < 1) return "just now";
  if (diffMins < 60) return `${diffMins}m ago`;
  if (diffHrs < 24) return `${diffHrs}h ago`;
  if (diffDays < 7) return `${diffDays}d ago`;
  return date.toLocaleDateString();
}
