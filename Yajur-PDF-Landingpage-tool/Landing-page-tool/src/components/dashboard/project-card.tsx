"use client";

import Link from "next/link";
import { Badge } from "@/components/ui/badge";
import type { Project } from "@/lib/types";

interface ProjectCardProps {
  project: Project;
  showOwner?: boolean;
}

function timeAgo(dateStr: string): string {
  const date = new Date(dateStr);
  const now = new Date();
  const seconds = Math.floor((now.getTime() - date.getTime()) / 1000);

  if (seconds < 60) return "just now";
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  if (days < 30) return `${days}d ago`;
  const months = Math.floor(days / 30);
  return `${months}mo ago`;
}

export function ProjectCard({ project, showOwner }: ProjectCardProps) {
  return (
    <Link
      href={`/project/${project.id}`}
      className="group glass-card rounded-2xl p-5 transition-all duration-300 hover:border-brand-purple/30 hover:shadow-lg hover:shadow-brand-purple/5 hover:-translate-y-0.5 block"
    >
      {/* Header */}
      <div className="flex items-start justify-between gap-2">
        <h3 className="text-base font-semibold text-white group-hover:gradient-text transition-all line-clamp-1">
          {project.name}
        </h3>
        <Badge status={project.status} />
      </div>

      {/* Owner (for admin view) */}
      {showOwner && project.user_profile && (
        <p className="mt-1.5 text-[11px] text-brand-purple-light">
          by {(project.user_profile as { full_name?: string; email?: string })?.full_name || (project.user_profile as { email?: string })?.email}
        </p>
      )}

      {/* PDF filename */}
      <div className="mt-3 flex items-center gap-2">
        <svg
          className="h-3.5 w-3.5 text-gray-600 shrink-0"
          fill="none"
          viewBox="0 0 24 24"
          stroke="currentColor"
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth={2}
            d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"
          />
        </svg>
        <span className="text-xs text-gray-500 truncate">
          {project.pdf_original_name || "No PDF"}
        </span>
      </div>

      {/* Deployed URL */}
      {project.vercel_url && (
        <div className="mt-2 flex items-center gap-2">
          <svg
            className="h-3.5 w-3.5 text-green-500 shrink-0"
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14"
            />
          </svg>
          <span className="text-xs text-green-400 truncate font-mono">
            {project.vercel_url}
          </span>
        </div>
      )}

      {/* Error */}
      {project.error_message && (
        <p className="mt-2 text-[11px] text-red-400 line-clamp-2">
          {project.error_message}
        </p>
      )}

      {/* Footer */}
      <div className="mt-4 pt-3 border-t border-dark-border/50 flex items-center justify-between">
        <span className="text-[10px] text-gray-600">
          Updated {timeAgo(project.updated_at)}
        </span>
        <span className="flex items-center gap-1 text-[10px] text-gray-600 opacity-0 group-hover:opacity-100 transition-opacity">
          <svg
            className="h-3 w-3"
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z"
            />
          </svg>
          Edit
        </span>
      </div>
    </Link>
  );
}
