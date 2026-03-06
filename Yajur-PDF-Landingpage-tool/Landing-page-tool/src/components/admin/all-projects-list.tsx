"use client";

import { useState, useEffect } from "react";
import Link from "next/link";
import { Badge } from "@/components/ui/badge";
import type { Project } from "@/lib/types";

export function AllProjectsList() {
  const [projects, setProjects] = useState<Project[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchProjects = async () => {
      try {
        const res = await fetch("/api/projects?all=true");
        if (!res.ok) throw new Error("Failed to fetch");
        const data = await res.json();
        setProjects(data.projects);
      } catch {
        console.error("Failed to load projects");
      } finally {
        setLoading(false);
      }
    };
    fetchProjects();
  }, []);

  if (loading) {
    return (
      <div className="flex justify-center py-12">
        <div className="h-8 w-8 animate-spin rounded-full border-2 border-brand-purple border-t-transparent" />
      </div>
    );
  }

  return (
    <div>
      <div className="mb-6">
        <h3 className="text-lg font-semibold text-white">All Projects</h3>
        <p className="text-sm text-gray-500 mt-1">
          {projects.length} {projects.length === 1 ? "project" : "projects"}{" "}
          across all users
        </p>
      </div>

      <div className="glass-card rounded-2xl overflow-hidden">
        <table className="w-full">
          <thead>
            <tr className="border-b border-dark-border/50">
              <th className="px-5 py-3 text-left text-[10px] font-semibold uppercase tracking-wider text-gray-500">
                Project
              </th>
              <th className="px-5 py-3 text-left text-[10px] font-semibold uppercase tracking-wider text-gray-500">
                Owner
              </th>
              <th className="px-5 py-3 text-left text-[10px] font-semibold uppercase tracking-wider text-gray-500">
                Status
              </th>
              <th className="px-5 py-3 text-left text-[10px] font-semibold uppercase tracking-wider text-gray-500">
                URL
              </th>
              <th className="px-5 py-3 text-left text-[10px] font-semibold uppercase tracking-wider text-gray-500">
                Created
              </th>
            </tr>
          </thead>
          <tbody>
            {projects.map((project) => {
              const ownerProfile = project.user_profile as
                | { full_name?: string; email?: string }
                | undefined;

              return (
                <tr
                  key={project.id}
                  className="border-b border-dark-border/30 hover:bg-dark-hover/50 transition-colors"
                >
                  <td className="px-5 py-3">
                    <Link
                      href={`/project/${project.id}`}
                      className="text-sm font-medium text-white hover:text-brand-purple-light transition-colors"
                    >
                      {project.name}
                    </Link>
                    <p className="text-[11px] text-gray-600 mt-0.5">
                      {project.pdf_original_name}
                    </p>
                  </td>
                  <td className="px-5 py-3 text-sm text-gray-400">
                    {ownerProfile?.full_name || ownerProfile?.email || "Unknown"}
                  </td>
                  <td className="px-5 py-3">
                    <Badge status={project.status} />
                  </td>
                  <td className="px-5 py-3">
                    {project.vercel_url ? (
                      <a
                        href={project.vercel_url}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="text-xs font-mono text-green-400 hover:underline truncate block max-w-[200px]"
                      >
                        {project.vercel_url}
                      </a>
                    ) : (
                      <span className="text-xs text-gray-600">—</span>
                    )}
                  </td>
                  <td className="px-5 py-3 text-xs text-gray-500">
                    {new Date(project.created_at).toLocaleDateString()}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>

        {projects.length === 0 && (
          <div className="py-12 text-center text-sm text-gray-500">
            No projects found
          </div>
        )}
      </div>
    </div>
  );
}
