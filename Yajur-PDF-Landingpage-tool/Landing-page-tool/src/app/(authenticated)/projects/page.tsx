import { redirect } from "next/navigation";
import Link from "next/link";
import { getCurrentUser } from "@/lib/auth-helpers";
import { getProjectsForUser } from "@/lib/projects";
import { ProjectCard } from "@/components/dashboard/project-card";
import { StatsBar } from "@/components/dashboard/stats-bar";

export const dynamic = "force-dynamic";

export default async function ProjectsPage() {
  const result = await getCurrentUser();
  if (!result) redirect("/login");

  const projects = await getProjectsForUser(result.user.id);
  const deployedCount = projects.filter((p) => p.status === "deployed").length;

  return (
    <div className="mx-auto max-w-7xl px-6 py-8">
      {/* Header */}
      <div className="flex items-center justify-between mb-8">
        <div>
          <h1 className="text-2xl font-bold text-white">Your Projects</h1>
          <p className="mt-1 text-sm text-gray-500">
            Manage and view all your landing page projects
          </p>
        </div>
        <StatsBar
          totalProjects={projects.length}
          deployedCount={deployedCount}
        />
      </div>

      {/* Projects Grid */}
      {projects.length > 0 ? (
        <div className="grid grid-cols-1 gap-5 sm:grid-cols-2 lg:grid-cols-3">
          {projects.map((project) => (
            <ProjectCard key={project.id} project={project} />
          ))}
        </div>
      ) : (
        <div className="glass-card rounded-2xl p-12 text-center">
          <svg
            className="mx-auto h-12 w-12 text-gray-600"
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={1.5}
              d="M3 7v10a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2h-6l-2-2H5a2 2 0 00-2 2z"
            />
          </svg>
          <h3 className="mt-4 text-lg font-semibold text-white">
            No projects yet
          </h3>
          <p className="mt-2 text-sm text-gray-500">
            Create your first project from the dashboard
          </p>
          <Link
            href="/"
            className="mt-6 inline-flex items-center gap-2 rounded-xl bg-gradient-to-r from-brand-purple to-brand-orange px-6 py-2.5 text-sm font-semibold text-white transition-all hover:shadow-lg hover:shadow-brand-purple/25 hover:-translate-y-0.5"
          >
            Go to Dashboard
          </Link>
        </div>
      )}
    </div>
  );
}
