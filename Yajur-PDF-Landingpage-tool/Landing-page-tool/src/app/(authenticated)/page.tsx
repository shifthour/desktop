import { redirect } from "next/navigation";
import { getCurrentUser } from "@/lib/auth-helpers";
import { getProjectsForUser } from "@/lib/projects";
import { ProjectCard } from "@/components/dashboard/project-card";
import { StatsBar } from "@/components/dashboard/stats-bar";
import { ToolCard } from "@/components/dashboard/tool-card";

export const dynamic = "force-dynamic";

export default async function DashboardPage() {
  const result = await getCurrentUser();
  if (!result) redirect("/login");

  const projects = await getProjectsForUser(result.user.id);
  const deployedCount = projects.filter((p) => p.status === "deployed").length;

  return (
    <div className="mx-auto max-w-7xl px-6 py-8">
      {/* Hero */}
      <div className="mb-10">
        <h1 className="text-3xl font-bold text-white">
          Welcome back,{" "}
          <span className="gradient-text">
            {result.profile.full_name.split(" ")[0]}
          </span>
        </h1>
        <p className="mt-2 text-sm text-gray-500">
          Choose a tool to create stunning investor landing pages
        </p>
      </div>

      {/* Tool Selection */}
      <div className="mb-12">
        <h2 className="text-lg font-semibold text-white mb-5">Create New Project</h2>
        <div className="grid grid-cols-1 gap-5 sm:grid-cols-2 lg:grid-cols-3">
          <ToolCard
            title="PDF to Landing Page"
            description="Upload a pitch deck PDF and convert it into an interactive investor landing page"
            icon="pdf"
            href="/project/new?source=pdf"
            available
          />
          <ToolCard
            title="Source Drive to Landing Page"
            description="Connect to SharePoint or Google Drive and convert documents into landing pages"
            icon="drive"
            href="/project/new?source=drive"
            available={false}
            comingSoon
          />
        </div>
      </div>

      {/* Recent Projects */}
      {projects.length > 0 && (
        <div>
          <div className="flex items-center justify-between mb-5">
            <h2 className="text-lg font-semibold text-white">Recent Projects</h2>
            <StatsBar
              totalProjects={projects.length}
              deployedCount={deployedCount}
            />
          </div>
          <div className="grid grid-cols-1 gap-5 sm:grid-cols-2 lg:grid-cols-3">
            {projects.map((project) => (
              <ProjectCard key={project.id} project={project} />
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
