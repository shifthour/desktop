import { Header } from "@/components/ui/header";
import { ProjectCard } from "@/components/dashboard/project-card";
import { CreateButton } from "@/components/dashboard/create-button";
import { getAllProjects } from "@/lib/projects";

export const dynamic = "force-dynamic";

export default async function Dashboard() {
  const projects = await getAllProjects();

  return (
    <div className="min-h-screen">
      <Header />

      <main className="mx-auto max-w-7xl px-6 py-10">
        {/* Hero section */}
        <div className="mb-12">
          <h1 className="text-4xl font-bold tracking-tight">
            <span className="gradient-text">Pitch Deck</span>{" "}
            <span className="text-white">Studio</span>
          </h1>
          <p className="mt-3 max-w-lg text-sm text-gray-500 leading-relaxed">
            Transform your investor pitch decks into stunning, interactive landing
            pages. Upload a PDF, let AI design it, and deploy to the web in minutes.
          </p>
        </div>

        {/* Stats bar */}
        {projects.length > 0 && (
          <div className="mb-8 flex items-center gap-6">
            <div className="flex items-center gap-2">
              <div className="h-2 w-2 rounded-full bg-brand-purple" />
              <span className="text-xs text-gray-500">
                <span className="font-semibold text-white">{projects.length}</span>{" "}
                {projects.length === 1 ? "project" : "projects"}
              </span>
            </div>
            <div className="flex items-center gap-2">
              <div className="h-2 w-2 rounded-full bg-green-500" />
              <span className="text-xs text-gray-500">
                <span className="font-semibold text-green-400">
                  {projects.filter((p) => p.status === "deployed").length}
                </span>{" "}
                deployed
              </span>
            </div>
          </div>
        )}

        {/* Projects grid */}
        <div className="grid grid-cols-1 gap-5 sm:grid-cols-2 lg:grid-cols-3">
          <CreateButton />
          {projects.map((project) => (
            <ProjectCard key={project.id} project={project} />
          ))}
        </div>

        {/* Empty state */}
        {projects.length === 0 && (
          <div className="mt-12 text-center">
            <div className="mx-auto flex h-20 w-20 items-center justify-center rounded-2xl bg-dark-card border border-dark-border">
              <svg className="h-10 w-10 text-gray-700" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
              </svg>
            </div>
            <h3 className="mt-4 text-lg font-semibold text-gray-400">
              No projects yet
            </h3>
            <p className="mt-1 text-sm text-gray-600">
              Create your first project to get started
            </p>
          </div>
        )}
      </main>
    </div>
  );
}
