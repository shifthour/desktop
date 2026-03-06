"use client";

import { useState, useEffect, useCallback } from "react";
import { useParams, useRouter } from "next/navigation";
import { Header } from "@/components/ui/header";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { PromptEditor } from "@/components/project/prompt-editor";
import { ProcessingAnimation } from "@/components/project/processing-animation";
import { DeploymentResult } from "@/components/project/deployment-result";
import type { Project } from "@/lib/types";

export default function ProjectDetailPage() {
  const params = useParams();
  const router = useRouter();
  const projectId = params.id as string;

  const [project, setProject] = useState<Project | null>(null);
  const [loading, setLoading] = useState(true);
  const [prompt, setPrompt] = useState("");
  const [processing, setProcessing] = useState<"generating" | "deploying" | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [showResult, setShowResult] = useState(false);

  const fetchProject = useCallback(async () => {
    try {
      const res = await fetch(`/api/projects/${projectId}`);
      if (!res.ok) throw new Error("Project not found");
      const data = await res.json();
      setProject(data);
      setPrompt(data.prompt);
    } catch {
      setError("Failed to load project");
    } finally {
      setLoading(false);
    }
  }, [projectId]);

  useEffect(() => {
    fetchProject();
  }, [fetchProject]);

  const handleRegenerate = async () => {
    if (!project) return;
    setError(null);
    setProcessing("generating");

    try {
      // Update prompt
      await fetch(`/api/projects/${projectId}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ prompt }),
      });

      // Generate
      const genRes = await fetch("/api/generate", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ projectId }),
      });

      if (!genRes.ok) {
        const data = await genRes.json();
        throw new Error(data.error || "Generation failed");
      }

      // Deploy
      setProcessing("deploying");
      const deployRes = await fetch("/api/deploy", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ projectId }),
      });

      if (!deployRes.ok) {
        const data = await deployRes.json();
        throw new Error(data.error || "Deployment failed");
      }

      setProcessing(null);
      setShowResult(true);
      await fetchProject();
    } catch (err) {
      setProcessing(null);
      setError(err instanceof Error ? err.message : "An error occurred");
      await fetchProject();
    }
  };

  const handleDelete = async () => {
    if (!confirm("Are you sure you want to delete this project?")) return;
    try {
      await fetch(`/api/projects/${projectId}`, { method: "DELETE" });
      router.push("/");
    } catch {
      setError("Failed to delete project");
    }
  };

  if (loading) {
    return (
      <div className="min-h-screen">
        <Header />
        <main className="mx-auto max-w-3xl px-6 py-10">
          <div className="flex items-center justify-center py-20">
            <div className="h-8 w-8 rounded-full border-2 border-brand-purple border-t-transparent animate-spin" />
          </div>
        </main>
      </div>
    );
  }

  if (!project) {
    return (
      <div className="min-h-screen">
        <Header />
        <main className="mx-auto max-w-3xl px-6 py-10 text-center">
          <h1 className="text-2xl font-bold text-white">Project Not Found</h1>
          <Button variant="secondary" className="mt-4" onClick={() => router.push("/")}>
            Back to Dashboard
          </Button>
        </main>
      </div>
    );
  }

  if (processing) {
    return (
      <div className="min-h-screen">
        <Header />
        <main className="mx-auto max-w-3xl px-6 py-10">
          <Card>
            <ProcessingAnimation stage={processing} />
          </Card>
        </main>
      </div>
    );
  }

  if (showResult && project.vercelUrl && project.githubRepoUrl) {
    return (
      <div className="min-h-screen">
        <Header />
        <main className="mx-auto max-w-3xl px-6 py-10">
          <Card>
            <DeploymentResult
              vercelUrl={project.vercelUrl}
              githubRepoUrl={project.githubRepoUrl}
            />
          </Card>
        </main>
      </div>
    );
  }

  const statusConfig: Record<string, { label: string; color: string }> = {
    draft: { label: "Draft", color: "bg-gray-500/20 text-gray-400 border-gray-500/30" },
    generating: { label: "Generating", color: "bg-yellow-500/20 text-yellow-400 border-yellow-500/30" },
    deploying: { label: "Deploying", color: "bg-blue-500/20 text-blue-400 border-blue-500/30" },
    deployed: { label: "Live", color: "bg-green-500/20 text-green-400 border-green-500/30" },
    error: { label: "Error", color: "bg-red-500/20 text-red-400 border-red-500/30" },
  };
  const status = statusConfig[project.status] || statusConfig.draft;

  return (
    <div className="min-h-screen">
      <Header />

      <main className="mx-auto max-w-3xl px-6 py-10">
        {/* Project header */}
        <div className="mb-8 flex items-start justify-between">
          <div>
            <div className="flex items-center gap-3">
              <h1 className="text-3xl font-bold text-white">{project.name}</h1>
              <span className={`rounded-full border px-2.5 py-0.5 text-[10px] font-semibold uppercase tracking-wider ${status.color}`}>
                {status.label}
              </span>
            </div>
            <p className="mt-1 text-sm text-gray-500">
              {project.pdfOriginalName} — Created{" "}
              {new Date(project.createdAt).toLocaleDateString()}
            </p>
          </div>
          <Button variant="danger" size="sm" onClick={handleDelete}>
            Delete
          </Button>
        </div>

        {/* Deployed URL */}
        {project.vercelUrl && (
          <Card className="mb-6">
            <div className="flex items-center justify-between">
              <div>
                <label className="text-[10px] font-semibold uppercase tracking-wider text-green-400">
                  Live URL
                </label>
                <a
                  href={project.vercelUrl}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="mt-1 block text-sm font-mono text-green-300 hover:underline"
                >
                  {project.vercelUrl}
                </a>
              </div>
              <a href={project.vercelUrl} target="_blank" rel="noopener noreferrer">
                <Button variant="secondary" size="sm">
                  Open
                  <svg className="h-3 w-3" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14" />
                  </svg>
                </Button>
              </a>
            </div>
            {project.githubRepoUrl && (
              <div className="mt-3 pt-3 border-t border-dark-border">
                <label className="text-[10px] font-semibold uppercase tracking-wider text-gray-500">
                  GitHub
                </label>
                <a
                  href={project.githubRepoUrl}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="mt-1 block text-xs font-mono text-gray-400 hover:text-brand-purple hover:underline"
                >
                  {project.githubRepoUrl}
                </a>
              </div>
            )}
          </Card>
        )}

        {/* Prompt editor */}
        <Card className="mb-6">
          <h2 className="mb-4 text-lg font-semibold text-white">
            AI Prompt
          </h2>
          <PromptEditor value={prompt} onChange={setPrompt} />
        </Card>

        {/* Preview */}
        {project.vercelUrl && (
          <Card className="mb-6">
            <h2 className="mb-4 text-lg font-semibold text-white">
              Current Preview
            </h2>
            <div className="overflow-hidden rounded-xl border border-dark-border">
              <iframe
                src={project.vercelUrl}
                className="h-[400px] w-full bg-white"
                title="Landing Page Preview"
              />
            </div>
          </Card>
        )}

        {/* Error */}
        {(error || project.errorMessage) && (
          <div className="mb-6 rounded-xl border border-red-500/20 bg-red-500/5 p-4">
            <p className="text-sm text-red-400">{error || project.errorMessage}</p>
          </div>
        )}

        {/* Actions */}
        <div className="flex items-center justify-between">
          <Button variant="ghost" onClick={() => router.push("/")}>
            ← Dashboard
          </Button>
          <Button variant="primary" size="lg" onClick={handleRegenerate}>
            {project.status === "deployed"
              ? "Regenerate & Redeploy"
              : "Generate & Deploy"}
            <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
            </svg>
          </Button>
        </div>
      </main>
    </div>
  );
}
