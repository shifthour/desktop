"use client";

import { useState, useCallback } from "react";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { ProgressSteps } from "@/components/ui/progress-steps";
import { PdfUploader } from "@/components/project/pdf-uploader";
import { PromptEditor } from "@/components/project/prompt-editor";
import { ProcessingAnimation } from "@/components/project/processing-animation";
import { DeploymentResult } from "@/components/project/deployment-result";
import { ResearchModeSelector } from "@/components/project/research-mode-selector";
import { getDefaultPrompt } from "@/lib/prompt-template";
import type { ResearchMode } from "@/lib/types";

const steps = [
  { label: "Upload", icon: "1" },
  { label: "Configure", icon: "2" },
  { label: "Generate", icon: "3" },
  { label: "Deploy", icon: "4" },
];

// Safely parse API responses — handles non-JSON error pages (e.g. Vercel timeouts)
async function safeJson(res: Response): Promise<{ ok: boolean; data: Record<string, unknown>; error?: string }> {
  const text = await res.text();
  try {
    const data = JSON.parse(text);
    if (!res.ok) {
      return { ok: false, data, error: data.error || `Request failed (${res.status})` };
    }
    return { ok: true, data };
  } catch {
    // Response is not JSON (e.g. Vercel timeout HTML page)
    return {
      ok: false,
      data: {},
      error: res.status === 504
        ? "Request timed out. The PDF may be too large or the AI is taking too long. Please try again."
        : `Server error (${res.status}): ${text.substring(0, 150)}`,
    };
  }
}

export default function NewProjectPage() {
  const [currentStep, setCurrentStep] = useState(0);
  const [pdfStoragePath, setPdfStoragePath] = useState("");
  const [pdfOriginalName, setPdfOriginalName] = useState("");
  const [projectName, setProjectName] = useState("");
  const [prompt, setPrompt] = useState("");
  const [projectId, setProjectId] = useState("");
  const [generatedHtml, setGeneratedHtml] = useState("");
  const [generating, setGenerating] = useState(false);
  const [deploying, setDeploying] = useState(false);
  const [vercelUrl, setVercelUrl] = useState("");
  const [githubRepoUrl, setGithubRepoUrl] = useState("");
  const [researchMode, setResearchMode] = useState<ResearchMode>("pdf_only");
  const [error, setError] = useState("");

  // When research mode changes, update the default prompt accordingly
  const handleResearchModeChange = useCallback(
    (mode: ResearchMode) => {
      setResearchMode(mode);
      setPrompt(getDefaultPrompt(mode));
    },
    []
  );

  const handleUploadComplete = (data: {
    pdf_storage_path: string;
    pdf_original_name: string;
    defaultPrompt: string;
  }) => {
    setPdfStoragePath(data.pdf_storage_path);
    setPdfOriginalName(data.pdf_original_name);
    setPrompt(data.defaultPrompt);
    // Auto-generate project name from filename
    const name = data.pdf_original_name
      .replace(/\.pdf$/i, "")
      .replace(/[_-]/g, " ")
      .replace(/\b\w/g, (c) => c.toUpperCase());
    setProjectName(name);
    setCurrentStep(1);
  };

  const handleGenerate = async () => {
    if (!projectName.trim()) {
      setError("Please enter a project name");
      return;
    }

    setError("");
    setGenerating(true);
    setCurrentStep(2);

    try {
      // Create project
      const createRes = await fetch("/api/projects", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: projectName,
          prompt,
          pdf_storage_path: pdfStoragePath,
          pdf_original_name: pdfOriginalName,
        }),
      });

      const createResult = await safeJson(createRes);
      if (!createResult.ok) {
        throw new Error(createResult.error || "Failed to create project");
      }

      const project = createResult.data;
      setProjectId(project.id as string);

      // Generate landing page
      const genRes = await fetch("/api/generate", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          projectId: project.id,
          researchMode,
        }),
      });

      const genResult = await safeJson(genRes);
      if (!genResult.ok) {
        throw new Error(genResult.error || "Generation failed");
      }

      setGeneratedHtml(genResult.data.html as string);
      setCurrentStep(3);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Generation failed");
      setCurrentStep(1);
    } finally {
      setGenerating(false);
    }
  };

  const handleDeploy = async () => {
    setError("");
    setDeploying(true);

    try {
      const res = await fetch("/api/deploy", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ projectId }),
      });

      const result = await safeJson(res);
      if (!result.ok) {
        throw new Error(result.error || "Deployment failed");
      }

      setVercelUrl(result.data.vercelUrl as string);
      setGithubRepoUrl(result.data.githubRepoUrl as string);
      setCurrentStep(4);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Deployment failed");
    } finally {
      setDeploying(false);
    }
  };

  return (
    <div className="mx-auto max-w-3xl px-6 py-8">
      {/* Progress Steps */}
      <div className="mb-8">
        <ProgressSteps steps={steps} currentStep={currentStep} />
      </div>

      <Card>
        {/* Step 0: Upload */}
        {currentStep === 0 && (
          <div>
            <h2 className="text-xl font-semibold text-white mb-1">
              Upload Pitch Deck
            </h2>
            <p className="text-sm text-gray-500 mb-6">
              Upload a PDF pitch deck to generate an investor landing page
            </p>
            <PdfUploader onUploadComplete={handleUploadComplete} />
          </div>
        )}

        {/* Step 1: Configure */}
        {currentStep === 1 && (
          <div>
            <h2 className="text-xl font-semibold text-white mb-1">
              Configure Project
            </h2>
            <p className="text-sm text-gray-500 mb-6">
              Name your project and customize the AI prompt
            </p>

            <div className="space-y-5">
              <ResearchModeSelector
                value={researchMode}
                onChange={handleResearchModeChange}
              />

              <div className="border-t border-dark-border" />

              <div>
                <label className="block text-sm font-semibold text-gray-300 mb-1.5">
                  Project Name
                </label>
                <input
                  type="text"
                  value={projectName}
                  onChange={(e) => setProjectName(e.target.value)}
                  placeholder="Enter project name"
                  className="w-full rounded-xl border border-dark-border bg-dark-bg/50 px-4 py-3 text-sm text-gray-200 placeholder-gray-600 transition-all focus:border-brand-purple/50 focus:outline-none focus:ring-1 focus:ring-brand-purple/30"
                />
              </div>

              <PromptEditor value={prompt} onChange={setPrompt} />

              {error && (
                <div className="rounded-xl border border-red-500/20 bg-red-500/5 px-4 py-3 text-sm text-red-400">
                  {error}
                </div>
              )}

              <div className="flex items-center justify-between pt-2">
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => setCurrentStep(0)}
                >
                  ← Back
                </Button>
                <Button
                  variant="primary"
                  onClick={handleGenerate}
                  loading={generating}
                >
                  Generate Landing Page
                </Button>
              </div>
            </div>
          </div>
        )}

        {/* Step 2: Generating */}
        {currentStep === 2 && (
          <ProcessingAnimation
            stage={deploying ? "deploying" : "generating"}
          />
        )}

        {/* Step 3: Preview + Deploy */}
        {currentStep === 3 && (
          <div>
            <h2 className="text-xl font-semibold text-white mb-1">
              Preview & Deploy
            </h2>
            <p className="text-sm text-gray-500 mb-6">
              Review your landing page and deploy it to the web
            </p>

            {/* Landing Page Name */}
            <div className="mb-6 rounded-xl border border-brand-purple/20 bg-brand-purple/5 px-4 py-3">
              <label className="text-[10px] font-semibold uppercase tracking-wider text-brand-purple">
                Landing Page Name
              </label>
              <p className="mt-1 text-lg font-bold text-white">
                {projectName}
              </p>
            </div>

            {/* Preview */}
            <div className="rounded-xl border border-dark-border overflow-hidden mb-6">
              <div className="bg-dark-card px-4 py-2 border-b border-dark-border">
                <span className="text-[10px] font-semibold uppercase tracking-wider text-gray-500">
                  Preview
                </span>
              </div>
              <iframe
                srcDoc={generatedHtml}
                className="w-full h-[500px] bg-white"
                sandbox="allow-scripts allow-same-origin"
                title="Landing page preview"
              />
            </div>

            {error && (
              <div className="mb-4 rounded-xl border border-red-500/20 bg-red-500/5 px-4 py-3 text-sm text-red-400">
                {error}
              </div>
            )}

            <div className="flex items-center justify-between">
              <Button
                variant="ghost"
                size="sm"
                onClick={() => setCurrentStep(1)}
              >
                ← Back to Configure
              </Button>
              <Button
                variant="primary"
                onClick={handleDeploy}
                loading={deploying}
              >
                <svg
                  className="h-4 w-4"
                  fill="none"
                  viewBox="0 0 24 24"
                  stroke="currentColor"
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={2}
                    d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-8l-4-4m0 0L8 8m4-4v12"
                  />
                </svg>
                Deploy to Vercel
              </Button>
            </div>
          </div>
        )}

        {/* Step 4: Success */}
        {currentStep === 4 && (
          <DeploymentResult
            projectName={projectName}
            vercelUrl={vercelUrl}
            githubRepoUrl={githubRepoUrl}
          />
        )}
      </Card>
    </div>
  );
}
