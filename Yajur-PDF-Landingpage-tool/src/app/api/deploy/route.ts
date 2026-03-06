import { NextRequest, NextResponse } from "next/server";
import { getProject, updateProject } from "@/lib/projects";
import { createRepoAndPush, updateRepoFile } from "@/lib/github";
import { deployToVercel } from "@/lib/vercel";
import { readFile } from "fs/promises";
import path from "path";

export const maxDuration = 120;

export async function POST(request: NextRequest) {
  try {
    const { projectId } = await request.json();

    if (!projectId) {
      return NextResponse.json(
        { error: "Missing projectId" },
        { status: 400 }
      );
    }

    const project = await getProject(projectId);
    if (!project) {
      return NextResponse.json(
        { error: "Project not found" },
        { status: 404 }
      );
    }

    if (!project.generatedHtmlPath) {
      return NextResponse.json(
        { error: "No generated HTML found. Please generate first." },
        { status: 400 }
      );
    }

    // Read the generated HTML
    const htmlContent = await readFile(
      path.join(process.cwd(), project.generatedHtmlPath),
      "utf-8"
    );

    // Update status to deploying
    await updateProject(projectId, { status: "deploying", errorMessage: null });

    const repoName = `yajur-${project.slug}`;
    const vercelProjectName = `yajur-${project.slug}`;

    try {
      // Step 1: Push to GitHub
      let githubRepoUrl: string;
      if (project.githubRepoUrl) {
        // Update existing repo
        await updateRepoFile(repoName, htmlContent);
        githubRepoUrl = project.githubRepoUrl;
      } else {
        // Create new repo
        const result = await createRepoAndPush(repoName, htmlContent);
        githubRepoUrl = result.repoUrl;
      }

      // Step 2: Deploy to Vercel
      const { url: vercelUrl, deploymentId } = await deployToVercel(
        vercelProjectName,
        htmlContent
      );

      // Update project with deployment info
      await updateProject(projectId, {
        githubRepoUrl,
        vercelUrl,
        vercelProjectName,
        status: "deployed",
      });

      return NextResponse.json({
        vercelUrl,
        githubRepoUrl,
        deploymentId,
      });
    } catch (deployError) {
      const errorMessage =
        deployError instanceof Error ? deployError.message : "Deployment failed";
      await updateProject(projectId, {
        status: "error",
        errorMessage,
      });
      return NextResponse.json({ error: errorMessage }, { status: 500 });
    }
  } catch (error) {
    console.error("Deploy error:", error);
    return NextResponse.json(
      { error: "Failed to deploy" },
      { status: 500 }
    );
  }
}
