import { NextRequest, NextResponse } from "next/server";
import { requireAuth } from "@/lib/auth-helpers";
import { getProject, updateProject } from "@/lib/projects";
import { createRepoAndPush, updateRepoFile } from "@/lib/github";
import { deployToVercel } from "@/lib/vercel";
import { logActivity } from "@/lib/activity";

export const maxDuration = 300;

export async function POST(request: NextRequest) {
  try {
    const { user } = await requireAuth();
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

    if (!project.generated_html) {
      return NextResponse.json(
        { error: "No generated HTML found. Please generate first." },
        { status: 400 }
      );
    }

    const htmlContent = project.generated_html;

    // Update status to deploying
    await updateProject(projectId, {
      status: "deploying",
      error_message: null,
    });

    const repoName = `yajur-${project.slug}`;
    const vercelProjectName = `yajur-${project.slug}`;

    try {
      // Step 1: Push to GitHub
      let githubRepoUrl: string;
      if (project.github_repo_url) {
        // Update existing repo
        await updateRepoFile(repoName, htmlContent);
        githubRepoUrl = project.github_repo_url;
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
        github_repo_url: githubRepoUrl,
        vercel_url: vercelUrl,
        vercel_project_name: vercelProjectName,
        status: "deployed",
      });

      await logActivity(
        user.id,
        "project_deployed",
        "project",
        projectId,
        { vercelUrl, githubRepoUrl, name: project.name }
      );

      return NextResponse.json({
        vercelUrl,
        githubRepoUrl,
        deploymentId,
      });
    } catch (deployError) {
      const errorMessage =
        deployError instanceof Error
          ? deployError.message
          : "Deployment failed";
      await updateProject(projectId, {
        status: "error",
        error_message: errorMessage,
      });
      return NextResponse.json({ error: errorMessage }, { status: 500 });
    }
  } catch (error) {
    console.error("Deploy error:", error);
    const message =
      error instanceof Error ? error.message : "Failed to deploy";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
