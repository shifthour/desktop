import { NextRequest, NextResponse } from "next/server";
import { requireAuth } from "@/lib/auth-helpers";
import { getProject, updateProject } from "@/lib/projects";
import { analyzePdfAndGenerate, editExistingHtml } from "@/lib/anthropic";
import { logActivity } from "@/lib/activity";
import type { ResearchMode } from "@/lib/types";

export const maxDuration = 300;

export async function POST(request: NextRequest) {
  try {
    const { user } = await requireAuth();
    const body = await request.json();
    const {
      projectId,
      researchMode = "pdf_only",
      editMode = false,
      editInstructions = "",
    } = body as {
      projectId: string;
      researchMode?: ResearchMode;
      editMode?: boolean;
      editInstructions?: string;
    };

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

    // Update status to generating
    await updateProject(projectId, {
      status: "generating",
      error_message: null,
    });

    try {
      let html: string;

      if (editMode && project.generated_html && editInstructions.trim()) {
        // EDIT MODE: Modify existing HTML based on user instructions
        html = await editExistingHtml(
          project.generated_html,
          editInstructions
        );

        await logActivity(
          user.id,
          "project_edited",
          "project",
          projectId,
          { name: project.name, editMode: true }
        );
      } else {
        // FULL GENERATION: Generate from scratch using PDF
        if (!project.pdf_storage_path) {
          return NextResponse.json(
            { error: "No PDF uploaded for this project" },
            { status: 400 }
          );
        }

        // Validate research mode
        const validModes: ResearchMode[] = ["pdf_only", "deep_research"];
        const mode: ResearchMode = validModes.includes(researchMode)
          ? researchMode
          : "pdf_only";

        html = await analyzePdfAndGenerate(
          project.pdf_storage_path,
          project.prompt,
          mode
        );

        await logActivity(
          user.id,
          "project_generated",
          "project",
          projectId,
          { name: project.name, researchMode: mode }
        );
      }

      // Store generated HTML in the database
      await updateProject(projectId, {
        generated_html: html,
        status: "draft",
      });

      return NextResponse.json({ html, projectId });
    } catch (genError) {
      const errorMessage =
        genError instanceof Error ? genError.message : "Generation failed";
      await updateProject(projectId, {
        status: "error",
        error_message: errorMessage,
      });
      return NextResponse.json({ error: errorMessage }, { status: 500 });
    }
  } catch (error) {
    console.error("Generate error:", error);
    const message =
      error instanceof Error ? error.message : "Failed to generate";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
