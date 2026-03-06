import { NextRequest, NextResponse } from "next/server";
import { getProject, updateProject } from "@/lib/projects";
import { analyzePdfAndGenerate } from "@/lib/anthropic";
import { writeFile, mkdir } from "fs/promises";
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

    // Update status to generating
    await updateProject(projectId, { status: "generating", errorMessage: null });

    try {
      // Call Claude API with the PDF
      const html = await analyzePdfAndGenerate(project.pdfPath, project.prompt);

      // Save generated HTML
      const generatedDir = path.join(process.cwd(), "data", "generated");
      await mkdir(generatedDir, { recursive: true });
      const htmlPath = `data/generated/${projectId}.html`;
      await writeFile(path.join(process.cwd(), htmlPath), html);

      // Update project with generated HTML path
      await updateProject(projectId, {
        generatedHtmlPath: htmlPath,
        status: "draft",
      });

      return NextResponse.json({ html, projectId });
    } catch (genError) {
      const errorMessage =
        genError instanceof Error ? genError.message : "Generation failed";
      await updateProject(projectId, {
        status: "error",
        errorMessage,
      });
      return NextResponse.json({ error: errorMessage }, { status: 500 });
    }
  } catch (error) {
    console.error("Generate error:", error);
    return NextResponse.json(
      { error: "Failed to generate landing page" },
      { status: 500 }
    );
  }
}
