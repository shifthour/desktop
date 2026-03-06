import { NextRequest, NextResponse } from "next/server";
import { getAllProjects, createProject } from "@/lib/projects";

export async function GET() {
  try {
    const projects = await getAllProjects();
    return NextResponse.json(projects);
  } catch (error) {
    console.error("Error fetching projects:", error);
    return NextResponse.json(
      { error: "Failed to fetch projects" },
      { status: 500 }
    );
  }
}

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const { name, prompt, pdfPath, pdfOriginalName } = body;

    if (!name || !prompt || !pdfPath) {
      return NextResponse.json(
        { error: "Missing required fields: name, prompt, pdfPath" },
        { status: 400 }
      );
    }

    const project = await createProject({
      name,
      prompt,
      pdfPath,
      pdfOriginalName: pdfOriginalName || "document.pdf",
    });

    return NextResponse.json(project, { status: 201 });
  } catch (error) {
    console.error("Error creating project:", error);
    return NextResponse.json(
      { error: "Failed to create project" },
      { status: 500 }
    );
  }
}
