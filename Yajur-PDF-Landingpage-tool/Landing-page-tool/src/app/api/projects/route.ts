import { NextRequest, NextResponse } from "next/server";
import { requireAuth } from "@/lib/auth-helpers";
import {
  getProjectsForUser,
  getAllProjects,
  createProject,
} from "@/lib/projects";
import { logActivity } from "@/lib/activity";

export const dynamic = "force-dynamic";

export async function GET(request: NextRequest) {
  try {
    const { user, profile } = await requireAuth();

    const searchParams = request.nextUrl.searchParams;
    const showAll = searchParams.get("all") === "true";

    let projects;
    if (showAll && profile.role === "admin") {
      projects = await getAllProjects();
    } else {
      projects = await getProjectsForUser(user.id);
    }

    return NextResponse.json({ projects });
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Failed to fetch projects";
    return NextResponse.json({ error: message }, { status: 401 });
  }
}

export async function POST(request: NextRequest) {
  try {
    const { user } = await requireAuth();
    const body = await request.json();

    const { name, prompt, pdf_storage_path, pdf_original_name } = body;

    if (!name || !pdf_storage_path) {
      return NextResponse.json(
        { error: "Missing required fields" },
        { status: 400 }
      );
    }

    const project = await createProject(
      {
        name,
        prompt: prompt || "",
        pdf_storage_path,
        pdf_original_name: pdf_original_name || "",
      },
      user.id
    );

    await logActivity(user.id, "project_created", "project", project.id, {
      name: project.name,
    });

    return NextResponse.json(project, { status: 201 });
  } catch (error) {
    console.error("Create project error:", error);
    const message =
      error instanceof Error ? error.message : "Failed to create project";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
