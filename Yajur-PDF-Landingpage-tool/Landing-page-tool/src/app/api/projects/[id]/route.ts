import { NextRequest, NextResponse } from "next/server";
import { requireAuth } from "@/lib/auth-helpers";
import { getProject, updateProject, deleteProject } from "@/lib/projects";
import { logActivity } from "@/lib/activity";

export async function GET(
  _request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    await requireAuth();
    const { id } = await params;

    const project = await getProject(id);
    if (!project) {
      return NextResponse.json(
        { error: "Project not found" },
        { status: 404 }
      );
    }

    return NextResponse.json(project);
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Failed to fetch project";
    return NextResponse.json({ error: message }, { status: 401 });
  }
}

export async function PUT(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { user } = await requireAuth();
    const { id } = await params;
    const body = await request.json();

    const project = await updateProject(id, body);

    await logActivity(user.id, "project_updated", "project", id, {
      fields: Object.keys(body),
    });

    return NextResponse.json(project);
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Failed to update project";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

export async function DELETE(
  _request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { user } = await requireAuth();
    const { id } = await params;

    await deleteProject(id);

    await logActivity(user.id, "project_deleted", "project", id);

    return NextResponse.json({ success: true });
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Failed to delete project";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
