import { createAdminClient } from "@/lib/supabase/admin";
import type { Project } from "@/lib/types";

// All server-side DB operations use admin client (service role key).
// Auth is verified via JWT session in auth-helpers.ts.
// Data isolation is enforced by filtering on user_id in queries.

function slugify(name: string): string {
  return name
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "")
    .substring(0, 50);
}

export async function getProjectsForUser(userId: string): Promise<Project[]> {
  const supabase = createAdminClient();

  const { data, error } = await supabase
    .from("yajur_projects")
    .select("*")
    .eq("user_id", userId)
    .order("updated_at", { ascending: false });

  if (error) throw new Error(`Failed to fetch projects: ${error.message}`);
  return (data || []) as Project[];
}

export async function getAllProjects(): Promise<Project[]> {
  const supabase = createAdminClient();

  const { data, error } = await supabase
    .from("yajur_projects")
    .select("*, user_profile:yajur_profiles!user_id(full_name, email)")
    .order("updated_at", { ascending: false });

  if (error) throw new Error(`Failed to fetch all projects: ${error.message}`);
  return (data || []) as Project[];
}

export async function getProject(id: string): Promise<Project | null> {
  const supabase = createAdminClient();

  const { data, error } = await supabase
    .from("yajur_projects")
    .select("*")
    .eq("id", id)
    .single();

  if (error) {
    if (error.code === "PGRST116") return null; // Not found
    throw new Error(`Failed to fetch project: ${error.message}`);
  }

  return data as Project;
}

export async function createProject(
  data: {
    name: string;
    prompt: string;
    pdf_storage_path: string;
    pdf_original_name: string;
  },
  userId: string
): Promise<Project> {
  const supabase = createAdminClient();

  const projectData = {
    user_id: userId,
    name: data.name,
    slug: slugify(data.name),
    pdf_storage_path: data.pdf_storage_path,
    pdf_original_name: data.pdf_original_name,
    prompt: data.prompt,
    status: "draft" as const,
  };

  const { data: project, error } = await supabase
    .from("yajur_projects")
    .insert(projectData)
    .select()
    .single();

  if (error) throw new Error(`Failed to create project: ${error.message}`);
  return project as Project;
}

export async function updateProject(
  id: string,
  updates: Partial<Project>
): Promise<Project> {
  const supabase = createAdminClient();

  // Remove fields that shouldn't be updated directly
  const { id: _id, user_id: _userId, created_at: _created, ...safeUpdates } =
    updates;

  const { data: project, error } = await supabase
    .from("yajur_projects")
    .update(safeUpdates)
    .eq("id", id)
    .select()
    .single();

  if (error) throw new Error(`Failed to update project: ${error.message}`);
  return project as Project;
}

export async function deleteProject(id: string): Promise<void> {
  const supabase = createAdminClient();

  // Get the project first to clean up storage
  const project = await getProject(id);
  if (!project) return;

  // Delete PDF from storage if exists
  if (project.pdf_storage_path) {
    try {
      await supabase.storage
        .from("yajur-pdfs")
        .remove([project.pdf_storage_path]);
    } catch {
      // Ignore storage deletion errors
    }
  }

  // Delete the project record
  const { error } = await supabase
    .from("yajur_projects")
    .delete()
    .eq("id", id);

  if (error) throw new Error(`Failed to delete project: ${error.message}`);
}

export async function updateProjectByAdmin(
  id: string,
  updates: Partial<Project>
): Promise<Project> {
  const supabase = createAdminClient();

  const { id: _id, user_id: _userId, created_at: _created, ...safeUpdates } =
    updates;

  const { data: project, error } = await supabase
    .from("yajur_projects")
    .update(safeUpdates)
    .eq("id", id)
    .select()
    .single();

  if (error) throw new Error(`Failed to update project: ${error.message}`);
  return project as Project;
}
