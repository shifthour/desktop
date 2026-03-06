export type ResearchMode = "pdf_only" | "deep_research";

export interface UserProfile {
  id: string;
  full_name: string;
  email: string;
  role: "admin" | "user";
  is_active: boolean;
  created_by: string | null;
  created_at: string;
  updated_at: string;
}

export interface Project {
  id: string;
  user_id: string;
  name: string;
  slug: string;
  pdf_storage_path: string | null;
  pdf_original_name: string | null;
  prompt: string;
  generated_html: string | null;
  github_repo_url: string | null;
  vercel_url: string | null;
  vercel_project_name: string | null;
  status: "draft" | "generating" | "deploying" | "deployed" | "error";
  error_message: string | null;
  created_at: string;
  updated_at: string;
  // Joined field (from queries)
  user_profile?: UserProfile;
}

export interface ActivityLogEntry {
  id: number;
  user_id: string;
  action: string;
  resource_type: "user" | "project" | "pdf" | null;
  resource_id: string | null;
  details: Record<string, unknown>;
  created_at: string;
  // Joined field
  user_profile?: UserProfile;
}

export interface CreateProjectRequest {
  name: string;
  prompt: string;
  pdf_storage_path: string;
  pdf_original_name: string;
}

export interface GenerateRequest {
  projectId: string;
  researchMode: ResearchMode;
}

export interface DeployRequest {
  projectId: string;
}

export interface CreateUserRequest {
  email: string;
  password: string;
  full_name: string;
  role: "admin" | "user";
}
