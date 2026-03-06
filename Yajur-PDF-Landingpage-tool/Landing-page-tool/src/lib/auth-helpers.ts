import { createAdminClient } from "@/lib/supabase/admin";
import { getSessionFromCookies } from "@/lib/auth";
import type { UserProfile } from "@/lib/types";

export async function getCurrentUser(): Promise<{
  user: { id: string; email: string };
  profile: UserProfile;
} | null> {
  // Read JWT from cookie
  const session = await getSessionFromCookies();
  if (!session) return null;

  // Fetch full profile from database
  const supabase = createAdminClient();
  const { data: profile, error } = await supabase
    .from("yajur_profiles")
    .select("id, full_name, email, role, is_active, created_by, created_at, updated_at")
    .eq("id", session.userId)
    .single();

  if (error || !profile) return null;

  return {
    user: { id: profile.id, email: profile.email },
    profile: profile as UserProfile,
  };
}

export async function requireAuth(): Promise<{
  user: { id: string; email: string };
  profile: UserProfile;
}> {
  const result = await getCurrentUser();
  if (!result) {
    throw new Error("Unauthorized");
  }
  if (!result.profile.is_active) {
    throw new Error("Account deactivated");
  }
  return result;
}

export async function requireAdmin(): Promise<{
  user: { id: string; email: string };
  profile: UserProfile;
}> {
  const result = await requireAuth();
  if (result.profile.role !== "admin") {
    throw new Error("Forbidden: Admin access required");
  }
  return result;
}
