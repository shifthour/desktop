import { createAdminClient } from "@/lib/supabase/admin";

type ActivityAction =
  | "user_login"
  | "user_created"
  | "user_updated"
  | "user_deactivated"
  | "project_created"
  | "project_updated"
  | "project_generated"
  | "project_edited"
  | "project_deployed"
  | "project_deleted"
  | "pdf_uploaded";

type ResourceType = "user" | "project" | "pdf";

// All activity logging uses admin client to bypass RLS
export async function logActivity(
  userId: string,
  action: ActivityAction,
  resourceType?: ResourceType,
  resourceId?: string,
  details?: Record<string, unknown>
): Promise<void> {
  try {
    const supabase = createAdminClient();

    await supabase.from("yajur_activity_log").insert({
      user_id: userId,
      action,
      resource_type: resourceType || null,
      resource_id: resourceId || null,
      details: details || {},
    });
  } catch (error) {
    // Don't let logging failures break the main flow
    console.error("Failed to log activity:", error);
  }
}

// Kept for backward compatibility, same as logActivity now
export async function logActivityAsAdmin(
  userId: string,
  action: ActivityAction,
  resourceType?: ResourceType,
  resourceId?: string,
  details?: Record<string, unknown>
): Promise<void> {
  return logActivity(userId, action, resourceType, resourceId, details);
}
