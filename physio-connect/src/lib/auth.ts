import { supabaseAdmin } from "./supabase-server";
import { cookies } from "next/headers";

export interface AdminSession {
  adminId: string;
  displayName: string;
  username: string;
}

export async function validateSession(): Promise<AdminSession | null> {
  try {
    const cookieStore = await cookies();
    const token = cookieStore.get("admin_session")?.value;

    if (!token) return null;

    const { data: session, error } = await supabaseAdmin
      .from("physioconnect_admin_sessions")
      .select("*, admin:physioconnect_admin_users(*)")
      .eq("token", token)
      .gt("expiresAt", new Date().toISOString())
      .single();

    if (error || !session) return null;

    return {
      adminId: session.adminUserId,
      displayName: session.admin.displayName,
      username: session.admin.username,
    };
  } catch {
    return null;
  }
}
