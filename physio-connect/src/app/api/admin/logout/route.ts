import { NextResponse } from "next/server";
import { supabaseAdmin } from "@/lib/supabase-server";
import { cookies } from "next/headers";

export async function POST() {
  try {
    const cookieStore = await cookies();
    const token = cookieStore.get("admin_session")?.value;

    if (token) {
      // Delete session from DB
      await supabaseAdmin
        .from("physioconnect_admin_sessions")
        .delete()
        .eq("token", token);
    }

    // Clear cookie
    cookieStore.set("admin_session", "", {
      httpOnly: true,
      secure: process.env.NODE_ENV === "production",
      sameSite: "lax",
      path: "/",
      maxAge: 0,
    });

    return NextResponse.json({ success: true });
  } catch {
    return NextResponse.json({ error: "Logout failed" }, { status: 500 });
  }
}
