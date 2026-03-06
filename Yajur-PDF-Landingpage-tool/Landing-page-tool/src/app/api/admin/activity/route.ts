import { NextRequest, NextResponse } from "next/server";
import { requireAdmin } from "@/lib/auth-helpers";
import { createAdminClient } from "@/lib/supabase/admin";

export const dynamic = "force-dynamic";

export async function GET(request: NextRequest) {
  try {
    await requireAdmin();

    const searchParams = request.nextUrl.searchParams;
    const limit = parseInt(searchParams.get("limit") || "50", 10);
    const offset = parseInt(searchParams.get("offset") || "0", 10);

    const supabase = createAdminClient();

    const { data: activities, error } = await supabase
      .from("yajur_activity_log")
      .select(
        "*, user_profile:yajur_profiles!user_id(full_name, email)"
      )
      .order("created_at", { ascending: false })
      .range(offset, offset + limit - 1);

    if (error) {
      return NextResponse.json(
        { error: `Failed to fetch activity: ${error.message}` },
        { status: 500 }
      );
    }

    return NextResponse.json({ activities });
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Unauthorized";
    const status = message.includes("Forbidden") ? 403 : 401;
    return NextResponse.json({ error: message }, { status });
  }
}
