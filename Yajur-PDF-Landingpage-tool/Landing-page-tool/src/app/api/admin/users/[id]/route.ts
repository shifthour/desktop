import { NextRequest, NextResponse } from "next/server";
import { requireAdmin } from "@/lib/auth-helpers";
import { createAdminClient } from "@/lib/supabase/admin";
import { logActivityAsAdmin } from "@/lib/activity";

export async function PUT(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { user: adminUser } = await requireAdmin();
    const { id } = await params;
    const body = await request.json();

    const supabase = createAdminClient();

    // Build update object
    const updates: Record<string, unknown> = {};
    if (body.role !== undefined) updates.role = body.role;
    if (body.is_active !== undefined) updates.is_active = body.is_active;
    if (body.full_name !== undefined) updates.full_name = body.full_name;

    if (Object.keys(updates).length === 0) {
      return NextResponse.json(
        { error: "No fields to update" },
        { status: 400 }
      );
    }

    const { data: profile, error } = await supabase
      .from("yajur_profiles")
      .update(updates)
      .eq("id", id)
      .select()
      .single();

    if (error) {
      return NextResponse.json(
        { error: `Failed to update user: ${error.message}` },
        { status: 500 }
      );
    }

    const action =
      body.is_active === false ? "user_deactivated" : "user_updated";

    await logActivityAsAdmin(adminUser.id, action, "user", id, {
      updates,
    });

    return NextResponse.json({ profile });
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Failed to update user";
    const status = message.includes("Forbidden") ? 403 : 500;
    return NextResponse.json({ error: message }, { status });
  }
}
