import { NextResponse } from "next/server";
import { requireAdmin } from "@/lib/auth-helpers";
import { createAdminClient } from "@/lib/supabase/admin";
import { logActivity } from "@/lib/activity";
import { hashPassword } from "@/lib/auth";

export const dynamic = "force-dynamic";

export async function GET() {
  try {
    await requireAdmin();

    const supabase = createAdminClient();
    const { data: users, error } = await supabase
      .from("yajur_profiles")
      .select("id, full_name, email, role, is_active, created_by, created_at, updated_at")
      .order("created_at", { ascending: false });

    if (error) {
      return NextResponse.json(
        { error: `Failed to fetch users: ${error.message}` },
        { status: 500 }
      );
    }

    return NextResponse.json({ users });
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Unauthorized";
    const status = message.includes("Forbidden") ? 403 : 401;
    return NextResponse.json({ error: message }, { status });
  }
}

export async function POST(request: Request) {
  try {
    const { user: adminUser } = await requireAdmin();
    const body = await request.json();

    const { email, password, full_name, role } = body;

    if (!email || !password || !full_name) {
      return NextResponse.json(
        { error: "Missing required fields: email, password, full_name" },
        { status: 400 }
      );
    }

    if (password.length < 6) {
      return NextResponse.json(
        { error: "Password must be at least 6 characters" },
        { status: 400 }
      );
    }

    const supabase = createAdminClient();

    // Check if email already exists
    const { data: existing } = await supabase
      .from("yajur_profiles")
      .select("id")
      .eq("email", email.toLowerCase().trim())
      .single();

    if (existing) {
      return NextResponse.json(
        { error: "A user with this email already exists" },
        { status: 409 }
      );
    }

    // Hash password and insert into our own table
    const password_hash = await hashPassword(password);

    const { data: newUser, error: createError } = await supabase
      .from("yajur_profiles")
      .insert({
        full_name,
        email: email.toLowerCase().trim(),
        password_hash,
        role: role || "user",
        created_by: adminUser.id,
      })
      .select("id, full_name, email, role, is_active, created_at")
      .single();

    if (createError) {
      return NextResponse.json(
        { error: `Failed to create user: ${createError.message}` },
        { status: 500 }
      );
    }

    await logActivity(
      adminUser.id,
      "user_created",
      "user",
      newUser.id,
      { email, full_name, role: role || "user" }
    );

    return NextResponse.json(
      { user: newUser, message: "User created successfully" },
      { status: 201 }
    );
  } catch (error) {
    console.error("Create user error:", error);
    const message =
      error instanceof Error ? error.message : "Failed to create user";
    const status = message.includes("Forbidden") ? 403 : 500;
    return NextResponse.json({ error: message }, { status });
  }
}
