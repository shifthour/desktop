import { NextRequest, NextResponse } from "next/server";
import { createAdminClient } from "@/lib/supabase/admin";
import { hashPassword } from "@/lib/auth";

export const dynamic = "force-dynamic";

// One-time setup endpoint to create admin user in yajur_profiles
export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const { email, password, full_name, setup_key } = body;

    // Simple protection
    if (setup_key !== "yajur-setup-2024") {
      return NextResponse.json({ error: "Invalid setup key" }, { status: 403 });
    }

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
      .select("id, email, role")
      .eq("email", email.toLowerCase().trim())
      .single();

    if (existing) {
      return NextResponse.json({
        success: true,
        message: `User ${email} already exists with role: ${existing.role}`,
        user: existing,
      });
    }

    // Hash password and insert
    const password_hash = await hashPassword(password);

    const { data: newUser, error: createError } = await supabase
      .from("yajur_profiles")
      .insert({
        full_name,
        email: email.toLowerCase().trim(),
        password_hash,
        role: "admin",
      })
      .select("id, full_name, email, role, is_active, created_at")
      .single();

    if (createError) {
      return NextResponse.json(
        { error: `Failed to create user: ${createError.message}` },
        { status: 500 }
      );
    }

    return NextResponse.json({
      success: true,
      message: `Admin user ${email} created successfully. You can now login at /login`,
      user: newUser,
    });
  } catch (error) {
    console.error("Setup error:", error);
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Setup failed" },
      { status: 500 }
    );
  }
}
