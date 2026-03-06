import { NextRequest, NextResponse } from "next/server";
import { createAdminClient } from "@/lib/supabase/admin";
import { verifyPassword, createSessionToken, COOKIE_NAME } from "@/lib/auth";

export async function POST(request: NextRequest) {
  try {
    const { email, password } = await request.json();

    if (!email || !password) {
      return NextResponse.json(
        { error: "Email and password are required" },
        { status: 400 }
      );
    }

    const supabase = createAdminClient();

    // Look up user in our own table
    const { data: user, error } = await supabase
      .from("yajur_profiles")
      .select("id, email, password_hash, full_name, role, is_active")
      .eq("email", email.toLowerCase().trim())
      .single();

    if (error || !user) {
      return NextResponse.json(
        { error: "Invalid email or password" },
        { status: 401 }
      );
    }

    if (!user.is_active) {
      return NextResponse.json(
        { error: "Account has been deactivated" },
        { status: 403 }
      );
    }

    // Verify password
    const isValid = await verifyPassword(password, user.password_hash);
    if (!isValid) {
      return NextResponse.json(
        { error: "Invalid email or password" },
        { status: 401 }
      );
    }

    // Create JWT session token
    const token = await createSessionToken(user.id, user.email, user.role);

    // Log activity
    try {
      await supabase.from("yajur_activity_log").insert({
        user_id: user.id,
        action: "user_login",
        details: {},
      });
    } catch {
      // Don't block login if logging fails
    }

    // Set HTTP-only cookie
    const response = NextResponse.json({
      success: true,
      user: {
        id: user.id,
        email: user.email,
        full_name: user.full_name,
        role: user.role,
      },
    });

    response.cookies.set(COOKIE_NAME, token, {
      httpOnly: true,
      secure: process.env.NODE_ENV === "production",
      sameSite: "lax",
      maxAge: 60 * 60 * 24 * 7, // 7 days
      path: "/",
    });

    return response;
  } catch (error) {
    console.error("Login error:", error);
    return NextResponse.json(
      { error: "Login failed" },
      { status: 500 }
    );
  }
}
