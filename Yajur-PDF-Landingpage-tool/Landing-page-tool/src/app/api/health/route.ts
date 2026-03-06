import { NextResponse } from "next/server";
import { createAdminClient } from "@/lib/supabase/admin";

export const dynamic = "force-dynamic";

export async function GET() {
  const checks: Record<string, unknown> = {
    timestamp: new Date().toISOString(),
    env: {
      NEXT_PUBLIC_SUPABASE_URL: !!process.env.NEXT_PUBLIC_SUPABASE_URL,
      SUPABASE_SERVICE_ROLE_KEY: !!process.env.SUPABASE_SERVICE_ROLE_KEY,
      ANTHROPIC_API_KEY: !!process.env.ANTHROPIC_API_KEY,
      VERCEL_TOKEN: !!process.env.VERCEL_TOKEN,
      GITHUB_TOKEN: !!process.env.GITHUB_TOKEN,
    },
  };

  try {
    if (
      !process.env.NEXT_PUBLIC_SUPABASE_URL ||
      !process.env.SUPABASE_SERVICE_ROLE_KEY
    ) {
      checks.database = { status: "error", message: "Missing Supabase env vars" };
    } else {
      const supabase = createAdminClient();

      const { data: profiles, error: profilesError } = await supabase
        .from("yajur_profiles")
        .select("id, email, role")
        .limit(5);

      if (profilesError) {
        checks.database = {
          status: "error",
          message: profilesError.message,
          code: profilesError.code,
        };
      } else {
        const { error: projectsError } = await supabase
          .from("yajur_projects")
          .select("id")
          .limit(1);

        const { error: activityError } = await supabase
          .from("yajur_activity_log")
          .select("id")
          .limit(1);

        checks.database = {
          status: "ok",
          profiles_count: profiles?.length ?? 0,
          profiles: profiles?.map((p) => ({
            email: p.email,
            role: p.role,
          })),
          tables: {
            yajur_profiles: !profilesError,
            yajur_projects: !projectsError,
            yajur_activity_log: !activityError,
          },
        };
      }
    }
  } catch (error) {
    checks.database = {
      status: "exception",
      message: error instanceof Error ? error.message : "Unknown error",
    };
  }

  const dbOk = (checks.database as Record<string, unknown>)?.status === "ok";
  return NextResponse.json(
    { status: dbOk ? "healthy" : "unhealthy", ...checks },
    { status: dbOk ? 200 : 503 }
  );
}
