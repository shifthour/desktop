import { NextRequest, NextResponse } from "next/server";
import { supabaseAdmin } from "@/lib/supabase-server";
import { validateSession } from "@/lib/auth";

export async function POST(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const session = await validateSession();
    if (!session) {
      return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
    }

    const { id } = await params;

    // 1. Fetch the application
    const { data: app, error: fetchErr } = await supabaseAdmin
      .from("physioconnect_applications")
      .select("*")
      .eq("id", id)
      .single();

    if (fetchErr || !app) {
      return NextResponse.json({ error: "Application not found" }, { status: 404 });
    }

    if (app.status === "approved") {
      return NextResponse.json({ error: "Application already approved" }, { status: 400 });
    }

    // 2. Generate a slug from name
    const fullName = `${app.firstName} ${app.lastName}`;
    const slug = fullName
      .toLowerCase()
      .replace(/[^a-z0-9\s]/g, "")
      .replace(/\s+/g, "-");

    // Check slug uniqueness, append random suffix if needed
    const { data: existing } = await supabaseAdmin
      .from("physioconnect_physiotherapists")
      .select("id")
      .eq("slug", slug)
      .single();

    const finalSlug = existing ? `${slug}-${Math.random().toString(36).slice(2, 6)}` : slug;

    // 3. Get signed URL for profile photo to use as photo field
    let photoUrl = "";
    if (app.profilePhotoUrl) {
      const { data: urlData } = await supabaseAdmin.storage
        .from("applications")
        .createSignedUrl(app.profilePhotoUrl, 60 * 60 * 24 * 365); // 1 year
      photoUrl = urlData?.signedUrl || "";
    }

    // 4. Determine experience number from range
    const expMap: Record<string, number> = {
      "2-5": 3,
      "5-10": 7,
      "10-15": 12,
      "15+": 17,
    };
    const experience = expMap[app.yearsExperience] || 5;

    // 5. Create physiotherapist record
    const { data: physio, error: physioErr } = await supabaseAdmin
      .from("physioconnect_physiotherapists")
      .insert({
        slug: finalSlug,
        name: fullName,
        photo: photoUrl,
        gender: "other",
        experience,
        bio: app.bio || `Qualified physiotherapist with ${app.yearsExperience} years of UK experience.`,
        rating: 0,
        reviewCount: 0,
        clinicName: "Independent Practitioner",
        locationArea: "TBC",
        locationCity: "London",
        locationLat: 51.5074,
        locationLng: -0.1278,
        verified: false,
        totalSessions: 0,
      })
      .select()
      .single();

    if (physioErr || !physio) {
      console.error("Physio create error:", physioErr);
      return NextResponse.json({ error: "Failed to create physio record" }, { status: 500 });
    }

    // 6. Create specialization links
    if (app.specialisations && app.specialisations.length > 0) {
      // Lookup specialization IDs by name (case-insensitive match)
      const { data: specs } = await supabaseAdmin
        .from("physioconnect_specializations")
        .select("id, name");

      if (specs) {
        const specMap = new Map(specs.map((s: { id: string; name: string }) => [s.name.toLowerCase(), s.id]));
        const links = app.specialisations
          .map((name: string) => {
            const specId = specMap.get(name.toLowerCase());
            return specId ? { physioId: physio.id, specializationId: specId } : null;
          })
          .filter(Boolean);

        if (links.length > 0) {
          await supabaseAdmin.from("physioconnect_physio_specializations").insert(links);
        }
      }
    }

    // 7. Create visit types
    const visitTypeLinks = [];
    if (app.homeVisit) visitTypeLinks.push({ physioId: physio.id, visitType: "home" });
    if (app.online) visitTypeLinks.push({ physioId: physio.id, visitType: "online" });
    if (visitTypeLinks.length > 0) {
      await supabaseAdmin.from("physioconnect_physio_visit_types").insert(visitTypeLinks);
    }

    // 8. Create qualifications from text
    if (app.qualifications) {
      const qualLines = app.qualifications
        .split(/[,\n]/)
        .map((q: string) => q.trim())
        .filter((q: string) => q.length > 0);

      if (qualLines.length > 0) {
        const qualRecords = qualLines.map((text: string, i: number) => ({
          physioId: physio.id,
          text,
          sortOrder: i,
        }));
        await supabaseAdmin.from("physioconnect_qualifications").insert(qualRecords);
      }
    }

    // 9. Add HCPC registration as qualification
    await supabaseAdmin.from("physioconnect_qualifications").insert({
      physioId: physio.id,
      text: `HCPC Registered (${app.hcpcNumber})`,
      sortOrder: 99,
    });

    // 10. Mark application as approved
    await supabaseAdmin
      .from("physioconnect_applications")
      .update({
        status: "approved",
        reviewedBy: session.adminId,
        reviewedAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
      })
      .eq("id", id);

    return NextResponse.json({
      success: true,
      physioId: physio.id,
      slug: finalSlug,
    });
  } catch (err) {
    console.error("Approval failed:", err);
    return NextResponse.json({ error: "Approval failed" }, { status: 500 });
  }
}
