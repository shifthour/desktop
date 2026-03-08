import { NextRequest, NextResponse } from "next/server";
import { supabase } from "@/lib/supabase";

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();

    // Basic validation
    const required = ["firstName", "lastName", "email", "phone", "hcpcNumber", "professionalBody", "membershipNumber", "yearsExperience"];
    for (const field of required) {
      if (!body[field]) {
        return NextResponse.json({ error: `${field} is required` }, { status: 400 });
      }
    }

    const { data, error } = await supabase
      .from("physioconnect_applications")
      .insert({
        firstName: body.firstName,
        lastName: body.lastName,
        email: body.email,
        phone: body.phone,
        dateOfBirth: body.dateOfBirth || null,
        profilePhotoUrl: body.profilePhotoUrl || null,
        resumeUrl: body.resumeUrl || null,
        idProofUrl: body.idProofUrl || null,
        hcpcNumber: body.hcpcNumber,
        professionalBody: body.professionalBody,
        membershipNumber: body.membershipNumber,
        yearsExperience: body.yearsExperience,
        qualifications: body.qualifications || null,
        specialisations: body.specialisations || [],
        serviceRadius: body.serviceRadius || null,
        homeVisit: body.homeVisit || false,
        online: body.online || false,
        weeklySchedule: body.weeklySchedule || {},
        bio: body.bio || null,
        eligibilityType: body.eligibilityType || null,
        visaDocUrl: body.visaDocUrl || null,
        passportPage1Url: body.passportPage1Url || null,
        passportPage2Url: body.passportPage2Url || null,
        agreeTerms: body.agreeTerms || false,
        agreePrivacy: body.agreePrivacy || false,
        agreeDBS: body.agreeDBS || false,
        agreeRightToWork: body.agreeRightToWork || false,
        status: "pending",
      })
      .select()
      .single();

    if (error) {
      console.error("Application insert error:", error);
      return NextResponse.json({ error: error.message }, { status: 500 });
    }

    return NextResponse.json({ id: data.id, success: true });
  } catch (err) {
    console.error("Application submission failed:", err);
    return NextResponse.json({ error: "Submission failed" }, { status: 500 });
  }
}
