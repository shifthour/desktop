import { NextRequest, NextResponse } from "next/server";
import { supabase } from "@/lib/supabase";
import { getReviewsByPhysioId } from "@/lib/data";

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params;

    const { data: reviews, error } = await supabase
      .from("physioconnect_reviews")
      .select("*")
      .eq("physioId", id)
      .order("date", { ascending: false });

    if (error) throw error;

    return NextResponse.json(reviews);
  } catch (error) {
    console.error("DB unavailable, falling back to static data:", error);
    const { id } = await params;
    const staticReviews = getReviewsByPhysioId(id);
    return NextResponse.json(staticReviews);
  }
}
