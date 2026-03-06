import { Physiotherapist, Review } from "./types";

const BASE = "/api";

export async function fetchPhysios(): Promise<Physiotherapist[]> {
  const res = await fetch(`${BASE}/physios`);
  if (!res.ok) throw new Error("Failed to fetch physios");
  return res.json();
}

export async function fetchPhysioBySlug(
  slug: string
): Promise<Physiotherapist | null> {
  const res = await fetch(`${BASE}/physios/${slug}`);
  if (res.status === 404) return null;
  if (!res.ok) throw new Error("Failed to fetch physio");
  return res.json();
}

export async function fetchPhysioById(
  id: string
): Promise<Physiotherapist | null> {
  const res = await fetch(`${BASE}/physio/${id}`);
  if (res.status === 404) return null;
  if (!res.ok) throw new Error("Failed to fetch physio");
  return res.json();
}

export async function fetchReviews(physioId: string): Promise<Review[]> {
  const res = await fetch(`${BASE}/physio/${physioId}/reviews`);
  if (!res.ok) throw new Error("Failed to fetch reviews");
  return res.json();
}

export interface SpecializationItem {
  id: string;
  name: string;
  icon: string;
  count: number;
  image: string;
}

export async function fetchSpecializations(): Promise<SpecializationItem[]> {
  const res = await fetch(`${BASE}/specializations`);
  if (!res.ok) throw new Error("Failed to fetch specializations");
  return res.json();
}
