import { Physiotherapist } from "./types";

/**
 * Generate dynamic availability slots for the next 14 days.
 * This replicates the logic from data.ts — availability is not stored in DB.
 */
function generateSlots(dateStr: string, booked: string[] = []) {
  const times = [
    "09:00", "09:30", "10:00", "10:30", "11:00", "11:30",
    "14:00", "14:30", "15:00", "15:30", "16:00", "16:30",
    "17:00", "17:30", "18:00",
  ];
  return {
    date: dateStr,
    slots: times.map((t) => ({
      time: t,
      available: !booked.includes(t),
    })),
  };
}

function getNextDays(count: number): string[] {
  const days: string[] = [];
  const today = new Date();
  for (let i = 0; i < count; i++) {
    const d = new Date(today);
    d.setDate(today.getDate() + i);
    days.push(d.toISOString().split("T")[0]);
  }
  return days;
}

function generateAvailability(physioIndex: number) {
  const nextDays = getNextDays(14);
  // Use varying booked patterns based on physio index for variety
  return nextDays.map((d, i) => {
    const pattern = (physioIndex + i) % 4;
    const booked =
      pattern === 0
        ? ["10:00", "14:30", "16:00"]
        : pattern === 1
        ? ["09:30", "11:00"]
        : pattern === 2
        ? ["15:00", "17:30"]
        : ["09:00", "11:30", "15:30"];
    return generateSlots(d, booked);
  });
}

/**
 * Transform a Prisma physiotherapist record (with includes) to the
 * frontend Physiotherapist interface.
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function transformPhysio(dbPhysio: any, index: number = 0): Physiotherapist {
  return {
    id: dbPhysio.id,
    slug: dbPhysio.slug,
    name: dbPhysio.name,
    photo: dbPhysio.photo,
    gender: dbPhysio.gender,
    specializations: dbPhysio.specializations.map(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (ps: any) => ps.specialization.name
    ),
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    qualifications: dbPhysio.qualifications.map((q: any) => q.text),
    experience: dbPhysio.experience,
    bio: dbPhysio.bio,
    rating: dbPhysio.rating,
    reviewCount: dbPhysio.reviewCount,
    clinicName: dbPhysio.clinicName,
    location: {
      area: dbPhysio.locationArea,
      city: dbPhysio.locationCity,
      lat: dbPhysio.locationLat,
      lng: dbPhysio.locationLng,
    },
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    visitTypes: dbPhysio.visitTypes.map((vt: any) => vt.visitType),
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    services: dbPhysio.services.map((s: any) => ({
      id: s.id,
      name: s.name,
      duration: s.duration,
      price: s.price,
      description: s.description,
    })),
    availability: generateAvailability(index),
    verified: dbPhysio.verified,
    totalSessions: dbPhysio.totalSessions,
  };
}

/** Standard Prisma include for fetching a full physiotherapist */
export const physioIncludes = {
  specializations: { include: { specialization: true } },
  qualifications: { orderBy: { sortOrder: "asc" as const } },
  visitTypes: true,
  services: { orderBy: { sortOrder: "asc" as const } },
};
