import Link from "next/link";
import { Physiotherapist } from "@/lib/types";

function getNextAvailable(physio: Physiotherapist): string | null {
  for (const day of physio.availability) {
    const slot = day.slots.find((s) => s.available);
    if (slot) {
      const date = new Date(day.date);
      const today = new Date();
      today.setHours(0, 0, 0, 0);
      const diff = Math.floor((date.getTime() - today.getTime()) / 86400000);
      const label = diff === 0 ? "Today" : diff === 1 ? "Tomorrow" : date.toLocaleDateString("en-GB", { weekday: "short", month: "short", day: "numeric" });
      return `${label}, ${slot.time}`;
    }
  }
  return null;
}

function getInitials(name: string) {
  return name.replace(/^Dr\.\s*/, "").split(" ").map(n => n[0]).join("").slice(0, 2).toUpperCase();
}

const avatarColors = [
  "from-primary to-primary-dark",
  "from-coral to-coral-dark",
  "from-sage to-sage-dark",
  "from-primary-400 to-primary-600",
  "from-coral-400 to-coral-600",
  "from-teal-500 to-teal-700",
];

export default function ProviderCard({ physio }: { physio: Physiotherapist }) {
  const nextSlot = getNextAvailable(physio);
  const minPrice = Math.min(...physio.services.map((s) => s.price));
  const colorIndex = parseInt(physio.id) % avatarColors.length;

  return (
    <div className="card-hover group">
      <div className="flex gap-4">
        {/* Avatar */}
        <div className={`w-16 h-16 sm:w-20 sm:h-20 rounded-2xl bg-gradient-to-br ${avatarColors[colorIndex]} flex items-center justify-center flex-shrink-0 shadow-md group-hover:shadow-lg transition-shadow`}>
          <span className="text-white font-bold text-lg sm:text-xl">{getInitials(physio.name)}</span>
        </div>

        <div className="flex-1 min-w-0">
          {/* Name & Price */}
          <div className="flex items-start justify-between gap-2">
            <div>
              <Link href={`/physio/${physio.slug}`} className="font-bold text-lg text-navy hover:text-primary transition-colors">
                {physio.name}
              </Link>
              {physio.verified && (
                <span className="badge-hcpc ml-2 text-[11px]">
                  <svg className="w-3.5 h-3.5" fill="currentColor" viewBox="0 0 20 20">
                    <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clipRule="evenodd" />
                  </svg>
                  HCPC Verified
                </span>
              )}
            </div>
            <span className="text-lg font-bold text-navy whitespace-nowrap">
              &pound;{minPrice}
              <span className="text-sm font-normal text-gray-400">/session</span>
            </span>
          </div>

          {/* Specialisations */}
          <p className="text-primary-600 text-sm mt-0.5 font-medium">
            {physio.specializations.join(" · ")}
          </p>

          {/* Rating & Experience */}
          <div className="flex items-center gap-4 mt-2 text-sm text-gray-500">
            <span className="flex items-center gap-1">
              <svg className="w-4 h-4 text-amber-400" fill="currentColor" viewBox="0 0 20 20">
                <path d="M9.049 2.927c.3-.921 1.603-.921 1.902 0l1.07 3.292a1 1 0 00.95.69h3.462c.969 0 1.371 1.24.588 1.81l-2.8 2.034a1 1 0 00-.364 1.118l1.07 3.292c.3.921-.755 1.688-1.54 1.118l-2.8-2.034a1 1 0 00-1.175 0l-2.8 2.034c-.784.57-1.838-.197-1.539-1.118l1.07-3.292a1 1 0 00-.364-1.118L2.98 8.72c-.783-.57-.38-1.81.588-1.81h3.461a1 1 0 00.951-.69l1.07-3.292z" />
              </svg>
              <span className="font-semibold text-navy">{physio.rating}</span>
              <span className="text-gray-400">({physio.reviewCount})</span>
            </span>
            <span className="text-gray-300">|</span>
            <span>{physio.experience} yrs experience</span>
          </div>

          {/* Location */}
          <div className="flex items-center gap-1.5 mt-1.5 text-sm text-gray-500">
            <svg className="w-4 h-4 text-coral" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth="2">
              <path strokeLinecap="round" strokeLinejoin="round" d="M17.657 16.657L13.414 20.9a1.998 1.998 0 01-2.827 0l-4.244-4.243a8 8 0 1111.314 0z" />
              <path strokeLinecap="round" strokeLinejoin="round" d="M15 11a3 3 0 11-6 0 3 3 0 016 0z" />
            </svg>
            {physio.location.area}, {physio.location.city}
          </div>

          {/* Visit types */}
          <div className="flex flex-wrap items-center gap-2 mt-3">
            {physio.visitTypes.includes("home") && (
              <span className="chip bg-primary-light text-primary-dark text-xs font-semibold">
                <svg className="w-3.5 h-3.5 mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth="2"><path strokeLinecap="round" strokeLinejoin="round" d="M3 12l2-2m0 0l7-7 7 7M5 10v10a1 1 0 001 1h3m10-11l2 2m-2-2v10a1 1 0 01-1 1h-3m-6 0a1 1 0 001-1v-4a1 1 0 011-1h2a1 1 0 011 1v4a1 1 0 001 1m-6 0h6" /></svg>
                Home Visit
              </span>
            )}
            {physio.visitTypes.includes("clinic") && (
              <span className="chip bg-coral-light text-coral-dark text-xs font-semibold">Clinic</span>
            )}
            {physio.visitTypes.includes("online") && (
              <span className="chip bg-sage-light text-sage-dark text-xs font-semibold">Online</span>
            )}
          </div>

          {/* Next available + Actions */}
          <div className="flex items-center justify-between mt-4 pt-3 border-t border-gray-100">
            {nextSlot && (
              <span className="text-sm font-medium text-primary flex items-center gap-1.5">
                <span className="w-2 h-2 rounded-full bg-primary inline-block animate-pulse"></span>
                Next: {nextSlot}
              </span>
            )}
            <div className="flex gap-2 ml-auto">
              <Link href={`/physio/${physio.slug}`} className="text-sm font-semibold text-primary hover:text-primary-dark transition-colors">
                View Profile
              </Link>
              <Link href={`/book/${physio.id}`} className="btn-coral text-sm !py-1.5 !px-5 !shadow-sm">
                Book Now
              </Link>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
