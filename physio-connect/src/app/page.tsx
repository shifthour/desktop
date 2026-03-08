import Link from "next/link";
import Image from "next/image";
import {
  testimonials,
  physiotherapists as staticPhysios,
  specializations as staticSpecs,
} from "@/lib/data";
import { supabase } from "@/lib/supabase";
import { transformPhysio, PHYSIO_SELECT } from "@/lib/transforms";
import ProviderCard from "@/components/ProviderCard";
import VideoHero from "@/components/VideoHero";

export const dynamic = "force-dynamic";

export default async function HomePage() {
  let topRated;
  let specializations;

  try {
    // Fetch top-rated physios from Supabase (Server Component)
    const { data: dbPhysios, error: physioError } = await supabase
      .from("physioconnect_physiotherapists")
      .select(PHYSIO_SELECT)
      .order("rating", { ascending: false })
      .limit(4);

    if (physioError) throw physioError;
    topRated = (dbPhysios || []).map((p, i) => transformPhysio(p, i));

    // Fetch specializations from Supabase
    const { data: specs, error: specError } = await supabase
      .from("physioconnect_specializations")
      .select("*")
      .order("name", { ascending: true });

    if (specError) throw specError;
    specializations = specs || staticSpecs;
  } catch (error) {
    console.error("DB unavailable, falling back to static data:", error);
    topRated = staticPhysios.slice(0, 4);
    specializations = staticSpecs;
  }

  return (
    <div>
      {/* ═══════════════════════════════════════════════════════════
          HERO — 8 Video Grid with Overlay
      ═══════════════════════════════════════════════════════════ */}
      <VideoHero />

      {/* ═══════════════════════════════════════════════════════════
          HOW IT WORKS
      ═══════════════════════════════════════════════════════════ */}
      <section id="how-it-works" className="py-20 bg-white scroll-mt-28">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="text-center mb-14">
            <h2 className="section-heading">How It Works</h2>
            <p className="section-subheading">Get professional physiotherapy at home in three simple steps</p>
          </div>
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-8 lg:gap-12">
            {[
              {
                step: "1", title: "Search & Compare", color: "primary",
                description: "Browse HCPC-verified physiotherapists by specialisation, location and availability. Read real patient reviews.",
                image: "/pics/search-compare.jpg",
                icon: (
                  <svg className="w-7 h-7" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth="1.5">
                    <path strokeLinecap="round" strokeLinejoin="round" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
                  </svg>
                ),
              },
              {
                step: "2", title: "Book Instantly", color: "coral",
                description: "Pick a convenient time slot from real-time availability. Choose a home visit, clinic or online session.",
                image: "/pics/book-instantly.jpg",
                icon: (
                  <svg className="w-7 h-7" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth="1.5">
                    <path strokeLinecap="round" strokeLinejoin="round" d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" />
                  </svg>
                ),
              },
              {
                step: "3", title: "Recover at Home", color: "sage",
                description: "Your physio comes to you. Receive expert treatment in the comfort of your own home. Start healing.",
                image: "/pics/home-visit-stretch.jpg",
                icon: (
                  <svg className="w-7 h-7" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth="1.5">
                    <path strokeLinecap="round" strokeLinejoin="round" d="M3 12l2-2m0 0l7-7 7 7M5 10v10a1 1 0 001 1h3m10-11l2 2m-2-2v10a1 1 0 01-1 1h-3m-6 0a1 1 0 001-1v-4a1 1 0 011-1h2a1 1 0 011 1v4a1 1 0 001 1m-6 0h6" />
                  </svg>
                ),
              },
            ].map((item) => (
              <div key={item.step} className="text-center group">
                <div className="relative w-full aspect-[4/3] rounded-2xl overflow-hidden mb-5 shadow-md group-hover:shadow-xl transition-shadow duration-300">
                  <Image
                    src={item.image}
                    alt={item.title}
                    fill
                    className="object-cover group-hover:scale-105 transition-transform duration-500"
                  />
                  <div className="absolute inset-0 bg-gradient-to-t from-black/40 to-transparent" />
                  <div className={`absolute top-3 left-3 w-10 h-10 rounded-xl flex items-center justify-center ${
                    item.color === "primary" ? "bg-primary text-white" :
                    item.color === "coral" ? "bg-coral text-white" :
                    "bg-sage text-white"
                  }`}>
                    {item.icon}
                  </div>
                  <div className={`absolute bottom-3 right-3 text-xs font-bold uppercase tracking-wider px-3 py-1 rounded-full text-white ${
                    item.color === "primary" ? "bg-primary/80" :
                    item.color === "coral" ? "bg-coral/80" :
                    "bg-sage-dark/80"
                  }`}>Step {item.step}</div>
                </div>
                <h3 className="text-xl font-bold text-navy mb-2">{item.title}</h3>
                <p className="text-gray-500 leading-relaxed">{item.description}</p>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* ═══════════════════════════════════════════════════════════
          TOP RATED PHYSIOTHERAPISTS
      ═══════════════════════════════════════════════════════════ */}
      <section className="py-20 bg-cream-dark/30">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex items-end justify-between mb-10">
            <div>
              <h2 className="section-heading">Top-Rated Physiotherapists</h2>
              <p className="text-gray-500 mt-2">Trusted by thousands of patients across London</p>
            </div>
            <Link href="/search" className="hidden sm:inline-flex items-center gap-1 text-primary font-semibold hover:text-primary-dark transition-colors group">
              View All
              <svg className="w-4 h-4 group-hover:translate-x-1 transition-transform" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth="2"><path strokeLinecap="round" strokeLinejoin="round" d="M9 5l7 7-7 7" /></svg>
            </Link>
          </div>
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-5">
            {topRated.map((physio) => (
              <ProviderCard key={physio.id} physio={physio} />
            ))}
          </div>
          <div className="sm:hidden text-center mt-6">
            <Link href="/search" className="btn-outline">View All Physiotherapists</Link>
          </div>
        </div>
      </section>

      {/* ═══════════════════════════════════════════════════════════
          BROWSE BY SPECIALISATION — using local pics
      ═══════════════════════════════════════════════════════════ */}
      <section className="py-20 bg-white">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="text-center mb-12">
            <h2 className="section-heading">Browse by Specialisation</h2>
            <p className="section-subheading">Find the right physio for your specific needs</p>
          </div>
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-5">
            {specializations.map((spec) => (
              <Link
                key={spec.name}
                href="/search"
                className="group relative rounded-2xl overflow-hidden shadow-md hover:shadow-xl transition-all duration-300 hover:-translate-y-1"
              >
                <div className="aspect-[4/3] relative">
                  <Image
                    src={spec.image}
                    alt={spec.name}
                    fill
                    className="object-cover group-hover:scale-105 transition-transform duration-500"
                  />
                  <div className="absolute inset-0 bg-gradient-to-t from-navy/80 via-navy/30 to-transparent" />
                </div>
                <div className="absolute bottom-0 left-0 right-0 p-4">
                  <h3 className="font-bold text-white text-sm sm:text-base drop-shadow-sm">
                    {spec.name}
                  </h3>
                  <p className="text-xs text-white/70 mt-0.5">{spec.count} Specialists</p>
                </div>
              </Link>
            ))}
          </div>
        </div>
      </section>

      {/* ═══════════════════════════════════════════════════════════
          AREAS WE COVER — with video background
      ═══════════════════════════════════════════════════════════ */}
      <section id="areas" className="py-20 bg-navy relative overflow-hidden scroll-mt-28">
        {/* Video Background */}
        <div className="absolute inset-0">
          <video
            autoPlay
            muted
            loop
            playsInline
            preload="metadata"
            className="w-full h-full object-cover opacity-30"
          >
            <source src="/videos/areas-coverage.mp4" type="video/mp4" />
          </video>
          <div className="absolute inset-0 bg-navy/60" />
        </div>

        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 relative z-10">
          <div className="text-center mb-12">
            <h2 className="text-3xl sm:text-4xl font-bold text-white leading-tight">Areas We Cover</h2>
            <p className="text-gray-300 mt-3 text-lg">Physiotherapy home visits across London — and expanding</p>
          </div>

          <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-4">
            {[
              { area: "Richmond", postcode: "TW9/TW10", physios: 24 },
              { area: "Islington", postcode: "N1/N7", physios: 31 },
              { area: "Kensington", postcode: "W8/W14", physios: 28 },
              { area: "Wimbledon", postcode: "SW19/SW20", physios: 19 },
              { area: "Clapham", postcode: "SW4/SW11", physios: 22 },
              { area: "Greenwich", postcode: "SE10/SE3", physios: 18 },
            ].map((loc) => (
              <Link
                key={loc.area}
                href="/search"
                className="group bg-white/10 backdrop-blur-sm border border-white/15 rounded-2xl p-5 text-center hover:bg-white/20 transition-all duration-300 hover:-translate-y-1"
              >
                <div className="w-10 h-10 rounded-full bg-coral/20 flex items-center justify-center mx-auto mb-3">
                  <svg className="w-5 h-5 text-coral" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth="2">
                    <path strokeLinecap="round" strokeLinejoin="round" d="M17.657 16.657L13.414 20.9a1.998 1.998 0 01-2.827 0l-4.244-4.243a8 8 0 1111.314 0z" />
                    <path strokeLinecap="round" strokeLinejoin="round" d="M15 11a3 3 0 11-6 0 3 3 0 016 0z" />
                  </svg>
                </div>
                <h3 className="text-white font-bold text-base">{loc.area}</h3>
                <p className="text-gray-400 text-xs mt-1">{loc.postcode}</p>
                <p className="text-primary-200 text-xs mt-2 font-medium">{loc.physios} Physios</p>
              </Link>
            ))}
          </div>

          <div className="text-center mt-10">
            <p className="text-gray-400 text-sm">Don&apos;t see your area? We&apos;re expanding rapidly across London and the UK.</p>
            <Link href="/search" className="inline-flex items-center gap-2 text-coral font-semibold mt-3 hover:text-coral-light transition-colors group">
              Check availability in your area
              <svg className="w-4 h-4 group-hover:translate-x-1 transition-transform" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth="2"><path strokeLinecap="round" strokeLinejoin="round" d="M9 5l7 7-7 7" /></svg>
            </Link>
          </div>
        </div>
      </section>

      {/* ═══════════════════════════════════════════════════════════
          PRICING TRANSPARENCY
      ═══════════════════════════════════════════════════════════ */}
      <section id="pricing" className="py-20 bg-white scroll-mt-28">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="text-center mb-12">
            <h2 className="section-heading">Transparent Pricing</h2>
            <p className="section-subheading">No hidden fees. No surprises. Pay only for what you book.</p>
          </div>

          <div className="grid grid-cols-1 sm:grid-cols-3 gap-6 max-w-5xl mx-auto">
            {[
              {
                type: "Online Consultation",
                price: "£45",
                priceSuffix: "from",
                duration: "30 min video call",
                color: "primary",
                features: ["Video assessment & advice", "Exercise prescription", "Follow-up plan", "Convenient from anywhere"],
                icon: (
                  <svg className="w-8 h-8" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth="1.5">
                    <path strokeLinecap="round" strokeLinejoin="round" d="M15 10l4.553-2.276A1 1 0 0121 8.618v6.764a1 1 0 01-1.447.894L15 14M5 18h8a2 2 0 002-2V8a2 2 0 00-2-2H5a2 2 0 00-2 2v8a2 2 0 002 2z" />
                  </svg>
                ),
              },
              {
                type: "Home Visit",
                price: "£95",
                priceSuffix: "from",
                duration: "45–60 min at your home",
                color: "sage",
                features: ["Treatment at your doorstep", "No travel for you", "Personalised home exercises", "DBS-checked physios only"],
                icon: (
                  <svg className="w-8 h-8" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth="1.5">
                    <path strokeLinecap="round" strokeLinejoin="round" d="M3 12l2-2m0 0l7-7 7 7M5 10v10a1 1 0 001 1h3m10-11l2 2m-2-2v10a1 1 0 01-1 1h-3m-6 0a1 1 0 001-1v-4a1 1 0 011-1h2a1 1 0 011 1v4a1 1 0 001 1m-6 0h6" />
                  </svg>
                ),
              },
              {
                type: "Home Visit Package",
                price: "£80",
                priceSuffix: "only",
                duration: "4 sessions at your home",
                color: "coral",
                popular: true,
                badge: "Best Value",
                features: ["Save £60 across 4 sessions", "Consistent physio each visit", "Ongoing progress tracking", "Priority booking availability"],
                icon: (
                  <svg className="w-8 h-8" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth="1.5">
                    <path strokeLinecap="round" strokeLinejoin="round" d="M21 11.25v8.25a1.5 1.5 0 01-1.5 1.5H5.25a1.5 1.5 0 01-1.5-1.5v-8.25M12 4.875A2.625 2.625 0 109.375 7.5H12m0-2.625V7.5m0-2.625A2.625 2.625 0 1114.625 7.5H12m0 0V21m-8.625-9.75h18c.621 0 1.125-.504 1.125-1.125v-1.5c0-.621-.504-1.125-1.125-1.125h-18c-.621 0-1.125.504-1.125 1.125v1.5c0 .621.504 1.125 1.125 1.125z" />
                  </svg>
                ),
              },
            ].map((plan) => (
              <div
                key={plan.type}
                className={`relative rounded-2xl p-6 border-2 transition-all duration-300 hover:-translate-y-1 ${
                  plan.popular
                    ? "border-coral bg-coral/5 shadow-xl scale-[1.02]"
                    : "border-gray-100 bg-white shadow-md hover:shadow-xl"
                }`}
              >
                {plan.popular && (
                  <div className="absolute -top-3 left-1/2 -translate-x-1/2 bg-coral text-white text-xs font-bold uppercase tracking-wider px-4 py-1 rounded-full whitespace-nowrap">
                    {plan.badge || "Most Popular"}
                  </div>
                )}
                <div className={`w-14 h-14 rounded-2xl flex items-center justify-center mb-4 ${
                  plan.color === "primary" ? "bg-primary-light text-primary" :
                  plan.color === "coral" ? "bg-coral-light text-coral" :
                  "bg-sage-light text-sage-dark"
                }`}>
                  {plan.icon}
                </div>
                <h3 className="text-lg font-bold text-navy">{plan.type}</h3>
                <p className="text-gray-400 text-sm mt-1">{plan.duration}</p>
                <div className="mt-4 mb-5">
                  <span className="text-xs text-gray-400 uppercase">{plan.priceSuffix}</span>
                  <span className="text-3xl font-bold text-navy ml-1">{plan.price}</span>
                  <span className="text-gray-400 text-sm"> / session</span>
                </div>
                <ul className="space-y-2.5 mb-6">
                  {plan.features.map((f) => (
                    <li key={f} className="flex items-start gap-2 text-sm text-gray-600">
                      <svg className="w-4 h-4 text-primary flex-shrink-0 mt-0.5" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth="2">
                        <path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" />
                      </svg>
                      {f}
                    </li>
                  ))}
                </ul>
                <Link href="/search" className={`block text-center py-2.5 rounded-xl font-semibold text-sm transition-all ${
                  plan.popular
                    ? "bg-coral text-white hover:bg-coral-dark"
                    : "bg-gray-50 text-navy hover:bg-primary-light hover:text-primary"
                }`}>
                  Find a Physio
                </Link>
              </div>
            ))}
          </div>

          <p className="text-center text-gray-400 text-sm mt-8">
            Prices vary by practitioner. Many accept private health insurance including Bupa, AXA, and Vitality.
          </p>
        </div>
      </section>

      {/* ═══════════════════════════════════════════════════════════
          STATS / METRICS BAR
      ═══════════════════════════════════════════════════════════ */}
      <section className="py-14 bg-gradient-to-r from-primary-dark via-primary to-primary-400 relative overflow-hidden">
        <div className="absolute inset-0 opacity-10">
          <div className="absolute top-4 left-1/4 w-32 h-32 border border-white/30 rounded-full" />
          <div className="absolute bottom-4 right-1/3 w-48 h-48 border border-white/20 rounded-full" />
          <div className="absolute top-1/2 left-1/2 w-24 h-24 border border-white/25 rounded-full" />
        </div>
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 relative z-10">
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-8 text-center">
            {[
              { value: "142+", label: "Verified Physios" },
              { value: "10,000+", label: "Home Visits" },
              { value: "4.8★", label: "Average Rating" },
              { value: "100%", label: "DBS Checked" },
            ].map((stat) => (
              <div key={stat.label}>
                <div className="text-3xl sm:text-4xl font-bold text-white">{stat.value}</div>
                <div className="text-primary-200 mt-1 text-sm sm:text-base">{stat.label}</div>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* ═══════════════════════════════════════════════════════════
          HOW WE VET OUR PHYSIOS
      ═══════════════════════════════════════════════════════════ */}
      <section className="py-20 bg-white">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="text-center mb-14">
            <h2 className="section-heading">How We Vet Our Physios</h2>
            <p className="section-subheading">Your safety is our top priority. Every physiotherapist goes through a rigorous 4-step verification.</p>
          </div>

          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6 max-w-5xl mx-auto">
            {[
              {
                step: "1",
                title: "HCPC Registration",
                description: "We verify every physiotherapist is registered with the Health and Care Professions Council — the UK regulator.",
                icon: (
                  <svg className="w-7 h-7" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth="1.5">
                    <path strokeLinecap="round" strokeLinejoin="round" d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z" />
                  </svg>
                ),
                color: "primary",
              },
              {
                step: "2",
                title: "Enhanced DBS Check",
                description: "All physios who offer home visits must hold a valid enhanced DBS check — updated within the last 3 years.",
                icon: (
                  <svg className="w-7 h-7" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth="1.5">
                    <path strokeLinecap="round" strokeLinejoin="round" d="M15 7a2 2 0 012 2m4 0a6 6 0 01-7.743 5.743L11 17H9v2H7v2H4a1 1 0 01-1-1v-2.586a1 1 0 01.293-.707l5.964-5.964A6 6 0 1121 9z" />
                  </svg>
                ),
                color: "coral",
              },
              {
                step: "3",
                title: "Insurance Verified",
                description: "Professional indemnity insurance is checked and must be current. We verify the policy covers home visits.",
                icon: (
                  <svg className="w-7 h-7" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth="1.5">
                    <path strokeLinecap="round" strokeLinejoin="round" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                  </svg>
                ),
                color: "sage",
              },
              {
                step: "4",
                title: "Qualifications Review",
                description: "Degrees, specialisations and CPD records are reviewed. Only qualified, experienced physios make the cut.",
                icon: (
                  <svg className="w-7 h-7" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth="1.5">
                    <path strokeLinecap="round" strokeLinejoin="round" d="M4.26 10.147a60.436 60.436 0 00-.491 6.347A48.627 48.627 0 0112 20.904a48.627 48.627 0 018.232-4.41 60.46 60.46 0 00-.491-6.347m-15.482 0a50.57 50.57 0 00-2.658-.813A59.905 59.905 0 0112 3.493a59.902 59.902 0 0110.399 5.84c-.896.248-1.783.52-2.658.814m-15.482 0A50.697 50.697 0 0112 13.489a50.702 50.702 0 017.74-3.342" />
                  </svg>
                ),
                color: "primary",
              },
            ].map((item) => (
              <div key={item.step} className="relative group">
                {/* Connector line */}
                {Number(item.step) < 4 && (
                  <div className="hidden lg:block absolute top-10 left-full w-full h-0.5 bg-gradient-to-r from-gray-200 to-transparent z-0" />
                )}
                <div className="relative bg-cream rounded-2xl p-6 hover:shadow-lg transition-all duration-300 hover:-translate-y-1 border border-gray-100">
                  <div className={`w-12 h-12 rounded-2xl flex items-center justify-center mb-4 ${
                    item.color === "primary" ? "bg-primary-light text-primary" :
                    item.color === "coral" ? "bg-coral-light text-coral" :
                    "bg-sage-light text-sage-dark"
                  }`}>
                    {item.icon}
                  </div>
                  <div className="text-xs font-bold text-gray-300 uppercase tracking-wider mb-2">Step {item.step}</div>
                  <h3 className="text-base font-bold text-navy mb-2">{item.title}</h3>
                  <p className="text-sm text-gray-500 leading-relaxed">{item.description}</p>
                </div>
              </div>
            ))}
          </div>

          <div className="text-center mt-10">
            <div className="inline-flex items-center gap-2 bg-primary-light/60 rounded-full px-5 py-2.5 border border-primary-100">
              <svg className="w-5 h-5 text-primary" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth="2">
                <path strokeLinecap="round" strokeLinejoin="round" d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z" />
              </svg>
              <span className="text-sm font-semibold text-primary-dark">100% of our physiotherapists pass all 4 verification steps</span>
            </div>
          </div>
        </div>
      </section>

      {/* ═══════════════════════════════════════════════════════════
          TESTIMONIALS
      ═══════════════════════════════════════════════════════════ */}
      <section className="py-20 bg-cream">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="text-center mb-12">
            <h2 className="section-heading">What Our Patients Say</h2>
            <p className="section-subheading">Real reviews from real patients across the UK</p>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
            {testimonials.map((t, i) => {
              const images = ["/pics/shoulder-exam.jpg", "/pics/weights-exercise.jpg", "/pics/home-rehab-smile.jpg"];
              return (
                <div key={i} className="card-hover relative">
                  <div className="absolute -top-3 -left-1 text-5xl text-primary-200 font-serif leading-none">&ldquo;</div>
                  <div className="flex gap-0.5 mb-4 pt-2">
                    {[...Array(t.rating)].map((_, j) => (
                      <svg key={j} className="w-5 h-5 text-amber-400" fill="currentColor" viewBox="0 0 20 20">
                        <path d="M9.049 2.927c.3-.921 1.603-.921 1.902 0l1.07 3.292a1 1 0 00.95.69h3.462c.969 0 1.371 1.24.588 1.81l-2.8 2.034a1 1 0 00-.364 1.118l1.07 3.292c.3.921-.755 1.688-1.54 1.118l-2.8-2.034a1 1 0 00-1.175 0l-2.8 2.034c-.784.57-1.838-.197-1.539-1.118l1.07-3.292a1 1 0 00-.364-1.118L2.98 8.72c-.783-.57-.38-1.81.588-1.81h3.461a1 1 0 00.951-.69l1.07-3.292z" />
                      </svg>
                    ))}
                  </div>
                  <p className="text-gray-600 leading-relaxed">&ldquo;{t.text}&rdquo;</p>
                  <div className="mt-4 pt-4 border-t border-gray-100 flex items-center gap-3">
                    <div className="w-10 h-10 rounded-full overflow-hidden flex-shrink-0">
                      <Image src={images[i]} alt={t.name} width={40} height={40} className="w-full h-full object-cover" />
                    </div>
                    <div>
                      <p className="font-semibold text-sm text-navy">{t.name}</p>
                      <p className="text-xs text-gray-400">Verified Patient</p>
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </section>

      {/* ═══════════════════════════════════════════════════════════
          FOR PHYSIOTHERAPISTS — using local pic as background
      ═══════════════════════════════════════════════════════════ */}
      <section id="for-physios" className="py-20 bg-white scroll-mt-28">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="rounded-3xl relative overflow-hidden">
            <div className="absolute inset-0">
              <Image
                src="/pics/overhead-dumbbell.jpg"
                alt="Physiotherapy session"
                fill
                className="object-cover"
              />
              <div className="absolute inset-0 bg-gradient-to-r from-navy/95 via-navy/85 to-navy/70" />
            </div>
            <div className="relative z-10 p-8 sm:p-12 lg:p-16">
              <div className="grid lg:grid-cols-2 gap-8 items-center">
                <div>
                  <h2 className="text-3xl sm:text-4xl font-bold text-white leading-tight">
                    Are You a Physiotherapist?
                  </h2>
                  <p className="mt-4 text-gray-300 text-lg leading-relaxed">
                    Join our growing network of HCPC-registered physiotherapists. Set your own hours, manage your bookings, and grow your practice with PhysioConnect.
                  </p>
                  <div className="mt-6 flex flex-wrap gap-4">
                    <Link href="/join" className="btn-coral !text-base">Join as a Physio</Link>
                    <Link href="/join" className="border-2 border-white/30 text-white px-6 py-3 rounded-full font-semibold hover:bg-white/10 transition-all">Learn More</Link>
                  </div>
                </div>
                <div className="grid grid-cols-2 gap-4">
                  {[
                    { icon: "📅", title: "Flexible Schedule", desc: "Set your own working hours" },
                    { icon: "💷", title: "Fair Commission", desc: "Keep more of your earnings" },
                    { icon: "📱", title: "Easy Management", desc: "Simple booking dashboard" },
                    { icon: "🌍", title: "Grow Your Reach", desc: "Access more local patients" },
                  ].map((item) => (
                    <div key={item.title} className="bg-white/10 backdrop-blur-sm rounded-xl p-4 border border-white/10 hover:bg-white/15 transition-colors">
                      <div className="text-2xl mb-2">{item.icon}</div>
                      <h4 className="font-semibold text-white text-sm">{item.title}</h4>
                      <p className="text-xs text-gray-400 mt-1">{item.desc}</p>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* ═══════════════════════════════════════════════════════════
          BLOG / HEALTH TIPS PREVIEW
      ═══════════════════════════════════════════════════════════ */}
      <section className="py-20 bg-cream">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="text-center mb-12">
            <h2 className="section-heading">Health Tips & Resources</h2>
            <p className="section-subheading">Expert advice from our physiotherapists to help you stay pain-free</p>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
            {[
              {
                title: "5 Exercises to Relieve Lower Back Pain at Home",
                excerpt: "Lower back pain affects 80% of adults at some point. These simple, physio-approved exercises can help reduce discomfort and prevent recurrence.",
                category: "Back Pain",
                readTime: "4 min read",
                image: "/pics/home-visit-stretch.jpg",
                color: "primary",
              },
              {
                title: "When to See a Physiotherapist After Surgery",
                excerpt: "Post-surgical rehabilitation is crucial for a full recovery. Learn the signs that indicate you need professional physiotherapy support.",
                category: "Recovery",
                readTime: "5 min read",
                image: "/pics/shoulder-exam.jpg",
                color: "coral",
              },
              {
                title: "How Home Visit Physiotherapy Works: What to Expect",
                excerpt: "First time booking a home visit? Here&apos;s everything you need to know — from what your physio brings to how to prepare your space.",
                category: "Guide",
                readTime: "3 min read",
                image: "/pics/home-rehab-smile.jpg",
                color: "sage",
              },
            ].map((post) => (
              <div key={post.title} className="group bg-white rounded-2xl overflow-hidden shadow-md hover:shadow-xl transition-all duration-300 hover:-translate-y-1 border border-gray-100">
                <div className="relative aspect-[16/9] overflow-hidden">
                  <Image
                    src={post.image}
                    alt={post.title}
                    fill
                    className="object-cover group-hover:scale-105 transition-transform duration-500"
                  />
                  <div className={`absolute top-3 left-3 text-xs font-bold uppercase tracking-wider px-3 py-1 rounded-full text-white ${
                    post.color === "primary" ? "bg-primary" :
                    post.color === "coral" ? "bg-coral" :
                    "bg-sage-dark"
                  }`}>
                    {post.category}
                  </div>
                </div>
                <div className="p-5">
                  <h3 className="text-base font-bold text-navy leading-snug mb-2 group-hover:text-primary transition-colors">
                    {post.title}
                  </h3>
                  <p className="text-sm text-gray-500 leading-relaxed mb-4">{post.excerpt}</p>
                  <div className="flex items-center justify-between">
                    <span className="text-xs text-gray-400">{post.readTime}</span>
                    <span className="text-sm font-semibold text-primary group-hover:translate-x-1 transition-transform inline-flex items-center gap-1">
                      Read More
                      <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth="2"><path strokeLinecap="round" strokeLinejoin="round" d="M9 5l7 7-7 7" /></svg>
                    </span>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* ═══════════════════════════════════════════════════════════
          FAQ SECTION
      ═══════════════════════════════════════════════════════════ */}
      <section id="faq" className="py-20 bg-white scroll-mt-28">
        <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="text-center mb-12">
            <h2 className="section-heading">Frequently Asked Questions</h2>
            <p className="section-subheading">Everything you need to know about PhysioConnect</p>
          </div>

          <div className="space-y-4">
            {[
              {
                q: "How does a home visit work?",
                a: "Once you book, your physio will arrive at your home at the scheduled time with all necessary equipment. They'll conduct a full assessment, provide treatment, and create a personalised exercise plan. Sessions typically last 45–60 minutes.",
              },
              {
                q: "Are all physiotherapists verified?",
                a: "Yes. Every physiotherapist on PhysioConnect is HCPC-registered, holds an enhanced DBS check, and has verified professional indemnity insurance. We also review their qualifications and experience before they join the platform.",
              },
              {
                q: "Is physiotherapy covered by private health insurance?",
                a: "Many of our physiotherapists are recognised by major insurers including Bupa, AXA Health, Vitality, and Aviva. You can filter by insurance provider when searching. We recommend checking with your insurer for your specific policy details.",
              },
              {
                q: "How quickly can I get an appointment?",
                a: "Many physiotherapists on our platform have same-day or next-day availability. You can see real-time availability when you search and book instantly — no waiting for callbacks.",
              },
              {
                q: "What should I prepare for a home visit?",
                a: "Just a clear space (about 2m x 2m) where your physio can work, and wear comfortable clothing. Your physio will bring any equipment needed. It's helpful to have any referral letters or scan results ready.",
              },
              {
                q: "Can I cancel or reschedule my appointment?",
                a: "Yes. You can cancel or reschedule for free up to 24 hours before your appointment. Cancellations within 24 hours may incur a fee — this varies by practitioner and is displayed at the time of booking.",
              },
            ].map((faq, i) => (
              <details key={i} className="group bg-cream rounded-2xl border border-gray-100 overflow-hidden">
                <summary className="flex items-center justify-between cursor-pointer p-5 sm:p-6 hover:bg-primary-light/30 transition-colors">
                  <span className="font-semibold text-navy pr-4">{faq.q}</span>
                  <svg className="w-5 h-5 text-primary flex-shrink-0 transition-transform duration-200 group-open:rotate-180" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth="2">
                    <path strokeLinecap="round" strokeLinejoin="round" d="M19 9l-7 7-7-7" />
                  </svg>
                </summary>
                <div className="px-5 sm:px-6 pb-5 sm:pb-6 text-gray-600 leading-relaxed text-sm border-t border-gray-100 pt-4">
                  {faq.a}
                </div>
              </details>
            ))}
          </div>

          <div className="text-center mt-10">
            <p className="text-gray-400 text-sm">Still have questions?</p>
            <Link href="/" className="inline-flex items-center gap-2 text-primary font-semibold mt-2 hover:text-primary-dark transition-colors">
              Contact our support team
              <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth="2"><path strokeLinecap="round" strokeLinejoin="round" d="M8 12h.01M12 12h.01M16 12h.01M21 12c0 4.418-4.03 8-9 8a9.863 9.863 0 01-4.255-.949L3 20l1.395-3.72C3.512 15.042 3 13.574 3 12c0-4.418 4.03-8 9-8s9 3.582 9 8z" /></svg>
            </Link>
          </div>
        </div>
      </section>

      {/* ═══════════════════════════════════════════════════════════
          FINAL CTA
      ═══════════════════════════════════════════════════════════ */}
      <section className="py-20 bg-gradient-to-b from-cream to-primary-50">
        <div className="max-w-3xl mx-auto px-4 text-center">
          <div className="inline-flex items-center gap-2 bg-coral-light rounded-full px-4 py-2 mb-6">
            <span className="text-coral text-sm font-semibold">Start Your Recovery Today</span>
          </div>
          <h2 className="text-3xl sm:text-4xl font-bold text-navy mb-4 leading-tight">
            Ready to Feel Better?
          </h2>
          <p className="text-gray-500 mb-8 text-lg leading-relaxed">
            Join thousands of patients who found the right physiotherapist with PhysioConnect. Your recovery journey starts with one click.
          </p>
          <div className="flex flex-col sm:flex-row gap-4 justify-center">
            <Link href="/search" className="btn-coral !text-lg !px-10">Find a Physiotherapist</Link>
            <Link href="/" className="btn-outline !text-lg !px-10">How It Works</Link>
          </div>
        </div>
      </section>
    </div>
  );
}
