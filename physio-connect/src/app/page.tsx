import Link from "next/link";
import Image from "next/image";
import { physiotherapists, specializations, testimonials } from "@/lib/data";
import ProviderCard from "@/components/ProviderCard";
import VideoHero from "@/components/VideoHero";

export default function HomePage() {
  const topRated = [...physiotherapists].sort((a, b) => b.rating - a.rating).slice(0, 4);

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
