"use client";

import { useRef, useState, useEffect } from "react";

const videos = [
  "/videos/physio-1.mp4",
  "/videos/physio-2.mp4",
  "/videos/physio-3.mp4",
  "/videos/physio-4.mp4",
  "/videos/physio-5.mp4",
  "/videos/physio-6.mp4",
  "/videos/physio-7.mp4",
  "/videos/physio-8.mp4",
];

const londonAreas = [
  { area: "Richmond", physioCount: 1 },
  { area: "Islington", physioCount: 1 },
  { area: "Kensington", physioCount: 1 },
  { area: "Wimbledon", physioCount: 1 },
  { area: "Clapham", physioCount: 1 },
  { area: "Greenwich", physioCount: 1 },
];

const conditions = [
  { name: "Sports Injury", icon: "🏃" },
  { name: "Back Pain", icon: "🔙" },
  { name: "Knee Rehab", icon: "🦵" },
  { name: "Neck Pain", icon: "🤕" },
  { name: "Post-Surgery Recovery", icon: "🏥" },
  { name: "Neurological Rehab", icon: "🧠" },
  { name: "Orthopedic", icon: "🦴" },
  { name: "Paediatric", icon: "👶" },
  { name: "Geriatric", icon: "🧓" },
  { name: "Cardiopulmonary", icon: "❤️" },
  { name: "Women's Health", icon: "🌸" },
  { name: "Shoulder Pain", icon: "💪" },
];

export default function VideoHero() {
  const [isMusicPlaying, setIsMusicPlaying] = useState(false);
  const audioCtxRef = useRef<AudioContext | null>(null);
  const gainRef = useRef<GainNode | null>(null);

  /* ── Search state ── */
  const [conditionQuery, setConditionQuery] = useState("");
  const [locationQuery, setLocationQuery] = useState("London");
  const [showConditionDropdown, setShowConditionDropdown] = useState(false);
  const [showLocationDropdown, setShowLocationDropdown] = useState(false);
  const conditionRef = useRef<HTMLDivElement>(null);
  const locationRef = useRef<HTMLDivElement>(null);

  const filteredConditions = conditions.filter((c) =>
    c.name.toLowerCase().includes(conditionQuery.toLowerCase())
  );
  const filteredAreas = londonAreas.filter((a) =>
    a.area.toLowerCase().includes(locationQuery.toLowerCase()) ||
    "london".includes(locationQuery.toLowerCase())
  );

  const searchHref = `/search${conditionQuery || locationQuery !== "London" ? "?" : ""}${conditionQuery ? `q=${encodeURIComponent(conditionQuery)}` : ""}${conditionQuery && locationQuery !== "London" ? "&" : ""}${locationQuery !== "London" ? `area=${encodeURIComponent(locationQuery)}` : ""}`;

  /* Close dropdowns on outside click */
  useEffect(() => {
    function handleClickOutside(e: MouseEvent) {
      if (conditionRef.current && !conditionRef.current.contains(e.target as Node)) {
        setShowConditionDropdown(false);
      }
      if (locationRef.current && !locationRef.current.contains(e.target as Node)) {
        setShowLocationDropdown(false);
      }
    }
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  function startAmbientMusic() {
    if (audioCtxRef.current) return;
    const ctx = new AudioContext();
    audioCtxRef.current = ctx;
    const master = ctx.createGain();
    master.gain.value = 0.08;
    master.connect(ctx.destination);
    gainRef.current = master;

    const frequencies = [174, 220, 261.6, 329.6, 392];
    frequencies.forEach((freq, i) => {
      const osc = ctx.createOscillator();
      osc.type = "sine";
      osc.frequency.value = freq;
      const oscGain = ctx.createGain();
      oscGain.gain.value = 0;
      const now = ctx.currentTime;
      const offset = i * 2;
      oscGain.gain.setValueAtTime(0, now + offset);
      oscGain.gain.linearRampToValueAtTime(0.15, now + offset + 3);
      for (let t = 0; t < 120; t += 8) {
        oscGain.gain.linearRampToValueAtTime(0.12 + (i % 2) * 0.04, now + offset + t + 4);
        oscGain.gain.linearRampToValueAtTime(0.06, now + offset + t + 8);
      }
      const filter = ctx.createBiquadFilter();
      filter.type = "lowpass";
      filter.frequency.value = 800;
      filter.Q.value = 1;
      osc.connect(oscGain);
      oscGain.connect(filter);
      filter.connect(master);
      osc.start(now + offset);
    });
    setIsMusicPlaying(true);
  }

  function stopAmbientMusic() {
    if (audioCtxRef.current) {
      audioCtxRef.current.close();
      audioCtxRef.current = null;
      gainRef.current = null;
    }
    setIsMusicPlaying(false);
  }

  function toggleMusic() {
    if (isMusicPlaying) stopAmbientMusic();
    else startAmbientMusic();
  }

  useEffect(() => {
    return () => { if (audioCtxRef.current) audioCtxRef.current.close(); };
  }, []);

  return (
    <>
      {/* ═══════════════════════════════════════════════════════════
          SECTION 1 — Pure Video Grid (no text overlay)
      ═══════════════════════════════════════════════════════════ */}
      <section className="relative bg-navy overflow-hidden">
        {/* Video Grid — 4 columns × 2 rows */}
        <div className="grid grid-cols-2 sm:grid-cols-4 grid-rows-2">
          {videos.map((src, i) => (
            <div key={i} className="relative aspect-video overflow-hidden">
              <video
                autoPlay
                muted
                loop
                playsInline
                preload="metadata"
                className="w-full h-full object-cover"
              >
                <source src={src} type="video/mp4" />
              </video>
              {/* Subtle vignette on each cell */}
              <div className="absolute inset-0 bg-gradient-to-t from-navy/20 to-transparent pointer-events-none" />
            </div>
          ))}
        </div>

        {/* Thin grid lines between videos */}
        <div className="absolute inset-0 grid grid-cols-2 sm:grid-cols-4 grid-rows-2 pointer-events-none z-10">
          {videos.map((_, i) => (
            <div key={i} className="border border-navy/30" />
          ))}
        </div>

        {/* Bottom gradient fade into content section */}
        <div className="absolute bottom-0 left-0 right-0 h-24 bg-gradient-to-t from-cream to-transparent z-10" />

        {/* Scroll indicator */}
        <div className="absolute bottom-4 left-1/2 -translate-x-1/2 z-20 animate-bounce">
          <svg className="w-6 h-6 text-navy/40" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth="2">
            <path strokeLinecap="round" strokeLinejoin="round" d="M19 14l-7 7m0 0l-7-7m7 7V3" />
          </svg>
        </div>
      </section>

      {/* ═══════════════════════════════════════════════════════════
          SECTION 2 — Content (below videos)
      ═══════════════════════════════════════════════════════════ */}
      <section className="relative bg-cream py-8 sm:py-10">
        <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 text-center">
          {/* Badge */}
          <div className="inline-flex items-center gap-2 bg-primary-light/50 backdrop-blur-sm rounded-full px-5 py-2.5 mb-4 border border-primary-100">
            <span className="w-2 h-2 bg-primary rounded-full animate-pulse" />
            <span className="text-sm font-medium text-primary-dark">HCPC Verified Physiotherapists Across the UK</span>
          </div>

          {/* Headline */}
          <h1 className="text-4xl sm:text-5xl lg:text-6xl font-bold text-navy leading-[1.08] tracking-tight">
            Expert Physio,{" "}
            <span className="relative inline-block">
              <span className="text-primary">At Your Door</span>
              <svg className="absolute -bottom-2 left-0 w-full" viewBox="0 0 300 12" fill="none">
                <path d="M2 8C60 3 120 2 150 4C180 6 240 10 298 5" stroke="#E8734A" strokeWidth="3" strokeLinecap="round" strokeOpacity="0.6" />
              </svg>
            </span>
          </h1>

          {/* Subtext */}
          <p className="mt-6 text-lg sm:text-xl text-gray-500 leading-relaxed max-w-2xl mx-auto">
            Find verified, DBS-checked physiotherapists near you. Book home visits in minutes. Start your recovery journey today.
          </p>

          {/* Search Bar */}
          <div className="mt-10 max-w-2xl mx-auto bg-white rounded-2xl shadow-xl p-2 sm:p-3 flex flex-col sm:flex-row gap-2 border border-gray-100/80">
            {/* Condition Input */}
            <div className="flex-1 relative" ref={conditionRef}>
              <svg className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-gray-400 z-10" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth="2"><path strokeLinecap="round" strokeLinejoin="round" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" /></svg>
              <input
                type="text"
                placeholder="What do you need help with?"
                value={conditionQuery}
                onChange={(e) => { setConditionQuery(e.target.value); setShowConditionDropdown(true); }}
                onFocus={() => setShowConditionDropdown(true)}
                className="w-full pl-10 pr-4 py-3.5 border-0 rounded-xl bg-gray-50 focus:bg-white focus:ring-2 focus:ring-primary/20 transition-all text-gray-700"
              />
              {showConditionDropdown && filteredConditions.length > 0 && (
                <div className="absolute top-full left-0 right-0 mt-1 bg-white rounded-xl shadow-2xl border border-gray-100 z-50 max-h-64 overflow-y-auto">
                  <div className="px-3 py-2 text-xs font-semibold text-gray-400 uppercase tracking-wider border-b border-gray-50">
                    Specialisations & Conditions
                  </div>
                  {filteredConditions.map((c) => (
                    <button
                      key={c.name}
                      type="button"
                      className="w-full text-left px-3 py-2.5 hover:bg-primary-light/50 transition-colors flex items-center gap-3 text-sm"
                      onClick={() => { setConditionQuery(c.name); setShowConditionDropdown(false); }}
                    >
                      <span className="text-lg">{c.icon}</span>
                      <span className="text-gray-700 font-medium">{c.name}</span>
                    </button>
                  ))}
                </div>
              )}
            </div>

            {/* Location Input */}
            <div className="flex-1 relative" ref={locationRef}>
              <svg className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-coral z-10" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth="2"><path strokeLinecap="round" strokeLinejoin="round" d="M17.657 16.657L13.414 20.9a1.998 1.998 0 01-2.827 0l-4.244-4.243a8 8 0 1111.314 0z" /><path strokeLinecap="round" strokeLinejoin="round" d="M15 11a3 3 0 11-6 0 3 3 0 016 0z" /></svg>
              <input
                type="text"
                placeholder="Enter area or postcode..."
                value={locationQuery}
                onChange={(e) => { setLocationQuery(e.target.value); setShowLocationDropdown(true); }}
                onFocus={() => setShowLocationDropdown(true)}
                className="w-full pl-10 pr-4 py-3.5 border-0 rounded-xl bg-gray-50 focus:bg-white focus:ring-2 focus:ring-primary/20 transition-all text-gray-700"
              />
              {showLocationDropdown && filteredAreas.length > 0 && (
                <div className="absolute top-full left-0 right-0 mt-1 bg-white rounded-xl shadow-2xl border border-gray-100 z-50 max-h-64 overflow-y-auto">
                  <div className="px-3 py-2 text-xs font-semibold text-gray-400 uppercase tracking-wider border-b border-gray-50">
                    London Areas
                  </div>
                  {filteredAreas.map((a) => (
                    <button
                      key={a.area}
                      type="button"
                      className="w-full text-left px-3 py-2.5 hover:bg-primary-light/50 transition-colors flex items-center gap-3 text-sm"
                      onClick={() => { setLocationQuery(a.area); setShowLocationDropdown(false); }}
                    >
                      <svg className="w-4 h-4 text-coral flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth="2">
                        <path strokeLinecap="round" strokeLinejoin="round" d="M17.657 16.657L13.414 20.9a1.998 1.998 0 01-2.827 0l-4.244-4.243a8 8 0 1111.314 0z" />
                        <path strokeLinecap="round" strokeLinejoin="round" d="M15 11a3 3 0 11-6 0 3 3 0 016 0z" />
                      </svg>
                      <div>
                        <span className="text-gray-700 font-medium">{a.area}</span>
                        <span className="text-gray-400 ml-1.5 text-xs">London</span>
                      </div>
                    </button>
                  ))}
                </div>
              )}
            </div>

            <a href={searchHref} className="btn-coral text-center whitespace-nowrap !rounded-xl !px-8">
              Search
            </a>
          </div>

          {/* Popular tags */}
          <div className="mt-6 flex flex-wrap items-center justify-center gap-2 text-sm">
            <span className="text-gray-400">Popular:</span>
            {["Back Pain", "Sports Injury", "Knee Rehab", "Post-Surgery", "Neck Pain"].map((tag) => (
              <a
                key={tag}
                href="/search"
                className="px-3 py-1.5 rounded-full bg-white text-primary-dark hover:bg-primary-light font-medium transition-colors border border-primary-100/50 text-xs shadow-sm"
              >
                {tag}
              </a>
            ))}
          </div>

          {/* Trust indicators */}
          <div className="mt-10 flex flex-wrap items-center justify-center gap-6 sm:gap-10">
            {[
              { icon: "✓", label: "HCPC Registered", sub: "All physiotherapists verified" },
              { icon: "🛡", label: "DBS Checked", sub: "Enhanced background checks" },
              { icon: "📋", label: "Fully Insured", sub: "Professional indemnity cover" },
              { icon: "🏠", label: "Home Visits", sub: "Treatment at your doorstep" },
            ].map((item) => (
              <div key={item.label} className="flex items-center gap-3">
                <div className="w-10 h-10 rounded-xl bg-primary-light flex items-center justify-center text-lg flex-shrink-0">
                  {item.icon}
                </div>
                <div className="text-left">
                  <p className="font-semibold text-sm text-navy">{item.label}</p>
                  <p className="text-xs text-gray-400">{item.sub}</p>
                </div>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* ── Music Toggle (fixed) ── */}
      <button
        onClick={toggleMusic}
        className="fixed bottom-6 right-6 z-50 w-12 h-12 rounded-full bg-white/90 backdrop-blur-md shadow-xl flex items-center justify-center hover:bg-white transition-all group border border-gray-200"
        aria-label={isMusicPlaying ? "Pause ambient music" : "Play ambient music"}
        title={isMusicPlaying ? "Pause music" : "Play relaxing music"}
      >
        {isMusicPlaying ? (
          <svg className="w-5 h-5 text-primary" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth="2">
            <path strokeLinecap="round" strokeLinejoin="round" d="M15.536 8.464a5 5 0 010 7.072m2.828-9.9a9 9 0 010 12.728M9 9v6m-3-3h.01M5.636 5.636a9 9 0 000 12.728" />
          </svg>
        ) : (
          <svg className="w-5 h-5 text-gray-500 group-hover:text-primary transition-colors" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth="2">
            <path strokeLinecap="round" strokeLinejoin="round" d="M5.586 15H4a1 1 0 01-1-1v-4a1 1 0 011-1h1.586l4.707-4.707C10.923 3.663 12 4.109 12 5v14c0 .891-1.077 1.337-1.707.707L5.586 15z" />
            <path strokeLinecap="round" strokeLinejoin="round" d="M17 14l2-2m0 0l2-2m-2 2l-2-2m2 2l2 2" />
          </svg>
        )}
      </button>
    </>
  );
}
