import type React from "react"
import type { Metadata } from "next"
import { Inter } from 'next/font/google'
import "./globals.css"
import ReviewsSchema from "@/components/reviews-schema"
import PerformanceMonitor from "@/components/performance-monitor"

const inter = Inter({ subsets: ["latin"] })

export const metadata: Metadata = {
  metadataBase: new URL('https://www.anahata-soukya.in'),
  title: "2BHK & 3BHK for sale in Whitefield, Bangalore | Anahata",
  description: "Looking for 2BHK and 3BHK for sale in Whitefield? Anahata offers premium 2BHK & 3BHK luxury gated community apartments with 50+ Amenities and more",
  keywords: [
    "Anahata",
    "2BHK apartments Whitefield",
    "3BHK flats Soukya Road",
    "luxury apartments Bangalore",
    "Whitefield real estate",
    "Soukya Road properties",
    "premium flats Bangalore",
    "gated community Whitefield",
    "apartments near ITPL",
    "Bangalore residential projects",
  ],
  authors: [{ name: "Anahata - Realm of Living" }],
  creator: "Anahata",
  publisher: "Anahata",
  robots: {
    index: true,
    follow: true,
    googleBot: {
      index: true,
      follow: true,
      "max-video-preview": -1,
      "max-image-preview": "large",
      "max-snippet": -1,
    },
  },
  openGraph: {
    type: "website",
    locale: "en_IN",
    url: "https://www.anahata-soukya.in",
    title: "Premium 2&3BHK Apartments | Luxury 2&3BHK Flats in Soukya Road WhiteField",
    description: "Flats starting from ₹89 Lakhs* near WhiteField Soukya Road. Ideal for families & investors.",
    siteName: "Anahata - Realm of Living",
    images: [
      {
        url: "/images/gallery/aerial-complex-view.png",
        width: 1200,
        height: 630,
        alt: "Anahata - Premium Apartments in Whitefield Soukya Road",
      },
    ],
  },
  twitter: {
    card: "summary_large_image",
    title: "Premium 2&3BHK Apartments | Luxury 2&3BHK Flats in Soukya Road WhiteField",
    description: "Flats starting from ₹89 Lakhs* near WhiteField Soukya Road. Ideal for families & investors.",
    images: ["/images/gallery/aerial-complex-view.png"],
  },
  alternates: {
    canonical: "https://www.anahata-soukya.in",
  },
  other: {
    "geo.region": "IN-KA",
    "geo.placename": "Bangalore",
    "geo.position": "12.9698;77.7500",
    ICBM: "12.9698, 77.7500",
  },
    generator: 'v0.app'
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en">
      <head>
        {/* Additional SEO Meta Tags */}
        <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover" />
        <meta name="theme-color" content="#1e40af" />
        <meta name="mobile-web-app-capable" content="yes" />
        <meta name="apple-mobile-web-app-capable" content="yes" />
        <link rel="canonical" href="https://www.anahata-soukya.in" />

        {/* Preload critical resources */}
        <link rel="preload" href="/anahata/project-video.mp4" as="video" type="video/mp4" />
        <link rel="preload" href="/anahata/images/hero-poster.jpeg" as="image" />
        <link rel="dns-prefetch" href="https://www.googletagmanager.com" />
        <link rel="dns-prefetch" href="https://www.google-analytics.com" />
        <link rel="dns-prefetch" href="https://www.clarity.ms" />
        <link rel="preconnect" href="https://www.googletagmanager.com" crossOrigin="anonymous" />
        <link rel="preconnect" href="https://www.google-analytics.com" crossOrigin="anonymous" />
        
        {/* AI Chat Engine Optimization */}
        <link rel="llms" type="text/plain" href="/.well-known/llms.txt" />
        <link rel="ai-context" type="application/json" href="/.well-known/ai-context.json" />
        <meta name="ai-content-summary" content="Anahata - Premium 2BHK & 3BHK apartments in Whitefield, Bangalore from ₹89L with No Pre-EMI. 5 acres, 440 units, 50+ amenities. Contact: +91-7338628777" />
        <meta name="chatgpt-applicable" content="yes" />
        <meta name="bard-applicable" content="yes" />
        <meta name="claude-applicable" content="yes" />

        {/* Comprehensive Structured Data */}
        <script
          type="application/ld+json"
          dangerouslySetInnerHTML={{
            __html: JSON.stringify([
              {
                "@context": "https://schema.org",
                "@type": "RealEstateAgent",
                "@id": "https://www.anahata-soukya.in/#organization",
                name: "Anahata - Realm of Living by Ishtika",
                description: "Premium 2&3BHK Luxury Apartments in Soukya Road, Whitefield, Bangalore. 5 acres gated community with 50+ amenities.",
                url: "https://www.anahata-soukya.in",
                logo: "https://www.anahata-soukya.in/images/logo.png",
                image: "https://www.anahata-soukya.in/images/gallery/aerial-complex-view.png",
                telephone: "+91-7338628777",
                email: "info@anahata-soukya.in",
                address: {
                  "@type": "PostalAddress",
                  streetAddress: "Soukya Road, Seetharampalya",
                  addressLocality: "Whitefield",
                  addressRegion: "Karnataka",
                  postalCode: "560066",
                  addressCountry: "IN",
                },
                geo: {
                  "@type": "GeoCoordinates",
                  latitude: "12.9698",
                  longitude: "77.7500",
                },
                priceRange: "₹89 Lakhs - ₹1.3 Cr",
                areaServed: ["Whitefield", "Bangalore", "ITPL", "Marathahalli"],
                openingHoursSpecification: {
                  "@type": "OpeningHoursSpecification",
                  dayOfWeek: ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"],
                  opens: "09:00",
                  closes: "19:00"
                },
                sameAs: [
                  "https://www.facebook.com/anahatawhitefield",
                  "https://www.instagram.com/anahatawhitefield",
                  "https://www.linkedin.com/company/anahata-whitefield"
                ]
              },
              {
                "@context": "https://schema.org",
                "@type": "ApartmentComplex",
                name: "Anahata Towers",
                description: "440 luxury apartments across 5 towers in 5 acres with 80% open space",
                numberOfAccommodationUnits: "440",
                numberOfAvailableAccommodationUnits: "350",
                floorSize: {
                  "@type": "QuantitativeValue",
                  minValue: "1164",
                  maxValue: "1758",
                  unitText: "SQ FT"
                },
                petsAllowed: true,
                amenityFeature: [
                  {"@type": "LocationFeatureSpecification", "name": "Swimming Pool", "value": true},
                  {"@type": "LocationFeatureSpecification", "name": "Gymnasium", "value": true},
                  {"@type": "LocationFeatureSpecification", "name": "Clubhouse", "value": true},
                  {"@type": "LocationFeatureSpecification", "name": "Children's Play Area", "value": true},
                  {"@type": "LocationFeatureSpecification", "name": "24/7 Security", "value": true},
                  {"@type": "LocationFeatureSpecification", "name": "Power Backup", "value": true},
                  {"@type": "LocationFeatureSpecification", "name": "Covered Parking", "value": true}
                ]
              },
              {
                "@context": "https://schema.org",
                "@type": "Product",
                name: "Anahata 2BHK Apartments",
                description: "Premium 2BHK apartments in Whitefield starting from ₹89 Lakhs",
                brand: {
                  "@type": "Brand",
                  name: "Ishtika"
                },
                offers: {
                  "@type": "AggregateOffer",
                  lowPrice: "8900000",
                  highPrice: "13000000",
                  priceCurrency: "INR",
                  availability: "https://schema.org/InStock",
                  validFrom: "2024-01-01",
                  validThrough: "2027-12-31",
                  seller: {
                    "@type": "RealEstateAgent",
                    name: "Anahata by Ishtika"
                  }
                },
                aggregateRating: {
                  "@type": "AggregateRating",
                  ratingValue: "4.7",
                  reviewCount: "89"
                }
              },
              {
                "@context": "https://schema.org",
                "@type": "WebSite",
                "@id": "https://www.anahata-soukya.in/#website",
                url: "https://www.anahata-soukya.in",
                name: "Anahata - Premium Apartments in Whitefield",
                description: "Official website of Anahata luxury apartments in Soukya Road, Whitefield, Bangalore",
                publisher: {
                  "@id": "https://www.anahata-soukya.in/#organization"
                },
                potentialAction: {
                  "@type": "SearchAction",
                  target: "https://www.anahata-soukya.in/?s={search_term_string}",
                  "query-input": "required name=search_term_string"
                }
              }
            ]),
          }}
        />

        {/* Google Tag Manager */}
        <script
          dangerouslySetInnerHTML={{
            __html: `
              (function(w,d,s,l,i){w[l]=w[l]||[];w[l].push({'gtm.start':new Date().getTime(),event:'gtm.js'});var f=d.getElementsByTagName(s)[0],j=d.createElement(s),dl=l!='dataLayer'?'&l='+l:'';j.async=true;j.src='https://www.googletagmanager.com/gtm.js?id='+i+dl;f.parentNode.insertBefore(j,f);})(window,document,'script','dataLayer','GTM-TBQ9RD8Z');
            `,
          }}
        />
        {/* End Google Tag Manager */}

        {/* Google tag (gtag.js) - Google Analytics 4 & Google Ads */}
        <script async src="https://www.googletagmanager.com/gtag/js?id=G-W6C28DJ6M3"></script>
        <script
          dangerouslySetInnerHTML={{
            __html: `
              window.dataLayer = window.dataLayer || [];
              function gtag(){dataLayer.push(arguments);}
              gtag('js', new Date());
              
              // Google Analytics 4
              gtag('config', 'G-W6C28DJ6M3');
              
              // Google Ads
              gtag('config', 'AW-17340305414');
            `,
          }}
        />

        {/* Microsoft Clarity Tracking Script */}
        <script
          type="text/javascript"
          defer
          dangerouslySetInnerHTML={{
            __html: `
              (function(c,l,a,r,i,t,y){
                c[a]=c[a]||function(){(c[a].q=c[a].q||[]).push(arguments)};
                t=l.createElement(r);t.async=1;t.src="https://www.clarity.ms/tag/"+i;
                y=l.getElementsByTagName(r)[0];y.parentNode.insertBefore(t,y);
              })(window, document, "clarity", "script", "sfqluu523a");
            `,
          }}
        />

        {/* Statcounter Code */}
        <script
          type="text/javascript"
          defer
          dangerouslySetInnerHTML={{
            __html: `
              var sc_project=13174805;
              var sc_invisible=1;
              var sc_security="13771086";
            `,
          }}
        />
        <script type="text/javascript" src="https://www.statcounter.com/counter/counter.js" defer></script>
        <noscript>
          <div className="statcounter">
            <a title="Web Analytics Made Easy - Statcounter" href="https://statcounter.com/" target="_blank" rel="noreferrer">
              <img className="statcounter" src="https://c.statcounter.com/13174805/0/13771086/1/" alt="Web Analytics Made Easy - Statcounter" referrerPolicy="no-referrer-when-downgrade" />
            </a>
          </div>
        </noscript>
        {/* End of Statcounter Code */}
      </head>
      <body className={inter.className}>
        {/* Google Tag Manager (noscript) */}
        <noscript><iframe src="https://www.googletagmanager.com/ns.html?id=GTM-TBQ9RD8Z" height="0" width="0" style={{display:"none",visibility:"hidden"}}></iframe></noscript>
        {/* End Google Tag Manager (noscript) */}
        <ReviewsSchema />
        <PerformanceMonitor />
        {children}
      </body>
    </html>
  )
}
