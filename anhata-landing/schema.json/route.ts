import { NextResponse } from 'next/server'

export async function GET() {
  const schema = {
    "@context": "https://schema.org",
    "@graph": [
      {
        "@type": "RealEstateAgent",
        "@id": "https://www.anahata-soukya.in/#organization",
        "name": "Anahata - Realm of Living by Ishtika",
        "description": "Premium 2&3BHK Luxury Apartments in Soukya Road, Whitefield, Bangalore. 5 acres gated community with 50+ amenities.",
        "url": "https://www.anahata-soukya.in",
        "logo": {
          "@type": "ImageObject",
          "url": "https://www.anahata-soukya.in/images/logo.png",
          "width": 300,
          "height": 120
        },
        "image": {
          "@type": "ImageObject",
          "url": "https://www.anahata-soukya.in/images/gallery/aerial-complex-view.png",
          "width": 1200,
          "height": 630
        },
        "telephone": "+91-7338628777",
        "email": "info@anahata-soukya.in",
        "address": {
          "@type": "PostalAddress",
          "streetAddress": "Soukya Road, Seetharampalya",
          "addressLocality": "Whitefield",
          "addressRegion": "Karnataka",
          "postalCode": "560066",
          "addressCountry": "IN"
        },
        "geo": {
          "@type": "GeoCoordinates",
          "latitude": "12.9698",
          "longitude": "77.7500"
        },
        "priceRange": "₹89 Lakhs - ₹1.3 Cr",
        "areaServed": [
          {
            "@type": "City",
            "name": "Whitefield"
          },
          {
            "@type": "City", 
            "name": "Bangalore"
          },
          {
            "@type": "Place",
            "name": "ITPL"
          },
          {
            "@type": "Place",
            "name": "Marathahalli"
          }
        ],
        "openingHoursSpecification": {
          "@type": "OpeningHoursSpecification",
          "dayOfWeek": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"],
          "opens": "09:00",
          "closes": "19:00"
        },
        "sameAs": [
          "https://www.facebook.com/anahatawhitefield",
          "https://www.instagram.com/anahatawhitefield",
          "https://www.linkedin.com/company/anahata-whitefield"
        ]
      },
      {
        "@type": "ApartmentComplex",
        "@id": "https://www.anahata-soukya.in/#apartment-complex",
        "name": "Anahata Towers",
        "description": "440 luxury apartments across 5 towers in 5 acres with 80% open space",
        "address": {
          "@type": "PostalAddress",
          "streetAddress": "Soukya Road, Seetharampalya",
          "addressLocality": "Whitefield",
          "addressRegion": "Karnataka",
          "postalCode": "560066",
          "addressCountry": "IN"
        },
        "numberOfAccommodationUnits": "440",
        "numberOfAvailableAccommodationUnits": "350",
        "floorSize": {
          "@type": "QuantitativeValue",
          "minValue": "1164",
          "maxValue": "1758",
          "unitText": "SQ FT"
        },
        "petsAllowed": true,
        "smokingAllowed": false,
        "amenityFeature": [
          {
            "@type": "LocationFeatureSpecification",
            "name": "Swimming Pool",
            "value": true
          },
          {
            "@type": "LocationFeatureSpecification", 
            "name": "Gymnasium",
            "value": true
          },
          {
            "@type": "LocationFeatureSpecification",
            "name": "Clubhouse",
            "value": true
          },
          {
            "@type": "LocationFeatureSpecification",
            "name": "Children's Play Area",
            "value": true
          },
          {
            "@type": "LocationFeatureSpecification",
            "name": "24/7 Security",
            "value": true
          },
          {
            "@type": "LocationFeatureSpecification",
            "name": "Power Backup",
            "value": true
          },
          {
            "@type": "LocationFeatureSpecification",
            "name": "Covered Parking",
            "value": true
          },
          {
            "@type": "LocationFeatureSpecification",
            "name": "Landscaped Gardens",
            "value": true
          },
          {
            "@type": "LocationFeatureSpecification",
            "name": "Sports Courts",
            "value": true
          },
          {
            "@type": "LocationFeatureSpecification",
            "name": "Jogging Track",
            "value": true
          }
        ],
        "geo": {
          "@type": "GeoCoordinates",
          "latitude": "12.9698",
          "longitude": "77.7500"
        },
        "image": [
          {
            "@type": "ImageObject",
            "url": "https://www.anahata-soukya.in/images/gallery/aerial-complex-view.png",
            "width": 1200,
            "height": 630
          },
          {
            "@type": "ImageObject",
            "url": "https://www.anahata-soukya.in/images/gallery/landscaped-pathway.png",
            "width": 1200,
            "height": 630
          },
          {
            "@type": "ImageObject",
            "url": "https://www.anahata-soukya.in/images/gallery/swimming-pool.png",
            "width": 1200,
            "height": 630
          }
        ]
      },
      {
        "@type": "Product",
        "@id": "https://www.anahata-soukya.in/#2bhk-apartments",
        "name": "Anahata 2BHK Apartments",
        "description": "Premium 2BHK apartments in Whitefield starting from ₹89 Lakhs with modern amenities and excellent connectivity",
        "brand": {
          "@type": "Brand",
          "name": "Ishtika"
        },
        "category": "Real Estate",
        "offers": {
          "@type": "AggregateOffer",
          "lowPrice": "8900000",
          "highPrice": "12000000",
          "priceCurrency": "INR",
          "availability": "https://schema.org/InStock",
          "validFrom": "2024-01-01",
          "validThrough": "2027-12-31",
          "seller": {
            "@type": "RealEstateAgent",
            "@id": "https://www.anahata-soukya.in/#organization"
          },
          "priceValidUntil": "2025-12-31"
        },
        "aggregateRating": {
          "@type": "AggregateRating",
          "ratingValue": "4.7",
          "reviewCount": "89",
          "bestRating": "5",
          "worstRating": "1"
        },
        "image": {
          "@type": "ImageObject", 
          "url": "https://www.anahata-soukya.in/images/floorplans/2bhk-floorplan.png",
          "width": 800,
          "height": 600
        }
      },
      {
        "@type": "Product",
        "@id": "https://www.anahata-soukya.in/#3bhk-apartments",
        "name": "Anahata 3BHK Apartments", 
        "description": "Spacious 3BHK apartments in Whitefield starting from ₹1.15 Cr with premium amenities and luxury finishes",
        "brand": {
          "@type": "Brand",
          "name": "Ishtika"
        },
        "category": "Real Estate",
        "offers": {
          "@type": "AggregateOffer",
          "lowPrice": "11500000",
          "highPrice": "13500000", 
          "priceCurrency": "INR",
          "availability": "https://schema.org/InStock",
          "validFrom": "2024-01-01",
          "validThrough": "2027-12-31",
          "seller": {
            "@type": "RealEstateAgent",
            "@id": "https://www.anahata-soukya.in/#organization"
          },
          "priceValidUntil": "2025-12-31"
        },
        "aggregateRating": {
          "@type": "AggregateRating",
          "ratingValue": "4.8",
          "reviewCount": "67",
          "bestRating": "5",
          "worstRating": "1"
        },
        "image": [
          {
            "@type": "ImageObject",
            "url": "https://www.anahata-soukya.in/images/floorplans/3bhk-2t-floorplan.png",
            "width": 800,
            "height": 600
          },
          {
            "@type": "ImageObject", 
            "url": "https://www.anahata-soukya.in/images/floorplans/3bhk-3t-floorplan.png",
            "width": 800,
            "height": 600
          }
        ]
      },
      {
        "@type": "WebSite",
        "@id": "https://www.anahata-soukya.in/#website",
        "url": "https://www.anahata-soukya.in",
        "name": "Anahata - Premium Apartments in Whitefield",
        "description": "Official website of Anahata luxury apartments in Soukya Road, Whitefield, Bangalore",
        "publisher": {
          "@id": "https://www.anahata-soukya.in/#organization"
        },
        "potentialAction": {
          "@type": "SearchAction",
          "target": {
            "@type": "EntryPoint",
            "urlTemplate": "https://www.anahata-soukya.in/?s={search_term_string}"
          },
          "query-input": "required name=search_term_string"
        },
        "inLanguage": "en-IN"
      }
    ]
  }

  return NextResponse.json(schema, {
    headers: {
      'Content-Type': 'application/ld+json',
      'Cache-Control': 'public, max-age=86400, s-maxage=86400',
    },
  })
}