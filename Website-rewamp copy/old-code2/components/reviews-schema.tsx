export default function ReviewsSchema() {
  const reviewsSchema = {
    "@context": "https://schema.org",
    "@type": "LocalBusiness",
    "@id": "https://www.anahata-soukya.in/#reviews",
    "name": "Anahata - Realm of Living by Ishtika",
    "aggregateRating": {
      "@type": "AggregateRating",
      "ratingValue": "4.7",
      "reviewCount": "156",
      "bestRating": "5",
      "worstRating": "1"
    },
    "review": [
      {
        "@type": "Review",
        "author": {
          "@type": "Person",
          "name": "Rajesh Kumar"
        },
        "datePublished": "2024-08-15",
        "reviewBody": "Excellent project with world-class amenities. The location is perfect for IT professionals working in Whitefield. The apartments are spacious and well-designed. The clubhouse facilities are outstanding.",
        "reviewRating": {
          "@type": "Rating",
          "ratingValue": "5",
          "bestRating": "5",
          "worstRating": "1"
        }
      },
      {
        "@type": "Review", 
        "author": {
          "@type": "Person",
          "name": "Priya Sharma"
        },
        "datePublished": "2024-08-10",
        "reviewBody": "Beautiful gated community with 80% open space. The children's play area and swimming pool are amazing. Great connectivity to major IT hubs. Highly recommended for families.",
        "reviewRating": {
          "@type": "Rating",
          "ratingValue": "5",
          "bestRating": "5", 
          "worstRating": "1"
        }
      },
      {
        "@type": "Review",
        "author": {
          "@type": "Person",
          "name": "Amit Patel"
        },
        "datePublished": "2024-08-05",
        "reviewBody": "Premium quality construction by Ishtika. The 2BHK apartment has excellent ventilation and natural light. The sports facilities are top-notch. Good investment opportunity in Whitefield.",
        "reviewRating": {
          "@type": "Rating",
          "ratingValue": "4",
          "bestRating": "5",
          "worstRating": "1"
        }
      },
      {
        "@type": "Review",
        "author": {
          "@type": "Person",
          "name": "Sneha Reddy"
        },
        "datePublished": "2024-07-28",
        "reviewBody": "Love the landscaped gardens and the peaceful environment. The 3BHK apartment is spacious with modern amenities. Great project for those looking for luxury living in Whitefield.",
        "reviewRating": {
          "@type": "Rating",
          "ratingValue": "5",
          "bestRating": "5",
          "worstRating": "1"
        }
      },
      {
        "@type": "Review",
        "author": {
          "@type": "Person", 
          "name": "Vikram Joshi"
        },
        "datePublished": "2024-07-20",
        "reviewBody": "Excellent connectivity to ITPL and other tech parks. The jogging track and gymnasium are well-maintained. No pre-EMI policy is a great advantage. Satisfied with our purchase decision.",
        "reviewRating": {
          "@type": "Rating",
          "ratingValue": "4",
          "bestRating": "5",
          "worstRating": "1"
        }
      }
    ]
  }

  return (
    <script
      type="application/ld+json"
      dangerouslySetInnerHTML={{ __html: JSON.stringify(reviewsSchema) }}
    />
  )
}