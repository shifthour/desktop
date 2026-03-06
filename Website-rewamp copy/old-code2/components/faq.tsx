"use client"

import { useState } from "react"
import { ChevronDown, ChevronUp } from "lucide-react"
import Link from "next/link"

interface FAQProps {
  filterType?: '2bhk' | '3bhk' | 'all'
}

const faqs = [
  // Original FAQs
  {
    question: "What is the price range for apartments in Anahata?",
    question_3bhk: "What is the price range for 3BHK apartments for sale in Anahata",
    answer: "Anahata offers 2BHK apartments starting from ₹89 Lakhs*, 3BHK 2T starting from ₹1.15 Cr*, and 3BHK 3T starting from ₹1.3 Cr*. All prices are inclusive of car parking and other basic amenities. GST and registration charges are extra as applicable.",
    answer_2bhk: "Anahata offers 2BHK apartments starting from ₹89 Lakhs*. All prices are inclusive of car parking and other basic amenities. GST and registration charges are extra as applicable.",
    answer_3bhk: "Anahata offers 3BHK 2T starting from ₹1.15 Cr*, and 3BHK 3T starting from ₹1.3 Cr*. All prices are inclusive of car parking and other basic amenities. GST and registration charges are extra as applicable."
  },
  {
    question: "Where is Anahata located?",
    answer: "Anahata is strategically located on Soukya Road, Whitefield, Bengaluru. It's well-connected to major IT parks, schools, hospitals, and shopping centers. The location offers excellent connectivity to ITPL (4 km), Marathahalli (8 km), and KR Puram Railway Station (6 km)."
  },
  {
    question: "What are the apartment sizes available?",
    question_3bhk: "What are 3 BHK apartment sizes available",
    answer: "We offer three configurations: 2BHK apartments ranging from 1164-1279 sq ft, 3BHK 2T apartments from 1511-1591 sq ft, and 3BHK 3T apartments from 1670-1758 sq ft. All apartments are designed with optimal space utilization and ventilation.",
    answer_2bhk: "We offer 2BHK apartments ranging from 1164-1279 sq ft. All apartments are designed with optimal space utilization and ventilation.",
    answer_3bhk: "We offer two configurations: 3BHK 2T apartments from 1511-1591 sq ft, and 3BHK 3T apartments from 1670-1758 sq ft. All apartments are designed with optimal space utilization and ventilation."
  },
  {
    question: "What amenities does Anahata offer?",
    question_3bhk: "What amenities are included in Anahat premium 3 BHK apartments in Bangalore",
    answer: "Anahata features 50+ premium amenities including a 20,000 sq ft clubhouse, swimming pool, gym, indoor games room, outdoor sports courts (tennis, basketball, badminton), children's play area, amphitheater, jogging track, landscaped gardens, and 24/7 security with CCTV surveillance."
  },
  {
    question: "Is there a Pre-EMI payment required?",
    question_3bhk: "Is there a Pre- EMi payment required for 3 bhk",
    answer: "No, Anahata offers a special benefit of No Pre-EMI till possession. You only start paying EMI after you receive possession of your apartment, making it easier on your finances during the construction period."
  },
  {
    question: "What is the possession timeline?",
    answer: "The project is expected to be completed and ready for possession by December 2027. However, we maintain transparency and will keep all buyers updated about the construction progress regularly."
  },
  {
    question: "Are home loans available for Anahata?",
    answer: "Yes, we have tie-ups with all major banks and financial institutions including SBI, HDFC, ICICI, Axis Bank, and others. Our sales team will assist you with the complete loan process and documentation."
  },
  {
    question: "What documents are required for booking?",
    answer: "For booking, you'll need: PAN Card, Aadhaar Card, passport-size photographs, and address proof. For home loan applicants, additional documents like salary slips, bank statements, and IT returns will be required."
  },
  {
    question: "Is the project RERA approved?",
    answer: "Yes, Anahata is fully RERA approved. All necessary approvals from BBMP, BDA, and other relevant authorities have been obtained. You can verify our RERA registration details on the official Karnataka RERA website."
  },
  {
    question: "What makes Anahata different from other projects in Whitefield?",
    answer: "Anahata stands out with its 80% open space, premium specifications, 5-acre sprawling campus, strategic location on Soukya Road, No Pre-EMI benefit, trusted developer Ishtika with a proven track record, and comprehensive amenities focused on luxury living."
  },
  {
    question: "Can NRIs invest in Anahata?",
    answer: "Yes, NRIs can invest in Anahata. We provide complete assistance with documentation, power of attorney processes, and have dedicated relationship managers for NRI customers. Payment can be made through NRE/NRO accounts."
  },
  {
    question: "What are the nearby landmarks and facilities?",
    answer: "Key nearby facilities include: Phoenix Marketcity (5 km), Columbia Asia Hospital (3 km), Ryan International School (2 km), ITPL Tech Park (4 km), Forum Shantiniketan Mall (7 km), and Bangalore International Airport (45 km via dedicated corridor)."
  },
  // SEO FAQs - Apartments in Whitefield on Soukya Road
  {
    question: "Why are Apartments in Whitefield on Soukya Road the best choice for families, professionals and investors?",
    answer: "The Soukya Road Apartments in Whitefield are perfect apartments to live in East Bangalore due to their Location, Comfort & Modern lifestyle. The development rate of this area is growing rapidly, making it one of the hottest locations in East Bangalore. These apartments have all the elements required by first time homebuyers including size, quiet environment and future growth. Real estate properties in the Whitefield area are designed to cater to families and professionals, offering ample Natural Light, Air Flow and Functionality within their Floor Plans."
  },
  {
    question: "How are Apartments in Whitefield on Soukya Road designed for modern living?",
    answer: "In today's world of modern living, buyers are interested in a lot more than just owning a house. The apartments in Whitefield located on Soukya Road represent exceptional planning and attention to detail. They cater to both a modern lifestyle and provide the quality of construction and overall thoughtful design, creating an ideal living environment for individuals or families. Apartments in Whitefield On Soukya Road are planned in a very quiet residential community while providing easy access to all city-based amenities."
  },
  {
    question: "Why is Samethanahalli a rapidly growing area to purchase an apartment?",
    answer: "The increase in demand for Apartments in Samethanahalli has created a unique marketplace for the new developer in an area that is experiencing tremendous growth. The proximity to Whitefield and East Bangalore is one factor that drives the growth of the new homebuyer market, but Samethanahalli is also developing with upgraded infrastructure and better access to all major highways. Investing in an Apartment in Samethanahalli Village means that you are investing in a location with great long-term potential."
  },
  {
    question: "What are the benefits of buying flats in Samethanahalli Village?",
    answer: "Many homebuyers choose an Apartment in Samethanahalli Village due to the larger living space, lower traffic levels, and the community-driven living environment that promotes the lifestyle of living in a suburb. The area offers tranquility while maintaining excellent connectivity to IT hubs and essential services. It's ideal for families, working professionals, as well as investors looking for a tranquil environment with easy city connections."
  },
  {
    question: "What makes the Whitefield Apartments at Soukya Road so unique?",
    answer: "The Anahata project Apartments located on Soukya Road are an example of a perfect match between location and lifestyle. They are created in a well-planned way as part of a gated community which gives residents both security and privacy. While being close to the famed IT hubs of Whitefield, Soukya Road also provides residents a serene environment in which to live. Therefore, the project can fit anyone from a homeowner to an investment purchaser."
  },
  {
    question: "What 2BHK & 3BHK flat options are available in Samethanahalli Village?",
    answer: "The Anahata project's thoughtfully planned homes suit different family needs with: Large open layouts for 2-bedroom apartments, Three-bedroom flats with open floor plans, Maximum usable space, and Natural air flow and daylight. All configurations are designed with optimal space utilization and ventilation to create comfortable living spaces."
  },
  {
    question: "What luxury surroundings do Whitefield apartments offer?",
    answer: "Anahata project is designed for modern living with elegance. Each apartment is constructed from quality materials, custom-designed with superior features, and decorated to reflect your personal style. Luxury residences in Whitefield, Bangalore offer lifestyle-related amenities including clubhouses, fitness centres, landscaped gardens, and recreational facilities. The luxury apartments aim to create a wonderful environment for relaxation and convenience for residents."
  },
  {
    question: "What are the location benefits of Whitefield Apartments on Soukya Road?",
    answer: "Anahata Luxury Apartments located in the Whitefield area of Bangalore are designed to offer a community-oriented lifestyle experience with the availability of fitness facilities, landscaped/outdoor open spaces, as well as other amenities that improve quality of life while providing comfort. The strategic location ensures excellent connectivity while maintaining a peaceful residential environment."
  },
  {
    question: "How is the connectivity from Samethanahalli Village to important locations?",
    answer: "Flats in Samethanahalli Village have a straightforward access point to Whitefield and other areas nearby. With workspaces nearby, numerous schools and universities, as well as hospitals and clinics, the area has a lot to offer. As road infrastructure improves, more and more working professionals are choosing to live in Samethanahalli Village for the perfect work-life balance."
  },
  {
    question: "Why are Apartments in Whitefield on Soukya Road a smart investment?",
    answer: "Investing in an apartment on Soukya Road in Whitefield is a great choice because this area has become very popular for high-quality housing. Due to the high demand for housing, apartments on Soukya Road in Whitefield will benefit from both highly valued properties as well as high rental rates going forward. The area's rapid development and proximity to IT hubs make it an excellent long-term investment opportunity."
  },
  {
    question: "Who should consider buying an apartment in Samethanahalli Village?",
    answer: "Apartments located in Samethanahalli Village are ideal for families, working professionals, as well as investors. This region may appeal to those homebuyers who would like to live in a tranquil environment that has easy access to the city connections. When purchasing an apartment in Samethanahalli Village, you will be comfortable with the opportunity for future development. Luxury Apartments located in Whitefield, Bangalore have been built with an emphasis on quality material and detail."
  },
  {
    question: "Are the Apartments in Whitefield a suitable option for families?",
    answer: "Yes, these homes have a large amount of space and they also have a safe area for recreation along with modern day conveniences. The gated community provides security, while amenities like children's play areas, landscaped gardens, and recreational facilities make it perfect for family living. The proximity to schools and hospitals adds to the family-friendly environment."
  },
  {
    question: "Is it a wise decision to purchase flats in Samethanahalli Village?",
    answer: "Yes, the Flats located in Samethanahalli Village are an excellent opportunity due to the expected growth along with a tranquil atmosphere. The area is experiencing rapid development with improved infrastructure, making it a smart investment choice. Property values are expected to appreciate significantly as the area continues to develop."
  },
  {
    question: "What are the distinct features of Luxury Apartments available in Whitefield, Bangalore?",
    answer: "They provide superb quality construction combined with state of the art amenities and the best possible location benefits. Features include premium specifications, thoughtful floor plans with natural light and ventilation, 50+ lifestyle amenities, 80% open space, and a serene environment within a well-planned gated community. The emphasis on quality materials and attention to detail sets these apartments apart."
  }
]

export default function FAQ({ filterType = 'all' }: FAQProps) {
  const [openIndex, setOpenIndex] = useState<number | null>(0)
  const [showAll, setShowAll] = useState(false)

  const toggleFAQ = (index: number) => {
    setOpenIndex(openIndex === index ? null : index)
  }

  // Get the appropriate question based on filter type
  const getQuestion = (faq: any) => {
    if (filterType === '3bhk' && faq.question_3bhk) {
      return faq.question_3bhk
    }
    if (filterType === '2bhk' && faq.question_2bhk) {
      return faq.question_2bhk
    }
    return faq.question
  }

  // Get the appropriate answer based on filter type
  const getAnswer = (faq: any) => {
    if (filterType === '2bhk' && faq.answer_2bhk) {
      return faq.answer_2bhk
    }
    if (filterType === '3bhk' && faq.answer_3bhk) {
      return faq.answer_3bhk
    }
    return faq.answer
  }

  const displayedFaqs = showAll ? faqs : faqs.slice(0, 5)

  // Generate FAQ schema markup
  const faqSchema = {
    "@context": "https://schema.org",
    "@type": "FAQPage",
    "mainEntity": faqs.map(faq => ({
      "@type": "Question",
      "name": getQuestion(faq),
      "acceptedAnswer": {
        "@type": "Answer",
        "text": getAnswer(faq)
      }
    }))
  }

  return (
    <section id="faq" className="py-16 bg-white">
      <div className="container mx-auto px-4">
        <div className="text-center mb-12">
          <h2 className="text-3xl md:text-4xl font-bold mb-6 text-[#E67E22]">Frequently Asked Questions</h2>
          <p className="text-[#E67E22] max-w-2xl mx-auto">
            Find answers to common questions about Anahata luxury apartments in Whitefield, Bangalore
          </p>
        </div>

        <div className="max-w-4xl mx-auto">
          <div className="space-y-4">
            {displayedFaqs.map((faq, index) => (
              <div
                key={index}
                className="bg-white rounded-lg shadow-md hover:shadow-[0_0_20px_rgba(230,126,34,0.5)] hover:ring-2 hover:ring-[#E67E22] transition-all"
              >
                <button
                  onClick={() => toggleFAQ(index)}
                  className="w-full px-6 py-4 text-left flex items-center justify-between focus:outline-none focus:ring-2 focus:ring-[#E67E22] focus:ring-opacity-50 rounded-lg"
                  aria-expanded={openIndex === index}
                  aria-controls={`faq-answer-${index}`}
                >
                  <h3 className="text-lg font-semibold text-gray-800 pr-4">
                    {getQuestion(faq)}
                  </h3>
                  <span className="flex-shrink-0 ml-2">
                    {openIndex === index ? (
                      <ChevronUp className="h-5 w-5 text-[#E67E22]" />
                    ) : (
                      <ChevronDown className="h-5 w-5 text-gray-400" />
                    )}
                  </span>
                </button>
                <div
                  id={`faq-answer-${index}`}
                  className={`px-6 overflow-hidden transition-all duration-300 ${
                    openIndex === index ? "max-h-96 pb-4" : "max-h-0"
                  }`}
                >
                  <p className="text-gray-600 leading-relaxed">
                    {getAnswer(faq)}
                  </p>
                </div>
              </div>
            ))}
          </div>

          {!showAll && faqs.length > 5 && (
            <div className="text-center mt-8">
              <button
                onClick={() => setShowAll(true)}
                className="inline-flex items-center px-6 py-3 bg-[#E67E22] hover:bg-[#c96b05] text-white rounded-lg font-semibold transition-colors"
              >
                Read More FAQs
                <ChevronDown className="ml-2 h-5 w-5" />
              </button>
            </div>
          )}

          <div className="mt-12 text-center">
            <p className="text-[#E67E22] mb-4">
              Still have questions? We're here to help!
            </p>
            <div className="flex flex-col sm:flex-row gap-4 justify-center">
              <a
                href="tel:+917338628777"
                className="inline-flex items-center justify-center px-6 py-3 bg-[#E67E22] hover:bg-[#c96b05] text-white rounded-lg font-semibold transition-colors"
              >
                Call Us: 7338628777
              </a>
              <Link
                href="/Booksitevisit?source=faq"
                className="inline-flex items-center justify-center px-6 py-3 bg-[#E67E22] hover:bg-[#c96b05] text-white rounded-lg font-semibold transition-colors"
              >
                Schedule Site Visit
              </Link>
            </div>
          </div>
        </div>
      </div>

      {/* FAQ Schema Markup */}
      <script
        type="application/ld+json"
        dangerouslySetInnerHTML={{ __html: JSON.stringify(faqSchema) }}
      />
    </section>
  )
}