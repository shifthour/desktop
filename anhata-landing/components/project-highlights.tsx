const highlights = [
  {
    title: "100% Vastu Compliant",
    description: "Experience enhanced ventilation for a healthier, happier home with complete Vastu compliance.",
  },
  {
    title: "Expansive Living Spaces",
    description: "Airy balconies bring the outdoors closer and spacious interiors offer room to breathe.",
  },
  {
    title: "Premium Club House",
    description: "27,000 sq ft clubhouse with world-class amenities for leisure, wellness, and recreation.",
  },
  {
    title: "Sustainable Design",
    description:
      "Thoughtfully designed for sustainability and wellness, every corner inspires movement and relaxation.",
  },
]

export default function ProjectHighlights() {
  return (
    <section className="py-16 relative">
      <div
        className="absolute inset-0 bg-cover bg-center bg-fixed"
        style={{
          backgroundImage: "url(/placeholder.svg?height=600&width=1200&query=modern residential complex aerial view)",
        }}
      >
        <div className="absolute inset-0 bg-black bg-opacity-60" />
      </div>

      <div className="relative container mx-auto px-4">
        <div className="max-w-4xl text-white">
          <h2 className="text-3xl md:text-4xl font-bold mb-6">Project Highlights</h2>
          <p className="text-lg mb-12 leading-relaxed">
            Where leisure and life thrive daily, Ishtika's Anahata Towers, A haven crafted for your soul. With over 50
            curated amenities for leisure, wellness, and recreation, life unfolds with grace, offering the perfect
            balance of space, serenity, and modern indulgence.
          </p>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
            {highlights.map((highlight, index) => (
              <div key={index}>
                <h4 className="text-xl font-semibold mb-3">{highlight.title}</h4>
                <p className="text-gray-200">{highlight.description}</p>
              </div>
            ))}
          </div>
        </div>
      </div>
    </section>
  )
}
