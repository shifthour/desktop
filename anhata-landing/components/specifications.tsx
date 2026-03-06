const specifications = [
  {
    category: "Structure",
    items: [
      "Seismic Zone II Compliant RCC framed structure",
      'External walls with 6"/8" Solid Block Masonry',
      'Internal walls with 6"/4" Solid Block Masonry',
      "Basement/Stilt/Surface parking as per design",
    ],
  },
  {
    category: "Flooring",
    items: [
      "Superior quality vitrified tiles (Kajaria, Somany, Johnson)",
      "Anti-skid ceramic/vitrified tiles for toilets, utility & balcony",
      "Granite for staircase",
    ],
  },
  {
    category: "Kitchen",
    items: [
      "Polished Granite Kitchen platform",
      "Stainless Steel Sink",
      "Provision for water purifier",
      "2 feet wall dado of premium tiles above kitchen platform",
      "Provision for washing machine in Utility area",
    ],
  },
  {
    category: "Doors & Windows",
    items: [
      "Main door: Wooden frame, hardwood shutter",
      "Internal doors: Hardwood door frame and shutter",
      "UPVC 2.5 track with mosquito mesh and translucent glass",
      "Stainless steel hardware for all doors",
    ],
  },
  {
    category: "Electrical",
    items: [
      "Fire resistant PVC insulated copper wires",
      "Havells/Anchor/V Guard switches",
      "AC points for master bedroom (2BHK), 2 bedrooms (3BHK)",
      "Grid Power from BESCOM for each home",
      "Power backup for elevators and common areas",
    ],
  },
  {
    category: "Plumbing & Sanitary",
    items: [
      "Jaquar, Grohe or equivalent sanitary fittings",
      "Wall mixer with shower rod for all showers",
      "Single lever mixer for all wash basins",
      "Dual piping for fresh water and recycled treated water",
    ],
  },
]

export default function Specifications() {
  return (
    <section className="py-16 bg-gray-50">
      <div className="container mx-auto px-4">
        <div className="text-center mb-12">
          <h2 className="text-3xl md:text-4xl font-bold mb-6">Elite Specifications</h2>
          <p className="text-gray-600 max-w-2xl mx-auto">
            Every detail is crafted with care, using premium materials and thoughtful design to create a home that
            blends beauty, comfort, and lasting quality for better living.
          </p>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-8">
          {specifications.map((spec, index) => (
            <div key={index} className="bg-white rounded-lg shadow-md p-6">
              <h3 className="text-xl font-bold mb-4 text-blue-600">{spec.category}</h3>
              <ul className="space-y-2">
                {spec.items.map((item, itemIndex) => (
                  <li key={itemIndex} className="text-gray-600 text-sm flex items-start">
                    <span className="text-blue-600 mr-2">•</span>
                    {item}
                  </li>
                ))}
              </ul>
            </div>
          ))}
        </div>
      </div>
    </section>
  )
}
