import { Project, Testimonial, CompanyInfo, TimelineEvent, Stats } from '@/types';

// Company Statistics
export const stats: Stats = {
  projects: 25,
  sqftDeveloped: 5000000,
  happyFamilies: 3500,
  yearsExperience: 15,
};

// Company Information
export const companyInfo: CompanyInfo = {
  name: 'Ishtika Homes',
  tagline: 'Building Dreams, Creating Legacies',
  description: 'Ishtika Homes is a leading real estate developer committed to creating exceptional living spaces that blend luxury, comfort, and sustainability. With over 15 years of excellence in the industry, we have delivered landmark residential projects that have redefined modern living.',
  founded: '2009',
  headquarters: 'Bangalore, Karnataka',
  totalProjects: 25,
  totalSqft: 5000000,
  happyFamilies: 3500,
  vision: 'To be the most trusted and admired real estate developer, known for innovation, quality, and customer satisfaction.',
  mission: 'To create world-class residential communities that enhance the quality of life for our customers while maintaining the highest standards of integrity and sustainability.',
  values: [
    'Customer First',
    'Quality Excellence',
    'Integrity & Transparency',
    'Innovation',
    'Sustainability',
    'Timely Delivery'
  ],
  awards: [
    {
      id: '1',
      title: 'Best Residential Project of the Year',
      year: '2023',
      organization: 'Real Estate Awards India'
    },
    {
      id: '2',
      title: 'Excellence in Construction Quality',
      year: '2022',
      organization: 'CREDAI'
    },
    {
      id: '3',
      title: 'Customer Satisfaction Award',
      year: '2023',
      organization: 'Property Times'
    }
  ]
};

// Timeline Events
export const timelineEvents: TimelineEvent[] = [
  {
    id: '1',
    year: '2009',
    title: 'Foundation',
    description: 'Ishtika Homes was established with a vision to create exceptional living spaces.'
  },
  {
    id: '2',
    year: '2012',
    title: 'First Landmark Project',
    description: 'Delivered our first residential project with 200+ happy families.'
  },
  {
    id: '3',
    year: '2015',
    title: 'Expansion',
    description: 'Expanded operations to multiple cities across South India.'
  },
  {
    id: '4',
    year: '2018',
    title: '1000 Families Milestone',
    description: 'Crossed the milestone of delivering homes to 1000+ families.'
  },
  {
    id: '5',
    year: '2020',
    title: 'Sustainability Initiative',
    description: 'Launched green building initiatives across all projects.'
  },
  {
    id: '6',
    year: '2023',
    title: 'Industry Recognition',
    description: 'Awarded Best Residential Developer of the Year.'
  }
];

// Projects Data
export const projects: Project[] = [
  {
    id: '1',
    title: 'Ishtika Anahata',
    slug: 'ishtika-anahata',
    subtitle: 'Luxury Living Redefined',
    status: 'ongoing',
    location: 'Whitefield',
    city: 'Bangalore',
    price: {
      min: 12000000,
      max: 35000000,
      currency: 'INR'
    },
    sizes: {
      min: 1250,
      max: 2500,
      unit: 'sq.ft'
    },
    configurations: ['2 BHK', '3 BHK', '4 BHK'],
    totalUnits: 180,
    mainImage: 'https://images.unsplash.com/photo-1600596542815-ffad4c1539a9?w=1920',
    images: [
      'https://images.unsplash.com/photo-1600596542815-ffad4c1539a9?w=1200',
      'https://images.unsplash.com/photo-1600607687939-ce8a6c25118c?w=1200',
      'https://images.unsplash.com/photo-1600566753190-17f0baa2a6c3?w=1200',
      'https://images.unsplash.com/photo-1600585154340-be6161a56a0c?w=1200',
      'https://images.unsplash.com/photo-1600607687644-aac4c3eac7f4?w=1200'
    ],
    description: 'Experience unparalleled luxury at Ishtika Anahata, where modern architecture meets serene living. Nestled in the heart of Whitefield, this premium residential project offers thoughtfully designed 2, 3 & 4 BHK apartments with world-class amenities and stunning views.',
    highlights: [
      '70% open space with lush landscaping',
      'Smart home automation in all units',
      'Grand clubhouse with premium facilities',
      'Temperature-controlled swimming pool',
      'State-of-the-art fitness center',
      'Children\'s play area and senior citizen zone',
      'Multi-tier security with CCTV surveillance',
      'Rainwater harvesting and solar panels',
      'Proximity to IT parks and business hubs',
      'Excellent connectivity to metro and airport'
    ],
    amenities: [
      { id: '1', name: 'Swimming Pool', icon: 'Waves', category: 'wellness', description: 'Temperature-controlled infinity pool' },
      { id: '2', name: 'Fitness Center', icon: 'Dumbbell', category: 'wellness', description: 'Fully-equipped modern gym' },
      { id: '3', name: 'Clubhouse', icon: 'Building2', category: 'lifestyle', description: 'Multi-purpose banquet hall' },
      { id: '4', name: 'Yoga & Meditation', icon: 'Heart', category: 'wellness', description: 'Dedicated wellness zone' },
      { id: '5', name: 'Indoor Games', icon: 'GamepadIcon', category: 'lifestyle', description: 'Table tennis, carrom, chess' },
      { id: '6', name: 'Kids Play Area', icon: 'Baby', category: 'lifestyle', description: 'Safe and engaging play zones' },
      { id: '7', name: 'Badminton Court', icon: 'Trophy', category: 'sports', description: 'Professional-grade court' },
      { id: '8', name: 'Basketball Court', icon: 'CircleDot', category: 'sports', description: 'Full-size basketball court' },
      { id: '9', name: 'Landscaped Gardens', icon: 'Trees', category: 'lifestyle', description: 'Beautiful green spaces' },
      { id: '10', name: 'Jogging Track', icon: 'Footprints', category: 'wellness', description: 'Dedicated walking/jogging path' },
      { id: '11', name: '24/7 Security', icon: 'Shield', category: 'security', description: 'Multi-tier security system' },
      { id: '12', name: 'Covered Parking', icon: 'Car', category: 'convenience', description: 'Multi-level secure parking' },
      { id: '13', name: 'Power Backup', icon: 'Zap', category: 'convenience', description: '100% DG backup' },
      { id: '14', name: 'Rainwater Harvesting', icon: 'Droplets', category: 'convenience', description: 'Eco-friendly water management' },
      { id: '15', name: 'EV Charging', icon: 'BatteryCharging', category: 'convenience', description: 'Electric vehicle charging points' }
    ],
    specifications: [
      {
        category: 'Structure',
        items: [
          { label: 'Foundation', value: 'RCC framed structure' },
          { label: 'Walls', value: '6" & 4" thick brick masonry' },
          { label: 'Height', value: '10 feet floor to floor' }
        ]
      },
      {
        category: 'Flooring',
        items: [
          { label: 'Living & Bedrooms', value: 'Vitrified tiles (2\'x2\')' },
          { label: 'Kitchen', value: 'Anti-skid ceramic tiles' },
          { label: 'Bathrooms', value: 'Anti-skid ceramic tiles' },
          { label: 'Balconies', value: 'Anti-skid ceramic tiles' }
        ]
      },
      {
        category: 'Kitchen',
        items: [
          { label: 'Countertop', value: 'Granite platform with stainless steel sink' },
          { label: 'Wall Dado', value: 'Ceramic tiles up to 2 feet' },
          { label: 'Provisions', value: 'Chimney, hob, water purifier points' }
        ]
      },
      {
        category: 'Bathrooms',
        items: [
          { label: 'Fittings', value: 'Premium CP fittings (Grohe/Jaquar)' },
          { label: 'Sanitary Ware', value: 'Branded sanitary ware' },
          { label: 'Wall Tiles', value: 'Ceramic tiles up to 7 feet' },
          { label: 'Geyser', value: 'Provision for geyser in all bathrooms' }
        ]
      },
      {
        category: 'Doors & Windows',
        items: [
          { label: 'Main Door', value: 'Teak wood frame with engineered wood shutters' },
          { label: 'Internal Doors', value: 'Hardwood frame with flush shutters' },
          { label: 'Windows', value: 'UPVC/aluminum windows with safety grills' }
        ]
      },
      {
        category: 'Electrical',
        items: [
          { label: 'Wiring', value: 'Concealed copper wiring (Finolex/Polycab)' },
          { label: 'Switches', value: 'Modular switches (Legrand/Schneider)' },
          { label: 'Points', value: 'Adequate light & power points as per plan' },
          { label: 'Backup', value: '100% DG backup for common areas' }
        ]
      }
    ],
    floorPlans: [
      {
        id: '1',
        type: '2 BHK',
        size: 1250,
        bedrooms: 2,
        bathrooms: 2,
        price: 12000000,
        image: 'https://images.unsplash.com/photo-1600585154526-990dced4db0d?w=800',
        available: true
      },
      {
        id: '2',
        type: '2 BHK Premium',
        size: 1400,
        bedrooms: 2,
        bathrooms: 2,
        price: 14500000,
        image: 'https://images.unsplash.com/photo-1600585154526-990dced4db0d?w=800',
        available: true
      },
      {
        id: '3',
        type: '3 BHK',
        size: 1650,
        bedrooms: 3,
        bathrooms: 3,
        price: 18000000,
        image: 'https://images.unsplash.com/photo-1600566753086-00f18fb6b3ea?w=800',
        available: true
      },
      {
        id: '4',
        type: '3 BHK Premium',
        size: 1850,
        bedrooms: 3,
        bathrooms: 3,
        price: 22000000,
        image: 'https://images.unsplash.com/photo-1600566753086-00f18fb6b3ea?w=800',
        available: true
      },
      {
        id: '5',
        type: '4 BHK',
        size: 2200,
        bedrooms: 4,
        bathrooms: 4,
        price: 28000000,
        image: 'https://images.unsplash.com/photo-1600607687644-c7171b42498f?w=800',
        available: true
      },
      {
        id: '6',
        type: '4 BHK Penthouse',
        size: 2500,
        bedrooms: 4,
        bathrooms: 4,
        price: 35000000,
        image: 'https://images.unsplash.com/photo-1600607687644-c7171b42498f?w=800',
        available: false
      }
    ],
    locationAdvantages: [
      { name: 'Phoenix Marketcity', distance: '2.5 km', icon: 'ShoppingBag', category: 'shopping' },
      { name: 'Columbia Asia Hospital', distance: '1.8 km', icon: 'Hospital', category: 'healthcare' },
      { name: 'International Schools', distance: '1.2 km', icon: 'GraduationCap', category: 'education' },
      { name: 'Whitefield Metro Station', distance: '3 km', icon: 'Train', category: 'transport' },
      { name: 'Bangalore Airport', distance: '35 km', icon: 'Plane', category: 'transport' },
      { name: 'IT Parks & Tech Corridors', distance: '4 km', icon: 'Building', category: 'workplace' },
      { name: 'VR Bengaluru Mall', distance: '3.5 km', icon: 'Store', category: 'shopping' },
      { name: 'Restaurants & Cafes', distance: '1 km', icon: 'UtensilsCrossed', category: 'entertainment' }
    ],
    possession: 'December 2025',
    reraNumber: 'PRM/KA/RERA/1251/309/PR/171021/004123',
    featured: true
  },
  {
    id: '2',
    title: 'Ishtika Tranquil Heights',
    slug: 'ishtika-tranquil-heights',
    subtitle: 'Serenity Meets Sophistication',
    status: 'upcoming',
    location: 'Electronic City',
    city: 'Bangalore',
    price: {
      min: 8500000,
      max: 22000000,
      currency: 'INR'
    },
    sizes: {
      min: 1100,
      max: 2200,
      unit: 'sq.ft'
    },
    configurations: ['2 BHK', '3 BHK'],
    totalUnits: 240,
    mainImage: 'https://images.unsplash.com/photo-1600607687920-4e2a09cf159d?w=1920',
    images: [
      'https://images.unsplash.com/photo-1600607687920-4e2a09cf159d?w=1200',
      'https://images.unsplash.com/photo-1600607687644-aac4c3eac7f4?w=1200',
      'https://images.unsplash.com/photo-1600585154340-be6161a56a0c?w=1200'
    ],
    description: 'Discover tranquil living in the heart of Electronic City. Ishtika Tranquil Heights offers thoughtfully designed 2 & 3 BHK apartments with modern amenities and excellent connectivity.',
    highlights: [
      'Prime location in Electronic City Phase 1',
      'Close to IT parks and business centers',
      'Modern clubhouse with recreational facilities',
      'Landscaped gardens and walking paths',
      'Children\'s play area and senior citizen zone',
      'Multi-tier security system',
      'Rainwater harvesting and solar lighting',
      'Easy access to metro and major highways'
    ],
    amenities: [
      { id: '1', name: 'Clubhouse', icon: 'Building2', category: 'lifestyle' },
      { id: '2', name: 'Fitness Center', icon: 'Dumbbell', category: 'wellness' },
      { id: '3', name: 'Kids Play Area', icon: 'Baby', category: 'lifestyle' },
      { id: '4', name: 'Landscaped Gardens', icon: 'Trees', category: 'lifestyle' },
      { id: '5', name: '24/7 Security', icon: 'Shield', category: 'security' },
      { id: '6', name: 'Covered Parking', icon: 'Car', category: 'convenience' }
    ],
    specifications: [],
    floorPlans: [
      {
        id: '1',
        type: '2 BHK',
        size: 1100,
        bedrooms: 2,
        bathrooms: 2,
        price: 8500000,
        image: 'https://images.unsplash.com/photo-1600585154526-990dced4db0d?w=800',
        available: true
      },
      {
        id: '2',
        type: '3 BHK',
        size: 1650,
        bedrooms: 3,
        bathrooms: 2,
        price: 14500000,
        image: 'https://images.unsplash.com/photo-1600566753086-00f18fb6b3ea?w=800',
        available: true
      }
    ],
    locationAdvantages: [
      { name: 'Electronic City Metro', distance: '2 km', icon: 'Train', category: 'transport' },
      { name: 'Infosys Campus', distance: '3 km', icon: 'Building', category: 'workplace' },
      { name: 'Apollo Hospital', distance: '5 km', icon: 'Hospital', category: 'healthcare' },
      { name: 'International Schools', distance: '2.5 km', icon: 'GraduationCap', category: 'education' }
    ],
    possession: 'March 2026',
    reraNumber: 'PRM/KA/RERA/1251/309/PR/210922/005234',
    featured: false
  },
  {
    id: '3',
    title: 'Ishtika Green Valley',
    slug: 'ishtika-green-valley',
    subtitle: 'Nature-Inspired Living',
    status: 'completed',
    location: 'Sarjapur Road',
    city: 'Bangalore',
    price: {
      min: 9500000,
      max: 25000000,
      currency: 'INR'
    },
    sizes: {
      min: 1200,
      max: 2300,
      unit: 'sq.ft'
    },
    configurations: ['2 BHK', '3 BHK', '4 BHK'],
    totalUnits: 200,
    mainImage: 'https://images.unsplash.com/photo-1600607687939-ce8a6c25118c?w=1920',
    images: [
      'https://images.unsplash.com/photo-1600607687939-ce8a6c25118c?w=1200',
      'https://images.unsplash.com/photo-1600566753151-384129cf4e3e?w=1200',
      'https://images.unsplash.com/photo-1600585154363-67eb9e2e2099?w=1200'
    ],
    description: 'A completed landmark project on Sarjapur Road, offering eco-friendly living with modern amenities. All 200 families have moved in and are enjoying their dream homes.',
    highlights: [
      'Completed and handed over project',
      '200 happy families',
      'Eco-friendly green building',
      'Award-winning architecture',
      'Excellent community living',
      'Premium amenities fully operational'
    ],
    amenities: [
      { id: '1', name: 'Swimming Pool', icon: 'Waves', category: 'wellness' },
      { id: '2', name: 'Clubhouse', icon: 'Building2', category: 'lifestyle' },
      { id: '3', name: 'Gym', icon: 'Dumbbell', category: 'wellness' },
      { id: '4', name: 'Sports Courts', icon: 'Trophy', category: 'sports' }
    ],
    specifications: [],
    floorPlans: [],
    locationAdvantages: [],
    possession: 'Handed Over - December 2022',
    reraNumber: 'PRM/KA/RERA/1251/309/PR/180520/003456',
    featured: false
  }
];

// Testimonials
export const testimonials: Testimonial[] = [
  {
    id: '1',
    name: 'Rajesh Kumar',
    role: 'Software Engineer',
    company: 'Tech Corp',
    image: 'https://i.pravatar.cc/150?img=12',
    rating: 5,
    comment: 'Ishtika Anahata has exceeded all our expectations. The quality of construction, attention to detail, and the amenities are world-class. The team was professional and transparent throughout the buying process. Highly recommended!',
    project: 'Ishtika Anahata',
    date: '2024-01-15'
  },
  {
    id: '2',
    name: 'Priya Sharma',
    role: 'Marketing Manager',
    image: 'https://i.pravatar.cc/150?img=5',
    rating: 5,
    comment: 'We moved into our dream home at Ishtika Green Valley last year. The location is perfect, the community is wonderful, and the maintenance is excellent. Thank you Ishtika Homes for making our dream come true!',
    project: 'Ishtika Green Valley',
    date: '2023-11-20'
  },
  {
    id: '3',
    name: 'Arun Menon',
    role: 'Business Owner',
    image: 'https://i.pravatar.cc/150?img=33',
    rating: 5,
    comment: 'The best decision we made was buying our apartment at Ishtika Anahata. The smart home features, the clubhouse, and the location - everything is perfect. The Ishtika team delivered on every promise.',
    project: 'Ishtika Anahata',
    date: '2024-02-10'
  },
  {
    id: '4',
    name: 'Meera Iyer',
    role: 'Doctor',
    image: 'https://i.pravatar.cc/150?img=45',
    rating: 5,
    comment: 'Excellent project with great amenities. The possession was on time, and the quality is outstanding. Living at Ishtika Green Valley has been a wonderful experience for our family.',
    project: 'Ishtika Green Valley',
    date: '2023-09-05'
  }
];
