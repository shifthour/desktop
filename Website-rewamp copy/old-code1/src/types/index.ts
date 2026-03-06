// Project Types
export interface Project {
  id: string;
  title: string;
  slug: string;
  subtitle: string;
  status: 'ongoing' | 'upcoming' | 'completed';
  location: string;
  city: string;
  price: {
    min: number;
    max: number;
    currency: string;
  };
  sizes: {
    min: number;
    max: number;
    unit: string;
  };
  configurations: string[];
  totalUnits: number;
  mainImage: string;
  images: string[];
  video?: string;
  description: string;
  highlights: string[];
  amenities: Amenity[];
  specifications: Specification[];
  floorPlans: FloorPlan[];
  locationAdvantages: LocationAdvantage[];
  possession: string;
  reraNumber?: string;
  featured: boolean;
}

export interface Amenity {
  id: string;
  name: string;
  icon: string;
  description?: string;
  category: 'sports' | 'wellness' | 'lifestyle' | 'security' | 'convenience';
}

export interface Specification {
  category: string;
  items: {
    label: string;
    value: string;
  }[];
}

export interface FloorPlan {
  id: string;
  type: string;
  size: number;
  bedrooms: number;
  bathrooms: number;
  price: number;
  image: string;
  available: boolean;
}

export interface LocationAdvantage {
  name: string;
  distance: string;
  icon: string;
  category: 'education' | 'healthcare' | 'transport' | 'entertainment' | 'shopping' | 'workplace';
}

// Testimonial Types
export interface Testimonial {
  id: string;
  name: string;
  role: string;
  company?: string;
  image: string;
  rating: number;
  comment: string;
  project?: string;
  date: string;
}

// Team Member Types
export interface TeamMember {
  id: string;
  name: string;
  role: string;
  image: string;
  bio: string;
  linkedin?: string;
  email?: string;
}

// Blog/News Types
export interface BlogPost {
  id: string;
  title: string;
  slug: string;
  excerpt: string;
  content: string;
  image: string;
  author: string;
  date: string;
  category: string;
  tags: string[];
  readTime: number;
}

// Contact Form Types
export interface ContactFormData {
  name: string;
  email: string;
  phone: string;
  subject?: string;
  message: string;
  projectInterest?: string;
  preferredContact?: 'email' | 'phone' | 'whatsapp';
}

export interface EnquiryFormData {
  name: string;
  email: string;
  phone: string;
  configuration?: string;
  projectId: string;
  message?: string;
  visitDate?: string;
}

// Company Information
export interface CompanyInfo {
  name: string;
  tagline: string;
  description: string;
  founded: string;
  headquarters: string;
  totalProjects: number;
  totalSqft: number;
  happyFamilies: number;
  awards: Award[];
  vision: string;
  mission: string;
  values: string[];
}

export interface Award {
  id: string;
  title: string;
  year: string;
  organization: string;
  image?: string;
}

// Timeline Event
export interface TimelineEvent {
  id: string;
  year: string;
  title: string;
  description: string;
  image?: string;
}

// Stats
export interface Stats {
  projects: number;
  sqftDeveloped: number;
  happyFamilies: number;
  yearsExperience: number;
}

// Navigation
export interface NavItem {
  label: string;
  href: string;
  submenu?: NavItem[];
}
