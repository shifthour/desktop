export interface Service {
  id: string;
  title: string;
  description: string;
  features: string[];
  icon: string;
  href: string;
}

export interface TeamMember {
  id: string;
  name: string;
  role: string;
  bio: string;
  image: string;
  linkedin?: string;
  email?: string;
}

export interface CaseStudy {
  id: string;
  title: string;
  client: string;
  description: string;
  results: string[];
  image: string;
  tags: string[];
}

export interface ContactFormData {
  name: string;
  email: string;
  company?: string;
  phone?: string;
  service: string;
  message: string;
}