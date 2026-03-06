export interface Physiotherapist {
  id: string;
  slug: string;
  name: string;
  photo: string;
  gender: "male" | "female";
  specializations: string[];
  qualifications: string[];
  experience: number;
  bio: string;
  rating: number;
  reviewCount: number;
  clinicName: string;
  location: {
    area: string;
    city: string;
    lat: number;
    lng: number;
  };
  visitTypes: ("clinic" | "home" | "online")[];
  services: Service[];
  availability: DayAvailability[];
  verified: boolean;
  totalSessions: number;
}

export interface Service {
  id: string;
  name: string;
  duration: number;
  price: number;
  description: string;
}

export interface DayAvailability {
  date: string;
  slots: TimeSlot[];
}

export interface TimeSlot {
  time: string;
  available: boolean;
  heldBy?: string;
  blockedByAdmin?: boolean;
}

export interface Review {
  id: string;
  physioId: string;
  patientName: string;
  rating: number;
  date: string;
  text: string;
  verified: boolean;
}

export interface Booking {
  id: string;
  physioId: string;
  physioName: string;
  patientName: string;
  patientEmail: string;
  patientPhone: string;
  serviceId: string;
  serviceName: string;
  date: string;
  time: string;
  duration: number;
  fee: number;
  platformFee: number;
  status: "confirmed" | "pending" | "cancelled" | "completed";
  reason?: string;
  createdAt: string;
}

export interface AdminStats {
  totalProviders: number;
  newProviders: number;
  bookingsThisMonth: number;
  bookingsGrowth: number;
  revenueThisMonth: number;
  revenueGrowth: number;
  pendingApprovals: number;
}

export type Specialization =
  | "Sports Injury"
  | "Neurological"
  | "Orthopedic"
  | "Pediatric"
  | "Geriatric"
  | "Cardiopulmonary"
  | "Women's Health"
  | "Post-Surgical";
