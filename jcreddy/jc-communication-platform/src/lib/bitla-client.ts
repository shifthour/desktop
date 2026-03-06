/**
 * Bitla API Client
 * Handles all communication with the Bitla ticketSimply API
 */

interface BitlaConfig {
  baseUrl: string
  apiKey?: string
  username?: string
  password?: string
}

interface LoginResponse {
  code: number
  user_id?: number
  key?: string
  name?: string
  travel_branch_id?: number
  message?: string
}

interface Route {
  id: number
  route_num: string
  route_status: string
}

interface PassengerDetails {
  pnr_number: string
  title: string
  name: string
  age: number
  mobile: string
  email: string
  seat_number: string
  is_ticket_confirm: boolean
  origin: string
  destination: string
  booked_by: string
  is_boarded: number
  boarding_at: string
  drop_off: string
  boarding_address: string
  boarding_landmark: string
  bording_date_time: string
  bp_stage_latitude: string
  bp_stage_longitude: string
  dp_stage_latitude: string
  dp_stage_longitude: string
  wake_up_call_applicable: boolean
  pre_boarding_applicable: boolean
  welcome_call_applicable: boolean
  is_trackingo_sms_allowed: boolean
  booked_date: string
}

interface TripData {
  route_num: string
  route_id: number
  travel_date: string
  coach_num: string
  available_seats: number
  blocked_count: number
  captain1_details: string
  captain2_details: string
  origin: string
  destination: string
  reservation_id: number
  route_departure_time: string
  route_duration: string
  trip_departure_date_time: string
  trip_arrival_date_time: string
  bus_type: string
  customer_helpline_number: string
  passenger_details: PassengerDetails[]
}

export class BitlaClient {
  private baseUrl: string
  private apiKey: string | null = null
  private username: string
  private password: string

  constructor(config: BitlaConfig) {
    this.baseUrl = config.baseUrl.replace(/\/$/, '')
    this.username = config.username || ''
    this.password = config.password || ''
    this.apiKey = config.apiKey || null
  }

  /**
   * Login to Bitla API and get API key
   */
  async login(): Promise<LoginResponse> {
    const url = `${this.baseUrl}/login.json?login=${encodeURIComponent(this.username)}&password=${encodeURIComponent(this.password)}`

    const response = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
    })

    const data: LoginResponse = await response.json()

    if (data.code === 200 && data.key) {
      this.apiKey = data.key
    }

    return data
  }

  /**
   * Ensure we have a valid API key
   */
  private async ensureApiKey(): Promise<string> {
    if (!this.apiKey) {
      const loginResult = await this.login()
      if (!loginResult.key) {
        throw new Error('Failed to authenticate with Bitla API')
      }
    }
    return this.apiKey!
  }

  /**
   * Get all active routes
   */
  async getRoutes(): Promise<Route[]> {
    const apiKey = await this.ensureApiKey()
    const url = `${this.baseUrl}/all_routes.json?api_key=${apiKey}`

    const response = await fetch(url, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
    })

    if (!response.ok) {
      throw new Error(`Failed to fetch routes: ${response.statusText}`)
    }

    const routes: Route[] = await response.json()
    return routes.filter(route => route.route_status === 'Active')
  }

  /**
   * Get passenger details for a specific route and date
   */
  async getPassengerDetails(routeId: number, date: string): Promise<TripData | null> {
    const apiKey = await this.ensureApiKey()
    const url = `${this.baseUrl}/get_passenger_details/${date}.json?api_key=${apiKey}&route_id=${routeId}`

    try {
      const response = await fetch(url, {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json',
        },
      })

      if (!response.ok) {
        if (response.status === 404) {
          return null // No data for this route/date
        }
        throw new Error(`Failed to fetch passenger details: ${response.statusText}`)
      }

      const data: TripData = await response.json()
      return data
    } catch (error) {
      console.error(`Error fetching passengers for route ${routeId} on ${date}:`, error)
      return null
    }
  }

  /**
   * Sync all data for today and tomorrow
   */
  async syncAllData(): Promise<{
    routes: Route[]
    trips: TripData[]
  }> {
    const routes = await this.getRoutes()
    const trips: TripData[] = []

    const today = new Date()
    const tomorrow = new Date(today)
    tomorrow.setDate(tomorrow.getDate() + 1)
    const dayAfter = new Date(today)
    dayAfter.setDate(dayAfter.getDate() + 2)

    const dates = [
      today.toISOString().split('T')[0],
      tomorrow.toISOString().split('T')[0],
      dayAfter.toISOString().split('T')[0],
    ]

    for (const route of routes) {
      for (const date of dates) {
        const tripData = await this.getPassengerDetails(route.id, date)
        if (tripData && tripData.passenger_details?.length > 0) {
          trips.push(tripData)
        }
        // Add small delay to avoid rate limiting
        await new Promise(resolve => setTimeout(resolve, 100))
      }
    }

    return { routes, trips }
  }
}

// Factory function to create client from environment variables
export function createBitlaClient(): BitlaClient {
  return new BitlaClient({
    baseUrl: process.env.BITLA_API_BASE_URL || 'http://myth.mythribus.com/api/',
    apiKey: process.env.BITLA_API_KEY,
    username: process.env.BITLA_USERNAME,
    password: process.env.BITLA_PASSWORD,
  })
}
