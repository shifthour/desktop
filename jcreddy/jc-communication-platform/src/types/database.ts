/**
 * Database types for Supabase
 * Auto-generated types can replace this file using Supabase CLI
 */

export interface Database {
  public: {
    Tables: {
      jc_operators: {
        Row: {
          id: string
          gds_operator_id: string | null
          operator_name: string
          operator_code: string
          api_base_url: string | null
          api_key_encrypted: string | null
          api_username: string | null
          logo_url: string | null
          contact_email: string | null
          contact_phone: string | null
          address: string | null
          settings: Record<string, any>
          is_active: boolean
          created_at: string
          updated_at: string
        }
        Insert: Omit<Database['public']['Tables']['jc_operators']['Row'], 'id' | 'created_at' | 'updated_at'>
        Update: Partial<Database['public']['Tables']['jc_operators']['Insert']>
      }
      jc_routes: {
        Row: {
          id: string
          operator_id: string
          bitla_route_id: number | null
          service_number: string | null
          route_name: string | null
          origin: string
          destination: string
          origin_code: string | null
          destination_code: string | null
          coach_type: string | null
          distance_km: number | null
          duration_hours: number | null
          base_fare: number | null
          departure_time: string | null
          arrival_time: string | null
          days_of_operation: string
          amenities: any[]
          multistation_fares: Record<string, any>
          status: string
          last_synced_at: string | null
          created_at: string
          updated_at: string
        }
        Insert: Omit<Database['public']['Tables']['jc_routes']['Row'], 'id' | 'created_at' | 'updated_at'>
        Update: Partial<Database['public']['Tables']['jc_routes']['Insert']>
      }
      jc_trips: {
        Row: {
          id: string
          route_id: string
          coach_id: string | null
          travel_date: string
          reservation_id: number | null
          service_number: string | null
          bus_type: string | null
          departure_time: string | null
          arrival_time: string | null
          departure_datetime: string | null
          arrival_datetime: string | null
          total_seats: number | null
          available_seats: number | null
          booked_seats: number
          blocked_seats: number
          captain1_name: string | null
          captain1_phone: string | null
          captain2_name: string | null
          captain2_phone: string | null
          conductor_name: string | null
          conductor_phone: string | null
          helpline_number: string | null
          trip_status: string
          delay_minutes: number
          current_location: Record<string, any> | null
          raw_data: Record<string, any> | null
          last_synced_at: string | null
          created_at: string
          updated_at: string
        }
        Insert: Omit<Database['public']['Tables']['jc_trips']['Row'], 'id' | 'created_at' | 'updated_at'>
        Update: Partial<Database['public']['Tables']['jc_trips']['Insert']>
      }
      jc_bookings: {
        Row: {
          id: string
          trip_id: string
          operator_id: string | null
          ticket_number: string
          travel_operator_pnr: string
          service_number: string | null
          travel_date: string
          origin: string | null
          destination: string | null
          boarding_point_id: string | null
          dropoff_point_id: string | null
          boarding_point_name: string | null
          boarding_address: string | null
          boarding_landmark: string | null
          boarding_time: string | null
          boarding_datetime: string | null
          boarding_latitude: number | null
          boarding_longitude: number | null
          dropoff_point_name: string | null
          dropoff_address: string | null
          dropoff_time: string | null
          dropoff_datetime: string | null
          dropoff_latitude: number | null
          dropoff_longitude: number | null
          total_seats: number
          total_fare: number | null
          base_fare: number | null
          service_tax: number | null
          cgst: number | null
          sgst: number | null
          igst: number | null
          convenience_fee: number | null
          discount: number
          payment_mode: string | null
          payment_status: string
          gst_in: string | null
          booked_by: string | null
          booked_by_login: string | null
          agent_id: string | null
          booking_date: string | null
          booking_source: string | null
          booking_status: string
          cancellation_date: string | null
          cancellation_reason: string | null
          cancellation_charge: number | null
          refund_amount: number | null
          raw_data: Record<string, any> | null
          created_at: string
          updated_at: string
        }
        Insert: Omit<Database['public']['Tables']['jc_bookings']['Row'], 'id' | 'created_at' | 'updated_at'>
        Update: Partial<Database['public']['Tables']['jc_bookings']['Insert']>
      }
      jc_passengers: {
        Row: {
          id: string
          booking_id: string
          trip_id: string | null
          pnr_number: string
          title: string | null
          first_name: string | null
          last_name: string | null
          full_name: string
          age: number | null
          gender: string | null
          mobile: string
          email: string | null
          id_proof_type: string | null
          id_proof_number: string | null
          seat_number: string
          seat_type: string | null
          seat_position: string | null
          fare: number | null
          base_fare: number | null
          cgst: number | null
          sgst: number | null
          igst: number | null
          is_primary: boolean
          is_confirmed: boolean
          is_boarded: boolean
          boarded_at: string | null
          boarding_location: Record<string, any> | null
          is_dropped: boolean
          dropped_at: string | null
          is_cancelled: boolean
          cancelled_at: string | null
          cancellation_charge: number | null
          refund_amount: number | null
          wake_up_call_applicable: boolean
          pre_boarding_applicable: boolean
          welcome_call_applicable: boolean
          is_sms_allowed: boolean
          is_whatsapp_allowed: boolean
          language_preference: string
          special_requirements: string | null
          raw_data: Record<string, any> | null
          created_at: string
          updated_at: string
        }
        Insert: Omit<Database['public']['Tables']['jc_passengers']['Row'], 'id' | 'created_at' | 'updated_at'>
        Update: Partial<Database['public']['Tables']['jc_passengers']['Insert']>
      }
      jc_notifications: {
        Row: {
          id: string
          passenger_id: string | null
          booking_id: string | null
          trip_id: string | null
          operator_id: string | null
          notification_type: string
          template_id: string | null
          recipient_mobile: string
          recipient_email: string | null
          recipient_name: string | null
          channel: string
          priority: number
          scheduled_at: string
          sent_at: string | null
          delivered_at: string | null
          read_at: string | null
          status: string
          message_content: string | null
          message_variables: Record<string, any>
          external_message_id: string | null
          provider: string | null
          provider_response: Record<string, any> | null
          error_message: string | null
          error_code: string | null
          retry_count: number
          max_retries: number
          next_retry_at: string | null
          metadata: Record<string, any>
          created_at: string
          updated_at: string
        }
        Insert: Omit<Database['public']['Tables']['jc_notifications']['Row'], 'id' | 'created_at' | 'updated_at'>
        Update: Partial<Database['public']['Tables']['jc_notifications']['Insert']>
      }
      jc_webhook_events: {
        Row: {
          id: string
          operator_id: string | null
          event_type: string
          endpoint: string
          method: string
          headers: Record<string, any> | null
          payload: Record<string, any>
          source_ip: string | null
          signature: string | null
          signature_valid: boolean | null
          processed: boolean
          processed_at: string | null
          processing_error: string | null
          retry_count: number
          created_at: string
        }
        Insert: Omit<Database['public']['Tables']['jc_webhook_events']['Row'], 'id' | 'created_at'>
        Update: Partial<Database['public']['Tables']['jc_webhook_events']['Insert']>
      }
      jc_daily_stats: {
        Row: {
          id: string
          operator_id: string | null
          stat_date: string
          total_bookings: number
          total_passengers: number
          total_cancellations: number
          total_revenue: number
          total_refunds: number
          notifications_sent: number
          notifications_delivered: number
          notifications_failed: number
          whatsapp_sent: number
          sms_sent: number
          trips_completed: number
          avg_occupancy_percent: number | null
          created_at: string
          updated_at: string
        }
        Insert: Omit<Database['public']['Tables']['jc_daily_stats']['Row'], 'id' | 'created_at' | 'updated_at'>
        Update: Partial<Database['public']['Tables']['jc_daily_stats']['Insert']>
      }
      jc_cancellations: {
        Row: {
          id: string
          booking_id: string | null
          travel_operator_pnr: string
          ticket_number: string | null
          cancellation_type: string
          seat_numbers: string[] | null
          passenger_names: string[] | null
          original_fare: number | null
          cancellation_charge: number | null
          refund_amount: number | null
          refund_status: string
          refund_transaction_id: string | null
          cancelled_by: string | null
          cancellation_source: string | null
          cancellation_reason: string | null
          cancelled_at: string
          refunded_at: string | null
          raw_data: Record<string, any> | null
          created_at: string
          updated_at: string
        }
        Insert: Omit<Database['public']['Tables']['jc_cancellations']['Row'], 'id' | 'created_at' | 'updated_at'>
        Update: Partial<Database['public']['Tables']['jc_cancellations']['Insert']>
      }
      jc_message_templates: {
        Row: {
          id: string
          operator_id: string | null
          template_code: string
          template_name: string
          notification_type: string
          channel: string
          language: string
          subject: string | null
          body: string
          variables: any[]
          whatsapp_template_name: string | null
          whatsapp_namespace: string | null
          sms_dlt_template_id: string | null
          is_active: boolean
          version: number
          created_at: string
          updated_at: string
        }
        Insert: Omit<Database['public']['Tables']['jc_message_templates']['Row'], 'id' | 'created_at' | 'updated_at'>
        Update: Partial<Database['public']['Tables']['jc_message_templates']['Insert']>
      }
      jc_demand_snapshots: {
        Row: {
          id: string
          trip_id: string
          route_id: string | null
          service_number: string | null
          travel_date: string
          departure_time: string | null
          snapshot_at: string
          total_seats: number | null
          available_seats: number | null
          booked_seats: number | null
          blocked_seats: number | null
          occupancy_percent: number | null
          bookings_last_1h: number
          bookings_last_6h: number
          bookings_last_24h: number
          cancellations_last_24h: number
          hours_to_departure: number | null
          day_of_week: number | null
          is_weekend: boolean
          is_holiday: boolean
          avg_fare_booked: number | null
          min_fare_booked: number | null
          max_fare_booked: number | null
          route_base_fare: number | null
          total_revenue_so_far: number
          created_at: string
        }
        Insert: Omit<Database['public']['Tables']['jc_demand_snapshots']['Row'], 'id' | 'created_at'>
        Update: Partial<Database['public']['Tables']['jc_demand_snapshots']['Insert']>
      }
      jc_pricing_config: {
        Row: {
          id: string
          config_key: string
          config_value: Record<string, any>
          description: string | null
          is_active: boolean
          created_at: string
          updated_at: string
        }
        Insert: Omit<Database['public']['Tables']['jc_pricing_config']['Row'], 'id' | 'created_at' | 'updated_at'>
        Update: Partial<Database['public']['Tables']['jc_pricing_config']['Insert']>
      }
      jc_pricing_rules: {
        Row: {
          id: string
          rule_name: string
          rule_type: string
          description: string | null
          condition: Record<string, any>
          multiplier: number
          priority: number
          is_active: boolean
          applies_to_routes: string[]
          created_at: string
          updated_at: string
        }
        Insert: Omit<Database['public']['Tables']['jc_pricing_rules']['Row'], 'id' | 'created_at' | 'updated_at'>
        Update: Partial<Database['public']['Tables']['jc_pricing_rules']['Insert']>
      }
      jc_pricing_recommendations: {
        Row: {
          id: string
          trip_id: string
          route_id: string | null
          snapshot_id: string | null
          service_number: string | null
          travel_date: string
          current_base_fare: number
          recommended_fare: number
          fare_change_percent: number
          fare_multiplier: number
          demand_score: number | null
          occupancy_percent: number | null
          hours_to_departure: number | null
          booking_velocity_1h: number | null
          applied_rules: Array<{ rule_id: string; rule_name: string; multiplier: number }>
          status: string
          reviewed_by: string | null
          reviewed_at: string | null
          review_notes: string | null
          generated_at: string
          expires_at: string | null
          estimated_revenue_impact: number | null
          created_at: string
          updated_at: string
        }
        Insert: Omit<Database['public']['Tables']['jc_pricing_recommendations']['Row'], 'id' | 'created_at' | 'updated_at'>
        Update: Partial<Database['public']['Tables']['jc_pricing_recommendations']['Insert']>
      }
    }
    Views: {}
    Functions: {}
    Enums: {}
  }
}

// Convenience types
export type Operator = Database['public']['Tables']['jc_operators']['Row']
export type Route = Database['public']['Tables']['jc_routes']['Row']
export type Trip = Database['public']['Tables']['jc_trips']['Row']
export type Booking = Database['public']['Tables']['jc_bookings']['Row']
export type Passenger = Database['public']['Tables']['jc_passengers']['Row']
export type Notification = Database['public']['Tables']['jc_notifications']['Row']
export type WebhookEvent = Database['public']['Tables']['jc_webhook_events']['Row']
export type DailyStats = Database['public']['Tables']['jc_daily_stats']['Row']
export type Cancellation = Database['public']['Tables']['jc_cancellations']['Row']
export type MessageTemplate = Database['public']['Tables']['jc_message_templates']['Row']
export type DemandSnapshot = Database['public']['Tables']['jc_demand_snapshots']['Row']
export type PricingConfig = Database['public']['Tables']['jc_pricing_config']['Row']
export type PricingRule = Database['public']['Tables']['jc_pricing_rules']['Row']
export type PricingRecommendation = Database['public']['Tables']['jc_pricing_recommendations']['Row']
