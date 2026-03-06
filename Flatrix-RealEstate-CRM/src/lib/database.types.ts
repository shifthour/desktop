export type Json =
  | string
  | number
  | boolean
  | null
  | { [key: string]: Json | undefined }
  | Json[]

export interface Database {
  public: {
    Tables: {
      flatrix_users: {
        Row: {
          id: string
          email: string
          password: string
          name: string
          phone: string | null
          role: 'ADMIN' | 'CHANNEL_PARTNER' | 'SALES_MANAGER' | 'AGENT'
          is_active: boolean
          channel_partner_id: string | null
          created_at: string
          updated_at: string
        }
        Insert: {
          id?: string
          email: string
          password: string
          name: string
          phone?: string | null
          role?: 'ADMIN' | 'CHANNEL_PARTNER' | 'SALES_MANAGER' | 'AGENT'
          is_active?: boolean
          channel_partner_id?: string | null
          created_at?: string
          updated_at?: string
        }
        Update: {
          id?: string
          email?: string
          password?: string
          name?: string
          phone?: string | null
          role?: 'ADMIN' | 'CHANNEL_PARTNER' | 'SALES_MANAGER' | 'AGENT'
          is_active?: boolean
          channel_partner_id?: string | null
          created_at?: string
          updated_at?: string
        }
      }
      flatrix_channel_partners: {
        Row: {
          id: string
          company_name: string
          registration_no: string | null
          address: string | null
          city: string | null
          state: string | null
          pincode: string | null
          website: string | null
          gst_number: string | null
          pan_number: string | null
          commission_rate: number
          total_earnings: number
          pending_payments: number
          is_active: boolean
          manager_id: string | null
          created_at: string
          updated_at: string
        }
        Insert: {
          id?: string
          company_name: string
          registration_no?: string | null
          address?: string | null
          city?: string | null
          state?: string | null
          pincode?: string | null
          website?: string | null
          gst_number?: string | null
          pan_number?: string | null
          commission_rate?: number
          total_earnings?: number
          pending_payments?: number
          is_active?: boolean
          manager_id?: string | null
          created_at?: string
          updated_at?: string
        }
        Update: {
          id?: string
          company_name?: string
          registration_no?: string | null
          address?: string | null
          city?: string | null
          state?: string | null
          pincode?: string | null
          website?: string | null
          gst_number?: string | null
          pan_number?: string | null
          commission_rate?: number
          total_earnings?: number
          pending_payments?: number
          is_active?: boolean
          manager_id?: string | null
          created_at?: string
          updated_at?: string
        }
      }
      flatrix_leads: {
        Row: {
          id: string
          first_name?: string
          last_name?: string | null
          email?: string | null
          phone?: string
          alternate_phone?: string | null
          assigned_to_id: string | null
          created_by_id: string | null
          channel_partner_id: string | null
          status: 'NEW' | 'CONTACTED' | 'QUALIFIED' | 'NEGOTIATION' | 'CONVERTED' | 'LOST' | 'DISQUALIFIED'
          budget_min: number | null
          budget_max: number | null
          interested_in: ('APARTMENT' | 'VILLA' | 'PLOT' | 'COMMERCIAL' | 'OFFICE_SPACE')[] | null
          preferred_location: string | null
          project_name: string | null
          source: string | null
          notes: string | null
          last_contacted_at: string | null
          next_followup_date: string | null
          created_at: string
          updated_at: string
        }
        Insert: {
          id?: string
          first_name?: string
          last_name?: string | null
          email?: string | null
          phone?: string
          alternate_phone?: string | null
          assigned_to_id?: string | null
          created_by_id?: string | null
          channel_partner_id?: string | null
          status?: 'NEW' | 'CONTACTED' | 'QUALIFIED' | 'NEGOTIATION' | 'CONVERTED' | 'LOST' | 'DISQUALIFIED'
          budget_min?: number | null
          budget_max?: number | null
          interested_in?: ('APARTMENT' | 'VILLA' | 'PLOT' | 'COMMERCIAL' | 'OFFICE_SPACE')[] | null
          preferred_location?: string | null
          project_name?: string | null
          source?: string | null
          notes?: string | null
          last_contacted_at?: string | null
          next_followup_date?: string | null
          created_at?: string
          updated_at?: string
        }
        Update: {
          id?: string
          first_name?: string
          last_name?: string | null
          email?: string | null
          phone?: string
          alternate_phone?: string | null
          assigned_to_id?: string | null
          created_by_id?: string | null
          channel_partner_id?: string | null
          status?: 'NEW' | 'CONTACTED' | 'QUALIFIED' | 'NEGOTIATION' | 'CONVERTED' | 'LOST' | 'DISQUALIFIED'
          budget_min?: number | null
          budget_max?: number | null
          interested_in?: ('APARTMENT' | 'VILLA' | 'PLOT' | 'COMMERCIAL' | 'OFFICE_SPACE')[] | null
          preferred_location?: string | null
          project_name?: string | null
          source?: string | null
          notes?: string | null
          last_contacted_at?: string | null
          next_followup_date?: string | null
          created_at?: string
          updated_at?: string
        }
      }
      flatrix_properties: {
        Row: {
          id: string
          unit_number: string
          type: 'APARTMENT' | 'VILLA' | 'PLOT' | 'COMMERCIAL' | 'OFFICE_SPACE'
          status: 'AVAILABLE' | 'BOOKED' | 'SOLD' | 'HOLD'
          floor: number | null
          area: number
          price: number
          facing: string | null
          bedrooms: number | null
          bathrooms: number | null
          parking: boolean
          furnishing: string | null
          project_id: string
          created_at: string
          updated_at: string
        }
        Insert: {
          id?: string
          unit_number: string
          type: 'APARTMENT' | 'VILLA' | 'PLOT' | 'COMMERCIAL' | 'OFFICE_SPACE'
          status?: 'AVAILABLE' | 'BOOKED' | 'SOLD' | 'HOLD'
          floor?: number | null
          area: number
          price: number
          facing?: string | null
          bedrooms?: number | null
          bathrooms?: number | null
          parking?: boolean
          furnishing?: string | null
          project_id: string
          created_at?: string
          updated_at?: string
        }
        Update: {
          id?: string
          unit_number?: string
          type?: 'APARTMENT' | 'VILLA' | 'PLOT' | 'COMMERCIAL' | 'OFFICE_SPACE'
          status?: 'AVAILABLE' | 'BOOKED' | 'SOLD' | 'HOLD'
          floor?: number | null
          area?: number
          price?: number
          facing?: string | null
          bedrooms?: number | null
          bathrooms?: number | null
          parking?: boolean
          furnishing?: string | null
          project_id?: string
          created_at?: string
          updated_at?: string
        }
      }
      flatrix_projects: {
        Row: {
          id: string
          name: string
          developer_name: string
          location: string
          city: string
          total_units: number
          available_units: number
          price_range: string | null
          description: string | null
          amenities: string[] | null
          brochure_url: string | null
          launch_date: string | null
          completion_date: string | null
          rera_number: string | null
          project_stage: 'Planning' | 'Under Construction' | 'Completed'
          start_date: string | null
          number_of_floors: number | null
          number_of_towers: number | null
          primary_contact_name: string | null
          primary_contact_phone: string | null
          alternate_contact_name: string | null
          alternate_contact_phone: string | null
          project_size_acres: number | null
          cover_image_url: string | null
          is_active: boolean
          created_at: string
          updated_at: string
        }
        Insert: {
          id?: string
          name: string
          developer_name: string
          location: string
          city: string
          total_units: number
          available_units: number
          price_range?: string | null
          description?: string | null
          amenities?: string[] | null
          brochure_url?: string | null
          launch_date?: string | null
          completion_date?: string | null
          rera_number?: string | null
          project_stage?: 'Planning' | 'Under Construction' | 'Completed'
          start_date?: string | null
          number_of_floors?: number | null
          number_of_towers?: number | null
          primary_contact_name?: string | null
          primary_contact_phone?: string | null
          alternate_contact_name?: string | null
          alternate_contact_phone?: string | null
          project_size_acres?: number | null
          cover_image_url?: string | null
          is_active?: boolean
          created_at?: string
          updated_at?: string
        }
        Update: {
          id?: string
          name?: string
          developer_name?: string
          location?: string
          city?: string
          total_units?: number
          available_units?: number
          price_range?: string | null
          description?: string | null
          amenities?: string[] | null
          brochure_url?: string | null
          launch_date?: string | null
          completion_date?: string | null
          rera_number?: string | null
          project_stage?: 'Planning' | 'Under Construction' | 'Completed'
          start_date?: string | null
          number_of_floors?: number | null
          number_of_towers?: number | null
          primary_contact_name?: string | null
          primary_contact_phone?: string | null
          alternate_contact_name?: string | null
          alternate_contact_phone?: string | null
          project_size_acres?: number | null
          cover_image_url?: string | null
          is_active?: boolean
          created_at?: string
          updated_at?: string
        }
      }
      flatrix_deals: {
        Row: {
          id: string
          deal_number: string
          lead_id: string
          property_id: string
          user_id: string
          channel_partner_id: string | null
          deal_value: number
          status: 'ACTIVE' | 'WON' | 'LOST' | 'ON_HOLD'
          booking_date: string
          closing_date: string | null
          notes: string | null
          documents: string[] | null
          site_visit_status: string | null
          site_visit_date: string | null
          conversion_status: string | null
          conversion_date: string | null
          attended_by: string | null
          created_at: string
          updated_at: string
        }
        Insert: {
          id?: string
          deal_number?: string
          lead_id: string
          property_id: string
          user_id: string
          channel_partner_id?: string | null
          deal_value: number
          status?: 'ACTIVE' | 'WON' | 'LOST' | 'ON_HOLD'
          booking_date?: string
          closing_date?: string | null
          notes?: string | null
          documents?: string[] | null
          site_visit_status?: string | null
          site_visit_date?: string | null
          conversion_status?: string | null
          conversion_date?: string | null
          attended_by?: string | null
          created_at?: string
          updated_at?: string
        }
        Update: {
          id?: string
          deal_number?: string
          lead_id?: string
          property_id?: string
          user_id?: string
          channel_partner_id?: string | null
          deal_value?: number
          status?: 'ACTIVE' | 'WON' | 'LOST' | 'ON_HOLD'
          booking_date?: string
          closing_date?: string | null
          notes?: string | null
          documents?: string[] | null
          site_visit_status?: string | null
          site_visit_date?: string | null
          conversion_status?: string | null
          conversion_date?: string | null
          attended_by?: string | null
          created_at?: string
          updated_at?: string
        }
      }
      flatrix_commissions: {
        Row: {
          id: string
          deal_id: string
          channel_partner_id: string
          amount: number
          percentage: number
          status: 'PENDING' | 'APPROVED' | 'PAID' | 'CANCELLED'
          approved_date: string | null
          paid_date: string | null
          payment_method: string | null
          transaction_ref: string | null
          notes: string | null
          created_at: string
          updated_at: string
        }
        Insert: {
          id?: string
          deal_id: string
          channel_partner_id: string
          amount: number
          percentage: number
          status?: 'PENDING' | 'APPROVED' | 'PAID' | 'CANCELLED'
          approved_date?: string | null
          paid_date?: string | null
          payment_method?: string | null
          transaction_ref?: string | null
          notes?: string | null
          created_at?: string
          updated_at?: string
        }
        Update: {
          id?: string
          deal_id?: string
          channel_partner_id?: string
          amount?: number
          percentage?: number
          status?: 'PENDING' | 'APPROVED' | 'PAID' | 'CANCELLED'
          approved_date?: string | null
          paid_date?: string | null
          payment_method?: string | null
          transaction_ref?: string | null
          notes?: string | null
          created_at?: string
          updated_at?: string
        }
      }
      flatrix_whatsapp_templates: {
        Row: {
          id: string
          project_id: string | null
          project_name_pattern: string
          greeting_template_name: string
          greeting_body: string | null
          brochure_template_name: string | null
          brochure_url: string | null
          video_template_name: string | null
          video_url: string | null
          template_language: string
          is_active: boolean
          is_default: boolean
          created_at: string
          updated_at: string
        }
        Insert: {
          id?: string
          project_id?: string | null
          project_name_pattern: string
          greeting_template_name: string
          greeting_body?: string | null
          brochure_template_name?: string | null
          brochure_url?: string | null
          video_template_name?: string | null
          video_url?: string | null
          template_language?: string
          is_active?: boolean
          is_default?: boolean
          created_at?: string
          updated_at?: string
        }
        Update: {
          id?: string
          project_id?: string | null
          project_name_pattern?: string
          greeting_template_name?: string
          greeting_body?: string | null
          brochure_template_name?: string | null
          brochure_url?: string | null
          video_template_name?: string | null
          video_url?: string | null
          template_language?: string
          is_active?: boolean
          is_default?: boolean
          created_at?: string
          updated_at?: string
        }
      }
      flatrix_whatsapp_logs: {
        Row: {
          id: string
          lead_id: string | null
          lead_phone: string
          lead_name: string | null
          project_name: string | null
          template_id: string | null
          message_type: 'greeting' | 'brochure' | 'video'
          wa_template_name: string | null
          wa_message_id: string | null
          status: 'pending' | 'sent' | 'delivered' | 'read' | 'failed'
          error_message: string | null
          error_code: string | null
          retry_count: number
          sent_at: string | null
          delivered_at: string | null
          read_at: string | null
          created_at: string
          updated_at: string
        }
        Insert: {
          id?: string
          lead_id?: string | null
          lead_phone: string
          lead_name?: string | null
          project_name?: string | null
          template_id?: string | null
          message_type?: 'greeting' | 'brochure' | 'video'
          wa_template_name?: string | null
          wa_message_id?: string | null
          status?: 'pending' | 'sent' | 'delivered' | 'read' | 'failed'
          error_message?: string | null
          error_code?: string | null
          retry_count?: number
          sent_at?: string | null
          delivered_at?: string | null
          read_at?: string | null
          created_at?: string
          updated_at?: string
        }
        Update: {
          id?: string
          lead_id?: string | null
          lead_phone?: string
          lead_name?: string | null
          project_name?: string | null
          template_id?: string | null
          message_type?: 'greeting' | 'brochure' | 'video'
          wa_template_name?: string | null
          wa_message_id?: string | null
          status?: 'pending' | 'sent' | 'delivered' | 'read' | 'failed'
          error_message?: string | null
          error_code?: string | null
          retry_count?: number
          sent_at?: string | null
          delivered_at?: string | null
          read_at?: string | null
          created_at?: string
          updated_at?: string
        }
      }
      flatrix_whatsapp_config: {
        Row: {
          id: string
          phone_number_id: string | null
          business_account_id: string | null
          webhook_verify_token: string | null
          is_enabled: boolean
          api_version: string
          created_at: string
          updated_at: string
        }
        Insert: {
          id?: string
          phone_number_id?: string | null
          business_account_id?: string | null
          webhook_verify_token?: string | null
          is_enabled?: boolean
          api_version?: string
          created_at?: string
          updated_at?: string
        }
        Update: {
          id?: string
          phone_number_id?: string | null
          business_account_id?: string | null
          webhook_verify_token?: string | null
          is_enabled?: boolean
          api_version?: string
          created_at?: string
          updated_at?: string
        }
      }
    }
  }
}