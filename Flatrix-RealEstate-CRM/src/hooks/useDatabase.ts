'use client'

import { useEffect, useState, useCallback, useMemo, useRef } from 'react'
import { supabase } from '@/lib/supabase'
import type { Database } from '@/lib/database.types'

type Tables = Database['public']['Tables']

// Generic hook for fetching data
export function useSupabaseQuery<T extends keyof Tables>(
  tableName: T,
  options?: {
    select?: string
    filters?: Record<string, any>
    orderBy?: { column: string; ascending?: boolean }
    limit?: number
  }
) {
  const [data, setData] = useState<Tables[T]['Row'][] | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [refreshKey, setRefreshKey] = useState(0)

  // Memoize options to prevent unnecessary re-fetches
  const optionsKey = useMemo(() => JSON.stringify(options), [options?.select, options?.limit, JSON.stringify(options?.filters), options?.orderBy?.column, options?.orderBy?.ascending])
  const hasFetchedRef = useRef(false)

  const fetchData = useCallback(async () => {
    try {
      setLoading(true)
      setError(null)

      let query = supabase.from(tableName).select(options?.select || '*', { count: 'exact' })

      // Apply filters
      if (options?.filters) {
        Object.entries(options.filters).forEach(([key, value]) => {
          if (value !== undefined && value !== null) {
            query = query.eq(key, value)
          }
        })
      }

      // Apply ordering
      if (options?.orderBy) {
        query = query.order(options.orderBy.column, {
          ascending: options.orderBy.ascending ?? false
        })
      }

      // Apply limit - if specified use it, otherwise fetch all records
      // Supabase has a default 1000 record limit, so we need to explicitly handle larger datasets
      if (options?.limit) {
        query = query.limit(options.limit)
      } else {
        // Fetch all records by setting a very high limit
        // Supabase's max is typically 1000, so we'll use range() for larger datasets
        const BATCH_SIZE = 1000
        let allResults: any[] = []
        let start = 0
        let hasMore = true

        while (hasMore) {
          const { data: batch, error, count } = await query.range(start, start + BATCH_SIZE - 1)

          if (error) {
            console.error(`[useDatabase] Error fetching ${tableName}:`, error)
            console.error(`[useDatabase] Query options:`, options)
            throw error
          }

          if (batch && batch.length > 0) {
            allResults = [...allResults, ...batch]
            start += BATCH_SIZE

            // Check if we've fetched all records
            if (count && allResults.length >= count) {
              hasMore = false
            } else if (batch.length < BATCH_SIZE) {
              hasMore = false
            }
          } else {
            hasMore = false
          }
        }

        console.log(`[useDatabase] Successfully fetched ${tableName}:`, allResults.length, 'records (batched)')
        setData(allResults || [])
        setLoading(false)
        return
      }

      const { data: result, error } = await query

      if (error) {
        console.error(`[useDatabase] Error fetching ${tableName}:`, error)
        console.error(`[useDatabase] Query options:`, options)
        throw error
      }

      console.log(`[useDatabase] Successfully fetched ${tableName}:`, result?.length || 0, 'records')
      setData((result as any) || [])
    } catch (err) {
      console.error(`[useDatabase] Exception fetching ${tableName}:`, err)
      setError(err instanceof Error ? err.message : 'An error occurred')
    } finally {
      setLoading(false)
    }
  }, [tableName, optionsKey])

  useEffect(() => {
    // Only fetch on initial mount or when refreshKey changes
    if (!hasFetchedRef.current || refreshKey > 0) {
      hasFetchedRef.current = true
      fetchData()
    }
  }, [fetchData, refreshKey])

  const refetch = useCallback(() => {
    setRefreshKey(prev => prev + 1)
  }, [])

  return { data, loading, error, refetch }
}

// Specific hooks for each table
export const useLeads = (filters?: Record<string, any>) => 
  useSupabaseQuery('flatrix_leads', {
    select: `
      *,
      assigned_to:flatrix_users!assigned_to_id(name),
      created_by:flatrix_users!created_by_id(name),
      channel_partner:flatrix_channel_partners!channel_partner_id(company_name)
    `,
    filters,
    orderBy: { column: 'created_at', ascending: false }
  })

export const useChannelPartners = (filters?: Record<string, any>) => 
  useSupabaseQuery('flatrix_channel_partners', {
    select: `
      *,
      manager:flatrix_users!manager_id(name)
    `,
    filters,
    orderBy: { column: 'created_at', ascending: false }
  })

export const useProperties = (filters?: Record<string, any>) => 
  useSupabaseQuery('flatrix_properties', {
    select: `
      *,
      project:flatrix_projects!project_id(name, location, developer_name)
    `,
    filters,
    orderBy: { column: 'created_at', ascending: false }
  })

export const useProjects = (filters?: Record<string, any>) => 
  useSupabaseQuery('flatrix_projects', {
    filters,
    orderBy: { column: 'created_at', ascending: false }
  })

export const useDeals = (filters?: Record<string, any>) =>
  useSupabaseQuery('flatrix_deals', {
    select: `
      *,
      lead:flatrix_leads!flatrix_deals_lead_id_fkey(
        id,
        name,
        phone,
        email,
        assigned_to_id,
        assigned_to:flatrix_users!assigned_to_id(name)
      )
    `,
    filters,
    orderBy: { column: 'created_at', ascending: false }
  })

export const useCommissions = (filters?: Record<string, any>) => 
  useSupabaseQuery('flatrix_commissions', {
    select: `
      *,
      deal:flatrix_deals!deal_id(
        deal_number, 
        deal_value,
        lead:flatrix_leads!lead_id(first_name, last_name),
        property:flatrix_properties!property_id(unit_number, project:flatrix_projects!project_id(name))
      ),
      channel_partner:flatrix_channel_partners!channel_partner_id(company_name)
    `,
    filters,
    orderBy: { column: 'created_at', ascending: false }
  })

export const useConversionHistory = (filters?: Record<string, any>) => 
  useSupabaseQuery('flatrix_conversion_history' as any, {
    filters,
    orderBy: { column: 'changed_at', ascending: false }
  })

// Dashboard stats
export const useDashboardStats = () => {
  const [stats, setStats] = useState({
    totalLeads: 0,
    activeDeals: 0,
    totalProperties: 0,
    totalRevenue: 0,
    pendingCommissions: 0,
    approvedCommissions: 0,
    paidCommissions: 0
  })
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    async function fetchStats() {
      try {
        // Get leads count
        const { count: leadsCount } = await supabase
          .from('flatrix_leads')
          .select('*', { count: 'exact', head: true })

        // Get active deals count
        const { count: activeDealsCount } = await supabase
          .from('flatrix_deals')
          .select('*', { count: 'exact', head: true })
          .eq('status', 'ACTIVE')

        // Get properties count
        const { count: propertiesCount } = await supabase
          .from('flatrix_properties')
          .select('*', { count: 'exact', head: true })

        // Get total revenue from won deals
        const { data: revenueData } = await supabase
          .from('flatrix_deals')
          .select('deal_value')
          .eq('status', 'WON')

        const totalRevenue = revenueData?.reduce((sum, deal) => sum + deal.deal_value, 0) || 0

        // Get commission stats
        const { data: commissionsData } = await supabase
          .from('flatrix_commissions')
          .select('amount, status')

        const pendingCommissions = commissionsData?.filter(c => c.status === 'PENDING').reduce((sum, c) => sum + c.amount, 0) || 0
        const approvedCommissions = commissionsData?.filter(c => c.status === 'APPROVED').reduce((sum, c) => sum + c.amount, 0) || 0
        const paidCommissions = commissionsData?.filter(c => c.status === 'PAID').reduce((sum, c) => sum + c.amount, 0) || 0

        setStats({
          totalLeads: leadsCount || 0,
          activeDeals: activeDealsCount || 0,
          totalProperties: propertiesCount || 0,
          totalRevenue,
          pendingCommissions,
          approvedCommissions,
          paidCommissions
        })
      } catch (error) {
        console.error('Error fetching dashboard stats:', error)
      } finally {
        setLoading(false)
      }
    }

    fetchStats()
  }, [])

  return { stats, loading }
}