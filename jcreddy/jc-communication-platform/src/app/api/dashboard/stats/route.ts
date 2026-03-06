import { NextRequest, NextResponse } from 'next/server'
import { createServerClient } from '@/lib/supabase'

/**
 * Dashboard statistics API
 */
export async function GET(request: NextRequest) {
  try {
    const supabase = createServerClient()
    const searchParams = request.nextUrl.searchParams
    const range = searchParams.get('range') || 'today'
    const dateType = searchParams.get('dateType') || 'travel' // 'travel' or 'booked'
    const customStart = searchParams.get('startDate') // For custom date range
    const customEnd = searchParams.get('endDate') // For custom date range

    const today = new Date().toISOString().split('T')[0]
    const yesterday = new Date(Date.now() - 86400000).toISOString().split('T')[0]
    const tomorrow = new Date(Date.now() + 86400000).toISOString().split('T')[0]

    // Calculate date range based on filter
    let startDate = today
    let endDate = tomorrow

    switch (range) {
      case 'yesterday':
        startDate = yesterday
        endDate = today
        break
      case '7days':
        startDate = new Date(Date.now() - 7 * 86400000).toISOString().split('T')[0]
        endDate = tomorrow
        break
      case 'next7days':
        startDate = today
        endDate = new Date(Date.now() + 7 * 86400000).toISOString().split('T')[0]
        break
      case '15days':
        startDate = new Date(Date.now() - 15 * 86400000).toISOString().split('T')[0]
        endDate = tomorrow
        break
      case 'month':
        startDate = new Date(new Date().getFullYear(), new Date().getMonth(), 1).toISOString().split('T')[0]
        endDate = tomorrow
        break
      case 'lastMonth':
        const lastMonth = new Date(new Date().getFullYear(), new Date().getMonth() - 1, 1)
        startDate = lastMonth.toISOString().split('T')[0]
        endDate = new Date(new Date().getFullYear(), new Date().getMonth(), 1).toISOString().split('T')[0]
        break
      case 'nextMonth':
        startDate = new Date(new Date().getFullYear(), new Date().getMonth() + 1, 1).toISOString().split('T')[0]
        endDate = new Date(new Date().getFullYear(), new Date().getMonth() + 2, 1).toISOString().split('T')[0]
        break
      case 'custom':
        if (customStart) startDate = customStart
        if (customEnd) endDate = customEnd
        break
      default:
        startDate = today
        endDate = tomorrow
    }

    // Determine which date field to use based on dateType
    // travel = travel_date, booked = created_at (booking date)
    const bookingDateField = dateType === 'travel' ? 'travel_date' : 'created_at'
    const useDateOnly = dateType === 'travel' // travel_date is date only, created_at has time

    // Get bookings count for the range
    let bookingsQuery = supabase
      .from('jc_bookings')
      .select('*', { count: 'exact', head: true })

    if (useDateOnly) {
      bookingsQuery = bookingsQuery.gte('travel_date', startDate).lt('travel_date', endDate)
    } else {
      bookingsQuery = bookingsQuery.gte('created_at', `${startDate}T00:00:00`).lt('created_at', `${endDate}T00:00:00`)
    }
    const { count: rangeBookings } = await bookingsQuery

    // Get yesterday's bookings for comparison (using same dateType)
    let yesterdayQuery = supabase
      .from('jc_bookings')
      .select('*', { count: 'exact', head: true })

    if (useDateOnly) {
      yesterdayQuery = yesterdayQuery.eq('travel_date', yesterday)
    } else {
      yesterdayQuery = yesterdayQuery.gte('created_at', `${yesterday}T00:00:00`).lt('created_at', `${today}T00:00:00`)
    }
    const { count: yesterdayBookings } = await yesterdayQuery

    // Get passengers count for the range
    const { count: rangePassengers } = await supabase
      .from('jc_passengers')
      .select('*', { count: 'exact', head: true })
      .gte('created_at', `${startDate}T00:00:00`)
      .lt('created_at', `${endDate}T00:00:00`)

    // Get active trips (today and tomorrow)
    // Note: tomorrow is already defined at the top
    const { count: activeTrips } = await supabase
      .from('jc_trips')
      .select('*', { count: 'exact', head: true })
      .gte('travel_date', today)
      .lte('travel_date', tomorrow)
      .eq('trip_status', 'scheduled')

    // Get notification stats
    const { count: pendingNotifications } = await supabase
      .from('jc_notifications')
      .select('*', { count: 'exact', head: true })
      .eq('status', 'pending')

    const { count: sentNotifications } = await supabase
      .from('jc_notifications')
      .select('*', { count: 'exact', head: true })
      .eq('status', 'sent')
      .gte('created_at', `${startDate}T00:00:00`)

    const { count: deliveredNotifications } = await supabase
      .from('jc_notifications')
      .select('*', { count: 'exact', head: true })
      .eq('status', 'delivered')
      .gte('created_at', `${startDate}T00:00:00`)

    const { count: failedNotifications } = await supabase
      .from('jc_notifications')
      .select('*', { count: 'exact', head: true })
      .eq('status', 'failed')
      .gte('created_at', `${startDate}T00:00:00`)

    // Get webhook events count
    const { count: webhookEvents } = await supabase
      .from('jc_webhook_events')
      .select('*', { count: 'exact', head: true })
      .gte('created_at', `${startDate}T00:00:00`)

    const { count: processedWebhooks } = await supabase
      .from('jc_webhook_events')
      .select('*', { count: 'exact', head: true })
      .eq('processed', true)
      .gte('created_at', `${startDate}T00:00:00`)

    // Get cancellations count
    const { count: rangeCancellations } = await supabase
      .from('jc_cancellations')
      .select('*', { count: 'exact', head: true })
      .gte('created_at', `${startDate}T00:00:00`)

    // Calculate revenue (sum of bookings in range)
    // NOTE: Supabase default limit is 1000, we need to explicitly set higher limit
    const { data: revenueData } = await supabase
      .from('jc_bookings')
      .select('total_fare, cgst, sgst')
      .gte('created_at', `${startDate}T00:00:00`)
      .lt('created_at', `${endDate}T00:00:00`)
      .eq('booking_status', 'confirmed')
      .range(0, 49999)

    const rangeRevenue = (revenueData as { total_fare: number; cgst: number; sgst: number }[] | null)?.reduce((sum, b) => sum + (b.total_fare || 0), 0) || 0
    const rangeGST = (revenueData as { total_fare: number; cgst: number; sgst: number }[] | null)?.reduce((sum, b) => sum + (b.cgst || 0) + (b.sgst || 0), 0) || 0

    // Calculate percentages and trends
    const todayBookings = rangeBookings || 0
    const bookingChange = yesterdayBookings
      ? Math.round((todayBookings - yesterdayBookings) / yesterdayBookings * 100)
      : 0

    const deliveryRate = (sentNotifications || 0) + (deliveredNotifications || 0) > 0
      ? Math.round((deliveredNotifications || 0) / ((sentNotifications || 0) + (deliveredNotifications || 0)) * 100)
      : 0

    // Get pending notifications with phone details (for pending tickets section)
    const { data: pendingTickets } = await supabase
      .from('jc_notifications')
      .select(`
        id, recipient_name, recipient_mobile, notification_type, scheduled_at,
        jc_bookings (travel_operator_pnr, origin, destination, service_number)
      `)
      .eq('status', 'pending')
      .order('scheduled_at', { ascending: true })
      .limit(10)

    // Get repeated vs new customers
    // NOTE: Supabase default limit is 1000, we need to explicitly set higher limit
    const { data: customersData } = await supabase
      .from('jc_passengers')
      .select('mobile')
      .gte('created_at', `${startDate}T00:00:00`)
      .lt('created_at', `${endDate}T00:00:00`)
      .range(0, 49999)

    // Count unique mobiles and their frequency
    const mobileCount: Record<string, number> = {}
    customersData?.forEach((p: any) => {
      if (p.mobile) {
        mobileCount[p.mobile] = (mobileCount[p.mobile] || 0) + 1
      }
    })
    const uniqueMobiles = Object.keys(mobileCount)

    // To find repeated customers, we need to check historical data
    // NOTE: Supabase default limit is 1000, we need to explicitly set higher limit
    const { data: historicalPassengers } = await supabase
      .from('jc_passengers')
      .select('mobile')
      .lt('created_at', `${startDate}T00:00:00`)
      .range(0, 49999)

    const historicalMobiles = new Set(historicalPassengers?.map((p: any) => p.mobile) || [])
    const repeatedCustomers = uniqueMobiles.filter(m => historicalMobiles.has(m)).length
    const newCustomers = uniqueMobiles.length - repeatedCustomers

    // Get gender-wise data
    // NOTE: Supabase default limit is 1000, we need to explicitly set higher limit
    const { data: genderData } = await supabase
      .from('jc_passengers')
      .select('gender')
      .gte('created_at', `${startDate}T00:00:00`)
      .lt('created_at', `${endDate}T00:00:00`)
      .range(0, 49999)

    const genderStats = {
      male: genderData?.filter((p: any) => p.gender?.toLowerCase() === 'male' || p.gender?.toLowerCase() === 'm').length || 0,
      female: genderData?.filter((p: any) => p.gender?.toLowerCase() === 'female' || p.gender?.toLowerCase() === 'f').length || 0,
      other: genderData?.filter((p: any) => p.gender && !['male', 'm', 'female', 'f'].includes(p.gender.toLowerCase())).length || 0,
    }

    // Get service-wise booking data (DOJ services)
    const { data: serviceBookings } = await supabase
      .from('jc_bookings')
      .select(`
        service_number, travel_date, total_fare,
        jc_trips (jc_routes (bitla_route_id, route_name, origin, destination))
      `)
      .gte('travel_date', today)
      .order('travel_date', { ascending: true })
      .limit(50)

    // Group by service and calculate stats
    const serviceStats: Record<string, { bookings: number; revenue: number; travelDate: string; routeInfo: any }> = {}
    serviceBookings?.forEach((b: any) => {
      const key = b.service_number || 'Unknown'
      if (!serviceStats[key]) {
        serviceStats[key] = {
          bookings: 0,
          revenue: 0,
          travelDate: b.travel_date,
          routeInfo: b.jc_trips?.jc_routes,
        }
      }
      serviceStats[key].bookings++
      serviceStats[key].revenue += b.total_fare || 0
    })

    // Get services with no recent bookings (potential services to monitor)
    const { data: allRoutes } = await supabase
      .from('jc_routes')
      .select('id, bitla_route_id, route_name, service_number, origin, destination')
      .limit(20)

    const activeServices = new Set(Object.keys(serviceStats))
    const inactiveServices = allRoutes?.filter((r: any) =>
      r.service_number && !activeServices.has(r.service_number)
    ) || []

    // Get booking trend data for chart (hourly for today, daily for longer ranges)
    // NOTE: Supabase default limit is 1000, we need to explicitly set higher limit
    const { data: bookingTrendData } = await supabase
      .from('jc_bookings')
      .select('created_at, total_fare')
      .gte('created_at', `${startDate}T00:00:00`)
      .lt('created_at', `${endDate}T00:00:00`)
      .order('created_at', { ascending: true })
      .range(0, 49999)

    // Process trend data
    let bookingTrend: { name: string; bookings: number; revenue: number }[] = []
    if (range === 'today' || range === 'yesterday') {
      // Hourly breakdown
      const hourlyData: Record<string, { bookings: number; revenue: number }> = {}
      for (let h = 6; h <= 23; h++) {
        const hourKey = `${h}:00`
        hourlyData[hourKey] = { bookings: 0, revenue: 0 }
      }
      bookingTrendData?.forEach((b: any) => {
        const hour = new Date(b.created_at).getHours()
        if (hour >= 6) {
          const hourKey = `${hour}:00`
          if (hourlyData[hourKey]) {
            hourlyData[hourKey].bookings++
            hourlyData[hourKey].revenue += b.total_fare || 0
          }
        }
      })
      bookingTrend = Object.entries(hourlyData).map(([name, data]) => ({
        name,
        bookings: data.bookings,
        revenue: data.revenue,
      }))
    } else {
      // Daily breakdown
      const dailyData: Record<string, { bookings: number; revenue: number }> = {}
      bookingTrendData?.forEach((b: any) => {
        const dateKey = new Date(b.created_at).toLocaleDateString('en-IN', { month: 'short', day: 'numeric' })
        if (!dailyData[dateKey]) {
          dailyData[dateKey] = { bookings: 0, revenue: 0 }
        }
        dailyData[dateKey].bookings++
        dailyData[dateKey].revenue += b.total_fare || 0
      })
      bookingTrend = Object.entries(dailyData).map(([name, data]) => ({
        name,
        bookings: data.bookings,
        revenue: data.revenue,
      }))
    }

    // P&L Analysis - Calculate expenses (mock for now, can be updated with actual expense data)
    const grossRevenue = rangeRevenue
    const estimatedExpenses = Math.round(grossRevenue * 0.7) // Placeholder: 70% of revenue as expenses
    const netProfit = grossRevenue - estimatedExpenses
    const profitMargin = grossRevenue > 0 ? Math.round((netProfit / grossRevenue) * 100) : 0

    return NextResponse.json({
      bookings: {
        today: rangeBookings || 0,
        yesterday: yesterdayBookings || 0,
        change: bookingChange,
      },
      passengers: {
        today: rangePassengers || 0,
        male: genderStats.male,
        female: genderStats.female,
        other: genderStats.other,
      },
      trips: {
        active: activeTrips || 0,
      },
      notifications: {
        pending: pendingNotifications || 0,
        sent: sentNotifications || 0,
        delivered: deliveredNotifications || 0,
        failed: failedNotifications || 0,
        deliveryRate,
      },
      webhooks: {
        total: webhookEvents || 0,
        processed: processedWebhooks || 0,
      },
      cancellations: {
        today: rangeCancellations || 0,
      },
      revenue: {
        today: rangeRevenue,
        gst: rangeGST,
        gross: grossRevenue,
        expenses: estimatedExpenses,
        net: netProfit,
        margin: profitMargin,
      },
      customers: {
        new: newCustomers,
        repeated: repeatedCustomers,
        total: uniqueMobiles.length,
      },
      pendingTickets: pendingTickets || [],
      serviceStats: Object.entries(serviceStats).map(([service, data]) => ({
        service,
        ...data,
      })),
      inactiveServices: inactiveServices || [],
      bookingTrend,
    })
  } catch (error: any) {
    console.error('Dashboard stats error:', error)
    return NextResponse.json(
      { error: error.message },
      { status: 500 }
    )
  }
}
