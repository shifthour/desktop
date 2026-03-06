import supabase from '../config/database.js';

export const getDashboardStats = async (req, res) => {
  try {
    const { pg_id, month, year } = req.query;

    // Use provided month/year or default to current
    const currentDate = new Date();
    const selectedMonth = month || currentDate.toLocaleString('default', { month: 'long' });
    const selectedYear = year ? parseInt(year) : currentDate.getFullYear();

    // Room statistics
    let roomsQuery = supabase
      .from('serene_rooms')
      .select('*');

    if (pg_id) {
      roomsQuery = roomsQuery.eq('pg_id', parseInt(pg_id));
    }

    const { data: rooms, error: roomsError } = await roomsQuery;
    if (roomsError) throw roomsError;

    const roomStats = {
      total_rooms: rooms.length,
      total_capacity: rooms.reduce((sum, room) => sum + (room.total_beds || 0), 0),
      occupied_rooms: rooms.filter(room => room.status === 'occupied').length,
      available_rooms: rooms.filter(room => room.status === 'available').length
    };

    // Tenant statistics - get all tenants with their rooms
    let tenantsQuery = supabase
      .from('serene_tenants')
      .select('*, serene_rooms!left(id, pg_id)');

    const { data: allTenants, error: tenantsError } = await tenantsQuery;
    if (tenantsError) throw tenantsError;

    // Filter by pg_id if specified
    let tenants = allTenants;
    if (pg_id) {
      tenants = allTenants.filter(t => t.serene_rooms?.pg_id === parseInt(pg_id));
    }

    const tenantStats = {
      active_tenants: tenants.filter(t => t.status === 'active').length,
      inactive_tenants: tenants.filter(t => t.status === 'inactive').length
    };

    // Payment statistics for selected month
    let paymentsQuery = supabase
      .from('serene_payments')
      .select('*, serene_tenants!inner(id, room_id, serene_rooms!inner(id, pg_id))')
      .eq('month', selectedMonth)
      .eq('year', selectedYear);

    if (pg_id) {
      paymentsQuery = paymentsQuery.eq('serene_tenants.serene_rooms.pg_id', parseInt(pg_id));
    }

    const { data: payments, error: paymentsError } = await paymentsQuery;
    if (paymentsError) throw paymentsError;

    const rentStats = {
      total_rent_collected: payments
        .filter(p => p.payment_type === 'rent')
        .reduce((sum, p) => sum + (p.amount || 0), 0),
      total_deposits: payments
        .filter(p => p.payment_type === 'deposit' || p.payment_type === 'advance')
        .reduce((sum, p) => sum + (p.amount || 0), 0),
      total_collected: payments.reduce((sum, p) => sum + (p.amount || 0), 0),
      payment_count: payments.length
    };

    // Total rent = sum of monthly_rent from all active tenants (occupied beds)
    const activeTenants = tenants.filter(t => t.status === 'active');
    const totalExpectedFromActiveTenants = activeTenants.reduce((sum, t) => sum + (t.monthly_rent || 0), 0);

    const expectedRent = {
      total_expected: totalExpectedFromActiveTenants
    };

    // Recent payments with joins
    let recentPaymentsQuery = supabase
      .from('serene_payments')
      .select(`
        *,
        serene_tenants!inner(id, name, room_id, serene_rooms(room_number))
      `)
      .order('payment_date', { ascending: false })
      .limit(10);

    if (pg_id) {
      recentPaymentsQuery = recentPaymentsQuery.eq('serene_tenants.serene_rooms.pg_id', parseInt(pg_id));
    }

    const { data: recentPaymentsData, error: recentPaymentsError } = await recentPaymentsQuery;
    if (recentPaymentsError) throw recentPaymentsError;

    // Flatten recent payments data
    const recentPayments = recentPaymentsData.map(p => ({
      ...p,
      tenant_name: p.serene_tenants?.name,
      room_number: p.serene_tenants?.serene_rooms?.room_number,
      serene_tenants: undefined
    }));

    // Recent tenants with joins
    let recentTenantsQuery = supabase
      .from('serene_tenants')
      .select('*, serene_rooms!left(room_number, pg_id)')
      .eq('status', 'active')
      .order('check_in_date', { ascending: false })
      .limit(5);

    if (pg_id) {
      recentTenantsQuery = recentTenantsQuery.eq('serene_rooms.pg_id', parseInt(pg_id));
    }

    const { data: recentTenantsData, error: recentTenantsError } = await recentTenantsQuery;
    if (recentTenantsError) throw recentTenantsError;

    // Flatten recent tenants data
    const recentTenants = recentTenantsData.map(t => ({
      ...t,
      room_number: t.serene_rooms?.room_number,
      serene_rooms: undefined
    }));

    // Calculate occupied beds (count of active tenants)
    let occupiedBedsQuery = supabase
      .from('serene_tenants')
      .select('id, serene_rooms!inner(id, pg_id)', { count: 'exact', head: false })
      .eq('status', 'active');

    if (pg_id) {
      occupiedBedsQuery = occupiedBedsQuery.eq('serene_rooms.pg_id', parseInt(pg_id));
    }

    const { data: occupiedBedsData, error: occupiedBedsError } = await occupiedBedsQuery;
    if (occupiedBedsError) throw occupiedBedsError;

    const occupiedBeds = {
      count: occupiedBedsData.length
    };

    const occupancyRate = roomStats.total_capacity > 0
      ? ((occupiedBeds.count / roomStats.total_capacity) * 100).toFixed(1)
      : 0;

    const collectionRate = expectedRent.total_expected > 0
      ? ((rentStats.total_rent_collected / expectedRent.total_expected) * 100).toFixed(1)
      : 0;

    // Expense statistics for selected month
    const months = [
      'January', 'February', 'March', 'April', 'May', 'June',
      'July', 'August', 'September', 'October', 'November', 'December'
    ];
    const monthIndex = months.indexOf(selectedMonth) + 1;

    // For expenses, we need to filter by date using PostgreSQL date functions
    // We'll use RPC or filter in JavaScript
    let expensesQuery = supabase
      .from('serene_expenses')
      .select('amount, expense_date');

    if (pg_id) {
      expensesQuery = expensesQuery.eq('pg_id', parseInt(pg_id));
    }

    const { data: allExpenses, error: expensesError } = await expensesQuery;
    if (expensesError) throw expensesError;

    // Filter expenses by month and year in JavaScript
    const filteredExpenses = allExpenses.filter(expense => {
      const expenseDate = new Date(expense.expense_date);
      return expenseDate.getFullYear() === selectedYear &&
             (expenseDate.getMonth() + 1) === monthIndex;
    });

    const expenseStats = {
      total_expenses: filteredExpenses.reduce((sum, e) => sum + (e.amount || 0), 0),
      expense_count: filteredExpenses.length
    };

    // Calculate profit (total rent collected - total expenses)
    const profit = rentStats.total_rent_collected - expenseStats.total_expenses;

    // Calculate correct values:
    // Total Rent = Expected rent from all active tenants (sum of monthly_rent)
    // Collected = Actually collected rent payments
    // Pending = Total Rent - Collected
    // Net Profit = Collected - Pending - Expenses
    const totalExpectedRent = expectedRent.total_expected;
    const collectedRent = rentStats.total_rent_collected;
    const pendingRent = Math.max(0, totalExpectedRent - collectedRent);
    const netProfit = collectedRent - pendingRent - expenseStats.total_expenses;

    res.json({
      roomStats: {
        ...roomStats,
        total_beds: roomStats.total_capacity,
        occupied_beds: occupiedBeds.count,
        vacant_beds: (roomStats.total_capacity || 0) - occupiedBeds.count,
        occupancyRate: parseFloat(occupancyRate)
      },
      tenantStats,
      rentStats: {
        ...rentStats,
        total_rent: totalExpectedRent,
        collected_rent: collectedRent,
        pending_rent: pendingRent,
        collectionRate: parseFloat(collectionRate),
        month: selectedMonth,
        year: selectedYear
      },
      expenseStats: {
        total_expenses: expenseStats.total_expenses,
        expense_count: expenseStats.expense_count,
        month: selectedMonth,
        year: selectedYear
      },
      profitStats: {
        profit: netProfit,
        month: selectedMonth,
        year: selectedYear
      },
      recentPayments,
      recentTenants
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

export const getMonthlyReport = async (req, res) => {
  try {
    const { month, year } = req.query;

    // Get payments with tenant and room information
    const { data: paymentsData, error: paymentsError } = await supabase
      .from('serene_payments')
      .select(`
        *,
        serene_tenants!inner(id, name, phone, room_id, serene_rooms(room_number))
      `)
      .eq('month', month)
      .eq('year', parseInt(year))
      .order('payment_date', { ascending: false });

    if (paymentsError) throw paymentsError;

    // Flatten payments data
    const payments = paymentsData.map(p => ({
      ...p,
      tenant_name: p.serene_tenants?.name,
      tenant_phone: p.serene_tenants?.phone,
      room_number: p.serene_tenants?.serene_rooms?.room_number,
      serene_tenants: undefined
    }));

    // Calculate summary statistics in JavaScript
    const summary = {
      total_rent: payments
        .filter(p => p.payment_type === 'rent')
        .reduce((sum, p) => sum + (p.amount || 0), 0),
      total_deposits: payments
        .filter(p => p.payment_type === 'deposit' || p.payment_type === 'advance')
        .reduce((sum, p) => sum + (p.amount || 0), 0),
      total_collected: payments.reduce((sum, p) => sum + (p.amount || 0), 0),
      payment_count: payments.length
    };

    res.json({
      month,
      year,
      summary,
      payments
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};
