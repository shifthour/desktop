import React, { useState, useEffect } from 'react';
import { Home, Users, TrendingUp, Bed, Building2, Receipt, TrendingDown, Download } from 'lucide-react';
import { dashboardAPI, pgAPI, roomsAPI, tenantsAPI, rentAPI } from '../services/api';
import * as XLSX from 'xlsx';
import axios from 'axios';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5000/api';

const Dashboard = () => {
  const [stats, setStats] = useState(null);
  const [loading, setLoading] = useState(true);
  const [pgs, setPgs] = useState([]);
  const [selectedPgId, setSelectedPgId] = useState('');

  const currentDate = new Date();
  const months = [
    'January', 'February', 'March', 'April', 'May', 'June',
    'July', 'August', 'September', 'October', 'November', 'December'
  ];
  const currentMonth = months[currentDate.getMonth()];
  const currentYear = currentDate.getFullYear();

  const [selectedMonth, setSelectedMonth] = useState(currentMonth);
  const [selectedYear, setSelectedYear] = useState(currentYear.toString());

  useEffect(() => {
    fetchPGs();
    fetchStats();
  }, []);

  useEffect(() => {
    fetchStats();
  }, [selectedPgId, selectedMonth, selectedYear]);

  const fetchPGs = async () => {
    try {
      const response = await pgAPI.getAll();
      setPgs(response.data);
    } catch (error) {
      console.error('Error fetching PGs:', error);
    }
  };

  const fetchStats = async () => {
    try {
      setLoading(true);
      const params = {
        ...(selectedPgId && { pg_id: selectedPgId }),
        month: selectedMonth,
        year: selectedYear
      };
      const response = await dashboardAPI.getStats(params);
      setStats(response.data);
    } catch (error) {
      console.error('Error fetching stats:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleExportData = async () => {
    try {
      setLoading(true);
      const token = localStorage.getItem('token');

      // Fetch all data in parallel
      const [
        pgResponse,
        roomsResponse,
        tenantsResponse,
        paymentsResponse,
        expensesResponse,
        paymentSubmissionsResponse
      ] = await Promise.all([
        pgAPI.getAll(),
        roomsAPI.getAll(),
        tenantsAPI.getAll(),
        rentAPI.getAll(),
        axios.get(`${API_URL}/expenses`, {
          headers: { Authorization: `Bearer ${token}` }
        }),
        axios.get(`${API_URL}/payment-submissions`, {
          headers: { Authorization: `Bearer ${token}` }
        })
      ]);

      // Create workbook
      const wb = XLSX.utils.book_new();

      // PG Properties Sheet
      const pgData = pgResponse.data.map(pg => ({
        'PG Name': pg.pg_name,
        'Location': pg.location,
        'Total Rooms': pg.total_rooms,
        'Contact': pg.contact_number,
        'Manager': pg.manager_name,
        'Address': pg.address
      }));
      const pgSheet = XLSX.utils.json_to_sheet(pgData);
      XLSX.utils.book_append_sheet(wb, pgSheet, 'PG Properties');

      // Rooms Sheet
      const roomsData = roomsResponse.data.map(room => ({
        'PG Name': room.pg_name,
        'Room Number': room.room_number,
        'Floor': room.floor,
        'Total Beds': room.total_beds,
        'Occupied Beds': room.occupied_count || 0,
        'Vacant Beds': (room.total_beds - (room.occupied_count || 0)),
        'Rent Per Bed': room.rent_per_bed,
        'Room Type': room.room_type,
        'AC/Non-AC': room.ac_non_ac
      }));
      const roomsSheet = XLSX.utils.json_to_sheet(roomsData);
      XLSX.utils.book_append_sheet(wb, roomsSheet, 'Rooms');

      // Tenants Sheet
      const tenantsData = tenantsResponse.data.map(tenant => ({
        'Name': tenant.name,
        'Email': tenant.email,
        'Phone': tenant.phone_number,
        'PG Name': tenant.pg_name,
        'Room Number': tenant.room_number,
        'Bed Number': tenant.bed_number,
        'Monthly Rent': tenant.monthly_rent,
        'Security Deposit': tenant.security_deposit,
        'Check-in Date': tenant.check_in_date ? new Date(tenant.check_in_date).toLocaleDateString() : '-',
        'Status': tenant.status,
        'Remarks': tenant.remarks || '-'
      }));
      const tenantsSheet = XLSX.utils.json_to_sheet(tenantsData);
      XLSX.utils.book_append_sheet(wb, tenantsSheet, 'Tenants');

      // Payments Sheet
      const paymentsData = paymentsResponse.data.map(payment => ({
        'Tenant Name': payment.tenant_name,
        'Room Number': payment.room_number,
        'Payment Type': payment.payment_type,
        'Amount': payment.amount,
        'Payment Date': payment.payment_date ? new Date(payment.payment_date).toLocaleDateString() : '-',
        'Payment Method': payment.payment_method,
        'Month': payment.month,
        'Year': payment.year,
        'Description': payment.description || '-'
      }));
      const paymentsSheet = XLSX.utils.json_to_sheet(paymentsData);
      XLSX.utils.book_append_sheet(wb, paymentsSheet, 'Payments');

      // Payment Review Sheet
      const paymentSubmissionsData = paymentSubmissionsResponse.data.map(submission => ({
        'Tenant Name': submission.tenant_name,
        'Room Number': submission.room_number,
        'PG Name': submission.pg_name,
        'Month': submission.month,
        'Year': submission.year,
        'Amount': submission.amount,
        'Payment Method': submission.payment_method || 'UPI',
        'Paid To': submission.paid_to || '-',
        'Submission Date': submission.submission_date ? new Date(submission.submission_date).toLocaleDateString() : '-',
        'Status': submission.status,
        'Reviewed At': submission.reviewed_at ? new Date(submission.reviewed_at).toLocaleDateString() : '-'
      }));
      const submissionsSheet = XLSX.utils.json_to_sheet(paymentSubmissionsData);
      XLSX.utils.book_append_sheet(wb, submissionsSheet, 'Payment Review');

      // Expenses Sheet
      const expensesData = expensesResponse.data.map(expense => ({
        'PG Name': expense.pg_name,
        'Category': expense.category,
        'Description': expense.description,
        'Amount': expense.amount,
        'Expense Date': expense.expense_date ? new Date(expense.expense_date).toLocaleDateString() : '-',
        'Payment Method': expense.payment_method || '-'
      }));
      const expensesSheet = XLSX.utils.json_to_sheet(expensesData);
      XLSX.utils.book_append_sheet(wb, expensesSheet, 'Expenses');

      // Generate file name with current date
      const fileName = `Serene_Living_Data_${new Date().toISOString().split('T')[0]}.xlsx`;

      // Download file
      XLSX.writeFile(wb, fileName);

      alert('Data exported successfully!');
    } catch (error) {
      console.error('Error exporting data:', error);
      alert('Error exporting data: ' + (error.response?.data?.error || error.message));
    } finally {
      setLoading(false);
    }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-gray-900"></div>
      </div>
    );
  }

  const statCards = [
    {
      title: 'Total Rooms',
      value: stats?.roomStats?.total_rooms || 0,
      icon: Home,
      color: 'bg-blue-500',
    },
    {
      title: 'Total Beds',
      value: stats?.roomStats?.total_beds || 0,
      icon: Bed,
      color: 'bg-purple-500',
    },
    {
      title: 'Occupied Beds',
      value: stats?.roomStats?.occupied_beds || 0,
      icon: Users,
      color: 'bg-green-500',
    },
    {
      title: 'Vacant Beds',
      value: stats?.roomStats?.vacant_beds || 0,
      icon: TrendingUp,
      color: 'bg-yellow-500',
    },
  ];

  const rentCards = [
    {
      title: 'Total Rent',
      value: `₹${(stats?.rentStats?.total_rent || 0).toLocaleString()}`,
      color: 'bg-indigo-500',
    },
    {
      title: 'Collected',
      value: `₹${(stats?.rentStats?.collected_rent || 0).toLocaleString()}`,
      color: 'bg-green-500',
    },
    {
      title: 'Pending',
      value: `₹${(stats?.rentStats?.pending_rent || 0).toLocaleString()}`,
      color: 'bg-red-500',
    },
    {
      title: 'Total Expenses',
      value: `₹${(stats?.expenseStats?.total_expenses || 0).toLocaleString()}`,
      color: 'bg-orange-500',
      icon: Receipt,
    },
    {
      title: 'Net Profit',
      value: `₹${(stats?.profitStats?.profit || 0).toLocaleString()}`,
      color: stats?.profitStats?.profit >= 0 ? 'bg-teal-500' : 'bg-red-500',
      icon: stats?.profitStats?.profit >= 0 ? TrendingUp : TrendingDown,
    },
  ];

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <h2 className="text-2xl font-bold text-gray-900">Dashboard</h2>
          <p className="text-gray-500 mt-1">Welcome to Serene Living PG Management</p>
        </div>
        <div className="flex items-center space-x-3">
          <button
            onClick={handleExportData}
            disabled={loading}
            className="btn-primary flex items-center space-x-2"
          >
            <Download size={18} />
            <span>Export Data</span>
          </button>

          <Building2 size={20} className="text-gray-500" />
          <select
            value={selectedPgId}
            onChange={(e) => setSelectedPgId(e.target.value)}
            className="input-field min-w-[200px]"
          >
            <option value="">All PG Properties</option>
            {pgs.map((pg) => (
              <option key={pg.id} value={pg.id}>
                {pg.pg_name}
              </option>
            ))}
          </select>

          <select
            value={selectedMonth}
            onChange={(e) => setSelectedMonth(e.target.value)}
            className="input-field min-w-[140px]"
          >
            {months.map((month) => (
              <option key={month} value={month}>{month}</option>
            ))}
          </select>

          <select
            value={selectedYear}
            onChange={(e) => setSelectedYear(e.target.value)}
            className="input-field min-w-[100px]"
          >
            {[currentYear - 1, currentYear, currentYear + 1].map((year) => (
              <option key={year} value={year}>{year}</option>
            ))}
          </select>
        </div>
      </div>

      {/* Statistics Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        {statCards.map((stat, index) => {
          const Icon = stat.icon;
          return (
            <div key={index} className="card">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm text-gray-500 font-medium">{stat.title}</p>
                  <p className="text-3xl font-bold text-gray-900 mt-2">{stat.value}</p>
                </div>
                <div className={`${stat.color} w-12 h-12 rounded-lg flex items-center justify-center`}>
                  <Icon className="text-white" size={24} />
                </div>
              </div>
            </div>
          );
        })}
      </div>

      {/* Occupancy Rate */}
      <div className="card">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">Occupancy Rate</h3>
        <div className="flex items-center space-x-4">
          <div className="flex-1">
            <div className="w-full bg-gray-200 rounded-full h-4">
              <div
                className="bg-green-500 h-4 rounded-full transition-all duration-300"
                style={{ width: `${stats?.roomStats?.occupancyRate || 0}%` }}
              ></div>
            </div>
          </div>
          <span className="text-2xl font-bold text-gray-900">
            {stats?.roomStats?.occupancyRate || 0}%
          </span>
        </div>
      </div>

      {/* Current Month Rent Collection */}
      <div className="card">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">
          {selectedMonth} {selectedYear} - Financial Overview
        </h3>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-4">
          {rentCards.map((card, index) => {
            const Icon = card.icon;
            return (
              <div key={index} className="text-center p-4 bg-gray-50 rounded-lg">
                {Icon && (
                  <div className="flex justify-center mb-2">
                    <Icon size={20} className="text-gray-600" />
                  </div>
                )}
                <p className="text-sm text-gray-500 font-medium">{card.title}</p>
                <p className={`text-2xl font-bold mt-2 ${
                  card.title === 'Collected' ? 'text-green-600' :
                  card.title === 'Pending' ? 'text-red-600' :
                  card.title === 'Total Expenses' ? 'text-orange-600' :
                  card.title === 'Net Profit' ? (stats?.profitStats?.profit >= 0 ? 'text-teal-600' : 'text-red-600') :
                  'text-gray-900'
                }`}>
                  {card.value}
                </p>
              </div>
            );
          })}
        </div>
        {stats?.rentStats?.total_rent > 0 && (
          <div className="mt-4">
            <div className="flex items-center justify-between text-sm text-gray-600 mb-2">
              <span>Collection Rate</span>
              <span className="font-semibold">{stats?.rentStats?.collectionRate}%</span>
            </div>
            <div className="w-full bg-gray-200 rounded-full h-3">
              <div
                className="bg-green-500 h-3 rounded-full transition-all duration-300"
                style={{ width: `${stats?.rentStats?.collectionRate}%` }}
              ></div>
            </div>
          </div>
        )}
      </div>

      {/* Recent Payments */}
      {stats?.recentPayments && stats.recentPayments.length > 0 && (
        <div className="card">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Recent Payments</h3>
          <div className="overflow-x-auto">
            <table className="min-w-full">
              <thead>
                <tr className="table-header">
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Date</th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Tenant</th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Room</th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Amount</th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Mode</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-200">
                {stats.recentPayments.map((payment, index) => (
                  <tr key={index} className="table-row">
                    <td className="px-4 py-3 text-sm text-gray-900">
                      {payment.payment_date ? new Date(payment.payment_date).toLocaleDateString() : '-'}
                    </td>
                    <td className="px-4 py-3 text-sm text-gray-900">{payment.tenant_name}</td>
                    <td className="px-4 py-3 text-sm text-gray-900">
                      {payment.room_number || '-'}
                    </td>
                    <td className="px-4 py-3 text-sm font-medium text-green-600">
                      ₹{payment.amount?.toLocaleString()}
                    </td>
                    <td className="px-4 py-3 text-sm text-gray-900">{payment.payment_method || '-'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
};

export default Dashboard;
