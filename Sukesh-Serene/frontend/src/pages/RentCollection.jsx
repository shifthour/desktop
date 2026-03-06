import React, { useState, useEffect } from 'react';
import { Plus, Edit, Trash2, RefreshCw, Download } from 'lucide-react';
import { rentAPI, tenantsAPI } from '../services/api';

const RentCollection = () => {
  const [payments, setPayments] = useState([]);
  const [tenants, setTenants] = useState([]);
  const [loading, setLoading] = useState(true);
  const [showModal, setShowModal] = useState(false);
  const [selectedMonth, setSelectedMonth] = useState('');
  const [selectedYear, setSelectedYear] = useState('');
  const [formData, setFormData] = useState({
    tenant_id: '',
    bed_id: '',
    month: '',
    year: '',
    rent_amount: '',
    paid_amount: '',
    payment_date: '',
    payment_mode: 'UPI',
    payment_account: '',
    remarks: '',
  });
  const [editingId, setEditingId] = useState(null);

  const months = [
    'January', 'February', 'March', 'April', 'May', 'June',
    'July', 'August', 'September', 'October', 'November', 'December'
  ];

  const currentDate = new Date();
  const currentMonth = months[currentDate.getMonth()];
  const currentYear = currentDate.getFullYear();

  useEffect(() => {
    setSelectedMonth(currentMonth);
    setSelectedYear(currentYear.toString());
    fetchTenants();
  }, []);

  useEffect(() => {
    if (selectedMonth && selectedYear) {
      fetchPayments();
    }
  }, [selectedMonth, selectedYear]);

  const fetchPayments = async () => {
    try {
      const response = await rentAPI.getAll(selectedMonth, selectedYear);
      setPayments(response.data);
    } catch (error) {
      console.error('Error fetching payments:', error);
    } finally {
      setLoading(false);
    }
  };

  const fetchTenants = async () => {
    try {
      const response = await tenantsAPI.getAll();
      setTenants(response.data);
    } catch (error) {
      console.error('Error fetching tenants:', error);
    }
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    try {
      const data = {
        ...formData,
        rent_amount: parseFloat(formData.rent_amount),
        paid_amount: parseFloat(formData.paid_amount),
      };

      if (editingId) {
        await rentAPI.update(editingId, data);
      } else {
        await rentAPI.create(data);
      }

      setShowModal(false);
      resetForm();
      fetchPayments();
    } catch (error) {
      alert('Error saving payment: ' + (error.response?.data?.error || error.message));
    }
  };

  const handleDelete = async (id) => {
    if (window.confirm('Are you sure you want to delete this payment?')) {
      try {
        await rentAPI.delete(id);
        fetchPayments();
      } catch (error) {
        alert('Error deleting payment: ' + (error.response?.data?.error || error.message));
      }
    }
  };

  const handleEdit = (payment) => {
    setFormData({
      tenant_id: payment.tenant_id,
      bed_id: payment.bed_id,
      month: payment.month,
      year: payment.year.toString(),
      rent_amount: payment.rent_amount,
      paid_amount: payment.paid_amount,
      payment_date: payment.payment_date || '',
      payment_mode: payment.payment_mode || 'UPI',
      payment_account: payment.payment_account || '',
      remarks: payment.remarks || '',
    });
    setEditingId(payment.id);
    setShowModal(true);
  };

  const handleGenerateRent = async () => {
    if (!selectedMonth || !selectedYear) {
      alert('Please select month and year');
      return;
    }

    if (window.confirm(`Generate rent for ${selectedMonth} ${selectedYear}?`)) {
      try {
        const response = await rentAPI.generateMonthly(selectedMonth, selectedYear);
        alert(response.data.message);
        fetchPayments();
      } catch (error) {
        alert('Error generating rent: ' + (error.response?.data?.error || error.message));
      }
    }
  };

  const resetForm = () => {
    setFormData({
      tenant_id: '',
      bed_id: '',
      month: selectedMonth,
      year: selectedYear,
      rent_amount: '',
      paid_amount: '',
      payment_date: '',
      payment_mode: 'UPI',
      payment_account: '',
      remarks: '',
    });
    setEditingId(null);
  };

  const handleTenantChange = (tenantId) => {
    const tenant = tenants.find(t => t.id === parseInt(tenantId));
    if (tenant) {
      setFormData({
        ...formData,
        tenant_id: tenantId,
        bed_id: tenant.bed_id,
        rent_amount: tenant.rent_amount || '',
      });
    }
  };

  const totalRent = payments.reduce((sum, p) => sum + (p.rent_amount || 0), 0);
  const totalCollected = payments.reduce((sum, p) => sum + (p.paid_amount || 0), 0);
  const totalPending = payments.reduce((sum, p) => sum + (p.balance || 0), 0);

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <h2 className="text-2xl font-bold text-gray-900">Rent Collection</h2>
          <p className="text-gray-500 mt-1">Track monthly rent payments</p>
        </div>
        <div className="flex space-x-3">
          <button
            onClick={handleGenerateRent}
            className="btn-secondary flex items-center space-x-2"
          >
            <RefreshCw size={20} />
            <span>Generate Rent</span>
          </button>
          <button
            onClick={() => {
              resetForm();
              setShowModal(true);
            }}
            className="btn-primary flex items-center space-x-2"
          >
            <Plus size={20} />
            <span>Add Payment</span>
          </button>
        </div>
      </div>

      {/* Filters */}
      <div className="card">
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Month
            </label>
            <select
              value={selectedMonth}
              onChange={(e) => setSelectedMonth(e.target.value)}
              className="input-field"
            >
              {months.map((month) => (
                <option key={month} value={month}>{month}</option>
              ))}
            </select>
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Year
            </label>
            <select
              value={selectedYear}
              onChange={(e) => setSelectedYear(e.target.value)}
              className="input-field"
            >
              {[currentYear - 1, currentYear, currentYear + 1].map((year) => (
                <option key={year} value={year}>{year}</option>
              ))}
            </select>
          </div>
        </div>
      </div>

      {/* Summary Cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        <div className="card">
          <p className="text-sm text-gray-500 font-medium">Total Rent</p>
          <p className="text-3xl font-bold text-gray-900 mt-2">
            ₹{totalRent.toLocaleString()}
          </p>
        </div>
        <div className="card">
          <p className="text-sm text-gray-500 font-medium">Collected</p>
          <p className="text-3xl font-bold text-green-600 mt-2">
            ₹{totalCollected.toLocaleString()}
          </p>
        </div>
        <div className="card">
          <p className="text-sm text-gray-500 font-medium">Pending</p>
          <p className="text-3xl font-bold text-red-600 mt-2">
            ₹{totalPending.toLocaleString()}
          </p>
        </div>
      </div>

      {/* Payments Table */}
      {loading ? (
        <div className="flex items-center justify-center h-64">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-gray-900"></div>
        </div>
      ) : (
        <div className="card overflow-x-auto">
          <table className="min-w-full">
            <thead>
              <tr className="table-header">
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Room</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Tenant</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Mobile</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Rent</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Paid</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Balance</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Payment Date</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Mode</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200">
              {payments.map((payment) => (
                <tr key={payment.id} className="table-row">
                  <td className="px-4 py-3 text-sm text-gray-900">
                    {payment.room_no}-{payment.bed_no}
                  </td>
                  <td className="px-4 py-3 text-sm font-medium text-gray-900">
                    {payment.tenant_name}
                  </td>
                  <td className="px-4 py-3 text-sm text-gray-900">
                    {payment.tenant_mobile}
                  </td>
                  <td className="px-4 py-3 text-sm text-gray-900">
                    ₹{payment.rent_amount?.toLocaleString()}
                  </td>
                  <td className="px-4 py-3 text-sm text-green-600 font-medium">
                    ₹{payment.paid_amount?.toLocaleString()}
                  </td>
                  <td className="px-4 py-3 text-sm text-red-600 font-medium">
                    ₹{payment.balance?.toLocaleString()}
                  </td>
                  <td className="px-4 py-3 text-sm text-gray-900">
                    {payment.payment_date
                      ? new Date(payment.payment_date).toLocaleDateString()
                      : '-'
                    }
                  </td>
                  <td className="px-4 py-3 text-sm text-gray-900">
                    {payment.payment_mode || '-'}
                  </td>
                  <td className="px-4 py-3 text-sm">
                    <div className="flex space-x-2">
                      <button
                        onClick={() => handleEdit(payment)}
                        className="p-1.5 text-gray-600 hover:bg-gray-100 rounded"
                        title="Edit"
                      >
                        <Edit size={16} />
                      </button>
                      <button
                        onClick={() => handleDelete(payment.id)}
                        className="p-1.5 text-red-600 hover:bg-red-50 rounded"
                        title="Delete"
                      >
                        <Trash2 size={16} />
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {payments.length === 0 && (
            <div className="text-center py-12 text-gray-500">
              No payments found for {selectedMonth} {selectedYear}.
              Click "Generate Rent" to create entries for active tenants.
            </div>
          )}
        </div>
      )}

      {/* Add/Edit Modal */}
      {showModal && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-lg p-6 max-w-2xl w-full max-h-[90vh] overflow-y-auto">
            <h3 className="text-xl font-bold text-gray-900 mb-4">
              {editingId ? 'Edit Payment' : 'Add New Payment'}
            </h3>
            <form onSubmit={handleSubmit} className="space-y-4">
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Tenant *
                  </label>
                  <select
                    value={formData.tenant_id}
                    onChange={(e) => handleTenantChange(e.target.value)}
                    className="input-field"
                    required
                    disabled={editingId}
                  >
                    <option value="">Select Tenant</option>
                    {tenants.map((tenant) => (
                      <option key={tenant.id} value={tenant.id}>
                        {tenant.name} - Room {tenant.room_no}-{tenant.bed_no}
                      </option>
                    ))}
                  </select>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Month *
                  </label>
                  <select
                    value={formData.month}
                    onChange={(e) => setFormData({ ...formData, month: e.target.value })}
                    className="input-field"
                    required
                  >
                    {months.map((month) => (
                      <option key={month} value={month}>{month}</option>
                    ))}
                  </select>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Year *
                  </label>
                  <select
                    value={formData.year}
                    onChange={(e) => setFormData({ ...formData, year: e.target.value })}
                    className="input-field"
                    required
                  >
                    {[currentYear - 1, currentYear, currentYear + 1].map((year) => (
                      <option key={year} value={year}>{year}</option>
                    ))}
                  </select>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Rent Amount *
                  </label>
                  <input
                    type="number"
                    value={formData.rent_amount}
                    onChange={(e) => setFormData({ ...formData, rent_amount: e.target.value })}
                    className="input-field"
                    required
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Paid Amount *
                  </label>
                  <input
                    type="number"
                    value={formData.paid_amount}
                    onChange={(e) => setFormData({ ...formData, paid_amount: e.target.value })}
                    className="input-field"
                    required
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Payment Date
                  </label>
                  <input
                    type="date"
                    value={formData.payment_date}
                    onChange={(e) => setFormData({ ...formData, payment_date: e.target.value })}
                    className="input-field"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Payment Mode
                  </label>
                  <select
                    value={formData.payment_mode}
                    onChange={(e) => setFormData({ ...formData, payment_mode: e.target.value })}
                    className="input-field"
                  >
                    <option value="Cash">Cash</option>
                    <option value="UPI">UPI</option>
                    <option value="Bank Transfer">Bank Transfer</option>
                    <option value="Cheque">Cheque</option>
                  </select>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Payment Account
                  </label>
                  <input
                    type="text"
                    value={formData.payment_account}
                    onChange={(e) => setFormData({ ...formData, payment_account: e.target.value })}
                    className="input-field"
                  />
                </div>
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Remarks
                </label>
                <textarea
                  value={formData.remarks}
                  onChange={(e) => setFormData({ ...formData, remarks: e.target.value })}
                  className="input-field"
                  rows="2"
                ></textarea>
              </div>

              <div className="flex space-x-3 pt-4">
                <button type="submit" className="btn-primary flex-1">
                  {editingId ? 'Update' : 'Create'}
                </button>
                <button
                  type="button"
                  onClick={() => {
                    setShowModal(false);
                    resetForm();
                  }}
                  className="btn-secondary flex-1"
                >
                  Cancel
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
};

export default RentCollection;
