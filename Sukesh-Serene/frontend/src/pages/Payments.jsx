import React, { useState, useEffect } from 'react';
import { Plus, IndianRupee } from 'lucide-react';
import { rentAPI, tenantsAPI, pgAPI, roomsAPI } from '../services/api';

const Payments = () => {
  const [payments, setPayments] = useState([]);
  const [tenants, setTenants] = useState([]);
  const [pgs, setPgs] = useState([]);
  const [rooms, setRooms] = useState([]);
  const [loading, setLoading] = useState(true);
  const [showModal, setShowModal] = useState(false);
  const [selectedMonth, setSelectedMonth] = useState('');
  const [selectedYear, setSelectedYear] = useState('');
  const [selectedPgId, setSelectedPgId] = useState('');
  const [selectedPaymentType, setSelectedPaymentType] = useState('');
  const [formData, setFormData] = useState({
    tenant_id: '',
    payment_type: 'rent',
    amount: '',
    payment_date: '',
    payment_method: 'UPI',
    month: '',
    year: '',
    description: ''
  });
  const [submitting, setSubmitting] = useState(false);

  // Modal filter states
  const [filterPgId, setFilterPgId] = useState('');
  const [filterRoomId, setFilterRoomId] = useState('');

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
    fetchPGs();
    fetchRooms();
  }, []);

  useEffect(() => {
    if (selectedMonth && selectedYear) {
      fetchPayments();
    }
  }, [selectedMonth, selectedYear, selectedPgId]);

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

  const fetchPGs = async () => {
    try {
      const response = await pgAPI.getAll();
      setPgs(response.data);
    } catch (error) {
      console.error('Error fetching PGs:', error);
    }
  };

  const fetchRooms = async () => {
    try {
      const response = await roomsAPI.getAll();
      setRooms(response.data);
    } catch (error) {
      console.error('Error fetching rooms:', error);
    }
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setSubmitting(true);
    try {
      const data = {
        ...formData,
        amount: parseFloat(formData.amount),
        year: parseInt(formData.year)
      };

      await rentAPI.create(data);

      setShowModal(false);
      resetForm();
      fetchPayments();
    } catch (error) {
      alert('Error saving payment: ' + (error.response?.data?.error || error.message));
    } finally {
      setSubmitting(false);
    }
  };

  const resetForm = () => {
    setFormData({
      tenant_id: '',
      payment_type: 'rent',
      amount: '',
      payment_date: '',
      payment_method: 'UPI',
      month: selectedMonth,
      year: selectedYear,
      description: ''
    });
    setFilterPgId('');
    setFilterRoomId('');
  };

  // Filter rooms based on selected property
  const filteredRooms = rooms.filter(room => {
    if (filterPgId && room.pg_id !== parseInt(filterPgId)) return false;
    return true;
  });

  // Filter tenants based on selected property and room (for modal)
  const filteredTenants = tenants.filter(tenant => {
    if (filterPgId && tenant.pg_id !== parseInt(filterPgId)) return false;
    if (filterRoomId && tenant.room_id !== parseInt(filterRoomId)) return false;
    return true;
  });

  // Filter payments based on selected property and payment type (for main list)
  const filteredPayments = payments.filter(payment => {
    if (selectedPgId) {
      const tenant = tenants.find(t => t.id === payment.tenant_id);
      if (tenant && tenant.pg_id !== parseInt(selectedPgId)) return false;
    }
    if (selectedPaymentType && payment.payment_type !== selectedPaymentType) return false;
    return true;
  });

  const totalAmount = filteredPayments.reduce((sum, p) => sum + (p.amount || 0), 0);

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <h2 className="text-2xl font-bold text-gray-900">Payments</h2>
          <p className="text-gray-500 mt-1">Track rent and other payments</p>
        </div>
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

      {/* Filters */}
      <div className="card">
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Property
            </label>
            <select
              value={selectedPgId}
              onChange={(e) => setSelectedPgId(e.target.value)}
              className="input-field"
            >
              <option value="">All Properties</option>
              {pgs.map((pg) => (
                <option key={pg.id} value={pg.id}>
                  {pg.pg_name}
                </option>
              ))}
            </select>
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Payment Type
            </label>
            <select
              value={selectedPaymentType}
              onChange={(e) => setSelectedPaymentType(e.target.value)}
              className="input-field"
            >
              <option value="">All Types</option>
              <option value="rent">Rent</option>
              <option value="deposit">Deposit</option>
              <option value="advance">Advance</option>
              <option value="maintenance">Maintenance</option>
              <option value="other">Other</option>
            </select>
          </div>
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

      {/* Summary */}
      <div className="card">
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-8">
            <div>
              <p className="text-sm text-gray-500 font-medium">
                {selectedPaymentType
                  ? `Total ${selectedPaymentType.charAt(0).toUpperCase() + selectedPaymentType.slice(1)}`
                  : 'Total Payments'}
              </p>
              <p className="text-3xl font-bold text-gray-900 mt-1">
                ₹{totalAmount.toLocaleString()}
              </p>
            </div>
            <div className="border-l pl-8">
              <p className="text-sm text-gray-500 font-medium">Users Paid</p>
              <p className="text-3xl font-bold text-gray-900 mt-1">
                {new Set(filteredPayments.map(p => p.tenant_id)).size}
              </p>
            </div>
          </div>
          <div className={`p-4 rounded-lg ${
            selectedPaymentType === 'rent' ? 'bg-blue-100' :
            selectedPaymentType === 'deposit' ? 'bg-purple-100' :
            'bg-green-100'
          }`}>
            <IndianRupee className={`${
              selectedPaymentType === 'rent' ? 'text-blue-600' :
              selectedPaymentType === 'deposit' ? 'text-purple-600' :
              'text-green-600'
            }`} size={32} />
          </div>
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
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Date</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Tenant</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Room</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Type</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Amount</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Method</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200">
              {filteredPayments.map((payment) => (
                <tr key={payment.id} className="table-row">
                  <td className="px-4 py-3 text-sm text-gray-900">
                    {payment.payment_date ? new Date(payment.payment_date).toLocaleDateString() : '-'}
                  </td>
                  <td className="px-4 py-3 text-sm font-medium text-gray-900">
                    {payment.tenant_name}
                  </td>
                  <td className="px-4 py-3 text-sm text-gray-900">
                    {payment.room_number || '-'}
                  </td>
                  <td className="px-4 py-3 text-sm">
                    <span className={`px-2 py-1 rounded-full text-xs font-medium ${
                      payment.payment_type === 'rent' ? 'bg-blue-100 text-blue-800' :
                      payment.payment_type === 'deposit' ? 'bg-purple-100 text-purple-800' :
                      'bg-green-100 text-green-800'
                    }`}>
                      {payment.payment_type}
                    </span>
                  </td>
                  <td className="px-4 py-3 text-sm font-medium text-green-600">
                    ₹{payment.amount?.toLocaleString()}
                  </td>
                  <td className="px-4 py-3 text-sm text-gray-900">
                    {payment.payment_method || '-'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {filteredPayments.length === 0 && (
            <div className="text-center py-12">
              <IndianRupee size={48} className="mx-auto text-gray-400 mb-4" />
              <p className="text-gray-500">
                {payments.length === 0
                  ? `No payments for ${selectedMonth} ${selectedYear}`
                  : 'No payments match the selected filters.'}
              </p>
            </div>
          )}

          {/* Total Sum Footer */}
          {filteredPayments.length > 0 && (
            <div className={`mt-4 p-4 rounded-lg border-2 ${
              selectedPaymentType === 'rent' ? 'bg-blue-50 border-blue-200' :
              selectedPaymentType === 'deposit' ? 'bg-purple-50 border-purple-200' :
              'bg-green-50 border-green-200'
            }`}>
              <div className="flex items-center justify-between">
                <div className="flex items-center space-x-3">
                  <IndianRupee className={`${
                    selectedPaymentType === 'rent' ? 'text-blue-600' :
                    selectedPaymentType === 'deposit' ? 'text-purple-600' :
                    'text-green-600'
                  }`} size={24} />
                  <div>
                    <p className="text-sm font-medium text-gray-600">
                      {selectedPaymentType
                        ? `Total ${selectedPaymentType.charAt(0).toUpperCase() + selectedPaymentType.slice(1)} Amount`
                        : 'Total Amount'}
                    </p>
                    <p className="text-xs text-gray-400">
                      {filteredPayments.length} payment{filteredPayments.length !== 1 ? 's' : ''} • {selectedMonth} {selectedYear}
                    </p>
                  </div>
                </div>
                <p className={`text-2xl font-bold ${
                  selectedPaymentType === 'rent' ? 'text-blue-700' :
                  selectedPaymentType === 'deposit' ? 'text-purple-700' :
                  'text-green-700'
                }`}>
                  ₹{totalAmount.toLocaleString()}
                </p>
              </div>
            </div>
          )}
        </div>
      )}

      {/* Add/Edit Modal */}
      {showModal && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-lg p-6 max-w-2xl w-full max-h-[90vh] overflow-y-auto">
            <h3 className="text-xl font-bold text-gray-900 mb-4">
              Add New Payment
            </h3>
            <form onSubmit={handleSubmit} className="space-y-4">
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Property Name
                  </label>
                  <select
                    value={filterPgId}
                    onChange={(e) => {
                      setFilterPgId(e.target.value);
                      setFilterRoomId('');
                      setFormData({ ...formData, tenant_id: '' });
                    }}
                    className="input-field"
                  >
                    <option value="">All Properties</option>
                    {pgs.map((pg) => (
                      <option key={pg.id} value={pg.id}>
                        {pg.pg_name}
                      </option>
                    ))}
                  </select>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Room Number
                  </label>
                  <select
                    value={filterRoomId}
                    onChange={(e) => {
                      setFilterRoomId(e.target.value);
                      setFormData({ ...formData, tenant_id: '' });
                    }}
                    className="input-field"
                  >
                    <option value="">All Rooms</option>
                    {filteredRooms.map((room) => (
                      <option key={room.id} value={room.id}>
                        Room {room.room_number}
                      </option>
                    ))}
                  </select>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Tenant *
                  </label>
                  <select
                    value={formData.tenant_id}
                    onChange={(e) => setFormData({ ...formData, tenant_id: e.target.value })}
                    className="input-field"
                    required
                  >
                    <option value="">Select Tenant</option>
                    {filteredTenants.map((tenant) => (
                      <option key={tenant.id} value={tenant.id}>
                        {tenant.name} - {tenant.room_number || 'No Room'}
                      </option>
                    ))}
                  </select>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Payment Type *
                  </label>
                  <select
                    value={formData.payment_type}
                    onChange={(e) => setFormData({ ...formData, payment_type: e.target.value })}
                    className="input-field"
                    required
                  >
                    <option value="rent">Rent</option>
                    <option value="deposit">Deposit</option>
                    <option value="advance">Advance</option>
                    <option value="maintenance">Maintenance</option>
                    <option value="other">Other</option>
                  </select>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Amount *
                  </label>
                  <input
                    type="number"
                    value={formData.amount}
                    onChange={(e) => setFormData({ ...formData, amount: e.target.value })}
                    className="input-field"
                    required
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Payment Date *
                  </label>
                  <input
                    type="date"
                    value={formData.payment_date}
                    onChange={(e) => setFormData({ ...formData, payment_date: e.target.value })}
                    className="input-field"
                    required
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Payment Method
                  </label>
                  <select
                    value={formData.payment_method}
                    onChange={(e) => setFormData({ ...formData, payment_method: e.target.value })}
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
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Description
                </label>
                <textarea
                  value={formData.description}
                  onChange={(e) => setFormData({ ...formData, description: e.target.value })}
                  className="input-field"
                  rows="2"
                ></textarea>
              </div>

              <div className="flex space-x-3 pt-4">
                <button
                  type="submit"
                  className="btn-primary flex-1 flex items-center justify-center"
                  disabled={submitting}
                >
                  {submitting ? (
                    <>
                      <svg className="animate-spin -ml-1 mr-3 h-5 w-5 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                        <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                        <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                      </svg>
                      Creating...
                    </>
                  ) : (
                    'Create'
                  )}
                </button>
                <button
                  type="button"
                  onClick={() => {
                    setShowModal(false);
                    resetForm();
                  }}
                  className="btn-secondary flex-1"
                  disabled={submitting}
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

export default Payments;
