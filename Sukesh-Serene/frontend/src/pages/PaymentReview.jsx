import React, { useState, useEffect } from 'react';
import { CheckCircle, XCircle, Eye, X, AlertCircle } from 'lucide-react';
import api, { pgAPI } from '../services/api';

const PaymentReview = () => {
  const [submissions, setSubmissions] = useState([]);
  const [notPaidTenants, setNotPaidTenants] = useState([]);
  const [pgs, setPgs] = useState([]);
  const [loading, setLoading] = useState(true);
  const [filter, setFilter] = useState('pending');
  const [selectedPgId, setSelectedPgId] = useState('');

  // Get user role from localStorage
  const user = JSON.parse(localStorage.getItem('user') || '{}');
  const isAdmin = user.role === 'admin';
  const currentDate = new Date();
  const months = [
    'January', 'February', 'March', 'April', 'May', 'June',
    'July', 'August', 'September', 'October', 'November', 'December'
  ];
  const currentMonth = months[currentDate.getMonth()];
  const currentYear = currentDate.getFullYear();

  const [selectedMonth, setSelectedMonth] = useState(currentMonth);
  const [selectedYear, setSelectedYear] = useState(currentYear.toString());
  const [showImageModal, setShowImageModal] = useState(false);
  const [selectedImage, setSelectedImage] = useState('');

  useEffect(() => {
    fetchPGs();
  }, []);

  useEffect(() => {
    if (filter === 'notpaid') {
      fetchNotPaidTenants();
    } else {
      fetchSubmissions();
    }
  }, [filter, selectedMonth, selectedYear, selectedPgId]);

  const fetchSubmissions = async () => {
    try {
      setLoading(true);
      const params = filter !== 'all' ? { status: filter } : {};
      const response = await api.get('/payment-submissions', { params });
      setSubmissions(response.data);
      setNotPaidTenants([]);
    } catch (error) {
      console.error('Error fetching submissions:', error);
    } finally {
      setLoading(false);
    }
  };

  const fetchNotPaidTenants = async () => {
    try {
      setLoading(true);
      const month = selectedMonth || currentMonth;
      const year = selectedYear || currentYear.toString();

      const params = { month, year };
      if (selectedPgId) params.pg_id = selectedPgId;

      const response = await api.get('/payment-submissions/not-paid', { params });
      setNotPaidTenants(response.data);
      setSubmissions([]);
    } catch (error) {
      console.error('Error fetching not paid tenants:', error);
    } finally {
      setLoading(false);
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

  const handleReview = async (id, status) => {
    try {
      const userData = JSON.parse(localStorage.getItem('user') || '{}');

      await api.put(`/payment-submissions/${id}/review`, {
        status,
        reviewed_by: userData.id
      });

      alert(`Payment ${status} successfully!`);
      fetchSubmissions();
    } catch (error) {
      alert('Error reviewing payment: ' + (error.response?.data?.error || error.message));
    }
  };

  const getStatusBadge = (status) => {
    const styles = {
      pending: 'bg-yellow-100 text-yellow-800',
      approved: 'bg-green-100 text-green-800',
      rejected: 'bg-red-100 text-red-800',
      notpaid: 'bg-gray-100 text-gray-800',
      directpayment: 'bg-blue-100 text-blue-800'
    };
    const labels = {
      pending: 'Pending',
      approved: 'Approved',
      rejected: 'Rejected',
      notpaid: 'Not Paid',
      directpayment: 'Direct Payment'
    };
    return (
      <span className={`px-3 py-1 rounded-full text-xs font-medium ${styles[status]}`}>
        {labels[status] || status.charAt(0).toUpperCase() + status.slice(1)}
      </span>
    );
  };

  // Filter submissions based on selected filters
  const filteredSubmissions = submissions.filter(submission => {
    if (selectedPgId && submission.pg_id !== parseInt(selectedPgId)) return false;
    if (selectedMonth && submission.month !== selectedMonth) return false;
    if (selectedYear && submission.year !== parseInt(selectedYear)) return false;
    return true;
  });

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-bold text-gray-900">Payment Review</h2>
        <p className="text-gray-500 mt-1">Review tenant payment submissions</p>
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
              Month
            </label>
            <select
              value={selectedMonth}
              onChange={(e) => setSelectedMonth(e.target.value)}
              className="input-field"
            >
              <option value="">All Months</option>
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
              <option value="">All Years</option>
              {[currentYear - 1, currentYear, currentYear + 1].map((year) => (
                <option key={year} value={year}>{year}</option>
              ))}
            </select>
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Status
            </label>
            <select
              value={filter}
              onChange={(e) => setFilter(e.target.value)}
              className="input-field"
            >
              <option value="pending">Pending</option>
              <option value="approved">Approved</option>
              <option value="rejected">Rejected</option>
              <option value="directpayment">Direct Payment</option>
              <option value="notpaid">Not Paid</option>
              <option value="all">All</option>
            </select>
          </div>
        </div>
      </div>

      {loading ? (
        <div className="flex items-center justify-center h-64">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-gray-900"></div>
        </div>
      ) : filter === 'notpaid' ? (
        /* Not Paid Tenants Table */
        <div className="card overflow-x-auto">
          {/* Summary for Not Paid */}
          {notPaidTenants.length > 0 && (
            <div className="mb-4 p-4 bg-red-50 border border-red-200 rounded-lg">
              <div className="flex items-center space-x-3">
                <AlertCircle className="text-red-600" size={24} />
                <div>
                  <p className="text-sm font-medium text-red-800">
                    {notPaidTenants.length} tenant{notPaidTenants.length !== 1 ? 's' : ''} have not submitted payment for {selectedMonth || currentMonth} {selectedYear || currentYear}
                  </p>
                  <p className="text-xs text-red-600 mt-1">
                    Total pending: ₹{notPaidTenants.reduce((sum, t) => sum + (t.monthly_rent || 0), 0).toLocaleString()}
                  </p>
                </div>
              </div>
            </div>
          )}
          <table className="min-w-full">
            <thead>
              <tr className="table-header">
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Tenant</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Phone</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Room</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">PG</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Month/Year</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Monthly Rent</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Status</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200">
              {notPaidTenants.map((tenant) => (
                <tr key={tenant.id} className="table-row bg-red-50">
                  <td className="px-4 py-3 text-sm font-medium text-gray-900">
                    {tenant.tenant_name}
                  </td>
                  <td className="px-4 py-3 text-sm text-gray-900">
                    {tenant.phone_number || '-'}
                  </td>
                  <td className="px-4 py-3 text-sm text-gray-900">
                    {tenant.room_number || 'Not assigned'}
                  </td>
                  <td className="px-4 py-3 text-sm text-gray-900">
                    {tenant.pg_name || '-'}
                  </td>
                  <td className="px-4 py-3 text-sm text-gray-900">
                    {tenant.month} {tenant.year}
                  </td>
                  <td className="px-4 py-3 text-sm font-medium text-red-600">
                    ₹{(tenant.monthly_rent || 0).toLocaleString()}
                  </td>
                  <td className="px-4 py-3 text-sm">
                    {getStatusBadge('notpaid')}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {notPaidTenants.length === 0 && (
            <div className="text-center py-12">
              <CheckCircle size={48} className="mx-auto text-green-400 mb-4" />
              <p className="text-gray-500">
                All tenants have submitted their payment for {selectedMonth || currentMonth} {selectedYear || currentYear}!
              </p>
            </div>
          )}
        </div>
      ) : (
        /* Regular Submissions Table */
        <div className="card overflow-x-auto">
          {/* Summary for Submissions */}
          {filteredSubmissions.length > 0 && (
            <div className={`mb-4 p-4 rounded-lg border ${
              filter === 'pending' ? 'bg-yellow-50 border-yellow-200' :
              filter === 'approved' ? 'bg-green-50 border-green-200' :
              filter === 'rejected' ? 'bg-red-50 border-red-200' :
              filter === 'directpayment' ? 'bg-blue-50 border-blue-200' :
              'bg-gray-50 border-gray-200'
            }`}>
              <div className="flex items-center justify-between">
                <div className="flex items-center space-x-6">
                  <div>
                    <p className={`text-sm font-medium ${
                      filter === 'pending' ? 'text-yellow-800' :
                      filter === 'approved' ? 'text-green-800' :
                      filter === 'rejected' ? 'text-red-800' :
                      filter === 'directpayment' ? 'text-blue-800' :
                      'text-gray-800'
                    }`}>
                      {filter === 'pending' ? 'Pending Submissions' :
                       filter === 'approved' ? 'Approved Submissions' :
                       filter === 'rejected' ? 'Rejected Submissions' :
                       filter === 'directpayment' ? 'Direct Payments' :
                       'All Submissions'}
                    </p>
                    <p className="text-2xl font-bold text-gray-900 mt-1">
                      {filteredSubmissions.length} record{filteredSubmissions.length !== 1 ? 's' : ''}
                    </p>
                  </div>
                  <div className="border-l pl-6">
                    <p className={`text-sm font-medium ${
                      filter === 'pending' ? 'text-yellow-800' :
                      filter === 'approved' ? 'text-green-800' :
                      filter === 'rejected' ? 'text-red-800' :
                      filter === 'directpayment' ? 'text-blue-800' :
                      'text-gray-800'
                    }`}>Total Amount</p>
                    <p className="text-2xl font-bold text-gray-900 mt-1">
                      ₹{filteredSubmissions.reduce((sum, s) => sum + (s.amount || 0), 0).toLocaleString()}
                    </p>
                  </div>
                </div>
              </div>
            </div>
          )}
          <table className="min-w-full">
            <thead>
              <tr className="table-header">
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Tenant</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Room</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Month/Year</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Amount</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Submitted</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Screenshot</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Status</th>
                {isAdmin && <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Actions</th>}
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200">
              {filteredSubmissions.map((submission) => (
                <tr key={submission.id} className="table-row">
                  <td className="px-4 py-3 text-sm font-medium text-gray-900">
                    {submission.tenant_name}
                  </td>
                  <td className="px-4 py-3 text-sm text-gray-900">
                    {submission.room_number || 'Not assigned'}
                  </td>
                  <td className="px-4 py-3 text-sm text-gray-900">
                    {submission.month} {submission.year}
                  </td>
                  <td className="px-4 py-3 text-sm text-gray-900">
                    ₹{submission.amount.toLocaleString()}
                  </td>
                  <td className="px-4 py-3 text-sm text-gray-900">
                    {new Date(submission.submission_date).toLocaleDateString()}
                  </td>
                  <td className="px-4 py-3 text-sm">
                    {submission.payment_screenshot ? (
                      <button
                        onClick={() => {
                          setSelectedImage(submission.payment_screenshot);
                          setShowImageModal(true);
                        }}
                        className="p-1.5 text-blue-600 hover:bg-blue-50 rounded"
                        title="View Screenshot"
                      >
                        <Eye size={16} />
                      </button>
                    ) : (
                      <span className="text-gray-400">-</span>
                    )}
                  </td>
                  <td className="px-4 py-3 text-sm">
                    {getStatusBadge(submission.status)}
                  </td>
                  {isAdmin && submission.status !== 'directpayment' && (
                    <td className="px-4 py-3 text-sm">
                      {submission.status === 'pending' && (
                        <div className="flex space-x-2">
                          <button
                            onClick={() => handleReview(submission.id, 'approved')}
                            className="p-1.5 text-green-600 hover:bg-green-50 rounded"
                            title="Approve"
                          >
                            <CheckCircle size={18} />
                          </button>
                          <button
                            onClick={() => handleReview(submission.id, 'rejected')}
                            className="p-1.5 text-red-600 hover:bg-red-50 rounded"
                            title="Reject"
                          >
                            <XCircle size={18} />
                          </button>
                        </div>
                      )}
                    </td>
                  )}
                  {isAdmin && submission.status === 'directpayment' && (
                    <td className="px-4 py-3 text-sm text-gray-400">-</td>
                  )}
                </tr>
              ))}
            </tbody>
          </table>
          {filteredSubmissions.length === 0 && (
            <div className="text-center py-12">
              <p className="text-gray-500">
                {submissions.length === 0
                  ? `No ${filter !== 'all' ? filter : ''} submissions found`
                  : 'No submissions match the selected filters.'}
              </p>
            </div>
          )}
        </div>
      )}

      {/* Image Modal */}
      {showImageModal && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-lg p-6 max-w-4xl w-full max-h-[90vh] overflow-y-auto">
            <div className="flex justify-between items-center mb-4">
              <h3 className="text-xl font-bold text-gray-900">Payment Screenshot</h3>
              <button
                onClick={() => {
                  setShowImageModal(false);
                  setSelectedImage('');
                }}
                className="p-2 text-gray-600 hover:bg-gray-100 rounded-full"
              >
                <X size={24} />
              </button>
            </div>
            <div className="flex justify-center">
              <img
                src={selectedImage}
                alt="Payment Screenshot"
                className="max-w-full h-auto rounded-lg shadow-lg"
              />
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default PaymentReview;
