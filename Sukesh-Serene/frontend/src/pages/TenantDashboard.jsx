import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { Home, IndianRupee, Upload, LogOut, X, Download, CheckCircle, Clock, XCircle, Lock, Calendar, Wallet, MessageSquare, AlertCircle } from 'lucide-react';
import axios from 'axios';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5000/api';

const TenantDashboard = () => {
  const navigate = useNavigate();
  const [tenantData, setTenantData] = useState(null);
  const [showPaymentModal, setShowPaymentModal] = useState(false);
  const [showQRModal, setShowQRModal] = useState(false);
  const [paymentScreenshot, setPaymentScreenshot] = useState('');
  const [screenshotPreview, setScreenshotPreview] = useState('');
  const [loading, setLoading] = useState(false);
  const [month, setMonth] = useState('');
  const [year, setYear] = useState(new Date().getFullYear());
  const [paymentMethod, setPaymentMethod] = useState('upi');
  const [paidTo, setPaidTo] = useState('');
  const [amount, setAmount] = useState('');
  const [paymentHistory, setPaymentHistory] = useState([]);
  const [showComplaintModal, setShowComplaintModal] = useState(false);
  const [complaintDescription, setComplaintDescription] = useState('');
  const [complaints, setComplaints] = useState([]);

  useEffect(() => {
    const data = localStorage.getItem('tenantData');
    if (!data) {
      navigate('/tenant/login');
      return;
    }
    const parsedData = JSON.parse(data);
    setTenantData(parsedData);
    fetchPaymentHistory(parsedData.id);
    fetchComplaints(parsedData.id);
    refreshTenantData(parsedData.id);

    // Refresh data when window gains focus
    const handleFocus = () => {
      refreshTenantData(parsedData.id);
      fetchComplaints(parsedData.id);
    };

    window.addEventListener('focus', handleFocus);

    return () => {
      window.removeEventListener('focus', handleFocus);
    };
  }, [navigate]);

  const refreshTenantData = async (tenantId) => {
    try {
      const token = localStorage.getItem('tenantToken');
      const response = await axios.get(
        `${API_URL}/tenants/${tenantId}`,
        {
          headers: { Authorization: `Bearer ${token}` }
        }
      );

      // Update both state and localStorage with fresh data
      setTenantData(response.data);
      localStorage.setItem('tenantData', JSON.stringify(response.data));
    } catch (err) {
      console.error('Error refreshing tenant data:', err);
    }
  };

  const fetchPaymentHistory = async (tenantId) => {
    try {
      const token = localStorage.getItem('tenantToken');
      const response = await axios.get(
        `${API_URL}/payment-submissions/tenant/${tenantId}`,
        {
          headers: { Authorization: `Bearer ${token}` }
        }
      );
      setPaymentHistory(response.data);
    } catch (err) {
      console.error('Error fetching payment history:', err);
    }
  };

  const fetchComplaints = async (tenantId) => {
    try {
      const token = localStorage.getItem('tenantToken');
      const response = await axios.get(
        `${API_URL}/complaints/tenant/${tenantId}`,
        {
          headers: { Authorization: `Bearer ${token}` }
        }
      );
      setComplaints(response.data);
    } catch (err) {
      console.error('Error fetching complaints:', err);
    }
  };

  const handleComplaintSubmit = async (e) => {
    e.preventDefault();

    if (!complaintDescription || complaintDescription.trim() === '') {
      alert('Please enter complaint description');
      return;
    }

    setLoading(true);

    try {
      const token = localStorage.getItem('tenantToken');
      await axios.post(
        `${API_URL}/complaints`,
        {
          tenant_id: tenantData.id,
          complaint_description: complaintDescription
        },
        {
          headers: { Authorization: `Bearer ${token}` }
        }
      );

      alert('Complaint raised successfully!');
      setShowComplaintModal(false);
      setComplaintDescription('');
      fetchComplaints(tenantData.id);
    } catch (err) {
      alert(err.response?.data?.error || 'Failed to raise complaint');
    } finally {
      setLoading(false);
    }
  };

  const handleLogout = () => {
    localStorage.removeItem('tenantToken');
    localStorage.removeItem('tenantData');
    navigate('/tenant/login');
  };

  const handleScreenshotUpload = (e) => {
    const file = e.target.files[0];
    if (file) {
      const reader = new FileReader();
      reader.onloadend = () => {
        const base64String = reader.result;
        setPaymentScreenshot(base64String);
        setScreenshotPreview(base64String);
      };
      reader.readAsDataURL(file);
    }
  };

  const handlePaymentSubmit = async (e) => {
    e.preventDefault();

    if (!month) {
      alert('Please select month');
      return;
    }

    if (paymentMethod === 'upi' && !paymentScreenshot) {
      alert('Please upload payment screenshot for UPI payment');
      return;
    }

    if (!amount || parseFloat(amount) <= 0) {
      alert('Please enter a valid payment amount');
      return;
    }

    if (paymentMethod === 'cash' && !paidTo) {
      alert('Please enter the name of the person you paid to');
      return;
    }

    setLoading(true);

    try {
      const token = localStorage.getItem('tenantToken');
      const response = await axios.post(
        `${API_URL}/payment-submissions`,
        {
          tenant_id: tenantData.id,
          month,
          year,
          amount: parseFloat(amount),
          payment_screenshot: paymentScreenshot,
          payment_method: paymentMethod,
          paid_to: paymentMethod === 'cash' ? paidTo : null,
          submission_date: new Date().toISOString().split('T')[0]
        },
        {
          headers: { Authorization: `Bearer ${token}` }
        }
      );

      alert('Payment submitted successfully! Waiting for admin approval.');
      setShowPaymentModal(false);
      setPaymentScreenshot('');
      setScreenshotPreview('');
      setMonth('');
      setPaymentMethod('upi');
      setPaidTo('');
      setAmount('');
      fetchPaymentHistory(tenantData.id);
    } catch (err) {
      alert(err.response?.data?.error || 'Failed to submit payment');
    } finally {
      setLoading(false);
    }
  };

  const downloadReceipt = (payment) => {
    const receiptWindow = window.open('', '_blank');

    const receiptHTML = `
      <!DOCTYPE html>
      <html>
      <head>
        <title>Payment Receipt - ${payment.month} ${payment.year}</title>
        <style>
          body { font-family: Arial, sans-serif; margin: 40px; }
          .receipt { max-width: 800px; margin: 0 auto; border: 2px solid #333; padding: 30px; }
          .header { text-align: center; border-bottom: 2px solid #333; padding-bottom: 20px; margin-bottom: 20px; }
          .logo { font-size: 28px; font-weight: bold; color: #0d9488; }
          .receipt-title { font-size: 20px; font-weight: bold; margin-top: 10px; }
          .info-row { display: flex; justify-content: space-between; margin: 10px 0; padding: 8px 0; border-bottom: 1px solid #ddd; }
          .label { font-weight: bold; }
          .amount-section { background: #f0f9ff; padding: 20px; margin: 20px 0; text-align: center; border-radius: 8px; }
          .amount { font-size: 32px; font-weight: bold; color: #0d9488; }
          .footer { margin-top: 30px; padding-top: 20px; border-top: 2px solid #333; text-align: center; font-size: 12px; color: #666; }
          .status { display: inline-block; padding: 5px 15px; background: #10b981; color: white; border-radius: 20px; font-size: 14px; }
          @media print {
            body { margin: 0; }
            .no-print { display: none; }
          }
        </style>
      </head>
      <body>
        <div class="receipt">
          <div class="header">
            <div class="logo">Serene Living PG</div>
            <div class="receipt-title">PAYMENT RECEIPT</div>
          </div>

          <div class="info-row">
            <span class="label">Receipt No:</span>
            <span>#${payment.id.toString().padStart(6, '0')}</span>
          </div>

          <div class="info-row">
            <span class="label">Tenant Name:</span>
            <span>${tenantData.name}</span>
          </div>

          <div class="info-row">
            <span class="label">Room Number:</span>
            <span>${tenantData.room_number || 'N/A'}</span>
          </div>

          <div class="info-row">
            <span class="label">Payment Period:</span>
            <span>${payment.month} ${payment.year}</span>
          </div>

          <div class="info-row">
            <span class="label">Payment Date:</span>
            <span>${new Date(payment.submission_date).toLocaleDateString()}</span>
          </div>

          <div class="info-row">
            <span class="label">Approved Date:</span>
            <span>${new Date(payment.reviewed_at).toLocaleDateString()}</span>
          </div>

          <div class="info-row">
            <span class="label">Status:</span>
            <span><span class="status">APPROVED</span></span>
          </div>

          <div class="amount-section">
            <div style="margin-bottom: 10px; color: #666;">Amount Paid</div>
            <div class="amount">₹${payment.amount.toLocaleString()}</div>
          </div>

          <div class="footer">
            <p><strong>Thank you for your payment!</strong></p>
            <p>This is a computer-generated receipt and does not require a signature.</p>
            <p>Generated on: ${new Date().toLocaleString()}</p>
          </div>
        </div>

        <div class="no-print" style="text-align: center; margin-top: 20px;">
          <button onclick="window.print()" style="background: #0d9488; color: white; padding: 12px 24px; border: none; border-radius: 8px; font-size: 16px; cursor: pointer;">
            Print Receipt
          </button>
          <button onclick="window.close()" style="background: #6b7280; color: white; padding: 12px 24px; border: none; border-radius: 8px; font-size: 16px; cursor: pointer; margin-left: 10px;">
            Close
          </button>
        </div>
      </body>
      </html>
    `;

    receiptWindow.document.write(receiptHTML);
    receiptWindow.document.close();
  };

  if (!tenantData) {
    return <div>Loading...</div>;
  }

  const months = [
    'January', 'February', 'March', 'April', 'May', 'June',
    'July', 'August', 'September', 'October', 'November', 'December'
  ];

  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 to-teal-50">
      {/* Header */}
      <div className="bg-white shadow-sm">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex justify-between items-center py-4">
            <div>
              <h1 className="text-2xl font-bold text-gray-900">Tenant Portal</h1>
              <p className="text-sm text-gray-500">Welcome, {tenantData.name}</p>
            </div>
            <div className="flex items-center space-x-2">
              <button
                onClick={() => navigate('/tenant/change-password')}
                className="flex items-center space-x-2 px-4 py-2 text-gray-700 hover:bg-gray-100 rounded-lg transition-colors"
              >
                <Lock size={20} />
                <span className="hidden sm:inline">Change Password</span>
              </button>
              <button
                onClick={handleLogout}
                className="flex items-center space-x-2 px-4 py-2 text-gray-700 hover:bg-gray-100 rounded-lg transition-colors"
              >
                <LogOut size={20} />
                <span className="hidden sm:inline">Logout</span>
              </button>
            </div>
          </div>
        </div>
      </div>

      {/* Main Content */}
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-8">
          {/* Tenant Info Card */}
          <div className="bg-white rounded-xl shadow-md p-6">
            <h2 className="text-lg font-semibold text-gray-900 mb-4">Your Information</h2>
            <div className="space-y-3">
              <div className="flex items-center space-x-3">
                <div className="w-10 h-10 bg-blue-100 rounded-lg flex items-center justify-center">
                  <Home size={20} className="text-blue-600" />
                </div>
                <div>
                  <p className="text-sm text-gray-500">Room Number</p>
                  <p className="font-semibold text-gray-900">{tenantData.room_number || 'Not assigned'}</p>
                </div>
              </div>
              <div className="flex items-center space-x-3">
                <div className="w-10 h-10 bg-green-100 rounded-lg flex items-center justify-center">
                  <IndianRupee size={20} className="text-green-600" />
                </div>
                <div>
                  <p className="text-sm text-gray-500">Monthly Rent</p>
                  <p className="font-semibold text-gray-900">
                    ₹{tenantData.monthly_rent ? tenantData.monthly_rent.toLocaleString() : '0'}
                  </p>
                </div>
              </div>
              <div className="flex items-center space-x-3">
                <div className="w-10 h-10 bg-purple-100 rounded-lg flex items-center justify-center">
                  <Wallet size={20} className="text-purple-600" />
                </div>
                <div>
                  <p className="text-sm text-gray-500">Security Deposit</p>
                  <p className="font-semibold text-gray-900">
                    ₹{tenantData.security_deposit ? tenantData.security_deposit.toLocaleString() : '0'}
                  </p>
                </div>
              </div>
              <div className="flex items-center space-x-3">
                <div className="w-10 h-10 bg-teal-100 rounded-lg flex items-center justify-center">
                  <Calendar size={20} className="text-teal-600" />
                </div>
                <div>
                  <p className="text-sm text-gray-500">Joining Date</p>
                  <p className="font-semibold text-gray-900">
                    {tenantData.check_in_date ? new Date(tenantData.check_in_date).toLocaleDateString() : 'Not set'}
                  </p>
                </div>
              </div>
            </div>
          </div>

          {/* Payment QR Code Card */}
          <div className="bg-white rounded-xl shadow-md p-6">
            <h2 className="text-lg font-semibold text-gray-900 mb-4">Payment Details</h2>

            {/* UPI Number */}
            {tenantData.upi_number && (
              <div className="mb-4 p-3 bg-teal-50 border border-teal-200 rounded-lg">
                <p className="text-sm text-teal-700 font-medium mb-1">UPI ID</p>
                <div className="flex items-center justify-between">
                  <p className="text-lg font-semibold text-teal-900">{tenantData.upi_number}</p>
                  <button
                    onClick={() => {
                      navigator.clipboard.writeText(tenantData.upi_number);
                      alert('UPI ID copied to clipboard!');
                    }}
                    className="text-teal-600 hover:text-teal-800 text-sm font-medium"
                  >
                    Copy
                  </button>
                </div>
              </div>
            )}

            {/* QR Code */}
            {tenantData.payment_qr_code ? (
              <div className="flex flex-col items-center">
                <div
                  onClick={() => setShowQRModal(true)}
                  className="cursor-pointer hover:opacity-80 transition-opacity"
                >
                  <img
                    src={tenantData.payment_qr_code}
                    alt="Payment QR Code"
                    className="w-48 h-48 object-contain border border-gray-200 rounded-lg"
                  />
                </div>
                <p className="text-sm text-gray-500 mt-2">Click to enlarge</p>
              </div>
            ) : !tenantData.upi_number ? (
              <p className="text-gray-500 text-center py-8">No payment details available. Please contact admin.</p>
            ) : null}
          </div>
        </div>

        {/* Submit Payment & Raise Complaint Buttons */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-8">
          <div className="bg-white rounded-xl shadow-md p-6">
            <h2 className="text-lg font-semibold text-gray-900 mb-4">Submit Payment</h2>
            <p className="text-gray-600 mb-4">
              After making the payment using the QR code above, upload your payment screenshot here.
            </p>
            <button
              onClick={() => {
                setShowPaymentModal(true);
                setAmount(tenantData?.monthly_rent || '');
              }}
              className="w-full bg-teal-600 text-white px-6 py-3 rounded-lg font-medium hover:bg-teal-700 transition-colors flex items-center justify-center space-x-2"
            >
              <Upload size={20} />
              <span>Submit Payment Proof</span>
            </button>
          </div>

          <div className="bg-white rounded-xl shadow-md p-6">
            <h2 className="text-lg font-semibold text-gray-900 mb-4">Raise Complaint</h2>
            <p className="text-gray-600 mb-4">
              Have an issue or concern? Let us know and we'll address it promptly.
            </p>
            <button
              onClick={() => setShowComplaintModal(true)}
              className="w-full bg-orange-600 text-white px-6 py-3 rounded-lg font-medium hover:bg-orange-700 transition-colors flex items-center justify-center space-x-2"
            >
              <MessageSquare size={20} />
              <span>Raise Complaint</span>
            </button>
          </div>
        </div>

        {/* Payment History */}
        <div className="bg-white rounded-xl shadow-md p-6">
          <h2 className="text-lg font-semibold text-gray-900 mb-4">Payment History</h2>
          {paymentHistory.length === 0 ? (
            <p className="text-gray-500 text-center py-8">No payment submissions yet.</p>
          ) : (
            <div className="overflow-x-auto">
              <table className="min-w-full">
                <thead>
                  <tr className="border-b border-gray-200">
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Period</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Amount</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Submitted On</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Status</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Action</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-200">
                  {paymentHistory.map((payment) => (
                    <tr key={payment.id}>
                      <td className="px-4 py-3 text-sm text-gray-900">
                        {payment.month} {payment.year}
                      </td>
                      <td className="px-4 py-3 text-sm font-medium text-gray-900">
                        ₹{payment.amount?.toLocaleString()}
                      </td>
                      <td className="px-4 py-3 text-sm text-gray-900">
                        {new Date(payment.submission_date).toLocaleDateString()}
                      </td>
                      <td className="px-4 py-3 text-sm">
                        {payment.status === 'approved' && (
                          <span className="inline-flex items-center space-x-1 px-3 py-1 rounded-full text-xs font-medium bg-green-100 text-green-800">
                            <CheckCircle size={14} />
                            <span>Approved</span>
                          </span>
                        )}
                        {payment.status === 'pending' && (
                          <span className="inline-flex items-center space-x-1 px-3 py-1 rounded-full text-xs font-medium bg-yellow-100 text-yellow-800">
                            <Clock size={14} />
                            <span>Pending</span>
                          </span>
                        )}
                        {payment.status === 'rejected' && (
                          <span className="inline-flex items-center space-x-1 px-3 py-1 rounded-full text-xs font-medium bg-red-100 text-red-800">
                            <XCircle size={14} />
                            <span>Rejected</span>
                          </span>
                        )}
                      </td>
                      <td className="px-4 py-3 text-sm">
                        {payment.status === 'approved' && (
                          <button
                            onClick={() => downloadReceipt(payment)}
                            className="inline-flex items-center space-x-1 px-3 py-1.5 bg-teal-600 text-white rounded-lg text-xs font-medium hover:bg-teal-700 transition-colors"
                          >
                            <Download size={14} />
                            <span>Download Receipt</span>
                          </button>
                        )}
                        {payment.status === 'pending' && (
                          <span className="text-xs text-gray-500">Awaiting approval</span>
                        )}
                        {payment.status === 'rejected' && (
                          <span className="text-xs text-gray-500">-</span>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>

        {/* Complaints History */}
        <div className="bg-white rounded-xl shadow-md p-6 mt-8">
          <h2 className="text-lg font-semibold text-gray-900 mb-4">My Complaints</h2>
          {complaints.length === 0 ? (
            <p className="text-gray-500 text-center py-8">No complaints raised yet.</p>
          ) : (
            <div className="space-y-4">
              {complaints.map((complaint) => (
                <div key={complaint.id} className="border border-gray-200 rounded-lg p-4">
                  <div className="flex justify-between items-start mb-2">
                    <div className="flex-1">
                      <p className="text-sm text-gray-500">
                        Raised on {new Date(complaint.created_at).toLocaleDateString()} at {new Date(complaint.created_at).toLocaleTimeString()}
                      </p>
                    </div>
                    <div>
                      {complaint.status === 'resolved' && (
                        <span className="inline-flex items-center space-x-1 px-3 py-1 rounded-full text-xs font-medium bg-green-100 text-green-800">
                          <CheckCircle size={14} />
                          <span>Resolved</span>
                        </span>
                      )}
                      {complaint.status === 'in_progress' && (
                        <span className="inline-flex items-center space-x-1 px-3 py-1 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
                          <Clock size={14} />
                          <span>In Progress</span>
                        </span>
                      )}
                      {complaint.status === 'raised' && (
                        <span className="inline-flex items-center space-x-1 px-3 py-1 rounded-full text-xs font-medium bg-yellow-100 text-yellow-800">
                          <AlertCircle size={14} />
                          <span>Raised</span>
                        </span>
                      )}
                    </div>
                  </div>
                  <div className="mb-2">
                    <p className="text-sm font-medium text-gray-700 mb-1">Your Complaint:</p>
                    <p className="text-sm text-gray-900">{complaint.complaint_description}</p>
                  </div>
                  {complaint.status === 'resolved' && complaint.resolution_description && (
                    <div className="mt-3 pt-3 border-t border-gray-200">
                      <p className="text-sm font-medium text-green-700 mb-1">Resolution:</p>
                      <p className="text-sm text-gray-900">{complaint.resolution_description}</p>
                      {complaint.resolved_at && (
                        <p className="text-xs text-gray-500 mt-1">
                          Resolved on {new Date(complaint.resolved_at).toLocaleDateString()} at {new Date(complaint.resolved_at).toLocaleTimeString()}
                        </p>
                      )}
                    </div>
                  )}
                </div>
              ))}
            </div>
          )}
        </div>
      </div>

      {/* QR Code Zoom Modal */}
      {showQRModal && (
        <div className="fixed inset-0 bg-black bg-opacity-75 flex items-center justify-center z-50 p-4" onClick={() => setShowQRModal(false)}>
          <div className="bg-white rounded-lg p-6 max-w-2xl w-full" onClick={(e) => e.stopPropagation()}>
            <div className="flex justify-between items-center mb-4">
              <h3 className="text-xl font-bold text-gray-900">Payment QR Code</h3>
              <button
                onClick={() => setShowQRModal(false)}
                className="p-2 text-gray-600 hover:bg-gray-100 rounded-full"
              >
                <X size={24} />
              </button>
            </div>
            <div className="flex flex-col items-center">
              <img
                src={tenantData.payment_qr_code}
                alt="Payment QR Code - Large View"
                className="w-full max-w-md h-auto object-contain border border-gray-200 rounded-lg"
              />
              <p className="text-sm text-gray-600 mt-4 text-center">
                Scan this QR code with any UPI app to make payment
              </p>
              <button
                onClick={() => {
                  const link = document.createElement('a');
                  link.href = tenantData.payment_qr_code;
                  link.download = `payment-qr-${tenantData.name}.png`;
                  link.click();
                }}
                className="mt-4 bg-teal-600 text-white px-6 py-2 rounded-lg font-medium hover:bg-teal-700 transition-colors flex items-center space-x-2"
              >
                <Download size={20} />
                <span>Download QR Code</span>
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Payment Submission Modal */}
      {showPaymentModal && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-lg p-6 max-w-md w-full max-h-[90vh] overflow-y-auto">
            <div className="flex justify-between items-center mb-4">
              <h3 className="text-xl font-bold text-gray-900">Submit Payment</h3>
              <button
                onClick={() => {
                  setShowPaymentModal(false);
                  setPaymentScreenshot('');
                  setScreenshotPreview('');
                  setPaymentMethod('upi');
                  setPaidTo('');
                  setAmount('');
                }}
                className="p-2 text-gray-600 hover:bg-gray-100 rounded-full"
              >
                <X size={24} />
              </button>
            </div>

            <form onSubmit={handlePaymentSubmit} className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Month *
                </label>
                <select
                  value={month}
                  onChange={(e) => setMonth(e.target.value)}
                  className="input-field"
                  required
                >
                  <option value="">Select Month</option>
                  {months.map((m) => (
                    <option key={m} value={m}>
                      {m}
                    </option>
                  ))}
                </select>
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Year *
                </label>
                <input
                  type="number"
                  value={year}
                  onChange={(e) => setYear(parseInt(e.target.value))}
                  className="input-field"
                  required
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Payment Method *
                </label>
                <select
                  value={paymentMethod}
                  onChange={(e) => setPaymentMethod(e.target.value)}
                  className="input-field"
                  required
                >
                  <option value="upi">UPI</option>
                  <option value="cash">Cash</option>
                </select>
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Amount *
                </label>
                <input
                  type="number"
                  value={amount}
                  onChange={(e) => setAmount(e.target.value)}
                  className="input-field"
                  placeholder="Enter payment amount"
                  required
                />
              </div>

              {paymentMethod === 'cash' && (
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Paid To *
                  </label>
                  <input
                    type="text"
                    value={paidTo}
                    onChange={(e) => setPaidTo(e.target.value)}
                    className="input-field"
                    placeholder="Enter name of person you paid to"
                    required
                  />
                </div>
              )}

              {paymentMethod === 'upi' && (
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Payment Screenshot *
                  </label>
                  <div className="space-y-3">
                    <label className="cursor-pointer block">
                      <div className="border-2 border-dashed border-gray-300 rounded-lg p-4 hover:border-gray-400 transition-colors">
                        <div className="flex items-center justify-center space-x-2 text-gray-600">
                          <Upload size={20} />
                          <span className="text-sm">Click to upload screenshot</span>
                        </div>
                      </div>
                      <input
                        type="file"
                        accept="image/*"
                        onChange={handleScreenshotUpload}
                        className="hidden"
                      />
                    </label>

                    {screenshotPreview && (
                      <div className="relative">
                        <img
                          src={screenshotPreview}
                          alt="Payment screenshot preview"
                          className="w-full h-48 object-cover rounded-lg"
                        />
                        <button
                          type="button"
                          onClick={() => {
                            setPaymentScreenshot('');
                            setScreenshotPreview('');
                          }}
                          className="absolute top-2 right-2 p-1 bg-red-500 text-white rounded-full hover:bg-red-600"
                        >
                          <X size={16} />
                        </button>
                      </div>
                    )}
                  </div>
                </div>
              )}

              <div className="flex space-x-3 pt-4">
                <button
                  type="submit"
                  disabled={loading}
                  className="btn-primary flex-1 disabled:opacity-50"
                >
                  {loading ? 'Submitting...' : 'Submit'}
                </button>
                <button
                  type="button"
                  onClick={() => {
                    setShowPaymentModal(false);
                    setPaymentScreenshot('');
                    setScreenshotPreview('');
                    setPaymentMethod('upi');
                    setPaidTo('');
                    setAmount('');
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

      {/* Raise Complaint Modal */}
      {showComplaintModal && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-lg p-6 max-w-2xl w-full max-h-[90vh] overflow-y-auto">
            <div className="flex justify-between items-center mb-4">
              <h3 className="text-xl font-bold text-gray-900">Raise a Complaint</h3>
              <button
                onClick={() => {
                  setShowComplaintModal(false);
                  setComplaintDescription('');
                }}
                className="p-2 text-gray-600 hover:bg-gray-100 rounded-full"
              >
                <X size={24} />
              </button>
            </div>

            <form onSubmit={handleComplaintSubmit}>
              <div className="mb-4">
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Complaint Description *
                </label>
                <textarea
                  value={complaintDescription}
                  onChange={(e) => setComplaintDescription(e.target.value)}
                  className="input-field"
                  rows="6"
                  placeholder="Please describe your issue or concern in detail..."
                  required
                ></textarea>
                <p className="text-xs text-gray-500 mt-1">
                  Our team will review your complaint and get back to you as soon as possible.
                </p>
              </div>

              <div className="flex space-x-3 pt-4">
                <button
                  type="submit"
                  disabled={loading}
                  className="btn-primary flex-1 disabled:opacity-50 flex items-center justify-center space-x-2"
                >
                  {loading ? (
                    <>
                      <svg className="animate-spin h-5 w-5 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                        <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                        <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                      </svg>
                      <span>Submitting...</span>
                    </>
                  ) : (
                    <>
                      <MessageSquare size={18} />
                      <span>Submit Complaint</span>
                    </>
                  )}
                </button>
                <button
                  type="button"
                  onClick={() => {
                    setShowComplaintModal(false);
                    setComplaintDescription('');
                  }}
                  className="btn-secondary flex-1"
                  disabled={loading}
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

export default TenantDashboard;
