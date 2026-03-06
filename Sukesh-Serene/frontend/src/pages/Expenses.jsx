import React, { useState, useEffect } from 'react';
import { Plus, Edit, Trash2, Upload, X, Eye, Receipt, IndianRupee } from 'lucide-react';
import axios from 'axios';
import { pgAPI } from '../services/api';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5000/api';

const Expenses = () => {
  const [expenses, setExpenses] = useState([]);
  const [pgs, setPgs] = useState([]);
  const [loading, setLoading] = useState(true);
  const [showModal, setShowModal] = useState(false);
  const [formData, setFormData] = useState({
    pg_id: '',
    category: '',
    description: '',
    amount: '',
    expense_date: '',
    payment_method: '',
    bill_image: ''
  });
  const [editingId, setEditingId] = useState(null);
  const [billPreview, setBillPreview] = useState('');
  const [showBillModal, setShowBillModal] = useState(false);
  const [selectedBillImage, setSelectedBillImage] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [deleting, setDeleting] = useState(null);

  // Filter states
  const [selectedPgId, setSelectedPgId] = useState('');
  const [selectedMonth, setSelectedMonth] = useState('');
  const [selectedYear, setSelectedYear] = useState('');
  const [selectedCategory, setSelectedCategory] = useState('');

  const months = [
    'January', 'February', 'March', 'April', 'May', 'June',
    'July', 'August', 'September', 'October', 'November', 'December'
  ];

  const currentDate = new Date();
  const currentYear = currentDate.getFullYear();

  useEffect(() => {
    fetchExpenses();
    fetchPGs();
  }, []);

  const fetchExpenses = async () => {
    try {
      const token = localStorage.getItem('token');
      const response = await axios.get(`${API_URL}/expenses`, {
        headers: { Authorization: `Bearer ${token}` }
      });
      setExpenses(response.data);
    } catch (error) {
      console.error('Error fetching expenses:', error);
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

  const handleBillUpload = (e) => {
    const file = e.target.files[0];
    if (file) {
      const reader = new FileReader();
      reader.onloadend = () => {
        const base64String = reader.result;
        setFormData({ ...formData, bill_image: base64String });
        setBillPreview(base64String);
      };
      reader.readAsDataURL(file);
    }
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setSubmitting(true);
    try {
      const token = localStorage.getItem('token');
      const data = {
        ...formData,
        amount: parseFloat(formData.amount)
      };

      if (editingId) {
        await axios.put(`${API_URL}/expenses/${editingId}`, data, {
          headers: { Authorization: `Bearer ${token}` }
        });
      } else {
        await axios.post(`${API_URL}/expenses`, data, {
          headers: { Authorization: `Bearer ${token}` }
        });
      }

      setShowModal(false);
      resetForm();
      fetchExpenses();
    } catch (error) {
      alert('Error saving expense: ' + (error.response?.data?.error || error.message));
    } finally {
      setSubmitting(false);
    }
  };

  const handleDelete = async (id) => {
    if (window.confirm('Are you sure you want to delete this expense?')) {
      setDeleting(id);
      try {
        const token = localStorage.getItem('token');
        await axios.delete(`${API_URL}/expenses/${id}`, {
          headers: { Authorization: `Bearer ${token}` }
        });
        fetchExpenses();
      } catch (error) {
        alert('Error deleting expense: ' + (error.response?.data?.error || error.message));
      } finally {
        setDeleting(null);
      }
    }
  };

  const handleEdit = (expense) => {
    setFormData({
      pg_id: expense.pg_id || '',
      category: expense.category,
      description: expense.description || '',
      amount: expense.amount,
      expense_date: expense.expense_date,
      payment_method: expense.payment_method || '',
      bill_image: expense.bill_image || ''
    });
    setBillPreview(expense.bill_image || '');
    setEditingId(expense.id);
    setShowModal(true);
  };

  const resetForm = () => {
    setFormData({
      pg_id: '',
      category: '',
      description: '',
      amount: '',
      expense_date: '',
      payment_method: '',
      bill_image: ''
    });
    setBillPreview('');
    setEditingId(null);
  };

  const categories = [
    'Maintenance',
    'Utilities',
    'Repairs',
    'Cleaning',
    'Security',
    'Office Supplies',
    'Staff Salary',
    'Internet/WiFi',
    'Groceries',
    'Provisions',
    'Other'
  ];

  const paymentMethods = ['Cash', 'UPI', 'Card', 'Bank Transfer', 'Other'];

  // Filter expenses based on selected filters
  const filteredExpenses = expenses.filter(expense => {
    if (selectedPgId && expense.pg_id !== parseInt(selectedPgId)) return false;
    if (selectedCategory && expense.category !== selectedCategory) return false;

    if (selectedMonth || selectedYear) {
      const expenseDate = new Date(expense.expense_date);
      const expenseMonth = months[expenseDate.getMonth()];
      const expenseYear = expenseDate.getFullYear();

      if (selectedMonth && expenseMonth !== selectedMonth) return false;
      if (selectedYear && expenseYear !== parseInt(selectedYear)) return false;
    }

    return true;
  });

  const totalAmount = filteredExpenses.reduce((sum, e) => sum + (e.amount || 0), 0);

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <h2 className="text-2xl font-bold text-gray-900">Expenses</h2>
          <p className="text-gray-500 mt-1">Track and manage PG expenses</p>
        </div>
        <button
          onClick={() => {
            resetForm();
            setShowModal(true);
          }}
          className="btn-primary flex items-center space-x-2"
        >
          <Plus size={20} />
          <span>Add Expense</span>
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
              Category
            </label>
            <select
              value={selectedCategory}
              onChange={(e) => setSelectedCategory(e.target.value)}
              className="input-field"
            >
              <option value="">All Categories</option>
              {categories.map((cat) => (
                <option key={cat} value={cat}>{cat}</option>
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
        </div>
      </div>

      {/* Summary */}
      <div className="card">
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-8">
            <div>
              <p className="text-sm text-gray-500 font-medium">
                {selectedCategory ? `Total ${selectedCategory}` : 'Total Expenses'}
              </p>
              <p className="text-3xl font-bold text-gray-900 mt-1">
                ₹{totalAmount.toLocaleString()}
              </p>
            </div>
            <div className="border-l pl-8">
              <p className="text-sm text-gray-500 font-medium">Entries</p>
              <p className="text-3xl font-bold text-gray-900 mt-1">
                {filteredExpenses.length}
              </p>
            </div>
          </div>
          <div className="bg-red-100 p-4 rounded-lg">
            <Receipt className="text-red-600" size={32} />
          </div>
        </div>
      </div>

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
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Category</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Description</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Amount</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Payment Method</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Bill</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200">
              {filteredExpenses.map((expense) => (
                <tr key={expense.id} className="table-row">
                  <td className="px-4 py-3 text-sm text-gray-900">
                    {new Date(expense.expense_date).toLocaleDateString()}
                  </td>
                  <td className="px-4 py-3 text-sm text-gray-900">{expense.category}</td>
                  <td className="px-4 py-3 text-sm text-gray-500">{expense.description || '-'}</td>
                  <td className="px-4 py-3 text-sm font-medium text-gray-900">
                    ₹{expense.amount.toLocaleString()}
                  </td>
                  <td className="px-4 py-3 text-sm text-gray-900">{expense.payment_method || '-'}</td>
                  <td className="px-4 py-3 text-sm">
                    {expense.bill_image ? (
                      <button
                        onClick={() => {
                          setSelectedBillImage(expense.bill_image);
                          setShowBillModal(true);
                        }}
                        className="p-1.5 text-blue-600 hover:bg-blue-50 rounded"
                        title="View Bill"
                      >
                        <Eye size={16} />
                      </button>
                    ) : (
                      <span className="text-gray-400">-</span>
                    )}
                  </td>
                  <td className="px-4 py-3 text-sm">
                    <div className="flex space-x-2">
                      <button
                        onClick={() => handleEdit(expense)}
                        className="p-1.5 text-gray-600 hover:bg-gray-100 rounded"
                        title="Edit"
                      >
                        <Edit size={16} />
                      </button>
                      <button
                        onClick={() => handleDelete(expense.id)}
                        className="p-1.5 text-red-600 hover:bg-red-50 rounded disabled:opacity-50 disabled:cursor-not-allowed"
                        title="Delete"
                        disabled={deleting === expense.id}
                      >
                        {deleting === expense.id ? (
                          <svg className="animate-spin h-4 w-4" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                            <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                          </svg>
                        ) : (
                          <Trash2 size={16} />
                        )}
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {filteredExpenses.length === 0 && (
            <div className="text-center py-12">
              <Receipt size={48} className="mx-auto text-gray-400 mb-4" />
              <p className="text-gray-500">
                {expenses.length === 0
                  ? 'No expenses yet. Add your first expense to get started!'
                  : 'No expenses match the selected filters.'}
              </p>
            </div>
          )}

          {/* Total Sum Footer */}
          {filteredExpenses.length > 0 && (
            <div className="mt-4 p-4 rounded-lg border-2 bg-red-50 border-red-200">
              <div className="flex items-center justify-between">
                <div className="flex items-center space-x-3">
                  <IndianRupee className="text-red-600" size={24} />
                  <div>
                    <p className="text-sm font-medium text-gray-600">
                      {selectedCategory ? `Total ${selectedCategory} Expenses` : 'Total Expenses'}
                    </p>
                    <p className="text-xs text-gray-400">
                      {filteredExpenses.length} expense{filteredExpenses.length !== 1 ? 's' : ''}
                      {selectedMonth && ` • ${selectedMonth}`}
                      {selectedYear && ` ${selectedYear}`}
                    </p>
                  </div>
                </div>
                <p className="text-2xl font-bold text-red-700">
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
              {editingId ? 'Edit Expense' : 'Add New Expense'}
            </h3>
            <form onSubmit={handleSubmit} className="space-y-4">
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Property
                  </label>
                  <select
                    value={formData.pg_id}
                    onChange={(e) => setFormData({ ...formData, pg_id: e.target.value })}
                    className="input-field"
                  >
                    <option value="">Select Property</option>
                    {pgs.map((pg) => (
                      <option key={pg.id} value={pg.id}>
                        {pg.pg_name}
                      </option>
                    ))}
                  </select>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Category *
                  </label>
                  <select
                    value={formData.category}
                    onChange={(e) => setFormData({ ...formData, category: e.target.value })}
                    className="input-field"
                    required
                  >
                    <option value="">Select Category</option>
                    {categories.map((cat) => (
                      <option key={cat} value={cat}>
                        {cat}
                      </option>
                    ))}
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
                    placeholder="0.00"
                    step="0.01"
                    required
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Expense Date *
                  </label>
                  <input
                    type="date"
                    value={formData.expense_date}
                    onChange={(e) => setFormData({ ...formData, expense_date: e.target.value })}
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
                    <option value="">Select Method</option>
                    {paymentMethods.map((method) => (
                      <option key={method} value={method}>
                        {method}
                      </option>
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
                  placeholder="Add notes or details about this expense"
                ></textarea>
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Bill/Receipt Image
                </label>
                <div className="space-y-3">
                  <label className="cursor-pointer block">
                    <div className="border-2 border-dashed border-gray-300 rounded-lg p-4 hover:border-gray-400 transition-colors">
                      <div className="flex items-center justify-center space-x-2 text-gray-600">
                        <Upload size={20} />
                        <span className="text-sm">Click to upload bill/receipt</span>
                      </div>
                    </div>
                    <input
                      type="file"
                      accept="image/*"
                      onChange={handleBillUpload}
                      className="hidden"
                    />
                  </label>

                  {billPreview && (
                    <div className="relative">
                      <img
                        src={billPreview}
                        alt="Bill preview"
                        className="w-full h-48 object-cover rounded-lg"
                      />
                      <button
                        type="button"
                        onClick={() => {
                          setFormData({ ...formData, bill_image: '' });
                          setBillPreview('');
                        }}
                        className="absolute top-2 right-2 p-1 bg-red-500 text-white rounded-full hover:bg-red-600"
                      >
                        <X size={16} />
                      </button>
                    </div>
                  )}
                </div>
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
                      {editingId ? 'Updating...' : 'Creating...'}
                    </>
                  ) : (
                    editingId ? 'Update' : 'Create'
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

      {/* Bill View Modal */}
      {showBillModal && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-lg p-6 max-w-4xl w-full max-h-[90vh] overflow-y-auto">
            <div className="flex justify-between items-center mb-4">
              <h3 className="text-xl font-bold text-gray-900">Bill/Receipt</h3>
              <button
                onClick={() => {
                  setShowBillModal(false);
                  setSelectedBillImage('');
                }}
                className="p-2 text-gray-600 hover:bg-gray-100 rounded-full"
              >
                <X size={24} />
              </button>
            </div>
            <div className="flex justify-center">
              <img
                src={selectedBillImage}
                alt="Bill"
                className="max-w-full h-auto rounded-lg shadow-lg"
              />
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default Expenses;
