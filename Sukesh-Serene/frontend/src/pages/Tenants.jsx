import React, { useState, useEffect } from 'react';
import { Plus, Edit, Trash2, Users as UsersIcon, Upload, X, Eye, ArrowUpDown, ArrowUp, ArrowDown, IndianRupee } from 'lucide-react';
import { tenantsAPI, roomsAPI, pgAPI } from '../services/api';

const Tenants = () => {
  const [tenants, setTenants] = useState([]);
  const [rooms, setRooms] = useState([]);
  const [allRooms, setAllRooms] = useState([]);
  const [pgs, setPgs] = useState([]);
  const [loading, setLoading] = useState(true);
  const [submitting, setSubmitting] = useState(false);
  const [deleting, setDeleting] = useState(null);
  const [showModal, setShowModal] = useState(false);
  const [formData, setFormData] = useState({
    pg_id: '',
    room_id: '',
    name: '',
    email: '',
    phone: '',
    aadhar_number: '',
    aadhar_card_image: '',
    occupation: '',
    emergency_contact: '',
    address: '',
    check_in_date: '',
    monthly_rent: '',
    security_deposit: '',
    password: 'user@123',
    payment_qr_code: '',
    upi_number: '',
    remarks: '',
    security_refunded: 'no',
    security_refunded_amount: '0'
  });
  const [editingId, setEditingId] = useState(null);
  const [imagePreview, setImagePreview] = useState('');
  const [qrPreview, setQrPreview] = useState('');
  const [showAadharModal, setShowAadharModal] = useState(false);
  const [selectedAadharImage, setSelectedAadharImage] = useState('');

  // Filter states
  const [filterPgId, setFilterPgId] = useState('');
  const [filterFloor, setFilterFloor] = useState('');
  const [filterRoomId, setFilterRoomId] = useState('');

  // Sorting states
  const [sortBy, setSortBy] = useState(null); // 'room' or 'date'
  const [sortOrder, setSortOrder] = useState('asc'); // 'asc' or 'desc'

  useEffect(() => {
    fetchTenants();
    fetchRooms();
    fetchAllRooms();
    fetchPGs();
  }, []);

  const fetchTenants = async () => {
    try {
      const response = await tenantsAPI.getAll();
      setTenants(response.data);
    } catch (error) {
      console.error('Error fetching tenants:', error);
    } finally {
      setLoading(false);
    }
  };

  const fetchRooms = async () => {
    try {
      const response = await roomsAPI.getAll();
      setRooms(response.data.filter(r => r.status === 'available' || r.occupied_count < r.total_beds));
    } catch (error) {
      console.error('Error fetching rooms:', error);
    }
  };

  const fetchAllRooms = async () => {
    try {
      const response = await roomsAPI.getAll();
      setAllRooms(response.data);
    } catch (error) {
      console.error('Error fetching all rooms:', error);
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

  const handleImageUpload = (e) => {
    const file = e.target.files[0];
    if (file) {
      const reader = new FileReader();
      reader.onloadend = () => {
        const base64String = reader.result;
        setFormData({ ...formData, aadhar_card_image: base64String });
        setImagePreview(base64String);
      };
      reader.readAsDataURL(file);
    }
  };

  const handleQrUpload = (e) => {
    const file = e.target.files[0];
    if (file) {
      const reader = new FileReader();
      reader.onloadend = () => {
        const base64String = reader.result;
        setFormData({ ...formData, payment_qr_code: base64String });
        setQrPreview(base64String);
      };
      reader.readAsDataURL(file);
    }
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setSubmitting(true);
    try {
      const data = {
        ...formData,
        room_id: formData.room_id || null,
        monthly_rent: formData.monthly_rent ? parseFloat(formData.monthly_rent) : null,
        security_deposit: formData.security_deposit ? parseFloat(formData.security_deposit) : null,
        password: editingId ? formData.password : 'user@123'
      };

      if (editingId) {
        await tenantsAPI.update(editingId, data);
      } else {
        await tenantsAPI.create(data);
      }

      setShowModal(false);
      resetForm();
      fetchTenants();
      fetchRooms();
    } catch (error) {
      alert('Error saving tenant: ' + (error.response?.data?.error || error.message));
    } finally {
      setSubmitting(false);
    }
  };

  const handleDelete = async (id) => {
    if (window.confirm('Are you sure you want to remove this tenant?')) {
      setDeleting(id);
      try {
        await tenantsAPI.delete(id);
        fetchTenants();
        fetchRooms();
      } catch (error) {
        alert('Error deleting tenant: ' + (error.response?.data?.error || error.message));
      } finally {
        setDeleting(null);
      }
    }
  };

  const handleEdit = (tenant) => {
    setFormData({
      pg_id: tenant.pg_id || '',
      room_id: tenant.room_id || '',
      name: tenant.name,
      email: tenant.email || '',
      phone: tenant.phone,
      aadhar_number: tenant.aadhar_number || '',
      aadhar_card_image: tenant.aadhar_card_image || '',
      occupation: tenant.occupation || '',
      emergency_contact: tenant.emergency_contact || '',
      address: tenant.address || '',
      check_in_date: tenant.check_in_date || '',
      monthly_rent: tenant.monthly_rent || '',
      security_deposit: tenant.security_deposit || '',
      password: '',
      payment_qr_code: tenant.payment_qr_code || '',
      upi_number: tenant.upi_number || '',
      remarks: tenant.remarks || '',
      security_refunded: tenant.security_refunded || 'no',
      security_refunded_amount: tenant.security_refunded_amount || '0'
    });
    setImagePreview(tenant.aadhar_card_image || '');
    setQrPreview(tenant.payment_qr_code || '');
    setEditingId(tenant.id);
    setShowModal(true);
  };

  const resetForm = () => {
    setFormData({
      pg_id: '',
      room_id: '',
      name: '',
      email: '',
      phone: '',
      aadhar_number: '',
      aadhar_card_image: '',
      occupation: '',
      emergency_contact: '',
      address: '',
      check_in_date: '',
      monthly_rent: '',
      security_deposit: '',
      password: 'user@123',
      payment_qr_code: '',
      upi_number: '',
      remarks: '',
      security_refunded: 'no',
      security_refunded_amount: '0'
    });
    setImagePreview('');
    setQrPreview('');
    setEditingId(null);
  };

  // Get unique floors from all rooms
  const uniqueFloors = [...new Set(allRooms.map(room => room.floor).filter(Boolean))].sort((a, b) => a - b);

  // Get rooms based on selected PG and floor
  const availableRoomsForFilter = allRooms.filter(room => {
    if (filterPgId && room.pg_id !== parseInt(filterPgId)) return false;
    if (filterFloor && room.floor !== parseInt(filterFloor)) return false;
    return true;
  });

  // Handle sorting
  const handleSort = (column) => {
    if (sortBy === column) {
      // Toggle sort order if clicking same column
      setSortOrder(sortOrder === 'asc' ? 'desc' : 'asc');
    } else {
      // Set new column and default to ascending
      setSortBy(column);
      setSortOrder('asc');
    }
  };

  // Filter tenants based on selected filters
  let filteredTenants = tenants.filter(tenant => {
    if (filterPgId && tenant.pg_id !== parseInt(filterPgId)) return false;
    if (filterFloor && tenant.floor !== parseInt(filterFloor)) return false;
    if (filterRoomId && tenant.room_id !== parseInt(filterRoomId)) return false;
    return true;
  });

  // Sort tenants if sortBy is set
  if (sortBy) {
    filteredTenants = [...filteredTenants].sort((a, b) => {
      let compareA, compareB;

      if (sortBy === 'room') {
        // Sort by room number
        compareA = a.room_number || '';
        compareB = b.room_number || '';

        // Handle numeric room numbers
        const numA = parseInt(compareA);
        const numB = parseInt(compareB);

        if (!isNaN(numA) && !isNaN(numB)) {
          return sortOrder === 'asc' ? numA - numB : numB - numA;
        }

        // Fallback to string comparison
        return sortOrder === 'asc'
          ? compareA.localeCompare(compareB)
          : compareB.localeCompare(compareA);
      } else if (sortBy === 'date') {
        // Sort by check-in date
        compareA = a.check_in_date ? new Date(a.check_in_date).getTime() : 0;
        compareB = b.check_in_date ? new Date(b.check_in_date).getTime() : 0;

        return sortOrder === 'asc' ? compareA - compareB : compareB - compareA;
      }

      return 0;
    });
  }

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <h2 className="text-2xl font-bold text-gray-900">Tenants</h2>
          <p className="text-gray-500 mt-1">Manage tenant information</p>
        </div>
        <button
          onClick={() => {
            resetForm();
            setShowModal(true);
          }}
          className="btn-primary flex items-center space-x-2"
        >
          <Plus size={20} />
          <span>Add Tenant</span>
        </button>
      </div>

      {/* Filters */}
      <div className="card">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Filter by Property
            </label>
            <select
              value={filterPgId}
              onChange={(e) => {
                setFilterPgId(e.target.value);
                setFilterFloor('');
                setFilterRoomId('');
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
              Filter by Floor
            </label>
            <select
              value={filterFloor}
              onChange={(e) => {
                setFilterFloor(e.target.value);
                setFilterRoomId('');
              }}
              className="input-field"
            >
              <option value="">All Floors</option>
              {uniqueFloors.map((floor) => (
                <option key={floor} value={floor}>
                  Floor {floor}
                </option>
              ))}
            </select>
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Filter by Room
            </label>
            <select
              value={filterRoomId}
              onChange={(e) => setFilterRoomId(e.target.value)}
              className="input-field"
            >
              <option value="">All Rooms</option>
              {availableRoomsForFilter.map((room) => (
                <option key={room.id} value={room.id}>
                  Room {room.room_number}
                </option>
              ))}
            </select>
          </div>
        </div>
      </div>

      {/* Stats Cards */}
      {!loading && filteredTenants.length > 0 && (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div className="card">
            <div className="flex items-center justify-between">
              <div className="flex items-center space-x-6">
                <div>
                  <p className="text-sm text-gray-500 font-medium">Total Rent</p>
                  <p className="text-2xl font-bold text-blue-600">
                    ₹{filteredTenants.reduce((sum, t) => sum + (t.monthly_rent || 0), 0).toLocaleString()}
                  </p>
                </div>
                <div className="border-l pl-6">
                  <p className="text-sm text-gray-500 font-medium">Tenants</p>
                  <p className="text-2xl font-bold text-gray-900">
                    {filteredTenants.filter(t => t.monthly_rent).length}
                  </p>
                </div>
              </div>
              <div className="bg-blue-100 p-3 rounded-lg">
                <IndianRupee className="text-blue-600" size={24} />
              </div>
            </div>
          </div>
          <div className="card">
            <div className="flex items-center justify-between">
              <div className="flex items-center space-x-6">
                <div>
                  <p className="text-sm text-gray-500 font-medium">Total Deposit</p>
                  <p className="text-2xl font-bold text-green-600">
                    ₹{filteredTenants.reduce((sum, t) => sum + (t.security_deposit || 0), 0).toLocaleString()}
                  </p>
                </div>
                <div className="border-l pl-6">
                  <p className="text-sm text-gray-500 font-medium">Tenants</p>
                  <p className="text-2xl font-bold text-gray-900">
                    {filteredTenants.filter(t => t.security_deposit).length}
                  </p>
                </div>
              </div>
              <div className="bg-green-100 p-3 rounded-lg">
                <IndianRupee className="text-green-600" size={24} />
              </div>
            </div>
          </div>
        </div>
      )}

      {loading ? (
        <div className="flex items-center justify-center h-64">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-gray-900"></div>
        </div>
      ) : (
        <div className="card overflow-x-auto">
          <table className="min-w-full">
            <thead>
              <tr className="table-header">
                <th className="px-2 py-2 text-left text-[10px] font-medium text-gray-500 uppercase">Name</th>
                <th
                  className="px-2 py-2 text-left text-[10px] font-medium text-gray-500 uppercase cursor-pointer hover:bg-gray-100 select-none"
                  onClick={() => handleSort('room')}
                >
                  <div className="flex items-center space-x-1">
                    <span>Room</span>
                    {sortBy === 'room' ? (
                      sortOrder === 'asc' ? <ArrowUp size={12} /> : <ArrowDown size={12} />
                    ) : (
                      <ArrowUpDown size={12} className="opacity-40" />
                    )}
                  </div>
                </th>
                <th className="px-2 py-2 text-left text-[10px] font-medium text-gray-500 uppercase">Phone</th>
                <th className="px-2 py-2 text-left text-[10px] font-medium text-gray-500 uppercase">Rent</th>
                <th className="px-2 py-2 text-left text-[10px] font-medium text-gray-500 uppercase">Deposit</th>
                <th className="px-2 py-2 text-left text-[10px] font-medium text-gray-500 uppercase">Refunded</th>
                <th className="px-2 py-2 text-left text-[10px] font-medium text-gray-500 uppercase">Ref. Amt</th>
                <th
                  className="px-2 py-2 text-left text-[10px] font-medium text-gray-500 uppercase cursor-pointer hover:bg-gray-100 select-none"
                  onClick={() => handleSort('date')}
                >
                  <div className="flex items-center space-x-1">
                    <span>Join Date</span>
                    {sortBy === 'date' ? (
                      sortOrder === 'asc' ? <ArrowUp size={12} /> : <ArrowDown size={12} />
                    ) : (
                      <ArrowUpDown size={12} className="opacity-40" />
                    )}
                  </div>
                </th>
                <th className="px-2 py-2 text-left text-[10px] font-medium text-gray-500 uppercase">Emergency</th>
                <th className="px-2 py-2 text-left text-[10px] font-medium text-gray-500 uppercase">Aadhar No</th>
                <th className="px-2 py-2 text-left text-[10px] font-medium text-gray-500 uppercase">Card</th>
                <th className="px-2 py-2 text-left text-[10px] font-medium text-gray-500 uppercase">Remarks</th>
                <th className="px-2 py-2 text-left text-[10px] font-medium text-gray-500 uppercase">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200">
              {filteredTenants.map((tenant) => (
                <tr key={tenant.id} className="table-row">
                  <td className="px-2 py-2 text-xs font-medium text-gray-900">{tenant.name}</td>
                  <td className="px-2 py-2 text-xs text-gray-900">
                    {tenant.room_number || 'N/A'}
                  </td>
                  <td className="px-2 py-2 text-xs text-gray-900">{tenant.phone}</td>
                  <td className="px-2 py-2 text-xs text-gray-900">
                    {tenant.monthly_rent ? `₹${tenant.monthly_rent.toLocaleString()}` : '-'}
                  </td>
                  <td className="px-2 py-2 text-xs text-gray-900">
                    {tenant.security_deposit ? `₹${tenant.security_deposit.toLocaleString()}` : '-'}
                  </td>
                  <td className="px-2 py-2 text-xs">
                    <span className={`px-1.5 py-0.5 text-[10px] rounded-full ${
                      tenant.security_refunded === 'yes'
                        ? 'bg-green-100 text-green-800'
                        : 'bg-gray-100 text-gray-800'
                    }`}>
                      {tenant.security_refunded === 'yes' ? 'Yes' : 'No'}
                    </span>
                  </td>
                  <td className="px-2 py-2 text-xs text-gray-900">
                    {tenant.security_refunded === 'yes' && tenant.security_refunded_amount
                      ? `₹${parseFloat(tenant.security_refunded_amount).toLocaleString()}`
                      : '-'}
                  </td>
                  <td className="px-2 py-2 text-xs text-gray-900">
                    {tenant.check_in_date ? new Date(tenant.check_in_date).toLocaleDateString() : '-'}
                  </td>
                  <td className="px-2 py-2 text-xs text-gray-900">
                    {tenant.emergency_contact || '-'}
                  </td>
                  <td className="px-2 py-2 text-xs text-gray-900">
                    {tenant.aadhar_number || '-'}
                  </td>
                  <td className="px-2 py-2 text-xs">
                    {tenant.aadhar_card_image ? (
                      <button
                        onClick={() => {
                          setSelectedAadharImage(tenant.aadhar_card_image);
                          setShowAadharModal(true);
                        }}
                        className="p-1 text-blue-600 hover:bg-blue-50 rounded"
                        title="View Aadhar Card"
                      >
                        <Eye size={14} />
                      </button>
                    ) : (
                      <span className="text-gray-400">-</span>
                    )}
                  </td>
                  <td className="px-2 py-2 text-xs text-gray-900">
                    <div className="max-w-[100px] truncate" title={tenant.remarks || '-'}>
                      {tenant.remarks || '-'}
                    </div>
                  </td>
                  <td className="px-2 py-2 text-xs whitespace-nowrap">
                    <div className="flex space-x-1">
                      <button
                        onClick={() => handleEdit(tenant)}
                        className="p-1 text-gray-600 hover:bg-gray-100 rounded"
                        title="Edit"
                      >
                        <Edit size={14} />
                      </button>
                      <button
                        onClick={() => handleDelete(tenant.id)}
                        className="p-1 text-red-600 hover:bg-red-50 rounded disabled:opacity-50 disabled:cursor-not-allowed"
                        title="Delete"
                        disabled={deleting === tenant.id}
                      >
                        {deleting === tenant.id ? (
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

          {/* Footer Stats */}
          {filteredTenants.length > 0 && (
            <div className="mt-4 grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="p-4 rounded-lg border-2 bg-blue-50 border-blue-200">
                <div className="flex items-center justify-between">
                  <div className="flex items-center space-x-3">
                    <IndianRupee className="text-blue-600" size={24} />
                    <div>
                      <p className="text-sm font-medium text-blue-800">Total Rent</p>
                      <p className="text-xs text-blue-600">
                        {filteredTenants.filter(t => t.monthly_rent).length} tenant{filteredTenants.filter(t => t.monthly_rent).length !== 1 ? 's' : ''}
                      </p>
                    </div>
                  </div>
                  <p className="text-2xl font-bold text-blue-700">
                    ₹{filteredTenants.reduce((sum, t) => sum + (t.monthly_rent || 0), 0).toLocaleString()}
                  </p>
                </div>
              </div>
              <div className="p-4 rounded-lg border-2 bg-green-50 border-green-200">
                <div className="flex items-center justify-between">
                  <div className="flex items-center space-x-3">
                    <IndianRupee className="text-green-600" size={24} />
                    <div>
                      <p className="text-sm font-medium text-green-800">Total Deposit</p>
                      <p className="text-xs text-green-600">
                        {filteredTenants.filter(t => t.security_deposit).length} tenant{filteredTenants.filter(t => t.security_deposit).length !== 1 ? 's' : ''}
                      </p>
                    </div>
                  </div>
                  <p className="text-2xl font-bold text-green-700">
                    ₹{filteredTenants.reduce((sum, t) => sum + (t.security_deposit || 0), 0).toLocaleString()}
                  </p>
                </div>
              </div>
            </div>
          )}

          {filteredTenants.length === 0 && (
            <div className="text-center py-12">
              <UsersIcon size={48} className="mx-auto text-gray-400 mb-4" />
              <p className="text-gray-500">
                {tenants.length === 0
                  ? 'No tenants yet. Add your first tenant to get started!'
                  : 'No tenants match the selected filters.'}
              </p>
            </div>
          )}
        </div>
      )}

      {/* Add/Edit Modal */}
      {showModal && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-lg p-6 max-w-2xl w-full max-h-[90vh] overflow-y-auto">
            <h3 className="text-xl font-bold text-gray-900 mb-4">
              {editingId ? 'Edit Tenant' : 'Add New Tenant'}
            </h3>
            <form onSubmit={handleSubmit} className="space-y-4">
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Name *
                  </label>
                  <input
                    type="text"
                    value={formData.name}
                    onChange={(e) => setFormData({ ...formData, name: e.target.value })}
                    className="input-field"
                    required
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Phone *
                  </label>
                  <input
                    type="tel"
                    value={formData.phone}
                    onChange={(e) => setFormData({ ...formData, phone: e.target.value })}
                    className="input-field"
                    required
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Email
                  </label>
                  <input
                    type="email"
                    value={formData.email}
                    onChange={(e) => setFormData({ ...formData, email: e.target.value })}
                    className="input-field"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    PG Property *
                  </label>
                  <select
                    value={formData.pg_id}
                    onChange={(e) => setFormData({ ...formData, pg_id: e.target.value, room_id: '' })}
                    className="input-field"
                    required
                  >
                    <option value="">Select PG Property</option>
                    {pgs.map((pg) => (
                      <option key={pg.id} value={pg.id}>
                        {pg.pg_name}
                      </option>
                    ))}
                  </select>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Room
                  </label>
                  <select
                    value={formData.room_id}
                    onChange={(e) => setFormData({ ...formData, room_id: e.target.value })}
                    className="input-field"
                    disabled={!formData.pg_id}
                  >
                    <option value="">No Room Assigned</option>
                    {rooms
                      .filter(room => !formData.pg_id || room.pg_id === parseInt(formData.pg_id))
                      .map((room) => (
                        <option key={room.id} value={room.id}>
                          Room {room.room_number} ({room.room_type}) - {room.occupied_count || 0}/{room.total_beds} occupied
                        </option>
                      ))}
                  </select>
                  {!formData.pg_id && (
                    <p className="text-xs text-gray-500 mt-1">Please select a PG property first</p>
                  )}
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Aadhar Number
                  </label>
                  <input
                    type="text"
                    value={formData.aadhar_number}
                    onChange={(e) => setFormData({ ...formData, aadhar_number: e.target.value })}
                    className="input-field"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Occupation
                  </label>
                  <input
                    type="text"
                    value={formData.occupation}
                    onChange={(e) => setFormData({ ...formData, occupation: e.target.value })}
                    className="input-field"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Emergency Contact
                  </label>
                  <input
                    type="tel"
                    value={formData.emergency_contact}
                    onChange={(e) => setFormData({ ...formData, emergency_contact: e.target.value })}
                    className="input-field"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Joining Date
                  </label>
                  <input
                    type="date"
                    value={formData.check_in_date}
                    onChange={(e) => setFormData({ ...formData, check_in_date: e.target.value })}
                    className="input-field"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Monthly Rent
                  </label>
                  <input
                    type="number"
                    value={formData.monthly_rent}
                    onChange={(e) => setFormData({ ...formData, monthly_rent: e.target.value })}
                    className="input-field"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Security Deposit
                  </label>
                  <input
                    type="number"
                    value={formData.security_deposit}
                    onChange={(e) => setFormData({ ...formData, security_deposit: e.target.value })}
                    className="input-field"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Security Refunded
                  </label>
                  <select
                    value={formData.security_refunded}
                    onChange={(e) => setFormData({ ...formData, security_refunded: e.target.value })}
                    className="input-field"
                  >
                    <option value="no">No</option>
                    <option value="yes">Yes</option>
                  </select>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Security Refunded Amount
                  </label>
                  <input
                    type="number"
                    value={formData.security_refunded_amount}
                    onChange={(e) => setFormData({ ...formData, security_refunded_amount: e.target.value })}
                    className="input-field"
                    disabled={formData.security_refunded === 'no'}
                  />
                </div>
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Address
                </label>
                <textarea
                  value={formData.address}
                  onChange={(e) => setFormData({ ...formData, address: e.target.value })}
                  className="input-field"
                  rows="2"
                ></textarea>
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
                  placeholder="Add any additional notes or remarks about the tenant..."
                ></textarea>
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Aadhar Card Image
                </label>
                <div className="space-y-3">
                  <div className="flex items-center space-x-3">
                    <label className="flex-1 cursor-pointer">
                      <div className="border-2 border-dashed border-gray-300 rounded-lg p-4 hover:border-gray-400 transition-colors">
                        <div className="flex items-center justify-center space-x-2 text-gray-600">
                          <Upload size={20} />
                          <span className="text-sm">Click to upload Aadhar card</span>
                        </div>
                      </div>
                      <input
                        type="file"
                        accept="image/*"
                        onChange={handleImageUpload}
                        className="hidden"
                      />
                    </label>
                  </div>

                  {imagePreview && (
                    <div className="relative">
                      <img
                        src={imagePreview}
                        alt="Aadhar card preview"
                        className="w-full h-48 object-cover rounded-lg"
                      />
                      <button
                        type="button"
                        onClick={() => {
                          setFormData({ ...formData, aadhar_card_image: '' });
                          setImagePreview('');
                        }}
                        className="absolute top-2 right-2 p-1 bg-red-500 text-white rounded-full hover:bg-red-600"
                      >
                        <X size={16} />
                      </button>
                    </div>
                  )}
                </div>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                {!editingId && (
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-2">
                      Default Password
                    </label>
                    <input
                      type="text"
                      value="user@123"
                      className="input-field bg-gray-100 cursor-not-allowed"
                      disabled
                    />
                    <p className="text-xs text-gray-500 mt-1">
                      This password will be used for tenant's first login. Tenant can change it after login.
                    </p>
                  </div>
                )}

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Payment QR Code
                  </label>
                  <div className="space-y-3">
                    <label className="cursor-pointer block">
                      <div className="border-2 border-dashed border-gray-300 rounded-lg p-3 hover:border-gray-400 transition-colors">
                        <div className="flex items-center justify-center space-x-2 text-gray-600">
                          <Upload size={18} />
                          <span className="text-sm">Upload QR Code</span>
                        </div>
                      </div>
                      <input
                        type="file"
                        accept="image/*"
                        onChange={handleQrUpload}
                        className="hidden"
                      />
                    </label>

                    {qrPreview && (
                      <div className="relative inline-block">
                        <img
                          src={qrPreview}
                          alt="QR code preview"
                          className="w-32 h-32 object-cover rounded-lg border border-gray-300"
                        />
                        <button
                          type="button"
                          onClick={() => {
                            setFormData({ ...formData, payment_qr_code: '' });
                            setQrPreview('');
                          }}
                          className="absolute -top-2 -right-2 p-1 bg-red-500 text-white rounded-full hover:bg-red-600"
                        >
                          <X size={14} />
                        </button>
                      </div>
                    )}
                  </div>
                </div>
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  UPI Number
                </label>
                <input
                  type="text"
                  value={formData.upi_number}
                  onChange={(e) => setFormData({ ...formData, upi_number: e.target.value })}
                  className="input-field"
                  placeholder="e.g., 9876543210@upi or name@bank"
                />
                <p className="text-xs text-gray-500 mt-1">
                  This UPI ID will be displayed to tenant for payments
                </p>
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

      {/* Aadhar Card View Modal */}
      {showAadharModal && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-lg p-6 max-w-4xl w-full max-h-[90vh] overflow-y-auto">
            <div className="flex justify-between items-center mb-4">
              <h3 className="text-xl font-bold text-gray-900">Aadhar Card</h3>
              <button
                onClick={() => {
                  setShowAadharModal(false);
                  setSelectedAadharImage('');
                }}
                className="p-2 text-gray-600 hover:bg-gray-100 rounded-full"
              >
                <X size={24} />
              </button>
            </div>
            <div className="flex justify-center">
              <img
                src={selectedAadharImage}
                alt="Aadhar Card"
                className="max-w-full h-auto rounded-lg shadow-lg"
              />
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default Tenants;
