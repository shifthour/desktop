import React, { useState, useEffect } from 'react';
import { Plus, Edit, Trash2, Home, Bed, Users } from 'lucide-react';
import { roomsAPI, pgAPI } from '../services/api';

const Rooms = () => {
  const [rooms, setRooms] = useState([]);
  const [pgs, setPgs] = useState([]);
  const [loading, setLoading] = useState(true);
  const [showModal, setShowModal] = useState(false);
  const [formData, setFormData] = useState({
    pg_id: '',
    room_number: '',
    floor: '',
    rent_amount: '',
    total_beds: 1
  });
  const [editingId, setEditingId] = useState(null);

  // Filter states
  const [filterPgId, setFilterPgId] = useState('');
  const [filterFloor, setFilterFloor] = useState('');
  const [filterStatus, setFilterStatus] = useState('');

  useEffect(() => {
    fetchRooms();
    fetchPGs();
  }, []);

  const fetchRooms = async () => {
    try {
      const response = await roomsAPI.getAll();
      setRooms(response.data);
    } catch (error) {
      console.error('Error fetching rooms:', error);
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

  const handleSubmit = async (e) => {
    e.preventDefault();
    try {
      if (editingId) {
        await roomsAPI.update(editingId, formData);
      } else {
        await roomsAPI.create(formData);
      }
      setShowModal(false);
      resetForm();
      fetchRooms();
    } catch (error) {
      alert('Error saving room: ' + (error.response?.data?.error || error.message));
    }
  };

  const handleDelete = async (id) => {
    if (window.confirm('Are you sure you want to delete this room?')) {
      try {
        await roomsAPI.delete(id);
        fetchRooms();
      } catch (error) {
        alert('Error deleting room: ' + (error.response?.data?.error || error.message));
      }
    }
  };

  const handleEdit = (room) => {
    setFormData({
      pg_id: room.pg_id || '',
      room_number: room.room_number,
      floor: room.floor || '',
      rent_amount: room.rent_amount || '',
      total_beds: room.total_beds
    });
    setEditingId(room.id);
    setShowModal(true);
  };

  const resetForm = () => {
    setFormData({
      pg_id: '',
      room_number: '',
      floor: '',
      rent_amount: '',
      total_beds: 1
    });
    setEditingId(null);
  };

  // Get unique floor numbers from rooms
  const uniqueFloors = [...new Set(rooms.map(room => room.floor).filter(Boolean))].sort((a, b) => a - b);

  // Filter rooms based on selected filters
  const filteredRooms = rooms.filter(room => {
    if (filterPgId && room.pg_id !== parseInt(filterPgId)) return false;
    if (filterFloor && room.floor !== parseInt(filterFloor)) return false;

    // Filter by status (available/occupied)
    if (filterStatus) {
      const occupiedCount = room.occupied_count || 0;
      const totalBeds = room.total_beds || 0;
      const isAvailable = occupiedCount < totalBeds;

      if (filterStatus === 'available' && !isAvailable) return false;
      if (filterStatus === 'occupied' && isAvailable) return false;
    }

    return true;
  });

  // Calculate stats based on filtered rooms
  const totalBeds = filteredRooms.reduce((sum, room) => sum + (room.total_beds || 0), 0);
  const occupiedBeds = filteredRooms.reduce((sum, room) => sum + (room.occupied_count || 0), 0);
  const emptyBeds = totalBeds - occupiedBeds;
  const occupancyRate = totalBeds > 0 ? ((occupiedBeds / totalBeds) * 100).toFixed(1) : 0;

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <h2 className="text-2xl font-bold text-gray-900">Rooms</h2>
          <p className="text-gray-500 mt-1">Manage your PG rooms</p>
        </div>
        <button
          onClick={() => {
            resetForm();
            setShowModal(true);
          }}
          className="btn-primary flex items-center space-x-2"
        >
          <Plus size={20} />
          <span>Add Room</span>
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
              onChange={(e) => setFilterPgId(e.target.value)}
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
              onChange={(e) => setFilterFloor(e.target.value)}
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
              Filter by Status
            </label>
            <select
              value={filterStatus}
              onChange={(e) => setFilterStatus(e.target.value)}
              className="input-field"
            >
              <option value="">All Rooms</option>
              <option value="available">Available</option>
              <option value="occupied">Occupied</option>
            </select>
          </div>
        </div>
      </div>

      {/* Stats Cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <div className="card">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-gray-500 font-medium">Total Beds</p>
              <p className="text-2xl font-bold text-gray-900">{totalBeds}</p>
            </div>
            <div className="bg-blue-100 p-3 rounded-lg">
              <Bed className="text-blue-600" size={24} />
            </div>
          </div>
        </div>
        <div className="card">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-gray-500 font-medium">Occupied Beds</p>
              <p className="text-2xl font-bold text-green-600">{occupiedBeds}</p>
            </div>
            <div className="bg-green-100 p-3 rounded-lg">
              <Users className="text-green-600" size={24} />
            </div>
          </div>
        </div>
        <div className="card">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-gray-500 font-medium">Empty Beds</p>
              <p className="text-2xl font-bold text-red-600">{emptyBeds}</p>
            </div>
            <div className="bg-red-100 p-3 rounded-lg">
              <Bed className="text-red-600" size={24} />
            </div>
          </div>
        </div>
        <div className="card">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-gray-500 font-medium">Occupancy Rate</p>
              <p className="text-2xl font-bold text-purple-600">{occupancyRate}%</p>
            </div>
            <div className="bg-purple-100 p-3 rounded-lg">
              <Home className="text-purple-600" size={24} />
            </div>
          </div>
        </div>
      </div>

      {loading ? (
        <div className="flex items-center justify-center h-64">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-gray-900"></div>
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {filteredRooms.map((room) => (
            <div key={room.id} className="card hover:shadow-lg transition-shadow">
              <div className="flex justify-between items-start mb-4">
                <div>
                  <h3 className="text-xl font-bold text-gray-900">Room {room.room_number}</h3>
                  {room.pg_name && (
                    <p className="text-xs text-blue-600 font-medium mt-1">📍 {room.pg_name}</p>
                  )}
                  {room.floor && <p className="text-sm text-gray-500">Floor {room.floor}</p>}
                </div>
                <div className="flex space-x-2">
                  <button
                    onClick={() => handleEdit(room)}
                    className="p-2 text-gray-600 hover:bg-gray-100 rounded-lg"
                    title="Edit"
                  >
                    <Edit size={18} />
                  </button>
                  <button
                    onClick={() => handleDelete(room.id)}
                    className="p-2 text-red-600 hover:bg-red-50 rounded-lg"
                    title="Delete"
                  >
                    <Trash2 size={18} />
                  </button>
                </div>
              </div>

              <div className="space-y-2">
                <div className="flex justify-between text-sm">
                  <span className="text-gray-500">Capacity:</span>
                  <span className="font-medium text-gray-900">{room.total_beds} beds</span>
                </div>
                <div className="flex justify-between text-sm">
                  <span className="text-gray-500">Occupied:</span>
                  <span className="font-medium text-green-600">{room.occupied_count || 0}</span>
                </div>
                <div className="flex justify-between text-sm">
                  <span className="text-gray-500">Rent:</span>
                  <span className="font-medium text-gray-900">
                    {room.rent_amount ? `₹${room.rent_amount.toLocaleString()}` : 'Not set'}
                  </span>
                </div>
              </div>
            </div>
          ))}
        </div>
      )}

      {filteredRooms.length === 0 && !loading && (
        <div className="text-center py-12 card">
          <Home size={48} className="mx-auto text-gray-400 mb-4" />
          <p className="text-gray-500">
            {rooms.length === 0
              ? 'No rooms yet. Add your first room to get started!'
              : 'No rooms match the selected filters.'}
          </p>
        </div>
      )}

      {/* Add/Edit Modal */}
      {showModal && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg p-6 max-w-md w-full mx-4 max-h-[90vh] overflow-y-auto">
            <h3 className="text-xl font-bold text-gray-900 mb-4">
              {editingId ? 'Edit Room' : 'Add New Room'}
            </h3>
            <form onSubmit={handleSubmit} className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Select PG
                </label>
                <select
                  value={formData.pg_id}
                  onChange={(e) => setFormData({ ...formData, pg_id: e.target.value })}
                  className="input-field"
                >
                  <option value="">-- Select PG --</option>
                  {pgs.map((pg) => (
                    <option key={pg.id} value={pg.id}>
                      {pg.pg_name}
                    </option>
                  ))}
                </select>
                {pgs.length === 0 && (
                  <p className="text-xs text-orange-600 mt-1">
                    Please add a PG property first from PG Properties page
                  </p>
                )}
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Room Number *
                </label>
                <input
                  type="text"
                  value={formData.room_number}
                  onChange={(e) => setFormData({ ...formData, room_number: e.target.value })}
                  className="input-field"
                  required
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Floor
                </label>
                <input
                  type="number"
                  value={formData.floor}
                  onChange={(e) => setFormData({ ...formData, floor: e.target.value })}
                  className="input-field"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Rent Amount
                </label>
                <input
                  type="number"
                  value={formData.rent_amount}
                  onChange={(e) => setFormData({ ...formData, rent_amount: e.target.value })}
                  className="input-field"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Total Beds *
                </label>
                <input
                  type="number"
                  min="1"
                  value={formData.total_beds}
                  onChange={(e) => setFormData({ ...formData, total_beds: parseInt(e.target.value) })}
                  className="input-field"
                  required
                />
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

export default Rooms;
