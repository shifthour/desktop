import React, { useState, useEffect } from 'react';
import { Plus, Edit, Trash2, Building2, Upload, X } from 'lucide-react';
import { pgAPI } from '../services/api';

const PGProperties = () => {
  const [pgs, setPgs] = useState([]);
  const [loading, setLoading] = useState(true);
  const [showModal, setShowModal] = useState(false);
  const [formData, setFormData] = useState({
    pg_name: '',
    address: '',
    total_floors: '',
    image_url: ''
  });
  const [editingId, setEditingId] = useState(null);
  const [imagePreview, setImagePreview] = useState('');

  useEffect(() => {
    fetchPGs();
  }, []);

  const fetchPGs = async () => {
    try {
      const response = await pgAPI.getAll();
      setPgs(response.data);
    } catch (error) {
      console.error('Error fetching PGs:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleImageUpload = (e) => {
    const file = e.target.files[0];
    if (file) {
      const reader = new FileReader();
      reader.onloadend = () => {
        const base64String = reader.result;
        setFormData({ ...formData, image_url: base64String });
        setImagePreview(base64String);
      };
      reader.readAsDataURL(file);
    }
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    try {
      if (editingId) {
        await pgAPI.update(editingId, formData);
      } else {
        await pgAPI.create(formData);
      }
      setShowModal(false);
      resetForm();
      fetchPGs();
    } catch (error) {
      alert('Error saving PG: ' + (error.response?.data?.error || error.message));
    }
  };

  const handleDelete = async (id) => {
    if (window.confirm('Are you sure you want to delete this PG?')) {
      try {
        await pgAPI.delete(id);
        fetchPGs();
      } catch (error) {
        alert('Error deleting PG: ' + (error.response?.data?.error || error.message));
      }
    }
  };

  const handleEdit = (pg) => {
    setFormData({
      pg_name: pg.pg_name,
      address: pg.address || '',
      total_floors: pg.total_floors || '',
      image_url: pg.image_url || ''
    });
    setImagePreview(pg.image_url || '');
    setEditingId(pg.id);
    setShowModal(true);
  };

  const resetForm = () => {
    setFormData({
      pg_name: '',
      address: '',
      total_floors: '',
      image_url: ''
    });
    setImagePreview('');
    setEditingId(null);
  };

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <h2 className="text-2xl font-bold text-gray-900">PG Properties</h2>
          <p className="text-gray-500 mt-1">Manage your PG locations</p>
        </div>
        <button
          onClick={() => {
            resetForm();
            setShowModal(true);
          }}
          className="btn-primary flex items-center space-x-2"
        >
          <Plus size={20} />
          <span>Add PG</span>
        </button>
      </div>

      {loading ? (
        <div className="flex items-center justify-center h-64">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-gray-900"></div>
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {pgs.map((pg) => (
            <div key={pg.id} className="card hover:shadow-lg transition-shadow">
              {pg.image_url && (
                <div className="mb-4 rounded-lg overflow-hidden h-48 bg-gray-100">
                  <img
                    src={pg.image_url}
                    alt={pg.pg_name}
                    className="w-full h-full object-cover"
                  />
                </div>
              )}
              {!pg.image_url && (
                <div className="mb-4 rounded-lg h-48 bg-gradient-to-br from-teal-400 to-blue-600 flex items-center justify-center">
                  <Building2 size={64} className="text-white opacity-50" />
                </div>
              )}

              <div className="flex justify-between items-start mb-4">
                <div>
                  <h3 className="text-xl font-bold text-gray-900">{pg.pg_name}</h3>
                  {pg.address && (
                    <p className="text-sm text-gray-500 mt-1">{pg.address}</p>
                  )}
                </div>
                <div className="flex space-x-2">
                  <button
                    onClick={() => handleEdit(pg)}
                    className="p-2 text-gray-600 hover:bg-gray-100 rounded-lg"
                    title="Edit"
                  >
                    <Edit size={18} />
                  </button>
                  <button
                    onClick={() => handleDelete(pg.id)}
                    className="p-2 text-red-600 hover:bg-red-50 rounded-lg"
                    title="Delete"
                  >
                    <Trash2 size={18} />
                  </button>
                </div>
              </div>

              <div className="space-y-2">
                <div className="flex justify-between text-sm">
                  <span className="text-gray-500">Total Floors:</span>
                  <span className="font-medium text-gray-900">
                    {pg.total_floors || 'Not specified'}
                  </span>
                </div>
              </div>
            </div>
          ))}
        </div>
      )}

      {pgs.length === 0 && !loading && (
        <div className="text-center py-12 card">
          <Building2 size={48} className="mx-auto text-gray-400 mb-4" />
          <p className="text-gray-500">No PG properties yet. Add your first PG to get started!</p>
        </div>
      )}

      {/* Add/Edit Modal */}
      {showModal && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-lg p-6 max-w-2xl w-full max-h-[90vh] overflow-y-auto">
            <h3 className="text-xl font-bold text-gray-900 mb-4">
              {editingId ? 'Edit PG Property' : 'Add New PG Property'}
            </h3>
            <form onSubmit={handleSubmit} className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  PG Name *
                </label>
                <input
                  type="text"
                  value={formData.pg_name}
                  onChange={(e) => setFormData({ ...formData, pg_name: e.target.value })}
                  className="input-field"
                  required
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Address
                </label>
                <textarea
                  value={formData.address}
                  onChange={(e) => setFormData({ ...formData, address: e.target.value })}
                  className="input-field"
                  rows="3"
                  placeholder="Enter full address"
                ></textarea>
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Total Floors
                </label>
                <input
                  type="number"
                  min="1"
                  value={formData.total_floors}
                  onChange={(e) => setFormData({ ...formData, total_floors: e.target.value })}
                  className="input-field"
                  placeholder="e.g., 3"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  PG Image
                </label>
                <div className="space-y-3">
                  <div className="flex items-center space-x-3">
                    <label className="flex-1 cursor-pointer">
                      <div className="border-2 border-dashed border-gray-300 rounded-lg p-4 hover:border-gray-400 transition-colors">
                        <div className="flex items-center justify-center space-x-2 text-gray-600">
                          <Upload size={20} />
                          <span className="text-sm">Click to upload image</span>
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
                        alt="Preview"
                        className="w-full h-48 object-cover rounded-lg"
                      />
                      <button
                        type="button"
                        onClick={() => {
                          setFormData({ ...formData, image_url: '' });
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

export default PGProperties;
