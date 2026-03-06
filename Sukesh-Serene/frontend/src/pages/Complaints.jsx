import React, { useState, useEffect } from 'react';
import { MessageSquare, CheckCircle, Clock, AlertCircle, X } from 'lucide-react';
import axios from 'axios';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5000/api';

const Complaints = () => {
  const [complaints, setComplaints] = useState([]);
  const [loading, setLoading] = useState(true);
  const [filter, setFilter] = useState('all');
  const [showResolveModal, setShowResolveModal] = useState(false);
  const [selectedComplaint, setSelectedComplaint] = useState(null);
  const [resolutionDescription, setResolutionDescription] = useState('');
  const [resolving, setResolving] = useState(false);

  useEffect(() => {
    fetchComplaints();
  }, [filter]);

  const fetchComplaints = async () => {
    try {
      setLoading(true);
      const token = localStorage.getItem('token');
      const params = filter !== 'all' ? { status: filter } : {};
      const response = await axios.get(`${API_URL}/complaints`, {
        params,
        headers: { Authorization: `Bearer ${token}` }
      });
      setComplaints(response.data);
    } catch (error) {
      console.error('Error fetching complaints:', error);
      alert('Error fetching complaints: ' + (error.response?.data?.error || error.message));
    } finally {
      setLoading(false);
    }
  };

  const handleResolve = async (e) => {
    e.preventDefault();

    if (!resolutionDescription || resolutionDescription.trim() === '') {
      alert('Please enter resolution description');
      return;
    }

    setResolving(true);

    try {
      const token = localStorage.getItem('token');
      const userData = JSON.parse(localStorage.getItem('user') || '{}');

      await axios.put(
        `${API_URL}/complaints/${selectedComplaint.id}/resolve`,
        {
          resolution_description: resolutionDescription,
          resolved_by: userData.id
        },
        {
          headers: { Authorization: `Bearer ${token}` }
        }
      );

      alert('Complaint resolved successfully!');
      setShowResolveModal(false);
      setSelectedComplaint(null);
      setResolutionDescription('');
      fetchComplaints();
    } catch (error) {
      alert('Error resolving complaint: ' + (error.response?.data?.error || error.message));
    } finally {
      setResolving(false);
    }
  };

  const handleStatusChange = async (complaintId, newStatus) => {
    try {
      const token = localStorage.getItem('token');
      await axios.put(
        `${API_URL}/complaints/${complaintId}/status`,
        { status: newStatus },
        {
          headers: { Authorization: `Bearer ${token}` }
        }
      );
      alert(`Complaint status updated to ${newStatus}`);
      fetchComplaints();
    } catch (error) {
      alert('Error updating status: ' + (error.response?.data?.error || error.message));
    }
  };

  const getStatusBadge = (status) => {
    const styles = {
      raised: 'bg-yellow-100 text-yellow-800',
      in_progress: 'bg-blue-100 text-blue-800',
      resolved: 'bg-green-100 text-green-800'
    };
    const icons = {
      raised: <AlertCircle size={14} />,
      in_progress: <Clock size={14} />,
      resolved: <CheckCircle size={14} />
    };
    const labels = {
      raised: 'Raised',
      in_progress: 'In Progress',
      resolved: 'Resolved'
    };

    return (
      <span className={`inline-flex items-center space-x-1 px-3 py-1 rounded-full text-xs font-medium ${styles[status]}`}>
        {icons[status]}
        <span>{labels[status]}</span>
      </span>
    );
  };

  const statusCounts = {
    all: complaints.length,
    raised: complaints.filter(c => c.status === 'raised').length,
    in_progress: complaints.filter(c => c.status === 'in_progress').length,
    resolved: complaints.filter(c => c.status === 'resolved').length
  };

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <h2 className="text-2xl font-bold text-gray-900">Complaints Management</h2>
          <p className="text-gray-500 mt-1">Review and resolve tenant complaints</p>
        </div>
      </div>

      {/* Stats Cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <div className="bg-white rounded-lg shadow p-4">
          <p className="text-sm text-gray-500">Total Complaints</p>
          <p className="text-2xl font-bold text-gray-900 mt-1">{statusCounts.all}</p>
        </div>
        <div className="bg-yellow-50 rounded-lg shadow p-4">
          <p className="text-sm text-yellow-700">Raised</p>
          <p className="text-2xl font-bold text-yellow-900 mt-1">{statusCounts.raised}</p>
        </div>
        <div className="bg-blue-50 rounded-lg shadow p-4">
          <p className="text-sm text-blue-700">In Progress</p>
          <p className="text-2xl font-bold text-blue-900 mt-1">{statusCounts.in_progress}</p>
        </div>
        <div className="bg-green-50 rounded-lg shadow p-4">
          <p className="text-sm text-green-700">Resolved</p>
          <p className="text-2xl font-bold text-green-900 mt-1">{statusCounts.resolved}</p>
        </div>
      </div>

      {/* Filter Tabs */}
      <div className="card">
        <div className="flex space-x-2 border-b border-gray-200">
          {[
            { key: 'all', label: 'All' },
            { key: 'raised', label: 'Raised' },
            { key: 'in_progress', label: 'In Progress' },
            { key: 'resolved', label: 'Resolved' }
          ].map((tab) => (
            <button
              key={tab.key}
              onClick={() => setFilter(tab.key)}
              className={`px-4 py-2 font-medium text-sm transition-colors ${
                filter === tab.key
                  ? 'border-b-2 border-teal-600 text-teal-600'
                  : 'text-gray-500 hover:text-gray-700'
              }`}
            >
              {tab.label} ({statusCounts[tab.key]})
            </button>
          ))}
        </div>
      </div>

      {/* Complaints List */}
      {loading ? (
        <div className="flex items-center justify-center h-64">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-gray-900"></div>
        </div>
      ) : (
        <div className="space-y-4">
          {complaints.length === 0 ? (
            <div className="card text-center py-12">
              <MessageSquare size={48} className="mx-auto text-gray-400 mb-4" />
              <p className="text-gray-500">
                {filter === 'all'
                  ? 'No complaints yet.'
                  : `No ${filter.replace('_', ' ')} complaints.`}
              </p>
            </div>
          ) : (
            complaints.map((complaint) => (
              <div key={complaint.id} className="card">
                <div className="flex justify-between items-start mb-4">
                  <div className="flex-1">
                    <div className="flex items-center space-x-3 mb-2">
                      <h3 className="text-lg font-semibold text-gray-900">
                        {complaint.tenant_name || 'Unknown Tenant'}
                      </h3>
                      {getStatusBadge(complaint.status)}
                    </div>
                    <div className="text-sm text-gray-600 space-y-1">
                      <p>
                        <span className="font-medium">Room:</span> {complaint.room_number || 'N/A'}
                        {complaint.pg_name && ` - ${complaint.pg_name}`}
                      </p>
                      <p>
                        <span className="font-medium">Phone:</span> {complaint.tenant_phone || 'N/A'}
                      </p>
                      <p>
                        <span className="font-medium">Raised on:</span>{' '}
                        {new Date(complaint.created_at).toLocaleString()}
                      </p>
                    </div>
                  </div>
                </div>

                <div className="mb-4">
                  <p className="text-sm font-medium text-gray-700 mb-1">Complaint:</p>
                  <p className="text-sm text-gray-900 bg-gray-50 p-3 rounded">
                    {complaint.complaint_description}
                  </p>
                </div>

                {complaint.status === 'resolved' && complaint.resolution_description && (
                  <div className="mb-4 bg-green-50 p-3 rounded">
                    <p className="text-sm font-medium text-green-700 mb-1">Resolution:</p>
                    <p className="text-sm text-gray-900">{complaint.resolution_description}</p>
                    <p className="text-xs text-gray-600 mt-2">
                      Resolved on {new Date(complaint.resolved_at).toLocaleString()}
                      {complaint.resolved_by_name && ` by ${complaint.resolved_by_name}`}
                    </p>
                  </div>
                )}

                {complaint.status !== 'resolved' && (
                  <div className="flex space-x-2 pt-3 border-t border-gray-200">
                    {complaint.status === 'raised' && (
                      <button
                        onClick={() => handleStatusChange(complaint.id, 'in_progress')}
                        className="px-4 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700 transition-colors"
                      >
                        Mark In Progress
                      </button>
                    )}
                    <button
                      onClick={() => {
                        setSelectedComplaint(complaint);
                        setShowResolveModal(true);
                      }}
                      className="px-4 py-2 bg-green-600 text-white rounded-lg text-sm font-medium hover:bg-green-700 transition-colors"
                    >
                      Resolve Complaint
                    </button>
                  </div>
                )}
              </div>
            ))
          )}
        </div>
      )}

      {/* Resolve Modal */}
      {showResolveModal && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-lg p-6 max-w-2xl w-full max-h-[90vh] overflow-y-auto">
            <div className="flex justify-between items-center mb-4">
              <h3 className="text-xl font-bold text-gray-900">Resolve Complaint</h3>
              <button
                onClick={() => {
                  setShowResolveModal(false);
                  setSelectedComplaint(null);
                  setResolutionDescription('');
                }}
                className="p-2 text-gray-600 hover:bg-gray-100 rounded-full"
              >
                <X size={24} />
              </button>
            </div>

            {selectedComplaint && (
              <div className="mb-4 bg-gray-50 p-3 rounded">
                <p className="text-sm font-medium text-gray-700 mb-1">Complaint:</p>
                <p className="text-sm text-gray-900">{selectedComplaint.complaint_description}</p>
              </div>
            )}

            <form onSubmit={handleResolve}>
              <div className="mb-4">
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Resolution Description *
                </label>
                <textarea
                  value={resolutionDescription}
                  onChange={(e) => setResolutionDescription(e.target.value)}
                  className="input-field"
                  rows="6"
                  placeholder="Describe how the complaint was resolved..."
                  required
                ></textarea>
              </div>

              <div className="flex space-x-3 pt-4">
                <button
                  type="submit"
                  disabled={resolving}
                  className="btn-primary flex-1 disabled:opacity-50"
                >
                  {resolving ? 'Resolving...' : 'Mark as Resolved'}
                </button>
                <button
                  type="button"
                  onClick={() => {
                    setShowResolveModal(false);
                    setSelectedComplaint(null);
                    setResolutionDescription('');
                  }}
                  className="btn-secondary flex-1"
                  disabled={resolving}
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

export default Complaints;
