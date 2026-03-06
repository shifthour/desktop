"use client";

import { useState, useEffect } from "react";
import { Button } from "@/components/ui/button";
import { Modal } from "@/components/ui/modal";
import type { UserProfile } from "@/lib/types";

export function UserManagement() {
  const [users, setUsers] = useState<UserProfile[]>([]);
  const [loading, setLoading] = useState(true);
  const [showCreateModal, setShowCreateModal] = useState(false);
  const [creating, setCreating] = useState(false);
  const [error, setError] = useState("");

  // Form state
  const [fullName, setFullName] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [role, setRole] = useState<"admin" | "user">("user");

  const fetchUsers = async () => {
    try {
      const res = await fetch("/api/admin/users");
      if (!res.ok) throw new Error("Failed to fetch users");
      const data = await res.json();
      setUsers(data.users);
    } catch {
      setError("Failed to load users");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchUsers();
  }, []);

  const handleCreate = async (e: React.FormEvent) => {
    e.preventDefault();
    setCreating(true);
    setError("");

    try {
      const res = await fetch("/api/admin/users", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ full_name: fullName, email, password, role }),
      });

      if (!res.ok) {
        const data = await res.json();
        throw new Error(data.error || "Failed to create user");
      }

      setShowCreateModal(false);
      setFullName("");
      setEmail("");
      setPassword("");
      setRole("user");
      fetchUsers();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to create user");
    } finally {
      setCreating(false);
    }
  };

  const handleToggleActive = async (
    userId: string,
    currentStatus: boolean
  ) => {
    try {
      const res = await fetch(`/api/admin/users/${userId}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ is_active: !currentStatus }),
      });

      if (!res.ok) throw new Error("Failed to update user");
      fetchUsers();
    } catch {
      setError("Failed to update user status");
    }
  };

  if (loading) {
    return (
      <div className="flex justify-center py-12">
        <div className="h-8 w-8 animate-spin rounded-full border-2 border-brand-purple border-t-transparent" />
      </div>
    );
  }

  return (
    <div>
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h3 className="text-lg font-semibold text-white">User Management</h3>
          <p className="text-sm text-gray-500 mt-1">
            {users.length} {users.length === 1 ? "user" : "users"} total
          </p>
        </div>
        <Button
          variant="primary"
          size="sm"
          onClick={() => setShowCreateModal(true)}
        >
          + Create User
        </Button>
      </div>

      {error && (
        <div className="mb-4 rounded-xl border border-red-500/20 bg-red-500/5 px-4 py-3 text-sm text-red-400">
          {error}
        </div>
      )}

      {/* Users Table */}
      <div className="glass-card rounded-2xl overflow-hidden">
        <table className="w-full">
          <thead>
            <tr className="border-b border-dark-border/50">
              <th className="px-5 py-3 text-left text-[10px] font-semibold uppercase tracking-wider text-gray-500">
                Name
              </th>
              <th className="px-5 py-3 text-left text-[10px] font-semibold uppercase tracking-wider text-gray-500">
                Email
              </th>
              <th className="px-5 py-3 text-left text-[10px] font-semibold uppercase tracking-wider text-gray-500">
                Role
              </th>
              <th className="px-5 py-3 text-left text-[10px] font-semibold uppercase tracking-wider text-gray-500">
                Status
              </th>
              <th className="px-5 py-3 text-left text-[10px] font-semibold uppercase tracking-wider text-gray-500">
                Created
              </th>
              <th className="px-5 py-3 text-right text-[10px] font-semibold uppercase tracking-wider text-gray-500">
                Actions
              </th>
            </tr>
          </thead>
          <tbody>
            {users.map((user) => (
              <tr
                key={user.id}
                className="border-b border-dark-border/30 hover:bg-dark-hover/50 transition-colors"
              >
                <td className="px-5 py-3">
                  <div className="flex items-center gap-3">
                    <div className="flex h-8 w-8 items-center justify-center rounded-full gradient-brand text-xs font-bold text-white">
                      {user.full_name
                        .split(" ")
                        .map((n) => n[0])
                        .join("")
                        .toUpperCase()
                        .slice(0, 2)}
                    </div>
                    <span className="text-sm font-medium text-white">
                      {user.full_name}
                    </span>
                  </div>
                </td>
                <td className="px-5 py-3 text-sm text-gray-400">
                  {user.email}
                </td>
                <td className="px-5 py-3">
                  <span
                    className={`inline-flex items-center rounded-full px-2.5 py-0.5 text-[10px] font-semibold uppercase tracking-wider ${
                      user.role === "admin"
                        ? "bg-brand-purple/10 text-brand-purple-light border border-brand-purple/20"
                        : "bg-gray-500/10 text-gray-400 border border-gray-500/20"
                    }`}
                  >
                    {user.role}
                  </span>
                </td>
                <td className="px-5 py-3">
                  <span
                    className={`inline-flex items-center gap-1.5 text-xs ${
                      user.is_active ? "text-green-400" : "text-red-400"
                    }`}
                  >
                    <span
                      className={`h-1.5 w-1.5 rounded-full ${
                        user.is_active ? "bg-green-400" : "bg-red-400"
                      }`}
                    />
                    {user.is_active ? "Active" : "Deactivated"}
                  </span>
                </td>
                <td className="px-5 py-3 text-xs text-gray-500">
                  {new Date(user.created_at).toLocaleDateString()}
                </td>
                <td className="px-5 py-3 text-right">
                  <button
                    onClick={() =>
                      handleToggleActive(user.id, user.is_active)
                    }
                    className={`text-xs font-medium transition-colors ${
                      user.is_active
                        ? "text-red-400 hover:text-red-300"
                        : "text-green-400 hover:text-green-300"
                    }`}
                  >
                    {user.is_active ? "Deactivate" : "Activate"}
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>

        {users.length === 0 && (
          <div className="py-12 text-center text-sm text-gray-500">
            No users found
          </div>
        )}
      </div>

      {/* Create User Modal */}
      <Modal
        isOpen={showCreateModal}
        onClose={() => setShowCreateModal(false)}
        title="Create New User"
      >
        <form onSubmit={handleCreate} className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-gray-300 mb-1.5">
              Full Name
            </label>
            <input
              type="text"
              value={fullName}
              onChange={(e) => setFullName(e.target.value)}
              required
              placeholder="John Doe"
              className="w-full rounded-xl border border-dark-border bg-dark-bg/50 px-4 py-2.5 text-sm text-gray-200 placeholder-gray-600 focus:border-brand-purple/50 focus:outline-none focus:ring-1 focus:ring-brand-purple/30"
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-300 mb-1.5">
              Email
            </label>
            <input
              type="email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              required
              placeholder="john@company.com"
              className="w-full rounded-xl border border-dark-border bg-dark-bg/50 px-4 py-2.5 text-sm text-gray-200 placeholder-gray-600 focus:border-brand-purple/50 focus:outline-none focus:ring-1 focus:ring-brand-purple/30"
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-300 mb-1.5">
              Password
            </label>
            <input
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              required
              minLength={6}
              placeholder="Min 6 characters"
              className="w-full rounded-xl border border-dark-border bg-dark-bg/50 px-4 py-2.5 text-sm text-gray-200 placeholder-gray-600 focus:border-brand-purple/50 focus:outline-none focus:ring-1 focus:ring-brand-purple/30"
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-300 mb-1.5">
              Role
            </label>
            <select
              value={role}
              onChange={(e) => setRole(e.target.value as "admin" | "user")}
              className="w-full rounded-xl border border-dark-border bg-dark-bg/50 px-4 py-2.5 text-sm text-gray-200 focus:border-brand-purple/50 focus:outline-none focus:ring-1 focus:ring-brand-purple/30"
            >
              <option value="user">User</option>
              <option value="admin">Admin</option>
            </select>
          </div>

          <div className="flex justify-end gap-3 pt-2">
            <Button
              type="button"
              variant="ghost"
              size="sm"
              onClick={() => setShowCreateModal(false)}
            >
              Cancel
            </Button>
            <Button type="submit" variant="primary" size="sm" loading={creating}>
              Create User
            </Button>
          </div>
        </form>
      </Modal>
    </div>
  );
}
