import React, { useState } from "react";
import { useNavigate } from "react-router-dom";
import { useAuth } from "@/lib/AuthContext";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Loader2, ArrowLeft } from "lucide-react";
import { Link } from "react-router-dom";
import { createPageUrl } from "../utils";

export default function Login() {
  const navigate = useNavigate();
  const { login: authLogin } = useAuth();

  // Support returnTo query param for post-login redirect
  const returnTo = new URLSearchParams(window.location.search).get("returnTo");

  const [tab, setTab] = useState("login");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  // Login form
  const [loginEmail, setLoginEmail] = useState("");
  const [loginPassword, setLoginPassword] = useState("");

  // Register form
  const [regName, setRegName] = useState("");
  const [regEmail, setRegEmail] = useState("");
  const [regPhone, setRegPhone] = useState("");
  const [regPassword, setRegPassword] = useState("");
  const [regConfirm, setRegConfirm] = useState("");

  const handleLogin = async (e) => {
    e.preventDefault();
    setError("");
    setLoading(true);
    try {
      const res = await fetch("/api/auth/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email: loginEmail, password: loginPassword }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || "Login failed");

      // Store token and user
      localStorage.setItem("physio_connect_token", data.token);
      authLogin(data.user);
      navigate(returnTo || "/");
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const handleRegister = async (e) => {
    e.preventDefault();
    setError("");

    if (regPassword !== regConfirm) {
      setError("Passwords do not match");
      return;
    }
    if (regPassword.length < 6) {
      setError("Password must be at least 6 characters");
      return;
    }

    setLoading(true);
    try {
      const res = await fetch("/api/auth/register", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          email: regEmail,
          password: regPassword,
          full_name: regName,
          phone: regPhone || undefined,
        }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || "Registration failed");

      localStorage.setItem("physio_connect_token", data.token);
      authLogin(data.user);
      navigate(returnTo || "/");
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-gray-50 flex items-center justify-center p-4">
      <div className="w-full max-w-md">
        <div className="mb-6">
          <Link
            to={createPageUrl("Home")}
            className="inline-flex items-center gap-2 text-sm text-gray-400 hover:text-gray-600 transition-colors"
          >
            <ArrowLeft className="w-4 h-4" />
            Back to Home
          </Link>
        </div>

        <div className="bg-white rounded-2xl border border-gray-100 shadow-lg p-6 sm:p-8">
          <div className="text-center mb-6">
            <img src="/logo.png" alt="PhysioConnect" className="w-60 h-60 object-contain mx-auto mb-3" />
            <h1 className="text-2xl font-bold text-gray-900">Welcome to PhysioConnect</h1>
            <p className="text-gray-400 mt-1 text-sm">Sign in to manage your appointments</p>
          </div>

          {error && (
            <div className="mb-4 p-3 bg-red-50 border border-red-200 rounded-xl text-sm text-red-700">
              {error}
            </div>
          )}

          <Tabs value={tab} onValueChange={setTab}>
            <TabsList className="w-full bg-gray-100 rounded-xl p-1 mb-6">
              <TabsTrigger value="login" className="flex-1 rounded-lg text-sm">
                Sign In
              </TabsTrigger>
              <TabsTrigger value="register" className="flex-1 rounded-lg text-sm">
                Register
              </TabsTrigger>
            </TabsList>

            <TabsContent value="login">
              <form onSubmit={handleLogin} className="space-y-4">
                <div>
                  <Label className="text-sm text-gray-700">Email</Label>
                  <Input
                    type="email"
                    value={loginEmail}
                    onChange={(e) => setLoginEmail(e.target.value)}
                    placeholder="you@example.com"
                    required
                    className="mt-1.5 rounded-xl h-12"
                  />
                </div>
                <div>
                  <Label className="text-sm text-gray-700">Password</Label>
                  <Input
                    type="password"
                    value={loginPassword}
                    onChange={(e) => setLoginPassword(e.target.value)}
                    placeholder="Enter your password"
                    required
                    className="mt-1.5 rounded-xl h-12"
                  />
                </div>
                <Button
                  type="submit"
                  disabled={loading}
                  className="w-full bg-teal-600 hover:bg-teal-700 rounded-xl h-12 text-sm"
                >
                  {loading ? <Loader2 className="w-4 h-4 animate-spin mr-2" /> : null}
                  Sign In
                </Button>

                <div className="text-center text-xs text-gray-400 pt-2">
                  Demo: <code className="bg-gray-100 px-1.5 py-0.5 rounded">admin@physioconnect.com</code> / <code className="bg-gray-100 px-1.5 py-0.5 rounded">admin123</code>
                </div>
              </form>
            </TabsContent>

            <TabsContent value="register">
              <form onSubmit={handleRegister} className="space-y-4">
                <div>
                  <Label className="text-sm text-gray-700">Full Name *</Label>
                  <Input
                    value={regName}
                    onChange={(e) => setRegName(e.target.value)}
                    placeholder="John Doe"
                    required
                    className="mt-1.5 rounded-xl h-12"
                  />
                </div>
                <div>
                  <Label className="text-sm text-gray-700">Email *</Label>
                  <Input
                    type="email"
                    value={regEmail}
                    onChange={(e) => setRegEmail(e.target.value)}
                    placeholder="you@example.com"
                    required
                    className="mt-1.5 rounded-xl h-12"
                  />
                </div>
                <div>
                  <Label className="text-sm text-gray-700">Phone</Label>
                  <Input
                    value={regPhone}
                    onChange={(e) => setRegPhone(e.target.value)}
                    placeholder="+44 7700 000000"
                    className="mt-1.5 rounded-xl h-12"
                  />
                </div>
                <div>
                  <Label className="text-sm text-gray-700">Password *</Label>
                  <Input
                    type="password"
                    value={regPassword}
                    onChange={(e) => setRegPassword(e.target.value)}
                    placeholder="At least 6 characters"
                    required
                    className="mt-1.5 rounded-xl h-12"
                  />
                </div>
                <div>
                  <Label className="text-sm text-gray-700">Confirm Password *</Label>
                  <Input
                    type="password"
                    value={regConfirm}
                    onChange={(e) => setRegConfirm(e.target.value)}
                    placeholder="Re-enter password"
                    required
                    className="mt-1.5 rounded-xl h-12"
                  />
                </div>
                <Button
                  type="submit"
                  disabled={loading}
                  className="w-full bg-teal-600 hover:bg-teal-700 rounded-xl h-12 text-sm"
                >
                  {loading ? <Loader2 className="w-4 h-4 animate-spin mr-2" /> : null}
                  Create Account
                </Button>
              </form>
            </TabsContent>
          </Tabs>
        </div>
      </div>
    </div>
  );
}
