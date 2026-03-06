import React, { useState, useEffect } from "react";
import { Link } from "react-router-dom";
import { createPageUrl } from "./utils";
import { useAuth } from "@/lib/AuthContext";
import { Button } from "@/components/ui/button";
import {
  Menu, X, Search, Calendar, UserPlus, LayoutDashboard, LogOut, Home, ChevronDown, Stethoscope
} from "lucide-react";
import {
  DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuSeparator, DropdownMenuTrigger
} from "@/components/ui/dropdown-menu";
import { cn } from "@/lib/utils";

export default function Layout({ children, currentPageName }) {
  const [mobileOpen, setMobileOpen] = useState(false);
  const [scrolled, setScrolled] = useState(false);
  const { user, isAuthenticated, logout, navigateToLogin } = useAuth();

  useEffect(() => {
    const handler = () => setScrolled(window.scrollY > 10);
    window.addEventListener("scroll", handler);
    return () => window.removeEventListener("scroll", handler);
  }, []);

  const isHome = currentPageName === "Home";

  const navLinks = [
    { label: "Find Physio", icon: Search, page: "Search" },
    { label: "My Appointments", icon: Calendar, page: "MyAppointments" },
  ];

  return (
    <div className="min-h-screen bg-white">
      <style>{`
        :root {
          --color-primary: #0d9488;
          --color-primary-light: #ccfbf1;
        }
      `}</style>

      <header className={cn(
        "fixed top-0 left-0 right-0 z-50 transition-all duration-300",
        scrolled || !isHome ? "bg-white/95 backdrop-blur-md shadow-sm border-b border-gray-100" : "bg-transparent"
      )}>
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex items-center justify-between h-32 sm:h-36">
            <Link to={createPageUrl("Home")} className="flex items-center">
              <img src="/logo.png" alt="PhysioConnect" className="w-56 h-32 object-contain" />
            </Link>

            {/* Desktop nav */}
            <nav className="hidden md:flex items-center gap-1">
              {navLinks.map(link => (
                <Link key={link.page} to={createPageUrl(link.page)}>
                  <Button
                    variant="ghost"
                    className={cn(
                      "rounded-xl text-sm",
                      currentPageName === link.page ? "bg-teal-50 text-teal-700" : "text-gray-600 hover:text-gray-900"
                    )}
                  >
                    <link.icon className="w-4 h-4 mr-2" />
                    {link.label}
                  </Button>
                </Link>
              ))}

              {isAuthenticated && user ? (
                <DropdownMenu>
                  <DropdownMenuTrigger asChild>
                    <Button variant="ghost" className="rounded-xl ml-2 gap-2 text-sm text-gray-600">
                      <div className="w-7 h-7 rounded-full bg-teal-100 flex items-center justify-center">
                        <span className="text-xs font-bold text-teal-700">{user.full_name?.charAt(0)?.toUpperCase() || "U"}</span>
                      </div>
                      <span className="hidden lg:inline">{user.full_name}</span>
                      <ChevronDown className="w-3 h-3" />
                    </Button>
                  </DropdownMenuTrigger>
                  <DropdownMenuContent align="end" className="w-48 rounded-xl">
                    {user.role === "admin" && (
                      <DropdownMenuItem asChild>
                        <Link to={createPageUrl("AdminDashboard")} className="flex items-center gap-2">
                          <LayoutDashboard className="w-4 h-4" />
                          Admin Dashboard
                        </Link>
                      </DropdownMenuItem>
                    )}
                    {user.role === "physio" && (
                      <DropdownMenuItem asChild>
                        <Link to={createPageUrl("PhysioDashboard")} className="flex items-center gap-2">
                          <Stethoscope className="w-4 h-4" />
                          My Dashboard
                        </Link>
                      </DropdownMenuItem>
                    )}
                    <DropdownMenuItem asChild>
                      <Link to={createPageUrl("MyAppointments")} className="flex items-center gap-2">
                        <Calendar className="w-4 h-4" />
                        My Appointments
                      </Link>
                    </DropdownMenuItem>
                    <DropdownMenuSeparator />
                    <DropdownMenuItem onClick={logout} className="text-red-600 flex items-center gap-2">
                      <LogOut className="w-4 h-4" />
                      Logout
                    </DropdownMenuItem>
                  </DropdownMenuContent>
                </DropdownMenu>
              ) : (
                <Link to="/Login">
                  <Button className="ml-2 bg-teal-600 hover:bg-teal-700 rounded-xl text-sm">
                    Sign In
                  </Button>
                </Link>
              )}
            </nav>

            {/* Mobile menu button */}
            <button onClick={() => setMobileOpen(!mobileOpen)} className="md:hidden p-2 rounded-xl hover:bg-gray-100">
              {mobileOpen ? <X className="w-5 h-5" /> : <Menu className="w-5 h-5" />}
            </button>
          </div>
        </div>

        {/* Mobile menu */}
        {mobileOpen && (
          <div className="md:hidden bg-white border-t border-gray-100 shadow-lg">
            <div className="px-4 py-4 space-y-1">
              {navLinks.map(link => (
                <Link key={link.page} to={createPageUrl(link.page)} onClick={() => setMobileOpen(false)}>
                  <div className={cn(
                    "flex items-center gap-3 px-4 py-3 rounded-xl text-sm transition-colors",
                    currentPageName === link.page ? "bg-teal-50 text-teal-700 font-medium" : "text-gray-600"
                  )}>
                    <link.icon className="w-4 h-4" />
                    {link.label}
                  </div>
                </Link>
              ))}
              {user?.role === "admin" && (
                <Link to={createPageUrl("AdminDashboard")} onClick={() => setMobileOpen(false)}>
                  <div className="flex items-center gap-3 px-4 py-3 rounded-xl text-sm text-gray-600">
                    <LayoutDashboard className="w-4 h-4" />
                    Admin Dashboard
                  </div>
                </Link>
              )}
              {user?.role === "physio" && (
                <Link to={createPageUrl("PhysioDashboard")} onClick={() => setMobileOpen(false)}>
                  <div className="flex items-center gap-3 px-4 py-3 rounded-xl text-sm text-gray-600">
                    <Stethoscope className="w-4 h-4" />
                    My Dashboard
                  </div>
                </Link>
              )}
              <div className="pt-3 border-t border-gray-100">
                {isAuthenticated && user ? (
                  <Button variant="outline" onClick={logout} className="w-full rounded-xl justify-start gap-2 text-red-600 border-red-200">
                    <LogOut className="w-4 h-4" /> Logout
                  </Button>
                ) : (
                  <Link to="/Login" onClick={() => setMobileOpen(false)}>
                    <Button className="w-full bg-teal-600 hover:bg-teal-700 rounded-xl">
                      Sign In
                    </Button>
                  </Link>
                )}
              </div>
            </div>
          </div>
        )}
      </header>

      <main className={cn(isHome ? "" : "pt-36")}>
        {children}
      </main>
    </div>
  );
}
