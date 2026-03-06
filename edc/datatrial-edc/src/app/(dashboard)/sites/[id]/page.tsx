"use client";

import { useState, useEffect, use } from "react";
import Link from "next/link";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  ArrowLeft,
  Edit,
  Building2,
  Users,
  UserCircle,
  MapPin,
  Phone,
  Mail,
  Package,
} from "lucide-react";

interface Site {
  id: string;
  siteCode: string;
  siteName: string;
  address: string | null;
  city: string | null;
  state: string | null;
  country: string | null;
  postalCode: string | null;
  phone: string | null;
  email: string | null;
  principalInvestigator: string | null;
  targetEnrollment: number | null;
  status: string;
  isActive: boolean;
  study: {
    id: string;
    studyCode: string;
    studyName: string;
  };
  users: Array<{
    id: string;
    firstName: string;
    lastName: string;
    email: string;
    role: { roleName: string };
  }>;
  _count: {
    subjects: number;
    shipments: number;
    ipInventory: number;
  };
}

export default function SiteDetailPage({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const { id: siteId } = use(params);
  const [site, setSite] = useState<Site | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    fetchSite();
  }, [siteId]);

  const fetchSite = async () => {
    try {
      const response = await fetch(`/api/sites/${siteId}`);
      if (!response.ok) {
        throw new Error("Site not found");
      }
      const data = await response.json();
      setSite(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load site");
    } finally {
      setLoading(false);
    }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-[400px]">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
      </div>
    );
  }

  if (error || !site) {
    return (
      <div className="text-center py-8">
        <p className="text-red-600 mb-4">{error || "Site not found"}</p>
        <Link href="/sites">
          <Button>Back to Sites</Button>
        </Link>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div className="flex items-center space-x-4">
          <Link href="/sites">
            <Button variant="ghost" size="icon">
              <ArrowLeft className="h-4 w-4" />
            </Button>
          </Link>
          <div>
            <h2 className="text-2xl font-bold text-gray-900">
              {site.siteCode}: {site.siteName}
            </h2>
            <p className="text-gray-500">
              {site.study.studyCode} - {site.study.studyName}
            </p>
          </div>
        </div>
        <Badge
          variant={
            site.status === "Active"
              ? "success"
              : site.status === "Closed"
              ? "destructive"
              : "secondary"
          }
          className="text-sm"
        >
          {site.status}
        </Badge>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm flex items-center text-gray-500">
              <UserCircle className="h-4 w-4 mr-2" />
              Enrolled Subjects
            </CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-2xl font-bold">{site._count.subjects}</p>
            <p className="text-xs text-gray-500">
              of {site.targetEnrollment || "unlimited"} target
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm flex items-center text-gray-500">
              <Users className="h-4 w-4 mr-2" />
              Site Users
            </CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-2xl font-bold">{site.users.length}</p>
            <p className="text-xs text-gray-500">active users</p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm flex items-center text-gray-500">
              <Package className="h-4 w-4 mr-2" />
              IP Inventory
            </CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-2xl font-bold">{site._count.ipInventory}</p>
            <p className="text-xs text-gray-500">kits in stock</p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm flex items-center text-gray-500">
              <Package className="h-4 w-4 mr-2" />
              Shipments
            </CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-2xl font-bold">{site._count.shipments}</p>
            <p className="text-xs text-gray-500">total shipments</p>
          </CardContent>
        </Card>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center">
              <Building2 className="h-5 w-5 mr-2" />
              Site Information
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div>
              <p className="text-sm text-gray-500">Principal Investigator</p>
              <p className="font-medium">{site.principalInvestigator || "-"}</p>
            </div>

            <div className="flex items-start space-x-2">
              <MapPin className="h-4 w-4 mt-1 text-gray-400" />
              <div>
                <p className="text-sm text-gray-500">Address</p>
                <p className="font-medium">
                  {[site.address, site.city, site.state, site.country, site.postalCode]
                    .filter(Boolean)
                    .join(", ") || "-"}
                </p>
              </div>
            </div>

            <div className="flex items-start space-x-2">
              <Phone className="h-4 w-4 mt-1 text-gray-400" />
              <div>
                <p className="text-sm text-gray-500">Phone</p>
                <p className="font-medium">{site.phone || "-"}</p>
              </div>
            </div>

            <div className="flex items-start space-x-2">
              <Mail className="h-4 w-4 mt-1 text-gray-400" />
              <div>
                <p className="text-sm text-gray-500">Email</p>
                <p className="font-medium">{site.email || "-"}</p>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="flex items-center">
              <Users className="h-5 w-5 mr-2" />
              Site Users
            </CardTitle>
          </CardHeader>
          <CardContent>
            {site.users.length === 0 ? (
              <p className="text-gray-500 text-center py-4">
                No users assigned to this site
              </p>
            ) : (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Name</TableHead>
                    <TableHead>Email</TableHead>
                    <TableHead>Role</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {site.users.map((user) => (
                    <TableRow key={user.id}>
                      <TableCell className="font-medium">
                        {user.firstName} {user.lastName}
                      </TableCell>
                      <TableCell className="text-sm text-gray-500">
                        {user.email}
                      </TableCell>
                      <TableCell>
                        <Badge variant="outline">{user.role.roleName}</Badge>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
