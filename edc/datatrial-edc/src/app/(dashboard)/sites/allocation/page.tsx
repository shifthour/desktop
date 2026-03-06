"use client";

import { useEffect, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
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
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Building2, Users, UserCircle, TrendingUp, AlertTriangle } from "lucide-react";

interface Study {
  id: string;
  studyCode: string;
  studyName: string;
}

interface Site {
  id: string;
  siteCode: string;
  siteName: string;
  city: string | null;
  country: string | null;
  principalInvestigator: string | null;
  targetEnrollment: number | null;
  status: string;
  _count: {
    subjects: number;
    users: number;
    shipments: number;
  };
}

export default function SiteAllocationPage() {
  const [studies, setStudies] = useState<Study[]>([]);
  const [selectedStudy, setSelectedStudy] = useState<string>("");
  const [sites, setSites] = useState<Site[]>([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    fetchStudies();
  }, []);

  useEffect(() => {
    if (selectedStudy) {
      fetchSites(selectedStudy);
    }
  }, [selectedStudy]);

  const fetchStudies = async () => {
    try {
      const response = await fetch("/api/studies");
      if (response.ok) {
        const data = await response.json();
        setStudies(data);
        if (data.length > 0) {
          setSelectedStudy(data[0].id);
        }
      }
    } catch (error) {
      console.error("Failed to fetch studies:", error);
    }
  };

  const fetchSites = async (studyId: string) => {
    setLoading(true);
    try {
      const response = await fetch(`/api/sites?studyId=${studyId}`);
      if (response.ok) {
        const data = await response.json();
        setSites(data);
      }
    } catch (error) {
      console.error("Failed to fetch sites:", error);
    } finally {
      setLoading(false);
    }
  };

  const totalSubjects = sites.reduce((sum, site) => sum + site._count.subjects, 0);
  const totalTarget = sites.reduce((sum, site) => sum + (site.targetEnrollment || 0), 0);
  const totalUsers = sites.reduce((sum, site) => sum + site._count.users, 0);
  const activeSites = sites.filter((site) => site.status === "Active").length;

  const getEnrollmentStatus = (current: number, target: number | null) => {
    if (!target) return "outline";
    const percentage = (current / target) * 100;
    if (percentage >= 100) return "success";
    if (percentage >= 75) return "default";
    if (percentage >= 50) return "secondary";
    return "destructive";
  };

  const getEnrollmentPercentage = (current: number, target: number | null) => {
    if (!target) return "-";
    return Math.round((current / target) * 100) + "%";
  };

  const overallProgress = totalTarget > 0 ? Math.round((totalSubjects / totalTarget) * 100) : 0;

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-bold text-gray-900">Site Allocation</h2>
        <p className="text-gray-500">Overview of site enrollment and resource allocation</p>
      </div>

      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base">Select Study</CardTitle>
        </CardHeader>
        <CardContent>
          <Select value={selectedStudy} onValueChange={setSelectedStudy}>
            <SelectTrigger className="w-full md:w-96">
              <SelectValue placeholder="Select a study" />
            </SelectTrigger>
            <SelectContent>
              {studies.map((study) => (
                <SelectItem key={study.id} value={study.id}>
                  {study.studyCode} - {study.studyName}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </CardContent>
      </Card>

      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm flex items-center text-gray-500">
              <Building2 className="h-4 w-4 mr-2" />
              Total Sites
            </CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-2xl font-bold">{sites.length}</p>
            <p className="text-xs text-gray-500">{activeSites} active</p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm flex items-center text-gray-500">
              <UserCircle className="h-4 w-4 mr-2" />
              Total Enrollment
            </CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-2xl font-bold">{totalSubjects}</p>
            <p className="text-xs text-gray-500">of {totalTarget} target</p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm flex items-center text-gray-500">
              <TrendingUp className="h-4 w-4 mr-2" />
              Overall Progress
            </CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-2xl font-bold">{overallProgress}%</p>
            <p className="text-xs text-gray-500">enrollment rate</p>
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
            <p className="text-2xl font-bold">{totalUsers}</p>
            <p className="text-xs text-gray-500">across all sites</p>
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center">
            <Building2 className="h-5 w-5 mr-2" />
            Site Enrollment Status
          </CardTitle>
        </CardHeader>
        <CardContent>
          {loading ? (
            <div className="flex items-center justify-center py-8">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
            </div>
          ) : sites.length === 0 ? (
            <div className="text-center py-8 text-gray-500">
              No sites available for this study
            </div>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Site</TableHead>
                  <TableHead>Location</TableHead>
                  <TableHead>PI</TableHead>
                  <TableHead>Enrolled</TableHead>
                  <TableHead>Target</TableHead>
                  <TableHead>Progress</TableHead>
                  <TableHead>Users</TableHead>
                  <TableHead>Status</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {sites.map((site) => (
                  <TableRow key={site.id}>
                    <TableCell>
                      <div>
                        <p className="font-medium">{site.siteCode}</p>
                        <p className="text-xs text-gray-500">{site.siteName}</p>
                      </div>
                    </TableCell>
                    <TableCell>
                      {[site.city, site.country].filter(Boolean).join(", ") || "-"}
                    </TableCell>
                    <TableCell>{site.principalInvestigator || "-"}</TableCell>
                    <TableCell>
                      <div className="flex items-center">
                        <UserCircle className="h-4 w-4 mr-1 text-gray-400" />
                        {site._count.subjects}
                      </div>
                    </TableCell>
                    <TableCell>{site.targetEnrollment || "-"}</TableCell>
                    <TableCell>
                      <Badge
                        variant={getEnrollmentStatus(
                          site._count.subjects,
                          site.targetEnrollment
                        ) as "success" | "default" | "secondary" | "destructive" | "outline"}
                      >
                        {getEnrollmentPercentage(site._count.subjects, site.targetEnrollment)}
                      </Badge>
                    </TableCell>
                    <TableCell>
                      <div className="flex items-center">
                        <Users className="h-4 w-4 mr-1 text-gray-400" />
                        {site._count.users}
                      </div>
                    </TableCell>
                    <TableCell>
                      <Badge
                        variant={
                          site.status === "Active"
                            ? "success"
                            : site.status === "Closed"
                            ? "destructive"
                            : "secondary"
                        }
                      >
                        {site.status}
                      </Badge>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>

      {sites.some(
        (site) =>
          site.targetEnrollment &&
          site._count.subjects < site.targetEnrollment * 0.5 &&
          site.status === "Active"
      ) && (
        <Card className="border-amber-200 bg-amber-50">
          <CardHeader className="pb-2">
            <CardTitle className="text-base flex items-center text-amber-700">
              <AlertTriangle className="h-4 w-4 mr-2" />
              Enrollment Alerts
            </CardTitle>
          </CardHeader>
          <CardContent>
            <ul className="space-y-1 text-sm text-amber-700">
              {sites
                .filter(
                  (site) =>
                    site.targetEnrollment &&
                    site._count.subjects < site.targetEnrollment * 0.5 &&
                    site.status === "Active"
                )
                .map((site) => (
                  <li key={site.id}>
                    {site.siteCode} ({site.siteName}) - Below 50% of enrollment target
                  </li>
                ))}
            </ul>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
