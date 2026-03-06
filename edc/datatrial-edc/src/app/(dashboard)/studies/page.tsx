"use client";

import { useEffect, useState } from "react";
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
import { Plus, Eye, Edit, Building2, Users } from "lucide-react";

interface Study {
  id: string;
  studyCode: string;
  studyName: string;
  protocolNumber: string | null;
  sponsorName: string | null;
  phase: string | null;
  status: string;
  therapeuticArea: string | null;
  createdAt: string;
  _count?: {
    sites: number;
    subjects: number;
  };
}

const statusColors: Record<string, "draft" | "active" | "completed" | "default"> = {
  draft: "draft",
  active: "active",
  completed: "completed",
  closed: "completed",
};

export default function StudiesPage() {
  const [studies, setStudies] = useState<Study[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchStudies();
  }, []);

  const fetchStudies = async () => {
    try {
      const response = await fetch("/api/studies");
      if (response.ok) {
        const data = await response.json();
        setStudies(data);
      }
    } catch (error) {
      console.error("Failed to fetch studies:", error);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold text-gray-900">Studies</h2>
          <p className="text-gray-500">Manage clinical trial studies</p>
        </div>
        <Link href="/studies/new">
          <Button>
            <Plus className="h-4 w-4 mr-2" />
            Create Study
          </Button>
        </Link>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>All Studies</CardTitle>
        </CardHeader>
        <CardContent>
          {loading ? (
            <div className="flex items-center justify-center py-8">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
            </div>
          ) : studies.length === 0 ? (
            <div className="text-center py-8">
              <p className="text-gray-500 mb-4">No studies found</p>
              <Link href="/studies/new">
                <Button>
                  <Plus className="h-4 w-4 mr-2" />
                  Create Your First Study
                </Button>
              </Link>
            </div>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Study Code</TableHead>
                  <TableHead>Study Name</TableHead>
                  <TableHead>Protocol</TableHead>
                  <TableHead>Sponsor</TableHead>
                  <TableHead>Phase</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Sites</TableHead>
                  <TableHead>Subjects</TableHead>
                  <TableHead className="text-right">Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {studies.map((study) => (
                  <TableRow key={study.id}>
                    <TableCell className="font-medium">{study.studyCode}</TableCell>
                    <TableCell>{study.studyName}</TableCell>
                    <TableCell>{study.protocolNumber || "-"}</TableCell>
                    <TableCell>{study.sponsorName || "-"}</TableCell>
                    <TableCell>{study.phase || "-"}</TableCell>
                    <TableCell>
                      <Badge variant={statusColors[study.status] || "default"}>
                        {study.status}
                      </Badge>
                    </TableCell>
                    <TableCell>
                      <div className="flex items-center">
                        <Building2 className="h-4 w-4 mr-1 text-gray-400" />
                        {study._count?.sites || 0}
                      </div>
                    </TableCell>
                    <TableCell>
                      <div className="flex items-center">
                        <Users className="h-4 w-4 mr-1 text-gray-400" />
                        {study._count?.subjects || 0}
                      </div>
                    </TableCell>
                    <TableCell className="text-right">
                      <div className="flex justify-end space-x-2">
                        <Link href={`/studies/${study.id}`}>
                          <Button variant="ghost" size="icon">
                            <Eye className="h-4 w-4" />
                          </Button>
                        </Link>
                        <Link href={`/studies/${study.id}/edit`}>
                          <Button variant="ghost" size="icon">
                            <Edit className="h-4 w-4" />
                          </Button>
                        </Link>
                      </div>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
