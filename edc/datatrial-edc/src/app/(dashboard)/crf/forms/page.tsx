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
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Plus, Eye, Edit, FileText, Layers, Calendar } from "lucide-react";

interface Study {
  id: string;
  studyCode: string;
  studyName: string;
}

interface CRFForm {
  id: string;
  formCode: string;
  formName: string;
  formType: string | null;
  isRepeating: boolean;
  isLogForm: boolean;
  sequenceNumber: number | null;
  isActive: boolean;
  _count: {
    fields: number;
    visitForms: number;
  };
  visitForms: Array<{
    visit: {
      visitCode: string;
      visitName: string;
    };
  }>;
}

export default function CRFFormsPage() {
  const [studies, setStudies] = useState<Study[]>([]);
  const [selectedStudy, setSelectedStudy] = useState<string>("");
  const [forms, setForms] = useState<CRFForm[]>([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    fetchStudies();
  }, []);

  useEffect(() => {
    if (selectedStudy) {
      fetchForms(selectedStudy);
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

  const fetchForms = async (studyId: string) => {
    setLoading(true);
    try {
      const response = await fetch(`/api/crf/forms?studyId=${studyId}`);
      if (response.ok) {
        const data = await response.json();
        setForms(data);
      }
    } catch (error) {
      console.error("Failed to fetch forms:", error);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold text-gray-900">CRF Forms</h2>
          <p className="text-gray-500">Design and manage Case Report Forms</p>
        </div>
        <Link href={`/crf/forms/new?studyId=${selectedStudy}`}>
          <Button disabled={!selectedStudy}>
            <Plus className="h-4 w-4 mr-2" />
            Create Form
          </Button>
        </Link>
      </div>

      {/* Study Selector */}
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

      {/* Forms List */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center">
            <FileText className="h-5 w-5 mr-2" />
            CRF Forms
          </CardTitle>
        </CardHeader>
        <CardContent>
          {!selectedStudy ? (
            <div className="text-center py-8 text-gray-500">
              Please select a study to view forms
            </div>
          ) : loading ? (
            <div className="flex items-center justify-center py-8">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
            </div>
          ) : forms.length === 0 ? (
            <div className="text-center py-8">
              <p className="text-gray-500 mb-4">No CRF forms found for this study</p>
              <Link href={`/crf/forms/new?studyId=${selectedStudy}`}>
                <Button>
                  <Plus className="h-4 w-4 mr-2" />
                  Create Your First Form
                </Button>
              </Link>
            </div>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Seq</TableHead>
                  <TableHead>Form Code</TableHead>
                  <TableHead>Form Name</TableHead>
                  <TableHead>Type</TableHead>
                  <TableHead>Fields</TableHead>
                  <TableHead>Visits</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead className="text-right">Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {forms.map((form) => (
                  <TableRow key={form.id}>
                    <TableCell>{form.sequenceNumber}</TableCell>
                    <TableCell className="font-medium">{form.formCode}</TableCell>
                    <TableCell>{form.formName}</TableCell>
                    <TableCell>
                      <div className="flex items-center space-x-1">
                        <span>{form.formType || "Simple CRF"}</span>
                        {form.isRepeating && (
                          <Badge variant="secondary" className="text-xs">
                            Repeating
                          </Badge>
                        )}
                        {form.isLogForm && (
                          <Badge variant="secondary" className="text-xs">
                            Log
                          </Badge>
                        )}
                      </div>
                    </TableCell>
                    <TableCell>
                      <div className="flex items-center">
                        <Layers className="h-4 w-4 mr-1 text-gray-400" />
                        {form._count.fields}
                      </div>
                    </TableCell>
                    <TableCell>
                      <div className="flex items-center">
                        <Calendar className="h-4 w-4 mr-1 text-gray-400" />
                        {form._count.visitForms}
                      </div>
                    </TableCell>
                    <TableCell>
                      <Badge variant={form.isActive ? "success" : "secondary"}>
                        {form.isActive ? "Active" : "Inactive"}
                      </Badge>
                    </TableCell>
                    <TableCell className="text-right">
                      <div className="flex justify-end space-x-2">
                        <Link href={`/crf/forms/${form.id}`}>
                          <Button variant="ghost" size="icon" title="View Form">
                            <Eye className="h-4 w-4" />
                          </Button>
                        </Link>
                        <Link href={`/crf/forms/${form.id}/design`}>
                          <Button variant="ghost" size="icon" title="Design Fields">
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
