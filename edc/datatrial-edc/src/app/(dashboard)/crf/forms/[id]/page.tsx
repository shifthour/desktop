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
  Layers,
  Calendar,
  FileText,
  Settings,
} from "lucide-react";

interface CRFForm {
  id: string;
  formCode: string;
  formName: string;
  formType: string | null;
  isRepeating: boolean;
  isLogForm: boolean;
  isActive: boolean;
  sequenceNumber: number | null;
  study: {
    studyCode: string;
    studyName: string;
  };
  fields: Array<{
    id: string;
    fieldCode: string;
    fieldLabel: string;
    fieldType: string;
    sequenceNumber: number;
    isRequired: boolean;
    isActive: boolean;
  }>;
  visitForms: Array<{
    visit: {
      visitCode: string;
      visitName: string;
    };
  }>;
  _count: {
    fields: number;
    visitForms: number;
  };
}

export default function FormDetailPage({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const { id: formId } = use(params);
  const [form, setForm] = useState<CRFForm | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    fetchForm();
  }, [formId]);

  const fetchForm = async () => {
    try {
      const response = await fetch(`/api/crf/forms/${formId}`);
      if (!response.ok) {
        throw new Error("Form not found");
      }
      const data = await response.json();
      setForm(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load form");
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

  if (error || !form) {
    return (
      <div className="text-center py-8">
        <p className="text-red-600 mb-4">{error || "Form not found"}</p>
        <Link href="/crf/forms">
          <Button>Back to Forms</Button>
        </Link>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center space-x-4">
          <Link href="/crf/forms">
            <Button variant="ghost" size="icon">
              <ArrowLeft className="h-4 w-4" />
            </Button>
          </Link>
          <div>
            <h2 className="text-2xl font-bold text-gray-900">
              {form.formCode}: {form.formName}
            </h2>
            <p className="text-gray-500">{form.study.studyCode} - {form.study.studyName}</p>
          </div>
        </div>
        <div className="flex space-x-2">
          <Link href={`/crf/forms/${formId}/design`}>
            <Button>
              <Edit className="h-4 w-4 mr-2" />
              Design Fields
            </Button>
          </Link>
        </div>
      </div>

      {/* Form Details */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm flex items-center">
              <FileText className="h-4 w-4 mr-2" />
              Form Type
            </CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-2xl font-semibold">{form.formType || "Simple CRF"}</p>
            <div className="flex gap-2 mt-2">
              {form.isRepeating && (
                <Badge variant="secondary">Repeating</Badge>
              )}
              {form.isLogForm && (
                <Badge variant="secondary">Log Form</Badge>
              )}
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm flex items-center">
              <Layers className="h-4 w-4 mr-2" />
              Fields
            </CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-2xl font-semibold">{form._count.fields}</p>
            <p className="text-sm text-gray-500">Total fields defined</p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm flex items-center">
              <Calendar className="h-4 w-4 mr-2" />
              Assigned Visits
            </CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-2xl font-semibold">{form._count.visitForms}</p>
            <p className="text-sm text-gray-500">Visits using this form</p>
          </CardContent>
        </Card>
      </div>

      {/* Fields Summary */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center justify-between">
            <span className="flex items-center">
              <Layers className="h-5 w-5 mr-2" />
              Form Fields
            </span>
            <Link href={`/crf/forms/${formId}/design`}>
              <Button variant="outline" size="sm">
                <Settings className="h-4 w-4 mr-2" />
                Manage Fields
              </Button>
            </Link>
          </CardTitle>
        </CardHeader>
        <CardContent>
          {form.fields.length === 0 ? (
            <div className="text-center py-8">
              <p className="text-gray-500 mb-4">No fields defined yet</p>
              <Link href={`/crf/forms/${formId}/design`}>
                <Button>Add Fields</Button>
              </Link>
            </div>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Seq</TableHead>
                  <TableHead>Field Code</TableHead>
                  <TableHead>Label</TableHead>
                  <TableHead>Type</TableHead>
                  <TableHead>Required</TableHead>
                  <TableHead>Status</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {form.fields.map((field) => (
                  <TableRow key={field.id}>
                    <TableCell>{field.sequenceNumber}</TableCell>
                    <TableCell className="font-medium">{field.fieldCode}</TableCell>
                    <TableCell>{field.fieldLabel}</TableCell>
                    <TableCell>
                      <Badge variant="outline">{field.fieldType}</Badge>
                    </TableCell>
                    <TableCell>
                      {field.isRequired ? (
                        <Badge variant="destructive" className="text-xs">Required</Badge>
                      ) : (
                        <span className="text-gray-400 text-xs">Optional</span>
                      )}
                    </TableCell>
                    <TableCell>
                      <Badge variant={field.isActive ? "success" : "secondary"}>
                        {field.isActive ? "Active" : "Inactive"}
                      </Badge>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>

      {/* Assigned Visits */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center">
            <Calendar className="h-5 w-5 mr-2" />
            Assigned to Visits
          </CardTitle>
        </CardHeader>
        <CardContent>
          {form.visitForms.length === 0 ? (
            <p className="text-gray-500 text-center py-4">
              This form is not assigned to any visits yet
            </p>
          ) : (
            <div className="flex flex-wrap gap-2">
              {form.visitForms.map((vf, index) => (
                <Badge key={index} variant="outline" className="px-3 py-1">
                  {vf.visit.visitCode} - {vf.visit.visitName}
                </Badge>
              ))}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
