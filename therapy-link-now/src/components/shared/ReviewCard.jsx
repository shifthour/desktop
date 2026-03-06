import React from "react";
import StarRating from "@/components/shared/StarRating";
import { format } from "date-fns";

export default function ReviewCard({ review }) {
  return (
    <div className="bg-white rounded-xl border border-gray-100 p-5 hover:shadow-sm transition-shadow">
      <div className="flex items-start justify-between">
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 rounded-full bg-gradient-to-br from-teal-100 to-emerald-100 flex items-center justify-center">
            <span className="text-sm font-bold text-teal-700">
              {review.patient_name?.charAt(0)?.toUpperCase() || "?"}
            </span>
          </div>
          <div>
            <p className="font-semibold text-gray-900 text-sm">{review.patient_name || "Anonymous"}</p>
            <p className="text-xs text-gray-400">
              {review.created_date ? format(new Date(review.created_date), "MMM d, yyyy") : ""}
            </p>
          </div>
        </div>
        <StarRating rating={review.rating} size="xs" />
      </div>
      {review.comment && (
        <p className="mt-3 text-sm text-gray-600 leading-relaxed">{review.comment}</p>
      )}
    </div>
  );
}