import { Review } from "@/lib/types";

export default function ReviewCard({ review }: { review: Review }) {
  const date = new Date(review.date);
  const now = new Date();
  const diffDays = Math.floor((now.getTime() - date.getTime()) / 86400000);
  const timeAgo =
    diffDays === 0
      ? "Today"
      : diffDays === 1
      ? "Yesterday"
      : diffDays < 7
      ? `${diffDays} days ago`
      : diffDays < 30
      ? `${Math.floor(diffDays / 7)} weeks ago`
      : `${Math.floor(diffDays / 30)} months ago`;

  return (
    <div className="border border-gray-100 rounded-lg p-4">
      <div className="flex items-center justify-between mb-2">
        <div className="flex items-center gap-2">
          <div className="w-8 h-8 rounded-full bg-gray-200 flex items-center justify-center text-sm font-bold text-gray-600">
            {review.patientName[0]}
          </div>
          <div>
            <span className="font-medium text-sm">{review.patientName}</span>
            <span className="text-gray-400 text-xs ml-2">{timeAgo}</span>
          </div>
        </div>
        <div className="flex items-center gap-0.5">
          {[...Array(5)].map((_, i) => (
            <svg
              key={i}
              className={`w-4 h-4 ${i < review.rating ? "text-amber-400" : "text-gray-200"}`}
              fill="currentColor"
              viewBox="0 0 20 20"
            >
              <path d="M9.049 2.927c.3-.921 1.603-.921 1.902 0l1.07 3.292a1 1 0 00.95.69h3.462c.969 0 1.371 1.24.588 1.81l-2.8 2.034a1 1 0 00-.364 1.118l1.07 3.292c.3.921-.755 1.688-1.54 1.118l-2.8-2.034a1 1 0 00-1.175 0l-2.8 2.034c-.784.57-1.838-.197-1.539-1.118l1.07-3.292a1 1 0 00-.364-1.118L2.98 8.72c-.783-.57-.38-1.81.588-1.81h3.461a1 1 0 00.951-.69l1.07-3.292z" />
            </svg>
          ))}
        </div>
      </div>
      <p className="text-gray-600 text-sm">{review.text}</p>
      {review.verified && (
        <span className="inline-flex items-center mt-2 text-xs text-accent font-medium">
          <svg className="w-3.5 h-3.5 mr-1" fill="currentColor" viewBox="0 0 20 20">
            <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clipRule="evenodd" />
          </svg>
          Verified Booking
        </span>
      )}
    </div>
  );
}
