import React from 'react';

export default function RentVsBuyTable() {
  return (
    <div className="w-full overflow-x-auto">
      <table className="w-full border-collapse bg-white shadow-lg rounded-lg overflow-hidden">
        <thead>
          <tr className="bg-orange-500 text-white">
            <th className="px-4 md:px-6 py-3 md:py-4 text-left text-sm md:text-base font-semibold">
              Scenario
            </th>
            <th className="px-4 md:px-6 py-3 md:py-4 text-left text-sm md:text-base font-semibold">
              5-Year Cost (₹)
            </th>
            <th className="px-4 md:px-6 py-3 md:py-4 text-left text-sm md:text-base font-semibold">
              Equity Built (₹)
            </th>
          </tr>
        </thead>
        <tbody>
          <tr className="border-b border-gray-200 hover:bg-gray-50 transition-colors">
            <td className="px-4 md:px-6 py-3 md:py-4 text-sm md:text-base font-medium text-gray-900">
              Renting
            </td>
            <td className="px-4 md:px-6 py-3 md:py-4 text-sm md:text-base text-gray-700">
              30–35L
            </td>
            <td className="px-4 md:px-6 py-3 md:py-4 text-sm md:text-base text-gray-700">
              0
            </td>
          </tr>
          <tr className="hover:bg-gray-50 transition-colors">
            <td className="px-4 md:px-6 py-3 md:py-4 text-sm md:text-base font-medium text-gray-900">
              Buying
            </td>
            <td className="px-4 md:px-6 py-3 md:py-4 text-sm md:text-base text-gray-700">
              32–34L
            </td>
            <td className="px-4 md:px-6 py-3 md:py-4 text-sm md:text-base text-green-600 font-semibold">
              20–25L
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  );
}