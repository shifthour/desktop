import Foundation
import HealthKit

class CSVExporter {
    func generateCSV(from samples: [HKQuantitySample]) -> String? {
        var csvString = "Date,Steps\n"
        
        for sample in samples {
            let date = ISO8601DateFormatter().string(from: sample.startDate)
            let steps = sample.quantity.doubleValue(for: .count())
            csvString += "\(date),\(Int(steps))\n"
        }
        
        guard let directory = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask).first else {
            return nil
        }
        
        let fileURL = directory.appendingPathComponent("HealthData.csv")
        
        do {
            try csvString.write(to: fileURL, atomically: true, encoding: .utf8)
            print("✅ CSV file saved at: \(fileURL.path)")

            // ✅ Print contents to console
            let fileContents = try String(contentsOf: fileURL, encoding: .utf8)
            print("📄 CSV Content:\n\(fileContents)")

            return fileURL.path
        } catch {
            print("❌ Error saving or reading CSV: \(error)")
            return nil
        }
    }
}

