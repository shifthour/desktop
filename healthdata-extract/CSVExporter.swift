import Foundation

class CSVExporter {
    func generateCSV(from data: [Date: Double]) -> URL? {
        var csvText = "Date,Steps\n"
        let formatter = ISO8601DateFormatter()

        for (date, steps) in data.sorted(by: { $0.key < $1.key }) {
            let dateStr = formatter.string(from: date)
            csvText.append("\(dateStr),\(steps)\n")
        }

        // Save to Documents directory
        let fileName = "health_data.csv"
        if let dir = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask).first {
            let fileURL = dir.appendingPathComponent(fileName)
            do {
                try csvText.write(to: fileURL, atomically: true, encoding: .utf8)
                print("✅ CSV File Created at: \(fileURL)")
                return fileURL
            } catch {
                print("❌ Failed to write CSV: \(error)")
            }
        }
        return nil
    }
}

