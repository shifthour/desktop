import SwiftUI

struct ContentView: View {
    var body: some View {
        Text("Exporting Health Data...")
            .padding()
            .onAppear {
                let healthManager = HealthManager()
                
                healthManager.requestAuthorization { granted in
                    if granted {
                        healthManager.fetchStepData { data in
                            if let path = CSVExporter().generateCSV(from: data) {
                                print("✅ CSV saved at: \(path)")

                                // ✅ Print CSV contents to console
                                do {
                                    let csvContent = try String(contentsOfFile: path, encoding: .utf8)
                                    print("📄 CSV Content:\n\(csvContent)")
                                } catch {
                                    print("❌ Failed to read CSV: \(error)")
                                }

                            } else {
                                print("❌ Failed to save CSV")
                            }
                        }
                    } else {
                        print("❌ HealthKit permission not granted")
                    }
                }
            }
    }
}

#Preview {
    ContentView()
}

