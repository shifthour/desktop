import SwiftUI

struct ContentView: View {
    var body: some View {
        Text("Health Data Export App")
            .padding()
            .onAppear {
                let healthManager = HealthManager()
                healthManager.requestAuthorization { granted in
                    if granted {
                        healthManager.fetchStepData { data in
                            if let path = CSVExporter().generateCSV(from: data) {
                                print("CSV saved at: \(path)")
                            }
                        }
                    }
                }
            }
    }
}

struct ContentView_Previews: PreviewProvider {
    static var previews: some View {
        ContentView()
    }
}

