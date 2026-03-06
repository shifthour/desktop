import HealthKit

class HealthManager {
    let healthStore = HKHealthStore()

    func requestAuthorization(completion: @escaping (Bool) -> Void) {
        guard HKHealthStore.isHealthDataAvailable() else {
            completion(false)
            return
        }

        let stepType = HKQuantityType.quantityType(forIdentifier: .stepCount)!

        let readTypes: Set = [stepType]

        healthStore.requestAuthorization(toShare: [], read: readTypes) { success, error in
            DispatchQueue.main.async {
                completion(success)
            }
        }
    }

    func fetchStepData(completion: @escaping ([HKQuantitySample]) -> Void) {
        let stepType = HKQuantityType.quantityType(forIdentifier: .stepCount)!
        let startDate = Calendar.current.date(byAdding: .day, value: -7, to: Date())
        let predicate = HKQuery.predicateForSamples(withStart: startDate, end: Date())

        let query = HKSampleQuery(sampleType: stepType, predicate: predicate, limit: HKObjectQueryNoLimit, sortDescriptors: nil) { _, samples, _ in
            guard let quantitySamples = samples as? [HKQuantitySample] else {
                completion([])
                return
            }

            completion(quantitySamples)
        }

        healthStore.execute(query)
    }
}
