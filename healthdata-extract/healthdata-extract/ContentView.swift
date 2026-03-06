import Foundation
import HealthKit

class HealthManager {
    let healthStore = HKHealthStore()
    
    func requestAuthorization(completion: @escaping (Bool) -> Void) {
        guard HKHealthStore.isHealthDataAvailable() else {
            completion(false)
            return
        }

        let stepType = HKObjectType.quantityType(forIdentifier: .stepCount)!
        healthStore.requestAuthorization(toShare: nil, read: [stepType]) { success, _ in
            completion(success)
        }
    }

    func fetchStepData(completion: @escaping ([Date: Double]) -> Void) {
        guard let stepType = HKQuantityType.quantityType(forIdentifier: .stepCount) else { return }
        
        let calendar = Calendar.current
        let now = Date()
        let startDate = calendar.date(byAdding: .day, value: -7, to: now)!

        let predicate = HKQuery.predicateForSamples(withStart: startDate, end: now, options: [])
        let interval = DateComponents(day: 1)

        let query = HKStatisticsCollectionQuery(
            quantityType: stepType,
            quantitySamplePredicate: predicate,
            options: .cumulativeSum,
            anchorDate: startDate,
            intervalComponents: interval
        )

        query.initialResultsHandler = { _, results, _ in
            var stepData: [Date: Double] = [:]
            results?.enumerateStatistics(from: startDate, to: now) { stat, _ in
                if let quantity = stat.sumQuantity() {
                    let steps = quantity.doubleValue(for: .count())
                    stepData[stat.startDate] = steps
                }
            }
            completion(stepData)
        }

        healthStore.execute(query)
    }
}

