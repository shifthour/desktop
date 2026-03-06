import { Card, CardContent } from "@/components/ui/card"
import { Button } from "@/components/ui/button"

export default function ContactSection() {
  return (
    <section className="py-16 bg-gray-50">
      <div className="container mx-auto px-4">
        <div className="max-w-md mx-auto">
          <Card>
            <CardContent className="p-6">
              <div className="mb-6 text-center">
                <h3 className="text-2xl font-bold mb-2">Get In Touch With Us</h3>
                <p className="text-gray-600">Pre-Bookings Open Now</p>
              </div>

              <form className="space-y-4">
                <input
                  type="text"
                  placeholder="Enter name"
                  className="w-full p-3 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                  required
                />

                <div className="flex space-x-2">
                  <select className="w-1/3 p-3 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500">
                    <option value="India (+91)">India (+91)</option>
                    <option value="UAE (+971)">UAE (+971)</option>
                    <option value="USA (+1)">USA (+1)</option>
                  </select>
                  <input
                    type="tel"
                    placeholder="Phone Number"
                    className="w-2/3 p-3 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                    required
                  />
                </div>

                <input
                  type="email"
                  placeholder="Enter email"
                  className="w-full p-3 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                />

                <Button type="submit" className="w-full bg-blue-600 hover:bg-blue-700">
                  Submit
                </Button>

                <p className="text-xs text-gray-500 text-center">🔒 We hate spam, and we respect your privacy.</p>
              </form>
            </CardContent>
          </Card>
        </div>
      </div>
    </section>
  )
}
