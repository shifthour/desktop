import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Clock, Phone, Mail, Calendar } from "lucide-react"

const activities = [
  {
    id: 1,
    type: "call",
    title: "Called Dr. Anu Rang",
    description: "Discussed ND 1000 Spectrophotometer requirements",
    time: "2 hours ago",
    status: "completed",
    icon: Phone,
  },
  {
    id: 2,
    type: "email",
    title: "Email sent to Kerala Agricultural University",
    description: "Quotation for ND 1000 Spectrophotometer",
    time: "4 hours ago",
    status: "sent",
    icon: Mail,
  },
  {
    id: 3,
    type: "meeting",
    title: "Site visit scheduled",
    description: "Installation planning for TSAR Labcare",
    time: "1 day ago",
    status: "scheduled",
    icon: Calendar,
  },
  {
    id: 4,
    type: "call",
    title: "Follow-up call with Eurofins",
    description: "AMC renewal discussion",
    time: "2 days ago",
    status: "pending",
    icon: Phone,
  },
]

const statusColors = {
  completed: "bg-green-100 text-green-800",
  sent: "bg-blue-100 text-blue-800",
  scheduled: "bg-yellow-100 text-yellow-800",
  pending: "bg-orange-100 text-orange-800",
}

export function RecentActivities() {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center">
          <Clock className="w-5 h-5 mr-2" />
          Recent Activities
        </CardTitle>
        <CardDescription>Your latest interactions and tasks</CardDescription>
      </CardHeader>
      <CardContent>
        <div className="space-y-4">
          {activities.map((activity) => (
            <div key={activity.id} className="flex items-start space-x-3 p-3 rounded-lg hover:bg-gray-50">
              <div className="flex-shrink-0">
                <div className="w-8 h-8 bg-blue-100 rounded-full flex items-center justify-center">
                  <activity.icon className="w-4 h-4 text-blue-600" />
                </div>
              </div>
              <div className="flex-1 min-w-0">
                <p className="text-sm font-medium text-gray-900">{activity.title}</p>
                <p className="text-sm text-gray-500">{activity.description}</p>
                <div className="flex items-center justify-between mt-2">
                  <p className="text-xs text-gray-400">{activity.time}</p>
                  <Badge className={statusColors[activity.status as keyof typeof statusColors]}>
                    {activity.status}
                  </Badge>
                </div>
              </div>
            </div>
          ))}
        </div>
      </CardContent>
    </Card>
  )
}
