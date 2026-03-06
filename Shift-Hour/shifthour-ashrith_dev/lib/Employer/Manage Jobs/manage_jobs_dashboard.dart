import 'package:flutter/material.dart';

class Manage_Jobs_HomePage extends StatefulWidget {
  const Manage_Jobs_HomePage({Key? key}) : super(key: key);

  @override
  _ManageHomePageState createState() => _ManageHomePageState();
}

class _ManageHomePageState extends State<Manage_Jobs_HomePage> {
  String searchQuery = "";
  String sortBy = "Recent";
  String activeTab = "all";

  final List<String> sortOptions = ["Recent", "Date", "Pay Rate"];

  final List<Map<String, dynamic>> jobs = [
    {
      "title": "Warehouse Associate",
      "status": "Open",
      "statusColor": const Color(0xFF10B981), // emerald-500
      "workers": "3/5",
      "date": "June 15, 2023",
      "time": "9:00 AM - 5:00 PM",
      "location": "Seattle, WA",
      "pay": "\$18.50/hr",
      "company": "Amazon",
      "companyLogo": "/placeholder.svg?height=40&width=40",
      "urgent": true,
    },
    {
      "title": "Event Staff",
      "status": "In Progress",
      "statusColor": const Color(0xFF3B82F6), // blue-500
      "workers": "10/10",
      "date": "June 10, 2023",
      "time": "6:00 PM - 11:00 PM",
      "location": "Portland, OR",
      "pay": "\$22.00/hr",
      "company": "EventPro",
      "companyLogo": "/placeholder.svg?height=40&width=40",
      "urgent": false,
    },
    {
      "title": "Delivery Driver",
      "status": "Completed",
      "statusColor": const Color(0xFF6B7280), // gray-500
      "workers": "2/2",
      "date": "June 5, 2023",
      "time": "8:00 AM - 4:00 PM",
      "location": "San Francisco, CA",
      "pay": "\$25.00/hr",
      "company": "DoorDash",
      "companyLogo": "/placeholder.svg?height=40&width=40",
      "urgent": false,
    },
    {
      "title": "Customer Service Representative",
      "status": "Open",
      "statusColor": const Color(0xFF10B981), // emerald-500
      "workers": "0/3",
      "date": "June 20, 2023",
      "time": "10:00 AM - 6:00 PM",
      "location": "Remote",
      "pay": "\$20.00/hr",
      "company": "Zendesk",
      "companyLogo": "/placeholder.svg?height=40&width=40",
      "urgent": true,
    },
    {
      "title": "Retail Associate",
      "status": "Cancelled",
      "statusColor": const Color(0xFFEF4444), // red-500
      "workers": "0/2",
      "date": "June 8, 2023",
      "time": "11:00 AM - 7:00 PM",
      "location": "Chicago, IL",
      "pay": "\$16.50/hr",
      "company": "Target",
      "companyLogo": "/placeholder.svg?height=40&width=40",
      "urgent": false,
    },
  ];

  List<Map<String, dynamic>> get filteredJobs {
    return jobs.where((job) {
      if (activeTab == "all") return true;
      if (activeTab == "open") return job["status"] == "Open";
      if (activeTab == "in-progress") return job["status"] == "In Progress";
      if (activeTab == "completed") return job["status"] == "Completed";
      if (activeTab == "cancelled") return job["status"] == "Cancelled";
      return true;
    }).toList();
  }

  @override
  Widget build(BuildContext context) {
    final isDark = Theme.of(context).brightness == Brightness.dark;

    return Scaffold(
      body: Column(
        children: [
          // Header with gradient
          Container(
            decoration: BoxDecoration(
              gradient: LinearGradient(
                colors: [
                  Theme.of(context).primaryColor,
                  Theme.of(context).primaryColor.withOpacity(0.8),
                ],
                begin: Alignment.centerLeft,
                end: Alignment.centerRight,
              ),
              boxShadow: [
                BoxShadow(
                  color: Colors.black.withOpacity(0.1),
                  blurRadius: 4,
                  offset: const Offset(0, 2),
                ),
              ],
            ),
            padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 24),
            child: SafeArea(
              child: Row(
                children: [
                  // Back Button
                  Container(
                    margin: const EdgeInsets.only(right: 12),
                    decoration: BoxDecoration(
                      color: Colors.white.withOpacity(0.2),
                      shape: BoxShape.circle,
                    ),
                    child: IconButton(
                      icon: const Icon(Icons.arrow_back, color: Colors.white),
                      onPressed: () {
                        // Handle back navigation
                        Navigator.pop(context);
                      },
                    ),
                  ),
                  // Title
                  const Expanded(
                    child: Text(
                      'Job Management',
                      style: TextStyle(
                        fontSize: 24,
                        fontWeight: FontWeight.bold,
                        color: Colors.white,
                      ),
                    ),
                  ),
                  // Avatar
                  Container(
                    decoration: BoxDecoration(
                      shape: BoxShape.circle,
                      border: Border.all(
                        color: Colors.white.withOpacity(0.2),
                        width: 2,
                      ),
                    ),
                    child: const CircleAvatar(
                      radius: 18,
                      backgroundImage: AssetImage('assets/placeholder.png'),
                      child: Text('JD', style: TextStyle(color: Colors.white)),
                    ),
                  ),
                ],
              ),
            ),
          ),

          // Main Content
          Expanded(
            child: Padding(
              padding: const EdgeInsets.all(16.0),
              child: Column(
                children: [
                  // Search and Filter Card
                  Card(
                    elevation: 1,
                    shape: RoundedRectangleBorder(
                      borderRadius: BorderRadius.circular(12),
                    ),
                    color:
                        isDark
                            ? const Color(0xFF1E293B)
                            : Colors.white, // slate-900 or white
                    child: Padding(
                      padding: const EdgeInsets.all(16.0),
                      child: Column(
                        children: [
                          Row(
                            children: [
                              Expanded(
                                child: TextField(
                                  onChanged: (value) {
                                    setState(() {
                                      searchQuery = value;
                                    });
                                  },
                                  decoration: InputDecoration(
                                    hintText:
                                        'Search jobs by title, location, or company...',
                                    prefixIcon: const Icon(Icons.search),
                                    fillColor:
                                        isDark
                                            ? const Color(
                                              0xFF1E293B,
                                            ) // slate-800
                                            : const Color(
                                              0xFFF8FAFC,
                                            ), // slate-50
                                    filled: true,
                                    border: OutlineInputBorder(
                                      borderRadius: BorderRadius.circular(8),
                                      borderSide: BorderSide(
                                        color:
                                            isDark
                                                ? const Color(
                                                  0xFF334155,
                                                ) // slate-700
                                                : const Color(
                                                  0xFFE2E8F0,
                                                ), // slate-200
                                      ),
                                    ),
                                    contentPadding: const EdgeInsets.symmetric(
                                      vertical: 10,
                                    ),
                                  ),
                                ),
                              ),
                              const SizedBox(width: 8),

                              // Sort Dropdown
                              PopupMenuButton<String>(
                                onSelected: (value) {
                                  setState(() {
                                    sortBy = value;
                                  });
                                },
                                child: Container(
                                  padding: const EdgeInsets.symmetric(
                                    horizontal: 12,
                                    vertical: 8,
                                  ),
                                  decoration: BoxDecoration(
                                    border: Border.all(
                                      color:
                                          isDark
                                              ? const Color(
                                                0xFF334155,
                                              ) // slate-700
                                              : const Color(
                                                0xFFE2E8F0,
                                              ), // slate-200
                                    ),
                                    borderRadius: BorderRadius.circular(8),
                                  ),
                                  child: Row(
                                    children: [
                                      Text('Sort: $sortBy'),
                                      const SizedBox(width: 4),
                                      const Icon(
                                        Icons.keyboard_arrow_down,
                                        size: 16,
                                      ),
                                    ],
                                  ),
                                ),
                                itemBuilder:
                                    (context) =>
                                        sortOptions
                                            .map(
                                              (option) => PopupMenuItem<String>(
                                                value: option,
                                                child: Text(option),
                                              ),
                                            )
                                            .toList(),
                              ),

                              const SizedBox(width: 8),

                              // Filter Button
                              Container(
                                decoration: BoxDecoration(
                                  border: Border.all(
                                    color:
                                        isDark
                                            ? const Color(
                                              0xFF334155,
                                            ) // slate-700
                                            : const Color(
                                              0xFFE2E8F0,
                                            ), // slate-200
                                  ),
                                  borderRadius: BorderRadius.circular(8),
                                ),
                                child: IconButton(
                                  icon: const Icon(Icons.filter_list),
                                  onPressed: () {},
                                ),
                              ),
                            ],
                          ),
                        ],
                      ),
                    ),
                  ),

                  const SizedBox(height: 24),

                  // Tabs
                  Container(
                    decoration: BoxDecoration(
                      color:
                          isDark
                              ? const Color(0xFF1E293B) // slate-800
                              : const Color(0xFFF1F5F9), // slate-100
                      borderRadius: BorderRadius.circular(12),
                    ),
                    padding: const EdgeInsets.all(4),
                    child: Row(
                      children: [
                        _buildTab('All Jobs', 'all'),
                        _buildTab('Open', 'open'),
                        _buildTab('In Progress', 'in-progress'),
                        _buildTab('Completed', 'completed'),
                        _buildTab('Cancelled', 'cancelled'),
                      ],
                    ),
                  ),

                  const SizedBox(height: 24),

                  // Job List
                  Expanded(
                    child:
                        filteredJobs.isEmpty
                            ? _buildEmptyState(isDark)
                            : ListView.builder(
                              itemCount: filteredJobs.length,
                              itemBuilder: (context, index) {
                                return Padding(
                                  padding: const EdgeInsets.only(bottom: 16),
                                  child: JobCard(
                                    job: filteredJobs[index],
                                    isDark: isDark,
                                  ),
                                );
                              },
                            ),
                  ),
                ],
              ),
            ),
          ),
        ],
      ),

      // FAB
      floatingActionButton: FloatingActionButton(
        onPressed: () {},
        backgroundColor: Theme.of(context).primaryColor,
        child: const Icon(Icons.add),
      ),
    );
  }

  Widget _buildTab(String title, String value) {
    return Expanded(
      child: GestureDetector(
        onTap: () {
          setState(() {
            activeTab = value;
          });
        },
        child: Container(
          padding: const EdgeInsets.symmetric(vertical: 12),
          decoration: BoxDecoration(
            color:
                activeTab == value
                    ? Theme.of(context).brightness == Brightness.dark
                        ? const Color(0xFF0F172A) // slate-900
                        : Colors.white
                    : Colors.transparent,
            borderRadius: BorderRadius.circular(8),
          ),
          child: Text(
            title,
            textAlign: TextAlign.center,
            style: TextStyle(
              fontWeight:
                  activeTab == value ? FontWeight.bold : FontWeight.normal,
            ),
          ),
        ),
      ),
    );
  }

  Widget _buildEmptyState(bool isDark) {
    return Center(
      child: Column(
        mainAxisAlignment: MainAxisAlignment.center,
        children: [
          Container(
            width: 64,
            height: 64,
            decoration: BoxDecoration(
              color:
                  isDark
                      ? const Color(0xFF1E293B) // slate-800
                      : const Color(0xFFF1F5F9), // slate-100
              shape: BoxShape.circle,
            ),
            child: Icon(
              Icons.work_outline,
              size: 32,
              color:
                  isDark
                      ? const Color(0xFF94A3B8) // slate-400
                      : const Color(0xFF94A3B8), // slate-400
            ),
          ),
          const SizedBox(height: 16),
          const Text(
            'No jobs found',
            style: TextStyle(fontSize: 18, fontWeight: FontWeight.w500),
          ),
          const SizedBox(height: 8),
          const Text(
            'Try adjusting your search or filter criteria',
            style: TextStyle(color: Colors.grey),
          ),
        ],
      ),
    );
  }
}

class JobCard extends StatelessWidget {
  final Map<String, dynamic> job;
  final bool isDark;

  const JobCard({Key? key, required this.job, required this.isDark})
    : super(key: key);

  @override
  Widget build(BuildContext context) {
    return Card(
      elevation: 0,
      shape: RoundedRectangleBorder(
        borderRadius: BorderRadius.circular(12),
        side: BorderSide(
          color:
              isDark
                  ? const Color(0xFF1E293B) // slate-800
                  : const Color(0xFFE2E8F0), // slate-200
        ),
      ),
      color:
          isDark ? const Color(0xFF0F172A) : Colors.white, // slate-900 or white
      child: Padding(
        padding: const EdgeInsets.all(20),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            // Urgent Badge
            if (job["urgent"])
              Container(
                margin: const EdgeInsets.only(bottom: 12),
                padding: const EdgeInsets.symmetric(
                  horizontal: 10,
                  vertical: 6,
                ),
                decoration: BoxDecoration(
                  color:
                      isDark
                          ? const Color(0xFF7F1D1D).withOpacity(0.2) // red-950
                          : const Color(0xFFFEE2E2), // red-50
                  borderRadius: BorderRadius.circular(16),
                  border: Border.all(
                    color:
                        isDark
                            ? const Color(0xFF991B1B) // red-800
                            : const Color(0xFFFCA5A5), // red-200
                  ),
                ),
                child: const Text(
                  'Urgent: Apply Now',
                  style: TextStyle(
                    color: Color(0xFFDC2626), // red-600
                    fontWeight: FontWeight.w500,
                    fontSize: 12,
                  ),
                ),
              ),

            // Job Header
            Row(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                // Company Logo
                Container(
                  width: 48,
                  height: 48,
                  decoration: BoxDecoration(
                    color: Theme.of(context).primaryColor.withOpacity(0.1),
                    borderRadius: BorderRadius.circular(8),
                  ),
                  child: Center(
                    child: Text(
                      job["company"].substring(0, 2).toUpperCase(),
                      style: TextStyle(
                        color: Theme.of(context).primaryColor,
                        fontWeight: FontWeight.bold,
                      ),
                    ),
                  ),
                ),

                const SizedBox(width: 16),

                // Job Info
                Expanded(
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      Text(
                        job["title"],
                        style: const TextStyle(
                          fontWeight: FontWeight.w600,
                          fontSize: 18,
                        ),
                      ),
                      Text(
                        job["company"],
                        style: TextStyle(
                          color: isDark ? Colors.grey[400] : Colors.grey[600],
                          fontSize: 14,
                        ),
                      ),
                      const SizedBox(height: 8),

                      // Status and Workers
                      Row(
                        children: [
                          // Status Badge
                          Container(
                            padding: const EdgeInsets.symmetric(
                              horizontal: 10,
                              vertical: 4,
                            ),
                            decoration: BoxDecoration(
                              color: job["statusColor"],
                              borderRadius: BorderRadius.circular(20),
                            ),
                            child: Text(
                              job["status"],
                              style: const TextStyle(
                                color: Colors.white,
                                fontSize: 12,
                                fontWeight: FontWeight.w500,
                              ),
                            ),
                          ),

                          const SizedBox(width: 8),

                          // Workers Badge
                          Container(
                            padding: const EdgeInsets.symmetric(
                              horizontal: 10,
                              vertical: 4,
                            ),
                            decoration: BoxDecoration(
                              color:
                                  isDark
                                      ? const Color(0xFF1E293B) // slate-800
                                      : const Color(0xFFF1F5F9), // slate-100
                              borderRadius: BorderRadius.circular(20),
                            ),
                            child: Text(
                              '${job["workers"]} Workers',
                              style: const TextStyle(fontSize: 12),
                            ),
                          ),
                        ],
                      ),
                    ],
                  ),
                ),

                // Action Buttons
                Row(
                  children: [
                    IconButton(
                      icon: const Icon(Icons.copy, size: 18),
                      onPressed: () {},
                      color: Colors.grey,
                    ),
                    IconButton(
                      icon: const Icon(Icons.edit, size: 18),
                      onPressed: () {},
                      color: Colors.grey,
                    ),
                    IconButton(
                      icon: const Icon(Icons.visibility, size: 18),
                      onPressed: () {},
                      color: Colors.grey,
                    ),
                  ],
                ),
              ],
            ),

            const Divider(height: 32),

            // Job Details
            Row(
              children: [
                // Left Column
                Expanded(
                  child: Column(
                    children: [
                      // Date
                      Row(
                        children: [
                          Container(
                            width: 32,
                            height: 32,
                            decoration: BoxDecoration(
                              color:
                                  isDark
                                      ? const Color(0xFF1E293B) // slate-800
                                      : const Color(0xFFF1F5F9), // slate-100
                              shape: BoxShape.circle,
                            ),
                            child: const Icon(
                              Icons.calendar_today,
                              size: 16,
                              color: Colors.grey,
                            ),
                          ),
                          const SizedBox(width: 8),
                          Text(job["date"]),
                        ],
                      ),

                      const SizedBox(height: 8),

                      // Time
                      Row(
                        children: [
                          Container(
                            width: 32,
                            height: 32,
                            decoration: BoxDecoration(
                              color:
                                  isDark
                                      ? const Color(0xFF1E293B) // slate-800
                                      : const Color(0xFFF1F5F9), // slate-100
                              shape: BoxShape.circle,
                            ),
                            child: const Icon(
                              Icons.access_time,
                              size: 16,
                              color: Colors.grey,
                            ),
                          ),
                          const SizedBox(width: 8),
                          Text(job["time"]),
                        ],
                      ),
                    ],
                  ),
                ),

                // Right Column
                Expanded(
                  child: Column(
                    children: [
                      // Location
                      Row(
                        children: [
                          Container(
                            width: 32,
                            height: 32,
                            decoration: BoxDecoration(
                              color:
                                  isDark
                                      ? const Color(0xFF1E293B) // slate-800
                                      : const Color(0xFFF1F5F9), // slate-100
                              shape: BoxShape.circle,
                            ),
                            child: const Icon(
                              Icons.location_on,
                              size: 16,
                              color: Colors.grey,
                            ),
                          ),
                          const SizedBox(width: 8),
                          Text(job["location"]),
                        ],
                      ),

                      const SizedBox(height: 8),

                      // Pay
                      Row(
                        children: [
                          Container(
                            width: 32,
                            height: 32,
                            decoration: BoxDecoration(
                              color:
                                  isDark
                                      ? const Color(0xFF1E293B) // slate-800
                                      : const Color(0xFFF1F5F9), // slate-100
                              shape: BoxShape.circle,
                            ),
                            child: const Icon(
                              Icons.attach_money,
                              size: 16,
                              color: Colors.grey,
                            ),
                          ),
                          const SizedBox(width: 8),
                          Text(
                            job["pay"],
                            style: const TextStyle(fontWeight: FontWeight.w500),
                          ),
                        ],
                      ),
                    ],
                  ),
                ),
              ],
            ),

            // View Details Button
            Container(
              margin: const EdgeInsets.only(top: 16),
              padding: const EdgeInsets.only(top: 16),
              decoration: BoxDecoration(
                border: Border(
                  top: BorderSide(
                    color:
                        isDark
                            ? const Color(0xFF1E293B) // slate-800
                            : const Color(0xFFE2E8F0), // slate-200
                  ),
                ),
              ),
              child: Align(
                alignment: Alignment.centerRight,
                child: TextButton(
                  onPressed: () {},
                  style: TextButton.styleFrom(
                    backgroundColor: Theme.of(
                      context,
                    ).primaryColor.withOpacity(0.1),
                    padding: const EdgeInsets.symmetric(
                      horizontal: 16,
                      vertical: 8,
                    ),
                    shape: RoundedRectangleBorder(
                      borderRadius: BorderRadius.circular(8),
                    ),
                  ),
                  child: Text(
                    'View Details',
                    style: TextStyle(color: Theme.of(context).primaryColor),
                  ),
                ),
              ),
            ),
          ],
        ),
      ),
    );
  }
}
