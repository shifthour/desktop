import 'package:flutter/material.dart';

// Define ServiceItem at the top level, before any classes that use it
class ServiceItem extends StatelessWidget {
  final IconData icon;
  final String title;
  final String subtitle;
  final List<Color> gradientColors;

  const ServiceItem({
    Key? key,
    required this.icon,
    required this.title,
    required this.subtitle,
    required this.gradientColors,
  }) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return Container(
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(12),
        border: Border.all(color: Colors.indigo.shade50),
        boxShadow: [
          BoxShadow(
            color: Colors.black.withOpacity(0.05),
            blurRadius: 4,
            offset: const Offset(0, 2),
          ),
        ],
      ),
      child: Material(
        color: Colors.transparent,
        borderRadius: BorderRadius.circular(12),
        child: InkWell(
          onTap: () {},
          borderRadius: BorderRadius.circular(12),
          child: Padding(
            padding: const EdgeInsets.all(16),
            child: Column(
              mainAxisAlignment: MainAxisAlignment.center,
              mainAxisSize: MainAxisSize.min,
              children: [
                // Icon with gradient background
                Container(
                  padding: const EdgeInsets.all(12),
                  decoration: BoxDecoration(
                    gradient: LinearGradient(
                      colors: gradientColors,
                      begin: Alignment.topLeft,
                      end: Alignment.bottomRight,
                    ),
                    borderRadius: BorderRadius.circular(12),
                  ),
                  child: Icon(icon, color: Colors.white, size: 32),
                ),
                const SizedBox(height: 16),

                // Title
                Flexible(
                  child: Text(
                    title,
                    style: const TextStyle(
                      fontSize: 16,
                      fontWeight: FontWeight.w600,
                      color: Color(0xFF1E1A5A),
                    ),
                    textAlign: TextAlign.center,
                    overflow: TextOverflow.ellipsis,
                    maxLines: 2,
                  ),
                ),
                const SizedBox(height: 4),

                // Subtitle
                Flexible(
                  child: Text(
                    subtitle,
                    style: const TextStyle(
                      fontSize: 14,
                      color: Color(0xFF4338CA),
                    ),
                    textAlign: TextAlign.center,
                    overflow: TextOverflow.ellipsis,
                    maxLines: 2,
                  ),
                ),
              ],
            ),
          ),
        ),
      ),
    );
  }
}

class HelpAndSupportPage extends StatefulWidget {
  const HelpAndSupportPage({Key? key}) : super(key: key);

  @override
  State<HelpAndSupportPage> createState() => _HelpAndSupportPageState();
}

class _HelpAndSupportPageState extends State<HelpAndSupportPage> {
  final TextEditingController _searchController = TextEditingController();
  int _tabIndex = 0;

  @override
  void dispose() {
    _searchController.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      // Add the app bar here
      appBar: AppBar(
        backgroundColor: Colors.white,
        elevation: 0,
        title: const Text(
          'Help & Support',
          style: TextStyle(
            color: Color(0xFF1E1A5A),
            fontWeight: FontWeight.bold,
            fontSize: 20,
          ),
        ),
        leading: IconButton(
          icon: const Icon(Icons.arrow_back, color: Color(0xFF4F46E5)),
          onPressed: () {
            Navigator.pop(context);
          },
        ),
        actions: [
          IconButton(
            icon: const Icon(Icons.search, color: Color(0xFF4F46E5)),
            onPressed: () {
              // Implement search functionality
            },
          ),
        ],
      ),
      body: Container(
        decoration: const BoxDecoration(
          gradient: LinearGradient(
            begin: Alignment.topCenter,
            end: Alignment.bottomCenter,
            colors: [Color(0xFFE8F0FF), Color(0xFFEEECFF)],
          ),
        ),
        child: SafeArea(
          child: Column(
            children: [
              // Main content
              Expanded(
                child: ListView(
                  padding: const EdgeInsets.all(16),
                  children: const [
                    SizedBox(height: 8),

                    // Contact Support Card
                    ContactSupportCard(),

                    SizedBox(height: 24),

                    // Help Center Card
                    HelpCenterCard(),

                    SizedBox(height: 24),

                    // Support Tickets Card
                    SupportTicketsCard(),

                    SizedBox(height: 24),

                    // Self-Service Card
                    SelfServiceCard(),

                    SizedBox(height: 24),

                    // Community Support Card
                    CommunitySupportCard(),

                    SizedBox(height: 32),
                  ],
                ),
              ),
            ],
          ),
        ),
      ),
    );
  }
}

class ContactSupportCard extends StatelessWidget {
  const ContactSupportCard({Key? key}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return Card(
      elevation: 8,
      shadowColor: Colors.black26,
      shape: const RoundedRectangleBorder(
        borderRadius: BorderRadius.all(Radius.circular(12)),
      ),
      clipBehavior: Clip.antiAlias,
      margin: EdgeInsets.zero,
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.stretch,
        children: [
          // Gradient top bar
          Container(
            height: 4,
            decoration: const BoxDecoration(
              gradient: LinearGradient(
                colors: [Color(0xFF3B82F6), Color(0xFF4F46E5)],
              ),
            ),
          ),

          // Card content
          Padding(
            padding: const EdgeInsets.all(20),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                const Text(
                  'Contact Support',
                  style: TextStyle(
                    fontSize: 24,
                    fontWeight: FontWeight.bold,
                    color: Color(0xFF1E1A5A),
                  ),
                ),
                const SizedBox(height: 20),

                // Live Chat
                _ContactOption(
                  title: 'Live Chat',
                  subtitle: 'Chat with a support agent',
                  buttonText: 'Start Chat',
                  icon: Icons.message_rounded,
                  iconColor: Colors.blue,
                ),

                const Divider(height: 32, thickness: 0.5),

                // Phone Support
                _ContactOption(
                  title: 'Phone Support',
                  subtitle: '(555) 123-4567',
                  buttonText: 'Call Now',
                  icon: Icons.phone,
                  iconColor: Colors.green,
                ),

                const Divider(height: 32, thickness: 0.5),

                // Email Support
                _ContactOption(
                  title: 'Email Support',
                  subtitle: 'support@workplatform.com',
                  buttonText: 'Email Us',
                  icon: Icons.mail,
                  iconColor: Colors.purple,
                ),

                const SizedBox(height: 20),

                // Support Hours
                Container(
                  padding: const EdgeInsets.all(16),
                  decoration: BoxDecoration(
                    color: const Color(0xFFF0F1FF),
                    borderRadius: BorderRadius.circular(12),
                  ),
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      const Text(
                        'Support Hours',
                        style: TextStyle(
                          fontSize: 14,
                          fontWeight: FontWeight.w600,
                          color: Color(0xFF312E81),
                        ),
                      ),
                      const SizedBox(height: 8),
                      Row(
                        mainAxisAlignment: MainAxisAlignment.spaceBetween,
                        children: [
                          Column(
                            crossAxisAlignment: CrossAxisAlignment.start,
                            children: const [
                              Text(
                                'Monday-Friday',
                                style: TextStyle(
                                  fontSize: 12,
                                  fontWeight: FontWeight.w500,
                                  color: Color(0xFF4338CA),
                                ),
                              ),
                              Text(
                                '8am-8pm',
                                style: TextStyle(
                                  fontSize: 14,
                                  fontWeight: FontWeight.bold,
                                  color: Color(0xFF312E81),
                                ),
                              ),
                            ],
                          ),
                          Column(
                            crossAxisAlignment: CrossAxisAlignment.start,
                            children: const [
                              Text(
                                'Saturday-Sunday',
                                style: TextStyle(
                                  fontSize: 12,
                                  fontWeight: FontWeight.w500,
                                  color: Color(0xFF4338CA),
                                ),
                              ),
                              Text(
                                '9am-5pm',
                                style: TextStyle(
                                  fontSize: 14,
                                  fontWeight: FontWeight.bold,
                                  color: Color(0xFF312E81),
                                ),
                              ),
                            ],
                          ),
                        ],
                      ),
                    ],
                  ),
                ),
              ],
            ),
          ),
        ],
      ),
    );
  }
}

class _ContactOption extends StatelessWidget {
  final String title;
  final String subtitle;
  final String buttonText;
  final IconData icon;
  final Color iconColor;

  const _ContactOption({
    Key? key,
    required this.title,
    required this.subtitle,
    required this.buttonText,
    required this.icon,
    required this.iconColor,
  }) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return Row(
      children: [
        // Icon container
        Container(
          padding: const EdgeInsets.all(12),
          decoration: BoxDecoration(
            color: Colors.white,
            borderRadius: BorderRadius.circular(12),
            boxShadow: [
              BoxShadow(
                color: Colors.black.withOpacity(0.05),
                blurRadius: 5,
                offset: const Offset(0, 2),
              ),
            ],
          ),
          child: Icon(icon, color: iconColor, size: 20),
        ),
        const SizedBox(width: 16),

        // Text information
        Expanded(
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Text(
                title,
                style: const TextStyle(
                  fontSize: 16,
                  fontWeight: FontWeight.w600,
                  color: Color(0xFF1E1A5A),
                ),
              ),
              Text(
                subtitle,
                style: const TextStyle(fontSize: 14, color: Color(0xFF4338CA)),
              ),
            ],
          ),
        ),

        // Button
        ElevatedButton.icon(
          onPressed: () {},
          icon: const Text(''),
          label: Row(
            mainAxisSize: MainAxisSize.min,
            children: [
              Text(buttonText, style: const TextStyle(color: Colors.white)),
              Text('', style: const TextStyle(color: Colors.white)),
              const SizedBox(width: 4),
              const Icon(Icons.arrow_forward, size: 16, color: Colors.white),
            ],
          ),
          style: ElevatedButton.styleFrom(
            backgroundColor: const Color(0xFF4F46E5),
            padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 10),
            shape: RoundedRectangleBorder(
              borderRadius: BorderRadius.circular(8),
            ),
            elevation: 2,
            foregroundColor:
                Colors.white, // This sets the default text and icon color
          ),
        ),
      ],
    );
  }
}

class HelpCenterCard extends StatefulWidget {
  const HelpCenterCard({Key? key}) : super(key: key);

  @override
  State<HelpCenterCard> createState() => _HelpCenterCardState();
}

class _HelpCenterCardState extends State<HelpCenterCard>
    with SingleTickerProviderStateMixin {
  late TabController _tabController;
  final TextEditingController _searchController = TextEditingController();
  int _expandedFaqIndex = 0;

  @override
  void initState() {
    super.initState();
    _tabController = TabController(length: 3, vsync: this);
  }

  @override
  void dispose() {
    _tabController.dispose();
    _searchController.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return Card(
      elevation: 8,
      shadowColor: Colors.black26,
      shape: const RoundedRectangleBorder(
        borderRadius: BorderRadius.all(Radius.circular(12)),
      ),
      clipBehavior: Clip.antiAlias,
      margin: EdgeInsets.zero,
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.stretch,
        children: [
          // Gradient top bar
          Container(
            height: 4,
            decoration: const BoxDecoration(
              gradient: LinearGradient(
                colors: [Color(0xFF8B5CF6), Color(0xFFEC4899)],
              ),
            ),
          ),

          // Card content
          Padding(
            padding: const EdgeInsets.all(20),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                // Header with title and button
                Row(
                  children: [
                    const Expanded(
                      child: Text(
                        'Help Center',
                        style: TextStyle(
                          fontSize: 24,
                          fontWeight: FontWeight.bold,
                          color: Color(0xFF1E1A5A),
                        ),
                      ),
                    ),

                    // Create Ticket button
                    Container(
                      decoration: BoxDecoration(
                        gradient: const LinearGradient(
                          colors: [Color(0xFF8B5CF6), Color(0xFFEC4899)],
                        ),
                        borderRadius: BorderRadius.circular(8),
                        boxShadow: [
                          BoxShadow(
                            color: const Color(0xFF8B5CF6).withOpacity(0.3),
                            blurRadius: 8,
                            offset: const Offset(0, 2),
                          ),
                        ],
                      ),
                      child: ElevatedButton.icon(
                        onPressed: () {},
                        icon: const Text('Create Ticket'),
                        label: const Icon(Icons.arrow_forward, size: 16),
                        style: ElevatedButton.styleFrom(
                          backgroundColor: Colors.transparent,
                          shadowColor: Colors.transparent,
                          padding: const EdgeInsets.symmetric(
                            horizontal: 16,
                            vertical: 10,
                          ),
                        ),
                      ),
                    ),
                  ],
                ),

                const SizedBox(height: 20),

                // Search input
                TextField(
                  controller: _searchController,
                  decoration: InputDecoration(
                    hintText: 'Search for help...',
                    prefixIcon: const Icon(
                      Icons.search,
                      color: Color(0xFF6366F1),
                    ),
                    fillColor: Colors.white,
                    filled: true,
                    border: OutlineInputBorder(
                      borderRadius: BorderRadius.circular(12),
                      borderSide: BorderSide(color: Colors.indigo.shade100),
                    ),
                    enabledBorder: OutlineInputBorder(
                      borderRadius: BorderRadius.circular(12),
                      borderSide: BorderSide(color: Colors.indigo.shade100),
                    ),
                    focusedBorder: OutlineInputBorder(
                      borderRadius: BorderRadius.circular(12),
                      borderSide: BorderSide(color: Colors.indigo.shade300),
                    ),
                    contentPadding: const EdgeInsets.symmetric(vertical: 16),
                  ),
                ),

                const SizedBox(height: 24),

                // Tabs
                Container(
                  decoration: BoxDecoration(
                    color: const Color(0xFFF0F1FF),
                    borderRadius: BorderRadius.circular(12),
                  ),
                  padding: const EdgeInsets.all(4),
                  child: TabBar(
                    controller: _tabController,
                    indicator: BoxDecoration(
                      color: Colors.white,
                      borderRadius: BorderRadius.circular(8),
                      boxShadow: [
                        BoxShadow(
                          color: Colors.black.withOpacity(0.05),
                          blurRadius: 4,
                          offset: const Offset(0, 2),
                        ),
                      ],
                    ),
                    labelColor: const Color(0xFF1E1A5A),
                    unselectedLabelColor: const Color(0xFF6366F1),
                    tabs: const [
                      Tab(text: 'FAQs'),
                      Tab(text: 'My Tickets'),
                      Tab(text: 'Guides'),
                    ],
                  ),
                ),

                const SizedBox(height: 20),

                // Tab content
                SizedBox(
                  height: 300,
                  child: TabBarView(
                    controller: _tabController,
                    children: [
                      // FAQs Tab
                      Container(
                        padding: const EdgeInsets.all(16),
                        decoration: BoxDecoration(
                          color: const Color(0xFFF0F1FF),
                          borderRadius: BorderRadius.circular(12),
                        ),
                        child: ListView(
                          shrinkWrap: true,
                          children: [
                            _buildFaqItem(
                              index: 0,
                              question: 'How do payments get processed?',
                              answer:
                                  'Payments are processed every Friday for the previous week\'s work. Direct deposits typically appear in your account within 1-2 business days.',
                            ),
                            const SizedBox(height: 12),
                            _buildFaqItem(
                              index: 1,
                              question:
                                  'What is the shift cancellation policy?',
                            ),
                            const SizedBox(height: 12),
                            _buildFaqItem(
                              index: 2,
                              question:
                                  'How do I update my skills and availability?',
                            ),
                            const SizedBox(height: 12),
                            _buildFaqItem(
                              index: 3,
                              question: 'What is the tardiness policy?',
                            ),
                            const SizedBox(height: 12),
                            _buildFaqItem(
                              index: 4,
                              question: 'How can I dispute a rating?',
                            ),
                          ],
                        ),
                      ),

                      // My Tickets Tab
                      Container(
                        padding: const EdgeInsets.all(32),
                        decoration: BoxDecoration(
                          color: const Color(0xFFF0F1FF),
                          borderRadius: BorderRadius.circular(12),
                        ),
                        child: Column(
                          mainAxisAlignment: MainAxisAlignment.center,
                          children: const [
                            Icon(
                              Icons.message_rounded,
                              size: 48,
                              color: Color(0xFF6366F1),
                            ),
                            SizedBox(height: 12),
                            Text(
                              'Your tickets will appear here',
                              style: TextStyle(
                                fontSize: 18,
                                fontWeight: FontWeight.w500,
                                color: Color(0xFF6366F1),
                              ),
                            ),
                          ],
                        ),
                      ),

                      // Guides Tab
                      Container(
                        padding: const EdgeInsets.all(32),
                        decoration: BoxDecoration(
                          color: const Color(0xFFF0F1FF),
                          borderRadius: BorderRadius.circular(12),
                        ),
                        child: Column(
                          mainAxisAlignment: MainAxisAlignment.center,
                          children: const [
                            Icon(
                              Icons.play_circle,
                              size: 48,
                              color: Color(0xFF6366F1),
                            ),
                            SizedBox(height: 12),
                            Text(
                              'Guides will appear here',
                              style: TextStyle(
                                fontSize: 18,
                                fontWeight: FontWeight.w500,
                                color: Color(0xFF6366F1),
                              ),
                            ),
                          ],
                        ),
                      ),
                    ],
                  ),
                ),
              ],
            ),
          ),
        ],
      ),
    );
  }

  Widget _buildFaqItem({
    required int index,
    required String question,
    String? answer,
  }) {
    bool isExpanded = _expandedFaqIndex == index;

    return Container(
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(12),
        boxShadow: [
          BoxShadow(
            color: Colors.black.withOpacity(0.05),
            blurRadius: 4,
            offset: const Offset(0, 2),
          ),
        ],
      ),
      child: Material(
        color: Colors.transparent,
        child: InkWell(
          onTap: () {
            setState(() {
              _expandedFaqIndex = isExpanded ? -1 : index;
            });
          },
          borderRadius: BorderRadius.circular(12),
          child: Column(
            children: [
              Padding(
                padding: const EdgeInsets.all(16),
                child: Row(
                  children: [
                    Expanded(
                      child: Text(
                        question,
                        style: const TextStyle(
                          fontSize: 16,
                          fontWeight: FontWeight.w600,
                          color: Color(0xFF1E1A5A),
                        ),
                      ),
                    ),
                    Icon(
                      isExpanded
                          ? Icons.keyboard_arrow_up
                          : Icons.keyboard_arrow_down,
                      color: const Color(0xFF6366F1),
                    ),
                  ],
                ),
              ),
              if (isExpanded && answer != null)
                Container(
                  width: double.infinity,
                  padding: const EdgeInsets.fromLTRB(16, 0, 16, 16),
                  decoration: BoxDecoration(
                    border: Border(
                      top: BorderSide(color: Colors.indigo.shade100, width: 1),
                    ),
                  ),
                  child: Text(
                    answer,
                    style: TextStyle(
                      fontSize: 15,
                      color: Colors.indigo.shade800,
                    ),
                  ),
                ),
            ],
          ),
        ),
      ),
    );
  }
}

class SupportTicketsCard extends StatelessWidget {
  const SupportTicketsCard({Key? key}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return Card(
      elevation: 8,
      shadowColor: Colors.black26,
      shape: const RoundedRectangleBorder(
        borderRadius: BorderRadius.all(Radius.circular(12)),
      ),
      clipBehavior: Clip.antiAlias,
      margin: EdgeInsets.zero,
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.stretch,
        children: [
          // Gradient top bar
          Container(
            height: 4,
            decoration: const BoxDecoration(
              gradient: LinearGradient(
                colors: [Color(0xFF10B981), Color(0xFF14B8A6)],
              ),
            ),
          ),

          // Card content
          Padding(
            padding: const EdgeInsets.all(20),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                const Text(
                  'My Support Tickets',
                  style: TextStyle(
                    fontSize: 24,
                    fontWeight: FontWeight.bold,
                    color: Color(0xFF1E1A5A),
                  ),
                ),
                const SizedBox(height: 20),

                // Tickets list
                Container(
                  padding: const EdgeInsets.all(16),
                  decoration: BoxDecoration(
                    color: const Color(0xFFF0F1FF),
                    borderRadius: BorderRadius.circular(12),
                  ),
                  child: Column(
                    children: [
                      // Ticket 1
                      _TicketItem(
                        title: 'Payment not received',
                        category: 'Payments',
                        date: 'Jan 15, 2023',
                        status: 'Open',
                        statusColor: Colors.green,
                      ),
                      const SizedBox(height: 12),

                      // Ticket 2
                      _TicketItem(
                        title: 'App crashing during check-in',
                        category: 'Technical',
                        date: 'Jan 10, 2023',
                        status: 'In Progress',
                        statusColor: Colors.amber,
                      ),
                      const SizedBox(height: 12),

                      // Ticket 3
                      _TicketItem(
                        title: 'Unable to update availability',
                        category: 'Account',
                        date: 'Dec 28, 2022',
                        status: 'Resolved',
                        statusColor: Colors.grey,
                      ),
                    ],
                  ),
                ),
              ],
            ),
          ),
        ],
      ),
    );
  }
}

class _TicketItem extends StatelessWidget {
  final String title;
  final String category;
  final String date;
  final String status;
  final Color statusColor;

  const _TicketItem({
    Key? key,
    required this.title,
    required this.category,
    required this.date,
    required this.status,
    required this.statusColor,
  }) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return Container(
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(12),
        boxShadow: [
          BoxShadow(
            color: Colors.black.withOpacity(0.05),
            blurRadius: 4,
            offset: const Offset(0, 2),
          ),
        ],
      ),
      child: Material(
        color: Colors.transparent,
        borderRadius: BorderRadius.circular(12),
        child: InkWell(
          onTap: () {},
          borderRadius: BorderRadius.circular(12),
          child: Padding(
            padding: const EdgeInsets.all(16),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Row(
                  children: [
                    // Status badge
                    Container(
                      padding: const EdgeInsets.symmetric(
                        horizontal: 8,
                        vertical: 4,
                      ),
                      decoration: BoxDecoration(
                        color: statusColor,
                        borderRadius: BorderRadius.circular(4),
                      ),
                      child: Text(
                        status,
                        style: const TextStyle(
                          color: Colors.white,
                          fontSize: 12,
                          fontWeight: FontWeight.w500,
                        ),
                      ),
                    ),
                    const SizedBox(width: 12),

                    // Title
                    Expanded(
                      child: Text(
                        title,
                        style: const TextStyle(
                          fontSize: 16,
                          fontWeight: FontWeight.w600,
                          color: Color(0xFF1E1A5A),
                        ),
                      ),
                    ),
                  ],
                ),
                const SizedBox(height: 8),

                // Details
                Text(
                  'Category: $category • Created: $date',
                  style: TextStyle(fontSize: 14, color: Colors.indigo.shade700),
                ),
                const SizedBox(height: 12),

                // View details button
                Align(
                  alignment: Alignment.centerRight,
                  child: ElevatedButton.icon(
                    onPressed: () {},
                    icon: const Text(
                      'View Details',
                      style: TextStyle(color: Colors.white),
                    ),

                    label: const Icon(Icons.arrow_forward, size: 16),
                    style: ElevatedButton.styleFrom(
                      backgroundColor: const Color(0xFF4F46E5),
                      padding: const EdgeInsets.symmetric(
                        horizontal: 16,
                        vertical: 10,
                      ),
                      shape: RoundedRectangleBorder(
                        borderRadius: BorderRadius.circular(8),
                      ),
                    ),
                  ),
                ),
              ],
            ),
          ),
        ),
      ),
    );
  }
}

class SelfServiceCard extends StatelessWidget {
  const SelfServiceCard({Key? key}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return Card(
      elevation: 8,
      shadowColor: Colors.black26,
      shape: const RoundedRectangleBorder(
        borderRadius: BorderRadius.all(Radius.circular(12)),
      ),
      clipBehavior: Clip.antiAlias,
      margin: EdgeInsets.zero,
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.stretch,
        children: [
          // Gradient top bar
          Container(
            height: 4,
            decoration: const BoxDecoration(
              gradient: LinearGradient(
                colors: [Color(0xFF3B82F6), Color(0xFF06B6D4)],
              ),
            ),
          ),

          // Card content
          Padding(
            padding: const EdgeInsets.all(20),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                const Text(
                  'Self-Service Troubleshooting',
                  style: TextStyle(
                    fontSize: 24,
                    fontWeight: FontWeight.bold,
                    color: Color(0xFF1E1A5A),
                  ),
                ),
                const SizedBox(height: 20),

                // Service items grid
                GridView.count(
                  shrinkWrap: true,
                  physics: const NeverScrollableScrollPhysics(),
                  crossAxisCount: 3,
                  crossAxisSpacing: 12,
                  mainAxisSpacing: 12,
                  childAspectRatio: 0.8,
                  children: const [
                    ServiceItem(
                      icon: Icons.settings,
                      title: 'Troubleshooter',
                      subtitle: 'Step-by-step guides',
                      gradientColors: [Color(0xFF3B82F6), Color(0xFF4F46E5)],
                    ),
                    ServiceItem(
                      icon: Icons.play_circle,
                      title: 'Video Tutorials',
                      subtitle: 'Visual learning',
                      gradientColors: [Color(0xFF4F46E5), Color(0xFF8B5CF6)],
                    ),
                    ServiceItem(
                      icon: Icons.wifi,
                      title: 'System Status',
                      subtitle: 'Platform health',
                      gradientColors: [Color(0xFF8B5CF6), Color(0xFFEC4899)],
                    ),
                  ],
                ),
              ],
            ),
          ),
        ],
      ),
    );
  }
}

class CommunitySupportCard extends StatelessWidget {
  const CommunitySupportCard({Key? key}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return Card(
      elevation: 8,
      shadowColor: Colors.black26,
      shape: const RoundedRectangleBorder(
        borderRadius: BorderRadius.all(Radius.circular(12)),
      ),
      clipBehavior: Clip.antiAlias,
      margin: EdgeInsets.zero,
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.stretch,
        children: [
          // Gradient top bar
          Container(
            height: 4,
            decoration: const BoxDecoration(
              gradient: LinearGradient(
                colors: [Color(0xFFF59E0B), Color(0xFFFBBF24)],
              ),
            ),
          ),

          // Card content
          Padding(
            padding: const EdgeInsets.all(20),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                const Text(
                  'Community Support',
                  style: TextStyle(
                    fontSize: 24,
                    fontWeight: FontWeight.bold,
                    color: Color(0xFF1E1A5A),
                  ),
                ),
                const SizedBox(height: 20),

                // Service items grid
                GridView.count(
                  shrinkWrap: true,
                  physics: const NeverScrollableScrollPhysics(),
                  crossAxisCount: 2,
                  crossAxisSpacing: 12,
                  mainAxisSpacing: 12,
                  childAspectRatio: 0.9,
                  children: const [
                    CommunityServiceItem(
                      icon: Icons.forum,
                      title: 'Worker Forum',
                      subtitle: 'Peer-to-peer help',
                      gradientColors: [Color(0xFFFBBF24), Color(0xFFF59E0B)],
                    ),
                    CommunityServiceItem(
                      icon: Icons.thumb_up,
                      title: 'Top Solutions',
                      subtitle: 'Community voted',
                      gradientColors: [Color(0xFFF59E0B), Color(0xFFEF4444)],
                    ),
                  ],
                ),
              ],
            ),
          ),
        ],
      ),
    );
  }
}

class CommunityServiceItem extends StatelessWidget {
  final IconData icon;
  final String title;
  final String subtitle;
  final List<Color> gradientColors;

  const CommunityServiceItem({
    Key? key,
    required this.icon,
    required this.title,
    required this.subtitle,
    required this.gradientColors,
  }) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return Container(
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(12),
        border: Border.all(color: Colors.indigo.shade50),
        boxShadow: [
          BoxShadow(
            color: Colors.black.withOpacity(0.05),
            blurRadius: 4,
            offset: const Offset(0, 2),
          ),
        ],
      ),
      child: Material(
        color: Colors.transparent,
        borderRadius: BorderRadius.circular(12),
        child: InkWell(
          onTap: () {},
          borderRadius: BorderRadius.circular(12),
          child: Padding(
            padding: const EdgeInsets.all(16),
            child: Column(
              mainAxisAlignment: MainAxisAlignment.center,
              children: [
                // Icon with gradient background
                Container(
                  padding: const EdgeInsets.all(12),
                  decoration: BoxDecoration(
                    gradient: LinearGradient(
                      colors: gradientColors,
                      begin: Alignment.topLeft,
                      end: Alignment.bottomRight,
                    ),
                    borderRadius: BorderRadius.circular(12),
                  ),
                  child: Icon(icon, color: Colors.white, size: 32),
                ),
                const SizedBox(height: 16),

                // Title
                Text(
                  title,
                  style: const TextStyle(
                    fontSize: 16,
                    fontWeight: FontWeight.w600,
                    color: Color(0xFF1E1A5A),
                  ),
                  textAlign: TextAlign.center,
                ),
                const SizedBox(height: 4),

                // Subtitle
                Text(
                  subtitle,
                  style: const TextStyle(
                    fontSize: 14,
                    color: Color(0xFF4338CA),
                  ),
                  textAlign: TextAlign.center,
                ),
              ],
            ),
          ),
        ),
      ),
    );
  }
}
