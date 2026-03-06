import { Service } from '@/types';

export const services: Service[] = [
  {
    id: 'research-development',
    title: 'Research & Development',
    description: 'Comprehensive R&D services with cutting-edge equipment and expert guidance for breakthrough discoveries.',
    features: [
      'State-of-the-art laboratory facilities',
      'Expert scientific consultation',
      'Custom research protocols',
      'Data analysis and reporting',
      'Regulatory compliance support'
    ],
    icon: 'BeakerIcon',
    href: '/services/research-development'
  },
  {
    id: 'lab-equipment',
    title: 'Lab Equipment Access',
    description: 'Access professional-grade laboratory equipment without the overhead costs of ownership.',
    features: [
      'Advanced analytical instruments',
      'Flexible booking system',
      'Training and support included',
      'Maintenance and calibration',
      'Competitive hourly rates'
    ],
    icon: 'CogIcon',
    href: '/services/lab-equipment'
  },
  {
    id: 'consulting',
    title: 'Technical Consulting',
    description: 'Expert technical consulting to accelerate your research projects and solve complex challenges.',
    features: [
      'Project planning and strategy',
      'Technical problem solving',
      'Process optimization',
      'Quality assurance',
      'Regulatory guidance'
    ],
    icon: 'LightBulbIcon',
    href: '/services/consulting'
  },
  {
    id: 'custom-solutions',
    title: 'Custom Solutions',
    description: 'Tailored solutions designed specifically for your unique research requirements and objectives.',
    features: [
      'Bespoke research design',
      'Custom equipment setups',
      'Specialized methodologies',
      'Dedicated project management',
      'Full documentation'
    ],
    icon: 'PuzzlePieceIcon',
    href: '/services/custom-solutions'
  }
];