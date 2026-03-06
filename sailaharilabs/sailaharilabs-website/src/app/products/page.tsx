'use client';

import { motion, useInView, AnimatePresence } from 'framer-motion';
import { useRef, useState } from 'react';
import Image from 'next/image';
import Link from 'next/link';

// Types
type Product = {
  id: number;
  name: string;
  category: string;
  subcategory: string;
  description: string;
  image: string;
  features: string[];
};

type Category = {
  id: string;
  name: string;
  subcategories?: { id: string; name: string }[];
};

// Product Categories with Subcategories (matching old website structure)
const categories: Category[] = [
  { id: 'all', name: 'All Products' },
  {
    id: 'sample-prep',
    name: 'Sample Preparation Equipment',
    subcategories: [
      { id: 'media-diluters', name: 'Automated Media Diluters' },
      { id: 'media-dispensers', name: 'Automated Media Dispensers' },
      { id: 'media-preparator', name: 'Automated Media Preparator' },
      { id: 'tissue-dissociator', name: 'Tissue Dissociator' },
      { id: 'microtomes', name: 'Microtomes' },
      { id: 'centrifuges', name: 'Centrifuges' },
    ]
  },
  {
    id: 'analytical',
    name: 'Analytical Instruments',
    subcategories: [
      { id: 'spectrophotometer', name: 'Spectrophotometer' },
      { id: 'spr-spectrometer', name: 'SPR Spectrometer' },
      { id: 'pcr', name: 'PCR / Thermal Cycler' },
    ]
  },
  {
    id: 'imaging',
    name: 'Imaging & Microscope',
    subcategories: [
      { id: 'gel-imaging', name: 'Gel Documentation System' },
      { id: 'chemi-imager', name: 'Chemiluminescence Imager' },
      { id: 'live-cell', name: 'Live Cell Imaging' },
      { id: 'cell-counter', name: 'Automated Cell Counter' },
      { id: 'cell-separation', name: 'Cell Separation' },
    ]
  },
  {
    id: 'storage',
    name: 'Sample Storage',
    subcategories: [
      { id: 'co2-incubator', name: 'CO2 Incubators' },
    ]
  },
  {
    id: 'general',
    name: 'General Lab Equipment',
    subcategories: [
      { id: 'transilluminator', name: 'Transilluminator' },
      { id: 'electrophoresis', name: 'Electrophoresis' },
    ]
  },
];

// Products Data - From Labgig.in with actual images and subcategories
const products: Product[] = [
  // Sample Preparation Equipment - PCR/Thermal Cyclers (moved to Analytical)
  // Sample Preparation Equipment - Microtomes
  {
    id: 4,
    name: 'S700A Rotary Paraffin Microtome',
    category: 'sample-prep',
    subcategory: 'microtomes',
    description: 'Fully automatic paraffin slicer used extensively in medical and biological research for tissue sectioning after wax embedding.',
    image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/LabGig-S700A.png?resize=600%2C600&ssl=1',
    features: ['Fully Automatic', 'Precision Slicing', 'Medical Grade'],
  },
  {
    id: 5,
    name: 'S700 Rotary Paraffin Microtome',
    category: 'sample-prep',
    subcategory: 'microtomes',
    description: 'Semi-automatic paraffin microtome that embeds tissue in wax and slices it for clinical and research applications.',
    image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/LabGig-S700-Semi-Automatic.png?resize=600%2C600&ssl=1',
    features: ['Semi-Automatic', 'Clinical Use', 'Research Grade'],
  },
  {
    id: 6,
    name: 'FS800A Cryostat',
    category: 'sample-prep',
    subcategory: 'microtomes',
    description: 'Fully automatic cryostat with independent refrigeration systems for pathological diagnosis, immunohistochemistry, and spatial transcriptomics.',
    image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/LabGig-FS800A.png?resize=600%2C600&ssl=1',
    features: ['Fully Automatic', 'Independent Cooling', 'Multi-Application'],
  },
  // Sample Preparation Equipment - Tissue Dissociator
  {
    id: 7,
    name: 'DSC-800 Tissue Dissociator',
    category: 'sample-prep',
    subcategory: 'tissue-dissociator',
    description: 'A smarter way to dissociate single-cell suspension from human/mouse/rat tissue with high cell viability.',
    image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/LabGig-DSC800.png?resize=600%2C600&ssl=1',
    features: ['Single-Cell Prep', 'High Viability', 'Multi-Species'],
  },
  {
    id: 8,
    name: 'DSC-400 Tissue Dissociator',
    category: 'sample-prep',
    subcategory: 'tissue-dissociator',
    description: 'Designed to quickly prepare tissue into single-cell suspension with high cell viability or tissue homogenate.',
    image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/LabGig-DSC400.png?resize=600%2C600&ssl=1',
    features: ['Quick Prep', 'High Repeatability', 'Versatile'],
  },
  // Sample Preparation Equipment - Automated Media Dispensers
  {
    id: 9,
    name: 'DISTRIWEL-440',
    category: 'sample-prep',
    subcategory: 'media-dispensers',
    description: 'Automatic petri dish filling and stacking machine that allows you to prepare up to 440 plates (15ml) within 30 minutes.',
    image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/LabGig-Distriwel-440.jpg?resize=600%2C600&ssl=1',
    features: ['440 Plates/30min', 'Auto Stacking', 'High Throughput'],
  },
  {
    id: 14,
    name: 'POLYWEL',
    category: 'sample-prep',
    subcategory: 'media-dispensers',
    description: 'Automated tubes and bottles filler for rapid, versatile, and accurate dispensing of media, diluents, and other liquids from 1 to 9999.9 ml.',
    image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/LabGig-Polywel.jpg?resize=600%2C600&ssl=1',
    features: ['1-9999.9ml Range', 'Versatile', 'High Accuracy'],
  },
  // Sample Preparation Equipment - Automated Media Diluters
  {
    id: 10,
    name: 'DILUWEL Gravimetric Diluter',
    category: 'sample-prep',
    subcategory: 'media-diluters',
    description: 'Rapid, accurate gravimetric diluter for combining solid samples with the correct amount of diluent. Automates initial sample dilution tasks.',
    image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/LabGig-Diluwel-2.jpg?resize=600%2C600&ssl=1',
    features: ['Gravimetric', 'High Accuracy', 'Automated'],
  },
  {
    id: 11,
    name: 'DILUWEL-XL',
    category: 'sample-prep',
    subcategory: 'media-diluters',
    description: 'Large volume gravimetric diluter for samples from 10 to 375g with distribution precision <99%. High-flow pump at 6L/min.',
    image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/LabGig-DILUWEL-XL.jpg?resize=600%2C600&ssl=1',
    features: ['Large Volume', '6L/min Flow', '99% Precision'],
  },
  // Sample Preparation Equipment - Automated Media Preparator
  {
    id: 12,
    name: 'MEDIAWEL-10',
    category: 'sample-prep',
    subcategory: 'media-preparator',
    description: 'Automatic media preparator with 1-10L capacity for producing agar, broths, and dilution fluids rapidly and accurately.',
    image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/LabGig-Mediawel-10.png?resize=600%2C600&ssl=1',
    features: ['1-10L Capacity', 'Multi-Media', 'Automated'],
  },
  {
    id: 13,
    name: 'MEDIAWEL-30',
    category: 'sample-prep',
    subcategory: 'media-preparator',
    description: 'Larger capacity automatic media preparator (30L) for quick and accurate production of culture media and solutions.',
    image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/LabGig-Mediawel-30.png?resize=600%2C600&ssl=1',
    features: ['30L Capacity', 'Consistent Quality', 'Rapid Production'],
  },
  // Sample Preparation Equipment - Centrifuges
  {
    id: 15,
    name: 'TurboFuge Microcentrifuge',
    category: 'sample-prep',
    subcategory: 'centrifuges',
    description: 'Compact microcentrifuge with 24/36-place capacity providing flexible and efficient solutions with higher safety standards.',
    image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/LabGig-TurboFuge.png?resize=600%2C600&ssl=1',
    features: ['24/36 Place', 'Compact', 'High Safety'],
  },
  {
    id: 16,
    name: 'M1324R Micro High Speed Centrifuge',
    category: 'sample-prep',
    subcategory: 'centrifuges',
    description: 'Micro centrifuge available in room temperature and refrigerated versions. Rotation speed reaches 15,800 rpm with smooth operation.',
    image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/M1324-M1324R.png?resize=600%2C600&ssl=1',
    features: ['15,800 RPM', 'Refrigerated Option', 'Smooth Operation'],
  },
  {
    id: 17,
    name: 'M1416R High Speed Refrigerated Centrifuge',
    category: 'sample-prep',
    subcategory: 'centrifuges',
    description: 'Maximum speed 18,000 rpm (30,000×g) with 4×400mL capacity. Features horizontal rotor with adapters.',
    image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/LabGig-M1416R.png?resize=600%2C600&ssl=1',
    features: ['18,000 RPM', '30,000×g', '4×400mL'],
  },
  // Analytical Instruments - Spectrophotometer
  {
    id: 18,
    name: 'EzDrop 1000C Spectrophotometer',
    category: 'analytical',
    subcategory: 'spectrophotometer',
    description: 'Micro-volume/cuvette spectrophotometer that completes sample concentration measurement in just 3 seconds.',
    image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/LabGig-EzDrop-1000C.png?resize=600%2C600&ssl=1',
    features: ['3 Second Results', 'Dual Mode', 'Micro-Volume'],
  },
  {
    id: 19,
    name: 'EzDrop 1000 Spectrophotometer',
    category: 'analytical',
    subcategory: 'spectrophotometer',
    description: 'Micro-volume UV/Vis spectrophotometer that accelerates your research efficiency with rapid measurements.',
    image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/LabGig-EzDrop-1000.png?resize=600%2C600&ssl=1',
    features: ['UV/Vis Range', 'Micro-Volume', 'High Efficiency'],
  },
  // Analytical Instruments - SPR Spectrometer
  {
    id: 20,
    name: 'ezSPR',
    category: 'analytical',
    subcategory: 'spr-spectrometer',
    description: 'Leading edge bioanalytical platform for clinical analysis and life sciences. Benchtop Surface Plasmon Resonance instrument.',
    image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/LabGig-ezSPR.jpg?resize=600%2C600&ssl=1',
    features: ['SPR Technology', 'Benchtop', 'Life Sciences'],
  },
  {
    id: 21,
    name: 'P4SPR Dual Inlet Module',
    category: 'analytical',
    subcategory: 'spr-spectrometer',
    description: 'Surface Plasmon Resonance for rapid testing. Everyday tool for researchers to quickly develop bioassays and biomolecular interaction characterizations.',
    image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/LabGig-P4SPR-Dual-Inlet.png?resize=600%2C600&ssl=1',
    features: ['Rapid Testing', 'Dual Inlet', 'Bioassay Development'],
  },
  {
    id: 22,
    name: 'P4SPR Quad Inlet Module',
    category: 'analytical',
    subcategory: 'spr-spectrometer',
    description: 'Advanced SPR system for rapid bioassay development and biomolecular interaction analysis from the convenience of your lab bench.',
    image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/LabGig-P4SPR-Quad-Inlet.png?resize=600%2C600&ssl=1',
    features: ['Quad Inlet', 'Lab Bench Friendly', 'Advanced Analysis'],
  },
  // Analytical Instruments - PCR
  {
    id: 1,
    name: 'TurboCycler 2',
    category: 'analytical',
    subcategory: 'pcr',
    description: 'Thermal cycler designed specifically to enhance PCR efficiency and accuracy with 7-inch touchscreen and intuitive graphic user interface.',
    image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/LabGig-TurboCycler2.png?resize=600%2C600&ssl=1',
    features: ['7" Touchscreen', 'PCR Optimization', 'Intuitive UI'],
  },
  {
    id: 2,
    name: 'TurboCycler Lite',
    category: 'analytical',
    subcategory: 'pcr',
    description: 'Economical gradient thermal cycler with versatile capabilities, ideal choice for routine PCR tasks at an affordable price.',
    image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/LabGig-TurboCycler-Lite.png?resize=600%2C600&ssl=1',
    features: ['Gradient Cycling', 'Affordable', 'Routine PCR'],
  },
  {
    id: 3,
    name: 'MiniTurbo',
    category: 'analytical',
    subcategory: 'pcr',
    description: 'Portable thermal cycler designed for researchers who need to proceed with PCR right after sample collection. Performs like benchtop thermal cyclers.',
    image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/LabGig-MiniTurbo_PCR.png?resize=600%2C600&ssl=1',
    features: ['Highly Portable', 'Compact Design', 'Field Ready'],
  },
  {
    id: 23,
    name: 'M2-96G PCR Amplifier',
    category: 'analytical',
    subcategory: 'pcr',
    description: 'Provides precise temperature control and intelligent user operating system for rapid and specific amplification of target DNA fragments.',
    image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/LabGig-M2-96G.png?resize=600%2C600&ssl=1',
    features: ['96-Well', 'Precise Control', 'Intelligent System'],
  },
  // Imaging & Microscope - Gel Documentation System
  {
    id: 24,
    name: 'Magic Gel Doc System',
    category: 'imaging',
    subcategory: 'gel-imaging',
    description: 'Multifunctional imager combining gel documentation with compact design. Smart, space-saving with optional removable industrial computer.',
    image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/LabGig-Magic-Del-Doc-System.png?resize=600%2C600&ssl=1',
    features: ['Multifunctional', 'Compact', 'Industrial Computer'],
  },
  {
    id: 26,
    name: 'Magic Multifunctional Imager',
    category: 'imaging',
    subcategory: 'gel-imaging',
    description: 'Versatile imaging system with sensitive camera technology, electronic lifting platform, and automatic autofocus for various sample sizes.',
    image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/Magic-Multifunctional-Imager.jpg?resize=600%2C600&ssl=1',
    features: ['Auto Autofocus', 'Electronic Lift', 'Multi-Sample'],
  },
  // Imaging & Microscope - Chemiluminescence Imager
  {
    id: 25,
    name: 'Magic Mini Chemi Imager',
    category: 'imaging',
    subcategory: 'chemi-imager',
    description: 'Smart, space-saving chemiluminescence imaging system with optional removable industrial computer for flexibility.',
    image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/LabGig-Magic-Mini-Chemi-Imager.png?resize=600%2C600&ssl=1',
    features: ['Chemiluminescence', 'Space-Saving', 'High Flexibility'],
  },
  // Imaging & Microscope - Live Cell Imaging
  {
    id: 27,
    name: 'EzScope 101',
    category: 'imaging',
    subcategory: 'live-cell',
    description: 'Live cell imaging system that enables observation without removing cells from incubator. Streamlines research workflows.',
    image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/LabGig-EzScope-101.png?resize=600%2C600&ssl=1',
    features: ['Live Cell Imaging', 'Incubator Compatible', 'Real-Time'],
  },
  // Imaging & Microscope - Automated Cell Counter
  {
    id: 28,
    name: 'C100-Pro Automated Cell Counter',
    category: 'imaging',
    subcategory: 'cell-counter',
    description: 'Reliable cell counting device with highly intelligent analysis software and excellent microscopy optics structure. A real time-saver.',
    image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/LabGig-C100-Pro.png?resize=600%2C600&ssl=1',
    features: ['Intelligent Analysis', 'Excellent Optics', 'Time-Saving'],
  },
  {
    id: 29,
    name: 'C100/C100-SE Cell Counter',
    category: 'imaging',
    subcategory: 'cell-counter',
    description: 'Automated cell counters providing two models for BF&FL cell counting and analysis in just 9 seconds.',
    image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/LabGig-C100C100-SE.png?resize=600%2C600&ssl=1',
    features: ['9 Second Analysis', 'BF & FL Modes', 'Dual Models'],
  },
  {
    id: 30,
    name: 'Moxi GO II',
    category: 'imaging',
    subcategory: 'cell-counter',
    description: 'Mini automated cell counter combining Coulter Principle with laser-based fluorescence detection. 488nm laser with dual PMT channels.',
    image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/LabGig-Moxi-Go-II.png?resize=600%2C600&ssl=1',
    features: ['488nm Laser', 'Dual PMT', 'Coulter Principle'],
  },
  {
    id: 31,
    name: 'Moxi Z Cell Counter',
    category: 'imaging',
    subcategory: 'cell-counter',
    description: 'Combines Coulter Principle with patented thin-film sensor technology for highly accurate cell counting.',
    image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/LabGig-Moxi-Z.jpg?resize=600%2C600&ssl=1',
    features: ['Patented Sensor', 'High Accuracy', 'Advanced Tech'],
  },
  {
    id: 32,
    name: 'Moxi V Cell Analyzer',
    category: 'imaging',
    subcategory: 'cell-counter',
    description: 'Provides combination of volumetric cell sizing with simultaneous fluorescence for accurate cell counting, size analysis, and viability assessment.',
    image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/LabGig-Moxi_V.png?resize=600%2C600&ssl=1',
    features: ['Volumetric Sizing', 'Fluorescence', 'Viability Analysis'],
  },
  // Imaging & Microscope - Cell Separation
  {
    id: 33,
    name: 'Nano Magnetic Beads',
    category: 'imaging',
    subcategory: 'cell-separation',
    description: 'Self-developed magnetic bead sorting kits enabling rapid target cell isolation with high purity and viability. No elution required.',
    image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/LabGig-Magnetic-Microbeads.png?resize=600%2C600&ssl=1',
    features: ['High Purity', 'No Elution', 'Flow Cytometry Ready'],
  },
  // Sample Storage - CO2 Incubator
  {
    id: 34,
    name: 'D180 CO2 Incubator',
    category: 'storage',
    subcategory: 'co2-incubator',
    description: 'Air-jacketed carbon dioxide incubator providing suitable and stable environment for cell/tissue culture with 7-day performance trend chart.',
    image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/LabGig-D180-CO2-Incubator.png?resize=600%2C600&ssl=1',
    features: ['Smart Touchscreen', '7-Day Trending', 'Stable Environment'],
  },
  // General Lab Equipment - Transilluminator
  {
    id: 35,
    name: 'BLooK LED Transilluminator',
    category: 'general',
    subcategory: 'transilluminator',
    description: 'Extraordinary blue light LED transilluminator for detection of nucleic acids or proteins under non-UV conditions. 470nm wavelength prevents sample damage.',
    image: 'https://i0.wp.com/labgig.in/wp-content/uploads/woocommerce-placeholder.png?resize=600%2C600&ssl=1',
    features: ['470nm Blue LED', 'Non-UV', 'Sample Safe'],
  },
  {
    id: 36,
    name: 'Blue LED Transilluminator',
    category: 'general',
    subcategory: 'transilluminator',
    description: 'Used to observe, cut, and photograph electrophoresis gels. Compatible with SYBR Safe, SYBR Gold, SYBR Green, SYPRO Ruby, and more.',
    image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/LabGig-Blue-LED-Transilluminator.png?resize=600%2C600&ssl=1',
    features: ['Multi-Dye Compatible', 'Gel Photography', 'Safe Visualization'],
  },
  {
    id: 37,
    name: 'UV Transilluminator',
    category: 'general',
    subcategory: 'transilluminator',
    description: 'Equipment supporting popular dyes including ethidium bromide, gel red, and gold view for gel visualization.',
    image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/LabGig-UV-Trans-illuminator.jpg?resize=600%2C600&ssl=1',
    features: ['EtBr Compatible', 'Gel Red Support', 'Classic Design'],
  },
  // General Lab Equipment - Electrophoresis
  {
    id: 38,
    name: 'gelBLooK Electrophoresis System',
    category: 'general',
    subcategory: 'electrophoresis',
    description: 'Combines gel electrophoresis system with blue LED transilluminator for nucleic acid electrophoresis experiments.',
    image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2024/11/LabGig-gelBLook-Electrophoresis-System.png?resize=600%2C600&ssl=1',
    features: ['Integrated System', 'Blue LED', 'All-in-One'],
  },
  {
    id: 39,
    name: 'Electrophoresis Power Supply',
    category: 'general',
    subcategory: 'electrophoresis',
    description: 'Universal electrophoresis instruments suitable for horizontal, vertical, and transfer chambers with high-definition color LCD screen.',
    image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/LabGig-Electrophoresis-Power-Supply.png?resize=600%2C600&ssl=1',
    features: ['Universal Fit', 'Color LCD', 'Multi-Chamber'],
  },
  {
    id: 40,
    name: 'DOSYWEL Peristaltic Pump',
    category: 'general',
    subcategory: 'electrophoresis',
    description: 'Multi-functional, compact peristaltic pump providing quick and accurate liquid filling. Fills 9ml in 1 second with ±0.6ml accuracy.',
    image: 'https://i0.wp.com/labgig.in/wp-content/uploads/2023/09/LabGig-Dosywel.jpg?resize=600%2C600&ssl=1',
    features: ['9ml/second', '±0.6ml Accuracy', 'Compact'],
  },
];

export default function ProductsPage() {
  const [activeCategory, setActiveCategory] = useState('all');
  const [activeSubcategory, setActiveSubcategory] = useState<string | null>(null);
  const [showQuoteModal, setShowQuoteModal] = useState(false);
  const [selectedProduct, setSelectedProduct] = useState<string | null>(null);

  // Get active category object
  const activeCategoryObj = categories.find(c => c.id === activeCategory);

  // Filter products based on category and subcategory
  const filteredProducts = activeCategory === 'all'
    ? products
    : activeSubcategory
      ? products.filter(product => product.category === activeCategory && product.subcategory === activeSubcategory)
      : products.filter(product => product.category === activeCategory);

  const handleCategoryChange = (categoryId: string) => {
    setActiveCategory(categoryId);
    setActiveSubcategory(null); // Reset subcategory when main category changes
  };

  const handleRequestQuote = (productName: string) => {
    setSelectedProduct(productName);
    setShowQuoteModal(true);
  };

  return (
    <div className="overflow-hidden">
      {/* Hero Section */}
      <HeroSection />

      {/* Products Grid Section */}
      <ProductsGridSection
        categories={categories}
        products={filteredProducts}
        activeCategory={activeCategory}
        activeSubcategory={activeSubcategory}
        activeCategoryObj={activeCategoryObj}
        setActiveCategory={handleCategoryChange}
        setActiveSubcategory={setActiveSubcategory}
        onRequestQuote={handleRequestQuote}
      />

      {/* Quote Modal */}
      <AnimatePresence>
        {showQuoteModal && (
          <QuoteModal
            productName={selectedProduct}
            onClose={() => setShowQuoteModal(false)}
          />
        )}
      </AnimatePresence>
    </div>
  );
}

// Hero Section
function HeroSection() {
  return (
    <section className="relative pt-32 pb-16 spotlight">
      <div className="max-w-7xl mx-auto px-6 lg:px-8">
        <div className="grid lg:grid-cols-2 gap-12 items-center">
          <motion.div
            initial={{ opacity: 0, x: -50 }}
            animate={{ opacity: 1, x: 0 }}
            transition={{ duration: 0.8 }}
          >
            <div className="w-16 h-1 bg-gradient-to-r from-[#f97066] to-[#fb923c] mb-8 rounded-full" />
            <h1 className="text-4xl sm:text-5xl lg:text-6xl font-bold text-white leading-tight mb-6">
              PREMIUM{' '}
              <span className="gradient-text">LABORATORY EQUIPMENT</span>{' '}
              CATALOG
            </h1>
            <p className="text-gray-400 text-lg leading-relaxed mb-8">
              Explore our comprehensive range of high-quality laboratory instruments
              designed for precision, reliability, and performance.
            </p>
            <Link href="/contact">
              <motion.button
                whileHover={{ scale: 1.02 }}
                whileTap={{ scale: 0.98 }}
                className="btn-primary"
              >
                Request Custom Quote
              </motion.button>
            </Link>
          </motion.div>

          <motion.div
            initial={{ opacity: 0, x: 50 }}
            animate={{ opacity: 1, x: 0 }}
            transition={{ duration: 0.8, delay: 0.2 }}
            className="relative"
          >
            <div className="relative h-[400px] rounded-3xl overflow-hidden glass-card">
              <Image
                src="https://images.unsplash.com/photo-1582719471384-894fbb16e074?w=800"
                alt="Laboratory Equipment"
                fill
                className="object-cover"
              />
            </div>
            <div className="absolute -top-10 -right-10 w-40 h-40 bg-gradient-to-br from-[#f97066]/20 to-transparent rounded-full blur-3xl" />
          </motion.div>
        </div>
      </div>
    </section>
  );
}

// Products Grid Section
function ProductsGridSection({
  categories,
  products,
  activeCategory,
  activeSubcategory,
  activeCategoryObj,
  setActiveCategory,
  setActiveSubcategory,
  onRequestQuote,
}: {
  categories: Category[];
  products: Product[];
  activeCategory: string;
  activeSubcategory: string | null;
  activeCategoryObj: Category | undefined;
  setActiveCategory: (category: string) => void;
  setActiveSubcategory: (subcategory: string | null) => void;
  onRequestQuote: (productName: string) => void;
}) {
  const ref = useRef(null);
  const isInView = useInView(ref, { once: true, margin: '-100px' });

  return (
    <section ref={ref} className="py-16 relative">
      <div className="max-w-7xl mx-auto px-6 lg:px-8">
        {/* Main Categories Filter */}
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={isInView ? { opacity: 1, y: 0 } : {}}
          transition={{ duration: 0.6 }}
          className="flex flex-wrap justify-center gap-3 mb-6"
        >
          {categories.map((category) => (
            <motion.button
              key={category.id}
              whileHover={{ scale: 1.05 }}
              whileTap={{ scale: 0.95 }}
              onClick={() => setActiveCategory(category.id)}
              className={`px-3 sm:px-5 py-2 sm:py-2.5 rounded-xl text-xs sm:text-sm font-medium transition-all duration-300 ${
                activeCategory === category.id
                  ? 'bg-gradient-to-r from-[#f97066] to-[#fb923c] text-white'
                  : 'glass text-gray-300 hover:text-white hover:border-[#f97066]/50'
              }`}
            >
              {category.name}
            </motion.button>
          ))}
        </motion.div>

        {/* Subcategories Filter */}
        <AnimatePresence mode="wait">
          {activeCategoryObj?.subcategories && activeCategoryObj.subcategories.length > 0 && (
            <motion.div
              initial={{ opacity: 0, height: 0 }}
              animate={{ opacity: 1, height: 'auto' }}
              exit={{ opacity: 0, height: 0 }}
              transition={{ duration: 0.3 }}
              className="flex flex-wrap justify-center gap-2 mb-12"
            >
              <motion.button
                whileHover={{ scale: 1.05 }}
                whileTap={{ scale: 0.95 }}
                onClick={() => setActiveSubcategory(null)}
                className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-all duration-300 ${
                  activeSubcategory === null
                    ? 'bg-white/20 text-white border border-white/30'
                    : 'bg-white/5 text-gray-400 hover:text-white hover:bg-white/10'
                }`}
              >
                All {activeCategoryObj.name}
              </motion.button>
              {activeCategoryObj.subcategories.map((sub) => (
                <motion.button
                  key={sub.id}
                  whileHover={{ scale: 1.05 }}
                  whileTap={{ scale: 0.95 }}
                  onClick={() => setActiveSubcategory(sub.id)}
                  className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-all duration-300 ${
                    activeSubcategory === sub.id
                      ? 'bg-white/20 text-white border border-white/30'
                      : 'bg-white/5 text-gray-400 hover:text-white hover:bg-white/10'
                  }`}
                >
                  {sub.name}
                </motion.button>
              ))}
            </motion.div>
          )}
        </AnimatePresence>

        {/* Products Grid */}
        <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-8">
          <AnimatePresence mode="wait">
            {products.map((product, index) => (
              <motion.div
                key={product.id}
                initial={{ opacity: 0, y: 30 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -30 }}
                transition={{ duration: 0.4, delay: index * 0.1 }}
                className="glass-card rounded-2xl overflow-hidden group"
              >
                {/* Image */}
                <div className="relative h-56 overflow-hidden">
                  <Image
                    src={product.image}
                    alt={product.name}
                    fill
                    className="object-cover transition-transform duration-500 group-hover:scale-110"
                  />
                  <div className="absolute inset-0 bg-gradient-to-t from-[#0a0a0f] via-transparent to-transparent opacity-60" />
                </div>

                {/* Content */}
                <div className="p-6">
                  <h3 className="text-xl font-bold text-white mb-2 group-hover:text-[#f97066] transition-colors">
                    {product.name}
                  </h3>
                  <p className="text-gray-400 text-sm leading-relaxed mb-4">
                    {product.description}
                  </p>

                  {/* Features */}
                  <div className="flex flex-wrap gap-2 mb-4">
                    {product.features.map((feature) => (
                      <span
                        key={feature}
                        className="px-2 py-1 rounded-md bg-white/5 text-gray-300 text-xs"
                      >
                        {feature}
                      </span>
                    ))}
                  </div>

                  {/* CTA */}
                  <motion.button
                    whileHover={{ scale: 1.02 }}
                    whileTap={{ scale: 0.98 }}
                    onClick={() => onRequestQuote(product.name)}
                    className="w-full py-3 rounded-xl bg-gradient-to-r from-[#f97066] to-[#fb923c] text-white font-medium text-sm transition-all hover:shadow-lg hover:shadow-[#f97066]/30"
                  >
                    Request Quote
                  </motion.button>
                </div>
              </motion.div>
            ))}
          </AnimatePresence>
        </div>

        {/* Custom Requirements CTA */}
        <motion.div
          initial={{ opacity: 0, y: 30 }}
          animate={isInView ? { opacity: 1, y: 0 } : {}}
          transition={{ duration: 0.6, delay: 0.5 }}
          className="mt-16 text-center"
        >
          <div className="glass-card rounded-2xl p-8 lg:p-12 max-w-3xl mx-auto">
            <h3 className="text-2xl font-bold text-white mb-4">
              Need Custom Laboratory Equipment?
            </h3>
            <p className="text-gray-400 mb-6">
              Can&apos;t find what you&apos;re looking for? We specialize in custom instrumentation
              tailored to your specific research and development needs.
            </p>
            <Link href="/contact">
              <motion.button
                whileHover={{ scale: 1.02 }}
                whileTap={{ scale: 0.98 }}
                className="btn-primary"
              >
                Contact Us for Custom Solutions
              </motion.button>
            </Link>
          </div>
        </motion.div>
      </div>
    </section>
  );
}

// Quote Modal
function QuoteModal({
  productName,
  onClose,
}: {
  productName: string | null;
  onClose: () => void;
}) {
  const [submitted, setSubmitted] = useState(false);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    setSubmitted(true);
    setTimeout(() => {
      onClose();
      setSubmitted(false);
    }, 2000);
  };

  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm p-4"
      onClick={onClose}
    >
      <motion.div
        initial={{ scale: 0.9, opacity: 0 }}
        animate={{ scale: 1, opacity: 1 }}
        exit={{ scale: 0.9, opacity: 0 }}
        className="glass-card rounded-2xl p-8 max-w-md w-full"
        onClick={(e) => e.stopPropagation()}
      >
        {submitted ? (
          <div className="text-center py-8">
            <div className="w-16 h-16 rounded-full bg-gradient-to-br from-[#f97066] to-[#fb923c] flex items-center justify-center mx-auto mb-4">
              <svg className="w-8 h-8 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
              </svg>
            </div>
            <h3 className="text-2xl font-bold text-white mb-2">Request Sent!</h3>
            <p className="text-gray-400">We&apos;ll get back to you with a quote shortly.</p>
          </div>
        ) : (
          <>
            <div className="flex items-center justify-between mb-6">
              <h3 className="text-xl font-bold text-white">Request Quote</h3>
              <button
                onClick={onClose}
                className="text-gray-400 hover:text-white transition-colors"
              >
                <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            </div>

            {productName && (
              <div className="glass rounded-xl p-3 mb-6">
                <p className="text-sm text-gray-400">Product:</p>
                <p className="text-white font-medium">{productName}</p>
              </div>
            )}

            <form onSubmit={handleSubmit} className="space-y-4">
              <input
                type="text"
                placeholder="Your Name"
                required
                className="input-modern"
              />
              <input
                type="email"
                placeholder="Email Address"
                required
                className="input-modern"
              />
              <input
                type="tel"
                placeholder="Phone Number"
                className="input-modern"
              />
              <input
                type="number"
                placeholder="Quantity Required"
                min="1"
                className="input-modern"
              />
              <textarea
                placeholder="Additional Requirements (optional)"
                rows={3}
                className="input-modern resize-none"
              />
              <motion.button
                type="submit"
                whileHover={{ scale: 1.02 }}
                whileTap={{ scale: 0.98 }}
                className="btn-primary w-full"
              >
                Submit Request
              </motion.button>
            </form>
          </>
        )}
      </motion.div>
    </motion.div>
  );
}
