import React from 'react';
import Hero from '../components/home/Hero';
import Services from '../components/home/Services';
import TechStack from '../components/home/TechStack';
import Industries from '../components/home/Industries';
import FAQ from '../components/home/FAQ';
import CTA from '../components/home/CTA';
import Footer from '../components/home/Footer';

export default function Home() {
    return (
        <div>
            <Hero />
            <Services />
            <TechStack />
            <Industries />
            <FAQ />
            <CTA />
            <Footer />
        </div>
    );
}