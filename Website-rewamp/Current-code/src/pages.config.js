import About from './pages/About';
import Agastya from './pages/Agastya';
import Anahata from './pages/Anahata';
import Anahata2BHK from './pages/Anahata2BHK';
import Anahata3BHK from './pages/Anahata3BHK';
import AnahataBookSiteVisit from './pages/AnahataBookSiteVisit';
import AnahataThankYou from './pages/AnahataThankYou';
import Blog from './pages/Blog';
import BlogPost from './pages/BlogPost';
import Contact from './pages/Contact';
import Gallery from './pages/Gallery';
import Home from './pages/Home';
import Krishna from './pages/Krishna';
import Naadam from './pages/Naadam';
import Projects from './pages/Projects';
import Vashishta from './pages/Vashishta';
import Vyasa from './pages/Vyasa';
import PrivacyPolicy from './pages/PrivacyPolicy';


export const PAGES = {
    "about": About,
    "agastya": Agastya,
    "anahata": Anahata,
    "anahata/2bhk": Anahata2BHK,
    "anahata/3bhk": Anahata3BHK,
    "anahata/Booksitevisit": AnahataBookSiteVisit,
    "anahata/thankyou": AnahataThankYou,
    "blog": Blog,
    "BlogPost": BlogPost,
    "contact": Contact,
    "gallery": Gallery,
    "Home": Home,
    "krishna": Krishna,
    "naadam": Naadam,
    "projects": Projects,
    "vashishta": Vashishta,
    "vyasa": Vyasa,
    "privacy-policy": PrivacyPolicy,
}

export const pagesConfig = {
    mainPage: "Home",
    Pages: PAGES,
};
