// Map page names used in code to actual URL paths
const PAGE_ROUTES: Record<string, string> = {
    "Home": "/",
    "About": "/about",
    "Projects": "/projects",
    "Anahata": "/anahata",
    "Anahata2BHK": "/anahata/2bhk",
    "Anahata3BHK": "/anahata/3bhk",
    "AnahataBookSiteVisit": "/anahata/Booksitevisit",
    "AnahataThankYou": "/anahata/thankyou",
    "Krishna": "/krishna",
    "Vashishta": "/vashishta",
    "Naadam": "/naadam",
    "Vyasa": "/vyasa",
    "Agastya": "/agastya",
    "Gallery": "/gallery",
    "Blog": "/blog",
    "BlogPost": "/BlogPost",
    "Contact": "/contact",
};

export function createPageUrl(pageName: string) {
    return PAGE_ROUTES[pageName] || '/' + pageName.replace(/ /g, '-');
}
