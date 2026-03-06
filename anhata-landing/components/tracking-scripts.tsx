"use client"

import Script from 'next/script'

export default function TrackingScripts() {
  return (
    <>
      {/* Google Tag Manager */}
      <Script
        id="gtm-script"
        strategy="afterInteractive"
        dangerouslySetInnerHTML={{
          __html: `
            (function(w,d,s,l,i){w[l]=w[l]||[];w[l].push({'gtm.start':new Date().getTime(),event:'gtm.js'});var f=d.getElementsByTagName(s)[0],j=d.createElement(s),dl=l!='dataLayer'?'&l='+l:'';j.async=true;j.src='https://www.googletagmanager.com/gtm.js?id='+i+dl;f.parentNode.insertBefore(j,f);})(window,document,'script','dataLayer','GTM-NN9XF85R');
          `,
        }}
      />

      {/* Google Analytics 4 & Google Ads */}
      <Script
        src="https://www.googletagmanager.com/gtag/js?id=G-W6C28DJ6M3"
        strategy="afterInteractive"
      />
      <Script
        id="gtag-init"
        strategy="afterInteractive"
        dangerouslySetInnerHTML={{
          __html: `
            window.dataLayer = window.dataLayer || [];
            function gtag(){dataLayer.push(arguments);}
            gtag('js', new Date());

            // Google Analytics 4
            gtag('config', 'G-W6C28DJ6M3');

            // Google Ads
            gtag('config', 'AW-17340305414');
          `,
        }}
      />

      {/* Microsoft Clarity */}
      <Script
        id="clarity-script"
        strategy="afterInteractive"
        dangerouslySetInnerHTML={{
          __html: `
            (function(c,l,a,r,i,t,y){
              c[a]=c[a]||function(){(c[a].q=c[a].q||[]).push(arguments)};
              t=l.createElement(r);t.async=1;t.src="https://www.clarity.ms/tag/"+i;
              y=l.getElementsByTagName(r)[0];y.parentNode.insertBefore(t,y);
            })(window, document, "clarity", "script", "sfqluu523a");
          `,
        }}
      />

      {/* Statcounter */}
      <Script
        id="statcounter-config"
        strategy="afterInteractive"
        dangerouslySetInnerHTML={{
          __html: `
            var sc_project=13174805;
            var sc_invisible=1;
            var sc_security="13771086";
          `,
        }}
      />
      <Script
        src="https://www.statcounter.com/counter/counter.js"
        strategy="afterInteractive"
      />
    </>
  )
}
