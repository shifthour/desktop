// Inject Ishtika logo after page loads - persistent version
(function() {
  // Supabase Configuration
  var SUPABASE_URL = 'https://eflvzsfgoelonfclzrjy.supabase.co';
  var SUPABASE_ANON_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVmbHZ6c2Znb2Vsb25mY2x6cmp5Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDc2NTYxOTAsImV4cCI6MjA2MzIzMjE5MH0.LCxuFDJuBO8ggiqdWpv5uoOkplKWbcF8z5E_kdpOEWU';

  // Function to send data to Supabase
  async function sendToSupabase(formData) {
    try {
      var response = await fetch(SUPABASE_URL + '/rest/v1/flatrix_leads', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'apikey': SUPABASE_ANON_KEY,
          'Authorization': 'Bearer ' + SUPABASE_ANON_KEY,
          'Prefer': 'return=minimal'
        },
        body: JSON.stringify(formData)
      });

      if (response.ok) {
        console.log('Lead saved to Supabase successfully');
        return true;
      } else {
        console.error('Failed to save lead:', response.status);
        return false;
      }
    } catch (error) {
      console.error('Error saving to Supabase:', error);
      return false;
    }
  }

  // Intercept form submissions
  function interceptForms() {
    document.addEventListener('submit', function(e) {
      var form = e.target;
      if (form.tagName === 'FORM') {
        var formData = new FormData(form);
        var data = {
          name: formData.get('name') || formData.get('fullName') || '',
          phone: formData.get('phone') || formData.get('mobile') || formData.get('phoneNumber') || '',
          email: formData.get('email') || '',
          source: 'anahata-website',
          project: 'Anahata',
          created_at: new Date().toISOString()
        };

        // Only send if we have at least name or phone
        if (data.name || data.phone) {
          sendToSupabase(data);
        }
      }
    }, true);

    // Also intercept button clicks that might trigger form submission
    document.addEventListener('click', function(e) {
      var button = e.target.closest('button');
      if (button) {
        var buttonText = button.textContent.toLowerCase();
        if (buttonText.includes('submit') || buttonText.includes('book') || buttonText.includes('enquire') || buttonText.includes('send')) {
          // Find nearby form or input fields
          var form = button.closest('form');
          if (form) {
            var formData = new FormData(form);
            var data = {
              name: formData.get('name') || formData.get('fullName') || '',
              phone: formData.get('phone') || formData.get('mobile') || formData.get('phoneNumber') || '',
              email: formData.get('email') || '',
              source: 'anahata-website',
              project: 'Anahata',
              created_at: new Date().toISOString()
            };

            if (data.name || data.phone) {
              sendToSupabase(data);
            }
          } else {
            // Try to find inputs near the button
            var container = button.closest('div');
            if (container) {
              var inputs = container.querySelectorAll('input');
              var data = {
                source: 'anahata-website',
                project: 'Anahata',
                created_at: new Date().toISOString()
              };
              inputs.forEach(function(input) {
                var name = input.name || input.placeholder || '';
                var value = input.value;
                if (name.toLowerCase().includes('name') || name.toLowerCase().includes('full')) {
                  data.name = value;
                } else if (name.toLowerCase().includes('phone') || name.toLowerCase().includes('mobile')) {
                  data.phone = value;
                } else if (name.toLowerCase().includes('email')) {
                  data.email = value;
                }
              });

              if (data.name || data.phone) {
                sendToSupabase(data);
              }
            }
          }
        }
      }
    }, true);
  }

  function injectLogo() {
    var nav = document.querySelector('nav .flex.items-center.justify-between');
    if (nav && !document.getElementById('ishtika-vibranium-logo')) {
      var logoImg = document.createElement('img');
      logoImg.id = 'ishtika-vibranium-logo';
      logoImg.src = '/anahata-project/ishtika-logo.png';
      logoImg.alt = 'Ishtika Vibranium';
      logoImg.style.cssText = 'height: 40px; width: auto; margin-right: 16px; object-fit: contain; flex-shrink: 0;';
      nav.insertBefore(logoImg, nav.firstChild);
    }
  }

  // Change phone number
  function changePhoneNumber() {
    var phoneLinks = document.querySelectorAll('a[href*="tel:"]');
    phoneLinks.forEach(function(link) {
      link.href = 'tel:+919485645645';
      var span = link.querySelector('span');
      if (span && span.textContent.includes('7338628777')) {
        span.textContent = '9485 645 645';
      }
    });
  }

  // Hide "Discover More Premium Projects" section
  function hideDiscoverSection() {
    var headings = document.querySelectorAll('h2');
    headings.forEach(function(h2) {
      if (h2.textContent.includes('Discover More Premium Projects')) {
        var section = h2.closest('section');
        if (section) {
          section.style.display = 'none';
        }
      }
    });
  }

  // Run immediately
  injectLogo();
  hideDiscoverSection();
  changePhoneNumber();
  interceptForms();

  // Run multiple times to ensure it persists after React hydration
  setTimeout(injectLogo, 50);
  setTimeout(injectLogo, 100);
  setTimeout(injectLogo, 300);
  setTimeout(injectLogo, 500);
  setTimeout(injectLogo, 1000);
  setTimeout(injectLogo, 2000);

  setTimeout(hideDiscoverSection, 50);
  setTimeout(hideDiscoverSection, 100);
  setTimeout(hideDiscoverSection, 500);
  setTimeout(hideDiscoverSection, 1000);
  setTimeout(hideDiscoverSection, 2000);

  setTimeout(changePhoneNumber, 50);
  setTimeout(changePhoneNumber, 100);
  setTimeout(changePhoneNumber, 500);
  setTimeout(changePhoneNumber, 1000);
  setTimeout(changePhoneNumber, 2000);

  // Also run on DOMContentLoaded
  document.addEventListener('DOMContentLoaded', function() {
    injectLogo();
    hideDiscoverSection();
    changePhoneNumber();
    setTimeout(injectLogo, 100);
    setTimeout(injectLogo, 500);
    setTimeout(hideDiscoverSection, 100);
    setTimeout(hideDiscoverSection, 500);
    setTimeout(changePhoneNumber, 100);
    setTimeout(changePhoneNumber, 500);
  });

  // Also run on window load
  window.addEventListener('load', function() {
    injectLogo();
    hideDiscoverSection();
    changePhoneNumber();
    setTimeout(injectLogo, 100);
    setTimeout(injectLogo, 500);
    setTimeout(injectLogo, 1000);
    setTimeout(hideDiscoverSection, 100);
    setTimeout(hideDiscoverSection, 500);
    setTimeout(hideDiscoverSection, 1000);
    setTimeout(changePhoneNumber, 100);
    setTimeout(changePhoneNumber, 500);
    setTimeout(changePhoneNumber, 1000);
  });

  // Use MutationObserver to re-inject if React removes it
  var observer = new MutationObserver(function(mutations) {
    if (!document.getElementById('ishtika-vibranium-logo')) {
      injectLogo();
    }
    hideDiscoverSection();
  });

  // Start observing once DOM is ready
  function startObserver() {
    var nav = document.querySelector('nav');
    if (nav) {
      observer.observe(nav, { childList: true, subtree: true });
    }
    // Also observe main for section changes
    var main = document.querySelector('main');
    if (main) {
      observer.observe(main, { childList: true, subtree: true });
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', startObserver);
  } else {
    startObserver();
  }
})();
