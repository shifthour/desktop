<?php include('header.php'); ?>

<style>
/*    .features1-section-area .features-slider-area .featured-boxarea .content-area a {*/
/*        font-size:18px !important;*/
/*        color:white;*/
/*        line-height:normal !important;*/
/*    }*/
 
/*.features1-section-area .features-slider-area .featured-boxarea .content-area a:hover {*/
/*    color: white;*/
/*}*/

.main_tag{
    margin-top:-20px;
    font-size:30px !important;
    line-height: 25px !important;
}
@media only screen and (max-width: 480px) {
    .heading1 h1 {
        line-height: 25px !important;
    }
    .main_tag{
    margin-top:-10px;
    font-size:16px !important;
    line-height: 25px !important;
}
}

/*sound btn*/
/* From Uiverse.io by vinodjangid07 */ 
/* The switch - the box around the speaker*/
.toggleSwitch {
  width: 50px;
  height: 50px;
  position: relative;
  display: flex;
  align-items: center;
  justify-content: center;
  background-color: rgb(39, 39, 39);
  border-radius: 50%;
  cursor: pointer;
  transition-duration: .3s;
  box-shadow: 2px 2px 10px rgba(0, 0, 0, 0.13);
  overflow: hidden;
      top: -70px;
    left: 10px;
}

/* Hide default HTML checkbox */
#checkboxInput {
  display: none;
}

.bell {
  width: 18px;
}

.bell path {
  fill: white;
}

.speaker {
  width: 100%;
  height: 100%;
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 2;
  transition-duration: .3s;
}

.speaker svg {
  width: 18px;
}

.mute-speaker {
  position: absolute;
  width: 100%;
  height: 100%;
  display: flex;
  align-items: center;
  justify-content: center;
  opacity: 0;
  z-index: 3;
  transition-duration: .3s;
}

.mute-speaker svg {
  width: 18px;
}

#checkboxInput:checked +.toggleSwitch .speaker {
  opacity: 0;
  transition-duration: .3s;
}

#checkboxInput:checked +.toggleSwitch .mute-speaker {
  opacity: 1;
  transition-duration: .3s;
}

#checkboxInput:active + .toggleSwitch {
  transform: scale(0.7);
}

#checkboxInput:hover + .toggleSwitch {
  background-color: rgb(61, 61, 61);
}
</style>


<!--===== HERO AREA STARTS =======-->
<div class="hero1-section-area">
  <div class="container-fluid">
    <div class="row">
      
      
      
      <div class="col-lg-9  pl-1">
        <video 
          id="myVideo"
          style="width: 100%; border-radius: 20px;" 
          autoplay 
          muted 
          loop 
          playsinline>
          <source src="assets/video.mp4" type="video/mp4">
          Your browser does not support the video tag.
        </video>
        <input type="checkbox" id="checkboxInput">
        <label for="checkboxInput" class="toggleSwitch" onclick="audioToggle()">
    
            <div class="mute-speaker"><svg xmlns="http://www.w3.org/2000/svg" version="1.0" viewBox="0 0 75 75">
            <path d="M39.389,13.769 L22.235,28.606 L6,28.606 L6,47.699 L21.989,47.699 L39.389,62.75 L39.389,13.769z" style="stroke:#fff;stroke-width:5;stroke-linejoin:round;fill:#fff;"></path>
            <path d="M48,27.6a19.5,19.5 0 0 1 0,21.4M55.1,20.5a30,30 0 0 1 0,35.6M61.6,14a38.8,38.8 0 0 1 0,48.6" style="fill:none;stroke:#fff;stroke-width:5;stroke-linecap:round"></path>
            </svg></div>
            
            <div class="speaker"><svg version="1.0" viewBox="0 0 75 75" stroke="#fff" stroke-width="5">
            <path d="m39,14-17,15H6V48H22l17,15z" fill="#fff" stroke-linejoin="round"></path>
            <path d="m49,26 20,24m0-24-20,24" fill="#fff" stroke-linecap="round"></path>
            </svg></div>
    
        </label>
  
      </div>
      <div class="col-lg-3">
        <div class="all-counter-area row flex-row h-100">
            <div class="space50 d-lg-block d-none"></div>
            <div class="space10 d-lg-none d-block"></div>
          <div class="counter-area" data-aos="fade-left" data-aos-duration="800">
            <h2><span class="counter">12</span>+</h2>
            <p> Years Of Experience</p>
          </div>
          <div class="space50 d-lg-block d-none"></div>
            <div class="space10 d-lg-none d-block"></div>
          <div class="counter-area" data-aos="fade-left" data-aos-duration="1000">
            <h2><span class="counter">12</span>+</h2>
            <p>Established Projects</p>
          </div>
          <div class="space50 d-lg-block d-none"></div>
            <div class="space10 d-lg-none d-block"></div>
          <div class="counter-area" data-aos="fade-left" data-aos-duration="1100">
            <h2><span class="counter">5</span>+</h2>
            <p>Ongoing Projects</p>
          </div>
          <div class="space50 d-lg-block d-none"></div>
            <div class="space10 d-lg-none d-block"></div>
          <div class="counter-area" data-aos="fade-left" data-aos-duration="1200">
            <!--<img src="assets/img/all-images/others-img1.png" alt="">-->
            <!--<div class="space16"></div>-->
            <h2><span class="counter">1</span>K+</h2>
            <p>Happy Residents</p>
          </div>
        </div>

      </div>
      <div class="space30 d-lg-none d-block"></div>
      <div class="col-lg-12 mb-3">
        <div class="header-main-content heading1">
          <img src="assets/img/elements/star1.png" alt="" class="star1 keyframe5">
          <img src="assets/img/elements/star1.png" alt="" class="star2 keyframe5">
          <h1 class="text-anim">Crafting Homes, Inspiring Lifestyles</h1>
          <h1 class="text-anim main_tag" >Experience the Perfect Blend of Elegance and Tranquility with Ishtika Homes!</h1>
        </div>
      </div>
    </div>
  </div>
</div>

<script>
  const video = document.getElementById('myVideo');
  const checkbox = document.getElementById('checkboxInput');

  // Handle audio toggle
  checkbox.addEventListener('change', () => {
    video.muted = !checkbox.checked;
    video.play(); // Ensure video plays after toggle
  });

  // Pause video when it's out of view
  const observer = new IntersectionObserver(
    (entries) => {
      entries.forEach((entry) => {
        if (entry.isIntersecting) {
          video.play().catch(() => {}); // Resume if in view
        } else {
          video.pause(); // Pause if out of view
        }
      });
    },
    {
      threshold: 0.5, // Adjust as needed (0.5 = 50% of video must be visible)
    }
  );

  observer.observe(video);
</script>



<!--===== HERO AREA ENDS =======-->
<!--<div class="tabs-section-area">-->
<!--  <div class="container">-->
<!--    <div class="row">-->
<!--      <div class="col-lg-10 m-auto">-->
<!--        <div class="tabs-area">-->
<!--          <ul class="nav nav-pills mb-3" id="pills-tab" role="tablist">-->
<!--            <li class="nav-item" role="presentation">-->
<!--              <button class="nav-link active" id="pills-home-tab" data-bs-toggle="pill" data-bs-target="#pills-home" type="button" role="tab" aria-controls="pills-home" aria-selected="true">Rent</button>-->
<!--            </li>-->
<!--            <li class="nav-item" role="presentation">-->
<!--              <button class="nav-link" id="pills-profile-tab" data-bs-toggle="pill" data-bs-target="#pills-profile" type="button" role="tab" aria-controls="pills-profile" aria-selected="false">Buy</button>-->
<!--            </li>-->
<!--            <li class="nav-item" role="presentation">-->
<!--              <button class="nav-link" id="pills-contact-tab" data-bs-toggle="pill" data-bs-target="#pills-contact" type="button" role="tab" aria-controls="pills-contact" aria-selected="false">Sell</button>-->
<!--            </li>-->
<!--          </ul>-->
<!--          <div class="tab-content" id="pills-tabContent">-->
<!--            <div class="tab-pane fade show active" id="pills-home" role="tabpanel" aria-labelledby="pills-home-tab" tabindex="0">-->
              
<!--                <form id="search1" class="all-cities-area" action="search.php" method="post">-->
<!--                <div class="cities">-->
<!--                  <p>City/State</p>-->
<!--                  <div class="input-place after">-->
<!--                    <select name="state"  class="nice-select">-->
<!--                         <option value="Bengaluru" data-display="Bengaluru">Bengaluru</option>-->
<!--                       </select>-->
<!--                  </div>-->
<!--                </div>-->
<!--                <div class="cities">-->
<!--                  <p>Property </p>-->
<!--                  <div class="input-place after">-->
<!--                    <select name="country"  class="nice-select">-->
<!--                         <option value="krishna" data-display="Krishna">Krishna</option>-->
<!--                         <option value="vashishta">Vashishta</option>-->
<!--                         <option value="naadam">Naadam</option>-->
<!--                         <option value="vyasa">Vyasa</option>-->
<!--                         <option value="agastya">Agastya</option>-->
<!--                    </select>-->
<!--                  </div>-->
<!--                </div>-->

<!--                <div class="cities">-->
<!--                  <p>Price Range </p>-->
<!--                    <div class="input-place m-0">-->
<!--                      <select name="price"  class="nice-select">-->
<!--                           <option value="1" data-display="$4,50,000">$4,50,000</option>-->
<!--                           <option value="">$5,50,000</option>-->
<!--                           <option value="">$6,50,000</option>-->
<!--                           <option value="">$7,50,000</option>-->
<!--                           <option value="">$8,50,000</option>-->
<!--                           <option value="">$9,50,000</option>-->
<!--                         </select>-->
<!--                    </div>-->
<!--                </div>-->

<!--                <div class="cities">-->
<!--                  <div class="input-place">-->
<!--                    <a onclick="document.getElementById('search1').submit();" type="submit" class="header-btn1">Search <img src="assets/img/icons/search1.svg" alt=""></a>-->
<!--                  </div>-->
<!--                </div>-->
<!--                </form>-->
<!--            </div>-->
<!--            <div class="tab-pane fade" id="pills-profile" role="tabpanel" aria-labelledby="pills-profile-tab" tabindex="0">-->
<!--              <form id="search1" class="all-cities-area" action="search.php" method="post">-->
<!--                <div class="cities">-->
<!--                  <p>City/State</p>-->
<!--                  <div class="input-place after">-->
<!--                    <select name="state"  class="nice-select">-->
<!--                         <option value="Bengaluru" data-display="Bengaluru">Bengaluru</option>-->
<!--                       </select>-->
<!--                  </div>-->
<!--                </div>-->
<!--                <div class="cities">-->
<!--                  <p>Property </p>-->
<!--                  <div class="input-place after">-->
<!--                    <select name="country"  class="nice-select">-->
<!--                         <option value="Krishna" data-display="Krishna">Krishna</option>-->
<!--                         <option value="Vashishta">Vashishta</option>-->
<!--                         <option value="Naadam">Naadam</option>-->
<!--                         <option value="Vyasa">Vyasa</option>-->
<!--                         <option value="Agastya">Agastya</option>-->
<!--                    </select>-->
<!--                  </div>-->
<!--                </div>-->

<!--                <div class="cities">-->
<!--                  <p>Price Range </p>-->
<!--                    <div class="input-place m-0">-->
<!--                      <select name="price"  class="nice-select">-->
<!--                           <option value="1" data-display="$4,50,000">$4,50,000</option>-->
<!--                           <option value="">$5,50,000</option>-->
<!--                           <option value="">$6,50,000</option>-->
<!--                           <option value="">$7,50,000</option>-->
<!--                           <option value="">$8,50,000</option>-->
<!--                           <option value="">$9,50,000</option>-->
<!--                         </select>-->
<!--                    </div>-->
<!--                </div>-->

<!--                <div class="cities">-->
<!--                  <div class="input-place">-->
<!--                    <a onclick="document.getElementById('search1').submit();" type="submit" class="header-btn1">Search <img src="assets/img/icons/search1.svg" alt=""></a>-->
<!--                  </div>-->
<!--                </div>-->
<!--              </form>-->
<!--            </div>-->
<!--            <div class="tab-pane fade" id="pills-contact" role="tabpanel" aria-labelledby="pills-contact-tab" tabindex="0">-->
<!--              <form id="search1" class="all-cities-area" action="search.php" method="post">-->
<!--                <div class="cities">-->
<!--                  <p>City/State</p>-->
<!--                  <div class="input-place after">-->
<!--                    <select name="state"  class="nice-select">-->
<!--                         <option value="Bengaluru" data-display="Bengaluru">Bengaluru</option>-->
<!--                       </select>-->
<!--                  </div>-->
<!--                </div>-->
<!--                <div class="cities">-->
<!--                  <p>Property </p>-->
<!--                  <div class="input-place after">-->
<!--                    <select name="country"  class="nice-select">-->
<!--                         <option value="Krishna" data-display="Krishna">Krishna</option>-->
<!--                         <option value="Vashishta">Vashishta</option>-->
<!--                         <option value="Naadam">Naadam</option>-->
<!--                         <option value="Vyasa">Vyasa</option>-->
<!--                         <option value="Agastya">Agastya</option>-->
<!--                    </select>-->
<!--                  </div>-->
<!--                </div>-->

<!--                <div class="cities">-->
<!--                  <p>Price Range </p>-->
<!--                    <div class="input-place m-0">-->
<!--                      <select name="price"  class="nice-select">-->
<!--                           <option value="1" data-display="$4,50,000">$4,50,000</option>-->
<!--                           <option value="">$5,50,000</option>-->
<!--                           <option value="">$6,50,000</option>-->
<!--                           <option value="">$7,50,000</option>-->
<!--                           <option value="">$8,50,000</option>-->
<!--                           <option value="">$9,50,000</option>-->
<!--                         </select>-->
<!--                    </div>-->
<!--                </div>-->

<!--                <div class="cities">-->
<!--                  <div class="input-place">-->
<!--                    <a onclick="document.getElementById('search1').submit();" type="submit" class="header-btn1">Search <img src="assets/img/icons/search1.svg" alt=""></a>-->
<!--                  </div>-->
<!--                </div>-->
<!--              </form>-->
<!--            </div>-->
<!--          </div>-->
<!--        </div>-->
<!--      </div>-->
<!--    </div>-->
<!--  </div>-->
<!--</div>-->

<!--===== ABOUT AREA STARTS =======-->
<div class="about1-section-area sp1">
  <div class="container">
    <div class="row align-items-center">
      <div class="col-lg-7">
        <div class="about-images-area">
          <div class="row">
            <div class="col-lg-6 col-md-6">
              <div class="space90 d-lg-block d-none"></div>
              <div class="author-images">
                <div class="img1 image-anime reveal">
                  <img src="assets/img/all-images/about-img1.png" alt="">
                </div>
                <div class="city-area">
                  <div class="icons">
                    <img src="assets/img/icons/logo-icon1.svg" alt="">
                  </div>
                  <div class="content">
                    <!--<a href="">New York City</a>-->
                    <p>We showcase completed projects like Advaitha and White Pearl, designed with elegant architecture and spacious layouts.</p>
                  </div>
                </div>
              </div>
            </div>


            <div class="col-lg-6 col-md-6">
              <div class="author-images">
                <div class="img1 image-anime reveal">
                  <img src="assets/img/all-images/about-img2.png" alt="">
                </div>
                <div class="space30"></div>
                <div class="img1 image-anime reveal">
                  <img src="assets/img/all-images/about-img3.png" alt="">
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>

      <div class="col-lg-5">
        <div class="about-content-header heading2">
          <h5 class="text-anim"><img src="assets/img/icons/logo-icon1.svg" alt="">Ishtika - Sanskrit Name For Brick</h5>
          <h2 class="text-anim">Building Dreams, One At A Time! </h2>
          <div class="space24"></div>
          <p class="text-anim">At Ishtika Homes, we blend luxury and nature to create exceptional living spaces in Karnataka. Our projects focus on providing sustainable, vaastu-compliant residences that combine modern amenities with elegant design. With a commitment to quality and comfort, we craft homes that inspire lifestyles and elevate everyday living.</p>
          <div class="space32"></div>
          <div class="btn-area1 text-anim">
            <a href="about.php" class="header-btn1">Discover More <img src="assets/img/icons/arrow1.svg" alt=""></a>
          </div>
        </div>
      </div>
    </div>
  </div>
</div>
<!--===== ABOUT AREA ENDS =======-->

<!--===== FEATURES AREA STARTS =======-->
<div class="features1-section-area sp2">
  <div class="container">
    <div class="row">
      <div class="col-lg-4 m-auto">
        <div class="features-header text-center heading2">
          <h5 class="text-anim"><img src="assets/img/icons/logo-icon1.svg" alt="">Luxurious Living</h5>
          <h2 class="text-anim">Few Ongoing Projects</h2>
        </div>
      </div>
    </div>

    <div class="row">
      <div class="col-lg-12">
        <div class="features-slider-area owl-carousel">
          <div class="featured-boxarea">
            <div class="img1">
              <img src="assets/img/all-images/features-img1.png" alt="">
            </div>
            <!--<div class="sell-point">-->
            <!--  <a href="#" class="sell">Sell</a>-->
            <!--  <a href="#" class="featured">Featured</a>-->
            <!--</div>-->
            <div class="content-area">
              <a href="krishna.php">Krishna</a>
              <ul>
                    <li><a ><img src="assets/img/icons/location1.svg" alt="">Hosapete</a></li>
              </ul>
              <p>This 2.25 lakh Sqft residential project in Hospet spans 1.25 acres, offering 119 luxury flats with proximity to key landmarks, ideal for families.</p>
             
              <div class="price-area">
                  <div class="btn-area1 w-100 text-center">
                    <a href="krishna.php" class="header-btn1 w-100">View Project</a>
                  </div>
                </div>
            </div>
          </div>

          <div class="featured-boxarea">
            <div class="img1">
              <img src="assets/img/all-images/features-img2.png" alt="">
            </div>
            
            <div class="content-area">
              <a href="vashishta.php">Vashishta</a>
              <ul>
                    <li><a ><img src="assets/img/icons/location1.svg" alt="">Bengaluru</a></li>
              </ul>
              <p>A 1.25 lakh Sqft premium residential project in JP Nagar, blending comfort, affordability, and vaastu compliance amidst natural landscapes.</p>
              
              <div class="price-area">
                  <div class="btn-area1 w-100 text-center">
                    <a href="vashishta.php" class="header-btn1 w-100">View Project</a>
                  </div>
                </div>
            </div>
          </div>

          <div class="featured-boxarea">
            <div class="img1">
              <img src="assets/img/all-images/features-img3.png" alt="">
            </div>
            <!--<div class="sell-point">-->
            <!--  <a href="#" class="sell">Sell</a>-->
            <!--  <a href="#" class="featured">Featured</a>-->
            <!--</div>-->
            <div class="content-area">
              <a href="naadam.php">Naadam</a>
              <ul>
                    <li><a ><img src="assets/img/icons/location1.svg" alt="">Ballary</a></li>
              </ul>
              <p>A 1.42 lakh Sqft project in Bellary featuring lush landscapes, walkways, and kids' play areas, providing tranquility and a nature-centric life.</p>
              
              <div class="price-area">
                  <div class="btn-area1 w-100 text-center">
                    <a href="naadam.php" class="header-btn1 w-100">View Project</a>
                  </div>
                </div>
            </div>
          </div>

          <div class="featured-boxarea">
            <div class="img1">
              <img src="assets/img/all-images/features-img4.png" alt="">
            </div>
            
            <div class="content-area">
              <a href="vyasa.php">Vyasa</a>
              <ul>
                    <li><a ><img src="assets/img/icons/location1.svg" alt="">Ballary</a></li>
              </ul>
              <p>An 80,000 Sqft residential project in Bellary, offering vaastu-compliant luxury apartments set against beautiful natural landscapes.</p>
              
              <div class="price-area">
                  <div class="btn-area1 w-100 text-center">
                    <a href="vyasa.php" class="header-btn1 w-100">View Project</a>
                  </div>
                </div>
            </div>
          </div>

          <div class="featured-boxarea">
            <div class="img1">
              <img src="assets/img/all-images/features-img5.png" alt="">
            </div>
            
            <div class="content-area">
              <a href="anahata.php">Anahata</a>
              <ul>
                    <li><a href="#"><img src="assets/img/icons/location1.svg" alt="">Bengaluru</a></li>
              </ul>
              <p>A 440-unit, 2 & 3 BHK gated community in Bengaluru blending resort-style living with vaastu-compliant design for lasting comfort.</p>
              
              <div class="price-area">
                  <div class="btn-area1 w-100 text-center">
                    <a href="anahata.php" class="header-btn1 w-100">View Project</a>
                  </div>
                </div>
            </div>
          </div>

        </div>
      </div>
    </div>
  </div>
</div>
<!--===== FEATURES AREA ENDS =======-->

<!--===== GALLERY AREA STARTS =======-->
<div class="gallery1-section-area gallery sp1 ">
  <div class="container">
    <div class="row">
      <div class="col-lg-8 m-auto">
        <div class="gallery-header text-center heading2">
          <h5 class="text-anim"><img src="assets/img/icons/logo-icon1.svg" alt="">Modern & Chic</h5>
          <h2 class="text-anim">Our Completed Projects</h2>
        </div>
      </div>
    </div>
    <div class="row">
        
        <div class="col-lg-4 col-md-6 d-none">
        <div class="galler-author-area"  data-aos-duration="1100" >
          <div class="img1">
            <img src="assets/img/all-images/naadam-img1.png" alt="">
          </div>

          <div class="content-area">
            <a href="assets/img/all-images/naadam-img1.png" class="popup-img">Naadam</a>
          </div>
        </div>
        </div>
        
        <div class="col-lg-4 col-md-6 d-none">
        <div class="galler-author-area"  data-aos-duration="1100" >
          <div class="img1">
            <img src="assets/img/all-images/naadam-img8.png" alt="">
          </div>

          <div class="content-area">
            <a href="assets/img/all-images/naadam-img8.png" class="popup-img">Naadam</a>
          </div>
        </div>
        </div>
        
        <div class="col-lg-4 col-md-6 d-none">
        <div class="galler-author-area"  data-aos-duration="1100" >
          <div class="img1">
            <img src="assets/img/all-images/vashishta-img6.png" alt="">
          </div>

          <div class="content-area">
            <a href="assets/img/all-images/vashishta-img6.png" class="popup-img">Vashishta</a>
          </div>
        </div>
        </div>
        
        <div class="col-lg-4 col-md-6 d-none">
        <div class="galler-author-area"  data-aos-duration="1100" >
          <div class="img1">
            <img src="assets/img/all-images/krishna-img8.png" alt="">
          </div>

          <div class="content-area">
            <a href="assets/img/all-images/krishna-img8.png" class="popup-img">Krishna</a>
          </div>
        </div>
        </div>
        
      <div class="col-lg-4 col-md-6 d-none">
        <div class="galler-author-area"  data-aos-duration="1100" >
          <div class="img1">
            <img src="assets/img/all-images/vyasa-img3.png" alt="">
          </div>

          <div class="content-area">
            <a href="assets/img/all-images/vyasa-img3.png" class="popup-img">Vyasa</a>
          </div>
        </div>
        </div>
        
        <div class="col-lg-4 col-md-6">
        <div class="galler-author-area"  data-aos-duration="1100" >
          <div class="img1">
            <img src="assets/img/all-images/agastya-img3.png" alt="">
          </div>

          <div class="content-area">
            <a href="assets/img/all-images/agastya-img3.png" class="popup-img">Agastya</a>
          </div>
        </div>
        </div>
        
        <div class="col-lg-4 col-md-6">
        <div class="galler-author-area"data-aos-duration="1100" >
          <div class="img1">
            <img src="assets/img/all-images/gallery-img1.png" alt="">
          </div>

          <div class="content-area">
            <a href="assets/img/all-images/gallery-img1.png" class="popup-img">Sunrise</a>
          </div>
        </div>
        </div>
        
        <div class="col-lg-4 col-md-6">
        <div class="galler-author-area"data-aos-duration="1100" >
          <div class="img1">
            <img src="assets/img/all-images/gallery-img3.png" alt="">
          </div>

          <div class="content-area">
            <a href="assets/img/all-images/gallery-img3.png" class="popup-img">Advaitha</a>
          </div>
        </div>
        </div>
        
        <div class="col-lg-4 col-md-6">
        <div class="galler-author-area"data-aos-duration="1100" >
          <div class="img1">
            <img src="assets/img/all-images/gallery-img6.png" alt="">
          </div>

          <div class="content-area">
            <a href="assets/img/all-images/gallery-img6.png" class="popup-img">White Pearl</a>
          </div>
        </div>
        </div>
        
                <div class="col-lg-4 col-md-6">
        <div class="galler-author-area"  data-aos-duration="1100" >
          <div class="img1">
            <img src="assets/img/all-images/gallery-img7.png" alt="">
          </div>

          <div class="content-area">
            <a href="assets/img/all-images/gallery-img7.png" class="popup-img">Pride</a>
          </div>
        </div>
        </div>
        
        <div class="col-lg-4 col-md-6">
        <div class="galler-author-area"data-aos-duration="1100" >
          <div class="img1">
            <img src="assets/img/all-images/gallery-img8.png" alt="">
          </div>

          <div class="content-area">
            <a href="assets/img/all-images/gallery-img8.png" class="popup-img">Flora</a>
          </div>
        </div>
        </div>
        
        <div class="col-lg-4 col-md-6">
        <div class="galler-author-area"data-aos-duration="1100" >
          <div class="img1">
            <img src="assets/img/all-images/gallery-img13.png" alt="">
          </div>

          <div class="content-area">
            <a href="assets/img/all-images/gallery-img13.png" class="popup-img">Arcadia</a>
          </div>
        </div>
        </div>
        
        <div class="col-lg-4 col-md-6">
        <div class="galler-author-area"data-aos-duration="1100" >
          <div class="img1">
            <img src="assets/img/all-images/gallery-img14.png" alt="">
          </div>

          <div class="content-area">
            <a href="assets/img/all-images/gallery-img14.png" class="popup-img">Chanasya</a>
          </div>
        </div>
        </div>
        
        <div class="col-lg-4 col-md-6">
        <div class="galler-author-area"data-aos-duration="1100" >
          <div class="img1">
            <img src="assets/img/all-images/gallery-img11.png" alt="">
          </div>

          <div class="content-area">
            <a href="assets/img/all-images/gallery-img11.png" class="popup-img">Prakriti</a>
          </div>
        </div>
        </div>
        
        
    </div>
  </div>
</div>
<!--===== GALLERY AREA ENDS =======-->

<!--===== TESTIMONIAL AREA STARTS =======-->
<div class="testimonial1-section-area sp1">
  <div class="container">
    <div class="row">
      <div class="col-lg-8 m-auto">
        <div class="testimonia-header heading2 text-center">
          <h5 class="text-anim"><img src="assets/img/icons/logo-icon1.svg" alt="">Testimonials</h5>
          <h2 class="text-anim">Real Stories from Happy Homeowners</h2>
        </div>
      </div>
    </div>

    <div class="row">
      <div class="col-lg-10 m-auto">
        <div class="testimonial-sliders">
          <div class="row align-items-center">
           <div class="col-lg-6">
             <div class="teimonial-slider-nav-area foter-carousel">
               <div class="testimonial-slider-img reveal image-anime">
                <img src="assets/img/all-images/testimonial-img1.png" alt="">
               </div>
               <div class="testimonial-slider-img reveal image-anime">
                <img src="assets/img/all-images/testimonial-img3.png" alt="">
               </div>
               <div class="testimonial-slider-img reveal image-anime">
                <img src="assets/img/all-images/testimonial-img4.png" alt="">
               </div>
               <div class="testimonial-slider-img reveal image-anime">
                <img src="assets/img/all-images/testimonial-img1.png" alt="">
               </div>
               <div class="testimonial-slider-img reveal image-anime">
                <img src="assets/img/all-images/testimonial-img3.png" alt="">
               </div>
               <div class="testimonial-slider-img reveal image-anime">
                <img src="assets/img/all-images/testimonial-img4.png" alt="">
               </div>
             </div>
           </div>

           <div class="col-lg-6">
            <div class="testimonial-content-slider slider-nav1">
              <div class="testemonial-boxarea">
                <div class="img">
                  <img src="assets/img/icons/quito.svg" alt="">
                </div>
                <div class="space32"></div>
                <div class="content-area">
                  <!--<h4>Professional & Personable</h4>-->
                  <p>"Ishtika Homes has truly redefined luxury living for us. Our apartment in White Pearl is not only beautifully designed but also incorporates sustainable practices and vaastu principles, making it a perfect blend of elegance and comfort. The attention to detail and the modern amenities provided have enhanced our everyday living experience."</p>
                </div>
                <div class="author-content">
                  <div class="img1">
                    <img src="assets/img/all-images/testimonial-img2.png" alt="">
                  </div>
                  <div class="content">
                    <a >Priya Sharma</a>
                    <p>Bangalore</p>
                  </div>
                </div>
              </div>

              <div class="testemonial-boxarea">
                <div class="img">
                  <img src="assets/img/icons/quito.svg" alt="">
                </div>
                <div class="space32"></div>
                <div class="content-area">
                  <!--<h4>Professional & Personable</h4>-->
                  <p>"Choosing Ishtika Homes for our new residence was a decision we are proud of. The Advaitha project impressed us with its spacious layout and sophisticated design. The commitment to creating a nature-integrated living space while maintaining high standards of luxury and affordability is commendable."</p>
                </div>
                <div class="author-content">
                  <div class="img1">
                    <img src="assets/img/all-images/testimonial-img2.png" alt="">
                  </div>
                  <div class="content">
                    <a >Ravi Kumar</a>
                    <p>Bellary</p>
                  </div>
                </div>
              </div>

              <div class="testemonial-boxarea">
                <div class="img">
                  <img src="assets/img/icons/quito.svg" alt="">
                </div>
                <div class="space32"></div>
                <div class="content-area">
                  <!--<h4>Professional & Personable</h4>-->
                  <p>"We are thrilled with our home from Ishtika Homes. The blend of luxury and nature in their design philosophy is remarkable. The vaastu-compliant features and modern amenities provided in our residence have exceeded our expectations. It's clear that Ishtika Homes values quality and sustainable living."</p>
                </div>
                <div class="author-content">
                  <div class="img1">
                    <img src="assets/img/all-images/testimonial-img2.png" alt="">
                  </div>
                  <div class="content">
                    <a >Anita Desai</a>
                    <p>Hospet</p>
                  </div>
                </div>
              </div>
            </div>
            <div class="testimonial-arrows">
              <div class="testimonial-prev-arrow">
                <button><i class="fa-solid fa-arrow-left"></i></button>
              </div>
              <div class="testimonial-next-arrow">
                <button><i class="fa-solid fa-arrow-right"></i></button>
              </div>
            </div>
          </div>
          </div>
       </div>
      </div>
    </div>
  </div>
</div>
<!--===== TESTIMONIAL AREA ENDS =======-->

<!--===== PROPERTIES AREA STARTS =======-->
<div class="properties1-section-area sp1">
  <div class="container">
    <div class="row">
      <div class="col-lg-6 m-auto">
        <div class="property-header text-center heading2">
          <h5 class="text-anim"><img src="assets/img/icons/logo-icon1.svg" alt="">Coming Soon</h5>
          <h2 class="text-anim">Luxury High Rise Apartments</h2>
        </div>
      </div>
    </div>

    <div class="row">
      <div class="col-lg-8 m-auto">
        <div class="property-slider-area owl-carousel">
          <div class="property-boxarea">
            <div class="img1 image-anime reveal">
              <img src="assets/img/all-images/property-img1.png" alt="">
            </div>

            <div class="content-area d-none">
              <h2>$7,50,000</h2>
              <ul>
                <li><a href="#"><img src="assets/img/icons/sqft.svg" alt="">1350 sqft</a></li>
                <li><a href="#"><img src="assets/img/icons/bed.svg" alt="">3 Beds</a></li>
                <li class="m-0"><a href="#"><img src="assets/img/icons/bath.svg" alt="">2 Baths</a></li>
              </ul>
              <p>1722 Pollich Heights, Kamronview</p>
              <div class="space16"></div>
              <p>Listed by America Ordaz of Real Estate Agency</p>
              <div class="btn-area1">
                <a href="#" class="header-btn1">Buy Now <img src="assets/img/icons/arrow3.svg" alt=""></a>
              </div>
            </div>
          </div>

          <div class="property-boxarea">
            <div class="img1 image-anime reveal">
              <img src="assets/img/all-images/property-img2.png" alt="">
            </div>

            <div class="content-area d-none">
              <h2>$7,50,000</h2>
              <ul>
                <li><a href="#"><img src="assets/img/icons/sqft.svg" alt="">1350 sqft</a></li>
                <li><a href="#"><img src="assets/img/icons/bed.svg" alt="">3 Beds</a></li>
                <li class="m-0"><a href="#"><img src="assets/img/icons/bath.svg" alt="">2 Baths</a></li>
              </ul>
              <p>1722 Pollich Heights, Kamronview</p>
              <div class="space16"></div>
              <p>Listed by America Ordaz of Real Estate Agency</p>
              <div class="btn-area1">
                <a href="#" class="header-btn1">Buy Now <img src="assets/img/icons/arrow3.svg" alt=""></a>
              </div>
            </div>
          </div>

          <div class="property-boxarea">
            <div class="img1 image-anime reveal">
              <img src="assets/img/all-images/property-img3.png" alt="">
            </div>

            <div class="content-area d-none">
              <h2>$7,50,000</h2>
              <ul>
                <li><a href="#"><img src="assets/img/icons/sqft.svg" alt="">1350 sqft</a></li>
                <li><a href="#"><img src="assets/img/icons/bed.svg" alt="">3 Beds</a></li>
                <li class="m-0"><a href="#"><img src="assets/img/icons/bath.svg" alt="">2 Baths</a></li>
              </ul>
              <p>1722 Pollich Heights, Kamronview</p>
              <div class="space16"></div>
              <p>Listed by America Ordaz of Real Estate Agency</p>
              <div class="btn-area1">
                <a href="#" class="header-btn1">Buy Now <img src="assets/img/icons/arrow3.svg" alt=""></a>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</div>
<!--===== PROPERTIES AREA ENDS =======-->

<!--===== LOCATION AREA STARTS =======-->
<!--<div class="location1-section-area sp2">-->
<!--  <div class="container">-->
<!--    <div class="row">-->
<!--      <div class="col-lg-6 m-auto">-->
<!--        <div class="location-header text-center heading2">-->
<!--          <h5 class="text-anim"><img src="assets/img/icons/logo-icon1.svg" alt="">Our Location</h5>-->
<!--          <h2 class="text-anim">Find Properties In This Cities</h2>-->
<!--        </div>-->
<!--      </div>-->
<!--    </div>-->

<!--    <div class="row">-->
<!--      <div class="col-lg-3 col-md-6" data-aos="zoom-in" data-aos-duration="700">-->
<!--        <div class="location-images-area">-->
<!--          <div class="img1">-->
<!--            <img src="assets/img/all-images/location-img1.png" alt="">-->
<!--          </div>-->
<!--          <div class="content-area">-->
<!--            <a href="">Los Angles</a>-->
<!--            <p>8 Properties</p>-->
<!--            <a href="" class="readmore">Read More <i class="fa-solid fa-arrow-right"></i></a>-->
<!--          </div>-->
<!--        </div>-->
<!--      </div>-->

<!--      <div class="col-lg-4 col-md-6" data-aos="zoom-in" data-aos-duration="800">-->
<!--        <div class="location-images-area">-->
<!--          <div class="img1">-->
<!--            <img src="assets/img/all-images/location-img2.png" alt="">-->
<!--          </div>-->
<!--          <div class="content-area">-->
<!--            <a href="">Los Angles</a>-->
<!--            <p>8 Properties</p>-->
<!--            <a href="" class="readmore">Read More <i class="fa-solid fa-arrow-right"></i></a>-->
<!--          </div>-->
<!--        </div>-->
<!--      </div>-->

<!--      <div class="col-lg-5 col-md-6" data-aos="zoom-in" data-aos-duration="900">-->
<!--        <div class="location-images-area active">-->
<!--          <div class="img1">-->
<!--            <img src="assets/img/all-images/location-img6.png" alt="">-->
<!--          </div>-->
<!--          <div class="content-area">-->
<!--            <a href="">Los Angles</a>-->
<!--            <p>8 Properties</p>-->
<!--            <a href="" class="readmore">Read More <i class="fa-solid fa-arrow-right"></i></a>-->
<!--          </div>-->
<!--        </div>-->
<!--      </div>-->

<!--      <div class="col-lg-5 col-md-6" data-aos="zoom-in" data-aos-duration="1000">-->
<!--        <div class="location-images-area">-->
<!--          <div class="img1">-->
<!--            <img src="assets/img/all-images/location-img3.png" alt="">-->
<!--          </div>-->
<!--          <div class="content-area">-->
<!--            <a href="">Los Angles</a>-->
<!--            <p>8 Properties</p>-->
<!--            <a href="" class="readmore">Read More <i class="fa-solid fa-arrow-right"></i></a>-->
<!--          </div>-->
<!--        </div>-->
<!--      </div>-->

<!--      <div class="col-lg-4 col-md-6" data-aos="zoom-in" data-aos-duration="1100">-->
<!--        <div class="location-images-area">-->
<!--          <div class="img1">-->
<!--            <img src="assets/img/all-images/location-img4.png" alt="">-->
<!--          </div>-->
<!--          <div class="content-area">-->
<!--            <a href="">Los Angles</a>-->
<!--            <p>8 Properties</p>-->
<!--            <a href="" class="readmore">Read More <i class="fa-solid fa-arrow-right"></i></a>-->
<!--          </div>-->
<!--        </div>-->
<!--      </div>-->

<!--      <div class="col-lg-3 col-md-6" data-aos="zoom-in" data-aos-duration="1200">-->
<!--        <div class="location-images-area">-->
<!--          <div class="img1">-->
<!--            <img src="assets/img/all-images/location-img5.png" alt="">-->
<!--          </div>-->
<!--          <div class="content-area">-->
<!--            <a href="">Los Angles</a>-->
<!--            <p>8 Properties</p>-->
<!--            <a href="" class="readmore">Read More <i class="fa-solid fa-arrow-right"></i></a>-->
<!--          </div>-->
<!--        </div>-->
<!--      </div>-->
<!--    </div>-->
<!--  </div>-->
<!--</div>-->
<!--===== LOCATION AREA ENDS =======-->



<!--===== CONTACT AREA STARTS =======-->
<div class="contact1-section-area sp1">
  <div class="container">
    <div class="row">
      <div class="col-lg-7 m-auto">
        <div class="contact-header text-center heading2">
          <h5 class="text-anim"><img src="assets/img/icons/logo-icon1.svg" alt="">Valuation</h5>
          <h2 class="text-anim">Request Your Property Valuation</h2>
        </div>
      </div>
    </div>

    <div class="row">
      <div class="col-lg-10 m-auto" data-aos="zoom-in" data-aos-duration="1000">
        <div class="contact-author-boxarea">
          <h3>Personal Information</h3>
          <div class="row">
            <div class="col-lg-4">
              <div class="contact-input-area">
                <select name="country"  class="nice-select">
                  <option value="1" data-display="Select">Select</option>
                  <option value="">Krishna</option>
                  <option value="">Vashishta</option>
                  <option value="">Naadam</option>
                  <option value="">Vyasa</option>
                  <option value="">Agastya</option>
                </select>
              </div>
            </div>

            <div class="col-lg-4">
              <div class="contact-input-area">
                <input type="text" placeholder="First Name">
              </div>
            </div>

            <div class="col-lg-4">
              <div class="contact-input-area">
                <input type="text" placeholder="Last Name">
              </div>
            </div>

            <div class="col-lg-8">
              <div class="contact-input-area">
                <input type="email" placeholder="Email Address">
              </div>
            </div>

            <div class="col-lg-4">
              <div class="contact-input-area">
                <input type="number" placeholder="Phone Number">
              </div>
            </div>
            <div class="space50"></div>
            <h3>Property Information</h3>
          <div class="row">
            <div class="col-lg-4">
              <div class="contact-input-area">
                <select name="country"  class="nice-select">
                  <option value="1" data-display="Type">Type</option>
                  <option value="">Residential Property</option>
                  <option value="">Commercial Property</option>
                  <option value="">Industrial Property</option>
                  <option value="">Land Property</option>
                  <option value="">Mixed-Use Property</option>
                </select>
              </div>
            </div>

            <div class="col-lg-4">
              <div class="contact-input-area">
                <input type="text" placeholder="Zip Code">
              </div>
            </div>

            <div class="col-lg-4">
              <div class="contact-input-area">
                <select name="country"  class="nice-select">
                  <option value="1" data-display="City">City</option>
                  <option value="">France City</option>
                  <option value="">Belgium City</option>
                  <option value="">Norway City</option>
                  <option value="">Italy City</option>
                  <option value="">Denmark City</option>
                </select>
              </div>
            </div>

            <div class="col-lg-4">
              <div class="contact-input-area">
                <input type="text" placeholder="Number of Bedrooms">
              </div>
            </div>

            <div class="col-lg-4">
              <div class="contact-input-area">
                <input type="text" placeholder="N. of bathrooms">
              </div>
            </div>

            <div class="col-lg-4">
              <div class="contact-input-area">
                <input type="text" placeholder="Your Budget">
              </div>
            </div>
            <div class="space16"></div>
            <div class="col-lg-12">
              <div class="contact-input-area">
                <button type="submit" class="header-btn1">Submit Now <img src="assets/img/icons/arrow1.svg" alt=""></button>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</div>
</div>
<!--===== CONTACT AREA ENDS =======-->

<!--===== CTA AREA STARTS =======-->
<div class="cta1-section-area sp1">
  <div class="container">
    <div class="row align-items-center">
      <div class="col-lg-5">
        <div class="cta-header heading1">
          <h2 class="text-anim">We’re Here To Find Your New Home Project.</h2>
        </div>
      </div>
      <div class="col-lg-3"></div>
      <div class="col-lg-4">
        <div class="cta-btn-area">
          <a href="contact.php" class="header-btn1">Get a Quote <img src="assets/img/icons/arrow1.svg" alt=""></a>
          <a href="projects.php"  class="header-btn1 btn1">Our Projects<img src="assets/img/icons/arrow1.svg" alt=""></a>
        </div>
      </div>
    </div>
  </div>
</div>
<!--===== CTA AREA ENDS =======-->

<?php include('footer.php'); ?>