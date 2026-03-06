<?php include('header.php'); ?>
<style>
    .video{
        height: 52px;
    width: 52px;
    text-align: center;
    line-height: 52px;
    display: inline-block;
    transition: all 0.4s;
    border-radius: 50%;
    background: var(--ztc-bg-bg-2);
    color: var(--ztc-bg-bg-1);
    font-size: var(--ztc-font-size-font-s18);
    position: relative;
    z-index: 1;
    cursor: pointer;
    }
    .mfp-close-btn-in .mfp-close {
    color: #ffffff !important;
}
video{
    padding: 0 15vw;
}
@media(max-width:576px) {
    video{
    padding: 0 2vw;
}
}
</style>
<style>
.property-single-area .property-single-area ul{
    border-bottom:none;
}
    .poperty-details-pages .single-item .poperty-feature-wrap {
    display: flex;
    flex-wrap: wrap;
    justify-content: space-between;
    gap: 40px;
}

.poperty-details-pages .single-item .poperty-feature-wrap {
    display: block;
    /*grid-template-columns: repeat(2, 1fr);*/
    justify-content: space-between;
    gap: 40px
}

.poperty-details-pages .single-item .poperty-feature {
    margin: 0;
    padding: 0;
    display:grid;
    grid-template-columns: repeat(2, 1fr);
    list-style: none
}

.poperty-details-pages .single-item .poperty-feature li {
    display: flex;
    align-items: center;
    gap: 20px;
    line-height: 1;
    margin-bottom: 15px
}

.poperty-details-pages .single-item .poperty-feature li:last-child {
    margin-bottom: 0
}

.poperty-details-pages .single-item .poperty-feature li .icon {
    width: 40px;
    height: 40px;
    border: 2px solid rgba(22, 25, 30, .2);
    border-radius: 5px;
    display: flex;
    align-items: center;
    justify-content: center
}

.poperty-details-pages .single-item .poperty-feature li .content{
    width:90%;
}

.poperty-details-pages .single-item .poperty-feature li .content h6 {
    color: var(--title-color);
    font-family: var(--font-montserrat);
    font-size: 15px;
    font-weight: 600;
    /*margin-bottom: 8px;*/
    line-height: 1
}

.poperty-details-pages .single-item .poperty-feature li .content span {
    color: var(--text-color);
    font-family: var(--ztc-family-font2);
    font-size: 14px;
    font-weight: 400
}

.poperty-details-pages .single-item .kye-features ul {
    margin: 0;
    padding: 0;
    list-style: none;
    -moz-columns: 2;
    columns: 2
}

@media(max-width:767px) {
    .poperty-details-pages .single-item .kye-features ul {
        -moz-columns: 2;
        columns: 2
    }
}

@media(max-width:576px) {
    .poperty-details-pages .single-item .kye-features ul {
        -moz-columns: 1;
        columns: 1
    }
    .poperty-details-pages .single-item .poperty-feature {
    grid-template-columns: repeat(1, 1fr);
    list-style: none
}
}

.poperty-details-pages .single-item .kye-features ul li {
    color: var(--text-color);
    font-family: var(--ztc-family-font2);
    font-size: 16px;
    font-weight: 400;
    line-height: 1;
    display: flex;
    align-items: flex-start;
    gap: 10px;
    margin-bottom: 20px
}


.poperty-details-pages .single-item .overview-content {
    display: flex;
    justify-content: space-between;
    border-radius: 10px;
    border: 1px solid #eee;
    padding: 10px 10px
}

@media(max-width:767px) {
    .poperty-details-pages .single-item .overview-content {
        flex-direction: column;
        width: 100%;
        padding: 0px 0px
    }
}

.poperty-details-pages .single-item .overview-content ul {
    margin: 0;
    padding: 0;
    list-style: none;
    max-width: 100%;
    width: 100%;
    display:grid;
    grid-template-columns: repeat(2, 1fr);
}

@media(max-width:767px) {
    .poperty-details-pages .single-item .overview-content ul {
        max-width: 100%;
        width: 100%;
        margin-bottom: 20px;
       grid-template-columns: repeat(1, 1fr);
    }
}

.poperty-details-pages .single-item .overview-content ul li {
    color: var(--text-color);
    font-family: var(--ztc-family-font2);
    font-size: 15px;
    font-weight: 400;
    display: block;
    justify-content: space-between;
    gap:2px;
    line-height: 1;
    position: relative;
    padding: 10px 15px;
}

.poperty-details-pages .single-item .overview-content ul li span {
    font-family: var(--font-montserrat);
    line-height: 1;
    font-weight: 600
}

/*.poperty-details-pages .single-item .overview-content ul li:first-child {*/
/*    padding-top: 0*/
/*}*/

/*.poperty-details-pages .single-item .overview-content ul li:last-child {*/
/*    padding-bottom: 0*/
/*}*/

.poperty-details-pages .single-item .overview-content ul li:last-child::after {
    display: none;
    visibility: hidden
}

@media(max-width:767px) {
    .poperty-details-pages .single-item .overview-content ul li:last-child {
        padding-bottom: 10px
    }

    .poperty-details-pages .single-item .overview-content ul li:last-child::after {
        display: block;
        visibility: visible
    }
}

.poperty-details-pages .single-item .overview-content ul li::after {
    content: "";
    height: 1px;
    width: 100%;
    background: radial-gradient(50% 50% at 50% 50%, #EEE 0%, rgba(238, 238, 238, 0) 100%);
    position: absolute;
    bottom: 0;
    left: 0
}

.poperty-details-pages .single-item .faq-wrap {
    border-radius: 10px;
    border: 1px solid #eee;
    padding: 30px 20px
}

.faq-wrap .accordion .accordion-item {
    border-radius: 5px;
    border: 1px solid rgba(238, 238, 238, .5);
    background: #faf8fb;
    margin-bottom: 15px;
}

.faq-wrap .accordion .accordion-item .accordion-header .accordion-button:not(.collapsed) {
    border: transparent;
    box-shadow: none;
    background-color: transparent;
    color: var(--title-color)
}

.faq-wrap .accordion .accordion-item .accordion-header .accordion-button:not(.collapsed)::after {
    background-image: none;
    transform: unset;
    font-family: bootstrap-icons !important;
    content: "\f286";
    color: var(--title-color)
}

.faq-wrap .accordion .accordion-item .accordion-header .accordion-button {
    background: #faf8fb;
    color: var(--title-color);
    font-family: var(--font-montserrat);
    font-size: 15px;
    font-weight: 600;
    padding: 15px 25px 15px 25px;
}

.faq-wrap .accordion .accordion-item h5 {
    display:block;
    margin-bottom:0px;
}

.faq-wrap .accordion .accordion-item .accordion-header .accordion-button:focus {
    border: transparent;
    box-shadow: none
}

.faq-wrap .accordion .accordion-item .accordion-body {
    padding: 0 25px 15px;
    color: var(--text-color);
    font-family: var(--font-open-sans);
    font-size: 15px;
    font-weight: 400;
    line-height: 28px
}

@media(max-width:576px) {
    .faq-wrap .accordion .accordion-item .accordion-body {
        padding: 0 15px 15px
    }
    .faq-wrap .accordion .accordion-item .accordion-header .accordion-button {
    padding: 15px 15px 15px 15px;
    }
    .poperty-details-pages .single-item .faq-wrap {
    padding: 0px 0px;
}
}

.faq-wrap button {
    white-space: wrap
}


.faq-wrap .accordion .accordion-item .accordion-body {
    padding: 0 25px 15px;
    color: var(--text-color);
    font-family: var(--ztc-family-font2);
    font-size: 15px;
    font-weight: 400;
    line-height: 28px
}

@media(max-width:576px) {
    .faq-wrap .accordion .accordion-item .accordion-body {
        padding: 0 15px 15px
    }
}


.discount-banner {
    background-image: linear-gradient(rgba(0, 0, 0, 0.4), rgba(0, 0, 0, 0.4)), url(https://demo-egenslab.b-cdn.net/html/neckle/preview/assets/img/inner-page/discount-img.png);
    background-position: center center;
    background-size: cover;
    background-repeat: no-repeat;
    padding: 80px 50px;
    border-radius: 10px
}

@media(max-width:1399px) {
    .discount-banner {
        padding: 50px 25px
    }
}

.discount-banner .discount-content {
    border: 1px solid #fff;
    border-radius: 5px;
    padding: 30px;
    transform: skewY(-4deg);
    position: relative
}

.discount-banner .discount-content>span {
    color: white;
    font-family: var(--font-open-sans);
    font-size: 18px;
    font-style: italic;
    font-weight: 600;
    display: inline-block;
    margin-bottom: 10px
}

.discount-banner .discount-content h4 {
    color: white;
    font-family: var(--font-montserrat);
    font-size: 26px;
    font-style: italic;
    font-weight: 800;
    margin-bottom: 0
}

.discount-banner .discount-content h4 span {
    font-size: 24px;
    font-weight: 700
}

.discount-banner .discount-content .offer-area {
    max-width: 175px;
    margin-left: auto;
    margin-right: -40px;
    margin-top: 30px
}

.discount-banner .discount-content .offer-area span {
    background-image: url(../img/inner-page/icon/black-sharp.svg);
    color: white;
    font-family: var(--font-montserrat);
    font-size: 16px;
    font-style: italic;
    font-weight: 700;
    width: 130px;
    height: 50px;
    display: inline-flex;
    align-items: center;
    padding: 10px 20px;
    position: relative
}

.discount-banner .discount-content .offer-area h3 {
    background-image: url(../img/inner-page/icon/red-sharp.svg);
    margin-bottom: 0;
    color: white;
    font-family: var(--font-montserrat);
    font-size: 35px;
    font-style: italic;
    font-weight: 700;
    width: 171.912px;
    height: 106.681px;
    display: inline-flex;
    align-items: center;
    justify-content: end;
    padding-right: 40px;
    margin-top: -51px;
    margin-left: -20px
}

.discount-banner .discount-content .view-btns {
    position: absolute;
    bottom: -18px;
    left: 50%;
    transform: translateX(-50%)
}

.discount-banner .discount-content .view-btns a {
    padding: 7px 17px
}

.author-card {
    border-radius: 10px;
    background: #f0f0f0;
    padding: 35px 35px
}

@media(max-width:1399px) {
    .author-card {
        padding: 20px 20px
    }
}

.author-card .author-area {
    margin-bottom: 30px
}

.author-card .author-area .author-img {
    margin-bottom: 10px
}

.author-card .author-area .author-img img {
    width: 120px;
    height: 120px;
    border-radius: 8px;
}

.author-card .author-area .name-deg {
    line-height: 1;
    position: relative;
    padding-bottom: 15px
}

.author-card .author-area .name-deg::after {
    content: "";
    height: 1px;
    width: 160px;
    background: radial-gradient(50% 50% at 50% 50%, #EFB93F 0%, rgba(239, 185, 63, 0) 100%);
    left: 50%;
    transform: translateX(-50%);
    bottom: 0;
    position: absolute
}

.author-card .author-area .name-deg h6 {
    color: var(--title-color);
    font-family: var(--font-montserrat);
    font-size: 16px;
    font-weight: 700;
    margin-bottom: 5px
}

.author-card .author-area .name-deg span {
    color: var(--text-color);
    font-family: var(--font-open-sans);
    font-size: 12px;
    font-weight: 600
}

.author-card .author-area .view-listing-btn {
    color: #000;
    font-family: var(--font-montserrat);
    font-size: 14px;
    font-weight: 600;
    line-height: 1;
    transition: .35s
}

.author-card .author-area .view-listing-btn:hover {
    color: var(--primary-color1)
}

.author-card .contact-info {
    background-color: #fafafa;
    border-radius: 5px;
    padding: 25px;
    margin-bottom: 20px
}

.author-card .contact-info .single-contact {
    margin-bottom: 10px
}

.author-card .contact-info .single-contact:last-child {
    margin-bottom: 0
}

.author-card .contact-info .single-contact:last-child a span i {
    color: #f44336
}

.author-card .contact-info .single-contact a {
    display: flex;
    align-items: center;
    justify-content: space-between
}

.author-card .contact-info .single-contact a span {
    color: var(--title-color);
    font-family: var(--font-montserrat);
    font-size: 15px;
    font-weight: 500;
    display: flex;
    align-items: center;
    gap: 10px;
    cursor: pointer;
    transition: .35s
}

.author-card .contact-info .single-contact a span:hover {
    color: var(--primary-color1)
}

.author-card .contact-info .single-contact a i {
    font-size: 25px;
    color: #00d264
}

.author-card .button-group {
    display: flex;
    flex-direction: column;
    gap: 20px
}

.author-card .button-group .primary-btn3,
.author-card .button-group .primary-btn6 {
    justify-content: center
}


.primary-btn3 {
    border-radius: 5px;
    border:none;
    background-color: var(--ztc-bg-bg-2);
    font-family: var(--font-montserrat);
    font-weight: 600;
    font-size: 15px;
    color: #fff;
    padding: 15px 25px;
    display: inline-flex;
    align-items: center;
    white-space: nowrap;
    gap: 10px;
    transition: .5s;
    position: relative;
    overflow: hidden;
    z-index: 1;
    line-height: 1
}

.primary-btn3 svg {
    fill: #fff;
    transition: .5s
}

.primary-btn3:hover {
    color: white
}

.primary-btn3:hover svg {
    fill: white
}

.primary-btn3:hover.two svg path:first-child {
    fill: none;
    stroke: white
}

.primary-btn3:hover::after {
    transform: skewX(45deg) scale(1, 1)
}

.primary-btn3::after {
    position: absolute;
    content: "";
    display: block;
    left: 15%;
    right: -20%;
    top: -4%;
    height: 150%;
    width: 150%;
    bottom: 0;
    border-radius: 2px;
    background-color: var(--ztc-text-text-3);
    transform: skewX(45deg) scale(0, 1);
    z-index: -1;
    transition: all .5s ease-out 0s
}




.primary-btn6 {
    border-radius: 5px;
    background-color: var(--ztc-text-text-3);
    font-family: var(--font-montserrat);
    font-weight: 600;
    font-size: 15px;
    color: white;
    padding: 11px 28px;
    display: inline-flex;
    align-items: center;
    white-space: nowrap;
    gap: 10px;
    transition: .5s;
    position: relative;
    overflow: hidden;
    z-index: 1;
    border-radius: 5px;
    border: 1px solid var(--title-color)
}

.primary-btn6 svg {
    fill: var(--white-color);
    transition: .5s
}

.primary-btn6::after {
    position: absolute;
    content: "";
    display: block;
    left: 15%;
    right: -20%;
    top: -4%;
    height: 150%;
    width: 150%;
    bottom: 0;
    border-radius: 2px;
    background-color: white;
    transform: skewX(45deg) scale(0, 1);
    z-index: -1;
    transition: all .5s ease-out 0s
}

.primary-btn6:hover {
    color: var(--ztc-text-text-3)
}

.primary-btn6:hover svg {
    fill: var(--ztc-text-text-3)
}

.primary-btn6:hover::after {
    transform: skewX(45deg) scale(1, 1)
}


</style>



<!--===== PROPERTY AREA STARTS =======-->
<div class="property-single-area sp1 mt-5">
    <div class="container mt-3">
        <div class="row">
            <div class="col-lg-12">
                <div class="heading2 single-header">
                    <div class="content  w-75">
                        <h2>Krishna </h2>
                        <div class="space16"></div>
                        <h4 class=""><img src="assets/img/icons/location1.svg" alt="">Door No. 326 &327, Beside Venkateshwara Kalyana Mantapa, Ward No. 03, 1" Main Road, Patel Nagar, Hosapete, Vijayanagara (Dist) - 583 201.</h4>
                        
                    </div>

                    <div class="btn-area1">
                        <a href="assets/img/pdf/Krishna.pdf" class="header-btn1" target="_blank">
                            <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none">
                <path fill-rule="evenodd" clip-rule="evenodd"
                    d="M6.14982 0H14.1738L20.9744 7.08846V20.8756C20.9744 22.6027 19.5771 24 17.8559 24H6.14982C4.42269 24 3.02539 22.6027 3.02539 20.8756V3.12443C3.02536 1.3973 4.42266 0 6.14982 0Z"
                    fill="#E5252A" />
                <path opacity="0.302" fill-rule="evenodd" clip-rule="evenodd" d="M14.168 0V7.03449H20.9746L14.168 0Z"
                    fill="white" />
                <path
                    d="M6.49805 17.9072V13.5234H8.36312C8.82489 13.5234 9.19072 13.6494 9.46658 13.9072C9.74243 14.1591 9.88038 14.501 9.88038 14.9267C9.88038 15.3525 9.74243 15.6944 9.46658 15.9462C9.19072 16.2041 8.82489 16.33 8.36312 16.33H7.61949V17.9072H6.49805ZM7.61949 15.3765H8.23719C8.40509 15.3765 8.53703 15.3405 8.627 15.2566C8.71694 15.1786 8.76494 15.0707 8.76494 14.9268C8.76494 14.7829 8.71697 14.6749 8.627 14.5969C8.53706 14.513 8.40512 14.477 8.23719 14.477H7.61949V15.3765ZM10.3421 17.9072V13.5234H11.8953C12.2012 13.5234 12.4891 13.5654 12.7589 13.6554C13.0288 13.7453 13.2747 13.8713 13.4905 14.0452C13.7064 14.2131 13.8803 14.441 14.0063 14.7288C14.1262 15.0167 14.1922 15.3465 14.1922 15.7183C14.1922 16.0842 14.1262 16.414 14.0063 16.7018C13.8803 16.9897 13.7064 17.2176 13.4905 17.3855C13.2746 17.5594 13.0288 17.6853 12.7589 17.7753C12.4891 17.8652 12.2012 17.9072 11.8953 17.9072H10.3421ZM11.4396 16.9537H11.7634C11.9373 16.9537 12.0992 16.9358 12.2492 16.8938C12.3931 16.8518 12.531 16.7858 12.663 16.6959C12.7889 16.6059 12.8908 16.48 12.9628 16.3121C13.0348 16.1442 13.0708 15.9462 13.0708 15.7183C13.0708 15.4845 13.0348 15.2866 12.9628 15.1187C12.8908 14.9508 12.7889 14.8248 12.663 14.7348C12.531 14.6449 12.3931 14.5789 12.2492 14.537C12.0992 14.495 11.9373 14.477 11.7634 14.477H11.4396V16.9537ZM14.7559 17.9072V13.5234H17.8744V14.477H15.8774V15.1786H17.4726V16.1261H15.8774V17.9072H14.7559Z"
                    fill="white" />
            </svg>
                            Download Brochure </a>
                    </div>
                </div>
            </div>

            <div class="col-lg-6">
                <div class="img1  ">
                    <div id="video-container">
    <!-- Video Thumbnail (Image) -->
    <!--<img id="video-thumbnail" src="assets/img/all-images/krishna-img1.png" alt="Video Thumbnail" style="cursor: pointer;">-->
<!-- Video section with a play button -->
<div class="img1 video-btn-area w-100 h-100 justify-content-center align-items-center" style="display: flex; background-image: url(assets/img/all-images/krishna-img1.png); border-radius: 10px; background-size: cover; min-height: 400px;">
    <a href="#video-popup" class="popup-video">
        <span class="video"><i class="fa-solid fa-play"></i></span>
        <span class="play"></span>
    </a>
</div>

<!-- Hidden video content -->
<div id="video-popup" class="mfp-hide" class="d-flex justify-content-center">
    <video  style="width:100%; height:auto;" controls muted playsinline >
        <source src="assets/img/krishna.mp4" type="video/mp4">
        Your browser does not support the video tag.
    </video>
</div>

    
  </div>
                </div>
            </div>
            <div class="col-lg-6">
                <div class="images-area">
                    <div class="row">
                        <div class="col-lg-6 col-md-6">
                            <div class="img1 image-anime reveal">
                                <img src="assets/img/all-images/krishna-img2.png" alt="">
                            </div>
                        </div>
                        <div class="col-lg-6 col-md-6">
                            <div class="img1 image-anime reveal">
                                <img src="assets/img/all-images/krishna-img3.png" alt="">
                            </div>
                        </div>

                        <div class="col-lg-6 col-md-6">
                            <div class="img1 image-anime reveal">
                                <img src="assets/img/all-images/krishna-img4.png" alt="">
                            </div>
                        </div>
                        <div class="col-lg-6 col-md-6">
                            <div class="img1 image-anime reveal">
                                <img src="assets/img/all-images/krishna-img5.png" alt="">
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <div class="row">
            <div class="col-lg-8">
                <div class="property-single-area poperty-details-pages heading2">
                    <div class="space50 d-lg-block d-none"></div>
                    <div class="space30 d-lg-none d-block"></div>
                    
                    
                    
                    <h3>About The Property</h3>
                    <div class="space24"></div>
                    <p>Welcome to Krishna, a vibrant urban living experience in the heart of Hosapete. This modern apartment offers a unique blend of cultural heritage and contemporary convenience. More than just a residence, Krishna invites you to immerse yourself in Hosapete's rich history while enjoying the comforts of modern living—a true sanctuary for those who cherish both tradition and urban energy.</p>

                    <div class="space50 d-lg-block d-none"></div>
                    <div class="space30 d-lg-none d-block"></div>
<div class="single-item mb-50">
<div class="title-and-tab mb-30">

<h3>Property Specifications</h3>
<div class="space24"></div>
</div>
<div class="overview-content">
<ul>
    <li>
        <span>STRUCTURE</span><br>
        <p>RCC framed structure</p>
    </li>
    <li>
        <span>WINDOWS</span><br>
        <p>UPVC sliding windows with mosquito mesh</p>
    </li>
    <li>
        <span>PAINTING</span><br>
        <p>All internal walls with premium emulsion paint. Exterior fascia with premium paint.</p>
    </li>
    <li>
        <span>PLUMBING & SANITARY</span><br>
        <p>Hindware/Cera or equivalent sanitary fittings.</p>
        <p>Hindware/Cera or equivalent bath fittings.</p>
    </li>
    <li>
        <span>SECURITY</span><br>
        <p>CCTV surveillance.</p>
    </li>
    <li>
        <span>LIFTS</span><br>
        <p>3 lifts of adequate capacity</p>
    </li>
    <li>
        <span>POWER BACKUP</span><br>
        <p>Generator for lift, common areas; 0.5kVA to each flat</p>
    </li>
    <li>
        <span>ELECTRICAL & COMMUNICATION</span><br>
        <p>Fire resistant PVC insulated copper wires.</p>
        <p>Havells / Salzer / VGuard or equivalent switches</p>
    </li>
    <li>
        <span>FLOORING</span><br>
        <p>Superior quality vitrified tiles for kitchen, Dining, Living Area, Bedrooms and Foyer.</p>
        <p>Anti-skid ceramic/vitrified tiles for toilets, utility & Balcony</p>
    </li>
    <li>
        <span>DOORS</span><br>
        <p><b>Main door:</b> Teak wood frame with veneer finish shutters. Hardware of reputed make.</p>
        <p><b>Internal door:</b> Hardwood door frame / WPC / Mitti or Equivalent with flush door shutters.</p>
    </li>
    <li>
        <span>KITCHEN</span><br>
        <p>Polished Granite Kitchen platform, Stainless Steel Sink. Provision for water purifier.</p>
        <p>2 feet wall dado of premium tiles above the kitchen platform</p>
    </li>
</ul>


</div>
</div>


                    <div class="space50 d-lg-block d-none"></div>
                    <div class="space30 d-lg-none d-block"></div>
<div class="single-item mb-30">
<div class="kye-features">

<h3>Salient Features</h3>
<div class="space24"></div>
<ul>
    <li>
        <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 12 12">
            <path
                d="M6 11.25C4.60761 11.25 3.27226 10.6969 2.28769 9.71231C1.30312 8.72774 0.75 7.39239 0.75 6C0.75 4.60761 1.30312 3.27226 2.28769 2.28769C3.27226 1.30312 4.60761 0.75 6 0.75C7.39239 0.75 8.72774 1.30312 9.71231 2.28769C10.6969 3.27226 11.25 4.60761 11.25 6C11.25 7.39239 10.6969 8.72774 9.71231 9.71231C8.72774 10.6969 7.39239 11.25 6 11.25ZM6 12C7.5913 12 9.11742 11.3679 10.2426 10.2426C11.3679 9.11742 12 7.5913 12 6C12 4.4087 11.3679 2.88258 10.2426 1.75736C9.11742 0.632141 7.5913 0 6 0C4.4087 0 2.88258 0.632141 1.75736 1.75736C0.632141 2.88258 0 4.4087 0 6C0 7.5913 0.632141 9.11742 1.75736 10.2426C2.88258 11.3679 4.4087 12 6 12Z">
            </path>
            <path
                d="M8.22751 3.72747C8.22217 3.73264 8.21716 3.73816 8.21251 3.74397L5.60776 7.06272L4.03801 5.49222C3.93138 5.39286 3.79034 5.33876 3.64462 5.34134C3.49889 5.34391 3.35985 5.40294 3.25679 5.506C3.15373 5.60906 3.0947 5.7481 3.09213 5.89382C3.08956 6.03955 3.14365 6.18059 3.24301 6.28722L5.22751 8.27247C5.28097 8.32583 5.34463 8.36788 5.4147 8.39611C5.48476 8.42433 5.5598 8.43816 5.63532 8.43676C5.71084 8.43536 5.78531 8.41876 5.85428 8.38796C5.92325 8.35716 5.98531 8.31278 6.03676 8.25747L9.03076 4.51497C9.13271 4.40796 9.18845 4.26514 9.18593 4.11737C9.18341 3.9696 9.12284 3.82875 9.0173 3.72529C8.91177 3.62182 8.76975 3.56405 8.62196 3.56446C8.47417 3.56486 8.33247 3.62342 8.22751 3.72747Z">
            </path>
        </svg> No Common Walls
    </li>
    <li>
        <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 12 12">
            <path
                d="M6 11.25C4.60761 11.25 3.27226 10.6969 2.28769 9.71231C1.30312 8.72774 0.75 7.39239 0.75 6C0.75 4.60761 1.30312 3.27226 2.28769 2.28769C3.27226 1.30312 4.60761 0.75 6 0.75C7.39239 0.75 8.72774 1.30312 9.71231 2.28769C10.6969 3.27226 11.25 4.60761 11.25 6C11.25 7.39239 10.6969 8.72774 9.71231 9.71231C8.72774 10.6969 7.39239 11.25 6 11.25ZM6 12C7.5913 12 9.11742 11.3679 10.2426 10.2426C11.3679 9.11742 12 7.5913 12 6C12 4.4087 11.3679 2.88258 10.2426 1.75736C9.11742 0.632141 7.5913 0 6 0C4.4087 0 2.88258 0.632141 1.75736 1.75736C0.632141 2.88258 0 4.4087 0 6C0 7.5913 0.632141 9.11742 1.75736 10.2426C2.88258 11.3679 4.4087 12 6 12Z">
            </path>
            <path
                d="M8.22751 3.72747C8.22217 3.73264 8.21716 3.73816 8.21251 3.74397L5.60776 7.06272L4.03801 5.49222C3.93138 5.39286 3.79034 5.33876 3.64462 5.34134C3.49889 5.34391 3.35985 5.40294 3.25679 5.506C3.15373 5.60906 3.0947 5.7481 3.09213 5.89382C3.08956 6.03955 3.14365 6.18059 3.24301 6.28722L5.22751 8.27247C5.28097 8.32583 5.34463 8.36788 5.4147 8.39611C5.48476 8.42433 5.5598 8.43816 5.63532 8.43676C5.71084 8.43536 5.78531 8.41876 5.85428 8.38796C5.92325 8.35716 5.98531 8.31278 6.03676 8.25747L9.03076 4.51497C9.13271 4.40796 9.18845 4.26514 9.18593 4.11737C9.18341 3.9696 9.12284 3.82875 9.0173 3.72529C8.91177 3.62182 8.76975 3.56405 8.62196 3.56446C8.47417 3.56486 8.33247 3.62342 8.22751 3.72747Z">
            </path>
        </svg> Prime Location
    </li>
    <li>
        <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 12 12">
            <path
                d="M6 11.25C4.60761 11.25 3.27226 10.6969 2.28769 9.71231C1.30312 8.72774 0.75 7.39239 0.75 6C0.75 4.60761 1.30312 3.27226 2.28769 2.28769C3.27226 1.30312 4.60761 0.75 6 0.75C7.39239 0.75 8.72774 1.30312 9.71231 2.28769C10.6969 3.27226 11.25 4.60761 11.25 6C11.25 7.39239 10.6969 8.72774 9.71231 9.71231C8.72774 10.6969 7.39239 11.25 6 11.25ZM6 12C7.5913 12 9.11742 11.3679 10.2426 10.2426C11.3679 9.11742 12 7.5913 12 6C12 4.4087 11.3679 2.88258 10.2426 1.75736C9.11742 0.632141 7.5913 0 6 0C4.4087 0 2.88258 0.632141 1.75736 1.75736C0.632141 2.88258 0 4.4087 0 6C0 7.5913 0.632141 9.11742 1.75736 10.2426C2.88258 11.3679 4.4087 12 6 12Z">
            </path>
            <path
                d="M8.22751 3.72747C8.22217 3.73264 8.21716 3.73816 8.21251 3.74397L5.60776 7.06272L4.03801 5.49222C3.93138 5.39286 3.79034 5.33876 3.64462 5.34134C3.49889 5.34391 3.35985 5.40294 3.25679 5.506C3.15373 5.60906 3.0947 5.7481 3.09213 5.89382C3.08956 6.03955 3.14365 6.18059 3.24301 6.28722L5.22751 8.27247C5.28097 8.32583 5.34463 8.36788 5.4147 8.39611C5.48476 8.42433 5.5598 8.43816 5.63532 8.43676C5.71084 8.43536 5.78531 8.41876 5.85428 8.38796C5.92325 8.35716 5.98531 8.31278 6.03676 8.25747L9.03076 4.51497C9.13271 4.40796 9.18845 4.26514 9.18593 4.11737C9.18341 3.9696 9.12284 3.82875 9.0173 3.72529C8.91177 3.62182 8.76975 3.56405 8.62196 3.56446C8.47417 3.56486 8.33247 3.62342 8.22751 3.72747Z">
            </path>
        </svg> 100% Vaastu Complaint
    </li>
    <li>
        <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 12 12">
            <path
                d="M6 11.25C4.60761 11.25 3.27226 10.6969 2.28769 9.71231C1.30312 8.72774 0.75 7.39239 0.75 6C0.75 4.60761 1.30312 3.27226 2.28769 2.28769C3.27226 1.30312 4.60761 0.75 6 0.75C7.39239 0.75 8.72774 1.30312 9.71231 2.28769C10.6969 3.27226 11.25 4.60761 11.25 6C11.25 7.39239 10.6969 8.72774 9.71231 9.71231C8.72774 10.6969 7.39239 11.25 6 11.25ZM6 12C7.5913 12 9.11742 11.3679 10.2426 10.2426C11.3679 9.11742 12 7.5913 12 6C12 4.4087 11.3679 2.88258 10.2426 1.75736C9.11742 0.632141 7.5913 0 6 0C4.4087 0 2.88258 0.632141 1.75736 1.75736C0.632141 2.88258 0 4.4087 0 6C0 7.5913 0.632141 9.11742 1.75736 10.2426C2.88258 11.3679 4.4087 12 6 12Z">
            </path>
            <path
                d="M8.22751 3.72747C8.22217 3.73264 8.21716 3.73816 8.21251 3.74397L5.60776 7.06272L4.03801 5.49222C3.93138 5.39286 3.79034 5.33876 3.64462 5.34134C3.49889 5.34391 3.35985 5.40294 3.25679 5.506C3.15373 5.60906 3.0947 5.7481 3.09213 5.89382C3.08956 6.03955 3.14365 6.18059 3.24301 6.28722L5.22751 8.27247C5.28097 8.32583 5.34463 8.36788 5.4147 8.39611C5.48476 8.42433 5.5598 8.43816 5.63532 8.43676C5.71084 8.43536 5.78531 8.41876 5.85428 8.38796C5.92325 8.35716 5.98531 8.31278 6.03676 8.25747L9.03076 4.51497C9.13271 4.40796 9.18845 4.26514 9.18593 4.11737C9.18341 3.9696 9.12284 3.82875 9.0173 3.72529C8.91177 3.62182 8.76975 3.56405 8.62196 3.56446C8.47417 3.56486 8.33247 3.62342 8.22751 3.72747Z">
            </path>
        </svg> 100% Natural Light & Ventilation
    </li>
    <li>
        <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 12 12">
            <path
                d="M6 11.25C4.60761 11.25 3.27226 10.6969 2.28769 9.71231C1.30312 8.72774 0.75 7.39239 0.75 6C0.75 4.60761 1.30312 3.27226 2.28769 2.28769C3.27226 1.30312 4.60761 0.75 6 0.75C7.39239 0.75 8.72774 1.30312 9.71231 2.28769C10.6969 3.27226 11.25 4.60761 11.25 6C11.25 7.39239 10.6969 8.72774 9.71231 9.71231C8.72774 10.6969 7.39239 11.25 6 11.25ZM6 12C7.5913 12 9.11742 11.3679 10.2426 10.2426C11.3679 9.11742 12 7.5913 12 6C12 4.4087 11.3679 2.88258 10.2426 1.75736C9.11742 0.632141 7.5913 0 6 0C4.4087 0 2.88258 0.632141 1.75736 1.75736C0.632141 2.88258 0 4.4087 0 6C0 7.5913 0.632141 9.11742 1.75736 10.2426C2.88258 11.3679 4.4087 12 6 12Z">
            </path>
            <path
                d="M8.22751 3.72747C8.22217 3.73264 8.21716 3.73816 8.21251 3.74397L5.60776 7.06272L4.03801 5.49222C3.93138 5.39286 3.79034 5.33876 3.64462 5.34134C3.49889 5.34391 3.35985 5.40294 3.25679 5.506C3.15373 5.60906 3.0947 5.7481 3.09213 5.89382C3.08956 6.03955 3.14365 6.18059 3.24301 6.28722L5.22751 8.27247C5.28097 8.32583 5.34463 8.36788 5.4147 8.39611C5.48476 8.42433 5.5598 8.43816 5.63532 8.43676C5.71084 8.43536 5.78531 8.41876 5.85428 8.38796C5.92325 8.35716 5.98531 8.31278 6.03676 8.25747L9.03076 4.51497C9.13271 4.40796 9.18845 4.26514 9.18593 4.11737C9.18341 3.9696 9.12284 3.82875 9.0173 3.72529C8.91177 3.62182 8.76975 3.56405 8.62196 3.56446C8.47417 3.56486 8.33247 3.62342 8.22751 3.72747Z">
            </path>
        </svg> Close To Railway Station & Bus Stand
    </li>
    <li>
        <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 12 12">
            <path
                d="M6 11.25C4.60761 11.25 3.27226 10.6969 2.28769 9.71231C1.30312 8.72774 0.75 7.39239 0.75 6C0.75 4.60761 1.30312 3.27226 2.28769 2.28769C3.27226 1.30312 4.60761 0.75 6 0.75C7.39239 0.75 8.72774 1.30312 9.71231 2.28769C10.6969 3.27226 11.25 4.60761 11.25 6C11.25 7.39239 10.6969 8.72774 9.71231 9.71231C8.72774 10.6969 7.39239 11.25 6 11.25ZM6 12C7.5913 12 9.11742 11.3679 10.2426 10.2426C11.3679 9.11742 12 7.5913 12 6C12 4.4087 11.3679 2.88258 10.2426 1.75736C9.11742 0.632141 7.5913 0 6 0C4.4087 0 2.88258 0.632141 1.75736 1.75736C0.632141 2.88258 0 4.4087 0 6C0 7.5913 0.632141 9.11742 1.75736 10.2426C2.88258 11.3679 4.4087 12 6 12Z">
            </path>
            <path
                d="M8.22751 3.72747C8.22217 3.73264 8.21716 3.73816 8.21251 3.74397L5.60776 7.06272L4.03801 5.49222C3.93138 5.39286 3.79034 5.33876 3.64462 5.34134C3.49889 5.34391 3.35985 5.40294 3.25679 5.506C3.15373 5.60906 3.0947 5.7481 3.09213 5.89382C3.08956 6.03955 3.14365 6.18059 3.24301 6.28722L5.22751 8.27247C5.28097 8.32583 5.34463 8.36788 5.4147 8.39611C5.48476 8.42433 5.5598 8.43816 5.63532 8.43676C5.71084 8.43536 5.78531 8.41876 5.85428 8.38796C5.92325 8.35716 5.98531 8.31278 6.03676 8.25747L9.03076 4.51497C9.13271 4.40796 9.18845 4.26514 9.18593 4.11737C9.18341 3.9696 9.12284 3.82875 9.0173 3.72529C8.91177 3.62182 8.76975 3.56405 8.62196 3.56446C8.47417 3.56486 8.33247 3.62342 8.22751 3.72747Z">
            </path>
        </svg> General Spatial Floor Plan
    </li>
    <li>
        <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 12 12">
            <path
                d="M6 11.25C4.60761 11.25 3.27226 10.6969 2.28769 9.71231C1.30312 8.72774 0.75 7.39239 0.75 6C0.75 4.60761 1.30312 3.27226 2.28769 2.28769C3.27226 1.30312 4.60761 0.75 6 0.75C7.39239 0.75 8.72774 1.30312 9.71231 2.28769C10.6969 3.27226 11.25 4.60761 11.25 6C11.25 7.39239 10.6969 8.72774 9.71231 9.71231C8.72774 10.6969 7.39239 11.25 6 11.25ZM6 12C7.5913 12 9.11742 11.3679 10.2426 10.2426C11.3679 9.11742 12 7.5913 12 6C12 4.4087 11.3679 2.88258 10.2426 1.75736C9.11742 0.632141 7.5913 0 6 0C4.4087 0 2.88258 0.632141 1.75736 1.75736C0.632141 2.88258 0 4.4087 0 6C0 7.5913 0.632141 9.11742 1.75736 10.2426C2.88258 11.3679 4.4087 12 6 12Z">
            </path>
            <path
                d="M8.22751 3.72747C8.22217 3.73264 8.21716 3.73816 8.21251 3.74397L5.60776 7.06272L4.03801 5.49222C3.93138 5.39286 3.79034 5.33876 3.64462 5.34134C3.49889 5.34391 3.35985 5.40294 3.25679 5.506C3.15373 5.60906 3.0947 5.7481 3.09213 5.89382C3.08956 6.03955 3.14365 6.18059 3.24301 6.28722L5.22751 8.27247C5.28097 8.32583 5.34463 8.36788 5.4147 8.39611C5.48476 8.42433 5.5598 8.43816 5.63532 8.43676C5.71084 8.43536 5.78531 8.41876 5.85428 8.38796C5.92325 8.35716 5.98531 8.31278 6.03676 8.25747L9.03076 4.51497C9.13271 4.40796 9.18845 4.26514 9.18593 4.11737C9.18341 3.9696 9.12284 3.82875 9.0173 3.72529C8.91177 3.62182 8.76975 3.56405 8.62196 3.56446C8.47417 3.56486 8.33247 3.62342 8.22751 3.72747Z">
            </path>
        </svg> Broad & Wider Balconies
    </li>
    
</ul>
</div>
</div>

                    <div class="space50 d-lg-block d-none"></div>
                    <div class="space30 d-lg-none d-block"></div>
                    
<div class="single-item mb-50">
    <h3>Amenities </h3>
    <div class="space24"></div>
    <div class="poperty-feature-wrap">
        <ul class="poperty-feature">
            <li>
                <div class="icon">
                    <svg xmlns="http://www.w3.org/2000/svg" width="24px" height="24px" viewBox="0 0 24 24"><path fill="#737373" d="M6 6a1 1 0 0 1 2 0v6.026c.632.06 1.3.192 2 .415V11h6v3.485a6.3 6.3 0 0 0 2-.355V6a3 3 0 0 0-6 0a1 1 0 1 0 2 0a1 1 0 1 1 2 0v3h-6V6a3 3 0 0 0-6 0a1 1 0 0 0 2 0m.592 9.025c-1.024.126-1.7.547-1.986.766a1 1 0 0 1-1.212-1.592c.492-.374 1.493-.979 2.953-1.16c1.475-.181 3.336.08 5.513 1.317c3.788 2.154 6.836.653 7.63.183a1 1 0 0 1 1.02 1.72c-1.077.638-4.949 2.502-9.639-.164c-1.836-1.044-3.268-1.195-4.279-1.07M4.606 19.79c.287-.219.962-.64 1.986-.766c1.01-.125 2.443.026 4.28 1.07c4.69 2.666 8.56.802 9.638.165a1 1 0 1 0-1.02-1.721c-.794.47-3.842 1.971-7.63-.183c-2.177-1.237-4.038-1.498-5.513-1.316c-1.46.18-2.46.785-2.953 1.16a1 1 0 0 0 1.212 1.59"/></svg>
                </div>
                <div class="content">
                    <h6>Swimming Pool</h6>
                    <!--<span>RCC framed structure</span>-->
                </div>
            </li>
            <li>
                <div class="icon">
                    <svg xmlns="http://www.w3.org/2000/svg" width="24px" height="24px" viewBox="0 0 24 24"><path fill="#737373" d="m21.68 13.26l.36 1.97l-19.69 3.54L2 16.8l2.95-.53l-.35-1.97c-.1-.54.26-1.06.81-1.16c.54-.1 1.06.26 1.16.81l.35 1.96l9.84-1.76l-.35-1.97c-.1-.55.26-1.07.81-1.18c.54-.08 1.06.28 1.16.82l.35 1.97zM10.06 18.4L8 22h8l-2.42-4.23z"/></svg>
                </div>
                <div class="content">
                    <h6>Children Play Area</h6>
                    <!--<span>Superior quality vitrified tiles for kitchen, Dining, Living Area, Bedrooms and Foyer. Anti-skid ceramic/vitrified tiles for toilets, utility & Balcony</span>-->
                </div>
            </li>
            <li>
                <div class="icon">
                    <svg xmlns="http://www.w3.org/2000/svg" width="24px" height="24px" viewBox="0 0 24 24"><path fill="#737373" d="M21 10h-4V8l-4.5-1.8V4H15V2h-3.5v4.2L7 8v2H3c-.55 0-1 .45-1 1v11h8v-5h4v5h8V11c0-.55-.45-1-1-1M8 20H4v-3h4zm0-5H4v-3h4zm4-7c.55 0 1 .45 1 1s-.45 1-1 1s-1-.45-1-1s.45-1 1-1m2 7h-4v-3h4zm6 5h-4v-3h4zm0-5h-4v-3h4z"/></svg>
                </div>
                <div class="content">
                    <h6>Multipurpose Hall</h6>
                </div>
            </li>
            <li>
                <div class="icon">
                    <svg xmlns="http://www.w3.org/2000/svg" width="24px" height="24px" viewBox="0 0 32 32"><path fill="#737373" d="M8 5a3 3 0 0 1 3-3h10a3 3 0 0 1 3 3v1a4 4 0 0 1-3 3.874V13c.793 0 1.502.344 2.073.777c.577.438 1.085 1.018 1.502 1.626c.809 1.18 1.425 2.69 1.425 3.847a2.75 2.75 0 0 1-2.099 2.672l1.308 6.458c.292 1.48-1.049 2.464-1.805 0L21.28 22h-.312l-.09 3.982c-.043 1.533-1.293 1.334-1.35 0L19.13 22h-6.26l-.398 3.982c-.058 1.334-1.307 1.533-1.35 0L11.032 22h-.313l-2.124 6.38c-.756 2.464-2.097 1.48-1.805 0l1.308-6.458A2.75 2.75 0 0 1 6 19.25c0-1.156.616-2.667 1.425-3.847c.417-.608.925-1.188 1.502-1.626C9.498 13.344 10.207 13 11 13V9.874A4 4 0 0 1 8 6zm3 10c-.207 0-.498.093-.864.371c-.361.274-.728.678-1.061 1.163C8.384 17.542 8 18.656 8 19.25c0 .414.336.75.75.75h14.5a.75.75 0 0 0 .75-.75c0-.594-.384-1.708-1.075-2.716c-.333-.485-.7-.89-1.06-1.163c-.367-.278-.658-.371-.865-.371zm8-2v-3h-6v3zm3-7V5a1 1 0 0 0-1-1H11a1 1 0 0 0-1 1v1a2 2 0 0 0 2 2h8a2 2 0 0 0 2-2"/></svg>
                </div>
                <div class="content">
                    <h6>Outdoor Sitting Spaces</h6>
                </div>
            </li>
            <li>
                <div class="icon">
                    <svg xmlns="http://www.w3.org/2000/svg" width="24px" height="24px" viewBox="0 0 24 24"><path fill="#737373" d="m20.274 9.869l-3.442-4.915l1.639-1.147l3.441 4.915zm-1.884 2.54L16.67 9.95l-8.192 5.736l1.72 2.457l-1.638 1.148l-4.588-6.554L5.61 11.59l1.72 2.458l8.192-5.736l-1.72-2.458l1.638-1.147l4.588 6.554zm2.375-5.326l1.638-1.147l-1.147-1.638l-1.638 1.147zM7.168 19.046l-3.442-4.915l-1.638 1.147l3.441 4.915zm-2.786-.491l-1.638 1.147l-1.147-1.638l1.638-1.147z"/></svg>
                </div>
                <div class="content">
                    <h6>Gymnasium</h6>
                </div>
            </li>
            <li>
                <div class="icon">
                    <svg xmlns="http://www.w3.org/2000/svg" width="24px" height="24px" viewBox="0 0 24 24"><path fill="#737373" d="M23 4.1V2.3l-1.8-.2c-.1 0-.7-.1-1.7-.1c-4.1 0-7.1 1.2-8.8 3.3C9.4 4.5 7.6 4 5.5 4c-1 0-1.7.1-1.7.1l-1.9.3l.1 1.7c.1 3 1.6 8.7 6.8 8.7H9v3.4c-3.8.5-7 1.8-7 1.8v2h20v-2s-3.2-1.3-7-1.8V15c6.3-.1 8-7.2 8-10.9M12 18h-1v-5.6S10.8 9 8 9c0 0 1.5.8 1.9 3.7c-.4.1-.8.1-1.1.1C4.2 12.8 4 6.1 4 6.1S4.6 6 5.5 6c1.9 0 5 .4 5.9 3.1C11.9 4.6 17 4 19.5 4c.9 0 1.5.1 1.5.1s0 9-6.3 9H14c0-2 2-5 2-5c-3 1-3 4.9-3 4.9v5z"/></svg>
                </div>
                <div class="content">
                    <h6>Landscaped Garden</h6>
                </div>
            </li>
            <li>
                <div class="icon">
                    <svg xmlns="http://www.w3.org/2000/svg" width="24px" height="24px" viewBox="0 0 16 16"><g fill="#737373"><path d="M9.5 1.5a1.5 1.5 0 1 1-3 0a1.5 1.5 0 0 1 3 0M6.44 3.752A.75.75 0 0 1 7 3.5h1.445c.742 0 1.32.643 1.243 1.38l-.43 4.083a1.8 1.8 0 0 1-.088.395l-.318.906l.213.242a.8.8 0 0 1 .114.175l2 4.25a.75.75 0 1 1-1.357.638l-1.956-4.154l-1.68-1.921A.75.75 0 0 1 6 8.96l.138-2.613l-.435.489l-.464 2.786a.75.75 0 1 1-1.48-.246l.5-3a.75.75 0 0 1 .18-.375l2-2.25Z"/><path d="M6.25 11.745v-1.418l1.204 1.375l.261.524a.8.8 0 0 1-.12.231l-2.5 3.25a.75.75 0 1 1-1.19-.914zm4.22-4.215l-.494-.494l.205-1.843l.006-.067l1.124 1.124h1.44a.75.75 0 0 1 0 1.5H11a.75.75 0 0 1-.531-.22Z"/></g></svg>
                </div>
                <div class="content">
                    <h6>Walking Track</h6>
                </div>
            </li>
            <li>
                <div class="icon">
                    <svg xmlns="http://www.w3.org/2000/svg" width="24px" height="24px" viewBox="0 0 32 32"><path fill="#737373" d="m18.607 8.9l.498.148l3.51-6.075c.54-.93 1.74-1.25 2.67-.71s1.25 1.74.71 2.67l-3.509 6.073l.379.358l.674 2.481a3.6 3.6 0 0 1-.349 2.722l-7.57 13.106a2.65 2.65 0 0 1-2.3 1.334h-.002c-.466 0-.925-.125-1.327-.361l-4.657-2.7a2.65 2.65 0 0 1-.974-3.63l7.568-13.107a3.55 3.55 0 0 1 2.187-1.668zm-.035 2.071l-1.956.509a1.54 1.54 0 0 0-.952.726L8.09 25.322a.644.644 0 0 0 .243.893L13 28.92a.646.646 0 0 0 .887-.245l7.571-13.113a1.58 1.58 0 0 0 .151-1.2l-.527-1.939l-1.1-1.035zm6.368 14.072q.06-.263.06-.543a2.5 2.5 0 0 0-4.19-1.842zM22.5 27c.652 0 1.245-.25 1.69-.658l-4.13-2.385q-.06.263-.06.543a2.5 2.5 0 0 0 2.5 2.5"/></svg>
                </div>
                <div class="content">
                    <h6>Cricket Practice Net</h6>
                </div>
            </li>
        </ul>
        
    </div>
</div>


                    
                </div>
            </div>

            <div class="col-lg-4">
                <div class="agent-single-area">
<div class="author-card text-center mb-50">
    
    <div class="img1 image-anime reveal pb-3">
        <img class="w-100" style="object-fit:cover" src="assets/img/all-images/krishna-img6.png" alt="">
    </div>
    <div class="img1 image-anime reveal pb-3">
        <img class="w-100" style="object-fit:cover" src="assets/img/all-images/krishna-img7.png" alt="">
    </div>
    <div class="img1 image-anime reveal pb-3">
        <img class="w-100" style="object-fit:cover" src="assets/img/all-images/krishna-img8.png" alt="">
    </div>
    <div class="button-group">
        <a class="primary-btn3" href="gallery.php?krishna=true">
            <svg xmlns="http://www.w3.org/2000/svg" width="10" height="10" viewBox="0 0 10 10">
                <path
                    d="M8.33624 2.84003L1.17627 10L0 8.82373L7.15914 1.66376H0.849347V0H10V9.15065H8.33624V2.84003Z" />
            </svg> View Gallery
        </a>
        
    </div>
</div>                
                
                
                
                    <div class="space50 d-lg-block d-none"></div>
                <div class="space30 d-lg-none d-block"></div>
                <div class="single-contact-area heading2">
                    <!--<a href="property-single.html" class="sell">For Rent</a>-->
                    <!--<div class="space24"></div>-->
                    <h3>Plan Your Visit</h3>
                    <div class="space24"></div>
                    <p>For more information or to schedule a private showing, please contact</p>
                    <div class="space24"></div>
                    <div class="input-area">
                        <input type="text" placeholder="Your Name">
                    </div>
                    <div class="space16"></div>
                    <div class="input-area">
                        <input type="email" placeholder="Email Address">
                    </div>
                    <div class="space16"></div>
                    <div class="input-area">
                        <input type="number" placeholder="Phone Number">
                    </div>
                    <div class="space24"></div>
                    <div class="input-area">
                       <button class="header-btn1" type="submit">Request Information <img src="assets/img/icons/arrow1.svg" alt=""></button>
                    </div>
                </div>
                

</div>
            </div>
        </div>
        
        <div class="row">
            <div class="col-lg-8">
                <div class="property-single-area poperty-details-pages heading2">
                    
<!--                    <div class="space50 d-lg-block d-none"></div>-->
<!--                    <div class="space30 d-lg-none d-block"></div>-->
<!--<div class="single-item mb-50">-->

<!--<h3>Floor Plan</h3>-->
<!--<div class="space24"></div>-->
<!--<div class="poperty-video-area">-->
<!--<img class="rounded border" src="assets/img/all-images/floor-plan-img1.png" />-->
<!--</div>-->
<!--</div>-->
                    <div class="space50 d-lg-block d-none"></div>
                    <div class="space30 d-lg-none d-block"></div>
<div class="single-item mb-50">

<h3>Project Location</h3>
<div class="space24"></div>
<div class="poperty-video-area">
<img class="rounded border" src="assets/img/all-images/map-img1.png" />
</div>
</div>
                    <div class="space50 d-lg-block d-none"></div>
                    <div class="space30 d-lg-none d-block"></div>
<div class="single-item mb-50">
<div class="faq-area">
<h3>Frequently Asked Questions</h3>
<div class="space24"></div>
<div class="faq-wrap">
<div class="accordion accordion-flush" id="accordionFlushExample">
<div class="accordion-item">
<h5 class="accordion-header" id="flush-headingOne">
<button class="accordion-button collapsed" type="button" data-bs-toggle="collapse" data-bs-target="#flush-collapseOne" aria-expanded="false" aria-controls="flush-collapseOne">
What is the location advantage of Ishtika Krishna?
</button>
</h5>
<div id="flush-collapseOne" class="accordion-collapse collapse show" aria-labelledby="flush-headingOne" data-bs-parent="#accordionFlushExample">
<div class="accordion-body">Ishtika Krishna is strategically located with easy access to major roads, schools, hospitals, and shopping centers, making daily commuting and lifestyle needs convenient for residents.</div>
</div>
</div>
<div class="accordion-item">
<h5 class="accordion-header" id="flush-headingTwo">
<button class="accordion-button collapsed" type="button" data-bs-toggle="collapse" data-bs-target="#flush-collapseTwo" aria-expanded="false" aria-controls="flush-collapseTwo">
What types of housing units are available at Ishtika Krishna?
</button>
</h5>
<div id="flush-collapseTwo" class="accordion-collapse collapse" aria-labelledby="flush-headingTwo" data-bs-parent="#accordionFlushExample">
<div class="accordion-body">The project offers a variety of housing options, including 2BHK and 3BHK apartments, catering to different family sizes and preferences.</div>
</div>
</div>
<div class="accordion-item">
<h5 class="accordion-header" id="flush-headingThree">
<button class="accordion-button collapsed" type="button" data-bs-toggle="collapse" data-bs-target="#flush-collapseThree" aria-expanded="false" aria-controls="flush-collapseThree">
What are the key features of the construction quality at Ishtika Krishna?
</button>
</h5>
<div id="flush-collapseThree" class="accordion-collapse collapse" aria-labelledby="flush-headingThree" data-bs-parent="#accordionFlushExample">
<div class="accordion-body">Ishtika Krishna is built with high-quality construction materials, ensuring durability, safety, and a premium finish. The design focuses on maximizing natural light and ventilation within each unit.</div>
</div>
</div>
<div class="accordion-item">
<h5 class="accordion-header" id="flush-headingFour">
<button class="accordion-button collapsed" type="button" data-bs-toggle="collapse" data-bs-target="#flush-collapseFour" aria-expanded="false" aria-controls="flush-collapseFour">
Is Ishtika Krishna a RERA-registered project?
</button>
</h5>
<div id="flush-collapseFour" class="accordion-collapse collapse" aria-labelledby="flush-headingFour" data-bs-parent="#accordionFlushExample">
<div class="accordion-body">Yes, Ishtika Krishna is a RERA-registered project, ensuring transparency, timely delivery, and adherence to regulatory standards for buyers' peace of mind.</div>
</div>
</div>
</div>
</div>
</div>
</div>
                </div>
            </div>
            <div class="col-lg-4">
                <div class="agent-single-area">
                    <div class="space30 d-lg-block d-none"></div>
                <div class="space20 d-lg-none d-block"></div>
                
                <div class="single-contact-area heading2">
                <h3 class="border-0">Latest</h3>
<div class="row">
        <div class="col-lg-12">
          <div class="latest-images-area owl-carousel">
            <div class="auhtor-boxes-area">
              <div class="img1">
                <img src="assets/img/all-images/krishna-img1.png" alt="">
              </div>
              
            </div>
            
            <div class="auhtor-boxes-area">
              <div class="img1">
                <img src="assets/img/all-images/krishna-img2.png" alt="">
              </div>
              
            </div>
  
            <div class="auhtor-boxes-area">
              <div class="img1">
                <img src="assets/img/all-images/krishna-img3.png" alt="">
              </div>
              
            </div>
  
            <div class="auhtor-boxes-area">
              <div class="img1">
                <img src="assets/img/all-images/krishna-img4.png" alt="">
              </div>
              
            </div>
  
            <div class="auhtor-boxes-area">
              <div class="img1">
                <img src="assets/img/all-images/krishna-img5.png" alt="">
              </div>
              
            </div>
            
          </div>
        </div>
      </div>
      </div>
                </div>
            </div>
        </div>
    </div>
</div>

<!--===== OTHERS AREA STARTS =======-->
<div class="others-inner-section-area sp1 bg1">
    <div class="container">
      <div class="row">
        <div class="col-lg-5 m-auto">
          <div class="social-header text-center heading2">
            
            <h2 class="text-anim">Gallery</h2>
          </div>
        </div>
      </div>
  
      <div class="row">
        <div class="col-lg-12">
          <div class="slider-images-area owl-carousel">
            <div class="auhtor-boxes-area">
              <div class="img1">
                <img src="assets/img/all-images/krishna-img1.png" alt="">
              </div>
              <div class="icons">
                <a class="popup-img" href="assets/img/all-images/krishna-img1.png"><i class="fa-brands fa-plus"></i></a>
              </div>
            </div>
            
            <div class="auhtor-boxes-area">
              <div class="img1">
                <img src="assets/img/all-images/krishna-img2.png" alt="">
              </div>
              <div class="icons">
                <a class="popup-img" href="assets/img/all-images/krishna-img2.png"><i class="fa-brands fa-plus"></i></a>
              </div>
            </div>
  
            <div class="auhtor-boxes-area">
              <div class="img1">
                <img src="assets/img/all-images/krishna-img3.png" alt="">
              </div>
              <div class="icons">
                <a class="popup-img" href="assets/img/all-images/krishna-img3.png"><i class="fa-brands fa-plus"></i></a>
              </div>
            </div>
  
            <div class="auhtor-boxes-area">
              <div class="img1">
                <img src="assets/img/all-images/krishna-img4.png" alt="">
              </div>
              <div class="icons">
                <a class="popup-img" href="assets/img/all-images/krishna-img4.png"><i class="fa-brands fa-plus"></i></a>
              </div>
            </div>
  
            <div class="auhtor-boxes-area">
              <div class="img1">
                <img src="assets/img/all-images/krishna-img5.png" alt="">
              </div>
              <div class="icons">
                <a class="popup-img" href="assets/img/all-images/krishna-img5.png"><i class="fa-brands fa-plus"></i></a>
              </div>
            </div>
  
            <div class="auhtor-boxes-area">
              <div class="img1">
                <img src="assets/img/all-images/krishna-img6.png" alt="">
              </div>
              <div class="icons">
                <a class="popup-img" href="assets/img/all-images/krishna-img6.png"><i class="fa-brands fa-plus"></i></a>
              </div>
            </div>
            
            <div class="auhtor-boxes-area">
              <div class="img1">
                <img src="assets/img/all-images/krishna-img7.png" alt="">
              </div>
              <div class="icons">
                <a class="popup-img" href="assets/img/all-images/krishna-img7.png"><i class="fa-brands fa-plus"></i></a>
              </div>
            </div>
            
            <div class="auhtor-boxes-area">
              <div class="img1">
                <img src="assets/img/all-images/krishna-img8.png" alt="">
              </div>
              <div class="icons">
                <a class="popup-img" href="assets/img/all-images/krishna-img8.png"><i class="fa-brands fa-plus"></i></a>
              </div>
            </div>
            
          </div>
        </div>
      </div>
    </div>
  </div>
  <!--===== OTHERS AREA ENDS =======-->

<div class="property-inner-area sp2 p-0">
    <div class="container">
        <div class="row">
            <div class="col-lg-4 m-auto">
                <div class="heading2 text-center">
                    <h2>More Projects</h2>
                </div>
                <div class="space30 d-lg-block d-none"></div>
                <div class="space20 d-lg-none d-block"></div>
            </div>
        </div>
      <div class="row">
      <div class="col-lg-12">
        <div class="features-slider-area owl-carousel">
          
          <div class="property-boxarea">
            <div class="img1">
              <img src="assets/img/all-images/features-img2.png" alt="">
            </div>
            
            <div class="content-area">
              <a href="vashishta.php">Vashishta</a>
              <p>A 1.25 lakh Sqft premium residential project in JP Nagar, blending comfort, affordability, and vaastu compliance amidst natural landscapes.</p>
              
              <div class="price-area">
                  <div class="btn-area1 w-100 text-center">
                    <a href="vashishta.php" class="header-btn1 w-100">View Project</a>
                  </div>
                </div>
            </div>
          </div>

          <div class="property-boxarea">
            <div class="img1">
              <img src="assets/img/all-images/features-img3.png" alt="">
            </div>
            <!--<div class="sell-point">-->
            <!--  <a href="#" class="sell">Sell</a>-->
            <!--  <a href="#" class="featured">Featured</a>-->
            <!--</div>-->
            <div class="content-area">
              <a href="naadam.php">Naadam</a>
              <p>A 1.42 lakh Sqft project in Bellary featuring lush landscapes, walkways, and kids' play areas, providing tranquility and a nature-centric life.</p>
              
              <div class="price-area">
                  <div class="btn-area1 w-100 text-center">
                    <a href="naadam.php" class="header-btn1 w-100">View Project</a>
                  </div>
                </div>
            </div>
          </div>

          <div class="property-boxarea">
            <div class="img1">
              <img src="assets/img/all-images/features-img4.png" alt="">
            </div>
            
            <div class="content-area">
              <a href="vyasa.php">Vyasa</a>
              <p>An 80,000 Sqft residential project in Bellary, offering vaastu-compliant luxury apartments set against beautiful natural landscapes.</p>
              
              <div class="price-area">
                  <div class="btn-area1 w-100 text-center">
                    <a href="vyasa.php" class="header-btn1 w-100">View Project</a>
                  </div>
                </div>
            </div>
          </div>

          <div class="property-boxarea">
            <div class="img1">
              <img src="assets/img/all-images/features-img5.png" alt="">
            </div>
            
            <div class="content-area">
              <a href="agastya.php">Agastya</a>
              <p>A 1.10 lakh Sqft project designed for joyous living with ample sunlight, ventilation, and strategic connectivity.</p>
              
              <div class="price-area">
                  <div class="btn-area1 w-100 text-center">
                    <a href="agastya.php" class="header-btn1 w-100">View Project</a>
                  </div>
                </div>
            </div>
          </div>

        </div>
      </div>
    </div>
    </div>
  </div>
<!--===== PROPERTY AREA ENDS =======-->

<!--===== CTA AREA STARTS =======-->
<div class="cta1-section-area sp1">
  <div class="container">
    <div class="row align-items-center">
      <div class="col-lg-5">
        <div class="cta-header heading1">
          <h2 >We’re Here To Find Your New Home Project.</h2>
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


<script>

    
    $(document).ready(function() {
    // Initialize Magnific Popup for inline video
    $('.popup-video').magnificPopup({
        type: 'inline', // Use 'inline' type for local video
        midClick: true, // Allow opening popup on mid-click
        mainClass: 'mfp-fade', // Optional: Add fade effect
        removalDelay: 300, // Delay before popup is removed
        closeBtnInside: true, // Close button inside the popup
        showCloseBtn: true, // Ensure close button is shown
        callbacks: {
            open: function() {
                $('.mfp-close').show(); // Ensure close button visibility
            }
        }
    });
});

</script>

