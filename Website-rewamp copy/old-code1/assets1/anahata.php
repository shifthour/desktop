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
                        <h2>Anahata </h2>
                        <div class="space16"></div>
                        <h4 class=""><img src="assets/img/icons/location1.svg" alt="">Sy No. 5, Samethanahalli Village, Bengaluru,Karnataka 560067.</h4>
                        
                    </div>

                    <div class="btn-area1">
                        <a href="assets/img/pdf/Anahata.pdf" class="header-btn1" target="_blank">
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
                <div class="img1 image-anime reveal">
                    <img src="assets/img/all-images/features-img5.png" alt="">
                </div>
            </div>
            <div class="col-lg-6">
                <div class="images-area">
                    <div class="row">
                        <div class="col-lg-6 col-md-6">
                            <div class="img1 image-anime reveal">
                                <img src="assets/img/all-images/anahata-img2.png" alt="">
                            </div>
                        </div>
                        <div class="col-lg-6 col-md-6">
                            <div class="img1 image-anime reveal">
                                <img src="assets/img/all-images/anahata-img3.png" alt="">
                            </div>
                        </div>

                        <div class="col-lg-6 col-md-6">
                            <div class="img1 image-anime reveal">
                                <img src="assets/img/all-images/anahata-img4.png" alt="">
                            </div>
                        </div>
                        <div class="col-lg-6 col-md-6">
                            <div class="img1 image-anime reveal">
                                <img src="assets/img/all-images/anahata-img5.png" alt="">
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
                    <p>Tucked just off Soukya Road in the fast-growing Whitefield corridor, Anahata offers residents an elegant escape from the bustle of Bengaluru while keeping them minutes from top international schools, hospitals and shopping hubs such as Nexus Whitefield. Thoughtful master-planning delivers wide internal roads, abundant greenery and uninterrupted airflow, ensuring every apartment enjoys both privacy and panoramic views.</p>

                    <div class="space50 d-lg-block d-none"></div>
                    <div class="space30 d-lg-none d-block"></div>
<div class="single-item mb-50">
<div class="title-and-tab mb-30">

<h3>Elite Specifications</h3>
<div class="space24"></div>
</div>
<div class="overview-content">
<ul>
    <li>
        <span>STRUCTURE</span><br>
        <p>Seismic Zone II compliant RCC-framed structure; solid block masonry (6”/8” external, 6”/4” internal).</p>
    </li>
    
    <li>
        <span>FLOORING</span><br>
        <p>Vitrified tiles in living, dining, kitchen & bedrooms; antiskid ceramic tiles in toilets, balconies & utility.</p>
    </li>
    <li>
        <span>PAINTING</span><br>
        <p>All internal walls with premium emulsion paint. Exterior fascia with premium paint.</p>
    </li>
    <li>
        <span>DOORS & WINDOWS</span><br>
        <p>Main door: teak frame with veneer-finished shutter; internal doors: hardwood frame + flush shutters; UPVC 2.5-track sliders with mosquito mesh & toughened/­translucent glass.</p>
    </li>
    <li>
        <span>KITCHEN</span><br>
        <p>Polished granite platform, stainless-steel sink, designer 2-ft tile dado, provision for water purifier & hob/­chimney.</p>
    </li>
    <li>
        <span>PLUMBING & SANITARY</span><br>
        <p>Jaquar/Grohe (or equivalent) CP fittings, wall-mounted EWCs, suspended pipelines concealed above false ceilings; dual plumbing for recycled-water flushing.</p>
    </li>
    <li>
        <span>ELECTRICAL & COMMUNICATION</span><br>
        <p>Fire-resistant PVC-insulated copper wiring; Havells/Anchor/V-Guard switches; FTTH connectivity; individual BESCOM meters; DG back-up for common areas + 0.5 kVA/flat.</p>
    </li>
    <li>
        <span>PAINT & FINISHES</span><br>
        <p>Premium emulsion on internal walls; weather-proof exterior coatings.</p>
    </li>
    <li>
        <span>SAFETY & SECURITY</span><br>
        <p>CCTV surveillance, round-the-clock security, MS railings on balconies & terraces, compliant firefighting system.</p>
    </li>
    <li>
        <span>LIFTS</span><br>
        <p>Two high-speed passenger lifts of adequate capacity in every block.</p>
    </li>
</ul>


</div>
</div>


                    <!--<div class="space50 d-lg-block d-none"></div>-->
                    <!--<div class="space30 d-lg-none d-block"></div>-->
<div class="single-item mb-30 d-none">
<div class="kye-features">

<h3>Area Statements</h3>
<div class="space24"></div>
<table class="table table-striped border rounded">
  <thead class="table-dark">
    <tr>
      <th scope="col">F.No.</th>
      <th scope="col">Area</th>
      <th scope="col">Type</th>
      <th scope="col">Facing</th>
    </tr>
  </thead>
  <tbody>
    <tr>
    <th scope="row">1</th>
    <td>1904</td>
    <td>3BHK</td>
    <td>West</td>
</tr>
<tr>
    <th scope="row">2</th>
    <td>1803</td>
    <td>3BHK</td>
    <td>West</td>
</tr>
<tr>
    <th scope="row">3</th>
    <td>1698</td>
    <td>3BHK</td>
    <td>North</td>
</tr>
<tr>
    <th scope="row">4</th>
    <td>1698</td>
    <td>3BHK</td>
    <td>North</td>
</tr>
<tr>
    <th scope="row">5</th>
    <td>1698</td>
    <td>3BHK</td>
    <td>North</td>
</tr>
<tr>
    <th scope="row">6</th>
    <td>1698</td>
    <td>3BHK</td>
    <td>North</td>
</tr>
<tr>
    <th scope="row">7</th>
    <td>1698</td>
    <td>3BHK</td>
    <td>North</td>
</tr>
<tr>
    <th scope="row">8</th>
    <td>1790</td>
    <td>3BHK</td>
    <td>East</td>
</tr>
<tr>
    <th scope="row">9</th>
    <td>1225</td>
    <td>2BHK</td>
    <td>East</td>
</tr>
<tr>
    <th scope="row">10</th>
    <td>1220</td>
    <td>2BHK</td>
    <td>East</td>
</tr>
<tr>
    <th scope="row">11</th>
    <td>1246</td>
    <td>2BHK</td>
    <td>East</td>
</tr>
<tr>
    <th scope="row">12</th>
    <td>1328</td>
    <td>2BHK</td>
    <td>East</td>
</tr>

  </tbody>
</table>
</div>
</div>

                    <div class="space50 d-lg-block d-none"></div>
                    <div class="space30 d-lg-none d-block"></div>
                    
<div class="single-item mb-50">
    <h3>Amenities</h3>
    <div class="space24"></div>
    <div class="poperty-feature-wrap">
        <ul class="poperty-feature">
            <li>
                <div class="icon">
                    <svg xmlns="http://www.w3.org/2000/svg" width="24px" height="24px" viewBox="0 0 24 24"><path fill="#737373" d="M21 10h-4V8l-4.5-1.8V4H15V2h-3.5v4.2L7 8v2H3c-.55 0-1 .45-1 1v11h8v-5h4v5h8V11c0-.55-.45-1-1-1M8 20H4v-3h4zm0-5H4v-3h4zm4-7c.55 0 1 .45 1 1s-.45 1-1 1s-1-.45-1-1s.45-1 1-1m2 7h-4v-3h4zm6 5h-4v-3h4zm0-5h-4v-3h4z"></path></svg>
                </div>
                <div class="content">
                    <h6>Sports Courts</h6>
                    <!--<span>RCC framed structure</span>-->
                </div>
            </li>
            <li>
                <div class="icon">
                    <svg xmlns="http://www.w3.org/2000/svg" width="24px" height="24px" viewBox="0 0 24 24"><path fill="#737373" d="m20.71 16.1l-1.64-1.64a.94.94 0 0 1-.22-1.07a5.8 5.8 0 0 0 .54-2.39a.4.4 0 0 0 0-.1a5.74 5.74 0 0 0-1.06-3.34a14.2 14.2 0 0 0-5.17-4.42a7 7 0 0 0-8 1.31l-.67.67a7 7 0 0 0-1.31 8.05l.1.17a3 3 0 0 0-.84 2.06A3 3 0 0 0 7 17.94c.18.14.34.29.52.42a5.6 5.6 0 0 0 1.22.64h.09c.18.07.37.13.57.19h.15a5 5 0 0 0 .95.15h.62c.21 0 .41 0 .62-.06h.17a5.5 5.5 0 0 0 1.42-.45a1 1 0 0 1 1.07.21l1.46 1.46a3.4 3.4 0 0 0 2.39 1a2.85 2.85 0 0 0 2-.85l.38-.38a3 3 0 0 0 .08-4.17m-15.3.32a1 1 0 1 1 1-1a1 1 0 0 1-1 1m3.1.14l-.26-.2a3 3 0 0 0 .16-.94a3 3 0 0 0-3-3h-.38l-.09-.16a5 5 0 0 1 .93-5.75l.67-.67A5 5 0 0 1 12.28 5a12 12 0 0 1 4.26 3.57Zm10.78 2.37l-.37.38c-.42.42-1.07.34-1.61-.2l-1.46-1.45a3 3 0 0 0-3.34-.61a3.4 3.4 0 0 1-1 .31a3 3 0 0 1-.58.05h-.44l6.87-6.87a3.8 3.8 0 0 1-.34 2a3 3 0 0 0 .61 3.34l1.64 1.65a1 1 0 0 1 .02 1.4"></path></svg>
                </div>
                <div class="content">
                    <h6>Tennis & Volleyball</h6>
                    <!--<span>Superior quality vitrified tiles for kitchen, Dining, Living Area, Bedrooms and Foyer. Anti-skid ceramic/vitrified tiles for toilets, utility & Balcony</span>-->
                </div>
            </li>
            <li>
                <div class="icon">
                    <img src="https://demo-egenslab.b-cdn.net/html/neckle/preview/assets/img/inner-page/icon/bath.svg"
                        alt>
                </div>
                <div class="content">
                    <h6>Resort Pools</h6>
                </div>
            </li>
            <li>
                <div class="icon">
                    <img src="https://demo-egenslab.b-cdn.net/html/neckle/preview/assets/img/inner-page/icon/bed.svg"
                        alt>
                </div>
                <div class="content">
                    <h6>Play & Gazebo</h6>
                </div>
            </li>
            <li>
                <div class="icon">
                    <svg xmlns="http://www.w3.org/2000/svg" width="24px" height="24px" viewBox="0 0 24 24"><path fill="#737373" d="M23 4.1V2.3l-1.8-.2c-.1 0-.7-.1-1.7-.1c-4.1 0-7.1 1.2-8.8 3.3C9.4 4.5 7.6 4 5.5 4c-1 0-1.7.1-1.7.1l-1.9.3l.1 1.7c.1 3 1.6 8.7 6.8 8.7H9v3.4c-3.8.5-7 1.8-7 1.8v2h20v-2s-3.2-1.3-7-1.8V15c6.3-.1 8-7.2 8-10.9M12 18h-1v-5.6S10.8 9 8 9c0 0 1.5.8 1.9 3.7c-.4.1-.8.1-1.1.1C4.2 12.8 4 6.1 4 6.1S4.6 6 5.5 6c1.9 0 5 .4 5.9 3.1C11.9 4.6 17 4 19.5 4c.9 0 1.5.1 1.5.1s0 9-6.3 9H14c0-2 2-5 2-5c-3 1-3 4.9-3 4.9v5z"></path></svg>
                </div>
                <div class="content">
                    <h6>Amphitheatre Gardens</h6>
                </div>
            </li>
            <li>
                <div class="icon">
                    <svg xmlns="http://www.w3.org/2000/svg" width="24px" height="24px" viewBox="0 0 24 24"><path fill="#737373" d="m20.274 9.869l-3.442-4.915l1.639-1.147l3.441 4.915zm-1.884 2.54L16.67 9.95l-8.192 5.736l1.72 2.457l-1.638 1.148l-4.588-6.554L5.61 11.59l1.72 2.458l8.192-5.736l-1.72-2.458l1.638-1.147l4.588 6.554zm2.375-5.326l1.638-1.147l-1.147-1.638l-1.638 1.147zM7.168 19.046l-3.442-4.915l-1.638 1.147l3.441 4.915zm-2.786-.491l-1.638 1.147l-1.147-1.638l1.638-1.147z"/></svg>
                </div>
                <div class="content">
                    <h6>Gym & Deck</h6>
                </div>
            </li>
            <li>
                <div class="icon">
                    <svg xmlns="http://www.w3.org/2000/svg" width="24px" height="24px" viewBox="0 0 24 24"><path fill="#737373" d="M19.222 7.278V5.75a.417.417 0 0 0-.833 0v1.528h-1.667V5.75a.417.417 0 0 0-.833 0v1.528h-.833v.833A2.5 2.5 0 0 0 17 10.547v3.12l.831 6.132a1.11 1.11 0 0 1-2.22-.02V12a1.67 1.67 0 0 0-1.667-1.667h-1.666V3.62A1.646 1.646 0 0 0 10.611 2h-5a1.646 1.646 0 0 0-1.667 1.62V22h8.334V11.444h1.666A.557.557 0 0 1 14.5 12v7.777a2.222 2.222 0 1 0 4.444 0l-.833-6.149v-3.081a2.5 2.5 0 0 0 1.945-2.436v-.833ZM6.049 16.66l1.2-2.393H5.75l2.094-3.59h1.5l-1.5 2.393h1.795zm5.118-8.78a.535.535 0 0 1-.556.509h-5a.535.535 0 0 1-.555-.509V3.62a.535.535 0 0 1 .555-.509h5a.535.535 0 0 1 .556.509z"/></svg>
                </div>
                <div class="content">
                    <h6>EV Parking</h6>
                </div>
            </li>
            <li>
                <div class="icon">
                    <img src="https://demo-egenslab.b-cdn.net/html/neckle/preview/assets/img/inner-page/icon/garage.svg"
                        alt>
                </div>
                <div class="content">
                    <h6>Eco Utilities</h6>
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
        <img class="w-100" style="object-fit:cover" src="assets/img/all-images/anahata-img6.png" alt="">
    </div>
    <div class="img1 image-anime reveal pb-3">
        <img class="w-100" style="object-fit:cover" src="assets/img/all-images/anahata-img7.png" alt="">
    </div>
    <div class="img1 image-anime reveal pb-3">
        <img class="w-100" style="object-fit:cover" src="assets/img/all-images/anahata-img8.png" alt="">
    </div>
    <div class="button-group">
        <a class="primary-btn3" href="gallery.php?anahata=true">
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
                <div class="space50 d-lg-block d-none"></div>
                <div class="space30 d-lg-none d-block"></div>
                
                

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
<!--<img class="rounded border" src="assets/img/all-images/floor-plan-img3.png" />-->
<!--</div>-->
<!--</div>-->
                    <div class="space50 d-lg-block d-none"></div>
                    <div class="space30 d-lg-none d-block"></div>
<div class="single-item mb-50">

<h3>Project Location</h3>
<div class="space24"></div>
<div class="poperty-video-area">
<img class="rounded border" src="assets/img/all-images/map-img7.png" />
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
What types of residences are offered at Anahata?
</button>
</h5>
<div id="flush-collapseOne" class="accordion-collapse collapse show" aria-labelledby="flush-headingOne" data-bs-parent="#accordionFlushExample">
<div class="accordion-body">Spacious 2 & 3 BHK apartments with multiple layout options to suit nuclear and joint families.</div>
</div>
</div>
<div class="accordion-item">
<h5 class="accordion-header" id="flush-headingTwo">
<button class="accordion-button collapsed" type="button" data-bs-toggle="collapse" data-bs-target="#flush-collapseTwo" aria-expanded="false" aria-controls="flush-collapseTwo">
What makes Anahata’s location special?
</button>
</h5>
<div id="flush-collapseTwo" class="accordion-collapse collapse" aria-labelledby="flush-headingTwo" data-bs-parent="#accordionFlushExample">
<div class="accordion-body">The project sits right off Soukya Road, giving quick access to Whitefield’s IT parks, Nexus & Park Square malls, and top hospitals such as Manipal.</div>
</div>
</div>
<div class="accordion-item">
<h5 class="accordion-header" id="flush-headingThree">
<button class="accordion-button collapsed" type="button" data-bs-toggle="collapse" data-bs-target="#flush-collapseThree" aria-expanded="false" aria-controls="flush-collapseThree">
How well is Anahata connected to the rest of Bengaluru?
</button>
</h5>
<div id="flush-collapseThree" class="accordion-collapse collapse" aria-labelledby="flush-headingThree" data-bs-parent="#accordionFlushExample">
<div class="accordion-body">Whitefield Railway Station, the upcoming Metro Phase-II line, and the ORR ensure smooth commutes to the CBD and airport.</div>
</div>
</div>
<div class="accordion-item">
<h5 class="accordion-header" id="flush-headingFour">
<button class="accordion-button collapsed" type="button" data-bs-toggle="collapse" data-bs-target="#flush-collapseFour" aria-expanded="false" aria-controls="flush-collapseFour">
Is the community vaastu-compliant?
</button>
</h5>
<div id="flush-collapseFour" class="accordion-collapse collapse" aria-labelledby="flush-headingFour" data-bs-parent="#accordionFlushExample">
<div class="accordion-body">Yes—orientation, ventilation and common-area planning adhere to traditional vaastu principles for harmony and wellbeing.</div>
</div>
</div>
<div class="accordion-item">
<h5 class="accordion-header" id="flush-headingFive">
<button class="accordion-button collapsed" type="button" data-bs-toggle="collapse" data-bs-target="#flush-collapseFive" aria-expanded="false" aria-controls="flush-collapseFive">
What lifestyle can residents expect?
</button>
</h5>
<div id="flush-collapseFive" class="accordion-collapse collapse" aria-labelledby="flush-headingFive" data-bs-parent="#accordionFlushExample">
<div class="accordion-body">Resort-inspired sports courts, pools and landscaped greens foster an active yet tranquil lifestyle, backed by high-grade security and maintenance services. </div>
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
                <img src="assets/img/all-images/anahata-img2.png" alt="">
              </div>
              
            </div>
  
            <div class="auhtor-boxes-area">
              <div class="img1">
                <img src="assets/img/all-images/anahata-img3.png" alt="">
              </div>
              
            </div>
  
            <div class="auhtor-boxes-area">
              <div class="img1">
                <img src="assets/img/all-images/anahata-img4.png" alt="">
              </div>
              
            </div>
  
            <div class="auhtor-boxes-area">
              <div class="img1">
                <img src="assets/img/all-images/anahata-img5.png" alt="">
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
                <img src="assets/img/all-images/anahata-img2.png" alt="">
              </div>
              <div class="icons">
                <a class="popup-img" href="assets/img/all-images/anahata-img2.png"><i class="fa-brands fa-plus"></i></a>
              </div>
            </div>
  
            <div class="auhtor-boxes-area">
              <div class="img1">
                <img src="assets/img/all-images/anahata-img3.png" alt="">
              </div>
              <div class="icons">
                <a class="popup-img" href="assets/img/all-images/anahata-img3.png"><i class="fa-brands fa-plus"></i></a>
              </div>
            </div>
  
            <div class="auhtor-boxes-area">
              <div class="img1">
                <img src="assets/img/all-images/anahata-img4.png" alt="">
              </div>
              <div class="icons">
                <a class="popup-img" href="assets/img/all-images/anahata-img4.png"><i class="fa-brands fa-plus"></i></a>
              </div>
            </div>
  
            <div class="auhtor-boxes-area">
              <div class="img1">
                <img src="assets/img/all-images/anahata-img5.png" alt="">
              </div>
              <div class="icons">
                <a class="popup-img" href="assets/img/all-images/anahata-img5.png"><i class="fa-brands fa-plus"></i></a>
              </div>
            </div>
  
            <div class="auhtor-boxes-area">
              <div class="img1">
                <img src="assets/img/all-images/anahata-img6.png" alt="">
              </div>
              <div class="icons">
                <a class="popup-img" href="assets/img/all-images/anahata-img6.png"><i class="fa-brands fa-plus"></i></a>
              </div>
            </div>
            
          </div>
        </div>
      </div>
    </div>
  </div>
  <!--===== OTHERS AREA ENDS =======-->

<div class="property-inner-area sp2">
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
              <img src="assets/img/all-images/features-img1.png" alt="">
            </div>
            <!--<div class="sell-point">-->
            <!--  <a href="#" class="sell">Sell</a>-->
            <!--  <a href="#" class="featured">Featured</a>-->
            <!--</div>-->
            <div class="content-area">
              <a href="krishna.php">Krishna</a>
              <p>This 2.25 lakh Sqft residential project in Hospet spans 1.25 acres, offering 119 luxury flats with proximity to key landmarks, ideal for families.</p>
             
              <div class="price-area">
                  <div class="btn-area1 w-100 text-center">
                    <a href="krishna.php" class="header-btn1 w-100">View Project</a>
                  </div>
                </div>
            </div>
          </div>

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
              <a href="anahata.php">Anahata</a>
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

