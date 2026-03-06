'use client'

import { useEffect, useRef, useState } from 'react'

export default function Hero() {
  const videoRef = useRef<HTMLVideoElement>(null)
  const [isMuted, setIsMuted] = useState(false)

  useEffect(() => {
    const video = videoRef.current
    if (!video) return

    // Try to play with sound
    video.muted = false
    video.volume = 1.0

    const playPromise = video.play()
    if (playPromise !== undefined) {
      playPromise.catch(() => {
        // Autoplay with sound prevented, enable on first click
        const enableAudio = () => {
          video.muted = false
          video.volume = 1.0
          video.play()
          setIsMuted(false)
          document.removeEventListener('click', enableAudio)
        }
        document.addEventListener('click', enableAudio, { once: true })
      })
    }
  }, [])

  const toggleMute = () => {
    if (videoRef.current) {
      videoRef.current.muted = !videoRef.current.muted
      setIsMuted(videoRef.current.muted)
    }
  }

  return (
    <section id="home" className="relative h-screen flex items-center justify-center overflow-hidden">
      {/* Background Video */}
      <div className="absolute inset-0">
        <video
          ref={videoRef}
          autoPlay
          loop
          playsInline
          className="w-full h-full object-cover"
        >
          <source src="/images/hero-video.mp4" type="video/mp4" />
          Your browser does not support the video tag.
        </video>
        <div className="absolute inset-0" style={{ background: 'rgba(0, 0, 0, 0.2)' }}></div>
      </div>

      {/* Mute/Unmute Button */}
      <button
        onClick={toggleMute}
        className="absolute top-24 right-6 md:top-28 md:right-10 bg-white/20 backdrop-blur-sm hover:bg-white/30 text-white rounded-full p-3 md:p-4 transition z-20 shadow-lg"
      >
        <i className={`fas ${isMuted ? 'fa-volume-mute' : 'fa-volume-up'} text-xl md:text-2xl`}></i>
      </button>

      {/* Scroll Indicator */}
      <div className="absolute bottom-10 left-1/2 transform -translate-x-1/2 text-white animate-bounce z-10">
        <i className="fas fa-chevron-down text-2xl"></i>
      </div>
    </section>
  )
}
