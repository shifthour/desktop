"use client"

import { useState, useRef } from "react"
import { X, Maximize2, Minimize2, Volume2, VolumeX } from "lucide-react"
import { getImagePath } from "@/lib/utils-path"

export function FloatingVideoPlayer() {
  const [isMinimized, setIsMinimized] = useState(false)
  const [isFullscreen, setIsFullscreen] = useState(false)
  const [isMuted, setIsMuted] = useState(false)
  const [isVisible, setIsVisible] = useState(true)
  const videoRef = useRef<HTMLVideoElement>(null)
  const containerRef = useRef<HTMLDivElement>(null)

  const toggleFullscreen = () => {
    if (!containerRef.current) return

    if (!isFullscreen) {
      if (containerRef.current.requestFullscreen) {
        containerRef.current.requestFullscreen()
      }
      setIsFullscreen(true)
    } else {
      if (document.exitFullscreen) {
        document.exitFullscreen()
      }
      setIsFullscreen(false)
    }
  }

  const toggleMute = () => {
    if (videoRef.current) {
      videoRef.current.muted = !isMuted
      setIsMuted(!isMuted)
    }
  }

  if (!isVisible) return null

  return (
    <div
      ref={containerRef}
      className={`fixed z-50 transition-all duration-300 ${
        isMinimized
          ? "bottom-4 right-4 w-16 h-16"
          : isFullscreen
          ? "inset-0 w-full h-full"
          : "bottom-4 right-4 w-80 md:w-96 shadow-2xl"
      }`}
      style={{
        maxWidth: isFullscreen ? "100%" : isMinimized ? "64px" : "384px",
      }}
    >
      <div className="relative w-full h-full bg-black rounded-lg overflow-hidden">
        {/* Video */}
        <video
          ref={videoRef}
          className="w-full h-full object-cover"
          autoPlay
          loop
          muted={isMuted}
          playsInline
          src={getImagePath("/project-video.mp4")}
        />

        {/* Controls Overlay */}
        {!isMinimized && (
          <div className="absolute inset-0 bg-gradient-to-t from-black/60 via-transparent to-black/40 opacity-0 hover:opacity-100 transition-opacity duration-300">
            <div className="absolute top-2 right-2 flex gap-2">
              {/* Mute/Unmute Button */}
              <button
                onClick={toggleMute}
                className="p-2 bg-white/10 backdrop-blur-sm rounded-full hover:bg-white/20 transition-colors"
                aria-label={isMuted ? "Unmute" : "Mute"}
              >
                {isMuted ? (
                  <VolumeX className="w-4 h-4 text-white" />
                ) : (
                  <Volume2 className="w-4 h-4 text-white" />
                )}
              </button>

              {/* Minimize Button */}
              <button
                onClick={() => setIsMinimized(true)}
                className="p-2 bg-white/10 backdrop-blur-sm rounded-full hover:bg-white/20 transition-colors"
                aria-label="Minimize"
              >
                <Minimize2 className="w-4 h-4 text-white" />
              </button>

              {/* Fullscreen Button */}
              <button
                onClick={toggleFullscreen}
                className="p-2 bg-white/10 backdrop-blur-sm rounded-full hover:bg-white/20 transition-colors"
                aria-label="Fullscreen"
              >
                <Maximize2 className="w-4 h-4 text-white" />
              </button>

              {/* Close Button */}
              <button
                onClick={() => setIsVisible(false)}
                className="p-2 bg-white/10 backdrop-blur-sm rounded-full hover:bg-white/20 transition-colors"
                aria-label="Close"
              >
                <X className="w-4 h-4 text-white" />
              </button>
            </div>

            {/* Title */}
            <div className="absolute bottom-2 left-2 right-2">
              <p className="text-white text-sm font-medium drop-shadow-lg">
                Anahata Project Walkthrough
              </p>
            </div>
          </div>
        )}

        {/* Minimized State - Click to Restore */}
        {isMinimized && (
          <button
            onClick={() => setIsMinimized(false)}
            className="absolute inset-0 w-full h-full flex items-center justify-center bg-gradient-to-br from-[#ef8206] to-[#c96b05] hover:from-[#ff9017] hover:to-[#da7c16] transition-all"
            aria-label="Restore video"
          >
            <Maximize2 className="w-8 h-8 text-white" />
          </button>
        )}
      </div>
    </div>
  )
}
