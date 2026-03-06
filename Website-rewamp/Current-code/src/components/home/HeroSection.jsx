import React, { useState, useRef, useEffect, useCallback } from 'react';
import { Volume2, VolumeX } from 'lucide-react';

export default function HeroSection() {
  const [isMuted, setIsMuted] = useState(true);
  const playerRef = useRef(null);
  const iframeRef = useRef(null);

  useEffect(() => {
    // Load YouTube IFrame API
    if (!window.YT) {
      const tag = document.createElement('script');
      tag.src = 'https://www.youtube.com/iframe_api';
      const firstScriptTag = document.getElementsByTagName('script')[0];
      firstScriptTag.parentNode.insertBefore(tag, firstScriptTag);
    }

    const initPlayer = () => {
      if (window.YT && window.YT.Player) {
        playerRef.current = new window.YT.Player(iframeRef.current, {
          events: {
            onReady: (event) => {
              event.target.mute();
              event.target.playVideo();
            },
          },
        });
      }
    };

    if (window.YT && window.YT.Player) {
      initPlayer();
    } else {
      window.onYouTubeIframeAPIReady = initPlayer;
    }
  }, []);

  const toggleMute = useCallback(() => {
    if (playerRef.current) {
      if (isMuted) {
        playerRef.current.unMute();
      } else {
        playerRef.current.mute();
      }
      setIsMuted(!isMuted);
    }
  }, [isMuted]);

  return (
    <section className="relative h-[calc(56.25vw+100px)] md:min-h-screen bg-white overflow-hidden">
      {/* Video Background */}
      <div className="absolute inset-0 w-full h-full overflow-hidden hero-video-wrapper">
        <iframe
          ref={iframeRef}
          className="hero-video-iframe"
          id="hero-yt-player"
          src="https://www.youtube.com/embed/_UE-muzzbz4?autoplay=1&mute=1&loop=1&playlist=_UE-muzzbz4&controls=0&showinfo=0&modestbranding=1&rel=0&enablejsapi=1&origin=https://www.ishtikahomes.com"
          title="Ishtika Homes Video"
          allow="autoplay; encrypted-media"
          style={{
            pointerEvents: 'none',
            border: 'none',
          }}
        />
        <div className="absolute inset-0 bg-black/40" />
      </div>

      {/* Mute/Unmute Button - Top Left, below navbar on mobile */}
      <button
        onClick={toggleMute}
        className="absolute top-[120px] md:top-6 left-4 md:left-6 z-20 w-10 h-10 md:w-12 md:h-12 rounded-full bg-white/20 backdrop-blur-sm border border-white/30 flex items-center justify-center text-white hover:bg-white/30 transition-all duration-300"
        aria-label={isMuted ? "Unmute video" : "Mute video"}
      >
        {isMuted ? <VolumeX className="w-4 h-4 md:w-5 md:h-5" /> : <Volume2 className="w-4 h-4 md:w-5 md:h-5" />}
      </button>
    </section>
  );
}
