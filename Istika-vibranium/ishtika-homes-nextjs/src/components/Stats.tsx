'use client'

import { useEffect, useRef, useState } from 'react'

export default function Stats() {
  const [isVisible, setIsVisible] = useState(false)
  const statsRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const observer = new IntersectionObserver(
      (entries) => {
        if (entries[0].isIntersecting) {
          setIsVisible(true)
        }
      },
      { threshold: 0.5 }
    )

    if (statsRef.current) {
      observer.observe(statsRef.current)
    }

    return () => {
      if (statsRef.current) {
        observer.unobserve(statsRef.current)
      }
    }
  }, [])

  return (
    <section className="py-6 md:py-8 bg-white" ref={statsRef}>
      <div className="container mx-auto px-4">
        <div className="grid grid-cols-2 md:grid-cols-4 gap-6 md:gap-8 text-center">
          <div
            className={`transition-all duration-800 ${
              isVisible ? 'opacity-100 translate-x-0' : 'opacity-0 translate-x-10'
            }`}
            style={{ transitionDelay: '0ms' }}
          >
            <Counter target={12} isVisible={isVisible} />
            <p className="text-base md:text-lg font-medium mt-1 text-gray-700">Projects Delivered</p>
          </div>

          <div
            className={`transition-all duration-800 ${
              isVisible ? 'opacity-100 translate-x-0' : 'opacity-0 translate-x-10'
            }`}
            style={{ transitionDelay: '100ms' }}
          >
            <Counter target={5} isVisible={isVisible} />
            <p className="text-base md:text-lg font-medium mt-1 text-gray-700">Ongoing Projects</p>
          </div>

          <div
            className={`transition-all duration-800 ${
              isVisible ? 'opacity-100 translate-x-0' : 'opacity-0 translate-x-10'
            }`}
            style={{ transitionDelay: '200ms' }}
          >
            <div className="counter">2000+</div>
            <p className="text-base md:text-lg font-medium mt-1 text-gray-700">Happy Families</p>
          </div>

          <div
            className={`transition-all duration-800 ${
              isVisible ? 'opacity-100 translate-x-0' : 'opacity-0 translate-x-10'
            }`}
            style={{ transitionDelay: '300ms' }}
          >
            <Counter target={12} isVisible={isVisible} />
            <p className="text-base md:text-lg font-medium mt-1 text-gray-700">Years of Experience</p>
          </div>
        </div>
      </div>
    </section>
  )
}

function Counter({ target, isVisible }: { target: number; isVisible: boolean }) {
  const [count, setCount] = useState(0)

  useEffect(() => {
    if (!isVisible) return

    const duration = 2000 // 2 seconds
    const steps = 60
    const increment = target / steps
    const stepDuration = duration / steps

    let current = 0
    const timer = setInterval(() => {
      current += increment
      if (current >= target) {
        setCount(target)
        clearInterval(timer)
      } else {
        setCount(Math.ceil(current))
      }
    }, stepDuration)

    return () => clearInterval(timer)
  }, [isVisible, target])

  return <div className="counter">{count}</div>
}
