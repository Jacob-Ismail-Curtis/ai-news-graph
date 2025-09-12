// src/components/Footer.tsx

export default function Footer() {
  return (
    <footer className="border-t border-black/5 dark:border-white/10">
      <div className="container mx-auto px-4 py-8 flex flex-col sm:flex-row items-center justify-between gap-4 text-sm text-muted">
        
        {/* Left: copyright */}
        <p className="order-2 sm:order-1">
          © {new Date().getFullYear()} Jacob Curtis
        </p>
      </div>
    </footer>
  )
}
