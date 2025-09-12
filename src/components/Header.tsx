// src/components/Header.tsx
import { Link, NavLink } from 'react-router-dom'
export default function Header() {
  return (
    <>
      <a
        href="#main"
        className="sr-only focus:not-sr-only absolute left-2 top-2 z-[100]
                   rounded-md bg-black text-white px-3 py-1 text-sm
                   focus:outline-none focus:ring-2 focus:ring-brand/40"
      >
        Skip to content
      </a>

      <header className="sticky top-0 z-40 backdrop-blur bg-white/70 dark:bg-slate-900/80 border-b border-black/5 dark:border-white/10">
        <div className="container mx-auto px-4 h-14 flex items-center justify-between">
          {/* Brand */}
          <Link to="/" className="flex items-center gap-2 font-bold tracking-tight">
            <span className="inline-flex h-7 w-7 items-center justify-center rounded-lg bg-brand text-white text-xs shadow-soft">JC</span>
            <span className="hidden sm:inline">Jacob Curtis</span>
          </Link>

          {/* Tabs */}
          <nav className="flex items-center gap-2">
            <Tab to="/">Blog</Tab>
            <Tab to="/career">Career</Tab>
          </nav>
        </div>
      </header>
    </>
  )
}

function Tab({ to, children }: { to: string; children: React.ReactNode }) {
  return (
    <NavLink
      to={to}
      end
      className={({ isActive }) =>
        [
          "inline-flex items-center h-9 rounded-full px-3 text-sm transition",
          "border border-black/10 dark:border-white/10",
          "hover:bg-black/5 dark:hover:bg-white/5 focus:outline-none focus:ring-2 focus:ring-brand/40",
          isActive ? "bg-black/5 dark:bg-white/5 font-semibold" : "text-slate-700 dark:text-slate-200"
        ].join(" ")
      }
    >
      {children}
    </NavLink>
  )
}
