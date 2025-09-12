// src/components/Layout.tsx
import Header from './Header'
import Footer from './Footer'

export default function Layout({ children }: { children: React.ReactNode }) {
  return (
    <div className="min-h-dvh flex flex-col">
      <Header />
      <main className="flex-1 container mx-auto px-4 py-10">{children}</main>
      <Footer />
    </div>
  )
}
