export default function Loading() {
  return (
    <main id="main" className="container loading-page" aria-busy="true">
      <span className="eyebrow">GETTING THE PUBLIC RECORD</span>
      <div className="skeleton-title" />
      <div className="skeleton-rows">
        {[0, 1, 2, 3].map((n) => (
          <div key={n} />
        ))}
      </div>
      <span className="sr-only" role="status">
        Loading page…
      </span>
    </main>
  );
}
