"use client";
export default function ErrorPage({ reset }: { reset: () => void }) {
  return (
    <main id="main" className="container empty-page">
      <span className="eyebrow">A MOMENTARY INTERRUPTION</span>
      <h1>This page couldn’t load.</h1>
      <p>Your data hasn’t been changed. Please try again.</p>
      <button className="button primary" onClick={reset}>
        Try again
      </button>
    </main>
  );
}
