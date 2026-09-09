import Link from 'next/link';

export default function NotFound() {
  return (
    <main>
      <h1>This page is unavailable</h1>
      <p>Vail is being rebuilt.</p>
      <Link href="/">Return home</Link>
    </main>
  );
}
