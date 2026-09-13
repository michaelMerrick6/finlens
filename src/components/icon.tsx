import type { CSSProperties } from "react";
export function Icon({
  name,
  size = 20,
  style,
}: {
  name:
    | "arrow"
    | "search"
    | "bell"
    | "plus"
    | "check"
    | "close"
    | "external"
    | "chevron"
    | "grid"
    | "people"
    | "bookmark";
  size?: number;
  style?: CSSProperties;
}) {
  const paths = {
    arrow: "M4 12h16m-6-6 6 6-6 6",
    search: "m16 16 5 5M10.5 3a7.5 7.5 0 1 0 0 15 7.5 7.5 0 0 0 0-15",
    bell: "M18 8a6 6 0 0 0-12 0c0 7-3 7-3 9h18c0-2-3-2-3-9M10 21h4",
    plus: "M12 5v14M5 12h14",
    check: "m5 12 4 4L19 6",
    close: "m6 6 12 12M6 18 18 6",
    external: "M14 3h7v7m0-7L10 14M10 3H4v17h17v-6",
    chevron: "m9 5 7 7-7 7",
    grid: "M3 3h7v7H3zM14 3h7v7h-7zM3 14h7v7H3zM14 14h7v7h-7z",
    people:
      "M8 3a4 4 0 1 0 0 8 4 4 0 0 0 0-8M1 21v-3a6 6 0 0 1 12 0v3m3-17a4 4 0 0 1 0 8m1 3a6 6 0 0 1 6 6",
    bookmark: "M6 3h12v18l-6-4-6 4z",
  };
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.6"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
      style={style}
    >
      <path d={paths[name]} />
    </svg>
  );
}
