export type Disclosure = {
  asset_type?: string | null;
  activity_note?: string;
  activity_label?: string;
  is_contribution?: boolean;
  id: string;
  member_id: string | null;
  politician_name: string | null;
  ticker: string | null;
  transaction_type: string | null;
  amount_range: string | null;
  published_date: string | null;
  transaction_date: string | null;
  source_url: string | null;
  asset_name?: string | null;
  chamber?: string | null;
  party?: string | null;
  congress_members?: {
    first_name?: string | null;
    last_name?: string | null;
    party?: string | null;
    chamber?: string | null;
    state?: string | null;
  } | null;
};
export function dateLabel(value?: string | null) {
  if (!value) return "Not available";
  const date = new Date(value.slice(0, 10) + "T12:00:00Z");
  return Number.isNaN(date.getTime())
    ? "Not available"
    : new Intl.DateTimeFormat("en-US", {
        month: "short",
        day: "numeric",
        year: "numeric",
        timeZone: "UTC",
      }).format(date);
}
export function initials(name: string) {
  return name
    .split(/\s+/)
    .filter(Boolean)
    .map((s) => s[0])
    .filter(Boolean)
    .slice(0, 2)
    .join("");
}
export function partyLabel(party?: string | null) {
  return (
    (
      { D: "Democrat", R: "Republican", I: "Independent" } as Record<
        string,
        string
      >
    )[party || ""] ||
    party ||
    "Party not listed"
  );
}
export function directionLabel(direction?: string | null) {
  return direction === "buy"
    ? "Purchase"
    : direction === "sell"
      ? "Sale"
      : direction === "exchange"
        ? "Exchange"
        : "Other";
}
export function sourceLink(value?: string | null) {
  try {
    const url = new URL(value || "");
    return url.protocol === "https:" || url.protocol === "http:"
      ? url.href
      : null;
  } catch {
    return null;
  }
}
