"use client";

import Image from "next/image";
import { useState } from "react";
import { getPoliticianPhotoUrl } from "@/lib/politician-photos";
import { getTickerLogoUrl } from "@/lib/company-logos";
import { initials } from "@/lib/ui-format";

function IdentityImage({ src, fallback, className, size }: {
  src: string | null;
  fallback: string;
  className: string;
  size: number;
}) {
  const [failedUrl, setFailedUrl] = useState<string | null>(null);
  return (
    <span className={className} aria-hidden="true">
      {src && src !== failedUrl ? (
        <Image src={src} alt="" width={size} height={size} unoptimized
          onError={() => setFailedUrl(src)} />
      ) : fallback}
    </span>
  );
}

export function Avatar({ name, memberId, src, large = false }: {
  name: string;
  memberId?: string | null;
  src?: string;
  large?: boolean;
}) {
  return <IdentityImage src={src || getPoliticianPhotoUrl(memberId, "225x275", name)}
    fallback={initials(name)} className={`avatar ${large ? "large" : ""}`} size={large ? 96 : 48} />;
}

export function CompanyLogo({ ticker, large = false }: { ticker?: string | null; large?: boolean }) {
  return <IdentityImage src={ticker ? getTickerLogoUrl(ticker, large ? 96 : 64) : null}
    fallback={ticker?.slice(0, 2) || "—"} className={`company-logo ${large ? "large" : ""}`} size={large ? 56 : 28} />;
}
