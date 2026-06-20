'use client';

import Image, { type ImageLoaderProps } from 'next/image';
import { useState } from 'react';

import { getPartyPresentation } from '@/lib/politics';
import { getPoliticianPhotoUrl } from '@/lib/politician-photos';

const passthroughImageLoader = ({ src }: ImageLoaderProps) => src;

function initialsForName(name: string) {
  const tokens = name.trim().split(/\s+/).filter(Boolean);
  if (tokens.length === 0) {
    return '?';
  }
  if (tokens.length === 1) {
    return tokens[0].slice(0, 2).toUpperCase();
  }
  return `${tokens[0][0] || ''}${tokens[tokens.length - 1][0] || ''}`.toUpperCase();
}

export default function PoliticianHeadshot({
  memberId,
  name,
  party,
  size = 40,
  className = '',
}: {
  memberId: string | null | undefined;
  name: string;
  party?: string | null;
  size?: number;
  className?: string;
}) {
  const [failedUrl, setFailedUrl] = useState<string | null>(null);
  const resolvedPhotoUrl = getPoliticianPhotoUrl(memberId, '225x275', name);
  const photoUrl = resolvedPhotoUrl && failedUrl !== resolvedPhotoUrl ? resolvedPhotoUrl : null;
  const partyPresentation = getPartyPresentation(party || null, memberId || null);
  const initials = initialsForName(name);

  if (photoUrl) {
    return (
      <div
        className={`shrink-0 overflow-hidden rounded-full border border-white/10 bg-white/[0.04] ${className}`.trim()}
        style={{ width: size, height: size }}
      >
        <Image
          loader={passthroughImageLoader}
          unoptimized
          src={photoUrl}
          alt={name}
          width={size}
          height={size}
          sizes={`${size}px`}
          className="h-full w-full object-cover"
          onError={() => setFailedUrl(photoUrl)}
        />
      </div>
    );
  }

  return (
    <div
      className={`flex shrink-0 items-center justify-center rounded-full border text-white ${className}`.trim()}
      style={{
        width: size,
        height: size,
        borderColor: `${partyPresentation.color}55`,
        background: `linear-gradient(135deg, ${partyPresentation.color}, rgba(15, 23, 42, 0.95))`,
      }}
    >
      <span style={{ fontSize: Math.max(10, Math.round(size * 0.34)), fontWeight: 700 }}>{initials}</span>
    </div>
  );
}
