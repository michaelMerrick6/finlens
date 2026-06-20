export type PoliticianPhotoSize = '225x275' | '450x550' | 'original';

const DEFAULT_POLITICIAN_PHOTO_SIZE: PoliticianPhotoSize = '225x275';

const POLITICIAN_NAME_TO_MEMBER_ID: Record<string, string> = {
  'david j taylor': 'T000480',
  'david j. taylor': 'T000480',
  'daniel meuser': 'M001204',
  'dwight evans': 'E000296',
  'elizabeth fletcher': 'F000468',
  'gilbert cisneros': 'C001123',
  'greg steube': 'S001214',
  'josh gottheimer': 'G000583',
  'john mcguire': 'M001227',
  'michael mccaul': 'M001157',
  'michael t. mccaul': 'M001157',
  'nancy pelosi': 'P000197',
  'richard dean dr mccormick': 'M001218',
  'rich mccormick': 'M001218',
  'ro khanna': 'K000389',
  'roh khanna': 'K000389',
  'shelley moore capito': 'C001047',
  'tim moore': 'M001235',
  'tom carper': 'C000174',
  'tommy tuberville': 'T000278',
};

export function isBioguideMemberId(value: string | null | undefined): boolean {
  return /^[A-Z]\d{6}$/i.test(String(value || '').trim());
}

export function normalizeBioguideMemberId(value: string | null | undefined): string | null {
  const rawValue = String(value || '').trim().toUpperCase();
  const exactMatch = rawValue.match(/^[A-Z]\d{6}$/);
  if (exactMatch) {
    return exactMatch[0];
  }

  return rawValue.match(/[A-Z]\d{6}/)?.[0] || null;
}

function normalizePoliticianName(value: string | null | undefined): string {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/\b(rep|sen|representative|senator|hon|dr)\.?\b/g, '')
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

export function resolvePoliticianPhotoMemberId(
  memberId: string | null | undefined,
  name?: string | null,
): string | null {
  const normalizedMemberId = normalizeBioguideMemberId(memberId);
  if (normalizedMemberId) {
    return normalizedMemberId;
  }

  const normalizedName = normalizePoliticianName(name);
  return POLITICIAN_NAME_TO_MEMBER_ID[normalizedName] || null;
}

export function getPoliticianPhotoUrl(
  memberId: string | null | undefined,
  size: PoliticianPhotoSize = DEFAULT_POLITICIAN_PHOTO_SIZE,
  name?: string | null,
): string | null {
  const normalizedMemberId = resolvePoliticianPhotoMemberId(memberId, name);
  if (!normalizedMemberId) {
    return null;
  }

  return `https://unitedstates.github.io/images/congress/${size}/${normalizedMemberId}.jpg`;
}
