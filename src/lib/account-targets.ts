const COMMON_NAME_SUFFIXES = new Set(['jr', 'sr', 'ii', 'iii', 'iv', 'v']);
const FIRST_NAME_ALIAS_GROUPS = [
  new Set(['bill', 'billy', 'will', 'william']),
  new Set(['dan', 'daniel', 'danny']),
  new Set(['dave', 'david']),
  new Set(['jim', 'jimmy', 'james']),
  new Set(['rick', 'richard']),
  new Set(['ted', 'rafael']),
  new Set(['tom', 'tommy', 'thomas']),
];

const FIRST_NAME_ALIAS_MAP = new Map<string, Set<string>>();
for (const group of FIRST_NAME_ALIAS_GROUPS) {
  for (const token of group) {
    FIRST_NAME_ALIAS_MAP.set(token, group);
  }
}

const POLITICIAN_ALIAS_BY_ID = new Map<string, string[]>([
  ['P000197', ['pelosi', 'nancy pelosi']],
  ['O000172', ['aoc', 'alexandria ocasio cortez', 'ocasio cortez']],
  ['M000355', ['mitch mcconnell', 'mcconnell']],
  ['J000299', ['mike johnson']],
  ['M001190', ['markwayne mullin', 'markwayne']],
]);

export type CongressMemberRecord = {
  id: string;
  first_name: string | null;
  last_name: string | null;
  chamber?: string | null;
  active?: boolean | null;
  state?: string | null;
  party?: string | null;
};

function asciiNormalize(value: string) {
  return value.normalize('NFKD').replace(/[\u0300-\u036f]/g, '');
}

export function normalizeNameTokens(value: string) {
  return asciiNormalize(value || '')
    .toLowerCase()
    .match(/[a-z]+/g)
    ?.filter((token) => !COMMON_NAME_SUFFIXES.has(token)) || [];
}

export function normalizeActorKey(value: string) {
  return asciiNormalize(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9]/g, '');
}

function aliasTokens(token: string) {
  return FIRST_NAME_ALIAS_MAP.get(token) || new Set([token]);
}

function firstNameTokensMatch(firstTokens: string[], memberFirstName: string | null | undefined) {
  const memberTokens = normalizeNameTokens(memberFirstName || '');
  if (!firstTokens.length || !memberTokens.length) {
    return false;
  }

  for (const filedToken of firstTokens) {
    for (const memberToken of memberTokens) {
      if (filedToken === memberToken) {
        return true;
      }
      const filedAliases = aliasTokens(filedToken);
      const memberAliases = aliasTokens(memberToken);
      if ([...filedAliases].some((value) => memberAliases.has(value))) {
        return true;
      }
      if (filedToken.length === 1 && memberToken.startsWith(filedToken)) {
        return true;
      }
      if (memberToken.length === 1 && filedToken.startsWith(memberToken)) {
        return true;
      }
      if (
        Math.min(filedToken.length, memberToken.length) >= 3 &&
        (filedToken.startsWith(memberToken) || memberToken.startsWith(filedToken))
      ) {
        return true;
      }
    }
  }

  return false;
}

function isPlaceholderMember(member: CongressMemberRecord) {
  return String(member.id || '').startsWith('unknown-');
}

export function resolvePoliticianTarget(rawValue: string, members: CongressMemberRecord[]) {
  const candidate = rawValue.trim();
  if (!candidate) {
    return null;
  }

  const exactIdMatch = members.find((member) => member.id.toUpperCase() === candidate.toUpperCase());
  if (exactIdMatch) {
    return exactIdMatch;
  }

  const normalizedCandidate = asciiNormalize(candidate || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
  const aliasMatch = members.find((member) => {
    const aliases = POLITICIAN_ALIAS_BY_ID.get(String(member.id || '').toUpperCase()) || [];
    return aliases.includes(normalizedCandidate);
  });
  if (aliasMatch) {
    return aliasMatch;
  }

  const tokens = normalizeNameTokens(candidate);
  if (tokens.length < 2) {
    return null;
  }

  const exactCandidates: CongressMemberRecord[] = [];
  const activeCandidates: CongressMemberRecord[] = [];

  for (const member of members) {
    if (isPlaceholderMember(member)) {
      continue;
    }

    const memberLastTokens = normalizeNameTokens(member.last_name || '');
    if (!memberLastTokens.length || tokens.length <= memberLastTokens.length) {
      continue;
    }

    const lastTokens = tokens.slice(tokens.length - memberLastTokens.length);
    if (lastTokens.join('|') !== memberLastTokens.join('|')) {
      continue;
    }

    const firstTokens = tokens.slice(0, tokens.length - memberLastTokens.length);
    exactCandidates.push(member);

    if (member.active !== false) {
      activeCandidates.push(member);
      if (firstNameTokensMatch(firstTokens, member.first_name)) {
        return member;
      }
    }
  }

  if (activeCandidates.length === 1) {
    return activeCandidates[0];
  }
  if (exactCandidates.length === 1) {
    return exactCandidates[0];
  }
  return null;
}
