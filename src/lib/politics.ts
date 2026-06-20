const DEMOCRATIC_CAUCUS_MEMBER_IDS = new Set([
    'K000383', // Angus King
    'S000033', // Bernie Sanders
]);

const REPUBLICAN_CAUCUS_MEMBER_IDS = new Set<string>([]);

type PartyPresentation = {
    code: string;
    label: string;
    color: string;
    className: string;
};

export function getPartyPresentation(party?: string | null, memberId?: string | null): PartyPresentation {
    const normalizedParty = (party || '').trim();

    if (normalizedParty === 'Democrat') {
        return { code: 'D', label: 'Democrat', color: '#3b82f6', className: 'text-blue-400' };
    }

    if (normalizedParty === 'Republican') {
        return { code: 'R', label: 'Republican', color: '#ef4444', className: 'text-red-400' };
    }

    if (normalizedParty === 'Independent' && memberId && DEMOCRATIC_CAUCUS_MEMBER_IDS.has(memberId)) {
        return { code: 'D', label: 'Independent (D caucus)', color: '#3b82f6', className: 'text-blue-400' };
    }

    if (normalizedParty === 'Independent' && memberId && REPUBLICAN_CAUCUS_MEMBER_IDS.has(memberId)) {
        return { code: 'R', label: 'Independent (R caucus)', color: '#ef4444', className: 'text-red-400' };
    }

    if (normalizedParty === 'Independent') {
        return { code: 'I', label: 'Independent', color: '#9ca3af', className: 'text-gray-300' };
    }

    if (normalizedParty) {
        return {
            code: normalizedParty.charAt(0).toUpperCase(),
            label: normalizedParty,
            color: '#9ca3af',
            className: 'text-gray-300',
        };
    }

    return { code: '?', label: 'Unknown', color: '#9ca3af', className: 'text-gray-300' };
}
