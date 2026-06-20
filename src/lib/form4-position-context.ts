import 'server-only';

import { normalizeInsiderDirection } from '@/lib/insider-trades';
import type { TickerInsiderTransaction } from '@/lib/ticker-intelligence-types';

const SEC_FETCH_HEADERS = {
  Accept: 'text/plain,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
  'User-Agent': process.env.SEC_USER_AGENT || 'Vail/1.0 mikemerricka@gmail.com',
};

const FORM4_DOCUMENT_CACHE = new Map<string, Promise<ParsedForm4Transaction[] | null>>();

type ParsedForm4Transaction = {
  index: number;
  direction: 'buy' | 'sell' | 'other';
  transactionDate: string | null;
  amount: number | null;
  value: number | null;
  sharesOwnedAfterTransaction: number | null;
};

function normalizeText(value: string | null | undefined) {
  return String(value || '').trim();
}

function parseNumericValue(value: string | null | undefined) {
  const normalized = normalizeText(value).replace(/,/g, '');
  if (!normalized) {
    return null;
  }

  const numeric = Number(normalized);
  return Number.isFinite(numeric) ? numeric : null;
}

function roundNumeric(value: number | null, digits: number) {
  if (value === null || !Number.isFinite(value)) {
    return null;
  }

  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}

function splitForm4SourceUrl(sourceUrl: string) {
  const normalized = normalizeText(sourceUrl);
  if (!normalized) {
    return { documentUrl: null, transactionIndex: null };
  }

  const [documentUrl, fragment] = normalized.split('#', 2);
  const transactionMatch = fragment?.match(/^tx-(\d+)$/i);

  return {
    documentUrl: documentUrl || null,
    transactionIndex: transactionMatch ? Number(transactionMatch[1]) : null,
  };
}

function extractForm4Xml(text: string) {
  const xmlMatches = text.matchAll(/<XML>([\s\S]*?)<\/XML>/gi);
  for (const match of xmlMatches) {
    const candidate = match[1] || '';
    if (/<ownershipDocument[\s>]/i.test(candidate) || /<nonDerivativeTransaction\b/i.test(candidate)) {
      return candidate;
    }
  }

  if (/<ownershipDocument[\s>]/i.test(text) || /<nonDerivativeTransaction\b/i.test(text)) {
    return text;
  }

  return null;
}

function extractNestedValue(block: string, tagName: string) {
  const match = block.match(
    new RegExp(`<${tagName}(?:\\s[^>]*)?>[\\s\\S]*?<value>\\s*([^<]*)\\s*</value>[\\s\\S]*?</${tagName}>`, 'i'),
  );
  return normalizeText(match?.[1]) || null;
}

function extractSimpleValue(block: string, tagName: string) {
  const match = block.match(new RegExp(`<${tagName}(?:\\s[^>]*)?>\\s*([^<]*)\\s*</${tagName}>`, 'i'));
  return normalizeText(match?.[1]) || null;
}

function resolveDirection(transactionCode: string | null, acquiredDisposedCode: string | null) {
  const fromCode = normalizeInsiderDirection(transactionCode);
  return fromCode !== 'other' ? fromCode : normalizeInsiderDirection(acquiredDisposedCode);
}

function parseForm4Transactions(documentText: string): ParsedForm4Transaction[] | null {
  const xmlBody = extractForm4Xml(documentText);
  if (!xmlBody) {
    return null;
  }

  const blocks = xmlBody.match(/<nonDerivativeTransaction\b[\s\S]*?<\/nonDerivativeTransaction>/gi) || [];
  if (!blocks.length) {
    return [];
  }

  return blocks.map((block, index) => {
    const transactionCode = extractSimpleValue(block, 'transactionCode');
    const acquiredDisposedCode = extractNestedValue(block, 'transactionAcquiredDisposedCode');
    const direction = resolveDirection(transactionCode, acquiredDisposedCode);
    const amount = parseNumericValue(extractNestedValue(block, 'transactionShares'));
    const price = parseNumericValue(extractNestedValue(block, 'transactionPricePerShare'));
    const sharesOwnedAfterTransaction = parseNumericValue(
      extractNestedValue(block, 'sharesOwnedFollowingTransaction'),
    );

    return {
      index,
      direction,
      transactionDate: normalizeText(extractNestedValue(block, 'transactionDate')) || null,
      amount,
      value: amount !== null && price !== null ? amount * price : null,
      sharesOwnedAfterTransaction,
    };
  });
}

async function loadForm4Transactions(documentUrl: string) {
  const cached = FORM4_DOCUMENT_CACHE.get(documentUrl);
  if (cached) {
    return cached;
  }

  const request = (async () => {
    try {
      const response = await fetch(documentUrl, {
        headers: SEC_FETCH_HEADERS,
        cache: 'force-cache',
      });

      if (!response.ok) {
        return null;
      }

      const text = await response.text();
      return parseForm4Transactions(text);
    } catch {
      return null;
    }
  })();

  FORM4_DOCUMENT_CACHE.set(documentUrl, request);
  return request;
}

function sameNumericValue(left: number | null, right: number | null, tolerance = 0.0001) {
  if (left === null || right === null) {
    return false;
  }
  return Math.abs(left - right) <= tolerance;
}

function findMatchingTransaction(
  trade: TickerInsiderTransaction,
  parsedTransactions: ParsedForm4Transaction[],
  transactionIndex: number | null,
) {
  if (transactionIndex !== null) {
    const indexed = parsedTransactions[transactionIndex];
    if (indexed) {
      return indexed;
    }
  }

  const targetDate = normalizeText(trade.transactionDate || trade.publishedDate) || null;
  const directionalMatches = parsedTransactions.filter((parsed) => parsed.direction === trade.direction);
  const datedMatches = targetDate
    ? directionalMatches.filter((parsed) => !parsed.transactionDate || parsed.transactionDate === targetDate)
    : directionalMatches;

  const amountMatch =
    datedMatches.find((parsed) => sameNumericValue(parsed.amount, trade.amount)) ||
    directionalMatches.find((parsed) => sameNumericValue(parsed.amount, trade.amount));

  if (amountMatch) {
    return amountMatch;
  }

  const valueMatch =
    datedMatches.find((parsed) => sameNumericValue(parsed.value, trade.value, 0.01)) ||
    directionalMatches.find((parsed) => sameNumericValue(parsed.value, trade.value, 0.01));

  return valueMatch || datedMatches[0] || directionalMatches[0] || null;
}

function buildPositionContext(
  trade: TickerInsiderTransaction,
  matchedTransaction: ParsedForm4Transaction | null,
) {
  if (!matchedTransaction) {
    return null;
  }

  const sharesOwnedAfterTransaction = matchedTransaction.sharesOwnedAfterTransaction;
  const amount = trade.amount ?? matchedTransaction.amount;
  if (sharesOwnedAfterTransaction === null || amount === null || amount <= 0 || trade.direction === 'other') {
    return null;
  }

  const sharesOwnedBeforeTransaction =
    trade.direction === 'sell'
      ? sharesOwnedAfterTransaction + amount
      : Math.max(sharesOwnedAfterTransaction - amount, 0);
  const holdingChangePct =
    sharesOwnedBeforeTransaction > 0 ? amount / sharesOwnedBeforeTransaction : null;

  return {
    sharesOwnedAfterTransaction: roundNumeric(sharesOwnedAfterTransaction, 4),
    sharesOwnedBeforeTransaction: roundNumeric(sharesOwnedBeforeTransaction, 4),
    holdingChangePct: roundNumeric(holdingChangePct, 6),
  };
}

async function enrichInsiderTransaction(trade: TickerInsiderTransaction) {
  if (!trade.sourceUrl || trade.direction === 'other') {
    return trade;
  }

  const { documentUrl, transactionIndex } = splitForm4SourceUrl(trade.sourceUrl);
  if (!documentUrl) {
    return trade;
  }

  const parsedTransactions = await loadForm4Transactions(documentUrl);
  if (!parsedTransactions?.length) {
    return trade;
  }

  const matchedTransaction = findMatchingTransaction(trade, parsedTransactions, transactionIndex);
  const positionContext = buildPositionContext(trade, matchedTransaction);
  return positionContext ? { ...trade, ...positionContext } : trade;
}

export async function enrichInsiderTransactionsWithPositions(
  transactions: TickerInsiderTransaction[],
  limit: number,
) {
  if (!transactions.length || limit <= 0) {
    return transactions;
  }

  const cappedLimit = Math.min(limit, transactions.length);
  const enrichedPrefix = await Promise.all(
    transactions.slice(0, cappedLimit).map((trade) => enrichInsiderTransaction(trade)),
  );

  return [...enrichedPrefix, ...transactions.slice(cappedLimit)];
}
