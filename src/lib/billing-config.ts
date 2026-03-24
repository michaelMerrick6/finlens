export type BillingPlanKey = 'free' | 'pro';

const PAID_BILLING_STATUSES = new Set(['active', 'trialing', 'past_due']);

function parsePositiveInteger(value: string | undefined, fallback: number) {
  const parsed = Number.parseInt(value || '', 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

export function getFreeFollowLimit() {
  return parsePositiveInteger(process.env.BILLING_FREE_FOLLOW_LIMIT, 3);
}

export function getProFollowLimit() {
  return parsePositiveInteger(process.env.BILLING_PRO_FOLLOW_LIMIT, 10);
}

export function normalizeBillingPlanKey(value: string | null | undefined): BillingPlanKey {
  return String(value || '').trim().toLowerCase() === 'pro' ? 'pro' : 'free';
}

export function normalizeBillingStatus(value: string | null | undefined) {
  const normalized = String(value || '').trim().toLowerCase();
  return normalized || 'free';
}

export function isPaidBillingStatus(value: string | null | undefined) {
  return PAID_BILLING_STATUSES.has(normalizeBillingStatus(value));
}

export function getBillingPlanName(planKey: BillingPlanKey) {
  return planKey === 'pro' ? 'Vail Pro' : 'Free';
}

export function resolveBillingPlanKey(
  priceId: string | null | undefined,
  status: string | null | undefined
): BillingPlanKey {
  const configuredProPriceId = String(process.env.STRIPE_VAIL_PRO_PRICE_ID || '').trim();
  if (priceId && configuredProPriceId && priceId === configuredProPriceId) {
    return 'pro';
  }

  return isPaidBillingStatus(status) ? 'pro' : 'free';
}

export function resolveBillingFollowLimit(planKey: BillingPlanKey, status: string | null | undefined) {
  return planKey === 'pro' && isPaidBillingStatus(status) ? getProFollowLimit() : getFreeFollowLimit();
}
