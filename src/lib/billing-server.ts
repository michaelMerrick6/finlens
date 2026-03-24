import 'server-only';

import Stripe from 'stripe';
import type { User } from '@supabase/supabase-js';

import {
  getBillingPlanName,
  getFreeFollowLimit,
  getProFollowLimit,
  normalizeBillingPlanKey,
  normalizeBillingStatus,
  resolveBillingFollowLimit,
  resolveBillingPlanKey,
} from '@/lib/billing-config';
import { ApiRouteError } from '@/lib/auth-server';
import { getAdminSupabase } from '@/lib/supabase-admin';

type BillingProfileRow = {
  id: string;
  email: string | null;
  display_name: string | null;
  follow_limit: number | null;
  stripe_customer_id: string | null;
  stripe_subscription_id: string | null;
  stripe_price_id: string | null;
  billing_plan_key: string | null;
  billing_status: string | null;
  billing_current_period_end: string | null;
  billing_cancel_at_period_end: boolean | null;
};

type BillingSyncPayload = {
  userId?: string | null;
  customerId: string | null;
  subscriptionId: string | null;
  priceId: string | null;
  status: string | null;
  currentPeriodEnd: number | null;
  cancelAtPeriodEnd: boolean;
};

const BILLING_ERROR_MESSAGE = 'Billing is not configured yet.';

function getAppBaseUrl() {
  const value = process.env.APP_BASE_URL || process.env.NEXT_PUBLIC_APP_URL || 'http://localhost:3000';
  return value.replace(/\/+$/, '');
}

function getStripePriceId() {
  const value = String(process.env.STRIPE_VAIL_PRO_PRICE_ID || '').trim();
  if (!value) {
    throw new ApiRouteError(503, 'BILLING_NOT_READY', 'Missing STRIPE_VAIL_PRO_PRICE_ID.');
  }
  return value;
}

function getStripeWebhookSecret() {
  const value = String(process.env.STRIPE_WEBHOOK_SECRET || '').trim();
  if (!value) {
    throw new ApiRouteError(503, 'BILLING_NOT_READY', 'Missing STRIPE_WEBHOOK_SECRET.');
  }
  return value;
}

function getStripeClient() {
  const secretKey = String(process.env.STRIPE_SECRET_KEY || '').trim();
  if (!secretKey) {
    throw new ApiRouteError(503, 'BILLING_NOT_READY', BILLING_ERROR_MESSAGE);
  }

  return new Stripe(secretKey);
}

export function isBillingCheckoutConfigured() {
  return Boolean(String(process.env.STRIPE_SECRET_KEY || '').trim() && String(process.env.STRIPE_VAIL_PRO_PRICE_ID || '').trim());
}

export function isBillingPortalConfigured() {
  return Boolean(String(process.env.STRIPE_SECRET_KEY || '').trim());
}

async function ensureBillingProfile(user: User): Promise<BillingProfileRow> {
  const supabase = getAdminSupabase();
  const existing = await supabase
    .from('profiles')
    .select(
      'id,email,display_name,follow_limit,stripe_customer_id,stripe_subscription_id,stripe_price_id,billing_plan_key,billing_status,billing_current_period_end,billing_cancel_at_period_end'
    )
    .eq('id', user.id)
    .maybeSingle();

  if (existing.error) {
    throw existing.error;
  }

  if (existing.data) {
    return existing.data as BillingProfileRow;
  }

  const inserted = await supabase
    .from('profiles')
    .insert({
      id: user.id,
      email: user.email || null,
      display_name: user.user_metadata?.display_name || user.user_metadata?.full_name || user.email?.split('@')[0] || null,
      alert_email: user.email || null,
      follow_limit: getFreeFollowLimit(),
      billing_plan_key: 'free',
      billing_status: 'free',
    })
    .select(
      'id,email,display_name,follow_limit,stripe_customer_id,stripe_subscription_id,stripe_price_id,billing_plan_key,billing_status,billing_current_period_end,billing_cancel_at_period_end'
    )
    .single();

  if (inserted.error) {
    throw inserted.error;
  }

  return inserted.data as BillingProfileRow;
}

async function updateProfileBillingState(profileId: string, payload: BillingSyncPayload) {
  const supabase = getAdminSupabase();
  const planKey = resolveBillingPlanKey(payload.priceId, payload.status);
  const normalizedStatus = normalizeBillingStatus(payload.status);
  const followLimit = resolveBillingFollowLimit(planKey, normalizedStatus);

  const response = await supabase
    .from('profiles')
    .update({
      stripe_customer_id: payload.customerId,
      stripe_subscription_id: payload.subscriptionId,
      stripe_price_id: payload.priceId,
      billing_plan_key: planKey,
      billing_status: normalizedStatus,
      billing_current_period_end: payload.currentPeriodEnd ? new Date(payload.currentPeriodEnd * 1000).toISOString() : null,
      billing_cancel_at_period_end: payload.cancelAtPeriodEnd,
      follow_limit: followLimit,
    })
    .eq('id', profileId)
    .select('id')
    .single();

  if (response.error) {
    throw response.error;
  }
}

async function findProfileIdForBillingPayload(payload: BillingSyncPayload) {
  const supabase = getAdminSupabase();

  if (payload.userId) {
    const byId = await supabase.from('profiles').select('id').eq('id', payload.userId).maybeSingle();
    if (byId.error) {
      throw byId.error;
    }
    if (byId.data?.id) {
      return String(byId.data.id);
    }
  }

  if (payload.customerId) {
    const byCustomer = await supabase
      .from('profiles')
      .select('id')
      .eq('stripe_customer_id', payload.customerId)
      .maybeSingle();
    if (byCustomer.error) {
      throw byCustomer.error;
    }
    if (byCustomer.data?.id) {
      return String(byCustomer.data.id);
    }
  }

  throw new Error(`Could not find a profile for Stripe customer ${payload.customerId || 'unknown'}.`);
}

function subscriptionPriceId(subscription: Stripe.Subscription) {
  return subscription.items.data[0]?.price?.id || null;
}

function subscriptionCurrentPeriodEnd(subscription: Stripe.Subscription) {
  const values = subscription.items.data
    .map((item) => item.current_period_end || 0)
    .filter((value) => value > 0);
  return values.length ? Math.max(...values) : null;
}

async function ensureStripeCustomer(profile: BillingProfileRow, user: User) {
  const stripe = getStripeClient();

  if (profile.stripe_customer_id) {
    return profile.stripe_customer_id;
  }

  const customer = await stripe.customers.create({
    email: user.email || profile.email || undefined,
    name: profile.display_name || undefined,
    metadata: {
      supabase_user_id: user.id,
    },
  });

  const supabase = getAdminSupabase();
  const response = await supabase
    .from('profiles')
    .update({ stripe_customer_id: customer.id })
    .eq('id', user.id);

  if (response.error) {
    throw response.error;
  }

  return customer.id;
}

export async function createCheckoutSession(user: User) {
  if (!isBillingCheckoutConfigured()) {
    throw new ApiRouteError(503, 'BILLING_NOT_READY', BILLING_ERROR_MESSAGE);
  }

  const stripe = getStripeClient();
  const profile = await ensureBillingProfile(user);
  const customerId = await ensureStripeCustomer(profile, user);

  const session = await stripe.checkout.sessions.create({
    mode: 'subscription',
    customer: customerId,
    client_reference_id: user.id,
    payment_method_types: ['card'],
    allow_promotion_codes: true,
    success_url: `${getAppBaseUrl()}/alerts?billing=success`,
    cancel_url: `${getAppBaseUrl()}/alerts?billing=cancelled`,
    line_items: [
      {
        price: getStripePriceId(),
        quantity: 1,
      },
    ],
    metadata: {
      supabase_user_id: user.id,
      plan_key: 'pro',
    },
    subscription_data: {
      metadata: {
        supabase_user_id: user.id,
        plan_key: 'pro',
      },
    },
  });

  if (!session.url) {
    throw new Error('Stripe did not return a checkout URL.');
  }

  return session.url;
}

export async function createBillingPortalSession(user: User) {
  if (!isBillingPortalConfigured()) {
    throw new ApiRouteError(503, 'BILLING_NOT_READY', BILLING_ERROR_MESSAGE);
  }

  const stripe = getStripeClient();
  const profile = await ensureBillingProfile(user);
  const customerId = await ensureStripeCustomer(profile, user);
  const session = await stripe.billingPortal.sessions.create({
    customer: customerId,
    return_url: `${getAppBaseUrl()}/alerts?billing=portal`,
  });

  return session.url;
}

async function recordWebhookEvent(event: Stripe.Event) {
  const supabase = getAdminSupabase();
  const response = await supabase.from('stripe_webhook_events').insert({
    id: event.id,
    event_type: event.type,
    payload: event as unknown as Record<string, unknown>,
  });

  if (!response.error) {
    return true;
  }

  if (String(response.error.code || '') === '23505') {
    return false;
  }

  throw response.error;
}

export async function constructStripeEvent(signature: string | null, payload: string) {
  if (!signature) {
    throw new ApiRouteError(400, 'BILLING_BAD_SIGNATURE', 'Missing Stripe signature.');
  }

  const stripe = getStripeClient();
  return stripe.webhooks.constructEvent(payload, signature, getStripeWebhookSecret());
}

export async function syncBillingFromStripeSubscription(subscription: Stripe.Subscription, userId?: string | null) {
  const payload: BillingSyncPayload = {
    userId: userId || subscription.metadata?.supabase_user_id || null,
    customerId: typeof subscription.customer === 'string' ? subscription.customer : subscription.customer.id,
    subscriptionId: subscription.id,
    priceId: subscriptionPriceId(subscription),
    status: subscription.status,
    currentPeriodEnd: subscriptionCurrentPeriodEnd(subscription),
    cancelAtPeriodEnd: Boolean(subscription.cancel_at_period_end),
  };

  const profileId = await findProfileIdForBillingPayload(payload);
  await updateProfileBillingState(profileId, payload);
}

export async function syncBillingFromCheckoutSession(session: Stripe.Checkout.Session) {
  const userId =
    session.client_reference_id ||
    session.metadata?.supabase_user_id ||
    null;

  const customerId = typeof session.customer === 'string' ? session.customer : session.customer?.id || null;
  const subscriptionId =
    typeof session.subscription === 'string' ? session.subscription : session.subscription?.id || null;

  if (!subscriptionId) {
    if (!customerId) {
      throw new Error('Checkout session is missing customer context.');
    }

    const profileId = await findProfileIdForBillingPayload({
      userId,
      customerId,
      subscriptionId: null,
      priceId: null,
      status: 'incomplete',
      currentPeriodEnd: null,
      cancelAtPeriodEnd: false,
    });

    await updateProfileBillingState(profileId, {
      userId,
      customerId,
      subscriptionId: null,
      priceId: null,
      status: 'incomplete',
      currentPeriodEnd: null,
      cancelAtPeriodEnd: false,
    });
    return;
  }

  const stripe = getStripeClient();
  const subscription = await stripe.subscriptions.retrieve(subscriptionId);
  await syncBillingFromStripeSubscription(subscription, userId);
}

export async function syncBillingFromStripeEvent(event: Stripe.Event) {
  const recorded = await recordWebhookEvent(event);
  if (!recorded) {
    return { processed: false, duplicate: true };
  }

  if (event.type === 'checkout.session.completed') {
    await syncBillingFromCheckoutSession(event.data.object as Stripe.Checkout.Session);
    return { processed: true, duplicate: false };
  }

  if (
    event.type === 'customer.subscription.created' ||
    event.type === 'customer.subscription.updated' ||
    event.type === 'customer.subscription.deleted'
  ) {
    await syncBillingFromStripeSubscription(event.data.object as Stripe.Subscription);
    return { processed: true, duplicate: false };
  }

  return { processed: true, duplicate: false };
}

export function describeBillingSummary(planKey: string | null | undefined, status: string | null | undefined) {
  const normalizedPlan = normalizeBillingPlanKey(planKey);
  const normalizedStatus = normalizeBillingStatus(status);
  return {
    planKey: normalizedPlan,
    planName: getBillingPlanName(normalizedPlan),
    status: normalizedStatus,
    followLimit: resolveBillingFollowLimit(normalizedPlan, normalizedStatus),
    freeFollowLimit: getFreeFollowLimit(),
    proFollowLimit: getProFollowLimit(),
  };
}
