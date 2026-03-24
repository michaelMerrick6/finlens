-- Vail Phase 6 billing foundation
-- Apply after phase 5. Non-destructive.

ALTER TABLE public.profiles
ADD COLUMN IF NOT EXISTS billing_plan_key TEXT NOT NULL DEFAULT 'free',
ADD COLUMN IF NOT EXISTS billing_status TEXT NOT NULL DEFAULT 'free',
ADD COLUMN IF NOT EXISTS stripe_customer_id TEXT,
ADD COLUMN IF NOT EXISTS stripe_subscription_id TEXT,
ADD COLUMN IF NOT EXISTS stripe_price_id TEXT,
ADD COLUMN IF NOT EXISTS billing_current_period_end TIMESTAMP WITH TIME ZONE,
ADD COLUMN IF NOT EXISTS billing_cancel_at_period_end BOOLEAN NOT NULL DEFAULT FALSE;

CREATE UNIQUE INDEX IF NOT EXISTS idx_profiles_stripe_customer_id
ON public.profiles(stripe_customer_id)
WHERE stripe_customer_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_profiles_stripe_subscription_id
ON public.profiles(stripe_subscription_id)
WHERE stripe_subscription_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_profiles_billing_status
ON public.profiles(billing_status);

ALTER TABLE public.profiles
ALTER COLUMN follow_limit SET DEFAULT 3;

UPDATE public.profiles
SET billing_plan_key = COALESCE(NULLIF(billing_plan_key, ''), 'free'),
    billing_status = COALESCE(NULLIF(billing_status, ''), 'free'),
    follow_limit = CASE
      WHEN COALESCE(NULLIF(billing_plan_key, ''), 'free') = 'pro'
       AND COALESCE(NULLIF(billing_status, ''), 'free') IN ('active', 'trialing', 'past_due')
        THEN 10
      ELSE 3
    END
WHERE TRUE;

CREATE TABLE IF NOT EXISTS public.stripe_webhook_events (
    id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_stripe_webhook_events_type
ON public.stripe_webhook_events(event_type, created_at DESC);
