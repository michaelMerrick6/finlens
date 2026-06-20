-- Vail Phase 8 cluster alert delivery channels
-- Apply after phase 7. Non-destructive.

ALTER TABLE public.profiles
ADD COLUMN IF NOT EXISTS cluster_alert_channels TEXT[] NOT NULL DEFAULT ARRAY['email']::TEXT[];

UPDATE public.profiles
SET cluster_alert_channels = ARRAY['email']::TEXT[]
WHERE cluster_alert_channels IS NULL OR cardinality(cluster_alert_channels) = 0;
