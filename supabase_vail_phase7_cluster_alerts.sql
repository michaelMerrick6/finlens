-- Vail Phase 7 global cluster alert preference
-- Apply after phase 6. Non-destructive.

ALTER TABLE public.profiles
ADD COLUMN IF NOT EXISTS cluster_alerts_enabled BOOLEAN NOT NULL DEFAULT FALSE;
