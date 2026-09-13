-- Tracking and email delivery are separate choices. Preserve existing preferences.
ALTER TABLE public.profiles ALTER COLUMN email_enabled SET DEFAULT FALSE;
