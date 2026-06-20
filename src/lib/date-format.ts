const DATE_ONLY_RE = /^\d{4}-\d{2}-\d{2}$/;

function parseDisplayDate(value: string): Date {
  if (DATE_ONLY_RE.test(value)) {
    return new Date(`${value}T12:00:00Z`);
  }
  return new Date(value);
}

export function formatDateValue(
  value: string | null | undefined,
  options: Intl.DateTimeFormatOptions,
  timeZone = 'America/Los_Angeles',
) {
  if (!value) {
    return 'N/A';
  }
  const date = parseDisplayDate(value);
  if (Number.isNaN(date.getTime())) {
    return 'N/A';
  }

  return new Intl.DateTimeFormat('en-US', {
    ...options,
    timeZone,
  }).format(date);
}

export function formatCalendarDate(
  value: string | null | undefined,
  timeZone = 'America/Los_Angeles',
) {
  return formatDateValue(
    value,
    {
      month: 'short',
      day: 'numeric',
      year: 'numeric',
    },
    timeZone,
  );
}

export function formatShortCalendarDate(
  value: string | null | undefined,
  timeZone = 'America/Los_Angeles',
) {
  return formatDateValue(
    value,
    {
      month: 'numeric',
      day: 'numeric',
      year: '2-digit',
    },
    timeZone,
  );
}

export function formatDateTimeValue(
  value: string | null | undefined,
  timeZone = 'America/Los_Angeles',
) {
  return formatDateValue(
    value,
    {
      dateStyle: 'medium',
      timeStyle: 'short',
    },
    timeZone,
  );
}
