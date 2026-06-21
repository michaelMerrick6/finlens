"use client";

import Image, { type ImageLoaderProps } from "next/image";
import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";
import type { Session } from "@supabase/supabase-js";
import {
  AlertTriangle,
  ArrowRight,
  Bell,
  BellPlus,
  Building2,
  CheckCircle2,
  ChevronRight,
  Landmark,
  Mail,
  MessageSquare,
  Plus,
  Radio,
  Send,
  Settings2,
  ShieldAlert,
  Trash2,
  TrendingUp,
  UserRound,
  X,
  Zap,
} from "lucide-react";

import DashboardClusterModal from "@/components/DashboardClusterModal";
import PoliticianHeadshot from "@/components/PoliticianHeadshot";
import type {
  AccountActorFollow,
  AccountAlertPreview,
  AccountMatchedSignal,
  AccountState,
  AlertMode,
} from "@/lib/account-types";
import {
  CreateSignalModal,
  CustomFiltersForPerson,
  CustomFiltersForStock,
  DEFAULT_PERSON_FILTERS,
  DEFAULT_STOCK_FILTERS,
  type AccountApiClient,
  type NotifyMode,
  Pill,
  useAccountApi,
} from "@/components/CreateSignalModal";
import { getTickerLogoUrl } from "@/lib/company-logos";
import type { DashboardClusterDetail } from "@/lib/dashboard-cluster-types";
import { formatDateTimeValue } from "@/lib/date-format";
import { supabase } from "@/lib/supabase";

// ─── Types ────────────────────────────────────────────────────────────────────

function channelLabel(channel: string) {
  if (channel === "email") return "Email";
  if (channel === "sms") return "Text";
  return channel;
}

function signalStatusTone(status: string) {
  if (status === "sent") {
    return "border-emerald-500/20 bg-emerald-500/5 text-emerald-400";
  }
  if (status === "failed") {
    return "border-red-500/20 bg-red-500/5 text-red-400";
  }
  return "border-amber-500/20 bg-amber-500/5 text-amber-400";
}

function signalTypeLabel(value: string) {
  return value
    .split(/[_\s-]+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function signalActivityDate(signal: AccountMatchedSignal) {
  return signal.publishedAt || signal.occurredAt;
}

type GroupedSentAlert = {
  id: string;
  title: string;
  summary: string | null;
  ticker: string | null;
  actorName: string | null;
  sourceUrl: string | null;
  alertAt: string;
  channels: string[];
};

type AlertClusterPreview = {
  id: string;
  ticker: string;
  title: string;
  summary: string;
  ruleLabel: string;
  actorPreview: string | null;
  actorCount: number;
  amountLabel: string | null;
  sourceLabel: string;
  publishedAt: string | null;
  direction: "buy" | "sell" | null;
};

type DirectionFilter = "all" | "buy" | "sell";

function sortDeliveryChannels(channels: string[]) {
  const order = new Map([
    ["email", 0],
    ["sms", 1],
  ]);
  return [...channels].sort(
    (left, right) => (order.get(left) ?? 99) - (order.get(right) ?? 99),
  );
}

function groupedSentAlerts(
  history: AccountState["history"],
): GroupedSentAlert[] {
  const grouped = new Map<string, GroupedSentAlert>();
  for (const item of history) {
    if (item.status !== "sent") {
      continue;
    }
    const alertAt = item.sentAt || item.queuedAt;
    if (!alertAt) {
      continue;
    }
    const key = item.signalEventId || `${item.title || "signal"}|${alertAt}`;
    const existing = grouped.get(key);
    if (!existing) {
      grouped.set(key, {
        id: key,
        title: item.title || "Signal alert",
        summary: item.summary,
        ticker: item.ticker,
        actorName: item.actorName,
        sourceUrl: item.sourceUrl,
        alertAt,
        channels: [item.channel],
      });
      continue;
    }

    if (alertAt > existing.alertAt) {
      existing.alertAt = alertAt;
    }
    if (!existing.summary && item.summary) {
      existing.summary = item.summary;
    }
    if (!existing.ticker && item.ticker) {
      existing.ticker = item.ticker;
    }
    if (!existing.actorName && item.actorName) {
      existing.actorName = item.actorName;
    }
    if (!existing.sourceUrl && item.sourceUrl) {
      existing.sourceUrl = item.sourceUrl;
    }
    if (!existing.channels.includes(item.channel)) {
      existing.channels.push(item.channel);
    }
  }

  return [...grouped.values()]
    .map((item) => ({
      ...item,
      channels: sortDeliveryChannels(item.channels),
    }))
    .sort((left, right) => right.alertAt.localeCompare(left.alertAt));
}

const passthroughImageLoader = ({ src }: ImageLoaderProps) => src;
const ALERTS_STATE_CACHE_TTL_MS = 5 * 60 * 1000;

type CachedAlertsState = {
  userId: string;
  savedAt: number;
  state: AccountState;
};

let inMemoryAlertsStateCache: CachedAlertsState | null = null;

function alertsStateCacheKey(userId: string) {
  return `vail:alerts-state:${userId}`;
}

function hasPreviewData(state: AccountState) {
  return (
    state.alertPreview.matchedSignalCount > 0 ||
    state.alertPreview.clusterSignalCount > 0 ||
    state.alertPreview.timeline.length > 0 ||
    state.alertPreview.followPreviews.length > 0
  );
}

function hasFreshAlertsCache(cache: CachedAlertsState | null, userId: string) {
  return (
    cache?.userId === userId &&
    Date.now() - cache.savedAt < ALERTS_STATE_CACHE_TTL_MS
  );
}

function readAlertsStateCache(userId: string) {
  const memoryCache = inMemoryAlertsStateCache;
  if (
    memoryCache?.userId === userId &&
    Date.now() - memoryCache.savedAt < ALERTS_STATE_CACHE_TTL_MS
  ) {
    return memoryCache.state;
  }

  if (typeof window === "undefined") {
    return null;
  }

  try {
    const raw = window.sessionStorage.getItem(alertsStateCacheKey(userId));
    if (!raw) return null;
    const parsed = JSON.parse(raw) as CachedAlertsState;
    if (!hasFreshAlertsCache(parsed, userId)) return null;
    inMemoryAlertsStateCache = parsed;
    return parsed.state;
  } catch {
    return null;
  }
}

function writeAlertsStateCache(state: AccountState) {
  const payload: CachedAlertsState = {
    userId: state.user.id,
    savedAt: Date.now(),
    state,
  };
  inMemoryAlertsStateCache = payload;

  if (typeof window === "undefined") {
    return;
  }

  try {
    window.sessionStorage.setItem(
      alertsStateCacheKey(state.user.id),
      JSON.stringify(payload),
    );
  } catch {
    // Cache is an optimization only; ignore storage quota/private-mode failures.
  }
}

function clearAlertsStateCache() {
  inMemoryAlertsStateCache = null;
}

type ActivityActorVisual = {
  memberId: string | null;
  party: string | null;
};

function compactActorKey(value: string | null | undefined) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function metadataString(metadata: Record<string, unknown>, key: string) {
  const value = metadata[key];
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function politicianMemberIdFromFollow(follow: AccountActorFollow) {
  const metadataMemberId = metadataString(follow.metadata, "member_id");
  const actorKey = String(follow.actorKey || "").trim();
  const candidate = metadataMemberId || actorKey;
  return /^[a-z]\d{6}$/i.test(candidate) ? candidate.toUpperCase() : null;
}

function actorVisualForFollow(follow: AccountActorFollow): ActivityActorVisual {
  return {
    memberId:
      follow.actorType === "politician"
        ? politicianMemberIdFromFollow(follow)
        : null,
    party: metadataString(follow.metadata, "party"),
  };
}

function tickerFallbackHue(ticker: string) {
  return (
    ticker.split("").reduce((total, char) => total + char.charCodeAt(0), 0) %
    360
  );
}

function ActivityTickerLogo({ ticker }: { ticker: string | null }) {
  const [failedUrl, setFailedUrl] = useState<string | null>(null);
  const normalizedTicker = String(ticker || "")
    .trim()
    .toUpperCase();
  const logoUrl = normalizedTicker
    ? getTickerLogoUrl(normalizedTicker, 40)
    : null;
  const activeLogoUrl = logoUrl && failedUrl !== logoUrl ? logoUrl : null;

  if (activeLogoUrl) {
    return (
      <div className="h-7 w-7 shrink-0 overflow-hidden rounded-full border border-white/10 bg-black/50 shadow-[0_0_0_2px_rgba(0,0,0,0.65)]">
        <Image
          loader={passthroughImageLoader}
          unoptimized
          src={activeLogoUrl}
          alt={normalizedTicker}
          width={28}
          height={28}
          sizes="28px"
          className="h-full w-full object-contain p-0.5"
          onError={() => setFailedUrl(activeLogoUrl)}
        />
      </div>
    );
  }

  if (!normalizedTicker) {
    return (
      <div className="flex h-7 w-7 shrink-0 items-center justify-center rounded-full border border-white/10 bg-zinc-900 text-zinc-500 shadow-[0_0_0_2px_rgba(0,0,0,0.65)]">
        <TrendingUp className="h-3.5 w-3.5" />
      </div>
    );
  }

  const hue = tickerFallbackHue(normalizedTicker);
  return (
    <div
      className="flex h-7 w-7 shrink-0 items-center justify-center rounded-full border border-white/10 text-[10px] font-bold text-white shadow-[0_0_0_2px_rgba(0,0,0,0.65)]"
      style={{
        background: `linear-gradient(135deg, hsl(${hue},65%,45%), hsl(${(hue + 40) % 360},65%,34%))`,
      }}
    >
      {normalizedTicker.slice(0, 2)}
    </div>
  );
}

function ActivityActorAvatar({
  signal,
  actorVisual,
}: {
  signal: AccountMatchedSignal;
  actorVisual?: ActivityActorVisual | undefined;
}) {
  const actorName =
    signal.actorName || (signal.isCluster ? "Cluster" : "Signal");
  const memberId = signal.actorMemberId || actorVisual?.memberId || null;

  if (signal.isCluster) {
    return (
      <div className="flex h-[42px] w-[42px] shrink-0 items-center justify-center rounded-full border border-emerald-400/20 bg-emerald-500/10 text-emerald-300 ring-2 ring-black/60 shadow-[0_0_18px_rgba(16,185,129,0.08)]">
        <UserRound className="h-4 w-4" />
      </div>
    );
  }

  if (signal.actorName) {
    return (
      <PoliticianHeadshot
        memberId={memberId}
        name={actorName}
        party={actorVisual?.party || null}
        size={42}
        className="border-white/10 ring-2 ring-black/60"
      />
    );
  }

  const Icon = signal.isCluster ? Bell : TrendingUp;
  return (
    <div className="flex h-[42px] w-[42px] shrink-0 items-center justify-center rounded-full border border-white/10 bg-white/[0.04] text-zinc-500 ring-2 ring-black/60">
      <Icon className="h-4 w-4" />
    </div>
  );
}

function directionTone(direction: string | null) {
  const normalized = String(direction || "").toLowerCase();
  if (!normalized) {
    return null;
  }
  if (
    normalized.includes("sell") ||
    normalized.includes("sale") ||
    normalized.includes("decrease") ||
    normalized.includes("exit")
  ) {
    return {
      label: normalized.includes("exit") ? "Exit" : "Sell",
      className: "border-red-500/20 bg-red-500/10 text-red-300",
    };
  }
  if (
    normalized.includes("buy") ||
    normalized.includes("purchase") ||
    normalized.includes("increase") ||
    normalized.includes("new")
  ) {
    return {
      label: normalized.includes("increase") ? "Increase" : "Buy",
      className: "border-emerald-500/20 bg-emerald-500/10 text-emerald-300",
    };
  }
  return {
    label: signalTypeLabel(normalized),
    className: "border-white/[0.08] bg-white/[0.035] text-zinc-400",
  };
}

function clusterDirection(
  direction: string | null,
): AlertClusterPreview["direction"] {
  const normalized = String(direction || "").toLowerCase();
  if (/(sell|sale|decrease|exit)/.test(normalized)) {
    return "sell";
  }
  if (/(buy|purchase|increase|new|accumulation)/.test(normalized)) {
    return "buy";
  }
  return null;
}

function signalDirection(signal: AccountMatchedSignal): "buy" | "sell" | null {
  return clusterDirection(
    [
      signal.direction,
      signal.title,
      signal.summary,
      signal.signalType,
      ...signal.behaviorLabels,
    ]
      .filter(Boolean)
      .join(" "),
  );
}

function DirectionFilterButton({
  active,
  label,
  onClick,
  tone = "default",
}: {
  active: boolean;
  label: string;
  onClick: () => void;
  tone?: "default" | "buy" | "sell";
}) {
  const activeClass =
    tone === "buy"
      ? "border-emerald-500/25 bg-emerald-500/10 text-emerald-300"
      : tone === "sell"
        ? "border-red-500/25 bg-red-500/10 text-red-300"
        : "border-white/[0.12] bg-white/[0.045] text-zinc-200";

  return (
    <button
      type="button"
      onClick={onClick}
      className={`rounded-full border px-3 py-1 text-[11px] font-medium transition ${
        active
          ? activeClass
          : "border-white/[0.06] bg-white/[0.02] text-zinc-500 hover:border-white/[0.12] hover:text-zinc-300"
      }`}
    >
      {label}
    </button>
  );
}

function inferClusterActorCount(signal: AccountMatchedSignal) {
  const text = [signal.title, signal.summary].filter(Boolean).join(" ");
  const match = text.match(
    /\b(\d+)\s+(?:distinct\s+)?(?:insiders|members|actors|sources|politicians|funds)\b/i,
  );
  return match ? Number(match[1]) : 1;
}

function clusterRuleLabel(signal: AccountMatchedSignal) {
  const signalType = String(signal.signalType || "").toLowerCase();
  if (signalType.includes("cross")) {
    return "Cross-source";
  }
  if (signalType.includes("insider")) {
    return "Insider cluster";
  }
  if (signalType.includes("congress") || signalType.includes("politician")) {
    return "Congress cluster";
  }
  return (
    signal.behaviorLabels.find((label) =>
      label.toLowerCase().includes("cluster"),
    ) || "Cluster"
  );
}

function clusterPreviewFromSignal(
  signal: AccountMatchedSignal,
): AlertClusterPreview {
  return {
    id: signal.id,
    ticker: signal.ticker || "N/A",
    title: signal.title,
    summary: signal.summary || "",
    ruleLabel: clusterRuleLabel(signal),
    actorPreview: signal.actorName,
    actorCount: inferClusterActorCount(signal),
    amountLabel: null,
    sourceLabel: signal.source || "Cluster",
    publishedAt: signal.publishedAt || signal.occurredAt,
    direction: clusterDirection(signal.direction),
  };
}

function FollowActivityRow({
  signal,
  actorVisual,
  onOpenCluster,
}: {
  signal: AccountMatchedSignal;
  actorVisual?: ActivityActorVisual | undefined;
  onOpenCluster?: (signal: AccountMatchedSignal) => void;
}) {
  const activityDate = signalActivityDate(signal);
  const direction = directionTone(signal.direction);
  const typeLabel = signal.isCluster
    ? "Cluster"
    : signalTypeLabel(signal.signalType);
  const openCluster = signal.isCluster ? onOpenCluster : undefined;
  const isClickableCluster = Boolean(openCluster);

  return (
    <div
      role={isClickableCluster ? "button" : undefined}
      tabIndex={isClickableCluster ? 0 : undefined}
      onClick={() => {
        openCluster?.(signal);
      }}
      onKeyDown={(event) => {
        if (!openCluster || (event.key !== "Enter" && event.key !== " ")) {
          return;
        }
        event.preventDefault();
        openCluster(signal);
      }}
      className={`group flex items-center gap-3 border-b border-white/[0.06] bg-white/[0.012] px-4 py-3 transition last:border-0 hover:bg-white/[0.035] ${
        isClickableCluster
          ? "cursor-pointer focus:outline-none focus:ring-1 focus:ring-emerald-500/25"
          : ""
      }`}
    >
      <div className="relative h-[48px] w-[56px] shrink-0">
        <ActivityActorAvatar signal={signal} actorVisual={actorVisual} />
        <div className="absolute -bottom-0.5 right-0">
          <ActivityTickerLogo ticker={signal.ticker} />
        </div>
      </div>

      <div className="min-w-0 flex-1">
        <div className="flex min-w-0 items-center gap-2 text-[11px] text-zinc-500">
          <span className="shrink-0 rounded-md border border-white/[0.06] bg-white/[0.02] px-2 py-0.5 text-[10px] font-medium text-zinc-400">
            {typeLabel}
          </span>
          {signal.ticker ? (
            <span className="shrink-0 font-semibold tracking-[0.14em] text-cyan-200">
              {signal.ticker}
            </span>
          ) : null}
          {activityDate ? (
            <span className="hidden truncate sm:inline">
              {formatDateTimeValue(activityDate)}
            </span>
          ) : null}
        </div>

        <div className="mt-1 flex min-w-0 items-center gap-2">
          <div className="truncate text-sm font-medium text-white transition group-hover:text-[#10b981]">
            {signal.title}
          </div>
          {direction ? (
            <span
              className={`hidden shrink-0 rounded-full border px-2 py-0.5 text-[10px] font-semibold sm:inline-flex ${direction.className}`}
            >
              {direction.label}
            </span>
          ) : null}
        </div>

        {signal.summary ? (
          <div className="mt-0.5 truncate text-xs leading-5 text-zinc-500">
            {signal.summary}
          </div>
        ) : null}
      </div>

      <div className="shrink-0">
        {signal.sourceUrl ? (
          <a
            href={signal.sourceUrl}
            target="_blank"
            rel="noreferrer"
            onClick={(event) => event.stopPropagation()}
            className="inline-flex shrink-0 items-center gap-1.5 text-[11px] text-cyan-400 transition hover:text-cyan-300"
          >
            <span className="hidden sm:inline">Source</span>
            <ChevronRight className="h-3 w-3" />
          </a>
        ) : null}
      </div>
    </div>
  );
}

function SentAlertRow({ item }: { item: GroupedSentAlert }) {
  const deliveryLabel = item.channels.map(channelLabel).join(" + ");
  const alertMeta = [
    deliveryLabel,
    formatDateTimeValue(item.alertAt),
    item.ticker || item.actorName,
  ].filter(Boolean);

  return (
    <div className="group flex items-center gap-3 border-b border-white/[0.06] bg-white/[0.012] px-4 py-3 transition last:border-0 hover:bg-white/[0.035]">
      <div className="flex h-[42px] w-[42px] shrink-0 items-center justify-center rounded-full border border-emerald-500/15 bg-emerald-500/10 text-emerald-300 ring-2 ring-black/60">
        <Send className="h-4 w-4" />
      </div>

      <div className="min-w-0 flex-1">
        <div className="flex min-w-0 items-center gap-2 text-[11px] text-zinc-500">
          <span className="shrink-0 rounded-md border border-emerald-500/20 bg-emerald-500/10 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.12em] text-emerald-300">
            Sent alert
          </span>
          <span className="truncate">{alertMeta.join(" • ")}</span>
        </div>

        <div className="mt-1 truncate text-sm font-medium text-white transition group-hover:text-[#10b981]">
          {item.title}
        </div>
        {item.summary ? (
          <div className="mt-0.5 truncate text-xs leading-5 text-zinc-500">
            {item.summary}
          </div>
        ) : null}
      </div>

      <div className="shrink-0">
        <div className="flex items-center gap-1.5">
          {item.channels.map((channel) => (
            <span
              key={`${item.id}-${channel}`}
              className={`inline-flex items-center gap-1 rounded-full border px-2 py-0.5 text-[10px] font-semibold ${
                channel === "sms"
                  ? "border-cyan-500/20 bg-cyan-500/10 text-cyan-300"
                  : "border-blue-500/20 bg-blue-500/10 text-blue-300"
              }`}
            >
              {channel === "sms" ? (
                <MessageSquare className="h-3 w-3" />
              ) : (
                <Mail className="h-3 w-3" />
              )}
              {channelLabel(channel)}
            </span>
          ))}
          {item.sourceUrl ? (
            <a
              href={item.sourceUrl}
              target="_blank"
              rel="noreferrer"
              className="inline-flex shrink-0 items-center gap-1.5 text-[11px] text-cyan-400 transition hover:text-cyan-300"
            >
              <span className="hidden sm:inline">Source</span>
              <ChevronRight className="h-3 w-3" />
            </a>
          ) : null}
        </div>
      </div>
    </div>
  );
}

function RecentFollowActivity({
  state,
  loading,
  historyLoading,
  error,
  accessToken,
}: {
  state: AccountState;
  loading?: boolean;
  historyLoading?: boolean;
  error?: string;
  accessToken?: string;
}) {
  const [mode, setMode] = useState<"matched" | "sent">("matched");
  const [filter, setFilter] = useState<"all" | "trades" | "clusters">("all");
  const [directionFilter, setDirectionFilter] =
    useState<DirectionFilter>("all");
  const [visibleCount, setVisibleCount] = useState(12);
  const [selectedCluster, setSelectedCluster] =
    useState<AlertClusterPreview | null>(null);
  const [clusterDetail, setClusterDetail] =
    useState<DashboardClusterDetail | null>(null);
  const [clusterDetailLoading, setClusterDetailLoading] = useState(false);
  const [clusterDetailError, setClusterDetailError] = useState("");
  const activity = state.alertPreview.timeline;
  const matchedCountLabel =
    loading && activity.length === 0 ? "…" : activity.length;
  const sentAlerts = useMemo(
    () => groupedSentAlerts(state.history),
    [state.history],
  );
  const filteredActivity = useMemo(() => activity.filter((signal) => {
    if (filter === "clusters" && !signal.isCluster) {
      return false;
    }
    if (filter === "trades" && signal.isCluster) {
      return false;
    }
    if (
      directionFilter !== "all" &&
      signalDirection(signal) !== directionFilter
    ) {
      return false;
    }
    return true;
  }), [activity, directionFilter, filter]);
  const visibleActivity = useMemo(
    () => filteredActivity.slice(0, visibleCount),
    [filteredActivity, visibleCount],
  );
  const actorVisuals = useMemo(() => new Map(
    state.follows.actors.map(
      (follow) =>
        [
          compactActorKey(follow.actorName),
          actorVisualForFollow(follow),
        ] as const,
    ),
  ), [state.follows.actors]);

  useEffect(() => {
    if (!selectedCluster) {
      return;
    }

    const controller = new AbortController();
    let cancelled = false;

    fetch(
      `/api/dashboard-cluster?key=${encodeURIComponent(selectedCluster.id)}`,
      {
        signal: controller.signal,
        headers: accessToken ? { Authorization: `Bearer ${accessToken}` } : undefined,
      },
    )
      .then(async (response) => {
        if (!response.ok) {
          const payload = (await response.json().catch(() => null)) as {
            error?: string;
          } | null;
          throw new Error(
            payload?.error || "Could not load this cluster right now.",
          );
        }
        return (await response.json()) as DashboardClusterDetail;
      })
      .then((payload) => {
        if (!cancelled) {
          setClusterDetail(payload);
        }
      })
      .catch((loadError) => {
        if (
          cancelled ||
          (loadError instanceof Error && loadError.name === "AbortError")
        ) {
          return;
        }
        setClusterDetailError(
          loadError instanceof Error
            ? loadError.message
            : "Could not load this cluster right now.",
        );
      })
      .finally(() => {
        if (!cancelled) {
          setClusterDetailLoading(false);
        }
      });

    return () => {
      cancelled = true;
      controller.abort();
    };
  }, [accessToken, selectedCluster]);

  function openCluster(signal: AccountMatchedSignal) {
    setClusterDetail(null);
    setClusterDetailError("");
    setClusterDetailLoading(true);
    setSelectedCluster(clusterPreviewFromSignal(signal));
  }

  return (
    <>
      <section className="overflow-hidden rounded-2xl border border-white/[0.07] bg-white/[0.015]">
        <div className="flex items-center justify-between gap-4 border-b border-white/[0.06] px-5 py-4">
          <div className="flex items-center gap-2">
            <Radio className="h-3.5 w-3.5 text-emerald-400" />
            <h2 className="text-sm font-semibold text-white">Recent alerts</h2>
          </div>
          <div className="flex items-center gap-3">
            <div className="inline-flex rounded-full border border-white/[0.07] bg-black/20 p-1">
              {(
                [
                  ["matched", "Matched", matchedCountLabel],
                  ["sent", "Sent", historyLoading ? "…" : sentAlerts.length],
                ] as const
              ).map(([key, label, count]) => (
                <button
                  key={key}
                  type="button"
                  onClick={() => {
                    setMode(key);
                    if (key === "matched") {
                      setVisibleCount(12);
                    }
                  }}
                  className={`rounded-full px-3 py-1 text-[11px] font-medium transition ${
                    mode === key
                      ? "bg-emerald-500/12 text-emerald-300"
                      : "text-zinc-500 hover:text-zinc-300"
                  }`}
                >
                  {label}{" "}
                  <span className="text-[10px] text-zinc-500">{count}</span>
                </button>
              ))}
            </div>
            <div className="text-[10px] uppercase tracking-[0.14em] text-zinc-600">
              {loading && mode === "matched"
                ? "Scanning data..."
                : mode === "matched"
                  ? `${filteredActivity.length} matched`
                  : historyLoading
                    ? "Loading sent..."
                    : `${sentAlerts.length} sent`}
            </div>
          </div>
        </div>

        {mode === "matched" ? (
          <div className="flex flex-wrap items-center gap-2 border-b border-white/[0.06] px-5 py-3">
            <div className="flex flex-wrap items-center gap-2">
              {(
                [
                  ["all", "All"],
                  ["trades", "Trades"],
                  ["clusters", "Clusters"],
                ] as const
              ).map(([key, label]) => (
                <button
                  key={key}
                  type="button"
                  onClick={() => {
                    setFilter(key);
                    setVisibleCount(12);
                  }}
                  className={`rounded-full border px-3 py-1 text-[11px] font-medium transition ${
                    filter === key
                      ? "border-emerald-500/25 bg-emerald-500/10 text-emerald-300"
                      : "border-white/[0.06] bg-white/[0.02] text-zinc-500 hover:border-white/[0.12] hover:text-zinc-300"
                  }`}
                >
                  {label}
                </button>
              ))}
            </div>
            <div className="hidden h-5 w-px bg-white/[0.08] sm:block" />
            <div className="flex flex-wrap items-center gap-2">
              <DirectionFilterButton
                active={directionFilter === "all"}
                label="Both"
                onClick={() => {
                  setDirectionFilter("all");
                  setVisibleCount(12);
                }}
              />
              <DirectionFilterButton
                active={directionFilter === "buy"}
                label="Buy"
                tone="buy"
                onClick={() => {
                  setDirectionFilter("buy");
                  setVisibleCount(12);
                }}
              />
              <DirectionFilterButton
                active={directionFilter === "sell"}
                label="Sell"
                tone="sell"
                onClick={() => {
                  setDirectionFilter("sell");
                  setVisibleCount(12);
                }}
              />
            </div>
          </div>
        ) : (
          <div className="border-b border-white/[0.06] px-5 py-3 text-[11px] text-zinc-500">
            Delivered alerts are grouped by transaction so one row can show{" "}
            <span className="text-zinc-300">Email</span> or{" "}
            <span className="text-zinc-300">Email + Text</span>.
          </div>
        )}

        {mode === "matched" && visibleActivity.length > 0 ? (
          <>
            <div>
              {visibleActivity.map((signal) => (
                <FollowActivityRow
                  key={signal.id}
                  signal={signal}
                  actorVisual={actorVisuals.get(
                    compactActorKey(signal.actorName),
                  )}
                  onOpenCluster={openCluster}
                />
              ))}
            </div>
            {visibleCount < filteredActivity.length ? (
              <div className="border-t border-white/[0.06] px-5 py-4">
                <button
                  type="button"
                  onClick={() => setVisibleCount((count) => count + 12)}
                  className="rounded-lg border border-white/[0.08] bg-white/[0.025] px-3 py-1.5 text-xs font-medium text-zinc-400 transition hover:border-white/[0.14] hover:text-white"
                >
                  Load more alerts
                </button>
              </div>
            ) : null}
          </>
        ) : mode === "sent" && sentAlerts.length > 0 ? (
          <div>
            {sentAlerts.map((item) => (
              <SentAlertRow key={item.id} item={item} />
            ))}
          </div>
        ) : mode === "sent" && historyLoading ? (
          <div className="px-5 py-8 text-sm text-zinc-500">
            Loading sent alerts...
          </div>
        ) : loading && mode === "matched" ? (
          <div className="px-5 py-8 text-sm text-zinc-500">
            Loading recent activity...
          </div>
        ) : error && mode === "matched" ? (
          <div className="px-5 py-8 text-sm text-amber-400">{error}</div>
        ) : (
          <div className="px-5 py-8 text-sm text-zinc-500">
            {mode === "sent"
              ? "No sent alerts yet."
              : filter === "all"
                ? "No recent data found for the people or tickers you follow yet."
                : `No recent ${filter} matched your follows.`}
          </div>
        )}
      </section>

      <DashboardClusterModal
        cluster={selectedCluster}
        detail={clusterDetail}
        loading={clusterDetailLoading}
        error={clusterDetailError}
        open={Boolean(selectedCluster)}
        onClose={() => {
          setSelectedCluster(null);
          setClusterDetail(null);
          setClusterDetailError("");
          setClusterDetailLoading(false);
        }}
      />
    </>
  );
}

// ─── Edit Signal Modal ────────────────────────────────────────────────────────

type EditingSignal = {
  kind: "ticker" | "actor";
  id: string;
  label: string;
  sublabel: string;
  type: "stock" | "politician" | "insider" | "fund";
  alertMode: AlertMode;
};

function EditSignalModal({
  signal,
  api,
  onUpdated,
  onDeleted,
  onClose,
}: {
  signal: EditingSignal;
  api: AccountApiClient;
  onUpdated: (state: AccountState) => void;
  onDeleted: (state: AccountState) => void;
  onClose: () => void;
}) {
  const alertModeToNotify = (m: AlertMode): NotifyMode =>
    m === "activity" ? "all" : m === "unusual" ? "unusual" : "custom";

  const [notifyMode, setNotifyMode] = useState<NotifyMode>(
    alertModeToNotify(signal.alertMode),
  );
  const [personFilters, setPersonFilters] = useState({
    ...DEFAULT_PERSON_FILTERS,
  });
  const [stockFilters, setStockFilters] = useState({
    ...DEFAULT_STOCK_FILTERS,
  });
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const [confirmDelete, setConfirmDelete] = useState(false);

  function getAlertMode(): AlertMode {
    if (notifyMode === "all") return "activity";
    if (notifyMode === "unusual") return "unusual";
    return "both";
  }

  async function handleSave() {
    setSaving(true);
    setError("");
    try {
      const p = await api("/api/account/follows", {
        method: "PATCH",
        body: JSON.stringify({
          kind: signal.kind,
          id: signal.id,
          alertMode: getAlertMode(),
        }),
      });
      if (p.state) onUpdated(p.state);
      onClose();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to update.");
    } finally {
      setSaving(false);
    }
  }

  async function handleDelete() {
    setSaving(true);
    setError("");
    try {
      const p = await api("/api/account/follows", {
        method: "DELETE",
        body: JSON.stringify({ kind: signal.kind, id: signal.id }),
      });
      if (p.state) onDeleted(p.state);
      onClose();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to delete.");
    } finally {
      setSaving(false);
    }
  }

  const typeIcon =
    signal.type === "stock"
      ? TrendingUp
      : signal.type === "politician"
        ? Landmark
        : signal.type === "fund"
          ? Building2
          : ShieldAlert;
  const typeColor =
    signal.type === "stock"
      ? "text-amber-400 bg-amber-500/10"
      : signal.type === "politician"
        ? "text-blue-400 bg-blue-500/10"
        : signal.type === "fund"
          ? "text-emerald-300 bg-emerald-500/10"
          : "text-violet-400 bg-violet-500/10";
  const TypeIcon = typeIcon;

  return (
    <div className="fixed inset-0 z-50 flex items-start justify-center p-4 pt-[12vh]">
      <div
        className="absolute inset-0 bg-black/60 backdrop-blur-sm"
        onClick={onClose}
      />

      <div className="relative w-full max-w-md overflow-hidden rounded-2xl border border-white/[0.08] bg-[#111111] shadow-2xl shadow-black/50">
        {/* Header with signal info */}
        <div className="border-b border-white/[0.06] px-5 py-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <div
                className={`flex h-10 w-10 shrink-0 items-center justify-center rounded-xl ${typeColor}`}
              >
                <TypeIcon className="h-4 w-4" />
              </div>
              <div>
                <h2 className="text-[15px] font-semibold text-white">
                  {signal.label}
                </h2>
                <p className="text-[11px] text-zinc-500">{signal.sublabel}</p>
              </div>
            </div>
            <button
              onClick={onClose}
              className="flex h-7 w-7 items-center justify-center rounded-md text-zinc-500 transition hover:bg-white/5 hover:text-white"
            >
              <X className="h-4 w-4" />
            </button>
          </div>
        </div>

        <div className="max-h-[60vh] overflow-y-auto">
          <div className="space-y-4 px-5 py-4">
            {/* Notify Mode */}
            <div>
              <label className="mb-1.5 block text-[10px] font-semibold uppercase tracking-[0.12em] text-zinc-600">
                Notify me on
              </label>
              <div className="flex gap-2">
                <Pill
                  active={notifyMode === "all"}
                  onClick={() => setNotifyMode("all")}
                >
                  All activity
                </Pill>
                <Pill
                  active={notifyMode === "unusual"}
                  onClick={() => setNotifyMode("unusual")}
                >
                  Unusual only
                </Pill>
                <Pill
                  active={notifyMode === "custom"}
                  onClick={() => setNotifyMode("custom")}
                >
                  Custom
                </Pill>
              </div>
            </div>

            {/* Custom Filters (context-aware) */}
            {notifyMode === "custom" && signal.type === "stock" && (
              <CustomFiltersForStock
                api={api}
                filters={stockFilters}
                onChange={setStockFilters}
              />
            )}
            {notifyMode === "custom" &&
              signal.type !== "stock" &&
              signal.type !== "fund" && (
                <CustomFiltersForPerson
                  signalType={signal.type as "politician" | "insider"}
                  filters={personFilters}
                  onChange={setPersonFilters}
                />
              )}

            {/* Error */}
            {error && (
              <div className="flex items-center gap-2 rounded-lg border border-red-500/20 bg-red-500/5 px-3 py-2 text-xs text-red-400">
                <AlertTriangle className="h-3.5 w-3.5 shrink-0" /> {error}
              </div>
            )}
          </div>
        </div>

        {/* Footer — Save + Delete */}
        <div className="flex items-center justify-between border-t border-white/[0.06] px-5 py-3.5">
          <div>
            {!confirmDelete ? (
              <button
                type="button"
                onClick={() => setConfirmDelete(true)}
                disabled={saving}
                className="flex items-center gap-1.5 rounded-lg px-3 py-1.5 text-xs text-zinc-600 transition hover:bg-red-500/10 hover:text-red-400"
              >
                <Trash2 className="h-3 w-3" /> Remove
              </button>
            ) : (
              <div className="flex items-center gap-2">
                <button
                  type="button"
                  onClick={handleDelete}
                  disabled={saving}
                  className="rounded-lg bg-red-500/15 px-3 py-1.5 text-xs font-medium text-red-400 transition hover:bg-red-500/25"
                >
                  Confirm delete
                </button>
                <button
                  type="button"
                  onClick={() => setConfirmDelete(false)}
                  className="rounded-lg px-2 py-1.5 text-xs text-zinc-600 transition hover:text-zinc-300"
                >
                  Cancel
                </button>
              </div>
            )}
          </div>
          <div className="flex items-center gap-3">
            <button
              type="button"
              onClick={onClose}
              className="rounded-lg px-4 py-2 text-sm text-zinc-400 transition hover:text-white"
            >
              Cancel
            </button>
            <button
              type="button"
              onClick={handleSave}
              disabled={saving}
              className="rounded-xl bg-[#10b981] px-5 py-2.5 text-sm font-semibold text-black transition hover:bg-[#34d399] disabled:opacity-40"
            >
              {saving ? "Saving…" : "Save"}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

// ─── Signal Row ───────────────────────────────────────────────────────────────

function SignalRow({
  label,
  sublabel,
  icon: Icon,
  iconColor,
  alertMode,
  onClick,
  onDelete,
  saving,
  highlight = false,
}: {
  label: string;
  sublabel: string;
  icon: React.ElementType;
  iconColor: string;
  alertMode: AlertMode;
  onClick: () => void;
  onDelete: () => void;
  saving: boolean;
  highlight?: boolean;
}) {
  const modeLabel =
    alertMode === "activity"
      ? "All activity"
      : alertMode === "unusual"
        ? "Unusual only"
        : "Custom";

  return (
    <div
      className={`group flex items-center gap-3 rounded-xl border bg-white/[0.02] px-4 py-3 transition hover:border-white/[0.1] ${
        highlight
          ? "border-emerald-400/20 shadow-[0_0_24px_rgba(16,185,129,0.07)] animate-[pulse_700ms_ease-out_1]"
          : "border-white/[0.06]"
      }`}
    >
      <button
        type="button"
        onClick={onClick}
        className="flex min-w-0 flex-1 items-center gap-3 text-left"
      >
        <div
          className={`flex h-8 w-8 shrink-0 items-center justify-center rounded-lg ${iconColor}`}
        >
          <Icon className="h-3.5 w-3.5" />
        </div>
        <div className="min-w-0 flex-1">
          <div className="truncate text-sm font-medium text-white group-hover:text-[#10b981] transition">
            {label}
          </div>
          <div className="truncate text-[11px] text-zinc-600">{sublabel}</div>
        </div>
        <ChevronRight className="h-3.5 w-3.5 shrink-0 text-zinc-700 transition group-hover:text-zinc-400" />
      </button>
      <div className="flex items-center gap-2 border-l border-white/[0.06] pl-3">
        <span className="rounded-md border border-white/[0.06] bg-white/[0.02] px-2 py-0.5 text-[10px] font-medium text-zinc-500">
          {modeLabel}
        </span>
        <button
          onClick={(e) => {
            e.stopPropagation();
            onDelete();
          }}
          disabled={saving}
          className="flex h-7 w-7 items-center justify-center rounded-lg text-zinc-700 transition hover:bg-red-500/10 hover:text-red-400"
        >
          <Trash2 className="h-3 w-3" />
        </button>
      </div>
    </div>
  );
}

// ─── Main ─────────────────────────────────────────────────────────────────────

export function SignalsPage({
  recentSignalsSource = "deliveryHistory",
}: {
  recentSignalsSource?: "deliveryHistory" | "followActivity";
} = {}) {
  const [session, setSession] = useState<Session | null>(null);
  const [sessionLoading, setSessionLoading] = useState(true);
  const [state, setState] = useState<AccountState | null>(null);
  const [loading, setLoading] = useState(false);
  const [activityLoading, setActivityLoading] = useState(false);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [activityError, setActivityError] = useState("");
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const [showCreate, setShowCreate] = useState(false);
  const [editingSignal, setEditingSignal] = useState<EditingSignal | null>(
    null,
  );

  const [emailInput, setEmailInput] = useState("");
  const [emailEnabled, setEmailEnabled] = useState(true);
  const [textPhone, setTextPhone] = useState("");
  const [textEnabled, setTextEnabled] = useState(false);

  const api = useAccountApi(session);

  const syncInputsFromState = useCallback((nextState: AccountState) => {
    setEmailInput(nextState.profile.alertEmail || nextState.user.email || "");
    setEmailEnabled(nextState.profile.emailEnabled);
    setTextPhone(nextState.profile.textPhone || "");
    setTextEnabled(nextState.profile.textEnabled);
  }, []);

  const applyAccountState = useCallback(
    (
      nextState: AccountState,
      options: { preserveExistingActivity?: boolean; syncInputs?: boolean } = {},
    ) => {
      setState((current) => {
        let merged = nextState;
        if (
          options.preserveExistingActivity &&
          current?.user.id === nextState.user.id
        ) {
          merged = {
            ...nextState,
            alertPreview:
              !hasPreviewData(nextState) && hasPreviewData(current)
                ? current.alertPreview
                : nextState.alertPreview,
            history:
              nextState.history.length === 0 && current.history.length > 0
                ? current.history
                : nextState.history,
          };
        }
        writeAlertsStateCache(merged);
        return merged;
      });

      if (options.syncInputs !== false) {
        syncInputsFromState(nextState);
      }
    },
    [syncInputsFromState],
  );

  useEffect(() => {
    let mounted = true;
    supabase.auth.getSession().then(({ data }) => {
      if (!mounted) return;
      setSession(data.session);
      setSessionLoading(false);
    });
    const { data } = supabase.auth.onAuthStateChange((_ev, s) => {
      setSession(s);
      setSessionLoading(false);
    });
    return () => {
      mounted = false;
      data.subscription.unsubscribe();
    };
  }, []);

  useEffect(() => {
    if (!session?.user.id) {
      clearAlertsStateCache();
      return;
    }

    const cachedState = readAlertsStateCache(session.user.id);
    if (!cachedState) {
      return;
    }

    setState(cachedState);
    syncInputsFromState(cachedState);
  }, [session?.user.id, syncInputsFromState]);

  const mergeAlertPreview = useCallback((alertPreview: AccountAlertPreview) => {
    setState((current) => {
      if (!current) return current;
      const next = { ...current, alertPreview };
      writeAlertsStateCache(next);
      return next;
    });
  }, []);

  const mergeHistory = useCallback((history: AccountState["history"]) => {
    setState((current) => {
      if (!current) return current;
      const next = { ...current, history };
      writeAlertsStateCache(next);
      return next;
    });
  }, []);

  const loadFollowActivity = useCallback(async () => {
    if (!session) return;
    setActivityLoading(true);
    setActivityError("");
    try {
      const p = await api("/api/account/alert-preview");
      if (p.alertPreview) {
        mergeAlertPreview(p.alertPreview);
      }
    } catch {
      setActivityError(
        "Recent activity is taking longer than expected. Your signals still loaded.",
      );
    } finally {
      setActivityLoading(false);
    }
  }, [api, mergeAlertPreview, session]);

  const loadHistory = useCallback(async () => {
    if (!session) return;
    setHistoryLoading(true);
    try {
      const p = await api("/api/account/history");
      if (Array.isArray(p.history)) {
        mergeHistory(p.history);
      }
    } catch {
      // History is secondary to the page shell; keep the UI usable if it lags.
    } finally {
      setHistoryLoading(false);
    }
  }, [api, mergeHistory, session]);

  const loadState = useCallback(async () => {
    if (!session) return;
    setLoading(true);
    setError("");
    try {
      const lightweight = recentSignalsSource === "followActivity";
      const p = await api(
        lightweight ? "/api/account/state?preview=0&history=0" : "/api/account/state",
      );
      if (!p.state) throw new Error("Missing state");
      applyAccountState(p.state, { preserveExistingActivity: lightweight });
      if (lightweight) {
        void loadHistory();
        void loadFollowActivity();
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load.");
    } finally {
      setLoading(false);
    }
  }, [session, api, recentSignalsSource, applyAccountState, loadHistory, loadFollowActivity]);

  useEffect(() => {
    if (session) loadState();
    else if (!sessionLoading) setState(null);
  }, [session, sessionLoading, loadState]);

  async function deleteFollow(kind: "ticker" | "actor", id: string) {
    setSaving(true);
    setError("");
    setMessage("");
    try {
      const p = await api("/api/account/follows", {
        method: "DELETE",
        body: JSON.stringify({ kind, id }),
      });
      if (p.state) {
        applyAccountState(p.state, { preserveExistingActivity: true });
      }
      setMessage("Signal removed.");
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed.");
    } finally {
      setSaving(false);
    }
  }

  async function deleteClusterFollow() {
    setSaving(true);
    setError("");
    setMessage("");
    try {
      await api("/api/account/cluster-alerts", {
        method: "POST",
        body: JSON.stringify({ enabled: false }),
      });
      await loadState();
      setMessage("Cluster alert removed.");
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed.");
    } finally {
      setSaving(false);
    }
  }

  async function saveEmail() {
    setSaving(true);
    setError("");
    setMessage("");
    try {
      const p = await api("/api/account/delivery/email", {
        method: "POST",
        body: JSON.stringify({ alertEmail: emailInput, enabled: emailEnabled }),
      });
      if (p.state) {
        applyAccountState(p.state, { preserveExistingActivity: true });
      }
      setMessage("Email settings saved.");
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed.");
    } finally {
      setSaving(false);
    }
  }

  async function saveText() {
    setSaving(true);
    setError("");
    setMessage("");
    try {
      const p = await api("/api/account/delivery/sms", {
        method: "POST",
        body: JSON.stringify({ phoneNumber: textPhone, enabled: textEnabled }),
      });
      if (p.state) {
        applyAccountState(p.state, { preserveExistingActivity: true });
      }
      setMessage("Text settings saved.");
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed.");
    } finally {
      setSaving(false);
    }
  }

  async function sendTest() {
    setSaving(true);
    setError("");
    setMessage("");
    try {
      const p = await api("/api/account/test-alert", { method: "POST" });
      if (p.state) {
        applyAccountState(p.state, { preserveExistingActivity: true });
      }
      const sent = p.result?.sentChannels || [];
      setMessage(
        sent.length ? `Test sent via ${sent.join(" and ")}.` : "Test sent.",
      );
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed.");
    } finally {
      setSaving(false);
    }
  }

  const hasDelivery = Boolean(
    state &&
    ((state.profile.emailEnabled && state.subscriptions.email.destination) ||
      (state.profile.textEnabled && state.subscriptions.sms.destination)),
  );
  const totalFollows = state?.followCount || 0;
  const connectedChannels = state
    ? Number(
        Boolean(
          state.profile.emailEnabled && state.subscriptions.email.destination,
        ),
      ) +
      Number(
        Boolean(
          state.profile.textEnabled && state.subscriptions.sms.destination,
        ),
      )
    : 0;
  const activityPreviewReady = state ? hasPreviewData(state) : false;
  const matchedSummaryLabel =
    state && activityLoading && !activityPreviewReady
      ? "…"
      : String(state?.alertPreview.matchedSignalCount || 0);
  const clusterSummaryLabel =
    state && activityLoading && !activityPreviewReady
      ? "…"
      : String(state?.alertPreview.clusterSignalCount || 0);

  if (sessionLoading) {
    return (
      <div className="mx-auto max-w-2xl px-4 py-12">
        <div className="rounded-2xl border border-white/[0.06] bg-white/[0.02] p-8 text-center text-sm text-zinc-500">
          Loading…
        </div>
      </div>
    );
  }

  if (!session) {
    return (
      <div className="mx-auto max-w-2xl px-4 py-16 text-center sm:px-6">
        <div className="inline-flex items-center gap-2 rounded-full border border-[#10b981]/20 bg-[#10b981]/10 px-3 py-1 text-[10px] font-semibold uppercase tracking-[0.2em] text-[#10b981]">
          <Zap className="h-3 w-3" /> Signals
        </div>
        <h1 className="mt-4 text-3xl font-semibold tracking-tight text-white">
          Never miss a move.
        </h1>
        <p className="mx-auto mt-2 max-w-md text-sm text-zinc-500">
          Get notified when politicians trade, insiders file, or hedge funds
          shift positions. Set up in seconds.
        </p>
        <Link
          href="/auth"
          className="mt-6 inline-flex items-center gap-2 rounded-xl bg-[#10b981] px-5 py-2.5 text-sm font-semibold text-black transition hover:bg-[#34d399]"
        >
          Sign in to get started <ArrowRight className="h-4 w-4" />
        </Link>
      </div>
    );
  }

  return (
    <>
      <div className="mx-auto max-w-6xl px-4 py-8 sm:px-6">
        <div className="flex flex-col gap-4 sm:flex-row sm:items-end sm:justify-between">
          <div>
            <h1 className="text-3xl font-semibold tracking-tight text-white">
              Alerts
            </h1>
            <p className="mt-1.5 text-sm text-zinc-500">
              Recent activity from the people and stocks you follow.
            </p>
          </div>
          <button
            onClick={() => setShowCreate(true)}
            className="flex items-center justify-center gap-2 rounded-lg bg-[#10b981] px-3.5 py-2 text-xs font-semibold text-black transition hover:bg-[#34d399]"
          >
            <Plus className="h-4 w-4" /> New alert
          </button>
        </div>

        {message && (
          <div className="mt-4 flex items-center gap-2 rounded-lg border border-emerald-500/20 bg-emerald-500/5 px-3 py-2 text-xs text-emerald-400">
            <CheckCircle2 className="h-3.5 w-3.5 shrink-0" /> {message}
          </div>
        )}
        {error && (
          <div className="mt-4 flex items-center gap-2 rounded-lg border border-red-500/20 bg-red-500/5 px-3 py-2 text-xs text-red-400">
            <AlertTriangle className="h-3.5 w-3.5 shrink-0" /> {error}
          </div>
        )}

        {loading && !state ? (
          <div className="mt-8 rounded-2xl border border-white/[0.06] bg-white/[0.02] p-12 text-center text-sm text-zinc-500">
            Loading…
          </div>
        ) : state ? (
          <div>
            <div className="mt-6 flex flex-wrap items-center gap-x-5 gap-y-2 border-y border-white/[0.06] py-3 text-[11px] text-zinc-500">
              <span>
                <strong className="font-semibold text-zinc-200">
                  {totalFollows}
                </strong>{" "}
                following
              </span>
              <span>
                <strong className="font-semibold text-zinc-200">
                  {matchedSummaryLabel}
                </strong>{" "}
                recent alerts
              </span>
              <span>
                <strong className="font-semibold text-zinc-200">
                  {clusterSummaryLabel}
                </strong>{" "}
                clusters
              </span>
              <span className="flex items-center gap-1.5">
                <span
                  className={`h-1.5 w-1.5 rounded-full ${connectedChannels ? "bg-emerald-400" : "bg-zinc-700"}`}
                />
                {connectedChannels
                  ? `${connectedChannels} delivery ${connectedChannels === 1 ? "channel" : "channels"}`
                  : "Delivery off"}
              </span>
            </div>

            <div className="mt-5 grid items-stretch gap-5 lg:grid-cols-2">
              <section className="flex min-h-[360px] flex-col overflow-hidden rounded-2xl border border-white/[0.07] bg-white/[0.015]">
                <div className="flex items-center justify-between border-b border-white/[0.06] px-5 py-4">
                  <div>
                    <div className="flex items-center gap-2 text-sm font-semibold text-white">
                      <Bell className="h-3.5 w-3.5" />
                      Following
                    </div>
                  </div>
                  <div className="text-[11px] text-zinc-600">
                    {state.followCount} / {state.followLimit}
                  </div>
                </div>

                <div className="flex-1 p-4">
                  {totalFollows === 0 ? (
                    <div className="rounded-xl border border-dashed border-white/[0.08] px-6 py-12 text-center">
                      <BellPlus className="mx-auto h-8 w-8 text-zinc-700" />
                      <div className="mt-3 text-sm text-zinc-400">
                        No active alerts
                      </div>
                      <div className="mt-1 text-xs text-zinc-600">
                        Start with a stock, politician, or insider.
                      </div>
                      <button
                        onClick={() => setShowCreate(true)}
                        className="mt-4 inline-flex items-center gap-1.5 rounded-lg border border-[#10b981]/20 bg-[#10b981]/10 px-3 py-1.5 text-xs font-medium text-[#10b981] transition hover:bg-[#10b981]/20"
                      >
                        <Plus className="h-3 w-3" /> Create your first alert
                      </button>
                    </div>
                  ) : (
                    <div className="space-y-2">
                      {state.follows.cluster ? (
                        <SignalRow
                          label={state.follows.cluster.label}
                          sublabel="Cluster feed"
                          icon={BellPlus}
                          iconColor="text-emerald-300 bg-emerald-500/10"
                          alertMode="activity"
                          onClick={() => undefined}
                          onDelete={deleteClusterFollow}
                          saving={saving}
                          highlight
                        />
                      ) : null}
                      {state.follows.tickers.map((f) => (
                        <SignalRow
                          key={f.id}
                          label={f.ticker}
                          sublabel="Stock — politician, insider & fund activity"
                          icon={TrendingUp}
                          iconColor="text-amber-400 bg-amber-500/10"
                          alertMode={f.alertMode}
                          onClick={() =>
                            setEditingSignal({
                              kind: "ticker",
                              id: f.id,
                              label: f.ticker,
                              sublabel:
                                "Stock — politician, insider & fund activity",
                              type: "stock",
                              alertMode: f.alertMode,
                            })
                          }
                          onDelete={() => deleteFollow("ticker", f.id)}
                          saving={saving}
                        />
                      ))}
                      {state.follows.actors.map((f) => {
                        const actorSublabel =
                          f.actorType === "politician"
                            ? "Congress member"
                            : f.actorType === "fund"
                              ? "Hedge fund"
                              : "Corporate insider";
                        const ActorIcon =
                          f.actorType === "politician"
                            ? Landmark
                            : f.actorType === "fund"
                              ? Building2
                              : ShieldAlert;
                        const actorIconColor =
                          f.actorType === "politician"
                            ? "text-blue-400 bg-blue-500/10"
                            : f.actorType === "fund"
                              ? "text-emerald-300 bg-emerald-500/10"
                              : "text-violet-400 bg-violet-500/10";

                        return (
                          <SignalRow
                            key={f.id}
                            label={f.actorName}
                            sublabel={actorSublabel}
                            icon={ActorIcon}
                            iconColor={actorIconColor}
                            alertMode={f.alertMode}
                            onClick={() =>
                              setEditingSignal({
                                kind: "actor",
                                id: f.id,
                                label: f.actorName,
                                sublabel: actorSublabel,
                                type: f.actorType,
                                alertMode: f.alertMode,
                              })
                            }
                            onDelete={() => deleteFollow("actor", f.id)}
                            saving={saving}
                          />
                        );
                      })}
                    </div>
                  )}
                </div>
              </section>

              <aside className="flex min-h-[360px] flex-col overflow-hidden rounded-2xl border border-white/[0.07] bg-white/[0.015]">
                <div className="flex items-center justify-between gap-3 border-b border-white/[0.06] px-5 py-4">
                  <div className="flex items-center gap-2 text-sm font-semibold text-white">
                    <Settings2 className="h-3.5 w-3.5 text-zinc-500" />
                    Delivery
                  </div>
                  <div className="text-[11px] text-zinc-600">
                    {connectedChannels} / 2
                  </div>
                </div>

                <div className="flex-1 space-y-3 p-4">
                  <div className="rounded-xl border border-white/[0.06] bg-black/15 p-3">
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-2 text-sm font-medium text-white">
                        <Mail className="h-3.5 w-3.5 text-blue-400" /> Email
                      </div>
                      <label className="flex items-center gap-1.5 text-[10px] text-zinc-500">
                        <input
                          type="checkbox"
                          checked={emailEnabled}
                          onChange={(e) => setEmailEnabled(e.target.checked)}
                          className="h-3 w-3 rounded border-white/10 bg-[#0b1020]"
                        />
                        On
                      </label>
                    </div>
                    <input
                      type="email"
                      value={emailInput}
                      onChange={(e) => setEmailInput(e.target.value)}
                      placeholder="you@example.com"
                      className="mt-2.5 w-full rounded-lg border border-white/[0.06] bg-white/[0.02] px-3 py-2 text-xs text-white outline-none placeholder:text-zinc-600 focus:border-blue-500/30"
                    />
                    <button
                      onClick={saveEmail}
                      disabled={saving}
                      className="mt-2 rounded-md bg-blue-500/15 px-3 py-1 text-[10px] font-semibold text-blue-400 transition hover:bg-blue-500/25 disabled:opacity-50"
                    >
                      Save email
                    </button>
                  </div>

                  <div className="rounded-xl border border-white/[0.06] bg-black/15 p-3">
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-2 text-sm font-medium text-white">
                        <MessageSquare className="h-3.5 w-3.5 text-cyan-400" />{" "}
                        Text
                      </div>
                      <label className="flex items-center gap-1.5 text-[10px] text-zinc-500">
                        <input
                          type="checkbox"
                          checked={textEnabled}
                          onChange={(e) => setTextEnabled(e.target.checked)}
                          className="h-3 w-3 rounded border-white/10 bg-[#0b1020]"
                        />
                        On
                      </label>
                    </div>
                    <input
                      type="tel"
                      value={textPhone}
                      onChange={(e) => setTextPhone(e.target.value)}
                      placeholder="+1 555 123 4567"
                      className="mt-2.5 w-full rounded-lg border border-white/[0.06] bg-white/[0.02] px-3 py-2 text-xs text-white outline-none placeholder:text-zinc-600 focus:border-cyan-500/30"
                    />
                    <div className="mt-1.5 text-[10px] text-zinc-700">
                      {state.subscriptions.sms.destination
                        ? "Phone saved"
                        : "Add a phone number to enable text alerts"}
                    </div>
                    <button
                      onClick={saveText}
                      disabled={saving}
                      className="mt-2 rounded-md bg-cyan-500/15 px-3 py-1 text-[10px] font-semibold text-cyan-400 transition hover:bg-cyan-500/25 disabled:opacity-50"
                    >
                      Save text
                    </button>
                  </div>

                  {hasDelivery ? (
                    <button
                      onClick={sendTest}
                      disabled={saving}
                      className="flex w-full items-center justify-center gap-2 rounded-lg border border-[#10b981]/20 bg-[#10b981]/5 px-3 py-2 text-[11px] font-medium text-[#10b981] transition hover:bg-[#10b981]/15 disabled:opacity-50"
                    >
                      <Send className="h-3.5 w-3.5" />
                      Send test alert
                    </button>
                  ) : (
                    <div className="rounded-lg border border-amber-500/15 bg-amber-500/5 px-3 py-2 text-[11px] leading-5 text-amber-400">
                      Connect at least one channel to receive alerts.
                    </div>
                  )}
                </div>
              </aside>
            </div>

            <div className="mt-5">
              {recentSignalsSource === "followActivity" ? (
                <RecentFollowActivity
                  state={state}
                  loading={activityLoading}
                  historyLoading={historyLoading}
                  error={activityError}
                  accessToken={session?.access_token}
                />
              ) : (
                <section className="overflow-hidden rounded-2xl border border-white/[0.07] bg-white/[0.015]">
                  <div className="border-b border-white/[0.06] px-5 py-4">
                    <div className="flex items-center gap-2 text-sm font-semibold text-white">
                      <Bell className="h-3.5 w-3.5 text-emerald-400" />
                      Recent deliveries
                    </div>
                  </div>
                  {state.history.length > 0 ? (
                    <div>
                      {state.history.map((item) => (
                        <div
                          key={item.id}
                          className="flex flex-wrap items-start justify-between gap-3 border-b border-white/[0.06] px-5 py-4 last:border-0"
                        >
                          <div className="min-w-0 flex-1">
                            <div className="truncate text-sm font-medium text-white">
                              {item.title || "Signal delivery"}
                            </div>
                            <div className="mt-1 flex flex-wrap items-center gap-2 text-[11px] text-zinc-500">
                              <span>{channelLabel(item.channel)}</span>
                              <span className="text-white/10">·</span>
                              <span>
                                {formatDateTimeValue(
                                  item.sentAt || item.queuedAt,
                                )}
                              </span>
                              {item.ticker || item.actorName ? (
                                <span>· {item.ticker || item.actorName}</span>
                              ) : null}
                            </div>
                          </div>
                          <div
                            className={`rounded-full border px-2.5 py-1 text-[10px] font-semibold uppercase tracking-[0.15em] ${signalStatusTone(item.status)}`}
                          >
                            {item.status}
                          </div>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <div className="px-5 py-8 text-sm text-zinc-500">
                      No alerts sent yet.
                    </div>
                  )}
                </section>
              )}
            </div>
          </div>
        ) : null}
      </div>

      {showCreate && session && (
        <CreateSignalModal
          session={session}
          onCreated={(s) => {
            applyAccountState(s, { preserveExistingActivity: true });
            setMessage("Signal created.");
          }}
          onClose={() => setShowCreate(false)}
        />
      )}

      {editingSignal && (
        <EditSignalModal
          signal={editingSignal}
          api={api}
          onUpdated={(s) => {
            applyAccountState(s, { preserveExistingActivity: true });
            setMessage("Signal updated.");
          }}
          onDeleted={(s) => {
            applyAccountState(s, { preserveExistingActivity: true });
            setMessage("Signal removed.");
          }}
          onClose={() => setEditingSignal(null)}
        />
      )}
    </>
  );
}
