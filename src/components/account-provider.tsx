"use client";
import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useState,
  useRef,
  type ReactNode,
  type CSSProperties,
} from "react";
import { createClient, type SupabaseClient, type Session } from "@supabase/supabase-js";
import type { AccountState } from "@/lib/account-types";
import { prepareTrackingChime } from "@/lib/tracking-chime";
import { Modal } from "./modal";
import { Icon } from "./icon";

type Context = {
  account: AccountState | null;
  loading: boolean;
  error: string;
  session: Session | null;
  openSignIn: () => void;
  mutate: (path: string, body: object, method?: string) => Promise<void>;
  signOut: () => Promise<void>;
  refresh: () => void;
  reauthenticate: () => Promise<void>;
  hqAccess: { current: { userId: string; password: string } | null };
};
// Keep one browser auth client across development hot reloads as well as renders.
const browserAuth = globalThis as typeof globalThis & {
  __vailAuthClient?: { url: string; key: string; client: SupabaseClient };
};
function getBrowserClient(url: string, key: string) {
  if (!url || !key) return null;
  if (typeof window === "undefined") return createClient(url, key, { auth: { persistSession: false, autoRefreshToken: false, detectSessionInUrl: false } });
  const cached = browserAuth.__vailAuthClient;
  if (cached?.url === url && cached.key === key) return cached.client;
  const client = createClient(url, key);
  browserAuth.__vailAuthClient = { url, key, client };
  return client;
}
const AccountContext = createContext<Context | null>(null);
export function useAccount() {
  const value = useContext(AccountContext);
  if (!value) throw new Error("Missing account provider");
  return value;
}
export function AccountProvider({
  children,
  url,
  publicKey,
}: {
  children: ReactNode;
  url: string;
  publicKey: string;
}) {
  const [client] = useState(() =>
    getBrowserClient(url, publicKey),
  );
  const hqAccess = useRef<{ userId: string; password: string } | null>(null);
  const activeUser = useRef<string | undefined>(undefined);
  const [session, setSession] = useState<Session | null>(null);
  const [account, setAccount] = useState<AccountState | null>(null);
  const [loading, setLoading] = useState(Boolean(client));
  const [error, setError] = useState("");
  const [revision, setRevision] = useState(0);
  const [open, setOpen] = useState(false);
  const [email, setEmail] = useState("");
  const [sending, setSending] = useState(false);
  const [googleBusy, setGoogleBusy] = useState(false);
  const [sent, setSent] = useState(false);
  const [loginError, setLoginError] = useState("");
  useEffect(() => {
    if (!client) return;
    let active = true;
    client.auth
      .getSession()
      .then(({ data, error }) => {
        if (active) {
          setSession(data.session);
          if (error)
            setError(
              "Your session could not be restored. Please sign in again.",
            );
          setLoading(Boolean(data.session));
        }
      })
      .catch(() => {
        if (active) {
          setError("Your session could not be restored. Please sign in again.");
          setLoading(false);
        }
      });
    const { data } = client.auth.onAuthStateChange((_event, next) => {
      if (!active) return;
      if (!next) setLoading(false);
      if (activeUser.current !== next?.user.id) {
        hqAccess.current = null;
        setAccount(null);
        setError("");
        setLoading(Boolean(next));
      }
      activeUser.current = next?.user.id;
      setSession(next);
      if (next) setOpen(false);
    });
    return () => {
      active = false;
      data.subscription.unsubscribe();
    };
  }, [client]);
  useEffect(() => {
    if (!session) {
      setAccount(null);
      return;
    }
    const controller = new AbortController();
    setLoading(true);
    setError("");
    fetch("/api/account/state?history=0&preview=0", {
      headers: { Authorization: `Bearer ${session.access_token}` },
      signal: controller.signal,
    })
      .then(async (r) => {
        if (!r.ok)
          throw new Error(
            "Unable to load your tracking list. Please try again.",
          );
        return r.json();
      })
      .then((data) => {
        if (!controller.signal.aborted) setAccount(data.state);
      })
      .catch((e) => {
        if (!controller.signal.aborted) setError(e.message);
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });
    return () => controller.abort();
  }, [session, revision]);
  async function mutate(path: string, body: object, method = "POST") {
    if (!session) {
      setOpen(true);
      throw new Error("Sign in to save your tracking list.");
    }
    const r = await fetch(path, {
      method,
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${session.access_token}`,
      },
      body: JSON.stringify(body),
    });
    const data = await r.json();
    if (!r.ok)
      throw new Error(
        r.status === 401
          ? "Please sign in again."
          : data.error || "Could not save your changes.",
      );
    if (data.state) setAccount(data.state);
    else setRevision((n) => n + 1);
  }
  const reauthenticate = useCallback(async () => {
    await client?.auth.signOut({ scope: "local" });
    hqAccess.current = null;
    setSession(null);
    setAccount(null);
    setSent(false);
    setLoginError("Your session has expired. Please sign in again to continue.");
    setOpen(true);
  }, [client]);

  return (
    <AccountContext.Provider
      value={{
        hqAccess,
        account,
        session,
        loading,
        error,
        openSignIn: () => {
          setOpen(true);
          setSent(false);
          setLoginError("");
        },
        mutate,
        refresh: () => setRevision((n) => n + 1),
        reauthenticate,
        signOut: async () => {
          const result = await client?.auth.signOut({ scope: "local" });
          if (result?.error) throw result.error;
          setAccount(null);
          hqAccess.current = null;
          setSession(null);
        },
      }}
    >
      {children}
      {open && (
        <Modal title="Your Vail account" onClose={() => setOpen(false)}>
          <div className="modal-symbol">
            <Icon name="bookmark" size={27} />
          </div>
          <h2>
            Keep your eye on
            <br />
            what matters.
          </h2>
          <p className="muted">
            Sign in to track politicians and choose whether to receive email
            alerts. Browsing is always open.
          </p>
          <button
            type="button"
            className="button secondary full google-sign-in"
            disabled={sending || googleBusy}
            onClick={async () => {
              setGoogleBusy(true);
              setLoginError("");
              try {
                if (!client) throw new Error("Sign-in is temporarily unavailable. Please try again later.");
                const { data, error } = await client.auth.signInWithOAuth({
                  provider: "google",
                  options: {
                    redirectTo: `${window.location.origin}/tracking`,
                    skipBrowserRedirect: true,
                  },
                });
                if (error) throw error;
                if (!data.url) throw new Error("Google sign-in is unavailable. Please try again.");
                window.location.assign(data.url);
              } catch (e) {
                setLoginError(e instanceof Error ? e.message : "Could not start Google sign-in.");
                setGoogleBusy(false);
              }
            }}
          >
            <svg width="20" height="20" viewBox="0 0 24 24" aria-hidden="true">
              <path fill="#4285F4" d="M21.6 12.23c0-.71-.06-1.39-.18-2.05H12v3.88h5.38a4.6 4.6 0 0 1-2 3.02v2.51h3.24c1.9-1.75 2.98-4.32 2.98-7.36Z" />
              <path fill="#34A853" d="M12 22c2.7 0 4.96-.9 6.62-2.41l-3.24-2.51c-.9.6-2.05.96-3.38.96-2.6 0-4.81-1.76-5.6-4.12H3.06v2.59A10 10 0 0 0 12 22Z" />
              <path fill="#FBBC05" d="M6.4 13.92a6 6 0 0 1 0-3.84V7.49H3.06a10 10 0 0 0 0 9.02l3.34-2.59Z" />
              <path fill="#EA4335" d="M12 5.96c1.47 0 2.79.51 3.82 1.51l2.87-2.87A9.6 9.6 0 0 0 12 2a10 10 0 0 0-8.94 5.49l3.34 2.59C7.19 7.72 9.4 5.96 12 5.96Z" />
            </svg>
            {googleBusy ? "Opening Google…" : "Continue with Google"}
          </button>
          <div className="auth-divider"><span>or use email</span></div>
          {sent ? (
            <div className="success" role="status">
              Check your inbox for a secure sign-in link. You can close this
              window and return using the link.
            </div>
          ) : (
            <form
              onSubmit={async (e) => {
                e.preventDefault();
                setSending(true);
                setLoginError("");
                try {
                  if (!client)
                    throw new Error(
                      "Sign-in is temporarily unavailable. Please try again later.",
                    );
                  const { error } = await client.auth.signInWithOtp({
                    email: email.trim(),
                    options: {
                      emailRedirectTo: `${window.location.origin}/tracking`,
                    },
                  });
                  if (error) throw error;
                  setSent(true);
                } catch (e) {
                  setLoginError(
                    e instanceof Error
                      ? e.message
                      : "Could not send a sign-in link.",
                  );
                } finally {
                  setSending(false);
                }
              }}
            >
              <label className="field-label" htmlFor="sign-in-email">
                Email address
              </label>
              <input
                id="sign-in-email"
                type="email"
                autoComplete="email"
                placeholder="you@example.com"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                required
              />
              <button className="button primary full" disabled={sending || googleBusy}>
                {sending ? "Sending link…" : "Email me a sign-in link"}
                <Icon name="arrow" size={17} />
              </button>
            </form>
          )}
          {loginError && <p className="error" role="alert">{loginError}</p>}
          <p className="fine-print">
            No password to remember. Email alerts are a separate choice.
          </p>
        </Modal>
      )}
    </AccountContext.Provider>
  );
}
export function TrackButton({ id, name }: { id: string; name: string }) {
  const [burst, setBurst] = useState(0);
  const { account, session, loading, openSignIn, mutate } = useAccount();
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const tracked = account?.follows.actors.find(
    (a) =>
      a.actorType === "politician" &&
      a.actorKey.toLowerCase() === id.toLowerCase(),
  );
  return (
    <div className="track-control">
      {burst > 0 && (
        <span className="track-confetti" key={burst} aria-hidden="true" onAnimationEnd={() => setBurst(0)}>
          {Array.from({ length: 10 }, (_, i) => {
            const angle = (i / 10) * Math.PI * 2;
            const distance = 32 + (i % 3) * 9;
            return <i key={i} style={{
              "--confetti-x": `${Math.cos(angle) * distance}px`,
              "--confetti-y": `${Math.sin(angle) * distance - 16}px`,
              "--confetti-spin": `${(i % 2 ? 1 : -1) * (60 + i * 12)}deg`,
              background: ["#486d59", "#9eaf83", "#d4b66b", "#b9c9b0"][i % 4],
              borderRadius: i % 3 === 0 ? "50%" : "1px",
            } as CSSProperties} />;
          })}
        </span>
      )}
      <button
        className={`button ${tracked ? "secondary" : "primary"}`}
        disabled={busy || loading}
        onClick={async () => {
          if (!session) {
            openSignIn();
            return;
          }
          if (
            !tracked &&
            account &&
            account.followCount >= account.followLimit
          ) {
            setError(
              `Your list supports ${account.followLimit} tracked items. Remove one in Tracking to add someone new.`,
            );
            return;
          }
          const chime = tracked ? null : prepareTrackingChime();
          setBusy(true);
          setError("");
          try {
            await mutate(
              "/api/account/follows",
              tracked
                ? { kind: "actor", id: tracked.id }
                : {
                    kind: "actor",
                    actorType: "politician",
                    actorName: name,
                    actorKey: id,
                    alertMode: "activity",
                  },
              tracked ? "DELETE" : "POST",
            );
            chime?.play();
            if (!tracked && !window.matchMedia("(prefers-reduced-motion: reduce)").matches) {
              setBurst(n => n + 1);
            }
          } catch (e) {
            chime?.cancel();
            setError(e instanceof Error ? e.message : "Could not save.");
          } finally {
            setBusy(false);
          }
        }}
      >
        <Icon name={tracked ? "check" : "plus"} size={16} />
        {busy ? "Saving…" : tracked ? "Tracking" : "Track politician"}
      </button>
      {error && (
        <p className="error" role="alert">
          {error}
        </p>
      )}
    </div>
  );
}
