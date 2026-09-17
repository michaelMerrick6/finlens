-- Public disclosure-derived data only; only the service role may write.
create table if not exists public.politician_holdings_sync (
 member_id text primary key,
 snapshot jsonb not null,
 updated_at timestamptz not null default now()
);
alter table public.politician_holdings_sync enable row level security;
drop policy if exists "Public holdings snapshots" on public.politician_holdings_sync;
create policy "Public holdings snapshots" on public.politician_holdings_sync for select using (true);
grant select on public.politician_holdings_sync to anon, authenticated;
revoke insert, update, delete on public.politician_holdings_sync from anon, authenticated;
grant all on public.politician_holdings_sync to service_role;
