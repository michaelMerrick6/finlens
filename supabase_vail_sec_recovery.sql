-- Apply before deploying SEC recovery workers. Additive, service-role-only RPCs.
BEGIN;
CREATE TABLE IF NOT EXISTS public.sec_filing_queue (
 accession text PRIMARY KEY CHECK (accession ~ '^[0-9]{10}-[0-9]{2}-[0-9]{6}$'),
 filing jsonb NOT NULL,
 status text NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','failed','complete')),
 last_error text,
 next_attempt_at timestamptz NOT NULL DEFAULT now(),
 updated_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE public.sec_filing_queue ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.sec_filing_queue FROM anon, authenticated;
GRANT ALL ON public.sec_filing_queue TO service_role;
CREATE INDEX IF NOT EXISTS sec_filing_queue_due ON public.sec_filing_queue(status,next_attempt_at);
CREATE OR REPLACE FUNCTION public.sec_accession(url text) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$ SELECT substring(url from '([0-9]{10}-[0-9]{2}-[0-9]{6})'); $$;
CREATE INDEX IF NOT EXISTS insider_accession_idx ON public.insider_trades(public.sec_accession(source_url));

CREATE OR REPLACE FUNCTION public.publish_insider_filing(target_accession text, trades jsonb)
RETURNS integer LANGUAGE plpgsql SECURITY DEFINER SET search_path=public,pg_temp AS $$
DECLARE old_rows jsonb; new_rows jsonb;
BEGIN
 IF target_accession !~ '^[0-9]{10}-[0-9]{2}-[0-9]{6}$' OR jsonb_typeof(trades) IS DISTINCT FROM 'array' THEN
  RAISE EXCEPTION 'Invalid filing or payload';
 END IF;
 PERFORM pg_advisory_xact_lock(hashtextextended('insider:'||target_accession,0));
 IF NOT EXISTS(SELECT 1 FROM sec_filing_queue WHERE accession=target_accession) THEN RAISE EXCEPTION 'Register filing before publication'; END IF;
 IF EXISTS(SELECT 1 FROM jsonb_populate_recordset(NULL::insider_trades,trades) x
  WHERE public.sec_accession(x.source_url) IS DISTINCT FROM target_accession
    OR x.transaction_code NOT IN ('buy','sell') OR x.transaction_code IS NULL
    OR x.transaction_date>x.published_date) THEN RAISE EXCEPTION 'Invalid filing rows'; END IF;
 SELECT coalesce(jsonb_agg(j ORDER BY j),'[]'::jsonb) INTO old_rows
  FROM (SELECT to_jsonb(t)-'id'-'created_at' j FROM insider_trades t WHERE public.sec_accession(source_url)=target_accession) q;
 SELECT coalesce(jsonb_agg(j ORDER BY j),'[]'::jsonb) INTO new_rows
  FROM (SELECT to_jsonb(t)-'id'-'created_at' j FROM jsonb_populate_recordset(NULL::insider_trades,trades) t) q;
 IF old_rows IS DISTINCT FROM new_rows THEN
  DELETE FROM insider_trades WHERE public.sec_accession(source_url)=target_accession;
  INSERT INTO insider_trades(ticker,filer_name,filer_relation,transaction_date,published_date,transaction_code,amount,price,value,source_url)
  SELECT ticker,filer_name,filer_relation,transaction_date,published_date,transaction_code,amount,price,value,source_url
  FROM jsonb_populate_recordset(NULL::insider_trades,trades);
 END IF;
 UPDATE sec_filing_queue SET status='complete',last_error=NULL,updated_at=now() WHERE accession=target_accession;
 RETURN jsonb_array_length(trades);
END $$;

CREATE OR REPLACE FUNCTION public.replace_13f_period(target_fund text,target_period date,holdings jsonb)
RETURNS integer LANGUAGE plpgsql SECURITY DEFINER SET search_path=public,pg_temp AS $$
BEGIN
 IF jsonb_typeof(holdings) IS DISTINCT FROM 'array' OR jsonb_array_length(holdings)=0 THEN RAISE EXCEPTION 'Refusing empty holdings replacement'; END IF;
 IF EXISTS(SELECT 1 FROM jsonb_populate_recordset(NULL::institutional_holdings,holdings) x
  WHERE x.fund_name IS DISTINCT FROM target_fund OR x.report_period IS DISTINCT FROM target_period OR x.ticker IS NULL) THEN
  RAISE EXCEPTION 'Holdings do not match target period';
 END IF;
 PERFORM pg_advisory_xact_lock(hashtextextended('13f:'||target_fund||':'||target_period::text,0));
 DELETE FROM institutional_holdings WHERE fund_name=target_fund AND report_period=target_period;
 INSERT INTO institutional_holdings(fund_name,ticker,report_period,published_date,shares_held,value_held,qoq_change_shares,qoq_change_percent,source_url)
 SELECT fund_name,ticker,report_period,published_date,shares_held,value_held,qoq_change_shares,qoq_change_percent,source_url
 FROM jsonb_populate_recordset(NULL::institutional_holdings,holdings);
 RETURN jsonb_array_length(holdings);
END $$;
REVOKE ALL ON FUNCTION public.publish_insider_filing(text,jsonb),public.replace_13f_period(text,date,jsonb) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.publish_insider_filing(text,jsonb),public.replace_13f_period(text,date,jsonb) TO service_role;
NOTIFY pgrst,'reload schema';
COMMIT;
