-- Supabase Database Schema

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 1. Companies Table
CREATE TABLE public.companies (
    ticker VARCHAR(10) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    sector VARCHAR(100),
    industry VARCHAR(100),
    market_cap BIGINT,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 2. Politician Trades Table
CREATE TABLE public.politician_trades (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    politician_name VARCHAR(255) NOT NULL,
    chamber VARCHAR(50) NOT NULL, -- 'House' or 'Senate'
    party VARCHAR(50),
    ticker VARCHAR(10) REFERENCES public.companies(ticker),
    transaction_date DATE NOT NULL,
    published_date DATE NOT NULL,
    transaction_type VARCHAR(50) NOT NULL, -- 'Purchase', 'Sale (Full)', 'Sale (Partial)'
    asset_type VARCHAR(50),
    amount_range VARCHAR(100) NOT NULL, -- e.g., '$1,001 - $15,000'
    source_url TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 3. Insider Trades (Form 4) Table
CREATE TABLE public.insider_trades (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ticker VARCHAR(10) REFERENCES public.companies(ticker),
    filer_name VARCHAR(255) NOT NULL,
    filer_relation VARCHAR(255) NOT NULL, -- e.g., 'CEO', 'Director', '10% Owner'
    transaction_date DATE NOT NULL,
    published_date DATE NOT NULL,
    transaction_code VARCHAR(10) NOT NULL, -- e.g., 'P' for Purchase, 'S' for Sale
    amount NUMERIC, -- Number of shares
    price NUMERIC, -- Price per share
    value NUMERIC, -- Total value (amount * price)
    source_url TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 4. Institutional Holdings (13F-HR) Table
CREATE TABLE public.institutional_holdings (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    fund_name VARCHAR(255) NOT NULL,
    ticker VARCHAR(10) REFERENCES public.companies(ticker),
    report_period DATE NOT NULL, -- The end of the quarter
    published_date DATE NOT NULL,
    shares_held BIGINT NOT NULL,
    value_held BIGINT,
    qoq_change_shares BIGINT, -- Change in shares from previous quarter
    qoq_change_percent NUMERIC, -- Percentage change from previous quarter
    source_url TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(fund_name, ticker, report_period)
);

-- Indexes for performance
CREATE INDEX idx_politician_trades_ticker ON public.politician_trades(ticker);
CREATE INDEX idx_insider_trades_ticker ON public.insider_trades(ticker);
CREATE INDEX idx_institutional_holdings_ticker ON public.institutional_holdings(ticker);

-- RLS (Row Level Security) - Read Only for anonymous users (the frontend)
ALTER TABLE public.companies ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.politician_trades ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.insider_trades ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.institutional_holdings ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Allow public read-only access" ON public.companies FOR SELECT USING (true);
CREATE POLICY "Allow public read-only access" ON public.politician_trades FOR SELECT USING (true);
CREATE POLICY "Allow public read-only access" ON public.insider_trades FOR SELECT USING (true);
CREATE POLICY "Allow public read-only access" ON public.institutional_holdings FOR SELECT USING (true);
