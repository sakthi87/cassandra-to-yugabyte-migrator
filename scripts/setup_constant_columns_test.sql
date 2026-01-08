-- SQL script to add constant columns to the target table for testing
-- Run this BEFORE running the migration test

-- Add constant columns to the table (if they don't exist)
ALTER TABLE public.dda_pstd_fincl_txn_cnsmr_by_accntnbr 
ADD COLUMN IF NOT EXISTS created_by TEXT;

ALTER TABLE public.dda_pstd_fincl_txn_cnsmr_by_accntnbr 
ADD COLUMN IF NOT EXISTS migration_date DATE;

ALTER TABLE public.dda_pstd_fincl_txn_cnsmr_by_accntnbr 
ADD COLUMN IF NOT EXISTS source_system TEXT;

ALTER TABLE public.dda_pstd_fincl_txn_cnsmr_by_accntnbr 
ADD COLUMN IF NOT EXISTS migration_run_id BIGINT;

-- Verify columns were added
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_schema = 'public' 
  AND table_name = 'dda_pstd_fincl_txn_cnsmr_by_accntnbr'
  AND column_name IN ('created_by', 'migration_date', 'source_system', 'migration_run_id')
ORDER BY column_name;

