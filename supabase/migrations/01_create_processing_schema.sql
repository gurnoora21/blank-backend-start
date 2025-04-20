
-- Function to handle batch claims atomically
CREATE OR REPLACE FUNCTION public.claim_processing_batches(p_limit integer DEFAULT 5)
RETURNS SETOF processing_batches
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  v_worker_id TEXT := gen_random_uuid()::TEXT;
  v_now TIMESTAMPTZ := now();
BEGIN
  RETURN QUERY
  WITH claimable_batches AS (
    SELECT id
    FROM public.processing_batches
    WHERE 
      status = 'pending' AND
      (claim_expires_at IS NULL OR claim_expires_at < v_now)
    ORDER BY 
      retry_count ASC, -- process retries less frequently
      created_at ASC
    LIMIT p_limit
    FOR UPDATE SKIP LOCKED
  ),
  updated_batches AS (
    UPDATE public.processing_batches
    SET 
      status = 'processing',
      claimed_by = v_worker_id,
      claim_expires_at = v_now + interval '5 minutes',
      started_at = COALESCE(started_at, v_now),
      updated_at = v_now
    FROM claimable_batches
    WHERE processing_batches.id = claimable_batches.id
    RETURNING processing_batches.*
  )
  SELECT * FROM updated_batches;
END;
$$;

-- Function to reset expired batches
CREATE OR REPLACE FUNCTION public.reset_expired_batches(p_expiry_minutes integer DEFAULT 30)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  v_count INTEGER;
BEGIN
  UPDATE public.processing_batches
  SET status = 'pending', 
      claimed_by = NULL,
      claim_expires_at = NULL,
      updated_at = now(),
      retry_count = COALESCE(retry_count, 0),
      error_message = COALESCE(error_message, '') || ' Batch expired and was reset.'
  WHERE 
    status = 'processing' AND
    claim_expires_at < (now() - (p_expiry_minutes || ' minutes')::interval);
  
  GET DIAGNOSTICS v_count = ROW_COUNT;
  RETURN v_count;
END;
$$;

-- Function to requeue items from dead letter queue
CREATE OR REPLACE FUNCTION public.requeue_dead_letter_items(p_limit integer DEFAULT 100)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  v_count INTEGER;
  v_item RECORD;
BEGIN
  v_count := 0;
  
  -- Loop through dead letter items to requeue
  FOR v_item IN 
    SELECT * FROM public.dead_letter_items
    WHERE retry_count < 3 -- Only retry a few times
    ORDER BY created_at
    LIMIT p_limit
  LOOP
    -- Create a new batch for this item
    INSERT INTO public.processing_batches 
      (batch_type, status, metadata, retry_count)
    VALUES 
      (v_item.item_type, 'pending', v_item.metadata, COALESCE(v_item.retry_count, 0) + 1);
    
    -- Update the retry count on the dead letter item
    UPDATE public.dead_letter_items
    SET retry_count = COALESCE(retry_count, 0) + 1,
        updated_at = now()
    WHERE id = v_item.id;
    
    v_count := v_count + 1;
  END LOOP;
  
  RETURN v_count;
END;
$$;

-- Function to get queue metrics
CREATE OR REPLACE FUNCTION public.get_queue_depths()
RETURNS json
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  v_result json;
BEGIN
  WITH batch_stats AS (
    SELECT 
      batch_type,
      COUNT(*) FILTER (WHERE status = 'pending') AS pending_count,
      COUNT(*) FILTER (WHERE status = 'processing') AS processing_count,
      COUNT(*) FILTER (WHERE status = 'completed') AS completed_count,
      COUNT(*) FILTER (WHERE status = 'error') AS error_count,
      COUNT(*) FILTER (WHERE status = 'pending' AND created_at < now() - interval '1 hour') AS pending_old_count
    FROM public.processing_batches
    GROUP BY batch_type
  )
  SELECT json_agg(batch_stats)
  INTO v_result
  FROM batch_stats;
  
  RETURN v_result;
END;
$$;

-- Function to clean up old completed batches
CREATE OR REPLACE FUNCTION public.cleanup_old_batches(p_days_old integer DEFAULT 7)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  v_count INTEGER;
BEGIN
  DELETE FROM public.processing_batches
  WHERE 
    status = 'completed' AND
    completed_at < (now() - (p_days_old || ' days')::interval);
  
  GET DIAGNOSTICS v_count = ROW_COUNT;
  RETURN v_count;
END;
$$;

-- Create processing_batches table if it doesn't exist
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'processing_batches') THEN
    -- Create enum for processing status if it doesn't exist
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'processing_status') THEN
      CREATE TYPE processing_status AS ENUM ('pending', 'processing', 'completed', 'error');
    END IF;
    
    -- Create the processing_batches table
    CREATE TABLE public.processing_batches (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      batch_type TEXT NOT NULL,
      status processing_status NOT NULL DEFAULT 'pending',
      priority INTEGER NOT NULL DEFAULT 5,
      retry_count INTEGER NOT NULL DEFAULT 0,
      items_total INTEGER DEFAULT 0,
      items_processed INTEGER DEFAULT 0,
      items_failed INTEGER DEFAULT 0,
      claimed_by TEXT,
      claim_expires_at TIMESTAMPTZ,
      started_at TIMESTAMPTZ,
      completed_at TIMESTAMPTZ,
      error_message TEXT,
      metadata JSONB,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    
    -- Create indexes for efficient querying
    CREATE INDEX idx_processing_batches_status ON public.processing_batches(status);
    CREATE INDEX idx_processing_batches_batch_type ON public.processing_batches(batch_type);
    CREATE INDEX idx_processing_batches_claimed_by ON public.processing_batches(claimed_by);
    CREATE INDEX idx_processing_batches_claim_expires_at ON public.processing_batches(claim_expires_at);
    CREATE INDEX idx_processing_batches_created_at ON public.processing_batches(created_at);
    CREATE INDEX idx_processing_batches_metadata ON public.processing_batches USING gin(metadata);
    
    -- Create a unique constraint for idempotency
    CREATE UNIQUE INDEX idx_batches_unique_metadata 
    ON public.processing_batches (batch_type, md5(metadata::text))
    WHERE status IN ('pending', 'processing');
  END IF;
  
  -- Create dead_letter_items table if it doesn't exist
  IF NOT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'dead_letter_items') THEN
    CREATE TABLE public.dead_letter_items (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      item_type TEXT NOT NULL,
      error_message TEXT,
      original_batch_id UUID NOT NULL,
      original_item_id TEXT NOT NULL,
      retry_count INTEGER DEFAULT 0,
      metadata JSONB,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    
    CREATE INDEX idx_dead_letter_items_type ON public.dead_letter_items(item_type);
    CREATE INDEX idx_dead_letter_items_created_at ON public.dead_letter_items(created_at);
    CREATE INDEX idx_dead_letter_items_original_batch_id ON public.dead_letter_items(original_batch_id);
  END IF;
END;
$$;

-- Create trigger for updating timestamps
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger WHERE tgname = 'set_processing_batches_updated_at'
  ) THEN
    CREATE TRIGGER set_processing_batches_updated_at
    BEFORE UPDATE ON public.processing_batches
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
  END IF;
  
  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger WHERE tgname = 'set_dead_letter_items_updated_at'
  ) THEN
    CREATE TRIGGER set_dead_letter_items_updated_at
    BEFORE UPDATE ON public.dead_letter_items
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
  END IF;
END;
$$;
