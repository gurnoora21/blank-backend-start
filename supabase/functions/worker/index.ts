
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { supabase } from "../shared/api-clients.ts";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

const MAX_CONCURRENT_JOBS = 3;
const RETRY_LIMITS = {
  'discover-artists': 3,
  'album_page': 5,
  'track_page': 5,
  'producer_discovery': 3,
  'default': 3
};

// Structure for logging
interface LogEntry {
  timestamp: string;
  request_id?: string;
  function: string;
  event: string;
  batch_id?: string;
  status?: string;
  latency_ms?: number;
  error?: string;
  metadata?: any;
}

function log(entry: LogEntry) {
  console.log(JSON.stringify(entry));
}

// Centralized error handler with retry logic
async function handleError(batchId: string, error: Error, batchType: string) {
  try {
    // Get current batch data to check retry count
    const { data: batch } = await supabase
      .from('processing_batches')
      .select('retry_count, metadata')
      .eq('id', batchId)
      .single();
    
    const retryCount = (batch?.retry_count || 0) + 1;
    const retryLimit = RETRY_LIMITS[batchType as keyof typeof RETRY_LIMITS] || RETRY_LIMITS.default;
    
    if (retryCount < retryLimit) {
      // Calculate backoff delay: 500ms * 2^retryCount
      const backoffDelay = 500 * Math.pow(2, retryCount - 1);
      
      // Update batch with retry information
      await supabase
        .from('processing_batches')
        .update({
          status: 'pending',
          retry_count: retryCount,
          error_message: error.message,
          // Schedule for future processing with backoff
          updated_at: new Date(Date.now() + backoffDelay).toISOString()
        })
        .eq('id', batchId);
      
      log({
        timestamp: new Date().toISOString(),
        function: 'worker',
        event: 'batch_retry_scheduled',
        batch_id: batchId,
        status: 'pending',
        metadata: { retry_count: retryCount, backoff_delay_ms: backoffDelay }
      });
    } else {
      // Move to dead letter queue
      await supabase
        .from('processing_batches')
        .update({
          status: 'error',
          error_message: error.message,
          completed_at: new Date().toISOString()
        })
        .eq('id', batchId);
      
      // Add to dead letter queue
      await supabase
        .from('dead_letter_items')
        .insert({
          item_type: batchType,
          error_message: error.message,
          original_batch_id: batchId,
          original_item_id: batchId,
          metadata: batch?.metadata || {}
        });
      
      log({
        timestamp: new Date().toISOString(),
        function: 'worker',
        event: 'batch_moved_to_dlq',
        batch_id: batchId,
        status: 'error',
        error: error.message
      });
    }
  } catch (dbError) {
    console.error("Error handling batch failure:", dbError);
  }
}

// Process a single batch by invoking the appropriate function
async function processBatch(batchId: string, batchType: string, metadata: any) {
  const startTime = Date.now();
  
  try {
    let functionName;
    
    // Map batch types to function names
    switch (batchType) {
      case 'album_discovery':
        functionName = 'process-album-page';
        break;
      case 'album_page':
        functionName = 'process-album-page';
        break;
      case 'track_discovery':
      case 'track_page':
        functionName = 'process-track-page';
        break;
      case 'producer_discovery':
        functionName = 'identify-producers';
        break;
      default:
        functionName = batchType;
    }
    
    // Invoke the edge function
    const response = await fetch(
      `${Deno.env.get('SUPABASE_URL')}/functions/v1/${functionName}`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${Deno.env.get('SUPABASE_ANON_KEY')}`,
        },
        body: JSON.stringify(metadata)
      }
    );
    
    const latency = Date.now() - startTime;
    
    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`Function ${functionName} failed: ${errorText}`);
    }
    
    // Mark batch as completed
    await supabase
      .from('processing_batches')
      .update({
        status: 'completed',
        completed_at: new Date().toISOString(),
        items_processed: 1,
        items_total: 1
      })
      .eq('id', batchId);
    
    log({
      timestamp: new Date().toISOString(),
      function: 'worker',
      event: 'batch_completed',
      batch_id: batchId,
      status: 'completed',
      latency_ms: latency
    });
    
    return true;
  } catch (error) {
    log({
      timestamp: new Date().toISOString(),
      function: 'worker',
      event: 'batch_error',
      batch_id: batchId,
      status: 'error',
      latency_ms: Date.now() - startTime,
      error: error.message
    });
    
    await handleError(batchId, error, batchType);
    return false;
  }
}

// Main handler to claim and process batches
async function claimAndProcessBatches() {
  try {
    // Check how many batches are currently in progress
    const { data: inProgressCount } = await supabase
      .from('processing_batches')
      .select('id', { count: 'exact', head: true })
      .eq('status', 'processing');
    
    if ((inProgressCount || 0) >= MAX_CONCURRENT_JOBS) {
      log({
        timestamp: new Date().toISOString(),
        function: 'worker',
        event: 'max_concurrent_jobs_reached',
        metadata: { current: inProgressCount, max: MAX_CONCURRENT_JOBS }
      });
      return { claimed: 0 };
    }
    
    // Define how many jobs we can claim
    const jobsToFetch = MAX_CONCURRENT_JOBS - (inProgressCount || 0);
    
    // Claim batches atomically
    const { data: batches, error } = await supabase.rpc('claim_processing_batches', {
      p_limit: jobsToFetch
    });
    
    if (error) {
      throw error;
    }
    
    if (!batches || batches.length === 0) {
      return { claimed: 0 };
    }
    
    log({
      timestamp: new Date().toISOString(),
      function: 'worker',
      event: 'batches_claimed',
      metadata: { count: batches.length }
    });
    
    // Process all claimed batches
    const processingPromises = batches.map(batch => 
      processBatch(batch.id, batch.batch_type, batch.metadata)
    );
    
    const results = await Promise.allSettled(processingPromises);
    
    return {
      claimed: batches.length,
      completed: results.filter(r => r.status === 'fulfilled' && r.value).length,
      failed: results.filter(r => r.status === 'rejected' || !r.value).length
    };
  } catch (error) {
    log({
      timestamp: new Date().toISOString(),
      function: 'worker',
      event: 'claim_error',
      error: error.message
    });
    
    return { claimed: 0, error: error.message };
  }
}

serve(async (req) => {
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }
  
  try {
    const startTime = Date.now();
    
    const result = await claimAndProcessBatches();
    
    log({
      timestamp: new Date().toISOString(),
      function: 'worker',
      event: 'worker_run_completed',
      latency_ms: Date.now() - startTime,
      metadata: result
    });
    
    return new Response(
      JSON.stringify(result),
      { 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        status: 200 
      }
    );
  } catch (error) {
    console.error('Worker error:', error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        status: 500 
      }
    );
  }
});
