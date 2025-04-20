
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { supabase } from "../shared/api-clients.ts";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

serve(async (req) => {
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    const startTime = Date.now();
    
    // Log the start of maintenance
    console.log(JSON.stringify({
      timestamp: new Date().toISOString(),
      function: 'maintenance',
      event: 'maintenance_started'
    }));
    
    // Reset expired batches
    const { data: resetCount, error: resetError } = await supabase
      .rpc('reset_expired_batches', { p_expiry_minutes: 30 });
      
    if (resetError) throw resetError;

    // Requeue dead letter items
    const { data: requeueCount, error: requeueError } = await supabase
      .rpc('requeue_dead_letter_items', { p_limit: 100 });
      
    if (requeueError) throw requeueError;
    
    // Clean up completed batches older than 7 days
    const { data: cleanupCount, error: cleanupError } = await supabase
      .rpc('cleanup_old_batches', { p_days_old: 7 });
      
    if (cleanupError) throw cleanupError;
    
    // Log completion
    console.log(JSON.stringify({
      timestamp: new Date().toISOString(),
      function: 'maintenance',
      event: 'maintenance_completed',
      latency_ms: Date.now() - startTime,
      metadata: { 
        resetCount, 
        requeueCount,
        cleanupCount 
      }
    }));

    return new Response(
      JSON.stringify({ 
        success: true, 
        resetCount, 
        requeueCount,
        cleanupCount,
        duration_ms: Date.now() - startTime
      }), {
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
      }
    );
  } catch (error) {
    console.error(JSON.stringify({
      timestamp: new Date().toISOString(),
      function: 'maintenance',
      event: 'maintenance_error',
      error: error.message
    }));
    
    return new Response(
      JSON.stringify({ error: error.message }), {
        status: 500,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
      }
    );
  }
});
