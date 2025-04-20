
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
    // Reset expired batches
    const { data: resetCount } = await supabase
      .rpc('reset_expired_batches', { p_expiry_minutes: 30 });

    // Requeue dead letter items
    const { data: requeueCount } = await supabase
      .rpc('requeue_dead_letter_items', { p_limit: 100 });

    return new Response(
      JSON.stringify({ 
        success: true, 
        resetCount, 
        requeueCount 
      }), {
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
      }
    );
  } catch (error) {
    console.error('Error:', error.message);
    return new Response(
      JSON.stringify({ error: error.message }), {
        status: 500,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
      }
    );
  }
});
