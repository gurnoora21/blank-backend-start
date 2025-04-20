
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { supabase } from "../shared/api-clients.ts";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

// Schedule types and their respective cron patterns
const SCHEDULES = {
  'discover-artists': '0 * * * *', // Every hour
  'maintenance': '*/15 * * * *',   // Every 15 minutes
  'worker': '*/2 * * * *',         // Every 2 minutes
  'monitor': '*/30 * * * *'        // Every 30 minutes
};

function getCurrentCronMinute() {
  const now = new Date();
  return now.getMinutes();
}

function shouldRunSchedule(cronPattern: string) {
  const currentMinute = getCurrentCronMinute();
  
  // Simple cron parser for our limited patterns
  if (cronPattern === '* * * * *') {
    return true; // Every minute
  }
  
  if (cronPattern.startsWith('*/')) {
    // Every X minutes pattern
    const interval = parseInt(cronPattern.split('/')[1].split(' ')[0]);
    return currentMinute % interval === 0;
  }
  
  if (/^\d+\s/.test(cronPattern)) {
    // Specific minute pattern
    const minute = parseInt(cronPattern.split(' ')[0]);
    return currentMinute === minute;
  }
  
  return false;
}

async function invokeFunction(functionName: string) {
  try {
    const response = await fetch(
      `${Deno.env.get('SUPABASE_URL')}/functions/v1/${functionName}`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${Deno.env.get('SUPABASE_ANON_KEY')}`,
        },
        body: JSON.stringify({ scheduled: true, timestamp: new Date().toISOString() })
      }
    );
    
    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`Function ${functionName} failed: ${errorText}`);
    }
    
    return { 
      function: functionName, 
      status: 'invoked', 
      timestamp: new Date().toISOString() 
    };
  } catch (error) {
    console.error(`Error invoking ${functionName}:`, error);
    return { 
      function: functionName, 
      status: 'error', 
      error: error.message,
      timestamp: new Date().toISOString()
    };
  }
}

// Run the right schedules based on current time
async function runScheduler() {
  const results = [];
  
  for (const [functionName, cronPattern] of Object.entries(SCHEDULES)) {
    if (shouldRunSchedule(cronPattern)) {
      console.log(`Scheduling function ${functionName} based on pattern ${cronPattern}`);
      const result = await invokeFunction(functionName);
      results.push(result);
    }
  }
  
  return results;
}

serve(async (req) => {
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }
  
  try {
    const results = await runScheduler();
    
    return new Response(
      JSON.stringify({ 
        timestamp: new Date().toISOString(),
        minute: getCurrentCronMinute(),
        results 
      }),
      { 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        status: 200 
      }
    );
  } catch (error) {
    console.error('Scheduler error:', error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        status: 500 
      }
    );
  }
});
