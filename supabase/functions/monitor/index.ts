
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { supabase } from "../shared/api-clients.ts";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

// Thresholds for alerts
const ALERT_THRESHOLDS = {
  dead_letter_items: 10,       // Alert if more than 10 dead letter items
  error_batches: 20,           // Alert if more than 20 error batches
  stalled_batches: 5,          // Alert if more than 5 batches stuck in processing
  processing_time_minutes: 30   // Alert if batches processing for > 30 minutes
};

async function checkSystemHealth() {
  const healthReport = {
    timestamp: new Date().toISOString(),
    alerts: [],
    metrics: {}
  };
  
  // Check dead letter items
  const { data: deadLetterCount } = await supabase
    .from('dead_letter_items')
    .select('*', { count: 'exact', head: true })
    .gte('created_at', new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString());
  
  healthReport.metrics.dead_letter_items_24h = deadLetterCount || 0;
  
  if ((deadLetterCount || 0) > ALERT_THRESHOLDS.dead_letter_items) {
    healthReport.alerts.push({
      level: 'warning',
      message: `High number of dead letter items: ${deadLetterCount} in last 24h`,
      metric: 'dead_letter_items_24h',
      threshold: ALERT_THRESHOLDS.dead_letter_items
    });
  }
  
  // Check error batches
  const { data: errorBatchesCount } = await supabase
    .from('processing_batches')
    .select('*', { count: 'exact', head: true })
    .eq('status', 'error')
    .gte('updated_at', new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString());
  
  healthReport.metrics.error_batches_24h = errorBatchesCount || 0;
  
  if ((errorBatchesCount || 0) > ALERT_THRESHOLDS.error_batches) {
    healthReport.alerts.push({
      level: 'warning',
      message: `High number of failed batches: ${errorBatchesCount} in last 24h`,
      metric: 'error_batches_24h',
      threshold: ALERT_THRESHOLDS.error_batches
    });
  }
  
  // Check stalled batches
  const stalledThreshold = new Date(Date.now() - ALERT_THRESHOLDS.processing_time_minutes * 60 * 1000).toISOString();
  
  const { data: stalledBatches } = await supabase
    .from('processing_batches')
    .select('*')
    .eq('status', 'processing')
    .lt('started_at', stalledThreshold);
  
  const stalledCount = stalledBatches?.length || 0;
  healthReport.metrics.stalled_batches = stalledCount;
  
  if (stalledCount > ALERT_THRESHOLDS.stalled_batches) {
    healthReport.alerts.push({
      level: 'critical',
      message: `${stalledCount} batches stuck in processing for >${ALERT_THRESHOLDS.processing_time_minutes} minutes`,
      metric: 'stalled_batches',
      threshold: ALERT_THRESHOLDS.stalled_batches
    });
  }
  
  // Queue depth metrics
  const { data: queueDepths } = await supabase.rpc('get_queue_depths');
  if (queueDepths) {
    healthReport.metrics.queue_depths = queueDepths;
  }
  
  // API rate limit usage
  const { data: rateLimits } = await supabase
    .from('api_rate_limits')
    .select('*');
  
  if (rateLimits) {
    const rateLimitMetrics = {};
    
    rateLimits.forEach(limit => {
      const remainingPercent = limit.requests_limit 
        ? (limit.requests_remaining / limit.requests_limit) * 100 
        : null;
      
      rateLimitMetrics[`${limit.api_name}_${limit.endpoint}`] = {
        remaining: limit.requests_remaining,
        limit: limit.requests_limit,
        remaining_percent: remainingPercent,
        reset_at: limit.reset_at
      };
      
      // Alert if rate limit is low
      if (remainingPercent !== null && remainingPercent < 20) {
        healthReport.alerts.push({
          level: 'warning',
          message: `${limit.api_name} API rate limit low: ${limit.requests_remaining}/${limit.requests_limit} (${remainingPercent.toFixed(1)}%)`,
          metric: 'rate_limit',
          api: limit.api_name,
          endpoint: limit.endpoint
        });
      }
    });
    
    healthReport.metrics.rate_limits = rateLimitMetrics;
  }
  
  // If system has critical alerts, attempt to fix issues automatically
  if (healthReport.alerts.some(alert => alert.level === 'critical')) {
    // Reset stalled batches
    if (stalledCount > 0) {
      await supabase.rpc('reset_expired_batches', { 
        p_expiry_minutes: ALERT_THRESHOLDS.processing_time_minutes 
      });
      
      healthReport.actions = healthReport.actions || [];
      healthReport.actions.push({
        action: 'reset_stalled_batches',
        count: stalledCount,
        timestamp: new Date().toISOString()
      });
    }
  }
  
  return healthReport;
}

// Function to send alert (would be implemented with actual alerting service)
async function sendAlert(healthReport) {
  if (healthReport.alerts.length === 0) {
    return { sent: false, reason: 'no_alerts' };
  }
  
  console.log('ALERT: System health issues detected:', JSON.stringify(healthReport, null, 2));
  
  // Here you would integrate with a notification service like Slack, email, etc.
  // For now, we just log the alerts
  
  return { 
    sent: true, 
    count: healthReport.alerts.length,
    timestamp: new Date().toISOString()
  };
}

serve(async (req) => {
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }
  
  try {
    const healthReport = await checkSystemHealth();
    const alertResult = await sendAlert(healthReport);
    
    healthReport.alert_sent = alertResult;
    
    return new Response(
      JSON.stringify(healthReport),
      { 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        status: 200
      }
    );
  } catch (error) {
    console.error('Monitor error:', error.message);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { 
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        status: 500 
      }
    );
  }
});
