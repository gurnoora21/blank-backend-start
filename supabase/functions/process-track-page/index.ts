
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { SpotifyClient, supabase } from "../shared/api-clients.ts";
import { RateLimitGate } from "../shared/rate-limit.ts";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

serve(async (req) => {
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    const { album_id, offset = 0, limit = 50 } = await req.json();
    
    const spotify = new SpotifyClient();
    const rateLimiter = new RateLimitGate('spotify', '/albums/tracks', supabase);

    if (!await rateLimiter.checkRateLimit()) {
      throw new Error('Rate limit exceeded');
    }

    const response = await spotify.fetch(`/albums/${album_id}/tracks?offset=${offset}&limit=${limit}`);
    const data = await response.json();

    // Update rate limits
    const remaining = parseInt(response.headers.get('x-ratelimit-remaining') || '0');
    const resetAt = new Date(parseInt(response.headers.get('x-ratelimit-reset') || '0') * 1000);
    await rateLimiter.updateRateLimit(remaining, limit, resetAt);

    // Upsert tracks
    const { error } = await supabase
      .from('tracks')
      .upsert(data.items.map((item: any) => ({
        spotify_id: item.id,
        name: item.name,
        preview_url: item.preview_url,
        spotify_url: item.external_urls.spotify,
      })));

    if (error) throw error;

    // Create track_albums relations
    await supabase
      .from('track_albums')
      .upsert(data.items.map((item: any, index: number) => ({
        track_id: item.id,
        album_id,
        track_number: item.track_number,
        disc_number: item.disc_number
      })));

    // Emit next events
    if (data.items.length === limit) {
      // More pages to process
      await supabase
        .from('processing_batches')
        .insert({
          batch_type: 'track_page',
          metadata: { album_id, offset: offset + limit, limit }
        });
    } else {
      // No more pages, emit track.completed for each track
      for (const track of data.items) {
        await supabase
          .from('processing_batches')
          .insert({
            batch_type: 'producer_discovery',
            metadata: { track_id: track.id }
          });
      }
    }

    return new Response(JSON.stringify({ success: true }), {
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    });
  } catch (error) {
    console.error('Error:', error.message);
    
    // Add to dead letter queue
    await supabase
      .from('dead_letter_items')
      .insert({
        item_type: 'track_page_error',
        error_message: error.message,
        metadata: { error: error.message }
      });

    return new Response(JSON.stringify({ error: error.message }), {
      status: 500,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    });
  }
});
