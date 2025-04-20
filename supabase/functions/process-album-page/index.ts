
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
    const { artist_id, offset = 0, limit = 50 } = await req.json();
    
    const spotify = new SpotifyClient();
    const rateLimiter = new RateLimitGate('spotify', '/artists/albums', supabase);

    if (!await rateLimiter.checkRateLimit()) {
      throw new Error('Rate limit exceeded');
    }

    const response = await spotify.fetch(`/artists/${artist_id}/albums?offset=${offset}&limit=${limit}`);
    const data = await response.json();

    // Update rate limits
    const remaining = parseInt(response.headers.get('x-ratelimit-remaining') || '0');
    const resetAt = new Date(parseInt(response.headers.get('x-ratelimit-reset') || '0') * 1000);
    await rateLimiter.updateRateLimit(remaining, limit, resetAt);

    // Upsert albums
    const { error } = await supabase
      .from('albums')
      .upsert(data.items.map((item: any) => ({
        spotify_id: item.id,
        name: item.name,
        artist_id,
        image_url: item.images[0]?.url,
        release_date: item.release_date,
        total_tracks: item.total_tracks,
        spotify_url: item.external_urls.spotify,
        album_type: item.album_type,
      })));

    if (error) throw error;

    // Emit next events
    if (data.items.length === limit) {
      // More pages to process
      await supabase
        .from('processing_batches')
        .insert({
          batch_type: 'album_page',
          metadata: { artist_id, offset: offset + limit, limit }
        });
    } else {
      // No more pages, emit album.completed
      await supabase
        .from('processing_batches')
        .insert({
          batch_type: 'track_discovery',
          metadata: { artist_id }
        });
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
        item_type: 'album_page_error',
        error_message: error.message,
        metadata: { error: error.message }
      });

    return new Response(JSON.stringify({ error: error.message }), {
      status: 500,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    });
  }
});
