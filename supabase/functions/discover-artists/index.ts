
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
    const spotify = new SpotifyClient();
    const rateLimiter = new RateLimitGate('spotify', '/artists', supabase);

    if (!await rateLimiter.checkRateLimit()) {
      throw new Error('Rate limit exceeded');
    }

    const response = await spotify.fetch('/artists?limit=50');
    const data = await response.json();

    // Update rate limits
    const remaining = parseInt(response.headers.get('x-ratelimit-remaining') || '0');
    const limit = parseInt(response.headers.get('x-ratelimit-limit') || '0');
    const resetAt = new Date(parseInt(response.headers.get('x-ratelimit-reset') || '0') * 1000);
    await rateLimiter.updateRateLimit(remaining, limit, resetAt);

    // Upsert artists
    const { error } = await supabase
      .from('artists')
      .upsert(data.items.map((item: any) => ({
        spotify_id: item.id,
        name: item.name,
        image_url: item.images[0]?.url,
        popularity: item.popularity,
        genres: item.genres,
        spotify_url: item.external_urls.spotify,
      })));

    if (error) throw error;

    // Emit artist.discovered events
    for (const artist of data.items) {
      await supabase
        .from('processing_batches')
        .insert({
          batch_type: 'album_discovery',
          metadata: { artist_id: artist.id }
        });
    }

    return new Response(JSON.stringify({ success: true }), {
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    });
  } catch (error) {
    console.error('Error:', error.message);
    return new Response(JSON.stringify({ error: error.message }), {
      status: 500,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    });
  }
});
