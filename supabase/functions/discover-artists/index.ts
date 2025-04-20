
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

  const requestId = crypto.randomUUID();
  const startTime = Date.now();
  
  // Log execution start
  console.log(JSON.stringify({
    timestamp: new Date().toISOString(),
    request_id: requestId,
    function: 'discover-artists',
    event: 'function_started'
  }));

  try {
    const spotify = new SpotifyClient();
    const rateLimiter = new RateLimitGate('spotify', '/artists', supabase);

    if (!await rateLimiter.checkRateLimit()) {
      throw new Error('Rate limit exceeded');
    }

    // Get request parameters - allowing manual search or scheduled discovery
    let searchParams = {};
    if (req.method === 'POST') {
      try {
        const body = await req.json();
        searchParams = body;
      } catch (e) {
        // Default if no body or invalid JSON
        searchParams = {};
      }
    }
    
    // Log the request parameters
    console.log(JSON.stringify({
      timestamp: new Date().toISOString(),
      request_id: requestId,
      function: 'discover-artists',
      event: 'request_parameters',
      metadata: searchParams
    }));

    // Set up the API parameters for Spotify
    const apiParams = new URLSearchParams();
    
    if (searchParams.query) {
      apiParams.append('q', searchParams.query);
      apiParams.append('type', 'artist');
      apiParams.append('limit', searchParams.limit || '20');
    } else {
      // In scheduled mode, fetch trending artists
      apiParams.append('limit', '50');
    }
    
    const endpoint = searchParams.query 
      ? `/search?${apiParams.toString()}`
      : '/recommendations/available-genre-seeds';
    
    // Log the API request
    console.log(JSON.stringify({
      timestamp: new Date().toISOString(),
      request_id: requestId,
      function: 'discover-artists',
      event: 'api_request',
      metadata: { endpoint }
    }));

    // Make the API call to Spotify
    const response = await spotify.fetch(endpoint);
    const data = await response.json();
    
    if (!response.ok) {
      throw new Error(`Spotify API error: ${JSON.stringify(data)}`);
    }

    // Update rate limits
    const remaining = parseInt(response.headers.get('x-ratelimit-remaining') || '0');
    const limit = parseInt(response.headers.get('x-ratelimit-limit') || '0');
    const resetAt = new Date(parseInt(response.headers.get('x-ratelimit-reset') || '0') * 1000);
    await rateLimiter.updateRateLimit(remaining, limit, resetAt);
    
    // Process the artist data
    let artists = [];
    if (searchParams.query) {
      // Search response
      artists = data.artists?.items || [];
    } else if (data.genres) {
      // Fetch a few artists from each genre
      const genres = data.genres.slice(0, 5); // Limit to 5 genres to avoid rate limits
      
      for (const genre of genres) {
        try {
          const genreSearchParams = new URLSearchParams();
          genreSearchParams.append('q', `genre:"${genre}"`);
          genreSearchParams.append('type', 'artist');
          genreSearchParams.append('limit', '5');
          
          const genreResponse = await spotify.fetch(`/search?${genreSearchParams.toString()}`);
          const genreData = await genreResponse.json();
          
          if (genreResponse.ok && genreData.artists?.items) {
            artists = [...artists, ...genreData.artists.items];
          }
          
          // Add small delay to avoid rate limits
          await new Promise(resolve => setTimeout(resolve, 250));
        } catch (e) {
          console.error(`Error fetching artists for genre ${genre}:`, e);
        }
      }
    }
    
    // Log the results
    console.log(JSON.stringify({
      timestamp: new Date().toISOString(),
      request_id: requestId,
      function: 'discover-artists',
      event: 'artists_discovered',
      metadata: { count: artists.length }
    }));

    if (artists.length === 0) {
      return new Response(
        JSON.stringify({ 
          success: true, 
          message: "No artists found",
          count: 0
        }), {
          headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        }
      );
    }

    // Upsert artists with idempotency to avoid duplicates
    const { error } = await supabase
      .from('artists')
      .upsert(artists.map((item) => ({
        spotify_id: item.id,
        name: item.name,
        image_url: item.images[0]?.url,
        popularity: item.popularity,
        genres: item.genres,
        spotify_url: item.external_urls.spotify,
      })), { 
        onConflict: 'spotify_id',
        ignoreDuplicates: false
      });

    if (error) throw error;

    // Create album discovery batches for each artist
    const batches = artists.map(artist => ({
      batch_type: 'album_discovery',
      metadata: { artist_id: artist.id },
      status: 'pending'
    }));
    
    const { error: batchError } = await supabase
      .from('processing_batches')
      .upsert(batches, {
        onConflict: 'batch_type, metadata->artist_id',
        ignoreDuplicates: true
      });
    
    if (batchError) throw batchError;
    
    // Log function completion
    console.log(JSON.stringify({
      timestamp: new Date().toISOString(),
      request_id: requestId,
      function: 'discover-artists',
      event: 'function_completed',
      latency_ms: Date.now() - startTime,
      metadata: { 
        artists_count: artists.length,
        batches_count: batches.length
      }
    }));

    return new Response(
      JSON.stringify({ 
        success: true, 
        artists_count: artists.length,
        batches_created: batches.length,
        duration_ms: Date.now() - startTime
      }), {
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
      }
    );
  } catch (error) {
    // Log the error
    console.error(JSON.stringify({
      timestamp: new Date().toISOString(),
      request_id: requestId,
      function: 'discover-artists',
      event: 'function_error',
      error: error.message,
      latency_ms: Date.now() - startTime
    }));
    
    return new Response(
      JSON.stringify({ error: error.message }), {
        status: 500,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
      }
    );
  }
});
