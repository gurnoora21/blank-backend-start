
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { GeniusClient, DiscogsClient, supabase } from "../shared/api-clients.ts";
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
    const { track_id } = await req.json();
    
    const genius = new GeniusClient();
    const discogs = new DiscogsClient();
    
    // Get track details
    const { data: track } = await supabase
      .from('tracks')
      .select('*, artists:track_artists(name)')
      .eq('id', track_id)
      .single();

    if (!track) throw new Error('Track not found');

    // Search Genius
    const geniusRateLimiter = new RateLimitGate('genius', '/search', supabase);
    if (!await geniusRateLimiter.checkRateLimit()) {
      throw new Error('Genius rate limit exceeded');
    }

    const searchResponse = await genius.fetch(`/search?q=${encodeURIComponent(track.name)}`);
    const searchData = await searchResponse.json();

    let producers = [];
    
    if (searchData.response.hits.length > 0) {
      const songId = searchData.response.hits[0].result.id;
      const songResponse = await genius.fetch(`/songs/${songId}`);
      const songData = await songResponse.json();
      
      producers = songData.response.song.producer_artists || [];
    }

    // Search Discogs
    const discogsRateLimiter = new RateLimitGate('discogs', '/database/search', supabase);
    if (!await discogsRateLimiter.checkRateLimit()) {
      throw new Error('Discogs rate limit exceeded');
    }

    const discogsResponse = await discogs.fetch(`/database/search?q=${encodeURIComponent(track.name)}&type=release`);
    const discogsData = await discogsResponse.json();

    if (discogsData.results.length > 0) {
      const releaseId = discogsData.results[0].id;
      const releaseResponse = await discogs.fetch(`/releases/${releaseId}`);
      const releaseData = await releaseResponse.json();
      
      // Add extraCredits to producers array
      if (releaseData.extraartists) {
        producers = [...producers, ...releaseData.extraartists.filter((artist: any) => 
          artist.role.toLowerCase().includes('producer')
        )];
      }
    }

    // Normalize and upsert producers
    for (const producer of producers) {
      const { data: normalizedProducer } = await supabase
        .rpc('normalize_producer_name', { name: producer.name });

      const { data: producerRecord } = await supabase
        .from('producers')
        .upsert({
          name: normalizedProducer,
          metadata: producer
        })
        .select()
        .single();

      // Create track_producers relation
      await supabase
        .from('track_producers')
        .upsert({
          track_id,
          producer_id: producerRecord.id,
          source: producer.source || 'unknown',
          confidence: 'medium'
        });
    }

    // Emit producer.completed event
    await supabase
      .from('processing_batches')
      .insert({
        batch_type: 'producer_completed',
        metadata: { track_id }
      });

    return new Response(JSON.stringify({ success: true }), {
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    });
  } catch (error) {
    console.error('Error:', error.message);
    
    // Add to dead letter queue
    await supabase
      .from('dead_letter_items')
      .insert({
        item_type: 'producer_page_error',
        error_message: error.message,
        metadata: { error: error.message }
      });

    return new Response(JSON.stringify({ error: error.message }), {
      status: 500,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    });
  }
});
