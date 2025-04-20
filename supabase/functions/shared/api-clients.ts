
import { createClient } from '@supabase/supabase-js';

const supabaseUrl = Deno.env.get('SUPABASE_URL')!;
const supabaseServiceKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!;

export const supabase = createClient(supabaseUrl, supabaseServiceKey);

export class SpotifyClient {
  private accessToken: string | null = null;
  private tokenExpiry: Date | null = null;

  constructor() {}

  private async refreshToken() {
    const clientId = Deno.env.get('SPOTIFY_CLIENT_ID');
    const clientSecret = Deno.env.get('SPOTIFY_CLIENT_SECRET');
    
    const response = await fetch('https://accounts.spotify.com/api/token', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        Authorization: `Basic ${btoa(`${clientId}:${clientSecret}`)}`,
      },
      body: 'grant_type=client_credentials',
    });

    const data = await response.json();
    this.accessToken = data.access_token;
    this.tokenExpiry = new Date(Date.now() + data.expires_in * 1000);
  }

  async fetch(endpoint: string, options: RequestInit = {}) {
    if (!this.accessToken || !this.tokenExpiry || this.tokenExpiry < new Date()) {
      await this.refreshToken();
    }

    return fetch(`https://api.spotify.com/v1${endpoint}`, {
      ...options,
      headers: {
        ...options.headers,
        Authorization: `Bearer ${this.accessToken}`,
      },
    });
  }
}

export class GeniusClient {
  constructor(private accessToken: string = Deno.env.get('GENIUS_ACCESS_TOKEN')!) {}

  async fetch(endpoint: string, options: RequestInit = {}) {
    return fetch(`https://api.genius.com${endpoint}`, {
      ...options,
      headers: {
        ...options.headers,
        Authorization: `Bearer ${this.accessToken}`,
      },
    });
  }
}

export class DiscogsClient {
  constructor(
    private consumerKey: string = Deno.env.get('DISCOGS_CONSUMER_KEY')!,
    private consumerSecret: string = Deno.env.get('DISCOGS_CONSUMER_SECRET')!
  ) {}

  async fetch(endpoint: string, options: RequestInit = {}) {
    return fetch(`https://api.discogs.com${endpoint}`, {
      ...options,
      headers: {
        ...options.headers,
        Authorization: `Discogs key=${this.consumerKey}, secret=${this.consumerSecret}`,
      },
    });
  }
}
