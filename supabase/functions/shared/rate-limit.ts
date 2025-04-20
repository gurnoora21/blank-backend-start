
export class RateLimitGate {
  constructor(
    private apiName: string,
    private endpoint: string,
    private supabaseClient: any
  ) {}

  async checkRateLimit(): Promise<boolean> {
    const { data: rateLimit } = await this.supabaseClient
      .from('api_rate_limits')
      .select('*')
      .eq('api_name', this.apiName)
      .eq('endpoint', this.endpoint)
      .single();

    if (!rateLimit) return true;

    if (rateLimit.requests_remaining <= 0 && rateLimit.reset_at > new Date()) {
      const waitTime = new Date(rateLimit.reset_at).getTime() - Date.now();
      await new Promise(resolve => setTimeout(resolve, waitTime));
      return true;
    }

    return rateLimit.requests_remaining > 0;
  }

  async updateRateLimit(remaining: number, limit: number, resetAt: Date, lastResponse?: any): Promise<void> {
    await this.supabaseClient.rpc('track_api_rate_limit', {
      p_api_name: this.apiName,
      p_endpoint: this.endpoint,
      p_requests_remaining: remaining,
      p_requests_limit: limit,
      p_reset_at: resetAt.toISOString(),
      p_last_response: lastResponse
    });
  }
}
