-- Create share_sessions table for batch file sharing
CREATE TABLE IF NOT EXISTS public.share_sessions (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    session_id VARCHAR(20) UNIQUE NOT NULL,
    user_id UUID REFERENCES auth.users(id) ON DELETE CASCADE,
    file_ids TEXT[] NOT NULL,
    title VARCHAR(255),
    description TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    expires_at TIMESTAMPTZ,
    access_count INTEGER DEFAULT 0,
    is_active BOOLEAN DEFAULT TRUE,
    
    -- Indexes for performance
    CONSTRAINT unique_session_id UNIQUE (session_id)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_share_sessions_user_id ON public.share_sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_share_sessions_session_id ON public.share_sessions(session_id);
CREATE INDEX IF NOT EXISTS idx_share_sessions_is_active ON public.share_sessions(is_active);
CREATE INDEX IF NOT EXISTS idx_share_sessions_expires_at ON public.share_sessions(expires_at);

-- Create RLS policies
ALTER TABLE public.share_sessions ENABLE ROW LEVEL SECURITY;

-- Policy: Users can create their own share sessions
CREATE POLICY "Users can create share sessions" ON public.share_sessions
    FOR INSERT
    WITH CHECK (
        auth.uid() = user_id OR user_id IS NULL
    );

-- Policy: Users can view their own share sessions
CREATE POLICY "Users can view own share sessions" ON public.share_sessions
    FOR SELECT
    USING (
        auth.uid() = user_id OR user_id IS NULL
    );

-- Policy: Anyone can view active share sessions (for public sharing)
CREATE POLICY "Anyone can view active share sessions" ON public.share_sessions
    FOR SELECT
    USING (
        is_active = TRUE
    );

-- Policy: Users can update their own share sessions
CREATE POLICY "Users can update own share sessions" ON public.share_sessions
    FOR UPDATE
    USING (auth.uid() = user_id)
    WITH CHECK (auth.uid() = user_id);

-- Policy: Users can delete their own share sessions
CREATE POLICY "Users can delete own share sessions" ON public.share_sessions
    FOR DELETE
    USING (auth.uid() = user_id);

-- Create function to increment access count
CREATE OR REPLACE FUNCTION increment_session_access(p_session_id VARCHAR)
RETURNS VOID AS $$
BEGIN
    UPDATE public.share_sessions
    SET access_count = access_count + 1
    WHERE session_id = p_session_id AND is_active = TRUE;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Grant necessary permissions
GRANT USAGE ON SCHEMA public TO anon, authenticated;
GRANT ALL ON public.share_sessions TO anon, authenticated;
GRANT EXECUTE ON FUNCTION increment_session_access TO anon, authenticated;

-- Add comment to table
COMMENT ON TABLE public.share_sessions IS 'Stores shareable sessions for batch file downloads';
