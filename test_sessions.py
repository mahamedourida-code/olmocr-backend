"""Test the share sessions functionality."""

import asyncio
from app.services.supabase_service import get_supabase_service

async def test_sessions():
    """Test if share_sessions table exists and is accessible."""
    try:
        print("Initializing Supabase service...")
        supabase = get_supabase_service()
        
        print("Testing share_sessions table...")
        # Try to fetch sessions (should return empty list if table exists)
        result = supabase.client.table('share_sessions').select('*').limit(1).execute()
        
        print(f"Success! Table exists. Found {len(result.data)} records.")
        print(f"Table response: {result.data}")
        
        return True
        
    except Exception as e:
        print(f"Error: {e}")
        print(f"Error type: {type(e)}")
        return False

if __name__ == "__main__":
    success = asyncio.run(test_sessions())
    if success:
        print("\n✓ Share sessions table is accessible")
    else:
        print("\n✗ Share sessions table is NOT accessible - needs to be created in Supabase")
