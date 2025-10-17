"""Run the share_sessions table migration."""

from app.services.supabase_service import get_supabase_service
import os

def run_migration():
    """Execute the SQL migration to create share_sessions table."""
    try:
        print("Reading migration SQL...")
        with open('create_share_sessions_table.sql', 'r') as f:
            sql = f.read()
        
        print("Connecting to Supabase...")
        supabase = get_supabase_service()
        
        # Split SQL into individual statements
        statements = [s.strip() for s in sql.split(';') if s.strip()]
        
        print(f"Executing {len(statements)} SQL statements...")
        
        for i, statement in enumerate(statements, 1):
            if statement:
                try:
                    print(f"  [{i}/{len(statements)}] Executing: {statement[:50]}...")
                    # Note: Supabase Python client doesn't have direct SQL execution
                    # You'll need to run this SQL directly in Supabase Dashboard
                    print("    ⚠️  Please run this statement in Supabase SQL Editor")
                except Exception as e:
                    print(f"    Error: {e}")
        
        print("\n" + "="*60)
        print("IMPORTANT: The SQL migration needs to be run manually!")
        print("="*60)
        print("\nSteps to complete the migration:")
        print("1. Go to your Supabase Dashboard: https://supabase.com/dashboard")
        print("2. Select your project")
        print("3. Go to SQL Editor (left sidebar)")
        print("4. Create a new query")
        print("5. Copy the contents of 'create_share_sessions_table.sql'")
        print("6. Paste and run the SQL")
        print("\nThe SQL file is located at:")
        print(f"  {os.path.abspath('create_share_sessions_table.sql')}")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    run_migration()
