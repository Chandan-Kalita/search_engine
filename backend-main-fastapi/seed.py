"""
Database seeding script to create tables and insert sample data.
Run with: python seed.py
"""
import asyncio
from database import init_db_pool, close_db_pool, get_db_connection


async def create_tables():
    """Create database tables."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            # Create users table
            await cur.execute("""
                CREATE TABLE IF NOT EXISTS documents (
                    id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
                    title text,
                    content text,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            await conn.commit()
            print("✓ Documents table created")


async def seed_data():
    """Insert sample data into the database."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            # Insert sample documents
            documents = [
                ('Introduction to PostgreSQL', 'PostgreSQL is a powerful open-source relational database system.'),
                ('FastAPI Best Practices', 'FastAPI is a modern, fast web framework for building APIs with Python.'),
                ('Python Async Programming', 'Async programming in Python allows for concurrent execution of tasks.'),
                ('Database Seeding', 'Database seeding is the process of populating a database with initial data.'),
                ('Neon Cloud Database', 'Neon is a serverless PostgreSQL database service.')
            ]

            for title, content in documents:
                try:
                    await cur.execute(
                        "INSERT INTO documents (title, content) VALUES (%s, %s)",
                        (title, content)
                    )
                except Exception as e:
                    print(f"✗ Error inserting document '{title}': {e}")

            await conn.commit()

            await conn.commit()
            print(f"✓ Inserted {len(documents)} sample documents")


async def main():
    """Main function to run the seeding process."""
    print("Starting database seeding...")

    # Initialize database pool
    await init_db_pool()

    try:
        # Create tables
        await create_tables()

        # Seed data
        await seed_data()

        print("\n✓ Database seeding completed successfully!")

    except Exception as e:
        print(f"\n✗ Error during seeding: {e}")
        raise

    finally:
        # Close database pool
        await close_db_pool()


if __name__ == "__main__":
    asyncio.run(main())
