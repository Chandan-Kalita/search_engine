"""
Database seeding script to create tables and insert sample data.
Run with: python seed.py
"""
import asyncio
from database import init_db_pool, close_db_pool, get_db_connection
import csv

async def create_tables():
    """Create database tables."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            # Create users table
            await cur.execute("""
                DROP TABLE if EXISTS documents;
                CREATE TABLE IF NOT EXISTS documents (
                    id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
                    title text,
                    content text,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            await conn.commit()
            print("✓ Documents table created")

def get_documents():
    results = []
    with open("./misc/Tech_Posts_1k.csv", "r") as csvfile:
        content = csv.reader(csvfile)
        i=0
        for row in content:
            if i == 0:
                i=i+1
                continue
            results.append((row[0], row[1]))

        csvfile.close()

    return results


async def seed_data():
    """Insert sample data into the database."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            # Insert sample documents
            documents = get_documents()
            print("Inserting" , len(documents), " like: ", documents[0])
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
