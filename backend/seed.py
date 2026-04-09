import asyncio
import asyncpg
import os
from datetime import datetime, date

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", 5432))
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "appdb")


async def seed_database():
    """Seed the database with initial data"""
    
    conn = await asyncpg.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        database=POSTGRES_DB
    )
    
    try:
        # Clear existing data
        print("Clearing existing data...")
        await conn.execute("DELETE FROM user_annotations")
        await conn.execute("DELETE FROM chat_messages")
        await conn.execute("DELETE FROM chat_sessions")
        await conn.execute("DELETE FROM content_quarantine")
        await conn.execute("DELETE FROM import_logs")
        await conn.execute("DELETE FROM import_sources")
        await conn.execute("DELETE FROM section_topics")
        await conn.execute("DELETE FROM topics")
        await conn.execute("DELETE FROM code_references")
        await conn.execute("DELETE FROM code_section_versions")
        await conn.execute("DELETE FROM code_sections")
        await conn.execute("DELETE FROM code_books")
        await conn.execute("DELETE FROM code_cycles")
        await conn.execute("DELETE FROM external_standards")
        await conn.execute("DELETE FROM publishing_orgs")
        
        # Seed Publishing Organizations
        print("Seeding publishing organizations...")
        publishing_orgs = [
            ("ICC", "International Code Council"),
            ("NFPA", "National Fire Protection Association"),
            ("IAPMO", "International Association of Plumbing and Mechanical Officials"),
            ("CBSC", "California Building Standards Commission")
        ]
        
        org_ids = {}
        for abbr, full_name in publishing_orgs:
            org_id = await conn.fetchval(
                """INSERT INTO publishing_orgs (abbreviation, full_name) 
                   VALUES ($1, $2) RETURNING id""",
                abbr, full_name
            )
            org_ids[abbr] = org_id
        
        # Seed Code Cycles
        print("Seeding code cycles...")
        cycles = [
            (2022, "2022 California Building Code", "California"),
            (2025, "2025 California Building Code", "California")
        ]
        
        cycle_ids = {}
        for year, name, jurisdiction in cycles:
            cycle_id = await conn.fetchval(
                """INSERT INTO code_cycles (year, name, jurisdiction) 
                   VALUES ($1, $2, $3) RETURNING id""",
                year, name, jurisdiction
            )
            cycle_ids[year] = cycle_id
        
        # Seed Code Books (Title 24 Parts 1-12)
        print("Seeding code books...")
        code_books = [
            (1, "Part 1 - Administration"),
            (2, "Part 2 - Building Planning"),
            (3, "Part 3 - Fire and Life Safety"),
            (4, "Part 4 - Accessibility"),
            (5, "Part 5 - General Building Safety Provisions"),
            (6, "Part 6 - Building Elements and Materials"),
            (7, "Part 7 - Fire-Resistance-Rated Construction"),
            (8, "Part 8 - Interior Finishes"),
            (9, "Part 9 - Structural Design"),
            (10, "Part 10 - Means of Egress"),
            (11, "Part 11 - Accessibility"),
            (12, "Part 12 - Interior Environment")
        ]
        
        cycle_2022 = cycle_ids[2022]
        cbsc_id = org_ids["CBSC"]
        
        book_ids = {}
        for part_num, title in code_books:
            book_id = await conn.fetchval(
                """INSERT INTO code_books 
                   (code_cycle_id, publishing_org_id, part_number, title, effective_date) 
                   VALUES ($1, $2, $3, $4, $5) RETURNING id""",
                cycle_2022, cbsc_id, part_num, title, date(2022, 1, 1)
            )
            book_ids[part_num] = book_id
        
        # Seed External Standards
        print("Seeding external standards...")
        external_standards = [
            ("ASTM E119", "Fire Test of Building Construction and Materials", "ASTM International", 2021),
            ("NFPA 13", "Installation of Sprinkler Systems", "NFPA", 2022),
            ("NFPA 72", "National Fire Alarm and Signaling Code", "NFPA", 2022),
            ("NFPA 101", "Life Safety Code", "NFPA", 2021),
            ("IBC 2021", "International Building Code", "ICC", 2021),
            ("IECC 2021", "International Energy Conservation Code", "ICC", 2021),
            ("ADA Standards", "Americans with Disabilities Act Standards", "DOJ", 2010),
            ("ASHRAE 62.1", "Ventilation for Acceptable Indoor Air Quality", "ASHRAE", 2022)
        ]
        
        for standard_id, title, org, year in external_standards:
            await conn.execute(
                """INSERT INTO external_standards 
                   (standard_id, title, organization, year_published) 
                   VALUES ($1, $2, $3, $4)""",
                standard_id, title, org, year
            )
        
        # Seed Topics
        print("Seeding topics...")
        topics_list = [
            "Fire Safety",
            "Accessibility",
            "Structural Design",
            "Electrical Systems",
            "Plumbing Systems",
            "HVAC",
            "Energy Efficiency",
            "Life Safety",
            "Building Materials",
            "Foundation Requirements",
            "Walls and Partitions",
            "Roofs",
            "Windows and Doors",
            "Stairways",
            "Elevators",
            "Means of Egress",
            "Vertical Openings",
            "Smoke Control",
            "Automatic Sprinklers",
            "Fire-Resistance Ratings"
        ]
        
        for topic in topics_list:
            await conn.execute(
                "INSERT INTO topics (name) VALUES ($1)",
                topic
            )
        
        # Seed Import Sources
        print("Seeding import sources...")
        import_sources = [
            ("California Building Standards", "pdf_crawl", "https://www.dgs.ca.gov/bsc"),
            ("ICC Code Updates", "web_scrape", "https://www.iccsafe.org"),
            ("NFPA Standards", "api_fetch", "https://www.nfpa.org")
        ]
        
        for name, source_type, url in import_sources:
            await conn.execute(
                """INSERT INTO import_sources (name, source_type, url) 
                   VALUES ($1, $2, $3)""",
                name, source_type, url
            )
        
        # Seed sample code sections for Part 2
        print("Seeding sample code sections...")
        book_id = book_ids[2]
        
        sample_sections = [
            ("Chapter 1", "101", "General", 
             "This chapter applies to the occupancy and maintenance of structures and premises.",
             1, "/2/1/101"),
            ("Chapter 2", "202", "Definitions",
             "The following words and terms shall, for the purposes of this code, have the meanings shown herein.",
             1, "/2/1/202"),
            ("Chapter 3", "301", "Use and Occupancy Classification",
             "Buildings and other structures shall be classified with respect to occupancy in one or more of the groups shown below.",
             1, "/2/1/301"),
            ("Chapter 4", "401", "General",
             "The provisions of this chapter shall apply to the planning and design of additions, alterations or repairs to any structure.",
             1, "/2/1/401")
        ]
        
        for chapter, section_num, title, content, depth, path in sample_sections:
            section_id = await conn.fetchval(
                """INSERT INTO code_sections 
                   (code_book_id, chapter, section_number, title, content, depth, path) 
                   VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id""",
                book_id, chapter, section_num, title, content, depth, path
            )
            
            # Add section version
            await conn.execute(
                """INSERT INTO code_section_versions 
                   (code_section_id, version_number, content, change_reason) 
                   VALUES ($1, $2, $3, $4)""",
                section_id, 1, content, "Initial version"
            )
        
        # Create a chat session
        print("Seeding initial chat session...")
        await conn.execute(
            """INSERT INTO chat_sessions (user_id, title) 
               VALUES ($1, $2)""",
            "default_user", "Welcome to Building Code Intelligence System"
        )
        
        print("Database seeding completed successfully!")
        
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(seed_database())
