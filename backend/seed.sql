-- Seed data for Building Code Intelligence System

-- Publishing Organizations
INSERT INTO publishing_orgs (abbreviation, full_name, website) VALUES
('ICC', 'International Code Council', 'https://www.iccsafe.org'),
('NFPA', 'National Fire Protection Association', 'https://www.nfpa.org'),
('IAPMO', 'International Association of Plumbing and Mechanical Officials', 'https://www.iapmo.org'),
('CBSC', 'California Building Standards Commission', 'https://www.dgs.ca.gov/BSC')
ON CONFLICT (abbreviation) DO NOTHING;

-- Code Cycles
INSERT INTO code_cycles (name, effective_date, expiration_date, status, adopting_authority) VALUES
('2022 California Building Standards', '2023-01-01', '2025-12-31', 'active', 'State of California'),
('2025 California Building Standards', '2026-01-01', NULL, 'upcoming', 'State of California')
ON CONFLICT DO NOTHING;

-- Code Books (Title 24 Parts 1-12, 2022 cycle)
-- Using subqueries to get the cycle_id and org_id dynamically
INSERT INTO code_books (code_name, abbreviation, part_number, cycle_id, base_model_code, base_model_abbreviation, base_code_year, publishing_org_id, category, status, effective_date) VALUES
('California Administrative Code', 'CAC', 'Part 1', (SELECT id FROM code_cycles WHERE name LIKE '2022%' LIMIT 1), NULL, NULL, NULL, (SELECT id FROM publishing_orgs WHERE abbreviation='CBSC'), 'Administrative', 'active', '2023-01-01'),
('California Building Code', 'CBC', 'Part 2', (SELECT id FROM code_cycles WHERE name LIKE '2022%' LIMIT 1), '2021 International Building Code', 'IBC', 2021, (SELECT id FROM publishing_orgs WHERE abbreviation='ICC'), 'Building', 'active', '2023-01-01'),
('California Residential Code', 'CRC', 'Part 2.5', (SELECT id FROM code_cycles WHERE name LIKE '2022%' LIMIT 1), '2021 International Residential Code', 'IRC', 2021, (SELECT id FROM publishing_orgs WHERE abbreviation='ICC'), 'Residential', 'active', '2023-01-01'),
('California Electrical Code', 'CEC', 'Part 3', (SELECT id FROM code_cycles WHERE name LIKE '2022%' LIMIT 1), '2020 National Electrical Code', 'NEC', 2020, (SELECT id FROM publishing_orgs WHERE abbreviation='NFPA'), 'Electrical', 'active', '2023-01-01'),
('California Mechanical Code', 'CMC', 'Part 4', (SELECT id FROM code_cycles WHERE name LIKE '2022%' LIMIT 1), '2021 Uniform Mechanical Code', 'UMC', 2021, (SELECT id FROM publishing_orgs WHERE abbreviation='IAPMO'), 'Mechanical', 'active', '2023-01-01'),
('California Plumbing Code', 'CPC', 'Part 5', (SELECT id FROM code_cycles WHERE name LIKE '2022%' LIMIT 1), '2021 Uniform Plumbing Code', 'UPC', 2021, (SELECT id FROM publishing_orgs WHERE abbreviation='IAPMO'), 'Plumbing', 'active', '2023-01-01'),
('California Energy Code', 'CEnC', 'Part 6', (SELECT id FROM code_cycles WHERE name LIKE '2022%' LIMIT 1), NULL, NULL, NULL, (SELECT id FROM publishing_orgs WHERE abbreviation='CBSC'), 'Energy', 'active', '2023-01-01'),
('California Historical Building Code', 'CHBC', 'Part 8', (SELECT id FROM code_cycles WHERE name LIKE '2022%' LIMIT 1), NULL, NULL, NULL, (SELECT id FROM publishing_orgs WHERE abbreviation='CBSC'), 'Historical', 'active', '2023-01-01'),
('California Fire Code', 'CFC', 'Part 9', (SELECT id FROM code_cycles WHERE name LIKE '2022%' LIMIT 1), '2021 International Fire Code', 'IFC', 2021, (SELECT id FROM publishing_orgs WHERE abbreviation='ICC'), 'Fire', 'active', '2023-01-01'),
('California Existing Building Code', 'CEBC', 'Part 10', (SELECT id FROM code_cycles WHERE name LIKE '2022%' LIMIT 1), '2021 International Existing Building Code', 'IEBC', 2021, (SELECT id FROM publishing_orgs WHERE abbreviation='ICC'), 'Existing Building', 'active', '2023-01-01'),
('California Green Building Standards Code', 'CALGreen', 'Part 11', (SELECT id FROM code_cycles WHERE name LIKE '2022%' LIMIT 1), NULL, NULL, NULL, (SELECT id FROM publishing_orgs WHERE abbreviation='CBSC'), 'Green Building', 'active', '2023-01-01'),
('California Referenced Standards Code', 'CRSC', 'Part 12', (SELECT id FROM code_cycles WHERE name LIKE '2022%' LIMIT 1), NULL, NULL, NULL, (SELECT id FROM publishing_orgs WHERE abbreviation='CBSC'), 'Referenced Standards', 'active', '2023-01-01');

-- External Standards
INSERT INTO external_standards (standard_id, title, organization, year_published) VALUES
('ASTM E119', 'Standard Test Methods for Fire Tests of Building Construction and Materials', 'ASTM International', 2020),
('ASTM E84', 'Standard Test Method for Surface Burning Characteristics of Building Materials', 'ASTM International', 2020),
('NFPA 13', 'Standard for the Installation of Sprinkler Systems', 'NFPA', 2022),
('NFPA 72', 'National Fire Alarm and Signaling Code', 'NFPA', 2022),
('NFPA 101', 'Life Safety Code', 'NFPA', 2021),
('NFPA 70', 'National Electrical Code (NEC)', 'NFPA', 2020),
('ACI 318', 'Building Code Requirements for Structural Concrete', 'ACI', 2019),
('AISC 360', 'Specification for Structural Steel Buildings', 'AISC', 2016),
('ASCE 7', 'Minimum Design Loads and Associated Criteria for Buildings', 'ASCE', 2022),
('ASHRAE 62.1', 'Ventilation for Acceptable Indoor Air Quality', 'ASHRAE', 2022)
ON CONFLICT (standard_id) DO NOTHING;

-- Topics
INSERT INTO topics (name, description) VALUES
('Fire-Rated Assemblies', 'Fire resistance ratings for walls, floors, roofs, and structural elements'),
('Means of Egress', 'Exit access, exits, and exit discharge requirements'),
('Occupancy Classification', 'Building use and occupancy group classifications'),
('Structural Design', 'Load combinations, seismic, wind, and gravity design'),
('Accessibility', 'ADA compliance and accessible design requirements'),
('Fire Protection Systems', 'Sprinklers, alarms, standpipes, and detection systems'),
('Energy Efficiency', 'Building envelope, HVAC efficiency, lighting requirements'),
('Plumbing Systems', 'Water supply, drainage, fixtures, and gas piping'),
('Electrical Systems', 'Wiring, circuits, grounding, and electrical equipment'),
('Mechanical Systems', 'HVAC, ventilation, exhaust, and refrigeration'),
('Building Heights and Areas', 'Allowable height, area, and number of stories'),
('Interior Finishes', 'Wall, ceiling, and floor finish flame spread requirements'),
('Roof Assemblies', 'Roof construction, fire classification, and weather protection'),
('Exterior Walls', 'Fire resistance, weather barriers, and opening protectives'),
('Foundations', 'Soils, footings, and foundation design requirements'),
('California Amendments', 'State-specific modifications to model codes'),
('Seismic Design', 'California-specific seismic requirements and amendments'),
('Wildfire Protection', 'WUI areas, Chapter 7A, and ignition-resistant construction'),
('Green Building', 'CALGreen mandatory and voluntary measures'),
('Referenced Standards', 'External standards referenced by California codes')
ON CONFLICT (name) DO NOTHING;

-- Import Sources
INSERT INTO import_sources (source_name, source_url, source_type, code_book_id, status, crawl_interval_hours) VALUES
('CBSC Official - Title 24', 'https://www.dgs.ca.gov/BSC/Codes', 'web_scrape', (SELECT id FROM code_books WHERE abbreviation='CBC' LIMIT 1), 'pending', 168),
('Internet Archive - Title 24 PDFs', 'https://archive.org/details/2022californiabu02unse', 'pdf_parse', (SELECT id FROM code_books WHERE abbreviation='CBC' LIMIT 1), 'pending', 720),
('ICC Digital Codes - IBC', 'https://codes.iccsafe.org/content/IBC2021P7', 'web_scrape', (SELECT id FROM code_books WHERE abbreviation='CBC' LIMIT 1), 'pending', 168),
('NFPA Free Access', 'https://www.nfpa.org/codes-and-standards', 'web_scrape', NULL, 'pending', 720);
