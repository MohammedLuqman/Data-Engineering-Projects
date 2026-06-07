CREATE TABLE yemeni_documents (
    id SERIAL PRIMARY KEY,
    document_id VARCHAR(50) UNIQUE NOT NULL,
    owner_name VARCHAR(255) NOT NULL,
    document_type VARCHAR(150) NOT NULL,
    city VARCHAR(100) NOT NULL,
    issue_date DATE NOT NULL,
    extracted_text TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);