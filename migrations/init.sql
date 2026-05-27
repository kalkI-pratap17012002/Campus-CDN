CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS files (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  filename TEXT NOT NULL,
  total_size BIGINT NOT NULL,
  total_chunks INTEGER NOT NULL,
  uploaded_by TEXT DEFAULT 'anonymous',
  uploaded_at TIMESTAMP DEFAULT NOW(),
  status TEXT DEFAULT 'uploading'
    CHECK (status IN ('uploading','ready','corrupted'))
);

CREATE TABLE IF NOT EXISTS chunks (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  file_id UUID REFERENCES files(id),
  chunk_index INTEGER NOT NULL,
  chunk_size INTEGER NOT NULL,
  sha256_hash TEXT NOT NULL,
  storage_path TEXT NOT NULL,
  created_at TIMESTAMP DEFAULT NOW()
);
