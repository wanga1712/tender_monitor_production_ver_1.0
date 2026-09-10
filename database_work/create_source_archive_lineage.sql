-- S7 source-of-truth raw archive lineage.
CREATE TABLE IF NOT EXISTS source_archive_manifest (
    archive_id BIGSERIAL PRIMARY KEY,
    law_family TEXT NOT NULL CHECK (law_family IN ('44_FZ', '223_FZ')),
    ingestion_direction TEXT NOT NULL CHECK (ingestion_direction IN ('FORWARD', 'BACKWARD')),
    source_date DATE NOT NULL,
    downloaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    archive_filename TEXT NOT NULL,
    archive_path TEXT NOT NULL,
    file_size BIGINT NOT NULL CHECK (file_size >= 0),
    sha256 TEXT NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    processing_status TEXT NOT NULL DEFAULT 'DOWNLOADED',
    UNIQUE (sha256)
);

CREATE TABLE IF NOT EXISTS source_xml_manifest (
    xml_id BIGSERIAL PRIMARY KEY,
    archive_id BIGINT NOT NULL REFERENCES source_archive_manifest(archive_id) ON DELETE CASCADE,
    archive_member_path TEXT,
    xml_filename TEXT NOT NULL,
    xml_document_type TEXT,
    schema_version TEXT,
    notice_number TEXT,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    parser_status TEXT NOT NULL DEFAULT 'EXTRACTED',
    UNIQUE (archive_id, archive_member_path)
);

ALTER TABLE source_xml_manifest
    ADD COLUMN IF NOT EXISTS archive_member_path TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS uq_source_xml_manifest_archive_member
    ON source_xml_manifest (archive_id, archive_member_path)
    WHERE archive_member_path IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_source_archive_manifest_age
    ON source_archive_manifest (downloaded_at);
CREATE INDEX IF NOT EXISTS idx_source_archive_manifest_identity
    ON source_archive_manifest (law_family, ingestion_direction, source_date);
CREATE INDEX IF NOT EXISTS idx_source_xml_manifest_notice
    ON source_xml_manifest (notice_number);
