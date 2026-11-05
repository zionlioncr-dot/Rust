BEGIN;

ALTER TABLE aml_assessments
    ADD COLUMN IF NOT EXISTS assessment_status TEXT NOT NULL DEFAULT 'evaluated'
    CHECK (assessment_status IN ('evaluated', 'manual_review'));

COMMIT;
