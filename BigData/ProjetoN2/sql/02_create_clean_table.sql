DROP TABLE IF EXISTS clean_mental_health_burnout_tech_2026;

CREATE TABLE clean_mental_health_burnout_tech_2026 AS
WITH deduplicated AS (
    SELECT DISTINCT ON (employee_id)
        *
    FROM raw_mental_health_burnout_tech_2026
    WHERE employee_id IS NOT NULL
    ORDER BY employee_id
)
SELECT
    employee_id,
    age,
    NULLIF(TRIM(gender), '') AS gender,
    NULLIF(TRIM(country), '') AS country,
    NULLIF(TRIM(job_role), '') AS job_role,
    NULLIF(TRIM(seniority_level), '') AS seniority_level,
    years_experience,
    years_at_company,
    NULLIF(TRIM(company_size), '') AS company_size,
    NULLIF(TRIM(industry), '') AS industry,
    NULLIF(TRIM(work_mode), '') AS work_mode,
    salary_usd,
    work_hours_per_week,
    meetings_per_day,
    team_size,
    sleep_hours_per_night,
    exercise_days_per_week,
    vacation_days_taken,
    therapy_access,
    uses_therapy,
    ai_tools_daily,
    manager_support_score,
    work_life_balance_score,
    job_satisfaction_score,
    social_support_score,
    deadline_pressure_score,
    autonomy_score,
    stress_score,
    burnout_score,
    phq9_score,
    NULLIF(TRIM(phq9_category), '') AS phq9_category,
    gad7_score,
    NULLIF(TRIM(gad7_category), '') AS gad7_category,
    NULLIF(TRIM(burnout_level), '') AS burnout_level,
    seeks_mental_health_support,
    job_change_intention
FROM deduplicated;

ALTER TABLE clean_mental_health_burnout_tech_2026
    ADD CONSTRAINT pk_clean_mental_health PRIMARY KEY (employee_id),
    ADD CONSTRAINT chk_clean_age CHECK (age BETWEEN 16 AND 80),
    ADD CONSTRAINT chk_clean_exercise_days CHECK (exercise_days_per_week BETWEEN 0 AND 7),
    ADD CONSTRAINT chk_clean_binary_therapy_access CHECK (therapy_access IN (0, 1)),
    ADD CONSTRAINT chk_clean_binary_uses_therapy CHECK (uses_therapy IN (0, 1)),
    ADD CONSTRAINT chk_clean_binary_ai_tools_daily CHECK (ai_tools_daily IN (0, 1)),
    ADD CONSTRAINT chk_clean_binary_support CHECK (seeks_mental_health_support IN (0, 1)),
    ADD CONSTRAINT chk_clean_binary_job_change CHECK (job_change_intention IN (0, 1));

CREATE INDEX idx_clean_country
    ON clean_mental_health_burnout_tech_2026 (country);

CREATE INDEX idx_clean_job_role
    ON clean_mental_health_burnout_tech_2026 (job_role);

CREATE INDEX idx_clean_burnout_level
    ON clean_mental_health_burnout_tech_2026 (burnout_level);

CREATE INDEX idx_clean_work_mode
    ON clean_mental_health_burnout_tech_2026 (work_mode);
