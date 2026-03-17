-- CTE rewrite of select_activity_details_parameterized.sql
-- Pure read-only: no CREATE TEMPORARY TABLE, ALTER, or INSERT required.
-- Requires MySQL 8.0+.  Accepts %(start)s / %(end)s in YYYYMMDDhhmmss format.

WITH targets AS (
    -- Target contacts (record_type_id = 3): the people an activity is directed at.
    -- Pulls contact name, ID, primary email, and primary address fields.
    SELECT
        act.id                          AS activity_id,
        c.sort_name                     AS contact_target,
        c.id                            AS contact_target_id,
        e.email                         AS email_target,
        addr.street_name,
        addr.street_number,
        addr.street_address,
        addr.city,
        addr.postal_code
    FROM civicrm_activity act
    INNER JOIN civicrm_activity_contact ac
           ON act.id = ac.activity_id AND ac.record_type_id = 3
    INNER JOIN civicrm_contact c
           ON ac.contact_id = c.id
    LEFT JOIN civicrm_email e
           ON ac.contact_id = e.contact_id AND e.is_primary = 1
    LEFT JOIN civicrm_address addr
           ON c.id = addr.contact_id AND addr.is_primary = 1
    WHERE act.is_test = 0
      AND act.is_deleted = 0
      AND act.is_current_revision = 1
      AND act.activity_date_time >= %(start)s
      AND act.activity_date_time <= %(end)s
      AND act.activity_type_id IN (35, 36, 7)
),

assignees AS (
    -- Assignee contacts (record_type_id = 1): the person responsible for the activity.
    -- Pulls contact name and ID only; email/address not needed for this role.
    SELECT
        act.id                          AS activity_id,
        c.sort_name                     AS contact_assignee,
        c.id                            AS contact_assignee_id
    FROM civicrm_activity act
    INNER JOIN civicrm_activity_contact ac
           ON act.id = ac.activity_id AND ac.record_type_id = 1
    INNER JOIN civicrm_contact c
           ON ac.contact_id = c.id
    WHERE act.is_test = 0
      AND act.is_deleted = 0
      AND act.is_current_revision = 1
      AND act.activity_date_time >= %(start)s
      AND act.activity_date_time <= %(end)s
      AND act.activity_type_id IN (35, 36, 7)
),

sources AS (
    -- Source contacts (record_type_id = 2): the person who created/initiated the activity.
    -- Pulls contact ID and primary email for attribution and notification purposes.
    SELECT
        act.id                          AS activity_id,
        c.id                            AS contact_source_id,
        e.email                         AS email_source
    FROM civicrm_activity act
    INNER JOIN civicrm_activity_contact ac
           ON act.id = ac.activity_id AND ac.record_type_id = 2
    INNER JOIN civicrm_contact c
           ON ac.contact_id = c.id
    LEFT JOIN civicrm_email e
           ON ac.contact_id = e.contact_id AND e.is_primary = 1
    WHERE act.is_test = 0
      AND act.is_deleted = 0
      AND act.is_current_revision = 1
      AND act.activity_date_time >= %(start)s
      AND act.activity_date_time <= %(end)s
      AND act.activity_type_id IN (35, 36, 7)
)

SELECT
    GROUP_CONCAT(DISTINCT a.contact_assignee    SEPARATOR '\n\n;') AS civicrm_contact_contact_assignee,
    GROUP_CONCAT(DISTINCT t.contact_target      SEPARATOR '\n\n;') AS civicrm_contact_contact_target,
    GROUP_CONCAT(DISTINCT s.contact_source_id   SEPARATOR '\n\n;') AS civicrm_contact_contact_source_id,
    GROUP_CONCAT(DISTINCT a.contact_assignee_id SEPARATOR '\n\n;') AS civicrm_contact_contact_assignee_id,
    GROUP_CONCAT(DISTINCT t.contact_target_id   SEPARATOR '\n\n;') AS civicrm_contact_contact_target_id,
    GROUP_CONCAT(DISTINCT s.email_source        SEPARATOR '\n\n;') AS civicrm_email_contact_source_email,
    GROUP_CONCAT(DISTINCT t.email_target        SEPARATOR '\n\n;') AS civicrm_email_contact_target_email,
    act.id                    AS civicrm_activity_id,
    act.source_record_id      AS civicrm_activity_source_record_id,
    act.activity_type_id      AS civicrm_activity_activity_type_id,
    act.subject               AS civicrm_activity_activity_subject,
    act.activity_date_time    AS civicrm_activity_activity_date_time,
    act.status_id             AS civicrm_activity_status_id,
    act.details               AS civicrm_activity_details
FROM civicrm_activity act
LEFT JOIN targets   t ON act.id = t.activity_id
LEFT JOIN assignees a ON act.id = a.activity_id
LEFT JOIN sources   s ON act.id = s.activity_id
WHERE act.is_test = 0
  AND act.is_deleted = 0
  AND act.is_current_revision = 1
  AND act.activity_date_time >= %(start)s
  AND act.activity_date_time <= %(end)s
  AND act.activity_type_id IN (35, 36, 7)
GROUP BY
    act.id,
    act.source_record_id,
    act.activity_type_id,
    act.subject,
    act.activity_date_time,
    act.status_id,
    act.details