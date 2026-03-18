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

-- REFACTORED: final SELECT reduced from 14 columns to 9 and aliased
-- to match downstream column names expected by the target table
-- selectActivityReport_temp.  Dropped columns: contact_source_id,
-- contact_assignee_id, contact_target_id, activity_id, source_record_id.
SELECT
    GROUP_CONCAT(DISTINCT a.contact_assignee    SEPARATOR '\n\n;') AS Assignee_Name_act,
    GROUP_CONCAT(DISTINCT t.contact_target      SEPARATOR '\n\n;') AS Target_Name_act,
    GROUP_CONCAT(DISTINCT s.email_source        SEPARATOR '\n\n;') AS Source_Email_act,
    GROUP_CONCAT(DISTINCT t.email_target        SEPARATOR '\n\n;') AS Target_Email_act,
    act.activity_type_id      AS Activity_Type_act,
    act.subject               AS Subject_act,
    act.activity_date_time    AS Activity_Date_act,
    act.status_id             AS Activity_Status_act,
    act.details               AS Activity_Details_act
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
-- REFACTORED: GROUP BY trimmed to match the 5 non-aggregated columns
-- (removed act.id, act.source_record_id which are no longer selected)
GROUP BY
    act.id,
    act.activity_type_id,
    act.subject,
    act.activity_date_time,
    act.status_id,
    act.details