-- Derived-table rewrite of cte_select_activity_detail.sql.
-- CTEs (WITH ... AS) require MariaDB 10.2+; this server is 10.1.x.
-- Inline subqueries in the FROM clause are equivalent and work on 10.1.
-- Accepts %(start)s / %(end)s in YYYYMMDDhhmmss format.
-- Currently the PROD .sql

/*
minimum expected fields
Target Name <- full name
Source Email	<- either their personal or IT/admin@ghfc
Target Email	<- either their personal or IT/admin@ghfc
Activity Type <- either: 1. 'Membership Signup',2. 'Change Membership Status',
3. 'Change Membership Type'
Subject <- full description of activity, ex. "Trial Membership - Trial Plan Application"
Activity Date	
Activity Status <- typically "Completed"

*/

SELECT
    GROUP_CONCAT(DISTINCT a.contact_assignee  SEPARATOR '\n\n;') AS Assignee_Name_act,
    GROUP_CONCAT(DISTINCT t.contact_target    SEPARATOR '\n\n;') AS Target_Name_act,
    GROUP_CONCAT(DISTINCT s.email_source      SEPARATOR '\n\n;') AS Source_Email_act,
    GROUP_CONCAT(DISTINCT t.email_target      SEPARATOR '\n\n;') AS Target_Email_act,
    CASE 
    WHEN act.activity_type_id = 7 THEN 'Membership Signup'
    WHEN act.activity_type_id = 35 THEN 'Change Membership Status'
    WHEN act.activity_type_id = 36 THEN 'Change Membership Type'
    ELSE act.activity_type_id
    END AS Activity_Type_act,
    -- act.activity_type_id    AS Activity_Type_act,
    act.subject             AS Subject_act, 
    act.activity_date_time  AS Activity_Date_act,
    CASE 
    WHEN act.status_id = 2 THEN 'Completed'
    ELSE act.status_id 
    END AS Activity_Status_act,
    act.details             AS Activity_Details_act
FROM civicrm_activity act

-- targets: contacts the activity is directed at (record_type_id = 3)
LEFT JOIN (
    SELECT
        act2.id             AS activity_id,
        c.sort_name         AS contact_target,
        c.id                AS contact_target_id,
        e.email             AS email_target
    FROM civicrm_activity act2
    INNER JOIN civicrm_activity_contact ac
           ON act2.id = ac.activity_id AND ac.record_type_id = 3
    INNER JOIN civicrm_contact c
           ON ac.contact_id = c.id
    LEFT  JOIN civicrm_email e
           ON ac.contact_id = e.contact_id AND e.is_primary = 1
    WHERE act2.is_test = 0
      AND act2.is_deleted = 0
      AND act2.is_current_revision = 1
      AND act2.activity_date_time >= %(start)s
      AND act2.activity_date_time <= %(end)s
      AND act2.activity_type_id IN (35, 36, 7)
) t ON act.id = t.activity_id

-- assignees: person responsible for the activity (record_type_id = 1)
LEFT JOIN (
    SELECT
        act3.id             AS activity_id,
        c.sort_name         AS contact_assignee,
        c.id                AS contact_assignee_id
    FROM civicrm_activity act3
    INNER JOIN civicrm_activity_contact ac
           ON act3.id = ac.activity_id AND ac.record_type_id = 1
    INNER JOIN civicrm_contact c
           ON ac.contact_id = c.id
    WHERE act3.is_test = 0
      AND act3.is_deleted = 0
      AND act3.is_current_revision = 1
      AND act3.activity_date_time >= %(start)s
      AND act3.activity_date_time <= %(end)s
      AND act3.activity_type_id IN (35, 36, 7)
) a ON act.id = a.activity_id

-- sources: person who created / initiated the activity (record_type_id = 2)
LEFT JOIN (
    SELECT
        act4.id             AS activity_id,
        c.id                AS contact_source_id,
        e.email             AS email_source
    FROM civicrm_activity act4
    INNER JOIN civicrm_activity_contact ac
           ON act4.id = ac.activity_id AND ac.record_type_id = 2
    INNER JOIN civicrm_contact c
           ON ac.contact_id = c.id
    LEFT  JOIN civicrm_email e
           ON ac.contact_id = e.contact_id AND e.is_primary = 1
    WHERE act4.is_test = 0
      AND act4.is_deleted = 0
      AND act4.is_current_revision = 1
      AND act4.activity_date_time >= %(start)s
      AND act4.activity_date_time <= %(end)s
      AND act4.activity_type_id IN (35, 36, 7)
) s ON act.id = s.activity_id

WHERE act.is_test = 0
  AND act.is_deleted = 0
  AND act.is_current_revision = 1
  AND act.activity_date_time >= %(start)s
  AND act.activity_date_time <= %(end)s
  AND act.activity_type_id IN (35, 36, 7)
GROUP BY
    act.id,
    act.activity_type_id,
    act.subject,
    act.activity_date_time,
    act.status_id,
    act.details