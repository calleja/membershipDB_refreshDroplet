SET SESSION group_concat_max_len = 65536;

SELECT
    GROUP_CONCAT(DISTINCT combined.assignee_name SEPARATOR ';') AS civicrm_contact_contact_assignee,
    GROUP_CONCAT(DISTINCT combined.target_name    SEPARATOR ';') AS civicrm_contact_contact_target,
    GROUP_CONCAT(DISTINCT combined.source_id      SEPARATOR ';') AS civicrm_contact_contact_source_id,
    GROUP_CONCAT(DISTINCT combined.assignee_id    SEPARATOR ';') AS civicrm_contact_contact_assignee_id,
    GROUP_CONCAT(DISTINCT combined.target_id      SEPARATOR ';') AS civicrm_contact_contact_target_id,
    GROUP_CONCAT(DISTINCT combined.source_email   SEPARATOR ';') AS civicrm_email_contact_source_email,
    GROUP_CONCAT(DISTINCT combined.target_email   SEPARATOR ';') AS civicrm_email_contact_target_email,
    MIN(combined.activity_id)        AS civicrm_activity_id,
    MIN(combined.source_record_id)   AS civicrm_activity_source_record_id,
    MIN(combined.activity_type_id)   AS civicrm_activity_activity_type_id,
    MIN(combined.subject)            AS civicrm_activity_activity_subject,
    MIN(combined.activity_date_time) AS civicrm_activity_activity_date_time,
    MIN(combined.status_id)          AS civicrm_activity_status_id,
    MIN(combined.details)            AS civicrm_activity_details

FROM (

    /* ── Target contacts (record_type_id = 3) ── */
    SELECT
        NULL                                        AS assignee_name,
        c_target.sort_name                          AS target_name,
        NULL                                        AS source_id,
        NULL                                        AS assignee_id,
        CAST(c_target.id AS CHAR(20))               AS target_id,
        NULL                                        AS source_email,
        e_target.email                              AS target_email,
        a.id                                        AS activity_id,
        a.source_record_id,
        a.activity_type_id,
        a.subject,
        a.activity_date_time,
        a.status_id,
        a.details
    FROM civicrm_activity a
    INNER JOIN civicrm_activity_contact ac
           ON a.id = ac.activity_id AND ac.record_type_id = 3
    INNER JOIN civicrm_contact c_target
           ON ac.contact_id = c_target.id
    LEFT  JOIN civicrm_email e_target
           ON ac.contact_id = e_target.contact_id AND e_target.is_primary = 1
    WHERE a.is_test = 0
      AND a.is_deleted = 0
      AND a.is_current_revision = 1
      AND a.activity_date_time >= 20250201000000
      AND a.activity_date_time <= 20250215000000
      AND a.activity_type_id IN (35, 36, 7)

    UNION ALL

    /* ── Assignee contacts (record_type_id = 1) ── */
    SELECT
        c_assignee.sort_name                        AS assignee_name,
        NULL                                        AS target_name,
        NULL                                        AS source_id,
        CAST(c_assignee.id AS CHAR(20))             AS assignee_id,
        NULL                                        AS target_id,
        NULL                                        AS source_email,
        NULL                                        AS target_email,
        a.id                                        AS activity_id,
        a.source_record_id,
        a.activity_type_id,
        a.subject,
        a.activity_date_time,
        a.status_id,
        a.details
    FROM civicrm_activity a
    INNER JOIN civicrm_activity_contact ac
           ON a.id = ac.activity_id AND ac.record_type_id = 1
    INNER JOIN civicrm_contact c_assignee
           ON ac.contact_id = c_assignee.id
    WHERE a.is_test = 0
      AND a.is_deleted = 0
      AND a.is_current_revision = 1
      AND a.activity_date_time >= 20250201000000
      AND a.activity_date_time <= 20250215000000
      AND a.activity_type_id IN (35, 36, 7)

    UNION ALL

    /* ── Source contacts (record_type_id = 2) ── */
    SELECT
        NULL                                        AS assignee_name,
        NULL                                        AS target_name,
        CAST(c_source.id AS CHAR(20))               AS source_id,
        NULL                                        AS assignee_id,
        NULL                                        AS target_id,
        e_source.email                              AS source_email,
        NULL                                        AS target_email,
        a.id                                        AS activity_id,
        a.source_record_id,
        a.activity_type_id,
        a.subject,
        a.activity_date_time,
        a.status_id,
        a.details
    FROM civicrm_activity a
    INNER JOIN civicrm_activity_contact ac
           ON a.id = ac.activity_id AND ac.record_type_id = 2
    INNER JOIN civicrm_contact c_source
           ON ac.contact_id = c_source.id
    LEFT  JOIN civicrm_email e_source
           ON ac.contact_id = e_source.contact_id AND e_source.is_primary = 1
    WHERE a.is_test = 0
      AND a.is_deleted = 0
      AND a.is_current_revision = 1
      AND a.activity_date_time >= 20250201000000
      AND a.activity_date_time <= 20250215000000
      AND a.activity_type_id IN (35, 36, 7)

) AS combined

GROUP BY combined.activity_id

ORDER BY MIN(combined.activity_date_time) ASC,
         FIELD(MIN(combined.activity_type_id),
               66,27,20,74,72,19,37,18,16,53,26,15,33,35,36,
               48,52,51,6,42,49,43,3,50,73,5,41,54,14,12,45,
               62,68,25,64,71,34,56,1,8,17,7,58,23,70,13,67,
               4,46,2,10,11,22,24,47,40,21,44,60,69,55,9,39,38) ASC;