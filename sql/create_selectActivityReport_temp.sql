-- DDL for the staging table on the target database (membership_ard).
-- Used by importer.ensure_table() which runs DROP + CREATE each sync cycle
-- to guarantee the schema matches the 9-column output of
-- cte_select_activity_detail.sql.
--
-- Column names match the aliases in the CTE's final SELECT so that
-- cursor.description column names line up with INSERT column names.

DROP TABLE IF EXISTS selectActivityReport_temp;

CREATE TABLE selectActivityReport_temp (
    Assignee_Name_act    TEXT            COMMENT 'GROUP_CONCAT of assignee sort_name values',
    Target_Name_act      TEXT            COMMENT 'GROUP_CONCAT of target sort_name values',
    Source_Email_act     VARCHAR(255)    COMMENT 'GROUP_CONCAT of source primary emails',
    Target_Email_act     VARCHAR(255)    COMMENT 'GROUP_CONCAT of target primary emails',
    Activity_Type_act    INT             COMMENT 'activity_type_id: 35, 36, or 7',
    Subject_act          VARCHAR(255)    COMMENT 'activity subject line',
    Activity_Date_act    DATETIME        COMMENT 'activity_date_time from civicrm_activity',
    Activity_Status_act  INT             COMMENT 'activity status_id',
    Activity_Details_act LONGTEXT        COMMENT 'free-text activity details / notes'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
