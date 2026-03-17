-- parameterized version of select_activity_details_raw.sql
-- accepts start/end in format YYYYMMDDhhmmss (CLI will convert from YYYYMMDD)

CREATE TEMPORARY TABLE `civicrm_tmp_e_dflt_82940231795caa350de72b85306a3272` ENGINE=InnoDB COLLATE utf8_unicode_ci AS SELECT civicrm_contact_target.sort_name as civicrm_contact_contact_target, civicrm_contact_target.id as civicrm_contact_contact_target_id, civicrm_email_target.email as civicrm_email_contact_target_email, activity_civireport.id as civicrm_activity_id, activity_civireport.source_record_id as civicrm_activity_source_record_id, activity_civireport.activity_type_id as civicrm_activity_activity_type_id, activity_civireport.subject as civicrm_activity_activity_subject, activity_civireport.activity_date_time as civicrm_activity_activity_date_time, activity_civireport.status_id as civicrm_activity_status_id, activity_civireport.details as civicrm_activity_details, address_civireport.street_name as civicrm_address_street_name, address_civireport.street_number as civicrm_address_street_number, address_civireport.street_address as civicrm_address_street_address, address_civireport.city as civicrm_address_city, address_civireport.postal_code as civicrm_address_postal_code  
      

FROM civicrm_activity activity_civireport
           INNER JOIN civicrm_activity_contact  activity_contact_civireport
                  ON activity_civireport.id = activity_contact_civireport.activity_id AND
                     activity_contact_civireport.record_type_id = 3
           INNER JOIN civicrm_contact civicrm_contact_target
                  ON activity_contact_civireport.contact_id = civicrm_contact_target.id
           
          
  LEFT JOIN civicrm_email civicrm_email_target
                 ON activity_contact_civireport.contact_id = civicrm_email_target.contact_id AND
                    civicrm_email_target.is_primary = 1
                 
  LEFT JOIN civicrm_address address_civireport
                           ON (civicrm_contact_target.id =
                               address_civireport.contact_id)  AND
                               address_civireport.is_primary = 1
  


WHERE activity_civireport.is_test = 0 AND
                                activity_civireport.is_deleted = 0 AND
                                activity_civireport.is_current_revision = 1 AND ( activity_civireport.activity_date_time >= %(start)s) AND ( activity_civireport.activity_date_time <= %(end)s) AND ( activity_civireport.activity_type_id IN (35, 36, 7) );



  ALTER TABLE  civicrm_tmp_e_dflt_82940231795caa350de72b85306a3272
  MODIFY COLUMN civicrm_contact_contact_target_id VARCHAR(128),
  ADD COLUMN civicrm_contact_contact_assignee VARCHAR(128),
  ADD COLUMN civicrm_contact_contact_source VARCHAR(128),
  ADD COLUMN civicrm_contact_contact_assignee_id VARCHAR(128),
  ADD COLUMN civicrm_contact_contact_source_id VARCHAR(128),
  ADD COLUMN civicrm_phone_contact_assignee_phone VARCHAR(128),
  ADD COLUMN civicrm_phone_contact_source_phone VARCHAR(128),
  ADD COLUMN civicrm_email_contact_assignee_email VARCHAR(128),
  ADD COLUMN civicrm_email_contact_source_email VARCHAR(128);



INSERT INTO civicrm_tmp_e_dflt_82940231795caa350de72b85306a3272 (civicrm_contact_contact_assignee,civicrm_contact_contact_assignee_id,civicrm_activity_id,civicrm_activity_source_record_id,civicrm_activity_activity_type_id,civicrm_activity_activity_subject,civicrm_activity_activity_date_time,civicrm_activity_status_id,civicrm_activity_details)
SELECT civicrm_contact_assignee.sort_name as civicrm_contact_contact_assignee, civicrm_contact_assignee.id as civicrm_contact_contact_assignee_id, activity_civireport.id as civicrm_activity_id, activity_civireport.source_record_id as civicrm_activity_source_record_id, activity_civireport.activity_type_id as civicrm_activity_activity_type_id, activity_civireport.subject as civicrm_activity_activity_subject, activity_civireport.activity_date_time as civicrm_activity_activity_date_time, activity_civireport.status_id as civicrm_activity_status_id, activity_civireport.details as civicrm_activity_details 

      

FROM civicrm_activity activity_civireport
           INNER JOIN civicrm_activity_contact  activity_contact_civireport
                  ON activity_civireport.id = activity_contact_civireport.activity_id AND
                     activity_contact_civireport.record_type_id = 1
           INNER JOIN civicrm_contact civicrm_contact_assignee
                  ON activity_contact_civireport.contact_id = civicrm_contact_assignee.id
           
          
  LEFT JOIN civicrm_email civicrm_email_assignee
                 ON activity_contact_civireport.contact_id = civicrm_email_assignee.contact_id AND
                    civicrm_email_assignee.is_primary = 1
                 
  LEFT JOIN civicrm_address address_civireport
                           ON (civicrm_contact_assignee.id =
                               address_civireport.contact_id)  AND
                               address_civireport.is_primary = 1
  


WHERE activity_civireport.is_test = 0 AND
                                activity_civireport.is_deleted = 0 AND
                                activity_civireport.is_current_revision = 1 AND ( activity_civireport.activity_date_time >= %(start)s) AND ( activity_civireport.activity_date_time <= %(end)s) AND ( activity_civireport.activity_type_id IN (35, 36, 7) );



INSERT INTO civicrm_tmp_e_dflt_82940231795caa350de72b85306a3272 (civicrm_contact_contact_source_id,civicrm_email_contact_source_email,civicrm_activity_id,civicrm_activity_source_record_id,civicrm_activity_activity_type_id,civicrm_activity_activity_subject,civicrm_activity_activity_date_time,civicrm_activity_status_id,civicrm_activity_details)
SELECT civicrm_contact_source.id as civicrm_contact_contact_source_id, civicrm_email_source.email as civicrm_email_contact_source_email, activity_civireport.id as civicrm_activity_id, activity_civireport.source_record_id as civicrm_activity_source_record_id, activity_civireport.activity_type_id as civicrm_activity_activity_type_id, activity_civireport.subject as civicrm_activity_activity_subject, activity_civireport.activity_date_time as civicrm_activity_activity_date_time, activity_civireport.status_id as civicrm_activity_status_id, activity_civireport.details as civicrm_activity_details 

      

FROM civicrm_activity activity_civireport
           INNER JOIN civicrm_activity_contact  activity_contact_civireport
                  ON activity_civireport.id = activity_contact_civireport.activity_id AND
                     activity_contact_civireport.record_type_id = 2
           INNER JOIN civicrm_contact civicrm_contact_source
                  ON activity_contact_civireport.contact_id = civicrm_contact_source.id
           
          
  LEFT JOIN civicrm_email civicrm_email_source
                 ON activity_contact_civireport.contact_id = civicrm_email_source.contact_id AND
                    civicrm_email_source.is_primary = 1
                 
  LEFT JOIN civicrm_address address_civireport
                           ON (civicrm_contact_source.id =
                               address_civireport.contact_id)  AND
                               address_civireport.is_primary = 1
  


WHERE activity_civireport.is_test = 0 AND
                                activity_civireport.is_deleted = 0 AND
                                activity_civireport.is_current_revision = 1 AND ( activity_civireport.activity_date_time >= %(start)s) AND ( activity_civireport.activity_date_time <= %(end)s) AND ( activity_civireport.activity_type_id IN (35, 36, 7) );



SELECT SQL_CALC_FOUND_ROWS GROUP_CONCAT(DISTINCT civicrm_contact_contact_assignee SEPARATOR '\n\n;') as civicrm_contact_contact_assignee, GROUP_CONCAT(DISTINCT civicrm_contact_contact_target SEPARATOR '\n\n;') as civicrm_contact_contact_target, GROUP_CONCAT(DISTINCT civicrm_contact_contact_source_id SEPARATOR '\n\n;') as civicrm_contact_contact_source_id, GROUP_CONCAT(DISTINCT civicrm_contact_contact_assignee_id SEPARATOR '\n\n;') as civicrm_contact_contact_assignee_id, GROUP_CONCAT(DISTINCT civicrm_contact_contact_target_id SEPARATOR '\n\n;') as civicrm_contact_contact_target_id, GROUP_CONCAT(DISTINCT civicrm_email_contact_source_email SEPARATOR '\n\n;') as civicrm_email_contact_source_email, GROUP_CONCAT(DISTINCT civicrm_email_contact_target_email SEPARATOR '\n\n;') as civicrm_email_contact_target_email, civicrm_activity_id, civicrm_activity_source_record_id, civicrm_activity_activity_type_id, civicrm_activity_activity_subject, civicrm_activity_activity_date_time, civicrm_activity_status_id, civicrm_activity_details 
      

FROM civicrm_tmp_e_dflt_82940231795caa350de72b85306a3272 tar
      INNER JOIN civicrm_activity activity_civireport ON activity_civireport.id = tar.civicrm_activity_id
      INNER JOIN civicrm_activity_contact activity_contact_civireport on activity_contact_civireport.activity_id = activity_civireport.id
      /* Lines 120-122 omitted */      
  LEFT JOIN civicrm_contact contact_civireport ON contact_civireport.id = activity_contact_civireport.contact_id
      
       

/* Lines 126-132 omitted */
LIMIT 0, 50

