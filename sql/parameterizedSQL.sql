SELECT civicrm_contact_target.sort_name as civicrm_contact_contact_target, civicrm_contact_target.id as civicrm_contact_contact_target_id, civicrm_email_target.email as civicrm_email_contact_target_email, activity_civireport.id as civicrm_activity_id, activity_civireport.source_record_id as civicrm_activity_source_record_id, activity_civireport.activity_type_id as civicrm_activity_activity_type_id, activity_civireport.subject as civicrm_activity_activity_subject, activity_civireport.activity_date_time as civicrm_activity_activity_date_time, activity_civireport.status_id as civicrm_activity_status_id, activity_civireport.details as civicrm_activity_details, address_civireport.street_name as civicrm_address_street_name, address_civireport.street_number as civicrm_address_street_number, address_civireport.street_address as civicrm_address_street_address, address_civireport.city as civicrm_address_city, address_civireport.postal_code as civicrm_address_postal_code  
      

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
