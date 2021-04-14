<?php

App::uses("CoJobBackend", "Model");

class XcdbsyncJob extends CoJobBackend {
  // Required by COmanage Plugins
  public $cmPluginType = "job";

  // Document foreign keys
  public $cmPluginHasMany = array();

  // Validation rules for table elements
  public $validate = array();

  // Current CO Job Object
  private $CoJob;

  // Current CO ID
  private $coId;

  /**
   * Expose menu items.
   * 
   * @return Array with menu location type as key and array of labels, controllers, actions as values.
   */
  public function cmPluginMenus() {
    return array();
  }


  /**
   * Create the WBS COU structure.
   *
   * @return void
   */
  public function createWbsCousAndDepartments() {

    $wbsCouStructureFile = fopen("../local/wbs_cou_structure.txt", "r");
    while(!feof($wbsCouStructureFile)) {
      $wbsCouStructureLine = trim(fgets($wbsCouStructureFile));

      // Skip lines beginning with hash character.
      if(substr($wbsCouStructureLine, 0, 1) == "#") {
        continue;
      }

      // Skip empty lines.
      if(empty($wbsCouStructureLine)) {
        continue;
      }

      $wbsCouInputs = explode(";", $wbsCouStructureLine);

      $couName = $wbsCouInputs[0];
      $couDescription = $wbsCouInputs[1];
      $couParentName = $wbsCouInputs[2];

      $currentCous = $this->CoJob->Co->Cou->allCous($this->coId, "hash");

      $couId = array_search($couName, $currentCous);

      if($couId === false) {
        $couParentId = array_search($couParentName, $currentCous);

        $this->CoJob->Co->Cou->clear();

        $cou = array();
        $cou['Cou']['co_id'] = $this->coId;
        $cou['Cou']['name'] = $couName;
        $cou['Cou']['description'] = $couDescription;
        $cou['Cou']['parent_id'] = $couParentId;

        if(!$this->CoJob->Co->Cou->save($cou)) {
          $this->log("Failed to save the COU " . print_r($cou, true));
        } else {
          $this->log("Created the COU " . $cou['Cou']['name']);
        }
      } else {
        $this->log("COU $couName already exists");
      }
    }

    fclose($wbsCouStructureFile);
  }

  /**
   * Create the top level WBS 2.0 COU.
   *
   * @return void
   */
  public function createWbs20Cou() {
    $currentCous = $this->CoJob->Co->Cou->allCous($this->coId);

    if(!in_array('WBS 2.0', $currentCous)) {
      $mou = array();
      $mou['Cou'] = array();
      $mou['Cou']['co_id'] = $this->coId;
      $mou['Cou']['name'] = 'WBS 2.0';
      $mou['Cou']['description'] = 'WBS 2.0';

      if(!$this->CoJob->Co->Cou->save($mou)) {
        $this->log("Failed to save the top level WBS 2.0 COU");
      } else {
        $this->log("Created the top level WBS 2.0 COU");
      }
    } else {
      $this->log("Top level WBS 2.0 COU already exists");
    }
  }

  /**
   * Create the top level WBS 2.0 COU.
   *
   * @return void
   */
  public function createXsedeUsernameExtendedType() {
    $args = array();
    $args['conditions']['CoExtendedType.co_id'] = $this->coId;
    $args['conditions']['CoExtendedType.attribute'] = 'Identifier.type';
    $args['conditions']['CoExtendedType.name'] = 'xsedeusername';
    $args['contain'] = false;

    $exType = $this->CoJob->Co->CoExtendedType->find("first", $args);
    if(!empty($exType)) {
      $this->log("XSEDE username extended type exists");
      return;
    }

    $this->CoJob->Co->CoExtendedType->clear();

    $data = array();
    $data['CoExtendedType']['co_id'] = $this->coId;
    $data['CoExtendedType']['attribute'] = 'Identifier.type';
    $data['CoExtendedType']['name'] = 'xsedeusername';
    $data['CoExtendedType']['display_name'] = 'XSEDE Username';
    $data['CoExtendedType']['status'] = SuspendableStatusEnum::Active;

    if(!$this->CoJob->Co->CoExtendedType->save($data)) {
      $this->log("Failed to save the XSEDE Username extended type " . print_r($data, true));
    }
  }


  /**
   * Execute the requested Job.
   *
   * @param  int   $coId    CO ID
   * @param  CoJob $CoJob   CO Job Object, id available at $CoJob->id
   * @param  array $params  Array of parameters, as requested via parameterFormat()
   * @throws InvalidArgumentException
   * @throws RuntimeException
   * @return void
   */
  public function execute($coId, $CoJob, $params) {
    $CoJob->update($CoJob->id, null, "full", null);

    $this->CoJob = $CoJob;
    $this->coId = $coId;

    // Bootstrap if so configured.
    $bootstrap = Configure::read('XcdbsyncJob.bootstrap');
    if($bootstrap) {
      // Create the top level WBS COU if not present.
      $this->createWbs20Cou();

      // Create the WBS COU structure if not present.
      $this->createWbsCousAndDepartments();

      // Create the XSEDE Username extended type if not present.
      $this->createXsedeUsernameExtendedType();

      // Process XSEDE username to WBS mapping file.
      $this->processBootstrapFile();  
    }

    // Synchronize existing CO Person records with XDCDB.
    $result = $this->synchronize();

    $synchronized = $result['synchronized'];
    $failed = $result['failed'];
    $status = $result['status'];
    $summary = "Successfully synchronized $synchronized records and recorded $failed failures";

    $CoJob->finish($CoJob->id, $summary, $status);
  }

  /**
   * Obtain the list of parameters supported by this Job.
   *
   * @since  COmanage Registry v3.3.0
   * @return Array Array of supported parameters.
   */
  public function parameterFormat() {

    $params = array();

    return $params;
  }

  /** 
   * Process the input bootstrap file to create CO Person records indexed
   * by XSEDE username and with CO Person Roles in WBS COUs.
   *
   * @return void
   */
  public function processBootstrapFile() {
    // Get current COUs so that we can create CO Person Roles attached to COUs.
    $currentCous = $this->CoJob->Co->Cou->allCous($this->coId, "hash");

    // Process the bootstrap file line by line, creating when necessary
    // all COmanage Registry objects.
    
    $bootstrapFile = fopen("../local/xsede_username_wbs.txt", "r");
    while(!feof($bootstrapFile)) {
      $bootstrapLine = trim(fgets($bootstrapFile));

      // Skip lines beginning with hash character.
      if(substr($bootstrapLine, 0, 1) == "#") {
        continue;
      }

      // Skip empty lines.
      if(empty($bootstrapLine)) {
        continue;
      }

      $bootstrapInputs = explode(";", $bootstrapLine);

      $xsedeUsername = $bootstrapInputs[0];
      $wbsNumber = $bootstrapInputs[1];

      // Search to see if this username exists.
      $args = array();
      $args['conditions']['Identifier.identifier'] = $xsedeUsername;
      $args['conditions']['Identifier.type'] = 'xsedeusername';
      $args['contain'] = false;

      $identifier = $this->CoJob->Co->CoPerson->Identifier->find('all', $args);

      if(empty($identifier)) {
        $this->log("Identifier of type xsedeusername with value $xsedeUsername not found");

        // Begin a transaction.
        $dataSource = $this->CoJob->getDataSource();
        $dataSource->begin();

        // Create the Org Identity.
        $this->CoJob->Co->CoPerson->CoOrgIdentityLink->OrgIdentity->clear();

        $data = array();
        $data['OrgIdentity'] = array();
        $data['OrgIdentity']['co_id'] = $this->coId;

        if(!$this->CoJob->Co->CoPerson->CoOrgIdentityLink->OrgIdentity->save($data)) {
          $this->log("ERROR could not create OrgIdentity");
          $this->log("ERROR OrgIdentity validation errors:");
          $this->log(print_r($this->CoJob->Co->CoPerson->CoOrgIdentityLink->OrgIdentity->validationErrors, true));
          $dataSource->rollback();
          continue;
        }

        // Record the OrgIdentity ID for later assignment.
        $orgIdentityId = $this->CoJob->Co->CoPerson->CoOrgIdentityLink->OrgIdentity->id;

        // Attach a Name to the OrgIdentity.
        $this->CoJob->Co->CoPerson->CoOrgIdentityLink->OrgIdentity->Name->clear();

        $data = array();
        $data['Name'] = array();
        $data['Name']['given'] = "placeholder";
        $data['Name']['type'] = NameEnum::Official;
        $data['Name']['primary_name'] = true;
        $data['Name']['org_identity_id'] = $orgIdentityId;

        if(!$this->CoJob->Co->CoPerson->CoOrgIdentityLink->OrgIdentity->Name->save($data)) {
          $this->log("ERROR could not create Name for OrgIdentity");
          $this->log("ERROR Name validation errors:");
          $this->log(print_r($this->CoJob->Co->CoPerson->CoOrgIdentityLink->OrgIdentity->Name->validationErrors, true));
          $dataSource->rollback();
          continue;
        }

        // Attach an Identifier of type EPPN to the OrgIdentity.
        $this->CoJob->Co->CoPerson->CoOrgIdentityLink->OrgIdentity->Identifier->clear();

        $data = array();
        $data['Identifier'] = array();
        $data['Identifier']['identifier'] = $xsedeUsername . '@xsede.org';
        $data['Identifier']['type'] = IdentifierEnum::ePPN;
        $data['Identifier']['status'] = SuspendableStatusEnum::Active;
        $data['Identifier']['login'] = true;
        $data['Identifier']['org_identity_id'] = $orgIdentityId;

        if(!$this->CoJob->Co->CoPerson->CoOrgIdentityLink->OrgIdentity->Identifier->save($data)) {
          $this->log("ERROR could not create Identifier for OrgIdentity");
          $this->log("ERROR Identifier validation errors:");
          $this->log(print_r($this->CoJob->Co->CoPerson->CoOrgIdentityLink->OrgIdentity->Identifier->validationErrors, true));
          $dataSource->rollback();
          continue;
        }
    
        // Create the CO Person object.
        $this->CoJob->Co->CoPerson->clear();

        $data = array();
        $data['CoPerson'] = array();
        $data['CoPerson']['co_id'] = $this->coId;
        $data['CoPerson']['status'] = StatusEnum::Active;

        if(!$this->CoJob->Co->CoPerson->save($data)) {
          $this->log("ERROR could not create CoPerson");
          $this->log("ERROR CoPerson validation errors:");
          $this->log(print_r($this->CoJob->Co->CoPerson->validationErrors, true));
          $dataSource->rollback();
          continue;
        }

        // Record the CoPerson ID for later assignment.
        $coPersonId = $this->CoJob->Co->CoPerson->id;

        // Link the CoPerson to OrgIdentity.
        $this->CoJob->Co->CoPerson->CoOrgIdentityLink->clear();

        $data = array();
        $data['CoOrgIdentityLink'] = array();
        $data['CoOrgIdentityLink']['co_person_id'] = $coPersonId;
        $data['CoOrgIdentityLink']['org_identity_id'] = $orgIdentityId;

        if(!$this->CoJob->Co->CoPerson->CoOrgIdentityLink->save($data)) {
          $this->log("ERROR could not link CoPerson and OrgIdentity");
          $this->log("ERROR CoOrgIdentityLink validation errors:");
          $this->log(print_r($this->CoJob->Co->CoPerson->CoOrgIdentityLink->validationErrors, true));
          $dataSource->rollback();
          continue;
        }

        // Attach a Name to the CoPerson record.
        $this->CoJob->Co->CoPerson->Name->clear();

        $data = array();
        $data['Name'] = array();
        $data['Name']['given'] = "placeholder";
        $data['Name']['type'] = NameEnum::Official;
        $data['Name']['primary_name'] = true;
        $data['Name']['co_person_id'] = $coPersonId;

        if(!$this->CoJob->Co->CoPerson->Name->save($data)) {
          $this->log("ERROR could not create Name for CoPerson");
          $this->log("ERROR Name validation errors:");
          $this->log(print_r($this->CoJob->Co->CoPerson->Name->validationErrors, true));
          $dataSource->rollback();
          continue;
        }

        // Attach a CoPersonRole to the CoPerson.
        $couId = array_search('WBS ' . $wbsNumber, $currentCous);

        $this->CoJob->Co->CoPerson->CoPersonRole->clear();
        
        $data = array();
        $data['CoPersonRole'] = array();
        $data['CoPersonRole']['co_person_id'] = $coPersonId;
        $data['CoPersonRole']['cou_id'] = $couId;
        $data['CoPersonRole']['status'] = StatusEnum::Active;
        $data['CoPersonRole']['affiliation'] = AffiliationEnum::Staff;

        if(!$this->CoJob->Co->CoPerson->CoPersonRole->save($data)) {
          $this->log("ERROR could not create CoPersonRole for CoPerson");
          $this->log("ERROR CoPersonRole validation errors:");
          $this->log(print_r($this->CoJob->Co->CoPerson->CoPersonRole->validationErrors, true));
          $dataSource->rollback();
          continue;
        }

        // Attach an Identifier with value XSEDE username to the CoPerson.
        $this->CoJob->Co->CoPerson->Identifier->clear();

        $data = array();
        $data['Identifier'] = array();
        $data['Identifier']['identifier'] = $xsedeUsername;
        $data['Identifier']['type'] = 'xsedeusername';
        $data['Identifier']['status'] = SuspendableStatusEnum::Active;
        $data['Identifier']['co_person_id'] = $coPersonId;

        $args = array();
        $args['validate'] = false;

        if(!$this->CoJob->Co->CoPerson->Identifier->save($data, $args)) {
          $this->log("ERROR could not create Identifier for CoPerson");
          $dataSource->rollback();
          continue;
        }

        // Commit the transaction.
        $dataSource->commit();

      } else {
        // CO Person record already exists so only consider CO Person Roles and create
        // new CO Person Role in the COU if it does not already exist.
        $newCouId = array_search('WBS ' . $wbsNumber, $currentCous);

        // Fetch the CO Person record and existing CO Person Roles.
        $coPersonId = $identifier[0]['Identifier']['co_person_id'];
        $args = array();
        $args['conditions']['CoPerson.co_id'] = $this->coId;
        $args['conditions']['CoPerson.status'] = StatusEnum::Active;
        $args['conditions']['CoPerson.id'] = $coPersonId;
        $args['contain']['CoPersonRole'] = [];

        $coPerson = $this->CoJob->Co->CoPerson->find("first", $args);

        $roleExists = false;
        foreach($coPerson['CoPersonRole'] as $role) {
          $existingCouId = $role['cou_id'];
          if($existingCouId == $newCouId) {
            $roleExists = true;
          }
        }

        if(!$roleExists) {
          $this->CoJob->Co->CoPerson->CoPersonRole->clear();
          
          $data = array();
          $data['CoPersonRole'] = array();
          $data['CoPersonRole']['co_person_id'] = $coPersonId;
          $data['CoPersonRole']['cou_id'] = $newCouId;
          $data['CoPersonRole']['status'] = StatusEnum::Active;
          $data['CoPersonRole']['affiliation'] = AffiliationEnum::Staff;

          if(!$this->CoJob->Co->CoPerson->CoPersonRole->save($data)) {
            $this->log("ERROR could not create CoPersonRole for CoPerson");
            $this->log("ERROR CoPersonRole validation errors:");
            $this->log(print_r($this->CoJob->Co->CoPerson->CoPersonRole->validationErrors, true));
          }
        }
      }
    }

    fclose($bootstrapFile);
  }

  /** 
   * Synchronize CO Person Name and EmailAddress details with
   * the XSEDE central database.
   *
   * @return array of number synchronized, number of failures, and JobStatusEnum
   */
  public function synchronize() {
    $synchronized = 0;
    $failed = 0;
    $ret = array();

    // Find all active CO Person records for the XSEDE staff CO.
    $args = array();
    $args['conditions']['CoPerson.co_id'] = $this->coId;
    $args['conditions']['CoPerson.status'] = StatusEnum::Active;
    $args['contain']['Identifier'] = [];
    $args['contain']['PrimaryName'] = [];
    $args['contain']['EmailAddress'] = [];
    $args['contain']['CoOrgIdentityLink']['OrgIdentity']['PrimaryName'] = [];
    $args['contain']['CoOrgIdentityLink']['OrgIdentity']['EmailAddress'] = [];
    $args['contain']['CoOrgIdentityLink']['OrgIdentity']['Identifier'] = [];

    $coPersonRecords = $this->CoJob->Co->CoPerson->find('all', $args);

    $coPersonCount = count($coPersonRecords);
    $coPersonRecordsProcessed = 0;

    // Loop over active CO Person records and synchronize with details
    // from the XDCDB.
    foreach($coPersonRecords as $coPerson) {
      // Return if we have been cancelled.
      if($this->CoJob->canceled($this->CoJob->id)) {
        $ret['synchronized'] = $synchronized;
        $ret['failed'] = $failed;
        $ret['status'] = JobStatusEnum::Canceled;
        return $ret;
      }

      $coPersonRecordsProcessed++;
      $percent = intval(round(($coPersonRecordsProcessed/$coPersonCount) * 100.0));
      $this->CoJob->setPercentComplete($this->CoJob->id, $percent);

      $coPersonId = $coPerson['CoPerson']['id'];

      // Find the XSEDE username.
      $xsedeUsername = null;
      if(!empty($coPerson['Identifier'])) {
        foreach($coPerson['Identifier'] as $identifier) {
          if($identifier['type'] == 'xsedeusername' && $identifier['status'] == SuspendableStatusEnum::Active) {
            $xsedeUsername = $identifier['identifier'];
            break;
          }
        }
      }

      if(empty($xsedeUsername)) {
        $jobHistoryRecordKey = $coPersonId;
        $jobHistoryComment = "CoPerson record has no XSEDE username";
        $this->CoJob->CoJobHistoryRecord->record($this->CoJob->id, $jobHistoryRecordKey, $jobHistoryComment, $coPersonId, null, JobStatusEnum::Failed);
        $failed++;
        continue;
      }

      $jobHistoryRecordKey = $xsedeUsername;

      $urlBase = Configure::read('XcdbsyncJob.xdcdb.userinfo.url.base');
      $url = $urlBase . $xsedeUsername;

      $ch = curl_init($url);

      curl_setopt($ch, CURLOPT_URL, $url);

      $headers = array();
      $headers[] = 'XA-AGENT: userinfo';
      $headers[] = 'XA-RESOURCE: registry-dev.xsede.org';
      $headers[] = 'XA-API-KEY: ' .  Configure::read('XcdbsyncJob.xdcdb.apikey');
      curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);

      curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);

      $response = curl_exec($ch);

      curl_close($ch);

      if($response === false) {
        $jobHistoryComment = "Unable to query XDCDB userinfo endpoint";
        $this->CoJob->CoJobHistoryRecord->record($this->CoJob->id, $jobHistoryRecordKey, $jobHistoryComment, $coPersonId, null, JobStatusEnum::Failed);
        $failed++;
        continue;
      }

      $decodedResponse = json_decode($response, true);
      $xdcdbRecord = $decodedResponse['result'];

      // Synchronize the CO Person Primary Name.
      if($coPerson['PrimaryName']['given'] != $xdcdbRecord['first_name']) {
        $this->CoJob->Co->CoPerson->Name->clear();
        $this->CoJob->Co->CoPerson->Name->id = $coPerson['PrimaryName']['id'];
        $this->CoJob->Co->CoPerson->Name->saveField('given', $xdcdbRecord['first_name']);
      }

      if($coPerson['PrimaryName']['family'] != $xdcdbRecord['last_name']) {
        $this->CoJob->Co->CoPerson->Name->clear();
        $this->CoJob->Co->CoPerson->Name->id = $coPerson['PrimaryName']['id'];
        $this->CoJob->Co->CoPerson->Name->saveField('family', $xdcdbRecord['last_name']);
      }

      // Sychronize the CO Person official EmailAddress.
      $official = null;
      foreach($coPerson['EmailAddress'] as $email) {
        if($email['type'] == EmailAddressEnum::Official) {
          $official = $email['mail'];
          $officialEmailId = $email['id'];
        }
      }
      if(!empty($official) && ($official != $xdcdbRecord['email'])) {
        $this->CoJob->Co->CoPerson->EmailAddress->clear();
        $this->CoJob->Co->CoPerson->EmailAddress->id = $officialEmailId;
        $this->CoJob->Co->CoPerson->EmailAddress->saveField('mail', $xdcdbRecord['email']);
      }
      if(empty($official)) {
        $data = array();
        $data['EmailAddress'] = array();
        $data['EmailAddress']['mail'] = $xdcdbRecord['email'];
        $data['EmailAddress']['type'] = EmailAddressEnum::Official;
        $data['EmailAddress']['verified'] = true;
        $data['EmailAddress']['co_person_id'] = $coPerson['CoPerson']['id'];

        $options = array();
        $options['trustVerified'] = true;

        $this->CoJob->Co->CoPerson->EmailAddress->clear();
        $this->CoJob->Co->CoPerson->EmailAddress->save($data, $options);
      }

      // Loop over the CoOrgIdentityLink to find the OrgIdentity with the XSEDE ePPN and
      // for that OrgIdentity synchronize the Primary Name and official Email Address.
      foreach($coPerson['CoOrgIdentityLink'] as $link) {
        $identifiers = $link['OrgIdentity']['Identifier'];
        foreach($identifiers as $i) {
          if($i['type'] == IdentifierEnum::ePPN && preg_match('/^.+@xsede.org$/i', $i['identifier'])) {

            // Synchronize the OrgIdentity Primary Name.
            $primary = $link['OrgIdentity']['PrimaryName'];  

            if($primary['given'] != $xdcdbRecord['first_name']) {
              $this->CoJob->Co->CoPerson->CoOrgIdentityLink->OrgIdentity->Name->clear();
              $this->CoJob->Co->CoPerson->CoOrgIdentityLink->OrgIdentity->Name->id = $primary['id'];
              $this->CoJob->Co->CoPerson->CoOrgIdentityLink->OrgIdentity->Name->saveField('given', $xdcdbRecord['first_name']);
            }

            if($primary['family'] != $xdcdbRecord['last_name']) {
              $this->CoJob->Co->CoPerson->CoOrgIdentityLink->OrgIdentity->Name->clear();
              $this->CoJob->Co->CoPerson->CoOrgIdentityLink->OrgIdentity->Name->id = $primary['id'];
              $this->CoJob->Co->CoPerson->CoOrgIdentityLink->OrgIdentity->Name->saveField('family', $xdcdbRecord['last_name']);
            }

            // Synchronize the OrgIdentity official EmailAddress.
            $emails = $link['OrgIdentity']['EmailAddress'];

            $official = null;
            foreach($emails as $email) {
              if($email['type'] == EmailAddressEnum::Official) {
                $official = $email['mail'];
                $officialEmailId = $email['id'];
              }
            }
            if(!empty($official) && ($official != $xdcdbRecord['email'])) {
              $this->CoJob->Co->CoPerson->CoOrgIdentityLink->OrgIdentity->EmailAddress->clear();
              $this->CoJob->Co->CoPerson->CoOrgIdentityLink->OrgIdentity->EmailAddress->id = $officialEmailId;
              $this->CoJob->Co->CoPerson->CoOrgIdentityLink->OrgIdentity->EmailAddress->saveField('mail', $xdcdbRecord['email']);
            }
            if(empty($official)) {
              $data = array();
              $data['EmailAddress'] = array();
              $data['EmailAddress']['mail'] = $xdcdbRecord['email'];
              $data['EmailAddress']['type'] = EmailAddressEnum::Official;
              $data['EmailAddress']['verified'] = true;
              $data['EmailAddress']['org_identity_id'] = $link['OrgIdentity']['id'];

              $options = array();
              $options['trustVerified'] = true;

              $this->CoJob->Co->CoPerson->CoOrgIdentityLink->OrgIdentity->EmailAddress->clear();
              $this->CoJob->Co->CoPerson->CoOrgIdentityLink->OrgIdentity->EmailAddress->save($data, $options);
            }

            break;
          }
        }
      }
      // TODO do we care about address and phone?

      $jobHistoryComment = "Synchronization completed successfully";
      $this->CoJob->CoJobHistoryRecord->record($this->CoJob->id, $jobHistoryRecordKey, $jobHistoryComment, $coPersonId, null, JobStatusEnum::Complete);
      $synchronized++;
    }

  $ret['synchronized'] = $synchronized;
  $ret['failed'] = $failed;
  $ret['status'] = JobStatusEnum::Complete;
  return $ret;
  }
}
