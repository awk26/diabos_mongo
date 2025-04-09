# from pymongo import MongoClient

# # Connect to MongoDB
# client = MongoClient("mongodb+srv://Diabos_read_only:Pass_123@production-cluster.1iv42.mongodb.net/database?retryWrites=true&w=majority")


# # Select the database
# db = client["diabos-production"]

# # Get collection names
# collections = db.list_collection_names()
# print(len(collections))


collections = [
    "aal-principal-ratings", "aal-query-ratings", "aal-ratings", "accesslevels", "accountBalances", "actions",
    "activitys", "activitytypes", "additionalnotifications", "addressbooks", "addresses", "addresss",
    "addresss_legalentity", "addresss_test", "addressses", "addresstypes", "adminsettings", "Agent_agree",
    "agent_decrypted_data", "agent_main", "agentadvices", "agentGrp", "agentIdmpping", "agentlist",
    "agentmapping_encrypted_data", "agentmapping59", "agentmappingslolts", "agentrecords", "agents",
    "agents_original_backup", "agents_test", "agentscsv", "agreement_test", "agreements", "agreementsids",
    "aisberthcalldatas", "aisportcalldatas", "aisvoyageforecasts", "all-ratings", "approvaltypes", "auditlogs",
    "Backup_before_url_update_dacostitems", "bank_docs_update", "banks", "banks_test", "banksstatus", "batchs",
    "beneficiaryrecords", "berthcalls", "berths", "blacklists", "bls", "branchs", "BTSCALLSCOMPARE", "cargos",
    "cargos_migration", "cargotypes", "cdcevents", "charterers", "charterers_encrypted_data", "citys", "clauses",
    "cmntemplates", "cntgmtoffset", "collection", "comments", "commoditys", "contacts", "contacttypes",
    "containermasters", "containers", "contracts", "contracttemplates", "costcenters", "costheads",
    "costheads_200523_chaitrali", "costheads_old", "costitem_t", "costitem_test", "costitem-portda",
    "costitemconfigurations", "costitems", "costitems_old", "costitems_old1", "costtemplates", "countrys",
    "countrys_org_07052023", "cpterms", "csirules", "csirules_old", "csirules_old_05112023", "currencys",
    "currencys_old", "currencys_old1", "currrates", "da_fields_update", "dachecklists", "dacostitem_test",
    "dacostitems", "dacostitems_migrate_test", "Dacostitems_VI", "dacostitemsinvoiceurlvi", "dadetails",
    "dadetails_addressId", "dafeildupdate", "dareceipts", "das", "datypes", "defaultTrustAccount",
    "deliveryorders", "demovideos", "departments", "deptsettings", "diabosusers", "dispatchs", "dmsdocs",
    "dmstasks", "docmasters", "documents", "dummy", "egms", "emailsessions", "emailtemplates", "employees",
    "enquiryitems", "enquirys", "events", "exceptionlogs", "exceptionlogs_original", "exceptions",
    "exportdatamappings", "FDACostSavings", "FdaOperatorTotalRemittance", "fdaremittance", "fdas",
    "FDATerminal", "features", "feedbacks", "fintechToken", "historicaldetails", "holidays", "IDACostSavings",
    "igms", "inappnotifications", "instructions", "invoiceitems", "invoices", "invoicesettings", "invoicetypes",
    "J25B1_dacostitems", "J25B1_dadetails", "J25B1_dareceipts", "J25B1_das", "J25B1_documents", "J25B1_events",
    "J25B1_exceptionlogs", "J25B1_payments", "J25B1_portcalls", "J25B1_queryfollowups", "J25B1_querys",
    "J25B1_sofs", "jobs", "kpisettings", "letterHead", "lettertypes", "locations", "locationtypes", "macnesgs",
    "manualfollowups", "marinetraffickeys", "menus", "MergeBeforeAgentPort_orgsettings", "MIGRATEDCALLS",
    "milestonemasters", "moduletypes", "new_org_with_id", "notificationmasters", "notifications",
    "notificationsettings", "odt-ratings", "ODTagreementsIds", "one-year-ratings", "org_2", "org_main",
    "orgsettings", "Original_before_remitance_dadetails", "paramstores", "partymasters", "paymentbackup",
    "paymentdocuments", "payments", "PDACostSavings", "pdatemplates", "pdatypes", "pendingverifications",
    "port_call_status", "PORT_MAPPING_AMENDMENTS", "port_test", "Portcall_test", "portcallapi",
    "portcallcargodetailscsv", "portcallIds", "portcallintegrationoldsystem", "portcallnews", "portcallNo",
    "portcalls", "portcalls_duplicate_data_index", "portcalls_duplicate_index_data", "portcalls_loa",
    "portcallsurl", "portcalltemplates", "portcalltype", "portdas", "portmapping59", "portnamecsv", "ports",
    "ports_test", "portservices", "porttariffrules", "products", "profile", "prospects", "qtycargo",
    "queryfollowups", "queryitems", "querys", "querysettings", "querystandardresponses", "quotes", "ratings",
    "ratings-aal", "ratings-aal-odt", "ratings-allorgids", "ratings-allorgids-querys", "ratings-created",
    "ratings-merged", "requestnewclauses", "roles", "roletypes", "sacostitems", "sanctionhistorys",
    "SDA_dacostitems", "SDA_dacostitems_test", "SDA_dadetails", "SDA_das", "SDACostSavings",
    "SDAOperatorTotalRemittance", "sdaremittance", "SDATerminals", "services", "servicetemplates",
    "servicetypes", "shipmanager_encrypted_data", "shipmanagers", "shippinglines", "slaparams", "slas",
    "sofs", "SOFSTOUPDATE", "states", "statuss", "stcostitems", "systemsettings", "systemtypes", "tariffparams",
    "tariffratemasters", "tariffrules", "tariffs", "taxtypes", "tenants", "terminalcsv", "terminals",
    "terminalsupdatedcsv", "terminaltypes", "terms", "test", "test_agent", "test_events", "test_org",
    "test_orgsetting", "test_ORGSETTINGS", "test_portcalls", "test_vendor", "test_vessel", "tests",
    "timezone_backup", "timezones", "todas", "todos", "transcripts", "trialda_180523", "trustAccounts",
    "trustAccounts_cms", "uoms", "uoms_before_rework", "user_orcl", "usernotificationsettings", "useroracleid",
    "users", "users_test", "usertypes", "utils", "vender_common_fields_empty", "vender_for_agreements",
    "vender_merged", "vendor_agree", "vendor_main", "vendornewreq", "vendorrecords", "vendors", "vessel_orcl",
    "vesselcategorys", "vessellist", "vesselpositions", "vesselrecords", "vessels", "vesselsubtypes",
    "vesseltypes", "VICostSavings", "viremarks", "vmsfails", "vmsportcallid", "voyages", "worldscales",
    "your_collection", "yourCollection", "yourCollectionName", "system.views"]

print(len(collections))
