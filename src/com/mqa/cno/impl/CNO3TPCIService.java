/*
 * Created by rao on 01-06-2021.
 *
 * Change History
 * Date         Author      Change Details
 *
 * 01-Jun-2021  Rao         Initial Version.
 */
package com.mqa.cno.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.ibm.mq.MQMessage;
import com.mongodb.client.*;
import com.mongodb.client.model.*;
import com.mqa.cno.entities.PrmList;
import com.mqa.daemonserver.mongo.impl.MongoCRUDResult;
import com.mqa.daemonserver.mongo.impl.MongoCRUDServer;
import com.mqa.mq.entities.MQQueueBinding;
import mqa.daemon.abstracts.BaseMQADaemon;
import org.bson.BsonNull;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import javax.print.Doc;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.mongodb.client.model.Filters.elemMatch;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

public class CNO3TPCIService extends MongoCRUDServer {
    // instance variables
    private MongoCollection<Document> _humanaBalancing;
    private MongoCollection<Document> _humanaAPEDMaster;
    private MongoCollection<Document> _humanaAPEDBalancing;
    private MongoCollection<Document> _humanaCommissionsMaster;
    private MongoCollection<Document> _humanaCommissionsBalancing;
    private MongoCollection<Document> _humanaDentalVisionMaster;
    private MongoCollection<Document> _humanaDentalVisionBalancing;
    private MongoCollection<Document> _humanaC145Balancing;
    private MongoCollection<Document> _humanaC145Detail;
    private MongoCollection<Document> _prmList;
    private MongoCollection<Document> _gridFs;

    private MongoCollection<Document> _aetnaBalancing;

    private MongoCollection<Document> _aetnaERMaster;
    private MongoCollection<Document> _aetnaCommissionsMaster;
    private MongoCollection<Document> _aetnaAPRMaster;


    Map<String, Object> gridFSproperties;
    Document apedDocument = new Document();
    List<Document> apedDocuments;
    FindIterable<Document> findAPED;

    DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");

    String canonicalDateFormat ="%Y%m%d";
    //String canonicalDateFormat ="%Y-%m-%d";
    private Document _baseDocument;

    public CNO3TPCIService(BaseMQADaemon parentDaemon, JsonNode listenerConfig, JsonNode commandConfig, MQQueueBinding binding) {
        super(parentDaemon, listenerConfig, commandConfig, binding);
    }

    ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();


    @Override
    protected void processCustomRequest(MQMessage message, MongoCRUDResult result) throws Exception {
        try {
            switch (super._messageOperation) {

                //Insert/Update Humana BalancingReport collection
                case "insertHumanaBalancingReport":
                    getHumanaBalancingCollection(message);
                    insertHumanaBalancingReport(_baseDocument, result);//insertMany
                    break;

                case "updateHumanaBalancingReport":
                    getHumanaBalancingCollection(message);
                    updateHumanaBalancingReport(_baseDocument, result);//update
                    break;

                case "getHumanaBalancingDocuments":
                    getHumanaBalancingCollection(message);
                    getHumanaBalancingDocuments(_baseDocument, result);//get
                    break;

                //Insert/Update HumanaAPED BalancingReport collection
                case "insertHumanaAPEDBalancingReport":
                    getHumanaAPEDBalancingCollection(message);
                    insertHumanaAPEDBalancingReport(_baseDocument, result);//insertMany
                    break;

                case "updateHumanaAPEDBalancingReport":
                    getHumanaAPEDBalancingCollection(message);
                    updateHumanaAPEDBalancingReport(_baseDocument, result);//update
                    break;

                case "getHumanaAPEDBalancingDocuments":
                    getHumanaAPEDBalancingCollection(message);
                    getHumanaAPEDBalancingDocuments(_baseDocument, result);//get
                    break;


                /*These methods are to update the APEDMaster collection*/
                case "updateHumanaAPEDMasterDocuments":
                    getHumanaAPEDMasterCollection(message);
                    updateHumanaAPEDMasterDocuments(_baseDocument, result);//update
                    break;

                case "getHumanaAPEDMasterLifeproDocuments":
                    getHumanaAPEDMasterCollection(message);
                getHumanaAPEDMasterLifeproDocuments(_baseDocument, result);//get
                break;


                 /*These methods are to update the APED C14.5 collection*/
                case "insertHumanaC145JSONDocuments":
                    getHumanaC145Collection(message);
                    insertHumanaC145JSONDocuments(_baseDocument, result);//insertMany
                    break;
                case "insertHumanaC145BalancingReport":
                    getHumanaC145BalancingCollection(message);
                    insertHumanaC145BalancingReport(_baseDocument, result);//insertMany
                    break;

                // Get all the Humana APED PreEdit configurations
                case "getHumanaAPEDPreEditsConfiguration":
                    getPrmList(message);
                    getHumanaAPEDPreEditsConfiguration(_baseDocument, result);//get
                    break;

                case "getLifeproBatchId":
                    getPrmList(message);
                    getLifeproBatchId(_baseDocument, result);//get
                    break;

                case "getHumanaAPEDStateCodes":
                    getPrmList(message);
                    getHumanaAPEDStateCodes(_baseDocument, result);//get
                    break;
                case "getHumanaAPEDCountyCodes":
                    getPrmList(message);
                    getHumanaAPEDCountyCodes(_baseDocument, result);//get
                    break;
                case "getHumanaAPEDProdDescCodes":
                    getPrmList(message);
                    getHumanaAPEDProdDescCodes(_baseDocument, result);//get
                    break;
                case "getHumanaAPEDPlanCodeLookup":
                    getPrmList(message);
                    getHumanaAPEDPlanCodeLookup(_baseDocument, result);//get
                    break;
                case "getHumanaAPEDNonCommPlansLookup":
                    getPrmList(message);
                    getHumanaAPEDNonCommPlansLookup(_baseDocument, result);//get
                    break;

                // The below two methods are to insert & update GridFS documents.
                case "insertGridFSDocuments":
                    getGridFsCollection(message);
                    gridFSproperties = super.getGridFSMessageProperties(message);
                    insertGridFSDocuments(_baseDocument, result, gridFSproperties);//insertGFS
                    break;
                case "updateGridFSDocuments":
                    getGridFsCollection(message);
                    updateGridFSDocuments(_baseDocument, result);//update
                    break;

                // The below two methods are to insert & update humanaCommissions collection documents.
                case "insertHumanaCommissionsBalancingReport":
                    getHumanaCommissionsBalancingCollection(message);
                    insertHumanaCommissionsBalancingReport(_baseDocument, result);//insertMany
                    break;

                case "updateHumanaCommissionsBalancingReport":
                    getHumanaCommissionsBalancingCollection(message);
                    updateHumanaCommissionsBalancingReport(_baseDocument, result);//updateMany
                    break;

                case "getHumanaCommissionsBalancingDocuments":
                    getHumanaCommissionsBalancingCollection(message);
                    getHumanaCommissionsBalancingDocuments(_baseDocument, result);//get
                    break;

                //Insert/Update/Get HumanaCommissions collection

                case "insertHumanaCommissionsMasterDocuments":
                    getHumanaCommissionsMasterCollection(message);
                    insertHumanaCommissionsMasterDocuments(_baseDocument, result);//insertMany
                    break;

                case "updateHumanaCommissionsMasterDocuments":
                    getHumanaCommissionsMasterCollection(message);
                    updateHumanaCommissionsMasterDocuments(_baseDocument, result);//update
                    break;

                case "getHumanaCommissionsPreEditsConfiguration":
                    getPrmList(message);
                    getHumanaCommissionsPreEditsConfiguration(_baseDocument, result);//get
                    break;

                case "getHumanaCommissionsMasterLifeproDocuments":
                    getHumanaCommissionsMasterCollection(message);
                    getHumanaCommissionsMasterLifeproDocuments(_baseDocument, result);//get
                    break;

                // The below two methods are to insert & update humanaDentalVision collection documents.
                case "insertHumanaDVBalancingReport":
                    getHumanaDVBalancingCollection(message);
                    insertHumanaDVBalancingReport(_baseDocument, result);//insertMany
                    break;

                case "updateHumanaDVBalancingReport":
                    getHumanaDVBalancingCollection(message);
                    updateHumanaDVBalancingReport(_baseDocument, result);//updateMany
                    break;

                case "getHumanaDVBalancingDocuments":
                    getHumanaDVBalancingCollection(message);
                    getHumanaDVBalancingDocuments(_baseDocument, result);//get
                    break;
                // The below methods are to insert & update Aetna Balancing collection documents.
                case "insertAetnaBalancingReport":
                    getAetnaBalancingCollection(message);
                    insertAetnaBalancingReport(_baseDocument, result);//insertMany
                    break;

                case "updateAetnaBalancingReport":
                    getAetnaBalancingCollection(message);
                    updateAetnaBalancingReport(_baseDocument, result);//updateMany
                    break;

                case "getAetnaBalancingDocuments":
                    getAetnaBalancingCollection(message);
                    getAetnaBalancingDocuments(_baseDocument, result);//get
                    break;

                //Insert/Update/Get Aetna Enrolment Roster collection

                case "insertAetnaERMasterDocuments":
                    getAetnaERMasterCollection(message);
                    insertAetnaERMasterDocuments(_baseDocument, result);//insertMany
                    break;

                case "updateAetnaERMasterDocuments":
                    getAetnaERMasterCollection(message);
                    updateAetnaERMasterDocuments(_baseDocument, result);//update
                    break;

                case "getAetnaPreEditsConfiguration":
                    getPrmList(message);
                    getAetnaPreEditsConfiguration(_baseDocument, result);//get
                    break;

                case "getAetnaERMasterLifeproDocuments":
                    getAetnaERMasterCollection(message);
                    getAetnaERMasterLifeproDocuments(_baseDocument, result);//get
                    break;

                //Insert/Update/Get Aetna Application Pipeline Rejects collection

                case "insertAetnaAPRMasterDocuments":
                    getAetnaAPRMasterCollection(message);
                    insertAetnaAPRMasterDocuments(_baseDocument, result);//insertMany
                    break;

                case "updateAetnaAPRMasterDocuments":
                    getAetnaAPRMasterCollection(message);
                    updateAetnaAPRMasterDocuments(_baseDocument, result);//update
                    break;

                case "getAetnaAPRMasterLifeproDocuments":
                    getAetnaAPRMasterCollection(message);
                    getAetnaAPRMasterLifeproDocuments(_baseDocument, result);//get
                    break;

                default:
                    break;

            }
        } catch (IllegalArgumentException exc) {
            if (exc.getMessage().startsWith("Illegal Operation"))
                throw exc;
            result.addError(_logger, exc);

        }
    }


    protected void insertHumanaBalancingReport(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a documents array to insert
        if (request == null || !request.containsKey("documents"))
            throw new IllegalArgumentException("No Document Array Specified");
        if (_humanaBalancing == null)
            throw new IllegalArgumentException("Humana APED Balancing Report collection instance is not initialized");
        try {
            _logger.info("insertHumanaBalancingReport - Beginning of method - " + dateFormat.format(new Date()));
            super.insert(request, result, _humanaBalancing);
            _logger.info("insertHumanaBalancingReport - End of method - " + dateFormat.format(new Date()));
        } catch (Exception e) {
            _logger.error("insertHumanaBalancingReport - insertHumanaBalancingReport method failed with exception  " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }


    protected void updateHumanaBalancingReport(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a filter
        if (request == null || !request.containsKey("documents"))
            throw new IllegalArgumentException("No Document Array Specified");
        if (_humanaBalancing == null)
            throw new IllegalArgumentException("Humana Balancing Report collection instance is not initialized");
        try {
            _logger.info("updateHumanaBalancingReport - Beginning of method - " + dateFormat.format(new Date()));

            List<Document> documents = request.getList("documents", Document.class);
            List<Document> balanceFilterDocs = new ArrayList<>();
            _logger.info("updateHumanaBalancingReport - Started updating Balancing Collection. " + dateFormat.format(new Date()));
            Document eftKeyDoc = new Document();
            for (Document doc : documents) {
                _logger.info("updateHumanaBalancingReport - Balancing document filter. " + doc.toJson());
            }
            super.update(request, result, _humanaBalancing, "updateMany");

        } catch (Exception e) {
            _logger.error("updateHumanaBalancingReport - updateHumanaBalancingReport method failed with exception  " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }

    protected void getHumanaBalancingDocuments(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a filter
        if (request == null || !request.containsKey("filter"))
            throw new IllegalArgumentException("No Filter Condition Specified");
        if (_humanaBalancing == null)
            throw new IllegalArgumentException("_humanaAPEDBalancing collection instance is not initialized");
        try {
            _logger.info("getHumanaBalancingDocuments - Beginning of method - " + dateFormat.format(new Date()));
            super.read(request, result, _humanaBalancing);
        } catch (Exception e) {
            _logger.error("getHumanaBalancingDocuments - getHumanaBalancingDocuments method failed with exception. " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }


    protected void insertHumanaAPEDBalancingReport(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a documents array to insert
        if (request == null || !request.containsKey("documents"))
            throw new IllegalArgumentException("No Document Array Specified");
        if (_humanaAPEDBalancing == null)
            throw new IllegalArgumentException("Humana APED Balancing Report collection instance is not initialized");
        try {
            _logger.info("insertHumanaAPEDBalancingReport - Beginning of method - " + dateFormat.format(new Date()));
            super.insert(request, result, _humanaAPEDBalancing);
            _logger.info("insertHumanaAPEDBalancingReport - End of method - " + dateFormat.format(new Date()));
        } catch (Exception e) {
            _logger.error("insertHumanaAPEDBalancingReport - insertHumanaAPEDBalancingReport method failed with exception  " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }


    protected void updateHumanaAPEDBalancingReport(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a filter
        if (request == null || !request.containsKey("documents"))
            throw new IllegalArgumentException("No Document Array Specified");
        if (_humanaAPEDBalancing == null)
            throw new IllegalArgumentException("Humana APED Balancing Report collection instance is not initialized");
        try {
            _logger.info("updateHumanaAPEDBalancingReport - Beginning of method - " + dateFormat.format(new Date()));

            List<Document> documents = request.getList("documents", Document.class);
            List<Document> balanceFilterDocs = new ArrayList<>();
            _logger.info("updateHumanaAPEDBalancingReport - Started updating Balancing Collection. " + dateFormat.format(new Date()));
            Document eftKeyDoc = new Document();
            for (Document doc : documents) {
                _logger.info("updateHumanaAPEDBalancingReport - Balancing document filter. " + doc.toJson());
            }
            super.update(request, result, _humanaAPEDBalancing, "updateMany");

        } catch (Exception e) {
            _logger.error("updateHumanaAPEDBalancingReport - updateHumanaAPEDBalancingReport method failed with exception  " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }

    protected void updateGridFSDocuments(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a filter
        if (request == null || !request.containsKey("documents"))
            throw new IllegalArgumentException("No Document Array Specified");
        if (_gridFs == null)
            throw new IllegalArgumentException("_gridFs collection instance is not initialized");
        try {
            _logger.info("updateGridFSDocuments - Beginning of method - " + dateFormat.format(new Date()));
            super.update(request, result, _gridFs, "updateMany");
        } catch (Exception e) {
            _logger.error("updateGridFSDocuments - updateGridFSDocuments method failed with exception  " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }

    protected void updateHumanaAPEDMasterDocuments(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a filter
        if (request == null || !request.containsKey("documents"))
            throw new IllegalArgumentException("No Document Array Specified");
        if (_humanaAPEDMaster == null)
            throw new IllegalArgumentException("Humana APED Master collection instance is not initialized");
        try {
            _logger.info("updateHumanaAPEDMasterDocuments - Beginning of method - " + dateFormat.format(new Date()));
            super.update(request, result, _humanaAPEDMaster, "updateMany");
        } catch (Exception e) {
            _logger.error("updateHumanaAPEDMasterDocuments - updateHumanaAPEDMasterDocuments method failed with exception  " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }

    protected void insertHumanaC145BalancingReport(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a documents array to insert
        if (request == null || !request.containsKey("documents"))
            throw new IllegalArgumentException("No Document Array Specified");
        if (_humanaC145Balancing == null)
            throw new IllegalArgumentException("Humana C145 Balancing Report collection instance is not initialized");
        try {
            _logger.info("insertHumanaC145BalancingReport - Beginning of method - " + dateFormat.format(new Date()));
            super.insert(request, result, _humanaC145Balancing);
            _logger.info("insertHumanaC145BalancingReport - End of method - " + dateFormat.format(new Date()));
        } catch (Exception e) {
            _logger.error("insertHumanaC145BalancingReport - insertHumanaC145BalancingReport method failed with exception  " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }

    protected void insertHumanaC145JSONDocuments(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a documents array to insert
        if (request == null || !request.containsKey("documents"))
            throw new IllegalArgumentException("No Document Array Specified");
        if (_humanaC145Detail == null)
            throw new IllegalArgumentException("Humana C145 collection instance is not initialized");
        try {
            _logger.info("insertHumanaC145JSONDocuments - Beginning of method - " + dateFormat.format(new Date()));
            super.insert(request, result, _humanaC145Detail);
            _logger.info("insertHumanaC145JSONDocuments - End of method - " + dateFormat.format(new Date()));
        } catch (Exception e) {
            _logger.error("insertHumanaC145JSONDocuments - insertHumanaC145JSONDocuments method failed with exception  " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }

    protected void getHumanaAPEDStateCodes(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a filter
        if (request == null || !request.containsKey("filter"))
            throw new IllegalArgumentException("No Filter Condition Specified");
        if (_prmList == null)
            throw new IllegalArgumentException("_prmList collection instance is not initialized");
        try {
            _logger.info("getHumanaAPEDStateCodes - Beginning of method - " + dateFormat.format(new Date()));
            super.read(request, result, _prmList);
        } catch (Exception e) {
            _logger.error("getHumanaAPEDStateCodes - getHumanaAPEDStateCodes method failed with exception. " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }
    protected void getHumanaAPEDCountyCodes(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a filter
        if (request == null || !request.containsKey("filter"))
            throw new IllegalArgumentException("No Filter Condition Specified");
        if (_prmList == null)
            throw new IllegalArgumentException("_prmList collection instance is not initialized");
        try {
            _logger.info("getHumanaAPEDCountyCodes - Beginning of method - " + dateFormat.format(new Date()));
            super.read(request, result, _prmList);
        } catch (Exception e) {
            _logger.error("getHumanaAPEDCountyCodes - getHumanaAPEDCountyCodes method failed with exception. " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }
    protected void getHumanaAPEDPreEditsConfiguration(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a filter
        if (request == null || !request.containsKey("filter"))
            throw new IllegalArgumentException("No Filter Condition Specified");
        if (_prmList == null)
            throw new IllegalArgumentException("_prmList collection instance is not initialized");
        try {
            _logger.info("getHumanaAPEDPreEditsConfiguration - Beginning of method - " + dateFormat.format(new Date()));
            super.read(request, result, _prmList);
        } catch (Exception e) {
            _logger.error("getHumanaAPEDPreEditsConfiguration - getHumanaAPEDPreEditsConfiguration method failed with exception. " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }

    protected void getHumanaCommissionsPreEditsConfiguration(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a filter
        if (request == null || !request.containsKey("filter"))
            throw new IllegalArgumentException("No Filter Condition Specified");
        if (_prmList == null)
            throw new IllegalArgumentException("_prmList collection instance is not initialized");
        try {
            _logger.info("getHumanaCommissionsPreEditsConfiguration - Beginning of method - " + dateFormat.format(new Date()));
            super.read(request, result, _prmList);
            _logger.debug("result - " + ow.writeValueAsString(result));
        } catch (Exception e) {
            _logger.error("getHumanaCommissionsPreEditsConfiguration - getHumanaCommissionsPreEditsConfiguration method failed with exception. " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }

    protected void getLifeproBatchId(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a filter
        if (request == null || !request.containsKey("filter"))
            throw new IllegalArgumentException("No Filter Condition Specified");
        if (_prmList == null)
            throw new IllegalArgumentException("_prmList collection instance is not initialized");
        try {
            _logger.info("getLifeproBatchId - Beginning of method - " + dateFormat.format(new Date()));
            super.read(request, result, _prmList);
            List<Document> resultDocs= result.getDocuments();

            Integer batchIdInt=0;

            for (Document doc:resultDocs) {
                if(doc.containsKey("info")){
                    List<Document> infoDocs=doc.getList("info", Document.class);
                    _logger.info("getLifeproBatchId - infoDocs - " + infoDocs.toString());
                    Document infoDoc=infoDocs.get(0);
                    _logger.info("getLifeproBatchId - infoDoc - " + infoDoc.toString());
                    String batchId="0";
                    if(infoDoc.get("key").toString().equals("BatchId"))
                        batchId=infoDoc.get("value").toString();
                    _logger.info("getLifeproBatchId - batchId - " + batchId);
                    batchIdInt=Integer.parseInt(batchId);
                    _logger.info("getLifeproBatchId - batchIdInt - " + batchIdInt.toString());
                    break;
                }
            }
            batchIdInt++;
            MongoCRUDResult updResult=new MongoCRUDResult();
            Document updateRequest = new Document();

            List<Document> updateDocuments= new ArrayList<>();
            Document updateFilter=new Document();
            updateFilter.append("type", "LIFEPRO_BATCHID")
                    .append("info.key", "BatchId");
            updateDocuments.add(new Document().append("filter", updateFilter)
                    .append("update",new Document("$set", new Document("info.$.value", batchIdInt.toString()))));

            updateRequest.put("documents",updateDocuments);
            _logger.info("getLifeproBatchId - updateRequest - " + updateRequest.toJson());
            super.update(updateRequest, updResult, _prmList, "updateMany");

        } catch (Exception e) {
            _logger.error("getLifeproBatchId - getLifeproBatchId method failed with exception. " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }

    protected void getHumanaAPEDProdDescCodes(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a filter
        if (request == null || !request.containsKey("filter"))
            throw new IllegalArgumentException("No Filter Condition Specified");
        if (_prmList == null)
            throw new IllegalArgumentException("_prmList collection instance is not initialized");
        try {
            _logger.info("getHumanaAPEDProdDescCodes - Beginning of method - " + dateFormat.format(new Date()));
            super.read(request, result, _prmList);
        } catch (Exception e) {
            _logger.error("getHumanaAPEDProdDescCodes - getHumanaAPEDProdDescCodes method failed with exception. " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }

    protected void getHumanaAPEDPlanCodeLookup(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a filter
        if (request == null || !request.containsKey("filter"))
            throw new IllegalArgumentException("No Filter Condition Specified");
        if (_prmList == null)
            throw new IllegalArgumentException("_prmList collection instance is not initialized");
        try {
            _logger.info("getHumanaAPEDPlanCodeLookup - Beginning of method - " + dateFormat.format(new Date()));
            super.read(request, result, _prmList);
        } catch (Exception e) {
            _logger.error("getHumanaAPEDPlanCodeLookup - getHumanaAPEDPlanCodeLookup method failed with exception. " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }

    protected void getHumanaAPEDBalancingDocuments(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a filter
        if (request == null || !request.containsKey("filter"))
            throw new IllegalArgumentException("No Filter Condition Specified");
        if (_humanaAPEDBalancing == null)
            throw new IllegalArgumentException("_humanaAPEDBalancing collection instance is not initialized");
        try {
            _logger.info("getHumanaAPEDBalancingDocuments - Beginning of method - " + dateFormat.format(new Date()));
            super.read(request, result, _humanaAPEDBalancing);
        } catch (Exception e) {
            _logger.error("getHumanaAPEDBalancingDocuments - getHumanaAPEDBalancingDocuments method failed with exception. " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }

    protected void getHumanaCommissionsBalancingDocuments(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a filter
        if (request == null || !request.containsKey("filter"))
            throw new IllegalArgumentException("No Filter Condition Specified");
        if (_humanaCommissionsBalancing == null)
            throw new IllegalArgumentException("_humanaCommissionsBalancing collection instance is not initialized");
        try {
            _logger.info("getHumanaCommissionsBalancingDocuments - Beginning of method - " + dateFormat.format(new Date()));
            _logger.debug("getHumanaCommissionsBalancingDocuments - filter - " + ow.writeValueAsString(request.get("filter")));
            super.read(request, result, _humanaCommissionsBalancing);
            _logger.debug("getHumanaCommissionsBalancingDocuments - result - " + ow.writeValueAsString(result));
        } catch (Exception e) {
            _logger.error("getHumanaCommissionsBalancingDocuments - getHumanaCommissionsBalancingDocuments method failed with exception. " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }

    protected void getHumanaAPEDMasterLifeproDocuments(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a filter
        if (request == null || !request.containsKey("filter"))
            throw new IllegalArgumentException("No Filter Condition Specified");
        if (_humanaAPEDMaster == null)
            throw new IllegalArgumentException("_humanaAPEDDetail collection instance is not initialized");
        try {
            Document apedFilter = request.get("filter", Document.class);
            _logger.info("getHumanaAPEDMasterLifeproDocuments - Beginning of method - " + dateFormat.format(new Date())+"\n Received Filter object. " + request.toJson());

            String processingKey = apedFilter.getString("processingKey");
            String productType = apedFilter.getString("productType");
            String processingStatus = apedFilter.getString("processingStatus");
            _logger.info(String.format("getHumanaAPEDMasterLifeproDocuments - processingKey - %s . productType - %s , processingStatus - %s.", processingKey, productType,processingStatus));

            Bson filter=new Document();
            filter=  Filters.and( Arrays.asList(Filters.eq("processingKey", processingKey),Filters.eq("productType", productType),Filters.eq("processingStatus", processingStatus),exists("effectiveDate", true)));
            
            AggregateIterable<Document> apedMasterDetails = _humanaAPEDMaster.aggregate(Arrays.asList(
                    Aggregates.match(filter
                            //and(eq("processingKey", processingKey), eq("cnoProductType", productType), eq("processingStatus", "6"))
                                    ),
                    Aggregates.project(fields(
                            include("umid",
                                    "processingKey",
                                    "processingStatus",
                                    "accretionStatus",
                                    "agentAlias",
                                    "agentCode",
                                    "agentSAN",
                                    "agentSSN",
                                    "medicareId",
                                    "memberFirstName",
                                    "memberLastName",
                                    "memberMidInitial",
                                    "memberPhoneNumber",
                                    "memberSSN",
                                    "duplicate",
                                    "validPreedits",
                                    "merged",
                                    "memberAddress",
                                    "policyStatus",
                                    "productDesc",
                                    "productName",
                                    "termReasonCode"),
                            computed("_id",  new Document().append("$toString","$_id")),
                            //computed("termReasonCode", "$itapedTermReasonCode"),
                            computed("planCode", "$planCode"),
                            computed("productType", "$productType"),
                            computed("vendorProductType", "$productType"),
                            computed("vendorPlanCode", "$vendorPlanCode"),
                            computed("memberDateOfBirth", eq("$dateToString", and(eq("date", "$memberBirthDate"), eq("format", canonicalDateFormat)))),
                            computed("fileDate", eq("$dateToString", and(eq("date", "$fileDate"), eq("format", canonicalDateFormat)))),
                            computed("effectiveDate", eq("$dateToString", and(eq("date", "$effectiveDate"), eq("format", canonicalDateFormat)))),
                            computed("originalEffectiveDate", eq("$dateToString", and(eq("date", "$originalEffectiveDate"), eq("format", canonicalDateFormat)))),
                            computed("termDate", eq("$dateToString", and(eq("date", "$termDate"), eq("format", canonicalDateFormat)))),
                            computed("policyStampDate", eq("$dateToString", and(eq("date", "$policyStampDate"), eq("format", canonicalDateFormat)))),
                            computed("agentEffectiveDate", eq("$dateToString", and(eq("date", "$agentEffectiveDate"), eq("format", canonicalDateFormat)))),
                            computed("agentTermDate", eq("$dateToString", and(eq("date", "$agentTermDate"), eq("format", canonicalDateFormat)))),
                            computed("lastUpdatedDate", eq("$dateToString", and(eq("date", "$lastUpdatedDate"), eq("format", canonicalDateFormat))))
                            )
                    )

                    )
            ).allowDiskUse(true);

            List<Document> responseDocs = new ArrayList<>();
            _logger.info("getHumanaAPEDMasterLifeproDocuments - Started adding result documents to List object responseDocs - " + dateFormat.format(new Date()));
            apedMasterDetails.iterator().forEachRemaining(responseDocs::add);
            _logger.info("getHumanaAPEDMasterLifeproDocuments - Completed adding result documents to List object responseDocs - " + dateFormat.format(new Date()));

            _logger.info("getHumanaAPEDMasterLifeproDocuments - Number of documents returned. " + responseDocs.size());
            result.setDocuments(responseDocs);
            _logger.info("getHumanaAPEDMasterLifeproDocuments - result Object after reading APEDMaster details." + result.toString());
            _logger.info("getHumanaAPEDMasterLifeproDocuments - End of method - " + dateFormat.format(new Date()));

        } catch (Exception e) {
            _logger.error("getHumanaAPEDMasterLifeproDocuments - getHumanaAPEDMasterLifeproDocuments method failed with exception. " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }
    protected void getHumanaAPEDNonCommPlansLookup(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a filter
        if (request == null || !request.containsKey("filter"))
            throw new IllegalArgumentException("No Filter Condition Specified");
        if (_prmList == null)
            throw new IllegalArgumentException("_prmList collection instance is not initialized");
        try {
            _logger.info("getHumanaAPEDNonCommPlansLookup - Beginning of method - " + dateFormat.format(new Date()));
            super.read(request, result, _prmList);
        } catch (Exception e) {
            _logger.error("getHumanaAPEDNonCommPlansLookup - getHumanaAPEDNonCommPlansLookup method failed with exception. " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }

    protected void getHumanaBalancingCollection(MQMessage message) throws Exception {
        _baseDocument = super._baseDocument;
        // routine to get the collection from the message
        checkRFHHeader(new String[]{"mqaCRUDTable"});
        String collectionNames = message.getStringProperty("usr.mqaCRUDTable");
        String[] collections = collectionNames.split("\\|");
        _humanaBalancing = getCollectionByName(collections[0]);

    }

    protected void getGridFsCollection(MQMessage message) throws Exception {
        _baseDocument = super._baseDocument;
        // routine to get the collection from the message
        checkRFHHeader(new String[]{"mqaCRUDTable"});
        String collectionNames = message.getStringProperty("usr.mqaCRUDTable");
        String[] collections = collectionNames.split("\\|");
        _gridFs = getCollectionByName(collections[0]);

        _logger.info("Found collection - "+collections[0]);

    }

    protected void getHumanaAPEDMasterCollection(MQMessage message) throws Exception {
        _baseDocument = super._baseDocument;
        // routine to get the collection from the message
        checkRFHHeader(new String[]{"mqaCRUDTable"});
        String collectionNames = message.getStringProperty("usr.mqaCRUDTable");
        String[] collections = collectionNames.split("\\|");
        _humanaAPEDMaster = getCollectionByName(collections[0]);
    }

    protected void getHumanaAPEDBalancingCollection(MQMessage message) throws Exception {
        _baseDocument = super._baseDocument;
        // routine to get the collection from the message
        checkRFHHeader(new String[]{"mqaCRUDTable"});
        String collectionNames = message.getStringProperty("usr.mqaCRUDTable");
        String[] collections = collectionNames.split("\\|");
        _humanaAPEDBalancing = getCollectionByName(collections[0]);

    }
    protected void getHumanaCommissionsMasterCollection(MQMessage message) throws Exception {
        _baseDocument = super._baseDocument;
        // routine to get the collection from the message
        checkRFHHeader(new String[]{"mqaCRUDTable"});
        String collectionNames = message.getStringProperty("usr.mqaCRUDTable");
        String[] collections = collectionNames.split("\\|");
        _humanaCommissionsMaster = getCollectionByName(collections[0]);
    }
    protected void getHumanaCommissionsBalancingCollection(MQMessage message) throws Exception {
        _baseDocument = super._baseDocument;
        // routine to get the collection from the message
        checkRFHHeader(new String[]{"mqaCRUDTable"});
        String collectionNames = message.getStringProperty("usr.mqaCRUDTable");
        String[] collections = collectionNames.split("\\|");
        _humanaCommissionsBalancing = getCollectionByName(collections[0]);

    }

    protected void getHumanaDVBalancingCollection(MQMessage message) throws Exception {
        _baseDocument = super._baseDocument;
        // routine to get the collection from the message
        checkRFHHeader(new String[]{"mqaCRUDTable"});
        String collectionNames = message.getStringProperty("usr.mqaCRUDTable");
        String[] collections = collectionNames.split("\\|");
        _humanaDentalVisionBalancing = getCollectionByName(collections[0]);
    }
    protected void getHumanaDVMasterCollection(MQMessage message) throws Exception {
        _baseDocument = super._baseDocument;
        // routine to get the collection from the message
        checkRFHHeader(new String[]{"mqaCRUDTable"});
        String collectionNames = message.getStringProperty("usr.mqaCRUDTable");
        String[] collections = collectionNames.split("\\|");
        _humanaDentalVisionMaster = getCollectionByName(collections[0]);

    }

    protected void getHumanaC145Collection(MQMessage message) throws Exception {
        _baseDocument = super._baseDocument;
        // routine to get the collection from the message
        checkRFHHeader(new String[]{"mqaCRUDTable"});
        String collectionNames = message.getStringProperty("usr.mqaCRUDTable");
        String[] collections = collectionNames.split("\\|");
        _humanaC145Detail = getCollectionByName(collections[0]);
    }
    protected void getHumanaC145BalancingCollection(MQMessage message) throws Exception {
        _baseDocument = super._baseDocument;
        // routine to get the collection from the message
        checkRFHHeader(new String[]{"mqaCRUDTable"});
        String collectionNames = message.getStringProperty("usr.mqaCRUDTable");
        String[] collections = collectionNames.split("\\|");
        _humanaC145Balancing = getCollectionByName(collections[0]);
    }
    protected void getPrmList(MQMessage message) throws Exception {
        _baseDocument = super._baseDocument;
        // routine to get the collection from the message
        checkRFHHeader(new String[]{"mqaCRUDTable"});
        String collectionNames = message.getStringProperty("usr.mqaCRUDTable");
        String[] collections = collectionNames.split("\\|");
        _prmList = getCollectionByName(collections[0]);
    }


    protected MongoCollection getCollectionByName(String collectionName) throws Exception {
        return super._mongoDB.getCollection(collectionName);
    }

    protected void checkRFHHeader(String[] mandatoryElements) throws Exception {
        // routine to check the RFH2 Header for specific fields otherwise throw exceptions
        for (String key : mandatoryElements)
            if (!super._messageProps.contains(key))
                throw new IllegalArgumentException(String.format("%s not found in RFH Header", key));

    }


    protected void insertGridFSDocuments(Document request, MongoCRUDResult result, Map<String, Object> gridFSproperties) throws Exception {
        // check that we have a documents array to insert
        if (request == null || !request.containsKey("documents"))
            throw new IllegalArgumentException("No Document Array Specified");
        try {
            _logger.info("insertGridFSDocuments - Beginning of method - " + dateFormat.format(new Date()));
            _logger.info("insertGridFSDocuments - Document count - " + request.getList("documents", Document.class).size() + " . Size - " + request.toString().length());
            super.insertGridFS(request, result, gridFSproperties);
        } catch (Exception e) {
            _logger.error("insertGridFSDocuments - insertGridFSDocuments method failed with exception  " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }


    protected void insertHumanaCommissionsBalancingReport(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a documents array to insert
        if (request == null || !request.containsKey("documents"))
            throw new IllegalArgumentException("No Document Array Specified");
        if (_humanaCommissionsBalancing == null)
            throw new IllegalArgumentException("Humana Commissions Balancing Report collection instance is not initialized");
        try {
            _logger.info("insertHumanaCommissionsBalancingReport - Beginning of method - " + dateFormat.format(new Date()));
            super.insert(request, result, _humanaCommissionsBalancing);
            _logger.info("insertHumanaCommissionsBalancingReport - End of method - " + dateFormat.format(new Date()));
        } catch (Exception e) {
            _logger.error("insertHumanaCommissionsBalancingReport - insertHumanaCommissionsBalancingReport method failed with exception  " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }

    protected void updateHumanaCommissionsBalancingReport(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a filter
        if (request == null || !request.containsKey("documents"))
            throw new IllegalArgumentException("No Document Array Specified");
        if (_humanaCommissionsBalancing == null)
            throw new IllegalArgumentException("Humana Commissions Balancing Report collection instance is not initialized");
        try {
            _logger.info("updateHumanaCommissionsBalancingReport - Beginning of method - " + dateFormat.format(new Date()));

            List<Document> documents = request.getList("documents", Document.class);
            List<Document> balanceFilterDocs = new ArrayList<>();
            _logger.info("updateHumanaCommissionsBalancingReport - Started updating Balancing Collection. " + dateFormat.format(new Date()));
            Document eftKeyDoc = new Document();
            for (Document doc : documents) {
                _logger.info("updateHumanaCommissionsBalancingReport - Balancing document filter. " + doc.toJson());
            }
            super.update(request, result, _humanaCommissionsBalancing, "updateMany");

        } catch (Exception e) {
            _logger.error("updateHumanaCommissionsBalancingReport - updateHumanaCommissionsBalancingReport method failed with exception  " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }


    protected void insertHumanaCommissionsMasterDocuments(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a documents array to insert
        if (request == null || !request.containsKey("documents"))
            throw new IllegalArgumentException("No Document Array Specified");
        if (_humanaCommissionsMaster == null)
            throw new IllegalArgumentException("Humana Commissions Master collection instance is not initialized");
        try {
            _logger.info("insertHumanaCommissionsMasterDocuments - Beginning of method - " + dateFormat.format(new Date()));
            super.insert(request, result, _humanaCommissionsMaster);
            _logger.info("insertHumanaCommissionsMasterDocuments - End of method - " + dateFormat.format(new Date()));
        } catch (Exception e) {
            _logger.error("insertHumanaCommissionsMasterDocuments - insertHumanaCommissionsMasterDocuments method failed with exception  " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }

    protected void updateHumanaCommissionsMasterDocuments(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a filter
        if (request == null || !request.containsKey("documents"))
            throw new IllegalArgumentException("No Document Array Specified");
        if (_humanaCommissionsMaster == null)
            throw new IllegalArgumentException("Humana Commissions Master collection instance is not initialized");
        try {
            _logger.info("updateHumanaCommissionsMasterDocuments - Beginning of method - " + dateFormat.format(new Date()));
            super.update(request, result, _humanaCommissionsMaster, "updateMany");
        } catch (Exception e) {
            _logger.error("updateHumanaCommissionsMasterDocuments - updateHumanaCommissionsMasterDocuments method failed with exception  " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }


    protected void getHumanaCommissionsMasterLifeproDocuments(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a filter
        if (request == null || !request.containsKey("filter"))
            throw new IllegalArgumentException("No Filter Condition Specified");
        if (_humanaCommissionsMaster == null)
            throw new IllegalArgumentException("_humanaCommissionsMaster collection instance is not initialized");
        try {
            Document docfilter = request.get("filter", Document.class);
            _logger.info("getHumanaCommissionsMasterLifeproDocuments - Beginning of method - " + dateFormat.format(new Date())+"\n Received Filter object. " + request.toJson());

            String processingKey = docfilter.getString("processingKey");
            String processingStatus = docfilter.getString("processingStatus");
            _logger.info(String.format("getHumanaCommissionsMasterLifeproDocuments - processingKey - %s . processingStatus - %s.", processingKey, processingStatus));

            Bson filter=new Document();
            filter=  Filters.and( Arrays.asList(Filters.eq("processingKey", processingKey),Filters.eq("processingStatus", processingStatus)));

            AggregateIterable<Document> masterDetails = _humanaCommissionsMaster.aggregate(Arrays.asList(
                    Aggregates.match(filter
                            //and(eq("processingKey", processingKey), eq("cnoProductType", productType), eq("processingStatus", "6"))
                    ),
                    Aggregates.project(fields(
                            include("vendorUMId",
                                    "processingKey",
                                    "processingStatus",
                                    "recordIndex",
                                    "payeeSAN",
                                    "payeeSSN",
                                    "agentSAN",
                                    "writingAgentSAN",
                                    "agentSSN",
                                    "writingAgentSSN",
                                    "writingAgentName",
                                    "platformCode",
                                    "businessCode",
                                    "groupNumber",
                                    "groupName",
                                    "productLineCode",
                                    "solarProductCode",
                                    "livesCount",
                                    "agentSplitPercent",
                                    "commissionsRate",
                                    "premiumAmount",
                                    "commissionAmount",
                                    "transactionTypeCode",
                                    "renewalIndicator",
                                    "personalId",
                                    "medicareId",
                                    "memberState",
                                    "productCode",
                                    "exchangeIndicator",
                                    "stateExchangeFedIndicator",
                                    "coverageType",
                                    "referringBrokerSAN",
                                    "referringBrokerSSN",
                                    "sfAliasId",
                                    "sfId",
                                    "agentId",
                                    "refBrokerAgentId",
                                    "applicationId",
                                    "prorateIndicator",
                                    "shortTermDisEnrollmentInd",
                                    "duplicate",
                                    "invalidDataType",
                                    "validPreedits"),
                            computed("_id",  new Document().append("$toString","$_id")),
                            computed("policyEffectiveDate", eq("$dateToString", and(eq("date", "$policyEffectiveDate"), eq("format", canonicalDateFormat)))),
                            computed("fileDate", eq("$dateToString", and(eq("date", "$fileDate"), eq("format", canonicalDateFormat)))),
                            computed("statementRunDate", eq("$dateToString", and(eq("date", "$statementRunDate"), eq("format", canonicalDateFormat)))),
                            computed("originalEffectiveDate", eq("$dateToString", and(eq("date", "$originalEffectiveDate"), eq("format", canonicalDateFormat)))),
                            computed("lastUpdatedDate", eq("$dateToString", and(eq("date", "$lastUpdatedDate"), eq("format", canonicalDateFormat))))
                            )
                    )

                    )
            ).allowDiskUse(true);

            List<Document> responseDocs = new ArrayList<>();
            _logger.info("getHumanaCommissionsMasterLifeproDocuments - Started adding result documents to List object responseDocs - " + dateFormat.format(new Date()));
            masterDetails.iterator().forEachRemaining(responseDocs::add);
            _logger.info("getHumanaCommissionsMasterLifeproDocuments - Completed adding result documents to List object responseDocs - " + dateFormat.format(new Date()));

            _logger.info("getHumanaCommissionsMasterLifeproDocuments - Number of documents returned. " + responseDocs.size());
            result.setDocuments(responseDocs);
            _logger.info("getHumanaCommissionsMasterLifeproDocuments - result Object after reading CommissionsMaster details." + result.toString());
            _logger.info("getHumanaCommissionsMasterLifeproDocuments - End of method - " + dateFormat.format(new Date()));

        } catch (Exception e) {
            _logger.error("getHumanaCommissionsMasterLifeproDocuments - getHumanaCommissionsMasterLifeproDocuments method failed with exception. " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }

    protected void getHumanaDVPreEditsConfiguration(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a filter
        if (request == null || !request.containsKey("filter"))
            throw new IllegalArgumentException("No Filter Condition Specified");
        if (_prmList == null)
            throw new IllegalArgumentException("_prmList collection instance is not initialized");
        try {
            _logger.info("getHumanaDVPreEditsConfiguration - Beginning of method - " + dateFormat.format(new Date()));
            super.read(request, result, _prmList);
        } catch (Exception e) {
            _logger.error("getHumanaDVPreEditsConfiguration - getHumanaDVPreEditsConfiguration method failed with exception. " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }

    protected void insertHumanaDVBalancingReport(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a documents array to insert
        if (request == null || !request.containsKey("documents"))
            throw new IllegalArgumentException("No Document Array Specified");
        if (_humanaDentalVisionBalancing == null)
            throw new IllegalArgumentException("Humana DentalVision Balancing Report collection instance is not initialized");
        try {
            _logger.info("insertHumanaDVBalancingReport - Beginning of method - " + dateFormat.format(new Date()));
            super.insert(request, result, _humanaDentalVisionBalancing);
            _logger.info("insertHumanaDVBalancingReport - End of method - " + dateFormat.format(new Date()));
        } catch (Exception e) {
            _logger.error("insertHumanaDVBalancingReport - insertHumanaDVBalancingReport method failed with exception  " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }

    protected void updateHumanaDVBalancingReport(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a filter
        if (request == null || !request.containsKey("documents"))
            throw new IllegalArgumentException("No Document Array Specified");
        if (_humanaDentalVisionBalancing == null)
            throw new IllegalArgumentException("Humana DentalVision Balancing Report collection instance is not initialized");
        try {
            _logger.info("updateHumanaDVBalancingReport - Beginning of method - " + dateFormat.format(new Date()));

            List<Document> documents = request.getList("documents", Document.class);
            List<Document> balanceFilterDocs = new ArrayList<>();
            _logger.info("updateHumanaDVBalancingReport - Started updating Balancing Collection. " + dateFormat.format(new Date()));
            Document eftKeyDoc = new Document();
            for (Document doc : documents) {
                _logger.info("updateHumanaDVBalancingReport - Balancing document filter. " + doc.toJson());
            }
            super.update(request, result, _humanaDentalVisionBalancing, "updateMany");

        } catch (Exception e) {
            _logger.error("updateHumanaDVBalancingReport - updateHumanaDVBalancingReport method failed with exception  " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }

    protected void getHumanaDVBalancingDocuments(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a filter
        if (request == null || !request.containsKey("filter"))
            throw new IllegalArgumentException("No Filter Condition Specified");
        if (_humanaDentalVisionBalancing == null)
            throw new IllegalArgumentException("_humanaCommissionsBalancing collection instance is not initialized");
        try {
            _logger.info("getHumanaDVBalancingDocuments - Beginning of method - " + dateFormat.format(new Date()));
            _logger.debug("getHumanaDVBalancingDocuments - filter - " + ow.writeValueAsString(request.get("filter")));
            super.read(request, result, _humanaDentalVisionBalancing);
            _logger.debug("getHumanaDVBalancingDocuments - result - " + ow.writeValueAsString(result));
        } catch (Exception e) {
            _logger.error("getHumanaDVBalancingDocuments - getHumanaDVBalancingDocuments method failed with exception. " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }
    protected void insertHumanaDVMasterDocuments(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a documents array to insert
        if (request == null || !request.containsKey("documents"))
            throw new IllegalArgumentException("No Document Array Specified");
        if (_humanaDentalVisionMaster == null)
            throw new IllegalArgumentException("Humana DentalVision Master collection instance is not initialized");
        try {
            _logger.info("insertHumanaDVMasterDocuments - Beginning of method - " + dateFormat.format(new Date()));
            super.insert(request, result, _humanaDentalVisionMaster);
            _logger.info("insertHumanaDVMasterDocuments - End of method - " + dateFormat.format(new Date()));
        } catch (Exception e) {
            _logger.error("insertHumanaDVMasterDocuments - insertHumanaDVMasterDocuments method failed with exception  " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }

    protected void updateHumanaDVMasterDocuments(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a filter
        if (request == null || !request.containsKey("documents"))
            throw new IllegalArgumentException("No Document Array Specified");
        if (_humanaDentalVisionMaster == null)
            throw new IllegalArgumentException("Humana DentalVision Master collection instance is not initialized");
        try {
            _logger.info("updateHumanaDVMasterDocuments - Beginning of method - " + dateFormat.format(new Date()));
            super.update(request, result, _humanaDentalVisionMaster, "updateMany");
        } catch (Exception e) {
            _logger.error("updateHumanaDVMasterDocuments - updateHumanaDVMasterDocuments method failed with exception  " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }


    protected void getHumanaDVMasterLifeproDocuments(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a filter
        if (request == null || !request.containsKey("filter"))
            throw new IllegalArgumentException("No Filter Condition Specified");
        if (_humanaDentalVisionMaster == null)
            throw new IllegalArgumentException("_humanaDentalVisionMaster collection instance is not initialized");
        try {
            Document docfilter = request.get("filter", Document.class);
            _logger.info("getHumanaDVMasterLifeproDocuments - Beginning of method - " + dateFormat.format(new Date())+"\n Received Filter object. " + request.toJson());

            String processingKey = docfilter.getString("processingKey");
            String processingStatus = docfilter.getString("processingStatus");
            _logger.info(String.format("getHumanaDVMasterLifeproDocuments - processingKey - %s . processingStatus - %s.", processingKey, processingStatus));

            Bson filter=new Document();
            filter=  Filters.and( Arrays.asList(Filters.eq("processingKey", processingKey),Filters.eq("processingStatus", processingStatus)));

            AggregateIterable<Document> masterDetails = _humanaDentalVisionMaster.aggregate(Arrays.asList(
                    Aggregates.match(filter
                            //and(eq("processingKey", processingKey), eq("cnoProductType", productType), eq("processingStatus", "6"))
                    ),
                    Aggregates.project(fields(
                            include("processingKey",
                                    "processingStatus",
                                    "recordIndex",
                                    "businessCode",
                                    "groupNumber",
                                    "groupNumberSequence",
                                    "lastName",
                                    "firstName",
                                    "productCode",
                                    "productName",
                                    "productLineCode",
                                    "address",
                                    "memberCity",
                                    "memberState",
                                    "memberZipCode",
                                    "subscriberCount",
                                    "memberCount",
                                    "dependentCount",
                                    "agentNumber",
                                    "agentName",
                                    "agentState",
                                    "writingAgentNumber",
                                    "writingAgentName",
                                    "writingAgentState",
                                    "alternateId",
                                    "planCode",
                                    "duplicate",
                                    "invalidDataType",
                                    "validPreedits"),
                            computed("_id",  new Document().append("$toString","$_id")),
                            computed("effectiveDate", eq("$dateToString", and(eq("date", "$effectiveDate"), eq("format", canonicalDateFormat)))),
                            computed("fileDate", eq("$dateToString", and(eq("date", "$fileDate"), eq("format", canonicalDateFormat)))),
                            computed("issueDate", eq("$dateToString", and(eq("date", "$issueDate"), eq("format", canonicalDateFormat)))),
                            computed("termDate", eq("$dateToString", and(eq("date", "$termDate"), eq("format", canonicalDateFormat)))),
                            computed("lastUpdatedDate", eq("$dateToString", and(eq("date", "$lastUpdatedDate"), eq("format", canonicalDateFormat))))
                            )
                    )

                    )
            ).allowDiskUse(true);

            List<Document> responseDocs = new ArrayList<>();
            _logger.info("getHumanaDVMasterLifeproDocuments - Started adding result documents to List object responseDocs - " + dateFormat.format(new Date()));
            masterDetails.iterator().forEachRemaining(responseDocs::add);
            _logger.info("getHumanaDVMasterLifeproDocuments - Completed adding result documents to List object responseDocs - " + dateFormat.format(new Date()));

            _logger.info("getHumanaDVMasterLifeproDocuments - Number of documents returned. " + responseDocs.size());
            result.setDocuments(responseDocs);
            _logger.info("getHumanaDVMasterLifeproDocuments - result Object after reading CommissionsMaster details." + result.toString());
            _logger.info("getHumanaDVMasterLifeproDocuments - End of method - " + dateFormat.format(new Date()));

        } catch (Exception e) {
            _logger.error("getHumanaDVMasterLifeproDocuments - getHumanaDVMasterLifeproDocuments method failed with exception. " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }

    protected void getAetnaBalancingCollection(MQMessage message) throws Exception {
        _baseDocument = super._baseDocument;
        // routine to get the collection from the message
        checkRFHHeader(new String[]{"mqaCRUDTable"});
        String collectionNames = message.getStringProperty("usr.mqaCRUDTable");
        String[] collections = collectionNames.split("\\|");
        _aetnaBalancing = getCollectionByName(collections[0]);

    }
    protected void getAetnaERMasterCollection(MQMessage message) throws Exception {
        _baseDocument = super._baseDocument;
        // routine to get the collection from the message
        checkRFHHeader(new String[]{"mqaCRUDTable"});
        String collectionNames = message.getStringProperty("usr.mqaCRUDTable");
        String[] collections = collectionNames.split("\\|");
        _aetnaERMaster = getCollectionByName(collections[0]);
    }

     protected void getAetnaCommissionsMasterCollection(MQMessage message) throws Exception {
        _baseDocument = super._baseDocument;
        // routine to get the collection from the message
        checkRFHHeader(new String[]{"mqaCRUDTable"});
        String collectionNames = message.getStringProperty("usr.mqaCRUDTable");
        String[] collections = collectionNames.split("\\|");
        _aetnaCommissionsMaster = getCollectionByName(collections[0]);
    }

    protected void getAetnaAPRMasterCollection(MQMessage message) throws Exception {
        _baseDocument = super._baseDocument;
        // routine to get the collection from the message
        checkRFHHeader(new String[]{"mqaCRUDTable"});
        String collectionNames = message.getStringProperty("usr.mqaCRUDTable");
        String[] collections = collectionNames.split("\\|");
        _aetnaAPRMaster = getCollectionByName(collections[0]);

    }


    protected void insertAetnaBalancingReport(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a documents array to insert
        if (request == null || !request.containsKey("documents"))
            throw new IllegalArgumentException("No Document Array Specified");
        if (_aetnaBalancing == null)
            throw new IllegalArgumentException("Aetna Balancing Report collection instance is not initialized");
        try {
            _logger.info("insertAetnaBalancingReport - Beginning of method - " + dateFormat.format(new Date()));
            super.insert(request, result, _aetnaBalancing);
            _logger.info("insertAetnaBalancingReport - End of method - " + dateFormat.format(new Date()));
        } catch (Exception e) {
            _logger.error("insertAetnaBalancingReport - insertAetnaBalancingReport method failed with exception  " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }


    protected void updateAetnaBalancingReport(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a filter
        if (request == null || !request.containsKey("documents"))
            throw new IllegalArgumentException("No Document Array Specified");
        if (_aetnaBalancing == null)
            throw new IllegalArgumentException("Aetna Enrolment Roster Balancing Report collection instance is not initialized");
        try {
            _logger.info("updateAetnaBalancingReport - Beginning of method - " + dateFormat.format(new Date()));

            List<Document> documents = request.getList("documents", Document.class);
            List<Document> balanceFilterDocs = new ArrayList<>();
            _logger.info("updateAetnaBalancingReport - Started updating Balancing Collection. " + dateFormat.format(new Date()));
            Document eftKeyDoc = new Document();
            for (Document doc : documents) {
                _logger.info("updateAetnaBalancingReport - Balancing document filter. " + doc.toJson());
            }
            super.update(request, result, _aetnaBalancing, "updateMany");

        } catch (Exception e) {
            _logger.error("updateAetnaBalancingReport - updateAetnaBalancingReport method failed with exception  " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }

    protected void getAetnaBalancingDocuments(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a filter
        if (request == null || !request.containsKey("filter"))
            throw new IllegalArgumentException("No Filter Condition Specified");
        if (_aetnaBalancing == null)
            throw new IllegalArgumentException("_aetnaERBalancing collection instance is not initialized");
        try {
            _logger.info("getAetnaBalancingDocuments - Beginning of method - " + dateFormat.format(new Date()));
            super.read(request, result, _aetnaBalancing);
        } catch (Exception e) {
            _logger.error("getAetnaBalancingDocuments - getAetnaBalancingDocuments method failed with exception. " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }

    protected void getAetnaPreEditsConfiguration(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a filter
        if (request == null || !request.containsKey("filter"))
            throw new IllegalArgumentException("No Filter Condition Specified");
        if (_prmList == null)
            throw new IllegalArgumentException("_prmList collection instance is not initialized");
        try {
            _logger.info("getAetnaPreEditsConfiguration - Beginning of method - " + dateFormat.format(new Date()));
            super.read(request, result, _prmList);
        } catch (Exception e) {
            _logger.error("getAetnaPreEditsConfiguration - getAetnaPreEditsConfiguration method failed with exception. " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }


    protected void insertAetnaERMasterDocuments(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a documents array to insert
        if (request == null || !request.containsKey("documents"))
            throw new IllegalArgumentException("No Document Array Specified");
        if (_aetnaERMaster == null)
            throw new IllegalArgumentException("Aetna Enrolment Roster Master collection instance is not initialized");
        try {
            _logger.info("insertAetnaERMasterDocuments - Beginning of method - " + dateFormat.format(new Date()));
            super.insert(request, result, _aetnaERMaster);
            _logger.info("insertAetnaERMasterDocuments - End of method - " + dateFormat.format(new Date()));
        } catch (Exception e) {
            _logger.error("insertAetnaERMasterDocuments - insertAetnaERMasterDocuments method failed with exception  " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }

    protected void updateAetnaERMasterDocuments(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a filter
        if (request == null || !request.containsKey("documents"))
            throw new IllegalArgumentException("No Document Array Specified");
        if (_aetnaERMaster == null)
            throw new IllegalArgumentException("Aetna Enrolment Roster Master collection instance is not initialized");
        try {
            _logger.info("updateAetnaERMasterDocuments - Beginning of method - " + dateFormat.format(new Date()));
            super.update(request, result, _aetnaERMaster, "updateMany");
        } catch (Exception e) {
            _logger.error("updateAetnaERMasterDocuments - updateAetnaERMasterDocuments method failed with exception  " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }


    protected void getAetnaERMasterLifeproDocuments(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a filter
        if (request == null || !request.containsKey("filter"))
            throw new IllegalArgumentException("No Filter Condition Specified");
        if (_aetnaERMaster == null)
            throw new IllegalArgumentException("_aetnaERMaster collection instance is not initialized");
        try {
            Document docfilter = request.get("filter", Document.class);
            _logger.info("getAetnaERMasterLifeproDocuments - Beginning of method - " + dateFormat.format(new Date())+"\n Received Filter object. " + request.toJson());

            String processingKey = docfilter.getString("processingKey");
            String processingStatus = docfilter.getString("processingStatus");
            _logger.info(String.format("getAetnaERMasterLifeproDocuments - processingKey - %s . processingStatus - %s.", processingKey, processingStatus));

            Bson filter=new Document();
            filter=  Filters.and( Arrays.asList(Filters.eq("processingKey", processingKey),Filters.eq("processingStatus", processingStatus)));

            AggregateIterable<Document> masterDetails = _aetnaERMaster.aggregate(Arrays.asList(
                    Aggregates.match(filter),
                    Aggregates.project(fields(
                            include("memberId",
                                    "processingKey",
                                    "processingStatus",
                                    "recordIndex",
                                    "medicareId",
                                    "memberSSN",
                                    "affinityPolicyId",
                                    "firstName",
                                    "lastName",
                                    "addressLine1",
                                    "addressLine2",
                                    "memberCity",
                                    "memberState",
                                    "memberZipCode",
                                    "memberStatus",
                                    "planName",
                                    "productCode",
                                    "pbpCode",
                                    "gaTIN",
                                    "brokerNPN",
                                    "brokerName",
                                    "planCode",
                                    "duplicate",
                                    "invalidDataType",
                                    "validPreedits"),
                            computed("_id",  new Document().append("$toString","$_id")),
                            computed("applicationSignatureDate", eq("$dateToString", and(eq("date", "$applicationSignatureDate"), eq("format", canonicalDateFormat)))),
                            computed("applicationReceiveDate", eq("$dateToString", and(eq("date", "$applicationReceiveDate"), eq("format", canonicalDateFormat)))),
                            computed("effectiveDate", eq("$dateToString", and(eq("date", "$effectiveDate"), eq("format", canonicalDateFormat)))),
                            computed("fileDate", eq("$dateToString", and(eq("date", "$fileDate"), eq("format", canonicalDateFormat)))),
                            computed("termDate", eq("$dateToString", and(eq("date", "$termDate"), eq("format", canonicalDateFormat)))),
                            computed("idCardIssueDate", eq("$dateToString", and(eq("date", "$idCardIssueDate"), eq("format", canonicalDateFormat)))),
                            computed("lastUpdatedDate", eq("$dateToString", and(eq("date", "$lastUpdatedDate"), eq("format", canonicalDateFormat))))
                            )
                    )

                    )
            ).allowDiskUse(true);

            List<Document> responseDocs = new ArrayList<>();
            _logger.info("getAetnaERMasterLifeproDocuments - Started adding result documents to List object responseDocs - " + dateFormat.format(new Date()));
            masterDetails.iterator().forEachRemaining(responseDocs::add);
            _logger.info("getAetnaERMasterLifeproDocuments - Completed adding result documents to List object responseDocs - " + dateFormat.format(new Date()));

            _logger.info("getAetnaERMasterLifeproDocuments - Number of documents returned. " + responseDocs.size());
            result.setDocuments(responseDocs);
            _logger.info("getAetnaERMasterLifeproDocuments - result Object after reading EnrolmentRoster details." + result.toString());
            _logger.info("getAetnaERMasterLifeproDocuments - End of method - " + dateFormat.format(new Date()));

        } catch (Exception e) {
            _logger.error("getAetnaERMasterLifeproDocuments - getAetnaERMasterLifeproDocuments method failed with exception. " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }


    protected void insertAetnaAPRMasterDocuments(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a documents array to insert
        if (request == null || !request.containsKey("documents"))
            throw new IllegalArgumentException("No Document Array Specified");
        if (_aetnaAPRMaster == null)
            throw new IllegalArgumentException("Aetna Application Pipeline Reject Master collection instance is not initialized");
        try {
            _logger.info("insertAetnaAPRMasterDocuments - Beginning of method - " + dateFormat.format(new Date()));
            super.insert(request, result, _aetnaAPRMaster);
            _logger.info("insertAetnaAPRMasterDocuments - End of method - " + dateFormat.format(new Date()));
        } catch (Exception e) {
            _logger.error("insertAetnaAPRMasterDocuments - insertAetnaAPRMasterDocuments method failed with exception  " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }

    protected void updateAetnaAPRMasterDocuments(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a filter
        if (request == null || !request.containsKey("documents"))
            throw new IllegalArgumentException("No Document Array Specified");
        if (_aetnaAPRMaster == null)
            throw new IllegalArgumentException("Aetna Application Pipeline Reject Master collection instance is not initialized");
        try {
            _logger.info("updateAetnaAPRMasterDocuments - Beginning of method - " + dateFormat.format(new Date()));
            super.update(request, result, _aetnaAPRMaster, "updateMany");
        } catch (Exception e) {
            _logger.error("updateAetnaAPRMasterDocuments - updateAetnaAPRMasterDocuments method failed with exception  " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }


    protected void getAetnaAPRMasterLifeproDocuments(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a filter
        if (request == null || !request.containsKey("filter"))
            throw new IllegalArgumentException("No Filter Condition Specified");
        if (_aetnaAPRMaster == null)
            throw new IllegalArgumentException("_aetnaAPRMaster collection instance is not initialized");
        try {
            Document docfilter = request.get("filter", Document.class);
            _logger.info("getAetnaAPRMasterLifeproDocuments - Beginning of method - " + dateFormat.format(new Date())+"\n Received Filter object. " + request.toJson());

            String processingKey = docfilter.getString("processingKey");
            String processingStatus = docfilter.getString("processingStatus");
            _logger.info(String.format("getAetnaAPRMasterLifeproDocuments - processingKey - %s . processingStatus - %s.", processingKey, processingStatus));

            Bson filter=new Document();
            filter=  Filters.and( Arrays.asList(Filters.eq("processingKey", processingKey),Filters.eq("processingStatus", processingStatus)));

            AggregateIterable<Document> masterDetails = _aetnaAPRMaster.aggregate(Arrays.asList(
                    Aggregates.match(filter),
                    Aggregates.project(fields(
                            include("memberId",
                                    "processingKey",
                                    "processingStatus",
                                    "recordIndex",
                                    "medicareId",
                                    "memberSSN",
                                    "affinityPolicyId",
                                    "firstName",
                                    "lastName",
                                    "memberCity",
                                    "memberState",
                                    "memberZipCode",
                                    "memberStatus",
                                    "planName",
                                    "gaTIN",
                                    "brokerNPN",
                                    "brokerName",
                                    "preSubmissionStatus",
                                    "cmsRejectCodes",
                                    "planCode",
                                    "policyTermReason",
                                    "policyStatus",
                                    "nonCommissionPlan",
                                    "duplicate",
                                    "invalidDataType",
                                    "validPreedits"),
                            computed("_id",  new Document().append("$toString","$_id")),
                            computed("applicationSignatureDate", eq("$dateToString", and(eq("date", "$applicationSignatureDate"), eq("format", canonicalDateFormat)))),
                            computed("applicationReceiveDate", eq("$dateToString", and(eq("date", "$applicationReceiveDate"), eq("format", canonicalDateFormat)))),
                            computed("effectiveDate", eq("$dateToString", and(eq("date", "$effectiveDate"), eq("format", canonicalDateFormat)))),
                            computed("fileDate", eq("$dateToString", and(eq("date", "$fileDate"), eq("format", canonicalDateFormat)))),
                            computed("cmsSubmissionDate", eq("$dateToString", and(eq("date", "$cmsSubmissionDate"), eq("format", canonicalDateFormat)))),
                            computed("cmsAcceptDate", eq("$dateToString", and(eq("date", "$cmsAcceptDate"), eq("format", canonicalDateFormat)))),
                            computed("cmsRejectDate", eq("$dateToString", and(eq("date", "$cmsRejectDate"), eq("format", canonicalDateFormat)))),
                            computed("lastUpdatedDate", eq("$dateToString", and(eq("date", "$lastUpdatedDate"), eq("format", canonicalDateFormat))))
                            )
                    )
                    )
            ).allowDiskUse(true);

            List<Document> responseDocs = new ArrayList<>();
            _logger.info("getAetnaAPRMasterLifeproDocuments - Started adding result documents to List object responseDocs - " + dateFormat.format(new Date()));
            masterDetails.iterator().forEachRemaining(responseDocs::add);
            _logger.info("getAetnaAPRMasterLifeproDocuments - Completed adding result documents to List object responseDocs - " + dateFormat.format(new Date()));

            _logger.info("getAetnaAPRMasterLifeproDocuments - Number of documents returned. " + responseDocs.size());
            result.setDocuments(responseDocs);
            _logger.info("getAetnaAPRMasterLifeproDocuments - result Object after reading EnrolmentRoster details." + result.toString());
            _logger.info("getAetnaAPRMasterLifeproDocuments - End of method - " + dateFormat.format(new Date()));

        } catch (Exception e) {
            _logger.error("getAetnaAPRMasterLifeproDocuments - getAetnaAPRMasterLifeproDocuments method failed with exception. " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }

}
