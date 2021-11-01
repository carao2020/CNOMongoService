/*
 * Created by rao on 31-12-2019.
 *
 * Change History
 * Date         Author      Change Details
 *
 * 23-Jan-2021  Rao         Updated logic in modifying status to "Generate IDF response" only for the records that are sent to BICPS rather than for all occurrences.
 * 25-Jan-2021  Rao         Added mqaJobId as additional field into statusAudit in method updateIDFClaimsDocuments() for claims collection
 */
package com.mqa.cno.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.ibm.mq.MQMessage;
import com.mongodb.client.*;
import com.mongodb.client.model.*;
import com.mqa.daemonserver.mongo.impl.MongoCRUDResult;
import com.mqa.daemonserver.mongo.impl.MongoCRUDServer;
import com.mqa.mq.entities.MQQueueBinding;
import mqa.daemon.abstracts.BaseMQADaemon;
import org.bson.BsonNull;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.mongodb.client.model.Filters.elemMatch;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;


/**
 * Created by rao on 31-12-2019.
 */
public class CNOMongoService extends MongoCRUDServer {

    // instance variables
    private MongoCollection<Document> _balancingCollection;
    private MongoCollection<Document> _claimsCollection;
    private MongoCollection<Document> _gridFSCollection;
    private MongoCollection<Document> _paymentSummaryCollection;
    private MongoCollection<Document> _idfDocCollection;
    private MongoCollection<Document> _collection1;

    Map<String, Object> gridFSproperties;

    Document updateBalancingRequest;
    Document updateClaimsRequest;
    Document updateGridFSRequest;
    List<Document> updateBalanceDocuments;
    List<Document> updateClaimDocuments;
    List<Document> updateGridFSDocuments;
    FindIterable<Document> findClaims;
    FindIterable<Document> findBalance;
    List<Document> claimDocs;
    List<Document> x12835Docs;
    List<Document> idfDocs;
    Document idfResponse = new Document();
    Document claimFilter;
    Document balanceFilter;

    DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
    private Document _baseDocument;

    public CNOMongoService(BaseMQADaemon parentDaemon, JsonNode listenerConfig, JsonNode commandConfig, MQQueueBinding binding) {
        super(parentDaemon, listenerConfig, commandConfig, binding);
    }

    @Override
    protected void processCustomRequest(MQMessage message, MongoCRUDResult result) throws Exception {
        try {
            switch (super._messageOperation) {
                case "updateIDFClaimsDocuments":
                    getCollections(message);
                    updateIDFClaimsDocuments(_baseDocument, result, "updateIDFClaimsDocuments");
                    break;
                case "updateIDFBalanceDocuments":
                    getCollections(message);
                    updateIDFBalanceDocuments(_baseDocument, result);
                    break;
                case "queryIDFPaymentValidation":
                    getCollections(message);
                    queryIDFPaymentValidation(_baseDocument, result);
                    break;
                case "queryIDFUnknownClaims":
                    getCollections(message);
                    queryIDFUnknownClaims(_baseDocument, result);
                    break;
                case "getIDFResponseDocuments":
                    getCollections(message);
                    getIDFResponseDocuments(_baseDocument, result, "getIDFResponseDocuments");
                    break;
                case "getVirtualCardReport":
                    getCollections(message);
                    getVirtualCardReport(_baseDocument, result, "getVirtualCardReport");
                    break;
                case "getX12835ST02Sequence":
                    getCollections(message);
                    getX12835ST02Sequence(_baseDocument, result, _balancingCollection);
                    break;
                case "getIDFResponseReport":
                    getCollections(message);
                    getIDFResponseReport(_baseDocument, result);
                    break;
                case "updatePaymentSummaryByIDF":
                    getCollections(message);
                    updatePaymentSummaryByIDF(_baseDocument);
                    break;
                case "updatePaymentSummaryByEftkey":
                    getCollections(message);
                    updatePaymentSummaryByEftkey(_baseDocument);
                    break;
                case "insertBalancingDocuments":
                    getCollections(message);
                    insertBalancingDocuments(_baseDocument, result);//insertMany
                    break;
                case "getDataBalancingDocuments":
                    getCollections(message);
                    getDataBalancingDocuments(_baseDocument, result);//read
                    break;
                case "getX12835TransformParms":
                    getCollections(message);
                    getX12835TransformParms(_baseDocument, result);//read
                    break;
                case "updateDataBalancingDocuments":
                    getCollections(message);
                    updateDataBalancingDocuments(_baseDocument, result); //updateMany
                    break;

                case "insertDataClaimsDocuments":
                    getCollections(message);
                    insertDataClaimsDocuments(_baseDocument, result);//insertMany
                    break;
                case "getX12835BalancingDocuments":
                    getCollections(message);
                    getX12835BalancingDocuments(_baseDocument, result);//read
                    break;
                case "updateX12835BalancingDocuments":
                    getCollections(message);
                    updateX12835BalancingDocuments(_baseDocument, result);//updateMany
                    break;
                case "updateX12835EnvelopeBalancingDocuments":
                    getCollections(message);
                    updateX12835EnvelopeBalancingDocuments(_baseDocument, result);//updateMany
                    break;
                case "updateX12835ClaimsDocuments":
                    getCollections(message);
                    updateX12835ClaimsDocuments(_baseDocument, result);//updateMany
                    break;
                case "updateX12835NCClaimsDocuments":
                    getCollections(message);
                    updateX12835NCClaimsDocuments(_baseDocument, result);//updateMany
                    break;
                case "getX12999BalancingDocuments":
                    getCollections(message);
                    getX12999BalancingDocuments(_baseDocument, result);//read
                    break;
                case "updateX12999BalancingDocuments":
                    getCollections(message);
                    updateX12999BalancingDocuments(_baseDocument, result);//updateMany
                    break;

                case "updateX12999ClaimsDocuments":
                    getCollections(message);
                    updateX12999ClaimsDocuments(_baseDocument, result);//updateMany
                    break;
                case "insertX12835GridFSDocuments":
                    getCollections(message);
                    gridFSproperties = super.getGridFSMessageProperties(message);
                    insertX12835GridFSDocuments(_baseDocument, result, gridFSproperties);//insertGFS
                    break;
                case "insertX12999GridFSDocuments":
                    getCollections(message);
                    gridFSproperties = super.getGridFSMessageProperties(message);
                    insertX12999GridFSDocuments(_baseDocument, result, gridFSproperties);//insertGFS
                    break;
                case "insertX12837GridFSDocuments":
                    getCollections(message);
                    gridFSproperties = super.getGridFSMessageProperties(message);
                    insertX12837GridFSDocuments(_baseDocument, result, gridFSproperties);//insertGFS
                    break;
                case "insertEFTDataGridFSDocuments":
                    getCollections(message);
                    gridFSproperties = super.getGridFSMessageProperties(message);
                    insertEFTDataGridFSDocuments(_baseDocument, result, gridFSproperties);//insertGFS
                    break;
                case "getIDFPaymentValidThreshold":
                    getCollections(message);
                    getIDFPaymentValidThreshold(_baseDocument, result);//read
                    break;

                case "insertIDFGridFSDocuments":
                    getCollections(message);
                    gridFSproperties = super.getGridFSMessageProperties(message);
                    insertIDFGridFSDocuments(_baseDocument, result, gridFSproperties);//insertGFS
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

    protected void insertBalancingDocuments(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a documents array to insert
        if (request == null || !request.containsKey("documents"))
            throw new IllegalArgumentException("No Document Array Specified");
        if (_collection1 == null)
            throw new IllegalArgumentException("balancing collection instance is not initialized");
        try {
            _logger.info("insertBalancingDocuments - Beginning of method - " + dateFormat.format(new Date()));

            super.insert(request, result, _collection1);
            List<Document> documents = request.getList("documents", Document.class);
            _logger.info("insertBalancingDocuments - Started updating paymentSummary Collection. " + dateFormat.format(new Date()));
            Document eftKeyDoc = new Document();
            for (Document doc : documents) {
                _logger.info("insertBalancingDocuments - Balancing document. " + doc.toJson());
                eftKeyDoc.append("filter", new Document().append("eftkey", doc.getString("eftkey")));
                _logger.info("insertBalancingDocuments - Request object that will be sent to  updatePaymentSummaryByEftkey(). " + eftKeyDoc.toJson());
                updatePaymentSummaryByEftkey(eftKeyDoc);
            }
            _logger.info("insertBalancingDocuments - Completed updating paymentSummary Collection. " + dateFormat.format(new Date()));
            _logger.info("insertBalancingDocuments - End of method - " + dateFormat.format(new Date()));
        } catch (Exception e) {
            _logger.error("insertBalancingDocuments - insertBalancingDocuments method failed with exception  " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }

    protected void getDataBalancingDocuments(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a filter
        if (request == null || !request.containsKey("filter"))
            throw new IllegalArgumentException("No Filter Condition Specified");
        if (_collection1 == null)
            throw new IllegalArgumentException("balancing collection instance is not initialized");
        try {
            _logger.info("getDataBalancingDocuments - Beginning of method - " + dateFormat.format(new Date()));
            _logger.info("getDataBalancingDocuments - Request object  - " + request.toJson());
            super.read(request, result, _collection1);
            _logger.info("getDataBalancingDocuments - Response object  - " + result.toString());
        } catch (Exception e) {
            _logger.error("getDataBalancingDocuments - getDataBalancingDocuments method failed with exception  " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }

    protected void getX12835TransformParms(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a filter
        if (request == null || !request.containsKey("filter"))
            throw new IllegalArgumentException("No Filter Condition Specified");
        if (_collection1 == null)
            throw new IllegalArgumentException("itxa_PrmList collection instance is not initialized");
        try {
            _logger.info("getX12835TransformParms - Beginning of method - " + dateFormat.format(new Date()));
            super.read(request, result, _collection1);
        } catch (Exception e) {
            _logger.error("getX12835TransformParms - getX12835TransformParms method failed with exception. " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }

    protected void updateDataBalancingDocuments(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a filter
        if (request == null || !request.containsKey("documents"))
            throw new IllegalArgumentException("No Document Array Specified");
        if (_collection1 == null)
            throw new IllegalArgumentException("balancing collection instance is not initialized");
        try {
            _logger.info("updateDataBalancingDocuments - Beginning of method - " + dateFormat.format(new Date()));
            super.update(request, result, _collection1, "updateMany");

            List<Document> documents = request.getList("documents", Document.class);
            List<Document> balanceFilterDocs = new ArrayList<>();
            _logger.info("updateDataBalancingDocuments - Started updating paymentSummary Collection. " + dateFormat.format(new Date()));
            Document eftKeyDoc = new Document();
            for (Document doc : documents) {
                _logger.info("updateDataBalancingDocuments - Balancing document filter. " + doc.toJson());
                findBalance = _balancingCollection.find(doc.get("filter", Document.class));
                findBalance.iterator().forEachRemaining(balanceFilterDocs::add);
                for (Document balanceDoc : balanceFilterDocs) {
                    eftKeyDoc.append("filter", new Document().append("eftkey", balanceDoc.getString("eftkey")));
                    _logger.info("updateDataBalancingDocuments - Request object that will be sent to  updatePaymentSummaryByEftkey(). " + eftKeyDoc.toJson());
                    updatePaymentSummaryByEftkey(eftKeyDoc);
                }
            }

        } catch (Exception e) {
            _logger.error("updateDataBalancingDocuments - updateDataBalancingDocuments method failed with exception  " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }

    protected void insertDataClaimsDocuments(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a documents array to insert
        if (request == null || !request.containsKey("documents"))
            throw new IllegalArgumentException("No Document Array Specified");
        if (_collection1 == null)
            throw new IllegalArgumentException("claims collection instance is not initialized");
        try {
            _logger.info("insertDataClaimsDocuments - Beginning of method - " + dateFormat.format(new Date()));
            super.insert(request, result, _collection1);
        } catch (Exception e) {
            _logger.error("insertDataClaimsDocuments - insertDataClaimsDocuments method failed with exception  " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }

    protected void getX12835BalancingDocuments(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a filter
        if (request == null || !request.containsKey("filter"))
            throw new IllegalArgumentException("No Filter Condition Specified");
        if (_collection1 == null)
            throw new IllegalArgumentException("balancing collection instance is not initialized");
        try {
            _logger.info("getX12835BalancingDocuments - Beginning of method - " + dateFormat.format(new Date()));
            super.read(request, result, _collection1);

            for (Document doc:result.getDocuments()) {
                _logger.info("getX12835BalancingDocuments - Documents returned - " + doc.toJson());
            }
            _logger.info("getX12835BalancingDocuments - End of method - " + dateFormat.format(new Date()));

        } catch (Exception e) {
            _logger.error("getX12835BalancingDocuments - getX12835BalancingDocuments method failed with exception. " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }

    protected void updateX12835BalancingDocuments(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a filter
        if (request == null || !request.containsKey("documents"))
            throw new IllegalArgumentException("No Document Array Specified");
        if (_collection1 == null)
            throw new IllegalArgumentException("balancing collection instance is not initialized");
        try {
            _logger.info("updateX12835BalancingDocuments - Beginning of method - " + dateFormat.format(new Date()));
            super.update(request, result, _collection1, "updateMany");

            List<Document> documents = request.getList("documents", Document.class);
            List<Document> balanceFilterDocs = new ArrayList<>();
            _logger.info("updateX12835BalancingDocuments - Started updating paymentSummary Collection. " + dateFormat.format(new Date()));
            Document eftKeyDoc = new Document();
            for (Document doc : documents) {
                _logger.info("updateX12835BalancingDocuments - Balancing document filter. " + doc.toJson());
                findBalance = _balancingCollection.find(doc.get("filter", Document.class));
                findBalance.iterator().forEachRemaining(balanceFilterDocs::add);
                for (Document balanceDoc : balanceFilterDocs) {
                    eftKeyDoc.append("filter", new Document().append("eftkey", balanceDoc.getString("eftkey")));
                    _logger.info("updateX12835BalancingDocuments - Request object that will be sent to  updatePaymentSummaryByEftkey(). " + eftKeyDoc.toJson());
                    updatePaymentSummaryByEftkey(eftKeyDoc);
                }
            }

        } catch (Exception e) {
            _logger.error("updateX12835BalancingDocuments - updateX12835BalancingDocuments method failed with exception  " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }

    protected void updateX12835EnvelopeBalancingDocuments(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a filter
        if (request == null || !request.containsKey("documents"))
            throw new IllegalArgumentException("No Document Array Specified");
        if (_collection1 == null)
            throw new IllegalArgumentException("balancing collection instance is not initialized");
        try {
            _logger.info("updateX12835EnvelopeBalancingDocuments - Beginning of method - " + dateFormat.format(new Date()));
            super.update(request, result, _collection1, "updateMany");

            List<Document> documents = request.getList("documents", Document.class);
            List<Document> balanceFilterDocs = new ArrayList<>();
            _logger.info("updateX12835EnvelopeBalancingDocuments - Started updating paymentSummary Collection. " + dateFormat.format(new Date()));
            Document eftKeyDoc = new Document();
            for (Document doc : documents) {
                _logger.info("updateX12835EnvelopeBalancingDocuments - Balancing document filter. " + doc.toJson());
                findBalance = _balancingCollection.find(doc.get("filter", Document.class));
                findBalance.iterator().forEachRemaining(balanceFilterDocs::add);
                for (Document balanceDoc : balanceFilterDocs) {
                    eftKeyDoc.append("filter", new Document().append("eftkey", balanceDoc.getString("eftkey")));
                    _logger.info("updateX12835EnvelopeBalancingDocuments - Request object that will be sent to  updatePaymentSummaryByEftkey(). " + eftKeyDoc.toJson());
                    updatePaymentSummaryByEftkey(eftKeyDoc);
                }
            }

        } catch (Exception e) {
            _logger.error("updateX12835EnvelopeBalancingDocuments - updateX12835EnvelopeBalancingDocuments method failed with exception  " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }


    protected void updateX12835ClaimsDocuments(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a filter
        if (request == null || !request.containsKey("documents"))
            throw new IllegalArgumentException("No Document Array Specified");
        if (_collection1 == null)
            throw new IllegalArgumentException("claims collection instance is not initialized");
        try {
            _logger.info("updateX12835ClaimsDocuments - Beginning of method - " + dateFormat.format(new Date()));
            super.update(request, result, _collection1, "updateMany");
        } catch (Exception e) {
            _logger.error("updateX12835ClaimsDocuments - updateX12835ClaimsDocuments method failed with exception  " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }

    protected void updateX12835NCClaimsDocuments(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a filter
        if (request == null || !request.containsKey("documents"))
            throw new IllegalArgumentException("No Document Array Specified");
        if (_collection1 == null)
            throw new IllegalArgumentException("claims collection instance is not initialized");
        try {
            _logger.info("updateX12835NCClaimsDocuments - Beginning of method - " + dateFormat.format(new Date()));
            super.update(request, result, _collection1, "updateMany");
        } catch (Exception e) {
            _logger.error("updateX12835NCClaimsDocuments - updateX12835NCClaimsDocuments method failed with exception  " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }

    protected void getX12999BalancingDocuments(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a filter
        if (request == null || !request.containsKey("filter"))
            throw new IllegalArgumentException("No Filter Condition Specified");
        if (_collection1 == null)
            throw new IllegalArgumentException("balancing collection instance is not initialized");
        try {
            _logger.info("getX12999BalancingDocuments - Beginning of method - " + dateFormat.format(new Date()));
            super.read(request, result, _collection1);
        } catch (Exception e) {
            _logger.error("getX12999BalancingDocuments - getX12999BalancingDocuments method failed with exception. " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }

    protected void updateX12999BalancingDocuments(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a filter
        if (request == null || !request.containsKey("documents"))
            throw new IllegalArgumentException("No Document Array Specified");
        if (_collection1 == null)
            throw new IllegalArgumentException("balancing collection instance is not initialized");
        try {
            _logger.info("updateX12999BalancingDocuments - Beginning of method - " + dateFormat.format(new Date()));
            _logger.info("updateX12999BalancingDocuments - Request object  - " + request.toJson());
            super.update(request, result, _collection1, "updateMany");

            _logger.info("updateX12999BalancingDocuments - Started updating paymentSummary Collection. " + dateFormat.format(new Date()));
            List<Document> documents = request.getList("documents", Document.class);
            Document balanceFilterDoc = documents.get(0).get("filter", Document.class);
            _logger.info("updateX12999BalancingDocuments - Balancing document filter. " + balanceFilterDoc.toJson());
            Document eftKeyDoc = new Document();
            eftKeyDoc.append("filter", new Document().append("eftkey", balanceFilterDoc.getString("eftkey")));
            _logger.info("updateX12999BalancingDocuments - Request object that will be sent to updatePaymentSummaryByEftkey(). " + eftKeyDoc.toJson());
            updatePaymentSummaryByEftkey(eftKeyDoc);

        } catch (Exception e) {
            _logger.error("updateX12999BalancingDocuments - updateX12999BalancingDocuments method failed with exception  " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }

    protected void updateX12999ClaimsDocuments(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a filter
        if (request == null || !request.containsKey("documents"))
            throw new IllegalArgumentException("No Document Array Specified");
        if (_collection1 == null)
            throw new IllegalArgumentException("claims collection instance is not initialized");
        try {
            _logger.info("updateX12999ClaimsDocuments - Beginning of method - " + dateFormat.format(new Date()));
            super.update(request, result, _collection1, "updateMany");
        } catch (Exception e) {
            _logger.error("updateX12999ClaimsDocuments - updateX12999ClaimsDocuments method failed with exception  " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }

    protected void insertX12835GridFSDocuments(Document request, MongoCRUDResult result, Map<String, Object> gridFSproperties) throws Exception {
        // check that we have a documents array to insert
        if (request == null || !request.containsKey("documents"))
            throw new IllegalArgumentException("No Document Array Specified");
        try {
            _logger.info("insertX12835GridFSDocuments - Beginning of method - " + dateFormat.format(new Date()));
            super.insertGridFS(request, result, gridFSproperties);
        } catch (Exception e) {
            _logger.error("insertX12835GridFSDocuments - insertX12835GridFSDocuments method failed with exception  " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }

    protected void insertX12999GridFSDocuments(Document request, MongoCRUDResult result, Map<String, Object> gridFSproperties) throws Exception {
        // check that we have a documents array to insert
        if (request == null || !request.containsKey("documents"))
            throw new IllegalArgumentException("No Document Array Specified");
        try {
            _logger.info("insertX12999GridFSDocuments - Beginning of method - " + dateFormat.format(new Date()));
            super.insertGridFS(request, result, gridFSproperties);
        } catch (Exception e) {
            _logger.error("insertX12999GridFSDocuments - insertX12999GridFSDocuments method failed with exception  " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }

    protected void insertX12837GridFSDocuments(Document request, MongoCRUDResult result, Map<String, Object> gridFSproperties) throws Exception {
        // check that we have a documents array to insert
        if (request == null || !request.containsKey("documents"))
            throw new IllegalArgumentException("No Document Array Specified");
        try {
            _logger.info("insertX12837GridFSDocuments - Beginning of method - " + dateFormat.format(new Date()));
            super.insertGridFS(request, result, gridFSproperties);
        } catch (Exception e) {
            _logger.error("insertX12837GridFSDocuments - insertX12837GridFSDocuments method failed with exception  " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }

    protected void insertEFTDataGridFSDocuments(Document request, MongoCRUDResult result, Map<String, Object> gridFSproperties) throws Exception {
        // check that we have a documents array to insert
        if (request == null || !request.containsKey("documents"))
            throw new IllegalArgumentException("No Document Array Specified");
        try {
            _logger.info("insertEFTDataGridFSDocuments - Beginning of method - " + dateFormat.format(new Date()));
            _logger.debug(request.toJson());

            super.insertGridFS(request, result, gridFSproperties);
        } catch (Exception e) {
            _logger.error("insertEFTDataGridFSDocuments - insertEFTDataGridFSDocuments method failed with exception  " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }

    protected void getIDFPaymentValidThreshold(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a filter
        if (request == null || !request.containsKey("filter"))
            throw new IllegalArgumentException("No Filter Condition Specified");
        if (_collection1 == null)
            throw new IllegalArgumentException("itxa_PrmList collection instance is not initialized");
        try {
            _logger.info("getIDFPaymentValidThreshold - Beginning of method - " + dateFormat.format(new Date()));
            super.read(request, result, _collection1);
        } catch (Exception e) {
            _logger.error("getIDFPaymentValidThreshold - getIDFPaymentValidThreshold method failed with exception. " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }

    protected void insertIDFGridFSDocuments(Document request, MongoCRUDResult result, Map<String, Object> gridFSproperties) throws Exception {
        // check that we have a documents array to insert
        if (request == null || !request.containsKey("documents"))
            throw new IllegalArgumentException("No Document Array Specified");
        try {
            _logger.info("insertIDFGridFSDocuments - Beginning of method - " + dateFormat.format(new Date()));
            super.insertGridFS(request, result, gridFSproperties);
        } catch (Exception e) {
            _logger.error("insertIDFGridFSDocuments - insertIDFGridFSDocuments method failed with exception  " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }

    protected void getCollections(MQMessage message) throws Exception {
        _baseDocument = super._baseDocument;
        // routine to get the collection from the message
        checkRFHHeader(new String[]{"mqaCRUDTable"});
        String collectionNames = message.getStringProperty("usr.mqaCRUDTable");
        String[] collections = collectionNames.split("\\|");
        _balancingCollection = getCollectionByName(collections[0]);
        _collection1 = getCollectionByName(collections[0]);
        if (collections.length > 1)
            _claimsCollection = getCollectionByName(collections[1]);
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

    protected void updateIDFClaimsDocuments(Document request, MongoCRUDResult result, String operation) throws Exception {

        try {
            String mftFileName, idfDateTime, idfJobId, subCompanyId, companyId, paymentMethod, paymentStatus, prevPaymentMethod, prevPaymentStatus,payerClaimNumber, trn02;
            Double requestedPaidAmount, paidAmount;
            Date startDate, endDate;
            int docIndex = -1;
            startDate=new Date();
            // check if we have valid json request
            if (request == null || !request.containsKey("idfClaims"))
                throw new IllegalArgumentException("No idfClaims document Specified");
            if (_claimsCollection == null)
                throw new IllegalArgumentException("Claims collection instance is not initialized");
            if (_balancingCollection == null)
                throw new IllegalArgumentException("Balancing collection instance is not initialized");
            if (operation == null)
                throw new IllegalArgumentException("Operation not specified");

            Document balanceDocument = request.get("idfBalance", Document.class);
            List<Document> claimDocuments = request.getList("idfClaims", Document.class);

            idfJobId = balanceDocument.get("mft", Document.class).getString("mqaJobId");
            subCompanyId = balanceDocument.getString("subCompanyId");
            companyId = claimDocuments.get(0).getString("fundingEntity");
            _logger.info("updateIDFClaimsDocuments - Started processing. CompanyId - " + companyId
                    + " - SubCompanyID - " + subCompanyId
                    + " - ClaimCount - " + claimDocuments.size()
                    + " Time - " + dateFormat.format(startDate)
            );
            if (claimDocuments.isEmpty())
                throw new IllegalArgumentException("No idfClaims documents found in the input JSOn document");

            updateClaimsRequest = new Document();
            updateClaimDocuments = new ArrayList<>();

            for (Document doc : claimDocuments) {
                docIndex++;
                _logger.debug("*******************************************************************************Start - Processing IDF document with Index "+docIndex+" *******************************************************************************");


                if (!doc.containsKey("trn02")) {
                    result.addError(_logger, new IllegalArgumentException(String.format("IDF JSON document at Index %d does not have trn02 field.Please verify the IDF file content if it has Funding Trace Number for all the records. The JSON document is %s", docIndex, doc)));
                    continue;
                } else {
                    trn02 = doc.getString("trn02");
                }
                if (!doc.containsKey("payerClaimNumber")) {
                    result.addError(_logger, new IllegalArgumentException(String.format(
                            "IDF JSON document at Index %d does not have payerClaimNumber field. Please verify the IDF file content if it has Payer Claim Number for all the records. The JSON document is %s", docIndex, doc)));
                    continue;
                } else {
                    payerClaimNumber = doc.getString("payerClaimNumber");
                }

                claimFilter = new Document();
                claimDocs = new ArrayList<>();
                mftFileName = doc.getString("MFTFileName");
                idfDateTime = doc.getDate("idfDateTime").toString();
                paymentMethod = doc.getString("paymentMethod").toUpperCase();
                paymentStatus = doc.getString("status").toUpperCase();
                paidAmount = doc.get("claimPayment", Double.class);

                _logger.debug("MFT Filename: - " + mftFileName + ". trn02 : - " + trn02 + ". payerClaimNumber : - " + payerClaimNumber + ". idfDateTime : - " + idfDateTime);
                //Prepare filter object to get the claimDocument from claims collection
                claimFilter.append("claimNumber", payerClaimNumber)
                        .append("x12835.trn02", trn02)
                        .append("x12835.eftkey", new Document("$exists", true));

                findClaims = _claimsCollection.find(claimFilter);
                findClaims.iterator().forEachRemaining(claimDocs::add);

                if (claimDocs.size() > 0) {
                    for (Document claim : claimDocs) {
                        try {
                            x12835Docs = new ArrayList<>();
                            int idfCount;
                            //get all the x12835 document present in claim document
                            if (claim.get("x12835").getClass() == ArrayList.class)
                                x12835Docs = claim.getList("x12835", Document.class);
                            else
                                x12835Docs.add(claim.get("x12835", Document.class));

                            if (x12835Docs.size() > 0) {
                                Document x12835 = new Document();
                                for (Document doc835 : x12835Docs) {
                                    if (doc835.getString("trn02").equals(trn02)) {
                                        x12835 = doc835;
                                        break;
                                    }
                                }
                                if (x12835.get("clp04").getClass() == Integer.class)
                                    requestedPaidAmount = x12835.get("clp04", Integer.class).doubleValue();
                                else
                                    requestedPaidAmount = x12835.get("clp04", Double.class);

                                //append the paymentValidation to idf ClaimsObject
                                if (Double.compare(paidAmount, requestedPaidAmount) == 0)
                                    doc.put("paymentValid", Boolean.TRUE);
                                else
                                    doc.put("paymentValid", Boolean.FALSE);

                                if (claim.containsKey("idf")) {
                                    idfDocs = claim.getList("idf", Document.class);
                                    idfCount = idfDocs.size();

                                    if (idfCount > 0) {
                                        Document prevIdfDoc = idfDocs.get(idfCount - 1);
                                        prevPaymentMethod = prevIdfDoc.getString("paymentMethod").toUpperCase();
                                        prevPaymentStatus = prevIdfDoc.getString("status").toUpperCase();
                                        _logger.debug(" Previous IDF MFTFilename - " + idfDocs.get(idfCount - 1).getString("MFTFileName") +
                                                ". Previous IDF Date - " + idfDocs.get(idfCount - 1).getDate("idfDateTime").toString()+
                                                ". Previous Payment Method - "+prevPaymentMethod+
                                                ". Previous Payment Status - "+prevPaymentStatus);

                                        _logger.debug("claimFilter - " + claimFilter.toJson());

                                        if ((prevPaymentMethod.equals("ACH") && prevPaymentStatus.equals("RETURNED") && (!paymentMethod.equals("ACH") && !paymentStatus.equals("RETURNED"))
                                                || (prevPaymentMethod.equals("VIRTUALCARD") && prevPaymentStatus.equals("CANCELLED") && (!paymentMethod.equals("VIRTUALCARD") && !paymentStatus.equals("CANCELLED"))))) {
                                            doc.putIfAbsent("reissueIndicator", Boolean.TRUE);
                                            /*Update an existing document*/
                                            updateClaimDocuments.add(new Document().append("filter", claimFilter)
                                                    .append("update", new Document("$set", new Document(String.format("idf.%d.hasReIssuePayment", idfCount - 1), Boolean.TRUE))));

                                        } else {
                                            doc.putIfAbsent("reissueIndicator", Boolean.FALSE);
                                            /*Update an existing document*/
                                            updateClaimDocuments.add(new Document().append("filter", claimFilter)
                                                    .append("update", new Document("$set", new Document(String.format("idf.%d.hasReIssuePayment", idfCount - 1), Boolean.FALSE))));
                                        }

                                    } else {
                                        doc.putIfAbsent("reissueIndicator", Boolean.FALSE);

                                    }

                                } else {
                                    doc.putIfAbsent("reissueIndicator", Boolean.FALSE);
                                }
                            }

                            updateClaimDocuments.add(new Document().append("filter", claimFilter)
                                    .append("update", new Document("$addToSet",
                                            new Document("idf", new Document(doc).append("identified", true))
                                                    .append("statusAudit", new Document("processingStatus", "Inbound IDF")))
                                            .append("$set", new Document("finalPaymentMethod", doc.getString("paymentMethod")).append("processedDate", new Date()).append("mqaJobId",idfJobId)
                                                    .append("finalPaymentStatus", doc.getString("status"))
                                                    .append("processingStatus", "Inbound IDF")
                                                    .append("processedDate", new Date()))));
                        } catch (Exception exc) {
                            result.addError(_logger, exc);
                            throw exc;
                        }
                    }
                } else {
                    updateClaimDocuments.add(new Document().append("filter", claimFilter)
                            .append("update", new Document("$addToSet",
                                    new Document("idf", new Document(doc).append("identified", false))
                                            .append("statusAudit", new Document("processingStatus", "Inbound IDF").append("processedDate", new Date()).append("mqaJobId", idfJobId)))
                                    .append("$set", new Document("finalPaymentMethod", doc.getString("paymentMethod"))
                                            .append("finalPaymentStatus", doc.getString("status"))
                                            .append("processingStatus", "Inbound IDF")
                                            .append("processedDate", new Date()))));
                }

                //_logger.debug("updateIDFClaimsDocuments - claims collection update request object. " + updateClaimDocuments.toString());

                _logger.debug("*******************************************************************************Complete - Processing IDF document with Index "+docIndex+" *******************************************************************************");

            }

            _logger.debug("Before calling builk update operation - " + dateFormat.format(new Date()));

            if (!updateClaimDocuments.isEmpty()) {
                updateClaimsRequest.put("documents", updateClaimDocuments);
                //Update claims collection
                _logger.debug("updateIDFClaimsDocuments - claims collection update request object. " + updateClaimsRequest.toJson());
                super.update(updateClaimsRequest, result, _claimsCollection, "updateMany");
            }

            _logger.debug("After calling builk update operation - " + dateFormat.format(new Date()));

            if (_idfDocCollection == null)
                _idfDocCollection = getCollectionByName("idfDocCollection");

            Document idfMFT = (Document) balanceDocument.get("mft");
            idfMFT.append("_id", idfJobId);
            List<Document> idfMFTDocs = new ArrayList<>();
            idfMFTDocs.add(idfMFT);
            Document idfMFTRequest = new Document();
            if (idfMFTDocs.size() > 0) {
                idfMFTRequest.put("documents", idfMFTDocs);
                _logger.debug("updateIDFClaimsDocuments - MFT document for IDF file that will be inserted into idfDocCollection. " + idfMFTDocs.get(0).toJson());
                super.insert(idfMFTRequest, result, _idfDocCollection);
            }
            endDate=new Date();

            _logger.debug("updateIDFClaimsDocuments - Added MFT document for IDF file is completed. " + dateFormat.format(new Date()));

            _logger.info("updateIDFClaimsDocuments - Completed processing. CompanyId - " + companyId
                    + " - SubCompanyID - " + subCompanyId
                    + " - ClaimCount - " + claimDocuments.size()
                    + " Time - " + dateFormat.format(endDate)
                    + " Execution time in Milliseconds - " + Math.abs(endDate.getTime() - startDate.getTime())
            );

        } catch (Exception e) {
            _logger.error("updateIDFClaimsDocuments - updating IDF documents failed. Exception is " + e);
            throw e;
        }
    }

    protected Document getIDFMFTDocument(String mqaJobId) throws Exception {

        _logger.info("getIDFMFTDocument - IDF mqaJobId. " + mqaJobId);
        // check that we have a filter

        if (_idfDocCollection == null)
            _idfDocCollection = getCollectionByName("idfDocCollection");

        Document mftDoc = new Document();
        FindIterable<Document> findIDFDoc = _idfDocCollection.find(eq("_id", mqaJobId));
        if (findIDFDoc.iterator().hasNext()) {
            mftDoc = findIDFDoc.iterator().next();
        }
        _logger.info("getIDFMFTDocument - The identified MFT document. " + mftDoc.toJson());
        return mftDoc;

    }

    protected void updateIDFBalanceDocuments(Document request, MongoCRUDResult result) throws Exception {

        if (request == null || !request.containsKey("documents"))
            throw new IllegalArgumentException("No Document Array Specified");
        List<Document> documents = request.getList("documents", Document.class);

        if (_claimsCollection == null)
            throw new IllegalArgumentException("Claims collection name was not mentioned in <mqaCRUDTable> field. " +
                    "This should be combination of <BalancingCollection>|<ClaimsCollection>");
        if (_balancingCollection == null)
            throw new IllegalArgumentException("Balancing collection name was not mentioned in <mqaCRUDTable> field. " +
                    "This should be combination of <BalancingCollection>|<ClaimsCollection>");
        try {

            Document updateDocument = documents.get(0);
            _logger.info("updateIDFBalanceDocuments - Request object - " + request.toJson());
            if (updateDocument == null || !updateDocument.containsKey("filter"))
                throw new IllegalArgumentException("No filter document Specified");
            Document claimFilter = updateDocument.get("filter", Document.class);
            _logger.info("updateIDFBalanceDocuments - Beginning of method - " + dateFormat.format(new Date()) + "\n Received Filter object. " + request.toJson());

            String mqaJobId;
            mqaJobId = claimFilter.getString("idfmqaJobId");
            if (mqaJobId == null)
                throw new IllegalArgumentException("Filter document Specified does not have idfmqaJobId key in it.");
            _logger.info("updateIDFBalanceDocuments - idfJobId. " + mqaJobId);
            Document mftDoc = getIDFMFTDocument(mqaJobId);
            List<? extends Bson> pipeline = Arrays.asList(
                    new Document()
                            .append("$match", new Document()
                                    .append("idf.mqaJobId", mqaJobId)
                            ),
                    new Document()
                            .append("$project", new Document()
                                    .append("eftkey", 1.0)
                                    .append("mainframeCreateDate", 1.0)
                                    .append("mainframeDate", 1.0)
                                    .append("parentCompany", 1.0)
                                    .append("company", 1.0)
                                    .append("companyId", 1.0)
                                    .append("subCompanyId", 1.0)
                                    .append("processingStatus", 1.0)
                                    .append("claimNumber", "$claimInfo.claimNumber")
                                    .append("checkEFTNumber", 1.0)
                                    .append("trn02", 1.0)
                                    .append("finalPaymentMethod", 1.0)
                                    .append("finalPaymentStatus", 1.0)
                                    .append("acceptReject", 1.0)
                                    .append("claimTotalCharge", "$claimInfo.claimTotalCharge")
                                    .append("claimPaidAmount", "$claimInfo.claimPaidAmount")
                                    .append("idf", new Document()
                                            .append("$arrayElemAt", Arrays.asList(
                                                    new Document()
                                                            .append("$filter", new Document()
                                                                    .append("input", "$idf")
                                                                    .append("as", "x")
                                                                    .append("cond", new Document()
                                                                            .append("$eq", Arrays.asList(
                                                                                    "$$x.mqaJobId",
                                                                                    mqaJobId
                                                                                    )
                                                                            )
                                                                    )
                                                            ),
                                                    -1.0
                                                    )
                                            )
                                    )
                            ),
                    new Document()
                            .append("$facet", new Document()
                                    .append("paymentMethodCounts", Arrays.asList(
                                            new Document()
                                                    .append("$group", new Document()
                                                            .append("_id", new Document()
                                                                    .append("eftkey", "$eftkey")
                                                                    .append("paymentMethod", new Document()
                                                                            .append("$ifNull", Arrays.asList(
                                                                                    "$idf.paymentMethod",
                                                                                    "NCP"
                                                                                    )
                                                                            )
                                                                    )
                                                            )
                                                            .append("claimCount", new Document()
                                                                    .append("$sum", 1.0)
                                                            )
                                                            .append("claimAmount", new Document()
                                                                    .append("$sum", "$idf.claimPayment")
                                                            )
                                                            .append("claimInfo", new Document()
                                                                    .append("$addToSet", new Document()
                                                                            .append("eftkey", "$eftkey")
                                                                            .append("company", "$company")
                                                                            .append("companyId", "$companyId")
                                                                            .append("businessId", "$businessId")
                                                                            .append("idfDateTime", "$idf.idfDateTime")
                                                                            .append("receivedDateTime", "$idf.receivedDateTime")
                                                                            .append("fileName", "$idf.MFTFileName")
                                                                    )
                                                            )
                                                    ),
                                            new Document()
                                                    .append("$group", new Document()
                                                            .append("_id", new Document()
                                                                    .append("eftkey", "$_id.eftkey")
                                                            )
                                                            .append("claimCount", new Document()
                                                                    .append("$sum", "$claimCount")
                                                            )
                                                            .append("claimAmount", new Document()
                                                                    .append("$sum", "$claimAmount")
                                                            )
                                                            .append("paymentMethodCounts", new Document()
                                                                    .append("$addToSet", new Document()
                                                                            .append("paymentMethod", "$_id.paymentMethod")
                                                                            .append("claimCount", new Document()
                                                                                    .append("$sum", "$claimCount")
                                                                            )
                                                                            .append("claimAmount", new Document()
                                                                                    .append("$sum", "$claimAmount")
                                                                            )
                                                                    )
                                                            )
                                                            .append("claimInfo", new Document()
                                                                    .append("$addToSet", new Document()
                                                                            .append("claimInfo", new Document()
                                                                                    .append("$arrayElemAt", Arrays.asList(
                                                                                            "$claimInfo",
                                                                                            0.0
                                                                                            )
                                                                                    )
                                                                            )
                                                                    )
                                                            )
                                                    ),
                                            new Document()
                                                    .append("$project", new Document()
                                                            .append("_id", 0.0)
                                                            .append("claimInfo", new Document()
                                                                    .append("$arrayElemAt", Arrays.asList(
                                                                            "$claimInfo",
                                                                            0.0
                                                                            )
                                                                    )
                                                            )
                                                            .append("claimCount", 1.0)
                                                            .append("paymentMethodCounts", 1.0)
                                                    ),
                                            new Document()
                                                    .append("$project", new Document()
                                                            .append("eftkey", "$claimInfo.claimInfo.eftkey")
                                                            .append("company", "$claimInfo.claimInfo.company")
                                                            .append("companyId", "$claimInfo.claimInfo.companyId")
                                                            .append("businessId", "$claimInfo.claimInfo.businessId")
                                                            .append("idfDateTime", "$claimInfo.claimInfo.idfDateTime")
                                                            .append("receivedDateTime", "$claimInfo.claimInfo.receivedDateTime")
                                                            .append("fileName", "$claimInfo.claimInfo.fileName")
                                                            .append("claimCount", 1.0)
                                                            .append("paymentMethodCounts", 1.0)
                                                    )
                                            )
                                    )
                                    .append("paymentStatusCounts", Arrays.asList(
                                            new Document()
                                                    .append("$group", new Document()
                                                            .append("_id", new Document()
                                                                    .append("eftkey", "$eftkey")
                                                                    .append("paymentStatus", new Document()
                                                                            .append("$ifNull", Arrays.asList(
                                                                                    "$idf.status",
                                                                                    "NCP"
                                                                                    )
                                                                            )
                                                                    )
                                                            )
                                                            .append("claimCount", new Document()
                                                                    .append("$sum", 1.0)
                                                            )
                                                            .append("claimAmount", new Document()
                                                                    .append("$sum", "$idf.claimPayment")
                                                            )
                                                            .append("claimInfo", new Document()
                                                                    .append("$addToSet", new Document()
                                                                            .append("eftkey", "$eftkey")
                                                                            .append("company", "$company")
                                                                            .append("companyId", "$companyId")
                                                                            .append("businessId", "$businessId")
                                                                            .append("idfDateTime", "$idf.idfDateTime")
                                                                            .append("receivedDateTime", "$idf.receivedDateTime")
                                                                            .append("fileName", "$idf.MFTFileName")
                                                                    )
                                                            )
                                                    ),
                                            new Document()
                                                    .append("$group", new Document()
                                                            .append("_id", new Document()
                                                                    .append("eftkey", "$_id.eftkey")
                                                            )
                                                            .append("claimCount", new Document()
                                                                    .append("$sum", "$claimCount")
                                                            )
                                                            .append("claimAmount", new Document()
                                                                    .append("$sum", "$claimAmount")
                                                            )
                                                            .append("paymentStatusCounts", new Document()
                                                                    .append("$addToSet", new Document()
                                                                            .append("paymentStatus", "$_id.paymentStatus")
                                                                            .append("claimCount", new Document()
                                                                                    .append("$sum", "$claimCount")
                                                                            )
                                                                            .append("claimAmount", new Document()
                                                                                    .append("$sum", "$claimAmount")
                                                                            )
                                                                    )
                                                            )
                                                            .append("claimInfo", new Document()
                                                                    .append("$addToSet", new Document()
                                                                            .append("claimInfo", new Document()
                                                                                    .append("$arrayElemAt", Arrays.asList(
                                                                                            "$claimInfo",
                                                                                            0.0
                                                                                            )
                                                                                    )
                                                                            )
                                                                    )
                                                            )
                                                    ),
                                            new Document()
                                                    .append("$project", new Document()
                                                            .append("_id", 0.0)
                                                            .append("claimInfo", new Document()
                                                                    .append("$arrayElemAt", Arrays.asList(
                                                                            "$claimInfo",
                                                                            0.0
                                                                            )
                                                                    )
                                                            )
                                                            .append("claimCount", 1.0)
                                                            .append("paymentStatusCounts", 1.0)
                                                    ),
                                            new Document()
                                                    .append("$project", new Document()
                                                            .append("eftkey", "$claimInfo.claimInfo.eftkey")
                                                            .append("company", "$claimInfo.claimInfo.company")
                                                            .append("companyId", "$claimInfo.claimInfo.companyId")
                                                            .append("businessId", "$claimInfo.claimInfo.businessId")
                                                            .append("idfDateTime", "$claimInfo.claimInfo.idfDateTime")
                                                            .append("receivedDateTime", "$claimInfo.claimInfo.receivedDateTime")
                                                            .append("fileName", "$claimInfo.claimInfo.fileName")
                                                            .append("claimCount", 1.0)
                                                            .append("paymentStatusCounts", 1.0)
                                                    )
                                            )
                                    )
                            ),
                    new Document()
                            .append("$unwind", new Document()
                                    .append("path", "$paymentMethodCounts")
                            ),
                    new Document()
                            .append("$project", new Document()
                                    .append("eftkey", "$paymentMethodCounts.eftkey")
                                    .append("company", "$paymentMethodCounts.company")
                                    .append("companyId", "$paymentMethodCounts.companyId")
                                    .append("businessId", "$paymentMethodCounts.businessId")
                                    .append("idfDateTime", "$paymentMethodCounts.idfDateTime")
                                    .append("receivedDateTime", "$paymentMethodCounts.receivedDateTime")
                                    .append("fileName", "$paymentMethodCounts.fileName")
                                    .append("claimCount", "$paymentMethodCounts.claimCount")
                                    .append("paymentMethodCounts", "$paymentMethodCounts.paymentMethodCounts")
                                    .append("paymentStatusCounts", new Document()
                                            .append("$filter", new Document()
                                                    .append("input", "$paymentStatusCounts")
                                                    .append("as", "x")
                                                    .append("cond", new Document()
                                                            .append("$eq", Arrays.asList(
                                                                    "$$x.eftkey",
                                                                    "$paymentMethodCounts.eftkey"
                                                                    )
                                                            )
                                                    )
                                            )
                                    )
                            ),
                    new Document()
                            .append("$project", new Document()
                                    .append("eftkey", 1.0)
                                    .append("company", 1.0)
                                    .append("companyId", 1.0)
                                    .append("businessId", 1.0)
                                    .append("idfDateTime", 1.0)
                                    .append("receivedDateTime", 1.0)
                                    .append("fileName", 1.0)
                                    .append("claimCount", 1.0)
                                    .append("paymentMethodCounts", 1.0)
                                    .append("paymentStatusCounts", new Document()
                                            .append("$arrayElemAt", Arrays.asList(
                                                    "$paymentStatusCounts.paymentStatusCounts",
                                                    0.0
                                                    )
                                            )
                                    )
                            )
            );

            AggregateIterable<Document> aggregateIterator = _claimsCollection.aggregate(pipeline)
                    .allowDiskUse(true);

            List<Document> resultArray = new ArrayList<>();
            aggregateIterator.iterator().forEachRemaining(resultArray::add);

            _logger.info("updateIDFBalanceDocuments - End aggregation of claims documents - " + dateFormat.format(new Date()));

            updateBalancingRequest = new Document();
            updateBalanceDocuments = new ArrayList<>();

            Document balanceStatusUpdate = new Document().append("processingStatus", "Inbound IDF")
                    .append("processedDate", new Date())
                    .append("mqaJobId", mqaJobId);

            _logger.info("updateIDFBalanceDocuments - Started updating Balancing collection with summaries - " + dateFormat.format(new Date()));
            String eftkey;
            for (Document idfBalanceDoc : resultArray) {
                try {
                    _logger.info("updateIDFBalanceDocuments - Facets Summaries documents - " + idfBalanceDoc.toJson());
                    eftkey = idfBalanceDoc.getString("eftkey");
                    idfBalanceDoc.remove("eftkey");
                    if (eftkey != null) {
                        balanceFilter = new Document().append("eftkey", eftkey);
                        idfBalanceDoc.append("mft", mftDoc);
                        updateBalanceDocuments.add(new Document().append("filter", balanceFilter)
                                .append("update",
                                        new Document()
                                                .append("$addToSet", new Document("idf", idfBalanceDoc)
                                                        .append("statusAudit", balanceStatusUpdate))
                                                .append("$set", new Document("status.processing", "Inbound IDF")
                                                        .append("status.lastUpdated", new Date()))));
                    }
                    _logger.info("updateIDFBalanceDocuments - The result object to be added for eftkey " + eftkey + " . " + idfBalanceDoc.toString());

                } catch (Exception exc) {
                    result.addError(_logger, exc);
                    throw exc;
                }
            }

            if (!updateBalanceDocuments.isEmpty()) {
                updateBalancingRequest.put("documents", updateBalanceDocuments);
                _logger.debug("updateIDFBalanceDocuments - Balancing collection update Request object. " + updateBalancingRequest.toJson());

                //Update balancing collection
                super.update(updateBalancingRequest, result, _balancingCollection, "updateMany");
                //read the claim Documents
                _logger.debug("updateIDFBalanceDocuments - Balancing collection update result object. " + result.toString());
            }
            _logger.info("updateIDFBalanceDocuments - End of updating Balancing collection with summaries - " + dateFormat.format(new Date()));


            _logger.info("updateIDFBalanceDocuments - Calling updatePaymentSummaryByIDF method. " + dateFormat.format(new Date()));
            updatePaymentSummaryByIDF(updateDocument);

        } catch (
                Exception e)

        {
            _logger.error("updateIDFBalanceDocuments - updating IDF documents failed. Exception is " + e);
            throw e;
        }

    }

    protected void queryIDFPaymentValidation(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a filter
        if (request == null || !request.containsKey("filter"))
            throw new IllegalArgumentException("No Filter Condition Specified");
        if (_claimsCollection == null)
            throw new IllegalArgumentException("Claims collection instance is not initialized");

        try {
            _logger.info("queryIDFPaymentValidation - Beginning of method - " + dateFormat.format(new Date()) + "\n Request object is" + request.toJson());
            // get the filter
            Document filter = request.get("filter", Document.class);
            String idfJobId = filter.getString("idfmqaJobId");
            if (idfJobId == null || idfJobId == "")
                throw new IllegalArgumentException("mqaJobId is not present in filter document sent by ITX map.");
            AggregateIterable<Document> summaries = _claimsCollection.aggregate(Arrays.asList(
                    Aggregates.match(new Document("idf.idfJobId", idfJobId).append("idf.paymentValid", Boolean.FALSE)),
                    Aggregates.group(null, Accumulators.sum("claimCount", 1))
            ));

            // execute the find
            List<Document> resultArray = new ArrayList<>();
            summaries.iterator().forEachRemaining(resultArray::add);
            result.setDocuments(resultArray);

            _logger.info("queryIDFPaymentValidation - Aggregate result. " + resultArray.toString());
            // update the summary
            result.addSummary("matched", resultArray.size());
            _logger.info("queryIDFPaymentValidation - result. " + result);
            _logger.info("queryIDFPaymentValidation - End of method - " + dateFormat.format(new Date()));

            //call updateIDFBalanceDocuments method to update IDF summary document within balancing collection
            //updateIDFBalanceDocuments(request, result);
            //call updatePaymentSummaryByIDF method to update Payment Summary document for all the EFTKeys that are affected by this IDF file.
            //updatePaymentSummaryByIDF(request);

        } catch (Exception e) {
            _logger.error("queryIDFPaymentValidation - queryIDFPaymentValidation method failed with exception  " + e);
            throw e;
        } finally {
            Runtime r = Runtime.getRuntime();
            r.gc();
        }
    }

    protected void queryIDFUnknownClaims(Document request, MongoCRUDResult result) throws Exception {
        // check that we have a filter
        if (request == null || !request.containsKey("filter"))
            throw new IllegalArgumentException("No Filter Condition Specified");
        if (_claimsCollection == null)
            throw new IllegalArgumentException("Claims collection instance is not initialized");

        try {
            _logger.info("queryIDFUnknownClaims - Beginning of method - " + dateFormat.format(new Date()) + "\n Request object is" + request.toJson());
            // get the filter
            Document filter = request.get("filter", Document.class);
            String idfJobId = filter.getString("idf.mqaJobId");
            if (idfJobId == null || idfJobId == "")
                throw new IllegalArgumentException("mqaJobId is not present in filter document sent by ITX map.");
            AggregateIterable<Document> summaries = _claimsCollection.aggregate(Arrays.asList(
                    Aggregates.match(and(Arrays.asList(eq("idf.idfJobId", idfJobId), exists("idf.identified", true), eq("idf.identified", false)))),
                    Aggregates.group(null, Accumulators.sum("claimCount", 1))
            ));

            // execute the find
            List<Document> resultArray = new ArrayList<>();
            summaries.iterator().forEachRemaining(resultArray::add);
            result.setDocuments(resultArray);
            _logger.info("queryIDFUnknownClaims - Aggregate result document. " + resultArray.toString());

            // update the summary
            result.addSummary("matched", resultArray.size());
            _logger.info("queryIDFUnknownClaims - End of queryIDFUnknownClaims process - " + dateFormat.format(new Date()));

            _logger.info("queryIDFUnknownClaims - Started processing additional steps - " + dateFormat.format(new Date()));
            _gridFSCollection = getCollectionByName("fs.files");
            updateGridFSIDFUnknownClaims(idfJobId, result, _gridFSCollection);
            updateGridFSIDFPaymentValidation(idfJobId, result, _gridFSCollection);
            updateGridFSIDFIndicators(idfJobId, result, _gridFSCollection);
            updateGridFSIDFFields(idfJobId, result, _gridFSCollection);
            _logger.info("queryIDFUnknownClaims - End of method - " + dateFormat.format(new Date()));

        } catch (Exception e) {
            _logger.error("queryIDFUnknownClaims - queryIDFUnknownClaims method failed with exception  " + e);
            throw e;
        }
    }

    protected void updateGridFSIDFUnknownClaims(String mqaJobId, MongoCRUDResult result, MongoCollection _gridFSCollection) throws Exception {

        if (_gridFSCollection == null)
            throw new IllegalArgumentException("Collection Instance Not Initialized");
        try {

            _logger.info("updateGridFSIDFUnknownClaims - Beginning of method - " + dateFormat.format(new Date()) + "\n IDF document mqaJobId. " + mqaJobId);
            claimFilter = new Document();
            claimDocs = new ArrayList<>();
            updateGridFSDocuments = new ArrayList<>();
            updateGridFSRequest = new Document();
            Document idf, gridFSFilter;
            //Prepare filter object to get the claimDocument from claims collection
            claimFilter.append("idf.idfJobId", mqaJobId)
                    .append("idf.identified", false);

            findClaims = _claimsCollection.find(claimFilter);
            findClaims.iterator().forEachRemaining(claimDocs::add);

            if (claimDocs.size() > 0) {
                for (Document claim : claimDocs) {
                    try {
                        idf = claim.getList("idf", Document.class).get(0);
                        gridFSFilter = new Document("metadata.mqaJobId", mqaJobId)
                                .append("metadata.fundingTraceNumber", idf.getString("fundingTraceNumber"))
                                .append("metadata.payerClaimNumber", idf.getString("payerClaimNumber"));

                        updateGridFSDocuments.add(new Document().append("filter", gridFSFilter)
                                .append("update", new Document("$set", new Document("metadata.unknown", "Yes"))));

                    } catch (Exception exc) {
                        result.addError(_logger, exc);
                        throw exc;
                    }
                }
            }
            if (!updateGridFSDocuments.isEmpty()) {
                updateGridFSRequest.put("documents", updateGridFSDocuments);
                //Update claims collection
                super.update(updateGridFSRequest, result, _gridFSCollection, "updateMany");
                _logger.debug("updateGridFSIDFUnknownClaims - fs.files collection update request object. " + updateGridFSRequest.toJson());
                _logger.info("updateGridFSIDFUnknownClaims - End of method - " + dateFormat.format(new Date()));
            }

            _logger.info("updateGridFSIDFUnknownClaims - End of method - " + dateFormat.format(new Date()));

        } catch (Exception e) {
            _logger.error("updateGridFSIDFUnknownClaims - updateGridFSIDFUnknownClaims method failed with exception  " + e);
            throw e;
        }
    }

    protected void updateGridFSIDFPaymentValidation(String mqaJobId, MongoCRUDResult result, MongoCollection _gridFSCollection) throws Exception {

        if (_gridFSCollection == null)
            throw new IllegalArgumentException("Collection Instance Not Initialized");
        try {

            _logger.info("updateGridFSIDFPaymentValidation - Beginning of method - " + dateFormat.format(new Date()) + "\n IDF document mqaJobId. " + mqaJobId);
            claimFilter = new Document();
            claimDocs = new ArrayList<>();
            updateGridFSDocuments = new ArrayList<>();
            updateGridFSRequest = new Document();
            Document idf, gridFSFilter;
            //Prepare filter object to get the claimDocument from claims collection
            claimFilter.append("idf.idfJobId", mqaJobId)
                    .append("idf.paymentValid", false);

            findClaims = _claimsCollection.find(claimFilter);
            findClaims.iterator().forEachRemaining(claimDocs::add);

            // Set paymentValid as "Yes" for all the IDF records first.
            updateGridFSDocuments.add(new Document().append("filter", new Document("metadata.mqaJobId", mqaJobId))
                    .append("update", new Document("$set", new Document("metadata.paymentValid", "Yes"))));


            if (claimDocs.size() > 0) {
                for (Document claim : claimDocs) {
                    try {
                        idf = claim.getList("idf", Document.class).get(0);
                        gridFSFilter = new Document("metadata.mqaJobId", mqaJobId)
                                .append("metadata.fundingTraceNumber", idf.getString("fundingTraceNumber"))
                                .append("metadata.payerClaimNumber", idf.getString("payerClaimNumber"));

                        updateGridFSDocuments.add(new Document().append("filter", gridFSFilter)
                                .append("update", new Document("$set", new Document("metadata.paymentValid", "No"))));

                    } catch (Exception exc) {
                        result.addError(_logger, exc);
                        throw exc;
                    }
                }
            }
            if (!updateGridFSDocuments.isEmpty()) {
                updateGridFSRequest.put("documents", updateGridFSDocuments);
                //Update claims collection
                super.update(updateGridFSRequest, result, _gridFSCollection, "updateMany");
                _logger.info("updateGridFSIDFPaymentValidation - fs.files collection update request object. " + updateGridFSRequest.toJson());
            }

            _logger.info("updateGridFSIDFPaymentValidation - End of method - " + dateFormat.format(new Date()));

        } catch (Exception e) {
            _logger.error("updateGridFSIDFPaymentValidation - updateGridFSIDFPaymentValidation method failed with exception  " + e);
            throw e;
        }
    }

    protected void updateGridFSIDFIndicators(String mqaJobId, MongoCRUDResult result, MongoCollection _gridFSCollection) throws Exception {

        if (_gridFSCollection == null)
            throw new IllegalArgumentException("Collection Instance Not Initialized");
        try {

            _logger.info("updateGridFSIDFIndicators - Beginning of method - " + dateFormat.format(new Date()) + "\n IDF document mqaJobId. " + mqaJobId);
            claimDocs = new ArrayList<>();
            updateGridFSDocuments = new ArrayList<>();
            updateGridFSRequest = new Document();
            Document idf, gridFSFilter;
            AggregateIterable<Document> findClaims = _claimsCollection.aggregate(Arrays.asList(
                    Aggregates.match(elemMatch("idf", and(eq("mqaJobId", mqaJobId), eq("identified", true)))),
                    Aggregates.project(fields(
                            excludeId(),
                            include("eftkey", "mainframeDate", "parentCompany", "company", "companyId", "businessId", "subCompanyId", "claimNumber", "checkEFTNumber"),
                            computed("idf",
                                    eq("$filter",
                                            and(
                                                    eq("input", "$idf"),
                                                    eq("as", "idf"),
                                                    eq("cond",
                                                            Arrays.asList("$$idf.idfJobId", mqaJobId)
                                                    )
                                            )
                                    )
                            )
                            )
                    ),
                    Aggregates.project(fields(
                            include("eftkey",
                                    "mainframeDate",
                                    "parentCompany",
                                    "company",
                                    "companyId",
                                    "businessId",
                                    "subCompanyId",
                                    "claimNumber",
                                    "checkEFTNumber"),
                            computed("idf", eq("$slice", Arrays.asList("$idf", -1L, 1L)))
                            )
                    ),
                    Aggregates.match(and(Arrays.asList(exists("idf.reissueIndicator", true), eq("idf.reissueIndicator", true))))
                    )
            ).allowDiskUse(true);

            findClaims.iterator().forEachRemaining(claimDocs::add);

            if (claimDocs.size() > 0) {
                for (Document claim : claimDocs) {
                    try {
                        //_logger.debug("updateGridFSIDFIndicators - claim Document is "+claim.toJson());
                        idf = claim.getList("idf", Document.class).get(0);
                        if (idf != null) {
                            gridFSFilter = new Document("metadata.mqaJobId", mqaJobId)
                                    .append("metadata.fundingTraceNumber", idf.getString("fundingTraceNumber"))
                                    .append("metadata.payerClaimNumber", idf.getString("payerClaimNumber"));

                            if (idf.getBoolean("reissueIndicator") != null && idf.getBoolean("reissueIndicator").equals(Boolean.TRUE)) {
                                updateGridFSDocuments.add(new Document().append("filter", gridFSFilter)
                                        .append("update", new Document("$set", new Document("metadata.reissueIndicator", "Yes"))));
                            } else {
                                updateGridFSDocuments.add(new Document().append("filter", gridFSFilter)
                                        .append("update", new Document("$set", new Document("metadata.reissueIndicator", "No"))));
                            }

                        }
                    } catch (Exception exc) {
                        result.addError(_logger, exc);
                        throw exc;
                    }
                }
            }
            if (!updateGridFSDocuments.isEmpty()) {
                updateGridFSRequest.put("documents", updateGridFSDocuments);
                //Update claims collection
                super.update(updateGridFSRequest, result, _gridFSCollection, "updateMany");
                _logger.debug("updateGridFSIDFIndicators - fs.files collection update request object. " + updateGridFSRequest.toJson());
            }
            _logger.info("updateGridFSIDFIndicators - End of method - " + dateFormat.format(new Date()));
        } catch (Exception e) {
            _logger.error("updateGridFSIDFIndicators - updateGridFSIDFIndicators method failed with exception  " + e);
            throw e;
        }
    }

    protected void updateGridFSIDFFields(String mqaJobId, MongoCRUDResult result, MongoCollection _gridFSCollection) throws Exception {

        if (_gridFSCollection == null)
            throw new IllegalArgumentException("Collection Instance Not Initialized");
        try {

            _logger.info("updateGridFSIDFFields - Beginning of method - " + dateFormat.format(new Date()) + "\n IDF document mqaJobId. " + mqaJobId);
            claimDocs = new ArrayList<>();
            updateGridFSDocuments = new ArrayList<>();
            updateGridFSRequest = new Document();
            Document idf, gridFSFilter;
            String checkEFTNumber, x12835CreateDateTime;

            _logger.info("updateGridFSIDFFields - Start Aggregating the documents " + dateFormat.format(new Date()));
            AggregateIterable<Document> findClaims = _claimsCollection.aggregate(Arrays.asList(
                    Aggregates.match(elemMatch("idf", and(eq("mqaJobId", mqaJobId), eq("identified", true)))),
                    Aggregates.project(fields(
                            excludeId(),
                            include("eftkey", "mainframeDate", "parentCompany", "company", "companyId", "businessId", "subCompanyId", "claimNumber", "checkEFTNumber")
                            , computed("x12835CreateDateTime",
                                    eq("$dateToString",
                                            and(
                                                    eq("format", "%Y%m%d%H%M%S"),
                                                    eq("date", eq("$arrayElemAt", Arrays.asList("$x12835.createDateTime", 0L))))))
                            , computed("x12835ISA09",
                                    eq("$arrayElemAt", Arrays.asList("$x12835.isa09", 0L)))
                            , computed("idf",
                                    eq("$filter",
                                            and(
                                                    eq("input", "$idf"),
                                                    eq("as", "x"),
                                                    eq("cond",
                                                            Arrays.asList("$$x.idfJobId", mqaJobId)
                                                    )
                                            )
                                    )
                            )
                            )
                    ),
                    Aggregates.project(fields(
                            include("eftkey",
                                    "mainframeDate",
                                    "parentCompany",
                                    "company",
                                    "companyId",
                                    "businessId",
                                    "subCompanyId",
                                    "claimNumber",
                                    "checkEFTNumber",
                                    "x12835CreateDateTime",
                                    "x12835ISA09"),
                            computed("idf", eq("$slice", Arrays.asList("$idf", -1L, 1L)))
                            )
                    )
                    )
            ).allowDiskUse(true);

            _logger.info("updateGridFSIDFFields - End Aggregating the documents " + dateFormat.format(new Date()));

            findClaims.iterator().forEachRemaining(claimDocs::add);
            _logger.info("updateGridFSIDFFields - End adding result documents to List Object " + dateFormat.format(new Date()));

            _logger.info("updateGridFSIDFFields - Total documents in the result object. " + claimDocs.size());

            if (claimDocs.size() > 0) {
                for (Document claim : claimDocs) {
                    try {
                        _logger.debug("updateGridFSIDFFields - claim Document is " + claim.toJson());
                        idf = claim.getList("idf", Document.class).get(0);
                        checkEFTNumber = claim.getString("checkEFTNumber");
                        x12835CreateDateTime = claim.getString("x12835CreateDateTime");
                        if (idf != null) {
                            gridFSFilter = new Document("metadata.mqaJobId", mqaJobId)
                                    .append("metadata.fundingTraceNumber", idf.getString("fundingTraceNumber"))
                                    .append("metadata.payerClaimNumber", idf.getString("payerClaimNumber"));

                            updateGridFSDocuments.add(new Document().append("filter", gridFSFilter)
                                    .append("update", new Document("$set", new Document("metadata.checkEFTNumber", checkEFTNumber).append("metadata.x12835CreateDateTime", x12835CreateDateTime))));
                        }
                    } catch (Exception exc) {
                        result.addError(_logger, exc);
                        throw exc;
                    }
                }
            }
            _logger.info("updateGridFSIDFFields - Completed preparing the updateGridFSDocuments object for updates. " + dateFormat.format(new Date()));

            if (!updateGridFSDocuments.isEmpty()) {
                updateGridFSRequest.put("documents", updateGridFSDocuments);
                //Update claims collection
                super.update(updateGridFSRequest, result, _gridFSCollection, "updateMany");
                _logger.debug("updateGridFSIDFFields - fs.files collection update request object. " + updateGridFSRequest.toJson());
            }

            _logger.info("updateGridFSIDFFields - End of method " + dateFormat.format(new Date()));

        } catch (Exception e) {
            _logger.error("updateGridFSIDFFields - updateGridFSIDFFields method failed with exception  " + e);
            throw e;
        }
    }

    protected void getIDFResponseDocuments(Document request, MongoCRUDResult result, String operation) throws Exception {

        if (_claimsCollection == null)
            throw new IllegalArgumentException("Claims collection name was not mentioned in <mqaCRUDTable> field. " +
                    "This should be combination of <BalancingCollection>|<ClaimsCollection>");
        if (_balancingCollection == null)
            throw new IllegalArgumentException("Balancing collection name was not mentioned in <mqaCRUDTable> field. " +
                    "This should be combination of <BalancingCollection>|<ClaimsCollection>");
        try {

            Document claimFilter = request.get("filter", Document.class);
            _logger.info("getIDFResponseDocuments - Beginning of method - " + dateFormat.format(new Date()) + "\n Received Filter object. " + request.toJson());

            String idfJobId = claimFilter.getString("idf.mqaJobId");
            _logger.info("getIDFResponseDocuments - idfJobId. " + idfJobId);

            //Build filter for status updates
            Document claimStatusFilter = new Document();
            claimStatusFilter.append("idf.idfJobId",idfJobId);
            claimStatusFilter.append("$or", Arrays.asList(
                    new Document()
                            .append("$and", Arrays.asList(
                                    new Document()
                                            .append("finalPaymentMethod", "ACH"),
                                    new Document()
                                            .append("finalPaymentStatus", "SETTLED")
                                    )
                            ),
                    new Document()
                            .append("$and", Arrays.asList(
                                    new Document()
                                            .append("finalPaymentMethod", "NON"),
                                    new Document()
                                            .append("finalPaymentStatus", "PAID")
                                    )
                            ),
                    new Document()
                            .append("$and", Arrays.asList(
                                    new Document()
                                            .append("finalPaymentMethod", "VIRTUALCARD"),
                                    new Document()
                                            .append("finalPaymentStatus", "PAID")
                                    )
                            ),
                    new Document()
                            .append("$and", Arrays.asList(
                                    new Document()
                                            .append("finalPaymentMethod", "CHK"),
                                    new Document()
                                            .append("finalPaymentStatus", "PAID")
                                    )
                            )
                    )
            );

            Document claimResponse = new Document();
            claimResponse.append("response", new Document().append("responseDate", new Date()).append("mqaJobId", idfJobId));

            Document updateClaimsRequest = new Document();
            List<Document> updateClaimDocuments = new ArrayList<>();
            updateClaimDocuments.add(new Document().append("filter", claimStatusFilter)
                    .append("update",
                            new Document()
                                    .append("$addToSet", claimResponse
                                            .append("statusAudit", new Document(new Document("processingStatus", "Generate IDF Response").append("processedDate", new Date())
                                                    .append("mqaJobId", idfJobId))))
                                    .append("$set", new Document("processingStatus", "Generate IDF Response").append("processedDate", new Date()))));

            updateClaimsRequest.put("documents", updateClaimDocuments);

            _logger.info("getIDFResponseDocuments - Request object for claims collection update. " + updateClaimsRequest.toJson());

            //Update claims collection
            super.update(updateClaimsRequest, result, _claimsCollection, "updateMany");

            _logger.info("getIDFResponseDocuments - Start aggregation of claims documents - " + dateFormat.format(new Date()));

            AggregateIterable<Document> facetSummaries = _claimsCollection.aggregate(Arrays.asList(
                    Aggregates.match(Filters.eq("idf.mqaJobId", idfJobId)),
                    Aggregates.facet(new Facet("paymentMethodCounts",
                                    Aggregates.group(new Document("eftkey", "$eftkey")
                                                    .append("paymentMethod", "$finalPaymentMethod"), Accumulators.sum("claimCount", 1)
                                            , Accumulators.sum("claimAmount", "$idf.claimPayment")),
                                    Aggregates.project(new Document("_id", 0)
                                            .append("eftkey", "$_id.eftkey")
                                            .append("paymentMethod", "$_id.paymentMethod")
                                            .append("claimCount", 1)
                                            .append("claimAmount", 1)),
                                    Aggregates.group(new Document("eftkey", "$eftkey"),
                                            Accumulators.addToSet("paymentMethodCounts", new Document("paymentMethod", "$paymentMethod")
                                                    .append("claimCount", "$claimCount").append("claimAmount", "$claimAmount"))),
                                    Aggregates.project(new Document("_id", 0)
                                            .append("eftkey", "$_id.eftkey")
                                            .append("paymentMethodCounts", "$paymentMethodCounts"))),

                            new Facet("paymentStatusCounts",
                                    Aggregates.group(new Document("eftkey", "$eftkey")
                                                    .append("paymentStatus", "$finalPaymentStatus"), Accumulators.sum("claimCount", 1)
                                            , Accumulators.sum("claimAmount", "$idf.claimPayment")),
                                    Aggregates.project(new Document("_id", 0)
                                            .append("eftkey", "$_id.eftkey")
                                            .append("paymentStatus", "$_id.paymentStatus")
                                            .append("claimCount", 1)
                                            .append("claimAmount", 1)),
                                    Aggregates.group(new Document("eftkey", "$eftkey"),
                                            Accumulators.addToSet("paymentStatusCounts", new Document("paymentStatus", "$paymentStatus")
                                                    .append("claimCount", "$claimCount").append("claimAmount", "$claimAmount"))),
                                    Aggregates.project(new Document("_id", 0)
                                            .append("eftkey", "$_id.eftkey")
                                            .append("paymentStatusCounts", "$paymentStatusCounts"))))
            )).allowDiskUse(true);

            _logger.info("getIDFResponseDocuments - End aggregation of claims documents - " + dateFormat.format(new Date()));

            updateBalancingRequest = new Document();
            updateBalanceDocuments = new ArrayList<>();
            List<Document> paymentMethodFacet;
            List<Document> paymentStatusFacet;
            _logger.info("getIDFResponseDocuments - Started updating Balancing collection with summaries - " + dateFormat.format(new Date()));
            for (Document facet : facetSummaries) {
                try {

                    _logger.info("getIDFResponseDocuments - Facets Summaries documents - " + facet.toJson());

                    paymentMethodFacet = facet.getList("paymentMethodCounts", Document.class);
                    paymentStatusFacet = facet.getList("paymentStatusCounts", Document.class);
                    String eftkey = "";
                    for (Document methodDoc : paymentMethodFacet) {
                        eftkey = methodDoc.getString("eftkey");
                        if (eftkey != null) {
                            balanceFilter = new Document().append("eftkey", eftkey);
                            idfResponse = new Document();
                            idfResponse.append("idfJobId", idfJobId)
                                    .append("responseDate", new Date())
                                    .append("paymentMethodCounts", methodDoc.get("paymentMethodCounts"));

                            for (Document statusDoc : paymentStatusFacet) {
                                if (statusDoc.getString("eftkey") != null && statusDoc.getString("eftkey").equals(eftkey)) {
                                    idfResponse.append("paymentStatusCounts", statusDoc.get("paymentStatusCounts"));
                                    continue;
                                }
                            }

                            updateBalanceDocuments.add(new Document().append("filter", balanceFilter)
                                    .append("update",
                                            new Document()
                                                    .append("$addToSet", new Document("idfResponse", idfResponse))
                                                    .append("$set", new Document("status.processing", "Generate IDF Response")
                                                            .append("status.lastUpdated", new Date()))));
                        }
                    }

                    _logger.info("getIDFResponseDocuments - The result object to be added for eftkey " + eftkey + " is . " + idfResponse.toString());

                } catch (Exception exc) {
                    result.addError(_logger, exc);
                    throw exc;
                }
            }

            if (!updateBalanceDocuments.isEmpty()) {
                updateBalancingRequest.put("documents", updateBalanceDocuments);
                _logger.debug("getIDFResponseDocuments - Balancing collection update Request object. " + updateBalancingRequest.toJson());

                //Update balancing collection
                super.update(updateBalancingRequest, result, _balancingCollection, "updateMany");
                //read the claim Documents
                _logger.debug("getIDFResponseDocuments - Balancing collection update result object. " + result.toString());
            }
            _logger.info("getIDFResponseDocuments - End of updating Balancing collection with summaries - " + dateFormat.format(new Date()));
            _logger.info("getIDFResponseDocuments - Start aggregating of claims response documents - " + dateFormat.format(new Date()));

            AggregateIterable<Document> idfResponse = _claimsCollection.aggregate(Arrays.asList(
                    Aggregates.match(elemMatch("idf", and(eq("mqaJobId", idfJobId), ne("identified", false)))),
                    Aggregates.project(fields(
                            excludeId(),
                            include("eftkey", "mainframeDate", "parentCompany", "company", "companyId", "businessId", "subCompanyId", "claimNumber", "checkEFTNumber", "finalPaymentMethod", "finalPaymentStatus"),
                            computed("providerName", "$claimInfo.provider.name"),
                            computed("providerNPI", "$claimInfo.provider.npi"),
                            computed("patientLastName", "$claimInfo.patient.lastName"),
                            computed("patientFirstName", "$claimInfo.patient.firstName"),
                            computed("patientIdCode", "$claimInfo.patient.patientIdCode"),
                            computed("patientIdValue", "$claimInfo.patient.patientIdValue"),
                            computed("patientPolicyNumber", "$claimInfo.patient.policyNumber"),
                            computed("providerTIN", "$claimInfo.provider.tin"),
                            computed("acceptReject", eq("$arrayElemAt", Arrays.asList("$x12999.acceptReject", 0L))),
                            computed("idf", eq("$slice", Arrays.asList("$idf", -1L, 1L)))
                            )
                    )
                    )
            ).allowDiskUse(true);


            _logger.info("getIDFResponseDocuments - End aggregating of claims response documents - " + dateFormat.format(new Date()));
            List<Document> responseDocs = new ArrayList<>();
            /*FileWriter respWriter = new FileWriter("idfResponse.json");
            respWriter.write("{documents:[");
            for (Document respDoc : idfResponse) {
                respWriter.write(respDoc.toJson());
                respWriter.write(",");
                responseDocs.add(respDoc);
            }
            respWriter.write("]}");
            respWriter.close();*/

            _logger.info("getIDFResponseDocuments - Started adding result documents to List object responseDocs - " + dateFormat.format(new Date()));
            idfResponse.iterator().forEachRemaining(responseDocs::add);
            _logger.info("getIDFResponseDocuments - Completed adding result documents to List object responseDocs - " + dateFormat.format(new Date()));

            _logger.info("getIDFResponseDocuments - Number of documents returned. " + responseDocs.size());
            result.setDocuments(responseDocs);
            _logger.debug("getIDFResponseDocuments - result Object after read IDF response documents." + result.toString());
            _logger.info("getIDFResponseDocuments - End of method - " + dateFormat.format(new Date()));

            updatePaymentSummaryByIDF(request);

        } catch (Exception e) {
            _logger.error("getIDFResponseDocuments - getIDFResponseDocuments failed. Exception is " + e);
            throw e;
        }
    }

    protected void getVirtualCardReport(Document request, MongoCRUDResult result, String operation) throws Exception {
        try {

            _logger.info("getVirtualCardReport - Beginning of method - " + dateFormat.format(new Date()));

            if (_claimsCollection == null)
                throw new IllegalArgumentException("Claims collection name was not mentioned in <mqaCRUDTable> field. " +
                        "This should be combination of <BalancingCollection>|<ClaimsCollection>");
            if (_balancingCollection == null)
                throw new IllegalArgumentException("Balancing collection name was not mentioned in <mqaCRUDTable> field. " +
                        "This should be combination of <BalancingCollection>|<ClaimsCollection>");

            Document claimFilter = request.get("filter", Document.class);

            _logger.info("getVirtualCardReport - Received filter object. " + claimFilter.toJson());

            String reportDateStr = claimFilter.getString("reportDate");
            Date reportDate = new SimpleDateFormat("yyyyMMdd").parse(reportDateStr);

            Calendar c = Calendar.getInstance();
            c.setTime(reportDate);
            c.add(Calendar.DATE, -6);
            reportDate = c.getTime();

            Document filterCondition = claimFilter.getList("filter", Document.class).get(0);

            _logger.info("getVirtualCardReport - Report date.  - " + dateFormat.format(reportDate));
            _logger.info("getVirtualCardReport - Start aggregation of claims documents - " + dateFormat.format(new Date()));

            AggregateIterable<Document> idfReport = _claimsCollection.aggregate(Arrays.asList(
                    Aggregates.match(Filters.and(
                            Arrays.asList(gte("idf.idfDateTime", reportDate),

                                    Filters.and(
                                            Arrays.asList(
                                                    Filters.eq("idf.paymentMethod", filterCondition.getString("paymentMethod")),
                                                    in("idf.status", filterCondition.getString("status").split("\\|"))))
                            ))),
                    Aggregates.project(fields(
                            excludeId(),
                            include("eftkey", "mainframeDate", "company", "companyId", "businessId", "subCompanyId", "claimNumber", "checkEFTNumber", "finalPaymentMethod", "finalPaymentStatus"),
                            include("idf")
                            )
                    ),
                    Aggregates.unwind("$idf"),
                    Aggregates.match(Filters.and(
                            Arrays.asList(
                                    Filters.eq("idf.paymentMethod", filterCondition.getString("paymentMethod")),
                                    in("idf.status", filterCondition.getString("status").split("\\|"))))
                    ),
                    Aggregates.project(fields(
                            excludeId(),
                            include("eftkey", "mainframeDate", "company", "companyId", "businessId", "subCompanyId", "claimNumber", "checkEFTNumber", "finalPaymentMethod", "finalPaymentStatus"),
                            computed("trn02", "$idf.trn02"),
                            computed("fundingTraceNumber", "$idf.fundingTraceNumber"),
                            computed("status", "$idf.status"),
                            computed("receivedDateTime", eq("$dateToString", and(eq("format", "%Y%m%d"), eq("date", "$idf.receivedDateTime")))),
                            computed("claimPayment", "$idf.claimPayment"),
                            computed("paymentDate", "$idf.paymentDate"),
                            computed("idfDateTime", eq("$dateToString", and(eq("format", "%Y%m%d"), eq("date", "$idf.idfDateTime")))),
                            computed("paymentMethod", "$idf.paymentMethod"),
                            computed("policyNumber", "$idf.patient.patientIdValue"),
                            computed("hasReIssuePayment", "$idf.hasReIssuePayment"),
                            computed("reIssueIndicator", "$idf.reIssueIndicator")
                            )
                    )
                    )
            ).allowDiskUse(true);
            _logger.info("getVirtualCardReport - End aggregating of claims response documents - " + dateFormat.format(new Date()));
            List<Document> reportDocs = new ArrayList<>();

            /*FileWriter reportWriter = new FileWriter("idfReport.txt");
            reportWriter.write("documents:[");
            for (Document reportDoc : idfReport) {
                reportWriter.write(reportDoc.toJson());
                reportWriter.write(",");
                reportDocs.add(reportDoc);
            }
            reportWriter.write("]");
            reportWriter.close();
            */

            _logger.info("getVirtualCardReport - Started adding result documents to List object reportDocs - " + dateFormat.format(new Date()));
            idfReport.iterator().forEachRemaining(reportDocs::add);
            _logger.info("getVirtualCardReport - Completed adding result documents to List object reportDocs - " + dateFormat.format(new Date()));

            _logger.info("getVirtualCardReport - Number of documents returned. " + reportDocs.size());
            result.setDocuments(reportDocs);
            _logger.debug("getVirtualCardReport - result Object after read IDF report documents." + result.toString());
            _logger.info("getVirtualCardReport - End of method - " + dateFormat.format(new Date()));
        } catch (Exception e) {
            _logger.error("Exception occurred in getVirtualCardReport. " + e.getStackTrace());
            throw e;
        }
    }

    protected void getX12835ST02Sequence(Document request, MongoCRUDResult result, MongoCollection collection) throws Exception {

        _logger.info("getX12835ST02 - MQMessage request object. " + request.toJson());
        // check that we have a filter
        if (request == null || !request.containsKey("filter"))
            throw new IllegalArgumentException("No sequence name was specified");
        if (collection == null)
            throw new IllegalArgumentException("Collection Instance Not Initialized");

        // get the filter
        Document filter = request.get("filter", Document.class);
        String sequenceName = filter.getString("key");

        Bson filterCond = Filters.eq("key", sequenceName);
        Document updDoc = new Document();
        updDoc.append("$inc", new Document().append("value", 1L));
        Document target = (Document) collection.findOneAndUpdate(
                filterCond,
                updDoc,
                new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));

        _logger.info("getX12835ST02 - The updated sequence object. " + target.toJson());

        super.read(request, result, collection);

        if (result.getDocuments().size() > 0) {
            _logger.info("getX12835ST02 - Response -");
            for (Document document : result.getDocuments())
                _logger.info(document.toJson());
        } else
            _logger.info("getX12835ST02 - Response has 0 documents in it.");


        _logger.info("getX12835ST02 - End of method - " + dateFormat.format(new Date()));
    }

    protected void getIDFResponseReport(Document request, MongoCRUDResult result) throws Exception {

        if (_claimsCollection == null)
            throw new IllegalArgumentException("Claims collection name was not mentioned in <mqaCRUDTable> field. " +
                    "This should be combination of <BalancingCollection>|<ClaimsCollection>");
        if (_balancingCollection == null)
            throw new IllegalArgumentException("Balancing collection name was not mentioned in <mqaCRUDTable> field. " +
                    "This should be combination of <BalancingCollection>|<ClaimsCollection>");
        try {

            Document claimFilter = request.get("filter", Document.class);
            _logger.info("getIDFResponseReport - Beginning of method - " + dateFormat.format(new Date()) + "\n Received Filter object is " + request.toJson());

            String mqaJobId = claimFilter.getString("idf.mqaJobId");
            _logger.info("getIDFResponseReport - mqaJobId is " + mqaJobId);

            _logger.info("getIDFResponseReport - Start aggregating of claims response report documents - " + dateFormat.format(new Date()));

            AggregateIterable<Document> idfReport = _claimsCollection.aggregate(Arrays.asList(
                    Aggregates.match(elemMatch("idf", and(eq("mqaJobId", mqaJobId), ne("identified", false)))),
                    Aggregates.unwind("$idf"),
                    Aggregates.match(eq("idf.mqaJobId", mqaJobId)),
                    Aggregates.project(fields(
                            excludeId(),
                            include("eftkey", "mainframeDate", "parentCompany", "company", "companyId", "businessId", "subCompanyId", "claimNumber", "checkEFTNumber", "finalPaymentMethod", "finalPaymentStatus"),
                            computed("providerName", "$claimInfo.provider.name"),
                            computed("providerNPI", "$claimInfo.provider.npi"),
                            computed("patientLastName", "$claimInfo.patient.lastName"),
                            computed("patientFirstName", "$claimInfo.patient.firstName"),
                            computed("patientIdCode", "$claimInfo.patient.patientIdCode"),
                            computed("patientIdValue", "$claimInfo.patient.patientIdValue"),
                            computed("patientPolicyNumber", "$claimInfo.patient.policyNumber"),
                            computed("providerTIN", "$claimInfo.provider.tin"),
                            computed("acceptReject", eq("$arrayElemAt", Arrays.asList("$x12999.acceptReject", 0L))),
                            computed("x12835CreateDateTime",
                                    eq("$dateToString",
                                            and(
                                                    eq("format", "%Y%m%d%H%M%S"),
                                                    eq("date", eq("$arrayElemAt", Arrays.asList("$x12835.createDateTime", 0L)))))),

                            computed("idf.mqaJobId", "$idf.mqaJobId"),
                            computed("idf.trn02", "$idf.trn02"),
                            computed("idf.claimPayment", "$idf.claimPayment"),
                            computed("idf.disbursmentTraceNumber", "$idf.disbursmentTraceNumber"),
                            computed("idf.disbursmentEntity", "$idf.disbursmentEntity"),
                            computed("idf.paymentDate", "$idf.paymentDate"),
                            computed("idf.payee.lastName", "$idf.payee.lastName"),
                            computed("idf.payee.firstName", "$idf.payee.firstName"),
                            computed("idf.status", "$idf.status"),
                            computed("idf.transactionType", "$idf.transactionType"),
                            computed("idf.reIssueIndicator", "$idf.reIssueIndicator"),
                            computed("idf.paymentMethod", "$idf.paymentMethod"),
                            computed("idf.paymentValid", "$idf.paymentValid"),
                            computed("idf.identified", "$idf.identified")
                    ))
                    )

            ).allowDiskUse(true);
            _logger.info("getIDFResponseReport - End aggregating of claims response documents - " + dateFormat.format(new Date()));
            List<Document> reportDocs = new ArrayList<>();
            /*FileWriter reportWriter = new FileWriter("idfResponseReport.json");
            reportWriter.write("documents:[");
            for (Document reportDoc : idfReport) {
                reportWriter.write(reportDoc.toJson());
                reportWriter.write(",");
                reportDocs.add(reportDoc);
            }
            reportWriter.write("]");
            reportWriter.close();
            */

            _logger.info("getIDFResponseReport - Started adding result documents to List object responseDocs - " + dateFormat.format(new Date()));
            idfReport.iterator().forEachRemaining(reportDocs::add);
            _logger.info("getIDFResponseReport - Completed adding result documents to List object responseDocs - " + dateFormat.format(new Date()));

            _logger.info("getIDFResponseReport - Number of documents returned." + reportDocs.size());
            result.setDocuments(reportDocs);
            _logger.debug("getIDFResponseReport - result Object after read IDF response documents." + result.toString());
            _logger.info("getIDFResponseReport - End of method - " + dateFormat.format(new Date()));

            _gridFSCollection = getCollectionByName("fs.files");
            //updateGridFSIDFFields(mqaJobId, result, _gridFSCollection);

        } catch (Exception e) {
            _logger.error("getIDFResponseReport - getIDFResponseReport failed. Exception is " + e);
            throw e;
        }
    }

    protected void updatePaymentSummary(String eftkey) throws Exception {
        // check that we have a filter
        if (eftkey == null)
            throw new IllegalArgumentException("eftkey is not sent as parameter to the method updatePaymentSummary");

        try {
            _logger.info("updatePaymentSummary - Beginning of method for eftkey - " + eftkey + ". " + dateFormat.format(new Date()));

            //<editor-fold desc="Aggregate Pipeline to generate PaymentSummary Document">
            List<? extends Bson> pipeline = Arrays.asList(
                    new Document()
                            .append("$match", new Document()
                                    .append("eftkey", eftkey)
                            ),
                    new Document()
                            .append("$facet", new Document()
                                    //<editor-fold desc="Facet control">
                                    .append("control", Arrays.asList(
                                            new Document()
                                                    .append("$project", new Document()
                                                            .append("eftkey", 1.0)
                                                            .append("parentCompany", 1.0)
                                                            .append("company", 1.0)
                                                            .append("companyId", 1.0)
                                                            .append("createDateTime", new Date())
                                                            .append("control", 1.0)
                                                    )
                                            )
                                    )
                                    //</editor-fold>
                                    //<editor-fold desc="Facet status">
                                    .append("status", Arrays.asList(
                                            new Document()
                                                    .append("$project", new Document()
                                                            .append("status", 1.0)
                                                    )
                                            )
                                    )
                                    //</editor-fold>
                                    //<editor-fold desc="Facet request">
                                    .append("request", Arrays.asList(
                                            new Document()
                                                    .append("$unwind", new Document()
                                                            .append("path", "$data")
                                                    ),
                                            new Document()
                                                    .append("$group", new Document()
                                                            .append("_id", new Document()
                                                                    .append("eftkey", "$eftkey")
                                                                    .append("companyId", "$data.companyId")
                                                                    .append("claimCount", "$data.fileName")
                                                                    .append("claimCount", "$data.claimCount")
                                                                    .append("claimAmount", "$data.claimAmount")
                                                                    .append("serviceLineCount", "$data.serviceLineCount")
                                                            )
                                                            .append("totalClaimCount", new Document()
                                                                    .append("$sum", 1.0)
                                                            )
                                                            .append("documents", new Document()
                                                                    .append("$addToSet", "$data")
                                                            )
                                                            .append("files", new Document()
                                                                    .append("$sum", 1.0)
                                                            )
                                                    ),
                                            new Document()
                                                    .append("$project", new Document()
                                                            .append("data", new Document()
                                                                    .append("$arrayElemAt", Arrays.asList(
                                                                            "$documents",
                                                                            0.0
                                                                            )
                                                                    )
                                                            )
                                                            .append("_id", 1.0)
                                                    ),
                                            new Document()
                                                    .append("$group", new Document()
                                                            .append("_id", "$_id.eftkey")
                                                            .append("totalClaimCount", new Document()
                                                                    .append("$sum", "$data.claimCount")
                                                            )
                                                            .append("totalClaimAmount", new Document()
                                                                    .append("$sum", "$data.claimAmount")
                                                            )
                                                            .append("totalServiceLineCount", new Document()
                                                                    .append("$sum", "$data.serviceLineCount")
                                                            )
                                                            .append("preEditClaimCount", new Document()
                                                                    .append("$sum", "$data.preedit_error.claimCount")
                                                            )
                                                            .append("preEditClaimAmount", new Document()
                                                                    .append("$sum", "$data.preedit_error.claimAmount")
                                                            )
                                                    ),
                                            new Document()
                                                    .append("$project", new Document()
                                                            .append("_id", 0.0)
                                                            .append("totalClaimCount", 1.0)
                                                            .append("totalClaimAmount", 1.0)
                                                            .append("totalServiceLineCount", 1.0)
                                                            .append("preEditClaimCount", 1.0)
                                                            .append("preEditClaimAmount", 1.0)
                                                    )
                                            )
                                    )
                                    //</editor-fold>
                                    //<editor-fold desc="Facet x12835">
                                    .append("x12835", Arrays.asList(
                                            new Document()
                                                    .append("$unwind", new Document()
                                                            .append("path", "$x12835")
                                                    ),
                                            new Document()
                                                    .append("$group", new Document()
                                                            .append("_id", new Document()
                                                                    .append("eftkey", "$eftkey")
                                                                    .append("gs06", "$x12835.gs06")
                                                                    .append("subCompanyId", "$x12835.subCompanyId")
                                                                    .append("claimCount", "$x12835.claimCount")
                                                                    .append("claimAmount", "$x12835.claimAmount")
                                                            )
                                                            .append("totalClaimCount", new Document()
                                                                    .append("$sum", 1.0)
                                                            )
                                                            .append("documents", new Document()
                                                                    .append("$addToSet", "$x12835")
                                                            )
                                                    ),
                                            new Document()
                                                    .append("$project", new Document()
                                                            .append("x12835", new Document()
                                                                    .append("$arrayElemAt", Arrays.asList(
                                                                            "$documents",
                                                                            0.0
                                                                            )
                                                                    )
                                                            )
                                                            .append("_id", 1.0)
                                                    ),
                                            new Document()
                                                    .append("$group", new Document()
                                                            .append("_id", "$_id.eftkey")
                                                            .append("x12835FilesCount", new Document()
                                                                    .append("$sum", 1.0)
                                                            )
                                                            .append("totalClaimCount", new Document()
                                                                    .append("$sum", "$x12835.claimCount")
                                                            )
                                                            .append("totalClaimAmount", new Document()
                                                                    .append("$sum", "$x12835.claimAmount")
                                                            )
                                                            .append("totalClaimCountPaid", new Document()
                                                                    .append("$sum", "$x12835.claimCountPaid")
                                                            )
                                                            .append("totalClaimCountNonPaid", new Document()
                                                                    .append("$sum", "$x12835.claimCountNonPaid")
                                                            )
                                                            .append("totalTransactionCount", new Document()
                                                                    .append("$sum", "$x12835.transactionCount")
                                                            )
                                                            .append("totalTransactionAmount", new Document()
                                                                    .append("$sum", "$x12835.transactionAmount")
                                                            )
                                                            .append("totalTransactionCountPaid", new Document()
                                                                    .append("$sum", "$x12835.transactionCountPaid")
                                                            )
                                                            .append("totalTransactionCountNonPaid", new Document()
                                                                    .append("$sum", "$x12835.transactionCountNonPaid")
                                                            )
                                                            .append("nonComplainceClaimCount", new Document()
                                                                    .append("$sum", "$x12835.noncompliance.claimCount")
                                                            )
                                                            .append("nonComplainceClaimAmount", new Document()
                                                                    .append("$sum", "$x12835.noncompliance.claimAmount")
                                                            )
                                                    ),
                                            new Document()
                                                    .append("$project", new Document()
                                                            .append("_id", 0.0)
                                                            .append("x12835FilesCount", 1.0)
                                                            .append("totalClaimCount", 1.0)
                                                            .append("totalClaimAmount", 1.0)
                                                            .append("totalClaimCountPaid", 1.0)
                                                            .append("totalClaimCountNonPaid", 1.0)
                                                            .append("totalTransactionCount", 1.0)
                                                            .append("totalTransactionAmount", 1.0)
                                                            .append("totalTransactionCountPaid", 1.0)
                                                            .append("totalTransactionCountNonPaid", 1.0)
                                                            .append("nonComplainceClaimCount", 1.0)
                                                            .append("nonComplainceClaimAmount", 1.0)
                                                    )
                                            )
                                    )
                                    //</editor-fold>
                                    //<editor-fold desc="Facet x12999">
                                    .append("x12999", Arrays.asList(
                                            new Document()
                                                    .append("$unwind", new Document()
                                                            .append("path", "$x12999")
                                                    ),
                                            new Document()
                                                    .append("$group", new Document()
                                                            .append("_id", new Document()
                                                                    .append("eftkey", "$eftkey")
                                                                    .append("gs06", "$x12999.originalGS06")
                                                                    .append("subCompanyId", "$x12999.subCompanyId")
                                                                    .append("transactionsIncluded", "$x12999.transactionsIncluded")
                                                                    .append("transactionsReceived", "$x12999.transactionsReceived")
                                                                    .append("transactionsAccepted", "$x12999.transactionsAccepted")
                                                            )
                                                            .append("x12999FilesCount", new Document()
                                                                    .append("$sum", 1.0)
                                                            )
                                                            .append("documents", new Document()
                                                                    .append("$addToSet", "$x12999")
                                                            )
                                                    ),
                                            new Document()
                                                    .append("$project", new Document()
                                                            .append("_id", 1.0)
                                                            .append("x12999FilesCount", 1.0)
                                                            .append("x12999", new Document()
                                                                    .append("$arrayElemAt", Arrays.asList(
                                                                            "$documents",
                                                                            0.0
                                                                            )
                                                                    )
                                                            )
                                                    ),
                                            new Document()
                                                    .append("$group", new Document()
                                                            .append("_id", "$_id.eftkey")
                                                            .append("totalTransactionsIncluded", new Document()
                                                                    .append("$sum", "$x12999.transactionsIncluded")
                                                            )
                                                            .append("totalTransactionsReceived", new Document()
                                                                    .append("$sum", "$x12999.transactionsReceived")
                                                            )
                                                            .append("totalTransactionsAccepted", new Document()
                                                                    .append("$sum", "$x12999.transactionsAccepted")
                                                            )
                                                            .append("totalTransactionsRejected", new Document()
                                                                    .append("$sum", "$x12999.rejected.transactionCount")
                                                            )
                                                            .append("totalRejectedClaimCount", new Document()
                                                                    .append("$sum", "$x12999.rejected.claimCount")
                                                            )
                                                            .append("x12999FilesCount", new Document()
                                                                    .append("$sum", "$x12999FilesCount")
                                                            )
                                                    ),
                                            new Document()
                                                    .append("$project", new Document()
                                                            .append("_id", 0.0)
                                                            .append("x12999FilesCount", 1.0)
                                                            .append("totalTransactionsIncluded", 1.0)
                                                            .append("totalTransactionsReceived", 1.0)
                                                            .append("totalTransactionsAccepted", 1.0)
                                                            .append("totalTransactionsRejected", 1.0)
                                                            .append("totalRejectedClaimCount", 1.0)
                                                    )
                                            )
                                    )
                                    //</editor-fold>
                                    //<editor-fold desc="Facet idfToBeReceived">
                                    .append("idfToBeReceived", Arrays.asList(
                                            new Document()
                                                    .append("$lookup", new Document()
                                                            .append("from", "claims")
                                                            .append("let", new Document()
                                                                    .append("eftkey", "$eftkey")
                                                            )
                                                            .append("pipeline", Arrays.asList(
                                                                    new Document()
                                                                            .append("$match", new Document()
                                                                                    .append("$expr", new Document()
                                                                                            .append("$and", Arrays.asList(
                                                                                                    new Document()
                                                                                                            .append("$eq", Arrays.asList(
                                                                                                                    "$eftkey",
                                                                                                                    "$$eftkey"
                                                                                                                    )
                                                                                                            ),
                                                                                                    new Document()
                                                                                                            .append("$eq", Arrays.asList(
                                                                                                                    "$processingStatus",
                                                                                                                    "X12 999"
                                                                                                                    )
                                                                                                            ),
                                                                                                    new Document()
                                                                                                            .append("$eq", Arrays.asList(
                                                                                                                    "$acceptReject",
                                                                                                                    "A"
                                                                                                                    )
                                                                                                            )
/*                                                                                                    new Document()
                                                                                                            .append("$gt", Arrays.asList(
                                                                                                                    "$x12999.acceptReject",
                                                                                                                    new BsonNull()
                                                                                                                    )
                                                                                                            )*/
                                                                                                    )
                                                                                            )
                                                                                    )
                                                                            ),
                                                                    new Document()
                                                                            .append("$group", new Document()
                                                                                    .append("_id", new Document()
                                                                                            .append("eftkey", "$eftkey")
                                                                                    )
                                                                                    .append("totalClaimCount", new Document()
                                                                                            .append("$sum", 1.0)
                                                                                    )
                                                                                    .append("totalClaimChargeAmount", new Document()
                                                                                            .append("$sum", "$claimInfo.claimTotalCharge")
                                                                                    )
                                                                                    .append("totalClaimPaidAmount", new Document()
                                                                                            .append("$sum", "$claimInfo.claimPaidAmount")
                                                                                    )
                                                                                    .append("idfFilesCount", new Document()
                                                                                            .append("$sum", 1))
                                                                            ),
                                                                    new Document()
                                                                            .append("$project", new Document()
                                                                                    .append("_id", 0.0)
                                                                                    .append("totalClaimCount", new Document()
                                                                                            .append("$ifNull", Arrays.asList(
                                                                                                    "$totalClaimCount",
                                                                                                    0.0
                                                                                                    )
                                                                                            )
                                                                                    )
                                                                                    .append("totalClaimChargeAmount", new Document()
                                                                                            .append("$ifNull", Arrays.asList(
                                                                                                    "$totalClaimChargeAmount",
                                                                                                    0.0
                                                                                                    )
                                                                                            )
                                                                                    )
                                                                                    .append("totalClaimPaidAmount", new Document()
                                                                                            .append("$ifNull", Arrays.asList(
                                                                                                    "$totalClaimPaidAmount",
                                                                                                    0.0
                                                                                                    )
                                                                                            )
                                                                                    )
                                                                                    .append("idfFilesCount", new Document()
                                                                                            .append("$ifNull", Arrays.asList(
                                                                                                    "$idfFilesCount",
                                                                                                    0.0
                                                                                                    )
                                                                                            )
                                                                                    )
                                                                            )
                                                                    )
                                                            )
                                                            .append("as", "idfToBeReceived")
                                                    ),
                                            new Document()
                                                    .append("$replaceRoot", new Document()
                                                            .append("newRoot", new Document()
                                                                    .append("$mergeObjects", "$idfToBeReceived")
                                                            )
                                                    )
                                    ))
                                    //</editor-fold>
                                    //<editor-fold desc="Facet idf">
                                    .append("idf", Arrays.asList(
                                            new Document()
                                                    .append("$lookup", new Document()
                                                            .append("from", "claims")
                                                            .append("let", new Document()
                                                                    .append("eftkey", "$eftkey")
                                                            )
                                                            .append("pipeline", Arrays.asList(
                                                                    new Document()
                                                                            .append("$match", new Document()
                                                                                    .append("$expr", new Document()
                                                                                            .append("$and", Arrays.asList(
                                                                                                    new Document()
                                                                                                            .append("$eq", Arrays.asList(
                                                                                                                    "$eftkey",
                                                                                                                    "$$eftkey"
                                                                                                                    )
                                                                                                            ),
                                                                                                    new Document()
                                                                                                            .append("$gt", Arrays.asList(
                                                                                                                    "$claimInfo.claimNumber",
                                                                                                                    new BsonNull()
                                                                                                                    )
                                                                                                            ),
                                                                                                    new Document()
                                                                                                            .append("$gt", Arrays.asList(
                                                                                                                    "$idf.payerClaimNumber",
                                                                                                                    new BsonNull()
                                                                                                                    )
                                                                                                            )
                                                                                                    )
                                                                                            )
                                                                                    )
                                                                            ),
                                                                    new Document()
                                                                            .append("$group", new Document()
                                                                                    .append("_id", new Document()
                                                                                            .append("eftkey", "$eftkey")
                                                                                            .append("claimNumber", "$claimInfo.claimNumber")
                                                                                            .append("checkEFTNumber", "$checkEFTNumber")
                                                                                    )
                                                                                    .append("totalClaimCount", new Document()
                                                                                            .append("$sum", 1.0)
                                                                                    )
                                                                                    .append("idf", new Document()
                                                                                            .append("$addToSet", new Document()
                                                                                                    .append("eftkey", "$eftkey")
                                                                                                    .append("mainframeCreateDate", 1.0)
                                                                                                    .append("mainframeDate", 1.0)
                                                                                                    .append("parentCompany", 1.0)
                                                                                                    .append("company", 1.0)
                                                                                                    .append("companyId", 1.0)
                                                                                                    .append("subCompanyId", 1.0)
                                                                                                    .append("processingStatus", 1.0)
                                                                                                    .append("claimNumber", "$claimInfo.claimNumber")
                                                                                                    .append("checkEFTNumber", 1.0)
                                                                                                    .append("trn02", 1.0)
                                                                                                    .append("finalPaymentMethod", 1.0)
                                                                                                    .append("finalPaymentStatus", 1.0)
                                                                                                    .append("acceptReject", 1.0)
                                                                                                    .append("claimTotalCharge", "$claimInfo.claimTotalCharge")
                                                                                                    .append("claimPaidAmount", "$claimInfo.claimPaidAmount")
                                                                                                    .append("idfFinal", new Document()
                                                                                                            .append("$arrayElemAt", Arrays.asList(
                                                                                                                    new Document()
                                                                                                                            .append("$slice", Arrays.asList(
                                                                                                                                    "$idf",
                                                                                                                                    -1.0
                                                                                                                                    )
                                                                                                                            ),
                                                                                                                    0.0
                                                                                                                    )
                                                                                                            )
                                                                                                    )
                                                                                                    .append("response", new Document()
                                                                                                            .append("$arrayElemAt", Arrays.asList(
                                                                                                                    new Document()
                                                                                                                            .append("$slice", Arrays.asList(
                                                                                                                                    "$response",
                                                                                                                                    -1.0
                                                                                                                                    )
                                                                                                                            ),
                                                                                                                    0.0
                                                                                                                    )
                                                                                                            )
                                                                                                    )
                                                                                            )
                                                                                    )
                                                                            ),
                                                                    new Document()
                                                                            .append("$project", new Document()
                                                                                    .append("_id", 0.0)
                                                                                    .append("idfDocs", new Document()
                                                                                            .append("$arrayElemAt", Arrays.asList(
                                                                                                    "$idf",
                                                                                                    0.0
                                                                                                    )
                                                                                            )
                                                                                    )
                                                                            ),
                                                                    new Document()
                                                                            .append("$replaceRoot", new Document()
                                                                                    .append("newRoot", new Document()
                                                                                            .append("$mergeObjects", "$idfDocs")
                                                                                    )
                                                                            ),
                                                                    new Document()
                                                                            .append("$facet", new Document()
                                                                                    .append("paymentMethodSummary", Arrays.asList(
                                                                                            new Document()
                                                                                                    .append("$group", new Document()
                                                                                                            .append("_id", new Document()
                                                                                                                    .append("eftkey", "$eftkey")
                                                                                                                    .append("paymentMethod", new Document()
                                                                                                                            .append("$ifNull", Arrays.asList(
                                                                                                                                    "$idfFinal.paymentMethod",
                                                                                                                                    "NCP"
                                                                                                                                    )
                                                                                                                            )
                                                                                                                    )
                                                                                                            )
                                                                                                            .append("totalClaimCount", new Document()
                                                                                                                    .append("$sum", 1.0)
                                                                                                            )
                                                                                                            .append("totalClaimChargedAmount", new Document()
                                                                                                                    .append("$sum", "$idfFinal.claimCharges")
                                                                                                            )
                                                                                                            .append("totalClaimPaymentAmount", new Document()
                                                                                                                    .append("$sum", "$idfFinal.claimPayment")
                                                                                                            )
                                                                                                    ),
                                                                                            new Document()
                                                                                                    .append("$group", new Document()
                                                                                                            .append("_id", new Document()
                                                                                                                    .append("eftkey", "$_id.eftkey")
                                                                                                            )
                                                                                                            .append("totalClaimCount", new Document()
                                                                                                                    .append("$sum", "$totalClaimCount")
                                                                                                            )
                                                                                                            .append("totalClaimChargedAmount", new Document()
                                                                                                                    .append("$sum", "$totalClaimChargedAmount")
                                                                                                            )
                                                                                                            .append("totalClaimPaymentAmount", new Document()
                                                                                                                    .append("$sum", "$totalClaimPaymentAmount")
                                                                                                            )
                                                                                                            .append("paymentMethod", new Document()
                                                                                                                    .append("$addToSet", new Document()
                                                                                                                            .append("paymentMethod", "$_id.paymentMethod")
                                                                                                                            .append("claimCount", new Document()
                                                                                                                                    .append("$sum", "$totalClaimCount")
                                                                                                                            )
                                                                                                                            .append("claimChargedAmount", new Document()
                                                                                                                                    .append("$sum", "$totalClaimChargedAmount")
                                                                                                                            )
                                                                                                                            .append("claimPaymentAmount", new Document()
                                                                                                                                    .append("$sum", "$totalClaimPaymentAmount")
                                                                                                                            )
                                                                                                                    )
                                                                                                            )
                                                                                                    ),
                                                                                            new Document()
                                                                                                    .append("$project", new Document()
                                                                                                            .append("_id", 0.0)
                                                                                                            .append("totalClaimCount", 1.0)
                                                                                                            .append("totalClaimChargedAmount", 1.0)
                                                                                                            .append("totalClaimPaymentAmount", 1.0)
                                                                                                            .append("paymentMethod", 1.0)
                                                                                                    )
                                                                                            )
                                                                                    )
                                                                                    .append("paymentStatusSummary", Arrays.asList(
                                                                                            new Document()
                                                                                                    .append("$group", new Document()
                                                                                                            .append("_id", new Document()
                                                                                                                    .append("eftkey", "$eftkey")
                                                                                                                    .append("paymentStatus", new Document()
                                                                                                                            .append("$ifNull", Arrays.asList(
                                                                                                                                    "$idfFinal.status",
                                                                                                                                    "NCP"
                                                                                                                                    )
                                                                                                                            )
                                                                                                                    )
                                                                                                            )
                                                                                                            .append("totalClaimCount", new Document()
                                                                                                                    .append("$sum", 1.0)
                                                                                                            )
                                                                                                            .append("totalClaimChargedAmount", new Document()
                                                                                                                    .append("$sum", "$idfFinal.claimCharges")
                                                                                                            )
                                                                                                            .append("totalClaimPaymentAmount", new Document()
                                                                                                                    .append("$sum", "$idfFinal.claimPayment")
                                                                                                            )
                                                                                                    ),
                                                                                            new Document()
                                                                                                    .append("$group", new Document()
                                                                                                            .append("_id", new Document()
                                                                                                                    .append("eftkey", "$_id.eftkey")
                                                                                                            )
                                                                                                            .append("totalClaimCount", new Document()
                                                                                                                    .append("$sum", "$totalClaimCount")
                                                                                                            )
                                                                                                            .append("totalClaimChargedAmount", new Document()
                                                                                                                    .append("$sum", "$totalClaimChargedAmount")
                                                                                                            )
                                                                                                            .append("totalClaimPaymentAmount", new Document()
                                                                                                                    .append("$sum", "$totalClaimPaymentAmount")
                                                                                                            )
                                                                                                            .append("paymentStatus", new Document()
                                                                                                                    .append("$addToSet", new Document()
                                                                                                                            .append("paymentStatus", "$_id.paymentStatus" )
                                                                                                                            .append("claimCount", new Document()
                                                                                                                                    .append("$sum", "$totalClaimCount")
                                                                                                                            )
                                                                                                                            .append("claimChargedAmount", new Document()
                                                                                                                                    .append("$sum", "$totalClaimChargedAmount")
                                                                                                                            )
                                                                                                                            .append("claimPaymentAmount", new Document()
                                                                                                                                    .append("$sum", "$totalClaimPaymentAmount")
                                                                                                                            )
                                                                                                                    )
                                                                                                            )
                                                                                                    ),
                                                                                            new Document()
                                                                                                    .append("$project", new Document()
                                                                                                            .append("_id", 0.0)
                                                                                                            .append("totalClaimCount", 1.0)
                                                                                                            .append("totalClaimChargedAmount", 1.0)
                                                                                                            .append("totalClaimPaymentAmount", 1.0)
                                                                                                            .append("paymentStatus", 1.0)
                                                                                                    )
                                                                                            )
                                                                                    )
                                                                                    .append("paymentReIssuesSummary", Arrays.asList(
                                                                                            new Document()
                                                                                                    .append("$match", new Document()
                                                                                                            .append("$expr", new Document()
                                                                                                                    .append("$eq", Arrays.asList(
                                                                                                                            new Document()
                                                                                                                                    .append("$ifNull", Arrays.asList(
                                                                                                                                            "$idfFinal.reissueIndicator",
                                                                                                                                            false
                                                                                                                                            )
                                                                                                                                    ),
                                                                                                                            true
                                                                                                                            )
                                                                                                                    )
                                                                                                            )
                                                                                                    ),
                                                                                            new Document()
                                                                                                    .append("$group", new Document()
                                                                                                            .append("_id", new BsonNull())
                                                                                                            .append("totalClaimCount", new Document()
                                                                                                                    .append("$sum", 1.0)
                                                                                                            )
                                                                                                            .append("totalClaimChargedAmount", new Document()
                                                                                                                    .append("$sum", "$idfFinal.claimCharges")
                                                                                                            )
                                                                                                            .append("totalClaimPaymentAmount", new Document()
                                                                                                                    .append("$sum", "$idfFinal.claimPayment")
                                                                                                            )
                                                                                                    ),
                                                                                            new Document()
                                                                                                    .append("$project", new Document()
                                                                                                            .append("_id", 0.0)
                                                                                                            .append("totalClaimCount", 1.0)
                                                                                                            .append("totalClaimChargedAmount", 1.0)
                                                                                                            .append("totalClaimPaymentAmount", 1.0)
                                                                                                    )
                                                                                            )
                                                                                    )
                                                                                    .append("paymentValidSummary", Arrays.asList(
                                                                                            new Document()
                                                                                                    .append("$match", new Document()
                                                                                                            .append("$expr", new Document()
                                                                                                                    .append("$eq", Arrays.asList(
                                                                                                                            new Document()
                                                                                                                                    .append("$ifNull", Arrays.asList(
                                                                                                                                            "$idfFinal.paymentValid",
                                                                                                                                            true
                                                                                                                                            )
                                                                                                                                    ),
                                                                                                                            false
                                                                                                                            )
                                                                                                                    )
                                                                                                            )
                                                                                                    ),
                                                                                            new Document()
                                                                                                    .append("$group", new Document()
                                                                                                            .append("_id", new BsonNull())
                                                                                                            .append("totalClaimCount", new Document()
                                                                                                                    .append("$sum", 1.0)
                                                                                                            )
                                                                                                            .append("totalClaimChargedAmount", new Document()
                                                                                                                    .append("$sum", "$idfFinal.claimCharges")
                                                                                                            )
                                                                                                            .append("totalClaimPaymentAmount", new Document()
                                                                                                                    .append("$sum", "$idfFinal.claimPayment")
                                                                                                            )
                                                                                                    ),
                                                                                            new Document()
                                                                                                    .append("$project", new Document()
                                                                                                            .append("_id", 0.0)
                                                                                                            .append("totalClaimCount", 1.0)
                                                                                                            .append("totalClaimChargedAmount", 1.0)
                                                                                                            .append("totalClaimPaymentAmount", 1.0)
                                                                                                    )
                                                                                            )
                                                                                    )
                                                                                    .append("paymentUnknownsSummary", Arrays.asList(
                                                                                            new Document()
                                                                                                    .append("$match", new Document()
                                                                                                            .append("$expr", new Document()
                                                                                                                    .append("$eq", Arrays.asList(
                                                                                                                            new Document()
                                                                                                                                    .append("$ifNull", Arrays.asList(
                                                                                                                                            "$idfFinal.identified",
                                                                                                                                            true
                                                                                                                                            )
                                                                                                                                    ),
                                                                                                                            false
                                                                                                                            )
                                                                                                                    )
                                                                                                            )
                                                                                                    ),
                                                                                            new Document()
                                                                                                    .append("$group", new Document()
                                                                                                            .append("_id", new BsonNull())
                                                                                                            .append("totalClaimCount", new Document()
                                                                                                                    .append("$sum", 1.0)
                                                                                                            )
                                                                                                            .append("totalClaimChargedAmount", new Document()
                                                                                                                    .append("$sum", "$idfFinal.claimCharges")
                                                                                                            )
                                                                                                            .append("totalClaimPaymentAmount", new Document()
                                                                                                                    .append("$sum", "$idfFinal.claimPayment")
                                                                                                            )
                                                                                                    ),
                                                                                            new Document()
                                                                                                    .append("$project", new Document()
                                                                                                            .append("_id", 0.0)
                                                                                                            .append("totalClaimCount", 1.0)
                                                                                                            .append("totalClaimChargedAmount", 1.0)
                                                                                                            .append("totalClaimPaymentAmount", 1.0)
                                                                                                    )
                                                                                            )
                                                                                    )
                                                                            ),
                                                                    new Document()
                                                                            .append("$project", new Document()
                                                                                    .append("paymentMethodSummary", new Document()
                                                                                            .append("$arrayElemAt", Arrays.asList(
                                                                                                    "$paymentMethodSummary",
                                                                                                    0.0
                                                                                                    )
                                                                                            )
                                                                                    )
                                                                                    .append("paymentStatusSummary", new Document()
                                                                                            .append("$arrayElemAt", Arrays.asList(
                                                                                                    "$paymentStatusSummary",
                                                                                                    0.0
                                                                                                    )
                                                                                            )
                                                                                    )
                                                                                    .append("paymentReIssuesSummary", new Document()
                                                                                            .append("$arrayElemAt", Arrays.asList(
                                                                                                    "$paymentReIssuesSummary",
                                                                                                    0.0
                                                                                                    )
                                                                                            )
                                                                                    )
                                                                                    .append("paymentValidSummary", new Document()
                                                                                            .append("$arrayElemAt", Arrays.asList(
                                                                                                    "$paymentValidSummary",
                                                                                                    0.0
                                                                                                    )
                                                                                            )
                                                                                    )
                                                                                    .append("paymentUnknownsSummary", new Document()
                                                                                            .append("$arrayElemAt", Arrays.asList(
                                                                                                    "$paymentUnknownsSummary",
                                                                                                    0.0
                                                                                                    )
                                                                                            )
                                                                                    )
                                                                            )
                                                                    )
                                                            )
                                                            .append("as", "idfClaims")
                                                    ),
                                            new Document()
                                                    .append("$project", new Document()
                                                            .append("_id", 0.0)
                                                            .append("idfFilesCount", new Document()
                                                                    .append("$cond", Arrays.asList(
                                                                            new Document()
                                                                                    .append("$isArray", Arrays.asList(
                                                                                            "$idf"
                                                                                            )
                                                                                    ),
                                                                            new Document()
                                                                                    .append("$size", "$idf"),
                                                                            0.0
                                                                            )
                                                                    )
                                                            )
                                                            .append("idfSummary", new Document()
                                                                    .append("$arrayElemAt", Arrays.asList(
                                                                            "$idfClaims",
                                                                            0.0
                                                                            )
                                                                    )
                                                            )
                                                    )
                                            )
                                    )
                                    //</editor-fold>
                                    //<editor-fold desc="Facet idfResponse">
                                    .append("idfResponse", Arrays.asList(
                                            new Document()
                                                    .append("$lookup", new Document()
                                                            .append("from", "claims")
                                                            .append("let", new Document()
                                                                    .append("eftkey", "$eftkey")
                                                            )
                                                            .append("pipeline", Arrays.asList(
                                                                    new Document()
                                                                            .append("$match", new Document()
                                                                                    .append("$expr", new Document()
                                                                                            .append("$and", Arrays.asList(
                                                                                                    new Document()
                                                                                                            .append("$eq", Arrays.asList(
                                                                                                                    "$eftkey",
                                                                                                                    "$$eftkey"
                                                                                                                    )
                                                                                                            ),
                                                                                                    new Document()
                                                                                                            .append("$gt", Arrays.asList(
                                                                                                                    "$claimInfo.claimNumber",
                                                                                                                    new BsonNull()
                                                                                                                    )
                                                                                                            ),
                                                                                                    new Document()
                                                                                                            .append("$gt", Arrays.asList(
                                                                                                                    "$idf.payerClaimNumber",
                                                                                                                    new BsonNull()
                                                                                                                    )
                                                                                                            )
                                                                                                    )
                                                                                            )
                                                                                    )
                                                                            ),
                                                                    new Document()
                                                                            .append("$group", new Document()
                                                                                    .append("_id", new Document()
                                                                                            .append("eftkey", "$eftkey")
                                                                                            .append("claimNumber", "$claimInfo.claimNumber")
                                                                                            .append("checkEFTNumber", "$checkEFTNumber")
                                                                                    )
                                                                                    .append("totalClaimCount", new Document()
                                                                                            .append("$sum", 1.0)
                                                                                    )
                                                                                    .append("idf", new Document()
                                                                                            .append("$addToSet", new Document()
                                                                                                    .append("eftkey", "$eftkey")
                                                                                                    .append("mainframeCreateDate", 1.0)
                                                                                                    .append("mainframeDate", 1.0)
                                                                                                    .append("parentCompany", 1.0)
                                                                                                    .append("company", 1.0)
                                                                                                    .append("companyId", 1.0)
                                                                                                    .append("subCompanyId", 1.0)
                                                                                                    .append("processingStatus", 1.0)
                                                                                                    .append("claimNumber", "$claimInfo.claimNumber")
                                                                                                    .append("checkEFTNumber", 1.0)
                                                                                                    .append("trn02", 1.0)
                                                                                                    .append("finalPaymentMethod", 1.0)
                                                                                                    .append("finalPaymentStatus", 1.0)
                                                                                                    .append("acceptReject", 1.0)
                                                                                                    .append("claimTotalCharge", "$claimInfo.claimTotalCharge")
                                                                                                    .append("claimPaidAmount", "$claimInfo.claimPaidAmount")
                                                                                                    .append("idfFinal", new Document()
                                                                                                            .append("$arrayElemAt", Arrays.asList(
                                                                                                                    new Document()
                                                                                                                            .append("$slice", Arrays.asList(
                                                                                                                                    "$idf",
                                                                                                                                    -1.0
                                                                                                                                    )
                                                                                                                            ),
                                                                                                                    0.0
                                                                                                                    )
                                                                                                            )
                                                                                                    )
                                                                                                    .append("response", new Document()
                                                                                                            .append("$arrayElemAt", Arrays.asList(
                                                                                                                    new Document()
                                                                                                                            .append("$slice", Arrays.asList(
                                                                                                                                    "$response",
                                                                                                                                    -1.0
                                                                                                                                    )
                                                                                                                            ),
                                                                                                                    0.0
                                                                                                                    )
                                                                                                            )
                                                                                                    )
                                                                                            )
                                                                                    )
                                                                            ),
                                                                    new Document()
                                                                            .append("$project", new Document()
                                                                                    .append("_id", 0.0)
                                                                                    .append("idfDocs", new Document()
                                                                                            .append("$arrayElemAt", Arrays.asList(
                                                                                                    "$idf",
                                                                                                    0.0
                                                                                                    )
                                                                                            )
                                                                                    )
                                                                            ),
                                                                    new Document()
                                                                            .append("$replaceRoot", new Document()
                                                                                    .append("newRoot", new Document()
                                                                                            .append("$mergeObjects", "$idfDocs")
                                                                                    )
                                                                            ),
                                                                    new Document()
                                                                            .append("$match", new Document()
                                                                                    .append("$expr", new Document()
                                                                                            .append("$or", Arrays.asList(
                                                                                                    new Document()
                                                                                                            .append("$and", Arrays.asList(
                                                                                                                    new Document()
                                                                                                                            .append("$eq", Arrays.asList(
                                                                                                                                    "$idfFinal.paymentMethod",
                                                                                                                                    "ACH"
                                                                                                                                    )
                                                                                                                            ),
                                                                                                                    new Document()
                                                                                                                            .append("$eq", Arrays.asList(
                                                                                                                                    "$idfFinal.status",
                                                                                                                                    "SETTLED"
                                                                                                                                    )
                                                                                                                            )
                                                                                                                    )
                                                                                                            ),
                                                                                                    new Document()
                                                                                                            .append("$and", Arrays.asList(
                                                                                                                    new Document()
                                                                                                                            .append("$eq", Arrays.asList(
                                                                                                                                    "$idfFinal.paymentMethod",
                                                                                                                                    "CHK"
                                                                                                                                    )
                                                                                                                            ),
                                                                                                                    new Document()
                                                                                                                            .append("$eq", Arrays.asList(
                                                                                                                                    "$idfFinal.status",
                                                                                                                                    "PAID"
                                                                                                                                    )
                                                                                                                            )
                                                                                                                    )
                                                                                                            ),
                                                                                                    new Document()
                                                                                                            .append("$and", Arrays.asList(
                                                                                                                    new Document()
                                                                                                                            .append("$eq", Arrays.asList(
                                                                                                                                    "$idfFinal.paymentMethod",
                                                                                                                                    "VIRTUALCARD"
                                                                                                                                    )
                                                                                                                            ),
                                                                                                                    new Document()
                                                                                                                            .append("$eq", Arrays.asList(
                                                                                                                                    "$idfFinal.status",
                                                                                                                                    "PAID"
                                                                                                                                    )
                                                                                                                            )
                                                                                                                    )
                                                                                                            ),
                                                                                                    new Document()
                                                                                                            .append("$and", Arrays.asList(
                                                                                                                    new Document()
                                                                                                                            .append("$eq", Arrays.asList(
                                                                                                                                    "$idfFinal.paymentMethod",
                                                                                                                                    "NON"
                                                                                                                                    )
                                                                                                                            ),
                                                                                                                    new Document()
                                                                                                                            .append("$eq", Arrays.asList(
                                                                                                                                    "$idfFinal.status",
                                                                                                                                    "PAID"
                                                                                                                                    )
                                                                                                                            )
                                                                                                                    )
                                                                                                            )
                                                                                                    )
                                                                                            )
                                                                                    )
                                                                            ),
                                                                    new Document()
                                                                            .append("$facet", new Document()
                                                                                    .append("paymentMethodSummary", Arrays.asList(
                                                                                            new Document()
                                                                                                    .append("$group", new Document()
                                                                                                            .append("_id", new Document()
                                                                                                                    .append("eftkey", "$eftkey")
                                                                                                                    .append("paymentMethod", "$idfFinal.paymentMethod")
                                                                                                            )
                                                                                                            .append("totalClaimCount", new Document()
                                                                                                                    .append("$sum", 1.0)
                                                                                                            )
                                                                                                            .append("totalClaimChargedAmount", new Document()
                                                                                                                    .append("$sum", "$idfFinal.claimCharges")
                                                                                                            )
                                                                                                            .append("totalClaimPaymentAmount", new Document()
                                                                                                                    .append("$sum", "$idfFinal.claimPayment")
                                                                                                            )
                                                                                                    ),
                                                                                            new Document()
                                                                                                    .append("$group", new Document()
                                                                                                            .append("_id", new Document()
                                                                                                                    .append("eftkey", "$_id.eftkey")
                                                                                                            )
                                                                                                            .append("totalClaimCount", new Document()
                                                                                                                    .append("$sum", "$totalClaimCount")
                                                                                                            )
                                                                                                            .append("totalClaimChargedAmount", new Document()
                                                                                                                    .append("$sum", "$totalClaimChargedAmount")
                                                                                                            )
                                                                                                            .append("totalClaimPaymentAmount", new Document()
                                                                                                                    .append("$sum", "$totalClaimPaymentAmount")
                                                                                                            )
                                                                                                            .append("paymentMethod", new Document()
                                                                                                                    .append("$addToSet", new Document()
                                                                                                                            .append("paymentMethod", "$_id.paymentMethod")
                                                                                                                            .append("claimCount", new Document()
                                                                                                                                    .append("$sum", "$totalClaimCount")
                                                                                                                            )
                                                                                                                            .append("claimChargedAmount", new Document()
                                                                                                                                    .append("$sum", "$totalClaimChargedAmount")
                                                                                                                            )
                                                                                                                            .append("claimPaymentAmount", new Document()
                                                                                                                                    .append("$sum", "$totalClaimPaymentAmount")
                                                                                                                            )
                                                                                                                    )
                                                                                                            )
                                                                                                    ),
                                                                                            new Document()
                                                                                                    .append("$project", new Document()
                                                                                                            .append("_id", 0.0)
                                                                                                            .append("totalClaimCount", 1.0)
                                                                                                            .append("totalClaimChargedAmount", 1.0)
                                                                                                            .append("totalClaimPaymentAmount", 1.0)
                                                                                                            .append("paymentMethod", 1.0)
                                                                                                    )
                                                                                            )
                                                                                    )
                                                                                    .append("paymentStatusSummary", Arrays.asList(
                                                                                            new Document()
                                                                                                    .append("$group", new Document()
                                                                                                            .append("_id", new Document()
                                                                                                                    .append("eftkey", "$eftkey")
                                                                                                                    .append("paymentStatus", "$idfFinal.status")
                                                                                                            )
                                                                                                            .append("totalClaimCount", new Document()
                                                                                                                    .append("$sum", 1.0)
                                                                                                            )
                                                                                                            .append("totalClaimChargedAmount", new Document()
                                                                                                                    .append("$sum", "$idfFinal.claimCharges")
                                                                                                            )
                                                                                                            .append("totalClaimPaymentAmount", new Document()
                                                                                                                    .append("$sum", "$idfFinal.claimPayment")
                                                                                                            )
                                                                                                    ),
                                                                                            new Document()
                                                                                                    .append("$group", new Document()
                                                                                                            .append("_id", new Document()
                                                                                                                    .append("eftkey", "$_id.eftkey")
                                                                                                            )
                                                                                                            .append("totalClaimCount", new Document()
                                                                                                                    .append("$sum", "$totalClaimCount")
                                                                                                            )
                                                                                                            .append("totalClaimChargedAmount", new Document()
                                                                                                                    .append("$sum", "$totalClaimChargedAmount")
                                                                                                            )
                                                                                                            .append("totalClaimPaymentAmount", new Document()
                                                                                                                    .append("$sum", "$totalClaimPaymentAmount")
                                                                                                            )
                                                                                                            .append("paymentStatus", new Document()
                                                                                                                    .append("$addToSet", new Document()
                                                                                                                            .append("paymentStatus", "$_id.paymentStatus")
                                                                                                                            .append("claimCount", new Document()
                                                                                                                                    .append("$sum", "$totalClaimCount")
                                                                                                                            )
                                                                                                                            .append("claimChargedAmount", new Document()
                                                                                                                                    .append("$sum", "$totalClaimChargedAmount")
                                                                                                                            )
                                                                                                                            .append("claimPaymentAmount", new Document()
                                                                                                                                    .append("$sum", "$totalClaimPaymentAmount")
                                                                                                                            )
                                                                                                                    )
                                                                                                            )
                                                                                                    ),
                                                                                            new Document()
                                                                                                    .append("$project", new Document()
                                                                                                            .append("_id", 0.0)
                                                                                                            .append("totalClaimCount", 1.0)
                                                                                                            .append("totalClaimChargedAmount", 1.0)
                                                                                                            .append("totalClaimPaymentAmount", 1.0)
                                                                                                            .append("paymentStatus", 1.0)
                                                                                                    )
                                                                                            )
                                                                                    )
                                                                                    .append("paymentsSummary", Arrays.asList(
                                                                                            new Document()
                                                                                                    .append("$group", new Document()
                                                                                                            .append("_id", new Document()
                                                                                                                    .append("eftkey", "$eftkey")
                                                                                                                    .append("paymentStatus", "$idfFinal.status")
                                                                                                                    .append("paymentMethod", "$idfFinal.paymentMethod")
                                                                                                            )
                                                                                                            .append("totalClaimCount", new Document()
                                                                                                                    .append("$sum", 1.0)
                                                                                                            )
                                                                                                            .append("totalClaimChargedAmount", new Document()
                                                                                                                    .append("$sum", "$idfFinal.claimCharges")
                                                                                                            )
                                                                                                            .append("totalClaimPaymentAmount", new Document()
                                                                                                                    .append("$sum", "$idfFinal.claimPayment")
                                                                                                            )
                                                                                                            .append("paymentDetail", new Document()
                                                                                                                    .append("$push", new Document()
                                                                                                                            .append("paymentMethod", "$idfFinal.paymentMethod")
                                                                                                                            .append("paymentStatus", "$idfFinal.status")
                                                                                                                            .append("claimCount", new Document()
                                                                                                                                    .append("$sum", 1.0)
                                                                                                                            )
                                                                                                                            .append("claimChargedAmount", new Document()
                                                                                                                                    .append("$sum", "$idfFinal.claimCharges")
                                                                                                                            )
                                                                                                                            .append("claimPaymentAmount", new Document()
                                                                                                                                    .append("$sum", "$idfFinal.claimPayment")
                                                                                                                            )
                                                                                                                    )
                                                                                                            )
                                                                                                    ),
                                                                                            new Document()
                                                                                                    .append("$group", new Document()
                                                                                                            .append("_id", new Document()
                                                                                                                    .append("eftkey", "$_id.eftkey")
                                                                                                            )
                                                                                                            .append("totalClaimCount", new Document()
                                                                                                                    .append("$sum", "$totalClaimCount")
                                                                                                            )
                                                                                                            .append("totalClaimChargedAmount", new Document()
                                                                                                                    .append("$sum", "$totalClaimChargedAmount")
                                                                                                            )
                                                                                                            .append("totalClaimPaymentAmount", new Document()
                                                                                                                    .append("$sum", "$totalClaimPaymentAmount")
                                                                                                            )
                                                                                                            .append("paymentMethod", new Document()
                                                                                                                    .append("$addToSet", new Document()
                                                                                                                            .append("paymentMethod", "$_id.paymentMethod")
                                                                                                                            .append("claimCount", new Document()
                                                                                                                                    .append("$sum", "$totalClaimCount")
                                                                                                                            )
                                                                                                                            .append("claimChargedAmount", new Document()
                                                                                                                                    .append("$sum", "$totalClaimChargedAmount")
                                                                                                                            )
                                                                                                                            .append("claimPaymentAmount", new Document()
                                                                                                                                    .append("$sum", "$totalClaimPaymentAmount")
                                                                                                                            )
                                                                                                                            .append("paymentStatus", new Document()
                                                                                                                                    .append("$reduce", new Document()
                                                                                                                                            .append("input", "$paymentDetail")
                                                                                                                                            .append("initialValue", new Document()
                                                                                                                                                    .append("paymentStatus", "$_id.paymentStatus")
                                                                                                                                                    .append("claimCount", 0.0)
                                                                                                                                                    .append("claimChargedAmount", 0.0)
                                                                                                                                                    .append("claimPaymentAmount", 0.0)
                                                                                                                                            )
                                                                                                                                            .append("in", new Document()
                                                                                                                                                    .append("paymentStatus", "$$value.paymentStatus")
                                                                                                                                                    .append("claimCount", new Document()
                                                                                                                                                            .append("$add", Arrays.asList(
                                                                                                                                                                    "$$value.claimCount",
                                                                                                                                                                    "$$this.claimCount"
                                                                                                                                                                    )
                                                                                                                                                            )
                                                                                                                                                    )
                                                                                                                                                    .append("claimChargedAmount", new Document()
                                                                                                                                                            .append("$add", Arrays.asList(
                                                                                                                                                                    "$$value.claimChargedAmount",
                                                                                                                                                                    "$$this.claimChargedAmount"
                                                                                                                                                                    )
                                                                                                                                                            )
                                                                                                                                                    )
                                                                                                                                                    .append("claimPaymentAmount", new Document()
                                                                                                                                                            .append("$add", Arrays.asList(
                                                                                                                                                                    "$$value.claimPaymentAmount",
                                                                                                                                                                    "$$this.claimPaymentAmount"
                                                                                                                                                                    )
                                                                                                                                                            )
                                                                                                                                                    )
                                                                                                                                            )
                                                                                                                                    )
                                                                                                                            )
                                                                                                                    )
                                                                                                            )
                                                                                                    ),
                                                                                            new Document()
                                                                                                    .append("$project", new Document()
                                                                                                            .append("_id", 0.0)
                                                                                                            .append("totalClaimCount", 1.0)
                                                                                                            .append("totalClaimChargedAmount", 1.0)
                                                                                                            .append("totalClaimPaymentAmount", 1.0)
                                                                                                            .append("paymentMethod", 1.0)
                                                                                                    )
                                                                                            )
                                                                                    )
                                                                            ),
                                                                    new Document()
                                                                            .append("$project", new Document()
                                                                                    .append("paymentMethodSummary", new Document()
                                                                                            .append("$arrayElemAt", Arrays.asList(
                                                                                                    "$paymentMethodSummary",
                                                                                                    0.0
                                                                                                    )
                                                                                            )
                                                                                    )
                                                                                    .append("paymentStatusSummary", new Document()
                                                                                            .append("$arrayElemAt", Arrays.asList(
                                                                                                    "$paymentStatusSummary",
                                                                                                    0.0
                                                                                                    )
                                                                                            )
                                                                                    )
                                                                                    .append("paymentsSummary", new Document()
                                                                                            .append("$arrayElemAt", Arrays.asList(
                                                                                                    "$paymentsSummary",
                                                                                                    0.0
                                                                                                    )
                                                                                            )
                                                                                    )
                                                                            )
                                                                    )
                                                            )
                                                            .append("as", "idfClaimsResponse")
                                                    ),
                                            new Document()
                                                    .append("$project", new Document()
                                                            .append("_id", 0.0)
                                                            .append("idfResponseFilesCount", new Document()
                                                                    .append("$cond", Arrays.asList(
                                                                            new Document()
                                                                                    .append("$isArray", Arrays.asList(
                                                                                            "$idfResponse"
                                                                                            )
                                                                                    ),
                                                                            new Document()
                                                                                    .append("$size", "$idfResponse"),
                                                                            0.0
                                                                            )
                                                                    )
                                                            )
                                                            .append("idfResponseSummary", new Document()
                                                                    .append("$arrayElemAt", Arrays.asList(
                                                                            "$idfClaimsResponse",
                                                                            0.0
                                                                            )
                                                                    )
                                                            )
                                                    )
                                            )
                                    )
                            ),
                    //</editor-fold>
                    //<editor-fold desc="Facet projections">
                    new Document()
                            .append("$project", new Document()
                                    .append("_id", new Document()
                                            .append("$arrayElemAt", Arrays.asList(
                                                    "$control._id",
                                                    0.0
                                                    )
                                            )
                                    )
                                    .append("eftkey", new Document()
                                            .append("$arrayElemAt", Arrays.asList(
                                                    "$control.eftkey",
                                                    0.0
                                                    )
                                            )
                                    )
                                    .append("parentCompany", new Document()
                                            .append("$arrayElemAt", Arrays.asList(
                                                    "$control.parentCompany",
                                                    0.0
                                                    )
                                            )
                                    )
                                    .append("company", new Document()
                                            .append("$arrayElemAt", Arrays.asList(
                                                    "$control.company",
                                                    0.0
                                                    )
                                            )
                                    )
                                    .append("companyId", new Document()
                                            .append("$arrayElemAt", Arrays.asList(
                                                    "$control.companyId",
                                                    0.0
                                                    )
                                            )
                                    )
                                    .append("createDateTime", new Document()
                                            .append("$arrayElemAt", Arrays.asList(
                                                    "$control.createDateTime",
                                                    0.0
                                                    )
                                            )
                                    )
                                    .append("status", new Document()
                                            .append("$arrayElemAt", Arrays.asList(
                                                    "$status.status",
                                                    0.0
                                                    )
                                            )
                                    )
                                    .append("control", new Document()
                                            .append("$arrayElemAt", Arrays.asList(
                                                    "$control.control",
                                                    0.0
                                                    )
                                            )
                                    )
                                    .append("request", new Document()
                                            .append("$arrayElemAt", Arrays.asList(
                                                    "$request",
                                                    0.0
                                                    )
                                            )
                                    )
                                    .append("x12835", new Document()
                                            .append("$arrayElemAt", Arrays.asList(
                                                    new Document()
                                                            .append("$filter", new Document()
                                                                    .append("input", "$x12835")
                                                                    .append("as", "x")
                                                                    .append("cond", new Document()
                                                                            .append("$gt", Arrays.asList(
                                                                                    "$$x.x12835FilesCount",
                                                                                    0.0
                                                                                    )
                                                                            )
                                                                    )
                                                            ),
                                                    0.0
                                                    )
                                            )
                                    )
                                    .append("x12999", new Document()
                                            .append("$arrayElemAt", Arrays.asList(
                                                    new Document()
                                                            .append("$filter", new Document()
                                                                    .append("input", "$x12999")
                                                                    .append("as", "x")
                                                                    .append("cond", new Document()
                                                                            .append("$gt", Arrays.asList(
                                                                                    "$$x.x12999FilesCount",
                                                                                    0.0
                                                                                    )
                                                                            )
                                                                    )
                                                            ),
                                                    0.0
                                                    )
                                            )
                                    )
                                    .append("idfToBeReceived", new Document()
                                            .append("$arrayElemAt", Arrays.asList(
                                                    new Document()
                                                            .append("$filter", new Document()
                                                                    .append("input", "$idfToBeReceived")
                                                                    .append("as", "x")
                                                                    .append("cond", new Document()
                                                                            .append("$gt", Arrays.asList(
                                                                                    "$$x.idfFilesCount",
                                                                                    0.0
                                                                                    )
                                                                            )
                                                                    )
                                                            ),
                                                    0.0
                                                    )
                                            )
                                    )
                                    .append("idf", new Document()
                                            .append("$arrayElemAt", Arrays.asList(
                                                    new Document()
                                                            .append("$filter", new Document()
                                                                    .append("input", "$idf")
                                                                    .append("as", "x")
                                                                    .append("cond", new Document()
                                                                            .append("$gt", Arrays.asList(
                                                                                    "$$x.idfFilesCount",
                                                                                    0.0
                                                                                    )
                                                                            )
                                                                    )
                                                            ),
                                                    0.0
                                                    )
                                            )
                                    )
                                    .append("idfResponse", new Document()
                                            .append("$arrayElemAt", Arrays.asList(
                                                    new Document()
                                                            .append("$filter", new Document()
                                                                    .append("input", "$idfResponse")
                                                                    .append("as", "x")
                                                                    .append("cond", new Document()
                                                                            .append("$gt", Arrays.asList(
                                                                                    "$$x.idfResponseFilesCount",
                                                                                    0.0
                                                                                    )
                                                                            )
                                                                    )
                                                            ),
                                                    0.0
                                                    )
                                            )
                                    )
                            ),
                    //</editor-fold>
                    //<editor-fold desc="Facet validations and populate defaults">
                    new Document()
                            .append("$addFields", new Document()
                                    .append("idfToBeReceived.totalClaimCount", new Document()
                                            .append("$ifNull", Arrays.asList(
                                                    "$idfToBeReceived.totalClaimCount",
                                                    0.0
                                                    )
                                            )
                                    )
                                    .append("idfToBeReceived.totalClaimChargeAmount", new Document()
                                            .append("$ifNull", Arrays.asList(
                                                    "$idfToBeReceived.totalClaimChargeAmount",
                                                    0.0
                                                    )
                                            )
                                    )
                                    .append("idfToBeReceived.totalClaimPaidAmount", new Document()
                                            .append("$ifNull", Arrays.asList(
                                                    "$idfToBeReceived.totalClaimPaidAmount",
                                                    0.0
                                                    )
                                            )
                                    )
                                    .append("idfToBeReceived.idfFilesCount", new Document()
                                            .append("$ifNull", Arrays.asList(
                                                    "$idfToBeReceived.idfFilesCount",
                                                    0.0
                                                    )
                                            )
                                    )
                            )
            );
            //</editor-fold>
            //</editor-fold>

            List<Document> resultArray = new ArrayList<>();
            AggregateIterable<Document> aggregateIterator = _balancingCollection.aggregate(pipeline)
                    .allowDiskUse(true);
            aggregateIterator.iterator().forEachRemaining(resultArray::add);

            List<Document> paymentSummaryDocs = new ArrayList<>();
            Document paymentSummaryRequest = new Document();
            MongoCRUDResult result = new MongoCRUDResult();
            _paymentSummaryCollection = getCollectionByName("paymentSummary");

            for (Document doc : resultArray) {
                _logger.info("updatePaymentSummary - Document is - " + doc.toJson());
                paymentSummaryDocs.add(doc);
            }
            if (paymentSummaryDocs.size() > 0) {

                paymentSummaryRequest.put("documents", paymentSummaryDocs);
                _logger.debug("updatePaymentSummary - paymentSummary collection update request object. " + paymentSummaryRequest.toJson());
                //Update balancing collection
                _logger.info("updatePaymentSummary - Beginning of paymentSummary Collection insert - " + dateFormat.format(new Date()));
                if (!paymentSummaryDocs.isEmpty())
                    super.insert(paymentSummaryRequest, result, _paymentSummaryCollection);

                _logger.info("updatePaymentSummary - End of method - " + dateFormat.format(new Date()));
            }

            _logger.info("updatePaymentSummary - End of method for eftkey - " + eftkey + ". " + dateFormat.format(new Date()));
        } catch (Exception e) {
            _logger.error("updatePaymentSummary - updatePaymentSummary method failed with exception  " + e);
            throw e;
        }
    }

    protected void updatePaymentSummaryByIDF(Document request) throws Exception {

        if (_claimsCollection == null)
            throw new IllegalArgumentException("Claims collection name was not mentioned in <mqaCRUDTable> field. " +
                    "This should be combination of <BalancingCollection>|<ClaimsCollection>");
        if (_balancingCollection == null)
            throw new IllegalArgumentException("Balancing collection name was not mentioned in <mqaCRUDTable> field. " +
                    "This should be combination of <BalancingCollection>|<ClaimsCollection>");
        try {

            Document idfFilter = request.get("filter", Document.class);
            _logger.info("updatePaymentSummaryByIDF - Beginning of method - " + dateFormat.format(new Date()) + "\n Received Filter object. " + request.toJson());

            String idfJobId = idfFilter.getString("idf.mqaJobId");

            if (idfJobId == null) {
                idfJobId = idfFilter.getString("idfmqaJobId");
            }
            if (idfJobId == null)
                throw new IllegalArgumentException("Filter document does not have any mqaJobId corresponding to IDF file. Please verify whether the" +
                        " calling ITX Map/Application is sending correct filter object. The filter object received is " + idfFilter.toJson());

            _logger.info("updatePaymentSummaryByIDF - idfJobId. " + idfJobId);
            AggregateIterable<Document> eftKeysIterator = _balancingCollection.aggregate(Arrays.asList(
                    Aggregates.match(eq("idf.mft.mqaJobId", idfJobId)),
                    Aggregates.group(new BsonNull(), Accumulators.addToSet("eftkeys", "$eftkey"))));

            // execute the find
            List<Document> eftKeysDoc = new ArrayList<>();
            eftKeysIterator.iterator().forEachRemaining(eftKeysDoc::add);

            _logger.info("updatePaymentSummaryByIDF - Total number of eftkeys that will be updated . " + eftKeysDoc.toArray().length);

            List<String> uniqueEftKeys = new ArrayList<>();
            if (eftKeysDoc.size() > 0)
                if (eftKeysDoc.size() > 0 && eftKeysDoc.get(0).get("eftkeys").getClass() == ArrayList.class)
                    uniqueEftKeys = eftKeysDoc.get(0).getList("eftkeys", String.class);

            if (uniqueEftKeys.size() > 0) {
                for (String eftkey : uniqueEftKeys) {
                    _logger.info("updatePaymentSummaryByIDF - Processing eftkey " + eftkey);
                    updatePaymentSummary(eftkey);
                }
                _logger.info("updatePaymentSummaryByIDF - End of method - " + dateFormat.format(new Date()));

            }
        } catch (
                Exception e)

        {
            _logger.error("updatePaymentSummaryByIDF - updatePaymentSummaryByIDF has an exception. Exception is " + e);
            throw e;
        }


    }

    protected void updatePaymentSummaryByEftkey(Document request) throws Exception {
        try {
            if (_balancingCollection == null)
                _balancingCollection = getCollectionByName("balancing");
            if (_claimsCollection == null)
                _claimsCollection = getCollectionByName("claims");
            Document eftkeyFilter = request.get("filter", Document.class);
            _logger.info("updatePaymentSummaryByEftkey - Beginning of method - " + dateFormat.format(new Date()) + "\n Received Filter object. " + request.toJson());

            String eftkey = eftkeyFilter.getString("eftkey");
            _logger.info("updatePaymentSummaryByEftkey - eftkey. " + eftkey);

            if (eftkey != "")
                updatePaymentSummary(eftkey);
            _logger.info("updatePaymentSummaryByEftkey - End of method - " + dateFormat.format(new Date()));

        } catch (
                Exception e)

        {
            _logger.error("updatePaymentSummaryByEftkey - updatePaymentSummaryByEftkey has an exception. Exception is " + e);
            throw e;
        }


    }
}