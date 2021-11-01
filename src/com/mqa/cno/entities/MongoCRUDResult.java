package com.mqa.cno.entities;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteError;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MongoCRUDResult
{
    public MongoCRUDResult()
    {
        status = new MongoCRUDStatus();
        status.Errors = new ArrayList<>();
        status.ReturnCode = 0;
        summary = new HashMap<>();
    }

    @JsonProperty("status")
    private MongoCRUDStatus status;

    @JsonProperty("summary")
    private Map<String, Object> summary;

    @JsonProperty("documents")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private List<Document> documents;


    //<editor-fold desc="Methods">
    public void addSummary(String key, Object value)
    {
        if (summary == null)
            summary = new HashMap<>();
        summary.put(key, value);
    }

    public void addError(Logger _logger, Exception exc)
    {
        status.ReturnCode = 1;
        if (status.Errors == null)
            status.Errors = new ArrayList<>();
        if (exc instanceof MongoBulkWriteException)
        {
            MongoBulkWriteException writeException = (MongoBulkWriteException)exc;
            for(BulkWriteError error: writeException.getWriteErrors())
                status.Errors.add(String.format("Failed to write Index %i - %s", error.getIndex(), error.getMessage()));
            return;
        }
        status.Errors.add(exc.getMessage() + "\n" + exc.getStackTrace().toString());
        _logger.error("Error Encountered", exc);
    }

    public void setDocuments(List<Document> docs)
    {
        documents = docs;
    }
    //</editor-fold>

}
class MongoCRUDStatus
{
    public int ReturnCode;
    public List<String> Errors;
}
