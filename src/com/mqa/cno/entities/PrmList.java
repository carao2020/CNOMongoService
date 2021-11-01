package com.mqa.cno.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.bson.codecs.pojo.annotations.BsonCreator;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;
import org.bson.codecs.pojo.annotations.BsonProperty;

import java.util.List;
import java.util.Map;

@BsonDiscriminator
public class PrmList
{
    public PrmList(){

    }

    @BsonCreator
    public PrmList(
            @BsonProperty("code") String code,
            @BsonProperty("description") String description,
            @BsonProperty("type") String type,
            @BsonProperty("schema_version") Integer schema_version,
            @BsonProperty("company_id") Integer company_id,
            @BsonProperty("summary") String summary,
            @BsonProperty("info") List<Infodoc> info){
        this.code=code;
        this.company_id=company_id;
        this.description=description;
        this.schema_version=schema_version;
        this.type=type;
        this.info=info;
        this.summary=summary;
    }
    @BsonProperty ("code")
    private String code;

    @BsonProperty("description")
    private String description;

    @BsonProperty("type")
    private String type;

    @BsonProperty("schema_version")
    private Integer schema_version;

    @BsonProperty("company_id")
    private Integer company_id;

    @BsonProperty("summary")
    private String summary;

    @BsonProperty("info")
    private List<Infodoc> info;

}
@BsonDiscriminator
class Infodoc
{
    @BsonCreator
    public Infodoc(
            @BsonProperty("key") String key,
            @BsonProperty("value") String value){
        this.key=key;
        this.value=value;
    }
    @BsonProperty("key")
    private String key;

    @BsonProperty("value")
    private String value;

}
