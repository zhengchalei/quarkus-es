package cc.amiya.quarkus;

import io.vertx.core.json.JsonObject;
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Path("/book")
public class BookResource {

    @Inject
    RestHighLevelClient restHighLevelClient;

    @PostConstruct
    public void createIndex() throws IOException {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("books");
        restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<Book> books(String name) throws IOException {
        SearchRequest searchRequest = new SearchRequest("books");
        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        return Arrays.stream(search.getHits().getHits())
                .map(SearchHit::getSourceAsString).map(JsonObject::new)
                .map(json -> json.mapTo(Book.class))
                .toList();
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    public Response addBook(@RequestBody Book book) throws IOException {
        IndexRequest request = new IndexRequest("books");
        request.id(book.id);
        request.source(JsonObject.mapFrom(book).toString(), XContentType.JSON);
        restHighLevelClient.index(request, RequestOptions.DEFAULT);
        return Response.ok().build();
    }

    private List<Book> search(String term, String match) throws IOException {
        SearchRequest searchRequest = new SearchRequest("books");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery(term, match));
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        List<Book> results = new ArrayList<>(hits.getHits().length);
        for (SearchHit hit : hits.getHits()) {
            String sourceAsString = hit.getSourceAsString();
            JsonObject json = new JsonObject(sourceAsString);
            results.add(json.mapTo(Book.class));
        }
        return results;
    }
}
