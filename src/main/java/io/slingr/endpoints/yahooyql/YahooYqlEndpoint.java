package io.slingr.endpoints.yahooyql;

import io.slingr.endpoints.HttpEndpoint;
import io.slingr.endpoints.exceptions.EndpointException;
import io.slingr.endpoints.exceptions.ErrorCode;
import io.slingr.endpoints.framework.annotations.EndpointFunction;
import io.slingr.endpoints.framework.annotations.EndpointWebService;
import io.slingr.endpoints.framework.annotations.SlingrEndpoint;
import io.slingr.endpoints.utils.Json;
import io.slingr.endpoints.ws.exchange.FunctionRequest;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>Yahoo YQL endpoint
 *
 * <p>Created by lefunes on 07/12/16.
 */
@SlingrEndpoint(name = "yahoo-yql")
public class YahooYqlEndpoint extends HttpEndpoint {
    private static final Logger logger = LoggerFactory.getLogger(YahooYqlEndpoint.class);

    private static final String YQL_URL = "https://query.yahooapis.com/v1/public/yql";
    private static final String ALL_TABLES_DATA_STORE = "store://datatables.org/alltableswithkeys";

    @Override
    public void endpointStarted() {
        httpService().setupDefaultParam("format", "json");
    }

    @Override
    public String getApiUri() {
        return YQL_URL;
    }

    @EndpointFunction
    public Json financeQuotes(Json request){
        try{
            final List<String> quotes = new ArrayList<>();
            if(request.contains("quotes")) {
                if (request.isList("quotes")) {
                    for (String quote : request.strings("quotes")) {
                        final String q = quote.replaceAll("\\s+", "").replaceAll("\\\"", "").trim();
                        if(StringUtils.isNotBlank(q)) {
                            quotes.add(String.format("\"%s\"", q));
                        }
                    }
                } else {
                    final String originalQuotes = request.string("quotes");
                    if(StringUtils.isNotBlank(originalQuotes)) {
                        for (String quote : originalQuotes.split(",")) {
                            final String q = quote.replaceAll("\\s+", "").replaceAll("\\\"", "").trim();
                            if(StringUtils.isNotBlank(q)) {
                                quotes.add(String.format("\"%s\"", q));
                            }
                        }
                    }
                }
            }

            if(quotes.isEmpty()){
                throw EndpointException.permanent(ErrorCode.ARGUMENT, "There is not quotes defined.");
            }

            final String query = String.format("select * from yahoo.finance.quotes where symbol in (%s)", StringUtils.join(quotes, ","));
            return executeQuery("financeQuotes", query, true);
        } catch (Exception ex){
            throw httpService().convertToEndpointException(ex);
        }
    }

    private Json executeQuery(final String queryName, String query, boolean allTables) throws Exception {
        logger.info(String.format("Executing YQL query [%s]: [%s]", queryName, query));
        final Json response = httpService().defaultGetRequest(yqlRequest(query, allTables));

        logger.info(String.format("Response to YQL query [%s]: [%s]", queryName, response));
        return response;
    }

    private static Json yqlRequest(String query, boolean allTables){
        Json params = Json.map().set("q", query);
        params = params.set("diagnostics", false);
        if(allTables){
            params = params.set("env", ALL_TABLES_DATA_STORE);
        }
        return Json.map().set("params", params);
    }

    @EndpointFunction(name = "post")
    public Json post(FunctionRequest request){
        // redirects the post to the get processor
        return defaultGetRequest(request);
    }

    @EndpointWebService(path = "/")
    public void disableWebhookProcessor(){
        // generic response for the HTTP methods that we want to disable the default webhook processor
        throw EndpointException.permanent(ErrorCode.CLIENT, "Webhook is not enabled").returnCode(400);
    }
}