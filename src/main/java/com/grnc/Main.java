package com.grnc;

import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.*;
import java.util.List;
import java.util.stream.Collectors;

public class Main {

    private static final String ATHENA_DATABASE = System.getenv("ATHENA_DATABASE");
    private static final String ATHENA_TABLE = System.getenv("ATHENA_TABLE");
    private static final String ATHENA_OUTPUT_S3_FOLDER_PATH = System.getenv("ATHENA_OUTPUT_S3_FOLDER_PATH");

    public static void main(String[] args) throws InterruptedException {

        AthenaClientFactory factory = new AthenaClientFactory();
        AthenaClient athenaClient = factory.createClient();

        String queryExecutionId = submitAthenaQuery(athenaClient);

        waitForQueryToComplete(athenaClient, queryExecutionId);

        processResultRows(athenaClient, queryExecutionId);
    }

    private static String submitAthenaQuery(AthenaClient athenaClient) {
        final String SIMPLE_ATHENA_QUERY = String.format("SELECT * FROM %s limit 10;", ATHENA_TABLE);

        QueryExecutionContext queryExecutionContext = QueryExecutionContext.builder()
                .database(ATHENA_DATABASE).build();
        ResultConfiguration resultConfiguration = ResultConfiguration.builder()
                .outputLocation(ATHENA_OUTPUT_S3_FOLDER_PATH).build();
        StartQueryExecutionRequest startQueryExecutionRequest = StartQueryExecutionRequest.builder()
                .queryString(SIMPLE_ATHENA_QUERY)
                .queryExecutionContext(queryExecutionContext)
                .resultConfiguration(resultConfiguration).build();
        StartQueryExecutionResponse startQueryExecutionResponse = athenaClient.startQueryExecution(startQueryExecutionRequest);
        return startQueryExecutionResponse.queryExecutionId();
    }

    private static void waitForQueryToComplete(AthenaClient athenaClient, String queryExecutionId) throws InterruptedException {
        GetQueryExecutionRequest getQueryExecutionRequest = GetQueryExecutionRequest.builder()
                .queryExecutionId(queryExecutionId).build();

        GetQueryExecutionResponse getQueryExecutionResponse;
        boolean isQueryStillRunning = true;
        while (isQueryStillRunning) {
            getQueryExecutionResponse = athenaClient.getQueryExecution(getQueryExecutionRequest);
            String queryState = getQueryExecutionResponse.queryExecution().status().state().toString();
            if (queryState.equals(QueryExecutionState.FAILED.toString())) {
                throw new RuntimeException("Query Failed to run with Error Message: " + getQueryExecutionResponse
                        .queryExecution().status().stateChangeReason());
            } else if (queryState.equals(QueryExecutionState.CANCELLED.toString())) {
                throw new RuntimeException("Query was cancelled.");
            } else if (queryState.equals(QueryExecutionState.SUCCEEDED.toString())) {
                isQueryStillRunning = false;
            } else {
                // Sleep an amount of time before retrying again.
                Thread.sleep(1000);
            }
            System.out.println("Current Status is: " + queryState);
        }
    }

    /**
     * This code calls Athena and retrieves the results of a query.
     * The query must be in a completed state before the results can be retrieved and
     * paginated. The first row of results are the column headers.
     */
    private static void processResultRows(AthenaClient athenaClient, String queryExecutionId) {
        GetQueryResultsRequest getQueryResultsRequest = GetQueryResultsRequest.builder()
                // Max Results can be set but if its not set,
                // it will choose the maximum page size
                // As of the writing of this code, the maximum value is 1000
                // .withMaxResults(1000)
                .queryExecutionId(queryExecutionId).build();

        athenaClient
                .getQueryResultsPaginator(getQueryResultsRequest)
                .stream()
                .map(GetQueryResultsResponse::resultSet)
                .map(ResultSet::rows)
                .forEach(Main::processRow);
    }

    /**
     * Prints like a csv file content.
     */
    private static void processRow(List<Row> rowList) {
        rowList
                .stream()
                .map(row -> row.data()
                        .stream()
                        .map(Datum::varCharValue)
                        .collect(Collectors.joining(",")))
                .forEach(System.out::println);
    }

}
