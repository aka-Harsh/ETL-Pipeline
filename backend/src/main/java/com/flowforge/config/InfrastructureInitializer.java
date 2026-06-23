package com.flowforge.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Runs on startup to ensure all required cloud infrastructure exists:
 *   - S3 buckets (flowforge-data, flowforge-output)
 *   - DynamoDB table (FlowForgeExecutions) with GSI
 *   - Seed test data files in S3 (if not already present)
 *
 * This replaces the manual init-localstack.sh + seed-data.sh scripts.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class InfrastructureInitializer implements ApplicationRunner {

    private final DynamoDbClient dynamoDbClient;
    private final S3Client s3Client;

    private static final String TABLE_NAME = "FlowForgeExecutions";
    private static final String BUCKET_DATA = "flowforge-data";
    private static final String BUCKET_OUTPUT = "flowforge-output";

    @Value("${aws.region:eu-west-2}")
    private String region;

    @Override
    public void run(ApplicationArguments args) {
        log.info("=== InfrastructureInitializer: ensuring cloud resources exist ===");
        ensureS3Buckets();
        ensureDynamoDbTable();
        seedTestData();
        log.info("=== InfrastructureInitializer: done ===");
    }

    // ── S3 ───────────────────────────────────────────────────────────────────────

    private void ensureS3Buckets() {
        createBucketIfAbsent(BUCKET_DATA);
        createBucketIfAbsent(BUCKET_OUTPUT);
    }

    private void createBucketIfAbsent(String bucket) {
        try {
            s3Client.headBucket(HeadBucketRequest.builder().bucket(bucket).build());
            log.info("S3 bucket already exists: {}", bucket);
        } catch (S3Exception e) {
            try {
                if ("us-east-1".equals(region)) {
                    s3Client.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
                } else {
                    s3Client.createBucket(CreateBucketRequest.builder()
                            .bucket(bucket)
                            .createBucketConfiguration(CreateBucketConfiguration.builder()
                                    .locationConstraint(region)
                                    .build())
                            .build());
                }
                log.info("Created S3 bucket: {}", bucket);
            } catch (Exception ex) {
                log.warn("Could not create bucket {}: {}", bucket, ex.getMessage());
            }
        }
    }

    // ── DynamoDB ─────────────────────────────────────────────────────────────────

    private void ensureDynamoDbTable() {
        try {
            dynamoDbClient.describeTable(DescribeTableRequest.builder().tableName(TABLE_NAME).build());
            log.info("DynamoDB table already exists: {}", TABLE_NAME);
        } catch (ResourceNotFoundException e) {
            createDynamoDbTable();
        } catch (Exception e) {
            log.warn("Could not check DynamoDB table: {}", e.getMessage());
        }
    }

    private void createDynamoDbTable() {
        try {
            dynamoDbClient.createTable(CreateTableRequest.builder()
                    .tableName(TABLE_NAME)
                    .billingMode(BillingMode.PAY_PER_REQUEST)
                    .attributeDefinitions(
                            AttributeDefinition.builder().attributeName("pipelineId").attributeType(ScalarAttributeType.S).build(),
                            AttributeDefinition.builder().attributeName("executionTimestamp").attributeType(ScalarAttributeType.S).build(),
                            AttributeDefinition.builder().attributeName("executionId").attributeType(ScalarAttributeType.S).build()
                    )
                    .keySchema(
                            KeySchemaElement.builder().attributeName("pipelineId").keyType(KeyType.HASH).build(),
                            KeySchemaElement.builder().attributeName("executionTimestamp").keyType(KeyType.RANGE).build()
                    )
                    .globalSecondaryIndexes(GlobalSecondaryIndex.builder()
                            .indexName("executionId-index")
                            .keySchema(KeySchemaElement.builder().attributeName("executionId").keyType(KeyType.HASH).build())
                            .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                            .build())
                    .build());

            // Wait until table is active
            dynamoDbClient.waiter().waitUntilTableExists(
                    DescribeTableRequest.builder().tableName(TABLE_NAME).build());
            log.info("Created DynamoDB table: {}", TABLE_NAME);
        } catch (Exception e) {
            log.warn("Could not create DynamoDB table: {}", e.getMessage());
        }
    }

    // ── Seed data ─────────────────────────────────────────────────────────────────

    private void seedTestData() {
        // Seed at both input/ prefix and root so the AI can use either path
        seedFile(BUCKET_DATA, "input/sales.csv", "test-data/sales.csv");
        seedFile(BUCKET_DATA, "input/products.json", "test-data/products.json");
        seedFile(BUCKET_DATA, "sales.csv", "test-data/sales.csv");
        seedFile(BUCKET_DATA, "products.json", "test-data/products.json");
    }

    private void seedFile(String bucket, String s3Key, String classpathResource) {
        // Check if file already exists in S3
        try {
            s3Client.headObject(HeadObjectRequest.builder().bucket(bucket).key(s3Key).build());
            log.info("Test data already present: s3://{}/{}", bucket, s3Key);
            return;
        } catch (Exception ignored) {
            // File not present — upload it
        }

        // Try classpath first (embedded in JAR), then filesystem relative to working dir
        try {
            byte[] data = null;

            // Try loading from classpath (will work if test-data is in src/main/resources)
            try {
                ClassPathResource resource = new ClassPathResource(classpathResource);
                if (resource.exists()) {
                    try (InputStream is = resource.getInputStream()) {
                        data = is.readAllBytes();
                    }
                }
            } catch (Exception ignored) {}

            // Fallback: load from filesystem (mounted volume or working directory)
            if (data == null) {
                Path fsPath = Path.of("/app/" + classpathResource);
                if (Files.exists(fsPath)) {
                    data = Files.readAllBytes(fsPath);
                }
            }

            if (data != null) {
                s3Client.putObject(
                        PutObjectRequest.builder().bucket(bucket).key(s3Key).build(),
                        RequestBody.fromBytes(data));
                log.info("Seeded test data: s3://{}/{} ({} bytes)", bucket, s3Key, data.length);
            } else {
                log.warn("Test data file not found for seeding: {}", classpathResource);
            }
        } catch (Exception e) {
            log.warn("Could not seed test data {}: {}", classpathResource, e.getMessage());
        }
    }
}
