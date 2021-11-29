import com.google.gson.Gson;
import com.mongodb.Block;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.client.*;
import com.mongodb.connection.ConnectionPoolSettings;
import org.bson.Document;
import com.mongodb.ConnectionString;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class app {
    public static void main(String[] args) {
        MongoCollection<Document> suppliers = createClient();
        latencyCheckRead(suppliers);
    }

    public static void usingConnectionString(){
        try {
            System.out.println("Starting method 1");
            MongoClient client = MongoClients.create("mongodb://mongoexp:p6AowPalsguPHqD8qrb0xo0igFqRmOJEqAg21iMRsHiHuEAAtJaSQYslZtSosPhG9vGi8cLp1UjYFanl7s3QrA==@mongoexp.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@mongoexp@");
            MongoDatabase database = client.getDatabase("suppliers");
            MongoCollection<Document> suppliers = database.getCollection("supplierdetails");
            for(int i =0; i<10;i++) {
                Date date = new Date();
                Document filter = new Document("guid","56c821cf-060c-4afe-a6ca-b50f597c495a");
                FindIterable<Document> documents = suppliers.find(filter);

                Date date2 = new Date();

                for (Document document : documents) {
                    System.out.println(document.toJson());
                }
                System.out.println(String.format("Time spent %d", date2.getTime() - date.getTime()));
            }
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
    }

    public static void latencyCheckRead(MongoCollection<Document> suppliers){
        try{
            System.out.println("Starting method 2");
            System.out.println(String.format("Number of records %s",suppliers.countDocuments()));
            int records = 10;
            List<String> guids = new Gson().fromJson("[\"56c821cf-060c-4afe-a6ca-b50f597c495a\",\"e8a2e031-c99c-4443-a5c5-dbf47ba4a4a9\",\"e8d6e41d-18fd-495c-8f23-d8417323f4d9\",\"390ca970-42a0-4684-949f-7407df177aed\",\"4df5e6f2-82ca-4f58-b94f-9c835849113b\",\"d6ce3e06-1092-4816-9e45-042aea8ef72a\",\"9a4fcce8-4fe4-4fbd-9893-d89b5dfe0999\",\"6440cf70-fa81-4d2f-8294-a85a2b98b56b\",\"c56da051-bb7d-4ba8-912e-2c5319175775\",\"5538e482-9f4e-468e-bf19-d95b783fae77\",\"56c821cf-060c-4afe-a6ca-b50f597c495a\",\"e8a2e031-c99c-4443-a5c5-dbf47ba4a4a9\",\"e8d6e41d-18fd-495c-8f23-d8417323f4d9\",\"390ca970-42a0-4684-949f-7407df177aed\",\"4df5e6f2-82ca-4f58-b94f-9c835849113b\",\"d6ce3e06-1092-4816-9e45-042aea8ef72a\",\"9a4fcce8-4fe4-4fbd-9893-d89b5dfe0999\",\"6440cf70-fa81-4d2f-8294-a85a2b98b56b\",\"c56da051-bb7d-4ba8-912e-2c5319175775\",\"5538e482-9f4e-468e-bf19-d95b783fae77\",\"56c821cf-060c-4afe-a6ca-b50f597c495a\",\"e8a2e031-c99c-4443-a5c5-dbf47ba4a4a9\",\"e8d6e41d-18fd-495c-8f23-d8417323f4d9\",\"390ca970-42a0-4684-949f-7407df177aed\",\"4df5e6f2-82ca-4f58-b94f-9c835849113b\",\"d6ce3e06-1092-4816-9e45-042aea8ef72a\",\"9a4fcce8-4fe4-4fbd-9893-d89b5dfe0999\",\"6440cf70-fa81-4d2f-8294-a85a2b98b56b\",\"c56da051-bb7d-4ba8-912e-2c5319175775\",\"5538e482-9f4e-468e-bf19-d95b783fae77\",\"56c821cf-060c-4afe-a6ca-b50f597c495a\",\"e8a2e031-c99c-4443-a5c5-dbf47ba4a4a9\",\"e8d6e41d-18fd-495c-8f23-d8417323f4d9\",\"390ca970-42a0-4684-949f-7407df177aed\",\"4df5e6f2-82ca-4f58-b94f-9c835849113b\",\"d6ce3e06-1092-4816-9e45-042aea8ef72a\",\"9a4fcce8-4fe4-4fbd-9893-d89b5dfe0999\",\"6440cf70-fa81-4d2f-8294-a85a2b98b56b\",\"c56da051-bb7d-4ba8-912e-2c5319175775\",\"5538e482-9f4e-468e-bf19-d95b783fae77\",\"56c821cf-060c-4afe-a6ca-b50f597c495a\",\"e8a2e031-c99c-4443-a5c5-dbf47ba4a4a9\",\"e8d6e41d-18fd-495c-8f23-d8417323f4d9\",\"390ca970-42a0-4684-949f-7407df177aed\",\"4df5e6f2-82ca-4f58-b94f-9c835849113b\",\"d6ce3e06-1092-4816-9e45-042aea8ef72a\",\"9a4fcce8-4fe4-4fbd-9893-d89b5dfe0999\",\"6440cf70-fa81-4d2f-8294-a85a2b98b56b\",\"c56da051-bb7d-4ba8-912e-2c5319175775\",\"5538e482-9f4e-468e-bf19-d95b783fae77\",\"56c821cf-060c-4afe-a6ca-b50f597c495a\",\"e8a2e031-c99c-4443-a5c5-dbf47ba4a4a9\",\"e8d6e41d-18fd-495c-8f23-d8417323f4d9\",\"390ca970-42a0-4684-949f-7407df177aed\",\"4df5e6f2-82ca-4f58-b94f-9c835849113b\",\"d6ce3e06-1092-4816-9e45-042aea8ef72a\",\"9a4fcce8-4fe4-4fbd-9893-d89b5dfe0999\",\"6440cf70-fa81-4d2f-8294-a85a2b98b56b\",\"c56da051-bb7d-4ba8-912e-2c5319175775\",\"5538e482-9f4e-468e-bf19-d95b783fae77\",\"56c821cf-060c-4afe-a6ca-b50f597c495a\",\"e8a2e031-c99c-4443-a5c5-dbf47ba4a4a9\",\"e8d6e41d-18fd-495c-8f23-d8417323f4d9\",\"390ca970-42a0-4684-949f-7407df177aed\",\"4df5e6f2-82ca-4f58-b94f-9c835849113b\",\"d6ce3e06-1092-4816-9e45-042aea8ef72a\",\"9a4fcce8-4fe4-4fbd-9893-d89b5dfe0999\",\"6440cf70-fa81-4d2f-8294-a85a2b98b56b\",\"c56da051-bb7d-4ba8-912e-2c5319175775\",\"5538e482-9f4e-468e-bf19-d95b783fae77\",\"56c821cf-060c-4afe-a6ca-b50f597c495a\",\"e8a2e031-c99c-4443-a5c5-dbf47ba4a4a9\",\"e8d6e41d-18fd-495c-8f23-d8417323f4d9\",\"390ca970-42a0-4684-949f-7407df177aed\",\"4df5e6f2-82ca-4f58-b94f-9c835849113b\",\"d6ce3e06-1092-4816-9e45-042aea8ef72a\",\"9a4fcce8-4fe4-4fbd-9893-d89b5dfe0999\",\"6440cf70-fa81-4d2f-8294-a85a2b98b56b\",\"c56da051-bb7d-4ba8-912e-2c5319175775\",\"5538e482-9f4e-468e-bf19-d95b783fae77\",\"56c821cf-060c-4afe-a6ca-b50f597c495a\",\"e8a2e031-c99c-4443-a5c5-dbf47ba4a4a9\",\"e8d6e41d-18fd-495c-8f23-d8417323f4d9\",\"390ca970-42a0-4684-949f-7407df177aed\",\"4df5e6f2-82ca-4f58-b94f-9c835849113b\",\"d6ce3e06-1092-4816-9e45-042aea8ef72a\",\"9a4fcce8-4fe4-4fbd-9893-d89b5dfe0999\",\"6440cf70-fa81-4d2f-8294-a85a2b98b56b\",\"c56da051-bb7d-4ba8-912e-2c5319175775\",\"5538e482-9f4e-468e-bf19-d95b783fae77\",\"56c821cf-060c-4afe-a6ca-b50f597c495a\",\"e8a2e031-c99c-4443-a5c5-dbf47ba4a4a9\",\"e8d6e41d-18fd-495c-8f23-d8417323f4d9\",\"390ca970-42a0-4684-949f-7407df177aed\",\"4df5e6f2-82ca-4f58-b94f-9c835849113b\",\"d6ce3e06-1092-4816-9e45-042aea8ef72a\",\"9a4fcce8-4fe4-4fbd-9893-d89b5dfe0999\",\"6440cf70-fa81-4d2f-8294-a85a2b98b56b\",\"c56da051-bb7d-4ba8-912e-2c5319175775\",\"5538e482-9f4e-468e-bf19-d95b783fae77\"]", List.class);
            for(int i =0; i<records;i++) {
                Date date = new Date();
                Document filter = new Document("guid",guids.get(i));
                FindIterable<Document> documents = suppliers.find(filter);

                Date date2 = new Date();

                for (Document document : documents) {
                    System.out.println(document.toJson());
                }
                System.out.println(String.format("Time spent %d", date2.getTime() - date.getTime()));
            }

        }catch (Exception e){
            System.out.println("Error in the program");
        }
    }

    public static List<String> fetchTopNRecords(int limit, MongoCollection<Document> suppliers){
        List<String> guids = new ArrayList<>();
        try{
            System.out.println("Starting method 4");


            for(int i =0; i<10;i++) {
                Document filter = new Document();
                FindIterable<Document> documents = suppliers.find(filter).limit(limit);

                for (Document document : documents) {
                   guids.add((String) document.get("guid"));
                }
            }
            System.out.println(new Gson().toJson(guids));

        }catch (Exception e){
            throw e;
        }
        return guids;
    }

    public static void insertDummyRecords(MongoCollection<Document> suppliers){
        final String json = "{\"guid\": \"GUID\", \"processId\": \"1234\", \"partnerships\": [{\"id\": \"owned\", \"processId\": \"34334\"}, {\"id\": \"dsv\", \"processId\": \"21343\"}], \"onboardingStatus\": \"1\"}";
        for(int i=0;i<5000;i++){
            UUID guid = UUID.randomUUID();
            System.out.println(String.format("%s Inserting record, GUID: %s",i,guid));
            String record = json.replace("GUID",guid.toString());
            Document document = Document.parse(record);
            suppliers.insertOne(document);
        }
    }

    public static MongoCollection createClient(){
        Block<ConnectionPoolSettings.Builder> connectionPoolBuilder = new Block<ConnectionPoolSettings.Builder>() {
            @Override
            public void apply(ConnectionPoolSettings.Builder builder) {
                builder.maxSize(10).minSize(5).maxConnectionIdleTime(120000, TimeUnit.MICROSECONDS).build();
            }
        };


        ConnectionString connectionString = new ConnectionString("mongodb://mongoexp.mongo.cosmos.azure.com:10255/");

        MongoClient mongoClient = MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .applyToSslSettings(builder -> {
                    builder.enabled(Boolean.TRUE);
                })
                .credential(MongoCredential.createCredential("mongoexp","suppliers","p6AowPalsguPHqD8qrb0xo0igFqRmOJEqAg21iMRsHiHuEAAtJaSQYslZtSosPhG9vGi8cLp1UjYFanl7s3QrA==".toCharArray()))
                .applyConnectionString(connectionString)
                .retryWrites(Boolean.FALSE)
                .applyToConnectionPoolSettings(connectionPoolBuilder).build());
        MongoDatabase database = mongoClient.getDatabase("suppliers");
        MongoCollection<Document> suppliers = database.getCollection("supplierdetails");
        return suppliers;
    }
}
