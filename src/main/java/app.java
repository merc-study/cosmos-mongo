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
//            insertDummyRecords(suppliers,1);
//        fetchTopNRecords(100,suppliers);

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


            int records = 100;
            long mili = 0;
            List<String> guids = new Gson().fromJson("[\"56c821cf-060c-4afe-a6ca-b50f597c495a\",\"e8a2e031-c99c-4443-a5c5-dbf47ba4a4a9\",\"e8d6e41d-18fd-495c-8f23-d8417323f4d9\",\"390ca970-42a0-4684-949f-7407df177aed\",\"4df5e6f2-82ca-4f58-b94f-9c835849113b\",\"d6ce3e06-1092-4816-9e45-042aea8ef72a\",\"9a4fcce8-4fe4-4fbd-9893-d89b5dfe0999\",\"6440cf70-fa81-4d2f-8294-a85a2b98b56b\",\"c56da051-bb7d-4ba8-912e-2c5319175775\",\"5538e482-9f4e-468e-bf19-d95b783fae77\",\"bd3b8bd8-446a-48fe-9d9d-3c73f81cff75\",\"dd193f46-8ac4-4228-b0c4-e1235f23f066\",\"183e235b-f936-448c-953b-8f66e0040a9d\",\"11d25e7e-6d88-41d5-bf76-ac254c4fc758\",\"e7436b9e-3f7b-413c-a7f0-373fa26a6854\",\"8a6327be-eec3-4ccb-a193-fbf2f03c807a\",\"78b8aab6-2ab8-43c6-8db8-a4a77296c318\",\"1ed6a197-6922-4c1a-a5b5-fb87c3c8f4b2\",\"f2d36dce-ce5c-4850-90db-de1d5a60a0b4\",\"e4c795f2-687c-48df-ab30-39453a619933\",\"87d556e6-324e-4705-a36d-defca574d29d\",\"c8e0ceee-2532-4a96-a300-a9f2fe22a92b\",\"6a769f57-7507-49a2-94e4-17b037c7abad\",\"07f526bc-0b59-433b-a739-18b10baf1159\",\"e36dcb9e-e81d-43db-8d9f-a9ec01d9ab83\",\"28f0de2a-8fa9-4cf8-85df-70fdcbe165d0\",\"4128e753-18f6-4b63-9fe1-3f64e9685cff\",\"14a42449-c1cb-446f-95ef-58325568b413\",\"94aa8dce-8bfc-46a8-a08a-a45dc6fe5928\",\"093f0c58-a162-4150-b424-fc3470344a08\",\"6818be50-15c1-4a59-b478-627851c8976e\",\"1c653fc6-1907-48cd-b203-5221d5e1703b\",\"bbdb6c2b-760d-4771-96de-9b1761c71354\",\"3b88590b-6d9c-4d6d-9398-8c3cd1195a64\",\"ea34a95b-0797-4a28-bebe-bcd5a0eebd25\",\"4a0c47bf-c865-40bb-9b86-20b9e6f43c51\",\"e0104969-e847-4ad6-8317-cf185611c567\",\"4805ec61-8f5e-4778-b2bd-f87e0610e6b8\",\"f141ab2c-4096-4fc6-800a-3c725e5c1147\",\"c32712d6-7efb-41e0-8895-ba4815a94e72\",\"cbab1004-c61d-4ff3-95d6-d7761b59cd8c\",\"71857a49-acbf-45e4-a79c-cfbb64f84288\",\"7ac0d1b8-325c-4e46-a96a-f3bdd4aac490\",\"a822e39a-890f-4fc2-aa60-440b18940483\",\"dc4656d5-1847-4582-9106-16f5041bde50\",\"0696082d-9ad0-48ef-b3de-24a9d76906d8\",\"614aa452-30fe-4137-8951-ca479f55149d\",\"d345dc2e-a1da-40be-8095-3f5667102961\",\"f7ee0af8-3262-445d-875a-851127957abb\",\"ec538909-a596-45a0-8802-0b11c1fd1d21\",\"d25574c0-1cd1-4cbd-ad8e-27329025ad41\",\"8382e20c-b963-4e79-9e87-6856a97f8dff\",\"09758976-5305-459e-adb0-cf792b3bb682\",\"71429f8a-3f95-4c38-a878-1cb0632c0f71\",\"1fb20efd-4624-464d-aae6-f4266242eaaa\",\"2affd7f4-302a-4252-aa67-bcff0c3653fc\",\"86a0fcdf-94e6-4e8b-9fad-a9dcc52922ad\",\"5ab5ffbc-5bc9-4021-85ef-1864793f99c2\",\"0778ea5e-53c0-423a-b50b-64eef21eec81\",\"91ab1ef3-515f-4105-896c-5abf0e3bb6d9\",\"8127d8b9-d60a-4a6d-835a-7fdb5d6e713b\",\"5fd8f061-fa6c-4fb7-a152-20f9e4c903fe\",\"f7d2ebd6-2f96-45a8-8231-5c87cff4259f\",\"68f8497b-7784-4715-bbb7-feaf10f26cb9\",\"0d7beec4-a085-4b5e-97df-0acfe32af7c1\",\"9925d943-4bfb-416f-97b9-dbec97ae6de2\",\"04030d74-0b43-4e41-890d-5a99404d907e\",\"e9c35092-b51e-41e4-a4c6-7533310b61f6\",\"cc1c2b82-8a2e-4ed5-8b7f-dd1eec8dc829\",\"61574bbf-a473-454a-8f69-ccb681963802\",\"b675854e-9692-40d1-8fd2-73542f9c9202\",\"1ed6817e-8a54-42f6-b2f0-358e00f7c0f4\",\"3c7e7acb-5f9c-4d1e-9c7c-531a5fc22c73\",\"329a9b2e-06fd-4428-b7fc-c44b96b73efd\",\"d53ac226-c998-49c5-86eb-4db699cf9830\",\"b78b4320-7496-499e-98cf-6534d45f9699\",\"47cfde7f-d99b-4547-bf0c-dbff111daae1\",\"4b37a9a1-6342-431f-be79-bcc3898f8a18\",\"28dc01ac-f7b1-4fff-99eb-a0bb0e099a8d\",\"faea7ab7-8463-4165-9455-590b047a94ee\",\"d4f97bb9-d88b-4cb3-b55e-4c24d3357df7\",\"59113cef-89bc-441c-b1f6-e83607ad2767\",\"0d351227-ecf6-424b-b9af-5d6e9cc1579c\",\"f304f8ba-6c4f-4cb3-9f89-c3c6e316f260\",\"bc77cc73-0280-4721-8fe3-2a60d8798d56\",\"3bdbd9ea-4355-48f3-9a2d-a0e91a4f3a07\",\"69ce66c5-c135-42ce-8c1c-ae718654bb07\",\"37006297-565a-4d88-9b79-04fc083cf8b1\",\"1062235e-aac8-4230-bb94-55f88cb3e745\",\"7bace873-6751-498c-af61-5708f5df853c\",\"110f7277-48fe-4e6a-9384-97952607ac3d\",\"15857f8e-19c9-4d80-af3d-09a11d8fe70a\",\"3e2c2a30-61ec-4401-8997-014939590f79\",\"2adbe647-c249-4b60-9ece-0bcbefac7525\",\"1356c521-39fe-4e2a-b70d-61e3b5fdaecd\",\"6f1af81e-8997-47f6-94d8-5355058f8ecd\",\"c62c3329-e5bc-42cc-a174-53e46b0d1d27\",\"281c7739-ca6a-4412-9dae-9c4dc03b856d\",\"fd400bd8-87fe-45db-9760-c1db3b5e039e\",\"3ddb4209-ac9e-4443-955a-b842da7b90e0\"]", List.class);
            Document filter1 = new Document("_id","31096851-835f-4329-b206-c4b5b3412042");
            FindIterable<Document> documents1 = suppliers.find(filter1);
            for (Document document : documents1) {
                System.out.println(document.toJson());
            }
            for(int i =1; i<records;i++) {
                Date date = new Date();
                Document filter = new Document("_id","31096851-835f-4329-b206-c4b5b3412042");
                Document documents = suppliers.find(filter).first();

                System.out.println(documents.toJson());

                Date date2 = new Date();
                System.out.println(String.format("%d Time spent %d", i, date2.getTime() - date.getTime()));
                mili += date2.getTime() - date.getTime();
            }
            System.out.println(String.format("Average time = %s",mili/99));

        }catch (Exception e){
            System.out.println("Error in the program");
        }
    }

    public static List<String> fetchTopNRecords(int limit, MongoCollection<Document> suppliers){
        List<String> guids = new ArrayList<>();
        try{
            System.out.println("Starting method 4");

            Document filter = new Document();
            FindIterable<Document> documents = suppliers.find(filter).limit(limit);

            for (Document document : documents) {
               guids.add((String) document.get("guid"));
            }

            System.out.println(new Gson().toJson(guids));

        }catch (Exception e){
            throw e;
        }
        return guids;
    }

    public static void insertDummyRecords(MongoCollection<Document> suppliers, int records){
        final String json = "{\"guid\": \"GUID\", \"processId\": \"1234\", \"partnerships\": [{\"id\": \"owned\", \"processId\": \"34334\"}, {\"id\": \"dsv\", \"processId\": \"21343\"}], \"onboardingStatus\": \"1\"}";
        List<Document> documents = new ArrayList<>();
        for(int i=0;i<records;i++){
            UUID guid = UUID.randomUUID();
            System.out.println(String.format("%s preparing record, GUID: %s",i,guid));
            String record = json.replace("GUID",guid.toString());
            Document document = Document.parse(record);
            document.put("_id",guid.toString());
            documents.add(document);
        }
        Date time1= new Date();
        suppliers.insertMany(documents);
        Date time2 = new Date();
        System.out.println(String.format("time for writing %s records %s",records, time2.getTime() - time1.getTime()));
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
                        .retryReads(Boolean.FALSE)
                .applyToConnectionPoolSettings(connectionPoolBuilder).build());
        MongoDatabase database = mongoClient.getDatabase("suppliers");
        MongoCollection<Document> suppliers = database.getCollection("supplierdetails");
        return suppliers;
    }
}
