package com.example.personal;

import com.google.gson.Gson;
import com.mongodb.Block;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.client.*;
import com.mongodb.connection.ConnectionPoolSettings;
import org.bson.Document;
import com.mongodb.ConnectionString;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class app {
    private static String welcomeMessage = "Welcome! Select an option ( 1 - 6) to proceed: \n 1. Check Latency. \n 2. Insert records \n 3. Fetch top n records \n 4. Fetch number of records \n 5. Bootstrap program \n 6. exit ";
    private static List<String> guids;
    private static int maxBatchSize;
    private static int latencyCheckIterations;
    private static MongoCollection<Document> suppliers;
    public static void main(String[] args) {
        try {
            bootStrapProgram();
            if(args.length > 0){
                System.out.println("Shorting the program to insert only");
                int records = Integer.parseInt(args[0]);
                System.out.println(String.format("Inserting %d records",records));
                insertDummyRecords(records);
            }else {
                Scanner scanner = new Scanner(System.in);
                int option = 1;
                int records = 1;
                final String os = System.getProperty("os.name");
                while (true) {


                    System.out.println(welcomeMessage);
                    option = scanner.nextInt();

                    switch (option) {
                        case 1:
                            latencyCheckRead();
                            break;
                        case 2:
                            System.out.println("Enter the number of records");
                            records = scanner.nextInt();
                            insertDummyRecords(records);
                            break;
                        case 3:
                            System.out.println("Enter the number of records");
                            records = scanner.nextInt();
                            fetchTopNRecords(records);
                            break;
                        case 4:
                            fetchRecordCount();
                            break;
                        case 5:
                            bootStrapProgram();
                            break;
                        case 6:
                            System.out.println("Exiting the program.");
                            System.exit(0);
                            break;

                        default:
                            System.out.println("Select valid choice");
                    }
                    System.out.println("\n\n\n\n\n\n");

                }
            }

        }catch (Exception e){
            System.out.println(String.format("Error in program %s",e.getMessage()));
        }

    }

    public static void bootStrapProgram() throws IOException {
        try {
            Path currentRelativePath = Paths.get("","db.properties");

            String s = currentRelativePath.toAbsolutePath().toString();
            System.out.println("Reading properties from  " + s);
            File file = new File(s);
            if(!file.exists()){
                System.out.println(String.format("Properties file %s should be present for the program to run",s));
                System.exit(1);
            }
            FileReader reader = new FileReader(s);


            Properties p = new Properties();
            p.load(reader);
            createClient(p);

            if(p.containsKey("guids")){
                guids = new Gson().fromJson((String)p.get("guids"),List.class);
            }
            if(guids == null || guids.size() == 0){
                guids = new ArrayList<>();
                guids.add(UUID.randomUUID().toString());
            }

            if(p.containsKey("maxBatchSize")){
                maxBatchSize = Integer.parseInt((String)p.get("maxBatchSize"));
            }else {
                maxBatchSize =  1000;
            }

            if(p.containsKey("latencyCheckIterations")){
                latencyCheckIterations = Integer.parseInt((String)p.get("latencyCheckIterations"));
            }else {
                latencyCheckIterations = 10;
            }

            System.out.println(String.format("Started application with %s write batchSize, %s latency iterations",maxBatchSize, latencyCheckIterations));
            reader.close();
        }catch (Exception e){
            System.out.println(String.format("Unable to start program, error %s",e.getMessage()));
            throw e;
        }

    }
    public static void fetchRecordCount(){
        System.out.println(String.format("Number of records %s",suppliers.countDocuments()));
    }

    public static void latencyCheckRead(){
        try{
            System.out.println("Latency check");

            long totalTime = 0;

            Document filter2 = new Document("guid",guids.get(0));
            Document documents1 = suppliers.find(filter2).first();

            int guidListSize = guids.size();
            for(int i =0; i<latencyCheckIterations;i++) {
                Date date = new Date();
                int guidIndex = i%guidListSize;
                Document filter = new Document("guid",guids.get(guidIndex));
                suppliers.find(filter).first();

                Date date2 = new Date();
                System.out.println(String.format("%d Time spent %d", i, date2.getTime() - date.getTime()));
                totalTime += date2.getTime() - date.getTime();
            }
            System.out.println(String.format("Average time = %s",totalTime/latencyCheckIterations));

        }catch (Exception e){
            throw e;
        }
    }

    public static List<String> fetchTopNRecords(int limit){
        List<String> guids = new ArrayList<>();
        try{
            System.out.println(String.format("Fetching top %d records",limit));

            Document filter = new Document();
            FindIterable<Document> documents = suppliers.find(filter).limit(limit);

            for (Document document : documents) {
                System.out.println(document.toJson());
            }


        }catch (Exception e){
            throw e;
        }
        return guids;
    }

    public static void insertDummyRecords(int records){
        final String json = "{\"guid\": \"GUID\", \"processId\": \"1234\", \"partnerships\": [{\"id\": \"owned\", \"processId\": \"34334\"}, {\"id\": \"dsv\", \"processId\": \"21343\"}], \"onboardingStatus\": \"1\"}";
        int batchs = (records+maxBatchSize -1)/maxBatchSize;

        for( int j = 0; j< batchs; j++) {
            List<Document> documents = new ArrayList<>();
            int batchSize = Math.min(records,maxBatchSize);
            for (int i = 0; i < batchSize; i++) {
                UUID guid = UUID.randomUUID();
                System.out.println(String.format("%s preparing record, GUID: %s", i, guid));
                String record = json.replace("GUID", guid.toString());
                Document document = Document.parse(record);
                document.put("_id", guid.toString());
                documents.add(document);
            }
            records -= batchSize;
            System.out.println(String.format("Batch %d, insert %d records", j, batchSize));
            Date time1 = new Date();
            suppliers.insertMany(documents);
            Date time2 = new Date();
            System.out.println(String.format("Time for writing %s records %s\n", batchSize, time2.getTime() - time1.getTime()));
        }
    }

    public static MongoCollection createClient(Properties p) {
        try {
            System.out.println("Connecting to database.");
            Block<ConnectionPoolSettings.Builder> connectionPoolBuilder = new Block<ConnectionPoolSettings.Builder>() {
                @Override
                public void apply(ConnectionPoolSettings.Builder builder) {
                    builder.maxSize(10).minSize(5).maxConnectionIdleTime(120000, TimeUnit.MICROSECONDS).build();
                }
            };

            ConnectionString connectionString = new ConnectionString((String) p.get("connectionString"));

            MongoClient mongoClient = MongoClients.create(MongoClientSettings.builder()
                    .applyConnectionString(connectionString)
                    .applyToSslSettings(builder -> {
                        builder.enabled(Boolean.TRUE);
                    })
                    .credential(MongoCredential.createCredential((String) p.get("userName"), (String) p.get("database"), ((String) p.get("password")).toCharArray()))
                    .applyConnectionString(connectionString)
                    .retryWrites(Boolean.FALSE)
                    .retryReads(Boolean.FALSE)
                    .applyToConnectionPoolSettings(connectionPoolBuilder).build());
            MongoDatabase database = mongoClient.getDatabase((String) p.get("database"));
            suppliers = database.getCollection((String) p.get("collection"));
            suppliers.find().limit(1);
            System.out.println("Connection successful!");
            return suppliers;
        }catch (Exception e){
            System.out.println(String.format("Error creating connection instance %s",e.getMessage()));
            throw e;
        }
    }
}
