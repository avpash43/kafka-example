import org.apache.kafka.clients.admin.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        AdminClient admin = KafkaAdminClient.create(config);

        createTopics(admin);
        ListTopicsResult ltr = admin.listTopics();
        deleteTopics(admin, ltr);

        ltr = admin.listTopics();
        printResult(ltr);
    }

    private static void printResult(ListTopicsResult ltr) {
        try {
            Set<String> set = ltr.names().get();
            log.info("Result list of workspaces: ");
            set.forEach(s -> {
                if(s.contains("-Space"))
                    System.out.println(s);
            });
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    private static void deleteTopics(AdminClient admin, ListTopicsResult ltr) {
        try {
            Set<String> set = ltr.names().get();
            log.info("List of workspaces: ");
            set.forEach(s -> {
                if(s.contains("-Space"))
                    log.info(s);
            });

            List<String> topicsToDelete = set.stream()
                    .filter(s -> s.contains("-Space1"))
                    .collect(Collectors.toList());

            DeleteTopicsResult dtr = admin.deleteTopics(topicsToDelete);
            dtr.all().get();
            if(!dtr.values().isEmpty()) {
                log.info("Topics are deleted:");
                dtr.values().forEach((key, value) -> log.info(key));
            } else {
                log.info("Topics are not found.");
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    private static void createTopics(AdminClient admin) {
        NewTopic topic1 = new NewTopic("Topic1-Space1", 1, (short) 1);
        NewTopic topic2 = new NewTopic("Topic2-Space1", 1, (short) 1);
        NewTopic topic3 = new NewTopic("Topic3-Space1", 1, (short) 1);
        NewTopic topic4 = new NewTopic("Topic4-Space1", 1, (short) 1);
        List<NewTopic> listNewTopics = Arrays.asList(topic1, topic2, topic3, topic4);

        CreateTopicsResult resultc = admin.createTopics(listNewTopics);

        try {
            resultc.all().get();
            log.info("Topics are created:");
            resultc.values().forEach((key, value) -> log.info(key));
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

    }
}
