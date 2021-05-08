package dev.gabrielcunha.ecommerce;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.sshd.client.SshClient;
import org.apache.sshd.client.channel.ClientChannel;
import org.apache.sshd.client.channel.ClientChannelEvent;
import org.apache.sshd.client.session.ClientSession;
import org.apache.sshd.common.channel.Channel;

import javax.net.ssl.SSLSession;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.EnumSet;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;


public class OrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {

        String username = "user";
        String password = "pass";
        String host = "192.168.0.185";
        int port = 22;
        long defaultTimeoutSeconds = 10l;
        String command = "ls\n";

        listFolderStructure(username, password, host, port, defaultTimeoutSeconds, command);

        var producer = new KafkaProducer<String, String>(properties());
        var value = "1298,5465,565";
        var record = new ProducerRecord<>("ECOMMERCE_NEW_ORDER",  value, value);

        producer.send(record, (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }
            System.out.println("sucesso enviando "+ "topic " + data.topic() + ":::partition " + data.partition()
                    + "/ offset " + data.offset() + " / timestamp"  + data.timestamp());
        }).get();

    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.0.185:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

    public static void listFolderStructure(String username, String password,
                                           String host, int port, long defaultTimeoutSeconds, String command) throws IOException {

        SshClient client = SshClient.setUpDefaultClient();
        client.start();

        try (ClientSession session = client.connect(username, host, port)
                .verify(defaultTimeoutSeconds, TimeUnit.SECONDS).getSession()) {
            session.addPasswordIdentity(password);
            session.auth().verify(defaultTimeoutSeconds, TimeUnit.SECONDS);

            try (ByteArrayOutputStream responseStream = new ByteArrayOutputStream();
                 ClientChannel channel = session.createChannel(Channel.CHANNEL_SHELL)) {
                channel.setOut(responseStream);
                try {
                    channel.open().verify(defaultTimeoutSeconds, TimeUnit.SECONDS);
                    try (OutputStream pipedIn = channel.getInvertedIn()) {
                        pipedIn.write(command.getBytes());
                        pipedIn.flush();
                    }

                    channel.waitFor(EnumSet.of(ClientChannelEvent.CLOSED),
                            TimeUnit.SECONDS.toMillis(defaultTimeoutSeconds));
                    String responseString = new String(responseStream.toByteArray());
                    System.out.println(responseString);
                } finally {
                    channel.close(false);
                }
            }
        } finally {
            client.stop();
        }
    }
}
