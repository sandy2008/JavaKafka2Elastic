public class KafkaConsumerTest {

    private static Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerTest.class);

    private static KafkaConsumer<byte[], String> kafkaConsumer;

    private static Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<TopicPartition, OffsetAndMetadata>();

    private static int count = 0;

    private static Thread thread;

    public static void main(String[] args) {
        Map<String, Object> config = new HashMap<String, Object>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "ip:port,ip:port");
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "xx-group");
        config.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 15000);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        kafkaConsumer = new KafkaConsumer<byte[], String>(config);
        kafkaConsumer.subscribe(Arrays.asList("test-topic"), new HandleRebalance());

        thread = Thread.currentThread();


        Settings settings = Settings.settingsBuilder().put("cluster.name", "taurus")
                .put("client.transport.sniff", true).build();
        TransportClient client = TransportClient.builder().settings(settings).build();
        try {
            String[] ips = "ip:port,ip:port".split(",");
            for (String ip : ips) {
                client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ip), 9300));
            }
        } catch (UnknownHostException e) {
            LOGGER.error("es集群主机名错误");
        }


        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                LOGGER.info("Starting exit...");
                kafkaConsumer.wakeup();
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            BulkRequestBuilder bulkRequest = client.prepareBulk();
            while (true) {
                ConsumerRecords<byte[], String> records = kafkaConsumer.poll(100);
                if (!records.isEmpty()) {
                    for (ConsumerRecord<byte[], String> record : records) {
                        String value = record.value();
                        bulkRequest.add(client.prepareIndex("index", "doc")
                                .setSource(buildXContentBuilder(value)));
                        currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));

                        count++;
                        if (count >= 1000) {
                            // 当达到了1000触发向kafka提交offset
                            kafkaConsumer.commitAsync(currentOffsets, new KafkaOffsetCommitCallback());
                            count = 0;
                        }
                    }
                    bulkRequest.execute().actionGet();
                    bulkRequest = client.prepareBulk();
                    kafkaConsumer.commitAsync(currentOffsets, new KafkaOffsetCommitCallback());
                    LOGGER.info("indexed {} records to es", records.count());
                }
            }
        } catch (WakeupException e) {
            // do not process, this is shutdown
            LOGGER.error("wakeup, start to shutdown, ", e);
        } catch (Exception e) {
            LOGGER.error("process records error");
        } finally {
            try {
                kafkaConsumer.commitSync(currentOffsets);
                LOGGER.info("finally commit the offset");
            } finally {
                kafkaConsumer.close();
            }
        }
    }

    private static class HandleRebalance implements ConsumerRebalanceListener {

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            kafkaConsumer.commitSync(currentOffsets);
            LOGGER.info("before rebalance, commit offset once");
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

        }

    }

    private static XContentBuilder buildXContentBuilder(String line) throws IOException {

        LogDto logDto = new LogDto(line);
        return jsonBuilder()
                .startObject()
                .field("filed1", logDto.getFiled1())
                .field("filed2", logDto.getFiled2())
                .endObject();
    }

    static class KafkaOffsetCommitCallback implements OffsetCommitCallback {

        private static final Logger LOGGER = LoggerFactory.getLogger(com.unionpaysmart.medusa.callback.KafkaOffsetCommitCallback.class);

        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            if (null != exception) {
                // 如果异步提交发生了异常
                LOGGER.error("commit failed for offsets {}, and exception is {}", offsets, exception);
            }
        }
    }

    static class LogDto {
        private String filed1;
        private String filed2;

        public LogDto(String log) {
            String[] detail = log.split(";");
            String date = detail[0];
            this.filed1 = detail[0];
            this.filed2 = detail[1];
        }

        public String getFiled1() {
            return filed1;
        }

        public void setFiled1(String filed1) {
            this.filed1 = filed1;
        }

        public String getFiled2() {
            return filed2;
        }

        public void setFiled2(String filed2) {
            this.filed2 = filed2;
        }
    }
}