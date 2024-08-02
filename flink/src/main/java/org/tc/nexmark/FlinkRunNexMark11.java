package src.main.java.org.tc.nexmark;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkRunNexMark11 {
    private static Logger LOG = LoggerFactory.getLogger(FlinkRunNexMark11.class);
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            LOG.info("====input generate data speed======");
            System.exit(-1);
        }
        int speed = Integer.parseInt(args[0]);
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(executionEnvironment);

//        streamTableEnvironment.executeSql("set pipeline.object-reuse = true;");
        streamTableEnvironment.executeSql("CREATE TEMPORARY TABLE nexmark_table(  \n" +
                "    event_type INT,\n" +
                "    person ROW < id BIGINT, name VARCHAR, emailAddress VARCHAR, creditCard VARCHAR, city VARCHAR, state VARCHAR, dateTime TIMESTAMP(3), extra VARCHAR >,\n" +
                "    auction ROW < id BIGINT, itemName VARCHAR, description VARCHAR, initialBid BIGINT, reserve BIGINT, dateTime TIMESTAMP(3), expires TIMESTAMP(3), seller BIGINT, category BIGINT, extra VARCHAR >,\n" +
                "    bid ROW < auction BIGINT, bidder BIGINT, price BIGINT, channel VARCHAR, url VARCHAR, dateTime TIMESTAMP(3), extra VARCHAR >,\n" +
                "    dateTime AS CASE \n" +
                "        WHEN event_type = 0 \n" +
                "        THEN person.dateTime \n" +
                "        WHEN event_type = 1 \n" +
                "        THEN auction.dateTime \n" +
                "        ELSE bid.dateTime \n" +
                "    END,\n" +
                "    WATERMARK FOR dateTime AS dateTime - INTERVAL '4' SECOND\n" +
                ") \n" +
                "WITH (\n" +
                "    'connector' = 'nexmark',\n" +
                "    'first-event.rate' = '" + speed + "',\n" +
                "    'next-event.rate' = '" + speed + "',\n" +
                "    'events.num' = '100000000',\n" +
                "    'person.proportion' = '2',\n" +
                "    'auction.proportion' = '6',\n" +
                "    'bid.proportion' = '92'\n" +
                "); \n");
        streamTableEnvironment.executeSql("CREATE TEMPORARY TABLE discard_sink (\n" +
                "    bidder BIGINT,\n" +
                "    bid_count BIGINT,\n" +
                "    starttime TIMESTAMP(3),\n" +
                "    endtime TIMESTAMP(3)\n" +
                ") \n" +
                "WITH ('connector' = 'blackhole');");
        streamTableEnvironment.executeSql("CREATE TEMPORARY VIEW bid AS\n" +
                "    SELECT \n" +
                "        bid.auction,\n" +
                "        bid.bidder,\n" +
                "        bid.price,\n" +
                "        bid.channel,\n" +
                "        bid.url,\n" +
                "        dateTime,\n" +
                "        bid.extra \n" +
                "    FROM\n" +
                "        nexmark_table \n" +
                "    WHERE\n" +
                "        event_type = 2;");
        streamTableEnvironment.executeSql("INSERT INTO discard_sink\n" +
                "    SELECT \n" +
                "        B.bidder,\n" +
                "        count(*) as bid_count,\n" +
                "        SESSION_START(B.dateTime, INTERVAL '10' SECOND) as starttime,\n" +
                "        SESSION_END(B.dateTime, INTERVAL '10' SECOND) as endtime \n" +
                "    FROM\n" +
                "        bid B \n" +
                "    GROUP BY\n" +
                "        B.bidder, SESSION(B.dateTime, INTERVAL '10' SECOND);");

//        executionEnvironment.execute("write query10");

    }
}
