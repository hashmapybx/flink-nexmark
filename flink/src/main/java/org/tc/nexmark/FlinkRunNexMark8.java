package src.main.java.org.tc.nexmark;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkRunNexMark8 {
    private static Logger LOG = LoggerFactory.getLogger(FlinkRunNexMark8.class);
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            LOG.info("====input generate data speed======");
            System.exit(-1);
        }
        int speed = Integer.parseInt(args[0]);
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(executionEnvironment);

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
        streamTableEnvironment.executeSql("CREATE TEMPORARY TABLE discard_sink (id BIGINT, name VARCHAR, stime TIMESTAMP(3)) \n" +
                "WITH ('connector' = 'blackhole');");
        streamTableEnvironment.executeSql("CREATE TEMPORARY VIEW person AS\n" +
                "    SELECT \n" +
                "        person.id,\n" +
                "        person.name,\n" +
                "        person.emailAddress,\n" +
                "        person.creditCard,\n" +
                "        person.city,\n" +
                "        person.state,\n" +
                "        dateTime,\n" +
                "        person.extra \n" +
                "    FROM\n" +
                "        nexmark_table \n" +
                "    WHERE\n" +
                "        event_type = 0;");
        streamTableEnvironment.executeSql("CREATE TEMPORARY VIEW auction AS\n" +
                "    SELECT \n" +
                "        auction.id,\n" +
                "        auction.itemName,\n" +
                "        auction.description,\n" +
                "        auction.initialBid,\n" +
                "        auction.reserve,\n" +
                "        dateTime,\n" +
                "        auction.expires,\n" +
                "        auction.seller,\n" +
                "        auction.category,\n" +
                "        auction.extra \n" +
                "    FROM\n" +
                "        nexmark_table \n" +
                "    WHERE\n" +
                "        event_type = 1;");
        streamTableEnvironment.executeSql("INSERT INTO discard_sink\n" +
                "    SELECT \n" +
                "        P.id, P.name, P.starttime \n" +
                "    FROM\n" +
                "        (\n" +
                "            SELECT \n" +
                "                P.id,\n" +
                "                P.name,\n" +
                "                TUMBLE_START(P.dateTime, INTERVAL '10' SECOND) AS starttime,\n" +
                "                TUMBLE_END(P.dateTime, INTERVAL '10' SECOND) AS endtime \n" +
                "            FROM\n" +
                "                person P \n" +
                "            GROUP BY\n" +
                "                P.id,\n" +
                "                P.name,\n" +
                "                TUMBLE(P.dateTime, INTERVAL '10' SECOND)\n" +
                "        ) P \n" +
                "        JOIN (\n" +
                "            SELECT \n" +
                "                A.seller,\n" +
                "                TUMBLE_START(A.dateTime, INTERVAL '10' SECOND) AS starttime,\n" +
                "                TUMBLE_END(A.dateTime, INTERVAL '10' SECOND) AS endtime \n" +
                "            FROM\n" +
                "                auction A \n" +
                "            GROUP BY\n" +
                "                A.seller,\n" +
                "                TUMBLE(A.dateTime, INTERVAL '10' SECOND)\n" +
                "        ) A\n" +
                "            ON P.id = A.seller \n" +
                "                AND P.starttime = A.starttime \n" +
                "                AND P.endtime = A.endtime;");
//        executionEnvironment.execute("write query8");

    }
}
