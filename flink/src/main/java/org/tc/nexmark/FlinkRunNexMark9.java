package src.main.java.org.tc.nexmark;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkRunNexMark9 {
    private static Logger LOG = LoggerFactory.getLogger(FlinkRunNexMark9.class);
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
        streamTableEnvironment.executeSql("CREATE TEMPORARY TABLE discard_sink (\n" +
                "    id BIGINT,\n" +
                "    itemName VARCHAR,\n" +
                "    description VARCHAR,\n" +
                "    initialBid BIGINT,\n" +
                "    reserve BIGINT,\n" +
                "    dateTime TIMESTAMP(3),\n" +
                "    expires TIMESTAMP(3),\n" +
                "    seller BIGINT,\n" +
                "    category BIGINT,\n" +
                "    extra VARCHAR,\n" +
                "    auction BIGINT,\n" +
                "    bidder BIGINT,\n" +
                "    price BIGINT,\n" +
                "    bid_dateTime TIMESTAMP(3),\n" +
                "    bid_extra VARCHAR\n" +
                ") \n" +
                "WITH ('connector' = 'blackhole');");
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
                "        id,\n" +
                "        itemName,\n" +
                "        description,\n" +
                "        initialBid,\n" +
                "        reserve,\n" +
                "        dateTime,\n" +
                "        expires,\n" +
                "        seller,\n" +
                "        category,\n" +
                "        extra,\n" +
                "        auction,\n" +
                "        bidder,\n" +
                "        price,\n" +
                "        bid_dateTime,\n" +
                "        bid_extra \n" +
                "    FROM\n" +
                "        (\n" +
                "            SELECT \n" +
                "                A.*,\n" +
                "                B.auction,\n" +
                "                B.bidder,\n" +
                "                B.price,\n" +
                "                B.dateTime AS bid_dateTime,\n" +
                "                B.extra AS bid_extra,\n" +
                "                ROW_NUMBER() \n" +
                "                    OVER (\n" +
                "                        PARTITION BY\n" +
                "                            A.id\n" +
                "                        ORDER BY\n" +
                "                            B.price DESC, B.dateTime ASC\n" +
                "                    ) AS rownum \n" +
                "            FROM\n" +
                "                auction A, bid B \n" +
                "            WHERE\n" +
                "                A.id = B.auction \n" +
                "                    AND B.dateTime BETWEEN A.dateTime AND A.expires\n" +
                "        ) \n" +
                "    WHERE\n" +
                "        rownum <= 1;");
//        executionEnvironment.execute("write query9");

    }
}
