package com.artisiou.butterflynet;

import com.beust.jcommander.JCommander;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.*;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Main {
    public static void main(String[] argv) {
        JCommanderParameters parameters = new JCommanderParameters();
        new JCommander(parameters, argv);

        System.out.println();

        ObjectMapper mapper = new ObjectMapper();
        try {

            //
            // MONGO
            //

            MongoClient mongoClient = new MongoClient("localhost", 27017);
            DB db = mongoClient.getDB("hdr");
            DBCollection tweetsColl = db.getCollection("tweets");
            DBCollection newsColl = db.getCollection("news");

            //
            // CONFIG
            //

            Config config = mapper.readValue(new File(parameters.config), Config.class);
            System.out.println("Tracking:       ");
            config.track.stream().forEach(s -> System.out.println("    " + s));
            System.out.println();
            System.out.println("consumerKey:    " + config.consumerKey);
            System.out.println("consumerSecret: " + config.consumerSecret);
            System.out.println("token:          " + config.accessTokenKey);
            System.out.println("secret:         " + config.accessTokenSecret);
            System.out.println();

            Authentication hosebirdAuth = new OAuth1(
                    config.consumerKey,
                    config.consumerSecret,
                    config.accessTokenKey,
                    config.accessTokenSecret
            );

            //
            // HBC
            //

            BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);
            Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
            StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
            hosebirdEndpoint.trackTerms(config.track);

            ClientBuilder builder = new ClientBuilder()
                    .name("BUTTERFLYNET")
                    .hosts(hosebirdHosts)
                    .authentication(hosebirdAuth)
                    .endpoint(hosebirdEndpoint)
                    .processor(new StringDelimitedProcessor(msgQueue));

            Client hosebirdClient = builder.build();

            hosebirdClient.connect();

            new Thread(
                    () -> {
                        while (!hosebirdClient.isDone()) {
                            try {
                                // Parse incoming JSON
                                String msg = msgQueue.take();
                                Object o = com.mongodb.util.JSON.parse(msg);
                                DBObject tweet = (DBObject) o;

                                // Insert new tweet in "tweets" collection
                                BasicDBObject dbObject = new BasicDBObject();
                                dbObject.append("rawJson", tweet);
                                tweetsColl.insert(dbObject);

                                // Update "news" collection
                                DBCursor cursor = newsColl.find(new BasicDBObject("id", 1));
                                BasicDBObject latest = new BasicDBObject("id", 1)
                                        .append("created_at", tweet.get("created_at"))
                                        .append("timestamp_ms", tweet.get("timestamp_ms"))
                                        .append("rawJson", tweet);
                                try {
                                    if (cursor.count() == 0) {
                                        newsColl.insert(latest);
                                    } else {
                                        newsColl.update(new BasicDBObject("id", 1), latest);

                                    }
                                } finally {
                                    cursor.close();
                                }

                                // Print
                                System.out.println(new StringBuilder()
                                                .append("ID: ")
                                                .append(tweet.get("id_str"))
                                );
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
            ).start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
