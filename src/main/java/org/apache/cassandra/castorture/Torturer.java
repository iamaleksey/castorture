/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.castorture;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class Torturer
{
    private static final String KS = "torture";
    private static final String CF = "session";

    private static final OptionParser parser = new OptionParser() {{
        accepts("h",  "Show this help message");
        accepts("n",  "Number of elements to add").withRequiredArg().ofType(Integer.class).defaultsTo(1000);
        accepts("c",  "Number of concurrent updaters").withRequiredArg().ofType(Integer.class).defaultsTo(5);
        accepts("r",  "Maximum retries per element").withRequiredArg().ofType(Integer.class).defaultsTo(10);
        accepts("ip", "The initial node ip to connect to").withRequiredArg().ofType(String.class).defaultsTo("127.0.0.1");
    }};

    public static void main(String[] args)
    {
        OptionSet options = parseOptions(args);

        int numElements = (Integer) options.valueOf("n");
        int numUpdaters = (Integer) options.valueOf("c");
        int retries = (Integer) options.valueOf("r");
        String ip = (String) options.valueOf("ip");

        System.out.println("Initializing CAS Torture...");
        System.out.println("Elements to add: " + numElements);
        System.out.println("Number of concurrent updaters: " + numUpdaters);
        System.out.println("Maximum retries per element: " + retries);
        System.out.println("Initial node ip: " + ip);
        System.out.println();

        Queue<Integer> queue = new ConcurrentLinkedDeque<>();
        for (int i = 0; i < numElements; i++)
            queue.add(i);

        try
        {
            Cluster cluster = new Cluster.Builder().addContactPoints(ip).build();
            Session session = cluster.connect();

            PreparedStatement update = setup(session);

            Set<Integer> acked = Sets.newConcurrentHashSet();

            List<Updater> updaters = new ArrayList<>(numUpdaters);
            for (int i = 0; i < numUpdaters; i++)
                updaters.add(new Updater(session, update, queue, acked, retries));

            long start = System.currentTimeMillis();

            for (Updater updater : updaters)
                updater.start();

            for (Updater updater : updaters)
                updater.join();

            System.out.println("\nCollecting results.");
            Set<Integer> survived = collectSurvived(session);
            System.out.println(survived);
            long delta = System.currentTimeMillis() - start;
            System.out.println("Writes completed in " + delta / 1000.0 + " seconds\n");

            printReport(numElements, acked, collectSurvived(session));

            System.exit(0);
        }
        catch (NoHostAvailableException e)
        {
            System.err.println("No alive hosts to use: " + e.getMessage());
            System.exit(1);
        }
        catch (Exception e)
        {
            System.err.println("Unexpected error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static Set<Integer> collectSurvived(Session session)
    {
        Row result = session.execute(QueryBuilder.select("elements")
                                                 .from(KS, CF)
                                                 .where(eq("id", 0))
                                                 .setConsistencyLevel(ConsistencyLevel.ALL))
                            .one();

        Set<Integer> survived = new TreeSet<>();
        for (String element : result.getString("elements").split(";"))
            survived.add(Integer.valueOf(element));
        return survived;
    }

    private static void printReport(int numElements, Set<Integer> acked, Set<Integer> survived)
    {
        System.out.println(numElements + " total");
        System.out.println(acked.size() + " acknowledged");
        System.out.println(survived.size() + " survivors");

        Set<Integer> lost = Sets.difference(acked, survived);
        if (!lost.isEmpty())
        {
            System.out.println(lost.size() + " acknowledged writes lost! (╯°□°）╯︵ ┻━┻");
            System.out.println(lost);
        }

        Set<Integer> unacked = Sets.difference(survived, acked);
        if (!unacked.isEmpty())
        {
            System.out.println(unacked.size() + " unacknowledged writes found! ヽ(´ー｀)ノ");
            System.out.println(unacked);
        }

        System.out.println(acked.size() / (double) numElements + " ack rate");
        System.out.println(lost.size() / (double) numElements + " loss rate");
        System.out.println(unacked.size() / (double) numElements + " unacknowledged but successful rate");
    }

    private static class Updater
    {
        private final Runner runner = new Runner();

        private final Session session;
        private final PreparedStatement update;
        private final Queue<Integer> queue;
        private final Set<Integer> acked;
        private final int maxRetries;

        public Updater(Session session, PreparedStatement update, Queue<Integer> queue, Set<Integer> acked, int maxRetries)
        {
            this.session = session;
            this.update = update;
            this.queue = queue;
            this.acked = acked;
            this.maxRetries = maxRetries;
            this.runner.setDaemon(true);
        }

        public void start()
        {
            this.runner.start();
        }

        public void join()
        {
            Uninterruptibles.joinUninterruptibly(this.runner);
        }

        private class Runner extends Thread
        {
            @Override
            public void run()
            {
                while (true)
                {
                    Integer next = queue.poll();
                    if (next == null)
                        break;
                    writeElement(next);
                }
            }

            private void writeElement(int element)
            {
                String current = null;
                int tries = 0;
                while (true) // if beaten, try again
                {
                    if (tries > maxRetries)
                    {
                        System.out.println(element + " fail");
                        break;
                    }
                    tries++;

                    if (current == null)
                    {
                        current = session.execute(QueryBuilder.select("elements")
                                                              .from(KS, CF)
                                                              .where(eq("id", 0))
                                                              .setConsistencyLevel(ConsistencyLevel.SERIAL))
                                         .one()
                                         .getString("elements");
                    }

                    if (Sets.newHashSet(current.split(";")).contains(String.valueOf(element)))
                        break; // we've timed out, but have actually written the value successfully.

                    String next = current.equals("") ? String.valueOf(element) : current + ";" + element;
                    Row result;
                    try
                    {
                        result = session.execute(update.bind(next, current)).one();
                    }
                    catch (WriteTimeoutException e)
                    {
                        current = null;
                        continue;
                    }

                    if (result.getBool("[applied]"))
                    {
                        acked.add(element);
                        System.out.println(element + " ok");
                        break;
                    }
                    current = result.getString("elements");
                }
            }
        }
    }

    private static PreparedStatement setup(Session session)
    {
        System.out.println("Creating schema and setting the initial elements value...");
        session.execute("DROP KEYSPACE IF EXISTS torture");
        session.execute("CREATE KEYSPACE torture WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }");
        session.execute("USE torture");
        session.execute("CREATE TABLE session (id int PRIMARY KEY, elements text)");
        session.execute(QueryBuilder.insertInto(KS, CF)
                                    .values(new String[]{ "id", "elements" },
                                            new Object[]{ 0, "" })
                                    .setConsistencyLevel(ConsistencyLevel.QUORUM));
        return session.prepare(QueryBuilder.update(KS, CF)
                                           .with(set("elements", bindMarker()))
                                           .where(eq("id", 0))
                                           .onlyIf(eq("elements", bindMarker()))
                                           .toString());
    }

    private static void printHelp()
    {
        System.out.println("Usage: torture [<option>]*\n");

        try
        {
            parser.printHelpOn(System.out);
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }
    }

    private static OptionSet parseOptions(String[] args)
    {
        try
        {
            OptionSet options = parser.parse(args);
            if (options.has("h"))
            {
                printHelp();
                System.exit(0);
            }
            return options;
        }
        catch (Exception e)
        {
            System.err.println("Error parsing options: " + e.getMessage());
            printHelp();
            System.exit(1);
            throw new AssertionError();
        }
    }
}
