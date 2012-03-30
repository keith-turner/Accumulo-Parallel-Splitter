import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.hadoop.io.Text;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This is a work around for ACCUMULO-348 that parallelizes adding splits to a table.
 */
public class ParallelSplitter {
  
  private static class SplitEnv {
    private Connector conn;
    private String tableName;
    private ExecutorService executor;
    private CountDownLatch latch;
    private AtomicReference<Exception> exception;
    
    SplitEnv(Connector conn, String tableName, ExecutorService executor, CountDownLatch latch, AtomicReference<Exception> exception) {
      this.conn = conn;
      this.tableName = tableName;
      this.executor = executor;
      this.latch = latch;
      this.exception = exception;
    }
  }
  
  private static class SplitTask implements Runnable {
    
    private List<Text> splits;
    private SplitEnv env;
    
    SplitTask(SplitEnv env, List<Text> splits) {
      this.env = env;
      this.splits = splits;
    }
    
    @Override
    public void run() {
      try {
        if (env.exception.get() != null)
          return;
        
        if (splits.size() <= 2) {
          env.conn.tableOperations().addSplits(env.tableName, new TreeSet<Text>(splits));
          for (int i = 0; i < splits.size(); i++)
            env.latch.countDown();
          return;
        }

        int mid = splits.size() / 2;

        // split the middle split point to ensure that child task split different tablets and can therefore
        // run in parallel
        env.conn.tableOperations().addSplits(env.tableName, new TreeSet<Text>(splits.subList(mid, mid + 1)));
        env.latch.countDown();

        env.executor.submit(new SplitTask(env, splits.subList(0, mid)));
        env.executor.submit(new SplitTask(env, splits.subList(mid + 1, splits.size())));

      } catch (Exception e) {
        env.exception.compareAndSet(null, e);
      }
    }
    
  }
  
  public static void addSplits(Connector conn, String tableName, SortedSet<Text> partitionKeys, int numThreads) throws Exception {
    List<Text> splits = new ArrayList<Text>(partitionKeys);
    // should be sorted because we copied from a sorted set, but that makes assumptions about
    // how the copy was done so resort to be sure.
    Collections.sort(splits);

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch latch = new CountDownLatch(splits.size());
    AtomicReference<Exception> exception = new AtomicReference<Exception>(null);
    
    executor.submit(new SplitTask(new SplitEnv(conn, tableName, executor, latch, exception), splits));
    
    while (!latch.await(100, TimeUnit.MILLISECONDS)) {
      if (exception.get() != null) {
        executor.shutdownNow();
        throw exception.get();
      }
    }
    
    executor.shutdown();
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 7) {
      System.err.println("Usage : " + ParallelSplitter.class.getName() + " <instance> <zoo keepers> <table> <user> <pass> <num threads> <file>");
      System.exit(-1);
    }
    
    String instance = args[0];
    String zooKeepers = args[1];
    String table = args[2];
    String user = args[3];
    String pass = args[4];
    int numThreads = Integer.parseInt(args[5]);
    String file = args[6];
    
    TreeSet<Text> splits = new TreeSet<Text>();
    
    Scanner scanner = new Scanner(new File(file));
    while (scanner.hasNextLine()) {
      splits.add(new Text(scanner.nextLine()));
    }

    ZooKeeperInstance zki = new ZooKeeperInstance(instance, zooKeepers);
    Connector conn = zki.getConnector(user, pass);
    
    addSplits(conn, table, splits, numThreads);

  }
  
}
