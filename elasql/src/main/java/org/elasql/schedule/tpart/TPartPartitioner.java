package org.elasql.schedule.tpart;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.procedure.tpart.TPartStoredProcedure;
import org.elasql.procedure.tpart.TPartStoredProcedure.ProcedureType;
import org.elasql.procedure.tpart.TPartStoredProcedureFactory;
import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.schedule.Scheduler;
import org.elasql.schedule.tpart.graph.Edge;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.schedule.tpart.graph.TxNode;
import org.elasql.schedule.tpart.sink.Sinker;
import org.elasql.server.Elasql;
import org.elasql.sql.PrimaryKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.storage.tx.recovery.DdRecoveryMgr;
import org.elasql.util.ElasqlProperties;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;

public class TPartPartitioner extends Task implements Scheduler {
	private static Logger logger = Logger.getLogger(TPartPartitioner.class.getName());

	private static final int NUM_TASK_PER_SINK;

	private TPartStoredProcedureFactory factory;
	
//	private File dumpDir = new File("batch_dump");

	static {
		NUM_TASK_PER_SINK = ElasqlProperties.getLoader()
				.getPropertyAsInteger(TPartPartitioner.class.getName() + ".ROUTING_BATCH", 10);
	}

	private BlockingQueue<StoredProcedureCall> spcQueue;
	private BatchNodeInserter inserter;
	private Sinker sinker;
	private TGraph graph;
	private boolean batchingEnabled = true;

	public TPartPartitioner(TPartStoredProcedureFactory factory, 
			BatchNodeInserter inserter, Sinker sinker, TGraph graph) {
		this(factory, inserter, sinker, graph, true);
	}
	
	public TPartPartitioner(TPartStoredProcedureFactory factory, 
			BatchNodeInserter inserter, Sinker sinker, TGraph graph,
			boolean isBatching) {
		this.factory = factory;
		this.inserter = inserter;
		this.sinker = sinker;
		this.graph = graph;
		this.batchingEnabled = isBatching;
		this.spcQueue = new LinkedBlockingQueue<StoredProcedureCall>();
		
		// Clear the dump dir
//		dumpDir.mkdirs();
//		for (File file : dumpDir.listFiles()) {
//			if (file.isFile())
//				file.delete();
//		}
	}

	public void schedule(StoredProcedureCall call) {
		try {
			spcQueue.put(call);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void run() {
		List<TPartStoredProcedureTask> batchedTasks = new LinkedList<TPartStoredProcedureTask>();
		
		while (true) {
			try {
				// blocked if the queue is empty
				StoredProcedureCall call = spcQueue.take();
				TPartStoredProcedureTask task = createStoredProcedureTask(call);

				// schedules the utility procedures directly without T-Part
				// module
				if (task.getProcedureType() == ProcedureType.UTILITY) {
//					VanillaDb.taskMgr().runTask(task);
					continue;
				}

				// TODO: Uncomment this when the migration module is migrated
//				if (task.getProcedureType() == ProcedureType.MIGRATION) {
//					// Process and dispatch it immediately
//					processMigrationTx(task);
//					continue;
//				}

				if (task.getProcedureType() == ProcedureType.NORMAL) {
					batchedTasks.add(task);
				}
				
				// sink current t-graph if # pending tx exceeds threshold
				if ((batchingEnabled && batchedTasks.size() >= NUM_TASK_PER_SINK)
						|| !batchingEnabled) {
					processBatch(batchedTasks);
					batchedTasks.clear();
				}

			} catch (InterruptedException ex) {
				if (logger.isLoggable(Level.SEVERE))
					logger.severe("fail to dequeue task");
			}
		}
	}
	
	private void processBatch(List<TPartStoredProcedureTask> batchedTasks) {
		
		// for reordering, find hot record first
				HashMap<PrimaryKey, Integer> read_counts = new HashMap<PrimaryKey, Integer>();
				PrimaryKey hotRecord = null;
				for (TPartStoredProcedureTask task : batchedTasks) {
					for (PrimaryKey res : task.getReadSet()) {

						if (read_counts.containsKey(res)) {
							read_counts.put(res, (int) read_counts.get(res) + 1);
						} else {
							read_counts.put(res, 1);
						}
					}

				}
				Entry<PrimaryKey, Integer> maxEntry = null;

				for (Entry<PrimaryKey, Integer> entry : read_counts.entrySet()) {
					if (maxEntry == null || entry.getValue() > maxEntry.getValue()) {
						maxEntry = entry;
					}
				}
				hotRecord = maxEntry.getKey();

				// get the tasks with hotrecord on their writes
				List<TPartStoredProcedureTask> hot_write_tasks = new LinkedList<TPartStoredProcedureTask>();
				for (TPartStoredProcedureTask task : batchedTasks) {
					for (PrimaryKey write : task.getWriteSet()) {
						if ((write == hotRecord) && (!hot_write_tasks.contains(task))) {
							hot_write_tasks.add(task);
						}
					}
				}
				// take hot_write_tasks out of tasks, insert hot_write_tasks to according
				// partition

		for (TPartStoredProcedureTask task : hot_write_tasks) {
			insertToResourcePart(graph, task, hotRecord);
		}
		batchedTasks.removeAll(hot_write_tasks);

		// add a task that replicates the hot record to every partition
		ReplicateResource(graph, hotRecord);		
		
		// Insert the batch of tasks
		inserter.insertBatch(graph, batchedTasks);
		
		// Debug
//		printGraphStatistics();
//		System.out.println(graph);
//		printStatistics();
//		printImbalStatistics();
//		collectGraphStatistics();
		
		// Sink the graph
		if (graph.getTxNodes().size() != 0) {
			Iterator<TPartStoredProcedureTask> plansTter = sinker.sink(graph);
			dispatchToTaskMgr(plansTter);
		}
		
		Elasql.partitionMetaMgr().setHotRecord(null);
	}
	
	private void insertToResourcePart(TGraph graph, TPartStoredProcedureTask task, PrimaryKey resource) {
		int part = graph.getResourcePosition(resource).getPartId();
		graph.insertTxNode(task, part);
	}
	
	private void ReplicateResource(TGraph graph, PrimaryKey resource) {
		// TODO: replicate resource
		Object[] par = new Object[1];
		par[0] = resource;
		for (int partId = 0; partId < Elasql.partitionMetaMgr().getCurrentNumOfParts(); partId++) {
			StoredProcedureCall sp = new StoredProcedureCall(-1, -1, 4, par);
			TPartStoredProcedureTask task = createStoredProcedureTask(sp);
			graph.insertTxNode(task, partId);
		}
		Elasql.partitionMetaMgr().setHotRecord(resource);
	}
	
	// TODO: Uncomment this when the migration module is migrated
//	private void processMigrationTx(TPartStoredProcedureTask task) {
//		// Insert the task to T-Graph
//		ColdMigrationProcedure sp = (ColdMigrationProcedure) task.getProcedure();
//		graph.insertTxNode(task, sp.getMigrationRange().getDestPartId());
//		
//		// Sink the graph
//		Iterator<TPartStoredProcedureTask> plansTter = sinker.sink(graph);
//		dispatchToTaskMgr(plansTter);
//	}

	private TPartStoredProcedureTask createStoredProcedureTask(StoredProcedureCall call) {
		if (call.isNoOpStoredProcCall()) {
			return new TPartStoredProcedureTask(call.getClientId(), call.getConnectionId(), call.getTxNum(), null);
		} else {
			TPartStoredProcedure<?> sp = factory.getStoredProcedure(call.getPid(), call.getTxNum());
			sp.prepare(call.getPars());

			if (!sp.isReadOnly())
				DdRecoveryMgr.logRequest(call);

			return new TPartStoredProcedureTask(call.getClientId(), call.getConnectionId(), call.getTxNum(), sp);
		}
	}

	private void dispatchToTaskMgr(Iterator<TPartStoredProcedureTask> plans) {
		while (plans.hasNext()) {
			TPartStoredProcedureTask p = plans.next();
			VanillaDb.taskMgr().runTask(p);
		}
	}
	
	private int batchId = 0;
	private int imbalanced = 0;
	private int remoteTxRead = 0;
	private int remoteSinkRead = 0;
	private int recordCount = 0;
	private long nextReportTime = 0;
	
	private void printGraphStatistics() {
		// XXX: Show the statistics of the T-Graph
		long time = (System.currentTimeMillis() - Elasql.START_TIME_MS) / 1000;
//		if (batchId % 100 == 0) {
//			String stat = graph.getStatistics();
//			System.out.println("Time: " + time);
//			System.out.println("T-Graph id: " + (batchId + 1));
//			System.out.print(stat);
			
			imbalanced += graph.getImbalancedDis();
			remoteTxRead += graph.getRemoteTxReads();
			remoteSinkRead += graph.getRemoteSinkReads();
			recordCount++;
			
			if (time >= nextReportTime) {
//				System.out.println("======== Total Statistics ========");
				System.out.println(String.format("Time: %d, avg. imbal: %f, avg. remote tx reads: %f, "
						+ "avg. remote sink reads: %f", time, ((double) imbalanced) / recordCount,
						((double) remoteTxRead) / recordCount, ((double) remoteSinkRead) / recordCount));
//				System.out.println("==================================\n");
				
				imbalanced = 0;
				remoteTxRead = 0;
				remoteSinkRead = 0;
				recordCount = 0;
				nextReportTime = time + 5;
				
				// Dump the current graph
//				GraphDumper.dumpToFile(new File(dumpDir, String.format("%d_%d.txt", time, batchId)), graph);
			}
//		}
		batchId++;
	}
	
	private int[] numberOfRemoteReads = new int[PartitionMetaMgr.NUM_PARTITIONS];
	private int[] numberOfTxns = new int[PartitionMetaMgr.NUM_PARTITIONS];
	private int[] numberOfDistTxns = new int[PartitionMetaMgr.NUM_PARTITIONS];
	private double[] ratioOfDistTxns = new double[PartitionMetaMgr.NUM_PARTITIONS];
	
	private void collectGraphStatistics() {
		long time = (System.currentTimeMillis() - Elasql.START_TIME_MS) / 1000;
		
		// collects
		for (TxNode node : graph.getTxNodes()) {
			int masterId = node.getPartId();
			boolean distTx = false;
			
			// count remote reads
			for (Edge e : node.getReadEdges()) {
				int resPartId = e.getTarget().getPartId();
				if (masterId != resPartId) {
					numberOfRemoteReads[masterId]++;
					distTx = true;
				}
			}
			
			// count transactions
			numberOfTxns[masterId]++;
			if (distTx) {
				numberOfDistTxns[masterId]++;
			}
		}
		
		// prints every period of time
		if (time >= nextReportTime) {
			System.out.println(String.format("Time: %d", time));
			
			// Count the % of dist. transactions
			for (int i = 0; i < ratioOfDistTxns.length; i++) {
				if (numberOfTxns[i] == 0) {
					ratioOfDistTxns[i] = 0.0;
				} else {
					ratioOfDistTxns[i] = numberOfDistTxns[i];
					ratioOfDistTxns[i] /= numberOfTxns[i];
				}
			}
			
			// # of transactions
			System.out.println(String.format("Number of transactions: %s.",
					Arrays.toString(numberOfTxns)));
			Arrays.fill(numberOfTxns, 0);
			
			// # of dist transactions
			System.out.println(String.format("Number of dist. transactions: %s.",
					Arrays.toString(numberOfDistTxns)));
			Arrays.fill(numberOfDistTxns, 0);
			
			// % of dist transactions
			System.out.println(String.format("Ratio of dist. transactions: %s.",
					Arrays.toString(ratioOfDistTxns)));
			
			// # of remote reads
			System.out.println(String.format("Number of remote reads: %s.",
					Arrays.toString(numberOfRemoteReads)));
			Arrays.fill(numberOfRemoteReads, 0);
			
			nextReportTime = time + 15;
		}
	}
	
	private int totalImbalanced = 0;
	private int numOfBatches = 0;
	
	private void printImbalStatistics() {
		long time = (System.currentTimeMillis() - Elasql.START_TIME_MS) / 1000;
		
		numOfBatches++;
		totalImbalanced += graph.getImbalancedDis();
		
		if (time >= nextReportTime) {
			double averageImb = totalImbalanced;
			averageImb /= numOfBatches;
			System.out.println(String.format("Average imbalanced: %f at time %d.",
					averageImb, time));
			
			numOfBatches = 0;
			totalImbalanced = 0;
			nextReportTime = time + 3;
		}
	}
}
