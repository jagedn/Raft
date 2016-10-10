package recipesService.raft;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import communication.DSException;
import communication.rmi.RMIsd;
import recipesService.CookingRecipes;
import recipesService.activitySimulation.ActivitySimulation;
import recipesService.communication.Host;
import recipesService.data.AddOperation;
import recipesService.data.Operation;
import recipesService.data.OperationType;
import recipesService.data.RemoveOperation;
import recipesService.raft.dataStructures.Index;
import recipesService.raft.dataStructures.LogEntry;
import recipesService.raft.dataStructures.PersistentState;
import recipesService.raftRPC.AppendEntriesResponse;
import recipesService.raftRPC.RequestVoteResponse;
import recipesService.test.client.RequestResponse;

public abstract class RaftConsensus extends CookingRecipes implements Raft {

	// current server
	private Host localHost;

	//
	// STATE
	//

	// raft persistent state state on all servers
	protected PersistentState persistentState;

	// raft volatile state on all servers
	private int commitIndex; // index of highest log entry known to be committed
								// (initialized to 0, increases monotonically)
	private int lastApplied; // index of highest log entry applied to state
								// machine (initialized to 0, increases
								// monotonically)

	// other
	private RaftState state = RaftState.FOLLOWER;

	// Leader
	private String leader;

	// Leader election
	private Timer electionTimeoutTimer; // timer for election timeout
	private long electionTimeout; // period of time that a follower receives no
									// communication.
									// If a timeout occurs, it assumes there is
									// no viable leader.
	private Set<Host> receivedVotes; // contains hosts that have voted for this
										// server as candidate in a leader
										// election

	//
	// LEADER
	//

	// Volatile state on leaders
	private Index nextIndex; // for each server, index of the next log entry to
								// send to that server (initialized to leader
								// last log index + 1)
	private Index matchIndex; // for each server, index of highest log known to
								// be replicated on server (initialized to 0,
								// increases monotonically)

	// Heartbeats on leaders
	private Timer leaderHeartbeatTimeoutTimer;
	private long leaderHeartbeatTimeout;

	//
	// CLUSTER
	//

	// general
	private int numServers; // number of servers in a Raft cluster.
							// 5 is a typical number, which allows the system to
							// tolerate two failures
	// partner servers
	private List<Host> otherServers; // list of partner servers (localHost not
										// included in the list)

	//
	// UTILS
	//

	static Random rnd = new Random();

	// =======================
	// === IMPLEMENTATION
	// =======================
	// =======================
	// === IMPLEMENTATION
	// =======================
	static final int TRACE = 100;
	static final int DEBUG = 50;
	static final int INFO = 5;
	static final int SEVERE = 0;
	static final int NONE = 0;

	static final java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat(
			"HH:mm:ss.S");

	static final int levelDebug = INFO;
	java.io.FileWriter fDebug = null;

	public void debug(int debug, String msg) {
		String line = String.format("%s %s", sdf.format(new java.util.Date()),
				msg);
		if (debug <= levelDebug) {
			System.out.println(line);
		}
		if (fDebug != null) {
			try {
				fDebug.write(line + "\n");
				fDebug.flush();
			} catch (IOException e) {
			}
		}
	}

	public RaftConsensus(long electionTimeout) { // electiontimeout is a
													// parameter in
													// config.properties file
		// set electionTimeout
		this.electionTimeout = electionTimeout;

		// set leaderHeartbeatTimeout
		this.leaderHeartbeatTimeout = electionTimeout / 3; // TODO: Cal
															// revisar-ne el
															// valor

		// Logs
		try {
			if (System.getProperty("FILE") != null)
				fDebug = new java.io.FileWriter(System.getProperty("FILE"),
						false);
		} catch (IOException e) {
		}

	}

	// sets localhost and other servers participating in the cluster
	protected void setServers(Host localHost, List<Host> otherServers) {

		this.localHost = localHost;

		// initialize persistent state on all servers
		persistentState = new PersistentState();

		// set servers list
		this.otherServers = otherServers;
		numServers = otherServers.size() + 1;

		// JAG-JCS
		// start timers
		if (electionTimeoutTimer != null)
			electionTimeoutTimer.cancel();
		electionTimeoutTimer = new Timer();
		electionTimeoutTimer.schedule(new TimerTask() {
			@Override
			public void run() {
				doTask();
			}
		}, electionTimeout, electionTimeout
				* (1 + (long) (2 * rnd.nextDouble())));

	}

	// connect
	public void connect() {
		/*
		 * ACTIONS TO DO EACH TIME THE SERVER CONNECTS (i.e. when it starts or
		 * after a failure or disconnection)
		 */

		// JAG-JSC
		debug(SEVERE, "Connected " + getServerId());
		synchronized (guard) {
			state = RaftState.FOLLOWER;
			persistentState.removeVotedFor();
			leader = null;
			connected.set(true);
		}

	}

	public void disconnect() {
		// electionTimeoutTimer.cancel();
		// if (localHost.getId().equals(leader)){
		// leaderHeartbeatTimeoutTimer.cancel();
		// }

		// JAG-JSC
		debug(SEVERE, "DisConnected");
		synchronized (guard) {
			connected.set(false);
		}

	}

	/*
	 * indicar si estamos conectados o no
	 */
	AtomicBoolean connected = new AtomicBoolean(false);

	/*
	 * Sincronizamos y notificamos en guard
	 */
	final Boolean guard = new Boolean(false);
	final AtomicBoolean guardAppend = new AtomicBoolean(false);

	/*
	 * utilidad para mantener un pool de hilos corriendo
	 */
	Executor executorThreadPool = Executors.newCachedThreadPool();

	/*
	 * contador de Cuantos hilos hay corriendo en un momento dado
	 */
	final AtomicInteger rpcRunning = new AtomicInteger();

	/*
	 * Booleano para terminar todos los hilos rapidamente
	 */
	final AtomicBoolean bAtomicEndDialog = new AtomicBoolean();

	/*
	 * Booleano para indicar el tipo de appendEntrie a enviar
	 */
	final AtomicBoolean bAppendHeartbeat = new AtomicBoolean(false);

	/*
	 * sin noticias de gurb
	 */
	final AtomicBoolean gurb = new AtomicBoolean();

	//
	// LEADER ELECTION
	//

	/*
	 * Leader election
	 */

	//
	// LOG REPLICATION
	//

	/*
	 * Log replication
	 */

	//
	// API
	//

	@Override
	public RequestVoteResponse requestVote(long term, String candidateId,
			int lastLogIndex, long lastLogTerm) throws RemoteException {

		synchronized (guard) {
			if (term < persistentState.getCurrentTerm()) {
				debug(INFO, String.format(
						"No le doy mi voto por term bajo %d < %d", term,
						persistentState.getCurrentTerm()));
				return new RequestVoteResponse(
						persistentState.getCurrentTerm(), false);
			}

			if (term == persistentState.getCurrentTerm() && lastLogIndex != 0
					&& lastLogIndex <= persistentState.getLastLogIndex()) {
				debug(INFO, String.format(
						"No le doy mi voto por term y log igual %d < %d", term,
						persistentState.getCurrentTerm()));
				return new RequestVoteResponse(
						persistentState.getCurrentTerm(), false);
			}

			if (persistentState.getVotedFor() != null
					&& persistentState.getVotedFor().equals(candidateId) == false) {
				debug(INFO,
						String.format(
								"No le doy mi voto a %s pq ya he votado %s en mi term %d y el suyo %d",
								candidateId, persistentState.getVotedFor(),
								persistentState.getCurrentTerm(), term));
				return new RequestVoteResponse(
						persistentState.getCurrentTerm(), false);
			}

			gurb.set(false);
			persistentState.setCurrentTerm(term);
			persistentState.setVotedFor(candidateId);
			state = RaftState.FOLLOWER;

			debug(INFO, String.format("le doy mi voto a %s", candidateId));

			return new RequestVoteResponse(persistentState.getCurrentTerm(),
					true);
		}

	}

	@Override
	public AppendEntriesResponse appendEntries(long term, String leaderId,
			int prevLogIndex, long prevLogTerm, List<LogEntry> entries,
			int leaderCommit) throws RemoteException {
		gurb.set(false);

		synchronized (guard) {
			if (state == RaftState.CANDIDATE) {
				if (this.persistentState.getCurrentTerm() > term) {
					debug(DEBUG,
							String.format("no acepto su entries pq yo soy mejor"));
					return new AppendEntriesResponse(
							this.persistentState.getCurrentTerm(), false);
				}
				debug(DEBUG,
						String.format("dejo de ser candidato por un leader mejor"));
				state = RaftState.FOLLOWER;
				persistentState.setCurrentTerm(term);
				persistentState.setVotedFor(leaderId);
			}

			if (term < persistentState.getCurrentTerm())
				return new AppendEntriesResponse(
						this.persistentState.getCurrentTerm(), false);

			leader = leaderId;

			// ping from server
			if (prevLogIndex == -1) {
				applyEntries(leaderCommit);
				return new AppendEntriesResponse(
						persistentState.getCurrentTerm(), true);
			}

			List<LogEntry> myLogEntry = getLog();
			if (myLogEntry.size() != 0) {
				debug(DEBUG, String.format("ajusto arrays %d el del server %d",
						myLogEntry.size(), entries.size()));

				while (entries.size() < myLogEntry.size()) {
					persistentState.deleteEntries(myLogEntry.size() - 1);
				}

				debug(DEBUG, String.format("busco inconsistencia"));

				for (int i = 0; i < myLogEntry.size(); i++) {
					if (entries.get(i).getTerm() != myLogEntry.get(i).getTerm()) {
						return new AppendEntriesResponse(entries.get(i)
								.getTerm(), false);
					}
				}
			}
			persistentState.setCurrentTerm(term);

			persistentState.addEntry(entries.get(prevLogIndex).getCommand());

			applyEntries(leaderCommit);

			AppendEntriesResponse ret = new AppendEntriesResponse(term, true);

			debug(INFO, String.format("Comando aceptado. State machine %s",
					getRecipes()));
			return ret;
		}
	}

	void applyEntries(int to) {
		List<LogEntry> log = getLog();
		while (commitIndex < to) {
			Operation operation = log.get(commitIndex).getCommand();
			if (operation.getType() == OperationType.ADD) {
				AddOperation add = (AddOperation) operation;
				addRecipe(add.getRecipe());
			}
			if (operation.getType() == OperationType.REMOVE) {
				RemoveOperation del = (RemoveOperation) operation;
				removeRecipe(del.getRecipeTitle());
			}
			commitIndex++;
		}
	}

	@Override
	public RequestResponse Request(Operation operation) throws RemoteException {

		debug(SEVERE, "Request recibido " + operation);

		try {

			synchronized (guard) {
				if (state != RaftState.LEADER) {
					debug(INFO,
							String.format("redirect request operation to "
									+ leader));
					RequestResponse ret = new RequestResponse(leader, false);
					return ret;
				}
			}

			persistentState.addEntry(operation);
			synchronized(guardAppend){
				guardAppend.set(true);
				debug(DEBUG, "esperando mayoria");
				guardAppend.wait();
							
				// no podemos garantizar mayoria
				if (rpcRunning.get() < otherServers.size() / 2) {
					debug(SEVERE,
							"No se puede garantizar mayoria porque estan caidos");
					synchronized (guard) {
						state = RaftState.FOLLOWER;
						persistentState.removeVotedFor();
					}
					RequestResponse ret = new RequestResponse(null, false);
					return ret;
				}

				commitIndex++;

				debug(DEBUG,
						String.format(
								"apply request operation with index %d. State machine %s",
								commitIndex, getRecipes().toString()));

				if (operation.getType() == OperationType.ADD) {
					AddOperation add = (AddOperation) operation;
					addRecipe(add.getRecipe());
				}
				if (operation.getType() == OperationType.REMOVE) {
					RemoveOperation del = (RemoveOperation) operation;
					removeRecipe(del.getRecipeTitle());
				}

				return new RequestResponse(getServerId(), true);
			}
		} catch (InterruptedException e) {
			debug(SEVERE, e.getLocalizedMessage());
			return new RequestResponse(getServerId(), false);
		}

	}

	//
	// Other methods
	//
	public String getServerId() {
		return localHost.getId();
	}

	public synchronized List<LogEntry> getLog() {
		return persistentState.getLog();
	}

	public long getCurrentTerm() {
		return persistentState.getCurrentTerm();
	}

	public String getLeaderId() {
		return leader;
	}

	// JAG-JSC
	/*
	 * Punto de entrada en cada tick para comprobar nuestro estado
	 */
	void doTask() {

		if (connected.get() == false) {
			return;
		}

		synchronized (guard) {
			if (state == RaftState.FOLLOWER
					&& persistentState.getVotedFor() == null) {
				debug(SEVERE, "Soy follower sin leader comienzo votacion");

				state = RaftState.CANDIDATE;
				persistentState.setVotedFor(getServerId());
				persistentState.nextTerm();
				gurb.set(false);

				receivedVotes = Collections
						.synchronizedSet(new HashSet<Host>());
				rpcRunning.set(otherServers.size());
				for (Host host : otherServers) {
					executorThreadPool.execute(new RequestVoteResponseThread(
							host, persistentState));
				}
				return;
			}

			if (state == RaftState.CANDIDATE) {
				// termino la votacion
				if (rpcRunning.get() < 1) {					
					debug(INFO,
							String.format("Termino la votacion sin leader, repetirla"));
					persistentState.removeVotedFor();
					state = RaftState.FOLLOWER;
					return;
				}

				debug(DEBUG, "Soy candidato y todavia estamos votando");
			}

			if (state == RaftState.LEADER) {
				// si el cliente espera mirar si ya hay mayoria de envios y
				// avisar
				if (guardAppend.get()) {
					if (rpcRunning.get() < otherServers.size() / 2
							|| matchIndex.majorityHigherOrEqual(persistentState
									.getLastLogIndex())) {
						guardAppend.set(false);
						synchronized (guardAppend) {
							guardAppend.notify();
						}
					}
				}
				return;
			}

			if (state == RaftState.FOLLOWER) {
				if (gurb.getAndSet(true)) {
					debug(DEBUG, "Soy follower de " + leader);
					// hay que controlar que nadie nos diga nada
					// desde hace un rato y pasar a candidato
					persistentState.removeVotedFor();
					debug(DEBUG,
							String.format(
									"no se nada del leader paso a follower",
									gurb.get()));
					return;
				}
				return;
			}
		}

	}

	/*
	 * Hilo para enviar nuestra solicitud a un host cuando nos responda veremos
	 * si nos ha votado o no en cualquier cosa decrementamos el numero de hilos
	 * corriendo y el ultimo notifica que ya esta hecha la votacion
	 */
	class RequestVoteResponseThread implements Runnable {

		Host host;
		long term;
		int lastLogIndex;
		long lastLogTerm;
		Thread thread;
		RequestVoteResponse response;

		RequestVoteResponseThread(Host host, PersistentState persistentState) {
			this.host = host;
			term = persistentState.getCurrentTerm();
			lastLogIndex = persistentState.getLastLogIndex();
			lastLogTerm = persistentState.getLastLogTerm();
		}

		public void run() {

			try {
				response = RMIsd.getInstance().requestVote(host, term,
						getServerId(), lastLogIndex, lastLogTerm);
				synchronized (guard) {
					if (state == RaftState.CANDIDATE) {
						if (response.isVoteGranted()) {
							receivedVotes.add(host);
							
							// comprobar si hemos ganado
							if (receivedVotes.size() > otherServers.size() / 2) {
								debug(SEVERE, String.format(
										"Soy leader Servers %d, votos %d",
										otherServers.size(), receivedVotes.size()));

								state = RaftState.LEADER;

								leader = getServerId();

								commitIndex = 0;

								nextIndex = new Index(otherServers,
										persistentState.getLastLogIndex() + 1);

								matchIndex = new Index(otherServers, 0);

								rpcRunning.set(otherServers.size());
								for (Host host : otherServers) {
									executorThreadPool
											.execute(new AppendEntriesResponseThread(
													host));
								}

								electionTimeoutTimer.cancel();
								leaderHeartbeatTimeoutTimer = new Timer();
								leaderHeartbeatTimeoutTimer.scheduleAtFixedRate(
										new TimerTask() {
											@Override
											public void run() {
												// TODO Auto-generated method stub
												doTask();
											}
										}, leaderHeartbeatTimeout,
										leaderHeartbeatTimeout);
							}
							
						} else {
							if (response.getTerm() > term) {
								state = RaftState.FOLLOWER;
								persistentState.setCurrentTerm(response
										.getTerm());
								leader = host.getId();
							}
						}
					}
				}

			} catch (Exception e) {
				debug(DEBUG, e.getMessage());
			}

			// El ultimo que cierre la puerta
			int running = rpcRunning.decrementAndGet();
			debug(DEBUG, String.format("termine, quedan %d", running));
		}

	};

	/*
	 * Utilidad para mantener el dialogo con un host cuando somos leader
	 * esperamos notificacion del hilo principal de una nueva entrada y la
	 * mandamos de forma consistente al host remoto
	 */
	class AppendEntriesResponseThread implements Runnable {

		final Host host;

		AppendEntriesResponseThread(Host host) {
			this.host = host;
		}

		public void run() {
			AppendEntriesResponse response;

			int prevLogIndex = -1;
			List<LogEntry> entries = null;
			LogEntry entry = null;
			long prevLogTerm = -1;
			long currentTerm = -1;

			while (connected.get()) {

				try {
					synchronized (this) {
						wait(leaderHeartbeatTimeout);
					}

					boolean rafaga = true;
					synchronized (guard) {
						if (state != RaftState.LEADER) {
							break;
						}
						entries = getLog();
						prevLogIndex = nextIndex.getIndex(host.getId()) - 1;
						currentTerm = persistentState.getCurrentTerm();
						if (prevLogIndex < entries.size()) {
							entry = entries.get(prevLogIndex);
							prevLogTerm = entry.getTerm();
							rafaga = false;
						}
					}

					if (rafaga) {
						response = RMIsd.getInstance().appendEntries(host,
								currentTerm, getServerId(), -1, -1, entries,
								commitIndex);

						if (response.getTerm() > currentTerm) {
							// esto es raro.
							debug(SEVERE, "Heartbeat, Soy leader en term="
									+ currentTerm + " y " + host.getId()
									+ " dice que su term=" + response.getTerm());

							synchronized (guard) {
								state = RaftState.FOLLOWER;
								leader = null;
								persistentState.removeVotedFor();
								break;
							}

						}
					} else {
						response = RMIsd.getInstance().appendEntries(host,
								currentTerm, getServerId(), prevLogIndex,
								prevLogTerm, entries, commitIndex);
						debug(INFO,
								String.format("Hilo recibido entrie %s", ""
										+ response.isSucceeded()));

						synchronized (guard) {
							if (response.isSucceeded()) {
								nextIndex.inc(host.getId());
								matchIndex.setIndex(host.getId(),
										prevLogIndex + 1);
							} else {
								debug(DEBUG,
										String.format(
												"Hilo buscando un consenso en log en %d",
												prevLogIndex));
								nextIndex.decrease(host.getId());
							}
						}

					}

				} catch (Exception e) {
					// TODO Auto-generated catch block
				}

			}
			rpcRunning.decrementAndGet();
		}
	};
}
