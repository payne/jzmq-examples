package org.zeromq.examples.loadbalancer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;

import org.apache.log4j.Logger;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQ.Socket;

/**
 * ZeroMQ Worker that contains units of capacity (threads) and requests work
 * via a REQ socket.
 * 
 * @author Stephen Riesenberg
 */
public class Worker implements Runnable {
	private static final Logger log = Logger.getLogger(Worker.class);
	private static Random rand = new Random(System.currentTimeMillis());
	private static final byte[] EMPTY_FRAME = new byte[0];
	
	private ZContext context;
	private Socket frontend;
	private Socket backend;
	
	private int workerId = 0;
	private String identity;
	private String endpoint = "tcp://localhost:5555";
	private Queue<String> workerQueue = new LinkedList<String>();
	
	/**
	 * Constructor.
	 */
	public Worker() {
		this.context = new ZContext();
		this.identity = getIdentity();
	}
	
	/**
	 * Constructor.
	 * 
	 * @param context An existing ZContext to reuse (shadow)
	 */
	public Worker(ZContext context) {
		this.context = ZContext.shadow(context);
		this.identity = getIdentity();
	}
	
	private String getIdentity() {
		String hostName;
		try {
			hostName = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException ex) {
			throw new Error(ex);
		}
		
		return hostName;
	}
	
	public void setIdentity(String identity) {
		this.identity = identity;
	}
	
	private String getRandomIdentity() {
		return String.format("%s-%04X", getIdentity(), Integer.valueOf(rand.nextInt()));
	}
	
	public void setRandomIdentity() {
		identity = getRandomIdentity();
	}
	
	public void setHost(String host) {
		this.endpoint = String.format("tcp://%s:5555", host);
	}
	
	public void setEndpoint(String endpoint) {
		this.endpoint = endpoint;
	}
	
	public void setIoThreads(int ioThreads) {
		context.setIoThreads(ioThreads);
	}
	
	/**
	 * Create and return a pre-configured REQ socket for communicating with
	 * this Worker's backend ROUTER socket, which proxies to a Master.
	 * 
	 * @return An inproc REQ socket
	 */
	public Socket createSocket() {
		String inproc = String.format("inproc://backend-%d", Integer.valueOf(hashCode()));
		String identity = String.valueOf(++workerId);
		log.info(String.format("F: Connecting REQ socket to %s with identity %s", inproc, identity));
		Socket socket = context.createSocket(ZMQ.REQ);
		socket.setIdentity(identity.getBytes());
		socket.connect(inproc);
		
		return socket;
	}
	
	@Override
	public void run() {
		// connect frontend socket
		log.info(String.format("F: Connecting DEALER socket to %s with identity %s", endpoint, identity));
		frontend = context.createSocket(ZMQ.DEALER);
		frontend.setIdentity(identity.getBytes());
		frontend.connect(endpoint);
		
		// bind backend socket
		String inproc = String.format("inproc://backend-%d", Integer.valueOf(hashCode()));
		log.info(String.format("B: Binding ROUTER socket to %s", inproc));
		backend = context.createSocket(ZMQ.ROUTER);
		backend.bind(inproc);
		
		// start main loop
		Poller poller = new Poller(2);
		poller.register(frontend, Poller.POLLIN);
		poller.register(backend, Poller.POLLIN);
		while (!Thread.currentThread().isInterrupted()) {
			if (poller.poll() < 0)
				break;
			
			if (poller.pollin(0))
				receiveFromMaster();
			
			if (poller.pollin(1))
				receiveFromThread();
		}
		
		log.info(String.format("Worker with identity %s has shut down", identity));
	}
	
	private void receiveFromMaster() {
		frontend.recv(); // empty frame
		String workerThread = workerQueue.poll();
		backend.send(workerThread, ZMQ.SNDMORE);
		backend.send(EMPTY_FRAME, ZMQ.SNDMORE);
		backend.send(frontend.recv(), 0);
	}
	
	private void receiveFromThread() {
		String workerThread = backend.recvStr();
		backend.recv(); // empty frame from REQ socket
		String command = backend.recvStr();
		switch (command) {
			case "READY":
				workerQueue.add(workerThread);
				frontend.send(EMPTY_FRAME, ZMQ.SNDMORE);
				frontend.send("READY");
				break;
		}
	}
	
	/**
	 * Destroy this Worker's ZMQ context.
	 */
	public void destroy() {
		context.destroy();
	}
}
