package com.lucid.comparison;

import com.lucid.common.LogUtils;
import com.lucid.common.Pair;
import com.lucid.common.Utils;
import io.atomix.catalyst.transport.Address;

import java.io.BufferedOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class BroadcastPaxosTest {

    private static int STARTING_PORT = 9000;

    private static int CLIENT_PORT = 8000;

    // args[0]: Number of paxos cycles
    public static void main(String[] args) {
        LogUtils.setCopycatLogLevel(LogUtils.LogLevel.ERROR);

        // Create addresses
        int clusterSize = Integer.parseInt(args[0]);
        final List<Address> addresses = new ArrayList<>();
        for (int i = 0; i < clusterSize; ++i) {
            addresses.add(new Address("127.0.0.1", STARTING_PORT + i));
        }

        // Start PaxosServers
        for (int i = 0; i < clusterSize; ++i) {
            final int index = i;
            final Role role = i == 0 ? Role.LEADER : Role.FOLLOWER;
            Utils.startThreadWithName(() -> new PaxosServer(addresses, role, index), "server-" + i);
        }

        // Wait for servers to fully connect
        Utils.sleepQuietly(5000);

        // Start Client
        try {
            PaxosClient paxosClient = new PaxosClient(addresses.get(0));

            System.out.println("Client started");
            paxosClient.send(Message.MSG);
            System.out.println("Client done");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private enum Message {
        PREP, ACK, COMMIT, MSG
    }

    private enum Role {
        LEADER, FOLLOWER
    }

    private static class PaxosServer {
        private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
        private List<Address> addresses;
        private Map<Address, ObjectOutputStream> objectOutputStreamMap;
        private Map<Integer, Semaphore> semaphoreMap;
        private Role role;
        private int index;
        private AtomicInteger atomicInteger = new AtomicInteger();

        public PaxosServer(List<Address> addresses, Role role, int index) {
            this.addresses = addresses;
            this.objectOutputStreamMap = new ConcurrentHashMap<>();
            this.semaphoreMap = new ConcurrentHashMap<>();
            this.role = role;
            this.index = index;

            // Connect to all other servers
            for (Address address : addresses) {
                new Connect(address);
            }

            // Open Server port
            Utils.startThreadWithName(() -> {
                try {
                    ServerSocket ss = new ServerSocket(addresses.get(index).port());
                    while (true) {
                        handleServer(ss.accept());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }, "server-accept");

            // Open Client port
            if (role == Role.LEADER) {
                Utils.startThreadWithName(() -> {
                    try {
                        ServerSocket ss = new ServerSocket(CLIENT_PORT);
                        while (true) {
                            handleClient(ss.accept());
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }, "client-accept");
            }
        }

        private class Connect implements Runnable {
            private Address address;

            public Connect(Address address) {
                this.address = address;
                executorService.submit(this);
            }

            @Override
            public void run() {
                Socket socket = null;
                try {
                    socket = new Socket(address.host(), address.port());
                    objectOutputStreamMap.put(address, new ObjectOutputStream(new BufferedOutputStream(socket.getOutputStream())));
                    if (objectOutputStreamMap.size() == addresses.size()) {
                        System.out.println(index + ": fully connected");
                    }
                } catch (Exception e) {
                    System.out.println("Retrying...");
                    Utils.closeQuietly(socket);
                    executorService.schedule(this, 500, TimeUnit.MILLISECONDS);
                }
            }
        }

        private void handleServer(Socket socket) {
            Runnable runnable = () -> {
                try {
                    ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                    ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                    handleMessage(oos, (Pair<Integer, Message>) ois.readObject());
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    Utils.closeQuietly(socket);
                }
            };
            Utils.startThreadWithName(runnable, "handle-server");
        }

        private void handleClient(Socket socket) {
            Runnable runnable = () -> {
                try {
                    ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                    ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                    Message msg = (Message) ois.readObject();
                    final int id = atomicInteger.getAndIncrement();
                    final Semaphore semaphore = new Semaphore(0);
                    semaphoreMap.put(id, semaphore);

                    // Start paxos prepare
                    for (Address address : addresses) {
                        Utils.startThreadWithName(() -> {
                            try {
                                objectOutputStreamMap.get(address).writeObject(new Pair<>(id, Message.PREP));
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }, "send-paxos-prepare");
                    }

                    // Wait for majority
                    semaphore.acquire(addresses.size() / 2 + 1);

                    // Send paxos commit
                    for (Address address : addresses) {
                        Utils.startThreadWithName(() -> {
                            try {
                                objectOutputStreamMap.get(address).writeObject(new Pair<>(id, Message.COMMIT));
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }, "send-paxos-commit");
                    }

                    // Send client success
                    oos.writeObject(Message.MSG);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    Utils.closeQuietly(socket);
                }
            };
            Utils.startThreadWithName(runnable, "handle-client");
        }

        private void handleMessage(ObjectOutputStream oos, Pair<Integer, Message> pair) throws Exception {
            switch (pair.getSecond()) {
                case PREP:
                    oos.writeObject(new Pair<>(pair.getFirst(), Message.ACK));
                    oos.flush();
                    break;
                case ACK:
                    semaphoreMap.get(pair.getFirst()).release();
                    break;
                case COMMIT:
                    // Okay. Nothing to do
                    break;
            }
        }
    }

    private static class PaxosClient {
        private ObjectOutputStream oos;
        private ObjectInputStream ois;

        public PaxosClient(Address address) {
            try {
                Socket socket = new Socket(address.host(), CLIENT_PORT);
                this.oos = new ObjectOutputStream(socket.getOutputStream());
                this.ois = new ObjectInputStream(socket.getInputStream());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void send(Message msg) throws Exception {
            oos.writeObject(msg);
            Message result = (Message) ois.readObject();
            if (result == null) {
                throw new Exception("Send failed");
            }
        }
    }
}
