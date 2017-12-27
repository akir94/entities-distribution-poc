package org.z.distributer;

import io.redisearch.client.Client;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;

import io.vertx.core.http.HttpServer;
import io.vertx.ext.bridge.BridgeEventType;
import io.vertx.ext.bridge.PermittedOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.ext.web.handler.sockjs.BridgeOptions;
import io.vertx.ext.web.handler.sockjs.SockJSHandler;
import io.vertx.ext.web.handler.sockjs.SockJSSocket;
import org.z.distributer.common.ClientSimulator;
import org.z.distributer.common.ClientState;
import org.z.distributer.common.ClientStateListener;
import org.z.distributer.common.Distributer;
import org.z.distributer.util.GsonJsonMessageCodec;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import io.vertx.ext.eventbus.bridge.tcp.TcpEventBusBridge;

public class VertxMain extends AbstractVerticle{

    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(new VertxMain());
    }

    @Override
    public void start(Future<Void> future) {
        HttpServer server = vertx.createHttpServer();

        Router router = Router.router(vertx);

        SockJSHandler sockJSHandler = SockJSHandler.create(vertx);
        PermittedOptions allAddresses = new PermittedOptions().setAddressRegex(".+");
        BridgeOptions options = new BridgeOptions();
        options.addInboundPermitted(allAddresses);
        options.addOutboundPermitted(allAddresses);

        sockJSHandler.bridge(options, be -> {
            System.out.println("event type = " + be.type());
            System.out.println("event body = " + be.getRawMessage().getString("body"));
            be.complete(true);
        });

        router.route("/eventbus/*").handler(sockJSHandler);

        listenToTCPBridge();

        EventBus eventBus = vertx.eventBus();
        eventBus.registerDefaultCodec(com.google.gson.JsonObject.class, new GsonJsonMessageCodec());

        ConcurrentMap<String, ClientState> clients = new ConcurrentHashMap<>();

        ClientStateListener clientStateListener = new ClientStateListener(clients);
        eventBus.consumer("setClientState", clientStateListener::handleVertxEvent);

        //Thread clientThread = new Thread(() -> clientThread(eventBus));
        //clientThread.start();

        Client redisearchClient = new Client("entitiesFeed", "192.168.0.53", 6379);
        Thread distributerThread = new Thread(() -> pollAndSendUpdates(eventBus, redisearchClient, clients));
        distributerThread.start();

        router.route().handler(StaticHandler.create());

        vertx.createHttpServer().requestHandler(router::accept).listen(7001, "192.168.0.53");
    }

    private void listenToTCPBridge() {
        TcpEventBusBridge bridge = TcpEventBusBridge.create(
                vertx,
                new io.vertx.ext.bridge.BridgeOptions()
                        .addInboundPermitted(new PermittedOptions().setAddressRegex(".*"))
                        .addOutboundPermitted(new PermittedOptions().setAddressRegex(".*")));

        bridge.listen(7000, res -> {
            if (res.succeeded()) {
                System.out.println("Listening started on port 7000");
            } else {
                System.out.println("Listening on port 7000 failed");
            }
        });
    }

    private static void pollAndSendUpdates(EventBus eventBus, Client redisearchClient,
                                           ConcurrentMap<String, ClientState> clients) {
        Distributer distributer = new Distributer(redisearchClient, clients,
                (address, data) -> eventBus.publish(address, data));
        try {
            while (true) {
                Thread.sleep(200);
                distributer.distribute();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void clientThread(EventBus eventBus) {
        ClientSimulator clientSimulator = new ClientSimulator(
                "clientThread",
                (eventName, data) -> eventBus.publish(eventName, data));
        try {
            eventBus.consumer("clientThread",
                    (message) -> System.out.println("clientThread received event called " + message.address() + " with data " + message.body()));

            while (true) {
                Thread.sleep(1000);
                clientSimulator.simulateStateChange();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
