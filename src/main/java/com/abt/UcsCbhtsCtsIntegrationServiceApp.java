package com.abt;

import akka.NotUsed;
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.server.Route;
import com.abt.util.EnvConfig;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletionStage;

import static akka.http.javadsl.server.Directives.concat;

public class UcsCbhtsCtsIntegrationServiceApp {
    public static String SECRETE_KEY = "";
    private static final String DEFAULT_HOST = "127.0.0.1";
    private static final int DEFAULT_PORT = 8080;
    private static final String DEFAULT_SECRET_KEY = "secret-key";

    static void startHttpServer(Route route, ActorSystem<?> system) {
        String host = EnvConfig.getOrDefault("INTEGRATION_SERVICE_HOST", DEFAULT_HOST);
        int port = EnvConfig.getIntOrDefault("INTEGRATION_SERVICE_PORT", DEFAULT_PORT);

        CompletionStage<ServerBinding> futureBinding =
                Http.get(system)
                        .newServerAt(host, port)
                        .bind(route);

        futureBinding.whenComplete((binding, exception) -> {
            if (binding != null) {
                InetSocketAddress address = binding.localAddress();
                system.log().info("Server online at http://{}:{}/", address.getHostString(), address.getPort());
            } else {
                system.log().error("Failed to bind HTTP endpoint, terminating system", exception);
                system.terminate();
            }
        });
    }

    private static void initializeSecretKey() {
        SECRETE_KEY = EnvConfig.getOrDefault("INTEGRATION_SERVICE_SECRET_KEY", DEFAULT_SECRET_KEY);
    }

    public static void main(String[] args) {
        Behavior<NotUsed> rootBehavior = Behaviors.setup(context -> {
            initializeSecretKey();

            ActorRef<UcsCtcIntegrationRegistry.Command> ctcIntegrationActor =
                    context.spawn(UcsCtcIntegrationRegistry.create(), "UcsCtcIntegration");

            UcsCbhtsCtsIntegrationRoutes cbhtsRoutes = new UcsCbhtsCtsIntegrationRoutes(context.getSystem());
            UcsCtcIntegrationRoutes ctcRoutes = new UcsCtcIntegrationRoutes(context.getSystem(), ctcIntegrationActor);

            startHttpServer(
                    concat(cbhtsRoutes.integrationRoutes(), ctcRoutes.ucsIntegrationRoutes()),
                    context.getSystem()
            );
            return Behaviors.empty();
        });

        ActorSystem.create(rootBehavior, "UcsCbhtsCtsIntegrationServiceServer");
    }
}
