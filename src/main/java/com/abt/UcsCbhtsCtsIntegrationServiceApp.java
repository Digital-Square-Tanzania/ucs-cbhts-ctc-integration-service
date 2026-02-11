package com.abt;

import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.server.Route;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletionStage;

public class UcsCbhtsCtsIntegrationServiceApp {
    static void startHttpServer(Route route, ActorSystem<?> system) {
        CompletionStage<ServerBinding> futureBinding =
                Http.get(system)
                        .newServerAt(
                                system.settings().config().getString("integration-service.service-host"),
                                system.settings().config().getInt("integration-service.service-port")
                        )
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

    public static void main(String[] args) {
        Behavior<NotUsed> rootBehavior = Behaviors.setup(context -> {
            UcsCbhtsCtsIntegrationRoutes routes = new UcsCbhtsCtsIntegrationRoutes(context.getSystem());
            startHttpServer(routes.integrationRoutes(), context.getSystem());
            return Behaviors.empty();
        });

        ActorSystem.create(rootBehavior, "UcsCbhtsCtsIntegrationServiceServer");
    }
}
