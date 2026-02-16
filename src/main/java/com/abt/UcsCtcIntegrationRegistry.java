package com.abt;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.abt.actors.SendIndexContactsProcessor;
import com.abt.actors.SendLtfMissapProcessor;
import com.abt.domain.IndexContactRequest;
import com.abt.domain.LtfClientRequest;
import com.abt.domain.Response;

//#UCSLab-registry-actor
public class UcsCtcIntegrationRegistry extends AbstractBehavior<UcsCtcIntegrationRegistry.Command> {

    private UcsCtcIntegrationRegistry(ActorContext<Command> context) {
        super(context);
    }

    public static Behavior<Command> create() {
        return Behaviors.setup(UcsCtcIntegrationRegistry::new);
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(SendLtfMissapRequests.class, this::onSendResults)
                .onMessage(SendIndexContacts.class, this::onSendRejection)
                .build();
    }

    private Behavior<Command> onSendResults(SendLtfMissapRequests command) {
        Response response = new SendLtfMissapProcessor().sendLtfMissap(command.ltfClientRequest, command.url, command.username, command.password);
        command.replyTo().tell(new ActionPerformed(response));
        return this;
    }

    //#user-case-classes

    private Behavior<Command> onSendRejection
            (SendIndexContacts command) {
        Response response = new SendIndexContactsProcessor().sendIndexContacts(command.indexContactRequest, command.url, command.username, command.password);
        command.replyTo().tell(new ActionPerformed(response));
        return this;
    }

    // actor protocol
    sealed interface Command {
    }

    public final static record SendLtfMissapRequests(LtfClientRequest ltfClientRequest,
                                                     String url, String username, String password,
                                                     ActorRef<ActionPerformed> replyTo) implements Command {
    }

    public final static record SendIndexContacts(IndexContactRequest indexContactRequest, String url, String username, String password,
                                                 ActorRef<ActionPerformed> replyTo) implements Command {
    }

    public final static class ActionPerformed implements Command {
        private final Response response;

        public ActionPerformed(Response response) {
            this.response = response;
        }

        public Response response() {
            return response;
        }

        public Response getResponse() {
            return response;
        }
    }

}
