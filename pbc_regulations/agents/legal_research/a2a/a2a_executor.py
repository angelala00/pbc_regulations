"""Expose the legal research streaming agent as an A2A executor."""

from __future__ import annotations

from typing import List, Optional

from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.events.event_queue import EventQueue
from a2a.types import (
    Message,
    TaskArtifactUpdateEvent,
    Task,
    TaskState,
    TaskStatus,
    TaskStatusUpdateEvent,
)
from a2a.utils import (
    new_agent_text_message,
    new_text_artifact,
)

from ..agent.agent_streaming import LegalResearchStreamingAgent


class LegalResearchAgentExecutor(AgentExecutor):
    """Wrap `LegalResearchStreamingAgent` so it can be called via the A2A runtime."""

    def __init__(self, *, agent: Optional[LegalResearchStreamingAgent] = None) -> None:
        self.agent = agent or LegalResearchStreamingAgent()

    async def execute(
        self,
        context: RequestContext,
        event_queue: EventQueue,
    ) -> None:
        """Process the A2A request by forwarding to the streaming legal research agent."""

        task = context.current_task
        message = context.message
        query = context.get_user_input() or ""

        if not message:
            raise ValueError("Missing user message.")

        if not task:
            # Fall back to a minimal task if the RequestContext didn't provide one.
            message_task_id = getattr(message, "task_id", None)
            message_context_id = getattr(message, "context_id", None)
            if not (message_task_id and message_context_id):
                raise RuntimeError(
                    "A2A invariant violation: missing task in RequestContext"
                )
            task = Task(
                id=message_task_id,
                context_id=message_context_id,
                status=TaskStatus(state=TaskState.working),
                history=[message] if isinstance(message, Message) else [],
                artifacts=[],
            )
            context.current_task = task

        #
        # 1) Send initial "working" state
        #
        await event_queue.enqueue_event(
            TaskStatusUpdateEvent(
                status=TaskStatus(state=TaskState.working),
                final=False,
                contextId=task.context_id,
                taskId=task.id,
            )
        )

        #
        # 2) Stream model output — each token becomes an artifact_update(append=True)
        #
        chunks: List[str] = []

        async for delta in self.agent.stream(query):

            if not delta:
                continue

            print(f"deltaaaaaaaaa:{delta}")

            chunks.append(delta)

            # Stream partial text by replacing the artifact (append=False) to avoid missing-append errors.
            await event_queue.enqueue_event(
                TaskArtifactUpdateEvent(
                    append=False,
                    contextId=task.context_id,
                    taskId=task.id,
                    artifact=new_text_artifact(
                        name="legal_research_stream",
                        description="Streaming partial response.",
                        text="".join(chunks),
                    ),
                )
            )

        #
        # 3) Final full answer artifact (append=False)
        #
        full_text = "".join(chunks)

        await event_queue.enqueue_event(
            TaskArtifactUpdateEvent(
                append=False,  # full replacement
                contextId=task.context_id,
                taskId=task.id,
                artifact=new_text_artifact(
                    name="legal_research_result",
                    description="Final result of legal research request.",
                    text=full_text,
                ),
            )
        )

        #
        # 4) Send final completed status
        #
        await event_queue.enqueue_event(
            TaskStatusUpdateEvent(
                status=TaskStatus(state=TaskState.completed),
                final=True,
                contextId=task.context_id,
                taskId=task.id,
            )
        )

    async def cancel(
        self,
        context: RequestContext,
        event_queue: EventQueue,
    ) -> None:
        """Cancellation not yet supported."""
        raise RuntimeError("Cancellation is not supported for LegalResearchAgentExecutor")


__all__ = ["LegalResearchAgentExecutor"]
