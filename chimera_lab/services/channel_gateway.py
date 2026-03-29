from __future__ import annotations

from chimera_lab.services.artifact_store import ArtifactStore


class ChannelGateway:
    def __init__(self, artifact_store: ArtifactStore) -> None:
        self.artifact_store = artifact_store

    def inbound(self, channel_id: str, user_id: str, text: str, attachments: list[str]) -> dict:
        return self.artifact_store.create(
            "channel_inbound",
            {
                "channel_id": channel_id,
                "user_id": user_id,
                "text": text,
                "attachments": attachments,
            },
            created_by="channel_gateway",
        )

    def outbound(self, channel_id: str, text: str, run_id: str | None, metadata: dict) -> dict:
        return self.artifact_store.create(
            "channel_outbound",
            {
                "channel_id": channel_id,
                "text": text,
                "run_id": run_id,
                "metadata": metadata,
            },
            created_by="channel_gateway",
        )
