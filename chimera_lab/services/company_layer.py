from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
from typing import Any


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(slots=True)
class OwnerApproval:
    approval_id: str
    action_type: str
    target_id: str
    approved_by: str
    reason: str
    created_at: str = field(default_factory=_now)


@dataclass(slots=True)
class TreasuryEntry:
    entry_id: str
    kind: str
    amount: float
    venture_id: str | None = None
    note: str = ""
    created_at: str = field(default_factory=_now)


@dataclass(slots=True)
class VentureUnit:
    venture_id: str
    name: str
    thesis: str
    owner_share: float = 1.0
    status: str = "incubating"
    budget: float = 0.0
    revenue: float = 0.0
    assets: list[str] = field(default_factory=list)


@dataclass(slots=True)
class ProductAsset:
    asset_id: str
    venture_id: str
    asset_type: str
    description: str
    pricing_model: str
    status: str = "draft"
    revenue: float = 0.0
    requires_owner_approval: bool = True


class TreasuryLedger:
    def __init__(self, starting_cash: float = 0.0) -> None:
        self.cash = float(starting_cash)
        self.reserved = 0.0
        self.entries: list[TreasuryEntry] = []

    @property
    def available(self) -> float:
        return self.cash - self.reserved

    def allocate(self, amount: float, venture_id: str, note: str = "") -> TreasuryEntry:
        if amount <= 0:
            raise ValueError("allocation_must_be_positive")
        if amount > self.available:
            raise ValueError("insufficient_treasury")
        self.reserved += amount
        entry = TreasuryEntry(entry_id=f"alloc_{len(self.entries) + 1}", kind="allocation", amount=amount, venture_id=venture_id, note=note)
        self.entries.append(entry)
        return entry

    def spend(self, amount: float, venture_id: str, note: str = "") -> TreasuryEntry:
        if amount <= 0:
            raise ValueError("spend_must_be_positive")
        if amount > self.reserved:
            raise ValueError("insufficient_reserved_cash")
        self.reserved -= amount
        self.cash -= amount
        entry = TreasuryEntry(entry_id=f"spend_{len(self.entries) + 1}", kind="spend", amount=amount, venture_id=venture_id, note=note)
        self.entries.append(entry)
        return entry

    def receive(self, amount: float, venture_id: str, note: str = "") -> TreasuryEntry:
        if amount <= 0:
            raise ValueError("revenue_must_be_positive")
        self.cash += amount
        entry = TreasuryEntry(entry_id=f"rev_{len(self.entries) + 1}", kind="revenue", amount=amount, venture_id=venture_id, note=note)
        self.entries.append(entry)
        return entry

    def simulate_month(self, burn_rate: float) -> dict[str, Any]:
        if burn_rate < 0:
            raise ValueError("burn_rate_must_be_non_negative")
        self.cash -= burn_rate
        self.entries.append(TreasuryEntry(entry_id=f"burn_{len(self.entries) + 1}", kind="burn", amount=burn_rate, note="monthly burn"))
        return {
            "cash": self.cash,
            "reserved": self.reserved,
            "available": self.available,
            "runway_months": self.runway_months(burn_rate),
        }

    def runway_months(self, burn_rate: float) -> float:
        if burn_rate <= 0:
            return float("inf")
        return max(0.0, self.available / burn_rate)


class AutonomousCompany:
    def __init__(self, owner_name: str, starting_cash: float = 0.0) -> None:
        self.owner_name = owner_name
        self.treasury = TreasuryLedger(starting_cash=starting_cash)
        self.ventures: dict[str, VentureUnit] = {}
        self.assets: dict[str, ProductAsset] = {}
        self.approvals: dict[str, OwnerApproval] = {}

    def create_venture(self, venture_id: str, name: str, thesis: str, budget: float = 0.0) -> VentureUnit:
        if venture_id in self.ventures:
            raise ValueError("venture_already_exists")
        venture = VentureUnit(venture_id=venture_id, name=name, thesis=thesis, budget=budget, owner_share=1.0)
        self.ventures[venture_id] = venture
        if budget > 0:
            self.treasury.allocate(budget, venture_id, note="initial venture allocation")
        return venture

    def propose_asset(self, asset_id: str, venture_id: str, asset_type: str, description: str, pricing_model: str) -> ProductAsset:
        self._require_venture(venture_id)
        asset = ProductAsset(
            asset_id=asset_id,
            venture_id=venture_id,
            asset_type=asset_type,
            description=description,
            pricing_model=pricing_model,
        )
        self.assets[asset_id] = asset
        self.ventures[venture_id].assets.append(asset_id)
        return asset

    def request_owner_approval(self, approval_id: str, action_type: str, target_id: str, reason: str, approved_by: str | None = None) -> OwnerApproval:
        approver = approved_by or self.owner_name
        if approver != self.owner_name:
            raise ValueError("only_owner_can_approve")
        approval = OwnerApproval(
            approval_id=approval_id,
            action_type=action_type,
            target_id=target_id,
            approved_by=approver,
            reason=reason,
        )
        self.approvals[approval_id] = approval
        return approval

    def promote_asset(self, asset_id: str, approval_id: str) -> ProductAsset:
        asset = self._require_asset(asset_id)
        approval = self.approvals.get(approval_id)
        if approval is None or approval.action_type != "promote_asset" or approval.target_id != asset_id:
            raise ValueError("asset_requires_owner_approval")
        asset.status = "live"
        return asset

    def transfer_budget(self, venture_id: str, amount: float, approval_id: str) -> TreasuryEntry:
        venture = self._require_venture(venture_id)
        approval = self.approvals.get(approval_id)
        if approval is None or approval.action_type != "transfer_budget" or approval.target_id != venture_id:
            raise ValueError("budget_transfer_requires_owner_approval")
        entry = self.treasury.allocate(amount, venture_id, note=approval.reason)
        venture.budget += amount
        return entry

    def record_revenue(self, asset_id: str, amount: float) -> TreasuryEntry:
        asset = self._require_asset(asset_id)
        venture = self._require_venture(asset.venture_id)
        venture.revenue += amount
        asset.revenue += amount
        return self.treasury.receive(amount, venture.venture_id, note=f"asset:{asset.asset_id}")

    def simulate_month(self, venture_burns: dict[str, float] | None = None, asset_revenue: dict[str, float] | None = None) -> dict[str, Any]:
        venture_burns = venture_burns or {}
        asset_revenue = asset_revenue or {}
        burn_total = 0.0
        for venture_id, venture in self.ventures.items():
            burn = float(venture_burns.get(venture_id, max(1.0, venture.budget * 0.1)))
            burn_total += burn
            venture.budget = max(0.0, venture.budget - burn)
            self.treasury.entries.append(TreasuryEntry(entry_id=f"venture_burn_{len(self.treasury.entries) + 1}", kind="venture_burn", amount=burn, venture_id=venture_id, note=venture.name))
        revenue_total = 0.0
        for asset_id, amount in asset_revenue.items():
            revenue_total += float(amount)
            self.record_revenue(asset_id, float(amount))
        treasury_state = self.treasury.simulate_month(burn_total)
        return {
            "owner": self.owner_name,
            "ventures": len(self.ventures),
            "assets": len(self.assets),
            "burn_total": burn_total,
            "revenue_total": revenue_total,
            "treasury": treasury_state,
        }

    def snapshot(self) -> dict[str, Any]:
        return {
            "owner": self.owner_name,
            "ventures": [asdict(venture) for venture in self.ventures.values()],
            "assets": [asdict(asset) for asset in self.assets.values()],
            "approvals": [asdict(approval) for approval in self.approvals.values()],
            "treasury": {
                "cash": self.treasury.cash,
                "reserved": self.treasury.reserved,
                "available": self.treasury.available,
            },
        }

    def _require_venture(self, venture_id: str) -> VentureUnit:
        venture = self.ventures.get(venture_id)
        if venture is None:
            raise KeyError(venture_id)
        return venture

    def _require_asset(self, asset_id: str) -> ProductAsset:
        asset = self.assets.get(asset_id)
        if asset is None:
            raise KeyError(asset_id)
        return asset
