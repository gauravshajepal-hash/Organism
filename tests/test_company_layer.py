from __future__ import annotations

import pytest

from chimera_lab.services.company_layer import AutonomousCompany


def test_company_layer_requires_owner_approval_and_tracks_treasury() -> None:
    company = AutonomousCompany(owner_name="human", starting_cash=500.0)
    venture = company.create_venture("venture-1", "Scout Core", "Turn scouting into revenue", budget=100.0)
    assert venture.owner_share == 1.0
    asset = company.propose_asset("asset-1", venture.venture_id, "api", "Paid scouting API", "subscription")

    with pytest.raises(ValueError):
        company.promote_asset(asset.asset_id, "missing")

    approval = company.request_owner_approval("approval-1", "promote_asset", asset.asset_id, "Launch the best idea")
    promoted = company.promote_asset(asset.asset_id, approval.approval_id)
    assert promoted.status == "live"

    transfer = company.request_owner_approval("approval-2", "transfer_budget", venture.venture_id, "Fund the venture")
    company.transfer_budget(venture.venture_id, 50.0, transfer.approval_id)
    revenue_entry = company.record_revenue(asset.asset_id, 25.0)
    assert revenue_entry.amount == 25.0

    monthly = company.simulate_month({venture.venture_id: 10.0}, {asset.asset_id: 15.0})
    assert monthly["ventures"] == 1
    assert monthly["assets"] == 1
    assert monthly["revenue_total"] == 15.0
    assert company.snapshot()["treasury"]["cash"] > 0

