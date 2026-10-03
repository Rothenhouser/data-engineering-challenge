"""OCPP MeterValues measurand names — the single source of truth.

Measurands are the named quantities inside a ``MeterValues`` sampledValue list,
e.g. ``{"measurand": "SoC", "value": "67", "unit": "Percent"}``. They are only
reported during a transaction (while the EV is connected), so a reading like SoC
is meaningful only for an in-session charger.

Using this enum instead of scattered string literals keeps the fold, the readers
and the dashboard referring to the exact same OCPP names.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any


class Measurand(StrEnum):
    POWER_ACTIVE_IMPORT = "Power.Active.Import"  # kW, drives energy estimation
    ENERGY_REGISTER = "Energy.Active.Import.Register"  # kWh, cumulative meter
    ENERGY_INTERVAL = "Energy.Active.Import.Interval"  # kWh, since last sample
    CURRENT_IMPORT = "Current.Import"  # A
    VOLTAGE = "Voltage"  # V
    SOC = "SoC"  # %, EV state of charge
    CURRENT_OFFERED = "Current.Offered"  # A
    POWER_OFFERED = "Power.Offered"  # W


def read_measurand(payload: dict[str, Any], measurand: Measurand) -> float | None:
    """Pull a single measurand's float value from a MeterValues payload.

    Returns ``None`` if the payload carries no reading for that measurand (or it
    is unparseable). Scans every meterValue/sampledValue entry.
    """
    for mv in payload.get("meterValue") or []:
        for sv in mv.get("sampledValue") or []:
            if sv.get("measurand") == measurand.value:
                try:
                    return float(sv["value"])
                except KeyError, TypeError, ValueError:
                    return None
    return None
