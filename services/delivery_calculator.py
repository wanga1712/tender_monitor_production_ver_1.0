"""
MODULE: services.delivery_calculator
RESPONSIBILITY: Calculate delivery costs based on distance and vehicle type.
ALLOWED: math, dataclasses, config.settings.
FORBIDDEN: Database access, external API calls.
ERRORS: None.
"""

from dataclasses import dataclass
from typing import Dict
import math
from core.models import VehicleType
from config.settings import DeliveryConfig


@dataclass
class DeliveryResult:
    vehicles_needed: int
    base_cost: float
    distance_cost: float
    total_cost: float
    notes: List[str]


class DeliveryCalculator:
    def __init__(self, config: DeliveryConfig):
        self.config = config

    def calculate_delivery(
            self,
            distance: float,
            pallets: int,
            vehicle_type: VehicleType
    ) -> DeliveryResult:
        """Расчет стоимости доставки"""

        if vehicle_type == VehicleType.GAZELLE:
            vehicles_needed = math.ceil(pallets / self.config.gazelle_capacity)
            base_cost = 0.0
        else:
            vehicles_needed = 1
            base_cost = self._get_base_cost(vehicle_type)

        distance_cost = distance * self.config.cost_per_km
        total_cost = base_cost + distance_cost

        notes = self._generate_notes(vehicle_type, pallets)

        return DeliveryResult(
            vehicles_needed=vehicles_needed,
            base_cost=base_cost,
            distance_cost=distance_cost,
            total_cost=total_cost,
            notes=notes
        )

    def _get_base_cost(self, vehicle_type: VehicleType) -> float:
        """Получение базовой стоимости по типу транспорта"""
        cost_map = {
            VehicleType.MANIPULATOR: self.config.manipulator_base_cost,
            VehicleType.REAR_LOADER: self.config.rear_loader_base_cost,
            VehicleType.SIDE_LOADER: self.config.side_loader_base_cost,
        }
        return cost_map.get(vehicle_type, 0.0)

    def _generate_notes(self, vehicle_type: VehicleType, pallets: int) -> List[str]:
        """Генерация примечаний к доставке"""
        notes = []

        if vehicle_type == VehicleType.GAZELLE and pallets > 30:
            notes.append("Для Газели рекомендуется не более 30 паллетов")

        if vehicle_type in [VehicleType.MANIPULATOR, VehicleType.SIDE_LOADER]:
            notes.append("Требуется подъезд с двух сторон")

        if not notes:
            notes.append("Стандартные условия доставки")

        return notes

    def get_vehicle_display_name(self, vehicle_type: VehicleType) -> str:
        """Получение читаемого названия транспорта"""
        names = {
            VehicleType.MANIPULATOR: "Манипулятор",
            VehicleType.REAR_LOADER: "Задняя погрузка",
            VehicleType.SIDE_LOADER: "Боковая погрузка",
            VehicleType.GAZELLE: "Газель"
        }
        return names.get(vehicle_type, vehicle_type.value)