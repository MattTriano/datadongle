"""
Shared fixtures and test helpers for bike_index tests.
"""

from unittest.mock import MagicMock

import pytest

from datadongle.collectors.bike_index.client import BikeIndexClient
from datadongle.collectors.bike_index.reader import BikeIndexReader
from datadongle.collectors.bike_index.spec import BikeIndexDatasetSpec

# ------------------------------------------------------------------ #
#  Test data builders
# ------------------------------------------------------------------ #


def make_search_bike(id, date_stolen=1700000000, **overrides):
    """Build a minimal search-result bike dict."""
    bike = {
        "id": id,
        "title": f"Bike {id}",
        "serial": f"SER{id}",
        "manufacturer_name": "TestBrand",
        "frame_model": "Model X",
        "frame_colors": ["Red"],
        "year": 2023,
        "stolen": True,
        "date_stolen": date_stolen,
        "description": "",
        "thumb": None,
        "url": f"https://bikeindex.org/bikes/{id}",
        "stolen_coordinates": [41.88, -87.63],
        "stolen_location": "Chicago, IL",
        "propulsion_type_slug": "foot-pedal",
        "cycle_type_slug": "bike",
        "status": "stolen",
    }
    bike.update(overrides)
    return bike


def make_detail_bike(id, date_stolen=1700000000, **overrides):
    """Build a minimal get_bike() response dict."""
    bike = make_search_bike(id, date_stolen)
    bike.update(
        {
            "registration_created_at": 1700000100,
            "registration_updated_at": 1700000200,
            "manufacturer_id": 42,
            "paint_description": None,
            "frame_size": "56cm",
            "frame_material_slug": "aluminum",
            "handlebar_type_slug": "drop",
            "front_gear_type_slug": None,
            "rear_gear_type_slug": None,
            "rear_wheel_size_iso_bsd": None,
            "front_wheel_size_iso_bsd": None,
            "rear_tire_narrow": True,
            "front_tire_narrow": None,
            "extra_registration_number": None,
            "additional_registration": None,
            "stolen_record": {
                "latitude": 41.89,
                "longitude": -87.62,
                "theft_description": "Locked outside",
                "locking_description": "U-lock",
                "lock_defeat_description": None,
                "police_report_number": "CPD-12345",
                "police_report_department": "Chicago PD",
            },
            "components": [{"type": "wheel", "brand": "Shimano"}],
            "public_images": [],
        }
    )
    bike.update(overrides)
    return bike


# ------------------------------------------------------------------ #
#  Fixtures
# ------------------------------------------------------------------ #


@pytest.fixture
def spec():
    return BikeIndexDatasetSpec(
        name="test_stolen_bikes",
        target_table="stolen_bikes",
        target_schema="raw_data",
        entity_key=["id"],
        location="Chicago, IL",
        distance=10,
    )


@pytest.fixture
def mock_client():
    return MagicMock(spec=BikeIndexClient)


@pytest.fixture
def reader(mock_client):
    return BikeIndexReader(client=mock_client)
