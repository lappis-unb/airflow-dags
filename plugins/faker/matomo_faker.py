# VisitsSummary.get
# VisitFrequency.get
# UserCountry.getRegion
# DevicesDetection.getType

import random

import pandas as pd
from faker import Faker


class MatomoFaker:
    """A class for generating fake Matomo data for various metrics."""

    class VisitsSummary:
        """A nested class for generating fake Matomo visits summary data."""

        @classmethod
        def get(cls):
            """Generate fake Matomo visits summary data.

            Returns:
            -------
                str: A CSV representation of the generated data.

            Example:
            -------
                MatomoFaker.VisitsSummary.get()
            """
            faker = Faker("pt_BR")
            data = [
                {
                    "nb_visits": faker.pyint(min_value=10000, max_value=100000),
                    "nb_actions": faker.pyint(min_value=10000, max_value=100000),
                    "nb_visits_converted": faker.pyint(),
                    "bounce_count": faker.pyint(min_value=80000, max_value=800000),
                    "sum_visit_length": faker.pyint(min_value=4000000, max_value=40000000),
                    "max_actions": faker.pyint(min_value=100),
                    "bounce_rate": f"{faker.pyint(min_value=0, max_value=100)}%",
                    "nb_actions_per_visit": faker.pyfloat(min_value=0, max_value=100),
                    "avg_time_on_site": faker.pyfloat(min_value=0, max_value=1000),
                }
            ]
            return pd.DataFrame.from_records(data).to_csv(index=False)

    class VisitFrequency:
        """A nested class for generating fake Matomo visit frequency data."""

        @classmethod
        def get(cls):
            """Generate fake Matomo visit frequency data.

            Returns:
            -------
                str: A CSV representation of the generated data.

            Example:
            -------
                MatomoFaker.VisitFrequency.get()
            """
            faker = Faker("pt_BR")
            data = [
                {
                    "nb_visits_new": faker.pyint(min_value=10000, max_value=100000),
                    "nb_actions_new": faker.pyint(min_value=10000, max_value=100000),
                    "nb_visits_converted_new": faker.pyint(),
                    "bounce_count_new": faker.pyint(min_value=80000, max_value=800000),
                    "sum_visit_length_new": faker.pyint(min_value=4000000, max_value=40000000),
                    "max_actions_new": faker.pyint(min_value=100),
                    "bounce_rate_new": f"{faker.pyint(min_value=0, max_value=100)}%",
                    "nb_actions_per_visit_new": faker.pyfloat(min_value=0, max_value=100, right_digits=2),
                    "avg_time_on_site_new": faker.pyfloat(min_value=0, max_value=1000),
                    "nb_visits_returning": faker.pyint(min_value=10000, max_value=100000),
                    "nb_actions_returning": faker.pyint(min_value=10000, max_value=100000),
                    "nb_visits_converted_returning": faker.pyint(),
                    "bounce_count_returning": faker.pyint(min_value=80000, max_value=800000),
                    "sum_visit_length_returning": faker.pyint(min_value=4000000, max_value=40000000),
                    "max_actions_returning": faker.pyint(min_value=100),
                    "bounce_rate_returning": f"{faker.pyint(min_value=0, max_value=100)}%",
                    "nb_actions_per_visit_returning": faker.pyfloat(
                        min_value=0, max_value=100, right_digits=2
                    ),
                    "avg_time_on_site_returning": faker.pyfloat(min_value=0, max_value=1000, right_digits=2),
                }
            ]
            return pd.DataFrame.from_records(data).to_csv(index=False)

    class UserCountry:
        """A nested class for generating fake Matomo user country data."""

        @classmethod
        def get_region(cls):
            """Generate fake Matomo user country and region data.

            Returns:
            -------
                str: A CSV representation of the generated data.

            Example:
            -------
                MatomoFaker.UserCountry.get_region()
            """
            faker = Faker("pt_BR")
            total = 10000
            brasil_gen = 0
            data = []
            br_states = [
                "AC",
                "AL",
                "AM",
                "AP",
                "BA",
                "CE",
                "DF",
                "ES",
                "GO",
                "MA",
                "MG",
                "MS",
                "MT",
                "PA",
                "PB",
                "PE",
                "PI",
                "PR",
                "RJ",
                "RN",
                "RO",
                "RR",
                "RS",
                "SC",
                "SE",
                "SP",
                "TO",
            ]
            for _ in range(total):
                lat, long, city, contry_code, estate = faker.location_on_land()

                if brasil_gen <= (total * 0.85):
                    while contry_code != "BR":
                        _, _, city, contry_code, estate = faker.location_on_land()
                        estate = random.choice(br_states)
                    brasil_gen += 1
                else:
                    while contry_code == "BR":
                        _, _, city, contry_code, estate = faker.location_on_land()

                data.append(
                    {
                        "label": f"{city.replace('_', ' ')}, {contry_code}",
                        "nb_visits": faker.pyint(min_value=10000, max_value=100000),
                        "nb_actions": faker.pyint(min_value=10000, max_value=100000),
                        "max_actions": faker.pyint(min_value=1, max_value=500),
                        "sum_visit_length": faker.pyint(min_value=100000, max_value=5000000),
                        "bounce_count": faker.pyint(min_value=1000, max_value=50000),
                        "nb_visits_converted": faker.pyint(),
                        "sum_daily_nb_uniq_visitors": faker.pyint(min_value=1000, max_value=50000),
                        "sum_daily_nb_users": faker.pyint(min_value=1000, max_value=50000),
                        "metadata_segment": f"regionCode=={estate.split('/')[-1]};countryCode=={contry_code}",
                        "metadata_region": f"{estate.split('/')[-1]}",
                        "metadata_country": f"{contry_code}",
                        "metadata_country_name": f"{contry_code}",
                        "metadata_region_name": f"{estate.split('/')[-1]}",
                        "metadata_logo": f"plugins/Morpheus/icons/dist/flags/{contry_code}.png",
                    }
                )
            return pd.DataFrame.from_records(data).drop_duplicates(subset=["label"]).to_csv(index=False)

    class DeviceDetection:
        """A nested class for generating fake Matomo device detection data."""

        @classmethod
        def get_type(cls):
            """Generate fake Matomo device detection data.

            Returns:
            -------
                str: A CSV representation of the generated data.

            Example:
            -------
                MatomoFaker.DeviceDetection.get_type()
            """
            faker = Faker("pt_BR")

            possible_devices = [
                "feature+phone",
                "wearable",
                "smart+speaker",
                "smart+display",
                "portable+media+player",
                "peripheral",
                "console",
                "camera",
                "tv",
                "tablet",
                "phablet",
                "desktop",
                "smartphone",
            ]

            data = [
                {
                    "label": device,
                    "nb_visits": faker.pyint(min_value=100000, max_value=200000),
                    "nb_actions": faker.pyint(min_value=190000, max_value=200000),
                    "max_actions": faker.pyint(min_value=300, max_value=400),
                    "sum_visit_length": faker.pyint(min_value=4900000, max_value=5000000),
                    "bounce_count": faker.pyint(min_value=97000, max_value=98000),
                    "nb_visits_converted": 0,
                    "sum_daily_nb_uniq_visitors": faker.pyint(min_value=109000, max_value=110000),
                    "sum_daily_nb_users": 0,
                    "metadata_segment": f"deviceType=={device}",
                    "metadata_logo": f"plugins/Morpheus/icons/dist/devices/{device}.png",
                }
                for device in possible_devices
            ]

            return pd.DataFrame.from_records(data).to_csv(index=False)
