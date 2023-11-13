import logging
import os
from datetime import datetime
from typing import List

import requests
from dateutil.parser import ParserError, parse

logger = logging.getLogger(__name__)

# If None, set to empty string to avoid runtime errors
API_BASE_URL = (
    os.environ.get("API_BASE_URL") if os.environ.get("API_BASE_URL") is not None else ""
)


class DESGWApi:
    """
    The class can be instantiated as follows:
    api = DESGWApi()
    """

    def __init__(self, debug_mode=False):
        self.api_url = API_BASE_URL
        self.debug_mode = debug_mode

    def add_galaxy(self, galaxy_data: dict) -> bool:
        """
        Add a new galaxy to the database. Please note that the keys in the dictionary must have the following names:

        ["galaxy_id", "photoz", "specz", "ra", "dec", "snsep", "gmag", "imag", "rmag", "zmag"]

        :param galaxy_data: A dictionary of galaxy data containing at minimum, the key "galaxy_id"
        :return: True if successful, False otherwise
        """
        galaxy_columns = [
            "galaxy_id",
            "photoz",
            "specz",
            "ra",
            "dec",
            "snsep",
            "gmag",
            "imag",
            "rmag",
            "zmag",
        ]
        required_columns = ["galaxy_id"]
        column_dtypes = [
            int,
            float,
            float,
            float,
            float,
            float,
            float,
            float,
            float,
            float,
        ]

        if self.debug_mode:
            # this column is mandatory because it's the primary key
            assert check_required_data(
                galaxy_data, required_columns, "add_galaxy"
            ), "Data wrong type or missing required fields, please check logfile"

            assert check_column_names(
                galaxy_data, galaxy_columns, "add_galaxy"
            ), "Columns are incorrect, please check logfile"

            assert check_dtype(
                galaxy_data, galaxy_columns, column_dtypes, "add_galaxy"
            ), "Data types are incorrect, please check logfile"

        else:
            if not check_required_data(galaxy_data, required_columns, "add_galaxy"):
                return False
            if not check_column_names(galaxy_data, galaxy_columns, "add_galaxy"):
                return False
            if not check_dtype(
                galaxy_data, galaxy_columns, column_dtypes, "add_galaxy"
            ):
                return False

        url = self.api_url + "galaxies/add"
        return post_request(galaxy_data, url)

    def add_trigger(self, trigger_data: dict) -> bool:
        """
        Add a new trigger to the database. Please note that the keys in the dictionary must have the following names:

        ["trigger_label", "type", "ligo_prob", "far", "distance", "mjd", "event_datetime", "mock",
         "image_url", "galaxy_percentage_file", "initial_skymap", "moon", "season"]

        :param trigger_data: a dictionary containing at minimum, trigger_label, event_datetime, mock, and season
        :return: True if successful, False otherwise
        """
        trigger_columns = [
            "trigger_label",
            "type",
            "ligo_prob",
            "far",
            "distance",
            "mjd",
            "event_datetime",
            "mock",
            "image_url",
            "galaxy_percentage_file",
            "initial_skymap",
            "moon",
            "season",
        ]
        required_columns = ["trigger_label", "event_datetime", "mock", "season"]

        column_dtypes = [
            str,
            str,
            float,
            float,
            float,
            float,
            datetime,
            bool,
            str,
            str,
            str,
            str,
            str,
        ]

        if self.debug_mode:
            assert check_required_data(
                trigger_data, required_columns, "add_trigger"
            ), "Data wrong type or missing required fields, check logfile"

            assert check_column_names(
                trigger_data, trigger_columns, "add_trigger"
            ), "Columns are incorrect, please check logfile"

            assert check_dtype(
                trigger_data, trigger_columns, column_dtypes, "add_trigger"
            ), "Data types are incorrect, please check logfile"

        else:
            if not check_required_data(trigger_data, trigger_columns, "add_trigger"):
                return False

            if not check_column_names(trigger_data, trigger_columns, "add_trigger"):
                return False

            if not check_dtype(
                trigger_data, trigger_columns, column_dtypes, "add_trigger"
            ):
                return False

        url = self.api_url + "triggers/add"

        return post_request(trigger_data, url)

    def add_candidate(self, candidate_data: dict) -> bool:
        """
        Add a new candidate to the database or update an existing candidate's ml_score

        Note1 - trigger_label will be converted to a primary-key id (trigger_id) by the api

        Note2 - The keys in the dictionary must have the following names

        ["candidate_label", "event_datetime", "trigger_label", "ra",
        "dec", "max_ml_score", "cnn_score", "first_mag",
        "path_to_fits", "host_final_z", "host_final_z_error",
        "trigger_mjd", "area", "gwid", "far", "light_curve_img",
        "survey", "pixsize", "mwebv", "mwebv_error", "redshift_helio",
        "redshift_final", "redshift_final_error", "host_galaxy_id", "galaxy_percentage"]


        :param candidate_data: To insert a new candidate, a dictionary containing at minimum, candidate_label, event_datetime, and trigger_label is required. To update, only the candidate_label and ml_score are required
        :return: True if successful, False otherwise
        """
        candidate_columns = [
            "candidate_label",
            "event_datetime",
            "trigger_label",
            "ra",
            "dec",
            "max_ml_score",
            "first_mag",
            "path_to_fits",
            "host_final_z",
            "host_final_z_error",
            "trigger_mjd",
            "area",
            "gwid",
            "far",
            "light_curve_img",
            "survey",
            "pixsize",
            "mwebv",
            "mwebv_error",
            "redshift_helio",
            "redshift_final",
            "redshift_final_error",
            "host_galaxy_id",
            "galaxy_percentage",
        ]

        required_columns = ["candidate_label", "event_datetime", "trigger_label"]

        column_dtypes = [
            str,
            datetime,
            str,
            float,
            float,
            float,
            float,
            str,
            float,
            float,
            float,
            float,
            int,
            float,
            str,
            str,
            float,
            float,
            float,
            float,
            float,
            float,
            int,
            float,
        ]

        if self.debug_mode:
            assert check_required_data(
                candidate_data, required_columns, "add_candidate"
            ), "Data wrong type or missing required fields, check logfile"

            assert check_column_names(
                candidate_data, candidate_columns, "add_candidate"
            ), "Columns are incorrect, please check logfile"

            assert check_dtype(
                candidate_data, candidate_columns, column_dtypes, "add_candidate"
            ), "Data types are incorrect, please check logfile"

        else:
            if not check_required_data(
                candidate_data, required_columns, "add_candidate"
            ):
                return False
            if not check_column_names(
                candidate_data, candidate_columns, "add_candidate"
            ):
                return False
            if not check_dtype(
                candidate_data, candidate_columns, column_dtypes, "add_candidate"
            ):
                return False

        # It's possible to update a candidate so check if the candidate exists first
        get_url = self.api_url + "candidates/data?candidate={0}".format(
            candidate_data.get("candidate_label")
        )
        request_dict = get_request(get_url)
        candidate_label = request_dict.get("candidate_label")

        if candidate_label:
            update_url = self.api_url + "candidates/update"
            return post_request(candidate_data, update_url)

        else:
            post_url = self.api_url + "candidates/add"
            return post_request(candidate_data, post_url)

    def add_candidate_object(self, object_data: dict) -> bool:
        """
        Add a new candidate object to the database, or update an existing candidate_object's cnn_score

        Please note that the keys in the dictionary must have the following names:

        ["candidate_label", "mjd", "flt", "field", "fluxcal",
        "fluxcal_error", "mag", "mag_error", "photflag",
        "photprob", "zpflux", "psf", "skysig", "skysig_t",
        "gain", "xpix", "ypix", "nite", "expnum", "ccdnum",
        "temp_img", "search_img", "diff_img"]

        :param object_data: To insert a new candidate_object, a dictionary containing at minimum, the candidate label and obj_id is required. To update, only the obj_id and cnn_score are required
        :return: True if successful, False otherwise
        """
        object_columns = [
            "obj_id",
            "candidate_label",
            "mjd",
            "flt",
            "field",
            "fluxcal",
            "fluxcal_error",
            "mag",
            "mag_error",
            "photflag",
            "photprob",
            "zpflux",
            "psf",
            "skysig",
            "skysig_t",
            "gain",
            "xpix",
            "ypix",
            "nite",
            "expnum",
            "ccdnum",
            "cnn_score",
            "temp_img",
            "search_img",
            "diff_img",
        ]

        required_columns = ["obj_id", "candidate_label"]

        column_dtypes = [
            int,
            str,
            float,
            str,
            str,
            float,
            float,
            float,
            float,
            float,
            float,
            float,
            float,
            float,
            float,
            float,
            float,
            float,
            float,
            float,
            float,
            float,
            str,
            str,
            str,
        ]

        if self.debug_mode:
            assert check_required_data(
                object_data, required_columns, "add_candidate_object"
            ), "Data wrong type or missing required fields, check logfile"

            assert check_column_names(
                object_data, object_columns, "add_candidate_object"
            ), "Columns are incorrect, please check logfile"

            assert check_dtype(
                object_data, object_columns, column_dtypes, "add_candidate_object"
            ), "Data types are incorrect, please check logfile"

        else:
            if not check_required_data(
                object_data, required_columns, "add_candidate_object"
            ):
                return False
            if not check_column_names(
                object_data, object_columns, "add_candidate_object"
            ):
                return False
            if not check_dtype(
                object_data, object_columns, column_dtypes, "add_candidate_object"
            ):
                return False

        # It's possible to update a candidate object so check if the candidate exists first
        get_url = self.api_url + "candidates/objects?obj_id={0}".format(
            object_data.get("obj_id")
        )
        request_dict = get_request(get_url)
        obj_id = request_dict.get("obj_id")

        if obj_id:
            update_url = self.api_url + "candidates/objects/update"
            return post_request(object_data, update_url)

        post_url = self.api_url + "candidates/objects/add"
        return post_request(object_data, post_url)

    def add_trigger_by_day(self, day_data: dict) -> bool:
        """
        Add a new daily trigger to the database.
        Please note that the keys must have the following names:

        ["trigger_label", "date", "n_hexes", "econ_prob",
        "econ_area", "need_area", "quality", "exp_time",
        "filter", "hours", "n_visits", "n_slots", "b_slot",
        "prob_vs_slot_plot", "centered_gif_plot", "ligo_prob_contour_plot",
        "des_prob_vs_ligo_prob_plot", "des_limit_mag_map",
        "des_limit_mag_map_src", "json_link", "log_link",
        "strategy_table", "final_skymap", "airmass", "cumulative_hex_prob"]

        :param day_data: a dictionary containing at minimum, the trigger_label and date
        :return: True if successful, False otherwise
        """

        day_columns = [
            "trigger_label",
            "date",
            "n_hexes",
            "econ_prob",
            "econ_area",
            "need_area",
            "quality",
            "exp_time",
            "filter",
            "hours",
            "n_visits",
            "n_slots",
            "b_slot",
            "prob_vs_slot_plot",
            "centered_gif_plot",
            "ligo_prob_contour_plot",
            "des_prob_vs_ligo_prob_plot",
            "des_limit_mag_map",
            "des_limit_mag_map_src",
            "json_link",
            "log_link",
            "strategy_table",
            "final_skymap",
            "airmass",
            "cumulative_hex_prob",
        ]

        required_columns = ["trigger_label", "date"]
        column_dtypes = [
            str,
            datetime,
            int,
            float,
            float,
            float,
            float,
            str,
            str,
            float,
            int,
            int,
            int,
            str,
            str,
            str,
            str,
            str,
            str,
            str,
            str,
            str,
            str,
            str,
            str,
        ]

        if self.debug_mode:
            assert check_required_data(
                day_data, required_columns, "add_trigger_by_day"
            ), "Data wrong type or missing required fields, check logfile"

            assert check_column_names(
                day_data, day_columns, "add_trigger_by_day"
            ), "Columns are incorrect, please check logfile"

            assert check_dtype(
                day_data, day_columns, column_dtypes, "add_trigger_by_day"
            ), "Data types are incorrect, please check logfile"
        else:
            if not check_required_data(
                day_data, required_columns, "add_trigger_by_day"
            ):
                return False
            if not check_column_names(day_data, day_columns, "add_trigger_by_day"):
                return False
            if not check_dtype(
                day_data, day_columns, column_dtypes, "add_trigger_by_day"
            ):
                return False

        post_url = self.api_url + "triggersbyday/add"

        return post_request(day_data, post_url)


def post_request(data: dict, url: str) -> bool:
    """
    Send the post request with the data and check the status.

    :param data: The data to be sent
    :param url: The full API URL
    :return: True if successful(status == 200), False if anything else
    """

    if test_connection():
        # use json param instead of data param to ensure ContentType header is set
        request = requests.post(url, json=data)
        # The content is a byte string, so you have to decode it first!
        api_message = request.content.decode(request.encoding)
        status = request.status_code
        if status == 200:
            logger.info(f"api response: {api_message}")
            logger.info("Data successfully added")
            return True

        logger.warning(f"api response: {api_message}")

        message = f"Failed to insert data with status {status}.\nValues attempted for insert are as follows:\n"
        for item in data.items():
            message += "key '{0}' had value '{1}' and data type '{2}'\n".format(
                item[0], item[1], type(item[1])
            )
        message += "json data:\n" + str(data)
        logger.warning(message)
        return False
    else:
        return False


def get_request(url: str) -> dict:
    """
    Do a get request and return the data in dictionary format
    If the connection fails, this will return an empty dictionary with the hope that subsequent the post_request
    will also fail to connect, and then return False

    :param url: the full url for the api request
    :return: a dictionary of the data in the request, empty if the request status_code is not 200
    """

    if test_connection():
        request = requests.get(url)

        if request.status_code == 200:
            try:
                return request.json()[0]
            except IndexError:
                return dict(request.json())
        else:
            return dict()
    else:
        return dict()


def test_connection() -> bool:
    """
    Test the connection to the api by sending a get request to the api_url

    :return: True if successful, False otherwise
    """
    try:
        request = requests.get(API_BASE_URL + "triggers/data")
        return True
    except (
        requests.exceptions.ConnectionError,
        requests.exceptions.MissingSchema,
    ) as e:
        logger.warning(f"Connection failed due to {e}")
        return False


def check_required_data(data: dict, required_columns: List[str], caller: str):
    """
    Checks the data is a dictionary, and the required columns are not None

    :param data: The data dictionary to be inserted
    :param required_columns: The required columns
    :param caller: The name of the function that called this function
    :return: True if all required columns are not None and data is a dict, else False
    """

    if not isinstance(data, dict):
        logger.warning(f"\n~~~\n function {caller} failed\n~~~")
        logger.warning(f"\nData must be a dictionary, not {type(data)}")
        logger.info(f"\nData was {data}")
        return False
    missing_columns = []
    for col in required_columns:
        if data.get(col) is None:
            missing_columns.append(col)
    if missing_columns:
        logger.warning(f"\n~~~\n function {caller} failed\n~~~")
        logger.warning(
            f"\nThe following *required* columns are incorrect or None {missing_columns}"
        )
        logger.info(f"\nData was {data}")
        return False
    return True


def check_column_names(data: dict, columns: List[str], caller: str) -> bool:
    """
    Confirms used column names are correct

    :param data: The data dictionary to be inserted. Ideally with keys equal to the values in the 'columns' parameter
    :param columns: The correct column names
    :param caller: The name of the function that called this function
    :return: True if column names are correct, else False
    """
    # Get the list of user supplied column names
    input_columns = list(data.keys())
    wrong_columns = []
    for col in input_columns:
        if col not in columns:
            wrong_columns.append(col)
    if wrong_columns:
        logger.warning(f"\n~~~\n function {caller} failed\n~~~")
        logger.warning(f"\nThe following columns are incorrect {wrong_columns}")
        logger.info(f"\nData was {data}")
        return False
    return True


def check_dtype(
    data: dict, columns: List[str], dtypes: List[type], caller: str
) -> bool:
    """
    Confirms the data types in `data` match expected data types in `dtypes` and
    writes the errors to the log

    :param data: a dictionary with keys equal to the values in the 'columns' parameter
    :param columns: The names of the columns in the database
    :param dtypes: The data types of the columns in the database
    :param caller: The name of the function that called this function
    :return: True if types are correct, else False
    """
    empty_columns = []
    incorrect_dtypes = []
    for column, dtype in zip(columns, dtypes):
        # I'm already checking for the required/non-nullable columns, so ignoring "Nones" here is okay
        if data.get(column) is not None:
            expected_type = dtype
            actual_type = type(data[column])

            # I expect most datetimes will be input as strings, so lets check for them
            if expected_type == datetime and actual_type == str:
                try:
                    actual_type = type(parse(data[column]))

                # if it fails, just keep it as a str and the logger will catch it
                except (ParserError, ValueError):
                    incorrect_dtypes.append(
                        {
                            "column": column,
                            "expected_type": expected_type,
                            "actual_type": actual_type,
                            "value": data[column],
                        }
                    )
                    continue

            if actual_type != expected_type:
                incorrect_dtypes.append(
                    {
                        "column": column,
                        "expected_type": expected_type,
                        "actual_type": actual_type,
                        "value": data[column],
                    }
                )
        else:
            empty_columns.append(column)
    if incorrect_dtypes:
        build_message = ""
        for row in incorrect_dtypes:
            build_message += "\nType for column '{0}' is '{1}' but was expected to be '{2}'. Had value '{3}'".format(
                row.get("column"),
                row.get("actual_type"),
                row.get("expected_type"),
                row.get("value"),
            )
        logger.warning(f"\n~~~\n function {caller} failed\n~~~")
        logger.warning(build_message)
        return False

    if empty_columns:
        message = (
            f"\nThese columns are *not* required, but had value None: "
            f"{empty_columns}.\nYou may need to check your data"
        )
        logger.warning(message)

    return True
