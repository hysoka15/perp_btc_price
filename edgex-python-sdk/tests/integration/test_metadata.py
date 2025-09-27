"""Integration tests for the metadata API."""

import unittest
import logging
from typing import Dict, Any

from tests.integration.base_test import BaseIntegrationTest
from tests.integration.config import TEST_CONTRACT_ID

# Configure logging
logger = logging.getLogger(__name__)


class TestMetadataAPI(BaseIntegrationTest):
    """Integration tests for the metadata API."""

    def test_get_metadata(self):
        """Test get_metadata method."""
        # Get metadata
        metadata = self.run_async(self.client.get_metadata())

        # Check response
        self.assertResponseSuccess(metadata)

        # Check data
        data = metadata.get("data", {})
        self.assertIn("contractList", data)
        self.assertIsInstance(data["contractList"], list)

        # Store contract list for other tests
        self.__class__.test_data["contract_list"] = data["contractList"]

        # Log contract count
        logger.info(f"Found {len(data['contractList'])} contracts")

    def test_get_server_time(self):
        """Test get_server_time method."""
        # Get server time
        server_time = self.run_async(self.client.get_server_time())

        # Check response
        self.assertResponseSuccess(server_time)

        # Check data
        data = server_time.get("data", {})
        # The API returns 'timeMillis' instead of 'serverTime'
        if "timeMillis" in data:
            self.assertIsInstance(data["timeMillis"], (int, str))
            logger.info(f"Server time: {data['timeMillis']}")
        elif "serverTime" in data:
            self.assertIsInstance(data["serverTime"], (int, str))
            logger.info(f"Server time: {data['serverTime']}")
        else:
            self.fail("Neither 'timeMillis' nor 'serverTime' found in response data")

    def test_contract_exists(self):
        """Test that the test contract exists in the contract list."""
        # Get contract list (fetch if not available)
        if "contract_list" not in self.__class__.test_data:
            # Fetch metadata to get contract list
            metadata = self.run_async(self.client.get_metadata())
            self.assertResponseSuccess(metadata)
            data = metadata.get("data", {})
            self.assertIn("contractList", data)
            self.__class__.test_data["contract_list"] = data["contractList"]

        # Get contract list
        contract_list = self.__class__.test_data["contract_list"]

        # Check if test contract exists
        contract = None
        for c in contract_list:
            if c.get("contractId") == TEST_CONTRACT_ID:
                contract = c
                break

        # Assert contract exists
        self.assertIsNotNone(contract, f"Test contract {TEST_CONTRACT_ID} not found in contract list")

        # Store contract for other tests
        self.__class__.test_data["test_contract"] = contract

        # Log contract details
        logger.info(f"Found test contract: {contract.get('contractId')}")


if __name__ == "__main__":
    unittest.main()
