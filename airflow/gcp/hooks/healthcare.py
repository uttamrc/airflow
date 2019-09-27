# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
This module contains a Google Cloud Healthcare Hook.
"""

from enum import Enum
from functools import wraps
import re
import time
from typing import Union, Dict, Optional, Callable, List

from google.api_core.retry import Retry
from googleapiclient.discover import Resource
from googleapiclient.http import HttpRequest

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.utils.log.logging_mixin import LoggingMixin


class GCPHealthcareHook(GoogleCloudBaseHook):
    """
    Hook for Google Cloud Healthcare API.
    https://cloud.google.com/healthcare/docs/apis

    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """

    def __init__(self, 
                 project_id: str,
                 region: str,
                 gcp_conn_id: str = "google_cloud_default", 
                 delegate_to: Optional[str] = None 
                 ) -> None:
        super().__init__(gcp_conn_id, delegate_to)
        self._client = None
        self.datasets_endpoint =  None
        self.project_id = project_id
        self.region = region
        

    def get_conn(self) -> Resource:
        """
        Retrieves connection to Cloud Speech.

        :return:  Resource object for interacting with the Healthcare API.
        :rtype: googleapiclient.discover.Resource
        """
        if not self._client:
            api_scopes = ['https://www.googleapis.com/auth/cloud-platform']
            api_version = 'v1beta1'
            discovery_api = 'https://healthcare.googleapis.com/$discovery/rest'
            service_name = 'healthcare'
            credentials = self._get_credentials()
            scoped_credentials = credentials.with_scopes(api_scopes)

            discovery_url = '{}?labels=CHC_BETA&version={}'.format(
                discovery_api, api_version)
            self._client = discovery.build(
                service_name,
                api_version,
                discoveryServiceUrl=discovery_url,
                credentials=scoped_credentials)

        return self._client

    @property
    def datasets_endpoint(self):
        """
        Returns endpoint for Healthcare Datasets for this hooks project / region.
        """
        if not self.datasets_endpoint:
            self.datasets_endpoint = self.get_conn().projects().locations().datasets()
        return  self.datasets_endpoint

    @value.setter
    def datasets_endpoint(self, endpoint):
        """
        Sets endpoint for Healthcare Datasets for this hooks project / region.
        """
        self.datasets_endpont = endpoint

    #TODO(jaketf) DICOM import / export w/ lro decortator
    #TODO(jaketf) FHIR import / export w/ lro decortator
    
    @_handle_long_running_operation
    def create_dataset(self, dataset_id: str, 
                       standard: Optional[HealthcareDatasetStandard]=None):
        """
        Creates a GCP Healthcare Dataset.
        
        :param dataset_id: ID for the Healthcare Dataset.
        :type dataset_id: str
        :param standard: Healthcare data standard for this dataset.
        :type standard: HealthcareDatasetStandard
        """
        dataset = GCPHealthcareDataset(self.project_id, self.region,
                                       dataset_id, standard)
        body = {}
        self.get_conn()
        request = self.datasets_endpoint.create(
            parent=dataset.get_parent_repr(),
            body=body,
            datasetId=dataset.dataset_id
        )

        return handle_logged_request(request, info_msg='Creating dataset: {}',
                                     replacements=[dataset]))
        

    def delete_dataset(self, dataset_id: str) -> None:
        """
        Deletes a GCP Healthcare Dataset.
        
        :param dataset_id: ID for the Healthcare Dataset.
        :type dataset_id: str
        :param standard: Healthcare data standard for this dataset.
        :type standard: HealthcareDatasetStandard
        """
        dataset = GCPHealthcareDataset(self.project_id, self.region,
                                       dataset_id, standard)
        body = {}
        request = self.datasets_endpoint.delete(
            parent=dataset.get_parent_repr(),
            body=body,
            datasetId=dataset.dataset_id
        )

        handle_logged_request(request, info_msg='Deleted dataset: {}',
                                     replacements=[dataset])
         
    def get_dataset(self, dataset_id: str) -> Dict[str, str]:
        """
        Gets an existing GCP Healthcare Dataset.
        
        :param dataset_id: ID for the Healthcare Dataset.
        :type dataset_id: str
        :param standard: Healthcare data standard for this dataset.
        :type standard: HealthcareDatasetStandard
        """
        dataset = GCPHealthcareDataset(self.project_id, self.region,
                                       dataset_id, standard)
        body = {}
        self.get_conn()
        request = self.datasets_endpoint.create(
            parent=dataset.get_parent_repr(),
            body=body,
            datasetId=dataset.dataset_id
        )

        return handle_logged_request(request, info_msg='Got dataset: {}',
                                     replacements=[dataset])

    @_handle_long_running_operation
    def deidentify_dataset(self, dataset_id: str, dest_dataset_id: str
                           config: Dict): 
        """
        Creates a new dataset containing de-identified data from the source.
        :param dataset_id: ID for the source Healthcare Dataset.
        :type dataset_id: str
        :param dest_dataset_id: ID for the destination Healthcare Dataset.
        :type dataset_id: str
        :param config: This should be a DeidentifyConfig deserialized as dict.
            https://cloud.google.com/healthcare/docs/reference/rest/v1beta1/projects.locations.datasets/deidentify#DeidentifyConfig
        :type config: dict
        """
        source =  self.get_dataset(dataset_id)
        dest = GCPHealthcareDataset(self.project_id, self.region, 
                                    dest_dataset_id)
        body = {
            'destinationDataset': dest.id,
            'config': config
        }
    
        request = self.datasets_endpoint.deidentify(
            sourceDataset=source.id, body=body
        )

        return handle_logged_request(request, 
                                    info_msg=\
            """
            De-identifying data in {} and written to {}.
            """, replacements=[dataset_id, dest_dataset_id])

        


    def list_datasets(self) -> List[str]:
        """
        Lists existing GCP Healthcare Datasets.
        
        :param dataset_id: ID for the Healthcare Dataset.
        :type dataset_id: str
        :param standard: Healthcare data standard for this dataset.
        :type standard: HealthcareDatasetStandard
        """
        dataset = GCPHealthcareDataset(self.project_id, self.region,
                                       dataset_id, standard)
        body = {}
        self.get_conn()
        request = self.datasets_endpoint.list(
            parent=dataset.get_parent_repr(),
        )

        return handle_logged_request(request, info_msg='Listing dataset: {}',
                                     replacements=[dataset]).get('datasets', [])


class GCPHealthcareDataset:
    #TODO(jaketf) doc string 
    def __init__(
        project_id: str
        region: str
        dataset_id: str
        standard: Optional[HealthcareDatasetStandard] = None):
    
        self.project_id = project_id
        self.region = region
        self.dataset_id = dataset_id
        self._standard = standard

    def get_parent_repr(self):
        """
        Returns the parent api path. 
        :return: string in 'projects/{}/locations/{}/datasets/{}'.
        :rtype: str
        """
        return 'projects/{}/locations/{}/}'.format(
                self.project_id, self.region)
        
    def full_api_repr(self):
        """
        Returns the api path. 
        :return: string in 'projects/{}/locations/{}/datasets/{}'.
        :rtype: str
        """
        return 'projects/{}/locations/{}/datasets/{}'.format(
                self.project_id, self.region, self.dataset_id)

    @property
    def id(self):
        return self.dataset_id 

    @property
    def standard(self):
        return self._standard

    @value.setter
    def standard(self, standard: HealthcareDatasetStandard):
        self._standard = standard 


class HealthcareDatasetStandard(Enum):
    DICOM = 'dicom'
    FHIR  = 'fhir'
    HL7   = 'hl7v2' #TODO(jaketf) does this belong here?
    IMAGE = 'image'
    TEXT  = 'text'

def handle_logged_request(request: HTTPRequest, info_msg: Optional[str]=None,
                   replacements: Optional[Union[Dict, List]]=None):
    log = LoggingMixin().log
    try:
        response = request.execute()
        msg = ""
        if info_msg:
            if replacements: 
                if isinstance(replacements, dict):
                    msg = info_msg.format(**replacements)
                if isinstance(replacements, list):
                    msg = info_msg.format(*replacements)
        log.info(msg)
        return response
    except HttpError err:
        log.error('Error, failed request {} with error: {}'.format(
                  request))
        raise err

def _handle_long_running_operation(self, func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        result = func(args, kwargs)
        lro_name = result.get('name')
        if lro_name:
            lro = _LongRunningOperation(self.datasets_endpoint, lro_name, poll_sleep)
            return lro.wait_for_done()
        else: 
            Exception('Response did not return the expected "name" in: {}'.format(result))
    return wrapper

class _LongRunningOperation(LoggingMixin):
    def __init__(
        self,
        healthcare_datasets_endpoint: Any,
        name: str,
        poll_sleep: int = 10,
    ) -> None:

        opeartion_name_pattern = re.compile('projects/.+/locations/.+/datasets/.+/operations/.+') 
        if not operation_namepatter.match(name):
            raise ValueError('Operation name must match teh pattern 
                "projects/PROJECT_ID/locations/REGION/datasets/DATASET_ID/operations/OPERATION_ID"')
        name_list = name.split('/')
        self._name = name
        self._healthcare_datasets_endpoint = healthcare_datasets_endpoint
        self._operation_location = location
        self._operation_id = name_list[-1]
        self._dataset_id = name_list[-3]
        self._poll_sleep = poll_sleep

    def _get_healthcare_operation(self) -> Dict:
        """
        Helper method to get list of operations that start with operation name or id

        :return: list of operations including id's
        :rtype: list
        """
        return self._healthcare_datasets_endpoint.operations().get(
                    datasetId=self._dataset_id,
                    operationId=self._operation_id)

    def is_operation_running(self) -> bool:
        """
        Helper method to check if operation is still running in healthcare

        :return: True if operation is running.
        :rtype: bool
        """
        return not self._get_healthcare_operation.get('done')


    def check_healthcare_operation_state(self) -> bool:
        """
        Helper method to check the state of all operations in healthcare for this task
        if operation failed raise exception

        :return: True if operation is done.
        :rtype: bool
        :raise: Exception
        """
        
        op = self._get_healthcare_operation()
        err = op.get('error')
        code = err.get('code') 
        if code:
            if code == 0:
                self.log.info("Operation: %s Succeeded!", str(op))
                return True
            else:
                self.log.debug("Current operation: %s", str(op))
                raise Exception(
                    "Google Cloud Healthcare operation {} failed with error: {}".format(
                        op['name'], op['error']))
            return False

    def wait_for_done(self) -> bool:
        """
        Helper method to wait for result of submitted operation.

        :return: True if operation is done.
        :rtype: bool
        :raise: Exception
        """
        while True:
            if self._is_operation_running():
                time.sleep(self._poll_sleep)
            else:
                return self.check_healthcare_operation_state():
