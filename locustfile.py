#
# Locust test file
#

from locust import HttpLocust, TaskSet, task
from threading import Lock

import os
import time
import uuid
import locust
import urllib
import requests, json

class UserBehavior(TaskSet):
    def __init__(self, *args, **kwargs):
        super(UserBehavior, self).__init__(*args, **kwargs)

        self.random_id = str(uuid.uuid4())
        self.config  = self.locust.config
        self.authorization = ""
        self.expires_on = 0

        print "-------------------------------------------- [2]"
        print "User init " + self.random_id + " ..."

    def on_start(self):
        print "-------------------------------------------- [3]"
        print "User startup " + self.random_id + " ..."
        (self.authorization, self.expires_on) = self.get_update_token()

    def get_update_token(self, expires_on = None):
        """
        If expires_on is not the same with the shared expires_on, get a new token.
        If access_token is not in the shared config, get a new token
        """
        generate_new_token = False
        l.acquire()
        if "access_token" not in self.config:
            generate_new_token = True
        elif expires_on is not None and "expires_on" in self.config and (expires_on == self.config["expires_on"]):
            generate_new_token = True
        if generate_new_token == True:
            (self.config["access_token"], self.config["expires_on"]) = self.login()

        authorization = "Bearer {0}".format(self.config["access_token"])
        l.release()

        return (authorization, self.config["expires_on"])

    def login(self):
        """
        Get a authorization token from microsoft OAuth2
        """
        url = "https://login.windows.net/{0}/oauth2/token".format(self.config["tenant_id"])

        querystring = {"api-version":"1.0"}

        payload = "grant_type=client_credentials&resource=https%3A%2F%2Fmanagement.azure.com%2F&client_secret={0}&client_id={1}".format(
            urllib.quote_plus(self.config["client_secret"]),
            urllib.quote_plus(self.config["client_id"]))
        headers = {
            'content-type': "application/x-www-form-urlencoded",
            'cache-control': "no-cache",
            'postman-token': "c4a0450e-a952-ce31-7371-20791123a315"
        }

        response = self.client.request("POST", url, data=payload, headers=headers, params=querystring)
        response_json = json.loads(response.text)

        return (response_json["access_token"], int(response_json["expires_on"]))

    @task(1)
    def get_pipeline(self):
        timestamp = int(time.time())
        expires_in = self.config["expires_on"] - timestamp

        #print "User: " + self.random_id + ", Expires in: " + str(expires_in) + ", Token: " + self.authorization
        url = "https://management.azure.com/subscriptions/{0}/resourcegroups/{1}/providers/Microsoft.DataFactory/factories/{2}/pipelines/{3}".format(
                    self.config["subscription_id"],
                    self.config["resourcegroup_id"],
                    self.config["datafactory_id"],
                    self.config["pipeline_id"])
        querystring = {"api-version":"2017-03-01-preview"}
        headers = {
            'authorization': self.authorization,
            'x-ms-datafactory-appmodel': "datafactoryV2",
            'pragma': "no-cache",
            'content-type': "application/json",
            'cache-control': "no-cache",
            'postman-token': "fcd30d9d-8ba2-b091-1a0f-02acf1faadec"
        }

        response = self.client.request("GET", url, headers=headers, params=querystring, name = "/factories/pipelines[id]")

def application_startup():
    print "-------------------------------------------- [1]"
    print "Application Startup ..."


class WebsiteUser(HttpLocust):
    task_set = UserBehavior
    min_wait = 250
    max_wait = 1000

    config = {}
    config["tenant_id"] = os.environ["TENANT_ID"]
    config["subscription_id"] = os.environ["SUBSCRIPTION_ID"]
    config["client_id"] = os.environ["CLIENT_ID"]
    config["client_secret"] = os.environ["CLIENT_SECRET"]

    config["datafactory_id"] = os.environ["DATAFACTORY_ID"]
    config["resourcegroup_id"] = os.environ["RESOURCEGROUP_ID"]
    config["pipeline_id"] = os.environ["PIPELINE_ID"]

l = Lock()
locust.events.locust_start_hatching += application_startup
