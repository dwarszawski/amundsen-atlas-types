import os

from atlasclient.client import Atlas


class AtlasClient:
    host = os.environ.get('ATLAS_HOST', 'localhost')
    port = os.environ.get('ATLAS_PORT', 21000)
    user = os.environ.get('ATLAS_USERNAME', 'admin')
    password = os.environ.get('ATLAS_PASSWORD', 'admin')
    timeout = os.environ.get('ATLAS_REQUEST_TIMEOUT', 10)

    def driver(self):
        return Atlas(host=self.host,
                     port=self.port,
                     username=self.user,
                     password=self.password,
                     timeout=self.timeout)


driver = AtlasClient().driver()
