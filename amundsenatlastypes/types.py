import pkg_resources

column_profile = pkg_resources.resource_string(__name__, "schema/column_profile.json").decode('utf-8')
