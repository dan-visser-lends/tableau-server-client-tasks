####
# This script downloads a workbook to sharepoint and deletes the workbook
# off the server.
#
# To run the script, you must have installed Python 2.7.9 or later,
# plus the 'requests' library:
#   http://docs.python-requests.org/en/latest/
#
# The script takes in the server address and username as arguments,
# where the server address has no trailing slash (e.g. http://localhost).
# Run the script in terminal by entering:
#   python publish_sample.py <server_address> <username>
#
# When running the script, it will prompt for the following:
# 'Name of workbook to move':        Enter name of workbook to move
# 'Destination server':              Enter name of server to move workbook into
# 'Destination server username':     Enter username to sign into destination server
# 'Password for source server':      Enter password to sign into source server
# 'Password for destination server': Enter password to sign into destination server
####

from version import VERSION
import requests # Contains methods used to make HTTP requests
import xml.etree.ElementTree as ET # Contains methods used to build and parse XML
import sys
import re
import getpass
import os

# The following packages are used to build a multi-part/mixed request.
# They are contained in the 'requests' library
from requests.packages.urllib3.fields import RequestField
from requests.packages.urllib3.filepost import encode_multipart_formdata

#####
# Move a specified workbook from a source server to a specified
# server's 'default' project by downloading workbook to a temp file.
#####

# The namespace for the REST API is 'http://tableausoftware.com/api' for Tableau Server 9.0
# or 'http://tableau.com/api' for Tableau Server 9.1 or later
xmlns = {'t': 'http://tableau.com/api'}

# The maximum size of a file that can be published in a single request is 64MB
FILESIZE_LIMIT = 1024 * 1024 * 64   # 64MB

# For when a workbook is over 64MB, break it into 5MB(standard chunk size) chunks
CHUNK_SIZE = 1024 * 1024 * 5    # 5MB

# If using python version 3.x, 'raw_input()' is changed to 'input()'
if sys.version[0] == '3': raw_input=input


class ApiCallError(Exception):
    pass


class UserDefinedFieldError(Exception):
    pass



def _check_status(server_response, success_code):
    """
    Checks the server response for possible errors.
    'server_response'       the response received from the server
    'success_code'          the expected success code for the response
    Throws an ApiCallError exception if the API call fails.
    """
    if server_response.status_code != success_code:
        parsed_response = ET.fromstring(server_response.text)

        # Obtain the 3 xml tags from the response: error, summary, and detail tags
        error_element = parsed_response.find('t:error', namespaces=xmlns)
        summary_element = parsed_response.find('.//t:summary', namespaces=xmlns)
        detail_element = parsed_response.find('.//t:detail', namespaces=xmlns)

        # Retrieve the error code, summary, and detail if the response contains them
        code = error_element.get('code', 'unknown') if error_element is not None else 'unknown code'
        summary = summary_element.text if summary_element is not None else 'unknown summary'
        detail = detail_element.text if detail_element is not None else 'unknown detail'
        error_message = '{0}: {1} - {2}'.format(code, summary, detail)
        raise ApiCallError(error_message)
    return


def sign_in(server, username, password, site=""):
    """
    Signs in to the server specified with the given credentials
    'server'   specified server address
    'username' is the name (not ID) of the user to sign in as.
               Note that most of the functions in this example require that the user
               have server administrator permissions.
    'password' is the password for the user.
    'site'     is the ID (as a string) of the site on the server to sign in to. The
               default is "", which signs in to the default site.
    Returns the authentication token and the site ID.
    """
    url = server + "/api/{0}/auth/signin".format(VERSION)

    # Builds the request
    xml_request = ET.Element('tsRequest')
    credentials_element = ET.SubElement(xml_request, 'credentials', name=username, password=password)
    ET.SubElement(credentials_element, 'site', contentUrl=site)
    xml_request = ET.tostring(xml_request)

    # Make the request to server
    server_response = requests.post(url, data=xml_request)
    _check_status(server_response, 200)

    # ASCII encode server response to enable displaying to console
    server_response = _encode_for_display(server_response.text)

    # Reads and parses the response
    parsed_response = ET.fromstring(server_response)

    # Gets the auth token and site ID
    token = parsed_response.find('t:credentials', namespaces=xmlns).get('token')
    site_id = parsed_response.find('.//t:site', namespaces=xmlns).get('id')
    user_id = parsed_response.find('.//t:user', namespaces=xmlns).get('id')
    return token, site_id, user_id


def sign_out(server, auth_token):
    """
    Destroys the active session and invalidates authentication token.
    'server'        specified server address
    'auth_token'    authentication token that grants user access to API calls
    """
    url = server + "/api/{0}/auth/signout".format(VERSION)
    server_response = requests.post(url, headers={'x-tableau-auth': auth_token})
    _check_status(server_response, 204)
    return


def download(server, auth_token, site_id, workbook_id):
    """
    Downloads the desired workbook from the server (temp-file).
    'server'        specified server address
    'auth_token'    authentication token that grants user access to API calls
    'site_id'       ID of the site that the user is signed into
    'workbook_id'   ID of the workbook to download
    Returns the filename of the workbook downloaded.
    """
    print("\tDownloading workbook to a temp file")
    url = server + "/api/{0}/sites/{1}/workbooks/{2}/content".format(VERSION, site_id, workbook_id)
    server_response = requests.get(url, headers={'x-tableau-auth': auth_token})
    _check_status(server_response, 200)

    # Header format: Content-Disposition: name="tableau_workbook"; filename="workbook-filename"
    filename = re.findall(r'filename="(.*)"', server_response.headers['Content-Disposition'])[0]
    with open(filename, 'wb') as f:
        f.write(server_response.content)
    return filename



def delete_workbook(server, auth_token, site_id, workbook_id, workbook_filename):
    """
    Deletes the temp workbook file, and workbook from the source project.
    'server'            specified server address
    'auth_token'        authentication token that grants user access to API calls
    'site_id'           ID of the site that the user is signed into
    'workbook_id'       ID of workbook to delete
    'workbook_filename' filename of temp workbook file to delete
    """
    # Builds the request to delete workbook from the source project on server
    url = server + "/api/{0}/sites/{1}/workbooks/{2}".format(VERSION, site_id, workbook_id)
    server_response = requests.delete(url, headers={'x-tableau-auth': auth_token})
    _check_status(server_response, 204)

    # Remove the temp file created for the download
    os.remove(workbook_filename)


def main():
    ##### STEP 0: Initialization #####
    if len(sys.argv) != 3:
        error = "2 arguments needed (server, username)"
        raise UserDefinedFieldError(error)
    source_server = sys.argv[1]
    source_username = sys.argv[2]
    workbook_name = raw_input("\nName of workbook to move: ")
    dest_server = raw_input("\nDestination server: ")
    dest_username = raw_input("\nDestination server username: ")

    print("\n*Moving '{0}' workbook to the 'default' project in {1}*".format(workbook_name, dest_server))
    source_password = getpass.getpass("Password for {0} on {1}: ".format(source_username, source_server))
    dest_password = getpass.getpass("Password for {0} on {1}: ".format(dest_username, dest_server))

    ##### STEP 1: Sign in #####
    print("\n1. Signing in to both sites to obtain authentication tokens")
    # Source server
    source_auth_token, source_site_id, source_user_id = sign_in(source_server, source_username, source_password)

    # Destination server
    dest_auth_token, dest_site_id, dest_user_id = sign_in(dest_server, dest_username, dest_password)

    ##### STEP 2: Find workbook id #####
    print("\n2. Finding workbook id of '{0}'".format(workbook_name))
    workbook_id = get_workbook_id(source_server, source_auth_token, source_user_id, source_site_id, workbook_name)

    ##### STEP 3: Find 'default' project id for destination server #####
    print("\n3. Finding 'default' project id for {0}".format(dest_server))
    dest_project_id = get_default_project_id(dest_server, dest_auth_token, dest_site_id)

    ##### STEP 4: Download workbook #####
    print("\n4. Downloading the workbook to move")
    workbook_filename = download(source_server, source_auth_token, source_site_id, workbook_id)

    ##### STEP 5: Publish to new site #####
    print("\n5. Publishing workbook to {0}".format(dest_server))
    publish_workbook(dest_server, dest_auth_token, dest_site_id, workbook_filename, dest_project_id)

    ##### STEP 6: Deleting workbook from the source site #####
    print("\n6. Deleting workbook from the original site and temp file")
    delete_workbook(source_server, source_auth_token, source_site_id, workbook_id, workbook_filename)

    ##### STEP 7: Sign out #####
    print("\n7. Signing out and invalidating the authentication token")
    sign_out(source_server, source_auth_token)
    sign_out(dest_server, dest_auth_token)


if __name__ == "__main__":
    main()