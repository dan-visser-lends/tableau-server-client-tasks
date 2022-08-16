
def send_email(mcontentssage, recipient, server): 



def send_notification(server_url, site_name, username, password, action, workbooks):

    tableau_auth = TSC.TableauAuth(username, password, site_name)
    server = TSC.Server(server_url, use_server_version=True)
    with server.auth.sign_in(tableau_auth):

        for wb in TSC.Pager(server.workbooks):

            if wb._webpage_url.split("workbooks/")[1] in workbooks:

                content = f"Hey! Your workbook {wb.name} in about about to be {action}. Please either view it in the next week to prevent this, or if you do not have access, please email the server administrator."

                send_email(content, recipient, server)


