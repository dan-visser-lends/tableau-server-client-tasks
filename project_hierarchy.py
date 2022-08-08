
from functools import partial
import tableauserverclient as TSC
import pandas as pd
from pathlib import Path
import conf as config 
import os

password = config.conf["password"]
server_url = config.conf["server"]
site_name = config.conf["site"]
username = config.conf["username"]

def getParentName(project_name): 
    project =  server.projects.filter(name=project_name)[0]
    
    for pr in TSC.Pager(server.projects):
        
        if pr.id == project.parent_id:

            return pr.name 

def getProjects():
    # Get all projects
    all_projects = list(TSC.Pager(server.projects))
    project_list = []
    for project in all_projects:
        project_list.append((project.name, project.id, project.parent_id))
    # Create dataframe of relevant attributes
    project_df = pd.DataFrame(data=project_list, columns=['ProjectNM', 'ProjectID', 'ParentProjectID'])
    # Map project to parent project 
    project_to_parent = {project.name: getParentName(project.name) for project in all_projects}
    # Map project to name
    project_to_name = {project.id: project.name for project in all_projects}
    return project_df, project_to_parent, project_to_name

def getHierarchy(project_id, project_to_parent, parent_list=None):
    if parent_list == None:
        parent_list = []
    parent_list.append(project_id)
    # reset project id to parent project id
    project_id = project_to_parent[project_id]
    # if there's a parent id, loop through again
    if project_id:
        getHierarchy(project_id, parent_list)

def getProjectHierarchy(project_df,):
    project_df['PathDSC'] = project_df.ProjectNM.apply(getHierarchy)
    # project_df['PathLevelNBR'] = project_df.apply(lambda row: len(row.PathDSC), axis=1)
    # project_df.sort_values(by='PathLevelNBR', ignore_index=True, inplace=True)
    return project_df

if __name__ == '__main__':

    tableau_auth = TSC.TableauAuth(username, password, site_name)
    server = TSC.Server(server_url, use_server_version=True)

    with server.auth.sign_in(tableau_auth):
        server.use_server_version()
        project_df, project_to_parent, project_to_name = getProjects()
        hierarchy_df = getProjectHierarchy(project_df)

        # Get current path of script
        filepath = Path(Path(__file__).parent) / 'zzzOutputs'
        filepath.mkdir(exist_ok=True)
        filename = filepath / 'ProjectHierarchy.csv'
        hierarchy_df.to_csv(filename, index=False)