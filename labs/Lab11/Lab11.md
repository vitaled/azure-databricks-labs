# Lab 11: Creating a Dedicated SQL Pool

## Goal 
During this lab you will learn how to create a dedicated sql pool in Synapse through the *Synaose Studio UI*

## Tasks

### Task 1: Creating the Databricks Workspace

Log in to the Azure Portal: [https://portal.azure.com](https://portal.azure.com)

In the Azure Synapse Studio UI click on **Manage** in the left-sided menu

<img src="Lab11_01.jpg" alt="Click on Manage" width="500"/>

In the *Manage* section click on *SQL Pools* and then on **+ New** 

Search for *Azure Synapse Analytics* in the search form and press enter

<img src="Lab11_02.jpg" alt="Search for Azure Synapse Analytics" width="500"/>

In the creation wizard provide the following details 

<img src="Lab11_03.jpg" alt="Click on create" width="500"/>

| Property       | Description                                                                                                                                     | Example                             |
| -------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------- |
| Dedicated SQL pool name | Provide a name for your Dedicated SQL Pool. You can use any name you want | dsqlpool001              |
| Performance level         | Select a performance level for your dedicated sql pool                                                                          | DW100c                         |

then click on **Review + Create**

In the next page: 

<img src="Lab11_04.jpg" alt="Finished deployment" width="500"/>

click on **Create**

This will start the creation of your *dedicated sql pool*. The deployment can take few minutes. When the deployment process finish you will see a green sign in the Status column for the newly created SQL pool

<img src="Lab11_06.jpg" alt="Finished deployment" width="500"/>

This Lab has been completed!