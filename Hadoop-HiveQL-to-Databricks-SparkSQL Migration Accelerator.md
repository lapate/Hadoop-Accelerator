# Accelerator

## Efficient Solution Migration

### Discovery and Assessment

1. Todo - Steps - 1
    - Describe steps
      ![Tag](./images/ADLS-Provision-complete.png)
2. Todo - Steps - 2
3. Todo - Steps - 3
4. Congratulations!! At this point you have successfully completed discovery and produced analysis results

    ![Successful completion](./images/ADLS-Provision-complete.png)

### Explore existing Hadoop environment and run typical HiveQL queries

1. Todo - SetUP
   Before starting the workshop, set up the hadoop environment (cloudera) that you will use to migrate data to adls. To save costs, the environment consists of a single node cluster. This cluster runs Cloudera 5.16.2 and Kafka 3.1, to simulate a legacy system.

Perform the following tasks:

    1.Sign in to the Azure portal using a web browser.

    2.On the Home page, click Subscriptions.
    
    3.Make a note of the Subscription ID associated with your account.
    
    4.In the toolbar, click Cloud Shell.
    5.In the Cloud Shell dropdown list, select PowerShell. Click Confirm if prompted.
    6.In the Cloud Shell toolbar, select Open new session.
    7.In the Cloud Shell toolbar, select Open new session.
    8.In the Cloud Shell toolbar, select Open editor.
    9.In the Files pane of the editor, select clouderasetup.ps1 to open the setup script. In the script, replace <your-subscription-id> with your subscription id, and replace <SAS> with the SAS URL of the cloudera disk that will be used to create the virtual machine. Your instructor should provide you with this URL.
    10.Press CTRL-S to save the file, and then press CTRL-Q to leave the editor.
    11.Run the script with the following command:
        
        As the script runs, you will see various messages when the resources are created. The script will take about 5 minutes to complete. When it has finished, it will display the IP address of the new virtual machine. Make a note of this address.

    12.Connect using SSH as the root user as shown below. Replace <ip address> with the IP address of the virtual machine. The password is Pa55w.rdDemo. Enter yes when prompted to connect.

        NOTE:

            You may need to wait for a minute while the virtual machine services start before continuing
    13.At the bash prompt, run the following command to set the password for the azureuser account. Provide a password of your own choosing. You'll use this account rather than root for running the Cloudera services.

        passwd azureuser
    14.Run the following command to sign out from the virtual machine and return to the PowerShell prompt:

        exit
    15.In the Azure portal, close the PowerShell pane.

    16.On the desktop, open a Web browser, and navigate to the URL <ip-address>:7180, where <ip-address> is the IP address of the virtual machine you noted earlier. You should see the Cloudera Manager login page.
        NOTE: Again. you may need to wait for a minute while the Cloudera Manager is initialized.


    
            

      ![Tag](./images/ADLS-Provision-complete.png)
    2. Todo - Steps - 2
    3. Todo - Steps - 3
4. Congratulations!! At this point you have successfully setup Cloudera Hadoop cluster. Mounted raw dataset and explored several typical queries

    ![Successful completion](./images/ADLS-Provision-complete.png)

### Migrate raw data from Hadoop File System to Azure Data Lake Storage gen2 using Azure Data Factory

1. Todo - Steps - 1
    - Describe steps
      ![Tag](./images/ADLS-Provision-complete.png)
2. Todo - Steps - 2
3. Todo - Steps - 3
4. Congratulations!! At this point you have successfully migrated data from Cloudera Hadoop Cluster to Azure Data Lake Storage using Azure Data Factory

    ![Successful completion](./images/ADLS-Provision-complete.png)

### Migrate raw data from Hadoop File System to Azure Data Lake Storage Gen2 using WanDisco partner solution

1. Todo - Steps - 1
    - Describe steps
      ![Tag](./images/ADLS-Provision-complete.png)
2. Todo - Steps - 2
3. Todo - Steps - 3
4. Congratulations!! At this point you have successfully migrated data from Cloudera Hadoop Cluster to Azure Data Lake Storage using WanDisco partner solution

    ![Successful completion](./images/ADLS-Provision-complete.png)

### Setup DataBricks environment and run matched SparkSQL queries. Explore the results

1. Todo - Steps - 1
    - Describe steps
      ![Tag](./images/ADLS-Provision-complete.png)
2. Todo - Steps - 2
3. Todo - Steps - 3
4. Congratulations!! At this point you have successfully setup DataBricks cluster. Mounted raw dateset as Azure Data Lake Storage (ADLS) GEN2 and explored the queries converted from HiveQL

    ![Successful completion](./images/ADLS-Provision-complete.png)
