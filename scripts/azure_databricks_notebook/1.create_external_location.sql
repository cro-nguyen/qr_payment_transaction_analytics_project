-- Create the external location required for this project in Azure Databricks SQL Notebook

-- Bronze
-- Silver

create external location databricks_vnpay_project_ext_bronze
  url 'abfss://bronze@vnpayproject.dfs.core.windows.net/'
  with (storage credential `databricks-vnpay-project-storage-credential`);

DESC EXTERNAL LOCATION databricks_vnpay_project_ext_bronze;

%fs
ls "abfss://bronze@vnpayproject.dfs.core.windows.net/"

create external location databricks_vnpay_project_ext_silver
  url 'abfss://silver@vnpayproject.dfs.core.windows.net/'
  with (storage credential `databricks-vnpay-project-storage-credential`);

