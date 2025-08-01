-- Create Catalogs and Schemas required for the project

-- Catalog - vnpayproject_dev (Without managed location)
-- Schemas - bronze, silver (With managed location)

create catalog if not exists vnpayproject_dev;

use catalog vnpayproject_dev;

create schema if not exists bronze
  managed location "abfss://bronze@vnpayproject.dfs.core.windows.net/";

create schema if not exists silver
  managed location "abfss://silver@vnpayproject.dfs.core.windows.net/";

show schemas;
