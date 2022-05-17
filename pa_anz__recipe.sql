-- query to output the csv which is used to update the uploads.pa_anz__recipe
-- to be run in RDS

-- cogs_upload.csv

select
    case
        when r.brand = 'EP' then 'AO'
        else r.country
        end country
     ,hf_week
     ,recipe_slot
     ,recipe_id
     ,is_mod_parent
     ,is_mod_sku
     ,mod_parent_id
     ,mod_sku_code
     ,mod_type
     ,recipe_size
     ,sku_code
     ,unit_size
     ,uom
     ,weight
     ,qty_2ppl
     ,qty_4ppl
     ,supplier_id
     ,supplier_split
     ,sku_unit_price
     ,sku_labor_price
     ,sku_packaging_price
     ,dc
     ,market
     ,quantity
     ,weight_in_kg
     ,total_price
     ,total_cost
from cogs.recipe r
where r.version = 4
  and r.hf_week >= '2020-W01'
order by r.country, r.hf_week, r.recipe_size, r.recipe_slot







-- query to update the table after dropping the CSV in CyberDuck
-- to be run in DWH

drop table if exists uploads.pa_anz__recipe;
create external table uploads.pa_anz__recipe
(country String
    ,hf_week String
    ,recipe_slot INT
    ,recipe_id String
    ,is_mod_parent INT
    ,is_mod_sku INT
    ,mod_parent_id INT
    ,mod_sku_code String
    ,mod_type String
    ,recipe_size INT
    ,sku_code String
    ,unit_size FLOAT
    ,uom STRING
    ,weight FLOAT
    ,qty_2ppl INT
    ,qty_4ppl INT
    ,supplier_id INT
    ,supplier_split FLOAT
    ,sku_unit_price FLOAT
    ,sku_labor_price FLOAT
    ,sku_packaging_price FLOAT
    ,dc String
    ,market String
    ,quantity FLOAT
    ,weight_in_kg FLOAT
    ,total_price FLOAT
    ,total_cost FLOAT)
row format delimited fields terminated by ','
location 's3a://hf-bi-dwh-uploader/pa_anz__recipe/'
tblproperties("skip.header.line.count"="1");

SELECT *
FROM uploads.pa_anz__recipe
where hf_week >= '2022-W17'