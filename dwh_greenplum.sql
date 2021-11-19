CREATE DATABASE FP_DWH;

create table if not exists dim_clients (
    client_id int, 
    fullname varchar(30), 
    location_area_id int
) with (appendoptimized=true, orientation=column, compresstype=zlib, compresslevel=5)
distributed by (client_id, location_area_id);

