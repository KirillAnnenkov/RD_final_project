# %%
import function.get_config as get_config

# import function.libs as libs

# Получить список таблиц для выгрузки из конфига
tables = get_config.config('./config/config_fp.json',"db_tables")

# %%

for table in tables[0:1]:
    libs.db_export_bronze(table_name = table)
    print(table='aisles')

# %%
