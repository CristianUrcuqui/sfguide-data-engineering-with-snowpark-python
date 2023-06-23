# Importa las librerías necesarias.
import time
from snowflake.snowpark import Session

# Define las tablas que vamos a usar.
POS_TABLES = ['country', 'franchise', 'location', 'menu', 'truck', 'order_header', 'order_detail']
CUSTOMER_TABLES = ['customer_loyalty']

# Crea un diccionario con los esquemas y las tablas correspondientes.
TABLE_DICT = {
    "pos": {"schema": "RAW_POS", "tables": POS_TABLES},
    "customer": {"schema": "RAW_CUSTOMER", "tables": CUSTOMER_TABLES}
}

def load_raw_table(session, tname=None, s3dir=None, year=None, schema=None):
    # Establece el esquema a usar en la sesión de Snowflake.
    session.use_schema(schema)
    
    # Configura la ubicación del archivo en base al año.
    if year is None:
        location = "@external.frostbyte_raw_stage/{}/{}".format(s3dir, tname)
    else:
        print('\tLoading year {}'.format(year)) 
        location = "@external.frostbyte_raw_stage/{}/{}/year={}".format(s3dir, tname, year)
    
    # Carga los datos del archivo en un DataFrame.
    df = session.read.option("compression", "snappy").parquet(location)
    
    # Copia los datos del DataFrame en la tabla de Snowflake.
    df.copy_into_table("{}".format(tname))

def load_all_raw_tables(session):
    # Aumenta el tamaño del almacén de Snowflake para mejorar el rendimiento.
    _ = session.sql("ALTER WAREHOUSE HOL_WH SET WAREHOUSE_SIZE = XLARGE WAIT_FOR_COMPLETION = TRUE").collect()

    # Carga todas las tablas en Snowflake.
    for s3dir, data in TABLE_DICT.items():
        tnames = data['tables']
        schema = data['schema']
        for tname in tnames:
            print("Loading {}".format(tname))
            if tname in ['order_header', 'order_detail']:
                for year in ['2019', '2020', '2021']:
                    load_raw_table(session, tname=tname, s3dir=s3dir, year=year, schema=schema)
            else:
                load_raw_table(session, tname=tname, s3dir=s3dir, schema=schema)

    # Devuelve el tamaño del almacén de Snowflake a su tamaño original.
    _ = session.sql("ALTER WAREHOUSE HOL_WH SET WAREHOUSE_SIZE = XSMALL").collect()

def validate_raw_tables(session):
    # Verifica los nombres de las columnas de las tablas.
    for tname in POS_TABLES:
        print('{}: \n\t{}\n'.format(tname, session.table('RAW_POS.{}'.format(tname)).columns))

    for tname in CUSTOMER_TABLES:
        print('{}: \n\t{}\n'.format(tname, session.table('RAW_CUSTOMER.{}'.format(tname)).columns))


# Esta parte del código se ejecuta si el script se ejecuta directamente (no se importa como módulo).
if __name__ == "__main__":
    import os, sys
    current_dir = os.getcwd()
    parent_dir = os.path.dirname(current_dir)
    sys.path.append(parent_dir)

    from utils import snowpark_utils
    # Obtiene la sesión de Snowpark.
    session = snowpark_utils.get_snowpark_session()

    # Llama a las funciones para cargar y validar las tablas.
    load_all_raw_tables(session)
    #validate_raw_tables(session)

    # Cierra la sesión.
    session.close()
