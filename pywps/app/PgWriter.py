import logging

import psycopg2
from osgeo import ogr, osr

# from pywps import Format, FORMATS   ImportError: cannot import name 'Format' ??
from pywps.configuration import get_config_value
from pywps import Process         # "from pywps.app import Process" throws an error: TypeError: module.__init__() takes at most 2 arguments (3 given)'
                                                # from pywps.app.Process import Process throws an error: ImportError: cannot import name 'Process'"
class PgWriter(object):
    def __init__(self, uuid, identifier, dbsettings): #dbsettings = db1 (nazev sekce z konfiguracniho souboru)
        self.dbname = get_config_value(dbsettings, "dbname")
        self.connstr = "dbname={} user={} password={} host={}".format(
            self.dbname,
            get_config_value(dbsettings, "user"), 
            get_config_value(dbsettings, "password"),
            get_config_value(dbsettings, "host")
        )

        self.schema_name = self.create_schema(identifier, uuid)
    def create_schema(self, identifier, uuid):
        schema_name = '{}_{}'.format(identifier.lower(),
                            str(uuid).replace("-", "_").lower()
            )
        try:
            conn = psycopg2.connect(self.connstr)
        except:
            raise Exception ("Database connection has not been established.")
        cur = conn.cursor()
        query = 'CREATE SCHEMA IF NOT EXISTS {};'.format(schema_name)  
        try:
            cur.execute(query)
        except:
            raise Exception("The query did not run succesfully.")
        conn.commit()
        cur.close()
        conn.close()
        return schema_name

    def store_output(self, file_name, identifier):
        #        try:
        logging.debug("Connect string: {}".format(self.connstr))
        dsc_in = ogr.Open("PG:" + self.connstr)
        if dsc_in is None:
            raise Exception("Reading data failed.")
        dsc_out = ogr.Open("PG:" + self.connstr)
        if dsc_out is None:
            raise Exception("Database connection has not been established.")
        layer = dsc_out.CopyLayer(dsc_in.GetLayer(), identifier, ['OVERWRITE=YES',
                                                  'SCHEMA={}'.format(self.schema_name)]
        )
        # TODO: layer is valid even copying failed (schema do not exists)
        if layer is None:
            raise Exception("Writing output data to database failed.")

        return identifier
            

    def store(self, outputs):
        for param in outputs:   
            self.store_output(param.file, param.identifier)
            param.data = '{}.{}.{}'.format(self.dbname, self.schema_name, param.identifier)
            
